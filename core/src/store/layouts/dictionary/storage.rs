//! How a resident dictionary's sorted terms are held and read: FSST or
//! plaintext term chunks, the cursors over them, the construction paths
//! (compression, adoption of a wire column) and the dictionary child's
//! write side.

use std::sync::Arc;

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::match_each_integer_ptype;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::io::read::scan_reader_chunks;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, buf_as_str};

use super::term_dict::TermDictionary;

/// The single column of the native container's `dictionary` child: non-nullable
/// utf8, row i holding the term with code i (sorted, so codes are lexicographic
/// ranks). Part of the wire contract, owned here — the module that builds and
/// reads the child — per the ownership rule in [`crate::store::schema`].
pub(crate) const COL_DICT_TERM: &str = "_dict_term";

/// Terms per FSST window when compressing at the source (see
/// [`TermDictionary::compress_windowed`]): the granularity at which a large
/// dictionary's serialized child is read back, point-read, and lifted
/// chunk-by-chunk. Small enough that touching one leaf fetches and adopts a
/// bounded slice of the column; large enough to amortize the copy of the
/// shared symbol table every window carries.
const DICT_CHUNK_ROWS: usize = 64 * 1024;

/// The chunks of a resident term column, with a cumulative-start table
/// mapping a global term index to (chunk, local index).
pub(super) struct ResidentChunks {
    pub(super) chunks: Vec<TermChunk>,
    /// `starts[i]` = global index of chunk i's first term; ascending.
    pub(super) starts: Vec<usize>,
    pub(super) len: usize,
}

impl ResidentChunks {
    /// The column `chunks` concatenate to, in order.
    pub(super) fn new(chunks: Vec<TermChunk>) -> Self {
        let mut starts = Vec::with_capacity(chunks.len());
        let mut len = 0usize;
        for chunk in &chunks {
            starts.push(len);
            len += chunk.len();
        }
        Self {
            chunks,
            starts,
            len,
        }
    }

    /// The chunk holding global index `i`, and `i` local to it.
    fn locate(&self, i: usize) -> (usize, usize) {
        chunk_of(&self.starts, i)
    }
}

/// One chunk of a term column, in the encoding it is held in.
pub(super) enum TermChunk {
    /// Plaintext terms. `bytes_at` is a zero-copy read.
    Canonical(VarBinViewArray),
    /// FSST-compressed terms: compact in memory, and every read decodes.
    Fsst(FsstTerms),
}

impl TermChunk {
    /// Adopt one term chunk: kept FSST when it arrived FSST (reads decode
    /// single rows), canonicalized otherwise (the write path compresses, but
    /// nothing in the format obliges a producer to have done so).
    pub(super) fn from_wire(chunk: ArrayRef, ctx: &mut vortex_array::ExecutionCtx) -> Result<Self> {
        match chunk.try_downcast::<FSST>() {
            Ok(fsst) => Ok(TermChunk::Fsst(FsstTerms::new(fsst)?)),
            Err(other) => Ok(TermChunk::Canonical(
                other
                    .execute::<VarBinViewArray>(ctx)
                    .map_err(VortexRdfError::Vortex)?,
            )),
        }
    }

    fn len(&self) -> usize {
        match self {
            TermChunk::Canonical(a) => a.len(),
            TermChunk::Fsst(f) => f.len(),
        }
    }

    /// The held column as an array, in its stored encoding.
    #[cfg(any(feature = "file-io", target_arch = "wasm32", test))]
    fn array(&self) -> ArrayRef {
        match self {
            TermChunk::Canonical(a) => a.clone().into_array(),
            TermChunk::Fsst(f) => f.array.clone().into_array(),
        }
    }

    /// A fresh cursor over this chunk's terms. Scratch is allocated lazily
    /// inside `bytes_at`, so a single-term decode allocates only for the
    /// chunk it touches.
    pub(super) fn cursor(&self) -> ChunkCursor<'_> {
        match self {
            TermChunk::Canonical(a) => ChunkCursor::Canonical(StrColReader::new(a)),
            TermChunk::Fsst(f) => ChunkCursor::Fsst {
                terms: f,
                scratch: Vec::new(),
            },
        }
    }
}

/// The chunk holding global index `i` of a chunked column whose chunks start
/// at `starts` (ascending, `starts[0] == 0`), and `i` local to that chunk.
pub(super) fn chunk_of(starts: &[usize], i: usize) -> (usize, usize) {
    let chunk = starts.partition_point(|&s| s <= i) - 1;
    (chunk, i - starts[chunk])
}

/// Reject a code outside a dictionary of `n_terms` terms.
pub(super) fn check_code(code: u32, n_terms: usize) -> Result<()> {
    if code as usize >= n_terms {
        return Err(VortexRdfError::Deserialization(format!(
            "Term code {} out of dictionary bounds ({})",
            code, n_terms
        )));
    }
    Ok(())
}

/// The `_dict_term` column of a dictionary child chunk, in its stored
/// encoding.
pub(super) fn term_column(
    chunk: ArrayRef,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<ArrayRef> {
    let struct_arr = chunk
        .execute::<StructArray>(ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(struct_arr
        .unmasked_field_by_name(COL_DICT_TERM)
        .map_err(VortexRdfError::Vortex)?
        .clone())
}

impl TermDictionary {
    /// A dictionary of no terms, held canonical: there is nothing to train
    /// FSST on.
    pub(crate) fn empty() -> Self {
        Self::new(ResidentChunks::new(vec![TermChunk::Canonical(
            VarBinViewArray::from_iter_str(std::iter::empty::<&str>()),
        )]))
    }

    /// Build from already-sorted unique term strings. The column is
    /// FSST-compressed here (see [`compress_windowed`](Self::compress_windowed)),
    /// so a built dictionary holds FSST chunks (windowed past
    /// [`DICT_CHUNK_ROWS`]) regardless of the encoding the writer later
    /// selects; only an empty dictionary stays canonical.
    pub(super) fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        Self::from_sorted_column(VarBinViewArray::from_iter_str(terms))
    }

    /// Build from an already-assembled column of sorted unique terms — the
    /// construction entry for callers that hold the plaintext column (the
    /// interning builder freezes one directly), and the single owner of the
    /// term-count guard and of the compression step.
    pub(crate) fn from_sorted_column(plain: VarBinViewArray) -> Result<Self> {
        // List offsets are i32, so the term count must fit in one.
        if plain.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                plain.len(),
                i32::MAX
            )));
        }
        Self::compress_windowed(plain, DICT_CHUNK_ROWS)
    }

    /// Adopt a term column's chunks through [`TermChunk::from_wire`]:
    /// already-FSST chunks are kept compressed, any other encoding is
    /// canonicalized.
    fn from_term_chunks(
        chunks: Vec<ArrayRef>,
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> Result<Self> {
        let mut adopted = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            adopted.push(TermChunk::from_wire(chunk, ctx)?);
        }
        if adopted.is_empty() {
            return Ok(Self::empty());
        }
        Ok(Self::new(ResidentChunks::new(adopted)))
    }

    /// FSST-compress a plaintext term column in windows of `window` terms:
    /// one symbol table trained on the whole column, each window compressed
    /// with it into a self-contained chunk that the serializer writes
    /// verbatim as one flat leaf. An empty column stays canonical: there is
    /// nothing to train a symbol table on.
    pub(super) fn compress_windowed(plain: VarBinViewArray, window: usize) -> Result<Self> {
        if plain.is_empty() {
            return Ok(Self::new(ResidentChunks::new(vec![TermChunk::Canonical(
                plain,
            )])));
        }
        let start = debug::timer();
        let len = plain.len();
        let array = plain.into_array();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let compressor = fsst_train_compressor(&array, &mut ctx).map_err(VortexRdfError::Vortex)?;
        let mut chunks = Vec::with_capacity(len.div_ceil(window));
        let mut at = 0usize;
        while at < len {
            let end = at.saturating_add(window).min(len);
            // Canonicalize the window before compressing: `fsst_compress`
            // requires a VarBinView, not the lazy wrapper `slice` returns.
            // The view copies share the parent's data buffers, so this is
            // per-window view headers, not a copy of the terms.
            let piece = array
                .slice(at..end)
                .map_err(VortexRdfError::Vortex)?
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?
                .into_array();
            let fsst =
                fsst_compress(&piece, &compressor, &mut ctx).map_err(VortexRdfError::Vortex)?;
            chunks.push(TermChunk::Fsst(FsstTerms::new(fsst)?));
            at = end;
        }
        log::debug!(
            "[Dictionary] FSST-compressed {} terms into {} windows in {:?}",
            len,
            chunks.len(),
            debug::elapsed(start)
        );
        Ok(Self::new(ResidentChunks::new(chunks)))
    }
}

/// Bytes an FSST symbol expands to at most — one code never yields more.
const FSST_SYMBOL_LEN: usize = 8;

/// Extra output headroom for `decompress_into`.
///
/// Its 8-symbols-at-a-time path only runs while the output has
/// `8 * FSST_SYMBOL_LEN` bytes of room left, so a buffer sized exactly to the
/// longest term silently falls back to the byte-at-a-time tail loop.
const FSST_DECODE_HEADROOM: usize = 8 * FSST_SYMBOL_LEN;

/// FSST-compressed sorted terms, with the pieces a hot lookup needs hoisted
/// out of the Vortex array.
pub(super) struct FsstTerms {
    /// Kept whole so the dictionary can be serialized without recompressing.
    array: FSSTArray,
    /// Code offsets, unpacked once at open so a per-row read is a slice
    /// index.
    offsets: Arc<[u32]>,
    /// Scratch size that keeps `decompress_into` on its fast path.
    scratch_cap: usize,
}

impl FsstTerms {
    fn new(array: FSSTArray) -> Result<Self> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let offsets = array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        // Width is whatever the writer's scheme selection produced — a small
        // dictionary's offsets fit in a u8 — so accept every integer type.
        // The u32 arm of the macro casts u32 -> u32; that is the price of one
        // arm covering every width.
        #[allow(clippy::unnecessary_cast)]
        let offsets: Arc<[u32]> = match_each_integer_ptype!(offsets.ptype(), |O| {
            offsets.as_slice::<O>().iter().map(|&o| o as u32).collect()
        });
        // An FSST code expands to at most one 8-byte symbol, so this bounds the
        // longest decoded term without decoding anything.
        let widest = offsets
            .windows(2)
            .map(|w| (w[1] - w[0]) as usize)
            .max()
            .unwrap_or(0);
        Ok(Self {
            array,
            offsets,
            scratch_cap: widest * FSST_SYMBOL_LEN + FSST_DECODE_HEADROOM,
        })
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn new_scratch(&self) -> Vec<u8> {
        Vec::with_capacity(self.scratch_cap)
    }

    /// Decode term `i` into `scratch`, returning the bytes written.
    fn decode_into<'a>(&self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
        let (start, end) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        let n = self.array.decompressor().decompress_into(
            &self.array.codes_bytes()[start..end],
            scratch.spare_capacity_mut(),
        );
        // SAFETY: `decompress_into` initialized the first `n` bytes.
        unsafe {
            scratch.set_len(n);
        }
        &scratch[..n]
    }
}

/// A cursor over a dictionary's terms.
///
/// `str_at` borrows from the cursor rather than from the dictionary because an
/// FSST read decodes into the cursor's own scratch buffer, so the borrow ends
/// at the next call. Callers needing several terms simultaneously — decoding a
/// quad's four roles — take one cursor per role.
pub(super) struct DictCursor<'a> {
    store: &'a ResidentChunks,
    /// One cursor per chunk, each with its own scratch, so a read maps
    /// the global index to its chunk and delegates.
    cursors: Vec<ChunkCursor<'a>>,
}

/// A cursor over one [`TermChunk`].
pub(super) enum ChunkCursor<'a> {
    Canonical(StrColReader<'a>),
    Fsst {
        terms: &'a FsstTerms,
        scratch: Vec<u8>,
    },
}

impl ChunkCursor<'_> {
    #[inline]
    pub(super) fn bytes_at(&mut self, local: usize) -> &[u8] {
        match self {
            ChunkCursor::Canonical(r) => r.bytes_at(local),
            ChunkCursor::Fsst { terms, scratch } => {
                // Allocated on first use: a windowed dictionary builds one
                // cursor per chunk, and most reads (binary-search probes,
                // single-term decodes) only ever touch a few of them.
                if scratch.capacity() == 0 {
                    *scratch = terms.new_scratch();
                }
                scratch.clear();
                terms.decode_into(local, scratch)
            }
        }
    }
}

impl<'a> DictCursor<'a> {
    pub(super) fn new(store: &'a ResidentChunks) -> Self {
        Self {
            store,
            cursors: store.chunks.iter().map(TermChunk::cursor).collect(),
        }
    }

    #[inline]
    pub(super) fn bytes_at(&mut self, i: usize) -> &[u8] {
        let (chunk, local) = self.store.locate(i);
        self.cursors[chunk].bytes_at(local)
    }

    #[inline]
    pub(super) fn str_at(&mut self, i: usize) -> Result<&str> {
        buf_as_str(self.bytes_at(i))
    }

    /// The index of the first term not below `needle` in byte order — the
    /// term count when every term is below it.
    pub(super) fn lower_bound(&mut self, needle: &[u8]) -> u32 {
        let (mut lo, mut hi) = (0usize, self.store.len);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.bytes_at(mid) < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo as u32
    }

    /// The index of the term spelled `needle`, `None` when absent: the lower
    /// bound and one equality probe.
    pub(super) fn search(&mut self, needle: &[u8]) -> Option<u32> {
        let lo = self.lower_bound(needle);
        ((lo as usize) < self.store.len && self.bytes_at(lo as usize) == needle).then_some(lo)
    }
}

impl TermDictionary {
    /// Read a dictionary child's whole term column into a resident
    /// dictionary, keeping each chunk's stored encoding (FSST where the
    /// writer compressed).
    ///
    /// Scans the child through [`scan_reader_chunks`] (inline, no runtime
    /// handle), so it serves the file-backed open, the buffered `open_buffer`
    /// open, and the wasm read path alike; each scan chunk's term column is
    /// adopted as one dictionary chunk.
    pub(crate) async fn from_child_reader(reader: vortex_layout::LayoutReaderRef) -> Result<Self> {
        if reader.row_count() == 0 {
            return Ok(Self::empty());
        }
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let mut chunks = Vec::new();
        for chunk in scan_reader_chunks(reader).await? {
            chunks.push(term_column(chunk, &mut ctx)?);
        }
        Self::from_term_chunks(chunks, &mut ctx)
    }
}

#[cfg(test)]
impl TermDictionary {
    /// The held term chunks as arrays, each in its stored encoding.
    pub(crate) fn term_chunks(&self) -> Vec<ArrayRef> {
        self.terms.chunks.iter().map(TermChunk::array).collect()
    }
}

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
impl TermDictionary {
    /// This dictionary as a native store component: the sorted term column,
    /// one chunk per held FSST window ([`child_chunks`](Self::child_chunks)),
    /// written verbatim through the pass-through strategy as the root's
    /// required `dictionary` child (see `container::dict_child_strategy`).
    pub(crate) fn to_write(&self) -> Result<crate::io::container::NativeComponentWrite> {
        use crate::io::container::{
            self, BufferedComponentSource, DICT_COMPONENT_NAME, NativeComponentWrite,
            StoreComponentDescriptor, StoreComponentRole,
        };
        let chunks = self.child_chunks()?;
        let dtype = chunks[0].dtype().clone();
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: DICT_COMPONENT_NAME.into(),
                role: StoreComponentRole::Dictionary,
                implementation: container::DICT_IMPLEMENTATION.into(),
                version: 1,
                required: true,
                sorted: true,
                dtype,
            },
            Arc::new(BufferedComponentSource::try_new(chunks).map_err(VortexRdfError::Vortex)?),
            container::dict_child_strategy(),
        )
        .map_err(VortexRdfError::Vortex)
    }

    /// The dictionary component's body, one `{_dict_term: utf8}` struct per
    /// held term chunk — row i of the concatenation = the term with code i,
    /// in the encoding the dictionary is held in (FSST when compressed at the
    /// source). Chunk-granular because each chunk is a self-contained array
    /// (independent FSST windows, see
    /// [`compress_windowed`](Self::compress_windowed)) written verbatim as
    /// one flat leaf, so its boundary survives as a split of the serialized
    /// child. Always at least one chunk, possibly empty — the child strategy
    /// needs a chunk to write a schema-complete component.
    pub(crate) fn child_chunks(&self) -> Result<Vec<ArrayRef>> {
        self.terms
            .chunks
            .iter()
            .map(|chunk| {
                let terms = chunk.array();
                let rows = terms.len();
                StructArray::try_new(
                    [COL_DICT_TERM].into(),
                    vec![terms],
                    rows,
                    Validity::NonNullable,
                )
                .map_err(VortexRdfError::Vortex)
                .map(|a| a.into_array())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Multi-window compression is invisible to lookups: a dictionary
    /// compressed in many small windows holds independent FSST chunks and
    /// answers exactly like the single-window form — every term, both
    /// directions, across window boundaries, and absent probes alike.
    #[test]
    fn windowed_compress_probe_parity() {
        let terms: Vec<String> = (0..1_000)
            .map(|i| format!("<http://example.org/term/{i:05}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let windowed = TermDictionary::compress_windowed(plain.clone(), 64).unwrap();
        assert_eq!(windowed.terms.chunks.len(), 1_000usize.div_ceil(64));
        assert_eq!(windowed.terms.len, 1_000);
        assert!(
            windowed
                .terms
                .chunks
                .iter()
                .all(|ch| matches!(ch, TermChunk::Fsst(_)))
        );
        let single = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        assert_eq!(single.terms.chunks.len(), 1);
        assert!(matches!(single.terms.chunks[0], TermChunk::Fsst(_)));
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(windowed.encode(term), Some(i as u32), "{term}");
            assert_eq!(windowed.decode(i as u32).as_deref(), Some(term.as_str()));
            assert_eq!(single.encode(term), Some(i as u32));
        }
        assert_eq!(windowed.encode("<http://absent>"), None);
        assert_eq!(windowed.decode(1_000), None);
    }

    /// A term column a producer wrote in plaintext is adopted canonical —
    /// as one chunk, and chunk by chunk when it arrives chunked — and
    /// answers exactly like a compressed dictionary would.
    #[test]
    fn plaintext_terms_adopt_canonical() {
        let terms: Vec<String> = (0..300)
            .map(|i| format!("<http://example.org/plain/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let mut ctx = VORTEX_SESSION.create_execution_ctx();

        let single =
            TermDictionary::from_term_chunks(vec![plain.clone().into_array()], &mut ctx).unwrap();
        assert_eq!(single.terms.chunks.len(), 1);
        assert!(matches!(single.terms.chunks[0], TermChunk::Canonical(_)));

        let column = plain.into_array();
        let pieces = vec![
            column.slice(0..120).unwrap(),
            column.slice(120..300).unwrap(),
        ];
        let chunked = TermDictionary::from_term_chunks(pieces, &mut ctx).unwrap();
        assert_eq!(chunked.terms.chunks.len(), 2);
        assert_eq!(chunked.terms.starts, vec![0, 120]);
        assert_eq!(chunked.terms.len, 300);
        assert!(
            chunked
                .terms
                .chunks
                .iter()
                .all(|ch| matches!(ch, TermChunk::Canonical(_)))
        );

        for (i, term) in terms.iter().enumerate() {
            for d in [&single, &chunked] {
                assert_eq!(d.encode(term), Some(i as u32), "{term}");
                assert_eq!(d.decode(i as u32).as_deref(), Some(term.as_str()));
            }
        }
        assert_eq!(chunked.encode("<http://absent>"), None);
        assert_eq!(chunked.decode(300), None);
    }
}
