//! How a resident [`TermDictionary`] holds its sorted terms ([`TermStore`])
//! and reads them (the cursors), how one is built (FSST compression) or
//! adopted from a written term column, and the dictionary child's read and
//! write sides, its zone-map bounds included.

#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use std::sync::Arc;

use vortex_array::arrays::primitive::PrimitiveArrayExt as _;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::PType;
use vortex_array::match_each_integer_ptype;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use vortex_buffer::Buffer;

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
/// [`TermDictionary::compress`]): the granularity at which a large
/// dictionary's serialized child is read back, point-read, and lifted
/// chunk-by-chunk. Small enough that touching one leaf fetches and adopts a
/// bounded slice of the column; large enough to amortize the copy of the
/// shared symbol table every window carries.
const DICT_CHUNK_ROWS: usize = 64 * 1024;

/// How a dictionary's sorted terms are held in memory.
///
/// The dictionary is *built* FSST-compressed (see [`TermDictionary::compress`])
/// and written out that way, so an FSST chunk is the normal case. A
/// [`TermChunk::Canonical`] chunk covers every other encoding: Vortex picks a
/// column's encoding when it writes, by sampling, and the selector is free to
/// choose something other than FSST — so a dictionary read back from a file
/// or IPC stream may arrive in any encoding, and the read path has to be
/// total over that. Anything that is not FSST is canonicalized to plaintext
/// on open.
pub(super) enum TermStore {
    /// One term chunk holding the whole column.
    Single(TermChunk),
    /// A multi-chunk term column (compressed in windows, or read back from a
    /// serialized dictionary child), each chunk kept in the encoding it was
    /// written in, so a large dictionary stays FSST-compressed through the
    /// resident lift.
    Chunked(ResidentChunks),
}

/// The chunks of a multi-chunk resident term column, with a cumulative-start
/// table mapping a global term index to (chunk, local index).
pub(super) struct ResidentChunks {
    pub(super) chunks: Vec<TermChunk>,
    /// `starts[i]` = global index of chunk i's first term; ascending.
    starts: Vec<usize>,
    pub(super) len: usize,
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
            Err(other) => Ok(TermChunk::Canonical(other.execute::<VarBinViewArray>(ctx)?)),
        }
    }

    pub(super) fn len(&self) -> usize {
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

    /// What this chunk costs a cache of chunks: the bytes it holds onto. An
    /// FSST chunk costs its code offsets (one `u32` per term, plus one),
    /// whether they view the leaf's own buffer or are a converted copy; its
    /// code bytes, which it only references, are not counted. A canonical
    /// chunk costs its array's `nbytes()`, which counts buffers that are
    /// slices of a map as well.
    #[cfg(feature = "file-io")]
    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            TermChunk::Canonical(a) => usize::try_from(a.nbytes()).unwrap_or(usize::MAX),
            TermChunk::Fsst(f) => f.offsets.len() * size_of::<u32>(),
        }
    }

    /// A fresh cursor over this chunk's terms. An FSST cursor's scratch
    /// starts empty and `bytes_at` grows it to the term it decodes, so a
    /// single-term decode allocates only for the chunk it touches.
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

impl ResidentChunks {
    /// The chunk holding global index `i`, and `i` local to it.
    fn locate(&self, i: usize) -> (usize, usize) {
        chunk_of(&self.starts, i)
    }
}

impl TermStore {
    /// Term `i`'s bytes: borrowed from a canonical chunk, or decoded into
    /// `scratch` from an FSST one. A read of one term at a time passes one
    /// scratch buffer through all its probes, where a cursor holds a buffer
    /// per chunk.
    pub(super) fn bytes_into<'a>(&'a self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
        let (chunk, local) = match self {
            TermStore::Single(chunk) => (chunk, i),
            TermStore::Chunked(c) => {
                let (k, local) = c.locate(i);
                (&c.chunks[k], local)
            }
        };
        match chunk {
            TermChunk::Canonical(a) => StrColReader::new(a).bytes_at(local),
            TermChunk::Fsst(f) => f.decode_into(local, scratch),
        }
    }
}

impl TermDictionary {
    /// A dictionary of no terms, held canonical: there is nothing to train
    /// FSST on.
    pub(crate) fn empty() -> Self {
        Self::new(TermStore::Single(TermChunk::Canonical(
            VarBinViewArray::from_iter_str(std::iter::empty::<&str>()),
        )))
    }

    /// Build from already-sorted unique term strings. The column is
    /// FSST-compressed here (see [`compress`](Self::compress)), so a built
    /// dictionary holds FSST chunks (windowed past [`DICT_CHUNK_ROWS`])
    /// regardless of the encoding the writer later selects; only an empty
    /// dictionary stays canonical.
    pub(super) fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        Self::from_sorted_column(VarBinViewArray::from_iter_str(terms))
    }

    /// Build from an already-assembled column of sorted unique terms — the
    /// construction entry for callers that hold the plaintext column (the
    /// interning builder freezes one directly), and the single owner of the
    /// term-count guard and of the compression step.
    pub(crate) fn from_sorted_column(plain: VarBinViewArray) -> Result<Self> {
        // A conservative cap, not a format limit: no dictionary past
        // `i32::MAX` terms has been built or tested, and nothing in the format
        // needs the count to fit an i32 (the column's length is a usize, each
        // FSST window's offsets span `DICT_CHUNK_ROWS` terms, and codes are
        // u64). This build holds every term in memory.
        if plain.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                plain.len(),
                i32::MAX
            )));
        }
        Self::compress(plain)
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
        let store = match adopted.len() {
            0 => return Ok(Self::empty()),
            1 => TermStore::Single(adopted.pop().expect("length checked above")),
            _ => {
                let mut starts = Vec::with_capacity(adopted.len());
                let mut len = 0usize;
                for chunk in &adopted {
                    starts.push(len);
                    len += chunk.len();
                }
                TermStore::Chunked(ResidentChunks {
                    chunks: adopted,
                    starts,
                    len,
                })
            }
        };
        Ok(Self::new(store))
    }

    /// FSST-compress a plaintext term column.
    ///
    /// An empty dictionary is left canonical: there is nothing to train a
    /// symbol table on, and `fsst_train_compressor` has no non-null rows to
    /// sample.
    ///
    /// A dictionary larger than [`DICT_CHUNK_ROWS`] is compressed in
    /// independent windows (one symbol table trained on the whole column,
    /// each window compressed with it): every window is a self-contained
    /// FSST array, so the serializer can write the chunks verbatim — no
    /// re-encoding, no slicing a parent array whose buffers every written
    /// chunk would then drag along — and the chunk boundaries become the
    /// file child's leaves, the granularity `FileBackedDict` point-reads.
    fn compress(plain: VarBinViewArray) -> Result<Self> {
        Self::compress_windowed(plain, DICT_CHUNK_ROWS)
    }

    /// [`compress`](Self::compress) with an explicit window, so tests can
    /// exercise the multi-window path without building 64 Ki terms.
    pub(super) fn compress_windowed(plain: VarBinViewArray, window: usize) -> Result<Self> {
        if plain.is_empty() {
            return Ok(Self::new(TermStore::Single(TermChunk::Canonical(plain))));
        }
        let start = debug::timer();
        let len = plain.len();
        let array = plain.into_array();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let compressor = fsst_train_compressor(&array, &mut ctx)?;
        if len <= window {
            let fsst = fsst_compress(&array, &compressor, &mut ctx)?;
            let terms = FsstTerms::new(fsst)?;
            log::debug!(
                "[Dictionary] FSST-compressed {} terms in {:?}",
                terms.len(),
                debug::elapsed(start)
            );
            return Ok(Self::new(TermStore::Single(TermChunk::Fsst(terms))));
        }
        let windows = len.div_ceil(window);
        let mut chunks = Vec::with_capacity(windows);
        let mut starts = Vec::with_capacity(windows);
        let mut at = 0usize;
        while at < len {
            let end = (at + window).min(len);
            // Canonicalize the window before compressing: `fsst_compress`
            // requires a VarBinView, not the lazy wrapper `slice` returns.
            // The view copies share the parent's data buffers, so this is
            // per-window view headers, not a copy of the terms.
            let piece = array
                .slice(at..end)?
                .execute::<VarBinViewArray>(&mut ctx)?
                .into_array();
            let fsst = fsst_compress(&piece, &compressor, &mut ctx)?;
            starts.push(at);
            chunks.push(TermChunk::Fsst(FsstTerms::new(fsst)?));
            at = end;
        }
        log::debug!(
            "[Dictionary] FSST-compressed {} terms into {} windows in {:?}",
            len,
            chunks.len(),
            debug::elapsed(start)
        );
        Ok(Self::new(TermStore::Chunked(ResidentChunks {
            chunks,
            starts,
            len,
        })))
    }
}

/// Bytes an FSST symbol expands to at most — one code never yields more.
const FSST_SYMBOL_LEN: usize = 8;

/// Extra output headroom for `decompress_into`.
///
/// Its 8-symbols-at-a-time path only runs while the output has
/// `8 * FSST_SYMBOL_LEN` bytes of room left, so a buffer sized exactly to a
/// term's expanded codes silently falls back to the byte-at-a-time tail loop.
const FSST_DECODE_HEADROOM: usize = 8 * FSST_SYMBOL_LEN;

/// The room a scratch buffer is given when a decode first allocates it: a term
/// of up to 120 FSST codes, past which only unusually long terms go. The
/// probes of a search over usual terms then decode into it without growing it.
const SCRATCH_SEED_BYTES: usize = 1024;

/// FSST-compressed sorted terms, with the pieces a hot lookup needs hoisted
/// out of the Vortex array.
pub(super) struct FsstTerms {
    /// Kept whole so the dictionary can be serialized without recompressing.
    array: FSSTArray,
    /// Code offsets as `u32`, so a per-row read is a slice index. A leaf's
    /// 32-bit offsets (`i32` or `u32`) are viewed in place (written offsets
    /// are never negative); every other width is converted to `u32` in a
    /// copy.
    offsets: Buffer<u32>,
}

impl FsstTerms {
    fn new(array: FSSTArray) -> Result<Self> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let offsets = array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)?;
        // 32-bit offsets are viewed as they are. A foreign writer may store
        // them in any other integer width, which is converted to `u32` in a
        // copy; the macro covers every width, so its `u32` arm casts
        // u32 -> u32 without being reached.
        #[allow(clippy::unnecessary_cast)]
        let offsets: Buffer<u32> = match offsets.ptype() {
            PType::U32 => offsets.to_buffer::<u32>(),
            PType::I32 => offsets.reinterpret_cast(PType::U32).to_buffer::<u32>(),
            ptype => match_each_integer_ptype!(ptype, |O| {
                offsets.as_slice::<O>().iter().map(|&o| o as u32).collect()
            }),
        };
        Ok(Self { array, offsets })
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    /// Decode term `i` into `scratch`, returning the bytes written. The
    /// scratch is grown to hold the term: an FSST code expands to at most
    /// one 8-byte symbol, and `decompress_into`'s 8-symbols-at-a-time path
    /// only runs while the output has `FSST_DECODE_HEADROOM` bytes of room
    /// left, so a buffer sized exactly to the term would fall back to the
    /// byte-at-a-time tail loop. A buffer is never given less than
    /// [`SCRATCH_SEED_BYTES`].
    fn decode_into<'a>(&self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
        let (start, end) = (self.offsets[i] as usize, self.offsets[i + 1] as usize);
        let codes = &self.array.codes_bytes()[start..end];
        scratch.clear();
        let needed = codes.len() * FSST_SYMBOL_LEN + FSST_DECODE_HEADROOM;
        #[cfg(test)]
        match scratch.capacity() {
            0 => scratch_hooks::note_fresh(),
            capacity if capacity < needed => scratch_hooks::note_regrown(),
            _ => {}
        }
        scratch.reserve(needed.max(SCRATCH_SEED_BYTES));
        let n = self
            .array
            .decompressor()
            .decompress_into(codes, scratch.spare_capacity_mut());
        // SAFETY: `decompress_into` initialized the first `n` bytes.
        unsafe {
            scratch.set_len(n);
        }
        &scratch[..n]
    }
}

/// Test hook: how many FSST reads on this thread found their scratch buffer
/// unallocated, and so allocated a fresh one.
#[cfg(test)]
pub(super) mod scratch_hooks {
    use std::cell::Cell;

    thread_local! {
        static FRESH: Cell<usize> = const { Cell::new(0) };
        static REGROWN: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn note_fresh() {
        FRESH.with(|c| c.set(c.get() + 1));
    }

    pub(super) fn note_regrown() {
        REGROWN.with(|c| c.set(c.get() + 1));
    }

    /// The fresh scratch buffers this thread's reads allocated so far.
    pub(crate) fn fresh() -> usize {
        FRESH.with(Cell::get)
    }

    /// The times this thread's reads found their buffer too small and grew
    /// it.
    pub(crate) fn regrown() -> usize {
        REGROWN.with(Cell::get)
    }
}

/// A cursor over a dictionary's terms.
///
/// `str_at` borrows from the cursor rather than from the dictionary because an
/// FSST read decodes into the cursor's own scratch buffer, so the borrow ends
/// at the next call. Callers needing several terms simultaneously — decoding a
/// quad's four roles — take one cursor per role.
pub(super) enum DictCursor<'a> {
    /// A cursor over a [`TermStore::Single`] store's one chunk.
    Single(ChunkCursor<'a>),
    /// A cursor over a [`TermStore::Chunked`] store.
    Chunked {
        store: &'a ResidentChunks,
        /// One cursor per chunk, each with its own scratch, so a read maps
        /// the global index to its chunk and delegates.
        cursors: Vec<ChunkCursor<'a>>,
    },
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
    /// Term `local` as a `&str`, borrowed from the cursor like
    /// [`bytes_at`](Self::bytes_at).
    #[cfg(feature = "file-io")]
    #[inline]
    pub(super) fn str_at(&mut self, local: usize) -> Result<&str> {
        buf_as_str(self.bytes_at(local))
    }

    #[inline]
    pub(super) fn bytes_at(&mut self, local: usize) -> &[u8] {
        match self {
            ChunkCursor::Canonical(r) => r.bytes_at(local),
            ChunkCursor::Fsst { terms, scratch } => terms.decode_into(local, scratch),
        }
    }
}

impl DictCursor<'_> {
    #[inline]
    pub(super) fn bytes_at(&mut self, i: usize) -> &[u8] {
        match self {
            DictCursor::Single(c) => c.bytes_at(i),
            DictCursor::Chunked { store, cursors } => {
                let (chunk, local) = store.locate(i);
                cursors[chunk].bytes_at(local)
            }
        }
    }

    #[inline]
    pub(super) fn str_at(&mut self, i: usize) -> Result<&str> {
        buf_as_str(self.bytes_at(i))
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
            let struct_arr = chunk.execute::<StructArray>(&mut ctx)?;
            chunks.push(struct_arr.unmasked_field_by_name(COL_DICT_TERM)?.clone());
        }
        Self::from_term_chunks(chunks, &mut ctx)
    }
}

#[cfg(test)]
impl TermDictionary {
    /// The held term chunks as arrays, each in its stored encoding.
    pub(crate) fn term_chunks(&self) -> Vec<ArrayRef> {
        match &self.terms {
            TermStore::Single(c) => vec![c.array()],
            TermStore::Chunked(c) => c.chunks.iter().map(TermChunk::array).collect(),
        }
    }
}

#[cfg(test)]
impl TermChunk {
    /// The address of an FSST chunk's code offsets, for tests that pin
    /// whether they view the leaf's own buffer.
    #[cfg(test)]
    pub(super) fn offsets_addr(&self) -> Option<usize> {
        match self {
            TermChunk::Canonical(_) => None,
            TermChunk::Fsst(f) => Some(f.offsets.as_ptr() as usize),
        }
    }
}

/// The zone aggregates recording a dictionary window's exact bounds, in the
/// order the zone table holds them: `vortex.max()` (last term), then
/// `vortex.min()` (first term). Not the bounded string aggregates Vortex
/// writes by default, which truncate long terms.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) fn window_bound_aggregates() -> Arc<[vortex_array::aggregate_fn::AggregateFnRef]> {
    use vortex_array::aggregate_fn::fns::{max::Max, min::Min};
    use vortex_array::aggregate_fn::{AggregateFnVTableExt as _, NumericalAggregateOpts};
    Arc::from([
        Max.bind(NumericalAggregateOpts::skip_nans()),
        Min.bind(NumericalAggregateOpts::skip_nans()),
    ])
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
                version: container::DICT_VERSION,
                required: true,
                sorted: true,
                dtype,
            },
            Arc::new(BufferedComponentSource::try_new(chunks)?),
            container::dict_child_strategy(self.uniform_window()),
        )
        .map_err(VortexRdfError::Vortex)
    }

    /// The dictionary component's body, one `{_dict_term: utf8}` struct per
    /// held term chunk — row i of the concatenation = the term with code i,
    /// in the encoding the dictionary is held in (FSST when compressed at the
    /// source). Chunk-granular because each chunk is a self-contained array
    /// (independent FSST windows, see [`compress`](Self::compress)) written
    /// verbatim as one flat leaf, so its boundary survives as a split of the
    /// serialized child. Always at least one chunk, possibly empty — the
    /// child strategy needs a chunk to write a schema-complete component.
    pub(crate) fn child_chunks(&self) -> Result<Vec<ArrayRef>> {
        let wrap = |terms: ArrayRef| -> Result<ArrayRef> {
            let rows = terms.len();
            StructArray::try_new(
                [COL_DICT_TERM].into(),
                vec![terms],
                rows,
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)
            .map(|a| a.into_array())
        };
        match &self.terms {
            TermStore::Single(c) => Ok(vec![wrap(c.array())?]),
            TermStore::Chunked(c) => c.chunks.iter().map(|chunk| wrap(chunk.array())).collect(),
        }
    }

    /// The window every held chunk but the last fills — the zone length the
    /// dictionary child is written with — or `None` when the chunks are not
    /// one uniform window (chunks adopted from a foreign file), which then
    /// go without a zone map. An empty dictionary has none.
    pub(crate) fn uniform_window(&self) -> Option<std::num::NonZeroUsize> {
        match &self.terms {
            TermStore::Single(chunk) => std::num::NonZeroUsize::new(chunk.len()),
            TermStore::Chunked(c) => {
                let (last, rest) = c.chunks.split_last()?;
                let window = rest.first().map_or(last.len(), TermChunk::len);
                let uniform =
                    rest.iter().all(|chunk| chunk.len() == window) && last.len() <= window;
                uniform
                    .then(|| std::num::NonZeroUsize::new(window))
                    .flatten()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema::TermCode;
    use vortex_array::arrays::VarBinArray;
    use vortex_array::dtype::{DType, NativePType, Nullability, PType};
    use vortex_array::validity::Validity;
    #[cfg(feature = "file-io")]
    use vortex_layout::layouts::zoned::Zoned;

    /// 32-bit code offsets are the very buffer of the array's offsets child:
    /// viewed, not copied.
    #[test]
    fn thirty_two_bit_offsets_are_viewed_not_copied() {
        let terms: Vec<String> = (0..300)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let dict = TermDictionary::compress_windowed(plain, 100).unwrap();
        let TermStore::Chunked(chunks) = &dict.terms else {
            panic!("a dictionary above the window size must be chunked");
        };
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        for chunk in &chunks.chunks {
            let TermChunk::Fsst(fsst) = chunk else {
                panic!("a built dictionary holds FSST chunks");
            };
            let child = fsst
                .array
                .codes_offsets()
                .clone()
                .execute::<PrimitiveArray>(&mut ctx)
                .unwrap();
            assert_eq!(child.ptype(), PType::I32, "the built offsets are 32-bit");
            assert_eq!(fsst.offsets.len(), child.len());
            assert_eq!(
                fsst.offsets.as_ptr() as usize,
                child.as_slice::<i32>().as_ptr() as usize
            );
            assert_eq!(chunk.offsets_addr(), Some(fsst.offsets.as_ptr() as usize));
        }
    }

    /// `array` rebuilt with its code offsets stored as `T`.
    fn with_offsets_as<T>(array: &FSSTArray, ctx: &mut vortex_array::ExecutionCtx) -> FSSTArray
    where
        T: NativePType + TryFrom<u32>,
        <T as TryFrom<u32>>::Error: std::fmt::Debug,
    {
        let offsets = array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(ctx)
            .unwrap();
        let narrowed = PrimitiveArray::from_iter(
            offsets
                .as_slice::<i32>()
                .iter()
                .map(|&o| T::try_from(o as u32).unwrap()),
        );
        let codes = VarBinArray::new(
            narrowed.into_array(),
            array.codes_bytes().clone(),
            DType::Binary(Nullability::NonNullable),
            Validity::NonNullable,
        );
        FSST::try_new_with_symbol_table(
            array.dtype().clone(),
            array.symbol_table(),
            codes,
            array.uncompressed_lengths().clone(),
            ctx,
        )
        .unwrap()
    }

    /// A chunk whose code offsets are stored as `T` holds the same offsets and
    /// decodes the same terms as the 32-bit original; it views the stored
    /// buffer only when `T` is 32 bits wide, and converts into a copy
    /// otherwise.
    fn check_offsets_stored_as<T>(original: &FsstTerms, terms: &[String])
    where
        T: NativePType + TryFrom<u32>,
        <T as TryFrom<u32>>::Error: std::fmt::Debug,
    {
        let name = std::any::type_name::<T>();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let rebuilt = with_offsets_as::<T>(&original.array, &mut ctx);
        let stored = rebuilt
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap();
        assert_eq!(
            stored.ptype(),
            T::PTYPE,
            "{name}: the offsets are stored so"
        );
        let chunk = FsstTerms::new(rebuilt).unwrap();
        assert_eq!(chunk.offsets[..], original.offsets[..], "{name}: offsets");
        assert_eq!(
            std::ptr::eq(
                chunk.offsets.as_ptr(),
                stored.as_slice::<T>().as_ptr().cast()
            ),
            T::PTYPE.byte_width() == 4,
            "{name}: viewed exactly when 32-bit"
        );
        let chunk = TermChunk::Fsst(chunk);
        #[cfg(feature = "file-io")]
        assert_eq!(chunk.heap_bytes(), 4 * original.offsets.len(), "{name}");
        let mut cursor = chunk.cursor();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(cursor.bytes_at(i), term.as_bytes(), "{name}: term {i}");
        }
    }

    #[test]
    fn fsst_code_offsets_of_every_width_decode_alike() {
        let terms: Vec<String> = (0..8).map(|i| format!("<http://e.org/{i}>")).collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let dict = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        let TermStore::Single(TermChunk::Fsst(original)) = &dict.terms else {
            panic!("a small dictionary is one FSST chunk");
        };
        // Every stored offset must fit the narrowest width tried.
        assert!(original.offsets.iter().all(|&o| o < 128));
        check_offsets_stored_as::<u8>(original, &terms);
        check_offsets_stored_as::<i8>(original, &terms);
        check_offsets_stored_as::<u16>(original, &terms);
        check_offsets_stored_as::<i16>(original, &terms);
        check_offsets_stored_as::<u32>(original, &terms);
        check_offsets_stored_as::<i32>(original, &terms);
        check_offsets_stored_as::<u64>(original, &terms);
        check_offsets_stored_as::<i64>(original, &terms);
    }

    /// A cursor's scratch buffer grows to each term it decodes: terms far
    /// longer than any before them, short ones after, every probe, batch and
    /// positional read through one chunk or several.
    #[test]
    fn scratch_grows_to_terms_far_longer_than_the_first() {
        let terms = crate::tests::widely_varying_terms();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let n = terms.len();
        for window in [usize::MAX, 6] {
            let dict = TermDictionary::compress_windowed(plain.clone(), window).unwrap();
            // One cursor: short, longer, longest, short again, descending.
            let mut cursor = dict.cursor();
            let order: Vec<usize> = (0..n).chain((0..n).rev()).chain([0, 9, 1, 5, 9]).collect();
            for &i in &order {
                assert_eq!(cursor.str_at(i).unwrap(), terms[i], "window {window}: {i}");
            }
            let codes: Vec<TermCode> = order.iter().map(|&i| i as TermCode).collect();
            let decoded = dict.decode_many(&codes);
            for (&i, term) in order.iter().zip(&decoded) {
                assert_eq!(term.as_deref(), Some(terms[i].as_str()), "window {window}");
            }
            for (i, term) in terms.iter().enumerate() {
                assert_eq!(dict.encode(term), Some(i as TermCode), "window {window}");
                assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
                assert_eq!(dict.decode(i as TermCode).as_deref(), Some(term.as_str()));
            }
            let past_the_longest = format!("{}~", terms[9]);
            assert_eq!(dict.encode(&past_the_longest), None);
            assert_eq!(dict.lower_bound(past_the_longest.as_bytes()), 10);
        }
    }

    /// After every read the scratch holds room for the term's codes expanded
    /// at 8 bytes each plus the headroom `decompress_into`'s block path needs:
    /// a smaller buffer still decodes, through the byte-at-a-time tail loop,
    /// so only the capacity shows it.
    #[test]
    fn scratch_keeps_the_decoders_block_path_headroom() {
        let terms = crate::tests::widely_varying_terms();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let dict = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        let TermStore::Single(chunk @ TermChunk::Fsst(fsst)) = &dict.terms else {
            panic!("a small dictionary is one FSST chunk");
        };
        let mut cursor = chunk.cursor();
        let order: Vec<usize> = (0..terms.len()).chain((0..terms.len()).rev()).collect();
        for i in order {
            assert_eq!(cursor.bytes_at(i), terms[i].as_bytes());
            let codes = (fsst.offsets[i + 1] - fsst.offsets[i]) as usize;
            let ChunkCursor::Fsst { scratch, .. } = &cursor else {
                unreachable!("an FSST chunk gives an FSST cursor");
            };
            assert!(
                scratch.capacity() >= codes * FSST_SYMBOL_LEN + FSST_DECODE_HEADROOM,
                "term {i}: {codes} code bytes, scratch {}",
                scratch.capacity()
            );
        }
    }

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
        match &windowed.terms {
            TermStore::Chunked(c) => {
                assert_eq!(c.chunks.len(), 1_000usize.div_ceil(64));
                assert_eq!(c.len, 1_000);
                assert!(c.chunks.iter().all(|ch| matches!(ch, TermChunk::Fsst(_))));
            }
            _ => panic!("a dictionary above the window size must be chunked"),
        }
        let single = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        assert!(matches!(
            single.terms,
            TermStore::Single(TermChunk::Fsst(_))
        ));
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(windowed.encode(term), Some(i as TermCode), "{term}");
            assert_eq!(
                windowed.decode(i as TermCode).as_deref(),
                Some(term.as_str())
            );
            assert_eq!(single.encode(term), Some(i as TermCode));
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
        assert!(matches!(
            single.terms,
            TermStore::Single(TermChunk::Canonical(_))
        ));

        let column = plain.into_array();
        let pieces = vec![
            column.slice(0..120).unwrap(),
            column.slice(120..300).unwrap(),
        ];
        let chunked = TermDictionary::from_term_chunks(pieces, &mut ctx).unwrap();
        match &chunked.terms {
            TermStore::Chunked(c) => {
                assert_eq!(c.chunks.len(), 2);
                assert_eq!(c.starts, vec![0, 120]);
                assert_eq!(c.len, 300);
                assert!(
                    c.chunks
                        .iter()
                        .all(|ch| matches!(ch, TermChunk::Canonical(_)))
                );
            }
            _ => panic!("a two-chunk column must adopt chunked"),
        }

        for (i, term) in terms.iter().enumerate() {
            for d in [&single, &chunked] {
                assert_eq!(d.encode(term), Some(i as TermCode), "{term}");
                assert_eq!(d.decode(i as TermCode).as_deref(), Some(term.as_str()));
            }
        }
        assert_eq!(chunked.encode("<http://absent>"), None);
        assert_eq!(chunked.decode(300), None);
    }

    /// Chunks that are not one uniform window (adopted from a foreign file)
    /// are written without zone maps, and read back unchanged.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn non_uniform_chunks_write_without_zone_maps() {
        let terms: Vec<String> = (0..300)
            .map(|i| format!("<http://example.org/plain/{i:04}>"))
            .collect();
        let column = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)).into_array();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let pieces = vec![
            column.slice(0..120).unwrap(),
            column.slice(120..300).unwrap(),
        ];
        let d = TermDictionary::from_term_chunks(pieces, &mut ctx).unwrap();
        assert_eq!(d.uniform_window(), None);
        let windowed = TermDictionary::compress_windowed(
            VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)),
            100,
        )
        .unwrap();
        assert_eq!(windowed.uniform_window(), std::num::NonZeroUsize::new(100));
        let bytes = crate::tests::write_dict_only_store(&d).await;
        let reread = crate::store::VortexRdfStore::from_bytes(&bytes)
            .await
            .unwrap();
        let snapshot = reread.dictionary_snapshot().unwrap();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(
                snapshot.decode(i as TermCode).as_deref(),
                Some(term.as_str())
            );
        }
        // And the term column carries no zone-map node at all.
        let native = crate::tests::open_native_bytes(bytes);
        assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());
    }

    /// A dictionary child without zone maps — what chunks of uneven length
    /// write — opens file-backed and point-reads across its chunk leaves.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn unzoned_dict_child_point_reads_file_backed() {
        use crate::store::layouts::dictionary::file_backed::{self, FileBackedDict};
        use crate::store::persist::native_file::NativeStoreFile;
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;

        let terms: Vec<String> = (0..300)
            .map(|i| format!("<http://example.org/plain/{i:04}>"))
            .collect();
        let column = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)).into_array();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let pieces = vec![
            column.slice(0..120).unwrap(),
            column.slice(120..300).unwrap(),
        ];
        let d = TermDictionary::from_term_chunks(pieces, &mut ctx).unwrap();
        assert_eq!(d.uniform_window(), None);
        let bytes = crate::tests::write_dict_only_store(&d).await;
        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = NativeStoreFile::try_new(file).unwrap();
        assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());
        let fbd = FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("an unzoned chunked child is point-readable");
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(
                fbd.encode(term).await.unwrap(),
                Some(i as TermCode),
                "{term}"
            );
        }
        // The canonical windows are cached, each charged its array and the
        // per-entry overhead.
        assert_eq!(fbd.debug_cached_windows(), [0, 1]);
        assert!(
            fbd.debug_cached_bytes() > 2 * file_backed::WINDOW_ENTRY_OVERHEAD,
            "{}",
            fbd.debug_cached_bytes()
        );
        assert_eq!(fbd.encode("<http://absent>").await.unwrap(), None);
        let codes = [0u64, 119, 120, 299];
        let decoded = fbd.decode_many(&codes).await.unwrap();
        for (code, term) in codes.iter().zip(&decoded) {
            assert_eq!(term.as_ref(), terms[*code as usize]);
        }
    }
}
