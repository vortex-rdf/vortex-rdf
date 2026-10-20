//! A resident [`TermDictionary`]'s terms: how they are held
//! ([`ResidentChunks`]) and read (single-term reads and cursors), built
//! (FSST-compressed in windows) or adopted from a written term column, and
//! written as the `dictionary` child with its windows' zone-map bounds.

use std::sync::Arc;

use vortex_array::arrays::primitive::PrimitiveArrayExt as _;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::PType;
use vortex_array::match_each_integer_ptype;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use vortex_buffer::Buffer;

use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::io::read::scan_reader_chunks;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, trusted_str};

use super::term_dict::TermDictionary;

/// The `dictionary` child's single column: non-nullable utf8, row i the term
/// with code i, globally sorted. Wire contract, owned here per the rule in
/// [`crate::store::schema`].
pub(crate) const COL_DICT_TERM: &str = "_dict_term";

/// Terms per FSST window of a built dictionary ([`TermDictionary::compress`]):
/// the granularity of the child's leaves, of a file-backed read and of a
/// chunk-by-chunk lift. Small enough that one leaf is a bounded slice of the
/// column, large enough to amortize the symbol table every window carries.
const DICT_CHUNK_ROWS: usize = 64 * 1024;

/// A dictionary's sorted terms in memory: one or more chunks (a built
/// dictionary's FSST windows, or a written child's chunks as read back) and
/// the start table mapping a global term index to (chunk, local index).
/// Always at least one chunk; an empty column is one empty chunk. A chunk
/// that arrives FSST stays FSST; any other encoding a writer chose is
/// canonicalized to plaintext.
pub(super) struct ResidentChunks {
    chunks: Vec<TermChunk>,
    /// `starts[i]` = global index of chunk i's first term; ascending.
    starts: Vec<usize>,
    len: usize,
}

/// One chunk of a term column, in the encoding it is held in.
pub(super) enum TermChunk {
    /// Plaintext terms; a read borrows the array's bytes.
    Canonical(VarBinViewArray),
    /// FSST-compressed terms; every read decodes one term.
    Fsst(FsstTerms),
}

impl TermChunk {
    /// Adopt a wire chunk: FSST stays FSST, any other encoding is
    /// canonicalized to plaintext.
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

    /// The chunk as an array, in its stored encoding.
    fn array(&self) -> ArrayRef {
        match self {
            TermChunk::Canonical(a) => a.clone().into_array(),
            TermChunk::Fsst(f) => f.array.clone().into_array(),
        }
    }

    /// What a window cache charges for this chunk. An FSST chunk is charged
    /// its code offsets (4 bytes per term, plus 4), whether they view the
    /// leaf's buffer or are a converted copy, but not the code bytes it only
    /// references. A canonical chunk is charged its array's `nbytes()`,
    /// which counts buffers sliced from a map too.
    #[cfg(feature = "file-io")]
    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            TermChunk::Canonical(a) => usize::try_from(a.nbytes()).unwrap_or(usize::MAX),
            TermChunk::Fsst(f) => f.offsets.len() * size_of::<u32>(),
        }
    }

    /// A cursor over this chunk's terms. An FSST cursor's scratch starts
    /// empty and grows to the terms it decodes, so a read allocates only for
    /// the chunks it touches.
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

/// The chunk holding global index `i` of chunks starting at `starts`
/// (ascending, `starts[0] == 0`), and `i` local to that chunk.
pub(super) fn chunk_of(starts: &[usize], i: usize) -> (usize, usize) {
    let chunk = starts.partition_point(|&s| s <= i) - 1;
    (chunk, i - starts[chunk])
}

impl ResidentChunks {
    /// The column `chunks` concatenate to; at least one chunk.
    fn new(chunks: Vec<TermChunk>) -> Self {
        debug_assert!(!chunks.is_empty(), "a term column holds at least one chunk");
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

    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// The chunk holding global index `i`, and `i` local to it. A
    /// one-chunk column answers without searching the start table.
    #[inline]
    fn locate(&self, i: usize) -> (&TermChunk, usize) {
        match self.chunks.as_slice() {
            [only] => (only, i),
            chunks => {
                let (chunk, local) = chunk_of(&self.starts, i);
                (&chunks[chunk], local)
            }
        }
    }

    /// Term `i`'s bytes: borrowed from a canonical chunk, or decoded into
    /// `scratch` from an FSST one. A single-term read passes one scratch
    /// through all its probes, where a cursor holds one per chunk.
    pub(super) fn bytes_into<'a>(&'a self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
        match self.locate(i) {
            (TermChunk::Canonical(a), local) => StrColReader::new(a).bytes_at(local),
            (TermChunk::Fsst(f), local) => f.decode_into(local, scratch),
        }
    }

    /// A cursor over the terms: one chunk cursor per chunk, each with its own
    /// scratch; a one-chunk column's builds only the first.
    pub(super) fn cursor(&self) -> DictCursor<'_> {
        let (first, rest) = self
            .chunks
            .split_first()
            .expect("a term column holds at least one chunk");
        DictCursor {
            store: self,
            first: first.cursor(),
            rest: if rest.is_empty() {
                Vec::new()
            } else {
                rest.iter().map(TermChunk::cursor).collect()
            },
        }
    }
}

impl TermDictionary {
    /// A dictionary of no terms, held canonical: there is nothing to train
    /// FSST on.
    pub(crate) fn empty() -> Self {
        Self::new(ResidentChunks::new(vec![TermChunk::Canonical(
            VarBinViewArray::from_iter_str(std::iter::empty::<&str>()),
        )]))
    }

    /// A dictionary of sorted unique `terms`, FSST-compressed in windows
    /// ([`compress`](Self::compress)).
    pub(super) fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        Self::from_sorted_column(VarBinViewArray::from_iter_str(terms))
    }

    /// [`from_sorted`](Self::from_sorted) over an assembled plaintext column;
    /// a `Serialization` error past `i32::MAX` terms.
    pub(crate) fn from_sorted_column(plain: VarBinViewArray) -> Result<Self> {
        // A conservative cap, not a format limit: nothing in the format needs
        // the count to fit an `i32` (codes are `u64`, and each FSST window's
        // offsets span `DICT_CHUNK_ROWS` terms), but no larger dictionary is
        // tested, and this build holds every term in memory.
        if plain.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                plain.len(),
                i32::MAX
            )));
        }
        Self::compress(plain)
    }

    /// The dictionary of a term column's `chunks`, each adopted through
    /// [`TermChunk::from_wire`]; empty chunks are dropped.
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

    /// `plain` FSST-compressed in windows of [`DICT_CHUNK_ROWS`] terms: one
    /// symbol table trained on the whole column, each window compressed with
    /// it into a self-contained array. The writer writes each window verbatim
    /// as one flat leaf (no re-encoding, no parent buffers dragged along),
    /// and those leaves are what a file-backed dictionary reads. An empty
    /// column stays canonical: there is nothing to train a symbol table on.
    fn compress(plain: VarBinViewArray) -> Result<Self> {
        Self::compress_windowed(plain, DICT_CHUNK_ROWS)
    }

    /// [`compress`](Self::compress) in windows of `window` terms, so tests
    /// reach the multi-window path with few terms.
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
        let compressor = fsst_train_compressor(&array, &mut ctx)?;
        let chunks = if len <= window {
            // One window: the column compresses as it is, unsliced.
            let fsst = fsst_compress(&array, &compressor, &mut ctx)?;
            vec![TermChunk::Fsst(FsstTerms::new(fsst)?)]
        } else {
            let mut chunks = Vec::with_capacity(len.div_ceil(window));
            let mut at = 0usize;
            while at < len {
                let end = (at + window).min(len);
                // `fsst_compress` takes a VarBinView, not the lazy slice. The
                // executed view shares the parent's data buffers: only the
                // window's view headers are copied, not its terms.
                let piece = array
                    .slice(at..end)?
                    .execute::<VarBinViewArray>(&mut ctx)?
                    .into_array();
                let fsst = fsst_compress(&piece, &compressor, &mut ctx)?;
                chunks.push(TermChunk::Fsst(FsstTerms::new(fsst)?));
                at = end;
            }
            chunks
        };
        log::debug!(
            "[Dictionary] FSST-compressed {} terms in {:?} (windows: {})",
            len,
            debug::elapsed(start),
            chunks.len()
        );
        Ok(Self::new(ResidentChunks::new(chunks)))
    }
}

/// Bytes one FSST code expands to at most.
const FSST_SYMBOL_LEN: usize = 8;

/// Output headroom that keeps `decompress_into` on its 8-symbols-at-a-time
/// path, which runs only while `8 * FSST_SYMBOL_LEN` bytes of room are left:
/// a buffer sized exactly to a term's expanded codes falls back to the
/// byte-at-a-time loop.
const FSST_DECODE_HEADROOM: usize = 8 * FSST_SYMBOL_LEN;

/// The room a decode first allocates a scratch with: a term of up to 120 FSST
/// codes, so the probes of a search over usual terms never grow it.
const SCRATCH_SEED_BYTES: usize = 1024;

/// FSST-compressed sorted terms, with the code offsets unpacked for per-row
/// reads.
pub(super) struct FsstTerms {
    /// Kept whole, so the dictionary serializes without recompressing.
    array: FSSTArray,
    /// Code offsets as `u32`, so a per-row read is a slice index: 32-bit
    /// offsets (`i32` or `u32`; written offsets are never negative) are viewed
    /// in place, any other width is converted into a copy.
    offsets: Buffer<u32>,
}

impl FsstTerms {
    fn new(array: FSSTArray) -> Result<Self> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let offsets = array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)?;
        // A foreign writer may store the offsets in any integer width. The
        // macro covers every width, so its `u32` arm (never reached) casts
        // u32 -> u32.
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

    /// Term `i` decoded into `scratch`, whose bytes it returns. The scratch is
    /// first grown to the term's codes expanded at [`FSST_SYMBOL_LEN`] bytes
    /// each plus [`FSST_DECODE_HEADROOM`], and to at least
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

/// Test hooks: the FSST reads on this thread that allocated a fresh scratch,
/// and those that grew one.
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
    pub(in crate::store::layouts::dictionary) fn fresh() -> usize {
        FRESH.with(Cell::get)
    }

    /// The times this thread's reads found their buffer too small and grew
    /// it.
    pub(in crate::store::layouts::dictionary) fn regrown() -> usize {
        REGROWN.with(Cell::get)
    }
}

/// A cursor over a dictionary's terms. A read borrows from the cursor, not
/// the dictionary, because an FSST read decodes into the cursor's scratch:
/// the borrow ends at the next read, so holding several terms at once (a
/// quad's four roles) takes one cursor each.
pub(super) struct DictCursor<'a> {
    store: &'a ResidentChunks,
    /// The first chunk's cursor, held inline, so a one-chunk column's cursor
    /// allocates nothing until a read grows its scratch.
    first: ChunkCursor<'a>,
    /// One cursor per further chunk, each with its own scratch.
    rest: Vec<ChunkCursor<'a>>,
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
    /// Term `local` as a `&str`, borrowed like [`bytes_at`](Self::bytes_at)
    /// and read unchecked ([`trusted_str`]): the chunk must be a Utf8 column
    /// of a store this crate wrote or the caller trusts.
    #[cfg(feature = "file-io")]
    #[inline]
    pub(super) fn str_at(&mut self, local: usize) -> &str {
        trusted_str(self.bytes_at(local))
    }

    /// Term `local`'s bytes, borrowed from the cursor until the next read.
    #[inline]
    pub(super) fn bytes_at(&mut self, local: usize) -> &[u8] {
        match self {
            ChunkCursor::Canonical(r) => r.bytes_at(local),
            ChunkCursor::Fsst { terms, scratch } => terms.decode_into(local, scratch),
        }
    }
}

impl DictCursor<'_> {
    /// Term `i`'s bytes, borrowed from the cursor until the next read.
    #[inline]
    pub(super) fn bytes_at(&mut self, i: usize) -> &[u8] {
        if self.rest.is_empty() {
            return self.first.bytes_at(i);
        }
        let (chunk, local) = chunk_of(&self.store.starts, i);
        let cursor = match chunk {
            0 => &mut self.first,
            k => &mut self.rest[k - 1],
        };
        cursor.bytes_at(local)
    }

    /// Term `i` as a `&str`, borrowed like [`bytes_at`](Self::bytes_at) and
    /// read unchecked ([`trusted_str`]): the terms must come from a store this
    /// crate wrote or the caller trusts.
    #[inline]
    pub(super) fn str_at(&mut self, i: usize) -> &str {
        trusted_str(self.bytes_at(i))
    }
}

impl TermDictionary {
    /// The resident dictionary of a `dictionary` child's whole term column,
    /// each scan chunk adopted as one chunk ([`TermChunk::from_wire`]). The
    /// scan runs inline ([`scan_reader_chunks`]), with no runtime handle, so
    /// it serves every open path, wasm's included.
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
        self.terms.chunks.iter().map(TermChunk::array).collect()
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
pub(crate) fn window_bound_aggregates() -> Arc<[vortex_array::aggregate_fn::AggregateFnRef]> {
    use vortex_array::aggregate_fn::fns::{max::Max, min::Min};
    use vortex_array::aggregate_fn::{AggregateFnVTableExt as _, NumericalAggregateOpts};
    Arc::from([
        Max.bind(NumericalAggregateOpts::skip_nans()),
        Min.bind(NumericalAggregateOpts::skip_nans()),
    ])
}

impl TermDictionary {
    /// This dictionary as the root's required `dictionary` component: the
    /// chunks of [`child_chunks`](Self::child_chunks), each written verbatim
    /// as one flat leaf, with one zone per window when the windows are
    /// uniform ([`uniform_window`](Self::uniform_window)).
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

    /// The component's body: one `{_dict_term: utf8}` struct per held chunk,
    /// in its stored encoding, row i of the concatenation the term with code
    /// i. Each chunk is written as one flat leaf, so the windows survive as
    /// the child's leaves. At least one chunk, possibly empty: the child
    /// strategy needs one to write a schema-complete component.
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
        self.terms
            .chunks
            .iter()
            .map(|chunk| wrap(chunk.array()))
            .collect()
    }

    /// The length every held chunk but the last fills, and the last at most:
    /// the zone length the child is written with, one zone per leaf. `None`
    /// for chunks of uneven length (adopted from a foreign file), which are
    /// written without a zone map. A single chunk is its own window; an empty
    /// dictionary has none.
    pub(crate) fn uniform_window(&self) -> Option<std::num::NonZeroUsize> {
        let (last, rest) = self.terms.chunks.split_last()?;
        let window = rest.first().map_or(last.len(), TermChunk::len);
        let uniform = rest.iter().all(|chunk| chunk.len() == window) && last.len() <= window;
        uniform
            .then(|| std::num::NonZeroUsize::new(window))
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema::TermCode;
    use vortex_array::arrays::VarBinArray;
    use vortex_array::dtype::{DType, NativePType, Nullability, PType};
    use vortex_array::validity::Validity;
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
        assert_eq!(
            dict.terms.chunks.len(),
            3,
            "a window of 100 cuts 300 terms in 3"
        );
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        for chunk in &dict.terms.chunks {
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
        let [TermChunk::Fsst(original)] = &dict.terms.chunks[..] else {
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
                assert_eq!(cursor.str_at(i), terms[i], "window {window}: {i}");
            }
            let codes: Vec<TermCode> = order.iter().map(|&i| i as TermCode).collect();
            let decoded = dict.decode_many(&codes);
            for (&i, term) in order.iter().zip(&decoded) {
                assert_eq!(term.as_deref(), Some(terms[i].as_str()), "window {window}");
            }
            for (i, term) in terms.iter().enumerate() {
                assert_eq!(
                    dict.encode_exact(term),
                    Some(i as TermCode),
                    "window {window}"
                );
                assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
                assert_eq!(dict.decode(i as TermCode).as_deref(), Some(term.as_str()));
            }
            let past_the_longest = format!("{}~", terms[9]);
            assert_eq!(dict.encode_exact(&past_the_longest), None);
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
        let [chunk @ TermChunk::Fsst(fsst)] = &dict.terms.chunks[..] else {
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
        let c = &windowed.terms;
        assert_eq!(c.chunks.len(), 1_000usize.div_ceil(64));
        assert_eq!(c.len, 1_000);
        assert!(c.chunks.iter().all(|ch| matches!(ch, TermChunk::Fsst(_))));
        let single = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        assert!(matches!(single.terms.chunks[..], [TermChunk::Fsst(_)]));
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(windowed.encode_exact(term), Some(i as TermCode), "{term}");
            assert_eq!(
                windowed.decode(i as TermCode).as_deref(),
                Some(term.as_str())
            );
            assert_eq!(single.encode_exact(term), Some(i as TermCode));
        }
        assert_eq!(windowed.encode_exact("<http://absent>"), None);
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
        assert!(matches!(single.terms.chunks[..], [TermChunk::Canonical(_)]));
        // A canonical chunk has no FSST code offsets to point at.
        assert_eq!(single.terms.chunks[0].offsets_addr(), None);

        let column = plain.into_array();
        let pieces = vec![
            column.slice(0..120).unwrap(),
            column.slice(120..300).unwrap(),
        ];
        let chunked = TermDictionary::from_term_chunks(pieces, &mut ctx).unwrap();
        let c = &chunked.terms;
        assert_eq!(c.chunks.len(), 2);
        assert_eq!(c.starts, vec![0, 120]);
        assert_eq!(c.len, 300);
        assert!(
            c.chunks
                .iter()
                .all(|ch| matches!(ch, TermChunk::Canonical(_)))
        );

        for (i, term) in terms.iter().enumerate() {
            for d in [&single, &chunked] {
                assert_eq!(d.encode_exact(term), Some(i as TermCode), "{term}");
                assert_eq!(d.decode(i as TermCode).as_deref(), Some(term.as_str()));
            }
        }
        assert_eq!(chunked.encode_exact("<http://absent>"), None);
        assert_eq!(chunked.decode(300), None);
    }

    /// Chunks that are not one uniform window (adopted from a foreign file)
    /// are written without zone maps, and read back unchanged.
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
        let snapshot = reread.dict_reader().unwrap().snapshot().unwrap();
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
                fbd.encode_exact(term).await.unwrap(),
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
        assert_eq!(fbd.encode_exact("<http://absent>").await.unwrap(), None);
        let codes = [0u64, 119, 120, 299];
        let decoded = fbd.decode_many(&codes).await.unwrap();
        for (code, term) in codes.iter().zip(&decoded) {
            assert_eq!(term.as_ref(), terms[*code as usize]);
        }
    }
}
