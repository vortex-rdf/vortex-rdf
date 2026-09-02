//! The global term dictionary backing [`LayoutStrategy::Dictionary`]:
//! the lexicographically sorted set of unique RDF term strings, where a term's
//! code is its sorted position. The s/p/o/g columns store these codes as u32.
//!
//! Because codes are sorted ranks, code comparisons are order-isomorphic to
//! string comparisons and term → code lookup is a binary search — no HashMap
//! is needed on the query side, and the terms stay in their compact columnar
//! form (see [`TermStore`]: FSST-compressed windows as built and written,
//! plaintext `VarBinViewArray` when a producer wrote them that way).
//!
//! [`LayoutStrategy::Dictionary`]: crate::store::layouts::LayoutStrategy::Dictionary

use crate::debug;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::types::StringViewType;
use vortex_array::arrays::ChunkedArray;
use vortex_arrow::byte_view::canonical_varbinview_to_arrow;

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::match_each_integer_ptype;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use crate::error::{Result, VortexRdfError};
use crate::io::read::scan_reader_chunks;
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, buf_as_str};

use super::ingest::BorrowedTermCodeMap;

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
enum TermStore {
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
    chunks: Vec<TermChunk>,
    /// `starts[i]` = global index of chunk i's first term; ascending.
    starts: Vec<usize>,
    len: usize,
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

impl ResidentChunks {
    /// The chunk holding global index `i`, and `i` local to it.
    fn locate(&self, i: usize) -> (usize, usize) {
        chunk_of(&self.starts, i)
    }
}

/// The frozen, sorted term dictionary in columnar form.
///
/// term → code is a host-side binary search; code → term reads the term at a
/// position. Both go through [`cursor`](Self::cursor), whose cost depends on
/// the encoding the terms are held in.
pub(crate) struct TermDictionary {
    terms: TermStore,
    /// Memo for [`encode`](Self::encode); see [`ProbeCache`].
    probes: ProbeCache,
    /// The whole term column as one Arrow `StringViewArray`, built on first
    /// use by [`arrow_values`](Self::arrow_values) and shared by every later
    /// caller (an Arrow dictionary array needs a single values array, so the
    /// batch export attaches this one to every batch of a stream).
    arrow_values: OnceLock<ArrowArrayRef>,
}

impl TermDictionary {
    /// Wrap the held terms, with an empty lookup memo.
    fn new(terms: TermStore) -> Self {
        Self {
            terms,
            probes: ProbeCache::new(),
            arrow_values: OnceLock::new(),
        }
    }

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
        // List offsets are i32, so the term count must fit in one.
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
        let compressor = fsst_train_compressor(&array, &mut ctx).map_err(VortexRdfError::Vortex)?;
        if len <= window {
            let fsst =
                fsst_compress(&array, &compressor, &mut ctx).map_err(VortexRdfError::Vortex)?;
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
                .slice(at..end)
                .map_err(VortexRdfError::Vortex)?
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?
                .into_array();
            let fsst =
                fsst_compress(&piece, &compressor, &mut ctx).map_err(VortexRdfError::Vortex)?;
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

    /// The dataset's unique terms, sorted — the raw material of
    /// [`from_quads_with_map`](Self::from_quads_with_map). Terms borrow from
    /// `quads`, so nothing is copied.
    fn sorted_unique_terms(quads: &[RawQuad]) -> (Vec<&str>, Duration, Duration) {
        let collect_start = debug::timer();
        let mut set: HashSet<&str> = HashSet::new();
        for q in quads {
            set.insert(&q.s);
            set.insert(&q.p);
            set.insert(&q.o);
            set.insert(&q.g);
        }
        let collect_elapsed = debug::elapsed(collect_start);
        let sort_start = debug::timer();
        let mut terms: Vec<&str> = set.into_iter().collect();
        terms.sort_unstable();
        (terms, collect_elapsed, debug::elapsed(sort_start))
    }

    /// Build the dictionary and its term → code map in one pass; the map
    /// borrows its keys from `quads`, so it holds one pointer pair per term
    /// and no string data. The streaming builders, whose quads cannot be
    /// borrowed from, use [`TermDictionaryBuilder::finish`] instead.
    ///
    /// [`TermDictionaryBuilder::finish`]: super::ingest::TermDictionaryBuilder::finish
    pub(crate) fn from_quads_with_map(
        quads: &[RawQuad],
    ) -> Result<(Self, BorrowedTermCodeMap<'_>)> {
        let total_start = debug::timer();
        let (terms, collect_elapsed, sort_elapsed) = Self::sorted_unique_terms(quads);
        let map_start = debug::timer();
        let code_map: BorrowedTermCodeMap<'_> = terms
            .iter()
            .enumerate()
            .map(|(code, term)| (*term, code as u32))
            .collect();
        let map_elapsed = debug::elapsed(map_start);
        let freeze_start = debug::timer();
        let dict = Self::from_sorted(terms.into_iter())?;
        log::debug!(
            "[Dictionary] Built dictionary + borrowed code map from {} quads ({} unique terms): collect {:?}, sort {:?}, map {:?}, freeze {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            map_elapsed,
            debug::elapsed(freeze_start),
            debug::elapsed(total_start)
        );
        Ok((dict, code_map))
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        match &self.terms {
            TermStore::Single(c) => c.len(),
            TermStore::Chunked(c) => c.len,
        }
    }

    /// A cursor over the terms. Holds the scratch buffer an FSST read decodes
    /// into, so callers needing several terms at once (a quad's four roles)
    /// must take one cursor per role.
    pub(super) fn cursor(&self) -> DictCursor<'_> {
        match &self.terms {
            TermStore::Single(c) => DictCursor::Single(c.cursor()),
            TermStore::Chunked(c) => DictCursor::Chunked {
                store: c,
                cursors: c.chunks.iter().map(TermChunk::cursor).collect(),
            },
        }
    }

    /// Decode a code back to its term string (canonical N-Triples form), or
    /// `None` if the code is out of the dictionary's range.
    pub(crate) fn decode(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i >= self.len() {
            return None;
        }
        self.cursor().str_at(i).ok().map(str::to_owned)
    }

    /// Encode a term to its code: its position in the sorted dictionary, or
    /// `None` when the dictionary does not hold it.
    ///
    /// Memoized. [`PatternCodes`] already collapses the repeats *within* one
    /// match; this catches the repeats *across* matches — the same predicate
    /// walked over many patterns, the same subject chained through several
    /// matches.
    ///
    /// [`PatternCodes`]: crate::store::layouts::PatternCodes
    pub(crate) fn encode(&self, term: &str) -> Option<u32> {
        if let Some(memoized) = self.probes.get(term) {
            return memoized;
        }
        let found = self.search(term);
        self.probes.put(term, found);
        found
    }

    /// The uncached binary search behind [`encode`](Self::encode): a
    /// three-way compare per step, returning as soon as the probe hits.
    fn search(&self, term: &str) -> Option<u32> {
        // FSST is not order-preserving, so the search cannot run over the
        // compressed codes: every probe decodes into the cursor's scratch
        // buffer.
        let mut cursor = self.cursor();
        let needle = term.as_bytes();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cursor.bytes_at(mid).cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid as u32),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// The first code whose term is byte-wise `>= needle` (`len()` when every
    /// term is smaller) — the partition-point twin of
    /// [`search`](Self::search), over raw bytes so a prefix successor that is
    /// not valid UTF-8 can still be probed.
    fn lower_bound_bytes(&self, needle: &[u8]) -> u32 {
        let mut cursor = self.cursor();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cursor.bytes_at(mid) < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo as u32
    }

    /// The whole term column as one Arrow `StringViewArray`, cached on first
    /// use.
    ///
    /// Canonical chunks convert buffer-sharing; FSST chunks decompress — once
    /// per dictionary, bounded by the dictionary's size, never by a result's.
    pub(crate) fn arrow_values(&self) -> Result<ArrowArrayRef> {
        if let Some(values) = self.arrow_values.get() {
            return Ok(values.clone());
        }
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let canonical = match &self.terms {
            TermStore::Single(TermChunk::Canonical(plain)) => plain.clone(),
            TermStore::Single(chunk) => chunk
                .array()
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?,
            TermStore::Chunked(chunked) => {
                let chunks: Vec<ArrayRef> = chunked.chunks.iter().map(TermChunk::array).collect();
                let dtype = chunks[0].dtype().clone();
                ChunkedArray::try_new(chunks, dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array()
                    .execute::<VarBinViewArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?
            }
        };
        let values = canonical_varbinview_to_arrow::<StringViewType>(&canonical, &mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        // A racing first call built its own copy; either one may win the slot.
        Ok(self.arrow_values.get_or_init(|| values).clone())
    }
}

/// Slots in a dictionary's [`ProbeCache`]. A power of two: the slot index is
/// the hash masked to this width.
///
/// Sized for the working set of a query workload — the bound terms of the
/// patterns currently being asked — not for the dictionary.
const PROBE_CACHE_SLOTS: usize = 256;

/// A fixed-size, direct-mapped memo of term → code lookups (absence
/// included): one slot per hash bucket, overwritten on collision, so its
/// footprint never grows. Entries never go stale: a dictionary's terms are
/// immutable and a mutation builds a new dictionary with a fresh cache.
///
/// Used by both [`TermDictionary`] and the file-backed form
/// ([`FileBackedDict`](super::file_backed::FileBackedDict)), whose miss is
/// the same binary search run over cached wire chunks.
pub(super) struct ProbeCache {
    slots: RwLock<Box<[Option<ProbeEntry>]>>,
}

struct ProbeEntry {
    /// A `String`, so an overwrite reuses the allocation: terms in a dataset
    /// are of similar length, so the replacing term usually fits the capacity
    /// the evicted one left behind. A miss is then a hash and a copy, with no
    /// allocator traffic.
    term: String,
    code: Option<u32>,
}

impl ProbeCache {
    pub(super) fn new() -> Self {
        Self {
            slots: RwLock::new(
                std::iter::repeat_with(|| None)
                    .take(PROBE_CACHE_SLOTS)
                    .collect(),
            ),
        }
    }

    /// FNV-1a over the whole term: RDF terms in a dataset share long IRI
    /// prefixes and differ only near the end, so the distinguishing bytes sit
    /// at the tail.
    fn slot(term: &str) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in term.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & (PROBE_CACHE_SLOTS - 1)
    }

    /// `Some(code_or_absent)` on a hit, `None` when this term is not memoized.
    ///
    /// A poisoned lock degrades to a miss rather than propagating: the memo is
    /// an optimization, and losing it must not fail a query.
    pub(super) fn get(&self, term: &str) -> Option<Option<u32>> {
        let slots = self.slots.read().ok()?;
        match &slots[Self::slot(term)] {
            Some(entry) if entry.term == term => Some(entry.code),
            _ => None,
        }
    }

    /// Memoize `code` for `term`, evicting whatever shared its slot.
    pub(super) fn put(&self, term: &str, code: Option<u32>) {
        if let Ok(mut slots) = self.slots.write() {
            let slot = Self::slot(term);
            match &mut slots[slot] {
                Some(entry) => {
                    entry.term.clear();
                    entry.term.push_str(term);
                    entry.code = code;
                }
                empty => {
                    *empty = Some(ProbeEntry {
                        term: term.to_owned(),
                        code,
                    })
                }
            }
        }
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

/// An immutable handle on a Dictionary-layout store's term dictionary, taken
/// with [`VortexRdfStore::code_read_snapshot`](crate::store::VortexRdfStore::code_read_snapshot).
///
/// Cloning is an `Arc` bump, and the snapshot retains only the dictionary — not
/// the store or its quad columns.
///
/// **Term codes are only meaningful against the dictionary they were produced
/// with.** Mutating a store re-encodes it against a *fresh* dictionary, so a
/// consumer holding codes must decode them through the snapshot taken when it
/// received them, not through the store as it stands later — otherwise codes
/// silently resolve to the wrong terms. Holding the snapshot keeps exactly the
/// dictionary those codes address alive, and nothing more.
#[derive(Clone)]
pub struct DictSnapshot(pub(crate) Arc<TermDictionary>);

impl DictSnapshot {
    /// Decode a term code to its N-Triples string, or `None` when the code is
    /// out of this dictionary's range.
    pub fn decode(&self, code: u32) -> Option<String> {
        self.0.decode(code)
    }

    /// Encode an N-Triples term string to its code (its position in the
    /// sorted dictionary), or `None` when this dictionary does not hold the
    /// term. The inverse of [`decode`](Self::decode); a binary search over
    /// the dictionary.
    pub fn encode(&self, term: &str) -> Option<u32> {
        self.0.encode(term)
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    /// The whole dictionary as one Arrow `StringViewArray`: element `i` is
    /// the N-Triples term of code `i`, so it is both a code → term lookup
    /// table and the values array of the batch export's `terms` encoding.
    ///
    /// Built on first use and cached for the dictionary's lifetime: canonical
    /// chunks convert buffer-sharing, FSST-compressed chunks decompress once
    /// (a cost bounded by the dictionary's size, not by any result's).
    pub fn to_arrow(&self) -> Result<ArrowArrayRef> {
        self.0.arrow_values()
    }

    /// The first code whose term is `>= term` in byte order ([`len`](Self::len)
    /// when every term is smaller). Because codes are lexicographic ranks,
    /// `lower_bound(a)..lower_bound(b)` is exactly the codes of the terms in
    /// `a..b`.
    pub fn lower_bound(&self, term: &str) -> u32 {
        self.0.lower_bound_bytes(term.as_bytes())
    }

    /// The half-open code range `[lo, hi)` of the terms whose N-Triples
    /// spelling starts with `prefix` (byte-wise). An IRI namespace is the
    /// prefix `"<http://…/"`, and the N-Triples kinds partition the space by
    /// first byte (`"` literals, `<` IRIs, `_` blank nodes), so kind bounds
    /// are prefix ranges too. Empty prefix ⇒ the full range.
    pub fn prefix_range(&self, prefix: &str) -> (u32, u32) {
        let lo = self.0.lower_bound_bytes(prefix.as_bytes());
        // The successor of the prefix in byte order: strip trailing 0xFF
        // bytes, then increment the last remaining one. All-0xFF (or empty)
        // has no successor — every term from `lo` on matches.
        let mut successor = prefix.as_bytes().to_vec();
        while successor.last() == Some(&0xFF) {
            successor.pop();
        }
        let hi = match successor.last_mut() {
            Some(last) => {
                *last += 1;
                self.0.lower_bound_bytes(&successor)
            }
            None => self.0.len() as u32,
        };
        (lo, hi)
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
            let struct_arr = chunk
                .execute::<StructArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;
            chunks.push(
                struct_arr
                    .unmasked_field_by_name(COL_DICT_TERM)
                    .map_err(VortexRdfError::Vortex)?
                    .clone(),
            );
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(terms: &[&str]) -> TermDictionary {
        let mut sorted = terms.to_vec();
        sorted.sort_unstable();
        TermDictionary::from_sorted(sorted.into_iter()).unwrap()
    }

    /// The memo must be invisible: every lookup agrees with the uncached search,
    /// on repeats and on absent terms alike.
    #[test]
    fn memoized_lookup_matches_the_search() {
        let terms: Vec<String> = (0..500)
            .map(|i| format!("<http://example.org/resource/{i:04}>"))
            .collect();
        let d = dict(&terms.iter().map(String::as_str).collect::<Vec<_>>());

        for probe in terms
            .iter()
            .map(String::as_str)
            .chain(["<http://absent>", ""])
        {
            let expected = d.search(probe);
            // Twice: the first call fills the slot, the second reads it back.
            assert_eq!(d.encode(probe), expected, "cold lookup of {probe}");
            assert_eq!(d.encode(probe), expected, "memoized lookup of {probe}");
        }
    }

    /// Two terms sharing a slot must not read each other's code. With one slot
    /// per bucket the second simply evicts the first, and both stay correct.
    #[test]
    fn colliding_terms_do_not_alias() {
        let terms: Vec<String> = (0..2_000)
            .map(|i| format!("<http://example.org/collide/{i:05}>"))
            .collect();
        let refs: Vec<&str> = terms.iter().map(String::as_str).collect();
        let d = dict(&refs);

        // Far more distinct terms than slots, so collisions are certain.
        assert!(terms.len() > PROBE_CACHE_SLOTS);
        let a = &refs[7];
        let b = refs
            .iter()
            .find(|t| ProbeCache::slot(t) == ProbeCache::slot(a) && *t != a)
            .expect("2000 terms over 256 slots must collide");

        assert_eq!(d.encode(a), d.search(a));
        assert_eq!(d.encode(b), d.search(b));
        // `b` evicted `a`; asking again must re-search, not return `b`'s code.
        assert_eq!(d.encode(a), d.search(a));
        assert_ne!(d.encode(a), d.encode(b));
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
                assert_eq!(d.encode(term), Some(i as u32), "{term}");
                assert_eq!(d.decode(i as u32).as_deref(), Some(term.as_str()));
            }
        }
        assert_eq!(chunked.encode("<http://absent>"), None);
        assert_eq!(chunked.decode(300), None);
    }

    /// The Arrow values array is the decode table verbatim — for a canonical
    /// single-chunk dictionary, a windowed FSST one, and the empty one — and
    /// the second call returns the cached array, not a rebuild.
    #[test]
    fn arrow_values_match_decode() {
        use arrow_array::Array as _;
        use arrow_array::cast::AsArray;

        let terms: Vec<String> = (0..300)
            .map(|i| format!("<http://example.org/arrow/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let canonical = {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            TermDictionary::from_term_chunks(vec![plain.clone().into_array()], &mut ctx).unwrap()
        };
        let windowed = TermDictionary::compress_windowed(plain, 64).unwrap();

        for d in [&canonical, &windowed] {
            let values = d.arrow_values().unwrap();
            let strings = values.as_string_view();
            assert_eq!(strings.len(), d.len());
            for code in 0..d.len() as u32 {
                assert_eq!(strings.value(code as usize), d.decode(code).unwrap());
                assert_eq!(d.encode(strings.value(code as usize)), Some(code));
            }
            let again = d.arrow_values().unwrap();
            assert!(Arc::ptr_eq(&values, &again), "second call must be cached");
        }

        assert_eq!(TermDictionary::empty().arrow_values().unwrap().len(), 0);
    }

    /// `lower_bound` is the partition point over the sorted terms, and
    /// `prefix_range` brackets exactly the terms spelled with the prefix —
    /// including at the byte-successor edge and for absent prefixes.
    #[test]
    fn bounds_bracket_the_sorted_terms() {
        let terms: Vec<String> = (0..40)
            .map(|i| format!("<http://a.example/{i:02}>"))
            .chain((0..40).map(|i| format!("<http://b.example/{i:02}>")))
            .chain((0..20).map(|i| format!("\"literal {i:02}\"")))
            .chain((0..10).map(|i| format!("_:blank{i}")))
            .collect();
        let refs: Vec<&str> = terms.iter().map(String::as_str).collect();
        let snapshot = DictSnapshot(Arc::new(dict(&refs)));

        let mut sorted = refs.clone();
        sorted.sort_unstable();
        for probe in ["", "<http://a.example/", "<http://b.example/2", "\"literal", "_:", "~past"] {
            let expected = sorted.partition_point(|t| *t < probe) as u32;
            assert_eq!(snapshot.lower_bound(probe), expected, "{probe:?}");
        }

        for prefix in ["<http://a.example/", "<http://b.example/", "\"", "<", "_:", ""] {
            let (lo, hi) = snapshot.prefix_range(prefix);
            let expected: Vec<u32> = (0..snapshot.len() as u32)
                .filter(|&c| snapshot.decode(c).unwrap().starts_with(prefix))
                .collect();
            assert_eq!((lo..hi).collect::<Vec<_>>(), expected, "{prefix:?}");
        }
        assert_eq!(snapshot.prefix_range("<http://c."), {
            let lo = snapshot.lower_bound("<http://c.");
            (lo, lo)
        });
        // A prefix byte-wise above every term yields the empty range at the end.
        let (lo, hi) = snapshot.prefix_range("\u{FF}");
        assert_eq!(lo, snapshot.len() as u32);
        assert_eq!(hi, snapshot.len() as u32);
    }
}
