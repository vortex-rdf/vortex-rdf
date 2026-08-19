//! The global term dictionary backing [`LayoutStrategy::Dictionary`]:
//! the lexicographically sorted set of unique RDF term strings, where a term's
//! ID is its sorted position. The s/p/o/g columns store these IDs as u32 codes.
//!
//! Because IDs are sorted ranks, ID comparisons are order-isomorphic to string
//! comparisons and term→ID lookup is a binary search — no HashMap is needed on
//! the query side, and the terms stay in their compact columnar form (see
//! [`TermStore`]: FSST-compressed windows as built and written, plaintext
//! `VarBinViewArray` when a producer wrote them that way) rather than as owned
//! `String`s.
//!
//! [`LayoutStrategy::Dictionary`]: crate::store::layouts::LayoutStrategy::Dictionary

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use web_time::Instant;

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{ChunkedArray, PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::DType;
use vortex_array::expr::{root, select};
use vortex_array::match_each_integer_ptype;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, fsst_compress, fsst_train_compressor};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, buf_as_str};

use super::ingest::BorrowedTermIdMap;

/// The single column of the native container's `dictionary` child: non-nullable
/// utf8, row i holding the term with code i (sorted, so codes are lexicographic
/// ranks). Part of the wire contract, owned here — the module that builds and
/// reads the child — per the ownership rule in [`crate::store::schema`].
pub(crate) const TERM_FIELD: &str = "_dict_term";

/// Terms per FSST window when compressing at the source (see
/// [`TermDictionary::compress`]): the granularity at which a large
/// dictionary's serialized child is read back, point-read, and lifted
/// chunk-by-chunk. Small enough that touching one leaf fetches and adopts a
/// bounded slice of the column; large enough that the per-window
/// symbol-table copy (~2 KiB) stays noise.
const DICT_CHUNK_ROWS: usize = 64 * 1024;

/// How a dictionary's sorted terms are held in memory.
///
/// The dictionary is *built* FSST-compressed (see [`TermDictionary::compress`])
/// and written out that way, so `Fsst` is the normal case. `Canonical` remains
/// reachable and is not a legacy path: Vortex picks a column's encoding when it
/// writes, by sampling, and the selector is free to choose something other than
/// FSST — so a dictionary read back from a file or IPC stream may arrive in any
/// encoding, and the read path has to be total over that. Anything that is not
/// FSST is canonicalized to plaintext on open.
#[derive(Clone)]
enum TermStore {
    /// Plaintext terms. `bytes_at` is a zero-copy read.
    Canonical(VarBinViewArray),
    /// FSST-compressed terms: ~4x smaller, and every read decodes.
    Fsst(FsstTerms),
    /// A multi-chunk term column read back from a serialized dictionary
    /// child, each chunk kept in the encoding it was written in. Holding the
    /// chunks (instead of canonicalizing them into one array) is what keeps a
    /// large dictionary FSST-compressed through the resident lift.
    Chunked(ChunkedTerms),
}

/// The chunks of a multi-chunk term column, with a cumulative-start table
/// mapping a global term index to (chunk, local index).
#[derive(Clone)]
pub(crate) struct ChunkedTerms {
    chunks: Vec<TermChunk>,
    /// `starts[i]` = global index of chunk i's first term; ascending.
    starts: Vec<usize>,
    len: usize,
}

#[derive(Clone)]
pub(crate) enum TermChunk {
    Canonical(VarBinViewArray),
    Fsst(FsstTerms),
}

impl TermChunk {
    /// Adopt one term chunk: kept FSST when it arrived FSST (reads decode
    /// single rows), canonicalized otherwise (the write path compresses, but
    /// nothing in the format obliges a producer to have done so).
    pub(crate) fn from_wire(chunk: ArrayRef, ctx: &mut vortex_array::ExecutionCtx) -> Result<Self> {
        match chunk.try_downcast::<FSST>() {
            Ok(fsst) => Ok(TermChunk::Fsst(FsstTerms::new(fsst)?)),
            Err(other) => Ok(TermChunk::Canonical(
                other
                    .execute::<VarBinViewArray>(ctx)
                    .map_err(VortexRdfError::Vortex)?,
            )),
        }
    }

    /// A fresh cursor over this chunk's terms. Scratch is allocated lazily
    /// inside `bytes_at`: one eager allocation per chunk would tax every
    /// single-term decode with O(chunks) mallocs.
    pub(crate) fn cursor(&self) -> ChunkCursor<'_> {
        match self {
            TermChunk::Canonical(a) => ChunkCursor::Canonical(StrColReader::new(a)),
            TermChunk::Fsst(f) => ChunkCursor::Fsst {
                terms: f,
                scratch: Vec::new(),
            },
        }
    }
}

impl ChunkedTerms {
    /// The chunk holding global index `i`, and `i` local to it.
    fn locate(&self, i: usize) -> (usize, usize) {
        let chunk = self.starts.partition_point(|&s| s <= i) - 1;
        (chunk, i - self.starts[chunk])
    }
}

/// The frozen, sorted term dictionary in columnar form.
///
/// term→ID is a host-side binary search; ID→term reads the term at a position.
/// Both go through [`reader`](Self::reader), whose cost depends on the encoding
/// the terms are held in.
pub(crate) struct TermDictionary {
    terms: TermStore,
    /// Memo for [`get_id`](Self::get_id); see [`ProbeCache`].
    probes: ProbeCache,
}

/// Cloning shares nothing: the memo starts empty rather than being copied.
///
/// The entries would still be *valid* — they describe the terms, which are
/// cloned with them — but a dictionary is normally shared through an `Arc`, so
/// an actual clone is rare enough that carrying the memo across is not worth
/// the copy.
impl Clone for TermDictionary {
    fn clone(&self) -> Self {
        Self {
            terms: self.terms.clone(),
            probes: ProbeCache::new(),
        }
    }
}

impl TermDictionary {
    /// Wrap the held terms, with an empty lookup memo.
    fn new(terms: TermStore) -> Self {
        Self {
            terms,
            probes: ProbeCache::new(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self::new(TermStore::Canonical(VarBinViewArray::from_iter_str(
            std::iter::empty::<&str>(),
        )))
    }

    /// Build from already-sorted unique term strings.
    ///
    /// The terms are FSST-compressed here rather than left for the writer to
    /// compress: which encoding a column gets is otherwise decided by sampling
    /// at write time and is not guaranteed to be FSST, so compressing at the
    /// source is what makes "the dictionary is FSST" an invariant this code
    /// owns instead of an assumption about somebody else's heuristic.
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

    /// Adopt a term column as read back from a file or IPC stream.
    ///
    /// Already-FSST terms are kept compressed — decoding them here would undo
    /// the point of writing them that way, and is what made opening a store
    /// cost a full plaintext copy of its dictionary. Any other encoding is
    /// canonicalized: the write path compresses, but nothing in the format
    /// obliges a producer to have done so.
    fn from_terms_array(elements: ArrayRef, ctx: &mut vortex_array::ExecutionCtx) -> Result<Self> {
        // Flatten `Chunked` containers into their chunks: the dictionary
        // child's writer splits large dictionaries into several chunks, each
        // independently FSST-compressed, and canonicalizing them into one
        // array would decompress the dictionary on open — exactly the copy
        // holding it compressed exists to avoid.
        use vortex_array::arrays::chunked::ChunkedArrayExt as _;
        let mut queue = vec![elements];
        let mut flat = Vec::new();
        while let Some(cur) = queue.pop() {
            match cur.try_downcast::<vortex_array::arrays::Chunked>() {
                // Reverse so the pop order preserves chunk order.
                Ok(chunked) => queue.extend(chunked.chunks().iter().rev().cloned()),
                Err(other) => flat.push(other),
            }
        }
        Self::from_term_chunks(flat, ctx)
    }

    /// Adopt a term column's chunks through [`TermChunk::from_wire`].
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
            0 => TermStore::Canonical(VarBinViewArray::from_iter_str(std::iter::empty::<&str>())),
            1 => match adopted.pop().expect("length checked above") {
                TermChunk::Canonical(a) => TermStore::Canonical(a),
                TermChunk::Fsst(f) => TermStore::Fsst(f),
            },
            _ => {
                let mut starts = Vec::with_capacity(adopted.len());
                let mut len = 0usize;
                for chunk in &adopted {
                    starts.push(len);
                    len += match chunk {
                        TermChunk::Canonical(a) => a.len(),
                        TermChunk::Fsst(f) => f.len(),
                    };
                }
                TermStore::Chunked(ChunkedTerms {
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
            return Ok(Self::new(TermStore::Canonical(plain)));
        }
        let start = Instant::now();
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
                start.elapsed()
            );
            return Ok(Self::new(TermStore::Fsst(terms)));
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
            start.elapsed()
        );
        Ok(Self::new(TermStore::Chunked(ChunkedTerms {
            chunks,
            starts,
            len,
        })))
    }

    /// The dataset's unique terms, sorted — the raw material of
    /// [`from_quads_with_map`](Self::from_quads_with_map). Terms borrow from
    /// `quads`, so nothing is copied.
    fn sorted_unique_terms(quads: &[RawQuad]) -> (Vec<&str>, Duration, Duration) {
        let collect_start = Instant::now();
        let mut set: HashSet<&str> = HashSet::new();
        for q in quads {
            set.insert(&q.s);
            set.insert(&q.p);
            set.insert(&q.o);
            set.insert(&q.g);
        }
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        let mut terms: Vec<&str> = set.into_iter().collect();
        terms.sort_unstable();
        (terms, collect_elapsed, sort_start.elapsed())
    }

    /// Build the dictionary *and* its term→ID map in one pass, with the map
    /// borrowing its keys from `quads`.
    ///
    /// A term's ID is its position in the sorted unique term list, which this
    /// already computes to build the dictionary — so the map costs one pointer
    /// pair per term and no string data at all. The owned-key alternative
    /// ([`TermDictionaryBuilder::finish`]) materializes every term on the heap
    /// a second time, which on a large dataset outweighs the dictionary
    /// itself; it exists for the streaming builders, whose quads cannot be
    /// borrowed from.
    ///
    /// [`TermDictionaryBuilder::finish`]: super::ingest::TermDictionaryBuilder::finish
    pub(crate) fn from_quads_with_map(quads: &[RawQuad]) -> Result<(Self, BorrowedTermIdMap<'_>)> {
        let total_start = Instant::now();
        let (terms, collect_elapsed, sort_elapsed) = Self::sorted_unique_terms(quads);
        let map_start = Instant::now();
        let id_map: BorrowedTermIdMap<'_> = terms
            .iter()
            .enumerate()
            .map(|(id, term)| (*term, id as u32))
            .collect();
        let map_elapsed = map_start.elapsed();
        let freeze_start = Instant::now();
        let dict = Self::from_sorted(terms.into_iter())?;
        log::debug!(
            "[Dictionary] Built dictionary + borrowed ID map from {} quads ({} unique terms): collect {:?}, sort {:?}, map {:?}, freeze {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            map_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok((dict, id_map))
    }

    pub(crate) fn len(&self) -> usize {
        match &self.terms {
            TermStore::Canonical(a) => a.len(),
            TermStore::Fsst(f) => f.len(),
            TermStore::Chunked(c) => c.len,
        }
    }

    /// A cursor over the terms. Holds the scratch buffer an FSST read decodes
    /// into, so callers needing several terms at once (a quad's four roles)
    /// must take one reader per role.
    pub(crate) fn reader(&self) -> DictReader<'_> {
        match &self.terms {
            TermStore::Canonical(a) => DictReader::Canonical(StrColReader::new(a)),
            TermStore::Fsst(f) => DictReader::Fsst {
                terms: f,
                scratch: f.new_scratch(),
            },
            TermStore::Chunked(c) => DictReader::Chunked {
                store: c,
                cursors: c.chunks.iter().map(TermChunk::cursor).collect(),
            },
        }
    }

    /// Decode a code back to its term string (canonical N-Triples form), or
    /// `None` if the code is out of the dictionary's range.
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i >= self.len() {
            return None;
        }
        self.reader().str_at(i).ok().map(str::to_owned)
    }

    /// Look up a term's ID: its position in the sorted dictionary, or `None`
    /// when the dictionary does not hold it.
    ///
    /// Memoized. [`PatternCodes`] already collapses the repeats *within* one
    /// match; this catches the repeats *across* matches, which is the shape
    /// real query workloads have — the same predicate walked over many
    /// patterns, the same subject chained through several matches.
    ///
    /// [`PatternCodes`]: crate::store::layouts::PatternCodes
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        if let Some(memoized) = self.probes.get(term) {
            return memoized;
        }
        let found = self.search(term);
        self.probes.put(term, found);
        found
    }

    /// The uncached binary search behind [`get_id`](Self::get_id).
    fn search(&self, term: &str) -> Option<u32> {
        // Direct byte-compare binary search over the sorted terms. The generic
        // `search_sorted` kernel would instead build a fresh `ExecutionCtx` and
        // a `Scalar` per probe, which profiling showed dominating
        // `match_pattern`'s fixed cost.
        //
        // FSST is not order-preserving — roughly half of adjacent sorted pairs
        // invert in compressed space — so the search cannot run over the codes
        // and every probe decodes into the reader's scratch buffer.
        let mut reader = self.reader();
        let needle = term.as_bytes();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match reader.bytes_at(mid).cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid as u32),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

/// Slots in a dictionary's [`ProbeCache`]. A power of two, so the slot index is
/// a mask rather than a modulo.
///
/// Sized for the working set of a query workload — the bound terms of the
/// patterns currently being asked — not for the dictionary, which is orders of
/// magnitude larger and would defeat the point of bounding this at all.
const PROBE_CACHE_SLOTS: usize = 256;

/// A memo of term → code lookups, fixed in size and direct-mapped: one slot per
/// hash bucket, overwritten on collision.
///
/// A miss costs a binary search over the sorted terms, decoding a term at every
/// step under FSST. Repeats are common enough across matches to be worth
/// catching.
///
/// Direct-mapped rather than an LRU because the memory matters more than the
/// hit rate here: a few KB per dictionary that never grows, against a structure
/// that would accumulate every term ever queried. Wasm linear memory is never
/// returned to the engine, so an unbounded cache is a leak by another name.
///
/// Entries cannot go stale. A dictionary's terms are immutable, so a term's
/// code — or its absence, which is memoized too — is a property of the
/// dictionary itself, not of when it was asked. Mutating a store builds a
/// *new* dictionary, and with it a new cache.
///
/// Shared by the resident dictionary above and the file-backed form
/// ([`FileBackedDict`](super::file_backed::FileBackedDict)), whose miss is the
/// same binary search run over cached wire chunks rather than resident terms.
pub(super) struct ProbeCache {
    slots: RwLock<Box<[Option<ProbeEntry>]>>,
}

struct ProbeEntry {
    /// A `String` rather than a `Box<str>` so an overwrite can reuse the
    /// allocation: terms in a dataset are of similar length, so the replacing
    /// term usually fits the capacity the evicted one left behind. A miss is
    /// then a hash and a copy, with no allocator traffic.
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

    /// FNV-1a over the whole term. Hashing a prefix would be cheaper but
    /// collides catastrophically on RDF data, where terms in a dataset share
    /// long IRI prefixes and differ only near the end.
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
#[derive(Clone)]
pub(crate) struct FsstTerms {
    /// Kept whole so the dictionary can be serialized without recompressing.
    array: FSSTArray,
    /// Code offsets, canonicalized once at open. The array stores them
    /// bit-packed, and unpacking a single value allocates a `Scalar` per call —
    /// far too expensive for a per-probe read inside a binary search.
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
        // dictionary's offsets fit in a u8 — so accept every integer type
        // rather than the couple a given dataset happens to yield.
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
/// `str_at` borrows from the reader rather than from the dictionary because an
/// FSST read decodes into the reader's own scratch buffer, so the borrow ends
/// at the next call. Callers needing several terms simultaneously — decoding a
/// quad's four roles — take one reader per role.
pub(crate) enum DictReader<'a> {
    Canonical(StrColReader<'a>),
    Fsst {
        terms: &'a FsstTerms,
        scratch: Vec<u8>,
    },
    Chunked {
        store: &'a ChunkedTerms,
        /// One cursor per chunk, each with its own scratch, so a read maps
        /// the global index to its chunk and delegates.
        cursors: Vec<ChunkCursor<'a>>,
    },
}

/// A [`DictReader`]'s per-chunk cursor over one chunk of a
/// [`TermStore::Chunked`] store.
pub(crate) enum ChunkCursor<'a> {
    Canonical(StrColReader<'a>),
    Fsst {
        terms: &'a FsstTerms,
        scratch: Vec<u8>,
    },
}

impl ChunkCursor<'_> {
    #[inline]
    pub(crate) fn bytes_at(&mut self, local: usize) -> &[u8] {
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

impl DictReader<'_> {
    #[inline]
    pub(crate) fn bytes_at(&mut self, i: usize) -> &[u8] {
        match self {
            DictReader::Canonical(r) => r.bytes_at(i),
            DictReader::Fsst { terms, scratch } => {
                scratch.clear();
                terms.decode_into(i, scratch)
            }
            DictReader::Chunked { store, cursors } => {
                let (chunk, local) = store.locate(i);
                cursors[chunk].bytes_at(local)
            }
        }
    }

    #[inline]
    pub(crate) fn str_at(&mut self, i: usize) -> Result<&str> {
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
        self.0.term_at(code)
    }

    /// Encode an N-Triples term string to its code (its position in the
    /// sorted dictionary), or `None` when this dictionary does not hold the
    /// term. The inverse of [`decode`](Self::decode); a binary search over
    /// the dictionary.
    pub fn encode(&self, term: &str) -> Option<u32> {
        self.0.get_id(term)
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

/// Read a dictionary child's whole term column into a resident
/// [`TermDictionary`], keeping each chunk's stored encoding (FSST where the
/// writer compressed).
///
/// Drives the scan's per-split futures inline and overlapped (`join_all`, no
/// runtime handle), so it serves the file-backed open, the buffered
/// `open_buffer` open, and the wasm read path alike.
pub(crate) async fn dict_from_reader(
    reader: vortex_layout::LayoutReaderRef,
) -> Result<TermDictionary> {
    if reader.row_count() == 0 {
        return Ok(TermDictionary::empty());
    }
    let scan = vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
        .with_projection(select([TERM_FIELD], root()));
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    let mut chunks = Vec::new();
    for res in futures::future::join_all(tasks).await {
        if let Some(chunk) = res.map_err(VortexRdfError::Vortex)? {
            let struct_arr = chunk
                .execute::<StructArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;
            chunks.push(
                struct_arr
                    .unmasked_field_by_name(TERM_FIELD)
                    .map_err(VortexRdfError::Vortex)?
                    .clone(),
            );
        }
    }
    TermDictionary::from_terms_array(
        match chunks.len() {
            1 => chunks.pop().expect("length checked above"),
            _ => {
                let dtype = DType::Utf8(vortex_array::dtype::Nullability::NonNullable);
                ChunkedArray::try_new(chunks, dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array()
            }
        },
        &mut ctx,
    )
}

/// The dictionary component's body, one `{_dict_term: utf8}` struct per held
/// term chunk — row i of the concatenation = the term with code i, in the
/// encoding the dictionary is held in (FSST when compressed at the source).
/// Chunk-granular because each chunk is a self-contained array (independent
/// FSST windows, see [`TermDictionary::compress`]) written verbatim as one
/// flat leaf, so its boundary survives as a split of the serialized child.
/// Always at least one chunk, possibly empty — the child strategy needs a
/// chunk to write a schema-complete component.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) fn dict_child_chunks(dict: &TermDictionary) -> Result<Vec<ArrayRef>> {
    let wrap = |terms: ArrayRef| -> Result<ArrayRef> {
        let rows = terms.len();
        StructArray::try_new(
            [TERM_FIELD].into(),
            vec![terms],
            rows,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
    };
    match &dict.terms {
        TermStore::Canonical(a) => Ok(vec![wrap(a.clone().into_array())?]),
        TermStore::Fsst(f) => Ok(vec![wrap(f.array.clone().into_array())?]),
        TermStore::Chunked(c) => c
            .chunks
            .iter()
            .map(|chunk| {
                wrap(match chunk {
                    TermChunk::Canonical(a) => a.clone().into_array(),
                    TermChunk::Fsst(f) => f.array.clone().into_array(),
                })
            })
            .collect(),
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
            assert_eq!(d.get_id(probe), expected, "cold lookup of {probe}");
            assert_eq!(d.get_id(probe), expected, "memoized lookup of {probe}");
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

        assert_eq!(d.get_id(a), d.search(a));
        assert_eq!(d.get_id(b), d.search(b));
        // `b` evicted `a`; asking again must re-search, not return `b`'s code.
        assert_eq!(d.get_id(a), d.search(a));
        assert_ne!(d.get_id(a), d.get_id(b));
    }

    /// A cloned dictionary answers the same, starting from an empty memo.
    #[test]
    fn clone_keeps_lookups_correct() {
        let d = dict(&["<http://a>", "<http://b>", "<http://c>"]);
        assert_eq!(d.get_id("<http://b>"), Some(1));
        let copy = d.clone();
        assert_eq!(copy.get_id("<http://b>"), Some(1));
        assert_eq!(copy.get_id("<http://zz>"), None);
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
        assert!(matches!(single.terms, TermStore::Fsst(_)));
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(windowed.get_id(term), Some(i as u32), "{term}");
            assert_eq!(windowed.term_at(i as u32).as_deref(), Some(term.as_str()));
            assert_eq!(single.get_id(term), Some(i as u32));
        }
        assert_eq!(windowed.get_id("<http://absent>"), None);
        assert_eq!(windowed.term_at(1_000), None);
    }
}
