//! The global term dictionary backing [`LayoutStrategy::Dictionary`]:
//! the lexicographically sorted set of unique RDF term strings, where a term's
//! code is its sorted position. The s/p/o/g columns store these codes as
//! [`TermCode`]s (`u64`).
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
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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

use crate::common::terms::canonical_spelling;
use crate::error::{Result, VortexRdfError};
use crate::io::read::scan_reader_chunks;
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, buf_as_str};
use crate::store::schema::TermCode;

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::ingest::BorrowedTermCodeMap;
use super::predicates::{KindRanges, TermPredicate, Verdict};

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
    fn bytes_into<'a>(&'a self, i: usize, scratch: &'a mut Vec<u8>) -> &'a [u8] {
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

/// The frozen, sorted term dictionary in columnar form.
///
/// term → code is a host-side binary search; code → term reads the term at a
/// position. A read of one term at a time decodes into a single scratch buffer
/// ([`TermStore::bytes_into`]), a batch through a [`cursor`](Self::cursor); the
/// cost of either depends on the encoding the terms are held in. Only the kind
/// ranges are kept beyond a call.
pub(crate) struct TermDictionary {
    terms: TermStore,
    /// The kind ranges, computed on first use (a few probes).
    kinds: OnceLock<KindRanges>,
    /// The code of the first term (see `code_base`).
    #[cfg(test)]
    base: TermCode,
}

/// The code a dictionary built or opened now gives its first term, under the
/// tests' `CodeBase` hook: it numbers a dictionary built or opened on its
/// thread from a base past `u32::MAX`, so a handful of terms exercise codes a
/// 32-bit width cannot hold. Read once, by a dictionary's constructor;
/// outside the tests every dictionary starts at 0 (a code is the term's rank,
/// see [`TermDictionary::base`]).
#[cfg(test)]
pub(crate) fn code_base() -> TermCode {
    crate::store::test_hooks::code_base()
}

/// The rank of `code` in a dictionary of `len` terms whose first code is
/// `base`, or `None` when no term of the dictionary has that code. The one
/// code → rank conversion: checked, never a cast, so a code past what a
/// `usize` holds (on a 32-bit target) is outside rather than wrapped onto
/// another term.
#[inline]
pub(crate) fn code_rank(code: TermCode, base: TermCode, len: usize) -> Option<usize> {
    let rank = usize::try_from(code.checked_sub(base)?).ok()?;
    (rank < len).then_some(rank)
}

/// The code of the term at `rank` of a dictionary whose first code is
/// `base`. Lossless: a `usize` is at most 64 bits on every target.
#[inline]
pub(crate) fn rank_code(rank: usize, base: TermCode) -> TermCode {
    base + rank as TermCode
}

impl TermDictionary {
    /// Wrap the held terms.
    fn new(terms: TermStore) -> Self {
        Self {
            terms,
            kinds: OnceLock::new(),
            #[cfg(test)]
            base: code_base(),
        }
    }

    /// The code of the first term: 0 outside the tests' code-base hook.
    #[inline]
    pub(crate) fn base(&self) -> TermCode {
        #[cfg(test)]
        {
            self.base
        }
        #[cfg(not(test))]
        {
            0
        }
    }

    /// The rank of `code`, or `None` when this dictionary holds no term
    /// with that code.
    #[inline]
    pub(crate) fn rank_of(&self, code: TermCode) -> Option<usize> {
        code_rank(code, self.base(), self.len())
    }

    /// The code of the term at `rank`.
    #[inline]
    pub(crate) fn code_at(&self, rank: usize) -> TermCode {
        rank_code(rank, self.base())
    }

    /// One past the last code: every code of this dictionary is below it.
    #[inline]
    pub(crate) fn code_end(&self) -> TermCode {
        self.code_at(self.len())
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
        let freeze_start = debug::timer();
        let dict = Self::from_sorted(terms.iter().copied())?;
        let freeze_elapsed = debug::elapsed(freeze_start);
        let map_start = debug::timer();
        let code_map: BorrowedTermCodeMap<'_> = terms
            .into_iter()
            .enumerate()
            .map(|(rank, term)| (term, dict.code_at(rank)))
            .collect();
        log::debug!(
            "[Dictionary] Built dictionary + borrowed code map from {} quads ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, map {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_elapsed,
            debug::elapsed(map_start),
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
    pub(crate) fn decode(&self, code: TermCode) -> Option<String> {
        let rank = self.rank_of(code)?;
        let mut scratch = Vec::new();
        buf_as_str(self.terms.bytes_into(rank, &mut scratch))
            .ok()
            .map(str::to_owned)
    }

    /// Encode a term to its code: its position in the sorted dictionary, or
    /// `None` when the dictionary does not hold it. A binary search with a
    /// three-way compare per step, returning as soon as the probe hits.
    pub(crate) fn encode(&self, term: &str) -> Option<TermCode> {
        // FSST is not order-preserving, so the search cannot run over the
        // compressed codes: every probe decodes, into one scratch buffer.
        let mut scratch = Vec::new();
        let needle = term.as_bytes();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.terms.bytes_into(mid, &mut scratch).cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(self.code_at(mid)),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// The code of the first term not below `needle` in byte order — the
    /// dictionary's size when every term is below it. A binary search with
    /// the same per-probe decode as [`encode`](Self::encode); the position
    /// is where `needle` would be inserted, so a present term's code and
    /// the start of a spelling prefix's run both come out of it.
    pub(crate) fn lower_bound(&self, needle: &[u8]) -> TermCode {
        let mut scratch = Vec::new();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.terms.bytes_into(mid, &mut scratch) < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.code_at(lo)
    }

    /// The codes of the terms spelled with `prefix`, as a half-open range:
    /// two lower bounds, of the prefix and of its byte successor (see
    /// [`prefix_successor`]). Term kinds are prefixes (`"`, `<`, `_:`), and
    /// so are IRI namespaces.
    pub(crate) fn prefix_range(&self, prefix: &str) -> Range<TermCode> {
        let lo = self.lower_bound(prefix.as_bytes());
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => self.lower_bound(&successor),
            None => self.code_end(),
        };
        lo..hi.max(lo)
    }

    /// The code ranges of the term kinds, computed once per dictionary.
    pub(crate) fn kind_ranges(&self) -> &KindRanges {
        self.kinds.get_or_init(|| {
            let default_graph = (self.len() > 0
                && self.terms.bytes_into(0, &mut Vec::new()).is_empty())
            .then(|| self.code_at(0));
            KindRanges {
                start: self.base(),
                default_graph,
                literals: self.prefix_range("\""),
                iris: self.prefix_range("<"),
                blanks: self.prefix_range("_:"),
                len: self.code_end(),
            }
        })
    }

    /// [`encode`](Self::encode) tolerant of spelling: the exact lookup first,
    /// then — only when the term's [`canonical_spelling`] differs from what
    /// was typed — the lookup of that spelling. A term absent under both is
    /// absent from the dictionary; malformed input is an error rather than
    /// an absence.
    pub(crate) fn encode_tolerant(&self, term: &str) -> Result<Option<TermCode>> {
        if let Some(code) = self.encode(term) {
            return Ok(Some(code));
        }
        let canonical = canonical_spelling(term)?;
        if canonical == term {
            return Ok(None);
        }
        Ok(self.encode(&canonical))
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub(crate) fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        terms
            .iter()
            .map(|term| self.encode_tolerant(term))
            .collect()
    }

    /// [`decode`](Self::decode) over a batch, in order, handed to
    /// `visit(position, term)` instead of collected: one cursor serves the
    /// batch (so a chunked dictionary keeps its warm chunk cursors), and an
    /// out-of-range code visits as `None`. Terms are trusted to be UTF-8, as
    /// the column's dtype declares.
    pub(crate) fn decode_many_with(
        &self,
        codes: &[TermCode],
        mut visit: impl FnMut(usize, Option<&str>),
    ) {
        let mut cursor = self.cursor();
        for (i, &code) in codes.iter().enumerate() {
            let term = self.rank_of(code).and_then(|rank| cursor.str_at(rank).ok());
            visit(i, term);
        }
    }

    /// [`decode_many_with`](Self::decode_many_with) collected: one owned
    /// string per decoded position.
    pub(crate) fn decode_many(&self, codes: &[TermCode]) -> Vec<Option<String>> {
        let mut terms = vec![None; codes.len()];
        self.decode_many_with(codes, |i, term| terms[i] = term.map(str::to_owned));
        terms
    }

    /// `predicate` over the candidate `codes` (ascending, unique, inside the
    /// dictionary): `(passed, undecided)`, both ascending subsets of `codes`,
    /// a candidate in neither having failed. A code its kind decides
    /// ([`TermPredicate::kind_verdict`]) is not read; every other one is.
    /// Nothing is memoized.
    pub(crate) fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        check_candidates(codes, self.base(), self.len())?;
        let kinds = self.kind_ranges();
        let mut cursor = self.cursor();
        let verdicts: Vec<Verdict> = codes
            .iter()
            .map(
                |&code| match predicate.kind_verdict(kinds.kind_of_code(code)) {
                    Some(verdict) => verdict,
                    // In range: the candidates were checked above.
                    None => match self.rank_of(code).map(|rank| cursor.str_at(rank)) {
                        Some(Ok(spelling)) => predicate.eval(spelling),
                        // A term that is not UTF-8 is nothing the rules speak of.
                        _ => Verdict::Unknown,
                    },
                },
            )
            .collect();
        Ok(split_verdicts(codes, &verdicts))
    }
}

/// The smallest byte string greater than every string starting with
/// `prefix`: the prefix with its trailing `0xFF` bytes dropped and the last
/// remaining byte incremented — or `None` when no such string exists (an
/// empty or all-`0xFF` prefix, which every string sorts under).
pub(super) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.len();
    while end > 0 && prefix[end - 1] == 0xFF {
        end -= 1;
    }
    if end == 0 {
        return None;
    }
    let mut successor = prefix[..end].to_vec();
    successor[end - 1] += 1;
    Some(successor)
}

/// `codes` must be strictly ascending and inside a dictionary of `len`
/// terms whose first code is `base` — the contract every candidate
/// evaluation checks before reading.
pub(super) fn check_candidates(codes: &[TermCode], base: TermCode, len: usize) -> Result<()> {
    if let Some(pair) = codes.windows(2).find(|pair| pair[0] >= pair[1]) {
        return Err(VortexRdfError::InvalidOperation(format!(
            "codes must be sorted and unique: {} follows {}",
            pair[1], pair[0]
        )));
    }
    // Ascending, so the ends bound every candidate.
    for &code in [codes.first(), codes.last()].into_iter().flatten() {
        if code_rank(code, base, len).is_none() {
            return Err(VortexRdfError::InvalidOperation(format!(
                "code {code} is outside the dictionary ({len} terms)"
            )));
        }
    }
    Ok(())
}

/// The candidates' verdicts as `(passed, undecided)`, both ascending
/// subsets of `codes`; a code in neither failed.
pub(super) fn split_verdicts(
    codes: &[TermCode],
    verdicts: &[Verdict],
) -> (Buffer<TermCode>, Buffer<TermCode>) {
    let (mut passed, mut undecided) = (Vec::new(), Vec::new());
    for (&code, verdict) in codes.iter().zip(verdicts) {
        match verdict {
            Verdict::True => passed.push(code),
            Verdict::Unknown => undecided.push(code),
            Verdict::False => {}
        }
    }
    (Buffer::from(passed), Buffer::from(undecided))
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
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
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
mod scratch_hooks {
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
    pub(super) fn fresh() -> usize {
        FRESH.with(Cell::get)
    }

    /// The times this thread's reads found their buffer too small and grew
    /// it.
    pub(super) fn regrown() -> usize {
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
    pub fn decode(&self, code: TermCode) -> Option<String> {
        self.0.decode(code)
    }

    /// Encode an N-Triples term string to its code (its position in the
    /// sorted dictionary), or `None` when this dictionary does not hold the
    /// term. The inverse of [`decode`](Self::decode); a binary search over
    /// the dictionary.
    pub fn encode(&self, term: &str) -> Option<TermCode> {
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

    /// [`encode`](Self::encode) tolerant of spelling: an IRI with or without
    /// angle brackets, a literal with escape variants, an `xsd:string`
    /// typing, an upper-case language tag, or a default-graph spelling
    /// (`""`, `default`, `[]`) all resolve to the code of the stored form
    /// (see [`canonical_spelling`]). Malformed input is an error.
    pub fn encode_tolerant(&self, term: &str) -> Result<Option<TermCode>> {
        self.0.encode_tolerant(term)
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        self.0.encode_many(terms)
    }

    /// [`decode`](Self::decode) over a batch, in order, through one cursor;
    /// an out-of-range code decodes to `None`.
    pub fn decode_many(&self, codes: &[TermCode]) -> Vec<Option<String>> {
        self.0.decode_many(codes)
    }

    /// [`decode_many`](Self::decode_many) without a `String` per term:
    /// `visit(i, term)` is called once for each position `i` of `codes`, in
    /// order, with the spelling borrowed from the cursor (`None` for an
    /// out-of-range code).
    pub fn decode_many_with(&self, codes: &[TermCode], visit: impl FnMut(usize, Option<&str>)) {
        self.0.decode_many_with(codes, visit);
    }

    /// The code of the first term not below `term` in byte order (the
    /// dictionary's size when every term is below it): a present term's own
    /// code, or where an absent one would sort.
    pub fn lower_bound(&self, term: &str) -> TermCode {
        self.0.lower_bound(term.as_bytes())
    }

    /// The half-open code range `(lo, hi)` of the terms spelled with
    /// `prefix`. Codes are lexicographic ranks of the N-Triples spelling, so
    /// a term kind (`"` for literals, `<` for IRIs, `_:` for blank nodes)
    /// and an IRI namespace (`<http://example.org/`) are each one range.
    pub fn prefix_range(&self, prefix: &str) -> (TermCode, TermCode) {
        let range = self.0.prefix_range(prefix);
        (range.start, range.end)
    }

    /// The code ranges of the term kinds.
    pub fn kind_ranges(&self) -> KindRanges {
        self.0.kind_ranges().clone()
    }

    /// `predicate` over the candidate `codes` — ascending, unique and inside
    /// the dictionary, else an `InvalidOperation` error: `(passed,
    /// undecided)`, both ascending subsets of `codes`; a candidate in
    /// neither fails the predicate. Only candidates whose kind does not
    /// decide them are read. Nothing is memoized.
    pub fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        self.0.filter_codes(predicate, codes)
    }
}

/// A handle on a Dictionary-layout store's term dictionary under either
/// residency, taken with
/// [`VortexRdfStore::dict_reader`](crate::store::VortexRdfStore::dict_reader):
/// the same term ↔ code surface as [`DictSnapshot`], asynchronous so a
/// dictionary left in its file can answer by reading it. Every method
/// completes without suspending on a resident dictionary.
///
/// Like a snapshot, codes are only meaningful against the dictionary they
/// were produced with; a file-backed reader additionally keeps the store's
/// file handle alive.
#[derive(Clone)]
pub struct DictReader(DictReaderInner);

#[derive(Clone)]
enum DictReaderInner {
    Resident(Arc<TermDictionary>),
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl From<DictSnapshot> for DictReader {
    /// The resident handle on a snapshot's dictionary.
    fn from(snapshot: DictSnapshot) -> Self {
        Self::resident(snapshot.0)
    }
}

impl DictReader {
    pub(crate) fn resident(dict: Arc<TermDictionary>) -> Self {
        Self(DictReaderInner::Resident(dict))
    }

    #[cfg(feature = "file-io")]
    pub(crate) fn file_backed(dict: FileBackedDict) -> Self {
        Self(DictReaderInner::FileBacked(dict))
    }

    /// Whether the terms are read from the file on demand (`true`) or held
    /// in memory (`false`).
    pub fn is_file_backed(&self) -> bool {
        match &self.0 {
            DictReaderInner::Resident(_) => false,
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(_) => true,
        }
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.len(),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.len(),
        }
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The synchronous [`DictSnapshot`] of a resident dictionary, `None`
    /// when the terms are file-backed.
    pub fn snapshot(&self) -> Option<DictSnapshot> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Some(DictSnapshot(Arc::clone(dict))),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(_) => None,
        }
    }

    /// The N-Triples string for `code`, or `None` when the code is out of
    /// range.
    pub async fn decode(&self, code: TermCode) -> Result<Option<String>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.decode(code)),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => {
                Ok(dict.decode_many_any(&[code]).await?.pop().flatten())
            }
        }
    }

    /// [`decode`](Self::decode) over a batch, in order: any order and
    /// repeats are fine (a file-backed dictionary reads each distinct code
    /// once, in one batch).
    pub async fn decode_many(&self, codes: &[TermCode]) -> Result<Vec<Option<String>>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.decode_many(codes)),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.decode_many_any(codes).await,
        }
    }

    /// [`decode_many`](Self::decode_many) without a `String` per term:
    /// `visit(i, term)` is called once for each position `i` of `codes`, with
    /// the spelling borrowed for the call (`None` for an out-of-range code).
    /// Codes may come in any order and repeat. A resident dictionary visits
    /// in the order of `codes`; a file-backed one visits the out-of-range
    /// positions first, then reads each distinct code once, in one batch, and
    /// visits every position holding it, in code order. The `Result` reports
    /// a failed read of the file; terms are trusted to be UTF-8, as the
    /// column's dtype declares.
    pub async fn decode_many_with(
        &self,
        codes: &[TermCode],
        visit: impl FnMut(usize, Option<&str>),
    ) -> Result<()> {
        match &self.0 {
            DictReaderInner::Resident(dict) => {
                dict.decode_many_with(codes, visit);
                Ok(())
            }
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.decode_many_with(codes, visit).await,
        }
    }

    /// The code of `term`, tolerant of spelling (see
    /// [`DictSnapshot::encode_tolerant`]).
    pub async fn encode(&self, term: &str) -> Result<Option<TermCode>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.encode_tolerant(term),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.encode_tolerant(term).await,
        }
    }

    /// [`encode`](Self::encode) over a batch, in order (a file-backed
    /// dictionary overlaps the lookups' reads).
    pub async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.encode_many(terms),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.encode_many(terms).await,
        }
    }

    /// See [`DictSnapshot::lower_bound`].
    pub async fn lower_bound(&self, term: &str) -> Result<TermCode> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.lower_bound(term.as_bytes())),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.lower_bound(term.as_bytes()).await,
        }
    }

    /// See [`DictSnapshot::prefix_range`].
    pub async fn prefix_range(&self, prefix: &str) -> Result<(TermCode, TermCode)> {
        let range = match &self.0 {
            DictReaderInner::Resident(dict) => dict.prefix_range(prefix),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.prefix_range(prefix).await?,
        };
        Ok((range.start, range.end))
    }

    /// See [`DictSnapshot::kind_ranges`].
    pub async fn kind_ranges(&self) -> Result<KindRanges> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.kind_ranges().clone()),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.kind_ranges().await,
        }
    }

    /// See [`DictSnapshot::filter_codes`]. A file-backed dictionary reads
    /// only the windows holding the candidates it must read.
    pub async fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.filter_codes(predicate, codes),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.filter_codes(predicate, codes).await,
        }
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
            Arc::new(BufferedComponentSource::try_new(chunks).map_err(VortexRdfError::Vortex)?),
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

    /// A single-term read — an encode, a lower bound, a decode — decodes every
    /// probe into one scratch buffer, so it allocates one buffer however many
    /// chunks its binary search crosses.
    #[test]
    fn a_single_term_read_allocates_one_scratch() {
        let terms: Vec<String> = (0..1024)
            .map(|i| format!("<http://example.org/term/{i:05}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let dict = TermDictionary::compress_windowed(plain, 32).unwrap();
        assert!(matches!(dict.terms, TermStore::Chunked(_)), "32 windows");
        for (i, term) in terms.iter().enumerate() {
            let fresh = scratch_hooks::fresh();
            assert_eq!(dict.encode(term), Some(i as TermCode));
            assert_eq!(scratch_hooks::fresh() - fresh, 1, "encode {i}");
            let fresh = scratch_hooks::fresh();
            assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
            assert_eq!(scratch_hooks::fresh() - fresh, 1, "lower_bound {i}");
            let fresh = scratch_hooks::fresh();
            assert_eq!(dict.decode(i as TermCode).as_deref(), Some(term.as_str()));
            assert_eq!(scratch_hooks::fresh() - fresh, 1, "decode {i}");
        }
    }

    /// A lookup's buffer starts with room for terms of usual length, so the
    /// probes of a search over such terms — of whatever lengths, in a single
    /// chunk — never grow it.
    #[test]
    fn a_lookup_over_usual_terms_never_regrows_its_scratch() {
        let terms: Vec<String> = (0..512)
            .map(|i| format!("<http://example.org/{}/{i:04}>", "x".repeat(i % 90)))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let dict = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
        assert!(matches!(dict.terms, TermStore::Single(TermChunk::Fsst(_))));
        let regrown = scratch_hooks::regrown();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(dict.encode(term), Some(i as TermCode));
            assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
        }
        assert_eq!(scratch_hooks::regrown(), regrown);
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
        use crate::store::layouts::dictionary::file_backed;
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
