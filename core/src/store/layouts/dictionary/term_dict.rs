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

use std::ops::Range;
use std::sync::OnceLock;

use vortex_buffer::Buffer;

use crate::common::terms::canonical_spelling;
use crate::error::{Result, VortexRdfError};
use crate::store::array::buf_as_str;
use crate::store::schema::TermCode;

use super::predicates::{KindRanges, TermPredicate, Verdict};
use super::storage::{DictCursor, TermChunk, TermStore};

/// The frozen, sorted term dictionary in columnar form.
///
/// term → code is a host-side binary search; code → term reads the term at a
/// position. A read of one term at a time decodes into a single scratch buffer
/// ([`TermStore::bytes_into`]), a batch through a [`cursor`](Self::cursor); the
/// cost of either depends on the encoding the terms are held in. Only the kind
/// ranges are kept beyond a call.
pub(crate) struct TermDictionary {
    pub(super) terms: TermStore,
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

/// The rank of `code` in a dictionary of `n_terms` terms whose first code
/// is `base`, or the error for a code outside it.
pub(super) fn check_code(code: TermCode, base: TermCode, n_terms: usize) -> Result<usize> {
    code_rank(code, base, n_terms).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "Term code {} out of dictionary bounds ({})",
            code, n_terms
        ))
    })
}

impl TermDictionary {
    /// Wrap the held terms.
    pub(super) fn new(terms: TermStore) -> Self {
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

#[cfg(test)]
mod tests {
    use super::super::storage::scratch_hooks;
    use super::*;
    use vortex_array::arrays::VarBinViewArray;

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
}
