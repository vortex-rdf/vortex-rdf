//! The frozen term dictionary: a store's unique N-Triples spellings, sorted,
//! a term's code ([`TermCode`], `u64`) being its rank. Code order is the
//! spellings' byte order, so term → code is a binary search over the held
//! terms and a spelling prefix is one code range. The searches and the
//! candidate checks here serve the file-backed form too.

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::OnceLock;

use vortex_buffer::Buffer;

use crate::common::terms::canonical_spelling;
use crate::error::{Result, VortexRdfError};
use crate::store::array::trusted_str;
use crate::store::schema::TermCode;

use super::predicates::{KindRanges, TermPredicate, Verdict};
use super::storage::{DictCursor, ResidentChunks};

/// The frozen, sorted term dictionary: term → code by binary search, code →
/// term by a read at the term's rank. A single-term read decodes every probe
/// into one scratch buffer ([`ResidentChunks::bytes_into`]); a batch reads
/// through a [`cursor`](Self::cursor). Terms are read as UTF-8 unchecked
/// ([`trusted_str`]), so they must come from a Utf8 column of a store this
/// crate wrote or the caller trusts. Only the kind ranges are cached across
/// calls.
pub(crate) struct TermDictionary {
    /// The identity of this dictionary (see
    /// [`DictReader::dictionary_id`](crate::DictReader::dictionary_id)).
    id: u64,
    pub(super) terms: ResidentChunks,
    code_space: CodeSpace,
    /// The kind ranges, computed on first use.
    kinds: OnceLock<KindRanges>,
}

/// The rank of `code` in a dictionary of `len` terms from `base`; `None` when
/// no term has that code. The one code → rank conversion, checked: a code
/// past what a `usize` holds (on a 32-bit target) is outside, never wrapped
/// onto another term.
#[inline]
fn code_rank(code: TermCode, base: TermCode, len: usize) -> Option<usize> {
    let rank = usize::try_from(code.checked_sub(base)?).ok()?;
    (rank < len).then_some(rank)
}

/// The codes of a dictionary's terms: each term's rank offset by the first
/// term's code. Both residencies convert between code and rank through one.
#[derive(Clone, Copy)]
pub(super) struct CodeSpace {
    /// The code of the first term: 0, unless the tests' `CodeBase` hook
    /// starts this thread's dictionaries past `u32::MAX`, so a few terms
    /// exercise codes no 32-bit width holds. Read once per dictionary.
    #[cfg(test)]
    base: TermCode,
    len: usize,
}

impl CodeSpace {
    /// The codes of `len` terms, from 0 (from the `CodeBase` hook's base
    /// under the tests).
    pub(super) fn new(len: usize) -> Self {
        Self {
            #[cfg(test)]
            base: crate::store::test_hooks::code_base().unwrap_or(0),
            len,
        }
    }

    /// The code of the first term: 0 outside the tests' code-base hook.
    #[inline]
    pub(super) fn base(self) -> TermCode {
        #[cfg(test)]
        {
            self.base
        }
        #[cfg(not(test))]
        {
            0
        }
    }

    #[inline]
    pub(super) fn len(self) -> usize {
        self.len
    }

    /// The rank of `code`; `None` when no term has it.
    #[inline]
    pub(super) fn rank_of(self, code: TermCode) -> Option<usize> {
        code_rank(code, self.base(), self.len)
    }

    /// The code of the term at `rank`. Lossless: a `usize` is at most 64
    /// bits on every target.
    #[inline]
    pub(super) fn code_at(self, rank: usize) -> TermCode {
        self.base() + rank as TermCode
    }

    /// One past the last code: every code is below it.
    #[inline]
    pub(super) fn code_end(self) -> TermCode {
        self.code_at(self.len)
    }
}

/// The rank of `code` in `code_space`; a `Deserialization` error for a code
/// outside it.
pub(super) fn check_code(code: TermCode, code_space: CodeSpace) -> Result<usize> {
    code_space.rank_of(code).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "Term code {} out of dictionary bounds ({})",
            code,
            code_space.len()
        ))
    })
}

/// The next dictionary identity (see
/// [`DictReader::dictionary_id`](crate::DictReader::dictionary_id)), from one
/// process-wide counter shared by resident and file-backed dictionaries.
pub(crate) fn next_dictionary_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl TermDictionary {
    /// A dictionary over `terms`, under a fresh identity.
    pub(super) fn new(terms: ResidentChunks) -> Self {
        Self {
            id: next_dictionary_id(),
            code_space: CodeSpace::new(terms.len()),
            terms,
            kinds: OnceLock::new(),
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    #[inline]
    pub(super) fn code_space(&self) -> CodeSpace {
        self.code_space
    }

    /// The rank of `code`; `None` when no term has it.
    #[inline]
    pub(crate) fn rank_of(&self, code: TermCode) -> Option<usize> {
        self.code_space.rank_of(code)
    }

    #[inline]
    pub(crate) fn code_at(&self, rank: usize) -> TermCode {
        self.code_space.code_at(rank)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.code_space.len()
    }

    /// A cursor over the terms. A read borrows the cursor's scratch until the
    /// next read, so holding several terms at once (a quad's four roles)
    /// takes one cursor each.
    pub(super) fn cursor(&self) -> DictCursor<'_> {
        self.terms.cursor()
    }

    /// The spelling of `code`; `None` when no term has it.
    pub(crate) fn decode(&self, code: TermCode) -> Option<String> {
        let rank = self.rank_of(code)?;
        let mut scratch = Vec::new();
        Some(trusted_str(self.terms.bytes_into(rank, &mut scratch)).to_owned())
    }

    /// The code of `term` exactly as spelled; `None` when the dictionary does
    /// not hold that spelling.
    pub(crate) fn encode_exact(&self, term: &str) -> Option<TermCode> {
        // FSST is not order-preserving, so the search cannot run over the
        // compressed codes: every probe decodes, into one scratch buffer.
        let mut scratch = Vec::new();
        let needle = term.as_bytes();
        find_by(0..self.len(), |i| {
            self.terms.bytes_into(i, &mut scratch).cmp(needle)
        })
        .map(|rank| self.code_at(rank))
    }

    /// The code of the first term not below `needle` in byte order: a present
    /// term's own code, or where an absent one would sort, and one past the
    /// last code when every term is below it. Each probe decodes into one
    /// scratch buffer, as in [`encode_exact`](Self::encode_exact).
    pub(crate) fn lower_bound(&self, needle: &[u8]) -> TermCode {
        let mut scratch = Vec::new();
        let rank = lower_bound_by(0..self.len(), |i| {
            self.terms.bytes_into(i, &mut scratch) < needle
        });
        self.code_at(rank)
    }

    /// The half-open code range of the terms spelled with `prefix`: the lower
    /// bounds of `prefix` and of its [`prefix_successor`]. A term kind (`"`,
    /// `<`, `_:`) and an IRI namespace are each a prefix.
    pub(crate) fn prefix_range(&self, prefix: &str) -> Range<TermCode> {
        let lo = self.lower_bound(prefix.as_bytes());
        let hi = prefix_successor(prefix.as_bytes()).map(|successor| self.lower_bound(&successor));
        prefix_range_from(lo, hi, self.code_space.code_end())
    }

    /// The code ranges of the term kinds, computed on first use.
    pub(crate) fn kind_ranges(&self) -> &KindRanges {
        self.kinds.get_or_init(|| {
            let first_is_empty =
                self.len() > 0 && self.terms.bytes_into(0, &mut Vec::new()).is_empty();
            KindRanges::new(
                self.code_space.base(),
                first_is_empty,
                self.prefix_range("\""),
                self.prefix_range("<"),
                self.prefix_range("_:"),
                self.code_space.code_end(),
            )
        })
    }

    /// The code of `term`, tolerant of spelling: the exact lookup, then that of
    /// its canonical spelling when it differs ([`tolerant_fallback`]).
    /// Malformed input is an error, not an absence.
    pub(crate) fn encode(&self, term: &str) -> Result<Option<TermCode>> {
        if let Some(code) = self.encode_exact(term) {
            return Ok(Some(code));
        }
        Ok(tolerant_fallback(term)?.and_then(|canonical| self.encode_exact(&canonical)))
    }

    /// [`encode`](Self::encode) over a batch, in order.
    pub(crate) fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        terms.iter().map(|term| self.encode(term)).collect()
    }

    /// [`decode`](Self::decode) over a batch, in order, each term handed to
    /// `visit(position, term)` borrowed from one cursor, whose chunk cursors
    /// stay warm across the batch; an out-of-range code visits as `None`.
    pub(crate) fn decode_many_with(
        &self,
        codes: &[TermCode],
        mut visit: impl FnMut(usize, Option<&str>),
    ) {
        let mut cursor = self.cursor();
        for (i, &code) in codes.iter().enumerate() {
            let term = self.rank_of(code).map(|rank| cursor.str_at(rank));
            visit(i, term);
        }
    }

    /// [`decode_many_with`](Self::decode_many_with), one owned string per
    /// position.
    pub(crate) fn decode_many(&self, codes: &[TermCode]) -> Vec<Option<String>> {
        let mut terms = vec![None; codes.len()];
        self.decode_many_with(codes, |i, term| terms[i] = term.map(str::to_owned));
        terms
    }

    /// `predicate` over the candidate `codes`, which must ascend strictly
    /// inside the dictionary ([`check_candidates`]): `(passed, undecided)`,
    /// both ascending subsets of `codes`; a candidate in neither failed. Only
    /// the candidates their kind does not decide
    /// ([`TermPredicate::kind_verdict`]) are read. Nothing is memoized.
    pub(crate) fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        check_candidates(codes, self.code_space)?;
        let (mut verdicts, read) = kind_verdicts(predicate, self.kind_ranges(), codes);
        let mut cursor = self.cursor();
        for at in read {
            // In range: the candidates were checked above.
            if let Some(rank) = self.rank_of(codes[at]) {
                verdicts[at] = predicate.eval(cursor.str_at(rank));
            }
        }
        Ok(split_verdicts(codes, &verdicts))
    }
}

/// The smallest byte string above every string starting with `prefix`: the
/// prefix with its trailing `0xFF` bytes dropped and its last remaining byte
/// incremented. `None` when no string is above them all (an empty or
/// all-`0xFF` prefix).
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

/// A prefix's code range from `lo`, the prefix's lower bound, and `hi`, its
/// successor's (`None` when it has none: every code from `lo` up to `end`
/// then has the prefix). Never below `lo`.
pub(super) fn prefix_range_from(
    lo: TermCode,
    hi: Option<TermCode>,
    end: TermCode,
) -> Range<TermCode> {
    lo..hi.unwrap_or(end).max(lo)
}

/// The spelling a failed exact lookup of `term` retries with: its
/// [`canonical_spelling`], or `None` when that is `term` itself. Malformed
/// input is an error.
pub(super) fn tolerant_fallback(term: &str) -> Result<Option<String>> {
    let canonical = canonical_spelling(term)?;
    Ok((canonical != term).then_some(canonical))
}

/// The first position of `range` where `is_below` fails, `is_below` holding
/// on a prefix of the range and failing on the rest; `range.end` when it
/// holds throughout. One probe per bisection step.
#[inline]
pub(super) fn lower_bound_by(
    range: Range<usize>,
    mut is_below: impl FnMut(usize) -> bool,
) -> usize {
    let (mut lo, mut hi) = (range.start, range.end);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if is_below(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The position of `range` where `cmp` is `Equal`, `cmp` being `Less` before
/// it and `Greater` after it; `None` when no position is. A bisection that
/// returns on the first hit.
#[inline]
pub(super) fn find_by(
    range: Range<usize>,
    mut cmp: impl FnMut(usize) -> Ordering,
) -> Option<usize> {
    let (mut lo, mut hi) = (range.start, range.end);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match cmp(mid) {
            Ordering::Less => lo = mid + 1,
            Ordering::Equal => return Some(mid),
            Ordering::Greater => hi = mid,
        }
    }
    None
}

/// An [`InvalidInput`](VortexRdfError::InvalidInput) error unless `codes`
/// ascend strictly inside `code_space`: the precondition every candidate
/// read checks before it reads.
pub(super) fn check_candidates(codes: &[TermCode], code_space: CodeSpace) -> Result<()> {
    if let Some(pair) = codes.windows(2).find(|pair| pair[0] >= pair[1]) {
        return Err(VortexRdfError::InvalidInput(format!(
            "codes must be sorted and unique: {} follows {}",
            pair[1], pair[0]
        )));
    }
    // Ascending, so the ends bound every candidate.
    for &code in [codes.first(), codes.last()].into_iter().flatten() {
        if code_space.rank_of(code).is_none() {
            return Err(VortexRdfError::InvalidInput(format!(
                "code {code} is outside the dictionary ({} terms)",
                code_space.len()
            )));
        }
    }
    Ok(())
}

/// Each candidate's verdict from its kind ([`TermPredicate::kind_verdict`]),
/// `Unknown` where only its spelling decides, beside the positions of those
/// candidates in `codes`, ascending: the ones to read.
pub(super) fn kind_verdicts(
    predicate: &TermPredicate,
    kinds: &KindRanges,
    codes: &[TermCode],
) -> (Vec<Verdict>, Vec<usize>) {
    let mut read = Vec::new();
    let verdicts = codes
        .iter()
        .enumerate()
        .map(|(at, &code)| {
            predicate
                .kind_verdict(kinds.kind_of_code(code))
                .unwrap_or_else(|| {
                    read.push(at);
                    Verdict::Unknown
                })
        })
        .collect();
    (verdicts, read)
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
        assert_eq!(dict.term_chunks().len(), 32, "32 windows");
        for (i, term) in terms.iter().enumerate() {
            let fresh = scratch_hooks::fresh();
            assert_eq!(dict.encode_exact(term), Some(i as TermCode));
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
        let chunks = dict.term_chunks();
        assert!(chunks.len() == 1 && chunks[0].is::<vortex_fsst::FSST>());
        let regrown = scratch_hooks::regrown();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(dict.encode_exact(term), Some(i as TermCode));
            assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
        }
        assert_eq!(scratch_hooks::regrown(), regrown);
    }
}
