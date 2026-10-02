//! The frozen, sorted term dictionary: term ↔ code lookups by binary
//! search over the held terms, prefix and kind ranges, predicate
//! partitions, and the memos that cache them.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, OnceLock, RwLock};

use vortex_buffer::Buffer;

use crate::common::terms::canonical_spelling;
use crate::error::Result;

use super::predicates::{KindRanges, Scanned, TermPredicate};
use super::storage::{DictCursor, TermChunk, TermStore};

/// The frozen, sorted term dictionary in columnar form.
///
/// term → code is a host-side binary search; code → term reads the term at a
/// position. Both go through [`cursor`](Self::cursor), whose cost depends on
/// the encoding the terms are held in.
pub(crate) struct TermDictionary {
    /// Identity of this dictionary instance — see [`DictReader::dictionary_id`].
    id: u64,
    pub(super) terms: TermStore,
    /// Memo for [`encode`](Self::encode); see [`EncodeMemo`].
    encode_memo: EncodeMemo,
    /// The kind ranges, computed on first use (a few probes).
    kinds: OnceLock<KindRanges>,
    /// Memo for [`filter_codes`](Self::filter_codes); see [`PredicateMemo`].
    predicates: PredicateMemo,
}

/// The next dictionary identity (see [`DictReader::dictionary_id`]): one
/// process-wide counter shared by resident and file-backed dictionaries.
pub(crate) fn next_dictionary_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl TermDictionary {
    /// This instance's identity.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Wrap the held terms, with an empty lookup memo.
    pub(super) fn new(terms: TermStore) -> Self {
        Self {
            id: next_dictionary_id(),
            terms,
            encode_memo: EncodeMemo::new(),
            kinds: OnceLock::new(),
            predicates: PredicateMemo::new(),
        }
    }
}

impl TermDictionary {
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
        if let Some(memoized) = self.encode_memo.get(term) {
            return memoized;
        }
        let found = self.search(term);
        self.encode_memo.put(term, found);
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

    /// The code of the first term not below `needle` in byte order — the
    /// dictionary's size when every term is below it. A binary search with
    /// the same per-probe decode as [`search`](Self::search); the position
    /// is where `needle` would be inserted, so a present term's code and
    /// the start of a spelling prefix's run both come out of it.
    pub(crate) fn lower_bound(&self, needle: &[u8]) -> u32 {
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

    /// The codes of the terms spelled with `prefix`, as a half-open range:
    /// two lower bounds, of the prefix and of its byte successor (see
    /// [`prefix_successor`]). Term kinds are prefixes (`"`, `<`, `_:`), and
    /// so are IRI namespaces.
    pub(crate) fn prefix_range(&self, prefix: &str) -> Range<u32> {
        let lo = self.lower_bound(prefix.as_bytes());
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => self.lower_bound(&successor),
            None => self.len() as u32,
        };
        lo..hi.max(lo)
    }

    /// The code ranges of the term kinds, computed once per dictionary.
    pub(crate) fn kind_ranges(&self) -> &KindRanges {
        self.kinds.get_or_init(|| {
            let default_graph =
                (self.len() > 0 && self.cursor().bytes_at(0).is_empty()).then_some(0);
            KindRanges {
                default_graph,
                literals: self.prefix_range("\""),
                iris: self.prefix_range("<"),
                blanks: self.prefix_range("_:"),
                len: self.len() as u32,
            }
        })
    }

    /// [`encode`](Self::encode) tolerant of spelling: the exact lookup first,
    /// then — only when the term's [`canonical_spelling`] differs from what
    /// was typed — the lookup of that spelling. A term absent under both is
    /// absent from the dictionary; malformed input is an error rather than
    /// an absence.
    pub(crate) fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
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
    pub(crate) fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        terms
            .iter()
            .map(|term| self.encode_tolerant(term))
            .collect()
    }

    /// [`decode`](Self::decode) over a batch, in order, through one cursor
    /// (so a chunked dictionary keeps its warm chunk cursors across the
    /// batch); an out-of-range code decodes to `None`.
    pub(crate) fn decode_many(&self, codes: &[u32]) -> Vec<Option<String>> {
        let len = self.len();
        let mut cursor = self.cursor();
        codes
            .iter()
            .map(|&code| {
                if (code as usize) < len {
                    cursor.str_at(code as usize).ok().map(str::to_owned)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Partition the codes by `predicate`'s verdicts — `(true, unknown)`,
    /// both ascending — in one pass over the predicate's scan range (see
    /// [`TermPredicate::scan_plan`]), memoized per dictionary by the
    /// predicate's canonical rendering ([`PredicateMemo`]).
    pub(crate) fn filter_codes(&self, predicate: &TermPredicate) -> VerdictSets {
        let key = predicate.to_string();
        if let Some(sets) = self.predicates.get(&key) {
            return sets;
        }
        let kinds = self.kind_ranges();
        let plan = predicate.scan_plan(kinds);
        let mut scanned = Scanned::default();
        if let Some(range) = plan.scan {
            let mut cursor = self.cursor();
            for code in range {
                match cursor.str_at(code as usize) {
                    Ok(spelling) => scanned.visit(predicate, code, spelling),
                    // A term that is not UTF-8 is nothing the rules speak of.
                    Err(_) => scanned.unknown.push(code),
                }
            }
        }
        let true_ranges: Vec<Range<u32>> = plan
            .true_prefixes
            .iter()
            .map(|prefix| self.prefix_range(prefix))
            .collect();
        let sets = Arc::new(predicate.assemble(kinds, scanned, &true_ranges));
        self.predicates.put(key, Arc::clone(&sets));
        sets
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

/// A predicate's partition of a dictionary's codes — `(true, unknown)`,
/// both ascending — shared between the memo and every caller.
pub(crate) type VerdictSets = Arc<(Buffer<u32>, Buffer<u32>)>;

/// Partitions a dictionary's [`PredicateMemo`] holds before the oldest is
/// evicted. A query workload asks a handful of distinct predicates per
/// query, and an entry can be as wide as the dictionary, so the memo is
/// bounded rather than keyed on everything ever asked.
const PREDICATE_MEMO_SLOTS: usize = 32;

/// A bounded first-in-first-out memo of predicate partitions, keyed by the
/// predicate's canonical rendering. Like [`EncodeMemo`], a poisoned lock
/// degrades to a miss.
pub(super) struct PredicateMemo {
    entries: RwLock<VecDeque<(String, VerdictSets)>>,
}

impl PredicateMemo {
    pub(super) fn new() -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(PREDICATE_MEMO_SLOTS)),
        }
    }

    pub(super) fn get(&self, key: &str) -> Option<VerdictSets> {
        let entries = self.entries.read().ok()?;
        entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, sets)| Arc::clone(sets))
    }

    pub(super) fn put(&self, key: String, sets: VerdictSets) {
        if let Ok(mut entries) = self.entries.write() {
            if entries.iter().any(|(k, _)| *k == key) {
                return;
            }
            if entries.len() == PREDICATE_MEMO_SLOTS {
                entries.pop_front();
            }
            entries.push_back((key, sets));
        }
    }
}

/// Slots in a dictionary's [`EncodeMemo`]. A power of two: the slot index is
/// the hash masked to this width.
///
/// Sized for the working set of a query workload — the bound terms of the
/// patterns currently being asked — not for the dictionary.
const ENCODE_MEMO_SLOTS: usize = 256;

/// A fixed-size, direct-mapped memo of term → code lookups (absence
/// included): one slot per hash bucket, overwritten on collision, so its
/// footprint never grows. Entries never go stale: a dictionary's terms are
/// immutable and a mutation builds a new dictionary with a fresh cache.
///
/// Used by both [`TermDictionary`] and the file-backed form
/// ([`FileBackedDict`](super::file_backed::FileBackedDict)), whose miss is
/// the same binary search run over cached wire chunks.
pub(super) struct EncodeMemo {
    slots: RwLock<Box<[Option<EncodeEntry>]>>,
}

struct EncodeEntry {
    /// A `String`, so an overwrite reuses the allocation: terms in a dataset
    /// are of similar length, so the replacing term usually fits the capacity
    /// the evicted one left behind. A miss is then a hash and a copy, with no
    /// allocator traffic.
    term: String,
    code: Option<u32>,
}

impl EncodeMemo {
    pub(super) fn new() -> Self {
        Self {
            slots: RwLock::new(
                std::iter::repeat_with(|| None)
                    .take(ENCODE_MEMO_SLOTS)
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
        (h as usize) & (ENCODE_MEMO_SLOTS - 1)
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
                    *empty = Some(EncodeEntry {
                        term: term.to_owned(),
                        code,
                    })
                }
            }
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
        assert!(terms.len() > ENCODE_MEMO_SLOTS);
        let a = &refs[7];
        let b = refs
            .iter()
            .find(|t| EncodeMemo::slot(t) == EncodeMemo::slot(a) && *t != a)
            .expect("2000 terms over 256 slots must collide");

        assert_eq!(d.encode(a), d.search(a));
        assert_eq!(d.encode(b), d.search(b));
        // `b` evicted `a`; asking again must re-search, not return `b`'s code.
        assert_eq!(d.encode(a), d.search(a));
        assert_ne!(d.encode(a), d.encode(b));
    }
}
