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
use super::storage::{DictCursor, ResidentChunks};

/// The frozen, sorted term dictionary: term → code by binary search, code →
/// term by position, both through a [`cursor`](Self::cursor). Codes are
/// meaningful only against the dictionary that produced them; a mutation
/// builds a fresh one. Equal [`id`](Self::id)s mean equal codes, different
/// ids promise nothing.
pub(crate) struct TermDictionary {
    id: u64,
    pub(super) terms: ResidentChunks,
    memos: DictMemos,
}

/// The next dictionary identity: one process-wide counter shared by resident
/// and file-backed dictionaries.
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

    /// Wrap the held terms, with empty memos.
    pub(super) fn new(terms: ResidentChunks) -> Self {
        Self {
            id: next_dictionary_id(),
            terms,
            memos: DictMemos::default(),
        }
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        self.terms.len
    }

    /// A cursor over the terms; one per simultaneously held term.
    pub(super) fn cursor(&self) -> DictCursor<'_> {
        DictCursor::new(&self.terms)
    }

    /// The term with code `code`, `None` when out of range.
    pub(crate) fn decode(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i >= self.len() {
            return None;
        }
        self.cursor().str_at(i).ok().map(str::to_owned)
    }

    /// The code of `term`, `None` when absent; memoized.
    pub(crate) fn encode(&self, term: &str) -> Option<u32> {
        if let Some(memoized) = self.memos.encode.get(term) {
            return memoized;
        }
        let found = self.search(term);
        self.memos.encode.put(term, found);
        found
    }

    /// The unmemoized lookup behind [`encode`](Self::encode).
    fn search(&self, term: &str) -> Option<u32> {
        self.cursor().search(term.as_bytes())
    }

    /// The code of the first term not below `needle` in byte order; the term
    /// count when every term is below it.
    pub(crate) fn lower_bound(&self, needle: &[u8]) -> u32 {
        self.cursor().lower_bound(needle)
    }

    /// The codes of the terms spelled with `prefix`: the lower bounds of the
    /// prefix and of its [`prefix_successor`].
    pub(crate) fn prefix_range(&self, prefix: &str) -> Range<u32> {
        let lo = self.lower_bound(prefix.as_bytes());
        let hi = prefix_successor(prefix.as_bytes()).map(|successor| self.lower_bound(&successor));
        prefix_range_from(lo, hi, self.len() as u32)
    }

    /// The code ranges of the term kinds, computed once.
    pub(crate) fn kind_ranges(&self) -> &KindRanges {
        self.memos.kinds.get_or_init(|| {
            let first_is_empty = self.len() > 0 && self.cursor().bytes_at(0).is_empty();
            KindRanges::new(
                first_is_empty,
                self.prefix_range("\""),
                self.prefix_range("<"),
                self.prefix_range("_:"),
                self.len() as u32,
            )
        })
    }

    /// [`encode`](Self::encode) of `term`, then of its canonical spelling
    /// when that differs. Malformed input is an error.
    pub(crate) fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        if let Some(code) = self.encode(term) {
            return Ok(Some(code));
        }
        Ok(tolerant_fallback(term)?.and_then(|canonical| self.encode(&canonical)))
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub(crate) fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        terms
            .iter()
            .map(|term| self.encode_tolerant(term))
            .collect()
    }

    /// [`decode`](Self::decode) over a batch, in order, through one cursor;
    /// an out-of-range code decodes to `None`.
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

    /// The `(true, unknown)` partition of the codes by `predicate` (see
    /// [`TermPredicate::partition`]), memoized.
    pub(crate) fn filter_codes(&self, predicate: &TermPredicate) -> VerdictSets {
        if let Some(sets) = self.memos.predicates.get(predicate) {
            return sets;
        }
        let kinds = self.kind_ranges();
        let partition = predicate.partition();
        let mut scanned = Scanned::default();
        if let Some(range) = partition.scan(kinds) {
            let mut cursor = self.cursor();
            for code in range {
                scanned.visit(predicate, code, cursor.str_at(code as usize));
            }
        }
        let true_ranges = partition
            .true_prefixes()
            .iter()
            .map(|prefix| self.prefix_range(prefix))
            .collect();
        let sets = Arc::new(partition.assemble(kinds, scanned, true_ranges));
        self.memos.predicates.put(predicate, Arc::clone(&sets));
        sets
    }
}

/// The smallest byte string above every string starting with `prefix`: the
/// trailing `0xFF` bytes dropped and the last remaining byte incremented;
/// `None` for an empty or all-`0xFF` prefix.
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

/// A prefix's code range from the lower bound `lo` of the prefix and `hi` of
/// its successor (`None`: every term from `lo` on has the prefix); never
/// below `lo`.
pub(super) fn prefix_range_from(lo: u32, hi: Option<u32>, len: u32) -> Range<u32> {
    lo..hi.unwrap_or(len).max(lo)
}

/// The spelling a failed exact lookup of `term` retries with: its
/// [`canonical_spelling`], or `None` when that is `term` itself. Malformed
/// input is an error.
pub(super) fn tolerant_fallback(term: &str) -> Result<Option<String>> {
    let canonical = canonical_spelling(term)?;
    Ok((canonical != term).then_some(canonical))
}

/// A predicate's partition of a dictionary's codes: `(true, unknown)`, both
/// ascending.
pub(crate) type VerdictSets = Arc<(Buffer<u32>, Buffer<u32>)>;

/// A dictionary's memos. Entries never go stale: the dictionary is
/// immutable, and a mutation builds a new one with fresh memos.
#[derive(Default)]
pub(super) struct DictMemos {
    /// term → code memo.
    pub(super) encode: EncodeMemo,
    /// The kind ranges, computed once.
    pub(super) kinds: OnceLock<KindRanges>,
    /// Partition memo.
    pub(super) predicates: PredicateMemo,
}

/// Partitions a [`PredicateMemo`] holds before the oldest is evicted.
const PREDICATE_MEMO_SLOTS: usize = 32;

/// A bounded first-in-first-out memo of predicate partitions, keyed by the
/// predicate's canonical rendering; a poisoned lock is a miss.
pub(super) struct PredicateMemo {
    entries: RwLock<VecDeque<(String, VerdictSets)>>,
}

impl Default for PredicateMemo {
    fn default() -> Self {
        Self {
            entries: RwLock::new(VecDeque::with_capacity(PREDICATE_MEMO_SLOTS)),
        }
    }
}

impl PredicateMemo {
    pub(super) fn get(&self, predicate: &TermPredicate) -> Option<VerdictSets> {
        let key = predicate.to_string();
        let entries = self.entries.read().ok()?;
        entries
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, sets)| Arc::clone(sets))
    }

    pub(super) fn put(&self, predicate: &TermPredicate, sets: VerdictSets) {
        let key = predicate.to_string();
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

/// Slots of an [`EncodeMemo`]; a power of two, the slot index being the hash
/// masked to it.
const ENCODE_MEMO_SLOTS: usize = 256;

/// A fixed-size, direct-mapped memo of term → code lookups, absence
/// included: one slot per hash bucket, overwritten on collision; a poisoned
/// lock is a miss.
pub(super) struct EncodeMemo {
    slots: RwLock<Box<[Option<EncodeEntry>]>>,
}

struct EncodeEntry {
    /// Reused across overwrites.
    term: String,
    code: Option<u32>,
}

impl Default for EncodeMemo {
    fn default() -> Self {
        Self {
            slots: RwLock::new(
                std::iter::repeat_with(|| None)
                    .take(ENCODE_MEMO_SLOTS)
                    .collect(),
            ),
        }
    }
}

impl EncodeMemo {
    /// FNV-1a over the whole term, masked to the slot count.
    fn slot(term: &str) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in term.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (h as usize) & (ENCODE_MEMO_SLOTS - 1)
    }

    /// `Some(code_or_absent)` on a hit, `None` when `term` is not memoized.
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

    /// Every memoized lookup agrees with the uncached search, on repeats and
    /// on absent terms alike.
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

    /// Two terms sharing a slot evict each other and never read each other's
    /// code.
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
