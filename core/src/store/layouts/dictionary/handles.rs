//! The handles on a Dictionary layout's term dictionary: [`DictAccess`],
//! the residency the resolved layout reads through, and the public
//! [`DictSnapshot`] and [`DictReader`] over it.

use std::sync::Arc;

use vortex_buffer::Buffer;

use crate::error::Result;
use crate::store::layouts::{PatternCodes, QuadPattern};

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::predicates::{KindRanges, TermPredicate};
use super::term_dict::TermDictionary;

/// Where a Dictionary layout reads its terms: `Resident` holds the whole
/// dictionary in memory, `FileBacked` reads them from the file's dictionary
/// child. [`resolve_pattern`](Self::resolve_pattern) is the only method that
/// does I/O during a match; it resolves every bound role into
/// [`PatternCodes`].
#[derive(Clone)]
pub(crate) enum DictAccess {
    /// The whole dictionary in memory.
    Resident(Arc<TermDictionary>),
    /// The dictionary left in its file, point-read on demand; chosen at open
    /// when the dictionary child outweighs the residency threshold and its
    /// layout shape is point-readable.
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Resolve every bound role of `pattern` into a [`PatternCodes`];
    /// afterwards no probe of the match touches the dictionary.
    pub(crate) async fn resolve_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        match self {
            DictAccess::Resident(dict) => {
                let mut codes = PatternCodes::resident(Arc::clone(dict));
                for term in pattern.bound_roles() {
                    codes.resolve(term, |t| dict.encode(t));
                }
                Ok(codes)
            }
            // The lookups are independent and run overlapped, each term
            // rendered into its own String.
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => {
                let rendered: Vec<String> = pattern.bound_roles().map(|t| t.to_string()).collect();
                let resolved =
                    futures::future::join_all(rendered.iter().map(|t| fb.encode(t))).await;
                let mut codes = PatternCodes::preresolved();
                for (term, code) in pattern.bound_roles().zip(resolved) {
                    let code = code?;
                    codes.resolve(term, |_| code);
                }
                Ok(codes)
            }
        }
    }

    /// The in-memory dictionary; `None` when file-backed.
    pub(crate) fn resident(&self) -> Option<&Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Some(dict),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// A handle on the dictionary under either residency.
    pub(crate) fn reader(&self) -> DictReader {
        DictReader(self.clone())
    }

    /// The whole dictionary in memory, lifting a file-backed one with one
    /// term-column scan. The lift is transient, not cached.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.lift_resident().await?)),
        }
    }

    /// Whether the terms are read from the file on demand.
    pub(crate) fn is_file_backed(&self) -> bool {
        match self {
            DictAccess::Resident(_) => false,
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => true,
        }
    }

    /// Identity of the dictionary (see [`DictReader::dictionary_id`]).
    pub(crate) fn id(&self) -> u64 {
        match self {
            DictAccess::Resident(dict) => dict.id(),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.id(),
        }
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        match self {
            DictAccess::Resident(dict) => dict.len(),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.len(),
        }
    }
}

/// An immutable handle on a Dictionary-layout store's term dictionary, taken
/// with [`VortexRdfStore::code_read_snapshot`](crate::store::VortexRdfStore::code_read_snapshot).
/// Cloning is an `Arc` bump; the snapshot retains only the dictionary.
///
/// Term codes are only meaningful against the dictionary they were produced
/// with: mutating a store re-encodes it against a fresh dictionary, so codes
/// are decoded through the snapshot taken when they were received, not
/// through the store as it stands later.
#[derive(Clone)]
pub struct DictSnapshot(pub(crate) Arc<TermDictionary>);

impl DictSnapshot {
    /// The N-Triples string of `code`, `None` when the code is out of range.
    pub fn decode(&self, code: u32) -> Option<String> {
        self.0.decode(code)
    }

    /// The code of the N-Triples string `term` (its position in the sorted
    /// dictionary), `None` when the dictionary does not hold it.
    pub fn encode(&self, term: &str) -> Option<u32> {
        self.0.encode(term)
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Identity of the dictionary (see [`DictReader::dictionary_id`]).
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    /// [`encode`](Self::encode) tolerant of spelling: an IRI with or without
    /// angle brackets, a literal's escape variants, an `xsd:string` typing,
    /// an upper-case language tag and the default-graph spellings (`""`,
    /// `default`, `[]`) all resolve to the code of the stored form (see
    /// [`canonical_spelling`]). Malformed input is an error.
    ///
    /// [`canonical_spelling`]: crate::common::terms::canonical_spelling
    pub fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        self.0.encode_tolerant(term)
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        self.0.encode_many(terms)
    }

    /// [`decode`](Self::decode) over a batch, in order; an out-of-range code
    /// decodes to `None`.
    pub fn decode_many(&self, codes: &[u32]) -> Vec<Option<String>> {
        self.0.decode_many(codes)
    }

    /// The code of the first term not below `term` in byte order; the term
    /// count when every term is below it.
    pub fn lower_bound(&self, term: &str) -> u32 {
        self.0.lower_bound(term.as_bytes())
    }

    /// The half-open code range `(lo, hi)` of the terms spelled with
    /// `prefix`: a term kind (`"`, `<`, `_:`) or an IRI namespace is one
    /// range.
    pub fn prefix_range(&self, prefix: &str) -> (u32, u32) {
        let range = self.0.prefix_range(prefix);
        (range.start, range.end)
    }

    /// The code ranges of the term kinds.
    pub fn kind_ranges(&self) -> KindRanges {
        self.0.kind_ranges().clone()
    }

    /// The codes for which `predicate` is definitely true, and the codes
    /// inside its [`domain`](TermPredicate::domain) whose verdict is unknown,
    /// both ascending. Codes outside the domain appear in neither list.
    /// Memoized per dictionary.
    pub fn filter_codes(&self, predicate: &TermPredicate) -> (Buffer<u32>, Buffer<u32>) {
        let sets = self.0.filter_codes(predicate);
        (sets.0.clone(), sets.1.clone())
    }
}

/// A handle on a Dictionary-layout store's term dictionary under either
/// residency, taken with
/// [`VortexRdfStore::dict_reader`](crate::store::VortexRdfStore::dict_reader):
/// [`DictSnapshot`]'s surface, asynchronous so a file-backed dictionary can
/// read its file; on a resident dictionary every method completes without
/// suspending. Codes are only meaningful against the dictionary they were
/// produced with; a file-backed reader keeps the store's file handle alive.
#[derive(Clone)]
pub struct DictReader(pub(crate) DictAccess);

impl From<DictSnapshot> for DictReader {
    /// The resident handle on a snapshot's dictionary.
    fn from(snapshot: DictSnapshot) -> Self {
        Self(DictAccess::Resident(snapshot.0))
    }
}

impl DictReader {
    /// Whether the terms are read from the file on demand.
    pub fn is_file_backed(&self) -> bool {
        self.0.is_file_backed()
    }

    /// Identity of the dictionary: equal for every view of one store,
    /// different for the dictionary a `compact` or a fresh open builds. Equal
    /// ids mean equal codes; different ids promise nothing.
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The [`DictSnapshot`] of a resident dictionary; `None` when file-backed.
    pub fn snapshot(&self) -> Option<DictSnapshot> {
        self.0.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
    }

    /// The N-Triples string of `code`, `None` when the code is out of range.
    pub async fn decode(&self, code: u32) -> Result<Option<String>> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.decode(code)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => {
                Ok(dict.decode_many_any(&[code]).await?.pop().flatten())
            }
        }
    }

    /// [`decode`](Self::decode) over a batch, in order; any order and
    /// repeats are accepted.
    pub async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Option<String>>> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.decode_many(codes)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.decode_many_any(codes).await,
        }
    }

    /// The code of `term`, tolerant of spelling (see
    /// [`DictSnapshot::encode_tolerant`]).
    pub async fn encode(&self, term: &str) -> Result<Option<u32>> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.encode_tolerant(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.encode_tolerant(term).await,
        }
    }

    /// [`encode`](Self::encode) over a batch, in order.
    pub async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.encode_many(terms),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.encode_many(terms).await,
        }
    }

    /// See [`DictSnapshot::lower_bound`].
    pub async fn lower_bound(&self, term: &str) -> Result<u32> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.lower_bound(term.as_bytes())),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.lower_bound(term.as_bytes()).await,
        }
    }

    /// See [`DictSnapshot::prefix_range`].
    pub async fn prefix_range(&self, prefix: &str) -> Result<(u32, u32)> {
        let range = match &self.0 {
            DictAccess::Resident(dict) => dict.prefix_range(prefix),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.prefix_range(prefix).await?,
        };
        Ok((range.start, range.end))
    }

    /// See [`DictSnapshot::kind_ranges`].
    pub async fn kind_ranges(&self) -> Result<KindRanges> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.kind_ranges().clone()),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.kind_ranges().await,
        }
    }

    /// See [`DictSnapshot::filter_codes`].
    pub async fn filter_codes(
        &self,
        predicate: &TermPredicate,
    ) -> Result<(Buffer<u32>, Buffer<u32>)> {
        let sets = match &self.0 {
            DictAccess::Resident(dict) => dict.filter_codes(predicate),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.filter_codes(predicate).await?,
        };
        Ok((sets.0.clone(), sets.1.clone()))
    }
}
