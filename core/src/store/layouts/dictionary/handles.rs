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

/// How a resolved Dictionary layout reaches its term dictionary: the
/// *residency* axis, sitting above `TermStore`'s encoding axis.
///
/// `Resident` holds the whole dictionary in memory; `FileBacked` leaves the
/// terms in the file's scannable dictionary child and reads them on demand,
/// which makes term↔code translation asynchronous. The method contract that
/// keeps both arms behind one seam:
///
/// - [`resolve_pattern`](Self::resolve_pattern) is the **async prelude**: the
///   one place a dictionary is allowed to perform I/O during a match. It runs
///   before the synchronous match core, pre-resolves every bound term of the
///   pattern, and hands back the match's [`PatternCodes`] witness — the only
///   way one is minted — so the core's synchronous probes can only ever run
///   over a prelude that ran, and answer from its codes without touching the
///   dictionary again. That witness is what confines a file-backed
///   dictionary's I/O to this method.
/// - [`resident`](Self::resident) hands out the in-memory dictionary itself
///   (`None` for `FileBacked`), for the paths that genuinely need the whole
///   column; [`ensure_resident`](Self::ensure_resident) lifts a file-backed
///   dictionary transiently when serialization must have it.
#[derive(Clone)]
pub(crate) enum DictAccess {
    /// The whole dictionary in memory (FSST-compressed or canonical).
    Resident(Arc<TermDictionary>),
    /// The dictionary left in its file, read on demand through wire-chunk
    /// point reads — chosen at open when the dictionary child outweighs the
    /// residency threshold *and* its layout shape is point-readable (see
    /// [`VortexRdfStore::from_file_with_dict_residency`](crate::store::VortexRdfStore::from_file_with_dict_residency)).
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Pre-resolve every bound term of `pattern` — the async prelude run
    /// before the synchronous match core — and mint the [`PatternCodes`]
    /// witness the core's probes run on.
    ///
    /// For `Resident` the lookups are in-memory binary searches, all resolved
    /// here so the invariant the match core is written against holds under
    /// either residency — *after the prelude, every bound role is in the
    /// witness* — which is what lets a file-backed dictionary do its I/O here
    /// and nowhere else.
    pub(crate) async fn resolve_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        match self {
            DictAccess::Resident(dict) => {
                let mut codes = PatternCodes::resident(Arc::clone(dict));
                for term in pattern.bound_roles() {
                    codes.resolve(term, |t| dict.encode(t));
                }
                Ok(codes)
            }
            // Each bound role costs one point-read binary search of the term
            // column (memoized in the probe cache); the resolved code is then
            // seeded into the witness so the sync match core never reaches
            // back here.
            //
            // The searches are independent, so they run overlapped rather
            // than one await after another: whatever chunk fetches they miss
            // on overlap instead of serializing. Concurrency is why each term
            // is rendered into its own String here instead of the pattern's
            // shared scratch buffer, and a race to fetch the same chunk is
            // already handled by its drop-the-loser `OnceLock`.
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

    /// The in-memory dictionary, or `None` when it is file-backed — sync
    /// callers (snapshots, in-memory chunk decode) treat `None` as "not
    /// available here"; paths that genuinely need the whole column go through
    /// [`ensure_resident`](Self::ensure_resident).
    pub(crate) fn resident(&self) -> Option<&Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Some(dict),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// A residency-agnostic handle on the dictionary: the resident one, or
    /// a clone of the file-backed handle (sharing its caches).
    pub(crate) fn reader(&self) -> DictReader {
        match self {
            DictAccess::Resident(dict) => DictReader::resident(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => DictReader::file_backed(fb.clone()),
        }
    }

    /// The whole dictionary in memory, lifting a file-backed one with a single
    /// term-column scan — for the operations that need the full column
    /// (serialization, compaction, tail-merge re-encoding). The lift is
    /// transient: it is not cached back into the access, so a store's steady
    /// state keeps the file-backed footprint.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.lift_resident().await?)),
        }
    }

    /// Whether reconstruction must decode through the file (async) rather
    /// than the resident dictionary.
    #[cfg(feature = "file-io")]
    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self, DictAccess::FileBacked(_))
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

    /// Identity of the dictionary behind this snapshot — see
    /// [`DictReader::dictionary_id`].
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
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
    pub fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        self.0.encode_tolerant(term)
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        self.0.encode_many(terms)
    }

    /// [`decode`](Self::decode) over a batch, in order, through one cursor;
    /// an out-of-range code decodes to `None`.
    pub fn decode_many(&self, codes: &[u32]) -> Vec<Option<String>> {
        self.0.decode_many(codes)
    }

    /// The code of the first term not below `term` in byte order (the
    /// dictionary's size when every term is below it): a present term's own
    /// code, or where an absent one would sort.
    pub fn lower_bound(&self, term: &str) -> u32 {
        self.0.lower_bound(term.as_bytes())
    }

    /// The half-open code range `(lo, hi)` of the terms spelled with
    /// `prefix`. Codes are lexicographic ranks of the N-Triples spelling, so
    /// a term kind (`"` for literals, `<` for IRIs, `_:` for blank nodes)
    /// and an IRI namespace (`<http://example.org/`) are each one range.
    pub fn prefix_range(&self, prefix: &str) -> (u32, u32) {
        let range = self.0.prefix_range(prefix);
        (range.start, range.end)
    }

    /// The code ranges of the term kinds.
    pub fn kind_ranges(&self) -> KindRanges {
        self.0.kind_ranges().clone()
    }

    /// Partition the codes by `predicate`: the ascending codes for which it
    /// is definitely true, and the ascending codes inside its domain whose
    /// verdict is unknown (for a full engine to decide). Codes outside the
    /// predicate's [`domain`](TermPredicate::domain) — non-literals, for the
    /// literal predicates — appear in neither list, since a caller decides
    /// them from the term's kind alone (see [`kind_ranges`](Self::kind_ranges)).
    /// One scan of the domain, memoized per dictionary.
    pub fn filter_codes(&self, predicate: &TermPredicate) -> (Buffer<u32>, Buffer<u32>) {
        let sets = self.0.filter_codes(predicate);
        (sets.0.clone(), sets.1.clone())
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

    /// Identity of the dictionary this handle reads: equal for every view
    /// of one store (its codes are one vocabulary), different for the
    /// dictionary a `compact` or a fresh open builds — so a consumer that
    /// caches codes knows when they stop applying. Two handles with
    /// different ids may still hold equal terms; only equal ids promise
    /// equal codes.
    pub fn dictionary_id(&self) -> u64 {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.id(),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.id(),
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
    pub async fn decode(&self, code: u32) -> Result<Option<String>> {
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
    pub async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Option<String>>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.decode_many(codes)),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.decode_many_any(codes).await,
        }
    }

    /// The code of `term`, tolerant of spelling (see
    /// [`DictSnapshot::encode_tolerant`]).
    pub async fn encode(&self, term: &str) -> Result<Option<u32>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.encode_tolerant(term),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.encode_tolerant(term).await,
        }
    }

    /// [`encode`](Self::encode) over a batch, in order (a file-backed
    /// dictionary overlaps the lookups' reads).
    pub async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        match &self.0 {
            DictReaderInner::Resident(dict) => dict.encode_many(terms),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.encode_many(terms).await,
        }
    }

    /// See [`DictSnapshot::lower_bound`].
    pub async fn lower_bound(&self, term: &str) -> Result<u32> {
        match &self.0 {
            DictReaderInner::Resident(dict) => Ok(dict.lower_bound(term.as_bytes())),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.lower_bound(term.as_bytes()).await,
        }
    }

    /// See [`DictSnapshot::prefix_range`].
    pub async fn prefix_range(&self, prefix: &str) -> Result<(u32, u32)> {
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

    /// See [`DictSnapshot::filter_codes`]. A file-backed dictionary scans
    /// the predicate's domain through its child once per distinct predicate
    /// (memoized like the resident form).
    pub async fn filter_codes(
        &self,
        predicate: &TermPredicate,
    ) -> Result<(Buffer<u32>, Buffer<u32>)> {
        let sets = match &self.0 {
            DictReaderInner::Resident(dict) => dict.filter_codes(predicate),
            #[cfg(feature = "file-io")]
            DictReaderInner::FileBacked(dict) => dict.filter_codes(predicate).await?,
        };
        Ok((sets.0.clone(), sets.1.clone()))
    }
}
