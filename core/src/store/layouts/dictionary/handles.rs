//! The *residency* axis of the Dictionary layout: how a resolved store
//! reaches its term dictionary — held whole in memory, or left in the file's
//! dictionary child and probed on demand. The dictionary itself lives in the
//! sibling modules — storage and FSST in [`storage`](super::storage),
//! probing in [`term_dict`](super::term_dict), the on-demand form in
//! [`file_backed`](super::file_backed) — and this seam is what couples them
//! to the layout's pattern vocabulary. The public handles on a store's
//! dictionary, [`DictSnapshot`] and [`DictReader`], sit beside the seam.

use std::sync::Arc;

use vortex_buffer::Buffer;

use crate::error::Result;
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::schema::TermCode;

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::predicates::{KindRanges, TermPredicate};
use super::term_dict::TermDictionary;

/// How a resolved Dictionary layout reaches its term dictionary: the
/// *residency* axis, sitting above `ResidentChunks`' encoding axis.
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
    /// The dictionary left in its file, read on demand through the leaves a
    /// call touches — what every file store opens with, unless the child's
    /// layout shape declines the handle (see
    /// [`VortexRdfStore::from_file`](crate::store::VortexRdfStore::from_file)).
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
            // Each bound role costs one window search of the mapped
            // dictionary child; the resolved code is then seeded into the
            // witness so the sync match core never reaches back here. The
            // searches are independent, so they run overlapped rather than
            // one await after another; concurrency is why each term is
            // rendered into its own String here.
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

    /// A residency-agnostic handle on the dictionary: a clone of this
    /// access (an `Arc` bump).
    pub(crate) fn reader(&self) -> DictReader {
        DictReader(self.clone())
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

    /// Whether the terms are read from the file on demand, so reconstruction
    /// must decode through the file (async) rather than the resident
    /// dictionary.
    pub(crate) fn is_file_backed(&self) -> bool {
        match self {
            DictAccess::Resident(_) => false,
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => true,
        }
    }

    /// The identity of the dictionary (see [`DictReader::dictionary_id`]).
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

    /// The identity of the snapshot's dictionary (see
    /// [`DictReader::dictionary_id`]).
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
    }

    /// [`encode`](Self::encode) tolerant of spelling: an IRI with or without
    /// angle brackets, a literal with escape variants, an `xsd:string`
    /// typing, an upper-case language tag, or a default-graph spelling
    /// (`""`, `default`, `[]`) all resolve to the code of the stored form
    /// (see [`canonical_spelling`](crate::common::terms::canonical_spelling)). Malformed input is an error.
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
pub struct DictReader(DictAccess);

impl From<DictSnapshot> for DictReader {
    /// The resident handle on a snapshot's dictionary.
    fn from(snapshot: DictSnapshot) -> Self {
        Self(DictAccess::Resident(snapshot.0))
    }
}

impl DictReader {
    /// Whether the terms are read from the file on demand (`true`) or held
    /// in memory (`false`).
    pub fn is_file_backed(&self) -> bool {
        self.0.is_file_backed()
    }

    /// Number of terms in the dictionary.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the dictionary holds no terms.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The identity of the dictionary this handle reads: the same for every
    /// view of one store, whose codes are one vocabulary, and a new one for
    /// the dictionary a build, an open or a `compact` makes, so a consumer
    /// that caches codes knows when they stop applying. Equal ids promise
    /// equal codes; dictionaries under different ids may still hold the same
    /// terms. Resident and file-backed dictionaries are numbered by one
    /// process-wide counter, and an id is never persisted.
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
    }

    /// The synchronous [`DictSnapshot`] of a resident dictionary, `None`
    /// when the terms are file-backed.
    pub fn snapshot(&self) -> Option<DictSnapshot> {
        self.0.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
    }

    /// The N-Triples string for `code`, or `None` when the code is out of
    /// range.
    pub async fn decode(&self, code: TermCode) -> Result<Option<String>> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.decode(code)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => {
                Ok(dict.decode_many_any(&[code]).await?.pop().flatten())
            }
        }
    }

    /// [`decode`](Self::decode) over a batch, in order: any order and
    /// repeats are fine (a file-backed dictionary reads each distinct code
    /// once, in one batch).
    pub async fn decode_many(&self, codes: &[TermCode]) -> Result<Vec<Option<String>>> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.decode_many(codes)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.decode_many_any(codes).await,
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
            DictAccess::Resident(dict) => {
                dict.decode_many_with(codes, visit);
                Ok(())
            }
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.decode_many_with(codes, visit).await,
        }
    }

    /// The code of `term`, tolerant of spelling (see
    /// [`DictSnapshot::encode_tolerant`]).
    pub async fn encode(&self, term: &str) -> Result<Option<TermCode>> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.encode_tolerant(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.encode_tolerant(term).await,
        }
    }

    /// [`encode`](Self::encode) over a batch, in order (a file-backed
    /// dictionary overlaps the lookups' reads).
    pub async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.encode_many(terms),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.encode_many(terms).await,
        }
    }

    /// See [`DictSnapshot::lower_bound`].
    pub async fn lower_bound(&self, term: &str) -> Result<TermCode> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.lower_bound(term.as_bytes())),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.lower_bound(term.as_bytes()).await,
        }
    }

    /// See [`DictSnapshot::prefix_range`].
    pub async fn prefix_range(&self, prefix: &str) -> Result<(TermCode, TermCode)> {
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

    /// See [`DictSnapshot::filter_codes`]. A file-backed dictionary reads
    /// only the windows holding the candidates it must read.
    pub async fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.filter_codes(predicate, codes),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.filter_codes(predicate, codes).await,
        }
    }
}
