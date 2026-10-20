//! How a resolved store reaches its term dictionary ([`DictAccess`]): held
//! whole in memory, or left in the file's `dictionary` child and read on
//! demand. Beside the seam are the public handles on a store's dictionary,
//! [`DictSnapshot`] and [`DictReader`].

use std::sync::Arc;

use vortex_buffer::Buffer;

use crate::error::Result;
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::schema::TermCode;

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::predicates::{KindRanges, TermPredicate};
use super::term_dict::TermDictionary;

/// How a resolved Dictionary layout reaches its term dictionary: whole in
/// memory (`Resident`), or left in the file's `dictionary` child and read on
/// demand (`FileBacked`), which makes term ↔ code translation asynchronous.
/// During a match the dictionary does I/O in
/// [`resolve_pattern`](Self::resolve_pattern) only: that async prelude
/// resolves every bound role into the match's [`PatternCodes`] witness,
/// which only the prelude mints, so the synchronous match core answers from
/// those codes and never touches the dictionary. Paths that need the whole
/// column take [`resident`](Self::resident), or lift a file-backed
/// dictionary transiently with [`ensure_resident`](Self::ensure_resident).
#[derive(Clone)]
pub(crate) enum DictAccess {
    /// The whole dictionary in memory (FSST-compressed or canonical).
    Resident(Arc<TermDictionary>),
    /// The dictionary left in its file, read through the leaves a call
    /// touches: what every file store opens with, unless the child's shape
    /// declines it
    /// ([`VortexRdfStore::from_file`](crate::store::VortexRdfStore::from_file)).
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Resolve every bound term of `pattern` into `codes`, the witness the
    /// async prelude
    /// ([`prepare_pattern`](crate::store::layouts::ResolvedLayout::prepare_pattern))
    /// minted. A resident dictionary resolves them here too, so under either
    /// residency every bound role is in the witness once the prelude has
    /// run.
    pub(in crate::store::layouts) async fn resolve_pattern(
        &self,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes<'_>,
    ) -> Result<()> {
        match self {
            DictAccess::Resident(dict) => {
                for term in pattern.bound_roles() {
                    codes.resolve(term, |t| dict.encode_exact(t));
                }
            }
            // One window search per bound role. The searches are independent
            // and run overlapped, so each term is rendered into its own
            // String.
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => {
                let rendered: Vec<String> = pattern.bound_roles().map(|t| t.to_string()).collect();
                let resolved =
                    futures::future::join_all(rendered.iter().map(|t| fb.encode_exact(t))).await;
                for (term, code) in pattern.bound_roles().zip(resolved) {
                    let code = code?;
                    codes.resolve(term, |_| code);
                }
            }
        }
        Ok(())
    }

    /// The in-memory dictionary; `None` when file-backed.
    pub(crate) fn resident(&self) -> Option<&Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Some(dict),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => None,
        }
    }

    /// A handle on the dictionary under either residency: a clone of this
    /// access (an `Arc` bump).
    pub(crate) fn reader(&self) -> DictReader {
        DictReader(self.clone())
    }

    /// The whole dictionary in memory, lifting a file-backed one with one
    /// term-column scan. The lift is transient, never cached into the
    /// access, so the store keeps its file-backed footprint.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.lift_resident().await?)),
        }
    }

    /// Whether the terms are read from the file on demand, so a decode goes
    /// through the file (async).
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

    pub(crate) fn len(&self) -> usize {
        match self {
            DictAccess::Resident(dict) => dict.len(),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.len(),
        }
    }
}

/// An immutable handle on a Dictionary-layout store's resident term
/// dictionary, taken with [`DictReader::snapshot`]. Cloning is an `Arc`
/// bump; the snapshot keeps only the dictionary alive, not the store or its
/// quad columns.
///
/// **Term codes are only meaningful against the dictionary they were produced
/// with.** Mutating a store re-encodes it against a fresh dictionary, so a
/// consumer holding codes decodes them through the snapshot taken when it
/// received them, not through the store as it stands later, where they can
/// silently resolve to other terms.
#[derive(Clone)]
pub struct DictSnapshot(pub(crate) Arc<TermDictionary>);

impl DictSnapshot {
    /// The N-Triples spelling of `code`; `None` when the code is out of
    /// range.
    pub fn decode(&self, code: TermCode) -> Option<String> {
        self.0.decode(code)
    }

    /// The code of `term` (its rank in the sorted dictionary); `None` when
    /// the dictionary does not hold it. Tolerant of spelling: an IRI with or
    /// without angle brackets, a literal's escape variants, an `xsd:string`
    /// typing, an upper-case language tag and the default-graph spellings
    /// (`""`, `default`, `[]`) all resolve to the stored form's code.
    /// Malformed input is an
    /// [`InvalidInput`](crate::VortexRdfError::InvalidInput) error.
    pub fn encode(&self, term: &str) -> Result<Option<TermCode>> {
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

    /// [`encode`](Self::encode) over a batch, in order.
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

    /// The code of the first term not below `term` in byte order: a present
    /// term's own code, or where an absent one would sort; the dictionary's
    /// size when every term is below it.
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

    /// `predicate` over the candidate `codes`, which must ascend strictly
    /// inside the dictionary (else an `InvalidInput` error): `(passed,
    /// undecided)`, both ascending subsets of `codes`. An undecided candidate
    /// is for a full SPARQL engine to decide; one in neither fails the
    /// predicate. Only the candidates their kind does not decide are read.
    /// Nothing is memoized.
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
/// [`DictSnapshot`]'s term ↔ code surface, asynchronous so a dictionary left
/// in its file can answer by reading it. On a resident dictionary every
/// method completes without suspending.
///
/// As with a snapshot, codes are only meaningful against the dictionary they
/// were produced with; a file-backed reader also keeps the store's file
/// handle alive.
#[derive(Clone)]
pub struct DictReader(DictAccess);

impl DictReader {
    /// Whether the terms are read from the file on demand.
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
    /// terms. Ids come from one process-wide counter and are never persisted.
    pub fn dictionary_id(&self) -> u64 {
        self.0.id()
    }

    /// The synchronous [`DictSnapshot`] of a resident dictionary; `None`
    /// when the terms are file-backed.
    pub fn snapshot(&self) -> Option<DictSnapshot> {
        self.0.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
    }

    /// The N-Triples spelling of `code`; `None` when the code is out of
    /// range.
    pub async fn decode(&self, code: TermCode) -> Result<Option<String>> {
        match &self.0 {
            DictAccess::Resident(dict) => Ok(dict.decode(code)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(_) => Ok(self.decode_many(&[code]).await?.pop().flatten()),
        }
    }

    /// [`decode`](Self::decode) over a batch, in order, for codes in any
    /// order and with repeats: [`decode_many_with`](Self::decode_many_with)
    /// collected into one owned string per position.
    pub async fn decode_many(&self, codes: &[TermCode]) -> Result<Vec<Option<String>>> {
        let mut terms = vec![None; codes.len()];
        self.decode_many_with(codes, |i, term| terms[i] = term.map(str::to_owned))
            .await?;
        Ok(terms)
    }

    /// [`decode_many`](Self::decode_many) without a `String` per term:
    /// `visit(i, term)` is called once for each position `i` of `codes`, with
    /// the spelling borrowed for the call (`None` for an out-of-range code).
    /// Codes may come in any order and repeat. A resident dictionary visits
    /// in the order of `codes`; a file-backed one visits the out-of-range
    /// positions first, then reads each distinct code once, in one batch, and
    /// visits every position holding it, in code order. The `Result` reports
    /// a failed read of the file. Terms are read as UTF-8 unchecked, so the
    /// store must be one this crate wrote or the caller trusts.
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
    /// [`DictSnapshot::encode`]).
    pub async fn encode(&self, term: &str) -> Result<Option<TermCode>> {
        match &self.0 {
            DictAccess::Resident(dict) => dict.encode(term),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(dict) => dict.encode(term).await,
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
