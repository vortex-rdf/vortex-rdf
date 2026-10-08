//! The *residency* axis of the Dictionary layout: how a resolved store
//! reaches its term dictionary — held whole in memory, or left in the file's
//! dictionary child and probed on demand. The dictionary itself lives in the
//! sibling modules — storage, FSST, and probing in
//! [`term_dict`](super::term_dict), the on-demand form in
//! [`file_backed`](super::file_backed) — and this seam is what couples them
//! to the layout's pattern vocabulary.

use std::sync::Arc;

use crate::error::Result;
use crate::store::layouts::{PatternCodes, QuadPattern};

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::term_dict::{DictReader, TermDictionary};

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

    /// A residency-agnostic handle on the dictionary: the resident one, or
    /// a clone of the file-backed handle (an `Arc` bump).
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
