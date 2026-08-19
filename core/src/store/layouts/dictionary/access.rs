//! The *residency* axis of the Dictionary layout: how a resolved store
//! reaches its term dictionary — held whole in memory, or left in the file's
//! dictionary child and probed on demand. The dictionary itself lives in the
//! sibling modules — storage, FSST, and probing in
//! [`term_dict`](super::term_dict), the on-demand form in
//! [`file_backed`](super::file_backed) — and this seam is what couples them
//! to the layout's pattern vocabulary.

use std::sync::Arc;

use crate::error::Result;
use crate::store::layouts::{PatternCodes, QuadPattern, TermRef};

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
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
    /// `VortexRdfStore::from_file`).
    #[cfg(feature = "file-io")]
    FileBacked(FileBackedDict),
}

impl DictAccess {
    /// Pre-resolve every bound term of `pattern` — the async prelude run
    /// before the synchronous match core — and mint the [`PatternCodes`]
    /// witness the core's probes run on.
    ///
    /// For `Resident` the lookups are in-memory binary searches, resolved
    /// eagerly rather than lazily at each use site: what this buys is the
    /// invariant the match core is written against — *after the prelude,
    /// every bound role is in the witness* — which is what lets a file-backed
    /// dictionary do its I/O here and nowhere else.
    pub(crate) async fn resolve_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        match self {
            DictAccess::Resident(dict) => {
                let mut codes = PatternCodes::resident(Arc::clone(dict));
                if let Some(s) = pattern.subject {
                    codes.resolve(TermRef::Subject(s), |t| dict.get_id(t));
                }
                if let Some(p) = pattern.predicate {
                    codes.resolve(TermRef::Predicate(p), |t| dict.get_id(t));
                }
                if let Some(o) = pattern.object {
                    codes.resolve(TermRef::Object(o), |t| dict.get_id(t));
                }
                if let Some(g) = pattern.graph {
                    codes.resolve(TermRef::Graph(g), |t| dict.get_id(t));
                }
                Ok(codes)
            }
            // Each bound role costs one point-read binary search of the term
            // column (memoized in the probe cache); the resolved code is then
            // seeded into the witness so the sync match core never reaches
            // back here.
            //
            // The four searches are independent, so they run overlapped
            // rather than one await after another: whatever chunk fetches
            // they miss on overlap instead of serializing. Concurrency is why
            // each term is rendered into its own String here instead of the
            // pattern's shared scratch buffer, and a race to fetch the same
            // chunk is already handled by its drop-the-loser `OnceLock`.
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => {
                let render = |term: Option<TermRef<'_>>| term.map(|t| t.to_string());
                let probe = |term: Option<String>| async move {
                    match term {
                        Some(t) => fb.get_id(&t).await.map(Some),
                        None => Ok(None),
                    }
                };
                let (s_id, p_id, o_id, g_id) = futures::join!(
                    probe(render(pattern.subject.map(TermRef::Subject))),
                    probe(render(pattern.predicate.map(TermRef::Predicate))),
                    probe(render(pattern.object.map(TermRef::Object))),
                    probe(render(pattern.graph.map(TermRef::Graph))),
                );
                let mut codes = PatternCodes::preresolved();
                if let Some(s) = pattern.subject {
                    let id = s_id?.expect("the subject role is bound, so it was probed");
                    codes.resolve(TermRef::Subject(s), |_| id);
                }
                if let Some(p) = pattern.predicate {
                    let id = p_id?.expect("the predicate role is bound, so it was probed");
                    codes.resolve(TermRef::Predicate(p), |_| id);
                }
                if let Some(o) = pattern.object {
                    let id = o_id?.expect("the object role is bound, so it was probed");
                    codes.resolve(TermRef::Object(o), |_| id);
                }
                if let Some(g) = pattern.graph {
                    let id = g_id?.expect("the graph role is bound, so it was probed");
                    codes.resolve(TermRef::Graph(g), |_| id);
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

    /// The whole dictionary in memory, lifting a file-backed one with a single
    /// term-column scan — for the operations that need the full column
    /// (serialization, compaction, tail-merge re-encoding). The lift is
    /// transient: it is not cached back into the access, so a store's steady
    /// state keeps the file-backed footprint.
    pub(crate) async fn ensure_resident(&self) -> Result<Arc<TermDictionary>> {
        match self {
            DictAccess::Resident(dict) => Ok(Arc::clone(dict)),
            #[cfg(feature = "file-io")]
            DictAccess::FileBacked(fb) => Ok(Arc::new(fb.load_resident().await?)),
        }
    }

    /// Whether reconstruction must decode through the file (async) rather
    /// than the resident dictionary.
    #[cfg(feature = "file-io")]
    pub(crate) fn is_file_backed(&self) -> bool {
        matches!(self, DictAccess::FileBacked(_))
    }
}
