//! The store's backing data: [`QuadsSource`] (the base — in memory or a
//! Vortex file — together with the [`ViewSelection`], tombstones, index
//! components and serve plan that determine which base rows a view exposes)
//! and [`Tail`] (rows appended since construction, held apart from the base).

use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_mask::Mask;

use crate::error::Result;
use crate::store::indexes::{InMemoryServePlan, IndexComponent};
use crate::store::scan::gather::gather_live;
use crate::store::view::canonical::LiveCanonical;
use crate::store::view::probes::StructProbes;
use crate::store::view::selection::{RowSelection, ViewSelection};

#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use std::path::PathBuf;
#[cfg(feature = "file-io")]
use vortex_array::expr::Expression;

/// A lazily-decoded view onto quad data: the base the store was constructed
/// from, plus which of its rows this view covers.
///
/// Both variants keep their base intact and narrow a [`RowSelection`] over
/// it, so base row ids stay meaningful for as long as the view lives: the
/// secondary indexes' `rid` columns address base rows across
/// `match_pattern`, and the unselected data stays reachable for later
/// mutation.
#[derive(Clone)]
pub(crate) enum QuadsSource {
    /// Quad data that is already loaded into a Vortex array.
    InMemory {
        /// The complete, shared array against which selections, tombstones,
        /// and secondary-index row ids are defined.
        base: ArrayRef,
        /// The base row ids visible through this particular store or derived
        /// view; narrowing a view changes this without rewriting `base`. May
        /// still be pending on a served match — see `serve`.
        selection: ViewSelection,
        /// Secondary-index components held beside `base`, in the same child
        /// schema as a native file's index children; empty for stores built
        /// without indexes. Shared by views (`Arc`): their `rid` columns
        /// address base rows, which selections and tombstones never renumber.
        /// A file-backed store carries none — its index data stays on disk
        /// as index children and resolves through pushed-down scans.
        components: Arc<[IndexComponent]>,
        /// Rows deleted since construction, one bit per base row (`None`
        /// until something is deleted). Applied by every read path through
        /// [`RowSelection::live_mask`]; reclaimed only by compaction.
        deleted: Option<Mask>,
        /// Lazily-resolved encoded-search probes over `base`'s columns,
        /// shared by every view over this base (see [`StructProbes`]);
        /// carried wherever `base` carries, and a fresh base takes a fresh
        /// cache.
        probes: Arc<StructProbes>,
        /// The live canonical form of `base`'s code columns, for the code
        /// reads an encoded (adopted) base cannot serve zero-copy: decoded
        /// per column on demand, shared by every holder alive, freed with the
        /// last (see [`LiveCanonical`]). Shared by views like `probes`; a
        /// built base's canonical columns never need it.
        canonical: Arc<LiveCanonical>,
        /// The index's plan for reading this view's rows from its own
        /// columns, present only while the selection is exactly the run the
        /// plan covers (any narrowing drops it, materializing a `Pending`
        /// selection first). See [`InMemoryServePlan`] for the mechanism and
        /// [`ViewSelection`] for the served/pending invariant.
        serve: Option<InMemoryServePlan>,
    },
    #[cfg(feature = "file-io")]
    /// Quad data read lazily from a Vortex file when a query is executed.
    File {
        /// The path the file was opened from. An owner's compaction rewrites
        /// its rows over this file atomically and reopens it
        /// (`compaction.rs`); a derived view's compaction never touches it.
        path: PathBuf,
        /// The dictionary-residency budget the store was opened with, so a
        /// compaction's reopen (`from_file_with_dict_residency`) preserves
        /// the same residency mode.
        dict_max_resident_bytes: u64,
        /// The shared file handle, including its cached schema, metadata, and
        /// layout reader used by scans and pruning. Every root row is a quad
        /// row (the dictionary and index copies ride as auxiliary children
        /// with their own row spaces), so `file.row_count()` is the store's
        /// row space.
        file: Arc<NativeStoreFile>,
        /// Pattern components not resolved to row ids, pushed down to the scan.
        filter: Option<Expression>,
        /// The file row ids visible through this store or derived view,
        /// typically narrowed by index lookups or pruning. May still be
        /// pending on a served match — see `serve`.
        selection: ViewSelection,
        /// Rows deleted since the store was opened, one bit per file row
        /// (`None` until something is deleted). Applied by every read path
        /// through [`RowSelection::live_mask`]; reclaimed only by compaction.
        deleted: Option<Mask>,
        /// The index's plan for reading this view's rows from its own
        /// columns, present only while the selection is exactly the run the
        /// plan covers (any narrowing drops it, materializing a `Pending`
        /// selection first). See [`FileServePlan`] for the mechanism and
        /// [`ViewSelection`] for the served/pending invariant.
        serve: Option<FileServePlan>,
    },
}

impl QuadsSource {
    /// The base row ids visible through this source, whichever backend holds
    /// the rows.
    pub(crate) fn view_selection(&self) -> &ViewSelection {
        match self {
            QuadsSource::InMemory { selection, .. } => selection,
            #[cfg(feature = "file-io")]
            QuadsSource::File { selection, .. } => selection,
        }
    }

    /// Whether this source still covers every base row: no pushed-down
    /// filter and an all-rows selection.
    pub(crate) fn is_unrefined(&self) -> bool {
        let unfiltered = match self {
            QuadsSource::InMemory { .. } => true,
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => filter.is_none(),
        };
        unfiltered && self.view_selection().is_all()
    }
}

/// Rows appended after construction, held apart from the base so the base —
/// its row ids, indexes, tombstones and file handle — is never rewritten.
/// Queries run the base's own paths and a mask scan over the tail and union
/// the two.
///
/// The rows are a single contiguous StructArray in the store's own primary
/// layout, except under the Dictionary layout, where they are Default-layout
/// N-Triples strings: an appended term has no code in the sorted dictionary,
/// so the tail keeps terms verbatim and patterns probe the base by code and
/// the tail by string. The tail is folded into the base — re-sorted,
/// re-encoded, re-indexed — by `compact_with_indexes`.
///
/// Selection and tombstones mirror the base's, in tail-local row ids
/// (`0..rows.len()`): views narrow `selection`, deletes set bits in `deleted`,
/// and every read path applies both (`scan::gather::gather_live`).
#[derive(Clone)]
pub(crate) struct Tail {
    /// The appended rows. Appends accrete as chunks of a ChunkedArray and are
    /// folded flat once the accreted rows outgrow the flatten policy
    /// ([`mutation::TAIL_FLATTEN_FLOOR`], [`mutation::TAIL_MAX_CHUNKS`]), so
    /// scans see at most a bounded chunk count.
    ///
    /// [`mutation::TAIL_FLATTEN_FLOOR`]: crate::store::mutation::TAIL_FLATTEN_FLOOR
    /// [`mutation::TAIL_MAX_CHUNKS`]: crate::store::mutation::TAIL_MAX_CHUNKS
    pub(crate) rows: ArrayRef,
    /// The tail rows visible through this store or derived view, in tail-local
    /// ids.
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row
    /// (`None` until something is deleted).
    pub(crate) deleted: Option<Mask>,
}

impl QuadsSource {
    /// The same rows, tombstones and caches under `selection`, without the
    /// serve plan: a plan is its run, and a selection that is not that run
    /// reads through the rows.
    pub(crate) fn with_selection(&self, selection: ViewSelection) -> Self {
        match self {
            QuadsSource::InMemory {
                base,
                components,
                deleted,
                probes,
                canonical,
                ..
            } => QuadsSource::InMemory {
                base: base.clone(),
                selection,
                components: Arc::clone(components),
                deleted: deleted.clone(),
                probes: Arc::clone(probes),
                canonical: Arc::clone(canonical),
                serve: None,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter,
                deleted,
                ..
            } => QuadsSource::File {
                path: path.clone(),
                dict_max_resident_bytes: *dict_max_resident_bytes,
                file: Arc::clone(file),
                filter: filter.clone(),
                selection,
                deleted: deleted.clone(),
                serve: None,
            },
        }
    }
}

impl Tail {
    /// The same rows and tombstones under a different `selection`.
    pub(crate) fn with_selection(&self, selection: RowSelection) -> Tail {
        Tail {
            rows: self.rows.clone(),
            selection,
            deleted: self.deleted.clone(),
        }
    }

    /// The tail rows visible through this store, tombstones dropped, in tail
    /// order.
    pub(crate) fn live_rows(&self) -> Result<ArrayRef> {
        gather_live(&self.rows, &self.selection, self.deleted.as_ref(), None)
    }
}
