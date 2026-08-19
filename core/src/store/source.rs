use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_mask::Mask;

use crate::store::indexes::{InMemoryServePlan, IndexComponent};
use crate::store::probes::BaseProbes;
use crate::store::selection::{RowSelection, ViewSelection};

#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use std::path::PathBuf;
#[cfg(feature = "file-io")]
use vortex_array::expr::Expression;

/// A lazily-decoded view onto quad data: the base the store was constructed
/// from, plus which of its rows this view covers.
///
/// Both variants keep their base intact and narrow a [`RowSelection`] over it
/// rather than rewriting rows, so base row ids stay meaningful for as long as
/// the view lives — that is what keeps secondary indexes usable across
/// `match_pattern` (their components' `rid` columns address base rows) and
/// what leaves the unselected data reachable for later mutation.
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
        /// The secondary-index components held beside `base` — the in-memory
        /// twins of a native file's index children, in the same child schema.
        /// Empty for stores built without indexes. Views share them (`Arc`):
        /// their `rid` columns address base rows, which a `RowSelection`
        /// never renumbers — only a physical gather invalidates them, and a
        /// gather constructs a fresh source, forcing that decision here.
        ///
        /// Living on this variant (not the store) makes the invariant
        /// structural: a file-backed store cannot carry in-memory components —
        /// its index data stays on disk as index children and resolves
        /// through pushed-down scans.
        components: Arc<[IndexComponent]>,
        /// Base rows deleted since construction, one bit per base row (`None`
        /// until something is deleted).
        ///
        /// Deleting tombstones here instead of rewriting `base`, so base row
        /// ids survive a delete and the secondary indexes built against them
        /// stay usable. Every read path must apply this — see
        /// [`RowSelection::live_mask`]. The tombstoned rows are only reclaimed
        /// by compaction.
        deleted: Option<Mask>,
        /// Lazily-resolved encoded-search probes over `base`'s columns,
        /// shared by every view over this base (probe resolution walks the
        /// encoding tree per call otherwise — the fixed cost of point reads
        /// on a compressed-resident base). Carried wherever `base` itself
        /// carries; a fresh base takes a fresh cache.
        probes: Arc<BaseProbes>,
        /// When this view's selection came from an index resolution over an
        /// otherwise-unrefined base, and that index holds the matched rows as a
        /// contiguous run of its own columns, the plan for `quads()` to slice
        /// them straight from `base` instead of gathering the primary columns at
        /// scattered row ids. Index-agnostic: only `SecondaryByCopy` currently
        /// supplies one (see [`InMemoryServePlan`] — the backend-typed plan,
        /// so this variant structurally cannot carry a file plan). `None` on
        /// any view narrowed further — the plan is only valid while its row
        /// run is exactly the selection.
        ///
        /// A `Pending` selection implies `serve` is `Some`: the plan is what
        /// makes deferring the resolution's exact ids safe, so any narrowing
        /// that drops the plan materializes the selection first.
        ///
        /// [`InMemoryServePlan`]: crate::store::indexes::InMemoryServePlan
        serve: Option<InMemoryServePlan>,
    },
    #[cfg(feature = "file-io")]
    /// Quad data read lazily from a Vortex file when a query is executed.
    File {
        /// The path the file was opened from. Kept so an OWNER's compaction
        /// can rewrite the store's rows back over their own source file
        /// (atomically) and reopen it, rather than degrading a file-backed
        /// store to an in-memory one. (A derived view's compaction never
        /// touches the file — its rows are a subset of data other readers
        /// share.)
        path: PathBuf,
        /// The dictionary-residency budget this store was opened with, so a
        /// compaction's reopen keeps the caller's pinned residency mode
        /// instead of silently reverting to the default.
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
        /// File rows deleted since the store was opened, one bit per file row
        /// (`None` until something is deleted).
        ///
        /// A file is immutable on disk, so a delete can't rewrite it; the rows
        /// are tombstoned here instead, exactly as for the in-memory variant.
        /// The file's row ids stay stable (more so than an in-memory base's —
        /// the file cannot change underneath), so the secondary indexes built
        /// against them survive a delete. Every read path must apply this —
        /// see [`RowSelection::live_mask`] — and it is only reclaimed by
        /// compaction.
        deleted: Option<Mask>,
        /// When this view's selection came from an index resolution over an
        /// otherwise-unrefined store, and that index can serve the matched rows
        /// from its own columns — where they sit in a contiguous, zone-prunable
        /// run — the plan for `quads()` to stream them from there instead of
        /// scattering row-id reads across the primary columns. Index-agnostic:
        /// any serving index supplies one, and only `SecondaryByCopy` currently
        /// does (see [`FileServePlan`] — the backend-typed plan, so this
        /// variant structurally cannot carry an in-memory plan). `None` on any
        /// view whose selection has been narrowed further — the plan is only
        /// valid while its filter selects exactly the selection's rows.
        ///
        /// A `Pending` selection implies `serve` is `Some`: with the plan
        /// attached, the resolution's exact ids — a second pushed-down scan of
        /// the same index child — stay deferred until a consumer needs the
        /// selection itself; any narrowing that drops the plan materializes
        /// the selection first.
        ///
        /// [`FileServePlan`]: crate::store::indexes::FileServePlan
        serve: Option<FileServePlan>,
    },
}

/// Rows appended after construction: the write-optimized delta over the
/// read-optimized base — the delta half of a delta/main design, kept as a
/// second, miniature in-memory source so appends never touch the base.
///
/// Appending to the base directly would rewrite it (invalidating the row ids
/// its secondary indexes address); tail rows live outside the base instead, so
/// `add_quads` costs O(tail) and the base — indexes, tombstones, file handle —
/// carries over untouched. Queries run the base's fast paths and a mask scan
/// over the tail, and union the two.
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
/// and every read path applies both (`gather_live`).
#[derive(Clone)]
pub(crate) struct Tail {
    /// The appended rows. Appends accrete as chunks of a ChunkedArray and are
    /// folded flat once the accreted rows outgrow the flatten policy
    /// (`add_quads`), so scans see at most a bounded chunk count.
    pub(crate) rows: ArrayRef,
    /// The tail rows visible through this store or derived view, in tail-local
    /// ids.
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row
    /// (`None` until something is deleted).
    pub(crate) deleted: Option<Mask>,
}
