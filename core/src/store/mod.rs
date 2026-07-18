pub mod vortex_rdf_store;
pub mod builders;
pub mod indexes;
pub mod layouts;
pub mod selection;

pub use builders::{
    VortexArrayBuilder,
    UnsortedStreamBuilder,
    SortedInMemoryBuilder,
    SortedStreamBuilder,
    BuilderStrategy,
};
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use vortex_rdf_store::VortexRdfStore;

use vortex_array::ArrayRef;
use vortex_mask::Mask;
use oxrdf::Quad;

use crate::store::selection::RowSelection;

#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use std::path::PathBuf;
#[cfg(feature = "file-io")]
use vortex_array::expr::Expression;
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

use crate::store::indexes::ServePlan;

/// A raw (un-encoded) quad holding term strings in N-Triples form.
/// This is the shared in-memory (and on-disk, for external sorting)
/// representation consumed by layouts, indexes and builders before
/// writing to Vortex arrays.
#[derive(
    Clone,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RawQuad {
    pub s: String,
    pub p: String,
    pub o: String,
    pub g: String,
}

impl Ord for RawQuad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.s.cmp(&other.s)
            .then_with(|| self.p.cmp(&other.p))
            .then_with(|| self.o.cmp(&other.o))
            .then_with(|| self.g.cmp(&other.g))
    }
}

impl PartialOrd for RawQuad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RawQuad {
    pub fn from_quad(q: &Quad) -> Self {
        RawQuad {
            s: q.subject.to_string(),
            p: q.predicate.to_string(),
            o: q.object.to_string(),
            g: match &q.graph_name {
                oxrdf::GraphName::DefaultGraph => String::new(),
                other => other.to_string(),
            },
        }
    }
}

/// Dictionary-encoded quad columns: [`RawQuad`] terms replaced by their u32
/// codes in the global sorted term dictionary. Produced by the Dictionary
/// layout's encoding pass and consumed by index builders, which can work on
/// codes directly (sorted-dictionary codes preserve lexicographic order).
pub(crate) struct QuadCodes {
    pub s: Vec<u32>,
    pub p: Vec<u32>,
    pub o: Vec<u32>,
    pub g: Vec<u32>,
}

/// A lazily-decoded view onto quad data: the base the store was constructed
/// from, plus which of its rows this view covers.
///
/// Both variants keep their base intact and narrow a [`RowSelection`] over it
/// rather than rewriting rows, so base row ids stay meaningful for as long as
/// the view lives — that is what keeps secondary indexes usable across
/// `match_pattern` (their `_idx_*_rid` columns address base rows) and what
/// leaves the unselected data reachable for later mutation.
#[derive(Clone)]
pub(crate) enum QuadsSource {
    /// Quad data that is already loaded into a Vortex array.
    InMemory {
        /// The complete, shared array against which selections, tombstones,
        /// and secondary-index row ids are defined.
        base: ArrayRef,
        /// The base row ids visible through this particular store or derived
        /// view; narrowing a view changes this without rewriting `base`.
        selection: RowSelection,
        /// Base rows deleted since construction, one bit per base row (`None`
        /// until something is deleted).
        ///
        /// Deleting tombstones here instead of rewriting `base`, so base row
        /// ids survive a delete and the secondary indexes built against them
        /// stay usable. Every read path must apply this — see
        /// [`RowSelection::live_mask`]. The tombstoned rows are only reclaimed
        /// by compaction.
        deleted: Option<Mask>,
        /// When this view's selection came from an index resolution over an
        /// otherwise-unrefined base, and that index holds the matched rows as a
        /// contiguous run of its own columns, the plan for `quads()` to slice
        /// them straight from `base` instead of gathering the primary columns at
        /// scattered row ids. Index-agnostic: only `SecondaryByCopy` currently
        /// supplies one (see [`ServePlan`]). `None` on any view narrowed further
        /// — the plan is only valid while its row run is exactly the selection.
        ///
        /// [`ServePlan`]: crate::store::indexes::ServePlan
        serve: Option<ServePlan>,
    },
    #[cfg(feature = "file-io")]
    /// Quad data read lazily from a Vortex file when a query is executed.
    File {
        /// The path the file was opened from. Kept so compaction can rewrite
        /// the store's rows back over their own source file (atomically) and
        /// reopen it, rather than degrading a file-backed store to an in-memory
        /// one. Carried across every derived view so a compaction of a match
        /// result still knows which file to overwrite.
        path: PathBuf,
        /// The shared file handle, including its cached schema, metadata, and
        /// layout reader used by scans and pruning.
        file: Arc<VortexFile>,
        /// Pattern components not resolved to row ids, pushed down to the scan.
        filter: Option<Expression>,
        /// The file row ids visible through this store or derived view,
        /// typically narrowed by index lookups or pruning.
        selection: RowSelection,
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
        /// does (see [`ServePlan`]). `None` on any view whose selection has been
        /// narrowed further — the plan is only valid while its filter selects
        /// exactly the selection's rows.
        ///
        /// [`ServePlan`]: crate::store::indexes::ServePlan
        serve: Option<ServePlan>,
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
    /// The appended rows, one contiguous StructArray (never per-add chunks —
    /// appends rebuild it, so scans stay flat).
    pub(crate) rows: ArrayRef,
    /// The tail rows visible through this store or derived view, in tail-local
    /// ids.
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row
    /// (`None` until something is deleted).
    pub(crate) deleted: Option<Mask>,
}
