//! A view's data: [`QuadsSource`] (the base, in memory or in a file as its
//! [`Backend`], with the [`ViewSelection`] and tombstones that pick the rows
//! a view exposes), [`Tail`] (the rows appended since construction) and
//! `selection` (the selection types). A derived view is built from its
//! parent's source through the constructors here
//! ([`QuadsSource::in_memory_with`], [`QuadsSource::with_selection`], …),
//! never field by field.

pub(crate) mod pending;
pub(crate) mod selection;
pub(crate) mod tail_keys;

use std::sync::Arc;

use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::{Chunked, ChunkedArray};
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::subject_sorted;
use crate::store::indexes::{InMemoryServePlan, IndexComponent};
use crate::store::layouts::{ChunkDecode, ResolvedLayout, StringLayout};
use crate::store::probes::StructProbes;
use crate::store::scan::gather::gather_live;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::view::tail_keys::TailKeys;
use crate::store::zones::ZoneTable;

#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use crate::store::scan::file_exprs::{CodeEqs, PushedFilter};
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter::SplitMasks;

/// A view's base rows: the base the store was constructed from, never
/// rewritten, and the selection and tombstones that narrow it. Base row ids
/// stay valid for as long as the view lives, so the index components' `rid`
/// columns address them across matches.
#[derive(Clone)]
pub(crate) struct QuadsSource {
    /// The base rows this view covers. It may be pending: on a served match
    /// (the backend's `serve`), or on a view built only to be counted or
    /// windowed (see [`ViewSelection`]).
    pub(crate) selection: ViewSelection,
    /// The tombstones: one bit per base row, set for the rows deleted since
    /// construction (on file, since the open); `None` until a delete. Every
    /// read applies them ([`RowSelection::live_mask`]); only compaction
    /// reclaims them.
    pub(crate) deleted: Option<Mask>,
    /// Where the base rows live, with what the backend keeps beside them.
    pub(crate) backend: Backend,
}

/// Where a [`QuadsSource`]'s base rows live.
#[derive(Clone)]
pub(crate) enum Backend {
    /// A base held as an array in memory.
    InMemory(MemoryBase),
    /// A base read from a native store file as reads need it.
    #[cfg(feature = "file-io")]
    File(FileBase),
}

/// An in-memory base: the array, the index components and probes held
/// beside it, and the serve plan of a served view.
#[derive(Clone)]
pub(crate) struct MemoryBase {
    /// The whole base array; selections, tombstones and index row ids
    /// address its rows.
    pub(crate) base: ArrayRef,
    /// The index components over `base`, in a native file's index child
    /// schema; empty without indexes. Shared across views (`Arc`): their
    /// `rid` columns address base rows, which views and tombstones never
    /// renumber.
    pub(crate) components: Arc<[IndexComponent]>,
    /// The encoded-search probes over `base`'s columns ([`StructProbes`]),
    /// resolved lazily and shared by every view of this base; a new base
    /// takes a new cache.
    pub(crate) probes: Arc<StructProbes>,
    /// The per-zone code bounds of `base`'s code columns ([`ZoneTable`]),
    /// shared by every view of this base; empty when the base has none.
    pub(crate) zones: Arc<ZoneTable>,
    /// The index's plan for reading this view's rows from the index's own
    /// columns ([`InMemoryServePlan`]). Present only while the selection is
    /// exactly the plan's run: any narrowing drops it, materializing a
    /// pending selection first (see [`ViewSelection`]).
    pub(crate) serve: Option<InMemoryServePlan>,
}

/// A file base: the file handle, the filter a match pushed down to its
/// reads, and the serve plan of a served view.
#[cfg(feature = "file-io")]
#[derive(Clone)]
pub(crate) struct FileBase {
    /// The shared file handle: its cached schema, metadata and layout
    /// reader, and the path an owner's compaction atomically rewrites before
    /// reopening it (`compaction.rs`; a derived view's compaction never
    /// touches the file). Every root row is a quad row, the dictionary and
    /// the index children having row spaces of their own, so
    /// `file.row_count()` is the base's row count.
    pub(crate) file: Arc<NativeStoreFile>,
    /// The filter the view's selected rows pass, in the state its windows
    /// left it ([`FileFilter`]).
    pub(crate) filter: FileFilter,
    /// The index's plan for reading this view's rows from the index's own
    /// columns ([`FileServePlan`]). Present only while the selection is
    /// exactly the plan's run: any narrowing drops it, materializing a
    /// pending selection first (see [`ViewSelection`]).
    pub(crate) serve: Option<FileServePlan>,
}

/// The filter a file view's selected rows pass: none, pending, evaluated by
/// a window into masks it kept, or resolved by a window into the selection.
/// Every view derived from this one over another selection starts from the
/// filter its rows must still pass, pending ([`derived`](Self::derived)).
#[cfg(feature = "file-io")]
#[derive(Clone, Default)]
pub(crate) enum FileFilter {
    /// No filter: every selected row is the view's.
    #[default]
    None,
    /// Pattern components no index resolved, pushed down to the reads,
    /// which evaluate them over the selection.
    Pending(PushedFilter),
    /// A pending filter a window evaluated over the whole selection and
    /// found to cut no row, beside its masks, one per evaluated split in file
    /// order: the view's reads take their rows from the masks, and its
    /// counts their bits, so no read evaluates the filter again. The filter
    /// stays for what narrows the view (a chained match, a keep, a delete),
    /// which evaluates it with the narrowing. Kept only for a filter of code
    /// equalities over columns with chunk probes, the one the reads by id
    /// check, and only while the masks are smaller than the ids of the rows
    /// they keep; they are read off their bits, so nothing caches beside
    /// them.
    Kept {
        filter: PushedFilter,
        masks: Arc<SplitMasks>,
    },
    /// A filter a window resolved into the exact selection, leaving its code
    /// equalities: every selected row passed it, so a read of those rows by
    /// id reads only the columns the equalities leave free and fills these
    /// with their codes ([`CodeEqs::for_points`]).
    Resolved(CodeEqs),
}

#[cfg(feature = "file-io")]
impl FileFilter {
    /// `filter` pending, or no filter.
    pub(crate) fn pending(filter: Option<PushedFilter>) -> Self {
        filter.map_or(FileFilter::None, FileFilter::Pending)
    }

    /// The filter the reads evaluate over the selection: a pending one; none
    /// once a window kept its masks or resolved it.
    pub(crate) fn to_evaluate(&self) -> Option<&PushedFilter> {
        match self {
            FileFilter::Pending(filter) => Some(filter),
            _ => None,
        }
    }

    /// The filter every selected row must still pass: a pending one, or one
    /// a window kept the masks of.
    pub(crate) fn over_selection(&self) -> Option<&PushedFilter> {
        match self {
            FileFilter::Pending(filter) | FileFilter::Kept { filter, .. } => Some(filter),
            FileFilter::None | FileFilter::Resolved(_) => None,
        }
    }

    /// The masks a window kept of the filter.
    pub(crate) fn kept_masks(&self) -> Option<&Arc<SplitMasks>> {
        match self {
            FileFilter::Kept { masks, .. } => Some(masks),
            _ => None,
        }
    }

    /// The codes every row the view's reads take holds in the columns they
    /// name: a kept filter's equalities, a resolved one's; none otherwise (a
    /// pending filter's reads by id check its equalities themselves).
    pub(crate) fn pins(&self) -> &CodeEqs {
        match self {
            FileFilter::Kept { filter, .. } => filter.eqs().unwrap_or(CodeEqs::NONE),
            FileFilter::Resolved(eqs) => eqs,
            FileFilter::None | FileFilter::Pending(_) => CodeEqs::NONE,
        }
    }

    /// The filter a view derived from this one over another selection starts
    /// with: the filter its rows must still pass, pending, without the masks
    /// or the codes kept for this view's selection.
    fn derived(&self) -> Self {
        FileFilter::pending(self.over_selection().cloned())
    }

    /// This filter resolved into an exact selection: the codes its
    /// equalities pin; none without a filter, or for one of another shape.
    fn resolved(&self) -> Self {
        match self.over_selection().and_then(PushedFilter::eqs) {
            Some(eqs) => FileFilter::Resolved(eqs.clone()),
            None => FileFilter::None,
        }
    }
}

impl QuadsSource {
    /// Every row of `backend`, nothing deleted: a store's own source after a
    /// construction or an open.
    pub(crate) fn new(backend: Backend) -> Self {
        QuadsSource {
            selection: ViewSelection::all(),
            deleted: None,
            backend,
        }
    }

    pub(crate) fn in_memory(&self) -> Option<&MemoryBase> {
        match &self.backend {
            Backend::InMemory(memory) => Some(memory),
            #[cfg(feature = "file-io")]
            Backend::File(_) => None,
        }
    }

    #[cfg(feature = "file-io")]
    pub(crate) fn file(&self) -> Option<&FileBase> {
        match &self.backend {
            Backend::InMemory(_) => None,
            Backend::File(base) => Some(base),
        }
    }

    /// The file and the filter a match pushed down to its reads, while one
    /// is pending ([`FileFilter::to_evaluate`]); `None` in memory, with no
    /// filter, or once a window kept its masks or resolved it.
    #[cfg(feature = "file-io")]
    pub(crate) fn pending_filter(&self) -> Option<(&NativeStoreFile, &PushedFilter)> {
        let base = self.file()?;
        Some((&base.file, base.filter.to_evaluate()?))
    }

    /// Whether the selection is pending with no serve plan: a view built only
    /// to be counted or windowed (`IdsNeed::CountOrWindow`). Counting,
    /// windowing or keeping resolves it; nothing that reads rows holds one,
    /// so no view a caller gets back is in this state.
    pub(crate) fn is_pending_without_plan(&self) -> bool {
        !matches!(self.selection, ViewSelection::Exact(_)) && self.served_component().is_none()
    }

    /// Whether this source still covers every base row: no pushed-down
    /// filter and an all-rows selection.
    pub(crate) fn is_unrefined(&self) -> bool {
        !self.has_filter() && self.selection.is_all()
    }

    /// The number of base rows: the in-memory array's length, or the file's
    /// row count.
    pub(crate) fn base_len(&self) -> usize {
        match &self.backend {
            Backend::InMemory(memory) => memory.base.len(),
            #[cfg(feature = "file-io")]
            Backend::File(FileBase { file, .. }) => file.row_count() as usize,
        }
    }

    pub(crate) fn is_file_backed(&self) -> bool {
        match &self.backend {
            Backend::InMemory(_) => false,
            #[cfg(feature = "file-io")]
            Backend::File(_) => true,
        }
    }

    /// Whether the base's rows are in global `(s, p, o, g)` order: an
    /// in-memory base's subject stamp, or the file's recorded
    /// `quads_sorted` provenance.
    pub(crate) fn subject_sorted(&self) -> bool {
        match &self.backend {
            Backend::InMemory(memory) => subject_sorted(&memory.base),
            #[cfg(feature = "file-io")]
            Backend::File(FileBase { file, .. }) => file.quads_sorted(),
        }
    }

    /// The live base rows a window's kept filter masks hold
    /// ([`FileFilter::Kept`]), counted off the masks; `None` without them,
    /// and in memory.
    pub(crate) fn evaluated_len(&self) -> Option<usize> {
        #[cfg(feature = "file-io")]
        if let Some(masks) = self.file().and_then(|base| base.filter.kept_masks()) {
            return Some(masks.kept());
        }
        None
    }

    /// This file source with `masks`, its filter evaluated over its exact
    /// selection, kept for its reads ([`FileFilter::Kept`]).
    #[cfg(feature = "file-io")]
    pub(crate) fn with_evaluated_masks(&self, masks: Arc<SplitMasks>) -> Self {
        debug_assert!(self.has_filter(), "masks are a pending filter's");
        debug_assert!(
            self.served_component().is_none(),
            "a served view reads through its plan"
        );
        debug_assert!(
            matches!(self.selection, ViewSelection::Exact(_)),
            "masks cover an exact selection"
        );
        let mut source = self.clone();
        if let Backend::File(base) = &mut source.backend
            && let Some(filter) = base.filter.over_selection().cloned()
        {
            base.filter = FileFilter::Kept { filter, masks };
        }
        source
    }

    /// Whether the selected rows must still pass a pushed-down file filter,
    /// pending or with its masks kept ([`FileFilter::over_selection`]); never
    /// for an in-memory source, whose matches resolve to exact row ids.
    pub(crate) fn has_filter(&self) -> bool {
        match &self.backend {
            Backend::InMemory(_) => false,
            #[cfg(feature = "file-io")]
            Backend::File(base) => base.filter.over_selection().is_some(),
        }
    }

    /// The index component whose serve plan answers this view's reads, if
    /// any.
    pub(crate) fn served_component(&self) -> Option<&'static str> {
        match &self.backend {
            Backend::InMemory(memory) => memory.serve.as_ref().map(|plan| plan.component()),
            #[cfg(feature = "file-io")]
            Backend::File(base) => base.serve.as_ref().map(|plan| plan.component()),
        }
    }

    /// The exact selection. A pending selection's ids are computed once and
    /// cached for every clone of the view: synchronously in memory, where they
    /// take no I/O, and through the index child's read on file.
    pub(crate) async fn materialized_selection(&self) -> Result<RowSelection> {
        match &self.backend {
            Backend::InMemory(_) => self.selection.materialized(),
            #[cfg(feature = "file-io")]
            Backend::File(_) => self.selection.materialized_async().await,
        }
    }

    /// The `limit` rows after skipping `offset` of a selection held pending
    /// for a window (`IdsNeed::CountOrWindow`), beside the base's live rows
    /// as far as the window knows them, read without the selection's other
    /// ids. A residual compare left pending compares its pieces only until
    /// the window's last live row is found ([`PendingResidual::window`]).
    /// A run, with no tombstones, has its width as the base's live size and
    /// reads its window's rows alone ([`LazyRowIds::window`]): in memory cut,
    /// before decoding, from a rid column whose rids ascend; on file read
    /// from the window's rows of a located run's rid column. `None` for any
    /// other view (an exact selection, a run of unknown width or whose rids
    /// do not ascend, a run under tombstones, a pending file filter), whose
    /// window comes from its materialized selection.
    ///
    /// [`LazyRowIds::window`]: crate::store::indexes::LazyRowIds::window
    /// [`PendingResidual::window`]: crate::store::view::pending::PendingResidual::window
    pub(crate) async fn located_window(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<(RowSelection, usize)>> {
        let lazy = match &self.selection {
            ViewSelection::Residual(residual) => {
                let (ids, live) = residual.window(offset, limit, self.deleted.as_ref())?;
                return Ok(Some((RowSelection::from_ids(ids), live)));
            }
            ViewSelection::Pending { ids, .. } => ids,
            ViewSelection::Exact(_) => return Ok(None),
        };
        let Some(live) = lazy
            .len_if_known()
            .filter(|_| self.deleted.is_none() && !self.has_filter())
        else {
            return Ok(None);
        };
        Ok(lazy
            .window(offset, limit)
            .await?
            .map(|ids| (RowSelection::from_ids(ids), live)))
    }

    /// This source under `selection`, its reads served by `serve`, over
    /// `memory` (its own in-memory base): the same base, components, probes
    /// and tombstones.
    #[inline]
    pub(crate) fn in_memory_with(
        &self,
        memory: &MemoryBase,
        selection: ViewSelection,
        serve: Option<InMemoryServePlan>,
    ) -> Self {
        QuadsSource {
            selection,
            deleted: self.deleted.clone(),
            backend: Backend::InMemory(MemoryBase {
                base: memory.base.clone(),
                components: Arc::clone(&memory.components),
                probes: Arc::clone(&memory.probes),
                zones: Arc::clone(&memory.zones),
                serve,
            }),
        }
    }

    /// This source under `filter` and `selection`, its reads served by
    /// `serve`, over `base` (its own file base): the same file handle and
    /// tombstones.
    #[inline]
    #[cfg(feature = "file-io")]
    pub(crate) fn file_with(
        &self,
        base: &FileBase,
        filter: FileFilter,
        selection: ViewSelection,
        serve: Option<FileServePlan>,
    ) -> Self {
        QuadsSource {
            selection,
            deleted: self.deleted.clone(),
            backend: Backend::File(FileBase {
                file: Arc::clone(&base.file),
                filter,
                serve,
            }),
        }
    }

    /// This source under `selection`, with no serve plan; a file source keeps
    /// the filter its rows must still pass, pending
    /// ([`FileFilter::derived`]). A pending `selection` keeps its deferred
    /// ids without a plan, which only a view built to be counted or windowed
    /// may hold ([`is_pending_without_plan`](Self::is_pending_without_plan)).
    #[inline]
    pub(crate) fn with_selection(&self, selection: ViewSelection) -> Self {
        match &self.backend {
            Backend::InMemory(memory) => self.in_memory_with(memory, selection, None),
            #[cfg(feature = "file-io")]
            Backend::File(base) => self.file_with(base, base.filter.derived(), selection, None),
        }
    }

    /// This source over every base row, with no filter and no serve plan:
    /// the same base (or file), components and tombstones, as an owner holds
    /// them.
    pub(crate) fn unrestricted(&self) -> Self {
        match &self.backend {
            Backend::InMemory(memory) => self.in_memory_with(memory, ViewSelection::all(), None),
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                self.file_with(base, FileFilter::None, ViewSelection::all(), None)
            }
        }
    }

    /// This source with its pushed-down filter resolved into its exact
    /// selection: the filter dropped, and the masks a window kept of it, the
    /// codes its equalities pin kept for the reads by id
    /// ([`FileFilter::Resolved`]); a no-op in memory.
    pub(crate) fn with_filter_resolved(mut self) -> Self {
        match &mut self.backend {
            Backend::InMemory(_) => {}
            #[cfg(feature = "file-io")]
            Backend::File(base) => base.filter = base.filter.resolved(),
        }
        self
    }

    /// This source with the base rows `doomed` sets added to its tombstones,
    /// sharing the base (or file) and the components: tombstones never
    /// renumber base rows, so the components' row ids stay valid.
    ///
    /// Only an owner's source takes tombstones: every base row selected and
    /// no filter pending ([`is_unrefined`](Self::is_unrefined)), so there is
    /// no plan to lose and no filter to keep. The delete that calls this is
    /// owner-gated.
    pub(crate) fn with_tombstones(&self, doomed: Mask) -> Self {
        debug_assert!(
            self.is_unrefined(),
            "only an owner's source (every row selected, no filter) takes tombstones"
        );
        let mut source = self.with_selection(self.selection.clone());
        source.deleted = Some(union_deleted(self.deleted.as_ref(), doomed));
        source
    }

    /// This source selecting no base row, with no serve plan and (in memory)
    /// no index components; a file source keeps its filter.
    pub(crate) fn emptied(&self) -> Self {
        let mut source = self.with_selection(ViewSelection::Exact(RowSelection::empty()));
        match &mut source.backend {
            Backend::InMemory(memory) => memory.components = Arc::from(Vec::new()),
            #[cfg(feature = "file-io")]
            Backend::File(_) => {}
        }
        source
    }
}

/// Rows appended since construction, held apart so the base (its row ids,
/// indexes, tombstones and file) is never rewritten. A match runs the base's
/// own paths, mask-scans the tail and unions the two. The rows are in the
/// layout [`layout_for`](Self::layout_for) gives the store's, and
/// `compact_with_indexes` folds them into the base, re-sorted, re-encoded
/// and re-indexed.
///
/// Selection and tombstones work as the base's do, in tail-local row ids
/// (`0..rows.len()`): views narrow `selection`, deletes set bits in `deleted`,
/// and every read applies both (`scan::gather::gather_live`).
#[derive(Clone)]
pub(crate) struct Tail {
    /// The appended rows: a flat first chunk and one chunk per later append,
    /// folded flat by the flatten policy ([`TAIL_FLATTEN_FLOOR`],
    /// [`TAIL_MAX_CHUNKS`]) so a scan sees a bounded number of chunks.
    pub(crate) rows: ArrayRef,
    /// The tail rows visible through this store or derived view, in tail-local
    /// ids.
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row
    /// (`None` until something is deleted).
    pub(crate) deleted: Option<Mask>,
    /// The string layout `rows` are stored in: [`layout_for`](Self::layout_for)
    /// the store's.
    pub(crate) layout: StringLayout,
    /// The keys of `rows`, which a fully bound match reads instead of
    /// comparing every row ([`TailKeys`]).
    pub(crate) keys: TailKeys,
}

/// Tail flatten policy: appended chunks are folded into the flat first chunk
/// once their row count reaches `max(flat_len, TAIL_FLATTEN_FLOOR)`, so the
/// tail is rewritten geometrically rather than on every add.
pub(crate) const TAIL_FLATTEN_FLOOR: usize = 1_024;
/// Tail flatten policy: appended chunks are folded into the flat first chunk
/// once the tail holds more than this many chunks, whatever their row counts,
/// so tail scans (which visit every chunk) stay dense.
pub(crate) const TAIL_MAX_CHUNKS: usize = 64;

impl Tail {
    /// The layout of a tail beside a base in `base`, always one that stores
    /// strings: the base's, except Default under the Dictionary layout, whose
    /// sorted dictionary has no code for an appended term; patterns then
    /// probe the base by code and the tail by string.
    pub(crate) fn layout_for(base: &ResolvedLayout) -> StringLayout {
        match base {
            ResolvedLayout::Default | ResolvedLayout::Dictionary(_) => StringLayout::Default,
            ResolvedLayout::TypedObject => StringLayout::TypedObject,
        }
    }

    /// A tail of fresh `rows` in `layout`, every row visible and none
    /// deleted, every row keyed.
    pub(crate) fn new(rows: ArrayRef, layout: StringLayout) -> Result<Tail> {
        let keys = TailKeys::of(&rows, layout)?;
        Ok(Tail {
            rows,
            selection: RowSelection::All,
            deleted: None,
            layout,
            keys,
        })
    }

    /// The same rows and tombstones under a different `selection`.
    pub(crate) fn with_selection(&self, selection: RowSelection) -> Tail {
        Tail {
            rows: self.rows.clone(),
            selection,
            deleted: self.deleted.clone(),
            layout: self.layout,
            keys: self.keys.clone(),
        }
    }

    /// How many tail rows this view selects that are not tombstoned.
    pub(crate) fn live_len(&self) -> usize {
        self.selection
            .live_len(self.deleted.as_ref(), self.rows.len())
    }

    /// This tail narrowed to its `limit` live rows after skipping `offset`,
    /// in tail order.
    pub(crate) fn window(&self, offset: usize, limit: usize) -> Tail {
        self.with_selection(self.selection.window(
            offset,
            limit,
            self.deleted.as_ref(),
            self.rows.len(),
        ))
    }

    /// This tail with the rows `doomed` selects (tail-local ids, as a matched
    /// view's tail selection holds them) added to its tombstones, under the
    /// same selection. Like [`QuadsSource::with_tombstones`], for an owner's
    /// tail only (selection `All`); the delete that calls it is owner-gated.
    pub(crate) fn tombstoned(&self, doomed: &RowSelection) -> Tail {
        Tail {
            rows: self.rows.clone(),
            selection: self.selection.clone(),
            deleted: Some(union_deleted(
                self.deleted.as_ref(),
                doomed.to_mask(self.rows.len()),
            )),
            layout: self.layout,
            keys: self.keys.clone(),
        }
    }

    /// The tail rows visible through this store, tombstones dropped, in tail
    /// order.
    pub(crate) fn live_rows(&self) -> Result<ArrayRef> {
        gather_live(&self.rows, &self.selection, self.deleted.as_ref(), None)
    }

    /// The tail rows visible through this store decoded to `T`, in tail
    /// order; a gather failure is the one `Err` element. Synchronous, as
    /// [`raw_quads`](Self::raw_quads) is.
    pub(crate) fn decode<T: ChunkDecode>(&self) -> Vec<Result<T>> {
        match self.live_rows() {
            Ok(rows) => T::from_strings(self.layout, &rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// The tail rows visible through this store, decoded to raw quads in
    /// tail order: the tail holds N-Triples strings, whatever the base's
    /// layout.
    pub(crate) fn raw_quads(&self) -> Result<Vec<RawQuad>> {
        self.layout.raw_quads(&self.live_rows()?)
    }

    /// A tail of this tail's live rows followed by `fresh`, every row
    /// visible: `fresh` is one more chunk, and the chunks after the first are
    /// folded into it per [`TAIL_FLATTEN_FLOOR`]/[`TAIL_MAX_CHUNKS`]. The new
    /// tail renumbers the rows, which is safe: views of the pre-append store
    /// keep the old tail, and an owner's selection is `All`.
    ///
    /// Without tombstones, under an owner's selection, the new tail holds this
    /// tail's rows in order and then `fresh`'s, so its keys are these keys
    /// extended by `fresh`'s alone. Dropping a tombstoned row renumbers every
    /// row after it, and the new tail's rows are keyed again.
    pub(crate) fn append(&self, fresh: ArrayRef) -> Result<Tail> {
        let old = self.live_rows()?;
        let dtype = old.dtype().clone();
        // Only `fresh` needs its dtype checked: every chunk of `old` has
        // `dtype` already, which a chunked array of up to `TAIL_MAX_CHUNKS`
        // chunks would otherwise check again on every append.
        if fresh.dtype() != &dtype {
            return Err(VortexRdfError::Vortex(vortex_error::vortex_err!(
                "appended rows of type {} do not match the tail's {dtype}",
                fresh.dtype()
            )));
        }
        let keys = match (&self.deleted, &self.selection) {
            (None, RowSelection::All) => Some(self.keys.extended(&fresh)?),
            _ => None,
        };
        let mut chunks = match old.clone().try_downcast::<Chunked>() {
            Ok(ch) => ch.chunks(),
            Err(_) => vec![old],
        };
        let flat_len = chunks[0].len();
        let accreted: usize = chunks[1..].iter().map(|c| c.len()).sum::<usize>() + fresh.len();
        chunks.push(fresh);
        let n_chunks = chunks.len();
        // SAFETY: every chunk has `dtype`: those of `old` are the chunks of an
        // array of `dtype` (a chunked array's own, or `old` itself), and the
        // last is `fresh`, checked above.
        let combined = unsafe { ChunkedArray::new_unchecked(chunks, dtype) }.into_array();
        let rows = if accreted >= flat_len.max(TAIL_FLATTEN_FLOOR) || n_chunks > TAIL_MAX_CHUNKS {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            combined
                .execute::<RecursiveCanonical>(&mut ctx)?
                .0
                .into_array()
        } else {
            combined
        };
        // `live_rows` dropped the tombstoned rows, so the new tail has none.
        match keys {
            Some(keys) => Ok(Tail {
                rows,
                selection: RowSelection::All,
                deleted: None,
                layout: self.layout,
                keys,
            }),
            None => Tail::new(rows, self.layout),
        }
    }
}

/// The `existing` tombstones with the `doomed` rows added.
fn union_deleted(existing: Option<&Mask>, doomed: Mask) -> Mask {
    match existing {
        Some(existing) => existing | &doomed,
        None => doomed,
    }
}
