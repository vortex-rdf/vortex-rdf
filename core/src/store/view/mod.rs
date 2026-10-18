//! The store's backing data: [`QuadsSource`] (the base — in memory or a
//! Vortex file — together with the [`ViewSelection`], tombstones, index
//! components and serve plan that determine which base rows a view exposes)
//! and [`Tail`] (rows appended since construction, held apart from the base).
//! A derived view is built from its parent's source through the constructors
//! here ([`QuadsSource::in_memory_with`], [`QuadsSource::with_selection`],
//! …), never field by field.

pub(crate) mod selection;

use std::sync::Arc;

use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::{Chunked, ChunkedArray};
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use vortex_mask::Mask;

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::subject_sorted;
use crate::store::indexes::{InMemoryServePlan, IndexComponent};
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::probes::StructProbes;
use crate::store::scan::gather::gather_live;
use crate::store::view::selection::{RowSelection, ViewSelection};

#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
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
        /// still be pending — on a served match (see `serve`), or on a view
        /// built only to be counted or windowed (see [`ViewSelection`]).
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
        /// The shared file handle, including its cached schema, metadata, and
        /// layout reader used by scans and pruning, and the path it was
        /// opened from, which an owner's compaction rewrites its rows over
        /// atomically before it reopens it (`compaction.rs`); a derived
        /// view's compaction never touches the file. Every root row is a quad
        /// row (the dictionary and index copies ride as auxiliary children
        /// with their own row spaces), so `file.row_count()` is the store's
        /// row space.
        file: Arc<NativeStoreFile>,
        /// Pattern components not resolved to row ids, pushed down to the scan.
        filter: Option<Expression>,
        /// The file row ids visible through this store or derived view,
        /// typically narrowed by index lookups or pruning. May still be
        /// pending — on a served match (see `serve`), or on a view built only
        /// to be counted or windowed (see [`ViewSelection`]).
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

    /// Whether the selection is still pending with no serve plan to read
    /// through — the state of a view built only to be counted or windowed
    /// (`IdsNeed::CountOrWindow`). It is resolved by counting, windowing or
    /// keeping the view; nothing that reads rows may hold one, so no view a
    /// caller gets back is in it.
    pub(crate) fn is_pending_without_plan(&self) -> bool {
        matches!(self.view_selection(), ViewSelection::Pending(_))
            && self.served_component().is_none()
    }

    /// Whether this source still covers every base row: no pushed-down
    /// filter and an all-rows selection.
    pub(crate) fn is_unrefined(&self) -> bool {
        !self.has_filter() && self.view_selection().is_all()
    }

    /// The number of base rows: the in-memory array's length, or the file's
    /// row count.
    pub(crate) fn base_len(&self) -> usize {
        match self {
            QuadsSource::InMemory { base, .. } => base.len(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.row_count() as usize,
        }
    }

    /// The tombstones, one bit per base row; `None` until something is
    /// deleted.
    pub(crate) fn deleted(&self) -> Option<&Mask> {
        match self {
            QuadsSource::InMemory { deleted, .. } => deleted.as_ref(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { deleted, .. } => deleted.as_ref(),
        }
    }

    /// The base's struct dtype — the layout's primary columns.
    pub(crate) fn dtype(&self) -> &DType {
        match self {
            QuadsSource::InMemory { base, .. } => base.dtype(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.dtype(),
        }
    }

    /// Whether the base is read from a file.
    pub(crate) fn is_file_backed(&self) -> bool {
        match self {
            QuadsSource::InMemory { .. } => false,
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => true,
        }
    }

    /// Whether the base's rows are in global `(s, p, o, g)` order: an
    /// in-memory base's subject stamp, or the file's recorded
    /// `quads_sorted` provenance.
    pub(crate) fn subject_sorted(&self) -> bool {
        match self {
            QuadsSource::InMemory { base, .. } => subject_sorted(base),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.quads_sorted(),
        }
    }

    /// Whether a pushed-down file filter is still to be evaluated; never for
    /// an in-memory source, whose matches resolve to exact row ids.
    pub(crate) fn has_filter(&self) -> bool {
        match self {
            QuadsSource::InMemory { .. } => false,
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => filter.is_some(),
        }
    }

    /// The file a file-backed source reads; `None` in memory.
    #[cfg(feature = "file-io")]
    pub(crate) fn file(&self) -> Option<&NativeStoreFile> {
        match self {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { file, .. } => Some(file),
        }
    }

    /// The pushed-down filter of a file-backed source; `None` in memory or
    /// when nothing is left to filter.
    #[cfg(feature = "file-io")]
    pub(crate) fn filter(&self) -> Option<&Expression> {
        match self {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { filter, .. } => filter.as_ref(),
        }
    }

    /// The index component whose serve plan answers this view's reads, if
    /// any.
    pub(crate) fn served_component(&self) -> Option<&'static str> {
        match self {
            QuadsSource::InMemory { serve, .. } => serve.as_ref().map(|plan| plan.component()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { serve, .. } => serve.as_ref().map(|plan| plan.component()),
        }
    }

    /// The serve plan answering a file-backed source's reads, if any; `None`
    /// in memory.
    #[cfg(feature = "file-io")]
    pub(crate) fn file_serve(&self) -> Option<&FileServePlan> {
        match self {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { serve, .. } => serve.as_ref(),
        }
    }

    /// The exact selection, running a pending resolution's deferred ids
    /// (cached for every later consumer and every clone of the view):
    /// synchronously in memory, where they never take I/O, and through the
    /// index child's deferred read on file.
    pub(crate) async fn materialized_selection(&self) -> Result<RowSelection> {
        match self {
            QuadsSource::InMemory { selection, .. } => selection.materialized(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { selection, .. } => selection.materialized_async().await,
        }
    }

    /// The `limit` rows after skipping `offset` of a located run held
    /// pending for a window (`IdsNeed::CountOrWindow`), read without the
    /// run's other ids, and the run's width as the base's live size. In
    /// memory a run whose rids ascend cuts the window out of its rid column
    /// before decoding ([`LazyRowIds::window`]); on file a located run reads
    /// only the window's rows of the child's rid column
    /// ([`LazyRowIds::window_async`]). `None` for any other view — an exact
    /// selection, a run of unknown width or whose rids do not ascend,
    /// tombstones, a pending file filter — whose window is cut from its
    /// materialized selection instead.
    ///
    /// [`LazyRowIds::window`]: crate::store::indexes::LazyRowIds::window
    /// [`LazyRowIds::window_async`]: crate::store::indexes::LazyRowIds::window_async
    pub(crate) async fn located_window(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<(RowSelection, usize)>> {
        let located = match self {
            QuadsSource::InMemory {
                selection: ViewSelection::Pending(lazy),
                deleted: None,
                ..
            } => match lazy.len_if_known() {
                Some(live) => lazy.window(offset, limit)?.map(|ids| (ids, live)),
                None => None,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                selection: ViewSelection::Pending(lazy),
                filter: None,
                deleted: None,
                ..
            } => match lazy.len_if_known() {
                Some(live) => lazy
                    .window_async(offset, limit)
                    .await?
                    .map(|ids| (ids, live)),
                None => None,
            },
            _ => None,
        };
        Ok(located.map(|(ids, live)| (RowSelection::from_ids(ids), live)))
    }

    /// This in-memory source under `selection`, its reads served by `serve`:
    /// the same base, components, tombstones and probes.
    #[inline]
    #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
    pub(crate) fn in_memory_with(
        &self,
        selection: ViewSelection,
        serve: Option<InMemoryServePlan>,
    ) -> Self {
        let QuadsSource::InMemory {
            base,
            components,
            deleted,
            probes,
            ..
        } = self
        else {
            unreachable!("in_memory_with is only called on an in-memory source");
        };
        QuadsSource::InMemory {
            base: base.clone(),
            selection,
            components: Arc::clone(components),
            deleted: deleted.clone(),
            probes: Arc::clone(probes),
            serve,
        }
    }

    /// This file source under `filter` and `selection`, its reads served by
    /// `serve`: the same file handle and tombstones.
    #[inline]
    #[cfg(feature = "file-io")]
    pub(crate) fn file_with(
        &self,
        filter: Option<Expression>,
        selection: ViewSelection,
        serve: Option<FileServePlan>,
    ) -> Self {
        let QuadsSource::File { file, deleted, .. } = self else {
            unreachable!("file_with is only called on a file source");
        };
        QuadsSource::File {
            file: Arc::clone(file),
            filter,
            selection,
            deleted: deleted.clone(),
            serve,
        }
    }

    /// This source under `selection`, with no serve plan; a file source keeps
    /// its filter. A pending `selection` keeps its deferred ids but no plan,
    /// which only a view built to be counted or windowed may hold (see
    /// [`is_pending_without_plan`](Self::is_pending_without_plan)).
    #[inline]
    pub(crate) fn with_selection(&self, selection: ViewSelection) -> Self {
        match self {
            QuadsSource::InMemory { .. } => self.in_memory_with(selection, None),
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => self.file_with(filter.clone(), selection, None),
        }
    }

    /// This source with its pushed-down filter dropped (a no-op in memory).
    pub(crate) fn without_filter(self) -> Self {
        match self {
            QuadsSource::InMemory { .. } => self,
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                selection,
                deleted,
                serve,
                ..
            } => QuadsSource::File {
                file,
                filter: None,
                selection,
                deleted,
                serve,
            },
        }
    }

    /// This source with the base rows `doomed` sets folded into its
    /// tombstones, under the same selection and with no serve plan. The base
    /// (or file) and the components are shared untouched: tombstoning never
    /// renumbers base rows, so the components' row ids stay valid.
    ///
    /// Only an owner's source takes tombstones: every base row selected and
    /// no filter pending ([`is_unrefined`](Self::is_unrefined)), so there is
    /// no plan to drop and no filter to keep. A served pending view would
    /// lose its plan and be left pending without one, and a filtered file
    /// view would keep a filter the delete has no use for; the delete that
    /// calls this is owner-gated.
    pub(crate) fn with_tombstones(&self, doomed: Mask) -> Self {
        debug_assert!(
            self.is_unrefined(),
            "only an owner's source (every row selected, no filter) takes tombstones"
        );
        let mut source = self.with_selection(self.view_selection().clone());
        match &mut source {
            QuadsSource::InMemory { deleted, .. } => {
                *deleted = Some(union_deleted(deleted.as_ref(), doomed));
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { deleted, .. } => {
                *deleted = Some(union_deleted(deleted.as_ref(), doomed));
            }
        }
        source
    }

    /// This source selecting no base row, with no serve plan and (in memory)
    /// no index components; a file source keeps its filter.
    pub(crate) fn emptied(&self) -> Self {
        let mut source = self.with_selection(ViewSelection::Exact(RowSelection::empty()));
        match &mut source {
            QuadsSource::InMemory { components, .. } => *components = Arc::from(Vec::new()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => {}
        }
        source
    }
}

/// Rows appended after construction, held apart from the base so the base —
/// its row ids, indexes, tombstones and file handle — is never rewritten.
/// Queries run the base's own paths and a mask scan over the tail and union
/// the two.
///
/// The rows are a single contiguous StructArray in `layout`: the store's own
/// primary layout, except under the Dictionary layout, where they are
/// Default-layout N-Triples strings (see [`layout_for`](Self::layout_for)):
/// an appended term has no code in the sorted dictionary, so the tail keeps
/// terms verbatim and patterns probe the base by code and the tail by
/// string. The tail is folded into the base — re-sorted, re-encoded,
/// re-indexed — by `compact_with_indexes`.
///
/// Selection and tombstones mirror the base's, in tail-local row ids
/// (`0..rows.len()`): views narrow `selection`, deletes set bits in `deleted`,
/// and every read path applies both (`scan::gather::gather_live`).
#[derive(Clone)]
pub(crate) struct Tail {
    /// The appended rows. Appends accrete as chunks of a ChunkedArray and are
    /// folded flat once the accreted rows outgrow the flatten policy
    /// ([`TAIL_FLATTEN_FLOOR`], [`TAIL_MAX_CHUNKS`]), so scans see at most a
    /// bounded chunk count.
    pub(crate) rows: ArrayRef,
    /// The tail rows visible through this store or derived view, in tail-local
    /// ids.
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row
    /// (`None` until something is deleted).
    pub(crate) deleted: Option<Mask>,
    /// The layout `rows` are stored in: [`layout_for`](Self::layout_for) the
    /// store's.
    pub(crate) layout: ResolvedLayout,
}

/// Tail flatten policy: appended chunks are folded into the flat first chunk
/// once their row count reaches `max(flat_len, TAIL_FLATTEN_FLOOR)` — so the
/// tail is rewritten geometrically, not on every add, and a small tail never
/// flattens below this floor.
pub(crate) const TAIL_FLATTEN_FLOOR: usize = 1_024;
/// Tail flatten policy: appended chunks are folded into the flat first chunk
/// once the tail holds more than this many chunks, whatever their row counts,
/// so tail scans (which visit every chunk) stay dense.
pub(crate) const TAIL_MAX_CHUNKS: usize = 64;

impl Tail {
    /// The layout a tail beside a base in `base` stores its rows in: the
    /// base's own, except under the Dictionary layout, where an appended term
    /// has no code in the sorted dictionary, so the tail holds Default-layout
    /// strings instead — patterns probe the base by code and the tail by
    /// string.
    pub(crate) fn layout_for(base: &ResolvedLayout) -> ResolvedLayout {
        match base {
            ResolvedLayout::Dictionary(_) => ResolvedLayout::Default,
            other => other.clone(),
        }
    }

    /// A tail of fresh `rows` in `layout`, every row visible and none
    /// deleted.
    pub(crate) fn new(rows: ArrayRef, layout: ResolvedLayout) -> Tail {
        Tail {
            rows,
            selection: RowSelection::All,
            deleted: None,
            layout,
        }
    }

    /// The same rows and tombstones under a different `selection`.
    pub(crate) fn with_selection(&self, selection: RowSelection) -> Tail {
        Tail {
            rows: self.rows.clone(),
            selection,
            deleted: self.deleted.clone(),
            layout: self.layout.clone(),
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

    /// This tail with the tail rows `doomed` selects (tail-local ids, as a
    /// matched view's tail selection is) folded into its tombstones.
    pub(crate) fn tombstoned(&self, doomed: &RowSelection) -> Tail {
        Tail {
            rows: self.rows.clone(),
            selection: self.selection.clone(),
            deleted: Some(union_deleted(
                self.deleted.as_ref(),
                doomed.to_mask(self.rows.len()),
            )),
            layout: self.layout.clone(),
        }
    }

    /// The tail rows visible through this store, tombstones dropped, in tail
    /// order.
    pub(crate) fn live_rows(&self) -> Result<ArrayRef> {
        gather_live(&self.rows, &self.selection, self.deleted.as_ref(), None)
    }

    /// The tail rows visible through this store decoded to `T`, in tail
    /// order, a gather failure as one `Err` element — synchronously, as for
    /// [`raw_quads`](Self::raw_quads).
    pub(crate) fn decode<T: ChunkDecode>(&self) -> Vec<Result<T>> {
        match self.live_rows() {
            Ok(rows) => T::decode(&self.layout, &rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// The tail rows visible through this store, decoded to raw quads in
    /// tail order. Never needs the async dictionary path: under the
    /// Dictionary layout the tail already holds N-Triples strings.
    pub(crate) fn raw_quads(&self) -> Result<Vec<RawQuad>> {
        self.layout.raw_quads(&self.live_rows()?)
    }

    /// A tail of this tail's live rows followed by `fresh`, every row
    /// visible. The fresh rows join the tail as one more chunk of a
    /// ChunkedArray accumulator; the accreted suffix is folded into the flat
    /// prefix per [`TAIL_FLATTEN_FLOOR`]/[`TAIL_MAX_CHUNKS`]. Renumbering the
    /// old tail's ids on flatten is safe: views of the pre-append store keep
    /// the old tail, and an owner's selections are `All`.
    pub(crate) fn append(&self, fresh: ArrayRef) -> Result<Tail> {
        let old = self.live_rows()?;
        let dtype = old.dtype().clone();
        let mut chunks = match old.clone().try_downcast::<Chunked>() {
            Ok(ch) => ch.chunks(),
            Err(_) => vec![old],
        };
        let flat_len = chunks[0].len();
        let accreted: usize = chunks[1..].iter().map(|c| c.len()).sum::<usize>() + fresh.len();
        chunks.push(fresh);
        let n_chunks = chunks.len();
        let combined = ChunkedArray::try_new(chunks, dtype)?.into_array();
        let rows = if accreted >= flat_len.max(TAIL_FLATTEN_FLOOR) || n_chunks > TAIL_MAX_CHUNKS {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            combined
                .execute::<RecursiveCanonical>(&mut ctx)?
                .0
                .into_array()
        } else {
            combined
        };
        // Gathering the live rows above dropped any tombstoned tail rows
        // already.
        Ok(Tail::new(rows, self.layout.clone()))
    }
}

/// Fold a freshly-doomed set of rows into existing tombstones, shared by the
/// base's delete path ([`QuadsSource::with_tombstones`]) and the tail's
/// ([`Tail::tombstoned`]).
fn union_deleted(existing: Option<&Mask>, doomed: Mask) -> Mask {
    match existing {
        Some(existing) => existing | &doomed,
        None => doomed,
    }
}
