//! The view model: [`QuadsSource`] (the base, in memory or in a file, with
//! the [`ViewSelection`], tombstones, index components and serve plan that
//! pick its visible rows) and [`Tail`] (rows appended since construction,
//! held apart from the base).

pub(crate) mod selection;

use std::sync::Arc;

use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::{Chunked, ChunkedArray};
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
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
use std::path::PathBuf;
#[cfg(feature = "file-io")]
use vortex_array::expr::Expression;

/// The base a view reads and which of its rows it exposes. Both variants keep
/// the base intact and narrow a selection of base row ids over it, so index
/// `rid` columns and the tombstone mask stay valid across every derived view.
#[derive(Clone)]
pub(crate) enum QuadsSource {
    /// A base held in memory.
    InMemory {
        /// The complete array that selections, tombstones and index row ids
        /// address.
        base: ArrayRef,
        /// The visible base rows; `Pending` only alongside `serve: Some`.
        selection: ViewSelection,
        /// Index components in the file's child schema, shared by every
        /// view; empty for a store built without indexes.
        components: Arc<[IndexComponent]>,
        /// Tombstones, one bit per base row; `None` until a delete. Every
        /// read applies them.
        deleted: Option<Mask>,
        /// Encoded-search probes over `base`'s columns, shared by every view
        /// over it.
        probes: Arc<StructProbes>,
        /// The index plan serving this view's rows; present only while the
        /// selection is exactly the plan's run.
        serve: Option<InMemoryServePlan>,
    },
    #[cfg(feature = "file-io")]
    /// A base read from a Vortex file on each scan.
    File {
        /// The path the file was opened from; an owner's compaction rewrites
        /// and reopens it.
        path: PathBuf,
        /// The dictionary-residency budget of the open, reused by
        /// compaction's reopen.
        dict_max_resident_bytes: u64,
        /// The shared file handle; `file.row_count()` is the base row space.
        file: Arc<NativeStoreFile>,
        /// Pattern components not resolved to row ids, pushed down to the
        /// scan.
        filter: Option<Expression>,
        /// The visible file rows; `Pending` only alongside `serve: Some`.
        selection: ViewSelection,
        /// Tombstones, one bit per file row; `None` until a delete. Every
        /// read applies them.
        deleted: Option<Mask>,
        /// The index plan serving this view's rows; present only while the
        /// selection is exactly the plan's run.
        serve: Option<FileServePlan>,
    },
}

impl QuadsSource {
    /// The visible base rows.
    pub(crate) fn view_selection(&self) -> &ViewSelection {
        match self {
            QuadsSource::InMemory { selection, .. } => selection,
            #[cfg(feature = "file-io")]
            QuadsSource::File { selection, .. } => selection,
        }
    }

    /// Whether every base row is visible: no pushed-down filter and an
    /// all-rows selection.
    pub(crate) fn is_unrefined(&self) -> bool {
        !self.has_filter() && self.view_selection().is_all()
    }

    /// The number of base rows.
    pub(crate) fn base_len(&self) -> usize {
        match self {
            QuadsSource::InMemory { base, .. } => base.len(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.row_count() as usize,
        }
    }

    /// The tombstone mask, `None` until a delete.
    pub(crate) fn deleted(&self) -> Option<&Mask> {
        match self {
            QuadsSource::InMemory { deleted, .. } => deleted.as_ref(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { deleted, .. } => deleted.as_ref(),
        }
    }

    /// The base's struct dtype.
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

    /// Whether the base's rows are in global `(s, p, o, g)` order.
    pub(crate) fn subject_sorted(&self) -> bool {
        match self {
            QuadsSource::InMemory { base, .. } => subject_sorted(base),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.quads_sorted(),
        }
    }

    /// Whether a pushed-down file filter is still to be evaluated.
    pub(crate) fn has_filter(&self) -> bool {
        match self {
            QuadsSource::InMemory { .. } => false,
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => filter.is_some(),
        }
    }

    /// The file handle of a file-backed source.
    #[cfg(feature = "file-io")]
    pub(crate) fn file(&self) -> Option<&NativeStoreFile> {
        match self {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { file, .. } => Some(file),
        }
    }

    /// The pushed-down filter of a file-backed source.
    #[cfg(feature = "file-io")]
    pub(crate) fn filter(&self) -> Option<&Expression> {
        match self {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { filter, .. } => filter.as_ref(),
        }
    }

    /// The index child whose plan serves this view, if any.
    pub(crate) fn served_component(&self) -> Option<&'static str> {
        match self {
            QuadsSource::InMemory { serve, .. } => serve.as_ref().map(|plan| plan.component()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { serve, .. } => serve.as_ref().map(|plan| plan.component()),
        }
    }

    /// The live base rows the selection covers, when known without I/O: an
    /// exact selection counts directly; a pending one answers from its run
    /// width (in memory also with tombstones, by reading the run). `None` for
    /// an unread file-child run with tombstones, or one not yet located.
    pub(crate) fn live_len_if_known(&self) -> Option<usize> {
        match self {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                ..
            } => match (selection, deleted) {
                (ViewSelection::Pending(lazy), Some(deleted)) => lazy
                    .materialized()
                    .ok()
                    .map(|ids| ids.iter().filter(|&&i| !deleted.value(i as usize)).count()),
                _ => selection.len_if_known(deleted.as_ref(), base.len()),
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                selection,
                deleted,
                serve,
                ..
            } => selection
                .len_if_known(deleted.as_ref(), file.row_count() as usize)
                .or_else(|| match (selection, deleted, serve) {
                    (ViewSelection::Pending(_), None, Some(plan)) => plan
                        .row_range()
                        .map(|r| usize::try_from(r.end - r.start).unwrap_or(usize::MAX)),
                    _ => None,
                }),
        }
    }

    /// The exact selection, running a pending resolution's deferred ids.
    pub(crate) async fn materialized_selection(&self) -> Result<RowSelection> {
        self.view_selection().materialized_async().await
    }

    /// This in-memory source under `selection`, served by `serve`.
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

    /// This file source under `filter` and `selection`, served by `serve`.
    #[cfg(feature = "file-io")]
    pub(crate) fn file_with(
        &self,
        filter: Option<Expression>,
        selection: ViewSelection,
        serve: Option<FileServePlan>,
    ) -> Self {
        let QuadsSource::File {
            path,
            dict_max_resident_bytes,
            file,
            deleted,
            ..
        } = self
        else {
            unreachable!("file_with is only called on a file source");
        };
        QuadsSource::File {
            path: path.clone(),
            dict_max_resident_bytes: *dict_max_resident_bytes,
            file: Arc::clone(file),
            filter,
            selection,
            deleted: deleted.clone(),
            serve,
        }
    }

    /// This source under `selection`, with no serve plan; a file source keeps
    /// its filter.
    pub(crate) fn with_selection(&self, selection: ViewSelection) -> Self {
        match self {
            QuadsSource::InMemory { .. } => self.in_memory_with(selection, None),
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => self.file_with(filter.clone(), selection, None),
        }
    }

    /// This source with its pushed-down filter dropped.
    pub(crate) fn without_filter(self) -> Self {
        match self {
            QuadsSource::InMemory { .. } => self,
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                selection,
                deleted,
                serve,
                ..
            } => QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter: None,
                selection,
                deleted,
                serve,
            },
        }
    }

    /// This source with `doomed` folded into its tombstones; the serve plan
    /// is dropped.
    pub(crate) fn with_tombstones(&self, doomed: Mask) -> Self {
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

    /// This source selecting no row, with no serve plan and (in memory) no
    /// index components.
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

/// Rows appended after construction, held apart from the base so its row
/// ids, indexes, tombstones and file handle are never rewritten. Patterns
/// match the tail independently of the base, by a scan over its selected
/// rows.
///
/// `rows` is one StructArray in `layout`: the store's own layout, except
/// under the Dictionary layout, where the tail holds Default-layout
/// N-Triples strings (an appended term has no code in the sorted
/// dictionary). Appends accrete as chunks and are flattened per
/// [`TAIL_FLATTEN_FLOOR`]/[`TAIL_MAX_CHUNKS`]; `compact_with_indexes` folds
/// the tail into the base. `selection` and `deleted` are in tail-local row
/// ids (`0..rows.len()`), and every read applies both.
#[derive(Clone)]
pub(crate) struct Tail {
    pub(crate) rows: ArrayRef,
    pub(crate) selection: RowSelection,
    /// Tail rows deleted since they were appended, one bit per tail row;
    /// `None` until a delete.
    pub(crate) deleted: Option<Mask>,
    /// The layout `rows` are stored in.
    pub(crate) layout: ResolvedLayout,
}

/// Appended chunks are flattened into the tail's first chunk once their rows
/// reach `max(first chunk rows, TAIL_FLATTEN_FLOOR)`.
pub(crate) const TAIL_FLATTEN_FLOOR: usize = 1_024;
/// Appended chunks are flattened once the tail holds more than this many
/// chunks, whatever their row counts.
pub(crate) const TAIL_MAX_CHUNKS: usize = 64;

impl Tail {
    /// The layout a tail under `base` stores its rows in.
    pub(crate) fn layout_for(base: &ResolvedLayout) -> ResolvedLayout {
        match base {
            ResolvedLayout::Dictionary(_) => ResolvedLayout::Default,
            other => other.clone(),
        }
    }

    /// A tail of `rows`, every row visible.
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

    /// The number of visible, live tail rows.
    pub(crate) fn live_len(&self) -> usize {
        self.selection
            .live_len(self.deleted.as_ref(), self.rows.len())
    }

    /// The `limit` live rows after skipping `offset`, in tail order.
    pub(crate) fn window(&self, offset: usize, limit: usize) -> Tail {
        self.with_selection(self.selection.window(
            offset,
            limit,
            self.deleted.as_ref(),
            self.rows.len(),
        ))
    }

    /// This tail with the rows `doomed` selects tombstoned.
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

    /// The visible live rows, in tail order.
    pub(crate) fn live_rows(&self) -> Result<ArrayRef> {
        gather_live(&self.rows, &self.selection, self.deleted.as_ref(), None)
    }

    /// The visible live rows decoded to `T`; a gather failure is one `Err`
    /// element.
    pub(crate) fn decode<T: ChunkDecode>(&self) -> Vec<Result<T>> {
        match self.live_rows() {
            Ok(rows) => T::decode(&self.layout, &rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// The visible live rows as raw quads.
    pub(crate) fn raw_quads(&self) -> Result<Vec<RawQuad>> {
        self.layout.raw_quads(&self.live_rows()?)
    }

    /// A tail of this tail's live rows followed by `fresh`, every row
    /// visible: `fresh` joins as one more chunk, and the accreted chunks are
    /// flattened into the first once they reach the flatten policy. Tail ids
    /// are renumbered; views of the previous store keep the previous tail.
    pub(crate) fn append(&self, fresh: ArrayRef) -> Result<Tail> {
        let old = self.live_rows()?;
        let dtype = old.dtype().clone();
        let mut chunks = match old.clone().try_downcast::<Chunked>() {
            Ok(chunked) => chunked.chunks(),
            Err(_) => vec![old],
        };
        let flat_len = chunks[0].len();
        let accreted: usize = chunks[1..].iter().map(|c| c.len()).sum::<usize>() + fresh.len();
        chunks.push(fresh);
        let n_chunks = chunks.len();
        let combined = ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        let rows = if accreted >= flat_len.max(TAIL_FLATTEN_FLOOR) || n_chunks > TAIL_MAX_CHUNKS {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            combined
                .execute::<RecursiveCanonical>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?
                .0
                .into_array()
        } else {
            combined
        };
        Ok(Tail::new(rows, self.layout.clone()))
    }
}

/// `existing` with `doomed` set.
fn union_deleted(existing: Option<&Mask>, doomed: Mask) -> Mask {
    match existing {
        Some(existing) => existing | &doomed,
        None => doomed,
    }
}
