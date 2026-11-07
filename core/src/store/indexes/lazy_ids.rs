//! Lazy row ids: the exact base row ids of an index resolution, run by the
//! first consumer that needs them ([`LazyRowIds`]); the in-memory run a
//! served view's plan reads beside them ([`ComponentRun`]); and the run that
//! keeps only its bound graph's rows, checked inside its component
//! ([`CheckedRun`]).

#[cfg(feature = "file-io")]
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use super::components::{self, run_order_row_ids, sorted_row_ids};
use super::get_or_try_init;
use super::graph_check::GraphCheck;
#[cfg(feature = "file-io")]
use super::rid_reads::{read_located_rids, scan_index_row_ids};
use crate::error::Result;
use crate::observe::observe;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::read::live::Live;
use crate::store::schema::RowId;
use crate::store::view::selection::window_rows;

/// The exact base row ids of an index resolution whose consumer may not need
/// them (a served one, or a located run that is only counted or windowed):
/// the *recipe*, run by the first consumer that needs the selection, its
/// result in a cell shared by every clone of the view that carries it (a
/// narrowed view's excepted: [`Unsorted`](Self::Unsorted)). A serve plan
/// reads without the ids, which on file cost a whole extra scan of the index
/// child, and a located run's width counts without them.
///
/// Two consumers racing on first need may both run the recipe; the source is
/// immutable, so they compute the same ids, whichever stores first wins and
/// both return the stored buffer. No lock is held across the computation.
#[derive(Clone)]
pub(crate) enum LazyRowIds {
    /// In-memory: a component's matched run ([`ComponentRun`]), decoded and
    /// sorted on demand, shared with every clone and, for a served view, its
    /// serve plan.
    Component(Arc<ComponentRun>),
    /// In-memory: a component's matched run that keeps only its bound
    /// graph's rows, checked inside the component ([`CheckedRun`]), its
    /// count and ids found on demand and shared with every clone.
    Checked(Arc<CheckedRun>),
    /// A narrowed served view's ids, in any order (its index's), which leave
    /// out every tombstoned row (a narrowing lists only live rows), so they
    /// count as they are; sorted into base order only on first need, into
    /// `sorted`: a cache each clone made before that copies rather than
    /// shares, as base-order consumers of a narrowed view are rare, so a
    /// narrowing allocates none.
    Unsorted {
        ids: Buffer<RowId>,
        sorted: OnceLock<Buffer<RowId>>,
    },
    /// File-backed: the rid-only pushed-down scan of the index child
    /// ([`scan_index_row_ids`]) the eager path would have run at match time,
    /// its ids kept in `sorted` for every clone. The child binds through its
    /// file's memo under its own name, so the deferred scan binds with the
    /// same identity every plan and eager scan of this component uses (see
    /// `BoundExprMemo`). `located` is the run of child rows the constraints
    /// select, when the resolver located it: exactly the rows the scan keeps,
    /// so its width is the ids' count. It answers
    /// [`len_if_known`](LazyRowIds::len_if_known) and nothing else: a serving
    /// child's rids follow its own sort order, so no window of the run is a
    /// window of its ids.
    #[cfg(feature = "file-io")]
    IndexChild {
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        located: Option<Range<u64>>,
        sorted: Arc<OnceLock<Buffer<RowId>>>,
    },
    /// File-backed: a located run of an index child's rid column, read on
    /// first need through [`read_located_rids`] and kept in `sorted` for
    /// every clone; its width is known up front, so a count reads none of
    /// it. `ascending` when the run's rids are already in row order, which a
    /// window of it reads by position.
    #[cfg(feature = "file-io")]
    LocatedRun {
        file: Arc<NativeStoreFile>,
        child: ChildReader,
        range: Range<u64>,
        ascending: bool,
        sorted: Arc<OnceLock<Buffer<RowId>>>,
    },
}

/// An in-memory component's matched run of base row ids, which the view's
/// lazy ids and its serve plan share: the run's slice of the component's rid
/// column, and the ids it decodes to in base order and in the component's
/// own order, each computed once. A served read takes the run order; a
/// consumer that needs base order sorts once, from the run order when that
/// is decoded.
///
pub(crate) struct ComponentRun {
    /// The run's slice of the component's rid column.
    rids: ArrayRef,
    /// Whether the rids are already in row id order, as a reference
    /// component's are within one value (its rows are ordered by
    /// `(val, rid)`), so a window of the run can be cut before anything is
    /// decoded.
    ascending: bool,
    /// The ids in base order.
    sorted: OnceLock<Buffer<RowId>>,
    /// The ids in the component's order.
    run_order: OnceLock<Buffer<RowId>>,
}

impl ComponentRun {
    /// The run whose rid-column slice is `rids`, `ascending` when those rids
    /// are already in row id order.
    pub(crate) fn new(rids: ArrayRef, ascending: bool) -> Self {
        Self {
            rids,
            ascending,
            sorted: OnceLock::new(),
            run_order: OnceLock::new(),
        }
    }

    /// The run's ids in the component's order, decoded on the first call
    /// ([`run_order_row_ids`]) and kept: the one decode a served view's reads
    /// and its lazy ids share.
    pub(crate) fn run_order(&self) -> Result<Buffer<RowId>> {
        get_or_try_init(&self.run_order, || run_order_row_ids(self.rids.clone()))
    }
}

/// An in-memory component's matched run that keeps only the rows of its
/// bound graph, checked inside the component ([`GraphCheck`]): the run's
/// slice of the component's rid column, and, each found once and kept, the
/// matches' count and their ids in the component's order and in base order.
/// No serve plan reads one: it would over-cover the run.
pub(crate) struct CheckedRun {
    /// The run's slice of the component's rid column.
    rids: ArrayRef,
    /// Whether the rids are already in row id order (a reference
    /// component's), so a window of the matches is cut in run order.
    ascending: bool,
    check: GraphCheck,
    count: OnceLock<usize>,
    run_order: OnceLock<Buffer<RowId>>,
    sorted: OnceLock<Buffer<RowId>>,
}

impl CheckedRun {
    /// The run whose rid-column slice is `rids` (`ascending` when those rids
    /// are in row id order), keeping the rows of the graph `check` checks.
    pub(crate) fn new(rids: ArrayRef, ascending: bool, check: GraphCheck) -> Self {
        Self {
            rids,
            ascending,
            check,
            count: OnceLock::new(),
            run_order: OnceLock::new(),
            sorted: OnceLock::new(),
        }
    }

    /// How many rows match, when known without counting: once counted, or
    /// once the ids are found.
    fn known_len(&self) -> Option<usize> {
        self.count
            .get()
            .copied()
            .or_else(|| self.sorted.get().or(self.run_order.get()).map(Buffer::len))
    }

    /// How many rows match: known, else counted zone by zone
    /// ([`GraphCheck::count`]) and kept.
    fn count(&self) -> Result<usize> {
        match self.known_len() {
            Some(known) => Ok(known),
            None => get_or_try_init(&self.count, || self.check.count()),
        }
    }

    /// `min(matches, limit)`: from the count when known, else the zones
    /// counted only until `limit` matches are ([`GraphCheck::count_capped`]).
    fn count_capped(&self, limit: usize) -> Result<usize> {
        Ok(match self.known_len() {
            Some(known) => known,
            None => self.check.count_capped(limit)?,
        }
        .min(limit))
    }

    /// The matches' ids in the component's order, found on the first call
    /// ([`GraphCheck::ids`]) and kept.
    fn run_order(&self) -> Result<Buffer<RowId>> {
        get_or_try_init(&self.run_order, || self.check.ids(&self.rids))
    }

    /// The matches `offset..offset + limit` of a run whose rids ascend, in
    /// base row order, beside the matches as far as they are known
    /// ([`Live`]): cut from the ids when they are found, else read from the
    /// zones the window spans ([`GraphCheck::window`]).
    fn window(&self, offset: usize, limit: usize) -> Result<(Buffer<RowId>, Live)> {
        match self.sorted.get().or(self.run_order.get()) {
            Some(ids) => {
                let rows = window_rows(offset, limit, ids.len() as u64)
                    .map_or(0..0, |rows| rows.start as usize..rows.end as usize);
                Ok((ids.slice(rows), Live::Exact(ids.len())))
            }
            None => self.check.window(&self.rids, offset, limit),
        }
    }
}

impl LazyRowIds {
    /// Test-only hook: whether the ids have been computed, so a test can pin
    /// a read answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.sorted_cell().get().is_some()
    }

    /// Where the ids sorted into base order are kept.
    fn sorted_cell(&self) -> &OnceLock<Buffer<RowId>> {
        match self {
            LazyRowIds::Component(run) => &run.sorted,
            LazyRowIds::Checked(run) => &run.sorted,
            LazyRowIds::Unsorted { sorted, .. } => sorted,
            #[cfg(feature = "file-io")]
            LazyRowIds::IndexChild { sorted, .. } | LazyRowIds::LocatedRun { sorted, .. } => sorted,
        }
    }

    /// Lazy ids of a narrowed served view: its live rows' `ids` in any order,
    /// sorted into base order on first need.
    pub(crate) fn from_unsorted_ids(ids: Buffer<RowId>) -> Self {
        LazyRowIds::Unsorted {
            ids,
            sorted: OnceLock::new(),
        }
    }

    /// Lazy ids scanned from a file's index child on first need, counted by
    /// `located`, the constraints' run of child rows when the resolver
    /// located it.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_index_child_scan(
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        located: Option<Range<u64>>,
    ) -> Self {
        LazyRowIds::IndexChild {
            child,
            constraints,
            located,
            sorted: Arc::default(),
        }
    }

    /// Lazy ids over the located run `range` of a file index `child`;
    /// `ascending` when the run's rids are already in row order, as a
    /// [`window`](Self::window) of them requires (a reference child's within
    /// one value; a copy child's follow its own sort order, never taken to
    /// ascend).
    #[cfg(feature = "file-io")]
    pub(crate) fn from_located_run(
        file: Arc<NativeStoreFile>,
        child: ChildReader,
        range: Range<u64>,
        ascending: bool,
    ) -> Self {
        LazyRowIds::LocatedRun {
            file,
            child,
            range,
            ascending,
            sorted: Arc::default(),
        }
    }

    /// The ids of rows `offset..offset + limit` of a run whose rids ascend,
    /// in base row order, read from the window's own rows and no others: an
    /// in-memory run's cut out of its rid column before they are decoded, a
    /// located file run's read from those rows of its child. A reference
    /// child's rows are ordered by `(val, rid)` (the order this crate's
    /// writers emit, docs/file-format.md §6), so within one value the rids
    /// ascend: row `k` of the run holds the run's `k`-th smallest rid, and a
    /// window of the run's rows is the same window of its ids in base order.
    /// A deep page costs its limit, and an empty window (no limit, or an
    /// offset at or past the run's end) reads nothing. `None` for any other
    /// source.
    pub(crate) async fn window(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Buffer<RowId>>> {
        let ids = match self {
            LazyRowIds::Component(run) if run.ascending => {
                match window_rows(offset, limit, run.rids.len() as u64) {
                    Some(rows) => {
                        sorted_row_ids(run.rids.slice(rows.start as usize..rows.end as usize)?)?
                    }
                    None => Buffer::empty(),
                }
            }
            // A checked run counts the zones before the window and reads
            // only the zones it spans.
            LazyRowIds::Checked(run) if run.ascending => run.window(offset, limit)?.0,
            #[cfg(feature = "file-io")]
            LazyRowIds::LocatedRun {
                file,
                child,
                range,
                ascending: true,
                ..
            } => match window_rows(offset, limit, range.end - range.start) {
                Some(rows) => {
                    let rows = range.start + rows.start..range.start + rows.end;
                    read_located_rids(file, child, rows).await?
                }
                None => Buffer::empty(),
            },
            _ => return Ok(None),
        };
        Ok(Some(ids))
    }

    /// [`window`](Self::window), beside the rows the ids cover as far as
    /// they are known ([`Live`]): every one, the run's width
    /// ([`len_if_known`](Self::len_if_known)), or for a run checked against
    /// its graph its matches, counted only until the window fills
    /// ([`CheckedRun::window`]). `None` where `window` is, or where the rows
    /// are not known without I/O.
    pub(crate) async fn window_with_live(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<(Buffer<RowId>, Live)>> {
        if let LazyRowIds::Checked(run) = self {
            return run
                .ascending
                .then(|| run.window(offset, limit))
                .transpose();
        }
        let Some(live) = self.len_if_known() else {
            return Ok(None);
        };
        Ok(self
            .window(offset, limit)
            .await?
            .map(|ids| (ids, Live::Exact(live))))
    }

    /// `min(rows, limit)` exactly, for a run checked against its graph: its
    /// zones counted only until `limit` matches are, unless its count is
    /// known ([`CheckedRun::count_capped`]). `None` for any other ids, whose
    /// capped count is their count ([`count`](Self::count)).
    pub(crate) fn count_capped(&self, limit: usize) -> Result<Option<usize>> {
        match self {
            LazyRowIds::Checked(run) => run.count_capped(limit).map(Some),
            _ => Ok(None),
        }
    }

    /// How many rows the ids cover, counted without I/O: what
    /// [`len_if_known`](Self::len_if_known) knows, and a run checked against
    /// its graph's matches, counted zone by zone and kept
    /// ([`CheckedRun::count`]). `None` for a file child's ids not read yet.
    pub(crate) fn count(&self) -> Result<Option<usize>> {
        match self {
            LazyRowIds::Checked(run) => run.count().map(Some),
            _ => Ok(self.len_if_known()),
        }
    }

    /// A narrowed served view's ids as they were handed over, in the index's
    /// order, every one live (a window's, keep's, partition's or chained
    /// match's): what a read in the index's order gathers at, with no run
    /// decoded. `None` for any other source.
    pub(crate) fn live_index_order(&self) -> Option<&Buffer<RowId>> {
        match self {
            LazyRowIds::Unsorted { ids, .. } => Some(ids),
            _ => None,
        }
    }

    /// How many rows the ids cover, when known without reading or counting
    /// anything: an in-memory run knows its width up front (so a count on a
    /// served match never decodes), and so does a located file run, whether
    /// its ids come from the run itself or from the child scan it bounds; a
    /// run checked against its graph once its matches are counted or found
    /// ([`count`](Self::count) counts them), and an unlocated child scan
    /// once its ids are read.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        match self {
            LazyRowIds::Component(run) => Some(run.rids.len()),
            LazyRowIds::Checked(run) => run.known_len(),
            LazyRowIds::Unsorted { ids, .. } => Some(ids.len()),
            #[cfg(feature = "file-io")]
            LazyRowIds::IndexChild {
                located, sorted, ..
            } => sorted
                .get()
                .map(Buffer::len)
                .or_else(|| located.as_ref().map(|run| (run.end - run.start) as usize)),
            #[cfg(feature = "file-io")]
            LazyRowIds::LocatedRun { range, .. } => Some((range.end - range.start) as usize),
        }
    }

    /// How many of the ids are live under the tombstones `deleted`, counted
    /// without I/O and never sorted: a narrowed view's are all live, so as
    /// many as they are; an in-memory run's are read against the mask in
    /// base order when that is kept, else in its component's order, decoded
    /// once and kept ([`ComponentRun::run_order`]); a file child's once read.
    /// `None` for a file child's ids not read yet.
    pub(crate) fn live_len_without_io(&self, deleted: &Mask) -> Result<Option<usize>> {
        let ids = match self {
            LazyRowIds::Unsorted { ids, .. } => return Ok(Some(ids.len())),
            LazyRowIds::Component(run) => match run.sorted.get() {
                Some(sorted) => sorted.clone(),
                None => run.run_order()?,
            },
            LazyRowIds::Checked(run) => match run.sorted.get() {
                Some(sorted) => sorted.clone(),
                None => run.run_order()?,
            },
            #[cfg(feature = "file-io")]
            LazyRowIds::IndexChild { sorted, .. } | LazyRowIds::LocatedRun { sorted, .. } => {
                match sorted.get() {
                    Some(sorted) => sorted.clone(),
                    None => return Ok(None),
                }
            }
        };
        observe!(LiveCount);
        Ok(Some(
            ids.iter()
                .filter(|&&id| !deleted.value(id as usize))
                .count(),
        ))
    }

    /// The ids, computed and cached on the first call; the awaiting form,
    /// which also runs a file child's deferred read.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized_async(&self) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.sorted_cell().get() {
            return Ok(ids.clone());
        }
        let ids = match self {
            LazyRowIds::Component(_) | LazyRowIds::Checked(_) | LazyRowIds::Unsorted { .. } => {
                return self.materialized();
            }
            LazyRowIds::IndexChild {
                child, constraints, ..
            } => scan_index_row_ids(child, constraints).await?,
            LazyRowIds::LocatedRun {
                file, child, range, ..
            } => read_located_rids(file, child, range.clone()).await?,
        };
        observe!(IdsSorted(ids.len()));
        Ok(self.sorted_cell().get_or_init(|| ids).clone())
    }

    /// The ids, computed and cached on the first call; the synchronous form,
    /// for in-memory sources (a file child's ids take I/O, and every consumer
    /// of a file view's selection is async: see
    /// [`materialized_async`](Self::materialized_async)). A file child's ids
    /// that `materialized_async` has already read come from the cache, which
    /// is how the file match folds them; unread ones never reach here.
    pub(crate) fn materialized(&self) -> Result<Buffer<RowId>> {
        get_or_try_init(self.sorted_cell(), || {
            let ids = match self {
                // A run already decoded in its component's order sorts a copy
                // of that, rather than decoding the rids again.
                LazyRowIds::Component(run) => match run.run_order.get() {
                    Some(run_order) => components::sorted_copy(run_order)?,
                    None => sorted_row_ids(run.rids.clone())?,
                },
                // A checked run's matching ids are kept as they are where
                // they ascend.
                LazyRowIds::Checked(run) => {
                    let ids = run.run_order()?;
                    match run.ascending || ids.is_sorted() {
                        true => ids,
                        false => components::sorted_copy(&ids)?,
                    }
                }
                LazyRowIds::Unsorted { ids, .. } => components::sorted_copy(ids)?,
                #[cfg(feature = "file-io")]
                LazyRowIds::IndexChild { .. } | LazyRowIds::LocatedRun { .. } => {
                    unreachable!(
                        "an in-memory view only ever carries a component's pending ids, and \
                         the file match reads a file run's ids before it folds them"
                    )
                }
            };
            observe!(IdsSorted(ids.len()));
            Ok(ids)
        })
    }
}
