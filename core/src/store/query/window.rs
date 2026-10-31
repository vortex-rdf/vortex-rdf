//! Windows: a view's rows after skipping `offset` and taking at most
//! `limit`, the `LIMIT`/`OFFSET` a query engine pushes below a pattern. A
//! window is a derived view that composes with
//! [`match_pattern`](VortexRdfStore::match_pattern) and the keeps in either
//! order and reads through the same paths (`size`, `code_columns_gathered`,
//! `quads`). A window that cannot cut a row is the view as it is; any other
//! over a file view with a pending filter evaluates its splits in file order
//! and stops at the first split that fills it, and one that finds it cuts no
//! row keeps the view, the filter's evaluation with it.

#[cfg(feature = "file-io")]
use std::sync::Arc;

use vortex_buffer::Buffer;

use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::store::VortexRdfStore;
use crate::store::indexes::LazyRowIds;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_filter, read_plan};
use crate::store::view::selection::{RowSelection, ViewSelection, window_rows};
#[cfg(feature = "file-io")]
use crate::store::{FileBase, FileFilter};

impl VortexRdfStore {
    /// The view's rows after skipping `offset` and taking at most `limit`.
    /// Available on every layout.
    ///
    /// A window that cannot cut a row is the view as it is, its serve plan,
    /// pending filter and row order included: one with no `offset` whose
    /// `limit` is at least the view's row count, or a bound on it, known
    /// without reading (the `rows.upper_bound` of
    /// [`view_statistics`](Self::view_statistics)). A file view whose reads
    /// take a pending filter of code equalities by its masks first is never
    /// the view as it is: its window evaluates the filter whatever the bound,
    /// as below, and so finds whether it cuts.
    ///
    /// A window of a view a by-copy index serves is served too: rows
    /// `offset..offset + limit` of the view in its index's order, then the
    /// tail's, read off the index as the view's are. A served file run whose
    /// width is unknown (a string layout's, or one a residual term filters)
    /// is read once by row index first, kept on the view for every narrowing
    /// of it, and windowed whether or not the limit cuts. Any other window is
    /// a position range over base order: base rows in base row order, then
    /// the tail's, with no serve plan. Either order is the same on every
    /// call, so paging by repeated windows reads each row once. A pending
    /// file filter is resolved only as far as the window reaches (its splits
    /// evaluated in file order, stopping at the first that fills the window),
    /// so `LIMIT` over a filtered scan costs the matching prefix of the file.
    /// A window with no `offset` whose filter matches fewer rows than its
    /// limit finds that it cuts no base row: the view keeps its rows, known
    /// exactly from then on, its reads and counts taking them from the
    /// filter's evaluation, which is never run again, and its tail is
    /// windowed to the rest of the limit.
    pub async fn window(&self, offset: usize, limit: usize) -> Result<Self> {
        Ok(self
            .window_cutting(offset, limit)
            .await?
            .unwrap_or_else(|| self.clone()))
    }

    /// [`window`](Self::window), `None` when the window cannot cut a row and
    /// so is the view as it is: a caller that holds the view keeps it, with
    /// no clone.
    pub(in crate::store) async fn window_cutting(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let t = debug::timer();
        let WindowCheck::MayCut { base_live } = self.window_check(offset, limit)? else {
            debug::step!(t, "[window] limit {limit} cuts no row: the view as it is");
            return Ok(None);
        };
        // A served view's window is served too, in its index's order.
        if let Some(windowed) = self.served_window_in_memory(offset, limit)? {
            observe!(Window);
            debug::step!(t, "[window] offset {offset} limit {limit} of a served run");
            return Ok(Some(windowed));
        }
        #[cfg(feature = "file-io")]
        if let Some(windowed) = self.served_window_file(offset, limit).await? {
            observe!(Window);
            debug::step!(
                t,
                "[window] offset {offset} limit {limit} of a served file run"
            );
            return Ok(Some(windowed));
        }
        observe!(Window);
        #[cfg(feature = "file-io")]
        if offset == 0
            && let Some(windowed) = self.filter_window(limit).await?
        {
            debug::step!(t, "[window] limit {limit} over the filter's masks");
            return Ok(Some(windowed));
        }
        // A located run held pending for this window reads only the window's
        // own rows, and its width is the base's live size; any other view
        // windows its materialized selection.
        let (windowed, base_live) = match self.quads.located_window(offset, limit).await? {
            Some(located) => located,
            None => {
                let selection = self.quads.materialized_selection().await?;
                self.base_window(&selection, offset, limit, base_live)
                    .await?
            }
        };
        let base_taken = windowed.len(self.quads.base_len());
        // The window is a position range over base order with the filter it
        // resolved applied, so the view carries exact ids, no filter and no
        // plan, only the codes the filter pinned, for its reads by id.
        let quads = self
            .quads
            .with_selection(ViewSelection::Exact(windowed))
            .with_filter_resolved();
        // Whatever the base could not fill falls to the tail: the offset it
        // did not consume, the limit it did not take.
        let tail = self.tail.as_ref().map(|tail| {
            tail.window(
                offset.saturating_sub(base_live),
                limit.saturating_sub(base_taken),
            )
        });
        debug::step!(
            t,
            "[window] offset {offset} limit {limit}: {base_taken} base rows"
        );
        Ok(Some(self.derived(quads, tail)))
    }

    /// The window of a view an in-memory plan serves, itself served: rows
    /// `offset..offset + limit` of the view in its index's order, a part of
    /// the plan's range ([`narrowed`](crate::store::indexes::InMemoryServePlan::narrowed))
    /// or a slice of the rows a keep kept, their row ids kept unsorted on
    /// the window
    /// ([`LazyRowIds::from_unsorted_ids`]); under tombstones the cut counts
    /// live rows. The tail is windowed with what the base did not fill, as
    /// [`window_cutting`](Self::window_cutting) windows it. A window that
    /// holds no base row selects none, with no plan. `None` for a view no
    /// in-memory plan serves.
    pub(in crate::store) fn served_window_in_memory(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        let range = plan.range();
        let (window, base_live) = match (plan.kept(), &self.quads.deleted) {
            // A kept view's window: a slice of its live kept rows.
            (Some(kept), deleted) => {
                let rows = kept.live(deleted.as_ref());
                let base_live = rows.len();
                let window = window_rows(offset, limit, base_live as u64).map(|cut| {
                    let rows = rows.slice(cut.start as usize..cut.end as usize);
                    let ids = rows.rids.clone();
                    (plan.with_rows(rows), ids)
                });
                (window, base_live)
            }
            (None, None) => {
                let base_live = range.len();
                let window = match window_rows(offset, limit, base_live as u64) {
                    Some(rows) => {
                        let (start, end) = (rows.start as usize, rows.end as usize);
                        let narrowed = plan.narrowed(range.start + start..range.start + end);
                        let ids = narrowed.live_rids(None)?;
                        Some((narrowed, ids))
                    }
                    None => None,
                };
                (window, base_live)
            }
            (None, Some(deleted)) => {
                let (positions, rids) = plan.live_rows(Some(deleted))?;
                let base_live = positions.len();
                let window = window_rows(offset, limit, base_live as u64).map(|rows| {
                    let (start, end) = (rows.start as usize, rows.end as usize);
                    let narrowed = plan.narrowed(positions[start]..positions[end - 1] + 1);
                    (narrowed, Buffer::from(rids[start..end].to_vec()))
                });
                (window, base_live)
            }
        };
        let base_taken = window.as_ref().map_or(0, |(_, ids)| ids.len());
        let quads = match window {
            Some((narrowed, ids)) => {
                // The ids leave out every tombstone: the window took live rows.
                let ids = LazyRowIds::from_unsorted_ids(ids, true);
                self.quads
                    .in_memory_with(memory, ViewSelection::pending(ids), Some(narrowed))
            }
            None => self
                .quads
                .with_selection(ViewSelection::Exact(RowSelection::empty())),
        };
        let tail = self.tail.as_ref().map(|tail| {
            tail.window(
                offset.saturating_sub(base_live),
                limit.saturating_sub(base_taken),
            )
        });
        Ok(Some(self.derived(quads, tail)))
    }

    /// The window of a view a file plan serves over a located run, itself
    /// served: rows `offset..offset + limit` of the view in its index's
    /// order, a part of the plan's located range
    /// ([`narrowed`](crate::store::indexes::FileServePlan::narrowed)), whose
    /// ids read on first need from those rows of the child's rid column
    /// ([`LazyRowIds::from_located_run`]), or a slice of the rows a keep
    /// kept, their ids kept unsorted. Under tombstones the cut counts live
    /// rows, the range's rid column read. The tail is windowed with what the
    /// base did not fill, as [`window_cutting`](Self::window_cutting)
    /// windows it. An unlocated run (a string layout's, or one a residual
    /// term filters) is located first
    /// ([`located`](crate::store::indexes::FileServePlan::located)): read
    /// once by row index, a located range when its rows are contiguous in
    /// the child, else kept rows. A window that holds no base row selects none,
    /// with no plan. `None` for a view no file plan serves.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn served_window_file(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        let Some(plan) = &base.serve else {
            return Ok(None);
        };
        let plan = &plan.located().await?;
        let deleted = self.quads.deleted.as_ref();
        // The window's plan and lazy ids, or `None` for no base row, beside
        // the base's live rows.
        let (window, base_live) = match (plan.kept(), plan.located_range(), deleted) {
            (None, Some(range), None) => {
                let base_live = (range.end - range.start) as usize;
                let window = window_rows(offset, limit, base_live as u64).map(|cut| {
                    let cut = range.start + cut.start..range.start + cut.end;
                    let ids = LazyRowIds::from_located_run(
                        Arc::clone(&base.file),
                        plan.child().clone(),
                        cut.clone(),
                    );
                    (plan.narrowed(cut), ids)
                });
                (window, base_live)
            }
            (kept, located, deleted) => {
                // The kept rows, or a located range's live rows.
                let rows = plan.live_rows(&base.file, deleted).await?;
                let base_live = rows.len();
                let window = window_rows(offset, limit, base_live as u64).map(|cut| {
                    let rows = rows.slice(cut.start as usize..cut.end as usize);
                    // The ids leave out every tombstone: the window took live
                    // rows.
                    let ids = LazyRowIds::from_unsorted_ids(rows.rids.clone(), true);
                    let narrowed = match (kept, located) {
                        (None, Some(_)) => {
                            let (first, last) = (rows.positions[0], rows.positions[rows.len() - 1]);
                            plan.narrowed(first..last + 1)
                        }
                        _ => plan.with_rows(rows),
                    };
                    (narrowed, ids)
                });
                (window, base_live)
            }
        };
        let base_taken = window
            .as_ref()
            .map_or(0, |(_, ids)| ids.len_if_known().unwrap_or(0));
        let quads = match window {
            Some((narrowed, ids)) => self.quads.file_with(
                base,
                base.filter.clone(),
                ViewSelection::pending(ids),
                Some(narrowed),
            ),
            None => self
                .quads
                .with_selection(ViewSelection::Exact(RowSelection::empty())),
        };
        let tail = self.tail.as_ref().map(|tail| {
            tail.window(
                offset.saturating_sub(base_live),
                limit.saturating_sub(base_taken),
            )
        });
        Ok(Some(self.derived(quads, tail)))
    }

    /// Whether a window of `limit` rows after `offset` can cut a row of this
    /// view, known without reading one. It cannot with no offset and a limit
    /// at least the `rows.upper_bound` of
    /// [`view_statistics`](Self::view_statistics), the base's rows plus the
    /// tail's live rows, both from [`QuadsSource::base_rows`](crate::store::QuadsSource::base_rows):
    /// the selected rows (a pending run's width, or every base row while that
    /// is unknown), tested first, which decodes nothing, then, only under
    /// tombstones and when those do not fit, their live rows where known
    /// without I/O, which in memory decodes a pending run's ids, counted once
    /// for the window to take. A view whose window kept its filter's masks
    /// knows the base's live rows exactly from them. A view pending without
    /// a serve plan always may cut: its window takes its rows into exact
    /// ids, the rows and order of the plain match, and where
    /// [`QuadsSource::located_window`](crate::store::QuadsSource::located_window)
    /// answers (an in-memory residual compare left pending, compared only as
    /// far as the window reaches; with no tombstones, an in-memory run whose
    /// rids ascend or a file run with no filter) it reads them without the
    /// selection's other ids. So does a
    /// file view whose reads take its pending filter by the masks first
    /// ([`reads_masks_first`](Self::reads_masks_first)), whatever its bound:
    /// its window evaluates the filter, which its reads would evaluate
    /// anyway, so its one-shot reads take the rows from that evaluation, its
    /// streams read the matches' ids, and its statistics are exact. That test
    /// runs after the bound's, which costs less, but under tombstones before
    /// it, as the bound may count live rows such a window never takes.
    fn window_check(&self, offset: usize, limit: usize) -> Result<WindowCheck> {
        if offset != 0 || self.quads.is_pending_without_plan() {
            return Ok(WindowCheck::MayCut { base_live: None });
        }
        #[cfg(feature = "file-io")]
        let tombstoned = self.quads.deleted.is_some();
        #[cfg(feature = "file-io")]
        if tombstoned && self.reads_masks_first()? {
            return Ok(WindowCheck::MayCut { base_live: None });
        }
        let tail = self.tail_size();
        let fits = |base: usize| base.saturating_add(tail) <= limit;
        let base = self.quads.base_rows(|selected| !fits(selected))?;
        let may_cut = WindowCheck::MayCut {
            base_live: base.live,
        };
        if !fits(base.upper_bound()) {
            return Ok(may_cut);
        }
        #[cfg(feature = "file-io")]
        if !tombstoned && self.reads_masks_first()? {
            return Ok(may_cut);
        }
        Ok(WindowCheck::CannotCut)
    }

    /// Whether this view's one-shot reads take its pending filter by the
    /// filter's masks first ([`ExactRead::Masks`](read_plan::ExactRead::Masks)):
    /// a file view with no serve plan and no masks a window kept, an exact
    /// selection, and a filter of code equalities over columns with chunk
    /// probes, at a selection the reads by id decline. The choice those
    /// reads make, made here without reading.
    #[cfg(feature = "file-io")]
    pub(in crate::store) fn reads_masks_first(&self) -> Result<bool> {
        let (
            Some(FileBase {
                file,
                filter: FileFilter::Pending(filter),
                serve: None,
            }),
            ViewSelection::Exact(selection),
        ) = (self.quads.file(), &self.quads.selection)
        else {
            return Ok(false);
        };
        let primary = self.layout.strategy().primary_column_names();
        read_plan::ExactRead::reads_masks(file, primary, Some(filter), selection)
    }

    /// `window(0, limit)` of a file view with a filter its selected rows must
    /// still pass, an exact selection and no serve plan: the filter's masks,
    /// a pending filter's evaluated in file order up to the split that brings
    /// the matches to `limit` ([`file_filter::split_masks_until`]), the ones
    /// a window kept taken as they are, the filter not evaluated again.
    /// `None` for any other view.
    ///
    /// With `limit` matches the window is their ids, in base order, with no
    /// filter (only the codes its equalities pin, for the reads by id) and
    /// the tail cut off. With fewer it cuts no base row: the view keeps its
    /// filter with the masks kept for its reads ([`FileFilter::Kept`]) where
    /// the masks are smaller than the matches' ids
    /// ([`SplitMasks::smaller_than_ids`]) and the filter is one the reads by
    /// id check ([`read_plan::point_readable`]), else holds those ids the
    /// same way, and its tail is windowed to the rest of the limit.
    #[cfg(feature = "file-io")]
    async fn filter_window(&self, limit: usize) -> Result<Option<Self>> {
        let (Some(base), ViewSelection::Exact(selection)) =
            (self.quads.file(), &self.quads.selection)
        else {
            return Ok(None);
        };
        if base.serve.is_some() {
            return Ok(None);
        }
        let (file, deleted) = (&base.file, self.quads.deleted.as_ref());
        let masks = match &base.filter {
            FileFilter::Kept { masks, .. } => Arc::clone(masks),
            FileFilter::Pending(filter) => Arc::new(
                file_filter::split_masks_until(file, filter, selection, deleted, limit).await?,
            ),
            FileFilter::None | FileFilter::Resolved(_) => return Ok(None),
        };
        let found = masks.kept();
        let primary = self.layout.strategy().primary_column_names();
        let keep_masks = found < limit
            && masks.smaller_than_ids()
            && read_plan::point_readable(file, primary, base.filter.over_selection());
        let (quads, tail_limit) = if keep_masks {
            (self.quads.with_evaluated_masks(masks), limit - found)
        } else {
            // The matches' ids, the filter resolved into them: no filter
            // left, only the codes it pinned, for the reads by id.
            let taken = found.min(limit);
            let ids = Buffer::from(masks.first_ids(taken));
            let selection = ViewSelection::Exact(RowSelection::from_ids(ids));
            (
                self.quads.with_selection(selection).with_filter_resolved(),
                limit - taken,
            )
        };
        let tail = self.tail.as_ref().map(|tail| tail.window(0, tail_limit));
        Ok(Some(self.derived(quads, tail)))
    }

    /// The base's window over its exact `selection`, and the base's live row
    /// count as far as the window needs it: a position range over the
    /// selection, stepping over tombstones, with the selection's live row
    /// count (`live`, when the window's check counted it); or, under a file
    /// filter, the window of the first matches with their count (at most
    /// `offset + limit`): a pending filter's evaluated only as far as the
    /// window reaches, a window's kept masks' read off their bits.
    async fn base_window(
        &self,
        selection: &RowSelection,
        offset: usize,
        limit: usize,
        live: Option<usize>,
    ) -> Result<(RowSelection, usize)> {
        let base_len = self.quads.base_len();
        let deleted = self.quads.deleted.as_ref();
        #[cfg(feature = "file-io")]
        if let Some(base) = self.quads.file() {
            // Only the matches the window can reach are read; fewer than
            // asked means the base is exhausted and their count is its live
            // size.
            let want = offset.saturating_add(limit);
            let found = match &base.filter {
                FileFilter::Kept { masks, .. } => Some(Buffer::from(masks.first_ids(want))),
                FileFilter::Pending(filter) => Some(
                    file_filter::first_matching_rows(&base.file, filter, selection, deleted, want)
                        .await?,
                ),
                FileFilter::None | FileFilter::Resolved(_) => None,
            };
            if let Some(found) = found {
                let live = if found.len() < want {
                    found.len()
                } else {
                    want
                };
                return Ok((
                    RowSelection::Ids(found).window(offset, limit, None, base_len),
                    live,
                ));
            }
        }
        let live = live.unwrap_or_else(|| selection.live_len(deleted, base_len));
        Ok((selection.window(offset, limit, deleted, base_len), live))
    }
}

/// What a window's check ([`window_check`](VortexRdfStore::window_check))
/// found.
enum WindowCheck {
    /// The window cannot cut a row: it is the view as it is.
    CannotCut,
    /// The window may cut a row. `base_live` is the base's live row count
    /// when the check counted it, which the window takes instead of
    /// counting again.
    MayCut { base_live: Option<usize> },
}
