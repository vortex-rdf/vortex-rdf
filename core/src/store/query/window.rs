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

#[cfg(feature = "file-io")]
use vortex_buffer::Buffer;

use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::store::VortexRdfStore;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_filter, read_plan};
use crate::store::view::selection::{RowSelection, ViewSelection};
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
    /// [`view_statistics`](Self::view_statistics)). With no `offset`, a
    /// served file view whose run's width is unknown reads the run's ids
    /// first, kept on the view for the window below, and is the view as it
    /// is when they fit. A file view whose reads take a pending filter of
    /// code equalities by its masks first is never the view as it is: its
    /// window evaluates the filter whatever the bound, as below, and so finds
    /// whether it cuts.
    ///
    /// Any other window is a position range over base order: base rows in
    /// base row order, then the tail's. Its serve plan is dropped, so a
    /// served view's rows come back in base order, not the index's. A
    /// pending file filter is resolved only as far as the window reaches
    /// (its splits evaluated in file order, stopping at the first that fills
    /// the window), so `LIMIT` over a filtered scan costs the matching prefix
    /// of the file. A window with no `offset` whose filter matches fewer rows
    /// than its limit finds that it cuts no base row: the view keeps its rows,
    /// known exactly from then on, its reads and counts taking them from the
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
        // A served file run whose width is unknown until its ids are read
        // reads them into the view's shared cell, where any window below
        // takes them from, and is the view as it is when they fit.
        #[cfg(feature = "file-io")]
        let base_live = if offset == 0
            && let Some(FileBase { serve: Some(_), .. }) = self.quads.file()
            && let ViewSelection::Pending { ids: lazy, .. } = &self.quads.selection
            && lazy.len_if_known().is_none()
        {
            lazy.materialized_async().await?;
            let WindowCheck::MayCut { base_live } = self.window_check(offset, limit)? else {
                debug::step!(t, "[window] limit {limit} cuts no row of the served run");
                return Ok(None);
            };
            base_live
        } else {
            base_live
        };
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
