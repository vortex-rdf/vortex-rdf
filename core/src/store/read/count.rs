//! Counting a view's rows: the exact size, its base and tail halves, the
//! capped count behind `exists`, and the base count a view knows without
//! reading ([`QuadsSource::live_len_if_known`]) that both the counts and the
//! planner metadata start from.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::view::selection::ViewSelection;
use crate::store::{QuadsSource, Tail, VortexRdfStore};

impl VortexRdfStore {
    /// Number of quads in the store.
    ///
    /// For a file-backed store with a pending `match_pattern` filter, this
    /// counts matching rows from the filter masks alone — only the columns the
    /// filter references are read, and no rows are projected or decoded.
    /// `file.row_count()` alone would report the unfiltered total.
    pub async fn size(&self) -> Result<usize> {
        Ok(self.base_size().await? + self.tail_size())
    }

    /// [`size`](Self::size) for the base alone — the rows the selection (and
    /// a pending file filter) cover, minus tombstones; the tail's rows are
    /// [`tail_size`](Self::tail_size).
    ///
    /// Whatever the base knows without reading answers outright
    /// ([`live_len_if_known`](QuadsSource::live_len_if_known)), unless a
    /// file filter is pending, whose selectivity is unknown. Otherwise a
    /// count needs the selection itself, so a deferred resolution's ids (a
    /// served match's index-child scan, a located run's rids under
    /// tombstones) are computed here, once, and cached on the view; a pending
    /// filter is then evaluated over the selected rows (the tombstoned ones
    /// excluded before counting).
    pub(crate) async fn base_size(&self) -> Result<usize> {
        if !self.quads.has_filter()
            && let Some(live) = self.quads.live_len_if_known()?
        {
            return Ok(live);
        }
        let selection = self.quads.materialized_selection().await?;
        #[cfg(feature = "file-io")]
        if let (Some(file), Some(filter)) = (self.quads.file(), self.quads.filter()) {
            return file_filter::count_matching_rows(
                file,
                filter,
                &selection,
                self.quads.deleted(),
            )
            .await;
        }
        Ok(selection.live_len(self.quads.deleted(), self.quads.base_len()))
    }

    /// The tail's contribution to [`size`](Self::size): its selection is
    /// always exact (tail matches are resolved eagerly), minus its own
    /// tombstones.
    pub(crate) fn tail_size(&self) -> usize {
        self.tail.as_ref().map_or(0, Tail::live_len)
    }

    /// `min(size, limit)`, stopping as soon as `limit` rows are known to
    /// exist: a file view with a pending filter evaluates its splits in file
    /// order and stops at the first that reaches the cap (an in-memory view
    /// knows its size without reading rows). Available on every layout.
    pub async fn size_capped(&self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let base = self.base_size_capped(limit).await?;
        if base >= limit {
            return Ok(limit);
        }
        Ok((base + self.tail_size()).min(limit))
    }

    /// [`base_size`](Self::base_size), a pending file filter stopping at
    /// `limit` matches.
    async fn base_size_capped(&self, limit: usize) -> Result<usize> {
        #[cfg(feature = "file-io")]
        if let (Some(file), Some(filter)) = (self.quads.file(), self.quads.filter()) {
            // The selection resolves first (a served view's ids come from its
            // index child); the filter then counts within it.
            let selection = self.quads.materialized_selection().await?;
            return file_filter::count_matching_rows_capped(
                file,
                filter,
                &selection,
                self.quads.deleted(),
                limit,
            )
            .await;
        }
        Ok(self.base_size().await?.min(limit))
    }

    /// Whether the view holds any quad — [`size_capped`](Self::size_capped)
    /// of one, so a filtered file view reads only up to its first match.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.size_capped(1).await? > 0)
    }
}

impl QuadsSource {
    /// The live base rows the selection covers, when that is known without
    /// I/O ([`ViewSelection::len_if_known`]): an exact selection less its
    /// tombstones, or a pending run's width while nothing is tombstoned — a
    /// served match's or a located run held for a count. On file a located
    /// serve plan also knows the width of the child run it serves (exactly
    /// the constrained rows) when the selection's own ids are unread. In
    /// memory a pending run under tombstones reads its ids to count them out
    /// — the one way this fails. `None` for a file run under tombstones
    /// (they are defined over primary row ids neither the run nor the plan
    /// holds), or one that is not located. A pushed-down filter is not
    /// applied: the count is then a bound on the view's rows.
    pub(crate) fn live_len_if_known(&self) -> Result<Option<usize>> {
        let base_len = self.base_len();
        Ok(match self {
            QuadsSource::InMemory {
                selection, deleted, ..
            } => match (selection, deleted) {
                // A run's live rows under tombstones: its ids, read in memory.
                (ViewSelection::Pending(_), Some(deleted)) => {
                    Some(selection.materialized()?.live_len(Some(deleted), base_len))
                }
                _ => selection.len_if_known(deleted.as_ref(), base_len),
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                selection,
                deleted,
                serve,
                ..
            } => selection
                .len_if_known(deleted.as_ref(), base_len)
                .or_else(|| match (selection, deleted, serve) {
                    (ViewSelection::Pending(_), None, Some(plan)) => {
                        plan.row_range().map(|range| {
                            usize::try_from(range.end - range.start).unwrap_or(usize::MAX)
                        })
                    }
                    _ => None,
                }),
        })
    }
}
