//! Counting a view's rows: the exact size, its base and tail halves, the
//! capped count behind `exists`, and the base count a view knows without
//! reading ([`QuadsSource::live_len_if_known`]) that both the counts and the
//! planner metadata start from.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
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
    /// served match's index-child scan, a file run's ids under tombstones)
    /// are computed here, once, and cached on the view; a pending filter is
    /// then evaluated over the selected rows (the tombstoned ones excluded
    /// before counting).
    pub(crate) async fn base_size(&self) -> Result<usize> {
        if !self.quads.has_filter()
            && let Some(live) = self.quads.live_len_if_known()?
        {
            return Ok(live);
        }
        let selection = self.quads.materialized_selection().await?;
        #[cfg(feature = "file-io")]
        if let Some(FileBase {
            file,
            filter: Some(filter),
            ..
        }) = self.quads.file()
        {
            return file_filter::count_matching_rows(
                file,
                filter,
                &selection,
                self.quads.deleted.as_ref(),
            )
            .await;
        }
        Ok(selection.live_len(self.quads.deleted.as_ref(), self.quads.base_len()))
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
        if let Some(FileBase {
            file,
            filter: Some(filter),
            ..
        }) = self.quads.file()
        {
            // The selection resolves first (a served view's ids come from its
            // index child); the filter then counts within it.
            let selection = self.quads.materialized_selection().await?;
            return file_filter::count_matching_rows_capped(
                file,
                filter,
                &selection,
                self.quads.deleted.as_ref(),
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
    /// I/O ([`ViewSelection::len_if_known`]), whichever backend holds them:
    /// an exact selection less its tombstones, a pending run's width while
    /// nothing is tombstoned — a served match's or a located run held for a
    /// count — and under tombstones a run's ids that take no I/O (an
    /// in-memory run's, decoded here, the one way this fails; a file run's
    /// once read). A pushed-down filter is not applied: the count is then a
    /// bound on the view's rows.
    ///
    /// [`ViewSelection::len_if_known`]: crate::store::view::selection::ViewSelection::len_if_known
    pub(crate) fn live_len_if_known(&self) -> Result<Option<usize>> {
        self.selection
            .len_if_known(self.deleted.as_ref(), self.base_len())
    }
}
