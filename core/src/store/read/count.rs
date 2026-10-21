//! Counting a view's rows: the exact size and its base and tail halves, the
//! capped count behind `exists`, and the base count known without reading
//! ([`QuadsSource::live_len_if_known`]) that the counts and the planner
//! metadata start from.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::{QuadsSource, Tail, VortexRdfStore};

impl VortexRdfStore {
    /// The number of quads this view holds, tombstoned rows excluded. A
    /// pending file filter is counted from its masks alone: only the columns
    /// it references are read, and no row is projected or decoded to a quad.
    pub async fn size(&self) -> Result<usize> {
        Ok(self.base_size().await? + self.tail_size())
    }

    /// [`size`](Self::size) of the base alone: the live rows the selection
    /// and a pending file filter cover. With no filter pending, a count known
    /// without reading ([`live_len_if_known`](QuadsSource::live_len_if_known))
    /// answers. Otherwise the selection materializes (a deferred resolution's
    /// ids, computed once and cached on the view) and a pending filter is
    /// evaluated over its live rows.
    pub(crate) async fn base_size(&self) -> Result<usize> {
        if !self.quads.has_filter()
            && let Some(live) = self.quads.live_len_if_known()?
        {
            return Ok(live);
        }
        let selection = self.quads.materialized_selection().await?;
        #[cfg(feature = "file-io")]
        if let Some((file, filter)) = self.quads.pending_filter() {
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

    /// The tail's live rows, the rest of [`size`](Self::size); a tail's
    /// selection is always exact, as tail matches resolve eagerly.
    pub(crate) fn tail_size(&self) -> usize {
        self.tail.as_ref().map_or(0, Tail::live_len)
    }

    /// `min(size, limit)`, stopping once `limit` rows are known to exist: a
    /// pending file filter evaluates its splits in file order and stops at
    /// the first that reaches the cap. Available on every layout.
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
        if let Some((file, filter)) = self.quads.pending_filter() {
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

    /// Whether the view holds any quad: [`size_capped`](Self::size_capped) at
    /// one, so a filtered file view reads only up to its first match.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.size_capped(1).await? > 0)
    }
}

impl QuadsSource {
    /// The live base rows the selection covers, when known without I/O
    /// ([`ViewSelection::len_if_known`]), on either backend: an exact
    /// selection less its tombstones, a pending run's width while nothing is
    /// tombstoned, and under tombstones a pending run's ids when they take no
    /// I/O (an in-memory run's, decoded here, the one way this fails; a file
    /// run's once read). A pending filter is not applied, so with one the
    /// count is a bound on the view's rows.
    ///
    /// [`ViewSelection::len_if_known`]: crate::store::view::selection::ViewSelection::len_if_known
    pub(crate) fn live_len_if_known(&self) -> Result<Option<usize>> {
        self.selection
            .len_if_known(self.deleted.as_ref(), self.base_len())
    }
}
