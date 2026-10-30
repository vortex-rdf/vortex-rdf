//! Counting a view's rows: the exact size and its base and tail halves, the
//! capped count behind `exists`, and the base count known without reading
//! ([`QuadsSource::live_len_if_known`], [`QuadsSource::base_rows`]) that the
//! counts, the planner metadata and the window start from.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::view::selection::ViewSelection;
use crate::store::{QuadsSource, Tail, VortexRdfStore};

impl VortexRdfStore {
    /// The number of quads this view holds, tombstoned rows excluded. A
    /// pending file filter is counted from its masks alone: only the columns
    /// it references are read, and no row is projected or decoded to a quad.
    pub async fn size(&self) -> Result<usize> {
        Ok(self.base_size().await? + self.tail_size())
    }

    /// [`size`](Self::size) of the base alone: the live rows the selection
    /// and a pending file filter cover. The masks a window kept of its filter
    /// answer when the view holds them; with no filter pending, a count known
    /// without reading ([`live_len_if_known`](QuadsSource::live_len_if_known))
    /// does. Otherwise the selection materializes (a deferred resolution's
    /// ids, computed once and cached on the view) and a pending filter is
    /// evaluated over its live rows.
    pub(crate) async fn base_size(&self) -> Result<usize> {
        if let Some(live) = self.quads.evaluated_len() {
            return Ok(live);
        }
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
    /// the first that reaches the cap, and an in-memory residual compare a
    /// count left pending compares its pieces in base row order and stops at
    /// the first that does. Available on every layout.
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

    /// [`base_size`](Self::base_size), a pending file filter or residual
    /// compare stopping at `limit` matches; a window's kept masks answer
    /// first.
    async fn base_size_capped(&self, limit: usize) -> Result<usize> {
        if let Some(live) = self.quads.evaluated_len() {
            return Ok(live.min(limit));
        }
        // A residual compare left pending compares its pieces only until
        // `limit` live rows are found.
        if let ViewSelection::Pending(lazy) = &self.quads.selection
            && let Some(count) = lazy.residual_count_capped(limit, self.quads.deleted.as_ref())?
        {
            return Ok(count);
        }
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

    /// The base's rows as far as they are known without I/O ([`BaseRows`]):
    /// the bound `view_statistics` reports and a window tests. Under
    /// tombstones the live rows are counted only when `count_live` asks for
    /// them, given the selected rows ([`live_len_if_known`](Self::live_len_if_known)
    /// walks the tombstones, and decodes an in-memory pending run's ids);
    /// without tombstones they are the selected rows wherever those are
    /// known.
    pub(crate) fn base_rows(&self, count_live: impl FnOnce(usize) -> bool) -> Result<BaseRows> {
        let base_len = self.base_len();
        let known = self.selection.len_if_known(None, base_len)?;
        let selected = known.unwrap_or(base_len);
        let live = match self.evaluated_len() {
            Some(live) => Some(live),
            None if self.deleted.is_none() => known,
            None if count_live(selected) => self.live_len_if_known()?,
            None => None,
        };
        Ok(BaseRows { live, selected })
    }
}

/// What a view's base rows number, as far as that is known without I/O
/// ([`QuadsSource::base_rows`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BaseRows {
    /// The live base rows: the matches a window's kept masks hold, or the
    /// selection's rows less its tombstones. `None` when they take I/O or
    /// were not counted. A pending filter is not applied: with one they bound
    /// the view's base rows.
    pub(crate) live: Option<usize>,
    /// The selection's rows before tombstones: a pending run's width, or
    /// every base row while that is unknown.
    pub(crate) selected: usize,
}

impl BaseRows {
    /// The most rows the base holds: its live rows where counted, else the
    /// selected rows.
    pub(crate) fn upper_bound(&self) -> usize {
        self.live.unwrap_or(self.selected)
    }
}
