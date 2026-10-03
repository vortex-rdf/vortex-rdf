//! Counting a view's rows: the exact size, its base and tail halves, and the
//! capped count behind `exists`.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::QuadsSource;
use crate::store::VortexRdfStore;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::view::Tail;

impl VortexRdfStore {
    /// Number of quads in the view: tombstones out; a pending file filter is
    /// evaluated over the columns it references, no row decoded.
    pub async fn size(&self) -> Result<usize> {
        Ok(self.base_size().await? + self.tail_size())
    }

    /// [`size`](Self::size) for the base alone. A served match's deferred
    /// ids materialize here unless the plan's run width answers.
    pub(crate) async fn base_size(&self) -> Result<usize> {
        if !self.quads.has_filter()
            && let Some(live) = self.quads.live_len_if_known()
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

    /// [`size`](Self::size) for the tail alone.
    pub(crate) fn tail_size(&self) -> usize {
        self.tail.as_ref().map_or(0, Tail::live_len)
    }

    /// `min(size, limit)`, stopping once `limit` rows are known: a file view
    /// with a pending filter evaluates its splits in file order and stops at
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
        if let QuadsSource::File {
            file,
            filter: Some(filter),
            selection,
            deleted,
            ..
        } = &self.quads
        {
            let selection = selection.materialized_async().await?;
            return file_filter::count_matching_rows_capped(
                file,
                filter,
                &selection,
                deleted.as_ref(),
                limit,
            )
            .await;
        }
        Ok(self.base_size().await?.min(limit))
    }

    /// Whether the view holds any quad: [`size_capped`](Self::size_capped)
    /// of one.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.size_capped(1).await? > 0)
    }
}
