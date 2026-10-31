//! Partitions: a view cut into views over disjoint, contiguous pieces of
//! its rows that read, in turn, the view, for a consumer that reads them in
//! parallel. A served view is cut in its index's order
//! ([`served_partitions`](VortexRdfStore::served_partitions)); any other in
//! base row order.

use std::num::NonZeroUsize;

use crate::error::Result;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{Tail, VortexRdfStore};

impl VortexRdfStore {
    /// This view cut into at most `n` views over disjoint, contiguous pieces
    /// of its rows, in the view's order, so reading the partitions in turn
    /// reads the view. A view a by-copy index serves (a match, or a window,
    /// keep or chained match of one) is cut in its index's order into served
    /// windows of about equal live rows, as [`window`](Self::window) cuts
    /// one (on file an unlocated run is read once by row index first), the
    /// tail with the last: every partition keeps the view's
    /// [`sort_order`](ViewStatistics::sort_order), the last only when the
    /// view reads no tail row. Any other view is cut in base row order: each
    /// keeps the view's pushed-down filter and tombstones and applies them to
    /// its own rows, its pieces about equal before filtering, not after; the
    /// append tail goes with the last, so over a sorted base every partition
    /// but the last keeps the `Spog` sort order, and the last keeps it only
    /// when the view reads no tail row. For `n` = 1 the one partition is the
    /// view itself, its serve plan and pending selection included; a view
    /// selecting no base row is its own one partition.
    pub async fn partitions(&self, n: NonZeroUsize) -> Result<Vec<Self>> {
        let n = n.get();
        if n == 1 {
            return Ok(vec![self.clone()]);
        }
        if let Some(parts) = self.served_partitions(n).await? {
            return Ok(parts);
        }
        let pieces = self
            .quads
            .materialized_selection()
            .await?
            .split(n, self.quads.base_len());
        if pieces.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let last = pieces.len() - 1;
        Ok(pieces
            .into_iter()
            .enumerate()
            .map(|(i, piece)| {
                let quads = self.quads.with_selection(ViewSelection::Exact(piece));
                self.derived(quads, self.partition_tail(i == last))
            })
            .collect())
    }

    /// A partition's tail: the view's whole for the `last` partition, none
    /// of it for the others.
    pub(in crate::store) fn partition_tail(&self, last: bool) -> Option<Tail> {
        self.tail.as_ref().map(|tail| {
            if last {
                tail.clone()
            } else {
                tail.with_selection(RowSelection::empty())
            }
        })
    }
}
