//! Batched probes: many patterns — each with its own keeps and window —
//! matched in one call, answering in input order. A query engine's
//! nested-loop join probes the store once per binding; batching lets a
//! file-backed store overlap those probes' I/O, and hands a binding layer
//! one call to release its lock around.

use futures::future::try_join_all;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};

use crate::error::Result;
use crate::store::narrowing::Keep;
use crate::store::schema::QuadColumn;

use super::IdsNeed;
use super::VortexRdfStore;

/// One probe of a batch: a pattern (`None` = free, as for
/// [`match_pattern`](VortexRdfStore::match_pattern)) with the narrowing to
/// apply to its match — keeps per column, then a window.
#[derive(Clone, Debug, Default)]
pub struct Probe {
    pub subject: Option<NamedOrBlankNode>,
    pub predicate: Option<NamedNode>,
    pub object: Option<Term>,
    pub graph: Option<GraphName>,
    /// Keeps applied to the match, in order (see
    /// [`keep_many`](VortexRdfStore::keep_many)).
    pub keeps: Vec<(QuadColumn, Keep)>,
    /// Rows to skip before the window (see
    /// [`window`](VortexRdfStore::window)); `0` skips none.
    pub offset: usize,
    /// Rows to take; `None` takes every row after the offset.
    pub limit: Option<usize>,
}

impl Probe {
    /// A probe of `pattern` with no narrowing.
    pub fn new(
        subject: Option<NamedOrBlankNode>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
        graph: Option<GraphName>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph,
            ..Self::default()
        }
    }

    /// This probe with a keep on `column` appended.
    pub fn keep(mut self, column: QuadColumn, keep: Keep) -> Self {
        self.keeps.push((column, keep));
        self
    }

    /// This probe windowed to `limit` rows after `offset`.
    pub fn window(mut self, offset: usize, limit: Option<usize>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Whether the probe narrows its match beyond the pattern.
    fn narrows(&self) -> bool {
        !self.keeps.is_empty() || self.offset != 0 || self.limit.is_some()
    }
}

impl VortexRdfStore {
    /// The view each probe narrows this store to, in input order:
    /// [`match_pattern`](Self::match_pattern), then the probe's keeps, then
    /// its window. The probes run concurrently, so a file-backed store
    /// overlaps their reads; in memory a match is CPU work and the batch
    /// runs it probe by probe. The first error ends the batch.
    pub async fn match_many(&self, probes: &[Probe]) -> Result<Vec<Self>> {
        try_join_all(probes.iter().map(|probe| self.run_probe(probe))).await
    }

    /// [`size`](Self::size) of each probe's view, in input order — or, for a
    /// probe with a limit, [`size_capped`](Self::size_capped) at that limit,
    /// which stops reading at the cap.
    pub async fn count_many(&self, probes: &[Probe]) -> Result<Vec<usize>> {
        try_join_all(probes.iter().map(|probe| async move {
            let view = self
                .run_pattern_and_keeps(probe, IdsNeed::CountOrWindow)
                .await?;
            match probe.limit {
                // An offset consumes rows before the cap counts them.
                Some(limit) => Ok(view
                    .size_capped(probe.offset.saturating_add(limit))
                    .await?
                    .saturating_sub(probe.offset)),
                None => Ok(view.size().await?.saturating_sub(probe.offset)),
            }
        }))
        .await
    }

    /// One probe's view: its pattern, keeps and window.
    pub async fn run_probe(&self, probe: &Probe) -> Result<Self> {
        // A windowed view is only windowed (which resolves what the window
        // reaches); an unwindowed one is read row by row.
        let windowed = probe.offset != 0 || probe.limit.is_some();
        let need = if windowed {
            IdsNeed::CountOrWindow
        } else {
            IdsNeed::Rows
        };
        let view = self.run_pattern_and_keeps(probe, need).await?;
        let view = if windowed {
            view.window(probe.offset, probe.limit.unwrap_or(usize::MAX))
                .await?
        } else {
            view
        };
        // The view leaves the store's hands, and its rows may be streamed.
        debug_assert!(
            !view.quads.is_pending_without_plan(),
            "a probe's view is never pending without a serve plan: only a count or a window holds one"
        );
        Ok(view)
    }

    /// One probe's view before its window: the pattern and the keeps.
    async fn run_pattern_and_keeps(&self, probe: &Probe, need: IdsNeed) -> Result<Self> {
        let matched = self
            .match_pattern_for(
                probe.subject.as_ref(),
                probe.predicate.as_ref(),
                probe.object.as_ref(),
                probe.graph.as_ref(),
                need,
            )
            .await?;
        if !probe.narrows() || probe.keeps.is_empty() {
            return Ok(matched);
        }
        matched.keep_many(&probe.keeps).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The batch future is `Send`, so a binding layer can spawn probes onto
    /// its runtime's workers.
    #[test]
    fn match_many_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        fn check(store: &VortexRdfStore, probes: &[Probe]) {
            let fut = store.match_many(probes);
            assert_send(&fut);
            let fut = store.count_many(probes);
            assert_send(&fut);
        }
        let _ = check;
    }
}
