//! Pattern matching: composing a pattern's restrictions into derived views
//! over the base and the tail. This module takes a pattern to the backend
//! that matches it, chains a match on a view that still holds a pattern's
//! pending matches into one match of both, and decides what a view's ids may
//! leave pending ([`IdsNeed`]); `memory` matches an in-memory base, `file` a
//! file's, and `tail` the append tail.

#[cfg(feature = "file-io")]
mod file;
mod memory;
mod tail;

use std::sync::Arc;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};

use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::store::indexes::ResolvedRowIds;
use crate::store::layouts::{OwnedQuadPattern, Prepared, QuadPattern};
use crate::store::scan::pieces::FIRST_PIECE_ROWS;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{Backend, VortexRdfStore};

/// What the caller of a match does with the view's selection: whether a
/// located index run may leave its row ids unread without a serve plan, and
/// an in-memory residual compare its rows uncompared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdsNeed {
    /// The view may be read row by row: every resolution's ids are computed,
    /// or deferred behind a serve plan (the public `match_pattern`).
    Rows,
    /// The view is only counted (`size`, `size_capped`), windowed or kept,
    /// and needs at most its first `reach` rows (`usize::MAX`: all of them):
    /// a run whose width is known keeps its ids deferred, so a count reads
    /// none and a window reads only the rows it takes, and an in-memory
    /// residual compare over more rows than `reach` is left pending, compared
    /// only as far as the count or the window reaches ([`PendingResidual`]).
    CountOrWindow { reach: usize },
}

impl IdsNeed {
    /// Whether the view is only counted, windowed or kept.
    pub(crate) fn counts_or_windows(self) -> bool {
        matches!(self, IdsNeed::CountOrWindow { .. })
    }

    /// Whether a residual compare over `rows` selected rows is left pending
    /// ([`PendingResidual`]): only a count or a window that needs fewer rows
    /// than the selection holds can stop early, and only over more rows than
    /// the first piece holds.
    fn defers_residual(self, rows: usize) -> bool {
        matches!(self, IdsNeed::CountOrWindow { reach } if reach < rows) && rows > FIRST_PIECE_ROWS
    }
}

impl VortexRdfStore {
    // ── pattern matching ──────────────────────────────────────────────────────

    /// A view of this store narrowed to the quads matching the pattern
    /// (`None` = free), with no quad decoded: the base and the tail are
    /// matched independently, and the restrictions compose with this view's
    /// own (a prefix binary search over the sorted base, an index's row ids,
    /// on file also a pushed-down filter or a pruned row range, then a
    /// column-wise scan of whatever remains). The view shares the base, so
    /// matches chain; every read of it excludes tombstoned rows.
    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        self.match_pattern_for(subject, predicate, object, graph, IdsNeed::Rows)
            .await
    }

    /// [`match_pattern`](Self::match_pattern) for a caller whose use of the
    /// view `need` describes; with [`IdsNeed::CountOrWindow`] the view must
    /// only be counted, windowed or kept (each of which resolves it). A view
    /// built for its rows is never pending without a serve plan, which a
    /// debug assertion checks.
    pub(crate) async fn match_pattern_for(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        need: IdsNeed,
    ) -> Result<Self> {
        let pattern = QuadPattern::new(subject, predicate, object, graph);
        let mut matched = self.match_base(pattern, need).await?;
        // The tail is matched whatever the base concluded: a term the base's
        // dictionary has never seen proves nothing about rows appended since
        // (the tail stores strings so such a term can still match).
        if let Some(tail) = &self.tail {
            matched.tail = Some(Self::match_tail(tail, pattern).await?);
        }
        debug_assert!(
            need.counts_or_windows() || !matched.quads.is_pending_without_plan(),
            "a view built for its rows (IdsNeed::Rows) is never pending without a serve plan"
        );
        Ok(matched)
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately). `need` is the caller's use of the view, which
    /// decides whether a resolution's ids may stay deferred.
    async fn match_base(&self, pattern: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        // A view whose base still holds the pending matches of one pattern
        // over every row is matched as one match of both patterns, which
        // never computes those ids.
        if let ViewSelection::Pending {
            matches: Some(first),
            ..
        } = &self.quads.selection
            && let Some(both) = first.and(pattern)
        {
            return self.match_both(both, need).await;
        }
        let t = debug::timer();

        let Prepared::Codes(mut codes) = self.layout.prepare_pattern(pattern).await? else {
            debug::step!(t, "[match_pattern] Layout proved the pattern unmatchable");
            return Ok(self.empty_view());
        };
        debug::step!(t, "[match_pattern] Prepared pattern codes");

        let every_row = self.quads.is_unrefined();
        let mut matched = match &self.quads.backend {
            Backend::InMemory(memory) => {
                self.match_base_in_memory(memory, pattern, &mut codes, need, t)?
            }
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                self.match_base_file(base, pattern, &mut codes, need, t)
                    .await?
            }
        };
        // Matched over every base row, pending ids are (with the filter the
        // match left beside them) exactly the pattern's matches. A view built
        // for its rows, which a caller may chain on, names the pattern where
        // its run is wide enough that computing the ids costs more than
        // keeping the pattern.
        if every_row
            && need == IdsNeed::Rows
            && let ViewSelection::Pending { ids, matches } = &mut matched.quads.selection
            && ids.len_if_known().is_none_or(|rows| rows >= CHAIN_MIN_ROWS)
        {
            *matches = Some(Arc::new(OwnedQuadPattern::of(pattern)));
        }
        Ok(matched)
    }

    /// The base of a chained match whose view still holds the pending
    /// matches of a pattern over every base row, `both` binding that
    /// pattern's roles and the chained one's: one match of `both` over every
    /// base row, which the pending ids never enter. A view built for its rows
    /// gets the ids in base order with no serve plan, as a chained match
    /// always leaves them; one only counted or windowed keeps what the match
    /// left.
    async fn match_both(&self, both: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        observe!(ChainMerged);
        let t = debug::timer();
        let every_row = self.derived(self.quads.unrestricted(), self.tail.clone());
        let matched = Box::pin(every_row.match_base(both, need)).await?;
        if need.counts_or_windows() {
            debug::step!(
                t,
                "[match_pattern] Chained match answered as one match of both patterns"
            );
            return Ok(matched);
        }
        let ids = matched.quads.materialized_selection().await?;
        debug::step!(
            t,
            "[match_pattern] Chained match answered as one match of both patterns (ids in base order)"
        );
        Ok(matched.derived(
            matched.quads.with_selection(ViewSelection::Exact(ids)),
            matched.tail.clone(),
        ))
    }

    // ── pattern matching helpers ─────────────────────────────────────────────

    /// Whether the store holds a quad equal to `quad`, tombstoned rows counting
    /// as absent: one fully-bound `match_pattern` over the base and the tail,
    /// through whatever fast path applies, read only as far as its first row
    /// ([`exists`](Self::exists)).
    pub async fn contains(&self, quad: &Quad) -> Result<bool> {
        let matched = self
            .match_pattern(
                Some(&quad.subject),
                Some(&quad.predicate),
                Some(&quad.object),
                Some(&quad.graph_name),
            )
            .await?;
        matched.exists().await
    }
}

/// `existing` narrowed by an index resolution's row ids: a lazy resolution
/// the caller may leave unread (`defer`) becomes the view's pending
/// selection, its ids computed by the first consumer that needs them; any
/// other lazy one's ids, like an eager resolution's, are intersected into
/// `existing`, which they can only narrow. Synchronous, as the in-memory
/// match is: an in-memory run's ids are computed here, while the file match
/// reads a file run's (they take I/O) before folding, so they are found
/// computed.
fn fold_row_ids(
    existing: RowSelection,
    row_ids: ResolvedRowIds,
    defer: bool,
) -> Result<ViewSelection> {
    Ok(match row_ids {
        ResolvedRowIds::Lazy(lazy) if defer => ViewSelection::pending(lazy),
        ResolvedRowIds::Lazy(lazy) => {
            ViewSelection::Exact(existing.intersect_ids(lazy.materialized()?))
        }
        #[cfg(feature = "file-io")]
        ResolvedRowIds::Eager(ids) => ViewSelection::Exact(existing.intersect_ids(ids)),
    })
}

/// The narrowest pending run whose match, built for its rows, names its
/// pattern, so that a match chained on its view is one match of both
/// patterns ([`VortexRdfStore::match_both`]): copying the pattern's terms
/// costs about what computing a thousand of the run's ids does (a few
/// allocations, some 2,000 instructions), and a run of unknown width (a file
/// run the resolver did not locate) would cost a scan of the index child.
const CHAIN_MIN_ROWS: usize = 1_024;

/// Selection size below which a view the subject search already narrowed
/// skips index routing and filters its remaining rows column-wise (see
/// `match_base`): a lookup costs the rows matching the component it resolves,
/// not the rows the view still holds. An unrefined view never skips.
const INDEX_ROUTING_MIN_ROWS: usize = 4_096;
