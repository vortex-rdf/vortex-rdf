//! Batched probes: many patterns, each with its own keeps and window, matched
//! in one call and answered in input order; a file-backed store overlaps the
//! probes' I/O.

use futures::future::try_join_all;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};

use crate::common::terms::{self, Trust, at};
use crate::error::Result;
use crate::store::query::narrowing::{Keep, WindowCheck};
use crate::store::schema::QuadColumn;

use crate::store::IdsNeed;
use crate::store::VortexRdfStore;

/// One probe of a batch: a pattern (`None` = free, as for
/// [`match_pattern`](VortexRdfStore::match_pattern)) with the narrowing to
/// apply to its match: keeps per column, then a window.
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

    /// A probe, with no narrowing, of a quad pattern spelled as text: the four
    /// optional term spellings the string-typed frontends (Python and the CLI)
    /// accept. The subject is a named or blank node, the predicate a named
    /// node, the object any term (a bare IRI, without angle brackets,
    /// included), the graph a named or blank node or a default-graph spelling
    /// (`""`, `default`, `[]`); a `None` slot stays free.
    ///
    /// The first invalid slot, in subject-predicate-object-graph order, is an
    /// [`InvalidInput`](crate::VortexRdfError::InvalidInput) error whose
    /// message leads with the slot (`pattern subject:`, `pattern predicate:`,
    /// `pattern object:` or `pattern graph:`), then the term parser's reason.
    /// The CLI picks the hint it reports for a flag by that label.
    pub fn parse(
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
        graph: Option<&str>,
    ) -> Result<Self> {
        Ok(Self::new(
            subject
                .map(|s| at("pattern subject", terms::subject(s, Trust::Input)))
                .transpose()?,
            predicate
                .map(|p| at("pattern predicate", terms::named_node(p, Trust::Input)))
                .transpose()?,
            object
                .map(|o| at("pattern object", terms::term(o, Trust::Input)))
                .transpose()?,
            graph
                .map(|g| at("pattern graph", terms::graph_name(g, Trust::Input)))
                .transpose()?,
        ))
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
}

impl VortexRdfStore {
    /// The view each probe narrows this store to, in input order:
    /// [`match_pattern`](Self::match_pattern), then the probe's keeps, then
    /// its window. The probes are joined concurrently on the calling task, so
    /// a file-backed store overlaps their reads, while in memory each probe's
    /// match is CPU work that runs in turn. The first error ends the batch.
    pub async fn match_many(&self, probes: &[Probe]) -> Result<Vec<Self>> {
        try_join_all(probes.iter().map(|probe| self.run_probe(probe))).await
    }

    /// [`count_probe`](Self::count_probe) of each probe, in input order,
    /// concurrently as for [`match_many`](Self::match_many).
    pub async fn count_many(&self, probes: &[Probe]) -> Result<Vec<usize>> {
        try_join_all(probes.iter().map(|probe| self.count_probe(probe))).await
    }

    /// The row count of one probe's view, its window included: with a limit,
    /// [`size_capped`](Self::size_capped) at `offset + limit`, which stops
    /// reading at the cap, less the offset; without one, [`size`](Self::size)
    /// less the offset.
    pub async fn count_probe(&self, probe: &Probe) -> Result<usize> {
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
    }

    /// One probe's view: its pattern, keeps and window.
    pub async fn run_probe(&self, probe: &Probe) -> Result<Self> {
        // A windowed match is only windowed, which resolves what the window
        // reaches, so its ids may wait; an unwindowed one is read row by row.
        let windowed = probe.offset != 0 || probe.limit.is_some();
        let need = if windowed {
            IdsNeed::CountOrWindow
        } else {
            IdsNeed::Rows
        };
        let view = self.run_pattern_and_keeps(probe, need).await?;
        // A window that cannot cut a row is the matched view itself.
        let limit = probe.limit.unwrap_or(usize::MAX);
        let view = if windowed {
            match view.window_check(probe.offset, limit)? {
                WindowCheck::CannotCut => view,
                WindowCheck::MayCut {
                    base_live,
                    masks_first,
                } => {
                    view.window_cut(probe.offset, limit, base_live, masks_first)
                        .await?
                }
            }
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
        if probe.keeps.is_empty() {
            return Ok(matched);
        }
        matched.keep_many(&probe.keeps).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::VortexRdfError;
    use oxrdf::Literal;

    /// The batch futures (`match_many`, `count_many`) and the one-probe
    /// `count_probe` future are `Send`, so a binding layer can spawn probes
    /// onto its runtime's workers.
    #[test]
    fn match_many_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        fn check(store: &VortexRdfStore, probes: &[Probe]) {
            let fut = store.match_many(probes);
            assert_send(&fut);
            let fut = store.count_many(probes);
            assert_send(&fut);
            let fut = store.count_probe(&probes[0]);
            assert_send(&fut);
        }
        let _ = check;
    }

    /// Each slot takes its position's spellings, a bare IRI included; a free
    /// slot stays free, and the probe is not narrowed.
    #[test]
    fn parse_binds_each_slot() {
        let probe = Probe::parse(
            Some("_:b0"),
            Some("<http://example.org/p>"),
            Some("\"v\"@en"),
            Some("http://example.org/g"),
        )
        .unwrap();
        assert!(
            matches!(&probe.subject, Some(NamedOrBlankNode::BlankNode(b)) if b.as_str() == "b0")
        );
        assert_eq!(
            probe.predicate,
            Some(NamedNode::new("http://example.org/p").unwrap())
        );
        assert_eq!(
            probe.object,
            Some(Term::Literal(
                Literal::new_language_tagged_literal("v", "en").unwrap()
            ))
        );
        assert_eq!(
            probe.graph,
            Some(GraphName::NamedNode(
                NamedNode::new("http://example.org/g").unwrap()
            ))
        );
        assert!(probe.keeps.is_empty() && probe.offset == 0 && probe.limit.is_none());

        // A bare IRI (no angle brackets) in the object slot is a named node.
        let probe = Probe::parse(None, None, Some("http://example.org/x"), None).unwrap();
        assert_eq!(
            probe.object,
            Some(Term::NamedNode(
                NamedNode::new("http://example.org/x").unwrap()
            ))
        );

        // A `None` slot stays free; "default" names the default graph.
        let probe = Probe::parse(None, None, None, Some("default")).unwrap();
        assert!(probe.subject.is_none() && probe.predicate.is_none() && probe.object.is_none());
        assert_eq!(probe.graph, Some(GraphName::DefaultGraph));
    }

    /// Pattern slots are user-typed, so an invalid term in any slot must
    /// error rather than silently match nothing, with a message that names
    /// the slot and then the invalid part (oxrdf's own reason follows the
    /// `: `).
    #[test]
    fn parse_names_the_failing_slot() {
        let in_slot = |slot: usize, spelling| {
            let mut slots: [Option<&str>; 4] = [None; 4];
            slots[slot] = Some(spelling);
            Probe::parse(slots[0], slots[1], slots[2], slots[3])
        };
        for (slot, spelling, message) in [
            (
                0,
                "no spaces allowed",
                "pattern subject: invalid IRI \"no spaces allowed\": ",
            ),
            (
                0,
                "_:a b",
                "pattern subject: invalid blank node \"_:a b\": ",
            ),
            (
                1,
                "not an iri",
                "pattern predicate: invalid IRI \"not an iri\": ",
            ),
            (
                1,
                "<not an iri>",
                "pattern predicate: invalid IRI \"not an iri\": ",
            ),
            (
                2,
                "\"unterminated",
                "pattern object: malformed literal \"\\\"unterminated\"",
            ),
            (
                2,
                "\"v\"@not a tag",
                "pattern object: invalid language tag \"not a tag\": ",
            ),
            (
                2,
                "\"x\"^^<not an iri>",
                "pattern object: invalid IRI \"not an iri\": ",
            ),
            (2, "_:a b", "pattern object: invalid blank node \"_:a b\": "),
            (
                3,
                "bad graph iri",
                "pattern graph: invalid IRI \"bad graph iri\": ",
            ),
            (3, "_:a b", "pattern graph: invalid blank node \"_:a b\": "),
        ] {
            match in_slot(slot, spelling) {
                Err(VortexRdfError::InvalidInput(got)) => {
                    assert!(
                        got.starts_with(message),
                        "slot {slot} {spelling:?}: {got:?}"
                    )
                }
                other => panic!("slot {slot} {spelling:?}: {other:?}"),
            }
        }

        // With several invalid slots, the first in s, p, o, g order is the
        // one reported.
        let all_invalid = Probe::parse(
            Some("bad subject"),
            Some("bad predicate"),
            Some("\"bad"),
            Some("bad graph"),
        );
        match all_invalid {
            Err(VortexRdfError::InvalidInput(got)) => {
                assert!(got.starts_with("pattern subject: "), "{got:?}")
            }
            other => panic!("{other:?}"),
        }
    }
}
