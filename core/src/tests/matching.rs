//! In-memory pattern matching: the per-layout probes, the prefix probe over
//! the (s, p, o, g) order, and view derivation.

use super::*;

// ─── In-memory matching ────────────────────────────────────────────────

#[tokio::test]
async fn test_match_pattern_default_layout() {
    probe_default(built_store(two_quads(), LayoutStrategy::Default, vec![]).await).await;
}

#[tokio::test]
async fn test_match_typed_object_layout() {
    let store = built_store(two_quads(), LayoutStrategy::TypedObject, vec![]).await;
    assert_eq!(store.layout(), LayoutStrategy::TypedObject);
    probe_typed_object(store).await;
}

/// Every term kind in every role round-trips through each layout and binds
/// exactly its rows: a blank-node subject, a blank-node graph, the default
/// graph bound explicitly, and IRI / blank / plain / language-tagged / typed
/// objects — with and without the copy index, whose sorted copies carry the
/// same terms.
#[tokio::test]
async fn test_term_kinds_match_under_every_layout() {
    let quads = term_kind_quads();
    let blank_subject = quads
        .iter()
        .find_map(|q| match &q.subject {
            NamedOrBlankNode::BlankNode(_) => Some(q.subject.clone()),
            _ => None,
        })
        .unwrap();
    let blank_graph = quads
        .iter()
        .find_map(|q| match &q.graph_name {
            GraphName::BlankNode(_) => Some(q.graph_name.clone()),
            _ => None,
        })
        .unwrap();
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
            let tag = format!("{layout:?} {indexes:?}");
            let store = built_store(quads.clone(), layout, indexes).await;
            assert_eq!(view_strings(&store).await, quad_strings(&quads), "{tag}");

            let by_blank_s = store
                .match_pattern(Some(&blank_subject), None, None, None)
                .await
                .unwrap();
            assert_eq!(
                view_strings(&by_blank_s).await,
                expected_strings(&quads, |i| quads[i].subject == blank_subject),
                "{tag}: blank subject"
            );
            let by_blank_g = store
                .match_pattern(None, None, None, Some(&blank_graph))
                .await
                .unwrap();
            assert_eq!(
                view_strings(&by_blank_g).await,
                expected_strings(&quads, |i| quads[i].graph_name == blank_graph),
                "{tag}: blank graph"
            );
            let by_default_g = store
                .match_pattern(None, None, None, Some(&GraphName::DefaultGraph))
                .await
                .unwrap();
            let default_rows = by_default_g.quads_vec().await.unwrap();
            assert_eq!(
                quad_strings(&default_rows),
                expected_strings(&quads, |i| quads[i].graph_name == GraphName::DefaultGraph),
                "{tag}: default graph"
            );
            assert!(
                default_rows
                    .iter()
                    .all(|q| q.graph_name == GraphName::DefaultGraph),
                "{tag}: default graph rows must decode to the default graph"
            );
            for quad in &quads {
                let by_o = store
                    .match_pattern(None, None, Some(&quad.object), None)
                    .await
                    .unwrap();
                assert_eq!(
                    view_strings(&by_o).await,
                    vec![quad.to_string()],
                    "{tag}: object {}",
                    quad.object
                );
            }
        }
    }
}

/// One subject wide enough to sit at or above the index-routing gate
/// (5,000 rows over 3 predicates and 1,667 objects) and a second, narrow
/// one sharing its objects — so a subject-then-object match can only be
/// right when the index resolution is intersected with the subject range.
pub(super) fn above_gate_quads() -> Vec<Quad> {
    let mut quads: Vec<Quad> = (0..5_000)
        .map(|i| {
            make_quad(
                "http://example.org/s0",
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 1_667),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    quads.push(make_quad(
        "http://example.org/s1",
        "http://example.org/p0",
        "o5",
        GraphName::DefaultGraph,
    ));
    quads.push(make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o7",
        GraphName::DefaultGraph,
    ));
    quads
}

/// Over [`above_gate_quads`] with the copy index: a bound subject resolves
/// to its row range first, and the residual object is then resolved by the
/// index whenever the range is at or above the routing gate — the result is
/// the intersection (exact ids, no longer a range, and never served, since
/// the subject already restricts the view). Below the gate the residual is
/// filtered column-wise to the same answer.
pub(super) async fn run_subject_range_then_index_routing_above_gate(
    store: &VortexRdfStore,
    quads: &[Quad],
) {
    let s0 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0").unwrap());
    let s1 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
    let o5 = Term::Literal(Literal::new_simple_literal("o5"));

    // "o5" is on s0 at i ≡ 5 (mod 1667) — 3 rows — and once on s1.
    let by_o = store
        .match_pattern(None, None, Some(&o5), None)
        .await
        .unwrap();
    assert_eq!(by_o.size().await.unwrap(), 4);

    let above = store
        .match_pattern(Some(&s0), None, Some(&o5), None)
        .await
        .unwrap();
    assert_eq!(above.debug_selection_range(), None);
    assert_eq!(served_component(&above), None);
    assert_eq!(
        view_strings(&above).await,
        expected_strings(quads, |i| quads[i].subject == s0 && quads[i].object == o5)
    );

    let below = store
        .match_pattern(Some(&s1), None, Some(&o5), None)
        .await
        .unwrap();
    assert_eq!(below.debug_selection_range(), None);
    assert_eq!(served_component(&below), None);
    assert_eq!(
        view_strings(&below).await,
        expected_strings(quads, |i| quads[i].subject == s1 && quads[i].object == o5)
    );
}

#[tokio::test]
async fn test_subject_range_then_index_routing_above_gate() {
    let quads = above_gate_quads();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    run_subject_range_then_index_routing_above_gate(&store, &quads).await;
}

// ─── Prefix probe: nested binary search over the (s, p, o, g) order ────

/// Four subjects × three predicates × two objects × two graphs, except that
/// the last subject carries only `p0` — so every predicate exists in the
/// dictionary while `s03 p1` matches nothing. Row `i` of the sorted base is
/// `((si * 3 + pi) * 2 + oi) * 2 + gi` for the first three subjects; `s03`
/// occupies rows 36..40.
fn prefix_quads() -> Vec<Quad> {
    let mut quads = Vec::new();
    for si in 0..4 {
        for pi in 0..3 {
            if si == 3 && pi > 0 {
                continue;
            }
            for oi in 0..2 {
                for gi in 0..2 {
                    let g = if gi == 0 {
                        GraphName::DefaultGraph
                    } else {
                        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap())
                    };
                    quads.push(make_quad(
                        &format!("http://example.org/s{si:02}"),
                        &format!("http://example.org/p{pi}"),
                        &format!("o{oi}"),
                        g,
                    ));
                }
            }
        }
    }
    quads
}

fn prefix_terms() -> (NamedOrBlankNode, NamedNode, Term, GraphName) {
    (
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s01").unwrap()),
        NamedNode::new("http://example.org/p1").unwrap(),
        Term::Literal(Literal::new_simple_literal("o1")),
        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
    )
}

/// Every bound prefix of the sort order resolves to one exact row range —
/// under the Dictionary layout, with and without secondary indexes (a
/// subject-narrowed view skips index routing, so the ranges must agree).
#[tokio::test]
async fn test_prefix_probe_resolves_bound_prefixes_to_ranges() {
    let (s, p, o, g) = prefix_terms();
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let store = built_store(prefix_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;

        let label = format!("{indexes:?}");
        let label = &label;
        let expect = |view: VortexRdfStore, range: std::ops::Range<u64>| async move {
            assert_eq!(
                view.debug_selection_range(),
                Some(range.clone()),
                "{label}: prefix must resolve to exactly the sorted run"
            );
            assert_eq!(
                view.size().await.unwrap(),
                (range.end - range.start) as usize
            );
            view
        };

        let by_s = store
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap();
        expect(by_s, 12..24).await;
        let by_sp = store
            .match_pattern(Some(&s), Some(&p), None, None)
            .await
            .unwrap();
        let by_sp = expect(by_sp, 16..20).await;
        for q in by_sp.quads_vec().await.unwrap() {
            assert_eq!(q.subject.to_string(), s.to_string());
            assert_eq!(q.predicate, p);
        }
        let by_spo = store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap();
        expect(by_spo, 18..20).await;
        let by_spog = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
            .await
            .unwrap();
        expect(by_spog, 19..20).await;
        // The default graph is a dictionary term like any other ("").
        let by_spo_default = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&GraphName::DefaultGraph))
            .await
            .unwrap();
        expect(by_spo_default, 18..19).await;
        // A run that narrows to nothing: `p1` exists, but not under `s03`.
        let s03 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
        let none = store
            .match_pattern(Some(&s03), Some(&p), None, None)
            .await
            .unwrap();
        assert_eq!(none.size().await.unwrap(), 0);
        assert!(none.quads_vec().await.unwrap().is_empty());
    }
}

/// The prefix ends at the first unbound role: a bound object or graph behind
/// an unbound predicate is left to the residual scan, whose ids are exact but
/// no longer a range.
#[tokio::test]
async fn test_prefix_probe_stops_at_the_first_unbound_role() {
    let (s, _, o, g) = prefix_terms();
    let store = built_store(prefix_quads(), LayoutStrategy::Dictionary, vec![]).await;

    let by_so = store
        .match_pattern(Some(&s), None, Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_so.debug_selection_range(), None);
    assert_eq!(by_so.size().await.unwrap(), 6); // 3 predicates × 2 graphs
    let by_sg = store
        .match_pattern(Some(&s), None, None, Some(&g))
        .await
        .unwrap();
    assert_eq!(by_sg.debug_selection_range(), None);
    assert_eq!(by_sg.size().await.unwrap(), 6); // 3 predicates × 2 objects
}

/// Under a string layout only the subject has a searchable column: the
/// prefix stops after it and the rest is answered by the residual scan. A
/// subject the store never saw narrows to an empty range.
#[tokio::test]
async fn test_prefix_probe_string_layout_narrows_by_subject_only() {
    let (s, p, o, _) = prefix_terms();
    let s99 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = built_store(prefix_quads(), layout, vec![]).await;
        let by_s = store
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap();
        assert_eq!(by_s.debug_selection_range(), Some(12..24), "{layout:?}");
        let by_spo = store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap();
        assert_eq!(by_spo.debug_selection_range(), None, "{layout:?}");
        assert_eq!(by_spo.size().await.unwrap(), 2, "{layout:?}");

        let by_missing = store
            .match_pattern(Some(&s99), None, None, None)
            .await
            .unwrap();
        assert!(
            by_missing
                .debug_selection_range()
                .is_some_and(|r| r.is_empty()),
            "{layout:?}: a missing subject must narrow to an empty range"
        );
        assert_eq!(by_missing.size().await.unwrap(), 0, "{layout:?}");
    }
}

/// A chained match nests inside the range the previous one left: the rows
/// of any sub-run of the sorted base are still in (s, p, o, g) order.
#[tokio::test]
async fn test_prefix_probe_chains_within_a_range() {
    let (s, p, o, _) = prefix_terms();
    let store = built_store(prefix_quads(), LayoutStrategy::Dictionary, vec![]).await;

    let by_s = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    let by_sp = by_s
        .match_pattern(Some(&s), Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(by_sp.debug_selection_range(), Some(16..20));
    let by_spo = by_sp
        .match_pattern(Some(&s), Some(&p), Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_spo.debug_selection_range(), Some(18..20));
    // Without the subject re-bound there is no prefix to probe; the residual
    // scan over the inherited range still answers exactly.
    let by_p_within = by_s
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_within.debug_selection_range(), None);
    assert_eq!(by_p_within.size().await.unwrap(), 4);
}

/// Rows without sorted provenance never see the probe: the arm declines and
/// the scan answers, so a foreign writer's unsorted file still matches
/// exactly.
#[tokio::test]
async fn test_prefix_probe_declines_an_unstamped_base() {
    let (s, p, o, _) = prefix_terms();
    let mut quads = prefix_quads();
    quads.reverse();
    let store = unstamped_store(&quads);
    assert_eq!(store.view_statistics().unwrap().sort_order, None);

    let by_s = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert_eq!(by_s.debug_selection_range(), None);
    assert_eq!(by_s.size().await.unwrap(), 12);
    let by_spo = store
        .match_pattern(Some(&s), Some(&p), Some(&o), None)
        .await
        .unwrap();
    assert_eq!(by_spo.debug_selection_range(), None);
    assert_eq!(by_spo.size().await.unwrap(), 2);
}

/// The base a view was derived from stays reachable: matching narrows a
/// selection, it does not throw the unselected rows away.
#[tokio::test]
async fn test_derived_view_does_not_lose_base_rows() {
    let quads: Vec<Quad> = (0..10)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                &format!("object {}", i % 2),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let store = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;

    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let matched = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 5);

    // Widening back out from the derived view reaches only what the view
    // selects (5 rows) — but the store it came from is untouched, and a
    // fresh match against it still sees all 10.
    let widened = matched.match_pattern(None, None, None, None).await.unwrap();
    assert_eq!(widened.size().await.unwrap(), 5);
    assert_eq!(store.size().await.unwrap(), 10);
}

/// A residual over an in-memory base reads its probe-bound columns through
/// the probes the store resolved for its base, so a match resolves none: a
/// reference index answers `o` of `(?, p, o)` and `p` is read at its rows.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_in_memory_residual_reads_through_the_store_probes() {
    use crate::store::scan::typed_eq::probe_hooks;
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let store = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    for _ in 0..2 {
        let (cached, resolved) = probe_hooks::binds();
        let matched = store
            .match_pattern(None, Some(&p1), Some(&o2), None)
            .await
            .unwrap();
        let (cached_after, resolved_after) = probe_hooks::binds();
        assert_eq!(resolved_after, resolved, "no probe is resolved per match");
        assert_eq!(
            cached_after,
            cached + 1,
            "`p` reads through the store's probe"
        );
        assert_eq!(
            view_strings(&matched).await,
            expected_strings(&quads, |i| i % 21 == 16)
        );
    }
}

/// On a sorted Dictionary file, the roles behind a bound subject narrow its
/// run through the chunk probes, as the in-memory prefix probe narrows it:
/// every bound prefix of the sort order is one exact row range and leaves no
/// filter, with and without secondary indexes. The prefix ends at the first
/// unbound role, whose bound successors stay a filter over the run.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_prefix_probe_resolves_bound_prefixes_to_ranges() {
    let (s, p, o, g) = prefix_terms();
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let (_dir, path) =
            write_store_file(prefix_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let label = format!("{indexes:?}");
        let expect = |view: &VortexRdfStore, range: std::ops::Range<u64>| {
            assert_eq!(view.debug_selection_range(), Some(range), "{label}");
            let stats = view.view_statistics().unwrap();
            assert_eq!(
                (stats.file_backed, stats.pending_filter),
                (true, false),
                "{label}"
            );
        };

        let by_sp = store
            .match_pattern(Some(&s), Some(&p), None, None)
            .await
            .unwrap();
        expect(&by_sp, 16..20);
        assert_eq!(by_sp.size().await.unwrap(), 4, "{label}");
        for q in by_sp.quads_vec().await.unwrap() {
            assert_eq!(q.subject.to_string(), s.to_string());
            assert_eq!(q.predicate, p);
        }
        let by_spo = store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap();
        expect(&by_spo, 18..20);
        assert_eq!(by_spo.size().await.unwrap(), 2, "{label}");
        let by_spog = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
            .await
            .unwrap();
        expect(&by_spog, 19..20);
        assert_eq!(by_spog.quads_vec().await.unwrap().len(), 1, "{label}");
        let by_spo_default = store
            .match_pattern(Some(&s), Some(&p), Some(&o), Some(&GraphName::DefaultGraph))
            .await
            .unwrap();
        expect(&by_spo_default, 18..19);
        // `p1` exists, but not under `s03`: the run narrows to nothing.
        let s03 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
        let none = store
            .match_pattern(Some(&s03), Some(&p), None, None)
            .await
            .unwrap();
        assert_eq!(none.size().await.unwrap(), 0, "{label}");
        assert!(none.quads_vec().await.unwrap().is_empty(), "{label}");

        // An unbound predicate ends the prefix: the object stays a filter
        // over the subject's run.
        let by_so = store
            .match_pattern(Some(&s), None, Some(&o), None)
            .await
            .unwrap();
        assert_eq!(by_so.debug_selection_range(), Some(12..24), "{label}");
        let stats = by_so.view_statistics().unwrap();
        assert_eq!(
            (stats.file_backed, stats.pending_filter),
            (true, true),
            "{label}"
        );
        assert_eq!(by_so.size().await.unwrap(), 6, "{label}");
    }
}
