//! A pattern matched on a narrowed served view filters the view's rows in
//! the index's order and stays served; on a view no narrowing touched it
//! matches as before.

use super::*;

/// A pattern chained on a narrowed served view (a window of `?p2??`) keeps
/// the window's rows its bound terms match, compared on the copy's own
/// columns (codes or strings), in the window's order, and stays served: in
/// both residencies, on every layout (TypedObject's copies hold the object as
/// a string, its base as a typed value), as written and under tombstones.
/// The object chain keeps one object's rows; the graph chain keeps rows of
/// every object, so its order is the index's, not base order. The window's
/// own predicate keeps every row; another, none.
#[tokio::test]
async fn chained_match_on_a_window_filters_in_index_order() {
    let quads = graphed_quads(5_000);
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let default = GraphName::DefaultGraph;
    // Every fifth of the predicate's quads.
    let doomed: Vec<Quad> = quads
        .iter()
        .filter(|quad| quad.predicate == p2)
        .step_by(5)
        .cloned()
        .collect();
    for layout in [
        LayoutStrategy::Dictionary,
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
    ] {
        let (_dir, path) =
            write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let file = VortexRdfStore::from_file(&path).await.unwrap();
        let memory = built_store(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let mut stores = Vec::new();
        for (residency, store) in [("file", file), ("memory", memory)] {
            stores.push((format!("{layout:?} {residency} as written"), store.clone()));
            let tombstoned = delete_all(&store, &doomed).await;
            stores.push((format!("{layout:?} {residency} tombstoned"), tombstoned));
        }
        for (tag, store) in stores {
            let window = matched_view(&store, None, Some(&p2), None, None)
                .await
                .window(10, 500)
                .await
                .unwrap();
            assert!(
                served_component(&window).is_some(),
                "{tag}: a served window"
            );
            let rows = window.quads_vec().await.unwrap();
            for (chain, object, graph) in [
                ("o3", Some(&o3), None),
                ("default graph", None, Some(&default)),
            ] {
                let tag = format!("{tag}, {chain}");
                let want: Vec<Quad> = rows
                    .iter()
                    .filter(|quad| object.is_none_or(|o| &quad.object == o))
                    .filter(|quad| graph.is_none_or(|g| &quad.graph_name == g))
                    .cloned()
                    .collect();
                assert!(
                    !want.is_empty() && want.len() < rows.len(),
                    "{tag}: the chain filters"
                );
                let chained = window
                    .match_pattern(None, None, object, graph)
                    .await
                    .unwrap();
                assert!(served_component(&chained).is_some(), "{tag}: served");
                assert_eq!(
                    chained.quads_vec().await.unwrap(),
                    want,
                    "{tag}: the window's rows, filtered, in its order"
                );
                assert_eq!(
                    chained.size().await.unwrap(),
                    want.len(),
                    "{tag}: the count"
                );
            }
            // A term no row of the window holds leaves none.
            let absent = plain_literal("object 99");
            let none = window
                .match_pattern(None, None, Some(&absent), None)
                .await
                .unwrap();
            assert_eq!(none.size().await.unwrap(), 0, "{tag}: an absent object");
            assert!(none.quads_vec().await.unwrap().is_empty(), "{tag}: no rows");
            // The window's own predicate again keeps every row, in order;
            // another predicate keeps none.
            let same = window
                .match_pattern(None, Some(&p2), None, None)
                .await
                .unwrap();
            assert_eq!(
                same.quads_vec().await.unwrap(),
                rows,
                "{tag}: the same predicate"
            );
            assert_eq!(same.size().await.unwrap(), rows.len(), "{tag}: its count");
            let other = window
                .match_pattern(None, Some(&iri("p3")), None, None)
                .await
                .unwrap();
            assert_eq!(other.size().await.unwrap(), 0, "{tag}: another predicate");
            assert!(
                other.quads_vec().await.unwrap().is_empty(),
                "{tag}: its rows"
            );
        }
    }
}

/// A pattern chained on a served view no window or keep narrowed is matched
/// as one match of both patterns, as before: the rows of the merged pattern.
/// The run is wide enough for its view to name its pattern (1,429 rows).
#[tokio::test]
async fn chained_match_on_an_unnarrowed_view_is_unchanged() {
    let quads = modular_quads(10_000, 7, 11);
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    for (tag, store) in [("file", file), ("memory", memory)] {
        let view = matched_view(&store, None, Some(&p2), None, None).await;
        assert!(served_component(&view).is_some(), "{tag}: served");
        let rec = Recorder::start();
        let chained = view
            .match_pattern(None, None, Some(&o3), None)
            .await
            .unwrap();
        assert_eq!(rec.count(&Event::ChainMerged), 1, "{tag}: merged");
        let merged = matched_view(&store, None, Some(&p2), Some(&o3), None)
            .await
            .quads_vec()
            .await
            .unwrap();
        assert!(!merged.is_empty(), "{tag}: rows to chain");
        assert_eq!(
            chained.quads_vec().await.unwrap(),
            merged,
            "{tag}: the rows"
        );
    }
}

/// A window that keeps every row of its view's run but cuts the append tail
/// is a narrowing like any other: a pattern chained on it is matched over
/// the copy's columns and stays served, its rows in the index's order (the
/// default graph's, every base row's, in `index:posg`'s (o, s) order).
#[tokio::test]
async fn chained_match_on_a_window_of_the_whole_run_stays_served() {
    let (p2, default) = (iri("p2"), GraphName::DefaultGraph);
    for (residency, _dir, store) in served_stores().await {
        let want = matched_view(&store, None, Some(&p2), None, None)
            .await
            .quads_vec()
            .await
            .unwrap();
        let store = store.add_quads(tail_quads()).await.unwrap();
        let p = matched_view(&store, None, Some(&p2), None, None).await;
        let whole_run = p.window(0, want.len()).await.unwrap();
        assert_eq!(
            whole_run.quads_vec().await.unwrap(),
            want,
            "{residency}: the run"
        );
        let chained = matched_view(&whole_run, None, None, None, Some(&default)).await;
        assert_eq!(
            served_component(&chained),
            Some("index:posg"),
            "{residency}: served"
        );
        assert_eq!(
            chained.quads_vec().await.unwrap(),
            want,
            "{residency}: index order"
        );
    }
}
