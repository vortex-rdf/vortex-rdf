//! Planner-facing metadata: generations and dictionary identities across
//! views and mutations, view statistics against the gathered rows, the
//! index child roster, and partitions that tile a view.

use std::num::NonZeroUsize;

use super::*;
use crate::store::{Keep, QuadColumn, RowCountHint, SelectionKind, SortOrder};
use crate::tests::chunks::base_ordered_codes;

fn fixture_quads() -> Vec<Quad> {
    let g = |name: &str| {
        GraphName::NamedNode(NamedNode::new(format!("http://example.org/{name}")).unwrap())
    };
    graph_modular_quads(3_000, 4, 5, 7, &[GraphName::DefaultGraph, g("g1"), g("g2")])
}

fn appended_quads(n: usize) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p9",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// Views share their store's generation and dictionary; mutations and
/// compaction change the generation, compaction the dictionary too.
#[tokio::test]
async fn test_generation_and_dictionary_identity() {
    let quads = fixture_quads();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let other = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    assert_ne!(
        store.generation(),
        other.generation(),
        "two builds, two generations"
    );
    let dict_id = store.dict_reader().unwrap().dictionary_id();
    assert_ne!(dict_id, other.dict_reader().unwrap().dictionary_id());

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let kept = view
        .keep(QuadColumn::S, &Keep::range(0..u32::MAX))
        .await
        .unwrap();
    let windowed = kept.window(3, 10).await.unwrap();
    let parts = store
        .partitions(NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    for (name, v) in [
        ("match", &view),
        ("keep", &kept),
        ("window", &windowed),
        ("partition", &parts[1]),
    ] {
        assert_eq!(
            v.generation(),
            store.generation(),
            "{name}: a view keeps the generation"
        );
        assert_eq!(
            v.dict_reader().unwrap().dictionary_id(),
            dict_id,
            "{name}: one vocabulary"
        );
        assert_eq!(
            v.dict_reader().unwrap().snapshot().unwrap().dictionary_id(),
            dict_id,
            "{name}: snapshot too"
        );
    }
    assert_eq!(store.empty_view().generation(), store.generation());

    let tailed = store.add_quads(appended_quads(5)).await.unwrap();
    assert_ne!(
        tailed.generation(),
        store.generation(),
        "an append is new data"
    );
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert_ne!(
        deleted.generation(),
        store.generation(),
        "a delete is new data"
    );
    assert_ne!(deleted.generation(), tailed.generation());
    assert_eq!(
        deleted.dict_reader().unwrap().dictionary_id(),
        dict_id,
        "a delete keeps the dictionary"
    );

    let compacted = tailed.compact().await.unwrap();
    assert_ne!(
        compacted.generation(),
        tailed.generation(),
        "a compaction is new data"
    );
    assert_ne!(
        compacted.dict_reader().unwrap().dictionary_id(),
        dict_id,
        "a compaction re-encodes against a new dictionary"
    );
}

/// Statistics describe the view exactly where no read is pending, and as a
/// bound otherwise; code bounds follow subject runs and pushed-down
/// equalities.
#[tokio::test]
async fn test_view_statistics_in_memory() {
    let quads = fixture_quads();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let whole = store.view_statistics();
    assert_eq!(whole.rows, RowCountHint::exact(quads.len()));
    assert_eq!(whole.sort_order, Some(SortOrder::Spog));
    assert_eq!(whole.selection, SelectionKind::All);
    assert_eq!(whole.served_component, None);
    assert!(!whole.pending_filter && !whole.file_backed);
    assert_eq!((whole.tombstones, whole.tail_rows), (0, 0));
    assert_eq!(whole.generation, store.generation());
    let all = base_ordered_codes(&store).await;
    let (s_lo, s_hi) = (
        all.iter().map(|r| r[0]).min().unwrap(),
        all.iter().map(|r| r[0]).max().unwrap(),
    );
    assert_eq!(
        whole.code_bounds[0],
        Some((s_lo, s_hi)),
        "the sorted subject column's extremes"
    );
    assert_eq!(whole.code_bounds[1..], [None, None, None]);

    // A subject prefix search: one run, bounded by its one subject.
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0017").unwrap());
    let by_s = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    let stats = by_s.view_statistics();
    let s_code = store
        .dict_reader()
        .unwrap()
        .encode("<http://example.org/s0017>")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats.rows, RowCountHint::exact(by_s.size().await.unwrap()));
    assert_eq!(stats.selection, SelectionKind::Range);
    assert_eq!(stats.code_bounds[0], Some((s_code, s_code)));

    // A served predicate match: pending run, exact count, index named.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics();
    assert_eq!(stats.rows, RowCountHint::exact(by_p.size().await.unwrap()));
    assert!(
        stats.served_component.is_some(),
        "an index served the match"
    );
    assert_eq!(
        stats.sort_order,
        Some(SortOrder::Spog),
        "the exports still come in base order"
    );

    // Tombstones and a tail are counted, and the tail drops the sort claim's
    // bounds but not the count.
    let mut deleted = store.clone();
    for quad in quads.iter().step_by(50).take(20) {
        deleted = deleted.delete_quad(quad).await.unwrap();
    }
    let stats = deleted.view_statistics();
    assert_eq!(stats.tombstones, 20);
    assert_eq!(stats.rows, RowCountHint::exact(quads.len() - 20));
    assert_eq!(
        stats.code_bounds[0], None,
        "tombstones leave the run's extremes unknown"
    );
    let strings =
        VortexRdfStore::from_quads(quad_stream(quads.clone()), LayoutStrategy::Default, vec![])
            .await
            .unwrap()
            .add_quads(appended_quads(7))
            .await
            .unwrap();
    let stats = strings.view_statistics();
    assert_eq!(stats.tail_rows, 7);
    assert_eq!(stats.rows, RowCountHint::exact(quads.len() + 7));
    assert_eq!(
        stats.code_bounds, [None; 4],
        "no codes under a string layout"
    );
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_view_statistics_file_backed() {
    let quads = fixture_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let whole = store.view_statistics();
    assert_eq!(whole.rows, RowCountHint::exact(quads.len()));
    assert_eq!(whole.sort_order, Some(SortOrder::Spog));
    assert!(whole.file_backed && !whole.pending_filter);

    // Unindexed: a predicate match pushes a filter down — a bound, and the
    // bound column's code.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics();
    let p_code = store
        .dict_reader()
        .unwrap()
        .encode("<http://example.org/p1>")
        .await
        .unwrap()
        .unwrap();
    assert!(stats.pending_filter);
    assert_eq!(stats.rows.exact, None);
    assert!(stats.rows.upper_bound >= by_p.size().await.unwrap());
    assert_eq!(stats.code_bounds[1], Some((p_code, p_code)));

    // Indexed: the match is served, its count exact.
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let indexed = VortexRdfStore::from_file(&path).await.unwrap();
    let by_p = indexed
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics();
    assert_eq!(stats.served_component, Some("index:posg"));
    assert_eq!(stats.rows, RowCountHint::exact(by_p.size().await.unwrap()));
    assert!(!stats.pending_filter);
}

/// The index roster, in memory and on file.
#[tokio::test]
async fn test_index_components() {
    let quads = fixture_quads();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let infos = store.index_components();
    let names: Vec<&str> = infos.iter().map(|i| i.name).collect();
    assert_eq!(
        names,
        ["index:posg", "index:ospg", "index:ref-o", "index:ref-p"]
    );
    assert_eq!(infos[0].sort_order, Some(SortOrder::Posg));
    assert_eq!(infos[1].sort_order, Some(SortOrder::Ospg));
    assert_eq!(infos[2].sort_order, None);
    assert_eq!(infos[0].index, IndexType::SecondaryByCopy);
    assert_eq!(infos[3].index, IndexType::SecondaryByReference);
    assert!(
        infos
            .iter()
            .all(|i| i.sorted && i.resident && i.rows == Some(quads.len()))
    );
    assert!(
        VortexRdfStore::from_quads(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            vec![]
        )
        .await
        .unwrap()
        .index_components()
        .is_empty()
    );
    #[cfg(feature = "file-io")]
    {
        let (_dir, path) = write_store_file(
            quads.clone(),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference],
        )
        .await;
        let file = VortexRdfStore::from_file(&path).await.unwrap();
        let infos = file.index_components();
        let names: Vec<&str> = infos.iter().map(|i| i.name).collect();
        assert_eq!(names, ["index:ref-o", "index:ref-p"]);
        assert!(
            infos
                .iter()
                .all(|i| i.sorted && !i.resident && i.rows == Some(quads.len()))
        );
    }
}

/// Partitions tile a view: read in turn they give the view's rows, each
/// over a disjoint contiguous piece, the tail with the last.
async fn assert_partitions_tile(view: &VortexRdfStore, tag: &str) {
    let expected = base_ordered_codes(view).await;
    for n in [1usize, 2, 3, 7, 64, 100_000] {
        let parts = view
            .partitions(NonZeroUsize::new(n).unwrap())
            .await
            .unwrap();
        assert!(
            !parts.is_empty() && parts.len() <= n,
            "{tag}: {n} -> {}",
            parts.len()
        );
        let mut got = Vec::new();
        let mut total = 0;
        for part in &parts {
            got.extend(base_ordered_codes(part).await);
            total += part.size().await.unwrap();
        }
        assert_eq!(got, expected, "{tag}: partitions({n}) tile the view");
        assert_eq!(total, expected.len(), "{tag}: sizes add up");
    }
}

#[tokio::test]
async fn test_partitions_in_memory() {
    let quads = fixture_quads();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0017").unwrap());
    let mut deleted = store.clone();
    for quad in quads.iter().step_by(9).take(40) {
        deleted = deleted.delete_quad(quad).await.unwrap();
    }
    for (tag, view) in [
        ("whole", store.clone()),
        (
            "served",
            store
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap(),
        ),
        (
            "run",
            store
                .match_pattern(Some(&s), None, None, None)
                .await
                .unwrap(),
        ),
        ("window", store.window(10, 500).await.unwrap()),
        ("tombstoned", deleted.clone()),
        ("empty", store.window(10_000, 1).await.unwrap()),
    ] {
        assert_partitions_tile(&view, tag).await;
    }
    // A string layout's tail rides with the last partition.
    let strings =
        VortexRdfStore::from_quads(quad_stream(quads.clone()), LayoutStrategy::Default, vec![])
            .await
            .unwrap()
            .add_quads(appended_quads(7))
            .await
            .unwrap();
    let parts = strings
        .partitions(NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(parts.len(), 4);
    let sizes: Vec<usize> = {
        let mut v = Vec::new();
        for p in &parts {
            v.push(p.size().await.unwrap());
        }
        v
    };
    assert_eq!(sizes.iter().sum::<usize>(), quads.len() + 7);
    assert_eq!(parts[3].tail_size(), 7);
    assert_eq!(parts[0].tail_size(), 0);
    let mut subjects = Vec::new();
    for p in &parts {
        subjects.extend(
            p.quads_vec()
                .await
                .unwrap()
                .into_iter()
                .map(|q| q.subject.to_string()),
        );
    }
    let expected: Vec<String> = strings
        .quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| q.subject.to_string())
        .collect();
    assert_eq!(subjects, expected);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_partitions_file_backed() {
    let quads = fixture_quads();
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o3 = Term::Literal(Literal::new_simple_literal("o3"));
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(13).take(30) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        for (tag, view) in [
            ("whole", store.clone()),
            (
                "predicate",
                store
                    .match_pattern(None, Some(&p1), None, None)
                    .await
                    .unwrap(),
            ),
            (
                "object",
                store
                    .match_pattern(None, None, Some(&o3), None)
                    .await
                    .unwrap(),
            ),
            ("window", store.window(10, 500).await.unwrap()),
            ("tombstoned", deleted.clone()),
        ] {
            assert_partitions_tile(&view, tag).await;
        }
    }
}
