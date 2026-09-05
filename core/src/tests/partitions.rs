//! Partitions: disjoint views that together cover a view's rows.

use std::num::NonZeroUsize;

use super::*;
use crate::store::Keep;

async fn spelled(view: &VortexRdfStore) -> Vec<String> {
    view.quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| q.to_string())
        .collect()
}

/// Over every shape an in-memory view takes, the partitions' rows
/// concatenated are the view's rows in its order, their sizes sum to its
/// size, a served view's partitions keep its plan, and the tail rides with
/// the last partition alone.
#[tokio::test]
async fn partitions_cover_the_view_in_order() {
    let quads = modular_quads(600, 3, 5);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let served = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(served.debug_has_serve_plan());
    let ranged = store.window(100, 250).await.unwrap();
    assert!(ranged.debug_selection_range().is_some());
    let ids = store
        .keep(
            QuadColumn::O,
            &Keep::set([dict.encode("\"object 1\"").unwrap()]),
        )
        .await
        .unwrap();
    let tailed = store
        .add_quads([make_quad(
            "http://example.org/tail",
            "http://example.org/p1",
            "x",
            GraphName::DefaultGraph,
        )])
        .await
        .unwrap();
    let deleted = store.delete_quad(&quads[7]).await.unwrap();
    for (tag, view) in [
        ("all", &store),
        ("served", &served),
        ("range", &ranged),
        ("ids", &ids),
        ("tailed", &tailed),
        ("deleted", &deleted),
    ] {
        let all = spelled(view).await;
        assert!(!all.is_empty(), "{tag}");
        for count in [1, 3, 8] {
            let parts = view
                .partitions(NonZeroUsize::new(count).unwrap())
                .await
                .unwrap();
            assert_eq!(
                parts.len(),
                count,
                "{tag}/{count}: exactly `count` partitions"
            );
            let mut rows = Vec::new();
            let mut sizes = 0;
            for part in &parts {
                rows.extend(spelled(part).await);
                sizes += part.size().await.unwrap();
            }
            assert_eq!(rows, all, "{tag}/{count}: the view's rows, in its order");
            assert_eq!(sizes, all.len(), "{tag}/{count}: the sizes sum");
            if tag == "served" && count > 1 {
                assert!(
                    parts.iter().all(|part| part.debug_has_serve_plan()),
                    "a served view's partitions keep its plan"
                );
            }
            if tag == "tailed" {
                let last = parts.last().unwrap();
                assert!(
                    spelled(last).await.last().unwrap().contains("tail"),
                    "the tail rides last"
                );
            }
        }
    }
}

/// A file view's partitions cover its rows too — a pushed-down filter with
/// each of them, a served run's rows read from the base in its order.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn file_partitions_cover_the_view() {
    let quads = modular_quads(600, 3, 5);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let served = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let (lo, hi) = dict.prefix_range("\"object 1");
    let filtered = store
        .keep(QuadColumn::O, &Keep::range(lo, hi))
        .await
        .unwrap();
    for (tag, view) in [
        ("all", &store),
        ("served", &served),
        ("filtered", &filtered),
    ] {
        let mut all = spelled(view).await;
        all.sort();
        for count in [1, 4] {
            let parts = view
                .partitions(NonZeroUsize::new(count).unwrap())
                .await
                .unwrap();
            assert_eq!(parts.len(), count);
            let mut rows = Vec::new();
            let mut sizes = 0;
            for part in &parts {
                rows.extend(spelled(part).await);
                sizes += part.size().await.unwrap();
            }
            rows.sort();
            assert_eq!(rows, all, "{tag}/{count}: the view's rows");
            assert_eq!(sizes, all.len(), "{tag}/{count}: the sizes sum");
        }
    }
}
