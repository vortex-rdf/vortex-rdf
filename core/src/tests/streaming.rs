//! Materialized and shared-term reads: `quads_vec` against the stream, and
//! `shared_quads_vec` / `shared_quad_chunks` under every layout and view.

use super::*;

// ─── Materialized reads ────────────────────────────────────────────────

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for the plain
/// store, a matched view, and a mutated store with a tail — building
/// `streams` chunk streams for it: none in memory, where the view decodes
/// as one chunk, and one on file.
async fn run_quads_vec_matches_stream(store: VortexRdfStore, kind: &str, streams: usize) {
    use crate::store::read::quads::chunk_stream_hooks;

    let p0 = iri("p0");
    let extra = make_quad(
        "http://example.org/tail-subject",
        "http://example.org/p0",
        "tail object",
        GraphName::DefaultGraph,
    );
    let views = [
        ("full", store.clone()),
        (
            "matched",
            matched_view(&store, None, Some(&p0), None, None).await,
        ),
        ("tailed", store.add_quad(extra).await.unwrap()),
    ];
    for (tag, view) in views {
        let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
        let built = chunk_stream_hooks::streams();
        let collected = view.quads_vec().await.unwrap();
        assert_eq!(
            chunk_stream_hooks::streams() - built,
            streams,
            "{kind}: {tag}: chunk streams quads_vec built"
        );
        assert_eq!(collected, streamed, "{kind}: {tag}");
    }
}

#[tokio::test]
async fn test_quads_vec_matches_stream_collection() {
    run_quads_vec_matches_stream(
        built_store(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![]).await,
        "in memory",
        0,
    )
    .await;
}

#[tokio::test]
async fn test_quads_vec_matches_stream_collection_file_backed() {
    let (_dir, path) =
        write_store_file(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    run_quads_vec_matches_stream(store, "file", 1).await;
}

// ─── Shared-term reads ─────────────────────────────────────────────────

/// `shared_quads_vec` yields `quads_vec`'s rows under every layout, for the
/// whole store and for a matched view, and its terms parse back to the same
/// quads.
#[tokio::test]
async fn test_shared_quads_match_quads_vec_across_layouts() {
    for layout in LAYOUTS {
        let store = built_store(dictionary_test_quads(), layout, vec![]).await;
        assert_shared_matches_quads(&store, &format!("{layout:?}")).await;
        let p0 = iri("p0");
        let matched = matched_view(&store, None, Some(&p0), None, None).await;
        assert_shared_matches_quads(&matched, &format!("{layout:?} matched")).await;
    }
}

/// Under the Dictionary layout a term repeated down a column is decoded once
/// and shared: rows carrying the same predicate or graph hold one allocation.
#[tokio::test]
async fn test_shared_quads_share_repeated_terms() {
    let store = built_store(modular_quads(64, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
    let rows = store.shared_quads_vec().await.unwrap();
    assert_eq!(rows.len(), 64);
    for a in &rows {
        for b in &rows {
            if a.p == b.p {
                assert!(
                    std::sync::Arc::ptr_eq(&a.p, &b.p),
                    "equal predicates must share one allocation"
                );
            }
            if a.g == b.g {
                assert!(
                    std::sync::Arc::ptr_eq(&a.g, &b.g),
                    "equal graphs must share one allocation"
                );
            }
        }
    }
}

/// An in-memory index-served match reads its shared rows off the serving
/// index's columns, and a tombstoned derivative off the gather path; both
/// must agree with `quads_vec`.
#[tokio::test]
async fn test_shared_quads_match_quads_vec_on_served_view() {
    let quads = modular_quads(64, 3, 4);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let p0 = iri("p0");
    let served = matched_view(&store, None, Some(&p0), None, None).await;
    assert_eq!(
        served_component(&served),
        Some("index:posg"),
        "the P match must be index-served"
    );
    let rows = assert_shared_matches_quads(&served, "served").await;
    assert_eq!(rows.len(), 22);

    let tombstoned = matched_view(
        &store.delete_quad(&quads[0]).await.unwrap(),
        None,
        Some(&p0),
        None,
        None,
    )
    .await;
    let rows = assert_shared_matches_quads(&tombstoned, "served, tombstoned").await;
    assert_eq!(rows.len(), 21);
}

/// Every view shape a file-backed store can take — the whole store, an
/// index-served match, a subject-narrowed match, an append tail, a
/// tombstone — decodes the same rows shared as owned.
#[tokio::test]
async fn test_shared_quads_match_quads_vec_on_views() {
    let quads = modular_quads(64, 3, 4);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p0 = iri("p0");
    let s1 = iri_subject("s01");
    let extra = make_quad(
        "http://example.org/tail-subject",
        "http://example.org/p0",
        "tail object",
        GraphName::DefaultGraph,
    );
    let served = matched_view(&store, None, Some(&p0), None, None).await;
    assert_eq!(
        served_component(&served),
        Some("index:posg"),
        "the P match must be index-served"
    );
    let views = [
        ("full", store.clone()),
        ("served", served),
        (
            "subject",
            matched_view(&store, Some(&s1), None, None, None).await,
        ),
        ("tailed", store.add_quad(extra).await.unwrap()),
        ("tombstoned", store.delete_quad(&quads[0]).await.unwrap()),
    ];
    for (tag, view) in views {
        assert_shared_matches_quads(&view, tag).await;
    }
}

/// Shared rows own their strings: held across a compaction that rebuilds
/// the dictionary (renumbering every code), they still read the terms they
/// were decoded with.
#[tokio::test]
async fn test_shared_quads_survive_compaction() {
    let store = built_store(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let held = store.shared_quads_vec().await.unwrap();
    let before = shared_tuple_rows(&held);

    // A term sorting before every existing one shifts every code.
    let early = make_quad(
        "http://example.org/a-first",
        "http://example.org/a-first",
        "a first",
        GraphName::DefaultGraph,
    );
    let compacted = store
        .add_quad(early)
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    let probe = "<http://example.org/p0>";
    assert_ne!(
        store.dict_reader().unwrap().encode(probe).await.unwrap(),
        compacted
            .dict_reader()
            .unwrap()
            .encode(probe)
            .await
            .unwrap(),
        "compaction must have renumbered the dictionary"
    );
    assert_eq!(compacted.size().await.unwrap(), 11);
    assert_eq!(shared_tuple_rows(&held), before);
}
