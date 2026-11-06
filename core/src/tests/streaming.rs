//! Materialized and shared-term reads: `quads_vec` against the stream,
//! `shared_quads_vec` / `shared_quad_chunks` under every layout and view,
//! and an in-memory view's streams in chunks of bounded rows.

use super::*;
use crate::store::test_hooks::ForcedChunkRows;

// ─── Materialized reads ────────────────────────────────────────────────

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for the plain
/// store, a matched view, and a mutated store with a tail — building
/// `streams` chunk streams for it: none in memory, where the view decodes
/// as one chunk, and one on file.
async fn run_quads_vec_matches_stream(store: VortexRdfStore, kind: &str, streams: usize) {
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
        let rec = Recorder::start();
        let collected = view.quads_vec().await.unwrap();
        assert_eq!(
            rec.count(&Event::ChunkStream),
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

/// Under the string layouts each term is read in place and shared down a
/// run of rows repeating it: a subject's rows hold one allocation of it, as
/// do the runs of an object (a typed literal the TypedObject layout spells
/// from its sub-columns) and of the graph, while the predicate, which
/// changes from row to row, is one allocation per row. The rows are
/// `quads_vec`'s.
#[tokio::test]
async fn test_shared_quads_share_runs_under_the_string_layouts() {
    use std::collections::HashSet;
    use std::sync::Arc;

    let quads: Vec<Quad> = (0..4)
        .flat_map(|s| {
            (0..3).map(move |p| {
                Quad::new(
                    iri_subject(&format!("s{s}")),
                    iri(&format!("p{p}")),
                    Term::Literal(Literal::new_typed_literal(
                        format!("{s}"),
                        oxrdf::vocab::xsd::INTEGER,
                    )),
                    iri_graph("g"),
                )
            })
        })
        .collect();
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = built_store(quads.clone(), layout, vec![]).await;
        let rows = store.shared_quads_vec().await.unwrap();
        assert_shared_matches_quads(&store, &format!("{layout:?}")).await;
        let allocations = |role: fn(&crate::store::SharedQuad) -> &Arc<str>| {
            rows.iter()
                .map(|row| Arc::as_ptr(role(row)) as *const u8)
                .collect::<HashSet<_>>()
                .len()
        };
        assert_eq!(allocations(|row| &row.s), 4, "{layout:?}: subjects");
        assert_eq!(allocations(|row| &row.p), 12, "{layout:?}: predicates");
        assert_eq!(allocations(|row| &row.o), 4, "{layout:?}: objects");
        assert_eq!(allocations(|row| &row.g), 1, "{layout:?}: graphs");
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

// ─── Bounded in-memory streams ─────────────────────────────────────────

/// The N-Quads bytes [`export_rdf`](crate::export_rdf) writes of `view`.
async fn nquads_export(view: &VortexRdfStore) -> Vec<u8> {
    let mut out = Vec::new();
    crate::export_rdf(view.clone(), &mut out, oxrdfio::RdfFormat::NQuads)
        .await
        .unwrap();
    out
}

/// Under [`ForcedChunkRows::set`]`(rows)` an in-memory `view` streams more
/// than one chunk, none of more than `rows` rows, and its streams read the
/// rows of its one-chunk reads: `shared_quad_chunks` concatenates to
/// `shared_quads_vec`, `quads()` collects to `quads_vec`, and the N-Quads
/// export writes the bytes it writes at the production cap.
async fn assert_bounded_chunks(view: &VortexRdfStore, rows: usize, tag: &str) {
    let (shared, quads) = (
        view.shared_quads_vec().await.unwrap(),
        view.quads_vec().await.unwrap(),
    );
    let lines = nquads_export(view).await;
    let _forced = ForcedChunkRows::set(rows);
    let chunks: Vec<Vec<_>> = view.shared_quad_chunks().collect().await;
    let sizes: Vec<usize> = chunks.iter().map(Vec::len).collect();
    assert!(
        sizes.len() > 1 && sizes.iter().all(|&n| n <= rows),
        "{tag}: {sizes:?}"
    );
    let chunked: Vec<crate::store::SharedQuad> =
        chunks.into_iter().flatten().map(|r| r.unwrap()).collect();
    assert_eq!(chunked, shared, "{tag}: shared_quad_chunks");
    let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
    assert_eq!(streamed, quads, "{tag}: quads()");
    assert_eq!(nquads_export(view).await, lines, "{tag}: export");
}

/// Every shape of in-memory view streams chunks of at most the forced rows,
/// which concatenate to its one-chunk reads, under every layout: the whole
/// store and a window of it (a run, sliced), a match and a tombstoned store
/// (live ids, gathered a window at a time) and a tail after the base's
/// rows; and an index-served match, a window of it, its tombstones, its tail
/// and (under the Dictionary layout) a keep of it, the served rows read off
/// the index's run a window at a time.
#[tokio::test]
async fn test_in_memory_chunks_concatenate_to_the_whole_view() {
    let quads = modular_quads(64, 3, 4);
    let late: Vec<Quad> = (0..10)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p0",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let p0 = iri("p0");
    for layout in LAYOUTS {
        let plain = built_store(quads.clone(), layout, vec![]).await;
        let indexed = built_store(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let served = matched_view(&indexed, None, Some(&p0), None, None).await;
        let mut views = vec![
            // A run of 64.
            ("whole", plain.clone()),
            // A run of 40.
            ("window", plain.window(3, 40).await.unwrap()),
            // 22 ids.
            (
                "matched",
                matched_view(&plain, None, Some(&p0), None, None).await,
            ),
            // 59 live ids.
            ("tombstoned", delete_all(&plain, &quads[..5]).await),
            // 64 rows, then 10.
            ("tailed", plain.add_quads(late.clone()).await.unwrap()),
            // A run of 22.
            ("served", served.clone()),
            ("served window", served.window(3, 15).await.unwrap()),
            (
                "served tombstoned",
                matched_view(
                    &delete_all(&indexed, &quads[..1]).await,
                    None,
                    Some(&p0),
                    None,
                    None,
                )
                .await,
            ),
            (
                "served tailed",
                matched_view(
                    &indexed.add_quads(late.clone()).await.unwrap(),
                    None,
                    Some(&p0),
                    None,
                    None,
                )
                .await,
            ),
        ];
        if layout == LayoutStrategy::Dictionary {
            // The kept rows: 11 of the 22.
            let codes = indexed
                .dict_reader()
                .unwrap()
                .encode_many(&["\"object 0\"", "\"object 2\""])
                .await
                .unwrap();
            views.push((
                "served keep",
                served
                    .keep(QuadColumn::O, &Keep::set(codes.into_iter().flatten()))
                    .await
                    .unwrap(),
            ));
        }
        for (name, view) in &views {
            let tag = format!("{layout:?} {name}");
            assert_eq!(
                served_component(view).is_some(),
                name.starts_with("served"),
                "{tag}"
            );
            assert_bounded_chunks(view, 7, &tag).await;
        }
    }
}

/// An in-memory chunk stream reads and decodes one chunk a pull, and no
/// pull suspends, as the JS bindings poll it (`now_or_never`): nothing is
/// read before the first poll, then each pull reads the chunk it yields, for
/// the whole store's run (sliced) and for an index-served match's (read off
/// the index's run).
#[tokio::test]
async fn test_in_memory_chunk_stream_decodes_a_chunk_a_pull() {
    use futures::FutureExt as _;
    let store = built_store(
        modular_quads(64, 3, 4),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let served = matched_view(&store, None, Some(&iri("p0")), None, None).await;
    let _forced = ForcedChunkRows::set(7);
    let decoded = |rec: &Recorder| rec.count_where(|e| matches!(e, Event::InMemoryChunk(_)));
    for (tag, view, want) in [
        ("whole", &store, vec![7, 7, 7, 7, 7, 7, 7, 7, 7, 1]),
        ("served", &served, vec![7, 7, 7, 1]),
    ] {
        let rec = Recorder::start();
        let mut chunks = view.shared_quad_chunks();
        assert_eq!(
            decoded(&rec),
            0,
            "{tag}: nothing read before the first poll"
        );
        let mut sizes = Vec::new();
        while let Some(chunk) = chunks
            .next()
            .now_or_never()
            .expect("an in-memory pull never suspends")
        {
            sizes.push(chunk.len());
            assert_eq!(decoded(&rec), sizes.len(), "{tag}: one chunk a pull");
        }
        assert_eq!(sizes, want, "{tag}");
    }
}

/// At the production cap an in-memory view's chunks hold at most 65,536
/// rows: a store of 160,000 rows streams two full chunks and the rest, a
/// tail one more chunk after them, and the chunks concatenate to the view's
/// one-chunk read.
#[tokio::test]
async fn test_in_memory_chunks_hold_at_most_65_536_rows() {
    // 160,000 rows, the file's bytes written once per process.
    let (_dir, path) = multi_window_dictionary_file().await;
    let store = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let tailed = store.add_quads(appended_quads(3)).await.unwrap();
    for (tag, view, want) in [
        ("whole", &store, vec![65_536, 65_536, 28_928]),
        ("tailed", &tailed, vec![65_536, 65_536, 28_928, 3]),
    ] {
        let chunks: Vec<Vec<_>> = view.shared_quad_chunks().collect().await;
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            want,
            "{tag}"
        );
        let rows: Vec<crate::store::SharedQuad> =
            chunks.into_iter().flatten().map(|r| r.unwrap()).collect();
        assert_eq!(rows, view.shared_quads_vec().await.unwrap(), "{tag}");
    }
}
