use super::*;
use std::sync::OnceLock;

// ─── 6) File-backed edge behavior ──────────────────────────────────────

#[tokio::test]
async fn test_file_backed_filtered_size() {
    // 20 quads alternating between two predicates (10 each).
    let quads: Vec<Quad> = (0..20)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 2),
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let (_dir, path) = write_store_file(quads, LayoutStrategy::Default, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 20);

    // size() on a filtered file-backed store must report the *filtered*
    // count, not file.row_count().
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 10);
}

#[tokio::test]
async fn test_file_backed_secondary_index_object_predicate() {
    let quads: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_object = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_object.size().await.unwrap(), 6);

    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_predicate = store
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    assert_eq!(by_predicate.size().await.unwrap(), 10);

    let by_both = store
        .match_pattern(None, Some(&p2), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_both.size().await.unwrap(), 2);
}

/// Regression: predicate matches whose zone-map hits are NOT contiguous
/// (clusters at both ends of an s-sorted file, with a middle zone whose
/// stats exclude the predicate) must all survive the metadata row-range
/// pre-pass. A first-gap cutoff would silently drop the trailing cluster.
#[tokio::test]
async fn test_file_backed_non_contiguous_predicate_matches() {
    // Three 8192-row zones. The rare predicate appears only in the first
    // and last 64 rows; the middle zone holds exclusively the common
    // predicate (lexicographically greater), so its zone map excludes the
    // rare one and creates an interior prunable gap.
    const N: usize = 3 * 8192;
    const CLUSTER: usize = 64;

    // The serialize of the 24,576-row fixture dominates this test's
    // runtime, so its bytes are built once per process.
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        let quads: Vec<Quad> = (0..N)
            .map(|i| {
                let p = if !(CLUSTER..N - CLUSTER).contains(&i) {
                    "http://example.org/pAAA"
                } else {
                    "http://example.org/pMMM"
                };
                make_quad(
                    &format!("http://example.org/s{:06}", i),
                    p,
                    "o",
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        bytes
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("noncontig.vortex");
    std::fs::write(&path, bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p_rare = NamedNode::new("http://example.org/pAAA").unwrap();
    let matched = store
        .match_pattern(None, Some(&p_rare), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 2 * CLUSTER);

    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), 2 * CLUSTER);
    // Both the leading and the trailing cluster must be present.
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == "<http://example.org/s000000>")
    );
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == format!("<http://example.org/s{:06}>", N - 1))
    );
}

/// Chained matches on a multi-zone file: a subject match narrows the store
/// to a row range from the zone maps; a subsequent indexed object match
/// must restrict its index row ids to that range (not discard the index),
/// and index-to-index chaining must intersect the two id lists.
#[tokio::test]
async fn test_file_backed_chained_subject_then_object_index() {
    const N: usize = 3 * 8192;

    // Built once per process, like the non-contiguous fixture above — the
    // two shapes differ (this one needs the reference index and modular
    // predicates), so they cannot share one file.
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        let quads: Vec<Quad> = (0..N)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:06}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("o{}", i % 5),
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        bytes
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chained.vortex");
    std::fs::write(&path, bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // The sorted subject column narrows a bound subject to a sub-range of
    // the file via zone-map pruning (row 12000 lives in the middle zone).
    let s_mid = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s012000").unwrap());
    let range = store.debug_subject_row_range(&s_mid).await.unwrap();
    let range = range.expect("sorted multi-zone file must narrow a bound subject");
    assert!(range.start <= 12000 && 12000 < range.end);
    assert!(
        range.end - range.start < N as u64,
        "envelope must exclude other zones"
    );

    // Subject match first (row range), then indexed object match: the
    // object index ids are restricted to the subject's range.
    let by_subject = store
        .match_pattern(Some(&s_mid), None, None, None)
        .await
        .unwrap();
    let o0 = Term::Literal(Literal::new_simple_literal("o0"));
    let chained = by_subject
        .match_pattern(None, None, Some(&o0), None)
        .await
        .unwrap();
    assert_eq!(chained.size().await.unwrap(), 1); // 12000 % 5 == 0
    let results: Vec<Quad> = chained.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].subject.to_string(),
        "<http://example.org/s012000>"
    );
    assert_eq!(results[0].object.to_string(), "\"o0\"");

    // Same chain with an object the subject's row doesn't carry.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let empty = by_subject
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    // Index-to-index chaining: object ids ∩ predicate ids.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let step1 = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    let step2 = step1
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    let expected = (0..N).filter(|i| i % 5 == 1 && i % 3 == 2).count();
    assert_eq!(step2.size().await.unwrap(), expected);
    let results: Vec<Quad> = step2.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results.len(), expected);
    assert!(
        results.iter().all(|q| {
            q.predicate == "http://example.org/p2" && q.object.to_string() == "\"o1\""
        })
    );
}

#[tokio::test]
async fn test_file_backed_subject_metadata_range_for_missing_subject() {
    let mut quads = modular_quads(25, 1, 1);
    quads.reverse();

    let (_dir, path) = write_store_file(quads, LayoutStrategy::Default, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let missing = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
    let row_range = store.debug_subject_row_range(&missing).await.unwrap();
    assert_eq!(row_range, Some(0..0));
    assert_eq!(
        store
            .match_pattern(Some(&missing), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
}

// ─── Bound-subject chunk-probe fast path ───────────────────────────────

/// Sorted Dictionary quads with 3-quad subject runs — the shape the encoded
/// chunk-probe fast path serves.
fn probe_path_quads(n_subjects: usize) -> Vec<Quad> {
    (0..n_subjects * 3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:04}", i / 3),
                &format!("http://example.org/p{}", i % 5),
                &format!("object {}", i % 9),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

fn subject(i: usize) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(NamedNode::new(format!("http://example.org/s{i:04}")).unwrap())
}

/// The fast path engages on a sorted Dictionary file — the debug hook
/// reports the exact run — and every subject-bound shape answers exactly
/// like the in-memory store over the same quads: first/mid/last subjects,
/// absent subjects, and [S]/[SP]/[SPO]/[SG] residual composition.
#[tokio::test]
async fn test_file_subject_probe_matches_memory() {
    let quads = probe_path_quads(2000);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let memory = VortexRdfStore::from_built(arr).unwrap();

    // Engagement, pinned directly: the exact 3-row run for a mid subject.
    let mid = subject(777);
    let range = store.debug_subject_bounds_range(&mid).await.unwrap();
    assert_eq!(range, Some(777 * 3..777 * 3 + 3), "fast path must engage");

    let p = NamedNode::new("http://example.org/p1").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 3"));
    let g = GraphName::DefaultGraph;
    for s in [subject(0), subject(777), subject(1999)] {
        for (pp, po, pg) in [
            (None, None, None),
            (Some(&p), None, None),
            (Some(&p), Some(&o), None),
            (None, None, Some(&g)),
        ] {
            let got = store.match_pattern(Some(&s), pp, po, pg).await.unwrap();
            let want = memory.match_pattern(Some(&s), pp, po, pg).await.unwrap();
            assert_eq!(
                view_strings(&got).await,
                view_strings(&want).await,
                "diverged on subject {s} pattern ({pp:?}, {po:?}, {pg:?})"
            );
        }
    }

    // Absent subject (never in the dictionary) short-circuits to empty.
    let absent = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/zz").unwrap());
    let empty = store
        .match_pattern(Some(&absent), None, None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// Tombstones and tails compose with the probed range: a deleted quad
/// leaves the run, an added quad joins it through the tail.
#[tokio::test]
async fn test_file_subject_probe_with_mutation() {
    let quads = probe_path_quads(100);
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let s50 = subject(50);
    let doomed = quads[50 * 3].clone();
    let store = store.delete_quad(&doomed).await.unwrap();
    let matched = store
        .match_pattern(Some(&s50), None, None, None)
        .await
        .unwrap();
    assert_eq!(
        matched.size().await.unwrap(),
        2,
        "tombstone must leave the run"
    );

    let fresh = make_quad(
        "http://example.org/s0050",
        "http://example.org/pnew",
        "fresh",
        GraphName::DefaultGraph,
    );
    let store = store.add_quads([fresh]).await.unwrap();
    let matched = store
        .match_pattern(Some(&s50), None, None, None)
        .await
        .unwrap();
    assert_eq!(
        matched.size().await.unwrap(),
        3,
        "the tail must rejoin the run"
    );
}

/// A chained match composes: narrowing by predicate first, then by subject,
/// must equal the in-memory result — and the second match must not attach a
/// serve plan (the view already carries a restriction).
#[tokio::test]
async fn test_file_subject_probe_chained() {
    let quads = probe_path_quads(500);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let memory = VortexRdfStore::from_built(arr).unwrap();

    let p = NamedNode::new("http://example.org/p2").unwrap();
    let s = subject(444);
    let got_first = store
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    let got = got_first
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert!(!got.debug_has_serve_plan());
    let want = memory
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap()
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert_eq!(view_strings(&got).await, view_strings(&want).await);
}

/// A file whose rows are not globally sorted must NOT engage the fast path —
/// no sortedness, no binary search — while subject matches stay correct
/// through the scan path.
///
/// The unsorted file comes from serializing a store that still carries an
/// append tail: the tail's rows are interleaved into the written order, so
/// the wire records no sorted provenance and the reread store's `s` column
/// carries no stamp.
#[tokio::test]
async fn test_file_subject_probe_requires_sorted() {
    let quads = probe_path_quads(50);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let tailed = VortexRdfStore::from_built(arr)
        .unwrap()
        .add_quad(make_quad(
            "http://example.org/fresh",
            "http://example.org/p0",
            "object 0",
            GraphName::DefaultGraph,
        ))
        .await
        .unwrap();
    let bytes = tailed.to_bytes().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unsorted.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let s = subject(10);
    assert_eq!(store.debug_subject_bounds_range(&s).await.unwrap(), None);
    let matched = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 3);
}
