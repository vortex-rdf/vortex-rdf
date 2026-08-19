use super::*;

// ─── 2) Core in-memory query semantics (no file I/O) ───────────────────

async fn run_match_pattern_test<B: VortexArrayBuilder>() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let arr = build_array::<B>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);

    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let empty = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_match_sorted_in_memory() {
    run_match_pattern_test::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_match_sorted_stream() {
    run_match_pattern_test::<SortedStreamBuilder>().await;
}

#[tokio::test]
async fn test_match_typed_object_layout() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );
    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::TypedObject);

    // Match by object literal — exercises the typed o_kind/o_value columns.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let matched = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 1);
    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), q1.subject.to_string());
    assert_eq!(results[0].object.to_string(), q1.object.to_string());

    // Match by predicate.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let matched_p = store
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    assert_eq!(matched_p.size().await.unwrap(), 1);

    // Non-existent object yields nothing.
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let empty = store
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_sorted_subject_binary_search() {
    // Multiple quads per subject: the binary-search fast path must return
    // the full [lo, hi) range for the matched subject.
    let mut quads: Vec<Quad> = Vec::new();
    for i in (0..10).rev() {
        for p in ["http://example.org/p1", "http://example.org/p2"] {
            quads.push(make_quad(
                &format!("http://example.org/s{:02}", i),
                p,
                "o",
                GraphName::DefaultGraph,
            ));
        }
    }

    let arr =
        build_array::<SortedInMemoryBuilder>(quad_stream(quads), LayoutStrategy::Default, vec![])
            .await
            .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let matched = store
        .match_pattern(Some(&s5), None, None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 2);

    // Subject + predicate narrows within the sliced range.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched_sp = store
        .match_pattern(Some(&s5), Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(matched_sp.size().await.unwrap(), 1);

    // Missing subject → empty via binary search short-circuit.
    let s99 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
    let empty = store
        .match_pattern(Some(&s99), None, None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
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

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

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

// ─── 2b) File-backed matching matrix (by layout) ───────────────────────

/// One cell of the 3-layout matrix: write the layout's dataset to a file,
/// open it, and hand the store to the layout's `probe` — the probes below
/// each bind the term family their layout represents differently on disk.
#[cfg(feature = "file-io")]
async fn run_match_pattern_file_test<F, Fut>(layout: LayoutStrategy, quads: Vec<Quad>, probe: F)
where
    F: FnOnce(VortexRdfStore) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (_dir, path) = write_store_file(quads, layout, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    probe(store).await;
}

/// The two-quad dataset the Default and TypedObject probes match over.
#[cfg(feature = "file-io")]
fn two_quads() -> Vec<Quad> {
    vec![
        make_quad(
            "http://example.org/s1",
            "http://example.org/p1",
            "o1",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s2",
            "http://example.org/p2",
            "o2",
            GraphName::DefaultGraph,
        ),
    ]
}

/// Default layout: a bound predicate over the string columns, hit and miss.
#[cfg(feature = "file-io")]
async fn probe_default(store: VortexRdfStore) {
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");

    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let empty = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// TypedObject layout: a bound object literal, so the match runs over the
/// typed o_kind/o_value columns.
#[cfg(feature = "file-io")]
async fn probe_typed_object(store: VortexRdfStore) {
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let filtered = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");

    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let empty = store
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// Dictionary layout (over `dictionary_test_quads`): a pushed-down integer
/// filter on the code columns, and a term absent from the dictionary.
#[cfg(feature = "file-io")]
async fn probe_dictionary(store: VortexRdfStore) {
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 4);

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_default() {
    run_match_pattern_file_test(LayoutStrategy::Default, two_quads(), probe_default).await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_typed_object() {
    run_match_pattern_file_test(LayoutStrategy::TypedObject, two_quads(), probe_typed_object).await;
}
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_match_file_dictionary() {
    run_match_pattern_file_test(
        LayoutStrategy::Dictionary,
        dictionary_test_quads(),
        probe_dictionary,
    )
    .await;
}
