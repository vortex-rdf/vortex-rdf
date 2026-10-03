//! Secondary indexes on file-backed stores: tombstoning over a file, the
//! copy family's served reads by layout, run location through
//! dictionary-coded index children, and the reference family's file
//! resolution.

use super::indexes::{ServeExpectations, copy_index_script_dataset, run_copy_index_serving_script};
use super::matching::{above_gate_quads, run_subject_range_then_index_routing_above_gate};
use super::*;

// ─── Deletes over a file ───────────────────────────────────────────────

/// A file's rows are tombstoned in place too, so deleting from a file-backed
/// store keeps its secondary indexes usable and never rewrites the file —
/// covering both the index-resolved delete path and the filter-scan one.
#[tokio::test]
async fn test_file_backed_delete_keeps_indexes() {
    let (_dir, path) = write_store_file(
        modular_quads(12, 2, 3),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Index-resolved delete: "object 0" is indexed, so this resolves to
    // exact file row ids (i = 0, 3, 6, 9) without a filter scan.
    let object0 = Term::Literal(Literal::new_simple_literal("object 0"));
    let after = store
        .delete_matching(None, None, Some(&object0), None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 8);
    assert_eq!(
        after.indexes(),
        &vec![IndexType::SecondaryByReference],
        "tombstoning a file row must not invalidate its index"
    );
    // The source view is untouched — it still sees all 12.
    assert_eq!(store.size().await.unwrap(), 12);

    // The index still routes the lookup after the delete, and the
    // tombstoned rows must not come back.
    assert_eq!(
        after
            .match_pattern(None, None, Some(&object0), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    // Predicate p0 (i even: 0,2,4,6,8,10) had rows 0 and 6 tombstoned.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_p0 = after
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_p0.size().await.unwrap(), 4);
    assert_eq!(by_p0.quads().unwrap().count().await, 4);

    // Filter-scan delete: a subject isn't index-resolved, so this exercises
    // the pruning + filter evaluation path that resolves the doomed rows.
    let s05 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let after2 = after
        .delete_matching(Some(&s05), None, None, None)
        .await
        .unwrap();
    assert_eq!(after2.size().await.unwrap(), 7);
    // s05 is object "object 2" (5 % 3); that lookup now returns one fewer.
    let object2 = Term::Literal(Literal::new_simple_literal("object 2"));
    assert_eq!(
        after2
            .match_pattern(None, None, Some(&object2), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        3,
    );

    // A sort-only compaction reclaims every tombstone and drops the index.
    let compacted = after2.compact_with_indexes(vec![]).await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), 7);
    assert!(compacted.indexes().is_empty());
    assert_eq!(compacted.quads().unwrap().count().await, 7);
}

// ─── SecondaryByCopy on a file ─────────────────────────────────────────

/// The copy index's serving script over a file of `layout`. A residual
/// graph constraint rides the copy-served scan's filter, so the plan is kept
/// on every layout; what `located` changes is when the ids resolve: a
/// located run (sorted dictionary-code copies) point-reads its small runs'
/// ids at match time and proves an empty run there, an unlocated one leaves
/// the rid scan pending until a consumer needs it.
async fn run_copy_index_file_test(layout: LayoutStrategy, located: bool) {
    let (quads, graphs) = copy_index_script_dataset();
    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    run_copy_index_serving_script(
        store,
        &quads,
        &graphs,
        ServeExpectations {
            served_pending: !located,
            residual_graph_served: true,
            never_predicate_pending: !located,
        },
    )
    .await;
}

/// A located run wider than the point-read cap keeps the deferred contract:
/// the rid scan stays pending until a consumer needs the selection, and the
/// served stream reads the run by range in several row-count splits — one
/// decoded chunk each — agreeing row for row with the primary read,
/// tombstones inside the run included.
#[tokio::test]
async fn test_copy_index_file_serving_wide_located_run_stays_pending() {
    // 10,000 rows per predicate: several served splits on any host (the
    // split policy floors at 1,024 rows), with the child's lead run longer
    // than the writer's first 8,192-row block, so its `p` column stays a
    // plain flat leaf (the dictionary-coded shape has its own test below).
    let quads = graph_modular_quads(30_000, 5, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    // POSG order puts p1's run right after p0's 10,000 rows.
    assert_eq!(
        by_p.debug_serve_row_range(),
        Some(10_000..20_000),
        "the run must be located for the range scan to serve it"
    );
    assert!(
        by_p.debug_selection_pending(),
        "a 10,000-row run exceeds the point-read cap and must stay deferred"
    );
    assert_eq!(by_p.size().await.unwrap(), 10_000);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    // The served stream arrives one chunk per row-count split of the run
    // (plus the empty tail item), never as the single chunk the child's own
    // split would make of it.
    let chunks: Vec<usize> = by_p
        .shared_quad_chunks()
        .unwrap()
        .map(|chunk| chunk.len())
        .filter(|len| futures::future::ready(*len > 0))
        .collect()
        .await;
    assert!(
        chunks.len() >= 2,
        "a wide located run is served in several splits, got {chunks:?}"
    );
    assert_eq!(chunks.iter().sum::<usize>(), 10_000);

    // A tombstone inside the run leaves every served split through the
    // family's rid column, and the count agrees.
    let deleted = store.delete_quad(&quads[1_501]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_after.debug_has_serve_plan());
    assert_eq!(by_p_after.size().await.unwrap(), 9_999);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i != 1_501)
    );
}

/// Index-child columns the writer dictionary-encodes at the layout level —
/// a lead column whose first block holds a few predicates, a second key with
/// a handful of objects — still locate their runs: the lead search, the
/// windowed second-key search and the point reads all probe through the
/// codes leaves, so every served shape reads by range and agrees with the
/// primary read.
#[tokio::test]
async fn test_copy_index_file_locates_dictionary_coded_columns() {
    // Three predicates of 3,000 rows: the POSG child's first block holds all
    // three, so its `p` column is dictionary-coded; `o` (seven objects) is
    // dictionary-coded in both families.
    let quads = graph_modular_quads(9_000, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Predicate-bound: the POSG lead run, located through the dictionary-coded
    // `p` column and served by range in several splits.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert_eq!(by_p.debug_serve_row_range(), Some(3_000..6_000));
    assert!(by_p.debug_selection_pending());
    assert_eq!(by_p.size().await.unwrap(), 3_000);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
    let chunks: Vec<usize> = by_p
        .shared_quad_chunks()
        .unwrap()
        .map(|chunk| chunk.len())
        .filter(|len| futures::future::ready(*len > 0))
        .collect()
        .await;
    assert!(
        chunks.len() >= 2,
        "served in several splits, got {chunks:?}"
    );

    // Predicate and object bound: the windowed second-key search inside the
    // lead run, through the dictionary-coded `o` column — `i ≡ 1 (mod 3)`
    // and `i ≡ 1 (mod 7)` is the second of p1's seven object sub-runs.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert!(by_po.debug_has_serve_plan());
    assert_eq!(by_po.debug_serve_row_range(), Some(3_429..3_858));
    assert_eq!(by_po.size().await.unwrap(), 429);
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 3 == 1 && i % 7 == 1)
    );

    // Object-bound: the OSPG lead run through its dictionary-coded `o`.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert_eq!(by_o.debug_serve_row_range(), Some(2_572..3_858));
    assert_eq!(by_o.size().await.unwrap(), 1_286);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 7 == 2)
    );

    // A term the store knows but never as a predicate: the located run is
    // empty, proving the pattern matches nothing at match time.
    let subject_as_p = NamedNode::new("http://example.org/s0000").unwrap();
    let zero = store
        .match_pattern(None, Some(&subject_as_p), None, None)
        .await
        .unwrap();
    assert!(!zero.debug_selection_pending());
    assert_eq!(zero.size().await.unwrap(), 0);

    // A tombstone inside a served run leaves every split through the rid
    // column.
    let deleted = store.delete_quad(&quads[1_501]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_after.size().await.unwrap(), 2_999);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i != 1_501)
    );
}

#[tokio::test]
async fn test_copy_index_file_default() {
    run_copy_index_file_test(LayoutStrategy::Default, false).await;
}

#[tokio::test]
async fn test_copy_index_file_typed_object() {
    run_copy_index_file_test(LayoutStrategy::TypedObject, false).await;
}

#[tokio::test]
async fn test_copy_index_file_dictionary() {
    run_copy_index_file_test(LayoutStrategy::Dictionary, true).await;
}

/// Graph-bound patterns over a file's copy index: a bound predicate, or
/// predicate and object, with a graph is still served by the copy family,
/// the graph riding the plan's filter instead of a located range; a located
/// (dictionary-coded) run resolves its ids by point reads at match time, an
/// unlocated one leaves them pending; tombstones and unknown graphs answer
/// exactly, under every layout.
async fn run_copy_index_file_graph_bound(layout: LayoutStrategy, located: bool) {
    let (quads, graphs) = copy_index_script_dataset();
    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Predicate and graph: i ≡ 1 (mod 3) in g1 (odd i) → 1, 7, 13, 19, 25.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p1), None, Some(&graphs[1]))
        .await
        .unwrap();
    assert!(by_pg.debug_has_serve_plan());
    assert_eq!(
        by_pg.debug_serve_row_range(),
        None,
        "a graph term keeps the located run off the plan"
    );
    assert_eq!(by_pg.debug_selection_pending(), !located);
    assert_eq!(by_pg.size().await.unwrap(), 5);
    assert_eq!(
        view_strings(&by_pg).await,
        expected_strings(&quads, |i| i % 3 == 1 && i % 2 == 1)
    );

    // Predicate, object and graph: i ≡ 1 (mod 15) in g0 (even i) → 16.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_pog = store
        .match_pattern(None, Some(&p1), Some(&o1), Some(&graphs[0]))
        .await
        .unwrap();
    assert!(by_pog.debug_has_serve_plan());
    assert_eq!(by_pog.debug_serve_row_range(), None);
    assert_eq!(by_pog.debug_selection_pending(), !located);
    assert_eq!(by_pog.size().await.unwrap(), 1);
    assert_eq!(
        view_strings(&by_pog).await,
        expected_strings(&quads, |i| i % 15 == 1 && i % 2 == 0)
    );

    // A graph the store never saw matches nothing.
    let unknown = GraphName::NamedNode(NamedNode::new("http://example.org/nope").unwrap());
    let none = store
        .match_pattern(None, Some(&p1), None, Some(&unknown))
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);
    assert_eq!(view_strings(&none).await, Vec::<String>::new());

    // A tombstone inside the served graph run leaves it.
    let deleted = store.delete_quad(&quads[7]).await.unwrap();
    let after = deleted
        .match_pattern(None, Some(&p1), None, Some(&graphs[1]))
        .await
        .unwrap();
    assert!(after.debug_has_serve_plan());
    assert_eq!(after.size().await.unwrap(), 4);
    assert_eq!(
        view_strings(&after).await,
        expected_strings(&quads, |i| i % 3 == 1 && i % 2 == 1 && i != 7)
    );
}

#[tokio::test]
async fn test_copy_index_file_graph_bound_default() {
    run_copy_index_file_graph_bound(LayoutStrategy::Default, false).await;
}

#[tokio::test]
async fn test_copy_index_file_graph_bound_typed_object() {
    run_copy_index_file_graph_bound(LayoutStrategy::TypedObject, false).await;
}

#[tokio::test]
async fn test_copy_index_file_graph_bound_dictionary() {
    run_copy_index_file_graph_bound(LayoutStrategy::Dictionary, true).await;
}

/// A bound subject on a file locates its row range first; a residual object
/// over a range at or above the routing gate is then still resolved by the
/// copy index and intersected with the range.
#[tokio::test]
async fn test_file_subject_range_then_index_routing_above_gate() {
    let quads = above_gate_quads();
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    run_subject_range_then_index_routing_above_gate(&store, &quads).await;
}

/// A file carrying both index families opens with both, answers the shapes
/// each family covers exactly like the in-memory build, and keeps both
/// through a `to_bytes`/`from_bytes` round trip.
#[tokio::test]
async fn test_file_with_both_index_kinds() {
    let quads = modular_quads(24, 3, 4);
    let both = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Default, both.clone()).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(file.indexes(), both.as_slice());

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        both.clone(),
    )
    .await
    .unwrap();
    let memory = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(memory.indexes(), both.as_slice());

    let adopted = VortexRdfStore::from_bytes(&file.to_bytes().await.unwrap())
        .await
        .unwrap();
    assert_eq!(adopted.indexes(), both.as_slice());

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    for (tag, p, o) in [
        ("P", Some(&p1), None),
        ("O", None, Some(&o1)),
        ("PO", Some(&p1), Some(&o1)),
    ] {
        let want = view_strings(&memory.match_pattern(None, p, o, None).await.unwrap()).await;
        assert!(!want.is_empty(), "{tag}");
        assert_eq!(
            view_strings(&file.match_pattern(None, p, o, None).await.unwrap()).await,
            want,
            "{tag}: file"
        );
        assert_eq!(
            view_strings(&adopted.match_pattern(None, p, o, None).await.unwrap()).await,
            want,
            "{tag}: adopted"
        );
    }
}

// ─── SecondaryByReference on a file ────────────────────────────────────

/// The file-backed reference index end to end: on a sorted dictionary-code
/// child every covered shape locates its matched run through the value
/// column's chunk probes (small runs read their row ids point by point, wide
/// ones by a scan restricted to the run); on a string-valued child the same
/// shapes decline the location and answer through the pushed-down scan. Both
/// must agree, row for row, with the in-memory store over the same quads.
async fn run_reference_index_file_test(layout: LayoutStrategy, located: bool) {
    // 900 quads: 300 per predicate (a run wider than the point-read cap) and
    // ~129 per object (one narrow enough to point-read).
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);

    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByReference]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByReference]);

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));

    // Object-bound: 129 rows — a located run inside the point-read cap.
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .debug_reference_index_located_run(None, Some(&o2))
            .await
            .unwrap()
            .map(|r| (r.end - r.start) as usize),
        located.then_some(129),
        "object location engages exactly on the code-valued child"
    );
    assert_eq!(by_o.size().await.unwrap(), 129);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(&quads, |i| i % 7 == 2)
    );
    // This index serves no quads: the reads gather the primary columns.
    assert!(!by_o.debug_has_serve_plan());

    // Predicate-bound: 300 rows — a located run past the cap, whose ids come
    // from the range-restricted rid scan.
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .debug_reference_index_located_run(Some(&p1), None)
            .await
            .unwrap()
            .map(|r| (r.end - r.start) as usize),
        located.then_some(300),
    );
    assert_eq!(by_p.size().await.unwrap(), 300);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );

    // Predicate and object bound: this index probes the object column only,
    // leaving the predicate as a residual filter over the located rows —
    // i ≡ 2 (mod 7) ∧ i ≡ 1 (mod 3) ⇔ i ≡ 16 (mod 21).
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(&quads, |i| i % 21 == 16)
    );

    // A term the store has never seen short-circuits before any location.
    let missing = Term::Literal(Literal::new_simple_literal("nope"));
    let none = store
        .match_pattern(None, None, Some(&missing), None)
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);

    // A term the store knows — but never as an object — locates an empty run
    // (or scans to the same conclusion) and answers empty.
    let subject_as_o = Term::NamedNode(NamedNode::new("http://example.org/s0000").unwrap());
    assert_eq!(
        store
            .debug_reference_index_located_run(None, Some(&subject_as_o))
            .await
            .unwrap()
            .map(|r| r.is_empty()),
        located.then_some(true),
    );
    let zero = store
        .match_pattern(None, None, Some(&subject_as_o), None)
        .await
        .unwrap();
    assert_eq!(view_strings(&zero).await, Vec::<String>::new());
    assert_eq!(zero.size().await.unwrap(), 0);

    // Tombstones ride the resolved ids: a deleted row leaves the run.
    let deleted = store.delete_quad(&quads[2]).await.unwrap();
    let after = deleted
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 128);
    assert_eq!(
        view_strings(&after).await,
        expected_strings(&quads, |i| i % 7 == 2 && i != 2)
    );

    // Chaining composes the resolutions the same way either path resolves
    // them.
    let chained = by_p
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&chained).await,
        expected_strings(&quads, |i| i % 21 == 16)
    );
}

#[tokio::test]
async fn test_reference_index_file_dictionary() {
    run_reference_index_file_test(LayoutStrategy::Dictionary, true).await;
}

#[tokio::test]
async fn test_reference_index_file_default() {
    run_reference_index_file_test(LayoutStrategy::Default, false).await;
}

#[tokio::test]
async fn test_reference_index_file_typed_object() {
    run_reference_index_file_test(LayoutStrategy::TypedObject, false).await;
}
