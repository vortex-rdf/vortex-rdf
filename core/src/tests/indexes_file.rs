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

/// A reference-index count over a located run (predicate-only or
/// object-only, nothing residual) is the run's width: no row id is read.
/// `limit` caps it; a window reads only the rows it takes, in base order;
/// a residual or tombstones send the count through the ids.
#[tokio::test]
async fn test_reference_index_counts_located_runs_from_width() {
    use crate::store::{IdsNeed, Probe};
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    for store in [
        VortexRdfStore::from_file(&path).await.unwrap(),
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    ] {
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let counted = store
            .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
            .await
            .unwrap();
        assert!(counted.debug_selection_pending());
        assert_eq!(counted.size().await.unwrap(), 300);
        assert_eq!(
            counted.debug_row_ids_materialized(),
            Some(false),
            "a count reads no row id"
        );
        let by_p = Probe::new(None, Some(p1.clone()), None, None);
        let by_o = Probe::new(None, None, Some(o2.clone()), None);
        assert_eq!(
            store
                .count_many(&[by_p.clone(), by_o.clone()])
                .await
                .unwrap(),
            vec![300, 129]
        );
        assert_eq!(
            store
                .count_many(&[
                    by_p.clone().window(0, Some(10)),
                    by_o.clone().window(0, Some(1_000))
                ])
                .await
                .unwrap(),
            vec![10, 129]
        );
        // The public match still computes its ids: its rows may be streamed.
        let matched = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert!(!matched.debug_selection_pending());
        let all = matched.code_columns_gathered().await.unwrap().unwrap();
        for (offset, limit) in [(0usize, 5usize), (7, 20), (295, 10), (300, 3)] {
            let windowed = store
                .run_probe(&by_p.clone().window(offset, Some(limit)))
                .await
                .unwrap();
            let cols = windowed.code_columns_gathered().await.unwrap().unwrap();
            let end = (offset + limit).min(300);
            assert_eq!(
                cols[0].as_slice(),
                &all[0].as_slice()[offset.min(300)..end],
                "({offset}, {limit})"
            );
        }
        let both = Probe::new(None, Some(p1.clone()), Some(o2.clone()), None);
        assert_eq!(
            store.count_many(&[both]).await.unwrap(),
            vec![expected_strings(&quads, |i| i % 21 == 16).len()]
        );
        let deleted = store.delete_quad(&quads[1]).await.unwrap();
        assert_eq!(deleted.count_many(&[by_p]).await.unwrap(), vec![299]);
    }
}

/// Every `(offset, limit)` window of the run `(p, o)` on `store`, against the
/// row path (the unwindowed rows, which the public match computes):
/// - the window holds the same slice of the rows, and its size is the slice's;
/// - the row ids requested from the located run (the read counter) grow by
///   exactly the window's size — so by none for an empty window, which is no
///   limit, or an offset at or past the run's end;
/// - the pending view the window is also taken beside keeps its own ids
///   unread;
/// - a count, capped or not, asks for no id.
pub(super) async fn assert_windows_read_only_their_rows(
    store: &VortexRdfStore,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    width: usize,
    windows: &[(usize, usize)],
) {
    use crate::store::{IdsNeed, Probe};
    let reads = || store.debug_located_rid_reads().unwrap();
    let probe = Probe::new(None, p.cloned(), o.cloned(), None);
    let span =
        |offset: usize, limit: usize| (offset.min(width), offset.saturating_add(limit).min(width));
    let all = store
        .match_pattern(None, p, o, None)
        .await
        .unwrap()
        .code_columns_gathered()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all[0].len(), width);

    // A count asks for no id, capped or not.
    let before = reads();
    let counted = store
        .count_many(&[
            probe.clone(),
            probe.clone().window(5, Some(10)),
            probe.clone().window(0, Some(1_000)),
        ])
        .await
        .unwrap();
    let (from, to) = span(5, 10);
    assert_eq!(counted, vec![width, to - from, width.min(1_000)]);
    assert_eq!(reads(), before, "a count reads no row id");

    let pending = store
        .match_pattern_for(None, p, o, None, IdsNeed::CountOrWindow)
        .await
        .unwrap();
    assert_eq!(pending.debug_row_ids_materialized(), Some(false));
    for &(offset, limit) in windows {
        let (from, to) = span(offset, limit);
        let before = reads();
        let window = store
            .run_probe(&probe.clone().window(offset, Some(limit)))
            .await
            .unwrap();
        assert_eq!(
            reads() - before,
            to - from,
            "({offset}, {limit}) reads exactly its own rows"
        );
        // The same window taken beside the pending view leaves its ids unread.
        let beside = pending.window(offset, limit).await.unwrap();
        assert_eq!(
            pending.debug_row_ids_materialized(),
            Some(false),
            "({offset}, {limit}) leaves the run's ids unread"
        );
        for view in [&window, &beside] {
            assert_eq!(view.size().await.unwrap(), to - from, "({offset}, {limit})");
            let cols = view.code_columns_gathered().await.unwrap().unwrap();
            for (col, whole) in cols.iter().zip(&all) {
                assert_eq!(
                    col.as_slice(),
                    &whole.as_slice()[from..to],
                    "({offset}, {limit})"
                );
            }
        }
    }
}

/// A window of a located run asks for exactly its own rows from the run — a
/// deep page costs its limit, one that reaches the run's end no more than
/// its rows, and an empty one (no limit, or an offset at or past the end)
/// none — point-read inside the 256-row cap and taken by a range scan
/// beyond it. What the run's width cannot answer (a residual term, a
/// tombstone) reads the run.
#[tokio::test]
async fn test_reference_index_window_reads_only_its_rows() {
    use crate::store::Probe;
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    // The 300-row predicate run: windows of 280 and 299 rows, and the whole
    // run, are past the point-read cap.
    assert_windows_read_only_their_rows(
        &store,
        Some(&p1),
        None,
        300,
        &[
            (0, 0),
            (0, 1),
            (0, 5),
            (7, 20),
            (40, 0),
            (100, 180),
            (0, 299),
            (298, 1),
            (250, 10),
            (200, 50),
            (150, 150),
            (295, 10),
            (299, 1),
            (299, usize::MAX),
            (0, 300),
            (0, usize::MAX),
            (300, 3),
            (400, 5),
            (usize::MAX, 5),
        ],
    )
    .await;
    // The 129-row object run, inside the cap throughout.
    assert_windows_read_only_their_rows(
        &store,
        None,
        Some(&o2),
        129,
        &[
            (0, 0),
            (0, 5),
            (10, 50),
            (0, 128),
            (127, 1),
            (100, 29),
            (120, 20),
            (128, 1),
            (0, 129),
            (129, 5),
            (usize::MAX, 1),
        ],
    )
    .await;

    // What the run's width cannot answer reads the run: a residual term
    // leaves the object's 129 ids to be filtered by the predicate, and a
    // tombstone sends the predicate's 300 through the ids.
    let reads = || store.debug_located_rid_reads().unwrap();
    let before = reads();
    let both = Probe::new(None, Some(p1.clone()), Some(o2.clone()), None);
    assert_eq!(store.count_many(&[both]).await.unwrap(), vec![43]);
    assert_eq!(reads() - before, 129);
    let deleted = store.delete_quad(&quads[1]).await.unwrap();
    let before = reads();
    let by_p = Probe::new(None, Some(p1.clone()), None, None);
    assert_eq!(deleted.count_many(&[by_p]).await.unwrap(), vec![299]);
    assert_eq!(reads() - before, 300);
}

/// A pending located run resolves wherever a consumer needs its ids: a keep
/// and a chained match first read the run, and an appended tail's rows follow
/// the base's in a count and across a window's end — every answer agreeing
/// with the exact view.
#[tokio::test]
async fn test_reference_index_pending_run_composes() {
    use crate::store::IdsNeed;
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    for store in [
        VortexRdfStore::from_file(&path).await.unwrap(),
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    ] {
        let all = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap()
            .code_columns_gathered()
            .await
            .unwrap()
            .unwrap();

        // A keep reads the pending run's ids, then narrows like any other.
        let pending = store
            .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
            .await
            .unwrap();
        assert!(pending.debug_selection_pending());
        // A residual term keeps the run's ids from staying pending: the
        // filter over them needs the ids.
        let residual = store
            .match_pattern_for(None, Some(&p1), Some(&o2), None, IdsNeed::CountOrWindow)
            .await
            .unwrap();
        assert!(!residual.debug_selection_pending());
        let o_code = all[2][0];
        let kept_rows: Vec<usize> = (0..all[2].len()).filter(|&i| all[2][i] == o_code).collect();
        assert!(kept_rows.len() > 1 && kept_rows.len() < all[2].len());
        let kept = pending
            .keep(QuadColumn::O, &Keep::set([o_code]))
            .await
            .unwrap();
        let kept_cols = kept.code_columns_gathered().await.unwrap().unwrap();
        for (col, whole) in kept_cols.iter().zip(&all) {
            let want: Vec<TermCode> = kept_rows.iter().map(|&i| whole[i]).collect();
            assert_eq!(col.as_slice(), want.as_slice());
        }
        // The same keep as a probe's, counted and windowed.
        let probe =
            Probe::new(None, Some(p1.clone()), None, None).keep(QuadColumn::O, Keep::set([o_code]));
        assert_eq!(
            store
                .count_many(&[probe.clone(), probe.clone().window(1, Some(2))])
                .await
                .unwrap(),
            vec![kept_rows.len(), 2]
        );

        // A chained match intersects the run's ids with its own.
        let chained = pending
            .match_pattern(None, None, Some(&o2), None)
            .await
            .unwrap();
        assert_eq!(
            view_strings(&chained).await,
            expected_strings(&quads, |i| i % 21 == 16)
        );

        // A view that is already restricted never counts a run by its width:
        // the run's rows are intersected with the view's own — whether the
        // restriction is a row selection or also a pending filter.
        let by_p_view = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        let by_po_view = store
            .match_pattern(None, Some(&p1), Some(&o2), None)
            .await
            .unwrap();
        let by_o = Probe::new(None, None, Some(o2.clone()), None);
        let by_p = Probe::new(None, Some(p1.clone()), None, None);
        assert_eq!(
            by_p_view
                .count_many(&[by_o.clone(), by_o.clone().window(0, Some(5))])
                .await
                .unwrap(),
            vec![43, 5]
        );
        assert_eq!(
            by_po_view
                .count_many(std::slice::from_ref(&by_p))
                .await
                .unwrap(),
            vec![43]
        );
        // i ≡ 16 (mod 21): the 43 rows are in subject order, so the window
        // (2, 3) is the third to fifth of them.
        let window = by_p_view
            .run_probe(&by_o.clone().window(2, Some(3)))
            .await
            .unwrap();
        assert_eq!(
            view_strings(&window).await,
            expected_strings(&quads, |i| [58, 79, 100].contains(&i))
        );

        // An appended tail follows the base: its row counts after the run's
        // 300, and a window across the boundary takes both sides.
        let appended = make_quad(
            "http://example.org/s9999",
            "http://example.org/p1",
            "o9",
            GraphName::DefaultGraph,
        );
        let tailed = store.add_quad(appended.clone()).await.unwrap();
        let by_p = Probe::new(None, Some(p1.clone()), None, None);
        assert_eq!(
            tailed
                .count_many(&[
                    by_p.clone(),
                    by_p.clone().window(295, Some(10)),
                    by_p.clone().window(0, Some(5)),
                    by_p.clone().window(300, Some(5)),
                ])
                .await
                .unwrap(),
            vec![301, 6, 5, 1]
        );
        let base_rows: Vec<&Quad> = quads.iter().skip(1).step_by(3).collect();
        assert_eq!(base_rows.len(), 300);
        let strings = |rows: Vec<&Quad>| {
            let mut strings: Vec<String> = rows.iter().map(|q| q.to_string()).collect();
            strings.sort();
            strings
        };
        for (offset, limit, want) in [
            (
                295usize,
                10usize,
                [&base_rows[295..], &[&appended][..]].concat(),
            ),
            (0, 5, base_rows[..5].to_vec()),
            (300, 5, vec![&appended]),
            (298, 1, vec![base_rows[298]]),
        ] {
            let window = tailed
                .run_probe(&by_p.clone().window(offset, Some(limit)))
                .await
                .unwrap();
            assert_eq!(
                view_strings(&window).await,
                strings(want),
                "({offset}, {limit})"
            );
        }
    }
}

/// A view built only to be counted or windowed is pending without a serve
/// plan — the one state nothing that reads rows may hold — and no view a
/// caller gets back is: not the row path's, nor a window, a keep, or a
/// probe's, windowed or not, with keeps or without.
#[tokio::test]
async fn test_only_a_counted_view_is_pending_without_a_plan() {
    use crate::store::{IdsNeed, Probe};
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    for store in [
        VortexRdfStore::from_file(&path).await.unwrap(),
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    ] {
        let counted = store
            .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
            .await
            .unwrap();
        assert!(counted.debug_pending_without_plan());
        // Resolving it leaves none behind, and the row path never builds one.
        let admit_all = Keep::range(0..TermCode::MAX);
        let window = counted.window(3, 10).await.unwrap();
        assert!(!window.debug_pending_without_plan());
        let kept = counted.keep(QuadColumn::O, &admit_all).await.unwrap();
        assert!(!kept.debug_pending_without_plan());
        let rows = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert!(!rows.debug_pending_without_plan());

        let by_p = Probe::new(None, Some(p1.clone()), None, None);
        let probes = [
            by_p.clone(),
            by_p.clone().window(0, Some(5)),
            by_p.clone().window(5, None),
            by_p.clone().window(0, Some(0)),
            by_p.clone().keep(QuadColumn::O, admit_all.clone()),
            by_p.clone()
                .keep(QuadColumn::O, admit_all.clone())
                .window(1, Some(2)),
        ];
        for probe in &probes {
            let view = store.run_probe(probe).await.unwrap();
            assert!(!view.debug_pending_without_plan(), "{probe:?}");
        }
        for view in store.match_many(&probes).await.unwrap() {
            assert!(!view.debug_pending_without_plan());
        }
    }
}

/// Streaming the rows of a view built only to be counted is refused loudly,
/// not answered from a selection nobody read.
async fn stream_a_counted_view(open: impl AsyncFnOnce(&std::path::Path) -> VortexRdfStore) {
    use crate::store::IdsNeed;
    let quads = graph_modular_quads(60, 3, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let store = open(&path).await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let counted = store
        .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
        .await
        .unwrap();
    assert!(counted.debug_pending_without_plan());
    let _ = counted.quads_vec().await;
}

#[tokio::test]
#[should_panic(expected = "count/window view")]
async fn test_a_counted_file_view_cannot_be_streamed() {
    stream_a_counted_view(async |path| VortexRdfStore::from_file(path).await.unwrap()).await;
}

#[tokio::test]
#[should_panic(expected = "count/window view")]
async fn test_a_counted_in_memory_view_cannot_be_streamed() {
    stream_a_counted_view(async |path| VortexRdfStore::from_file_in_memory(path).await.unwrap())
        .await;
}

/// The reference children's columns written as several flat leaves — as a
/// large file has them — change nothing for a located run that crosses a leaf
/// boundary: its count is its width, and every window, whether it straddles a
/// boundary or sits inside a leaf, point-read or range-scanned, holds its rows
/// and asks for exactly them.
#[tokio::test]
async fn test_reference_index_runs_across_rid_leaves() {
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let (_dir, path) = write_chunked_reference_store_file(&quads, 128).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByReference]);
    // 900 rows in 128-row leaves: seven full ones and a short one, in every
    // column of both children.
    for component in ["index:ref-p", "index:ref-o"] {
        for column in ["val", "rid"] {
            assert_eq!(
                store.debug_component_column_chunks(component, column),
                Some((900, 8)),
                "{component}.{column}"
            );
        }
    }

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    // p1's run is child rows 300..600, o2's 258..387: each holds a leaf
    // boundary (384 and 512; 384), so a window can straddle one.
    let p_run = store
        .debug_reference_index_located_run(Some(&p1), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p_run, 300..600);
    let o_run = store
        .debug_reference_index_located_run(None, Some(&o2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o_run, 258..387);
    for run in [&p_run, &o_run] {
        assert!(
            run.start / 128 < (run.end - 1) / 128,
            "{run:?} crosses a leaf"
        );
    }
    // Positions 84 and 212 of p1's run, and 126 of o2's, are the first rows
    // of a leaf.
    assert_windows_read_only_their_rows(
        &store,
        Some(&p1),
        None,
        300,
        &[
            (83, 2),
            (84, 1),
            (80, 10),
            (205, 15),
            (211, 2),
            (212, 1),
            (80, 140),
            (20, 270),
            (0, 129),
            (0, 300),
        ],
    )
    .await;
    assert_windows_read_only_their_rows(
        &store,
        None,
        Some(&o2),
        129,
        &[(125, 2), (126, 1), (120, 15), (100, 29), (0, 129)],
    )
    .await;
}

/// A run at the very start of a reference child (child row 0), and one that
/// ends at its last row, count and window like any other — on a child of one
/// leaf and on one of several.
#[tokio::test]
async fn test_reference_index_runs_at_the_children_edges() {
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let single = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let leaves = write_chunked_reference_store_file(&quads, 128).await;
    let predicate = |n: usize| NamedNode::new(format!("http://example.org/p{n}")).unwrap();
    let object = |n: usize| Term::Literal(Literal::new_simple_literal(format!("o{n}")));
    for (_dir, path) in [&single, &leaves] {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        // The lowest and the highest predicate and object codes: the first
        // and the last run of each child.
        for (p, o, run) in [
            (Some(predicate(0)), None, 0..300u64),
            (Some(predicate(2)), None, 600..900),
            (None, Some(object(0)), 0..129),
            (None, Some(object(6)), 772..900),
        ] {
            let located = store
                .debug_reference_index_located_run(p.as_ref(), o.as_ref())
                .await
                .unwrap();
            assert_eq!(located, Some(run.clone()), "{p:?} {o:?}");
            let width = (run.end - run.start) as usize;
            assert_windows_read_only_their_rows(
                &store,
                p.as_ref(),
                o.as_ref(),
                width,
                &[
                    (0, 1),
                    (0, 5),
                    (width - 5, 5),
                    (width - 1, 1),
                    (width - 1, 10),
                    (0, width),
                    (width, 1),
                ],
            )
            .await;
        }
    }
}

/// With named graphs the graph term is the one a reference index does not
/// cover: a predicate-only or object-only count is the located run's width
/// while the graph is free, and goes through the run's ids once the pattern
/// binds one — the same counts and windows as the row path either way, on
/// both backends.
#[tokio::test]
async fn test_reference_index_counts_with_named_graphs() {
    use crate::store::{IdsNeed, Probe};
    let named =
        |n: &str| GraphName::NamedNode(NamedNode::new(format!("http://example.org/{n}")).unwrap());
    // Four graphs against three predicates and seven objects: none of the
    // three determines another.
    let graphs = [
        GraphName::DefaultGraph,
        named("g1"),
        named("g2"),
        named("g3"),
    ];
    let quads = graph_modular_quads(900, 4, 3, 7, &graphs);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    for store in [
        VortexRdfStore::from_file(&path).await.unwrap(),
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    ] {
        for (p, o, run) in [(Some(&p1), None, 300usize), (None, Some(&o2), 129)] {
            for graph in [None, Some(&graphs[0]), Some(&graphs[1]), Some(&graphs[3])] {
                let label = format!("p {p:?} o {o:?} graph {graph:?}");
                let want = quads
                    .iter()
                    .filter(|q| {
                        p.is_none_or(|p| q.predicate == *p)
                            && o.is_none_or(|o| q.object == *o)
                            && graph.is_none_or(|g| q.graph_name == *g)
                    })
                    .count();
                assert!(want > 25, "{label}: {want}");

                // The row path.
                let rows = store.match_pattern(None, p, o, graph).await.unwrap();
                assert_eq!(rows.size().await.unwrap(), want, "{label}");
                let all = rows.code_columns_gathered().await.unwrap().unwrap();

                // Counted: the run's width while the graph is free, its ids
                // once the graph is bound (a residual the index does not cover).
                let counted = store
                    .match_pattern_for(None, p, o, graph, IdsNeed::CountOrWindow)
                    .await
                    .unwrap();
                assert_eq!(
                    counted.debug_selection_pending(),
                    graph.is_none(),
                    "{label}"
                );
                assert_eq!(counted.size().await.unwrap(), want, "{label}");
                let probe = Probe::new(None, p.cloned(), o.cloned(), graph.cloned());
                let before = store.debug_located_rid_reads();
                assert_eq!(
                    store
                        .count_many(std::slice::from_ref(&probe))
                        .await
                        .unwrap(),
                    vec![want],
                    "{label}"
                );
                if let Some(before) = before {
                    let read = store.debug_located_rid_reads().unwrap() - before;
                    assert_eq!(read, if graph.is_none() { 0 } else { run }, "{label}");
                }
                let capped = [
                    probe.clone().window(3, Some(20)),
                    probe.clone().window(want - 4, Some(10)),
                    probe.clone().window(want, Some(5)),
                ];
                assert_eq!(
                    store.count_many(&capped).await.unwrap(),
                    vec![20, 4, 0],
                    "{label}"
                );

                // Windows hold the same slice of the rows as the row path.
                for (offset, limit) in [(0usize, 5usize), (3, 20), (want - 4, 10), (want, 3)] {
                    let window = store
                        .run_probe(&probe.clone().window(offset, Some(limit)))
                        .await
                        .unwrap();
                    let (from, to) = (offset.min(want), (offset + limit).min(want));
                    assert_eq!(window.size().await.unwrap(), to - from, "{label}");
                    let cols = window.code_columns_gathered().await.unwrap().unwrap();
                    for (col, whole) in cols.iter().zip(&all) {
                        assert_eq!(
                            col.as_slice(),
                            &whole.as_slice()[from..to],
                            "{label} ({offset}, {limit})"
                        );
                    }
                }
            }
        }
    }
}
