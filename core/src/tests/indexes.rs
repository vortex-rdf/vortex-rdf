use super::*;

// ─── 4) Secondary index behavior ────────────────────────────────────────

#[tokio::test]
async fn test_multiple_indexes_deduplicated() {
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

    // The same index requested twice must not produce duplicate columns.
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::Default,
        vec![
            IndexType::SecondaryByReference,
            IndexType::SecondaryByReference,
        ],
    )
    .await
    .expect("build failed");

    // Schema: 4 primary columns + 4 reference index columns, exactly once.
    if let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() {
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        assert_eq!(
            names,
            [
                "s",
                "p",
                "o",
                "g",
                "_idx_o_val",
                "_idx_o_rid",
                "_idx_p_val",
                "_idx_p_rid"
            ]
        );
    } else {
        panic!("expected StructArray dtype");
    }

    // Index-routed matching still works.
    let store = VortexRdfStore::from_built(arr).unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 1);
}

/// A store derived by matching keeps its indexes, because a view narrows a
/// selection over the base rather than rewriting rows — so the `_idx_*_rid`
/// ids still address the data. This is what lets a chained match keep
/// routing through the index instead of degrading to a scan.
#[tokio::test]
async fn test_in_memory_derived_view_keeps_indexes() {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.indexes(), &vec![IndexType::SecondaryByReference]);

    // Match on the object index: 24 quads over 4 objects ⇒ 6 rows.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let matched = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 6);
    assert_eq!(
        matched.indexes(),
        &vec![IndexType::SecondaryByReference],
        "a derived view must keep the base's indexes"
    );

    // Chain a second, index-routed match onto the derived view. Of those 6
    // rows (i = 1, 5, 9, 13, 17, 21), the ones with predicate p1 are
    // i ≡ 1 (mod 3): 1, 13 — the intersection of two index lookups.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    let chained = matched
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    let mut got: Vec<String> = chained
        .quads()
        .unwrap()
        .map(|q| q.unwrap().subject.to_string())
        .collect()
        .await;
    got.sort();
    assert_eq!(
        got,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string()
        ]
    );

    // A sort-only compaction (empty index set) gathers the chained view
    // into a standalone store; it renumbers rows and so drops the indexes —
    // the quads must survive intact.
    let standalone = chained.compact_with_indexes(vec![]).await.unwrap();
    assert!(
        standalone.indexes().is_empty(),
        "gathering renumbers rows, so index ids cannot survive it"
    );
    assert_eq!(standalone.size().await.unwrap(), 2);
    let mut compacted: Vec<String> = standalone
        .quads()
        .unwrap()
        .map(|q| q.unwrap().subject.to_string())
        .collect()
        .await;
    compacted.sort();
    assert_eq!(compacted, got);
}

/// `compact_with_indexes` gathers the live rows and rebuilds the requested
/// indexes over the fresh `0..n` row order — so an independent, compacted
/// store keeps routing through its index instead of degrading to a full
/// scan. It also lets a store be re-indexed: the requested set is what the
/// result carries, whatever the source had.
#[tokio::test]
async fn test_compact_with_indexes_rebuilds() {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // A view over the object index: i = 1, 5, 9, 13, 17, 21 ⇒ 6 rows.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let view = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), 6);

    // An empty index set drops the index; a non-empty one rebuilds it.
    assert!(
        view.compact_with_indexes(vec![])
            .await
            .unwrap()
            .indexes()
            .is_empty()
    );
    let indexed = view
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(indexed.size().await.unwrap(), 6);

    // The rebuilt index routes over the new row order: of those 6 rows,
    // predicate p1 is i ≡ 1 (mod 3) ⇒ 1, 13. The result must be exact and
    // the store independent of its source.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    let routed = indexed
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    assert_eq!(
        subjects_of(&routed).await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]
    );
    assert_eq!(store.size().await.unwrap(), 24, "source untouched");

    // Re-indexing from nothing: an empty set drops every index,
    // and a store built without indexes gains one it never had.
    let bare = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let bare = VortexRdfStore::from_built(bare).unwrap();
    assert!(bare.indexes().is_empty());
    assert!(
        bare.compact_with_indexes(vec![])
            .await
            .unwrap()
            .indexes()
            .is_empty()
    );
    let reindexed = bare
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(reindexed.indexes(), &[IndexType::SecondaryByReference]);
    let routed = reindexed
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(routed.size().await.unwrap(), 6);
}

/// The index rebuild reads its value columns from the materialized array in
/// each layout's own representation: `o`/`p` strings (Default), u32 codes
/// (Dictionary), and the object term recomposed from typed sub-columns
/// (TypedObject). Exercise all three end-to-end.
async fn run_compact_with_indexes_layout(layout: LayoutStrategy) {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        layout,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    let indexed = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap()
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(indexed.size().await.unwrap(), 6);

    // Route through both the rebuilt object and predicate columns.
    let predicate = NamedNode::new("http://example.org/p1").unwrap();
    assert_eq!(
        subjects_of(
            &indexed
                .match_pattern(None, Some(&predicate), None, None)
                .await
                .unwrap()
        )
        .await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]
    );
    assert_eq!(
        indexed
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        6,
    );
}

#[tokio::test]
async fn test_compact_with_indexes_dictionary() {
    run_compact_with_indexes_layout(LayoutStrategy::Dictionary).await;
}

#[tokio::test]
async fn test_compact_with_indexes_typed_object() {
    run_compact_with_indexes_layout(LayoutStrategy::TypedObject).await;
}

/// Deleting tombstones rows instead of rewriting them, so base row ids —
/// and the secondary index built against them — survive the delete.
#[tokio::test]
async fn test_delete_keeps_indexes_usable() {
    let quads = modular_quads(12, 2, 3);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Drop one quad: subject s0, which also carries object "object 0".
    let after = store.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(after.size().await.unwrap(), 11);
    assert_eq!(
        after.indexes(),
        &vec![IndexType::SecondaryByReference],
        "tombstoning must not invalidate the index"
    );
    // The source store is untouched — mutations return a new store.
    assert_eq!(store.size().await.unwrap(), 12);

    // "object 0" is on i = 0, 3, 6, 9; the index still routes the lookup,
    // and the tombstoned row must not come back.
    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let matched = after
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 3);
    let mut subjects: Vec<String> = matched
        .quads()
        .unwrap()
        .map(|q| q.unwrap().subject.to_string())
        .collect()
        .await;
    subjects.sort();
    assert_eq!(
        subjects,
        vec![
            "<http://example.org/s03>".to_string(),
            "<http://example.org/s06>".to_string(),
            "<http://example.org/s09>".to_string()
        ]
    );

    // A sort-only compaction reclaims the tombstoned row (and drops the index).
    let compacted = after.compact_with_indexes(vec![]).await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), 11);
    assert!(compacted.indexes().is_empty());
}

/// A file's rows are tombstoned in place too, so deleting from a file-backed
/// store keeps its secondary indexes usable and never rewrites the file —
/// covering both the index-resolved delete path and the filter-scan one.
#[cfg(feature = "file-io")]
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
    // The file on disk is unchanged — the source store still sees all 12.
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

/// Compacting a file-backed store rewrites the compacted rows back over its
/// own source file and stays file-backed: an independent reopen of the path
/// sees the folded-in, tombstone-free data, the rebuilt index survives, and
/// a later append is folded into the file too.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_compaction_rewrites_source_file() {
    let (_dir, path) = write_store_file(
        modular_quads(12, 2, 3),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Delete "object 0" (i = 0, 3, 6, 9): 4 rows tombstoned, 8 live.
    let object0 = Term::Literal(Literal::new_simple_literal("object 0"));
    let after = store
        .delete_matching(None, None, Some(&object0), None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 8);

    // Compact, keeping the index set: tombstoned rows are reclaimed and the
    // source file is rewritten in place.
    let compacted = after.compact().await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), 8);
    assert_eq!(
        compacted.indexes(),
        &vec![IndexType::SecondaryByReference],
        "compaction rebuilds the store's index set"
    );
    // The rebuilt index routes over the compacted rows: the deleted object
    // is gone for good.
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&object0), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0,
    );

    // Proof the file itself was overwritten: an independent reopen of the
    // path sees the compacted, tombstone-free data — not the original 12.
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.size().await.unwrap(), 8);
    assert_eq!(reopened.indexes(), &vec![IndexType::SecondaryByReference]);

    // A file-backed store keeps its tail until an explicit compact; that
    // compaction folds the appended row into the file too.
    let extra = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let appended = reopened.add_quad(extra).await.unwrap();
    assert_eq!(appended.tail_len(), 1);
    let recompacted = appended.compact().await.unwrap();
    assert_eq!(recompacted.tail_len(), 0);
    assert_eq!(recompacted.size().await.unwrap(), 9);
    // The append now lives in the file on disk.
    let reopened2 = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened2.size().await.unwrap(), 9);
}

/// One cell of the layout × index matrix: build, check which carrier the
/// requested index families ride in, and run the shared match battery.
async fn run_index_matrix_cell<B: VortexArrayBuilder>(
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_name: &'static str,
    indexes: Indexes,
    quads: Vec<Quad>,
) {
    let arr = build_array::<B>(
        quad_stream(quads.clone()),
        layout,
        indexes.clone(),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "build failed for builder={builder_name} layout={layout_name} indexes={index_name}: {e}"
        )
    });

    if let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() {
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        let ref_cols = ["_idx_o_val", "_idx_o_rid", "_idx_p_val", "_idx_p_rid"];
        let copy_cols = [
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
            "_idx_ospg_s",
            "_idx_ospg_p",
            "_idx_ospg_o",
            "_idx_ospg_g",
            "_idx_ospg_rid",
        ];
        let expect_ref = indexes.contains(&IndexType::SecondaryByReference);
        let expect_copy = indexes.contains(&IndexType::SecondaryByCopy);
        // A requested family rides in exactly one carrier: welded
        // `_idx_*` row-space columns, or pre-split components beside
        // a primary-only array (the out-of-core builder's native
        // emission). Within the welded form, all-or-nothing.
        let welded_ref = ref_cols.iter().all(|c| names.contains(c));
        let welded_copy = copy_cols.iter().all(|c| names.contains(c));
        let component_names: Vec<&str> = arr.components.iter().map(|c| c.name).collect();
        let comp_ref = ["index:ref-o", "index:ref-p"]
            .iter()
            .all(|c| component_names.contains(c));
        let comp_copy = ["index:posg", "index:ospg"]
            .iter()
            .all(|c| component_names.contains(c));
        assert!(
            (welded_ref || comp_ref) == expect_ref
                && !(welded_ref && comp_ref)
                && (welded_copy || comp_copy) == expect_copy
                && !(welded_copy && comp_copy)
                && ref_cols.iter().all(|c| names.contains(c) == welded_ref)
                && copy_cols.iter().all(|c| names.contains(c) == welded_copy),
            "index column mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
    } else {
        panic!(
            "expected StructArray dtype for builder={builder_name} layout={layout_name} indexes={index_name}"
        );
    }

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(
        store.size().await.unwrap(),
        quads.len(),
        "size mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(
        by_pred.size().await.unwrap(),
        8,
        "predicate match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let by_obj = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(
        by_obj.size().await.unwrap(),
        6,
        "object match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_both = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(
        by_both.size().await.unwrap(),
        2,
        "combined match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(
        empty.size().await.unwrap(),
        0,
        "missing-term match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );
}

/// The 3-layout × 4-index-config matrix for one builder. The 12 cells are
/// independent in-memory builds, so they are spawned and joined rather than
/// run serially — per-cell failure context stays in each cell's panics,
/// re-raised through the join.
async fn run_index_matrix_test<B: VortexArrayBuilder + 'static>(builder_name: &'static str) {
    let quads = modular_quads(24, 3, 4);

    let layouts = [
        ("default", LayoutStrategy::Default),
        ("typed-object", LayoutStrategy::TypedObject),
        ("dictionary", LayoutStrategy::Dictionary),
    ];
    let index_configs: [(&'static str, Indexes); 4] = [
        ("none", vec![]),
        (
            "secondary-by-reference",
            vec![IndexType::SecondaryByReference],
        ),
        ("secondary-by-copy", vec![IndexType::SecondaryByCopy]),
        (
            "both",
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ),
    ];

    let mut cells = Vec::new();
    for (layout_name, layout) in layouts {
        for (index_name, indexes) in &index_configs {
            cells.push(tokio::spawn(run_index_matrix_cell::<B>(
                builder_name,
                layout_name,
                layout,
                index_name,
                indexes.clone(),
                quads.clone(),
            )));
        }
    }
    if let Err(e) = futures::future::try_join_all(cells).await {
        std::panic::resume_unwind(e.into_panic());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_matrix_sorted_in_memory() {
    run_index_matrix_test::<SortedInMemoryBuilder>("SortedInMemoryBuilder").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_matrix_sorted_stream() {
    run_index_matrix_test::<SortedStreamBuilder>("SortedStreamBuilder").await;
}

// ─── 4b) SecondaryByCopy: sorted full-copy index ────────────────────────

#[tokio::test]
async fn test_in_memory_copy_index_matching() {
    let quads = modular_quads(24, 3, 4);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate p1 marks i ≡ 1 (mod 3): 8 rows, via the POSG lead search.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p.size().await.unwrap(), 8);

    // Object "object 1" marks i ≡ 1 (mod 4): 6 rows, via the OSPG lead.
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let by_o = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_o.size().await.unwrap(), 6);

    // Both bound resolves in one (p, o) prefix probe:
    // i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 4) ⇔ i ≡ 1 (mod 12) → rows 1 and 13.
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_po.size().await.unwrap(), 2);
    let mut subjects: Vec<String> = by_po
        .quads()
        .unwrap()
        .try_collect::<Vec<Quad>>()
        .await
        .unwrap()
        .iter()
        .map(|q| q.subject.to_string())
        .collect();
    subjects.sort();
    assert_eq!(
        subjects,
        ["<http://example.org/s01>", "<http://example.org/s13>"]
    );

    // The derived view keeps the index, and a chained match through it
    // must agree with the single-call prefix probe.
    assert_eq!(by_p.indexes(), &[IndexType::SecondaryByCopy]);
    let chained = by_p
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(chained.size().await.unwrap(), 2);
}

/// The in-memory copy index's serving path: predicate / object /
/// predicate+object matches read the matched quads straight from the copy
/// family's contiguous run (a plain slice of the base, no row-id gather),
/// while a residual graph constraint or a chained match — which force a mask
/// scan or narrow the selection further — fall back to the row ids. Serving
/// applies exactly when the index fully resolves the pattern, which is also
/// exactly when no gather would otherwise happen.
#[tokio::test]
async fn test_in_memory_copy_index_serving() {
    let graphs = [
        GraphName::NamedNode(NamedNode::new("http://example.org/g0").unwrap()),
        GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap()),
    ];
    let quads: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                graphs[i % 2].clone(),
            )
        })
        .collect();
    let expected = |keep: &dyn Fn(usize) -> bool| -> Vec<String> {
        let mut strings: Vec<String> = quads
            .iter()
            .enumerate()
            .filter(|(i, _)| keep(*i))
            .map(|(_, q)| q.to_string())
            .collect();
        strings.sort();
        strings
    };

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate-bound: served from the POSG family's contiguous run — the
    // served `quads()` and the row-id `size()` must agree. The resolution's
    // exact ids stay pending (`size` answers from the run's width; only a
    // consumer that needs the ids decodes them).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert!(by_p.debug_selection_pending());
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(view_strings(&by_p).await, expected(&|i| i % 3 == 1));
    // A base-order gather cannot ride the plan: it materializes the pending
    // ids and must agree with the served read.
    assert_eq!(by_p.get_quads_array().await.unwrap().len(), 10);

    // Object-bound: served from the OSPG family.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert!(by_o.debug_selection_pending());
    assert_eq!(view_strings(&by_o).await, expected(&|i| i % 5 == 2));

    // Predicate and object: one (p, o) prefix probe fully resolves the
    // pattern, so the narrowed run is served directly.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert!(by_po.debug_has_serve_plan());
    assert!(by_po.debug_selection_pending());
    assert_eq!(view_strings(&by_po).await, expected(&|i| i % 15 == 1));

    // A residual graph constraint leaves a mask scan to run — which already
    // gathers the rows — so the serve plan is dropped (it would save
    // nothing), and the result still comes out right.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
        .await
        .unwrap();
    assert!(!by_pg.debug_has_serve_plan());
    // The residual scan needed the ids, so nothing is left pending either.
    assert!(!by_pg.debug_selection_pending());
    assert_eq!(
        view_strings(&by_pg).await,
        expected(&|i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining narrows the first view's row ids — materializing them — so
    // its serve plan drops.
    let chained = by_p
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert!(!chained.debug_has_serve_plan());
    assert!(!chained.debug_selection_pending());
    assert_eq!(
        view_strings(&chained).await,
        expected(&|i| i % 3 == 1 && i % 5 == 1)
    );

    // A tombstoned row vanishes from served streams too: the slice reads
    // copy rows, so the delete reaches it through the rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_after.debug_has_serve_plan());
    assert_eq!(by_p_after.size().await.unwrap(), 9);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected(&|i| i % 3 == 1 && i != 4)
    );

    // Deleting by a served pattern: the matcher's doomed view carries pending
    // ids, which the delete materializes into tombstones.
    let wiped = deleted
        .delete_matching(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(wiped.size().await.unwrap(), 20);
    let by_p_wiped = wiped
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_wiped.size().await.unwrap(), 0);
    assert_eq!(view_strings(&by_p_wiped).await, Vec::<String>::new());
}

/// A built store's resident form: construction compresses the base's code
/// columns and every component's integer children into probe-supported
/// encodings — no canonical primitives are retained — while every sorted
/// column still binds an encoded search probe, and the payload path still
/// serves codes (through the base's `vortex.shared` wrappers).
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_built_store_compresses_resident_form() {
    let quads = modular_quads(200, 4, 8);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    assert!(
        !store.debug_base_int_children_canonical(),
        "construction must retain compressed code columns, not canonical primitives"
    );
    assert!(
        store.debug_base_probe_resolvable(),
        "every sorted column of the compressed base must bind an encoded search probe"
    );
    for name in ["index:posg", "index:ospg"] {
        assert_eq!(
            store.debug_index_component_int_children_canonical(name),
            Some(false),
            "{name}: component children must stay compressed too"
        );
    }

    // The payload path still answers: codes decode to exactly the matched
    // quads (first touch materializes the shared canonical, then zero-copy).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let cols = matched
        .code_columns()
        .expect("compressed base still serves codes through its shared wrappers");
    let dict = matched.code_read_snapshot().unwrap();
    let mut got: Vec<String> = (0..cols[0].len())
        .map(|i| {
            format!(
                "{} {} {}",
                dict.decode(cols[0][i]).unwrap(),
                dict.decode(cols[1][i]).unwrap(),
                dict.decode(cols[2][i]).unwrap()
            )
        })
        .collect();
    got.sort();
    let mut want: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 4 == 1)
        .map(|(_, q)| format!("{} {} {}", q.subject, q.predicate, q.object))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

/// The bindings' code-column read on a served in-memory match: `code_columns`
/// rides the serve plan, reading the codes off the answering index's own
/// columns — so the resolution's row ids stay unmaterialized — and the codes
/// it hands out address the cached dictionary and name exactly the matched
/// quads.
#[tokio::test]
async fn test_code_columns_serves_from_the_answering_index() {
    let quads: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(matched.debug_selection_pending());

    let cols = matched
        .code_columns()
        .expect("an in-memory Dictionary view answers codes");
    assert_eq!(
        matched.debug_row_ids_materialized(),
        Some(false),
        "a served code read must not materialize the resolution's row ids"
    );
    let dict = matched.code_read_snapshot().unwrap();
    let mut got: Vec<String> = (0..cols[0].len())
        .map(|i| {
            format!(
                "{} {} {}",
                dict.decode(cols[0][i]).unwrap(),
                dict.decode(cols[1][i]).unwrap(),
                dict.decode(cols[2][i]).unwrap()
            )
        })
        .collect();
    got.sort();
    let mut want: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 1)
        .map(|(_, q)| format!("{} {} {}", q.subject, q.predicate, q.object))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

/// The file-backed copy index end to end: pattern shapes it accelerates,
/// copy-served `quads()` streams (including residual graph constraints and
/// tombstoned rows filtered through the family's rid column), and chained
/// matches falling back to row ids.
#[cfg(feature = "file-io")]
async fn run_copy_index_file_serving_test(layout: LayoutStrategy, located: bool) {
    let graphs = [
        GraphName::NamedNode(NamedNode::new("http://example.org/g0").unwrap()),
        GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap()),
    ];
    let quads: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                graphs[i % 2].clone(),
            )
        })
        .collect();
    let expected = |keep: &dyn Fn(usize) -> bool| -> Vec<String> {
        let mut strings: Vec<String> = quads
            .iter()
            .enumerate()
            .filter(|(i, _)| keep(*i))
            .map(|(_, q)| q.to_string())
            .collect();
        strings.sort();
        strings
    };

    let (_dir, path) =
        write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate-bound: i ≡ 1 (mod 3), served from the POSG family. On a
    // located resolution (sorted dictionary-code copies) the small run's
    // ids resolve eagerly by rid point reads; otherwise the rid scan stays
    // pending until `size` — the served `quads()` never runs it either way.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert_eq!(by_p.debug_selection_pending(), !located);
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(view_strings(&by_p).await, expected(&|i| i % 3 == 1));
    // A base-order gather cannot ride the plan: it materializes the pending
    // ids (the deferred index-child scan) and must agree with the served read.
    assert_eq!(by_p.get_quads_array().await.unwrap().len(), 10);

    // Object-bound: i ≡ 2 (mod 5), served from the OSPG family.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert_eq!(by_o.debug_selection_pending(), !located);
    assert_eq!(by_o.size().await.unwrap(), 6);
    assert_eq!(view_strings(&by_o).await, expected(&|i| i % 5 == 2));

    // Predicate and object bound: one (p, o) prefix resolution —
    // i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 5) ⇔ i ≡ 1 (mod 15).
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_po.size().await.unwrap(), 2);
    assert_eq!(view_strings(&by_po).await, expected(&|i| i % 15 == 1));

    // A residual graph constraint rides the copy-served scan's filter; the
    // selection's ids cover the resolved predicate only — `size` evaluates
    // the residual on top of them (eager on a located resolution, pending
    // otherwise).
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
        .await
        .unwrap();
    assert!(by_pg.debug_has_serve_plan());
    assert_eq!(by_pg.debug_selection_pending(), !located);
    assert_eq!(by_pg.size().await.unwrap(), 5);
    assert_eq!(
        view_strings(&by_pg).await,
        expected(&|i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining a second match narrows the first view's row ids (the copy
    // plan is dropped — its filter no longer selects exactly the rows).
    let chained = by_p
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert!(!chained.debug_has_serve_plan());
    assert!(!chained.debug_selection_pending());
    assert_eq!(
        view_strings(&chained).await,
        expected(&|i| i % 3 == 1 && i % 5 == 1)
    );

    // A term the store has never seen short-circuits to empty.
    let missing = NamedNode::new("http://example.org/nope").unwrap();
    let none = store
        .match_pattern(None, Some(&missing), None, None)
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);

    // A term the store knows — but never as a predicate — probes the index
    // child and finds nothing.
    let subject_as_p = NamedNode::new("http://example.org/s00").unwrap();
    let zero = store
        .match_pattern(None, Some(&subject_as_p), None, None)
        .await
        .unwrap();
    // A located resolution proves the emptiness at match time and
    // short-circuits; an unlocated one only discovers it at the consumer.
    assert_eq!(zero.debug_selection_pending(), !located);
    assert_eq!(view_strings(&zero).await, Vec::<String>::new());
    assert_eq!(zero.size().await.unwrap(), 0);

    // A tombstoned row must vanish from copy-served streams too: the scan
    // reads copy rows, so the delete reaches it through the rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_after.size().await.unwrap(), 9);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected(&|i| i % 3 == 1 && i != 4)
    );

    // Deleting by a served pattern: the matcher's doomed view carries pending
    // ids, which the delete materializes into tombstones.
    let wiped = deleted
        .delete_matching(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(wiped.size().await.unwrap(), 20);
    let by_p_wiped = wiped
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(by_p_wiped.size().await.unwrap(), 0);
    assert_eq!(view_strings(&by_p_wiped).await, Vec::<String>::new());
}

/// A located run wider than the point-read cap keeps the deferred contract:
/// the rid scan stays pending until a consumer needs the selection, and the
/// served stream reads through the filter scan — both agreeing with the
/// in-memory store.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_copy_index_file_serving_wide_located_run_stays_pending() {
    let quads: Vec<Quad> = (0..900)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:04}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 7),
                GraphName::DefaultGraph,
            )
        })
        .collect();
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
    assert!(
        by_p.debug_selection_pending(),
        "a 300-row run exceeds the point-read cap and must stay deferred"
    );
    assert_eq!(by_p.size().await.unwrap(), 300);
    let mut want: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 1)
        .map(|(_, q)| q.to_string())
        .collect();
    want.sort();
    assert_eq!(view_strings(&by_p).await, want);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_copy_index_file_serving_default() {
    run_copy_index_file_serving_test(LayoutStrategy::Default, false).await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_copy_index_file_serving_typed() {
    run_copy_index_file_serving_test(LayoutStrategy::TypedObject, false).await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_copy_index_file_serving_dictionary() {
    run_copy_index_file_serving_test(LayoutStrategy::Dictionary, true).await;
}

// ─── 4c) SecondaryByReference on a file ─────────────────────────────────

/// The file-backed reference index end to end: on a sorted dictionary-code
/// child every covered shape locates its matched run through the value
/// column's chunk probes (small runs read their row ids point by point, wide
/// ones by a scan restricted to the run); on a string-valued child the same
/// shapes decline the location and answer through the pushed-down scan. Both
/// must agree, row for row, with the in-memory store over the same quads.
#[cfg(feature = "file-io")]
async fn run_reference_index_file_test(layout: LayoutStrategy, located: bool) {
    // 900 quads: 300 per predicate (a run wider than the point-read cap) and
    // ~129 per object (one narrow enough to point-read).
    let quads: Vec<Quad> = (0..900)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:04}"),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 7),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let expected = |keep: &dyn Fn(usize) -> bool| -> Vec<String> {
        let mut strings: Vec<String> = quads
            .iter()
            .enumerate()
            .filter(|(i, _)| keep(*i))
            .map(|(_, q)| q.to_string())
            .collect();
        strings.sort();
        strings
    };

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
    assert_eq!(view_strings(&by_o).await, expected(&|i| i % 7 == 2));
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
    assert_eq!(view_strings(&by_p).await, expected(&|i| i % 3 == 1));

    // Predicate and object bound: this index probes the object column only,
    // leaving the predicate as a residual filter over the located rows —
    // i ≡ 2 (mod 7) ∧ i ≡ 1 (mod 3) ⇔ i ≡ 16 (mod 21).
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(view_strings(&by_po).await, expected(&|i| i % 21 == 16));

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
        expected(&|i| i % 7 == 2 && i != 2)
    );

    // Chaining composes the resolutions the same way either path resolves
    // them.
    let chained = by_p
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(view_strings(&chained).await, expected(&|i| i % 21 == 16));
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_reference_index_file_dictionary() {
    run_reference_index_file_test(LayoutStrategy::Dictionary, true).await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_reference_index_file_default() {
    run_reference_index_file_test(LayoutStrategy::Default, false).await;
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_reference_index_file_typed() {
    run_reference_index_file_test(LayoutStrategy::TypedObject, false).await;
}
