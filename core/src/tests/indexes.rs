//! Secondary indexes on in-memory stores: the layout × index-family matrix,
//! index survival across views, deletes and compaction, and the copy
//! family's serving path.

use super::*;
use crate::store::{ExportOptions, QuadColumn, TermEncoding};

// ─── Secondary index behavior ──────────────────────────────────────────

/// The same index requested twice contributes its children exactly once,
/// under every layout: the quad rows carry the layout's primary columns and
/// nothing else, the roster is deduplicated, and where the layout has a
/// single `o` column each child's `val` column shares its dtype.
#[tokio::test]
async fn test_duplicate_index_requests_are_deduplicated() {
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(two_quads()),
            layout,
            vec![
                IndexType::SecondaryByReference,
                IndexType::SecondaryByReference,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("{layout:?}: build failed: {e}"));

        let vortex_array::dtype::DType::Struct(fields, _) = arr.array.dtype() else {
            panic!("{layout:?}: expected StructArray dtype");
        };
        let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
        assert_eq!(names, primary_columns(layout), "{layout:?}");
        assert_eq!(
            component_names(&arr),
            ["index:ref-o", "index:ref-p"],
            "{layout:?}"
        );
        if let Some(o_dtype) = fields.field("o") {
            for component in &arr.components {
                let rows = component.rows().unwrap();
                let vortex_array::dtype::DType::Struct(child_fields, _) = rows.dtype() else {
                    panic!("{layout:?}: expected a struct child");
                };
                assert_eq!(
                    child_fields.field("val"),
                    Some(o_dtype.clone()),
                    "{layout:?}"
                );
            }
        }

        // Index-routed matching still works.
        let store = VortexRdfStore::from_built(arr).unwrap();
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size().await.unwrap(), 1, "{layout:?}");
    }
}

/// A store derived by matching keeps its indexes, because a view narrows a
/// selection over the base rather than rewriting rows — so the components'
/// `rid` values still address the data. This is what lets a chained match keep
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
    assert_eq!(
        subjects_of(&chained).await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string()
        ]
    );
}

/// `compact_with_indexes` carries exactly the requested index set, whatever
/// the source had: an empty set drops the index while keeping every live
/// row, the source is left untouched, and a store built without indexes
/// gains one it never had. (The rebuild over the fresh row order is pinned
/// per layout by `run_compact_with_indexes_layout`.)
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

    // An empty index set drops the index and keeps exactly the view's rows.
    let sorted = view.compact_with_indexes(vec![]).await.unwrap();
    assert!(sorted.indexes().is_empty());
    assert_eq!(sorted.size().await.unwrap(), 6);
    assert_eq!(
        subjects_of(&sorted).await,
        [1, 5, 9, 13, 17, 21]
            .map(|i| format!("<http://example.org/s{i:02}>"))
            .to_vec()
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

    // The copy family rebuilds the same way: its sorted copies are cut from
    // the gathered rows, and a predicate match is then served from them.
    let by_copy = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap()
        .compact_with_indexes(vec![IndexType::SecondaryByCopy])
        .await
        .unwrap();
    assert_eq!(by_copy.indexes(), &[IndexType::SecondaryByCopy]);
    assert_eq!(by_copy.size().await.unwrap(), 6);
    let served = by_copy
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    assert!(
        served.debug_has_serve_plan(),
        "{layout:?}: a rebuilt copy index must serve the predicate match"
    );
    assert_eq!(
        view_strings(&served).await,
        expected_strings(&quads, |i| i % 4 == 1 && i % 3 == 1)
    );
}

#[tokio::test]
async fn test_compact_with_indexes_default() {
    run_compact_with_indexes_layout(LayoutStrategy::Default).await;
}

/// Compacting down to nothing still builds the requested indexes: a store
/// whose rows all went away keeps the roster (and, under the Dictionary
/// layout, its children's code dtypes) a non-empty compaction would have
/// given it, so the result is a usable indexed store rather than one that
/// silently lost its indexes.
async fn run_compact_to_empty_keeps_indexes(layout: LayoutStrategy) {
    let quads = modular_quads(8, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        layout,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // A pattern nothing matches: the view is empty, and so is its compaction.
    let absent = Term::Literal(Literal::new_simple_literal("no such object"));
    let empty = store
        .match_pattern(None, None, Some(&absent), None)
        .await
        .unwrap()
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
    assert_eq!(empty.indexes(), &[IndexType::SecondaryByReference]);

    // The empty children are still routable — a probe answers empty rather
    // than erroring or falling back.
    let object = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        empty
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn test_compact_to_empty_keeps_indexes_default() {
    run_compact_to_empty_keeps_indexes(LayoutStrategy::Default).await;
}

#[tokio::test]
async fn test_compact_to_empty_keeps_indexes_dictionary() {
    run_compact_to_empty_keeps_indexes(LayoutStrategy::Dictionary).await;
}

#[tokio::test]
async fn test_compact_to_empty_keeps_indexes_typed_object() {
    run_compact_to_empty_keeps_indexes(LayoutStrategy::TypedObject).await;
}

/// The TypedObject rebuild recomposes every object kind from the typed
/// sub-columns — IRI, blank node, plain, language-tagged and typed literals
/// — so a reference index built by compaction routes each of them to exactly
/// its row.
#[tokio::test]
async fn test_compact_with_indexes_typed_object_routes_every_object_kind() {
    let quads = term_kind_quads();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert!(store.indexes().is_empty());

    let indexed = store
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(view_strings(&indexed).await, quad_strings(&quads));
    for quad in &quads {
        let by_o = indexed
            .match_pattern(None, None, Some(&quad.object), None)
            .await
            .unwrap();
        assert_eq!(
            view_strings(&by_o).await,
            vec![quad.to_string()],
            "object {} must route to exactly its row",
            quad.object
        );
    }
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
    assert_eq!(
        subjects_of(&matched).await,
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
        // Whatever the builder or layout, index data never rides in the quad
        // rows: those carry the layout's primary columns and nothing else.
        assert_eq!(
            names,
            primary_columns(layout),
            "builder={builder_name} layout={layout_name} indexes={index_name}",
        );
        let expect_ref = indexes.contains(&IndexType::SecondaryByReference);
        let expect_copy = indexes.contains(&IndexType::SecondaryByCopy);
        // Each requested family contributes its children, all or nothing.
        let components = component_names(&arr);
        let has = |roster: [&str; 2]| {
            let present = roster.iter().filter(|c| components.contains(c)).count();
            assert!(
                present == 0 || present == roster.len(),
                "partial component roster {components:?} for builder={builder_name} layout={layout_name} indexes={index_name}",
            );
            present == roster.len()
        };
        assert!(
            has(["index:ref-o", "index:ref-p"]) == expect_ref
                && has(["index:posg", "index:ospg"]) == expect_copy,
            "index component mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
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

    // The copy family serves a predicate- or object-bound match whenever it
    // is present, whatever else is; without it nothing serves.
    let expect_served = indexes.contains(&IndexType::SecondaryByCopy);

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
    assert_eq!(
        by_pred.debug_has_serve_plan(),
        expect_served,
        "predicate serve plan mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
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
    assert_eq!(
        by_obj.debug_has_serve_plan(),
        expect_served,
        "object serve plan mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
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

// ─── SecondaryByCopy: sorted full-copy index ───────────────────────────

/// The two graphs and the 30-quad dataset every copy-index serving script
/// runs over: `s{i:02} p{i % 3} "o{i % 5}"` alternating between the graphs.
pub(super) fn copy_index_script_dataset() -> (Vec<Quad>, [GraphName; 2]) {
    let graphs = [
        GraphName::NamedNode(NamedNode::new("http://example.org/g0").unwrap()),
        GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap()),
    ];
    (graph_modular_quads(30, 2, 3, 5, &graphs), graphs)
}

/// What differs between the copy index's serving paths — in memory versus on
/// a file, and a located run versus a scanned one — while the served results
/// stay the same.
pub(super) struct ServeExpectations {
    /// Whether a served match leaves its exact row ids pending: an in-memory
    /// run and an unlocated file run answer `size` from the run itself, a
    /// located file run resolves its ids eagerly by point reads.
    pub(super) served_pending: bool,
    /// Whether a residual graph constraint keeps the serve plan (the file
    /// scan carries the residual as a filter) or drops it (the in-memory
    /// path runs a mask scan that already gathers the rows).
    pub(super) residual_graph_served: bool,
    /// Whether a known term that never occurs as a predicate is only found
    /// empty at the consumer (pending) or proven empty at match time.
    pub(super) never_predicate_pending: bool,
}

/// The copy index's serving script over `store`, built from `quads` (the
/// [`copy_index_script_dataset`]) with `SecondaryByCopy`: predicate / object
/// / predicate+object matches read the matched quads straight from the copy
/// family's contiguous run, a residual graph constraint and a chained match
/// still answer exactly, appended and tombstoned rows reach the served
/// stream, and a delete by a served pattern lands its tombstones.
pub(super) async fn run_copy_index_serving_script(
    store: VortexRdfStore,
    quads: &[Quad],
    graphs: &[GraphName],
    expect: ServeExpectations,
) {
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

    // Predicate-bound: i ≡ 1 (mod 3), served from the POSG family's run —
    // the served `quads()` and the row-id `size()` must agree.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p.debug_has_serve_plan());
    assert_eq!(by_p.debug_selection_pending(), expect.served_pending);
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(quads, |i| i % 3 == 1)
    );
    // The derived view keeps the index.
    assert_eq!(by_p.indexes(), &[IndexType::SecondaryByCopy]);
    // A base-order gather cannot ride the plan: it materializes the ids and
    // must agree with the served read.
    assert_eq!(by_p.selected_rows().await.unwrap().len(), 10);

    // Object-bound: i ≡ 2 (mod 5), served from the OSPG family.
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert!(by_o.debug_has_serve_plan());
    assert_eq!(by_o.debug_selection_pending(), expect.served_pending);
    assert_eq!(by_o.size().await.unwrap(), 6);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(quads, |i| i % 5 == 2)
    );

    // Predicate and object: one (p, o) prefix resolution fully resolves the
    // pattern — i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 5) ⇔ i ≡ 1 (mod 15) — so the
    // narrowed run is served directly.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let by_po = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert!(by_po.debug_has_serve_plan());
    assert_eq!(by_po.debug_selection_pending(), expect.served_pending);
    assert_eq!(by_po.size().await.unwrap(), 2);
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(quads, |i| i % 15 == 1)
    );

    // A residual graph constraint on top of the resolved predicate.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let by_pg = store
        .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
        .await
        .unwrap();
    assert_eq!(by_pg.debug_has_serve_plan(), expect.residual_graph_served);
    assert_eq!(
        by_pg.debug_selection_pending(),
        expect.residual_graph_served && expect.served_pending
    );
    assert_eq!(by_pg.size().await.unwrap(), 5);
    assert_eq!(
        view_strings(&by_pg).await,
        expected_strings(quads, |i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining narrows the first view's row ids — materializing them — so
    // its serve plan drops (its filter no longer selects exactly the rows).
    let chained = by_p
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert!(!chained.debug_has_serve_plan());
    assert!(!chained.debug_selection_pending());
    assert_eq!(
        view_strings(&chained).await,
        expected_strings(quads, |i| i % 3 == 1 && i % 5 == 1)
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
    assert_eq!(
        zero.debug_selection_pending(),
        expect.never_predicate_pending
    );
    assert_eq!(view_strings(&zero).await, Vec::<String>::new());
    assert_eq!(zero.size().await.unwrap(), 0);

    // An appended quad rides the tail beside the served base run: the
    // served predicate match still engages for the base and the tail row
    // joins its rows, on a view that keeps the index.
    let appended = make_quad(
        "http://example.org/s99",
        "http://example.org/p1",
        "o1",
        graphs[0].clone(),
    );
    let tailed = store.add_quad(appended.clone()).await.unwrap();
    assert_eq!(tailed.indexes(), &[IndexType::SecondaryByCopy]);
    let by_p_tailed = tailed
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_tailed.debug_has_serve_plan());
    assert_eq!(by_p_tailed.size().await.unwrap(), 11);
    let mut want = expected_strings(quads, |i| i % 3 == 1);
    want.push(appended.to_string());
    want.sort();
    assert_eq!(view_strings(&by_p_tailed).await, want);

    // A tombstoned row vanishes from served streams too: the read takes copy
    // rows, so the delete reaches it through the family's rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(by_p_after.debug_has_serve_plan());
    assert_eq!(by_p_after.size().await.unwrap(), 9);
    assert_eq!(
        view_strings(&by_p_after).await,
        expected_strings(quads, |i| i % 3 == 1 && i != 4)
    );

    // Deleting by a served pattern: the matcher's doomed view carries the
    // resolution's ids, which the delete materializes into tombstones.
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

/// The serving script over an in-memory store: every served run answers
/// from a slice of the base with its ids pending, a residual graph
/// constraint drops the plan for a mask scan that gathers the rows itself,
/// and a known-but-never-predicate term is proven empty by the in-memory
/// probe at match time.
#[tokio::test]
async fn test_in_memory_copy_index_serving() {
    let (quads, graphs) = copy_index_script_dataset();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    run_copy_index_serving_script(
        store,
        &quads,
        &graphs,
        ServeExpectations {
            served_pending: true,
            residual_graph_served: false,
            never_predicate_pending: false,
        },
    )
    .await;
}

// ─── Resident form and code reads over indexes ─────────────────────────

/// A built store's resident form: the base's code columns are flat
/// canonical primitives — served zero-copy by every code read, and still
/// binding an encoded search probe — while every component's integer
/// children are compressed into probe-supported encodings, the columns
/// nothing ever reads as a payload.
#[tokio::test]
async fn test_built_store_resident_form() {
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
        store.debug_base_int_children_canonical(),
        "construction must hold the base's code columns as canonical primitives"
    );
    assert!(
        store.debug_base_probe_resolvable(),
        "every sorted column of the canonical base must bind an encoded search probe"
    );
    for name in ["index:posg", "index:ospg"] {
        assert_eq!(
            store.debug_index_component_int_children_canonical(name),
            Some(false),
            "{name}: component children must be compressed"
        );
    }

    // The payload path answers off the canonical columns: codes decode to
    // exactly the matched quads, and no live canonical form is ever made.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let cols = matched
        .code_columns(&QuadColumn::ALL)
        .unwrap()
        .expect("a canonical base serves codes without a decode");
    for idx in 0..4 {
        assert_eq!(matched.debug_live_canonical_alive(idx), Some(false));
    }
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
    let quads = graph_modular_quads(30, 2, 3, 5, &[GraphName::DefaultGraph]);
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
        .code_columns(&QuadColumn::ALL)
        .unwrap()
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

/// A served run wider than a point read exports its codes as slices of the
/// component's live canonical form: two exports of one match are the same
/// buffers, a projection decodes only its columns, the form is freed with
/// the last batch, the row ids stay unmaterialized — and a run under 1/32 of
/// the component decodes itself, leaving the cache cold. Tombstones filter
/// the run through its rid column.
#[tokio::test]
async fn test_wide_served_run_exports_slices_of_the_component() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt32Type;
    use futures::TryStreamExt;

    async fn export(
        view: &VortexRdfStore,
        projection: Option<&[QuadColumn]>,
    ) -> Vec<arrow_array::RecordBatch> {
        let mut options = ExportOptions::new(TermEncoding::Codes);
        if let Some(projection) = projection {
            options = options.projection(projection);
        }
        view.to_record_batches(&options)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }
    fn ptr(batch: &arrow_array::RecordBatch, column: usize) -> *const u32 {
        batch
            .column(column)
            .as_primitive::<UInt32Type>()
            .values()
            .as_ptr()
    }
    let spelled = |view: &VortexRdfStore, batches: &[arrow_array::RecordBatch]| -> Vec<String> {
        let dict = view.code_read_snapshot().unwrap();
        let dict = &dict;
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |i| {
                    (0..3)
                        .map(|c| {
                            dict.decode(batch.column(c).as_primitive::<UInt32Type>().value(i))
                                .unwrap()
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
            })
            .collect();
        rows.sort();
        rows
    };
    let expected = |quads: &[Quad], p: usize, skip: Option<&Quad>| -> Vec<String> {
        let mut want: Vec<String> = quads
            .iter()
            .filter(|q| q.predicate.as_str() == format!("http://example.org/p{p}"))
            .filter(|q| skip != Some(q))
            .map(|q| format!("{} {} {}", q.subject, q.predicate, q.object))
            .collect();
        want.sort();
        want
    };

    // 3,000 rows, three predicates: a 1,000-row run, a third of the component.
    let quads = graph_modular_quads(3000, 4, 3, 5, &[GraphName::DefaultGraph]);
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let matched = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(matched.debug_selection_pending());
    let posg = "index:posg";
    for idx in 0..4 {
        assert_eq!(
            matched.debug_component_canonical_alive(posg, idx),
            Some(false)
        );
    }

    let first = export(&matched, None).await;
    let second = export(&matched, None).await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].num_rows(), 1000);
    for column in 0..4 {
        assert_eq!(
            ptr(&first[0], column),
            ptr(&second[0], column),
            "column {column}: both exports slice the component's one live form"
        );
        assert_eq!(
            matched.debug_component_canonical_alive(posg, column),
            Some(true)
        );
    }
    assert_eq!(
        matched.debug_row_ids_materialized(),
        Some(false),
        "a served export never materializes the resolution's row ids"
    );
    assert_eq!(spelled(&matched, &first), expected(&quads, 0, None));
    drop(first);
    drop(second);
    for idx in 0..4 {
        assert_eq!(
            matched.debug_component_canonical_alive(posg, idx),
            Some(false),
            "column {idx} is freed with the last batch"
        );
    }

    // A projection decodes only the columns it names.
    let projected = export(&matched, Some(&[QuadColumn::S, QuadColumn::O])).await;
    assert_eq!(projected[0].num_columns(), 2);
    for (idx, alive) in [(0, true), (1, false), (2, true), (3, false)] {
        assert_eq!(
            matched.debug_component_canonical_alive(posg, idx),
            Some(alive)
        );
    }
    drop(projected);

    // Tombstones: the run is filtered through its rid column.
    let store = store.delete_quad(&quads[0]).await.unwrap();
    let matched = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    let live = export(&matched, None).await;
    assert_eq!(live.iter().map(|b| b.num_rows()).sum::<usize>(), 999);
    assert_eq!(
        spelled(&matched, &live),
        expected(&quads, 0, Some(&quads[0]))
    );

    // A run under 1/32 of the component decodes itself; the cache stays cold.
    let quads = graph_modular_quads(40_000, 5, 100, 7, &[GraphName::DefaultGraph]);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let p7 = NamedNode::new("http://example.org/p7").unwrap();
    let matched = store
        .match_pattern(None, Some(&p7), None, None)
        .await
        .unwrap();
    let narrow = export(&matched, None).await;
    assert_eq!(narrow[0].num_rows(), 400);
    for idx in 0..4 {
        assert_eq!(
            matched.debug_component_canonical_alive(posg, idx),
            Some(false)
        );
    }
    assert_eq!(spelled(&matched, &narrow), expected(&quads, 7, None));
}
