//! Secondary indexes on in-memory stores: the layout × index-family matrix,
//! index survival across views, deletes and compaction, and the copy
//! family's serving path.

use super::*;

// ─── Secondary index behavior ──────────────────────────────────────────

/// The same index requested twice contributes its children exactly once,
/// under every layout: the quad rows carry the layout's primary columns and
/// nothing else, the roster is deduplicated, and where the layout has a
/// single `o` column each child's `val` column shares its dtype. The
/// out-of-core builder dedups a repeated, non-canonical request the same
/// way, and reports the index set and its children in `ALL_INDEX_TYPES`
/// order whatever the request's.
#[tokio::test]
async fn test_duplicate_index_requests_are_deduplicated() {
    for layout in LAYOUTS {
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
        let p1 = iri("p1");
        let matched = matched_view(&store, None, Some(&p1), None, None).await;
        assert_eq!(matched.size().await.unwrap(), 1, "{layout:?}");

        // The out-of-core builder, on a repeated request out of order: each
        // family once, in `ALL_INDEX_TYPES` order.
        let streamed = build_array::<SortedStreamBuilder>(
            quad_stream(two_quads()),
            layout,
            vec![
                IndexType::SecondaryByReference,
                IndexType::SecondaryByCopy,
                IndexType::SecondaryByReference,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("{layout:?}: stream build failed: {e}"));
        let streamed = VortexRdfStore::from_built(streamed).unwrap();
        assert_eq!(
            streamed.indexes(),
            [IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            "{layout:?}"
        );
        let children: Vec<&str> = streamed
            .index_components()
            .unwrap()
            .iter()
            .map(|child| child.name)
            .collect();
        assert_eq!(
            children,
            ["index:posg", "index:ospg", "index:ref-o", "index:ref-p"],
            "{layout:?}"
        );
    }
}

/// A store derived by matching keeps its indexes, because a view narrows a
/// selection over the base rather than rewriting rows — so the components'
/// `rid` values still address the data. This is what lets a chained match keep
/// routing through the index instead of degrading to a scan.
#[tokio::test]
async fn test_in_memory_derived_view_keeps_indexes() {
    let quads = modular_quads(24, 3, 4);

    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    assert_eq!(store.indexes(), &vec![IndexType::SecondaryByReference]);

    // Match on the object index: 24 quads over 4 objects ⇒ 6 rows.
    let object = plain_literal("object 1");
    let matched = matched_view(&store, None, None, Some(&object), None).await;
    assert_eq!(matched.size().await.unwrap(), 6);
    assert_eq!(
        matched.indexes(),
        &vec![IndexType::SecondaryByReference],
        "a derived view must keep the base's indexes"
    );

    // Chain a second, index-routed match onto the derived view. Of those 6
    // rows (i = 1, 5, 9, 13, 17, 21), the ones with predicate p1 are
    // i ≡ 1 (mod 3): 1, 13 — the intersection of two index lookups.
    let predicate = iri("p1");
    let chained = matched_view(&matched, None, Some(&predicate), None, None).await;
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

    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    // A view over the object index: i = 1, 5, 9, 13, 17, 21 ⇒ 6 rows.
    let object = plain_literal("object 1");
    let view = matched_view(&store, None, None, Some(&object), None).await;
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
    let bare = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;
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
    let routed = matched_view(&reindexed, None, None, Some(&object), None).await;
    assert_eq!(routed.size().await.unwrap(), 6);
}

/// The index rebuild reads its value columns from the materialized array in
/// each layout's own representation: `o`/`p` strings (Default), codes
/// (Dictionary), and the object term recomposed from typed sub-columns
/// (TypedObject). Exercise all three end-to-end.
async fn run_compact_with_indexes_layout(layout: LayoutStrategy) {
    let quads = modular_quads(24, 3, 4);

    let store = built_store(quads.clone(), layout, vec![IndexType::SecondaryByReference]).await;

    let object = plain_literal("object 1");
    let indexed = matched_view(&store, None, None, Some(&object), None)
        .await
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(indexed.size().await.unwrap(), 6);

    // Route through both the rebuilt object and predicate columns.
    let predicate = iri("p1");
    assert_eq!(
        subjects_of(&matched_view(&indexed, None, Some(&predicate), None, None).await).await,
        vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]
    );
    assert_eq!(
        matched_size(&indexed, None, None, Some(&object), None).await,
        6,
    );

    // The copy family rebuilds the same way: its sorted copies are cut from
    // the gathered rows, and a predicate match is then served from them.
    let by_copy = matched_view(&store, None, None, Some(&object), None)
        .await
        .compact_with_indexes(vec![IndexType::SecondaryByCopy])
        .await
        .unwrap();
    assert_eq!(by_copy.indexes(), &[IndexType::SecondaryByCopy]);
    assert_eq!(by_copy.size().await.unwrap(), 6);
    let served = matched_view(&by_copy, None, Some(&predicate), None, None).await;
    assert_eq!(
        served_component(&served),
        Some("index:posg"),
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
    let store = built_store(quads.clone(), layout, vec![IndexType::SecondaryByReference]).await;

    // A pattern nothing matches: the view is empty, and so is its compaction.
    let absent = plain_literal("no such object");
    let empty = matched_view(&store, None, None, Some(&absent), None)
        .await
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
    assert_eq!(empty.indexes(), &[IndexType::SecondaryByReference]);

    // The empty children are still routable — a probe answers empty rather
    // than erroring or falling back.
    let object = plain_literal("object 1");
    assert_eq!(
        matched_size(&empty, None, None, Some(&object), None).await,
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
    let store = built_store(quads.clone(), LayoutStrategy::TypedObject, vec![]).await;
    assert!(store.indexes().is_empty());

    let indexed = store
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(indexed.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(view_strings(&indexed).await, quad_strings(&quads));
    for quad in &quads {
        let by_o = matched_view(&indexed, None, None, Some(&quad.object), None).await;
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

    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

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
    let object = plain_literal("object 0");
    let matched = matched_view(&after, None, None, Some(&object), None).await;
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
/// requested index families ride in and that construction resolved their
/// probes, and run the shared match battery, the code layout's searches
/// reading those probes.
async fn run_index_matrix_cell<B: VortexArrayBuilder>(
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_name: &'static str,
    indexes: Indexes,
    quads: Vec<Quad>,
) {
    use crate::store::scan::typed_eq::probe_hooks;

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

    let names = component_names(&arr);
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(
        store.size().await.unwrap(),
        quads.len(),
        "size mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );
    // Construction resolves every component's probes, ahead of any query.
    for name in names {
        assert_eq!(
            store.debug_index_component_probes_resolved(name),
            Some(true),
            "{name} probes for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
    }

    // Where the columns hold codes, a bound subject and an index-resolved
    // predicate or object are searched through the probes the construction
    // cached, none resolved for the call.
    // Assumes an in-memory match never suspends between the hooks' per-thread reads.
    let cached_searches = |before: (usize, usize), what: &str| {
        let (cached, resolved) = probe_hooks::binds();
        assert!(
            cached > before.0 && resolved == before.1,
            "{what}: ({cached}, {resolved}) after {before:?} for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
    };
    let codes = layout == LayoutStrategy::Dictionary;
    let s05 = iri_subject("s05");
    let before = probe_hooks::binds();
    let by_subject = matched_view(&store, Some(&s05), None, None, None).await;
    if codes {
        cached_searches(before, "subject");
    }
    assert_eq!(
        view_strings(&by_subject).await,
        expected_strings(&quads, |i| i == 5),
        "subject match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    // The copy family serves a predicate- or object-bound match whenever it
    // is present, whatever else is; without it nothing serves.
    let expect_served = indexes.contains(&IndexType::SecondaryByCopy);
    let index_searched = codes && !indexes.is_empty();

    let p0 = iri("p0");
    let before = probe_hooks::binds();
    let by_pred = matched_view(&store, None, Some(&p0), None, None).await;
    if index_searched {
        cached_searches(before, "predicate");
    }
    assert_eq!(
        by_pred.size().await.unwrap(),
        8,
        "predicate match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );
    assert_eq!(
        served_component(&by_pred),
        expect_served.then_some("index:posg"),
        "predicate serve plan mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let o1 = plain_literal("object 1");
    let before = probe_hooks::binds();
    let by_obj = matched_view(&store, None, None, Some(&o1), None).await;
    if index_searched {
        cached_searches(before, "object");
    }
    assert_eq!(
        by_obj.size().await.unwrap(),
        6,
        "object match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );
    assert_eq!(
        served_component(&by_obj),
        expect_served.then_some("index:ospg"),
        "object serve plan mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    let p1 = iri("p1");
    let by_both = matched_view(&store, None, Some(&p1), Some(&o1), None).await;
    assert_eq!(
        by_both.size().await.unwrap(),
        2,
        "combined match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
    );

    // A subject, a predicate or an object the store does not hold matches
    // nothing.
    let missing_s = iri_subject("nope");
    let missing_p = iri("nope");
    let missing_o = plain_literal("nope");
    for (role, size) in [
        (
            "subject",
            matched_size(&store, Some(&missing_s), None, None, None).await,
        ),
        (
            "predicate",
            matched_size(&store, None, Some(&missing_p), None, None).await,
        ),
        (
            "object",
            matched_size(&store, None, None, Some(&missing_o), None).await,
        ),
    ] {
        assert_eq!(
            size, 0,
            "missing-{role} match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
        );
    }
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
        ("both", ALL_INDEX_TYPES.to_vec()),
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
    let graphs = [iri_graph("g0"), iri_graph("g1")];
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
    let p1 = iri("p1");
    let by_p = matched_view(&store, None, Some(&p1), None, None).await;
    assert_eq!(served_component(&by_p), Some("index:posg"));
    assert_eq!(selection_pending(&by_p), expect.served_pending);
    assert_eq!(by_p.size().await.unwrap(), 10);
    assert_eq!(
        view_strings(&by_p).await,
        expected_strings(quads, |i| i % 3 == 1)
    );
    // The derived view keeps the index.
    assert_eq!(by_p.indexes(), &[IndexType::SecondaryByCopy]);
    // A base-order gather cannot ride the plan: it materializes the ids and
    // must agree with the served read.
    let parts = by_p.to_serializable_parts().await.unwrap();
    assert_eq!(parts.array.len(), 10);

    // Object-bound: i ≡ 2 (mod 5), served from the OSPG family.
    let o2 = plain_literal("o2");
    let by_o = matched_view(&store, None, None, Some(&o2), None).await;
    assert_eq!(served_component(&by_o), Some("index:ospg"));
    assert_eq!(selection_pending(&by_o), expect.served_pending);
    assert_eq!(by_o.size().await.unwrap(), 6);
    assert_eq!(
        view_strings(&by_o).await,
        expected_strings(quads, |i| i % 5 == 2)
    );

    // Predicate and object: one (p, o) prefix resolution fully resolves the
    // pattern — i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 5) ⇔ i ≡ 1 (mod 15) — so the
    // narrowed run is served directly.
    let o1 = plain_literal("o1");
    let by_po = matched_view(&store, None, Some(&p1), Some(&o1), None).await;
    assert_eq!(served_component(&by_po), Some("index:posg"));
    assert_eq!(selection_pending(&by_po), expect.served_pending);
    assert_eq!(by_po.size().await.unwrap(), 2);
    assert_eq!(
        view_strings(&by_po).await,
        expected_strings(quads, |i| i % 15 == 1)
    );

    // A residual graph constraint on top of the resolved predicate.
    let p2 = iri("p2");
    let by_pg = matched_view(&store, None, Some(&p2), None, Some(&graphs[0])).await;
    assert_eq!(
        served_component(&by_pg),
        expect.residual_graph_served.then_some("index:posg")
    );
    assert_eq!(
        selection_pending(&by_pg),
        expect.residual_graph_served && expect.served_pending
    );
    assert_eq!(by_pg.size().await.unwrap(), 5);
    assert_eq!(
        view_strings(&by_pg).await,
        expected_strings(quads, |i| i % 3 == 2 && i % 2 == 0)
    );

    // Chaining narrows the first view's row ids — materializing them — so
    // its serve plan drops (its filter no longer selects exactly the rows).
    let chained = matched_view(&by_p, None, None, Some(&o1), None).await;
    assert_eq!(served_component(&chained), None);
    assert!(!selection_pending(&chained));
    assert_eq!(
        view_strings(&chained).await,
        expected_strings(quads, |i| i % 3 == 1 && i % 5 == 1)
    );

    // A term the store has never seen short-circuits to empty.
    let missing = iri("nope");
    let none = matched_view(&store, None, Some(&missing), None, None).await;
    assert_eq!(none.size().await.unwrap(), 0);

    // A term the store knows — but never as a predicate — probes the index
    // child and finds nothing.
    let subject_as_p = iri("s00");
    let zero = matched_view(&store, None, Some(&subject_as_p), None, None).await;
    assert_eq!(selection_pending(&zero), expect.never_predicate_pending);
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
    let by_p_tailed = matched_view(&tailed, None, Some(&p1), None, None).await;
    assert_eq!(served_component(&by_p_tailed), Some("index:posg"));
    assert_eq!(by_p_tailed.size().await.unwrap(), 11);
    let mut want = expected_strings(quads, |i| i % 3 == 1);
    want.push(appended.to_string());
    want.sort();
    assert_eq!(view_strings(&by_p_tailed).await, want);

    // A tombstoned row vanishes from served streams too: the read takes copy
    // rows, so the delete reaches it through the family's rid column.
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    let by_p_after = matched_view(&deleted, None, Some(&p1), None, None).await;
    assert_eq!(served_component(&by_p_after), Some("index:posg"));
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
    let by_p_wiped = matched_view(&wiped, None, Some(&p1), None, None).await;
    assert_eq!(by_p_wiped.size().await.unwrap(), 0);
    assert_eq!(view_strings(&by_p_wiped).await, Vec::<String>::new());
}

/// The serving script over an in-memory store: every served run answers
/// from a slice of the base with its ids pending, a residual graph
/// constraint drops the plan for a mask scan that gathers the rows itself,
/// and a known-but-never-predicate term is proven empty by the in-memory
/// probe at match time. The N-Quads export of a served view reads it in base
/// order, not the plan's. Over code columns, a served run within the
/// point-read cap is read point by point.
#[tokio::test]
async fn test_in_memory_copy_index_serving() {
    use crate::store::indexes::serve::serve_hooks::point_reads;

    async fn nquads(view: VortexRdfStore) -> String {
        let mut out = Vec::new();
        crate::export_rdf(view, &mut out, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        String::from_utf8(out).unwrap()
    }

    let (quads, graphs) = copy_index_script_dataset();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    run_copy_index_serving_script(
        store.clone(),
        &quads,
        &graphs,
        ServeExpectations {
            served_pending: true,
            residual_graph_served: false,
            never_predicate_pending: false,
        },
    )
    .await;

    // A served view's N-Quads export lists its quads in base (s, p, o, g)
    // order; `quads()` reads them in the index's (p, o, s, g) order. A window
    // that cannot cut a row is the served view as it is, plan and order
    // included, while one that cuts a row drops the plan and reads base
    // order.
    let p1 = iri("p1");
    let by_p = matched_view(&store, None, Some(&p1), None, None).await;
    assert_eq!(served_component(&by_p), Some("index:posg"));
    let size = by_p.size().await.unwrap();
    let whole = by_p.window(0, size).await.unwrap();
    assert_eq!(served_component(&whole), Some("index:posg"));
    let cut = by_p.window(0, size - 1).await.unwrap();
    assert_eq!(served_component(&cut), None);
    let exported = nquads(by_p.clone()).await;
    assert_eq!(exported, nquads(whole.clone()).await);
    let lines: Vec<&str> = exported.lines().collect();
    assert!(lines.is_sorted(), "base (s, p, o, g) order: {lines:#?}");
    assert_eq!(
        nquads(cut.clone()).await.lines().collect::<Vec<_>>(),
        lines[..size - 1]
    );
    let cut_lines: Vec<String> = cut
        .quads()
        .map_ok(|q| format!("{q} ."))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        cut_lines,
        lines[..size - 1],
        "a cut window reads base order"
    );
    let served: Vec<Quad> = by_p.quads().try_collect().await.unwrap();
    assert_eq!(
        quad_strings(&served),
        expected_strings(&quads, |i| i % 3 == 1)
    );
    let index_key = |q: &Quad| (q.object.to_string(), q.subject.to_string());
    assert!(
        served
            .windows(2)
            .all(|w| index_key(&w[0]) < index_key(&w[1])),
        "the index's order: {served:#?}"
    );
    let served_lines: Vec<String> = served.iter().map(|q| format!("{q} .")).collect();
    assert_ne!(served_lines, lines, "the two orders differ on this run");
    let whole_served: Vec<Quad> = whole.quads().try_collect().await.unwrap();
    assert_eq!(
        whole_served, served,
        "a window that cannot cut keeps the index's order"
    );

    // Over code columns a served run within the point-read cap is read point
    // by point through the component's probes, and a wider one is sliced;
    // both read the matched quads.
    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let o1 = plain_literal("o1");
    for (object, width, point_read) in [(Some(&o1), 43, true), (None, 300, false)] {
        let by_p = matched_view(&store, None, Some(&p1), object, None).await;
        assert_eq!(served_component(&by_p), Some("index:posg"));
        let before = point_reads();
        let served: Vec<Quad> = by_p.quads().try_collect().await.unwrap();
        assert_eq!(point_reads() - before, usize::from(point_read), "{width}");
        assert_eq!(
            quad_strings(&served),
            expected_strings(&quads, |i| i % 3 == 1 && (object.is_none() || i % 7 == 1)),
            "{width}"
        );
        assert_eq!(served.len(), width);
    }
}

// ─── Resident form and code reads over indexes ─────────────────────────

/// A built store's resident form: construction compresses the base's code
/// columns and every component's integer children into probe-supported
/// encodings — no canonical primitives are retained — while every sorted
/// column still binds an encoded search probe, and the payload path still
/// serves codes (through the base's `vortex.shared` wrappers). A column that
/// arrives in another encoding, and a nullable struct, are left as they are.
#[tokio::test]
async fn test_built_store_compresses_resident_form() {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    use vortex_array::arrays::{PrimitiveArray, Struct, StructArray, VarBinViewArray};
    use vortex_array::validity::Validity;

    let quads = modular_quads(200, 4, 8);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

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
    let p1 = iri("p1");
    let matched = matched_view(&store, None, Some(&p1), None, None).await;
    let cols = matched
        .code_columns()
        .expect("compressed base still serves codes through its shared wrappers");
    let mut got = decoded_rows(&cols, &matched.dict_reader().unwrap()).await;
    got.sort();
    assert_eq!(got, expected_rows(&quads, |i| i % 4 == 1));

    // What construction leaves alone comes back as it went in: a code column
    // already in an encoding no probe reads (pcodec) is
    // neither decoded nor re-encoded.
    let unprobed = unprobed_object_store().await;
    assert_eq!(unprobed.debug_base_child_int_canonical("o"), Some(false));
    let base = unprobed.to_serializable_parts().await.unwrap().array;
    let base = base.try_downcast::<Struct>().unwrap();
    assert_eq!(
        base.unmasked_field_by_name("o")
            .unwrap()
            .encoding_id()
            .as_str(),
        "vortex.pco"
    );
    // The resident helpers' declines: a nullable struct passes through as
    // it is, and the code accessors hand out no primitive for a column that
    // is not a non-nullable code column.
    let nullable = StructArray::try_new(
        ["s"].into(),
        vec![PrimitiveArray::from_iter([1u64, 2]).into_array()],
        2,
        Validity::AllValid,
    )
    .unwrap()
    .into_array();
    let passed =
        crate::store::resident::with_compressed_int_children(nullable.clone(), true).unwrap();
    assert_eq!(passed.addr(), nullable.addr());
    for column in [
        VarBinViewArray::from_iter_str(["a", "b"]).into_array(),
        PrimitiveArray::from_option_iter([Some(1u64), None]).into_array(),
    ] {
        assert!(crate::store::resident::shared_code_primitive(&column).is_none());
        assert!(crate::store::resident::cached_code_primitive(&column).is_none());
    }
}

/// The bindings' code-column read on a served in-memory match: `code_columns`
/// rides the serve plan, reading the codes off the answering index's own
/// columns — so the resolution's row ids stay unmaterialized — and the codes
/// it hands out address the cached dictionary and name exactly the matched
/// quads.
#[tokio::test]
async fn test_code_columns_serves_from_the_answering_index() {
    let quads = graph_modular_quads(30, 2, 3, 5, &[GraphName::DefaultGraph]);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let p1 = iri("p1");
    let matched = matched_view(&store, None, Some(&p1), None, None).await;
    assert!(selection_pending(&matched));

    let cols = matched
        .code_columns()
        .expect("an in-memory Dictionary view answers codes");
    assert_eq!(
        matched.debug_row_ids_materialized(),
        Some(false),
        "a served code read must not materialize the resolution's row ids"
    );
    let mut got = decoded_rows(&cols, &matched.dict_reader().unwrap()).await;
    got.sort();
    assert_eq!(got, expected_rows(&quads, |i| i % 3 == 1));
}

/// A window of an in-memory reference index's located run decodes exactly its
/// own rows of the run (rows of one value are in row id order, so a window of
/// the run is a window of its ids): none for an empty window — no limit, or an
/// offset at or past the run's end — and never more than the run. The window
/// holds the same rows as the slice of the whole run, and the pending view it
/// is taken beside keeps its ids undecoded. A copy index's run is in key
/// order, so a window that can cut it decodes it whole (and shares it with
/// the pending view), while one that cannot cut a row is the served match as
/// it is and decodes nothing.
#[tokio::test]
async fn test_in_memory_window_decodes_only_its_rows_of_a_located_run() {
    use crate::store::{IdsNeed, Probe};

    let quads = graph_modular_quads(900, 4, 3, 7, &[GraphName::DefaultGraph]);
    let p1 = iri("p1");
    let width = 300;
    let span =
        |offset: usize, limit: usize| (offset.min(width), offset.saturating_add(limit).min(width));
    let windows = [
        (0, 0),
        (0, 1),
        (7, 5),
        (0, 299),
        (290, 50),
        (299, 10),
        (300, 5),
        (400, 5),
        (0, 1_000),
        (5, usize::MAX),
    ];

    for (index, decoded_whole) in [
        (IndexType::SecondaryByReference, false),
        (IndexType::SecondaryByCopy, true),
    ] {
        let store = streamed_store(quads.clone(), LayoutStrategy::Dictionary, vec![index]).await;
        let all = matched_view(&store, None, Some(&p1), None, None)
            .await
            .code_columns_gathered()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all[0].len(), width, "{index:?}");
        let probe = Probe::new(None, Some(p1.clone()), None, None);
        let pending = store
            .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
            .await
            .unwrap();
        assert_eq!(
            pending.debug_row_ids_materialized(),
            Some(false),
            "{index:?}"
        );

        for (offset, limit) in windows {
            let (from, to) = span(offset, limit);
            let served = decoded_whole && offset == 0 && limit >= width;
            let expected = if served {
                0
            } else if decoded_whole {
                width
            } else {
                to - from
            };
            let before = crate::store::test_hooks::decoded_rids();
            let window = store
                .run_probe(&probe.clone().window(offset, Some(limit)))
                .await
                .unwrap();
            let decoded = crate::store::test_hooks::decoded_rids() - before;
            assert_eq!(decoded, expected, "{index:?} ({offset}, {limit}) decodes");
            assert_eq!(
                served_component(&window).is_some(),
                served,
                "{index:?} ({offset}, {limit}) keeps the plan"
            );

            let beside = pending.window(offset, limit).await.unwrap();
            if !decoded_whole {
                assert_eq!(
                    pending.debug_row_ids_materialized(),
                    Some(false),
                    "{index:?} ({offset}, {limit}) leaves the run's ids undecoded"
                );
            }
            for view in [&window, &beside] {
                assert_eq!(
                    view.size().await.unwrap(),
                    to - from,
                    "{index:?} ({offset}, {limit})"
                );
                let cols = view.code_columns_gathered().await.unwrap().unwrap();
                for (col, whole) in cols.iter().zip(&all) {
                    assert_eq!(
                        col.as_slice(),
                        &whole.as_slice()[from..to],
                        "{index:?} ({offset}, {limit})"
                    );
                }
            }
        }
    }
}
