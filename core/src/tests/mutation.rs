use super::*;

// ─── 5) Mutation behavior ───────────────────────────────────────────────

async fn run_add_delete_test<B: VortexArrayBuilder>() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );

    let arr = build_array::<B>(
        quad_stream(vec![q1.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.size().await.unwrap(), 1);

    let q2 = make_quad(
        "http://example.org/s2",
        "http://example.org/p2",
        "o2",
        GraphName::DefaultGraph,
    );
    let store = store.add_quad(q2.clone()).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 2);

    let store = store.delete_quad(&q2).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 1);

    let store = store.delete_quad(&q1).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_add_delete_sorted_in_memory() {
    run_add_delete_test::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_add_delete_sorted_stream() {
    run_add_delete_test::<SortedStreamBuilder>().await;
}

#[tokio::test]
async fn test_multiple_append() {
    let mut store = VortexRdfStore::empty();

    for i in 0..10 {
        let q = make_quad(
            &format!("http://example.org/s{}", i),
            "http://example.org/p",
            "o",
            GraphName::DefaultGraph,
        );
        store = store.add_quad(q).await.unwrap();
    }

    assert_eq!(store.size().await.unwrap(), 10);

    let p = NamedNode::new("http://example.org/p").unwrap();
    let matched = store
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 10);

    let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
    let matched_s5 = store
        .match_pattern(Some(&s5), None, None, None)
        .await
        .unwrap();
    assert_eq!(matched_s5.size().await.unwrap(), 1);
}

/// Appends land in the tail, never the base — so the base's secondary
/// indexes survive an add, and queries union the base's fast paths with a
/// tail scan.
#[tokio::test]
async fn test_add_quads_keeps_indexes() {
    let quads = modular_quads(12, 2, 3);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let added = store
        .add_quads([
            make_quad(
                "http://example.org/s90",
                "http://example.org/p0",
                "object 0",
                GraphName::DefaultGraph,
            ),
            make_quad(
                "http://example.org/s91",
                "http://example.org/p9",
                "object 9",
                GraphName::DefaultGraph,
            ),
        ])
        .await
        .unwrap();
    assert_eq!(added.size().await.unwrap(), 14);
    assert_eq!(
        added.indexes(),
        &[IndexType::SecondaryByReference],
        "appending must not invalidate the base's indexes"
    );
    assert_eq!(store.size().await.unwrap(), 12, "source untouched");

    // An index-routed base lookup unions with the tail scan: "object 0" is
    // on base rows 0, 3, 6, 9 and on the appended s90.
    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let matched = added
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    let subjects = subjects_of(&matched).await;
    assert_eq!(subjects.len(), 5);
    assert!(subjects.contains(&"<http://example.org/s90>".to_string()));

    // Terms the base has never seen — the index proves the base empty —
    // still match in the tail.
    let p9 = NamedNode::new("http://example.org/p9").unwrap();
    assert_eq!(
        added
            .match_pattern(None, Some(&p9), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
    let s90 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s90").unwrap());
    assert_eq!(
        added
            .match_pattern(Some(&s90), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );

    // Chained matches narrow base and tail together: of the five
    // "object 0" rows, p0 holds on base rows 0 and 6, and on s90.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let chained = matched
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(chained.size().await.unwrap(), 3);

    // Deletes tombstone in the tail exactly as in the base.
    let deleted_tail = added
        .delete_matching(Some(&s90), None, None, None)
        .await
        .unwrap();
    assert_eq!(deleted_tail.size().await.unwrap(), 13);
    assert_eq!(
        deleted_tail
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );
    let deleted_base = deleted_tail.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(deleted_base.size().await.unwrap(), 12);
    assert_eq!(
        deleted_base
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        3
    );
}

/// `add_quads` follows RDF/JS dataset (set) semantics: a quad equal to an
/// existing one, or repeated within the batch, is skipped — and a deleted
/// quad counts as absent, so it can be re-added.
#[tokio::test]
async fn test_add_quads_set_semantics() {
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
    let q3 = make_quad(
        "http://example.org/s3",
        "http://example.org/p3",
        "o3",
        GraphName::DefaultGraph,
    );

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(vec![q1.clone(), q2.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Adding an existing quad is a no-op.
    let same = store.add_quad(q1.clone()).await.unwrap();
    assert_eq!(same.size().await.unwrap(), 2);

    // In-batch duplicates and existing quads are both skipped.
    let added = store
        .add_quads([q3.clone(), q3.clone(), q1.clone()])
        .await
        .unwrap();
    assert_eq!(added.size().await.unwrap(), 3);
    assert!(added.contains(&q3).await.unwrap());

    // A tombstoned quad is absent, so re-adding it takes effect.
    let deleted = added.delete_quad(&q3).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 2);
    assert!(!deleted.contains(&q3).await.unwrap());
    let readded = deleted.add_quad(q3.clone()).await.unwrap();
    assert_eq!(readded.size().await.unwrap(), 3);
    assert!(readded.contains(&q3).await.unwrap());
}

/// `delete_matching` drops every quad a pattern selects, using the same
/// matcher `match_pattern` uses to find them.
#[tokio::test]
async fn test_delete_matching_pattern() {
    let quads = modular_quads(12, 2, 3);

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Delete every quad with predicate p0 (i even): 6 of the 12.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let after = store
        .delete_matching(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 6);
    assert_eq!(
        after
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    assert_eq!(after.quads().unwrap().count().await, 6);

    // Deleting the same pattern twice is idempotent, not a double-count.
    let again = after
        .delete_matching(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(again.size().await.unwrap(), 6);

    // A pattern matching nothing leaves the store alone.
    let missing = NamedNode::new("http://example.org/nope").unwrap();
    let untouched = again
        .delete_matching(None, Some(&missing), None, None)
        .await
        .unwrap();
    assert_eq!(untouched.size().await.unwrap(), 6);
}

/// When an append pushes the tail past the auto-compaction thresholds,
/// `add_quads` finishes by folding the tail into the base: the returned
/// store is compacted — SPOG-sorted, tail-less, indexes rebuilt — while
/// smaller appends leave the tail in place.
#[tokio::test]
async fn test_add_quads_auto_compacts_past_threshold() {
    let batch = |range: std::ops::Range<usize>| -> Vec<Quad> {
        range
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:05}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 5),
                    GraphName::DefaultGraph,
                )
            })
            .collect()
    };

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(batch(0..10)),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Below the floor the tail simply accumulates.
    let small = store.add_quads(batch(10..110)).await.unwrap();
    assert_eq!(small.tail_len(), 100);
    assert_eq!(small.size().await.unwrap(), 110);

    // This append lands the tail at 4_100 rows — past the 4_096 floor —
    // so it comes back compacted.
    let compacted = small.add_quads(batch(110..4_110)).await.unwrap();
    assert_eq!(compacted.tail_len(), 0, "the threshold add must compact");
    assert_eq!(compacted.size().await.unwrap(), 4_110);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);

    // The compacted base is SPOG-sorted and fully routable: subject
    // binary search and the rebuilt object index both answer.
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s02050").unwrap());
    assert_eq!(
        compacted
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
    let object = Term::Literal(Literal::new_simple_literal("object 3"));
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        822
    );
}

/// File-backed stores auto-compact like in-memory ones: an append that
/// pushes the tail past the thresholds folds it in — rewriting the source
/// file in place and staying file-backed — while smaller appends leave the
/// tail (and the file on disk) untouched.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_add_auto_compacts_past_threshold() {
    let quads: Vec<Quad> = (0..4)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i),
                "http://example.org/p0",
                "object 0",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // A small append stays below the floor: the tail accumulates and the
    // file on disk is not rewritten.
    let small_batch: Vec<Quad> = (4..14)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i),
                "http://example.org/p0",
                "object 1",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let small = store.add_quads(small_batch).await.unwrap();
    assert_eq!(small.tail_len(), 10);
    assert_eq!(
        VortexRdfStore::from_file(&path)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4,
        "a sub-threshold append must not rewrite the file"
    );

    // This append lands the tail past the 4_096 floor, so it comes back
    // compacted: tail folded, indexes rebuilt.
    let batch: Vec<Quad> = (4..4_204)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let compacted = store.add_quads(batch).await.unwrap();
    assert_eq!(compacted.tail_len(), 0, "the threshold add must compact");
    assert_eq!(compacted.size().await.unwrap(), 4_204);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);

    // It stayed file-backed: an independent reopen of the path sees the
    // folded-in rows — the append rewrote the source file — and the rebuilt
    // subject fast path and object index both answer.
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.size().await.unwrap(), 4_204);
    assert_eq!(reopened.indexes(), &[IndexType::SecondaryByReference]);
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s02050").unwrap());
    assert_eq!(
        reopened
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
}

/// Under the Dictionary layout the tail stores term strings (an appended
/// term has no code in the sorted dictionary), and patterns probe the base
/// by code and the tail by string — so a term the dictionary has never
/// seen still matches appended quads.
#[tokio::test]
async fn test_dictionary_add_probes_tail_by_string() {
    let quads: Vec<Quad> = (0..12)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 2),
                &format!("object {}", i % 3),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Every term of the appended quad is absent from the dictionary.
    let novel = make_quad(
        "http://example.org/brand-new-subject",
        "http://example.org/brand-new-predicate",
        "brand new object",
        GraphName::DefaultGraph,
    );
    let added = store.add_quad(novel.clone()).await.unwrap();
    assert_eq!(added.size().await.unwrap(), 13);
    assert!(added.contains(&novel).await.unwrap());

    // The base's dictionary proves these terms unmatchable *in the base*;
    // the tail must still answer.
    let new_object = Term::Literal(Literal::new_simple_literal("brand new object"));
    assert_eq!(
        added
            .match_pattern(None, None, Some(&new_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
    // Dictionary-coded base terms keep routing as before.
    let old_object = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        added
            .match_pattern(None, None, Some(&old_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );

    // Serializing re-encodes base and tail against a fresh dictionary, so
    // the written parts stand alone.
    let parts = added.to_serializable_parts().await.unwrap();
    let reloaded = VortexRdfStore::from_parts(parts).unwrap();
    assert_eq!(reloaded.size().await.unwrap(), 13);
    assert_eq!(
        reloaded
            .match_pattern(None, None, Some(&new_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );

    // Compaction folds the tail in: fresh dictionary, rebuilt index, and
    // both old and new terms answer through the base again.
    let compacted = added
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(compacted.size().await.unwrap(), 13);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&new_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&old_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );
}

/// Compaction (`compact_with_indexes`) re-sorts by (s, p, o, g): the
/// tail and any tombstones are folded away, the quads come back in SPOG
/// order, and the subject binary-search fast path is restored alongside
/// the rebuilt indexes.
#[tokio::test]
async fn test_compaction_folds_tail_and_sorts() {
    // The base leaves gaps at s0 and s3 so the appended rows have to be
    // interleaved into it, not merely appended after it.
    let quads: Vec<Quad> = [1, 2, 4, 5, 6, 7]
        .iter()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p0",
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

    let added = store
        .add_quads([
            make_quad(
                "http://example.org/s0",
                "http://example.org/p0",
                "object 0",
                GraphName::DefaultGraph,
            ),
            make_quad(
                "http://example.org/s3",
                "http://example.org/p0",
                "object 1",
                GraphName::DefaultGraph,
            ),
        ])
        .await
        .unwrap();
    let deleted = added.delete_quad(&quads[3]).await.unwrap(); // drops s5
    assert_eq!(deleted.size().await.unwrap(), 7);

    let compacted = deleted
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(compacted.size().await.unwrap(), 7);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);

    // The rows come back in global SPOG order, with the appended s0 and s3
    // interleaved into the base's run and the tombstoned s5 gone.
    assert_eq!(
        subjects_of(&compacted).await,
        vec![
            "<http://example.org/s0>".to_string(),
            "<http://example.org/s1>".to_string(),
            "<http://example.org/s2>".to_string(),
            "<http://example.org/s3>".to_string(),
            "<http://example.org/s4>".to_string(),
            "<http://example.org/s6>".to_string(),
            "<http://example.org/s7>".to_string(),
        ]
    );

    // Subject lookups and the rebuilt object index both answer.
    let s0 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0").unwrap());
    assert_eq!(
        compacted
            .match_pattern(Some(&s0), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );

    // The compacted store owns its rows: it mutates freely.
    let again = compacted
        .add_quad(make_quad(
            "http://example.org/s5",
            "http://example.org/p0",
            "object 0",
            GraphName::DefaultGraph,
        ))
        .await
        .unwrap();
    assert_eq!(again.size().await.unwrap(), 8);
}

/// File-backed stores append the same way: the file stays immutable, the
/// tail lives in memory beside it, and queries union the pushed-down scan
/// with the tail scan.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_add_quads() {
    let quads = modular_quads(12, 2, 3);

    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let added = store
        .add_quads([
            make_quad(
                "http://example.org/s90",
                "http://example.org/p0",
                "object 0",
                GraphName::DefaultGraph,
            ),
            make_quad(
                "http://example.org/s91",
                "http://example.org/p9",
                "object 9",
                GraphName::DefaultGraph,
            ),
        ])
        .await
        .unwrap();
    assert_eq!(added.size().await.unwrap(), 14);
    assert_eq!(added.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(
        store.size().await.unwrap(),
        12,
        "the file view is untouched"
    );

    // Index-routed file lookup + tail scan union.
    let object = Term::Literal(Literal::new_simple_literal("object 0"));
    let subjects = subjects_of(
        &added
            .match_pattern(None, None, Some(&object), None)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(subjects.len(), 5);
    assert!(subjects.contains(&"<http://example.org/s90>".to_string()));
    // A term only the tail knows.
    let object9 = Term::Literal(Literal::new_simple_literal("object 9"));
    assert_eq!(
        added
            .match_pattern(None, None, Some(&object9), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );

    // Deletes hit base (tombstone mask over the file) and tail alike.
    let deleted = added.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 13);

    // Compaction folds file + tail into a sorted, indexed in-memory store.
    let compacted = deleted
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(compacted.size().await.unwrap(), 13);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(
        compacted
            .match_pattern(None, None, Some(&object9), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );
}

/// `owned()` on a file-backed match view must produce an independent
/// in-memory copy and leave the shared source file untouched — a view's
/// compaction rewriting the file would destroy every row outside the view
/// for all other readers of that path.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_owned_file_view_leaves_source_file_intact() {
    let quads = modular_quads(12, 3, 4);
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Default, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let independent = view.owned().await.unwrap();
    assert_eq!(independent.size().await.unwrap(), 4, "the view's own rows");

    // The shared file still holds every row.
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(
        reopened.size().await.unwrap(),
        12,
        "owned() must never rewrite the shared source file"
    );
    // And the independent copy is mutable without touching the file.
    let smaller = independent
        .delete_matching(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(smaller.size().await.unwrap(), 0);
    assert_eq!(
        VortexRdfStore::from_file(&path)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        12
    );
}

/// Mutations belong to the store that owns its rows. A narrowed view is a
/// window onto a shared base, so it rejects them and points at the way out.
#[tokio::test]
async fn test_derived_view_rejects_mutations() {
    let quads: Vec<Quad> = (0..6)
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
    let view = store
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), 3);

    for result in [
        view.add_quad(quads[0].clone()).await.err(),
        view.delete_quad(&quads[0]).await.err(),
        view.delete_matching(None, None, Some(&object), None)
            .await
            .err(),
    ] {
        let message = result
            .expect("a derived view must reject mutations")
            .to_string();
        assert!(
            message.contains("owned()"),
            "the error should point at the way out, got: {message}"
        );
    }

    // `owned()` yields an independent copy that mutates freely, and leaves
    // the store it came from alone.
    let owned = view.owned().await.unwrap();
    let edited = owned.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(edited.size().await.unwrap(), 2);
    assert_eq!(store.size().await.unwrap(), 6);

    // An unconstrained view covers exactly the base, so it counts as an
    // owner: mutating it is the same as mutating the store it came from.
    let whole = store.match_pattern(None, None, None, None).await.unwrap();
    assert_eq!(
        whole
            .delete_quad(&quads[0])
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        5
    );
}
