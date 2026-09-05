//! Mutation: appends into the tail, tombstoning deletes, set semantics,
//! auto-compaction thresholds, and the owner-only rule for views.

use super::*;
use crate::store::QuadColumn;

// ─── Mutation ──────────────────────────────────────────────────────────

/// An append then a delete of the tail row and of the base row take the
/// store down to empty, one row at a time.
#[tokio::test]
async fn test_add_then_delete_to_empty() {
    let q1 = make_quad(
        "http://example.org/s1",
        "http://example.org/p1",
        "o1",
        GraphName::DefaultGraph,
    );

    let arr = build_array::<SortedInMemoryBuilder>(
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

/// One quad in its own subject run, under a predicate the base never
/// carries, so a predicate match counts tail rows alone.
fn tail_quad(i: usize) -> Quad {
    make_quad(
        &format!("http://example.org/t{i:05}"),
        "http://example.org/tail",
        "appended",
        GraphName::DefaultGraph,
    )
}

/// Every chunk of the accreted tail, or `None` when the tail is one flat
/// array.
fn tail_chunks(store: &VortexRdfStore) -> Option<Vec<vortex_array::ArrayRef>> {
    use vortex_array::arrays::Chunked;
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    store
        .debug_tail_rows()
        .expect("a tail")
        .clone()
        .try_downcast::<Chunked>()
        .ok()
        .map(|chunked| chunked.chunks())
}

/// Appends accrete onto the tail as chunks; the append that takes the chunk
/// count past `TAIL_MAX_CHUNKS` flattens the tail into one array, which then
/// accretes again — and a view taken before the flatten keeps its rows.
#[tokio::test]
async fn test_tail_accretion_flattens_at_chunk_bound() {
    use crate::store::test_hooks::TAIL_MAX_CHUNKS;

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(4, 2, 2)),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let mut store = VortexRdfStore::from_built(arr).unwrap();
    assert!(store.debug_tail_rows().is_none());

    for i in 0..TAIL_MAX_CHUNKS {
        store = store.add_quad(tail_quad(i)).await.unwrap();
    }
    let chunks = tail_chunks(&store).expect("the tail accretes as chunks");
    assert_eq!(chunks.len(), TAIL_MAX_CHUNKS);
    assert!(chunks.iter().all(|c| c.len() == 1));
    assert_eq!(store.tail_len(), TAIL_MAX_CHUNKS);

    let tail_p = NamedNode::new("http://example.org/tail").unwrap();
    let view = store
        .match_pattern(None, Some(&tail_p), None, None)
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), TAIL_MAX_CHUNKS);

    // One chunk past the bound: the tail is flattened.
    let flattened = store.add_quad(tail_quad(TAIL_MAX_CHUNKS)).await.unwrap();
    assert!(
        tail_chunks(&flattened).is_none(),
        "the append past TAIL_MAX_CHUNKS must flatten the tail"
    );
    assert_eq!(flattened.tail_len(), TAIL_MAX_CHUNKS + 1);

    // The flat tail is the prefix the next append accretes onto.
    let again = flattened
        .add_quad(tail_quad(TAIL_MAX_CHUNKS + 1))
        .await
        .unwrap();
    let chunks = tail_chunks(&again).expect("accretion resumes on the flat prefix");
    assert_eq!(
        chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
        [TAIL_MAX_CHUNKS + 1, 1]
    );

    // The pre-flatten view still reads its own tail.
    let dropped = again.delete_quad(&tail_quad(3)).await.unwrap();
    assert_eq!(dropped.size().await.unwrap(), 4 + TAIL_MAX_CHUNKS + 1);
    assert_eq!(view.size().await.unwrap(), TAIL_MAX_CHUNKS);
    assert_eq!(
        again
            .match_pattern(None, Some(&tail_p), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        TAIL_MAX_CHUNKS + 2
    );
}

/// The accreted suffix is folded into the flat prefix once it reaches
/// `max(prefix rows, TAIL_FLATTEN_FLOOR)`: below the floor a few chunks stay
/// chunks, and once the prefix outgrows the floor the prefix's own size is
/// the bound.
#[tokio::test]
async fn test_tail_accretion_flattens_at_row_floor() {
    use crate::store::test_hooks::TAIL_FLATTEN_FLOOR;

    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(4, 2, 2)),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // The first append is the flat prefix; a suffix one row short of the
    // floor stays a chunk beside it.
    let one = store.add_quad(tail_quad(0)).await.unwrap();
    assert!(tail_chunks(&one).is_none());
    let below = one
        .add_quads((1..TAIL_FLATTEN_FLOOR).map(tail_quad))
        .await
        .unwrap();
    assert_eq!(
        tail_chunks(&below)
            .expect("below the floor the suffix stays a chunk")
            .iter()
            .map(|c| c.len())
            .collect::<Vec<_>>(),
        [1, TAIL_FLATTEN_FLOOR - 1]
    );
    let tail_p = NamedNode::new("http://example.org/tail").unwrap();
    let view = below
        .match_pattern(None, Some(&tail_p), None, None)
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), TAIL_FLATTEN_FLOOR);

    // The row that brings the suffix to the floor flattens the tail.
    let at_floor = below.add_quad(tail_quad(TAIL_FLATTEN_FLOOR)).await.unwrap();
    assert!(
        tail_chunks(&at_floor).is_none(),
        "a suffix reaching TAIL_FLATTEN_FLOOR must flatten"
    );
    assert_eq!(at_floor.tail_len(), TAIL_FLATTEN_FLOOR + 1);

    // With a prefix above the floor, a suffix of exactly the floor is still
    // short of the prefix and stays a chunk.
    let above = at_floor
        .add_quads((TAIL_FLATTEN_FLOOR + 1..2 * TAIL_FLATTEN_FLOOR + 1).map(tail_quad))
        .await
        .unwrap();
    assert_eq!(
        tail_chunks(&above)
            .expect("a suffix below the prefix's size stays a chunk")
            .iter()
            .map(|c| c.len())
            .collect::<Vec<_>>(),
        [TAIL_FLATTEN_FLOOR + 1, TAIL_FLATTEN_FLOOR]
    );

    // A delete on the grown owner leaves the pre-flatten view alone.
    let dropped = above.delete_quad(&tail_quad(5)).await.unwrap();
    assert_eq!(dropped.size().await.unwrap(), 4 + 2 * TAIL_FLATTEN_FLOOR);
    assert_eq!(view.size().await.unwrap(), TAIL_FLATTEN_FLOOR);
}

/// A tailed Dictionary view's `selected_rows` re-encodes base and tail
/// together against a fresh dictionary: the result holds every row as codes
/// in the base's column shape, those codes address the dictionary
/// `to_serializable_parts` hands out, and the cached one is withheld.
#[tokio::test]
async fn test_selected_rows_on_tailed_dictionary_store() {
    let quads = modular_quads(12, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let base_rows = store.selected_rows().await.unwrap();

    let novel = make_quad(
        "http://example.org/brand-new-subject",
        "http://example.org/brand-new-predicate",
        "brand new object",
        GraphName::DefaultGraph,
    );
    let tailed = store.add_quad(novel.clone()).await.unwrap();
    assert!(
        tailed.code_read_snapshot().is_none(),
        "a tailed store's codes address no cached dictionary"
    );

    let rows = tailed.selected_rows().await.unwrap();
    assert_eq!(rows.len(), 13);
    assert_eq!(rows.len(), tailed.size().await.unwrap());
    assert_eq!(rows.dtype(), base_rows.dtype());

    let parts = tailed.to_serializable_parts().await.unwrap();
    let dict = parts
        .dict
        .expect("a Dictionary rebuild carries its dictionary");
    let decoded: Vec<Quad> = crate::store::layouts::dictionary::decode_chunk(&rows, &dict)
        .into_iter()
        .map(|q| q.unwrap())
        .collect();
    let mut expected = quads.clone();
    expected.push(novel);
    assert_eq!(quad_strings(&decoded), quad_strings(&expected));
}

/// Under the string layouts a tailed view's `selected_rows` is the base's
/// rows and the tail's, chunked together in the base's own vocabulary.
#[tokio::test]
async fn test_selected_rows_on_tailed_default_store() {
    use vortex_array::arrays::Chunked;
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;

    let quads = modular_quads(12, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let appended = [tail_quad(0), tail_quad(1)];
    let tailed = store.add_quads(appended.clone()).await.unwrap();

    let rows = tailed.selected_rows().await.unwrap();
    assert_eq!(rows.len(), 14);
    let chunked = rows
        .clone()
        .try_downcast::<Chunked>()
        .expect("base and tail chunk together");
    assert_eq!(
        chunked.chunks().iter().map(|c| c.len()).collect::<Vec<_>>(),
        [12, 2]
    );
    let decoded: Vec<Quad> = crate::store::layouts::ResolvedLayout::Default
        .decode_chunk(&rows)
        .into_iter()
        .map(|q| q.unwrap())
        .collect();
    let mut expected = quads.clone();
    expected.extend(appended);
    assert_eq!(quad_strings(&decoded), quad_strings(&expected));
}

/// The in-memory `code_columns` fast path skips tombstoned rows in each of
/// its selection shapes — the whole base, a subject range, a row-id list,
/// an index-served run — so the codes it serves decode to exactly the
/// surviving rows.
#[tokio::test]
async fn test_code_columns_skips_tombstoned_rows_in_memory() {
    // Three quads per subject, so a subject match is a multi-row range.
    let quads: Vec<Quad> = (0..12)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i / 3),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {}", i % 4),
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
    // Row 4: s01 p1 "object 0".
    let deleted = store.delete_quad(&quads[4]).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 11);
    let dict = deleted.code_read_snapshot().unwrap();

    let s1 = subject_node(1, 2);
    let by_subject = deleted
        .match_pattern(Some(&s1), None, None, None)
        .await
        .unwrap();
    assert!(
        by_subject.debug_selection_range().is_some(),
        "a bound subject narrows to a row range"
    );
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_predicate = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(
        !by_predicate.debug_has_serve_plan(),
        "a reference index resolves to row ids, never a serve plan"
    );

    // The same rows under a by-copy index: the predicate match is served off
    // the index's own columns, and the tombstone must be honoured there too.
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let served_deleted = VortexRdfStore::from_built(arr)
        .unwrap()
        .delete_quad(&quads[4])
        .await
        .unwrap();
    let served_dict = served_deleted.code_read_snapshot().unwrap();
    let served = served_deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(
        served.debug_has_serve_plan(),
        "a by-copy index serves the predicate match"
    );

    for (tag, view, dict, live) in [
        ("owner", &deleted, &dict, 11),
        ("subject range", &by_subject, &dict, 2),
        ("row ids", &by_predicate, &dict, 3),
        ("served, tombstoned", &served, &served_dict, 3),
    ] {
        let [s, p, o, g] = <[_; 4]>::try_from(
            view.code_columns(&QuadColumn::ALL)
                .unwrap()
                .unwrap_or_else(|| panic!("{tag}: the in-memory fast path must serve codes")),
        )
        .ok()
        .unwrap();
        assert_eq!(s.len(), live, "{tag}");
        let decoded: Vec<(String, String, String, String)> = (0..s.len())
            .map(|i| {
                (
                    dict.decode(s[i]).unwrap(),
                    dict.decode(p[i]).unwrap(),
                    dict.decode(o[i]).unwrap(),
                    dict.decode(g[i]).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            decoded,
            tuple_rows(&view.quads_vec().await.unwrap()),
            "{tag}: codes must decode to the surviving rows"
        );
    }
}

/// `delete_matching` on a partial pattern tombstones its base hits and its
/// tail hits in one step.
#[tokio::test]
async fn test_delete_matching_spans_base_and_tail() {
    let quads = modular_quads(12, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let tailed = store
        .add_quads([make_quad(
            "http://example.org/s99",
            "http://example.org/p0",
            "object 9",
            GraphName::DefaultGraph,
        )])
        .await
        .unwrap();
    assert_eq!(tailed.size().await.unwrap(), 13);

    // p0 is on base rows 0, 3, 6, 9 and on the appended s99.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let after = tailed
        .delete_matching(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(after.size().await.unwrap(), 13 - 5);
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
    let remaining = after.quads_vec().await.unwrap();
    assert_eq!(remaining.len(), 8);
    assert!(remaining.iter().all(|q| q.predicate != p0));
    assert_eq!(
        quad_strings(&remaining),
        expected_strings(&quads, |i| i % 3 != 0)
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

/// Compacting a file-backed store rewrites the compacted rows back over its
/// own source file and stays file-backed: an independent reopen of the path
/// sees the folded-in, tombstone-free data, the rebuilt index survives, a
/// later append is folded into the file too, and no sibling artifact (temp
/// file, spill run) is left beside the store.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_file_backed_compaction_rewrites_source_file() {
    for index in [IndexType::SecondaryByReference, IndexType::SecondaryByCopy] {
        run_file_backed_compaction(index).await;
    }
}

#[cfg(feature = "file-io")]
async fn run_file_backed_compaction(index: IndexType) {
    let quads = modular_quads(12, 2, 3);
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Default, vec![index]).await;
    let by_copy = index == IndexType::SecondaryByCopy;
    let only_the_store_file = || {
        let entries: Vec<std::path::PathBuf> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(
            entries,
            std::slice::from_ref(&path),
            "compaction must leave no sibling artifacts"
        );
    };

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
    only_the_store_file();
    assert_eq!(compacted.size().await.unwrap(), 8);
    assert_eq!(
        compacted.indexes(),
        &vec![index],
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
    assert_eq!(reopened.indexes(), &vec![index]);

    // A file-backed store keeps its tail until an explicit compact; that
    // compaction folds the appended row into the file too.
    let extra = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let appended = reopened.add_quad(extra.clone()).await.unwrap();
    assert_eq!(appended.tail_len(), 1);
    let recompacted = appended.compact().await.unwrap();
    only_the_store_file();
    assert_eq!(recompacted.tail_len(), 0);
    assert_eq!(recompacted.size().await.unwrap(), 9);
    // The append now lives in the file on disk, and the rebuilt index routes
    // a predicate match over the compacted rows plus the appended one.
    let reopened2 = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened2.size().await.unwrap(), 9);
    assert_eq!(reopened2.indexes(), &vec![index]);
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_p = reopened2
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(
        by_p.debug_has_serve_plan(),
        by_copy,
        "a copy index serves the predicate match from its sorted copy"
    );
    let mut expected: Vec<Quad> = quads
        .iter()
        .filter(|q| q.predicate == p0 && q.object != object0)
        .cloned()
        .collect();
    expected.push(extra);
    let mut expected = tuple_rows(&expected);
    expected.sort();
    let mut got = tuple_rows(&by_p.quads_vec().await.unwrap());
    got.sort();
    assert_eq!(got, expected, "{index:?}");
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

    // A delete finds the tail row by its strings and tombstones it there.
    let dropped = added.delete_quad(&novel).await.unwrap();
    assert_eq!(dropped.size().await.unwrap(), 12);
    assert!(!dropped.contains(&novel).await.unwrap());
    assert_eq!(
        dropped
            .match_pattern(None, None, Some(&new_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        dropped
            .match_pattern(None, None, Some(&old_object), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4,
        "a tail tombstone leaves the base alone"
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

/// File-backed stores append the same way: the file is untouched by the
/// append (the tail lives in memory beside it) and queries union the
/// pushed-down scan with the tail scan.
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

    // A tail row is tombstoned in the tail, with the file untouched.
    let s90 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s90").unwrap());
    let tail_deleted = added
        .delete_matching(Some(&s90), None, None, None)
        .await
        .unwrap();
    assert_eq!(tail_deleted.size().await.unwrap(), 13);
    assert_eq!(
        tail_deleted
            .match_pattern(Some(&s90), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        subjects_of(
            &tail_deleted
                .match_pattern(None, None, Some(&object), None)
                .await
                .unwrap()
        )
        .await
        .len(),
        4
    );

    // Deletes hit base (tombstone mask over the file) and tail alike.
    let deleted = added.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 13);

    // Compaction folds the tail into the source file (rewritten in place)
    // and stays file-backed.
    let compacted = deleted
        .compact_with_indexes(vec![IndexType::SecondaryByReference])
        .await
        .unwrap();
    assert_eq!(compacted.size().await.unwrap(), 13);
    assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(
        VortexRdfStore::from_file(&path)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        13,
        "compaction rewrites the source file"
    );
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
