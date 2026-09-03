//! The Dictionary layout in memory: code round-trips, the decode memo,
//! index composition over codes, and the FSST-held term column.

use super::*;

// ─── Dictionary layout ─────────────────────────────────────────────────

async fn run_dictionary_roundtrip<B: VortexArrayBuilder>(builder: &str) {
    let quads = dictionary_test_quads();
    let arr = build_array::<B>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap_or_else(|e| panic!("{builder}: dictionary build failed: {e}"));

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{builder}");

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads), "{builder}");
}

#[tokio::test]
async fn test_dictionary_roundtrip_both_builders() {
    run_dictionary_roundtrip::<SortedInMemoryBuilder>("SortedInMemoryBuilder").await;
    run_dictionary_roundtrip::<SortedStreamBuilder>("SortedStreamBuilder").await;
}

/// The push-based sink builds what the sorted in-memory builder builds from
/// the same quads: the same decoded quads, index roster and component
/// names, and a store that serves its index-routed matches the same way.
#[tokio::test]
async fn test_dictionary_quad_sink_matches_sorted_in_memory_builder() {
    use crate::store::RawQuad;
    use crate::store::layouts::dictionary::DictionaryQuadSink;

    let quads = dictionary_test_quads();
    let mut sink = DictionaryQuadSink::new(vec![IndexType::SecondaryByCopy]);
    for quad in &quads {
        sink.push(RawQuad::from_quad(quad));
    }
    let sunk = sink.finish().unwrap();
    let built = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    assert_eq!(component_names(&sunk), component_names(&built));
    assert_eq!(sunk.array.len(), built.array.len());

    let sunk = VortexRdfStore::from_built(sunk).unwrap();
    let built = VortexRdfStore::from_built(built).unwrap();
    assert_eq!(sunk.layout(), LayoutStrategy::Dictionary);
    assert_eq!(sunk.indexes(), built.indexes());
    assert_eq!(view_strings(&sunk).await, quad_strings(&quads));
    assert_eq!(view_strings(&sunk).await, view_strings(&built).await);

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let served = sunk
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(served.debug_has_serve_plan());
    assert_eq!(
        view_strings(&served).await,
        view_strings(
            &built
                .match_pattern(None, Some(&p0), None, None)
                .await
                .unwrap()
        )
        .await
    );
}

/// Decoding memoizes each role's terms in a fixed-size, direct-mapped table,
/// so codes that land in the same slot must still decode to their own terms —
/// a memo that trusted a slot without checking whose code filled it would
/// hand back a colliding row's term. Wide enough (~4k distinct terms over
/// 1024 slots) that collisions are certain, with repeated predicates and
/// graph names alongside them to exercise the hit path in the same pass.
#[tokio::test]
async fn test_dictionary_decode_survives_memo_slot_collisions() {
    let graph = |i: usize| {
        if i.is_multiple_of(3) {
            GraphName::DefaultGraph
        } else {
            GraphName::NamedNode(
                NamedNode::new(format!("http://example.org/graph/{}", i % 5)).unwrap(),
            )
        }
    };
    let quads: Vec<Quad> = (0..2_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:05}"),
                &format!("http://example.org/predicate/{}", i % 7),
                &format!("object value {i:05}"),
                graph(i),
            )
        })
        .collect();

    let arr = build_array::<SortedStreamBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    // ~2000 subjects + 2000 objects + 7 predicates + 4 graphs: far past the
    // memo's slot count, so distinct codes share slots.
    assert!(store.dictionary_snapshot().unwrap().0.len() > 1024);

    let decoded = store.quads_vec().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // The same rows reached through a match (a shorter chunk, decoded by the
    // same path) must agree term for term.
    let p3 = NamedNode::new("http://example.org/predicate/3").unwrap();
    let matched = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap()
        .quads_vec()
        .await
        .unwrap();
    let expected: Vec<Quad> = quads
        .iter()
        .filter(|q| q.predicate == p3.as_ref())
        .cloned()
        .collect();
    assert!(!expected.is_empty());
    assert_eq!(quad_strings(&matched), quad_strings(&expected));
}

#[tokio::test]
async fn test_dictionary_match_and_mutations() {
    let quads = dictionary_test_quads();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Subject match: hits the IsSorted binary-search fast path on the u32 column.
    let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
    let by_subject = store
        .match_pattern(Some(&s3), None, None, None)
        .await
        .unwrap();
    assert_eq!(by_subject.size().await.unwrap(), 1);
    let results: Vec<Quad> = by_subject.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s03>");

    // Predicate match: mask scan over the u32 codes (p0 occurs for i = 0,3,6,9).
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_pred.size().await.unwrap(), 4);

    // Terms absent from the dictionary match nothing (both routing paths).
    let missing_s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/nope").unwrap());
    assert_eq!(
        store
            .match_pattern(Some(&missing_s), None, None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    assert_eq!(
        store
            .match_pattern(None, Some(&missing_p), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );

    // delete_quad works (mask-based); the cached dictionary is propagated.
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert_eq!(deleted.size().await.unwrap(), 9);
    let decoded: Vec<Quad> = deleted.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded.len(), 9);
    assert!(!quad_strings(&decoded).contains(&quads[0].to_string()));

    // add_quad works despite the sorted codes: the appended quad lands in
    // the string tail (its terms need no dictionary code), so re-adding
    // the deleted quad brings the store back to its full size.
    let readded = deleted.add_quad(quads[0].clone()).await.unwrap();
    assert_eq!(readded.size().await.unwrap(), 10);
    let decoded: Vec<Quad> = readded.quads().unwrap().try_collect().await.unwrap();
    assert!(quad_strings(&decoded).contains(&quads[0].to_string()));
}

#[tokio::test]
async fn test_dictionary_layout_secondary_index_compatibility() {
    let quads = dictionary_test_quads();

    // Dictionary layout composes with secondary reference indexes.
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![
            IndexType::SecondaryByReference,
            IndexType::SecondaryByReference,
        ],
    )
    .await
    .unwrap();

    // The deduplicated roster and the children's code dtype are pinned by
    // `indexes::test_duplicate_index_requests_are_deduplicated`.
    assert_eq!(component_names(&arr), ["index:ref-o", "index:ref-p"]);
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Full roundtrip decode with the index columns present.
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // Predicate-only match: routes through the code-based `index:ref-p` child
    // (p0 occurs for i = 0,3,6,9).
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_pred = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_pred.size().await.unwrap(), 4);
    let results: Vec<Quad> = by_pred.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    // Object-only match: routes through the code-based `index:ref-o` child and
    // decodes through the store-cached dictionary ("object 1" for i = 1,5,9).
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let by_obj = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_obj.size().await.unwrap(), 3);
    let results: Vec<Quad> = by_obj.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.object.to_string() == "\"object 1\"")
    );

    // Combined o+p pattern: `index:ref-o` resolves the object to row ids and
    // the predicate is tested as the residual over those rows (i = 1,5,9
    // with p1 → only i = 1).
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_both = store
        .match_pattern(None, Some(&p1), Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(by_both.size().await.unwrap(), 1);
    let results: Vec<Quad> = by_both.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s01>");

    // Terms absent from the dictionary match nothing through the index paths.
    let missing_o = Term::Literal(Literal::new_simple_literal("nope"));
    assert_eq!(
        store
            .match_pattern(None, None, Some(&missing_o), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    assert_eq!(
        store
            .match_pattern(None, Some(&missing_p), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_dictionary_sorted_with_secondary_index() {
    // Sorted builder + Dictionary layout + secondary indexes: the subject
    // binary-search fast path and the index routing must compose — after
    // the subject binary-search range, the remaining bound terms are tested
    // as the residual over that range.
    let quads = dictionary_test_quads();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // s + o pattern (i = 5: s05 has "object 1").
    let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let matched = store
        .match_pattern(Some(&s5), None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 1);
    let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s05>");
    assert_eq!(results[0].object.to_string(), "\"object 1\"");

    // s + o with an object that exists but not on that subject.
    let o0 = Term::Literal(Literal::new_simple_literal("object 0"));
    assert_eq!(
        store
            .match_pattern(Some(&s5), None, Some(&o0), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn test_dictionary_empty_dataset() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(vec![]),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(arr.array.len(), 0);

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 0);
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert!(decoded.is_empty());
}

/// A built dictionary is one canonical chunk: the plaintext column as the
/// builder froze it, compressed only when written. The assertion is on the
/// encoding id because a compressed form decodes identically and would
/// otherwise go unnoticed.
#[tokio::test]
async fn test_built_dictionary_is_one_canonical_chunk() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(fsst_dictionary_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_dictionary_canonical(&store, "built");
}

/// `code_read_snapshot` is the one "codes are decodable" gate the frontends
/// share: Dictionary layout, empty append tail, resident dictionary — all
/// three, or `None`. The residency term is covered beside the file-backed
/// suite (`dictionary_file_backed`); this exercises the layout and tail terms.
#[tokio::test]
async fn test_code_read_snapshot_gate() {
    let quads = dictionary_test_quads();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    assert!(store.code_read_snapshot().is_some());

    // An append opens a string tail whose rows have no codes in the cached
    // dictionary: the gate closes even though `dictionary_snapshot` alone
    // still answers — exactly the difference that makes the gate the safe
    // predicate for code-typed reads.
    let tailed = store
        .add_quad(make_quad(
            "http://example.org/appended",
            "http://example.org/p0",
            "fresh object",
            GraphName::DefaultGraph,
        ))
        .await
        .unwrap();
    assert_ne!(tailed.tail_len(), 0);
    assert!(tailed.dictionary_snapshot().is_some());
    assert!(tailed.code_read_snapshot().is_none());

    // Folding the tail back into the base reopens the gate.
    let compacted = tailed.compact().await.unwrap();
    assert_eq!(compacted.tail_len(), 0);
    assert!(compacted.code_read_snapshot().is_some());

    // Non-Dictionary layouts have no codes at all.
    let arr =
        build_array::<SortedInMemoryBuilder>(quad_stream(quads), LayoutStrategy::Default, vec![])
            .await
            .unwrap();
    let string_store = VortexRdfStore::from_built(arr).unwrap();
    assert!(string_store.code_read_snapshot().is_none());
}

/// A store adopted from its own bytes hands out a code-read snapshot, and
/// the codes its rows and its matched views gather decode against that
/// snapshot to exactly the quads it was built from.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_code_read_snapshot_survives_from_bytes() {
    let quads = dictionary_test_quads();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let adopted = VortexRdfStore::from_bytes(&store.to_bytes().await.unwrap())
        .await
        .unwrap();
    let dict = adopted
        .code_read_snapshot()
        .expect("an adopted Dictionary store is code-readable");

    let decode_rows = |cols: &[vortex_buffer::Buffer<u32>; 4]| -> Vec<String> {
        let mut rows: Vec<String> = (0..cols[0].len())
            .map(|i| {
                let term = |c: &vortex_buffer::Buffer<u32>| {
                    dict.decode(c[i])
                        .expect("codes address the adopted dictionary")
                };
                format!(
                    "{} {} {} {}",
                    term(&cols[0]),
                    term(&cols[1]),
                    term(&cols[2]),
                    term(&cols[3])
                )
            })
            .collect();
        rows.sort();
        rows
    };
    let raw_rows = |keep: &dyn Fn(&Quad) -> bool| -> Vec<String> {
        let mut rows: Vec<String> = quads
            .iter()
            .filter(|q| keep(q))
            .map(|q| {
                let r = crate::store::RawQuad::from_quad(q);
                format!("{} {} {} {}", r.s, r.p, r.o, r.g)
            })
            .collect();
        rows.sort();
        rows
    };

    let gathered = adopted.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(decode_rows(&gathered), raw_rows(&|_| true));

    // The gather the bindings pair with the snapshot: a served match's
    // codes, read off the answering index.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = adopted
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert!(matched.debug_has_serve_plan());
    let matched_codes = matched.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(
        decode_rows(&matched_codes),
        raw_rows(&|q| q.predicate == p0.as_ref())
    );
}
