//! The Dictionary layout in memory: code round-trips, the decode memo,
//! index composition over codes, and the FSST-held term column.

use super::*;

// ─── Dictionary layout ─────────────────────────────────────────────────

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
    assert_eq!(served_component(&served), Some("index:posg"));
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
    assert!(store.dict_reader().unwrap().len() > 1024);

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
    let decoded: Vec<Quad> = store.quads().try_collect().await.unwrap();
    assert!(decoded.is_empty());
}

/// Every chunk of the term column a build holds is FSST-encoded. The
/// assertion is on the encoding id because a fall back to plaintext decodes
/// identically and would otherwise go unnoticed.
#[tokio::test]
async fn test_built_dictionary_terms_are_fsst() {
    let store = built_store(fsst_dictionary_quads(), LayoutStrategy::Dictionary, vec![]).await;
    assert_dictionary_terms_fsst(&store, "built");
}

/// A store adopted from its own bytes hands out a dictionary reader with a
/// resident snapshot, and the codes its rows and its matched views gather
/// decode, through that reader, to exactly the quads it was built from.
#[tokio::test]
async fn test_dict_reader_survives_from_bytes() {
    let quads = dictionary_test_quads();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let adopted = VortexRdfStore::from_bytes(&store.to_bytes().await.unwrap())
        .await
        .unwrap();
    let dict = adopted
        .dict_reader()
        .expect("an adopted Dictionary store hands out a dictionary reader");
    let snapshot = dict
        .snapshot()
        .expect("an adopted Dictionary store holds its dictionary resident");
    assert_eq!(
        dict.dictionary_id(),
        snapshot.dictionary_id(),
        "the snapshot and the reader are handles on one dictionary"
    );

    let gathered = adopted.code_columns_gathered().await.unwrap().unwrap();
    let mut rows = decoded_rows(&gathered, &dict).await;
    rows.sort();
    assert_eq!(rows, expected_rows(&quads, |_| true));

    // The gather the bindings pair with the snapshot: a served match's
    // codes, read off the answering index.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = adopted
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(served_component(&matched), Some("index:posg"));
    let matched_codes = matched.code_columns_gathered().await.unwrap().unwrap();
    let mut rows = decoded_rows(&matched_codes, &dict).await;
    rows.sort();
    assert_eq!(rows, expected_rows(&quads, |i| quads[i].predicate == p0));
}
