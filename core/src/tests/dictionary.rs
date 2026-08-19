use super::*;
#[cfg(feature = "file-io")]
use crate::io::container;
#[cfg(feature = "file-io")]
use vortex_array::IntoArray as _;
#[cfg(feature = "file-io")]
use vortex_array::arrays::struct_::StructArray;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamAdapter;
#[cfg(feature = "file-io")]
use vortex_array::validity::Validity;
#[cfg(feature = "file-io")]
use vortex_buffer::Buffer;

// ─── 7) Dictionary layout ──────────────────────────────────────────────
async fn run_dictionary_roundtrip<B: VortexArrayBuilder>() {
    let quads = dictionary_test_quads();
    let arr = build_array::<B>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .expect("dictionary build failed");

    let store = VortexRdfStore::from_built(arr).unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));
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
async fn test_dictionary_sorted_in_memory() {
    run_dictionary_roundtrip::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_dictionary_sorted_stream() {
    run_dictionary_roundtrip::<SortedStreamBuilder>().await;
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

    // Deduped index columns appear exactly once, and under the Dictionary
    // layout the index value columns hold u32 codes — same dtype as the
    // primary code columns — instead of strings.
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
            ],
        );
        assert_eq!(fields.field("_idx_o_val"), fields.field("o"));
        assert_eq!(fields.field("_idx_p_val"), fields.field("p"));
    } else {
        panic!("expected StructArray dtype");
    }

    let store = VortexRdfStore::from_built(arr).unwrap();

    // Full roundtrip decode with the index columns present.
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // Predicate-only match: routes through the code-based `_idx_p_*` index
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

    // Object-only match: routes through the code-based `_idx_o_*` index and
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

    // Combined o+p pattern: the object index narrows first; the derived
    // store (stale index columns stripped) mask-scans the predicate
    // (i = 1,5,9 with p1 → only i = 1).
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
    // the subject slice, the derived store's stale index columns are
    // stripped and the remaining terms are mask-scanned.
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

/// The `_dict_term` column of one dictionary-child chunk (the form
/// `dict_child_chunks` emits), for asserting on its encoding.
#[cfg(feature = "file-io")]
fn term_chunk_of(chunk: &vortex_array::ArrayRef) -> vortex_array::ArrayRef {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .unwrap()
        .unmasked_field_by_name(crate::store::layouts::dictionary::term_dict::TERM_FIELD)
        .unwrap()
        .clone()
}

/// The dictionary is held and round-tripped FSST-compressed, and that is an
/// invariant this code owns rather than one it inherits: which encoding a
/// column gets is otherwise decided by the writer's sampling and is free to
/// be something else. Asserting on the encoding is the only way this stays
/// true — the terms decode correctly either way, so a regression to
/// plaintext would be silent.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_terms_are_fsst_through_bytes() {
    let quads: Vec<Quad> = (0..2_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:06}"),
                &format!("http://example.org/predicate/{}", i % 16),
                &format!("object value {:06}", i / 2),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    // Every chunk of the held term column must be FSST — a lift that
    // canonicalized (decompressed) any chunk fails here.
    let assert_fsst = |store: &VortexRdfStore, when: &str| {
        let dict = store.dictionary_snapshot().unwrap().0;
        for chunk in crate::store::layouts::dictionary::dict_child_chunks(&dict).unwrap() {
            assert_eq!(
                term_chunk_of(&chunk).encoding_id().to_string(),
                "vortex.fsst",
                "{when}: dictionary chunk not FSST"
            );
        }
    };
    assert_fsst(&store, "built");

    // Written out and read back still compressed, not canonicalized on open.
    let bytes = store.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_fsst(&reread, "reread");

    // And the terms still resolve, through the compressed representation.
    assert_eq!(reread.size().await.unwrap(), 2_000);
    let p = NamedNode::new("http://example.org/predicate/3").unwrap();
    let matched = reread
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 125);
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_file_roundtrip() {
    use crate::io::ser::quads_stream_to_vortex_writer;

    let quads = dictionary_test_quads();

    // Streaming write to an in-memory buffer...
    let mut bytes: Vec<u8> = Vec::new();
    quads_stream_to_vortex_writer(
        quad_stream(quads.clone()),
        &mut bytes,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();

    // ...then open it as a file-backed store (loads the dictionary via a
    // single-column projection scan).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dict.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 10);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));

    // Pushed-down integer filter on the code columns.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 4);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    // A term absent from the dictionary yields an always-false filter.
    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// The native container end to end through the path-based writer: one
/// self-contained file whose root carries the transparent quad-source child
/// and the dictionary child, reopening with identical results.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_native_file_roundtrip() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;

    // One self-contained file: the native root with the quad-source child
    // and the dictionary as an auxiliary child.
    assert!(!path.with_extension("dict.vortex").exists());
    let file = crate::io::native_file::open_vortex_file(&path)
        .await
        .unwrap();
    assert!(container::is_native_file(&file));
    let names: Vec<_> = file.footer().layout().child_names().collect();
    assert_eq!(names.first().map(|n| n.as_ref()), Some("quad-source"));
    assert!(names.iter().any(|n| n.as_ref() == "dictionary"));

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), quads.len());

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 4);
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));
}

/// An empty Dictionary store still writes (and reopens through) the
/// dictionary segment — the key's presence is unconditional for the layout.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_dictionary_empty_store_roundtrip() {
    let (_dir, path) = write_store_file(Vec::new(), LayoutStrategy::Dictionary, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 0);
}

/// A file written by a generic Vortex writer — a valid Vortex file, but not a
/// native store — fails at open with an actionable error naming the expected
/// root layout, rather than a scan-time surprise.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_foreign_vortex_file_is_rejected() {
    use vortex_file::WriteOptionsSessionExt as _;

    // A bare u32 code schema, written by the raw Vortex writer under a
    // plain root layout.
    let col = || Buffer::from_iter([1u32, 2, 3]).into_array();
    let array = StructArray::try_new(
        ["s", "p", "o", "g"].into(),
        vec![col(), col(), col(), col()],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bare.vortex");
    let dtype = array.dtype().clone();
    let stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(array) })),
    );
    let mut writer = tokio::fs::File::create(&path).await.unwrap();
    crate::session::VORTEX_SESSION
        .write_options()
        .write(&mut writer, stream)
        .await
        .unwrap();

    let err = VortexRdfStore::from_file(&path)
        .await
        .err()
        .expect("open should fail");
    assert!(
        err.to_string().contains("not a vortex-rdf store file"),
        "unexpected error: {err}"
    );
}

/// A dictionary big enough that the writer splits its child into several
/// chunks must survive the resident lift still FSST-compressed, chunk by
/// chunk, with probe parity — the multi-chunk arm of the term store.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_large_dictionary_child_lift_keeps_fsst() {
    use crate::io::container::default_child_strategy;
    use crate::store::RawQuad;
    use crate::store::layouts::dictionary::{
        TermDictionaryBuilder, dict_child_chunks, dict_from_reader,
    };
    use vortex_buffer::ByteBuffer;
    use vortex_file::OpenOptionsSessionExt as _;

    // ~200k unique terms → several independent FSST windows, so the child is
    // written (verbatim, through the pass-through strategy) as several flat
    // leaves and splits back into several chunks.
    let mut builder = TermDictionaryBuilder::new();
    for i in 0..50_000u32 {
        builder.insert_quad(&RawQuad {
            s: format!("<http://example.org/subject/{i:07}>"),
            p: format!("<http://example.org/predicate/{i:07}>"),
            o: format!("\"object value number {i:07}\""),
            g: format!("<http://example.org/graph/{i:07}>"),
        });
    }
    let (dict, _id_map) = builder.finish().unwrap();
    let len = dict.len() as u64;

    // A minimal native file: a one-row quad child plus the dictionary child.
    let quads = vortex_array::arrays::StructArray::try_new(
        ["s", "p", "o", "g"].into(),
        (0..4)
            .map(|_| vortex_buffer::Buffer::from_iter([0u32]).into_array())
            .collect::<Vec<_>>(),
        1,
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dtype = quads.dtype().clone();
    let stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(quads) })),
    );
    let component = crate::io::ser::dict_component(&dict).unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        stream,
        default_child_strategy(),
        false,
        vec![component],
    )
    .await
    .unwrap();
    assert!(bytes.len() > 1 << 20, "file too small to force chunking");

    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))
        .unwrap();
    let typed = file
        .footer()
        .layout()
        .as_::<container::RdfStoreLayoutVTable>();
    let (_, child) = container::store_component(typed, container::DICT_COMPONENT_NAME)
        .unwrap()
        .unwrap();
    assert_eq!(child.row_count(), len);
    let reader = child
        .new_reader(
            container::DICT_COMPONENT_NAME.into(),
            file.segment_source(),
            file.session(),
            &Default::default(),
        )
        .unwrap();
    let lifted = dict_from_reader(reader).await.unwrap();

    // Multi-chunk, and every chunk still FSST.
    let chunks = dict_child_chunks(&lifted).unwrap();
    assert!(chunks.len() > 1, "large dictionary should lift multi-chunk");
    for chunk in &chunks {
        assert_eq!(
            term_chunk_of(chunk).encoding_id().to_string(),
            "vortex.fsst"
        );
    }

    // Probe parity across chunk boundaries, both directions.
    for code in (0..len as u32).step_by(7919) {
        let term = dict.term_at(code).unwrap();
        assert_eq!(lifted.get_id(&term), Some(code), "{term}");
        assert_eq!(lifted.term_at(code).as_deref(), Some(term.as_str()));
    }
    assert_eq!(lifted.get_id("\u{10FFFF}"), None);
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
