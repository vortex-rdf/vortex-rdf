use super::*;
use crate::store::builders::sorted_in_memory::build_sorted_chunk_stream;
use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};

// ─── 3) Streaming/chunking behavior ─────────────────────────────────────

/// The chunk-boundary harness every builder shares: chunk_size 3 over the 10
/// given quads must split as [3, 3, 3, 1] from both builders, whatever
/// the `layout`. Hands back each builder's collected chunks — plus the
/// dictionary the stream carried beside them, for the Dictionary layout —
/// for the callers' layout-specific follow-up assertions.
async fn run_chunk_boundary_builders(
    layout: LayoutStrategy,
    quads: &[Quad],
) -> Vec<(
    &'static str,
    Vec<vortex_array::ArrayRef>,
    Option<std::sync::Arc<crate::store::layouts::dictionary::TermDictionary>>,
)> {
    let mut out = Vec::new();
    for (name, result) in [
        (
            "sorted_in_memory",
            build_sorted_chunk_stream(Box::new(quad_stream(quads.to_vec())), layout, vec![], 3)
                .await,
        ),
        (
            "sorted_stream",
            build_sorted_stream_chunk_stream(
                Box::new(quad_stream(quads.to_vec())),
                layout,
                vec![],
                3,
                None,
            )
            .await,
        ),
    ] {
        let built = result.unwrap_or_else(|e| panic!("{name}: {e}"));
        let dict = built.dict.clone();
        let chunks: Vec<_> = built
            .chunks
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|c| c.unwrap_or_else(|e| panic!("{name}: {e}")))
            .collect();
        let lens: Vec<usize> = chunks.iter().map(|c| c.len()).collect();
        assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");
        out.push((name, chunks, dict));
    }
    out
}

#[tokio::test]
async fn test_streaming_chunk_boundaries() {
    let quads: Vec<Quad> = (0..10)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    for (name, chunks, _) in run_chunk_boundary_builders(LayoutStrategy::Default, &quads).await {
        if let vortex_array::dtype::DType::Struct(fields, _) = chunks[0].dtype() {
            let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
            assert_eq!(names, ["s", "p", "o", "g"], "{name}");
        } else {
            panic!("{name}: expected struct dtype");
        }
    }
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_streaming_write_read_roundtrip() {
    // Zero-padded so the build's (s, p, o, g) order is the input order, which
    // is what the positional assertions below read.
    let quads: Vec<Quad> = (0..25)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                &format!("object value {}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    // Streaming write to an in-memory Vortex file...
    let bytes = quads_stream_to_vortex(quad_stream(quads.clone()))
        .await
        .unwrap();

    // ...then load it back as a store from the file bytes.
    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 25);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded[0].subject.to_string(), quads[0].subject.to_string());
    assert_eq!(decoded[24].object.to_string(), quads[24].object.to_string());
}

#[tokio::test]
async fn test_sorted_streaming_chunk_boundaries() {
    // Quads fed in REVERSE subject order; both sorted builders must emit
    // globally sorted output across chunk boundaries.
    let quads: Vec<Quad> = (0..10)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    for (name, chunks, _) in run_chunk_boundary_builders(LayoutStrategy::Default, &quads).await {
        // Decode all chunks in order and verify global subject sort.
        let subjects: Vec<String> = chunks
            .iter()
            .flat_map(|c| store::layouts::ResolvedLayout::Default.decode_chunk(c))
            .map(|q| q.unwrap().subject.to_string())
            .collect();
        let mut sorted = subjects.clone();
        sorted.sort();
        assert_eq!(subjects, sorted, "{name}: output not globally sorted");
        assert_eq!(subjects.len(), 10, "{name}: wrong quad count");
    }
}

#[tokio::test]
async fn test_dictionary_streaming_chunk_boundaries() {
    use crate::store::builders::assemble_chunks;

    let quads = dictionary_test_quads();

    for (name, chunks, dict) in
        run_chunk_boundary_builders(LayoutStrategy::Dictionary, &quads).await
    {
        // Reassemble and decode through a store: the chunks hold bare
        // codes, and the dictionary the stream carried beside them is
        // handed back with the reassembled array — all chunks' codes must
        // reference that same global dictionary.
        let arr = assemble_chunks(chunks, LayoutStrategy::Dictionary, &vec![]).unwrap();
        let store = VortexRdfStore::from_built(crate::store::builders::BuiltArray {
            array: arr,
            components: Vec::new(),
            dict,
        })
        .unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{name}");
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(
            quad_strings(&decoded),
            quad_strings(&quads),
            "{name}: bad roundtrip"
        );
    }
}

/// The out-of-core indexed pipeline when the data genuinely spills: with a
/// chunk size far below the dataset the ingest produces several runs, so the
/// quad merge and every index family's `(value, row id)` sort all run
/// through their spill-and-K-way-merge branch rather than the single-run
/// in-memory one. The sibling tests all fit in one run, which leaves that
/// branch — and the global sortedness of the index columns it must still
/// produce — otherwise unexercised for the string layouts.
#[tokio::test]
async fn test_sorted_streaming_spilled_indexes_match_in_memory() {
    let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    // Fed in reverse so nothing is accidentally in order, and wide enough
    // (24 quads over a chunk size of 3) to force 8 runs.
    let mut quads = modular_quads(24, 3, 4);
    quads.reverse();

    // The quad chunk stream is primary-only (index families stream as native
    // components beside it); chunk sizes still follow the merge windows.
    let built = build_sorted_stream_chunk_stream(
        Box::new(quad_stream(quads.clone())),
        LayoutStrategy::Default,
        indexes.clone(),
        3,
        None,
    )
    .await
    .expect("spilled build");
    assert_eq!(
        built.components.len(),
        4,
        "posg/ospg/ref-o/ref-p components"
    );
    let chunks: Vec<_> = built
        .chunks
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|c| c.expect("chunk"))
        .collect();
    assert_eq!(
        chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
        [3, 3, 3, 3, 3, 3, 3, 3],
        "unexpected chunk sizes"
    );

    // The materializing path re-glues the streamed components into the
    // in-memory row space, index routing included.
    let built = crate::store::builders::sorted_stream::build_sorted_stream_array(
        Box::new(quad_stream(quads.clone())),
        LayoutStrategy::Default,
        indexes.clone(),
        3,
    )
    .await
    .expect("spilled array build");
    let store = VortexRdfStore::from_built(built).unwrap();
    assert_eq!(store.indexes(), &indexes[..]);

    // Every quad survived the spill round-trip, in global subject order.
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    let mut expected = quad_strings(&quads);
    expected.sort();
    assert_eq!(quad_strings(&decoded), expected);

    // And the index columns the spill merge emitted actually route: the
    // same selectivities the single-run copy-index test asserts.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        8
    );
    assert_eq!(
        store
            .match_pattern(None, None, Some(&o1), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        store
            .match_pattern(None, Some(&p1), Some(&o1), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        2
    );
}

#[cfg(feature = "file-io")]
async fn run_sorted_streaming_write_test() {
    let quads: Vec<Quad> = (0..25)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(
        quad_stream(quads),
        &mut buffer,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();

    let store = VortexRdfStore::from_bytes(&buffer).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 25);

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded[0].subject.to_string(), "<http://example.org/s00>");
    assert_eq!(decoded[24].subject.to_string(), "<http://example.org/s24>");
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_streaming_write() {
    run_sorted_streaming_write_test().await;
}

/// Whether a built array's `s` column carries the IsSorted stamp — the
/// binary-search fast path's sole license, so the builder tests assert on it
/// directly.
fn assert_subject_stamp(arr: vortex_array::ArrayRef, expect_sorted: bool, name: &str) {
    use vortex_array::expr::stats::{Precision, Stat, StatsProvider};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr.execute::<StructArray>(&mut ctx).unwrap();
    let s_col = struct_arr.unmasked_field_by_name("s").unwrap();
    let is_sorted = match s_col.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    };
    assert_eq!(is_sorted, expect_sorted, "{name}: IsSorted stat mismatch");
}

/// Reverse-subject-order quads, so only a genuinely sorting builder may end
/// up with a sorted `s` column.
fn reverse_order_quads() -> Vec<Quad> {
    let mut quads = modular_quads(10, 1, 1);
    quads.reverse();
    quads
}

#[tokio::test]
async fn test_sorted_builder_stamps_is_sorted() {
    let quads = reverse_order_quads();

    let sorted = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    assert_subject_stamp(sorted.array, true, "sorted_in_memory");
}

/// `quads_vec` (the exact-size materialization) must yield the same quads,
/// in the same order, as one-at-a-time stream collection — for a plain
/// store, a matched view, and a mutated store with a tail.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_quads_vec_matches_stream_collection() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let extra = make_quad(
        "http://example.org/tail-subject",
        "http://example.org/p0",
        "tail object",
        GraphName::DefaultGraph,
    );
    let views = [
        ("full", store.clone()),
        (
            "matched",
            store
                .match_pattern(None, Some(&p0), None, None)
                .await
                .unwrap(),
        ),
        ("tailed", store.add_quad(extra).await.unwrap()),
    ];
    for (tag, view) in views {
        let streamed: Vec<Quad> = view.quads().unwrap().try_collect().await.unwrap();
        let collected = view.quads_vec().await.unwrap();
        assert_eq!(collected, streamed, "{tag}");
    }
}
