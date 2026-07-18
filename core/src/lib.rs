pub mod error;
pub mod io;
pub mod store;
pub mod common;

pub use error::VortexRdfError;
pub use io::{
    deserialize,
    array_from_ipc_reader,
};
#[cfg(feature = "file-io")]
pub use io::{
    serialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer,
    quads_stream_to_vortex_writer_with_builder,
    load_vortex_file_ref,
};

pub use store::{
    VortexRdfStore,
    VortexArrayBuilder,
    SortedInMemoryBuilder,
    SortedStreamBuilder,
    UnsortedStreamBuilder,
    BuilderStrategy,
    IndexType,
    Indexes,
    LayoutStrategy,
};

#[cfg(not(target_arch = "wasm32"))]
use mimalloc::MiMalloc;
#[cfg(not(target_arch = "wasm32"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt, stream};
    use oxrdf::{GraphName, Literal, NamedNode, Quad, NamedOrBlankNode, Term};

    fn make_quad(s: &str, p: &str, o_lit: &str, g: GraphName) -> Quad {
        Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new(s).unwrap()),
            NamedNode::new(p).unwrap(),
            Term::Literal(Literal::new_simple_literal(o_lit)),
            g,
        )
    }

    fn quad_stream(quads: Vec<Quad>) -> impl futures::Stream<Item = crate::error::Result<Quad>> + Unpin + Send + 'static {
        stream::iter(quads.into_iter().map(Ok::<_, VortexRdfError>))
    }

    // ─── 1) Foundational roundtrip tests ───────────────────────────────────

    #[tokio::test]
    async fn test_roundtrip() {
        let quad = make_quad(
            "http://example.org/s",
            "http://example.org/p",
            "hello",
            GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
        );

        let arr = VortexRdfStore::build_vortex_array(quad_stream(vec![quad.clone()]))
            .await
            .expect("build failed");
        let store = VortexRdfStore::new(arr).unwrap();

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
        assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
        assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
        assert_eq!(decoded[0].graph_name.to_string(), quad.graph_name.to_string());
    }

    async fn run_builder_roundtrip<B: VortexArrayBuilder>() {
        let quad = make_quad(
            "http://example.org/s",
            "http://example.org/p",
            "hello",
            GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
        );

        let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
            quad_stream(vec![quad.clone()]),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .expect("build failed");

        let store = VortexRdfStore::new(arr).unwrap();
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
        assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
        assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
        assert_eq!(decoded[0].graph_name.to_string(), quad.graph_name.to_string());
    }

    #[tokio::test] async fn test_sorted_in_memory()    { run_builder_roundtrip::<SortedInMemoryBuilder>().await; }
    #[tokio::test] async fn test_sorted_stream()       { run_builder_roundtrip::<SortedStreamBuilder>().await; }
    #[tokio::test] async fn test_unsorted_stream()     { run_builder_roundtrip::<UnsortedStreamBuilder>().await; }

    async fn run_match_pattern_test<B: VortexArrayBuilder>() {
        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);

        let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .expect("build failed");
        let store = VortexRdfStore::new(arr).unwrap();

        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let filtered = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 1);

        let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);
    }

    // ─── 2) Core in-memory query semantics (no file I/O) ───────────────────

    #[cfg(feature = "file-io")]
    async fn run_match_pattern_file_test<B: VortexArrayBuilder>(layout: LayoutStrategy) {
        use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<B, _, _>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            &mut bytes,
            layout,
            vec![],
        )
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!(
            "vortex_rdf_match_file_test_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("match.vortex");
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let filtered = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 1);
        let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "file-io")]
    async fn run_match_pattern_file_typed_object_test<B: VortexArrayBuilder>() {
        use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<B, _, _>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            &mut bytes,
            LayoutStrategy::TypedObject,
            vec![],
        )
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!(
            "vortex_rdf_match_typed_file_test_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("match_typed.vortex");
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let filtered = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 1);
        let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), q1.subject.to_string());

        let o3 = Term::Literal(Literal::new_simple_literal("o3"));
        let empty = store.match_pattern(None, None, Some(&o3), None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "file-io")]
    async fn run_match_pattern_file_dictionary_test<B: VortexArrayBuilder>() {
        use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

        let quads = dictionary_test_quads();

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<B, _, _>(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
        )
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!(
            "vortex_rdf_match_dict_file_test_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("match_dict.vortex");
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let filtered = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 4);

        let missing_p = NamedNode::new("http://example.org/nope").unwrap();
        let empty = store.match_pattern(None, Some(&missing_p), None, None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test] async fn test_match_sorted_in_memory()    { run_match_pattern_test::<SortedInMemoryBuilder>().await; }
    #[tokio::test] async fn test_match_sorted_stream()       { run_match_pattern_test::<SortedStreamBuilder>().await; }
    #[tokio::test] async fn test_match_unsorted_stream()     { run_match_pattern_test::<UnsortedStreamBuilder>().await; }

    // ─── 2b) File-backed matching matrix (by layout/builder) ───────────────

    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_sorted_in_memory() {
        run_match_pattern_file_test::<SortedInMemoryBuilder>(LayoutStrategy::Default).await; 
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_sorted_stream() { 
        run_match_pattern_file_test::<SortedStreamBuilder>(LayoutStrategy::Default).await;
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_unsorted_stream() { 
        run_match_pattern_file_test::<UnsortedStreamBuilder>(LayoutStrategy::Default).await;
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_typed_sorted_in_memory() { 
        run_match_pattern_file_typed_object_test::<SortedInMemoryBuilder>().await;
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_typed_sorted_stream() { 
        run_match_pattern_file_typed_object_test::<SortedStreamBuilder>().await; 
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_typed_unsorted_stream() { 
        run_match_pattern_file_typed_object_test::<UnsortedStreamBuilder>().await; 
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_dictionary_sorted_in_memory() { 
        run_match_pattern_file_dictionary_test::<SortedInMemoryBuilder>().await; 
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_dictionary_sorted_stream() { 
        run_match_pattern_file_dictionary_test::<SortedStreamBuilder>().await; 
    }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_match_file_dictionary_unsorted_stream() { 
        run_match_pattern_file_dictionary_test::<UnsortedStreamBuilder>().await; 
    }

    // ─── 3) Streaming/chunking behavior ─────────────────────────────────────

    #[tokio::test]
    async fn test_streaming_chunk_boundaries() {
        use crate::store::builders::unsorted_stream::build_chunk_stream;

        let quads: Vec<Quad> = (0..10)
            .map(|i| make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p", "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        // chunk_size = 3 over 10 quads → chunks of 3, 3, 3, 1.
        let (dtype, chunks) = build_chunk_stream(
            Box::new(quad_stream(quads)),
            LayoutStrategy::Default,
            vec![],
            3,
        )
        .await
        .unwrap();

        if let vortex_array::dtype::DType::Struct(fields, _) = &dtype {
            let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
            assert_eq!(names, ["s", "p", "o", "g"]);
        } else {
            panic!("expected struct dtype");
        }

        let collected: Vec<_> = chunks.collect().await;
        let lens: Vec<usize> = collected.iter().map(|c| c.as_ref().unwrap().len()).collect();
        assert_eq!(lens, [3, 3, 3, 1]);
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_streaming_write_read_roundtrip() {
        let quads: Vec<Quad> = (0..25)
            .map(|i| make_quad(
                &format!("http://example.org/s{}", i),
                "http://example.org/p",
                &format!("object value {}", i),
                GraphName::DefaultGraph,
            ))
            .collect();

        // Streaming write to an in-memory Vortex file...
        let bytes = quads_stream_to_vortex(quad_stream(quads.clone())).await.unwrap();

        // ...then load it back as a Vortex file and decode.
        let arr = load_vortex_file_ref(vortex::buffer::Buffer::from(bytes)).await.unwrap();
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.size().await.unwrap(), 25);

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(decoded[0].subject.to_string(), quads[0].subject.to_string());
        assert_eq!(decoded[24].object.to_string(), quads[24].object.to_string());
    }

    #[tokio::test]
    async fn test_sorted_streaming_chunk_boundaries() {
        use crate::store::builders::sorted_in_memory::build_sorted_chunk_stream;
        use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;

        // Quads fed in REVERSE subject order; both sorted builders must emit
        // globally sorted output across chunk boundaries.
        let quads: Vec<Quad> = (0..10).rev()
            .map(|i| make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p", "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        for (name, result) in [
            ("sorted_in_memory", build_sorted_chunk_stream(
                Box::new(quad_stream(quads.clone())), LayoutStrategy::Default, vec![], 3,
            ).await),
            ("sorted_stream", build_sorted_stream_chunk_stream(
                Box::new(quad_stream(quads.clone())), LayoutStrategy::Default, vec![], 3,
            ).await),
        ] {
            let (_dtype, chunks) = result.unwrap_or_else(|e| panic!("{name}: {e}"));
            let collected: Vec<_> = chunks.collect().await;

            let lens: Vec<usize> = collected.iter().map(|c| c.as_ref().unwrap().len()).collect();
            assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");

            // Decode all chunks in order and verify global subject sort.
            let subjects: Vec<String> = collected.iter()
                .flat_map(|c| store::layouts::ResolvedLayout::Default.decode_chunk(c.as_ref().unwrap()))
                .map(|q| q.unwrap().subject.to_string())
                .collect();
            let mut sorted = subjects.clone();
            sorted.sort();
            assert_eq!(subjects, sorted, "{name}: output not globally sorted");
            assert_eq!(subjects.len(), 10, "{name}: wrong quad count");
        }
    }

    #[cfg(feature = "file-io")]
    async fn run_sorted_streaming_write_test<B: VortexArrayBuilder>() {
        let quads: Vec<Quad> = (0..25).rev()
            .map(|i| make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p", "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        let mut buffer = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<B, _, _>(
            quad_stream(quads),
            &mut buffer,
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();

        let arr = load_vortex_file_ref(vortex::buffer::Buffer::from(buffer.clone())).await.unwrap();
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.size().await.unwrap(), 25);

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(decoded[0].subject.to_string(), "<http://example.org/s00>");
        assert_eq!(decoded[24].subject.to_string(), "<http://example.org/s24>");
    }

    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_sorted_in_memory_streaming_write() { run_sorted_streaming_write_test::<SortedInMemoryBuilder>().await; }
    #[cfg(feature = "file-io")]
    #[tokio::test] async fn test_sorted_stream_streaming_write()    { run_sorted_streaming_write_test::<SortedStreamBuilder>().await; }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_subject_metadata_range_for_missing_subject() {
        let quads: Vec<Quad> = (0..25).rev()
            .map(|i| make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_subject_range_{}.vortex", uuid::Uuid::new_v4()));
        let mut buffer = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads),
            &mut buffer,
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        std::fs::write(&path, &buffer).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let missing = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
        let row_range = store.debug_subject_row_range(&missing).await.unwrap();
        assert_eq!(row_range, Some(0..0));
        assert_eq!(store.match_pattern(Some(&missing), None, None, None).await.unwrap().size().await.unwrap(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_sorted_builder_stamps_is_sorted() {
        use vortex_array::expr::stats::{Stat, StatsProvider, Precision};
        use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
        use vortex_array::VortexSessionExecute;

        let quads: Vec<Quad> = (0..10).rev()
            .map(|i| make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p", "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        let check = |arr: vortex_array::ArrayRef, expect_sorted: bool, name: &str| {
            let mut ctx = crate::io::VORTEX_LIGHT_SESSION.create_execution_ctx();
            let struct_arr = arr.execute::<StructArray>(&mut ctx).unwrap();
            let s_col = struct_arr.unmasked_field_by_name("s").unwrap();
            let is_sorted = match s_col.statistics().get(Stat::IsSorted) {
                Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
                Precision::Absent => false,
            };
            assert_eq!(is_sorted, expect_sorted, "{name}: IsSorted stat mismatch");
        };

        let sorted = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()), LayoutStrategy::Default, vec![],
        ).await.unwrap();
        check(sorted, true, "sorted_in_memory");

        let unsorted = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads), LayoutStrategy::Default, vec![],
        ).await.unwrap();
        check(unsorted, false, "unsorted");
    }

    #[tokio::test]
    async fn test_sorted_subject_binary_search() {
        // Multiple quads per subject: the binary-search fast path must return
        // the full [lo, hi) range for the matched subject.
        let mut quads: Vec<Quad> = Vec::new();
        for i in (0..10).rev() {
            for p in ["http://example.org/p1", "http://example.org/p2"] {
                quads.push(make_quad(
                    &format!("http://example.org/s{:02}", i),
                    p, "o",
                    GraphName::DefaultGraph,
                ));
            }
        }

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads), LayoutStrategy::Default, vec![],
        ).await.unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
        let matched = store.match_pattern(Some(&s5), None, None, None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 2);

        // Subject + predicate narrows within the sliced range.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched_sp = store.match_pattern(Some(&s5), Some(&p1), None, None).await.unwrap();
        assert_eq!(matched_sp.size().await.unwrap(), 1);

        // Missing subject → empty via binary search short-circuit.
        let s99 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
        let empty = store.match_pattern(Some(&s99), None, None, None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);
    }

    // ─── 4) Secondary index behavior ────────────────────────────────────────

    #[tokio::test]
    async fn test_multiple_indexes_deduplicated() {
        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);

        // The same index requested twice must not produce duplicate columns.
        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference, IndexType::SecondaryByReference],
        )
        .await
        .expect("build failed");

        // Schema: 4 primary columns + 4 reference index columns, exactly once.
        if let vortex_array::dtype::DType::Struct(fields, _) = arr.dtype() {
            let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
            assert_eq!(names, ["s", "p", "o", "g", "_idx_o_val", "_idx_o_rid", "_idx_p_val", "_idx_p_rid"]);
        } else {
            panic!("expected StructArray dtype");
        }

        // Index-routed matching still works.
        let store = VortexRdfStore::new(arr).unwrap();
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 1);
    }

    /// A store derived by matching keeps its indexes, because a view narrows a
    /// selection over the base rather than rewriting rows — so the `_idx_*_rid`
    /// ids still address the data. This is what lets a chained match keep
    /// routing through the index instead of degrading to a scan.
    #[tokio::test]
    async fn test_in_memory_derived_view_keeps_indexes() {
        let quads: Vec<Quad> = (0..24)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.indexes(), &vec![IndexType::SecondaryByReference]);

        // Match on the object index: 24 quads over 4 objects ⇒ 6 rows.
        let object = Term::Literal(Literal::new_simple_literal("object 1"));
        let matched = store.match_pattern(None, None, Some(&object), None).await.unwrap();
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
        // Lexicographic, so `s13>` sorts before `s1>`.
        assert_eq!(
            got,
            vec![
                "<http://example.org/s13>".to_string(),
                "<http://example.org/s1>".to_string()
            ]
        );

        // Materializing is the one step that breaks row identity, so it is also
        // the only one that drops the indexes — the quads must survive intact.
        let standalone = chained.materialize().await.unwrap();
        assert!(
            standalone.indexes().is_empty(),
            "materializing renumbers rows, so index ids cannot survive it"
        );
        assert_eq!(standalone.size().await.unwrap(), 2);
        let mut materialized: Vec<String> = standalone
            .quads()
            .unwrap()
            .map(|q| q.unwrap().subject.to_string())
            .collect()
            .await;
        materialized.sort();
        assert_eq!(materialized, got);
    }

    /// `compact_with_indexes` gathers the live rows like `materialize`, but
    /// rebuilds the requested indexes over the fresh `0..n` row order — so an
    /// independent, compacted store keeps routing through its index instead of
    /// degrading to a full scan. It also lets a store be re-indexed: the
    /// requested set is what the result carries, whatever the source had.
    #[tokio::test]
    async fn test_compact_with_indexes_rebuilds() {
        let quads: Vec<Quad> = (0..24)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:02}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        // A view over the object index: i = 1, 5, 9, 13, 17, 21 ⇒ 6 rows.
        let object = Term::Literal(Literal::new_simple_literal("object 1"));
        let view = store.match_pattern(None, None, Some(&object), None).await.unwrap();
        assert_eq!(view.size().await.unwrap(), 6);

        // Plain materialize drops the index; compact_with_indexes rebuilds it.
        assert!(view.materialize().await.unwrap().indexes().is_empty());
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
        assert_eq!(subjects_of(&routed).await, vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]);
        assert_eq!(store.size().await.unwrap(), 24, "source untouched");

        // Re-indexing from nothing: an empty set behaves like plain materialize,
        // and a store built without indexes gains one it never had.
        let bare = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let bare = VortexRdfStore::new(bare).unwrap();
        assert!(bare.indexes().is_empty());
        assert!(bare.compact_with_indexes(vec![]).await.unwrap().indexes().is_empty());
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
        let quads: Vec<Quad> = (0..24)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:02}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            layout,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

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
        assert_eq!(subjects_of(&indexed.match_pattern(None, Some(&predicate), None, None).await.unwrap()).await, vec![
            "<http://example.org/s01>".to_string(),
            "<http://example.org/s13>".to_string(),
        ]);
        assert_eq!(
            indexed.match_pattern(None, None, Some(&object), None).await.unwrap().size().await.unwrap(),
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

    /// Sorted subject strings of every quad a store exposes.
    async fn subjects_of(store: &VortexRdfStore) -> Vec<String> {
        let mut got: Vec<String> = store
            .quads()
            .unwrap()
            .map(|q| q.unwrap().subject.to_string())
            .collect()
            .await;
        got.sort();
        got
    }

    /// Deleting tombstones rows instead of rewriting them, so base row ids —
    /// and the secondary index built against them — survive the delete.
    #[tokio::test]
    async fn test_delete_keeps_indexes_usable() {
        let quads: Vec<Quad> = (0..12)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    &format!("http://example.org/p{}", i % 2),
                    &format!("object {}", i % 3),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

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
        let matched = after.match_pattern(None, None, Some(&object), None).await.unwrap();
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
                "<http://example.org/s3>".to_string(),
                "<http://example.org/s6>".to_string(),
                "<http://example.org/s9>".to_string()
            ]
        );

        // Materializing reclaims the tombstoned row (and drops the index).
        let compacted = after.materialize().await.unwrap();
        assert_eq!(compacted.size().await.unwrap(), 11);
        assert!(compacted.indexes().is_empty());
    }

    /// `delete_matching` drops every quad a pattern selects, using the same
    /// matcher `match_pattern` uses to find them.
    #[tokio::test]
    async fn test_delete_matching_pattern() {
        let quads: Vec<Quad> = (0..12)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    &format!("http://example.org/p{}", i % 2),
                    &format!("object {}", i % 3),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        // Delete every quad with predicate p0 (i even): 6 of the 12.
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let after = store.delete_matching(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(after.size().await.unwrap(), 6);
        assert_eq!(
            after.match_pattern(None, Some(&p0), None, None).await.unwrap().size().await.unwrap(),
            0
        );
        assert_eq!(after.quads().unwrap().count().await, 6);

        // Deleting the same pattern twice is idempotent, not a double-count.
        let again = after.delete_matching(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(again.size().await.unwrap(), 6);

        // A pattern matching nothing leaves the store alone.
        let missing = NamedNode::new("http://example.org/nope").unwrap();
        let untouched = again.delete_matching(None, Some(&missing), None, None).await.unwrap();
        assert_eq!(untouched.size().await.unwrap(), 6);
    }

    /// A file's rows are tombstoned in place too, so deleting from a file-backed
    /// store keeps its secondary indexes usable and never rewrites the file —
    /// covering both the index-resolved delete path and the filter-scan one.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_delete_keeps_indexes() {
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

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads.clone()),
            &mut bytes,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_file_delete_{}.vortex", uuid::Uuid::new_v4()));
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        // Index-resolved delete: "object 0" is indexed, so this resolves to
        // exact file row ids (i = 0, 3, 6, 9) without a filter scan.
        let object0 = Term::Literal(Literal::new_simple_literal("object 0"));
        let after = store.delete_matching(None, None, Some(&object0), None).await.unwrap();
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
            after.match_pattern(None, None, Some(&object0), None).await.unwrap().size().await.unwrap(),
            0
        );
        // Predicate p0 (i even: 0,2,4,6,8,10) had rows 0 and 6 tombstoned.
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let by_p0 = after.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(by_p0.size().await.unwrap(), 4);
        assert_eq!(by_p0.quads().unwrap().count().await, 4);

        // Filter-scan delete: a subject isn't index-resolved, so this exercises
        // the pruning + filter evaluation path that resolves the doomed rows.
        let s05 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
        let after2 = after.delete_matching(Some(&s05), None, None, None).await.unwrap();
        assert_eq!(after2.size().await.unwrap(), 7);
        // s05 is object "object 2" (5 % 3); that lookup now returns one fewer.
        let object2 = Term::Literal(Literal::new_simple_literal("object 2"));
        assert_eq!(
            after2.match_pattern(None, None, Some(&object2), None).await.unwrap().size().await.unwrap(),
            3,
        );

        // Materializing reclaims every tombstone and drops the index.
        let compacted = after2.materialize().await.unwrap();
        assert_eq!(compacted.size().await.unwrap(), 7);
        assert!(compacted.indexes().is_empty());
        assert_eq!(compacted.quads().unwrap().count().await, 7);

        let _ = std::fs::remove_file(&path);
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

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let object = Term::Literal(Literal::new_simple_literal("object 0"));
        let view = store.match_pattern(None, None, Some(&object), None).await.unwrap();
        assert_eq!(view.size().await.unwrap(), 3);

        for result in [
            view.add_quad(quads[0].clone()).await.err(),
            view.delete_quad(&quads[0]).await.err(),
            view.delete_matching(None, None, Some(&object), None).await.err(),
        ] {
            let message = result.expect("a derived view must reject mutations").to_string();
            assert!(
                message.contains("materialize()"),
                "the error should point at the way out, got: {message}"
            );
        }

        // Materializing yields an independent copy that mutates freely, and
        // leaves the store it came from alone.
        let owned = view.materialize().await.unwrap();
        let edited = owned.delete_quad(&quads[0]).await.unwrap();
        assert_eq!(edited.size().await.unwrap(), 2);
        assert_eq!(store.size().await.unwrap(), 6);

        // An unconstrained view covers exactly the base, so it counts as an
        // owner: mutating it is the same as mutating the store it came from.
        let whole = store.match_pattern(None, None, None, None).await.unwrap();
        assert_eq!(whole.delete_quad(&quads[0]).await.unwrap().size().await.unwrap(), 5);
    }

    /// The base a view was derived from stays reachable: matching narrows a
    /// selection, it does not throw the unselected rows away.
    #[tokio::test]
    async fn test_derived_view_does_not_lose_base_rows() {
        let quads: Vec<Quad> = (0..10)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    "http://example.org/p",
                    &format!("object {}", i % 2),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let object = Term::Literal(Literal::new_simple_literal("object 0"));
        let matched = store.match_pattern(None, None, Some(&object), None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 5);

        // Widening back out from the derived view reaches only what the view
        // selects (5 rows) — but the store it came from is untouched, and a
        // fresh match against it still sees all 10.
        let widened = matched.match_pattern(None, None, None, None).await.unwrap();
        assert_eq!(widened.size().await.unwrap(), 5);
        assert_eq!(store.size().await.unwrap(), 10);
    }

    async fn run_index_matrix_test<B: VortexArrayBuilder>(builder_name: &str) {
        let quads: Vec<Quad> = (0..24)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let layouts = [
            ("default", LayoutStrategy::Default),
            ("typed-object", LayoutStrategy::TypedObject),
            ("dictionary", LayoutStrategy::Dictionary),
        ];
        let index_configs: [(&str, Indexes); 4] = [
            ("none", vec![]),
            ("secondary-by-reference", vec![IndexType::SecondaryByReference]),
            ("secondary-by-copy", vec![IndexType::SecondaryByCopy]),
            (
                "both",
                vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            ),
        ];

        for (layout_name, layout) in layouts {
            for (index_name, indexes) in &index_configs {
                let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
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

                if let vortex_array::dtype::DType::Struct(fields, _) = arr.dtype() {
                    let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
                    let ref_cols = ["_idx_o_val", "_idx_o_rid", "_idx_p_val", "_idx_p_rid"];
                    let copy_cols = [
                        "_idx_posg_s", "_idx_posg_p", "_idx_posg_o", "_idx_posg_g", "_idx_posg_rid",
                        "_idx_ospg_s", "_idx_ospg_p", "_idx_ospg_o", "_idx_ospg_g", "_idx_ospg_rid",
                    ];
                    let expect_ref = indexes.contains(&IndexType::SecondaryByReference);
                    let expect_copy = indexes.contains(&IndexType::SecondaryByCopy);
                    assert!(
                        ref_cols.iter().all(|c| names.contains(c) == expect_ref)
                            && copy_cols.iter().all(|c| names.contains(c) == expect_copy),
                        "index column mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                    );
                } else {
                    panic!(
                        "expected StructArray dtype for builder={builder_name} layout={layout_name} indexes={index_name}"
                    );
                }

                let store = VortexRdfStore::new(arr).unwrap();
                assert_eq!(
                    store.size().await.unwrap(),
                    quads.len(),
                    "size mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                );

                let p0 = NamedNode::new("http://example.org/p0").unwrap();
                let by_pred = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
                assert_eq!(
                    by_pred.size().await.unwrap(),
                    8,
                    "predicate match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                );

                let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
                let by_obj = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
                assert_eq!(
                    by_obj.size().await.unwrap(),
                    6,
                    "object match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                );

                let p1 = NamedNode::new("http://example.org/p1").unwrap();
                let by_both = store.match_pattern(None, Some(&p1), Some(&o1), None).await.unwrap();
                assert_eq!(
                    by_both.size().await.unwrap(),
                    2,
                    "combined match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                );

                let missing_p = NamedNode::new("http://example.org/nope").unwrap();
                let empty = store.match_pattern(None, Some(&missing_p), None, None).await.unwrap();
                assert_eq!(
                    empty.size().await.unwrap(),
                    0,
                    "missing-term match mismatch for builder={builder_name} layout={layout_name} indexes={index_name}",
                );
            }
        }
    }

    #[tokio::test]
    async fn test_index_matrix_sorted_in_memory() {
        run_index_matrix_test::<SortedInMemoryBuilder>("SortedInMemoryBuilder").await;
    }

    #[tokio::test]
    async fn test_index_matrix_sorted_stream() {
        run_index_matrix_test::<SortedStreamBuilder>("SortedStreamBuilder").await;
    }

    #[tokio::test]
    async fn test_index_matrix_unsorted_stream() {
        run_index_matrix_test::<UnsortedStreamBuilder>("UnsortedStreamBuilder").await;
    }

    // ─── 4b) SecondaryByCopy: sorted full-copy index ────────────────────────

    #[tokio::test]
    async fn test_in_memory_copy_index_matching() {
        let quads: Vec<Quad> = (0..24)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:02}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByCopy],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

        // Predicate p1 marks i ≡ 1 (mod 3): 8 rows, via the POSG lead search.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let by_p = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(by_p.size().await.unwrap(), 8);

        // Object "object 1" marks i ≡ 1 (mod 4): 6 rows, via the OSPG lead.
        let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
        let by_o = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
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
        assert_eq!(subjects, ["<http://example.org/s01>", "<http://example.org/s13>"]);

        // The derived view keeps the index, and a chained match through it
        // must agree with the single-call prefix probe.
        assert_eq!(by_p.indexes(), &[IndexType::SecondaryByCopy]);
        let chained = by_p.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(chained.size().await.unwrap(), 2);
    }

    /// The file-backed copy index end to end: pattern shapes it accelerates,
    /// copy-served `quads()` streams (including residual graph constraints and
    /// tombstoned rows filtered through the family's rid column), and chained
    /// matches falling back to row ids.
    #[cfg(feature = "file-io")]
    async fn run_copy_index_file_serving_test<B: VortexArrayBuilder>(layout: LayoutStrategy) {
        use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

        async fn matched_strings(view: &VortexRdfStore) -> Vec<String> {
            let quads: Vec<Quad> = view.quads().unwrap().try_collect().await.unwrap();
            let mut strings: Vec<String> = quads.iter().map(|q| q.to_string()).collect();
            strings.sort();
            strings
        }

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

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<B, _, _>(
            quad_stream(quads.clone()),
            &mut bytes,
            layout,
            vec![IndexType::SecondaryByCopy],
        )
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!(
            "vortex_rdf_copy_index_file_test_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("copy_index.vortex");
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);

        // Predicate-bound: i ≡ 1 (mod 3), served from the POSG family.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let by_p = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert!(by_p.debug_has_copy_scan());
        assert_eq!(by_p.size().await.unwrap(), 10);
        assert_eq!(matched_strings(&by_p).await, expected(&|i| i % 3 == 1));

        // Object-bound: i ≡ 2 (mod 5), served from the OSPG family.
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let by_o = store.match_pattern(None, None, Some(&o2), None).await.unwrap();
        assert!(by_o.debug_has_copy_scan());
        assert_eq!(by_o.size().await.unwrap(), 6);
        assert_eq!(matched_strings(&by_o).await, expected(&|i| i % 5 == 2));

        // Predicate and object bound: one (p, o) prefix resolution —
        // i ≡ 1 (mod 3) ∧ i ≡ 1 (mod 5) ⇔ i ≡ 1 (mod 15).
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let by_po = store
            .match_pattern(None, Some(&p1), Some(&o1), None)
            .await
            .unwrap();
        assert_eq!(by_po.size().await.unwrap(), 2);
        assert_eq!(matched_strings(&by_po).await, expected(&|i| i % 15 == 1));

        // A residual graph constraint rides the copy-served scan's filter.
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let by_pg = store
            .match_pattern(None, Some(&p2), None, Some(&graphs[0]))
            .await
            .unwrap();
        assert_eq!(by_pg.size().await.unwrap(), 5);
        assert_eq!(
            matched_strings(&by_pg).await,
            expected(&|i| i % 3 == 2 && i % 2 == 0)
        );

        // Chaining a second match narrows the first view's row ids (the copy
        // plan is dropped — its filter no longer selects exactly the rows).
        let chained = by_p.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert!(!chained.debug_has_copy_scan());
        assert_eq!(
            matched_strings(&chained).await,
            expected(&|i| i % 3 == 1 && i % 5 == 1)
        );

        // A term the store has never seen short-circuits to empty.
        let missing = NamedNode::new("http://example.org/nope").unwrap();
        let none = store.match_pattern(None, Some(&missing), None, None).await.unwrap();
        assert_eq!(none.size().await.unwrap(), 0);

        // A tombstoned row must vanish from copy-served streams too: the scan
        // reads copy rows, so the delete reaches it through the rid column.
        let deleted = store.delete_quad(&quads[4]).await.unwrap();
        let by_p_after = deleted.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(by_p_after.size().await.unwrap(), 9);
        assert_eq!(
            matched_strings(&by_p_after).await,
            expected(&|i| i % 3 == 1 && i != 4)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_copy_index_file_serving_default_sorted_stream() {
        run_copy_index_file_serving_test::<SortedStreamBuilder>(LayoutStrategy::Default).await;
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_copy_index_file_serving_typed_sorted_stream() {
        run_copy_index_file_serving_test::<SortedStreamBuilder>(LayoutStrategy::TypedObject).await;
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_copy_index_file_serving_dictionary_sorted_stream() {
        run_copy_index_file_serving_test::<SortedStreamBuilder>(LayoutStrategy::Dictionary).await;
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_copy_index_file_serving_default_sorted_in_memory() {
        run_copy_index_file_serving_test::<SortedInMemoryBuilder>(LayoutStrategy::Default).await;
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_copy_index_file_serving_default_unsorted_stream() {
        run_copy_index_file_serving_test::<UnsortedStreamBuilder>(LayoutStrategy::Default).await;
    }

    // ─── 5) Mutation behavior ───────────────────────────────────────────────

    async fn run_add_delete_test<B: VortexArrayBuilder>() {
        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);

        let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
            quad_stream(vec![q1.clone()]),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .expect("build failed");
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.size().await.unwrap(), 1);

        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);
        let store = store.add_quad(q2.clone()).await.unwrap();
        assert_eq!(store.size().await.unwrap(), 2);

        let store = store.delete_quad(&q2).await.unwrap();
        assert_eq!(store.size().await.unwrap(), 1);

        let store = store.delete_quad(&q1).await.unwrap();
        assert_eq!(store.size().await.unwrap(), 0);
    }

    #[tokio::test] async fn test_add_delete_sorted_in_memory()    { run_add_delete_test::<SortedInMemoryBuilder>().await; }
    #[tokio::test] async fn test_add_delete_sorted_stream()       { run_add_delete_test::<SortedStreamBuilder>().await; }
    #[tokio::test] async fn test_add_delete_unsorted_stream()     { run_add_delete_test::<UnsortedStreamBuilder>().await; }

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
        let matched = store.match_pattern(None, Some(&p), None, None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 10);

        let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
        let matched_s5 = store.match_pattern(Some(&s5), None, None, None).await.unwrap();
        assert_eq!(matched_s5.size().await.unwrap(), 1);
    }

    /// Appends land in the tail, never the base — so the base's secondary
    /// indexes survive an add, and queries union the base's fast paths with a
    /// tail scan.
    #[tokio::test]
    async fn test_add_quads_keeps_indexes() {
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

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let added = store
            .add_quads([
                make_quad("http://example.org/s90", "http://example.org/p0", "object 0", GraphName::DefaultGraph),
                make_quad("http://example.org/s91", "http://example.org/p9", "object 9", GraphName::DefaultGraph),
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
        let matched = added.match_pattern(None, None, Some(&object), None).await.unwrap();
        let subjects = subjects_of(&matched).await;
        assert_eq!(subjects.len(), 5);
        assert!(subjects.contains(&"<http://example.org/s90>".to_string()));

        // Terms the base has never seen — the index proves the base empty —
        // still match in the tail.
        let p9 = NamedNode::new("http://example.org/p9").unwrap();
        assert_eq!(
            added.match_pattern(None, Some(&p9), None, None).await.unwrap().size().await.unwrap(),
            1
        );
        let s90 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s90").unwrap());
        assert_eq!(
            added.match_pattern(Some(&s90), None, None, None).await.unwrap().size().await.unwrap(),
            1
        );

        // Chained matches narrow base and tail together: of the five
        // "object 0" rows, p0 holds on base rows 0 and 6, and on s90.
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let chained = matched.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(chained.size().await.unwrap(), 3);

        // Deletes tombstone in the tail exactly as in the base.
        let deleted_tail = added
            .delete_matching(Some(&s90), None, None, None)
            .await
            .unwrap();
        assert_eq!(deleted_tail.size().await.unwrap(), 13);
        assert_eq!(
            deleted_tail.match_pattern(None, None, Some(&object), None).await.unwrap().size().await.unwrap(),
            4
        );
        let deleted_base = deleted_tail.delete_quad(&quads[0]).await.unwrap();
        assert_eq!(deleted_base.size().await.unwrap(), 12);
        assert_eq!(
            deleted_base.match_pattern(None, None, Some(&object), None).await.unwrap().size().await.unwrap(),
            3
        );
    }

    /// `add_quads` follows RDF/JS dataset (set) semantics: a quad equal to an
    /// existing one, or repeated within the batch, is skipped — and a deleted
    /// quad counts as absent, so it can be re-added.
    #[tokio::test]
    async fn test_add_quads_set_semantics() {
        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);
        let q3 = make_quad("http://example.org/s3", "http://example.org/p3", "o3", GraphName::DefaultGraph);

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

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

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(batch(0..10)),
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

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
            compacted.match_pattern(Some(&s), None, None, None).await.unwrap().size().await.unwrap(),
            1
        );
        let object = Term::Literal(Literal::new_simple_literal("object 3"));
        assert_eq!(
            compacted.match_pattern(None, None, Some(&object), None).await.unwrap().size().await.unwrap(),
            822
        );
    }

    /// File-backed stores never auto-compact: folding the tail would pull the
    /// whole file into memory, a backend change an append must not make
    /// implicitly. The tail accumulates until `compact()` is called.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_add_never_auto_compacts() {
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

        let path =
            std::env::temp_dir().join(format!("vortex_rdf_autocompact_{}.vortex", std::process::id()));
        let file = tokio::fs::File::create(&path).await.unwrap();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads.clone()),
            file,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
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

        // 4_200 tail rows is past every in-memory threshold, yet the file
        // store keeps them as a tail.
        let added = store.add_quads(batch).await.unwrap();
        assert_eq!(added.tail_len(), 4_200);
        assert_eq!(added.size().await.unwrap(), 4_204);

        // Folding is explicit — and converts the store into a compacted
        // in-memory one, current indexes preserved.
        let compacted = added.compact().await.unwrap();
        assert_eq!(compacted.tail_len(), 0);
        assert_eq!(compacted.size().await.unwrap(), 4_204);
        assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);

        tokio::fs::remove_file(&path).await.ok();
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

        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

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
            added.match_pattern(None, None, Some(&new_object), None).await.unwrap().size().await.unwrap(),
            1
        );
        // Dictionary-coded base terms keep routing as before.
        let old_object = Term::Literal(Literal::new_simple_literal("object 1"));
        assert_eq!(
            added.match_pattern(None, None, Some(&old_object), None).await.unwrap().size().await.unwrap(),
            4
        );

        // Serializing re-encodes base and tail against a fresh dictionary, so
        // the written array stands alone.
        let arr = added.to_serializable_array().await.unwrap();
        let reloaded = VortexRdfStore::new(arr).unwrap();
        assert_eq!(reloaded.size().await.unwrap(), 13);
        assert_eq!(
            reloaded.match_pattern(None, None, Some(&new_object), None).await.unwrap().size().await.unwrap(),
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
            compacted.match_pattern(None, None, Some(&new_object), None).await.unwrap().size().await.unwrap(),
            1
        );
        assert_eq!(
            compacted.match_pattern(None, None, Some(&old_object), None).await.unwrap().size().await.unwrap(),
            4
        );
    }

    /// Compaction (`compact_with_indexes`) re-sorts by (s, p, o, g): the
    /// tail and any tombstones are folded away, the quads come back in SPOG
    /// order, and the subject binary-search fast path is restored alongside
    /// the rebuilt indexes.
    #[tokio::test]
    async fn test_compaction_folds_tail_and_sorts() {
        // Built unsorted (reverse subject order), so nothing is sorted going in.
        let quads: Vec<Quad> = (0..6)
            .rev()
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{}", i),
                    "http://example.org/p0",
                    &format!("object {}", i % 2),
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let added = store
            .add_quads([
                make_quad("http://example.org/s9", "http://example.org/p0", "object 0", GraphName::DefaultGraph),
                make_quad("http://example.org/s8", "http://example.org/p0", "object 1", GraphName::DefaultGraph),
            ])
            .await
            .unwrap();
        let deleted = added.delete_quad(&quads[0]).await.unwrap(); // drops s5
        assert_eq!(deleted.size().await.unwrap(), 7);

        let compacted = deleted
            .compact_with_indexes(vec![IndexType::SecondaryByReference])
            .await
            .unwrap();
        assert_eq!(compacted.size().await.unwrap(), 7);
        assert_eq!(compacted.indexes(), &[IndexType::SecondaryByReference]);

        // The rows come back in global SPOG order (tail rows interleaved, the
        // tombstoned s5 gone) — not in the unsorted insertion order.
        assert_eq!(subjects_of(&compacted).await, vec![
            "<http://example.org/s0>".to_string(),
            "<http://example.org/s1>".to_string(),
            "<http://example.org/s2>".to_string(),
            "<http://example.org/s3>".to_string(),
            "<http://example.org/s4>".to_string(),
            "<http://example.org/s8>".to_string(),
            "<http://example.org/s9>".to_string(),
        ]);

        // Subject lookups and the rebuilt object index both answer.
        let s9 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s9").unwrap());
        assert_eq!(
            compacted.match_pattern(Some(&s9), None, None, None).await.unwrap().size().await.unwrap(),
            1
        );
        let object = Term::Literal(Literal::new_simple_literal("object 0"));
        assert_eq!(
            compacted.match_pattern(None, None, Some(&object), None).await.unwrap().size().await.unwrap(),
            4
        );

        // The compacted store owns its rows: it mutates freely.
        let again = compacted
            .add_quad(make_quad("http://example.org/s7", "http://example.org/p0", "object 0", GraphName::DefaultGraph))
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

        let path = std::env::temp_dir().join(format!("vortex_rdf_add_{}.vortex", std::process::id()));
        let file = tokio::fs::File::create(&path).await.unwrap();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads.clone()),
            file,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let added = store
            .add_quads([
                make_quad("http://example.org/s90", "http://example.org/p0", "object 0", GraphName::DefaultGraph),
                make_quad("http://example.org/s91", "http://example.org/p9", "object 9", GraphName::DefaultGraph),
            ])
            .await
            .unwrap();
        assert_eq!(added.size().await.unwrap(), 14);
        assert_eq!(added.indexes(), &[IndexType::SecondaryByReference]);
        assert_eq!(store.size().await.unwrap(), 12, "the file view is untouched");

        // Index-routed file lookup + tail scan union.
        let object = Term::Literal(Literal::new_simple_literal("object 0"));
        let subjects = subjects_of(&added.match_pattern(None, None, Some(&object), None).await.unwrap()).await;
        assert_eq!(subjects.len(), 5);
        assert!(subjects.contains(&"<http://example.org/s90>".to_string()));
        // A term only the tail knows.
        let object9 = Term::Literal(Literal::new_simple_literal("object 9"));
        assert_eq!(
            added.match_pattern(None, None, Some(&object9), None).await.unwrap().size().await.unwrap(),
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
            compacted.match_pattern(None, None, Some(&object9), None).await.unwrap().size().await.unwrap(),
            1
        );

        tokio::fs::remove_file(&path).await.ok();
    }

    // ─── 6) Dictionary and file-backed edge behavior ────────────────────────

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_filtered_size() {
        // 20 quads alternating between two predicates (10 each).
        let quads: Vec<Quad> = (0..20)
            .map(|i| make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 2),
                "o",
                GraphName::DefaultGraph,
            ))
            .collect();

        let bytes = quads_stream_to_vortex(quad_stream(quads)).await.unwrap();
        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_size_test_{}.vortex", uuid::Uuid::new_v4()));
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_eq!(store.size().await.unwrap(), 20);

        // size() on a filtered file-backed store must report the *filtered*
        // count, not file.row_count().
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let filtered = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 10);

        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_secondary_index_object_predicate() {
        let quads: Vec<Quad> = (0..30)
            .map(|i| make_quad(
                &format!("http://example.org/s{}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            ))
            .collect();

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();

        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_file_index_match_{}.vortex", uuid::Uuid::new_v4()));
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let by_object = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(by_object.size().await.unwrap(), 6);

        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let by_predicate = store.match_pattern(None, Some(&p2), None, None).await.unwrap();
        assert_eq!(by_predicate.size().await.unwrap(), 10);

        let by_both = store.match_pattern(None, Some(&p2), Some(&o1), None).await.unwrap();
        assert_eq!(by_both.size().await.unwrap(), 2);

        let _ = std::fs::remove_file(&path);
    }

    /// Regression: predicate matches whose zone-map hits are NOT contiguous
    /// (clusters at both ends of an s-sorted file, with a middle zone whose
    /// stats exclude the predicate) must all survive the metadata row-range
    /// pre-pass. A first-gap cutoff would silently drop the trailing cluster.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_non_contiguous_predicate_matches() {
        // Three 8192-row zones. The rare predicate appears only in the first
        // and last 64 rows; the middle zone holds exclusively the common
        // predicate (lexicographically greater), so its zone map excludes the
        // rare one and creates an interior prunable gap.
        const N: usize = 3 * 8192;
        const CLUSTER: usize = 64;
        let quads: Vec<Quad> = (0..N)
            .map(|i| {
                let p = if i < CLUSTER || i >= N - CLUSTER {
                    "http://example.org/pAAA"
                } else {
                    "http://example.org/pMMM"
                };
                make_quad(
                    &format!("http://example.org/s{:06}", i),
                    p,
                    "o",
                    GraphName::DefaultGraph,
                )
            })
            .collect();

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![],
        )
        .await
        .unwrap();
        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_noncontig_{}.vortex", uuid::Uuid::new_v4()));
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let p_rare = NamedNode::new("http://example.org/pAAA").unwrap();
        let matched = store.match_pattern(None, Some(&p_rare), None, None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 2 * CLUSTER);

        let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results.len(), 2 * CLUSTER);
        // Both the leading and the trailing cluster must be present.
        assert!(results
            .iter()
            .any(|q| q.subject.to_string() == "<http://example.org/s000000>"));
        assert!(results
            .iter()
            .any(|q| q.subject.to_string() == format!("<http://example.org/s{:06}>", N - 1)));

        let _ = std::fs::remove_file(&path);
    }

    /// Chained matches on a multi-zone file: a subject match narrows the store
    /// to a row range from the zone maps; a subsequent indexed object match
    /// must restrict its index row ids to that range (not discard the index),
    /// and index-to-index chaining must intersect the two id lists.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_file_backed_chained_subject_then_object_index() {
        const N: usize = 3 * 8192;
        let quads: Vec<Quad> = (0..N)
            .map(|i| make_quad(
                &format!("http://example.org/s{:06}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 5),
                GraphName::DefaultGraph,
            ))
            .collect();

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Default,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let path = std::env::temp_dir()
            .join(format!("vortex_rdf_chained_index_{}.vortex", uuid::Uuid::new_v4()));
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();

        // The sorted subject column narrows a bound subject to a sub-range of
        // the file via zone-map pruning (row 12000 lives in the middle zone).
        let s_mid = NamedOrBlankNode::NamedNode(
            NamedNode::new("http://example.org/s012000").unwrap(),
        );
        let range = store.debug_subject_row_range(&s_mid).await.unwrap();
        let range = range.expect("sorted multi-zone file must narrow a bound subject");
        assert!(range.start <= 12000 && 12000 < range.end);
        assert!(range.end - range.start < N as u64, "envelope must exclude other zones");

        // Subject match first (row range), then indexed object match: the
        // object index ids are restricted to the subject's range.
        let by_subject = store.match_pattern(Some(&s_mid), None, None, None).await.unwrap();
        let o0 = Term::Literal(Literal::new_simple_literal("o0"));
        let chained = by_subject.match_pattern(None, None, Some(&o0), None).await.unwrap();
        assert_eq!(chained.size().await.unwrap(), 1); // 12000 % 5 == 0
        let results: Vec<Quad> = chained.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].subject.to_string(), "<http://example.org/s012000>");
        assert_eq!(results[0].object.to_string(), "\"o0\"");

        // Same chain with an object the subject's row doesn't carry.
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let empty = by_subject.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);

        // Index-to-index chaining: object ids ∩ predicate ids.
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let step1 = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
        let step2 = step1.match_pattern(None, Some(&p2), None, None).await.unwrap();
        let expected = (0..N).filter(|i| i % 5 == 1 && i % 3 == 2).count();
        assert_eq!(step2.size().await.unwrap(), expected);
        let results: Vec<Quad> = step2.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results.len(), expected);
        assert!(results.iter().all(|q| {
            q.predicate.to_string() == "<http://example.org/p2>"
                && q.object.to_string() == "\"o1\""
        }));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_match_typed_object_layout() {
        let q1 = make_quad("http://example.org/s1", "http://example.org/p1", "o1", GraphName::DefaultGraph);
        let q2 = make_quad("http://example.org/s2", "http://example.org/p2", "o2", GraphName::DefaultGraph);

        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(vec![q1.clone(), q2.clone()]),
            LayoutStrategy::TypedObject,
            vec![],
        )
        .await
        .expect("build failed");
        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.layout(), LayoutStrategy::TypedObject);

        // Match by object literal — exercises the typed o_kind/o_value columns.
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let matched = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 1);
        let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), q1.subject.to_string());
        assert_eq!(results[0].object.to_string(), q1.object.to_string());

        // Match by predicate.
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let matched_p = store.match_pattern(None, Some(&p2), None, None).await.unwrap();
        assert_eq!(matched_p.size().await.unwrap(), 1);

        // Non-existent object yields nothing.
        let o3 = Term::Literal(Literal::new_simple_literal("o3"));
        let empty = store.match_pattern(None, None, Some(&o3), None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);
    }

    fn dictionary_indexes() -> Indexes {
        vec![]
    }

    /// Quads with shared terms across positions, a named graph, and the
    /// default graph — exercises the single shared dictionary.
    fn dictionary_test_quads() -> Vec<Quad> {
        (0..10)
            .map(|i| {
                let g = if i % 2 == 0 {
                    GraphName::DefaultGraph
                } else {
                    GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap())
                };
                make_quad(
                    &format!("http://example.org/s{:02}", i),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 4),
                    g,
                )
            })
            .collect()
    }

    fn quad_strings(quads: &[Quad]) -> Vec<String> {
        let mut v: Vec<String> = quads.iter().map(|q| q.to_string()).collect();
        v.sort();
        v
    }

    async fn run_dictionary_roundtrip<B: VortexArrayBuilder>() {
        let quads = dictionary_test_quads();
        let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
        )
        .await
        .expect("dictionary build failed");

        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary);

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(quad_strings(&decoded), quad_strings(&quads));
    }

    #[tokio::test] async fn test_dictionary_sorted_in_memory() { run_dictionary_roundtrip::<SortedInMemoryBuilder>().await; }
    #[tokio::test] async fn test_dictionary_sorted_stream()    { run_dictionary_roundtrip::<SortedStreamBuilder>().await; }
    #[tokio::test] async fn test_dictionary_unsorted_stream()  { run_dictionary_roundtrip::<UnsortedStreamBuilder>().await; }

    #[tokio::test]
    async fn test_dictionary_streaming_chunk_boundaries() {
        use crate::store::builders::assemble_chunks;
        use crate::store::builders::unsorted_stream::build_chunk_stream;
        use crate::store::builders::sorted_in_memory::build_sorted_chunk_stream;
        use crate::store::builders::sorted_stream::build_sorted_stream_chunk_stream;

        let quads = dictionary_test_quads();

        for (name, result) in [
            ("unsorted_stream", build_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary, dictionary_indexes(), 3,
            ).await),
            ("sorted_in_memory", build_sorted_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary, dictionary_indexes(), 3,
            ).await),
            ("sorted_stream", build_sorted_stream_chunk_stream(
                Box::new(quad_stream(quads.clone())),
                LayoutStrategy::Dictionary, dictionary_indexes(), 3,
            ).await),
        ] {
            let (_dtype, chunks) = result.unwrap_or_else(|e| panic!("{name}: {e}"));
            let collected: Vec<_> = chunks.collect().await;
            let lens: Vec<usize> = collected.iter().map(|c| c.as_ref().unwrap().len()).collect();
            assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");

            // Reassemble and decode through a store: this fails unless chunk 0
            // carries the dictionary payload (row 0 of the assembled array)
            // and all chunks' codes reference the same global dictionary.
            let chunks: Vec<_> = collected.into_iter().map(|c| c.unwrap()).collect();
            let arr = assemble_chunks(chunks, LayoutStrategy::Dictionary, &dictionary_indexes()).unwrap();
            let store = VortexRdfStore::new(arr).unwrap();
            assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{name}");
            let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
            assert_eq!(quad_strings(&decoded), quad_strings(&quads), "{name}: bad roundtrip");
        }
    }

    #[tokio::test]
    async fn test_dictionary_match_and_mutations() {
        let quads = dictionary_test_quads();
        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        // Subject match: hits the IsSorted binary-search fast path on the u32 column.
        let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
        let by_subject = store.match_pattern(Some(&s3), None, None, None).await.unwrap();
        assert_eq!(by_subject.size().await.unwrap(), 1);
        let results: Vec<Quad> = by_subject.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), "<http://example.org/s03>");

        // Predicate match: mask scan over the u32 codes (p0 occurs for i = 0,3,6,9).
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let by_pred = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(by_pred.size().await.unwrap(), 4);

        // Terms absent from the dictionary match nothing (both routing paths).
        let missing_s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/nope").unwrap());
        assert_eq!(store.match_pattern(Some(&missing_s), None, None, None).await.unwrap().size().await.unwrap(), 0);
        let missing_p = NamedNode::new("http://example.org/nope").unwrap();
        assert_eq!(store.match_pattern(None, Some(&missing_p), None, None).await.unwrap().size().await.unwrap(), 0);

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
        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference, IndexType::SecondaryByReference],
        )
        .await
        .unwrap();

        // Deduped index columns appear exactly once, and under the Dictionary
        // layout the index value columns hold u32 codes — same dtype as the
        // primary code columns — instead of strings.
        if let vortex_array::dtype::DType::Struct(fields, _) = arr.dtype() {
            let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
            assert_eq!(
                names,
                ["s", "p", "o", "g", "_dict_terms", "_idx_o_val", "_idx_o_rid", "_idx_p_val", "_idx_p_rid"],
            );
            assert_eq!(fields.field("_idx_o_val"), fields.field("o"));
            assert_eq!(fields.field("_idx_p_val"), fields.field("p"));
        } else {
            panic!("expected StructArray dtype");
        }

        let store = VortexRdfStore::new(arr).unwrap();

        // Full roundtrip decode with the index columns present.
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(quad_strings(&decoded), quad_strings(&quads));

        // Predicate-only match: routes through the code-based `_idx_p_*` index
        // (p0 occurs for i = 0,3,6,9).
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let by_pred = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(by_pred.size().await.unwrap(), 4);
        let results: Vec<Quad> = by_pred.quads().unwrap().try_collect().await.unwrap();
        assert!(results.iter().all(|q| q.predicate == "http://example.org/p0"));

        // Object-only match: routes through the code-based `_idx_o_*` index and
        // decodes through the store-cached dictionary ("object 1" for i = 1,5,9).
        let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
        let by_obj = store.match_pattern(None, None, Some(&o1), None).await.unwrap();
        assert_eq!(by_obj.size().await.unwrap(), 3);
        let results: Vec<Quad> = by_obj.quads().unwrap().try_collect().await.unwrap();
        assert!(results.iter().all(|q| q.object.to_string() == "\"object 1\""));

        // Combined o+p pattern: the object index narrows first; the derived
        // store (stale index columns stripped) mask-scans the predicate
        // (i = 1,5,9 with p1 → only i = 1).
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let by_both = store.match_pattern(None, Some(&p1), Some(&o1), None).await.unwrap();
        assert_eq!(by_both.size().await.unwrap(), 1);
        let results: Vec<Quad> = by_both.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), "<http://example.org/s01>");

        // Terms absent from the dictionary match nothing through the index paths.
        let missing_o = Term::Literal(Literal::new_simple_literal("nope"));
        assert_eq!(store.match_pattern(None, None, Some(&missing_o), None).await.unwrap().size().await.unwrap(), 0);
        let missing_p = NamedNode::new("http://example.org/nope").unwrap();
        assert_eq!(store.match_pattern(None, Some(&missing_p), None, None).await.unwrap().size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_dictionary_sorted_with_secondary_index() {
        // Sorted builder + Dictionary layout + secondary indexes: the subject
        // binary-search fast path and the index routing must compose — after
        // the subject slice, the derived store's stale index columns are
        // stripped and the remaining terms are mask-scanned.
        let quads = dictionary_test_quads();
        let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::new(arr).unwrap();

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(quad_strings(&decoded), quad_strings(&quads));

        // s + o pattern (i = 5: s05 has "object 1").
        let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s05").unwrap());
        let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
        let matched = store.match_pattern(Some(&s5), None, Some(&o1), None).await.unwrap();
        assert_eq!(matched.size().await.unwrap(), 1);
        let results: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(results[0].subject.to_string(), "<http://example.org/s05>");
        assert_eq!(results[0].object.to_string(), "\"object 1\"");

        // s + o with an object that exists but not on that subject.
        let o0 = Term::Literal(Literal::new_simple_literal("object 0"));
        assert_eq!(store.match_pattern(Some(&s5), None, Some(&o0), None).await.unwrap().size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_dictionary_empty_dataset() {
        let arr = VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream(vec![]),
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
        )
        .await
        .unwrap();
        assert_eq!(arr.len(), 0);

        let store = VortexRdfStore::new(arr).unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary);
        assert_eq!(store.size().await.unwrap(), 0);
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert!(decoded.is_empty());
    }

    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn test_dictionary_file_roundtrip() {
        use crate::io::ser::quads_stream_to_vortex_writer_with_builder;

        let quads = dictionary_test_quads();

        // Streaming write (two-pass spill pipeline) to an in-memory buffer...
        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
            quad_stream(quads.clone()),
            &mut bytes,
            LayoutStrategy::Dictionary,
            dictionary_indexes(),
        )
        .await
        .unwrap();

        // ...then open it as a file-backed store (loads the dictionary via a
        // single-column projection scan).
        let dir = std::env::temp_dir().join(format!("vortex_rdf_dict_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dict.vortex");
        std::fs::write(&path, &bytes).unwrap();

        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary);
        assert_eq!(store.size().await.unwrap(), 10);

        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(quad_strings(&decoded), quad_strings(&quads));

        // Pushed-down integer filter on the code columns.
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let filtered = store.match_pattern(None, Some(&p0), None, None).await.unwrap();
        assert_eq!(filtered.size().await.unwrap(), 4);
        let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
        assert!(results.iter().all(|q| q.predicate == "http://example.org/p0"));

        // A term absent from the dictionary yields an always-false filter.
        let missing_p = NamedNode::new("http://example.org/nope").unwrap();
        let empty = store.match_pattern(None, Some(&missing_p), None, None).await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
