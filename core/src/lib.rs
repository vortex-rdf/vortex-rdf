pub mod common;
pub mod error;
pub mod index;
pub mod io;
pub mod store;

pub use error::VortexRdfError;
pub use io::{
    deserialize,
    array_from_ipc_reader,
    serialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer
};
#[cfg(feature = "file-io")]
pub use io::load_vortex_file_ref;
pub use io::{
    array_from_reader, deserialize, quads_stream_to_vortex, quads_stream_to_vortex_writer,
    serialize,
};

pub use store::{
    VortexRdfStore, 
    VortexArrayBuilder, 
    UnsortedInMemoryBuilder, 
    SortedInMemoryBuilder, 
    ChunkSortBuilder, 
    GlobalSortBuilder,
    BuilderStrategy
};

pub use index::{ChainedHash, RdfDictionary, SimpleDictionary};

use mimalloc::MiMalloc;
/*
 As indicated by vortex docs:
 https://docs.rs/vortex/latest/vortex/index.html#performance-optimization
*/
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{TryStreamExt, stream};
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
    use store::layout::flat::FlatLayout;

    #[tokio::test]

    async fn test_roundtrip_dict_index() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let dict_index = VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_index(
            stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))),
        )
        .await
        .expect("Serialization failed");
        let dict_store = VortexRdfStore::<SimpleDictionary, FlatLayout>::new(dict_index).unwrap();

        let decoded_quads: Vec<Quad> = dict_store.quads().unwrap().try_collect().await.unwrap();

        assert_eq!(1, decoded_quads.len());
        assert_eq!(
            quad.subject.to_string(),
            decoded_quads[0].subject.to_string()
        );
        assert_eq!(
            quad.predicate.to_string(),
            decoded_quads[0].predicate.to_string()
        );
        assert_eq!(quad.object.to_string(), decoded_quads[0].object.to_string());
        assert_eq!(
            quad.graph_name.to_string(),
            decoded_quads[0].graph_name.to_string()
        );
    }

    #[tokio::test]
    async fn test_roundtrip_chained_hash_index() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let chained_hash = VortexRdfStore::<ChainedHash, FlatLayout>::build_vortex_index(
            stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))),
        )
        .await
        .expect("Serialization failed");
        let chained_hash_store =
            VortexRdfStore::<ChainedHash, FlatLayout>::new(chained_hash).unwrap();

        let decoded_quads: Vec<Quad> = chained_hash_store
            .quads()
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(1, decoded_quads.len());
        assert_eq!(
            quad.subject.to_string(),
            decoded_quads[0].subject.to_string()
        );
        assert_eq!(
            quad.predicate.to_string(),
            decoded_quads[0].predicate.to_string()
        );
        assert_eq!(quad.object.to_string(), decoded_quads[0].object.to_string());
        assert_eq!(
            quad.graph_name.to_string(),
            decoded_quads[0].graph_name.to_string()
        );
    }

    async fn run_match_pattern_test<Dict: RdfDictionary + 'static, B: VortexArrayBuilder<Dict>>() {
        let s1 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;

        let s2 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s2").unwrap());
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let g2 = GraphName::DefaultGraph;

        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());
        let q2 = Quad::new(s2.clone(), p2.clone(), o2.clone(), g2.clone());

        let quads = vec![q1.clone(), q2.clone()];

        let arr = VortexRdfStore::<Dict>::build_vortex_array_with_builder::<B>(
            stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q)))
        )
        .await
        .expect("Serialization failed");
        let store = VortexRdfStore::<Dict>::new(arr).unwrap();

        // Match ?s <p1> ?o ?g
        let filtered = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size(), 1);

        let results: Vec<Quad> = filtered.quads()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(filtered.size(), 1);

        let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
        let res_q = results.first().unwrap();

        assert_eq!(res_q.subject.to_string(), s1.to_string());
        assert_eq!(res_q.predicate.to_string(), p1.to_string());
        assert_eq!(res_q.object.to_string(), o1.to_string());
        assert_eq!(res_q.graph_name.to_string(), g1.to_string());

        // Match ?s <non-existent> ?o ?g
        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size(), 0);
    }

    #[tokio::test]
    async fn test_match_unsorted_in_memory_simple_dict() {
        run_match_pattern_test::<SimpleDictionary, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_unsorted_in_memory_chained_hash() {
        run_match_pattern_test::<ChainedHash, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_sorted_in_memory_simple_dict() {
        run_match_pattern_test::<SimpleDictionary, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_sorted_in_memory_chained_hash() {
        run_match_pattern_test::<ChainedHash, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_chunk_sort_simple_dict() {
        run_match_pattern_test::<SimpleDictionary, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_chunk_sort_chained_hash() {
        run_match_pattern_test::<ChainedHash, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_global_sort_simple_dict() {
        run_match_pattern_test::<SimpleDictionary, GlobalSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_match_global_sort_chained_hash() {
        run_match_pattern_test::<ChainedHash, GlobalSortBuilder>().await;
    }

    async fn run_add_delete_quad_test<Dict: RdfDictionary + 'static, B: VortexArrayBuilder<Dict>>() {
        let s1 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;
        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());

        // Build a store with one initial quad using builder B
        let arr = VortexRdfStore::<Dict>::build_vortex_array_with_builder::<B>(
            stream::iter(vec![Ok::<_, VortexRdfError>(q1.clone())])
        )
        .await
        .expect("Serialization failed");
        let store = VortexRdfStore::<Dict>::new(arr).unwrap();
        assert_eq!(store.size(), 1);

        // Add a new quad
        let s2 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s2").unwrap());
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let g2 = GraphName::DefaultGraph;
        let q2 = Quad::new(s2, p2, o2, g2);

        let store = store.add_quad(q2.clone()).await.unwrap();
        assert_eq!(store.size(), 2);

        // Delete the added quad
        let store = store.delete_quad(&q2).await.unwrap();
        assert_eq!(store.size(), 1);

        // Delete the initial quad
        let store = store.delete_quad(&q1).await.unwrap();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_add_delete_unsorted_in_memory_simple_dict() {
        run_add_delete_quad_test::<SimpleDictionary, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_unsorted_in_memory_chained_hash() {
        run_add_delete_quad_test::<ChainedHash, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_sorted_in_memory_simple_dict() {
        run_add_delete_quad_test::<SimpleDictionary, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_sorted_in_memory_chained_hash() {
        run_add_delete_quad_test::<ChainedHash, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_chunk_sort_simple_dict() {
        run_add_delete_quad_test::<SimpleDictionary, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_chunk_sort_chained_hash() {
        run_add_delete_quad_test::<ChainedHash, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_global_sort_simple_dict() {
        run_add_delete_quad_test::<SimpleDictionary, GlobalSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_add_delete_global_sort_chained_hash() {
        run_add_delete_quad_test::<ChainedHash, GlobalSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_multiple_append_dict_index() {
        let mut store = VortexRdfStore::<SimpleDictionary, FlatLayout>::empty().unwrap();

        for i in 0..10 {
            let s = NamedOrBlankNode::NamedNode(
                NamedNode::new(format!("http://example.org/s{}", i)).unwrap(),
            );
            let p = NamedNode::new("http://example.org/p").unwrap();
            let o = Term::Literal(Literal::new_simple_literal("o"));
            let g = GraphName::DefaultGraph;
            let q = Quad::new(s, p, o, g);
            store = store.add_quad(q).await.unwrap();
        }

        assert_eq!(store.size(), 10);

        // Match p
        let p = NamedNode::new("http://example.org/p").unwrap();
        let matched = store
            .match_pattern(None, Some(&p), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size(), 10);

        // Match specific subject s5
        let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
        let matched_s5 = store
            .match_pattern(Some(&s5), None, None, None)
            .await
            .unwrap();
        assert_eq!(matched_s5.size(), 1);
    }

    #[tokio::test]
    async fn test_multiple_append_chained_hash_index() {
        let mut store = VortexRdfStore::<ChainedHash, FlatLayout>::empty().unwrap();

        for i in 0..10 {
            let s = NamedOrBlankNode::NamedNode(
                NamedNode::new(format!("http://example.org/s{}", i)).unwrap(),
            );
            let p = NamedNode::new("http://example.org/p").unwrap();
            let o = Term::Literal(Literal::new_simple_literal("o"));
            let g = GraphName::DefaultGraph;
            let q = Quad::new(s, p, o, g);
            store = store.add_quad(q).await.unwrap();
        }

        assert_eq!(store.size(), 10);

        // Match p
        let p = NamedNode::new("http://example.org/p").unwrap();
        let matched = store
            .match_pattern(None, Some(&p), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size(), 10);

        // Match specific subject s5
        let s5 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
        let matched_s5 = store
            .match_pattern(Some(&s5), None, None, None)
            .await
            .unwrap();
        assert_eq!(matched_s5.size(), 1);
    }

    async fn run_builder_roundtrip_test<Dict: RdfDictionary + 'static, B: VortexArrayBuilder<Dict>>() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let arr = VortexRdfStore::<Dict>::build_vortex_array_with_builder::<B>(
            stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q)))
        )
        .await
        .expect("Serialization failed");

        let store = VortexRdfStore::<Dict>::new(arr).unwrap();
        let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
        assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
        assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
        assert_eq!(decoded[0].graph_name.to_string(), quad.graph_name.to_string());
    }

    #[tokio::test]
    async fn test_unsorted_in_memory_simple_dict() {
        run_builder_roundtrip_test::<SimpleDictionary, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_unsorted_in_memory_chained_hash() {
        run_builder_roundtrip_test::<ChainedHash, UnsortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_sorted_in_memory_simple_dict() {
        run_builder_roundtrip_test::<SimpleDictionary, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_sorted_in_memory_chained_hash() {
        run_builder_roundtrip_test::<ChainedHash, SortedInMemoryBuilder>().await;
    }

    #[tokio::test]
    async fn test_chunk_sort_simple_dict() {
        run_builder_roundtrip_test::<SimpleDictionary, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_chunk_sort_chained_hash() {
        run_builder_roundtrip_test::<ChainedHash, ChunkSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_global_sort_simple_dict() {
        run_builder_roundtrip_test::<SimpleDictionary, GlobalSortBuilder>().await;
    }

    #[tokio::test]
    async fn test_global_sort_chained_hash() {
        run_builder_roundtrip_test::<ChainedHash, GlobalSortBuilder>().await;
    }
}
