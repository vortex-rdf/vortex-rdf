pub mod error;
pub mod io;
pub mod store;
pub mod common;
pub mod index;

pub use error::VortexRdfError;
pub use io::{
    deserialize,
    array_from_reader,
    serialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer
};
#[cfg(feature = "file-io")]
pub use io::load_vortex_file_ref;

pub use store::VortexRdfStore;

pub use index::{
    RdfDictionary,
    SimpleDictionary,
    ChainedHash,
};

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
    use oxrdf::{GraphName, Literal, NamedNode, Quad, Subject, Term};

    #[tokio::test]

    async fn test_roundtrip_dict_index() {
        let s = Subject::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let dict_index = VortexRdfStore::<SimpleDictionary>::build_vortex_index(stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))))
            .await
            .expect("Serialization failed");
        let dict_store = VortexRdfStore::<SimpleDictionary>::new(dict_index).unwrap();
        
        let decoded_quads: Vec<Quad> = dict_store.quads()
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
        assert_eq!(
            quad.object.to_string(),
            decoded_quads[0].object.to_string()
        );
        assert_eq!(
            quad.graph_name.to_string(),
            decoded_quads[0].graph_name.to_string()
        );
    }

    #[tokio::test]
    async fn test_roundtrip_chained_hash_index() {
        let s = Subject::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let chained_hash = VortexRdfStore::<ChainedHash>::build_vortex_index(stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))))
            .await
            .expect("Serialization failed");
        let chained_hash_store = VortexRdfStore::<ChainedHash>::new(chained_hash).unwrap();
        
        let decoded_quads: Vec<Quad> = chained_hash_store.quads()
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
        assert_eq!(
            quad.object.to_string(),
            decoded_quads[0].object.to_string()
        );
        assert_eq!(
            quad.graph_name.to_string(),
            decoded_quads[0].graph_name.to_string()
        );
    }

    #[tokio::test]
    async fn test_match_pattern_dict_index() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;

        let s2 = Subject::NamedNode(NamedNode::new("http://example.org/s2").unwrap());
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let g2 = GraphName::DefaultGraph;

        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());
        let q2 = Quad::new(s2.clone(), p2.clone(), o2.clone(), g2.clone());

        let quads = vec![q1.clone(), q2.clone()];

        let dict_index = VortexRdfStore::<SimpleDictionary>::build_vortex_index(stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))))
            .await
            .expect("Serialization failed");
        let dict_store = VortexRdfStore::<SimpleDictionary>::new(dict_index).unwrap();

        // Match ?s <p1> ?o ?g
        let filtered = dict_store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size(), 1);

        let results: Vec<Quad> = filtered.quads()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let res_q = results.first().unwrap();

        assert_eq!(res_q.subject.to_string(), s1.to_string());
        assert_eq!(res_q.predicate.to_string(), p1.to_string());
        assert_eq!(res_q.object.to_string(), o1.to_string());
        assert_eq!(res_q.graph_name.to_string(), g1.to_string());

        // Match ?s <non-existent> ?o ?g
        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = dict_store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size(), 0);
    }

    #[tokio::test]
    async fn test_match_pattern_chained_hash_index() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;

        let s2 = Subject::NamedNode(NamedNode::new("http://example.org/s2").unwrap());
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let g2 = GraphName::DefaultGraph;

        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());
        let q2 = Quad::new(s2.clone(), p2.clone(), o2.clone(), g2.clone());

        let quads = vec![q1.clone(), q2.clone()];

        let chained_hash = VortexRdfStore::<ChainedHash>::build_vortex_index(stream::iter(quads.into_iter().map(|q| Ok::<_, VortexRdfError>(q))))
            .await
            .expect("Serialization failed");
        let chained_hash_store = VortexRdfStore::<ChainedHash>::new(chained_hash).unwrap();

        // Match ?s <p1> ?o ?g
        let filtered = chained_hash_store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size(), 1);

        let results: Vec<Quad> = filtered.quads()
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let res_q = results.first().unwrap();

        assert_eq!(res_q.subject.to_string(), s1.to_string());
        assert_eq!(res_q.predicate.to_string(), p1.to_string());
        assert_eq!(res_q.object.to_string(), o1.to_string());
        assert_eq!(res_q.graph_name.to_string(), g1.to_string());

        // Match ?s <non-existent> ?o ?g
        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = chained_hash_store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size(), 0);
    }

    #[tokio::test]
    async fn test_add_delete_quad_dict_index() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;
        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());

        let store = VortexRdfStore::<SimpleDictionary>::empty();
        assert_eq!(store.size(), 0);

        // Add quad
        let store = store.add_quad(q1.clone()).await.unwrap();
        assert_eq!(store.size(), 1);

        // Delete quad
        let store = store.delete_quad(&q1).await.unwrap();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_add_delete_quad_chained_hash_index() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;
        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());

        let store = VortexRdfStore::<ChainedHash>::empty();
        assert_eq!(store.size(), 0);

        // Add quad
        let store = store.add_quad(q1.clone()).await.unwrap();
        assert_eq!(store.size(), 1);

        // Delete quad
        let store = store.delete_quad(&q1).await.unwrap();
        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_multiple_append_dict_index() {
        let mut store = VortexRdfStore::<SimpleDictionary>::empty();
        
        for i in 0..10 {
            let s = Subject::NamedNode(NamedNode::new(format!("http://example.org/s{}", i)).unwrap());
            let p = NamedNode::new("http://example.org/p").unwrap();
            let o = Term::Literal(Literal::new_simple_literal("o"));
            let g = GraphName::DefaultGraph;
            let q = Quad::new(s, p, o, g);
            store = store.add_quad(q).await.unwrap();
        }
        
        assert_eq!(store.size(), 10);
        
        // Match p
        let p = NamedNode::new("http://example.org/p").unwrap();
        let matched = store.match_pattern(None, Some(&p), None, None).await.unwrap();
        assert_eq!(matched.size(), 10);
        
        // Match specific subject s5
        let s5 = Subject::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
        let matched_s5 = store.match_pattern(Some(&s5), None, None, None).await.unwrap();
        assert_eq!(matched_s5.size(), 1);
    }

    #[tokio::test]
    async fn test_multiple_append_chained_hash_index() {
        let mut store = VortexRdfStore::<ChainedHash>::empty();
        
        for i in 0..10 {
            let s = Subject::NamedNode(NamedNode::new(format!("http://example.org/s{}", i)).unwrap());
            let p = NamedNode::new("http://example.org/p").unwrap();
            let o = Term::Literal(Literal::new_simple_literal("o"));
            let g = GraphName::DefaultGraph;
            let q = Quad::new(s, p, o, g);
            store = store.add_quad(q).await.unwrap();
        }
        
        assert_eq!(store.size(), 10);
        
        // Match p
        let p = NamedNode::new("http://example.org/p").unwrap();
        let matched = store.match_pattern(None, Some(&p), None, None).await.unwrap();
        assert_eq!(matched.size(), 10);
        
        // Match specific subject s5
        let s5 = Subject::NamedNode(NamedNode::new("http://example.org/s5").unwrap());
        let matched_s5 = store.match_pattern(Some(&s5), None, None, None).await.unwrap();
        assert_eq!(matched_s5.size(), 1);
    }
}
