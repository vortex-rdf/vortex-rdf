pub mod error;
pub mod io;
pub mod store;
pub mod utils;

pub use error::VortexRdfError;
pub use io::{
    deserialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer,
    serialize,
    vortex_to_quads,
    RdfFormat,
};
pub use store::dictionary::Dictionary;
pub use store::VortexRdfStore;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use oxrdf::{GraphName, Literal, NamedNode, Quad, Subject, Term};

    #[tokio::test]
    async fn test_roundtrip_quad() {
        let s = Subject::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let vortex_bytes = quads_stream_to_vortex(futures::stream::iter(quads.into_iter().map(Ok)))
            .await
            .expect("Serialization failed");
        let decoded_quads = vortex_to_quads(&vortex_bytes).await.expect("Deserialization failed");

        assert_eq!(1, decoded_quads.len());
        assert_eq!(
            quad.subject.to_string(),
            decoded_quads[0].subject.to_string()
        );
    }

    #[tokio::test]
    async fn test_match_pattern() {
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
        let vortex_bytes = quads_stream_to_vortex(futures::stream::iter(quads.into_iter().map(Ok)))
            .await
            .unwrap();
        let store = VortexRdfStore::from_bytes(&vortex_bytes).await.unwrap();

        // Match ?s <p1> ?o ?g
        let filtered = store.match_pattern(None, Some(&p1), None, None).await.unwrap();
        assert_eq!(filtered.size(), 1);

        let mut results = filtered.quads().unwrap();
        let res_q = results.next().await.unwrap().unwrap();
        assert_eq!(res_q.subject.to_string(), s1.to_string());

        // Match ?s <non-existent> ?o ?g
        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = store.match_pattern(None, Some(&p3), None, None).await.unwrap();
        assert_eq!(empty.size(), 0);
    }

    #[tokio::test]
    async fn test_add_delete_quad() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;
        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());

        let store = VortexRdfStore::empty().await.unwrap();
        assert_eq!(store.size(), 0);

        // Add quad
        let store = store.add_quad(q1.clone()).await.unwrap();
        assert_eq!(store.size(), 1);

        // Delete quad
        let store = store.delete_quad(&q1).await.unwrap();
        assert_eq!(store.size(), 0);
    }
}
