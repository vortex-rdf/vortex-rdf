//! The two quad shapes the store exchanges with builders and readers:
//! [`RawQuad`] (owned N-Triples strings, the builders' input) and
//! [`SharedQuad`] (`Arc<str>` terms, the shared-string read output).

use std::sync::Arc;

use oxrdf::Quad;

use crate::common::terms::quad_from_terms;
use crate::error::Result;

/// A quad whose terms are shared N-Triples strings, one `Arc<str>` per
/// distinct term of a decoded chunk; `g` is `""` for the default graph.
/// Equal content is the contract; pointer identity between equal terms is
/// not guaranteed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SharedQuad {
    /// Subject term.
    pub s: Arc<str>,
    /// Predicate term.
    pub p: Arc<str>,
    /// Object term.
    pub o: Arc<str>,
    /// Graph term; `""` for the default graph.
    pub g: Arc<str>,
}

impl SharedQuad {
    /// Parse the four terms into an owned oxrdf [`Quad`].
    pub fn to_quad(&self) -> Result<Quad> {
        quad_from_terms(&self.s, &self.p, &self.o, &self.g)
    }
}

impl From<RawQuad> for SharedQuad {
    fn from(raw: RawQuad) -> Self {
        Self {
            s: Arc::from(raw.s),
            p: Arc::from(raw.p),
            o: Arc::from(raw.o),
            g: Arc::from(raw.g),
        }
    }
}

/// A quad holding its terms as owned N-Triples strings: the builders' input
/// and the spill-file record. `Ord` is `(s, p, o, g)`; `g` is `""` for the
/// default graph.
#[derive(Clone, Hash, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RawQuad {
    /// Subject term.
    pub s: String,
    /// Predicate term.
    pub p: String,
    /// Object term.
    pub o: String,
    /// Graph term; `""` for the default graph.
    pub g: String,
}

impl Ord for RawQuad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.s
            .cmp(&other.s)
            .then_with(|| self.p.cmp(&other.p))
            .then_with(|| self.o.cmp(&other.o))
            .then_with(|| self.g.cmp(&other.g))
    }
}

impl PartialOrd for RawQuad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RawQuad {
    /// Render an oxrdf [`Quad`]'s four terms in N-Triples form.
    pub fn from_quad(q: &Quad) -> Self {
        RawQuad {
            s: match &q.subject {
                oxrdf::NamedOrBlankNode::NamedNode(n) => named_node_string(n),
                oxrdf::NamedOrBlankNode::BlankNode(b) => blank_node_string(b),
            },
            p: named_node_string(&q.predicate),
            o: match &q.object {
                oxrdf::Term::NamedNode(n) => named_node_string(n),
                oxrdf::Term::BlankNode(b) => blank_node_string(b),
                // Literals need escaping and suffixes: the Display form.
                other => other.to_string(),
            },
            g: match &q.graph_name {
                oxrdf::GraphName::DefaultGraph => String::new(),
                oxrdf::GraphName::NamedNode(n) => named_node_string(n),
                oxrdf::GraphName::BlankNode(b) => blank_node_string(b),
            },
        }
    }
}

/// `<iri>` in one exact-capacity allocation.
fn named_node_string(n: &oxrdf::NamedNode) -> String {
    let iri = n.as_str();
    let mut s = String::with_capacity(iri.len() + 2);
    s.push('<');
    s.push_str(iri);
    s.push('>');
    s
}

/// `_:id` in one exact-capacity allocation.
fn blank_node_string(b: &oxrdf::BlankNode) -> String {
    let id = b.as_str();
    let mut s = String::with_capacity(id.len() + 2);
    s.push_str("_:");
    s.push_str(id);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_quad_to_quad_rejects_an_object_in_no_term_form() {
        let quad = SharedQuad {
            s: "<http://example.org/s>".into(),
            p: "<http://example.org/p>".into(),
            o: "not a term".into(),
            g: "".into(),
        };
        assert!(quad.to_quad().is_err());
    }
}
