use oxrdf::Quad;

/// A raw (un-encoded) quad holding term strings in N-Triples form.
/// This is the shared in-memory (and on-disk, for external sorting)
/// representation consumed by layouts, indexes and builders before
/// writing to Vortex arrays.
#[derive(
    Clone,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RawQuad {
    pub s: String,
    pub p: String,
    pub o: String,
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
                // Literals need escaping and datatype/language suffixes —
                // keep the canonical Display implementation for those.
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

/// `<iri>` built directly with one exact-capacity allocation. IRIs need no
/// escaping in N-Triples, so this skips the `Display`/`format!` machinery —
/// which profiling showed costing ~17% of serialization (formatter dispatch
/// plus incremental `String` reallocation) across `from_quad`'s callers.
fn named_node_string(n: &oxrdf::NamedNode) -> String {
    let iri = n.as_str();
    let mut s = String::with_capacity(iri.len() + 2);
    s.push('<');
    s.push_str(iri);
    s.push('>');
    s
}

/// `_:id`, same rationale as [`named_node_string`].
fn blank_node_string(b: &oxrdf::BlankNode) -> String {
    let id = b.as_str();
    let mut s = String::with_capacity(id.len() + 2);
    s.push_str("_:");
    s.push_str(id);
    s
}
