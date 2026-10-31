//! The two quad shapes the store exchanges with builders and readers:
//! [`RawQuad`] (owned N-Triples strings, the builders' input) and
//! [`SharedQuad`] (`Arc<str>` terms, the shared-string read output).

use std::fmt::Write as _;
use std::sync::Arc;

use oxrdf::{Quad, TermRef};

use crate::common::terms::{Trust, at, graph_name, named_node, quad_from_terms, subject, term};
use crate::error::Result;

/// A quad whose terms are shared N-Triples strings: one `Arc<str>` per
/// distinct term of a decoded chunk, shared by reference count with every row
/// that repeats it. `g` is `""` for the default graph. Equal content is the
/// contract; pointer identity between equal terms is not guaranteed.
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

/// A chunk's rows written as N-Triples/N-Quads lines, `s p o [g] .` each with
/// every term spelled as the columns store it: the shape the
/// N-Triples/N-Quads export reads a chunk in, built straight from the
/// columns with no string per row.
pub(crate) struct NQuadLines {
    /// The lines, each ending in a newline.
    pub(crate) text: String,
    /// Whether some line names a graph, which N-Triples cannot hold.
    pub(crate) named_graph: bool,
}

impl NQuadLines {
    /// No lines yet, with room for `rows` lines of usual length.
    pub(crate) fn with_capacity(rows: usize) -> Self {
        Self {
            text: String::with_capacity(rows.saturating_mul(96)),
            named_graph: false,
        }
    }

    /// Append `term` (a subject, a predicate or an object) and the space
    /// after it.
    #[inline]
    pub(crate) fn term(&mut self, term: &str) {
        self.text.push_str(term);
        self.text.push(' ');
    }

    /// End the line: `graph` and a space unless it is the default graph's
    /// `""`, then `.` and a newline.
    #[inline]
    pub(crate) fn end(&mut self, graph: &str) {
        if !graph.is_empty() {
            self.named_graph = true;
            self.term(graph);
        }
        self.text.push_str(".\n");
    }

    /// Append one row's line: the export's per-row step, kept inline in
    /// its loop.
    #[inline]
    pub(crate) fn push(&mut self, [s, p, o, g]: [&str; 4]) {
        self.term(s);
        self.term(p);
        self.term(o);
        self.end(g);
    }
}

/// A quad holding its terms as owned N-Triples strings: the builders' input.
///
/// The strings are the canonical spelling of their terms, as
/// [`from_quad`](Self::from_quad), the parsers
/// ([`parse_quads_from_reader`](crate::common::terms::parse_quads_from_reader))
/// and [`canonical`](Self::canonical) render them: `xsd:string` typing
/// dropped, escapes resolved, language tags lower-cased.
///
/// **Build inputs must come from one of those three.** Builders intern the
/// spelling they are given and drop repeated quads by comparing it, without
/// parsing a term again, so another spelling of the same terms is a different
/// quad with its own dictionary entry. The fields are public for reading; to
/// write a quad from strings, use [`canonical`](Self::canonical).
///
/// The derived order compares `s`, `p`, `o` and `g` in turn, the store's
/// `(s, p, o, g)` sort order, so the fields keep that declaration order.
#[derive(
    Clone, Hash, PartialEq, Eq, PartialOrd, Ord, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
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

impl RawQuad {
    /// A quad from four term spellings, in the canonical spelling a build
    /// expects, so it interns and compares like one a parser produced.
    ///
    /// Each position takes the tolerant spellings of
    /// [`DictSnapshot::encode`](crate::DictSnapshot::encode) (an IRI with or
    /// without angle brackets, a blank node, a literal with escapes, an
    /// explicit `xsd:string` type or an upper-case language tag, the default
    /// graph as `""`, `default` or `[]`) and must hold a term its position can
    /// (no literal subject, predicate or graph, no blank predicate). `"x"`,
    /// `"x"^^<…#string>` and `"\u0078"` give the same quad, and `"y"@EN` gives
    /// `"y"@en`. A malformed spelling is an
    /// [`InvalidInput`](crate::VortexRdfError::InvalidInput) error naming the
    /// position.
    pub fn canonical(s: &str, p: &str, o: &str, g: &str) -> Result<Self> {
        // Each position is parsed as `Trust::Input`, as a pattern's slots
        // are, and the quad rendered by `from_quad`, so a term comes out
        // exactly as `canonical_spelling` renders it.
        Ok(RawQuad::from_quad(&Quad::new(
            at("quad subject", subject(s, Trust::Input))?,
            at("quad predicate", named_node(p, Trust::Input))?,
            at("quad object", term(o, Trust::Input))?,
            at("quad graph", graph_name(g, Trust::Input))?,
        )))
    }

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

/// Append `term`'s canonical spelling to `out`: the one
/// [`RawQuad::from_quad`] renders for it, `<iri>`, `_:id` or a literal's
/// Display form.
pub(crate) fn push_spelling(out: &mut String, term: TermRef<'_>) {
    match term {
        TermRef::NamedNode(n) => {
            out.push('<');
            out.push_str(n.as_str());
            out.push('>');
        }
        TermRef::BlankNode(b) => {
            out.push_str("_:");
            out.push_str(b.as_str());
        }
        TermRef::Literal(l) => write!(out, "{l}").expect("a String takes every write"),
    }
}

/// `<iri>` in one exact-capacity allocation (an IRI needs no escaping).
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

    /// A backslash, spelled so a source file carries no `\u` escape of its own.
    const BS: char = '\\';

    /// A quad's four terms as the plain tuple tests compare (`RawQuad` has no
    /// `Debug`).
    fn row(q: &RawQuad) -> (String, String, String, String) {
        (q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone())
    }

    fn canonical_o(o: &str) -> RawQuad {
        RawQuad::canonical("<http://ex.org/s>", "<http://ex.org/p>", o, "").unwrap()
    }

    /// Every spelling of an RDF term comes out as one: `xsd:string` typing is
    /// the plain literal, a `\u` escape is resolved, a language tag is
    /// lower-cased.
    #[test]
    fn canonical_gives_one_spelling_per_rdf_term() {
        let typed = "\"x\"^^<http://www.w3.org/2001/XMLSchema#string>";
        assert_eq!(canonical_o(typed).o, "\"x\"");
        assert_eq!(row(&canonical_o(typed)), row(&canonical_o("\"x\"")));

        let escaped = format!("\"{BS}u0078\"");
        assert_eq!(row(&canonical_o(&escaped)), row(&canonical_o("\"x\"")));

        assert_eq!(canonical_o("\"y\"@EN").o, "\"y\"@en");
        assert_eq!(row(&canonical_o("\"y\"@EN")), row(&canonical_o("\"y\"@en")));
        assert_eq!(row(&canonical_o("\"y\"@En")), row(&canonical_o("\"y\"@en")));

        // A different term stays different: an integer is not its lexical form.
        let integer = "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>";
        assert_eq!(canonical_o(integer).o, integer);
        assert_ne!(canonical_o(integer).o, canonical_o("\"1\"").o);
    }

    /// Each position takes the tolerant spellings a person types: IRIs with or
    /// without angle brackets, blank nodes, and the default graph as `""`,
    /// `default` or `[]`.
    #[test]
    fn canonical_accepts_the_spellings_of_each_position() {
        let want = (
            "<http://ex.org/s>".to_string(),
            "<http://ex.org/p>".to_string(),
            "<http://ex.org/o>".to_string(),
            "".to_string(),
        );
        for g in ["", "default", "DEFAULT", "[]"] {
            let q = RawQuad::canonical("http://ex.org/s", "http://ex.org/p", "http://ex.org/o", g)
                .unwrap();
            assert_eq!(row(&q), want, "graph {g:?}");
        }
        let blank = RawQuad::canonical("_:b0", "<http://ex.org/p>", "_:b1", "_:g").unwrap();
        assert_eq!(
            row(&blank),
            (
                "_:b0".to_string(),
                "<http://ex.org/p>".to_string(),
                "_:b1".to_string(),
                "_:g".to_string()
            )
        );
        let named = RawQuad::canonical(
            "<http://ex.org/s>",
            "<http://ex.org/p>",
            "\"o\"",
            "http://ex.org/g",
        )
        .unwrap();
        assert_eq!(named.g, "<http://ex.org/g>");
    }

    /// A malformed spelling, or a term in a position that cannot hold it, is
    /// refused rather than interned.
    #[test]
    fn canonical_rejects_what_is_no_quad() {
        let ok = ("<http://ex.org/s>", "<http://ex.org/p>", "\"o\"", "");
        let with = |s: &str, p: &str, o: &str, g: &str| RawQuad::canonical(s, p, o, g).is_err();
        assert!(!with(ok.0, ok.1, ok.2, ok.3));
        assert!(with("\"a literal\"", ok.1, ok.2, ok.3), "literal subject");
        assert!(with(ok.0, "_:b", ok.2, ok.3), "blank predicate");
        assert!(with(ok.0, "\"p\"", ok.2, ok.3), "literal predicate");
        assert!(with(ok.0, ok.1, ok.2, "\"g\""), "literal graph");
        assert!(
            with("<not an iri>", ok.1, ok.2, ok.3),
            "malformed subject IRI"
        );
        assert!(
            with(ok.0, ok.1, "\"unterminated", ok.3),
            "malformed literal"
        );
        assert!(
            with(ok.0, ok.1, "\"x\"@not_a_tag", ok.3),
            "bad language tag"
        );
        assert!(
            with(ok.0, ok.1, "\"x\"^^<not an iri>", ok.3),
            "bad datatype"
        );
        // The position is named ahead of the term parser's own message.
        for (quad, prefix) in [
            (
                ["<bad iri>", ok.1, ok.2, ok.3],
                "quad subject: invalid IRI \"bad iri\": ",
            ),
            (
                [ok.0, "_:b", ok.2, ok.3],
                "quad predicate: invalid IRI \"_:b\": ",
            ),
            (
                [ok.0, ok.1, "\"unterminated", ok.3],
                "quad object: malformed literal \"\\\"unterminated\"",
            ),
            (
                [ok.0, ok.1, ok.2, "_:a b"],
                "quad graph: invalid blank node \"_:a b\": ",
            ),
        ] {
            let [s, p, o, g] = quad;
            match RawQuad::canonical(s, p, o, g).err() {
                Some(crate::error::VortexRdfError::InvalidInput(message)) => {
                    assert!(message.starts_with(prefix), "{quad:?}: {message:?}")
                }
                other => panic!("{quad:?}: {other:?}"),
            }
        }
    }

    /// The constructor renders exactly what the other two sources of
    /// canonical quads do: the object as `canonical_spelling` renders it, the
    /// whole quad as `from_quad` renders the oxrdf quad.
    #[test]
    fn canonical_agrees_with_the_other_sources() {
        use crate::common::terms::canonical_spelling;
        for o in [
            "\"x\"",
            "\"x\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"y\"@EN",
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "http://ex.org/o",
            "_:b",
        ] {
            assert_eq!(canonical_o(o).o, canonical_spelling(o).unwrap(), "{o}");
        }
        let quad = oxrdf::Quad::new(
            oxrdf::NamedOrBlankNode::NamedNode(oxrdf::NamedNode::new("http://ex.org/s").unwrap()),
            oxrdf::NamedNode::new("http://ex.org/p").unwrap(),
            oxrdf::Term::Literal(oxrdf::Literal::new_language_tagged_literal("y", "EN").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        );
        assert_eq!(
            row(&canonical_o("\"y\"@EN")),
            row(&RawQuad::from_quad(&quad))
        );
    }

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
