//! RDF/JS term values ⇄ `oxrdf` terms: reading `termType`/`value`/
//! `language`/`datatype` off JS objects (or bare IRI strings) into owned,
//! validated terms, and the four-position match pattern built from them.

use js_sys::Reflect;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_rdf_core::VortexRdfStore as CoreStore;
use wasm_bindgen::prelude::*;

use crate::error::js_err;

struct RawTerm {
    term_type: String,
    value: String,
    language: Option<String>,
    datatype_iri: Option<String>,
}

thread_local! {
    /// The RDF/JS property names as JS strings, allocated once per module
    /// instance; `Reflect::get` takes a `JsValue` key and building one from a
    /// `&str` allocates per call.
    static KEY_TERM_TYPE: JsValue = JsValue::from_str("termType");
    static KEY_VALUE: JsValue = JsValue::from_str("value");
    static KEY_LANGUAGE: JsValue = JsValue::from_str("language");
    static KEY_DATATYPE: JsValue = JsValue::from_str("datatype");
}

/// One property read through a cached key.
fn get_prop(val: &JsValue, key: &'static std::thread::LocalKey<JsValue>) -> Option<JsValue> {
    key.with(|k| Reflect::get(val, k).ok())
}

/// The value's `termType` property as a string, when it has one.
fn term_type_of(val: &JsValue) -> Option<String> {
    get_prop(val, &KEY_TERM_TYPE)?.as_string()
}

fn js_to_term_raw(val: JsValue) -> Option<RawTerm> {
    if val.is_null() || val.is_undefined() {
        return None;
    }
    if let Some(s) = val.as_string() {
        return Some(RawTerm {
            term_type: "NamedNode".into(),
            value: s,
            language: None,
            datatype_iri: None,
        });
    }
    let term_type = term_type_of(&val)?;
    let value = get_prop(&val, &KEY_VALUE)?.as_string()?;
    // Only literals carry a language or datatype; reading them off a
    // NamedNode/BlankNode costs three boundary crossings to learn nothing.
    let (language, datatype_iri) = if term_type == "Literal" {
        (
            get_prop(&val, &KEY_LANGUAGE).and_then(|v| v.as_string()),
            get_prop(&val, &KEY_DATATYPE)
                .and_then(|dt| get_prop(&dt, &KEY_VALUE))
                .and_then(|v| v.as_string()),
        )
    } else {
        (None, None)
    };
    Some(RawTerm {
        term_type,
        value,
        language,
        datatype_iri,
    })
}

pub(crate) fn js_to_term(val: JsValue) -> Option<Term> {
    let raw = js_to_term_raw(val)?;
    match raw.term_type.as_str() {
        "NamedNode" => NamedNode::new(raw.value).ok().map(Term::NamedNode),
        "BlankNode" => oxrdf::BlankNode::new(raw.value).ok().map(Term::BlankNode),
        "Literal" => {
            if let Some(l) = raw.language
                && !l.is_empty()
            {
                return oxrdf::Literal::new_language_tagged_literal(raw.value, l)
                    .ok()
                    .map(Term::Literal);
            }
            if let Some(dt_iri) = raw.datatype_iri
                && let Ok(dt_node) = NamedNode::new(dt_iri)
            {
                return Some(Term::Literal(oxrdf::Literal::new_typed_literal(
                    raw.value, dt_node,
                )));
            }
            Some(Term::Literal(oxrdf::Literal::new_simple_literal(raw.value)))
        }
        _ => None,
    }
}

pub(crate) fn js_to_subject(val: JsValue) -> Option<NamedOrBlankNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b)),
        _ => None,
    }
}

pub(crate) fn js_to_named_node(val: JsValue) -> Option<NamedNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(n),
        _ => None,
    }
}

/// A graph term: a NamedNode or BlankNode through [`js_to_term`], or a
/// `DefaultGraph` term; anything else (including null/undefined) is `None`.
pub(crate) fn js_to_graph(val: JsValue) -> Option<GraphName> {
    match js_to_term(val.clone()) {
        Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
        Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
        Some(_) => None,
        None => (term_type_of(&val)? == "DefaultGraph").then_some(GraphName::DefaultGraph),
    }
}

/// An RDF/JS quad object as an `oxrdf::Quad`, or `None` when any of its four
/// term fields is missing or invalid. An absent (`null`/`undefined`) graph
/// field means the default graph; a present one must be a valid graph term.
pub(crate) fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g = Reflect::get(&val, &"graph".into()).ok()?;
    let g = if g.is_null() || g.is_undefined() {
        GraphName::DefaultGraph
    } else {
        js_to_graph(g)?
    };
    Some(Quad::new(s, p, o, g))
}

/// A four-position match pattern parsed from the JS arguments of `match`,
/// `getQuads`, `countQuads` and `matchArrowFFI`: `None` is a wildcard.
pub(crate) struct JsPattern {
    pub s: Option<NamedOrBlankNode>,
    pub p: Option<NamedNode>,
    pub o: Option<Term>,
    pub g: Option<GraphName>,
}

impl JsPattern {
    /// Parse the four pattern slots. A slot is a wildcard when it is
    /// `null`/`undefined` or an RDF/JS `Variable` term; any other value must
    /// parse as a term of the position's kind (a bare string is a NamedNode
    /// IRI), or the whole pattern is rejected with `Invalid {position} term`.
    pub(crate) fn parse(
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<Self, JsValue> {
        Ok(Self {
            s: pattern_slot(subject, "subject", js_to_subject)?,
            p: pattern_slot(predicate, "predicate", js_to_named_node)?,
            o: pattern_slot(object, "object", js_to_term)?,
            g: pattern_slot(graph, "graph", js_to_graph)?,
        })
    }

    /// The view of `store` matching this pattern, with the error shaped for JS.
    pub(crate) async fn matched(&self, store: &CoreStore) -> Result<CoreStore, JsValue> {
        store
            .match_pattern(
                self.s.as_ref(),
                self.p.as_ref(),
                self.o.as_ref(),
                self.g.as_ref(),
            )
            .await
            .map_err(js_err)
    }
}

/// One pattern slot: `Ok(None)` for a wildcard, `Ok(Some)` for a term of the
/// slot's kind, `Err` for a present value that is not one.
fn pattern_slot<T>(
    val: JsValue,
    position: &str,
    parse: fn(JsValue) -> Option<T>,
) -> Result<Option<T>, JsValue> {
    if val.is_null() || val.is_undefined() {
        return Ok(None);
    }
    if val.is_object() && term_type_of(&val).as_deref() == Some("Variable") {
        return Ok(None);
    }
    parse(val)
        .map(Some)
        .ok_or_else(|| js_err(format!("Invalid {position} term")))
}
