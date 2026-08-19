//! RDF/JS term values ⇄ `oxrdf` terms: reading `termType`/`value`/
//! `language`/`datatype` off JS objects (or bare IRI strings) into owned,
//! validated terms.

use js_sys::Reflect;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use wasm_bindgen::prelude::*;

struct RawTerm {
    term_type: String,
    value: String,
    language: Option<String>,
    datatype_iri: Option<String>,
}

thread_local! {
    /// The RDF/JS property names, allocated as JS strings once per module
    /// instance. `Reflect::get` takes a `JsValue` key, and building one from a
    /// `&str` allocates a JS string on every call — four per term parsed,
    /// which is why the pattern arguments cost more to read than to match.
    static KEY_TERM_TYPE: JsValue = JsValue::from_str("termType");
    static KEY_VALUE: JsValue = JsValue::from_str("value");
    static KEY_LANGUAGE: JsValue = JsValue::from_str("language");
    static KEY_DATATYPE: JsValue = JsValue::from_str("datatype");
}

/// One property read through a cached key.
fn get_prop(val: &JsValue, key: &'static std::thread::LocalKey<JsValue>) -> Option<JsValue> {
    key.with(|k| Reflect::get(val, k).ok())
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
    let term_type = get_prop(&val, &KEY_TERM_TYPE)?.as_string()?;
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
        "BlankNode" => Some(Term::BlankNode(oxrdf::BlankNode::new_unchecked(raw.value))),
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

pub(crate) fn js_to_graph(val: JsValue) -> Option<GraphName> {
    if let Some(term) = js_to_term_raw(val) {
        match term.term_type.as_str() {
            "NamedNode" => Some(GraphName::NamedNode(NamedNode::new(term.value).ok()?)),
            "BlankNode" => Some(GraphName::BlankNode(oxrdf::BlankNode::new_unchecked(
                term.value,
            ))),
            "DefaultGraph" => Some(GraphName::DefaultGraph),
            _ => None,
        }
    } else {
        None
    }
}

pub(crate) fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g =
        js_to_graph(Reflect::get(&val, &"graph".into()).ok()?).unwrap_or(GraphName::DefaultGraph);
    Some(Quad::new(s, p, o, g))
}
