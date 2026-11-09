//! RDF/JS term values ⇄ `oxrdf` terms. Each quad position has one validated
//! constructor — [`subject`], [`predicate`], [`object`], [`graph`] — that
//! builds its term from a [`TermKind`], a value and an extra. Both quad
//! decoders build every term through these: the packed cursor in `ingest.rs`,
//! and the reflection reader here, which reads `termType`/`value`/`language`/
//! `datatype` off JS objects. So does the four-position match pattern, also
//! built here, which alone also reads a bare string as a NamedNode IRI.

use std::thread::LocalKey;

use js_sys::Reflect;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_rdf_core::Probe;
use wasm_bindgen::prelude::*;

use crate::error::js_err;

/// The kind of an RDF/JS term, numbered as `pack-quads.js` tags terms in the
/// packed quad buffer.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TermKind {
    NamedNode = 0,
    BlankNode = 1,
    /// A literal with neither a language nor a datatype IRI.
    SimpleLiteral = 2,
    /// A literal whose extra is its language tag.
    LangLiteral = 3,
    /// A literal whose extra is its datatype IRI.
    TypedLiteral = 4,
    DefaultGraph = 5,
}

impl TermKind {
    /// The kind a packed tag byte names, or `None` for a byte that names none.
    #[inline]
    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::NamedNode,
            1 => Self::BlankNode,
            2 => Self::SimpleLiteral,
            3 => Self::LangLiteral,
            4 => Self::TypedLiteral,
            5 => Self::DefaultGraph,
            _ => return None,
        })
    }

    /// Whether a term of this kind has a value: every kind but the default
    /// graph.
    #[inline]
    pub(crate) fn has_value(self) -> bool {
        self != Self::DefaultGraph
    }

    /// Whether a term of this kind has an extra: a language tag or a datatype
    /// IRI.
    #[inline]
    pub(crate) fn has_extra(self) -> bool {
        matches!(self, Self::LangLiteral | Self::TypedLiteral)
    }
}

// The validated constructors. Each takes the same `(kind, value, extra)`, with
// an empty value or extra where the kind has none, and returns `None` for a
// kind its position does not hold or for an IRI, blank-node label, language
// tag or datatype IRI that does not parse. `V` is `&str` for the packed
// cursor and `String` for the reflection reader, so each term's strings are
// allocated once either way.

/// A subject: a NamedNode or a BlankNode.
#[inline]
pub(crate) fn subject<V: Into<String>>(
    kind: TermKind,
    value: V,
    _extra: V,
) -> Option<NamedOrBlankNode> {
    match kind {
        TermKind::NamedNode => NamedNode::new(value).ok().map(NamedOrBlankNode::NamedNode),
        TermKind::BlankNode => BlankNode::new(value).ok().map(NamedOrBlankNode::BlankNode),
        _ => None,
    }
}

/// A predicate: a NamedNode.
#[inline]
pub(crate) fn predicate<V: Into<String>>(kind: TermKind, value: V, _extra: V) -> Option<NamedNode> {
    match kind {
        TermKind::NamedNode => NamedNode::new(value).ok(),
        _ => None,
    }
}

/// An object: a NamedNode, a BlankNode or a literal of any of the three
/// literal kinds.
#[inline]
pub(crate) fn object<V: Into<String>>(kind: TermKind, value: V, extra: V) -> Option<Term> {
    Some(match kind {
        TermKind::NamedNode => Term::NamedNode(NamedNode::new(value).ok()?),
        TermKind::BlankNode => Term::BlankNode(BlankNode::new(value).ok()?),
        TermKind::SimpleLiteral => Term::Literal(Literal::new_simple_literal(value)),
        TermKind::LangLiteral => {
            Term::Literal(Literal::new_language_tagged_literal(value, extra).ok()?)
        }
        TermKind::TypedLiteral => Term::Literal(Literal::new_typed_literal(
            value,
            NamedNode::new(extra).ok()?,
        )),
        TermKind::DefaultGraph => return None,
    })
}

/// A graph: a NamedNode, a BlankNode or the default graph.
#[inline]
pub(crate) fn graph<V: Into<String>>(kind: TermKind, value: V, _extra: V) -> Option<GraphName> {
    match kind {
        TermKind::NamedNode => NamedNode::new(value).ok().map(GraphName::NamedNode),
        TermKind::BlankNode => BlankNode::new(value).ok().map(GraphName::BlankNode),
        TermKind::DefaultGraph => Some(GraphName::DefaultGraph),
        _ => None,
    }
}

thread_local! {
    /// The RDF/JS property names as JS strings, allocated once per module
    /// instance; `Reflect::get` takes a `JsValue` key and building one from a
    /// `&str` allocates per call.
    static KEY_TERM_TYPE: JsValue = JsValue::from_str("termType");
    static KEY_VALUE: JsValue = JsValue::from_str("value");
    static KEY_LANGUAGE: JsValue = JsValue::from_str("language");
    static KEY_DATATYPE: JsValue = JsValue::from_str("datatype");
    static KEY_SUBJECT: JsValue = JsValue::from_str("subject");
    static KEY_PREDICATE: JsValue = JsValue::from_str("predicate");
    static KEY_OBJECT: JsValue = JsValue::from_str("object");
    static KEY_GRAPH: JsValue = JsValue::from_str("graph");
}

/// One property read through a cached key; `None` when `val` is not an
/// object.
fn get_prop(val: &JsValue, key: &'static LocalKey<JsValue>) -> Option<JsValue> {
    key.with(|k| Reflect::get(val, k).ok())
}

/// The value's `termType` property as a string, when it has one.
fn term_type_of(val: &JsValue) -> Option<String> {
    get_prop(val, &KEY_TERM_TYPE)?.as_string()
}

/// A term as the validated constructors take it: its kind, its value and its
/// extra (the language of a [`TermKind::LangLiteral`], the datatype IRI of a
/// [`TermKind::TypedLiteral`], empty otherwise).
type TermParts = (TermKind, String, String);

/// The parts of the default graph, which has no value and no extra.
const DEFAULT_GRAPH: TermParts = (TermKind::DefaultGraph, String::new(), String::new());

/// Read an RDF/JS term into the parts `pack-quads.js` packs it as: the kind
/// its tag names, its `value` and its extra. `null` or `undefined` reads as
/// the default graph, which only the graph position holds. `None` for a value
/// that is no RDF/JS term: not an object with a known `termType` and a string
/// `value` (a bare string included).
fn term_parts(val: &JsValue) -> Option<TermParts> {
    if val.is_null_or_undefined() {
        return Some(DEFAULT_GRAPH);
    }
    let kind = match term_type_of(val)?.as_str() {
        "NamedNode" => TermKind::NamedNode,
        "BlankNode" => TermKind::BlankNode,
        "Literal" => return literal_parts(val),
        "DefaultGraph" => return Some(DEFAULT_GRAPH),
        _ => return None,
    };
    Some((kind, get_prop(val, &KEY_VALUE)?.as_string()?, String::new()))
}

/// A literal's parts. As in `pack-quads.js`, it is language-tagged when its
/// `language` is truthy, else typed when its `datatype` is an object whose
/// `value` is truthy, else simple. A truthy `datatype` that is not an object,
/// such as a bare IRI string, makes it no term.
fn literal_parts(val: &JsValue) -> Option<TermParts> {
    let value = get_prop(val, &KEY_VALUE)?.as_string()?;
    if let Some(language) = literal_extra(get_prop(val, &KEY_LANGUAGE))? {
        return Some((TermKind::LangLiteral, value, language));
    }
    let datatype = get_prop(val, &KEY_DATATYPE)?;
    let iri = if datatype.is_object() {
        literal_extra(get_prop(&datatype, &KEY_VALUE))?
    } else if datatype.is_falsy() {
        None
    } else {
        return None;
    };
    Some(match iri {
        Some(iri) => (TermKind::TypedLiteral, value, iri),
        None => (TermKind::SimpleLiteral, value, String::new()),
    })
}

/// A literal's `language` or `datatype.value`, judged by JS truthiness:
/// `Some(None)` when it is absent or falsy, `Some(Some)` for a non-empty
/// string, and `None` (no term) for a truthy value that is not a string.
fn literal_extra(val: Option<JsValue>) -> Option<Option<String>> {
    let Some(val) = val else {
        return Some(None);
    };
    match val.as_string() {
        Some(s) => Some((!s.is_empty()).then_some(s)),
        None => val.is_falsy().then_some(None),
    }
}

/// One field of a quad object, built by its position's constructor.
fn quad_term<T>(
    quad: &JsValue,
    field: &'static LocalKey<JsValue>,
    build: impl FnOnce(TermKind, String, String) -> Option<T>,
) -> Option<T> {
    let (kind, value, extra) = term_parts(&get_prop(quad, field)?)?;
    build(kind, value, extra)
}

/// An RDF/JS quad object as an `oxrdf::Quad`, or `None` when any of its four
/// fields is not a valid term of its position. An absent (`null` or
/// `undefined`) graph is the default graph.
pub(crate) fn js_to_quad(val: &JsValue) -> Option<Quad> {
    Some(Quad::new(
        quad_term(val, &KEY_SUBJECT, subject)?,
        quad_term(val, &KEY_PREDICATE, predicate)?,
        quad_term(val, &KEY_OBJECT, object)?,
        quad_term(val, &KEY_GRAPH, graph)?,
    ))
}

/// The quad argument of `has`, `addQuad` and `deleteQuad`, or the
/// `Invalid quad object` error each reports for a malformed one.
pub(crate) fn quad_arg(val: &JsValue) -> Result<Quad, JsValue> {
    js_to_quad(val).ok_or_else(|| js_err("Invalid quad object"))
}

/// A four-position match pattern as a core [`Probe`] with no narrowing,
/// parsed from the JS arguments of `match`, `getQuads`, `countQuads`,
/// `matchCodes` and `matchCodeChunks`, or from a probe of `matchCodesMany`
/// and `countQuadsMany`. A slot is a wildcard when it is `null`/`undefined`
/// or an RDF/JS `Variable` term; any other value must parse as a term of the
/// position's kind (a bare string is a NamedNode IRI), or the whole pattern
/// is rejected with `Invalid {position} term`.
pub(crate) fn pattern_probe(
    s: JsValue,
    p: JsValue,
    o: JsValue,
    g: JsValue,
) -> Result<Probe, JsValue> {
    Ok(Probe::new(
        pattern_slot(s, "subject", subject)?,
        pattern_slot(p, "predicate", predicate)?,
        pattern_slot(o, "object", object)?,
        pattern_slot(g, "graph", graph)?,
    ))
}

/// One pattern slot: `Ok(None)` for a wildcard, `Ok(Some)` for a term of the
/// slot's kind, `Err` for a present value that is not one. A bare string is
/// a NamedNode IRI here, and nowhere else: a quad holds RDF/JS terms only.
fn pattern_slot<T>(
    val: JsValue,
    position: &str,
    build: impl FnOnce(TermKind, String, String) -> Option<T>,
) -> Result<Option<T>, JsValue> {
    if val.is_null_or_undefined() {
        return Ok(None);
    }
    let parts = match val.as_string() {
        Some(iri) => Some((TermKind::NamedNode, iri, String::new())),
        None => term_parts(&val),
    };
    match parts.and_then(|(kind, value, extra)| build(kind, value, extra)) {
        Some(term) => Ok(Some(term)),
        None if term_type_of(&val).as_deref() == Some("Variable") => Ok(None),
        None => Err(js_err(format!("Invalid {position} term"))),
    }
}
