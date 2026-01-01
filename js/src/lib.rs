use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use std::io::Cursor;
use vortex_rdf_core::{deserialize, serialize, RdfFormat};
use wasm_bindgen::prelude::*;
use js_sys::{Object, Reflect};
use futures::StreamExt;

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"
import { Quad, Term, NamedNode, BlankNode, Literal, Quad_Subject, Quad_Predicate, Quad_Object, Quad_Graph } from '@rdfjs/types';

export interface VortexRdfStore {
    addQuad(quad: Quad): void;
    deleteQuad(quad: Quad): void;
    has(quad: Quad): boolean;
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<VortexRdfStore>;
    values(): Promise<IterableIterator<Quad>>;
}

export namespace VortexRdfStore {
    export function empty(): VortexRdfStore;
    export function fromBytes(bytes: Uint8Array): VortexRdfStore;
    export function fromString(input: string, format: string): VortexRdfStore;
}
"#;

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct VortexRdfStore {
    #[wasm_bindgen(skip)]
    pub(crate) inner: vortex_rdf_core::VortexRdfStore,
}

#[wasm_bindgen]
impl VortexRdfStore {
    #[wasm_bindgen(js_name = fromBytes)]
    pub async fn from_bytes(bytes: &[u8]) -> Result<VortexRdfStore, JsValue> {
        let inner = vortex_rdf_core::VortexRdfStore::from_bytes(bytes)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexRdfStore { inner })
    }

    pub fn empty() -> Result<VortexRdfStore, JsValue> {
        let inner = vortex_rdf_core::VortexRdfStore::empty()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexRdfStore { inner })
    }

    #[wasm_bindgen(js_name = fromString)]
    pub async fn from_string(input: String, format_name: &str) -> Result<VortexRdfStore, JsValue> {
        let format = match format_name.to_lowercase().as_str() {
            "nt" | "ntriples" => RdfFormat::NTriples,
            "nq" | "nquads" => RdfFormat::NQuads,
            "ttl" | "turtle" => RdfFormat::Turtle,
            "trig" => RdfFormat::TriG,
            "jsonld" => RdfFormat::JsonLd {
                profile: Default::default(),
            },
            _ => return Err(JsValue::from_str("Unsupported format")),
        };

        let mut bytes = Vec::new();
        // Vec<u8> implements VortexWrite
        serialize(Cursor::new(input), &mut bytes, format)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Self::from_bytes(&bytes).await
    }

    pub fn size(&self) -> usize {
        self.inner.size()
    }

    pub async fn has(&self, quad_js: JsValue) -> bool {
        if let Some(quad) = js_to_quad(quad_js) {
            self.inner
                .match_pattern(
                    Some(&quad.subject),
                    Some(&quad.predicate),
                    Some(&quad.object),
                    Some(&quad.graph_name),
                )
                .await
                .map(|ds| ds.size() > 0)
                .unwrap_or(false)
        } else {
            false
        }
    }

    #[wasm_bindgen(js_name = addQuad)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        if let Some(quad) = js_to_quad(quad_js) {
            self.inner = self.inner.add_quad(quad)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(())
        } else {
            Err(JsValue::from_str("Invalid quad object"))
        }
    }

    #[wasm_bindgen(js_name = deleteQuad)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        if let Some(quad) = js_to_quad(quad_js) {
            self.inner = self.inner.delete_quad(&quad)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(())
        } else {
            Err(JsValue::from_str("Invalid quad object"))
        }
    }

    #[wasm_bindgen(js_name = match)]
    pub async fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<VortexRdfStore, JsValue> {
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);

        let filtered = self
            .inner
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(VortexRdfStore { inner: filtered })
    }

    pub async fn values(&self) -> Result<js_sys::Iterator, JsValue> {
        let mut quads_stream = self.inner.quads().map_err(|e| JsValue::from_str(&e.to_string()))?;
        let js_array = js_sys::Array::new();
        while let Some(q_res) = quads_stream.next().await {
            if let Ok(q) = q_res {
                js_array.push(&quad_to_js(&q));
            }
        }
        Ok(js_array.values())
    }
}

fn term_to_js(term: &Term) -> JsValue {
    let obj = Object::new();
    match term {
        Term::NamedNode(n) => {
            Reflect::set(&obj, &"termType".into(), &"NamedNode".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &n.as_str().into()).unwrap();
        }
        Term::BlankNode(b) => {
            Reflect::set(&obj, &"termType".into(), &"BlankNode".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &b.as_str().into()).unwrap();
        }
        Term::Literal(l) => {
            Reflect::set(&obj, &"termType".into(), &"Literal".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &l.value().into()).unwrap();
            Reflect::set(&obj, &"datatype".into(), &term_to_js(&Term::NamedNode(l.datatype().into()))).unwrap();
            if let Some(lang) = l.language() {
                Reflect::set(&obj, &"language".into(), &lang.into()).unwrap();
            } else {
                Reflect::set(&obj, &"language".into(), &"".into()).unwrap();
            }
        }
    }
    
    // Add basic RDF-JS .equals() implementation
    let equals_script = "return other && this.termType === other.termType && this.value === other.value && this.language === (other.language || '') && (this.datatype ? this.datatype.equals(other.datatype) : !other.datatype)";
    let equals_fn = js_sys::Function::new_with_args("other", equals_script);
    Reflect::set(&obj, &"equals".into(), &equals_fn).unwrap();
    
    obj.into()
}

fn graph_name_to_js(graph: &GraphName) -> JsValue {
    let obj = Object::new();
    match graph {
        GraphName::DefaultGraph => {
            Reflect::set(&obj, &"termType".into(), &"DefaultGraph".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &"".into()).unwrap();
        }
        GraphName::NamedNode(n) => {
            Reflect::set(&obj, &"termType".into(), &"NamedNode".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &n.as_str().into()).unwrap();
        }
        GraphName::BlankNode(b) => {
            Reflect::set(&obj, &"termType".into(), &"BlankNode".into()).unwrap();
            Reflect::set(&obj, &"value".into(), &b.as_str().into()).unwrap();
        }
    }
    
    let equals_script = "return other && this.termType === other.termType && this.value === other.value";
    let equals_fn = js_sys::Function::new_with_args("other", equals_script);
    Reflect::set(&obj, &"equals".into(), &equals_fn).unwrap();
    
    obj.into()
}

fn quad_to_js(quad: &Quad) -> JsValue {
    let obj = Object::new();
    Reflect::set(&obj, &"subject".into(), &term_to_js(&quad.subject.clone().into())).unwrap();
    Reflect::set(&obj, &"predicate".into(), &term_to_js(&quad.predicate.clone().into())).unwrap();
    Reflect::set(&obj, &"object".into(), &term_to_js(&quad.object)).unwrap();
    Reflect::set(&obj, &"graph".into(), &graph_name_to_js(&quad.graph_name)).unwrap();
    
    let equals_script = "return other && this.subject.equals(other.subject) && this.predicate.equals(other.predicate) && this.object.equals(other.object) && this.graph.equals(other.graph)";
    let equals_fn = js_sys::Function::new_with_args("other", equals_script);
    Reflect::set(&obj, &"equals".into(), &equals_fn).unwrap();
    
    obj.into()
}

fn js_to_graph(val: JsValue) -> Option<GraphName> {
    if let Some(term) = js_to_term_raw(val) {
        match term.term_type.as_str() {
            "NamedNode" => Some(GraphName::NamedNode(NamedNode::new(term.value).ok()?)),
            "BlankNode" => Some(GraphName::BlankNode(oxrdf::BlankNode::new_unchecked(term.value))),
            "DefaultGraph" => Some(GraphName::DefaultGraph),
            _ => None
        }
    } else {
        None
    }
}

struct RawTerm {
    term_type: String,
    value: String,
    language: Option<String>,
    datatype_iri: Option<String>,
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

    let term_type = Reflect::get(&val, &"termType".into()).ok()?.as_string()?;
    let value = Reflect::get(&val, &"value".into()).ok()?.as_string()?;
    let language = Reflect::get(&val, &"language".into()).ok().and_then(|v| v.as_string());
    let datatype_iri = Reflect::get(&val, &"datatype".into()).ok()
        .and_then(|dt| Reflect::get(&dt, &"value".into()).ok())
        .and_then(|v| v.as_string());

    Some(RawTerm { term_type, value, language, datatype_iri })
}

fn js_to_term(val: JsValue) -> Option<Term> {
    let raw = js_to_term_raw(val)?;
    match raw.term_type.as_str() {
        "NamedNode" => NamedNode::new(raw.value).ok().map(Term::NamedNode),
        "BlankNode" => Some(Term::BlankNode(oxrdf::BlankNode::new_unchecked(raw.value))),
        "Literal" => {
            if let Some(l) = raw.language {
                if !l.is_empty() {
                    return oxrdf::Literal::new_language_tagged_literal(raw.value, l).ok().map(Term::Literal);
                }
            }
            if let Some(dt_iri) = raw.datatype_iri {
                if let Ok(dt_node) = NamedNode::new(dt_iri) {
                    return Some(Term::Literal(oxrdf::Literal::new_typed_literal(raw.value, dt_node)));
                }
            }
            Some(Term::Literal(oxrdf::Literal::new_simple_literal(raw.value)))
        }
        _ => None
    }
}

fn js_to_subject(val: JsValue) -> Option<Subject> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(Subject::NamedNode(n)),
        Term::BlankNode(b) => Some(Subject::BlankNode(b)),
        _ => None
    }
}

fn js_to_named_node(val: JsValue) -> Option<NamedNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(n),
        _ => None
    }
}

fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g = js_to_graph(Reflect::get(&val, &"graph".into()).ok()?).unwrap_or(GraphName::DefaultGraph);
    
    Some(Quad::new(s, p, o, g))
}

#[wasm_bindgen]
pub async fn nquads_to_vortex(nquads: String) -> Result<Vec<u8>, JsValue> {
    let mut buffer = Vec::new();
    serialize(Cursor::new(nquads), &mut buffer, RdfFormat::NQuads)
        .await
        .map_err(|e| JsValue::from_str(&format!("Vortex error: {}", e)))?;
    Ok(buffer)
}

#[wasm_bindgen]
pub async fn vortex_to_nquads(vortex_bytes: &[u8]) -> Result<String, JsValue> {
    let mut buffer = Vec::new();
    deserialize(vortex::buffer::Buffer::from(vortex_bytes.to_vec()), &mut buffer, RdfFormat::NQuads)
        .await
        .map_err(|e| JsValue::from_str(&format!("Vortex error: {}", e)))?;

    String::from_utf8(buffer).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
}
