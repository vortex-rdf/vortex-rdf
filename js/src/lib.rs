use oxrdf::{GraphName, NamedNode, Quad, NamedOrBlankNode, Term};
use oxrdfio::RdfFormat;
use std::io::Cursor;
use vortex_rdf_core::io::{
    deserialize,
    write_array_to_ipc,
    array_from_ipc_reader,
};
use vortex_rdf_core::VortexRdfStore;
use vortex_rdf_core::index::{SimpleDictionary, ChainedHash};
use vortex_rdf_core::common::indexes::{IndexType, detect_index_type};
use vortex_rdf_core::store::builders::{
    BuilderStrategy,
    UnsortedInMemoryBuilder,
    SortedInMemoryBuilder,
    SortedStreamBuilder,
    UnsortedStreamBuilder,
};
use vortex_rdf_core::common::utils::parse_quads_from_reader;
use wasm_bindgen::prelude::*;
use js_sys::{Object, Reflect};
use futures::StreamExt;



#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"
import { Quad, Term, NamedNode, BlankNode, Literal, Quad_Subject, Quad_Predicate, Quad_Object, Quad_Graph } from '@rdfjs/types';

export type BuilderStrategy = 'UnsortedInMemory' | 'SortedInMemory' | 'SortedStream' | 'UnsortedStream';

export interface VortexStore {
    addQuad(quad: Quad): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    has(quad: Quad): Promise<boolean>;
    size(): number;
    values(): Promise<IterableIterator<Quad>>;
}

export class SimpleDictionaryStore implements VortexStore {
    static empty(): SimpleDictionaryStore;
    static fromBytes(bytes: Uint8Array): Promise<SimpleDictionaryStore>;
    static fromString(input: string, format: string, builderStrategy?: BuilderStrategy): Promise<SimpleDictionaryStore>;
    
    addQuad(quad: Quad): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    has(quad: Quad): Promise<boolean>;
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<SimpleDictionaryStore>;
    size(): number;
    values(): Promise<IterableIterator<Quad>>;
}

export class ChainedHashStore implements VortexStore {
    static empty(): ChainedHashStore;
    static fromBytes(bytes: Uint8Array): Promise<ChainedHashStore>;
    static fromString(input: string, format: string, builderStrategy?: BuilderStrategy): Promise<ChainedHashStore>;

    addQuad(quad: Quad): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    has(quad: Quad): Promise<boolean>;
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<ChainedHashStore>;
    size(): number;
    values(): Promise<IterableIterator<Quad>>;
}

export function nquads_to_vortex(nquads: string, builderStrategy?: BuilderStrategy): Promise<Uint8Array>;
"#;

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

// -------------------------
// Dictionary Store Bindings
// -------------------------

#[wasm_bindgen]
pub struct SimpleDictionaryStore {
    #[wasm_bindgen(skip)]
    pub inner: VortexRdfStore<SimpleDictionary>,
}

#[wasm_bindgen]
impl SimpleDictionaryStore {
    #[wasm_bindgen(js_name = fromBytes)]
    pub async fn from_bytes(bytes: &[u8]) -> Result<SimpleDictionaryStore, JsValue> {
        let inner = store_from_bytes(bytes, IndexType::SimpleDictionary, "Provided bytes are not a SimpleDictionaryStore").await?;
        Ok(SimpleDictionaryStore { inner })
    }

    pub fn empty() -> SimpleDictionaryStore {
        SimpleDictionaryStore { inner: store_empty() }
    }

    #[wasm_bindgen(js_name = fromString)]
    pub async fn from_string(
        input: String,
        format_name: &str,
        builder_strategy: Option<String>,
    ) -> Result<SimpleDictionaryStore, JsValue> {
        let inner = store_from_string(input, format_name, builder_strategy).await?;
        Ok(SimpleDictionaryStore { inner })
    }

    pub fn size(&self) -> usize {
        self.inner.size()
    }

    pub async fn has(&self, quad_js: JsValue) -> bool {
        store_has(&self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = addQuad)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        store_add_quad(&mut self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = deleteQuad)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        store_delete_quad(&mut self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = match)]
    pub async fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<SimpleDictionaryStore, JsValue> {
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);

        let res = self.inner.match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(SimpleDictionaryStore { inner: res })
    }

    pub async fn values(&self) -> Result<js_sys::Iterator, JsValue> {
        store_values(&self.inner).await
    }
}

// -------------------------
// Chained Hash Store Bindings
// -------------------------

#[wasm_bindgen]
pub struct ChainedHashStore {
    #[wasm_bindgen(skip)]
    pub inner: VortexRdfStore<ChainedHash>,
}

#[wasm_bindgen]
impl ChainedHashStore {
    #[wasm_bindgen(js_name = fromBytes)]
    pub async fn from_bytes(bytes: &[u8]) -> Result<ChainedHashStore, JsValue> {
        let inner = store_from_bytes(bytes, IndexType::ChainedHash, "Provided bytes are not a ChainedHashStore").await?;
        Ok(ChainedHashStore { inner })
    }

    pub fn empty() -> ChainedHashStore {
        ChainedHashStore { inner: store_empty() }
    }

    #[wasm_bindgen(js_name = fromString)]
    pub async fn from_string(
        input: String,
        format_name: &str,
        builder_strategy: Option<String>,
    ) -> Result<ChainedHashStore, JsValue> {
        let inner = store_from_string(input, format_name, builder_strategy).await?;
        Ok(ChainedHashStore { inner })
    }

    pub fn size(&self) -> usize {
        self.inner.size()
    }

    pub async fn has(&self, quad_js: JsValue) -> bool {
        store_has(&self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = addQuad)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        store_add_quad(&mut self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = deleteQuad)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        store_delete_quad(&mut self.inner, quad_js).await
    }

    #[wasm_bindgen(js_name = match)]
    pub async fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<ChainedHashStore, JsValue> {
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);

        let res = self.inner.match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(ChainedHashStore { inner: res })
    }

    pub async fn values(&self) -> Result<js_sys::Iterator, JsValue> {
        store_values(&self.inner).await
    }
}

// Helpers

fn parse_format(format_name: &str) -> Result<RdfFormat, JsValue> {
    match format_name.to_lowercase().as_str() {
        "nt" | "ntriples" => Ok(RdfFormat::NTriples),
        "nq" | "nquads" => Ok(RdfFormat::NQuads),
        "ttl" | "turtle" => Ok(RdfFormat::Turtle),
        "trig" => Ok(RdfFormat::TriG),
        "jsonld" => Ok(RdfFormat::JsonLd {
            profile: Default::default(),
        }),
        _ => Err(JsValue::from_str("Unsupported format")),
    }
}

// ... Term conversions ...

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

fn js_to_subject(val: JsValue) -> Option<NamedOrBlankNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b)),
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
pub async fn nquads_to_vortex(nquads: String, builder_strategy: Option<String>) -> Result<Vec<u8>, JsValue> {
    let strategy = parse_builder_strategy(builder_strategy)?;
    let quads_stream = parse_quads_from_reader(Cursor::new(nquads), RdfFormat::NQuads);
    let vortex_array = build_vortex_array_with_strategy::<SimpleDictionary>(quads_stream, strategy)
        .await
        .map_err(|e| JsValue::from_str(&format!("Vortex build error: {}", e)))?;
    let mut buffer = Vec::new();
    write_array_to_ipc(vortex_array, &mut buffer)
        .map_err(|e| JsValue::from_str(&format!("Vortex serialization error: {}", e)))?;
    Ok(buffer)
}

#[wasm_bindgen]
pub async fn vortex_to_nquads(vortex_bytes: &[u8]) -> Result<String, JsValue> {
    let cursor = Cursor::new(vortex_bytes);
    let vortex_array = array_from_ipc_reader(cursor)
        .map_err(|e| JsValue::from_str(&format!("Vortex read error: {}", e)))?;

    let mut output_buffer = Vec::new();
    
    // Detect and deserialize
    match detect_index_type(&vortex_array) {
        IndexType::SimpleDictionary => {
            let store = VortexRdfStore::<SimpleDictionary>::new(vortex_array)
                .map_err(|e| JsValue::from_str(&format!("Store init error: {}", e)))?;
            deserialize(store, &mut output_buffer, RdfFormat::NQuads)
                .await
                .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;
        },
        IndexType::ChainedHash => {
             let store = VortexRdfStore::<ChainedHash>::new(vortex_array)
                .map_err(|e| JsValue::from_str(&format!("Store init error: {}", e)))?;
            deserialize(store, &mut output_buffer, RdfFormat::NQuads)
                .await
                .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;
        }
    }

    String::from_utf8(output_buffer).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
}

fn parse_builder_strategy(strategy_name: Option<String>) -> Result<BuilderStrategy, JsValue> {
    match strategy_name.as_deref() {
        Some("UnsortedInMemory") | None => Ok(BuilderStrategy::UnsortedInMemory),
        Some("SortedInMemory") => Ok(BuilderStrategy::SortedInMemory),

        Some("SortedStream") => Err(JsValue::from_str(
            "Sorted-stream strategy is not supported in WebAssembly environments due to lack of filesystem access."
        )),
        Some("UnsortedStream") => Err(JsValue::from_str(
            "Unsorted-stream strategy is not supported in WebAssembly environments due to lack of filesystem access."
        )),
        Some(other) => Err(JsValue::from_str(&format!("Unknown builder strategy: {}", other))),
    }
}

async fn build_vortex_array_with_strategy<Dict: vortex_rdf_core::index::RdfDictionary>(
    quads_stream: impl futures::Stream<Item = vortex_rdf_core::error::Result<oxrdf::Quad>> + Unpin + Send + 'static,
    strategy: BuilderStrategy,
) -> vortex_rdf_core::error::Result<vortex_array::ArrayRef> {
    match strategy {
        BuilderStrategy::UnsortedInMemory => {
            VortexRdfStore::<Dict>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quads_stream).await
        }
        BuilderStrategy::SortedInMemory => {
            VortexRdfStore::<Dict>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quads_stream).await
        }

        BuilderStrategy::SortedStream => {
            VortexRdfStore::<Dict>::build_vortex_array_with_builder::<SortedStreamBuilder>(quads_stream).await
        }
        BuilderStrategy::UnsortedStream => {
            VortexRdfStore::<Dict>::build_vortex_array_with_builder::<UnsortedStreamBuilder>(quads_stream).await
        }
    }
}

// -------------------------
// Generic Binding Helpers
// -------------------------

fn store_empty<Dict: vortex_rdf_core::index::RdfDictionary>() -> VortexRdfStore<Dict> {
    VortexRdfStore::<Dict>::empty()
}

async fn store_from_bytes<Dict: vortex_rdf_core::index::RdfDictionary>(
    bytes: &[u8],
    expected_index: IndexType,
    err_msg: &str,
) -> Result<VortexRdfStore<Dict>, JsValue> {
    let cursor = Cursor::new(bytes);
    let array = array_from_ipc_reader(cursor)
        .map_err(|e: vortex_rdf_core::error::VortexRdfError| JsValue::from_str(&e.to_string()))?;

    if detect_index_type(&array) != expected_index {
        return Err(JsValue::from_str(err_msg));
    }

    VortexRdfStore::<Dict>::new(array).map_err(|e| JsValue::from_str(&e.to_string()))
}

async fn store_from_string<Dict: vortex_rdf_core::index::RdfDictionary>(
    input: String,
    format_name: &str,
    builder_strategy: Option<String>,
) -> Result<VortexRdfStore<Dict>, JsValue> {
    let strategy = parse_builder_strategy(builder_strategy)?;
    let format = parse_format(format_name)?;
    let cursor = Cursor::new(input);
    let quads_stream = parse_quads_from_reader(cursor, format);

    let vortex_array = build_vortex_array_with_strategy::<Dict>(quads_stream, strategy)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    VortexRdfStore::<Dict>::new(vortex_array).map_err(|e| JsValue::from_str(&e.to_string()))
}

async fn store_has<Dict: vortex_rdf_core::index::RdfDictionary>(
    store: &VortexRdfStore<Dict>,
    quad_js: JsValue,
) -> bool {
    if let Some(quad) = js_to_quad(quad_js) {
        store.match_pattern(
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

async fn store_add_quad<Dict: vortex_rdf_core::index::RdfDictionary>(
    store: &mut VortexRdfStore<Dict>,
    quad_js: JsValue,
) -> Result<(), JsValue> {
    if let Some(quad) = js_to_quad(quad_js) {
        *store = store.add_quad(quad).await.map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    } else {
        Err(JsValue::from_str("Invalid quad object"))
    }
}

async fn store_delete_quad<Dict: vortex_rdf_core::index::RdfDictionary>(
    store: &mut VortexRdfStore<Dict>,
    quad_js: JsValue,
) -> Result<(), JsValue> {
    if let Some(quad) = js_to_quad(quad_js) {
        *store = store.delete_quad(&quad).await.map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    } else {
        Err(JsValue::from_str("Invalid quad object"))
    }
}

async fn store_values<Dict: vortex_rdf_core::index::RdfDictionary>(
    store: &VortexRdfStore<Dict>,
) -> Result<js_sys::Iterator, JsValue> {
    let mut quads_stream = store.quads().map_err(|e| JsValue::from_str(&e.to_string()))?;

    let js_array = js_sys::Array::new();
    while let Some(q_res) = quads_stream.next().await {
        if let Ok(q) = q_res {
            js_array.push(&quad_to_js(&q));
        }
    }
    Ok(js_array.values())
}
