use oxrdf::{GraphName, NamedNode, Quad, NamedOrBlankNode, Term};
use oxrdfio::RdfFormat;
use std::io::Cursor;
use vortex_rdf_core::io::{
    deserialize,
    write_array_to_ipc,
    array_from_ipc_reader,
    VORTEX_LIGHT_SESSION,
};
use vortex_rdf_core::{
    VortexRdfStore,
    BuilderStrategy,
    UnsortedStreamBuilder,
    SortedInMemoryBuilder,
    LayoutStrategy,
    IndexType,
    Indexes,
};
use vortex_rdf_core::common::utils::parse_quads_from_reader;
use vortex_rdf_core::error::Result as CoreResult;
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use wasm_bindgen::prelude::*;
use js_sys::{Object, Reflect};
use futures::{Stream, StreamExt, stream};

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"
import { Quad, Term, NamedNode, BlankNode, Literal, Quad_Subject, Quad_Predicate, Quad_Object, Quad_Graph } from '@rdfjs/types';

/**
 * How quads are ordered while the columnar array is built.
 * - 'Unsorted': natural insertion order. Cheapest to build, but every `match`
 *   falls back to a full column scan.
 * - 'Sorted': global in-memory sort by subject -> predicate -> object -> graph.
 *   Costs a sort at build time, but unlocks binary-search lookups on subject.
 *
 * The core's out-of-core 'SortedStream' builder is not available here: it
 * spills sorted runs to disk, which WebAssembly has no access to.
 */
export type BuilderStrategy = 'Unsorted' | 'Sorted';

/**
 * How quad terms are encoded into columns.
 * - 'Default': all four terms as N-Triples strings natively optimised by Vortex.
 * - 'TypedObject': the object is split into kind/value/datatype/language columns.
 * - 'Dictionary': every term is replaced by a u32 code into a global sorted term
 *   dictionary. More compact than 'Default'. Added quads live in an
 *   in-memory string tail until the store is serialized or compacted.
 */
export type LayoutStrategy = 'Default' | 'TypedObject' | 'Dictionary';

/**
 * Secondary indexes embedded alongside the primary quad columns.
 * 'SecondaryByReference' adds sorted predicate/object columns plus row-id
 * back-references, letting predicate-only and object-only patterns use a
 * binary search instead of a full scan.
 * 'SecondaryByCopy' embeds two complete extra copies of the quad columns —
 * one sorted by (p, o, s, g), one by (o, s, p, g) — so predicate- and
 * object-bound patterns (including predicate+object prefix lookups) get the
 * same sorted access path subjects have, at ~2x the storage.
 * Both are only effective with a 'Sorted' builder.
 */
export type IndexType = 'SecondaryByReference' | 'SecondaryByCopy';

/** RDF syntaxes accepted for parsing and emitted for serialization. */
export type RdfFormatName =
    | 'nt' | 'ntriples'
    | 'nq' | 'nquads'
    | 'ttl' | 'turtle'
    | 'trig'
    | 'n3'
    | 'rdf' | 'rdfxml' | 'xml'
    | 'jsonld';

/** Build-time configuration. Any omitted field keeps its default. */
export interface BuildOptions {
    /** @default 'Unsorted' */
    builder?: BuilderStrategy;
    /** @default 'Default' */
    layout?: LayoutStrategy;
    /** @default [] */
    indexes?: IndexType[];
}

/** A bare BuilderStrategy string is accepted as shorthand for `{ builder }`. */
export type BuildOptionsInput = BuildOptions | BuilderStrategy;

export class VortexStore {
    static empty(): VortexStore;
    static fromBytes(bytes: Uint8Array): Promise<VortexStore>;
    static fromString(input: string, format: RdfFormatName, options?: BuildOptionsInput): Promise<VortexStore>;
    static fromQuads(quads: Quad[], options?: BuildOptionsInput): Promise<VortexStore>;

    /** The layout this store's columns are encoded with. */
    layout(): LayoutStrategy;
    size(): Promise<number>;
    has(quad: Quad): Promise<boolean>;
    /** Add one quad in place (a quad already present is ignored, per RDF/JS). */
    addQuad(quad: Quad): Promise<void>;
    /**
     * Add many quads in one call — one tail rebuild for the whole batch,
     * where a loop over addQuad pays one per quad.
     */
    addQuads(quads: Quad[]): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<VortexStore>;
    values(): Promise<IterableIterator<Quad>>;
    /** Serialize to Vortex IPC bytes; read back with `VortexStore.fromBytes`. */
    toBytes(): Promise<Uint8Array>;
    /** Serialize the quads to an RDF syntax. */
    toRdf(format: RdfFormatName): Promise<string>;
}

export function rdf_to_vortex(input: string, format: RdfFormatName, options?: BuildOptionsInput): Promise<Uint8Array>;
export function vortex_to_rdf(vortex_bytes: Uint8Array, format: RdfFormatName): Promise<string>;
export function nquads_to_vortex(nquads: string, options?: BuildOptionsInput): Promise<Uint8Array>;
export function vortex_to_nquads(vortex_bytes: Uint8Array): Promise<string>;
"#;

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

// ─── VortexStore ─────────────────────────────────────────────────────────────

#[wasm_bindgen(skip_typescript)]
pub struct VortexStore {
    #[wasm_bindgen(skip)]
    pub inner: VortexRdfStore,
}

#[wasm_bindgen]
impl VortexStore {
    #[wasm_bindgen(skip_typescript)]
    pub fn empty() -> VortexStore {
        VortexStore { inner: VortexRdfStore::empty() }
    }

    #[wasm_bindgen(js_name = fromBytes, skip_typescript)]
    pub async fn from_bytes(bytes: &[u8]) -> Result<VortexStore, JsValue> {
        let cursor = Cursor::new(bytes);
        let array = array_from_ipc_reader(cursor)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let inner = VortexRdfStore::new(array)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexStore { inner })
    }

    #[wasm_bindgen(js_name = fromString, skip_typescript)]
    pub async fn from_string(
        input: String,
        format_name: &str,
        options: JsValue,
    ) -> Result<VortexStore, JsValue> {
        let format = parse_format(format_name)?;
        let config = parse_build_options(options)?;
        let quads_stream = parse_quads_from_reader(Cursor::new(input), format);
        let vortex_array = build_array(quads_stream, config).await?;

        let inner = VortexRdfStore::new(vortex_array)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexStore { inner })
    }

    /// Build directly from RDF/JS quads, skipping a serialize/parse round-trip.
    #[wasm_bindgen(js_name = fromQuads, skip_typescript)]
    pub async fn from_quads(quads: js_sys::Array, options: JsValue) -> Result<VortexStore, JsValue> {
        let config = parse_build_options(options)?;
        let quads = js_array_to_quads(quads)?;
        let vortex_array = build_array(stream::iter(quads.into_iter().map(Ok)), config).await?;

        let inner = VortexRdfStore::new(vortex_array)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexStore { inner })
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn layout(&self) -> String {
        layout_name(self.inner.layout()).to_string()
    }

    #[wasm_bindgen(js_name = toBytes, skip_typescript)]
    pub async fn to_bytes(&self) -> Result<Vec<u8>, JsValue> {
        // Not `get_quads_array`: a Dictionary-layout store derived from `match`
        // may have lost the term-dictionary payload that its codes decode
        // against, which would make the written bytes unreadable.
        let array = self.inner.to_serializable_array().await
            .map_err(|e| JsValue::from_str(&format!("Vortex read error: {}", e)))?;

        // A store derived from `match` holds an unevaluated `filter` node, which
        // has no IPC serialization. Evaluate it away first. Canonical form is not
        // recursive — a StructArray's fields may still be lazy — so this has to be
        // `RecursiveCanonical` rather than `StructArray`. Not an extra cost here:
        // `toBytes` materializes the whole payload into a JS buffer regardless.
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let array = array
            .execute::<RecursiveCanonical>(&mut ctx)
            .map_err(|e| JsValue::from_str(&format!("Vortex execution error: {}", e)))?
            .0
            .into_array();

        let mut buffer = Vec::new();
        write_array_to_ipc(array, &mut buffer)
            .map_err(|e| JsValue::from_str(&format!("Vortex serialization error: {}", e)))?;
        Ok(buffer)
    }

    #[wasm_bindgen(js_name = toRdf, skip_typescript)]
    pub async fn to_rdf(&self, format_name: &str) -> Result<String, JsValue> {
        let format = parse_format(format_name)?;
        let mut buffer = Vec::new();
        // Serialize through this store's own resolved layout, so a store derived
        // from `match` still decodes against the term dictionary it carries.
        deserialize(self.inner.clone(), &mut buffer, format)
            .await
            .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;
        String::from_utf8(buffer).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
    }

    #[wasm_bindgen(skip_typescript)]
    pub async fn size(&self) -> Result<usize, JsValue> {
        self.inner.size().await.map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(skip_typescript)]
    pub async fn has(&self, quad_js: JsValue) -> bool {
        match js_to_quad(quad_js) {
            Some(quad) => self.inner.contains(&quad).await.unwrap_or(false),
            None => false,
        }
    }

    /// This store's inner store as one that owns its rows, ready to be mutated
    /// in place.
    ///
    /// `add`/`delete` mutate the receiver and return nothing (per RDF/JS
    /// `DatasetCore`, which mutates in place). When the receiver already owns
    /// its rows — the common case, a store the caller built — this is a cheap
    /// clone that keeps its tombstones and indexes, so repeated deletes stay
    /// cheap and indexed. When the receiver is a lazy `match` view, RDF/JS
    /// requires the matched dataset to be independent of its source; core
    /// materializes it into an owning copy, rebuilding its indexes so the copy
    /// stays query-accelerated. Either way the source is never touched.
    async fn owned(&self) -> Result<VortexRdfStore, JsValue> {
        self.inner
            .owned()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = addQuad, skip_typescript)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| JsValue::from_str("Invalid quad object"))?;
        self.inner = self
            .owned()
            .await?
            .add_quad(quad)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen(js_name = addQuads, skip_typescript)]
    pub async fn add_quads(&mut self, quads_js: js_sys::Array) -> Result<(), JsValue> {
        let quads = js_array_to_quads(quads_js)?;
        self.inner = self
            .owned()
            .await?
            .add_quads(quads)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen(js_name = deleteQuad, skip_typescript)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| JsValue::from_str("Invalid quad object"))?;
        self.inner = self
            .owned()
            .await?
            .delete_quad(&quad)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen(js_name = match, skip_typescript)]
    pub async fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<VortexStore, JsValue> {
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);

        let inner = self.inner
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexStore { inner })
    }

    #[wasm_bindgen(skip_typescript)]
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

// ─── Free functions ───────────────────────────────────────────────────────────

#[wasm_bindgen(skip_typescript)]
pub async fn rdf_to_vortex(
    input: String,
    format_name: &str,
    options: JsValue,
) -> Result<Vec<u8>, JsValue> {
    let format = parse_format(format_name)?;
    let config = parse_build_options(options)?;
    let quads_stream = parse_quads_from_reader(Cursor::new(input), format);
    let vortex_array = build_array(quads_stream, config).await?;

    let mut buffer = Vec::new();
    write_array_to_ipc(vortex_array, &mut buffer)
        .map_err(|e| JsValue::from_str(&format!("Vortex serialization error: {}", e)))?;
    Ok(buffer)
}

#[wasm_bindgen(skip_typescript)]
pub async fn vortex_to_rdf(vortex_bytes: &[u8], format_name: &str) -> Result<String, JsValue> {
    let format = parse_format(format_name)?;
    let cursor = Cursor::new(vortex_bytes);
    let vortex_array = array_from_ipc_reader(cursor)
        .map_err(|e| JsValue::from_str(&format!("Vortex read error: {}", e)))?;

    let store = VortexRdfStore::new(vortex_array)
        .map_err(|e| JsValue::from_str(&format!("Store init error: {}", e)))?;

    let mut output_buffer = Vec::new();
    deserialize(store, &mut output_buffer, format)
        .await
        .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;

    String::from_utf8(output_buffer).map_err(|e| JsValue::from_str(&format!("UTF-8 error: {}", e)))
}

#[wasm_bindgen(skip_typescript)]
pub async fn nquads_to_vortex(nquads: String, options: JsValue) -> Result<Vec<u8>, JsValue> {
    rdf_to_vortex(nquads, "nquads", options).await
}

#[wasm_bindgen(skip_typescript)]
pub async fn vortex_to_nquads(vortex_bytes: &[u8]) -> Result<String, JsValue> {
    vortex_to_rdf(vortex_bytes, "nquads").await
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn parse_format(format_name: &str) -> Result<RdfFormat, JsValue> {
    match format_name.to_lowercase().as_str() {
        "nt" | "ntriples" => Ok(RdfFormat::NTriples),
        "nq" | "nquads" => Ok(RdfFormat::NQuads),
        "ttl" | "turtle" => Ok(RdfFormat::Turtle),
        "trig" => Ok(RdfFormat::TriG),
        "n3" => Ok(RdfFormat::N3),
        "rdf" | "rdfxml" | "xml" => Ok(RdfFormat::RdfXml),
        "jsonld" => Ok(RdfFormat::JsonLd { profile: Default::default() }),
        other => Err(JsValue::from_str(&format!(
            "Unsupported format: {}. Supported formats are 'ntriples', 'nquads', 'turtle', \
             'trig', 'n3', 'rdfxml' and 'jsonld'.",
            other
        ))),
    }
}

/// Build-time configuration resolved from the JS `BuildOptions` object.
struct BuildConfig {
    builder: BuilderStrategy,
    layout: LayoutStrategy,
    indexes: Indexes,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            builder: BuilderStrategy::UnsortedStream,
            layout: LayoutStrategy::Default,
            indexes: Vec::new(),
        }
    }
}

/// Run the quad stream through the builder named by `config`.
///
/// This is the single place that monomorphizes the builders, so every entry
/// point (`fromString`, `fromQuads`, `rdf_to_vortex`) offers the same choices.
async fn build_array(
    quads: impl Stream<Item = CoreResult<Quad>> + Unpin + Send + 'static,
    config: BuildConfig,
) -> Result<ArrayRef, JsValue> {
    let BuildConfig { builder, layout, indexes } = config;
    match builder {
        BuilderStrategy::UnsortedStream =>
            VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
                quads, layout, indexes,
            ).await,
        BuilderStrategy::SortedInMemory =>
            VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
                quads, layout, indexes,
            ).await,
        // Defensive: `parse_builder` never yields SortedStream, which spills to disk.
        BuilderStrategy::SortedStream => return Err(JsValue::from_str(
            "The sorted-stream builder strategy is not available in WebAssembly."
        )),
    }
    .map_err(|e| JsValue::from_str(&format!("Vortex build error: {}", e)))
}

/// Resolve the optional JS build options. Accepts `undefined`/`null` (all
/// defaults), a bare builder-strategy string, or a `BuildOptions` object.
fn parse_build_options(options: JsValue) -> Result<BuildConfig, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(BuildConfig::default());
    }
    if let Some(name) = options.as_string() {
        return Ok(BuildConfig { builder: parse_builder(&name)?, ..BuildConfig::default() });
    }

    let mut config = BuildConfig::default();
    if let Some(name) = get_string_option(&options, "builder")? {
        config.builder = parse_builder(&name)?;
    }
    if let Some(name) = get_string_option(&options, "layout")? {
        config.layout = parse_layout(&name)?;
    }
    let indexes = Reflect::get(&options, &"indexes".into())
        .map_err(|_| JsValue::from_str("Could not read the 'indexes' option"))?;
    if !indexes.is_null() && !indexes.is_undefined() {
        if !js_sys::Array::is_array(&indexes) {
            return Err(JsValue::from_str("Option 'indexes' must be an array"));
        }
        config.indexes = js_sys::Array::from(&indexes)
            .iter()
            .map(|value| match value.as_string() {
                Some(name) => parse_index(&name),
                None => Err(JsValue::from_str("Option 'indexes' must contain strings")),
            })
            .collect::<Result<Indexes, JsValue>>()?;
    }
    Ok(config)
}

/// Read an optional string field, erroring if present but not a string.
fn get_string_option(options: &JsValue, key: &str) -> Result<Option<String>, JsValue> {
    let value = Reflect::get(options, &key.into())
        .map_err(|_| JsValue::from_str(&format!("Could not read the '{}' option", key)))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    match value.as_string() {
        Some(name) => Ok(Some(name)),
        None => Err(JsValue::from_str(&format!("Option '{}' must be a string", key))),
    }
}

fn parse_builder(name: &str) -> Result<BuilderStrategy, JsValue> {
    match name {
        "Unsorted" => Ok(BuilderStrategy::UnsortedStream),
        "Sorted" => Ok(BuilderStrategy::SortedInMemory),
        other => Err(JsValue::from_str(&format!(
            "Unknown builder strategy: {}. Supported strategies are 'Unsorted' and 'Sorted'.",
            other
        ))),
    }
}

fn parse_layout(name: &str) -> Result<LayoutStrategy, JsValue> {
    match name {
        "Default" => Ok(LayoutStrategy::Default),
        "TypedObject" => Ok(LayoutStrategy::TypedObject),
        "Dictionary" => Ok(LayoutStrategy::Dictionary),
        other => Err(JsValue::from_str(&format!(
            "Unknown layout strategy: {}. Supported layouts are 'Default', 'TypedObject' \
             and 'Dictionary'.",
            other
        ))),
    }
}

fn parse_index(name: &str) -> Result<IndexType, JsValue> {
    match name {
        "SecondaryByReference" => Ok(IndexType::SecondaryByReference),
        "SecondaryByCopy" => Ok(IndexType::SecondaryByCopy),
        other => Err(JsValue::from_str(&format!(
            "Unknown index type: {}. Supported indexes are 'SecondaryByReference' \
             and 'SecondaryByCopy'.",
            other
        ))),
    }
}

fn layout_name(layout: LayoutStrategy) -> &'static str {
    match layout {
        LayoutStrategy::Default => "Default",
        LayoutStrategy::TypedObject => "TypedObject",
        LayoutStrategy::Dictionary => "Dictionary",
    }
}

fn js_array_to_quads(quads: js_sys::Array) -> Result<Vec<Quad>, JsValue> {
    quads
        .iter()
        .enumerate()
        .map(|(i, quad)| {
            js_to_quad(quad)
                .ok_or_else(|| JsValue::from_str(&format!("Invalid quad object at index {}", i)))
        })
        .collect()
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
    let equals_fn = js_sys::Function::new_with_args(
        "other",
        "return other && this.termType === other.termType && this.value === other.value && \
         this.language === (other.language || '') && \
         (this.datatype ? this.datatype.equals(other.datatype) : !other.datatype)",
    );
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
    let equals_fn = js_sys::Function::new_with_args(
        "other",
        "return other && this.termType === other.termType && this.value === other.value",
    );
    Reflect::set(&obj, &"equals".into(), &equals_fn).unwrap();
    obj.into()
}

fn quad_to_js(quad: &Quad) -> JsValue {
    let obj = Object::new();
    Reflect::set(&obj, &"subject".into(), &term_to_js(&quad.subject.clone().into())).unwrap();
    Reflect::set(&obj, &"predicate".into(), &term_to_js(&quad.predicate.clone().into())).unwrap();
    Reflect::set(&obj, &"object".into(), &term_to_js(&quad.object)).unwrap();
    Reflect::set(&obj, &"graph".into(), &graph_name_to_js(&quad.graph_name)).unwrap();
    let equals_fn = js_sys::Function::new_with_args(
        "other",
        "return other && this.subject.equals(other.subject) && this.predicate.equals(other.predicate) && \
         this.object.equals(other.object) && this.graph.equals(other.graph)",
    );
    Reflect::set(&obj, &"equals".into(), &equals_fn).unwrap();
    obj.into()
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
        return Some(RawTerm { term_type: "NamedNode".into(), value: s, language: None, datatype_iri: None });
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
            if let Some(l) = raw.language
                && !l.is_empty() {
                    return oxrdf::Literal::new_language_tagged_literal(raw.value, l).ok().map(Term::Literal);
                }
            if let Some(dt_iri) = raw.datatype_iri
                && let Ok(dt_node) = NamedNode::new(dt_iri) {
                    return Some(Term::Literal(oxrdf::Literal::new_typed_literal(raw.value, dt_node)));
                }
            Some(Term::Literal(oxrdf::Literal::new_simple_literal(raw.value)))
        }
        _ => None,
    }
}

fn js_to_subject(val: JsValue) -> Option<NamedOrBlankNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b)),
        _ => None,
    }
}

fn js_to_named_node(val: JsValue) -> Option<NamedNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(n),
        _ => None,
    }
}

fn js_to_graph(val: JsValue) -> Option<GraphName> {
    if let Some(term) = js_to_term_raw(val) {
        match term.term_type.as_str() {
            "NamedNode" => Some(GraphName::NamedNode(NamedNode::new(term.value).ok()?)),
            "BlankNode" => Some(GraphName::BlankNode(oxrdf::BlankNode::new_unchecked(term.value))),
            "DefaultGraph" => Some(GraphName::DefaultGraph),
            _ => None,
        }
    } else {
        None
    }
}

fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g = js_to_graph(Reflect::get(&val, &"graph".into()).ok()?).unwrap_or(GraphName::DefaultGraph);
    Some(Quad::new(s, p, o, g))
}
