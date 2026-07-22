use futures::channel::mpsc;
use futures::{Stream, StreamExt, stream};
use js_sys::{Object, Reflect};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::RdfFormat;
use std::cell::RefCell;
use std::io::Cursor;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use vortex_rdf_core::common::utils::parse_quads_from_reader;
use vortex_rdf_core::error::{Result as CoreResult, VortexRdfError};
use vortex_rdf_core::io::{
    VORTEX_LIGHT_SESSION, array_from_ipc_reader, deserialize, write_array_to_ipc,
};
use vortex_rdf_core::{
    BuilderStrategy, IndexType, Indexes, LayoutStrategy, SortedInMemoryBuilder,
    UnsortedStreamBuilder, VortexRdfStore as CoreStore,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

// The lazy RDF/JS read model (LazyQuad/LazyTerm + stream) lives in a local
// snippet (copied verbatim into the generated pkg; no runtime npm dependency).
// See js/js-snippets/lazy-rdf.js.
#[wasm_bindgen(module = "/js-snippets/lazy-rdf.js")]
extern "C" {
    /// Wrap the packed dictionary buffers into a `LazyDict` (decodes a term code
    /// to its string host-side, interned). Built once per store.
    #[wasm_bindgen(js_name = makeDictView)]
    fn make_dict_view(offsets: js_sys::Uint32Array, bytes: js_sys::Uint8Array) -> JsValue;

    /// Build a `LazyQuad[]` from a column payload — for `getQuads`.
    #[wasm_bindgen(js_name = buildLazyQuads)]
    fn build_lazy_quads(payload: &JsValue) -> js_sys::Array;

    /// Build a `Stream<LazyQuad>` from a `Promise<payload>` — so `match` returns
    /// synchronously while resolving its rows lazily.
    #[wasm_bindgen(js_name = makeLazyQuadStream)]
    fn make_lazy_quad_stream(payload_promise: &JsValue) -> JsValue;
}

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = r#"
import { Quad, Term, NamedNode, BlankNode, Literal, Quad_Subject, Quad_Predicate, Quad_Object, Quad_Graph, Stream } from '@rdfjs/types';

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
    /** @default 'Dictionary' */
    layout?: LayoutStrategy;
    /** @default [] */
    indexes?: IndexType[];
}

/** A bare BuilderStrategy string is accepted as shorthand for `{ builder }`. */
export type BuildOptionsInput = BuildOptions | BuilderStrategy;

export class VortexRdfStore {
    static empty(): VortexRdfStore;
    static fromBytes(bytes: Uint8Array): Promise<VortexRdfStore>;
    static fromString(input: string, format: RdfFormatName, options?: BuildOptionsInput): Promise<VortexRdfStore>;
    /** `quads` may be an array, or an RDF/JS `Stream<Quad>` (a Node-style event emitter). */
    static fromQuads(quads: Quad[] | Stream<Quad>, options?: BuildOptionsInput): Promise<VortexRdfStore>;

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
    /**
     * Stream the quads matching a pattern (the RDF/JS `Source.match` contract).
     * Pass `null`/`undefined` for a variable position. Returns **synchronously**
     * an RDF/JS `Stream<Quad>` (`.on('data'|'end'|'error', …)`, `.read()`) of
     * lazy `Quad`s: a term's string is decoded from the columnar data only when
     * its `.value`/`.termType` is read, and never eagerly. The stream also
     * implements `Symbol.asyncIterator`, so it can be consumed with `for await`
     * (cast to `AsyncIterable<Quad>` in typed code).
     */
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Stream<Quad>;
    /**
     * Materialize the quads matching a pattern into an array of lazy `Quad`s —
     * the array-returning counterpart of `match`. `async` (returns a `Promise`)
     * because resolving the match crosses the WebAssembly boundary; the returned
     * `Quad`s still decode their term strings lazily on access.
     */
    getQuads(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<Quad[]>;
    /**
     * Low-level (Dictionary layout only). Resolve a pattern to the matched rows'
     * raw u32 term codes — four columnar `Uint32Array`s — without materializing
     * any term strings. Returns `null` if the store is not Dictionary layout.
     * Resolve codes to terms with `decodeTerm`. `match`/`getQuads` build on this.
     */
    matchCodes(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Promise<{ s: Uint32Array; p: Uint32Array; o: Uint32Array; g: Uint32Array; length: number } | null>;
    /** Low-level. Decode a Dictionary-layout term code to its N-Triples term string. */
    decodeTerm(code: number): string | undefined;
    /** Low-level. Encode an N-Triples term string to its Dictionary-layout code (inverse of decodeTerm). */
    encodeTerm(term: string): number | undefined;
    /** Serialize to Vortex IPC bytes; read back with `VortexRdfStore.fromBytes`. */
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

// ─── VortexRdfStore ─────────────────────────────────────────────────────────────

#[wasm_bindgen(skip_typescript)]
pub struct VortexRdfStore {
    #[wasm_bindgen(skip)]
    pub inner: CoreStore,
    // The store's term dictionary as a JS `LazyDict`, built once on the first
    // Dictionary-layout read and shared by every LazyTerm this store produces
    // (their `.equals` fast path keys on its identity). Not exposed to JS.
    dict_view: RefCell<Option<JsValue>>,
}

impl VortexRdfStore {
    fn wrap(inner: CoreStore) -> Self {
        Self {
            inner,
            dict_view: RefCell::new(None),
        }
    }

    /// The dictionary for decoding a match's `u32` code columns, or `None` when
    /// the code path does not apply (non-Dictionary layout, or the store carries
    /// an append tail — appended quads are re-encoded against a *fresh*
    /// dictionary, so `get_quads_array`'s codes would not match the store's
    /// cached one; those reads fall back to the always-correct term path).
    fn code_path_dict(&self) -> Option<JsValue> {
        if self.inner.tail_len() != 0 {
            return None;
        }
        self.dict_view()
    }

    /// The store's `LazyDict` (built once from the packed dictionary buffers),
    /// or `None` when this store is not Dictionary-layout.
    fn dict_view(&self) -> Option<JsValue> {
        if let Some(dv) = self.dict_view.borrow().as_ref() {
            return Some(dv.clone());
        }
        let (offsets, bytes) = self.inner.dictionary_buffers()?;
        let offs = js_sys::Uint32Array::new_with_length(offsets.len() as u32);
        offs.copy_from(&offsets);
        let bys = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
        bys.copy_from(&bytes);
        let dv = make_dict_view(offs, bys);
        *self.dict_view.borrow_mut() = Some(dv.clone());
        Some(dv)
    }
}

#[wasm_bindgen]
impl VortexRdfStore {
    #[wasm_bindgen(skip_typescript)]
    pub fn empty() -> VortexRdfStore {
        VortexRdfStore::wrap(CoreStore::empty())
    }

    #[wasm_bindgen(js_name = fromBytes, skip_typescript)]
    pub async fn from_bytes(bytes: &[u8]) -> Result<VortexRdfStore, JsValue> {
        let cursor = Cursor::new(bytes);
        let array = array_from_ipc_reader(cursor).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let inner = CoreStore::new(array).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexRdfStore::wrap(inner))
    }

    #[wasm_bindgen(js_name = fromString, skip_typescript)]
    pub async fn from_string(
        input: String,
        format_name: &str,
        options: JsValue,
    ) -> Result<VortexRdfStore, JsValue> {
        let format = parse_format(format_name)?;
        let config = parse_build_options(options)?;
        let quads_stream = parse_quads_from_reader(Cursor::new(input), format);
        let vortex_array = build_array(quads_stream, config).await?;

        let inner = CoreStore::new(vortex_array).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexRdfStore::wrap(inner))
    }

    /// Build directly from RDF/JS quads — either an array or a `Stream<Quad>`
    /// (a Node-style event emitter) — skipping a serialize/parse round-trip.
    #[wasm_bindgen(js_name = fromQuads, skip_typescript)]
    pub async fn from_quads(quads: JsValue, options: JsValue) -> Result<VortexRdfStore, JsValue> {
        let config = parse_build_options(options)?;
        let quad_stream = js_to_quad_stream(quads)?;
        let vortex_array = build_array(quad_stream, config).await?;

        let inner = CoreStore::new(vortex_array).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(VortexRdfStore::wrap(inner))
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
        let array = self
            .inner
            .to_serializable_array()
            .await
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
        self.inner
            .size()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
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
    async fn owned(&self) -> Result<CoreStore, JsValue> {
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
        // The dictionary may have changed (auto-compaction re-encodes); drop the
        // cached view so the next read rebuilds it from the new base.
        self.dict_view.replace(None);
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
        self.dict_view.replace(None);
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
        self.dict_view.replace(None);
        Ok(())
    }

    /// RDF/JS `Source.match`: stream the quads matching a pattern as a
    /// `Stream<Quad>` of lazy, zero-copy `LazyQuad`s.
    ///
    /// Returns synchronously. The pattern is resolved lazily inside a `Promise`
    /// that yields a columnar payload, handed to a minimal RDF/JS `Stream`
    /// (`.on('data'|'end'|'error', …)`, `.read()`, and — as a convenience —
    /// `Symbol.asyncIterator` for `for await`). No term strings are materialized
    /// until a `LazyTerm`'s `.value`/`.termType` is read.
    #[wasm_bindgen(js_name = match, skip_typescript)]
    pub fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> JsValue {
        // Parse the pattern eagerly (cheap, synchronous) so only owned oxrdf
        // terms — not JsValues — are moved into the resolving future.
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);
        // Ensure the shared dictionary view synchronously (Dictionary layout);
        // it is not dependent on the matched rows and must be built off `self`.
        let dict = self.code_path_dict();
        let inner = self.inner.clone();
        let promise =
            future_to_promise(async move { match_columns(inner, dict, s, p, o, g).await });
        make_lazy_quad_stream(&promise.into())
    }

    /// Materialize the quads matching a pattern into a `LazyQuad[]` — the
    /// array-returning counterpart of [`match`](Self::match_pattern).
    #[wasm_bindgen(js_name = getQuads, skip_typescript)]
    pub async fn get_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<js_sys::Array, JsValue> {
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);
        let dict = self.code_path_dict();
        let payload = match_columns(self.inner.clone(), dict, s, p, o, g).await?;
        Ok(build_lazy_quads(&payload))
    }

    /// **Prototype (Dictionary layout only).** Resolve a pattern and hand back
    /// the matched rows as raw `u32` term codes — four `Uint32Array` columns
    /// `{ s, p, o, g, length }`, or `null` if this store is not Dictionary
    /// layout. No term strings are materialized; the caller resolves codes to
    /// terms lazily via [`decodeTerm`](Self::decode_term). This is the
    /// zero-copy-until-observed read path being evaluated against `getQuads`.
    #[wasm_bindgen(js_name = matchCodes, skip_typescript)]
    pub async fn match_codes(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<JsValue, JsValue> {
        // Codes are only meaningful against the store's cached dictionary, which
        // holds for a pristine Dictionary store. An append tail re-encodes
        // against a fresh dictionary, so codes would not resolve via `decodeTerm`.
        if self.inner.layout() != LayoutStrategy::Dictionary || self.inner.tail_len() != 0 {
            return Ok(JsValue::NULL);
        }
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);
        let matched = self
            .inner
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let arr = matched
            .get_quads_array()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = arr
            .execute::<StructArray>(&mut ctx)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        macro_rules! u32_col {
            ($name:expr) => {{
                let col = struct_arr
                    .unmasked_field_by_name($name)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                let prim = col
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                let slice = prim.as_slice::<u32>();
                // Copy into a JS-owned Uint32Array (safe against wasm memory
                // growth, which would detach a zero-copy view).
                let ta = js_sys::Uint32Array::new_with_length(slice.len() as u32);
                ta.copy_from(slice);
                JsValue::from(ta)
            }};
        }

        let result = Object::new();
        Reflect::set(&result, &"s".into(), &u32_col!("s"))?;
        Reflect::set(&result, &"p".into(), &u32_col!("p"))?;
        Reflect::set(&result, &"o".into(), &u32_col!("o"))?;
        Reflect::set(&result, &"g".into(), &u32_col!("g"))?;
        Reflect::set(
            &result,
            &"length".into(),
            &JsValue::from_f64(struct_arr.len() as f64),
        )?;
        Ok(result.into())
    }

    /// **Prototype.** Decode a Dictionary-layout term code to its N-Triples term
    /// string (`<iri>`, `_:blank`, `"lit"@lang`, `"lit"^^<dt>`, or `""` for the
    /// default graph). `undefined` if not Dictionary layout or out of range.
    #[wasm_bindgen(js_name = decodeTerm, skip_typescript)]
    pub fn decode_term(&self, code: u32) -> Option<String> {
        self.inner.decode_code(code)
    }

    /// **Prototype.** Encode an N-Triples term string to its Dictionary-layout
    /// code (inverse of `decodeTerm`). `undefined` if not Dictionary layout or
    /// the term is absent from the dictionary.
    #[wasm_bindgen(js_name = encodeTerm, skip_typescript)]
    pub fn encode_term(&self, term: &str) -> Option<u32> {
        self.inner.encode_code(term)
    }
}

/// Resolve a pattern and pack the matched rows into the columnar payload the JS
/// lazy read model consumes. Shared by `match` and `getQuads`.
///
/// Dictionary layout (`dict` is `Some`) ships four `u32` code columns plus the
/// shared dictionary — no term strings are touched. Other layouts ship packed
/// N-Triples term columns (`{offsets, bytes}`), decoded once from `quads()`.
async fn match_columns(
    store: CoreStore,
    dict: Option<JsValue>,
    subject: Option<NamedOrBlankNode>,
    predicate: Option<NamedNode>,
    object: Option<Term>,
    graph: Option<GraphName>,
) -> Result<JsValue, JsValue> {
    let matched = store
        .match_pattern(
            subject.as_ref(),
            predicate.as_ref(),
            object.as_ref(),
            graph.as_ref(),
        )
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let payload = Object::new();

    if let Some(dict) = dict {
        // Code payload: u32 columns + the shared dictionary.
        let arr = matched
            .get_quads_array()
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = arr
            .execute::<StructArray>(&mut ctx)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        macro_rules! u32_col {
            ($name:expr) => {{
                let col = struct_arr
                    .unmasked_field_by_name($name)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                let prim = col
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
                let slice = prim.as_slice::<u32>();
                let ta = js_sys::Uint32Array::new_with_length(slice.len() as u32);
                ta.copy_from(slice);
                JsValue::from(ta)
            }};
        }
        Reflect::set(&payload, &"kind".into(), &"code".into())?;
        Reflect::set(&payload, &"s".into(), &u32_col!("s"))?;
        Reflect::set(&payload, &"p".into(), &u32_col!("p"))?;
        Reflect::set(&payload, &"o".into(), &u32_col!("o"))?;
        Reflect::set(&payload, &"g".into(), &u32_col!("g"))?;
        Reflect::set(&payload, &"dict".into(), &dict)?;
        Reflect::set(
            &payload,
            &"length".into(),
            &JsValue::from_f64(struct_arr.len() as f64),
        )?;
    } else {
        // Term payload: packed N-Triples term columns for non-Dictionary layouts.
        let mut quads_stream = matched
            .quads()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        // (offsets seeded with a leading 0, bytes) per s/p/o/g column.
        let mut cols: [(Vec<u32>, Vec<u8>); 4] = [
            (vec![0], Vec::new()),
            (vec![0], Vec::new()),
            (vec![0], Vec::new()),
            (vec![0], Vec::new()),
        ];
        let mut n = 0u32;
        while let Some(q_res) = quads_stream.next().await {
            let q = q_res.map_err(|e| JsValue::from_str(&e.to_string()))?;
            let terms = [
                q.subject.to_string(),
                q.predicate.to_string(),
                q.object.to_string(),
                match &q.graph_name {
                    GraphName::DefaultGraph => String::new(),
                    other => other.to_string(),
                },
            ];
            for (col, term) in cols.iter_mut().zip(terms.iter()) {
                col.1.extend_from_slice(term.as_bytes());
                col.0.push(col.1.len() as u32);
            }
            n += 1;
        }
        Reflect::set(&payload, &"kind".into(), &"term".into())?;
        for (name, (offsets, bytes)) in ["s", "p", "o", "g"].iter().zip(cols.iter()) {
            Reflect::set(&payload, &(*name).into(), &term_column(offsets, bytes))?;
        }
        Reflect::set(&payload, &"length".into(), &JsValue::from_f64(n as f64))?;
    }
    Ok(payload.into())
}

/// Pack one term column's offsets/bytes into a `{offsets, bytes}` JS object.
fn term_column(offsets: &[u32], bytes: &[u8]) -> JsValue {
    let offs = js_sys::Uint32Array::new_with_length(offsets.len() as u32);
    offs.copy_from(offsets);
    let bys = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
    bys.copy_from(bytes);
    let obj = Object::new();
    Reflect::set(&obj, &"offsets".into(), &offs).unwrap();
    Reflect::set(&obj, &"bytes".into(), &bys).unwrap();
    obj.into()
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

    let store = CoreStore::new(vortex_array)
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
        "jsonld" => Ok(RdfFormat::JsonLd {
            profile: Default::default(),
        }),
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
            // Dictionary is the JS default: it is the most compact layout and
            // backs the zero-copy code-based read model (integer `.equals`).
            layout: LayoutStrategy::Dictionary,
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
    let BuildConfig {
        builder,
        layout,
        indexes,
    } = config;
    match builder {
        BuilderStrategy::UnsortedStream => {
            CoreStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
                quads, layout, indexes,
            )
            .await
        }
        BuilderStrategy::SortedInMemory => {
            CoreStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
                quads, layout, indexes,
            )
            .await
        }
        // Defensive: `parse_builder` never yields SortedStream, which spills to disk.
        BuilderStrategy::SortedStream => {
            return Err(JsValue::from_str(
                "The sorted-stream builder strategy is not available in WebAssembly.",
            ));
        }
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
        return Ok(BuildConfig {
            builder: parse_builder(&name)?,
            ..BuildConfig::default()
        });
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
        None => Err(JsValue::from_str(&format!(
            "Option '{}' must be a string",
            key
        ))),
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

/// A quad stream boxed for `build_array`, whichever of the two `fromQuads`
/// input shapes it came from.
type BoxedQuadStream = Box<dyn Stream<Item = CoreResult<Quad>> + Unpin + Send>;

/// Accept either shape `fromQuads` allows: a plain array (eagerly validated
/// and wrapped in `stream::iter`), or an RDF/JS `Stream<Quad>` (consumed
/// through its event-emitter interface).
fn js_to_quad_stream(value: JsValue) -> Result<BoxedQuadStream, JsValue> {
    if js_sys::Array::is_array(&value) {
        let quads = js_array_to_quads(js_sys::Array::from(&value))?;
        let stream: BoxedQuadStream = Box::new(stream::iter(
            quads.into_iter().map(Ok::<Quad, VortexRdfError>),
        ));
        return Ok(stream);
    }
    rdfjs_stream_to_quads(value)
}

/// Consume an RDF/JS `Stream<Quad>` — a Node-style event emitter with
/// `'data'`/`'end'`/`'error'` events — by registering listeners that forward
/// each event into an unbounded channel, and handing back the receiving end
/// as a plain Rust stream.
///
/// The listeners are intentionally leaked (`Closure::forget`): `fromQuads` is
/// called once per stream and the callbacks must stay valid for as long as
/// the JS source stream can still fire events, which for a one-shot event
/// listener has no natural Rust-side owner to drop them. Without an explicit
/// `close_channel()` on `'end'`, the receiver would otherwise wait forever
/// for a value that will never come.
fn rdfjs_stream_to_quads(stream_val: JsValue) -> Result<BoxedQuadStream, JsValue> {
    let on = Reflect::get(&stream_val, &"on".into())
        .ok()
        .and_then(|f| f.dyn_into::<js_sys::Function>().ok())
        .ok_or_else(|| {
            JsValue::from_str(
                "fromQuads expects an array of quads or an RDF/JS Stream \
                 (an object with an 'on' method)",
            )
        })?;

    let (tx, rx) = mpsc::unbounded::<CoreResult<Quad>>();

    let tx_data = tx.clone();
    let on_data = Closure::wrap(Box::new(move |quad_js: JsValue| {
        let item = js_to_quad(quad_js).ok_or_else(|| {
            VortexRdfError::Deserialization("Invalid quad object in RDF/JS stream".to_string())
        });
        let _ = tx_data.unbounded_send(item);
    }) as Box<dyn FnMut(JsValue)>);

    let tx_error = tx.clone();
    let on_error = Closure::wrap(Box::new(move |err: JsValue| {
        let message = err
            .as_string()
            .or_else(|| Reflect::get(&err, &"message".into()).ok()?.as_string())
            .unwrap_or_else(|| "RDF/JS stream error".to_string());
        let _ = tx_error.unbounded_send(Err(VortexRdfError::Deserialization(message)));
    }) as Box<dyn FnMut(JsValue)>);

    let tx_end = tx;
    let on_end = Closure::wrap(Box::new(move || {
        tx_end.close_channel();
    }) as Box<dyn FnMut()>);

    on.call2(&stream_val, &"data".into(), on_data.as_ref())
        .map_err(|_| JsValue::from_str("Failed to attach a 'data' listener to the stream"))?;
    on.call2(&stream_val, &"error".into(), on_error.as_ref())
        .map_err(|_| JsValue::from_str("Failed to attach an 'error' listener to the stream"))?;
    on.call2(&stream_val, &"end".into(), on_end.as_ref())
        .map_err(|_| JsValue::from_str("Failed to attach an 'end' listener to the stream"))?;

    on_data.forget();
    on_error.forget();
    on_end.forget();

    Ok(Box::new(rx))
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
    let language = Reflect::get(&val, &"language".into())
        .ok()
        .and_then(|v| v.as_string());
    let datatype_iri = Reflect::get(&val, &"datatype".into())
        .ok()
        .and_then(|dt| Reflect::get(&dt, &"value".into()).ok())
        .and_then(|v| v.as_string());
    Some(RawTerm {
        term_type,
        value,
        language,
        datatype_iri,
    })
}

fn js_to_term(val: JsValue) -> Option<Term> {
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

fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g =
        js_to_graph(Reflect::get(&val, &"graph".into()).ok()?).unwrap_or(GraphName::DefaultGraph);
    Some(Quad::new(s, p, o, g))
}
