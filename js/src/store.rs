//! The `VortexRdfStore` and `TermDict` wasm bindings, and the columnar
//! payload construction behind `match`/`getQuads` (`match_columns`).

use std::cell::RefCell;
use std::io::{Cursor, Write};

use futures::StreamExt;
use js_sys::{Object, Reflect};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_rdf_core::common::export::export_rdf;
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::{DictSnapshot, LayoutStrategy, VortexRdfStore as CoreStore};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::error::{js_err, js_err_ctx};
use crate::ingest::{js_array_to_dictionary_array, js_array_to_quads, js_to_quad_stream};
use crate::options::{build_array, parse_build_options, parse_format};
use crate::terms::{js_to_graph, js_to_named_node, js_to_quad, js_to_subject, js_to_term};

// The lazy RDF/JS read model (LazyQuad/LazyTerm + stream) lives in a local
// snippet (copied verbatim into the generated pkg; no runtime npm dependency).
// See js/js-snippets/lazy-rdf.js.
#[wasm_bindgen(module = "/js-snippets/lazy-rdf.js")]
extern "C" {
    /// Wrap a `TermDict` handle into a `LazyDict`, which decodes a term code to
    /// its string on demand and interns the result. Built once per store read.
    #[wasm_bindgen(js_name = makeDictView)]
    fn make_dict_view(dict: TermDict) -> JsValue;

    /// Build a `LazyQuad[]` from a column payload — for `getQuads`.
    #[wasm_bindgen(js_name = buildLazyQuads)]
    fn build_lazy_quads(payload: &JsValue) -> js_sys::Array;

    /// Build a `Stream<LazyQuad>` from a `Promise<payload>` — so `match` returns
    /// synchronously while resolving its rows lazily.
    #[wasm_bindgen(js_name = makeLazyQuadStream)]
    fn make_lazy_quad_stream(payload_promise: &JsValue) -> JsValue;

}

// ─── VortexRdfStore ─────────────────────────────────────────────────────────────

#[wasm_bindgen(skip_typescript)]
pub struct VortexRdfStore {
    inner: CoreStore,
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
    /// the code path does not apply. Core's
    /// [`code_read_snapshot`](CoreStore::code_read_snapshot) is the one
    /// "codes are decodable" gate (Dictionary layout, no append tail, resident
    /// dictionary — see its doc for why anything less decodes to wrong terms);
    /// reads it declines fall back to the always-correct term path.
    fn code_path_dict(&self) -> Option<JsValue> {
        let snapshot = self.inner.code_read_snapshot()?;
        Some(self.dict_view(snapshot))
    }

    /// The store's `LazyDict` over `snapshot`, built once and cached.
    ///
    /// The `LazyDict` holds a [`DictSnapshot`] and decodes each code the first
    /// time it is observed, interning the result — so a query pays one boundary
    /// crossing per *distinct* term it actually reads, and a query that only
    /// counts rows or compares terms by code pays none at all. Building it is
    /// O(1): nothing is flattened or copied up front.
    ///
    /// Because the snapshot is immutable, `LazyQuad`s handed out before a
    /// mutation keep decoding against the dictionary their codes address, even
    /// though `self.dict_view` is dropped so later reads pick up the new one.
    fn dict_view(&self, snapshot: DictSnapshot) -> JsValue {
        if let Some(dv) = self.dict_view.borrow().as_ref() {
            return dv.clone();
        }
        let dv = make_dict_view(TermDict { snapshot });
        *self.dict_view.borrow_mut() = Some(dv.clone());
        dv
    }
}

/// An immutable handle on a store's term dictionary, decoding a `u32` term code
/// to its N-Triples string.
///
/// Handed to the JS lazy read model so that codes produced by one read stay
/// decodable after the store is mutated — a mutation re-encodes the store
/// against a fresh dictionary, which would otherwise silently resolve old codes
/// to the wrong terms. Retains the dictionary only, not the store's quad data.
#[wasm_bindgen(skip_typescript)]
pub struct TermDict {
    snapshot: DictSnapshot,
}

#[wasm_bindgen]
impl TermDict {
    /// Decode a term code, or `undefined` when it is out of range.
    #[wasm_bindgen(js_name = decode)]
    pub fn decode(&self, code: u32) -> Option<String> {
        self.snapshot.decode(code)
    }

    /// Encode an N-Triples term string to its code (inverse of
    /// [`decode`](Self::decode)), or `undefined` when this dictionary does
    /// not hold the term.
    #[wasm_bindgen(js_name = encode)]
    pub fn encode(&self, term: &str) -> Option<u32> {
        self.snapshot.encode(term)
    }
}

#[wasm_bindgen]
impl VortexRdfStore {
    #[wasm_bindgen(skip_typescript)]
    pub fn empty() -> VortexRdfStore {
        VortexRdfStore::wrap(CoreStore::empty())
    }

    /// Taking `Vec<u8>` makes wasm-bindgen hand over ownership of the buffer
    /// it marshalled from the caller's `Uint8Array`, so the whole load pays
    /// exactly one copy — the unavoidable JS→wasm boundary crossing. A
    /// borrowed `&[u8]` here would anchor that marshalled buffer for the
    /// whole async decode while core copied it again: 2x file size of
    /// transient high-water, which wasm linear memory never gives back.
    #[wasm_bindgen(js_name = fromBytes, skip_typescript)]
    pub async fn from_bytes(bytes: Vec<u8>) -> Result<VortexRdfStore, JsValue> {
        let inner = CoreStore::from_bytes_owned(bytes).await.map_err(js_err)?;
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
        let built = build_array(quads_stream, config).await?;

        let inner = CoreStore::from_built(built).map_err(js_err)?;
        Ok(VortexRdfStore::wrap(inner))
    }

    /// Build directly from RDF/JS quads — either an array or a `Stream<Quad>`
    /// (a Node-style event emitter) — skipping a serialize/parse round-trip.
    #[wasm_bindgen(js_name = fromQuads, skip_typescript)]
    pub async fn from_quads(quads: JsValue, options: JsValue) -> Result<VortexRdfStore, JsValue> {
        let config = parse_build_options(options)?;

        // Array + Dictionary layout: push each quad straight into the
        // interning sink as its packed chunk is decoded. The stream path
        // below would first collect the whole array into a `Vec<RawQuad>`
        // (a `'static` stream cannot borrow from the decode loop), putting
        // four owned Strings per quad on the ingest high-water mark.
        if config.layout == LayoutStrategy::Dictionary && js_sys::Array::is_array(&quads) {
            let built = js_array_to_dictionary_array(js_sys::Array::from(&quads), config.indexes)?;
            let inner = CoreStore::from_built(built).map_err(js_err)?;
            return Ok(VortexRdfStore::wrap(inner));
        }

        let quad_stream = js_to_quad_stream(quads)?;
        let built = build_array(quad_stream, config).await?;

        let inner = CoreStore::from_built(built).map_err(js_err)?;
        Ok(VortexRdfStore::wrap(inner))
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn layout(&self) -> String {
        // Core's Display: the canonical kebab-case name every frontend reports.
        self.inner.layout().to_string()
    }

    #[wasm_bindgen(js_name = toBytes, skip_typescript)]
    pub async fn to_bytes(&self) -> Result<Vec<u8>, JsValue> {
        // Complete native-container bytes: the quad table is the transparent
        // root child and, under the Dictionary layout, the FSST-compressed
        // term dictionary and index copies ride as auxiliary children, so the
        // bytes are self-describing and `fromBytes` (or a native `from_file`
        // after writing them to disk) reads them back.
        self.inner
            .to_bytes()
            .await
            .map_err(|e| js_err_ctx("Vortex serialization error", e))
    }

    #[wasm_bindgen(js_name = toRdf, skip_typescript)]
    pub async fn to_rdf(&self, format_name: &str) -> Result<String, JsValue> {
        let format = parse_format(format_name)?;
        let mut buffer = Vec::new();
        // Serialize through this store's own resolved layout, so a store derived
        // from `match` still decodes against the term dictionary it carries.
        export_rdf(self.inner.clone(), &mut buffer, format)
            .await
            .map_err(|e| js_err_ctx("Deserialize error", e))?;
        String::from_utf8(buffer).map_err(|e| js_err_ctx("UTF-8 error", e))
    }

    #[wasm_bindgen(skip_typescript)]
    pub async fn size(&self) -> Result<usize, JsValue> {
        self.inner.size().await.map_err(js_err)
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
        self.inner.owned().await.map_err(js_err)
    }

    #[wasm_bindgen(js_name = addQuad, skip_typescript)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| js_err("Invalid quad object"))?;
        self.inner = self.owned().await?.add_quad(quad).await.map_err(js_err)?;
        // The dictionary may have changed (auto-compaction re-encodes); drop the
        // cached view so the next read takes a snapshot of the new one. Rebuilding
        // is O(1), and any `LazyQuad` already handed out keeps the snapshot its
        // codes address alive, so it still decodes correctly.
        self.dict_view.replace(None);
        Ok(())
    }

    #[wasm_bindgen(js_name = addQuads, skip_typescript)]
    pub async fn add_quads(&mut self, quads_js: js_sys::Array) -> Result<(), JsValue> {
        let quads = js_array_to_quads(quads_js)?;
        self.inner = self.owned().await?.add_quads(quads).await.map_err(js_err)?;
        self.dict_view.replace(None);
        Ok(())
    }

    #[wasm_bindgen(js_name = deleteQuad, skip_typescript)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| js_err("Invalid quad object"))?;
        self.inner = self
            .owned()
            .await?
            .delete_quad(&quad)
            .await
            .map_err(js_err)?;
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
    ///
    /// Returns synchronously: no wasm read path performs I/O, so there is
    /// nothing to await (see [`resolve_now`]). The quads still decode their
    /// term strings lazily on access.
    #[wasm_bindgen(js_name = getQuads, skip_typescript)]
    pub fn get_quads(
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
        let payload = resolve_now(match_columns(self.inner.clone(), dict, s, p, o, g))??;
        Ok(build_lazy_quads(&payload))
    }

    /// **Prototype (Dictionary layout only).** Resolve a pattern and hand back
    /// the matched rows as raw `u32` term codes — four `Uint32Array` columns
    /// `{ s, p, o, g, length }`, or `null` unless this store is Dictionary
    /// layout with no append tail. No term strings are materialized; the caller
    /// resolves codes to terms lazily through the [`termDict`](Self::term_dict)
    /// handle. This is the zero-copy-until-observed read path being evaluated
    /// against `getQuads`, which builds its own columnar payload rather than
    /// this one.
    #[wasm_bindgen(js_name = matchCodes, skip_typescript)]
    pub fn match_codes(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<JsValue, JsValue> {
        // Codes are only meaningful against the store's cached dictionary.
        // Core's `code_read_snapshot` is the one gate for that (Dictionary
        // layout, no append tail — appends re-encode against a fresh
        // dictionary — and a resident snapshot); short of it, codes would not
        // resolve via `termDict`, so report the code read model unavailable.
        if self.inner.code_read_snapshot().is_none() {
            return Ok(JsValue::NULL);
        }
        let s = js_to_subject(subject);
        let p = js_to_named_node(predicate);
        let o = js_to_term(object);
        let g = js_to_graph(graph);
        let matched =
            resolve_now(
                self.inner
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref()),
            )?
            .map_err(js_err)?;

        let result = Object::new();
        let Some(n) = resolve_now(set_code_columns(&result, &matched))?? else {
            return Ok(JsValue::NULL);
        };
        Reflect::set(&result, &"length".into(), &JsValue::from_f64(n as f64))?;
        Ok(result.into())
    }

    /// **Prototype.** An immutable [`TermDict`] handle on this store's term
    /// dictionary — the one door to code↔term translation (`decode`/`encode`
    /// of N-Triples term strings: `<iri>`, `_:blank`, `"lit"@lang`,
    /// `"lit"^^<dt>`, or `""` for the default graph). `undefined` short of
    /// core's code-read gate ([`code_read_snapshot`](CoreStore::code_read_snapshot):
    /// Dictionary layout, no append tail, resident dictionary). The handle
    /// stays valid — and keeps decoding correctly — after the store is
    /// mutated, because it retains the dictionary its codes address.
    #[wasm_bindgen(js_name = termDict, skip_typescript)]
    pub fn term_dict(&self) -> Option<TermDict> {
        let snapshot = self.inner.code_read_snapshot()?;
        Some(TermDict { snapshot })
    }
}

/// Drive a read future to completion without suspending, or `Err` if it would
/// have suspended.
///
/// The read paths are `async` because a file-backed store resolves its rows
/// (and its dictionary) through I/O — but this crate builds core with
/// `default-features = false`, so `file-io` is compiled out and no
/// `QuadsSource::File` exists here. Every await in a wasm read is therefore
/// already resolved, and wrapping one in a `Promise` only buys the caller a
/// microtask turn. Polling once is exactly that reasoning made checkable: a
/// future that did suspend surfaces as an error instead of a hang.
fn resolve_now<F: std::future::Future>(future: F) -> Result<F::Output, JsValue> {
    use futures::FutureExt;

    future
        .now_or_never()
        .ok_or_else(|| js_err("read suspended: no wasm read path performs I/O"))
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
        .map_err(js_err)?;
    let payload = Object::new();

    // Code payload: u32 columns + the shared dictionary.
    if let Some(dict) = dict
        && let Some(n) = set_code_columns(&payload, &matched).await?
    {
        Reflect::set(&payload, &"kind".into(), &"code".into())?;
        Reflect::set(&payload, &"dict".into(), &dict)?;
        Reflect::set(&payload, &"length".into(), &JsValue::from_f64(n as f64))?;
        return Ok(payload.into());
    }

    // Term payload: packed N-Triples term columns — the always-correct path,
    // taken whenever the rows cannot be described as codes against the store's
    // cached dictionary.
    let mut quads_stream = matched.quads().map_err(js_err)?;
    // (offsets seeded with a leading 0, bytes) per s/p/o/g column.
    let mut cols: [(Vec<u32>, Vec<u8>); 4] = [
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
    ];
    let mut n = 0u32;
    while let Some(q_res) = quads_stream.next().await {
        let q = q_res.map_err(js_err)?;
        // Each term's N-Triples form is written straight into its column's
        // byte buffer (oxrdf terms `Display` as N-Triples) — no per-term
        // String transient. The default graph is the empty string in this
        // payload's vocabulary, not the "DEFAULT" its `Display` prints.
        let terms: [&dyn std::fmt::Display; 4] = [
            &q.subject,
            &q.predicate,
            &q.object,
            match &q.graph_name {
                GraphName::DefaultGraph => &"",
                other => other,
            },
        ];
        for (col, term) in cols.iter_mut().zip(terms) {
            write!(col.1, "{term}").expect("writing to a Vec<u8> cannot fail");
            col.0.push(col.1.len() as u32);
        }
        n += 1;
    }
    Reflect::set(&payload, &"kind".into(), &"term".into())?;
    for (name, (offsets, bytes)) in ["s", "p", "o", "g"].iter().zip(cols.iter()) {
        Reflect::set(&payload, &(*name).into(), &term_column(offsets, bytes))?;
    }
    Reflect::set(&payload, &"length".into(), &JsValue::from_f64(n as f64))?;
    Ok(payload.into())
}

/// Set a matched view's four `u32` code columns on `payload` under `s`/`p`/`o`/
/// `g`, returning the row count — or `None` when codes are not that view's
/// vocabulary at all, in which case nothing is set and the caller falls back to
/// the term path.
async fn set_code_columns(payload: &Object, matched: &CoreStore) -> Result<Option<usize>, JsValue> {
    let Some(cols) = matched.code_columns_gathered().await.map_err(js_err)? else {
        return Ok(None);
    };
    for (name, col) in ["s", "p", "o", "g"].iter().zip(cols.iter()) {
        // Copy into a JS-owned Uint32Array (safe against wasm memory growth,
        // which would detach a zero-copy view).
        let ta = js_sys::Uint32Array::new_with_length(col.len() as u32);
        ta.copy_from(col);
        Reflect::set(payload, &(*name).into(), &ta)?;
    }
    Ok(Some(cols[0].len()))
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
