//! The `VortexRdfStore` and `TermDict` wasm bindings, and the columnar
//! payload construction behind `match`/`getQuads` (`match_payload`).

use std::cell::RefCell;
use std::io::Cursor;

use arrow_ipc::writer::StreamWriter;
use futures::StreamExt;
use js_sys::{Object, Reflect};
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::{
    DictSnapshot, LayoutStrategy, Result as CoreResult, VortexRdfStore as CoreStore, export_rdf,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::error::{js_err, js_err_ctx};
use crate::ingest::{js_array_to_dictionary_array, js_array_to_quads, js_to_quad_stream};
use crate::options::{build_array, parse_arrow_options, parse_build_options, parse_format};
use crate::terms::{JsPattern, js_to_quad};

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

    /// Takes ownership of the buffer wasm-bindgen marshalled from the caller's
    /// `Uint8Array`, so the load holds a single copy of the bytes.
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

        // Array + Dictionary layout: push each decoded quad straight into the
        // interning sink so no `Vec<RawQuad>` of the whole array is ever built.
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

    /// The secondary indexes this store was built with, as their canonical
    /// kebab-case names.
    #[wasm_bindgen(skip_typescript)]
    pub fn indexes(&self) -> Vec<String> {
        self.inner
            .indexes()
            .iter()
            .map(|index| index.to_string())
            .collect()
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
        export_rdf(self.inner.clone(), &mut buffer, format)
            .await
            .map_err(|e| js_err_ctx("RDF serialization error", e))?;
        String::from_utf8(buffer).map_err(|e| js_err_ctx("UTF-8 error", e))
    }

    #[wasm_bindgen(skip_typescript)]
    pub async fn size(&self) -> Result<usize, JsValue> {
        self.inner.size().await.map_err(js_err)
    }

    /// Whether the quad is in the store. Rejects on a malformed quad object,
    /// like `addQuad`/`deleteQuad`.
    #[wasm_bindgen(skip_typescript)]
    pub async fn has(&self, quad_js: JsValue) -> Result<bool, JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| js_err("Invalid quad object"))?;
        self.inner.contains(&quad).await.map_err(js_err)
    }

    /// The inner store as one owning its rows, ready for in-place mutation;
    /// `add*`/`delete*` replace `self.inner` with the result (RDF/JS
    /// `DatasetCore` mutates in place).
    async fn owned(&self) -> Result<CoreStore, JsValue> {
        self.inner.owned().await.map_err(js_err)
    }

    /// Apply one core mutation to the owned store and install the result.
    ///
    /// The dictionary may have changed (auto-compaction re-encodes), so the
    /// cached `LazyDict` view is dropped and the next read snapshots the new
    /// one; any `LazyQuad` already handed out keeps the snapshot its codes
    /// address alive.
    async fn mutate(
        &mut self,
        op: impl AsyncFnOnce(&CoreStore) -> CoreResult<CoreStore>,
    ) -> Result<(), JsValue> {
        let owned = self.owned().await?;
        self.inner = op(&owned).await.map_err(js_err)?;
        self.dict_view.replace(None);
        Ok(())
    }

    #[wasm_bindgen(js_name = addQuad, skip_typescript)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| js_err("Invalid quad object"))?;
        self.mutate(async |s| s.add_quad(quad).await).await
    }

    #[wasm_bindgen(js_name = addQuads, skip_typescript)]
    pub async fn add_quads(&mut self, quads_js: js_sys::Array) -> Result<(), JsValue> {
        let quads = js_array_to_quads(quads_js)?;
        self.mutate(async |s| s.add_quads(quads).await).await
    }

    #[wasm_bindgen(js_name = deleteQuad, skip_typescript)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = js_to_quad(quad_js).ok_or_else(|| js_err("Invalid quad object"))?;
        self.mutate(async |s| s.delete_quad(&quad).await).await
    }

    /// RDF/JS `Source.match`: stream the quads matching a pattern as a
    /// `Stream<Quad>` of lazy, zero-copy `LazyQuad`s.
    ///
    /// Returns synchronously. The pattern is resolved lazily inside a `Promise`
    /// that yields a columnar payload, handed to a minimal RDF/JS `Stream`
    /// (`.on('data'|'end'|'error', …)`, `.read()`, and — as a convenience —
    /// `Symbol.asyncIterator` for `for await`). No term strings are materialized
    /// until a `LazyTerm`'s `.value`/`.termType` is read. An invalid pattern
    /// term throws synchronously.
    #[wasm_bindgen(js_name = match, skip_typescript)]
    pub fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<JsValue, JsValue> {
        // Parse the pattern eagerly (cheap, synchronous) so only owned oxrdf
        // terms — not JsValues — are moved into the resolving future.
        let pattern = JsPattern::parse(subject, predicate, object, graph)?;
        // Ensure the shared dictionary view synchronously (Dictionary layout);
        // it is not dependent on the matched rows and must be built off `self`.
        let dict = self.code_path_dict();
        let inner = self.inner.clone();
        let promise = future_to_promise(async move { match_payload(inner, dict, pattern).await });
        Ok(make_lazy_quad_stream(&promise.into()))
    }

    /// Materialize the quads matching a pattern into a `LazyQuad[]` — the
    /// array-returning counterpart of [`match`](Self::match_pattern).
    ///
    /// Returns synchronously: no wasm read path performs I/O, so there is
    /// nothing to await (see `resolve_now`). The quads still decode their
    /// term strings lazily on access.
    #[wasm_bindgen(js_name = getQuads, skip_typescript)]
    pub fn get_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<js_sys::Array, JsValue> {
        let pattern = JsPattern::parse(subject, predicate, object, graph)?;
        let dict = self.code_path_dict();
        let payload = resolve_now(match_payload(self.inner.clone(), dict, pattern))??;
        Ok(build_lazy_quads(&payload))
    }

    /// Number of quads matching a pattern, counted from the match's row
    /// selection alone — no term string is materialized.
    #[wasm_bindgen(js_name = countQuads, skip_typescript)]
    pub fn count_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<usize, JsValue> {
        let pattern = JsPattern::parse(subject, predicate, object, graph)?;
        resolve_now(async move {
            let view = pattern.matched(&self.inner).await?;
            view.size().await.map_err(js_err)
        })?
    }

    /// Low-level: the quads matching a pattern as an Arrow IPC stream
    /// (`Uint8Array`) — the bytes `apache-arrow`'s `tableFromIPC`,
    /// DuckDB-WASM, Arquero or Perspective read. One record batch per decode
    /// chunk over core's quad schema: columns `s`, `p`, `o`, `g`, or
    /// `options.projection` in that order; `options.encoding` selects the
    /// cell type (`codes` u32 term codes, `terms` the codes as dictionary
    /// keys over the whole term dictionary, `strings` N-Triples strings).
    /// The bytes are a copy out of wasm memory, as the `Uint32Array`s of the
    /// lazy quad payload are. Throws on an invalid pattern term or option,
    /// and on an encoding the layout cannot serve.
    #[wasm_bindgen(js_name = matchArrowIPC, skip_typescript)]
    pub fn match_arrow_ipc(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: JsValue,
    ) -> Result<Vec<u8>, JsValue> {
        let pattern = JsPattern::parse(subject, predicate, object, graph)?;
        let (encoding, projection) = parse_arrow_options(options)?;
        resolve_now(async move {
            let matched = pattern.matched(&self.inner).await?;
            let mut batches = matched
                .to_record_batches(encoding, projection.as_deref())
                .await
                .map_err(js_err)?;
            let mut writer = StreamWriter::try_new(Vec::new(), &batches.schema()).map_err(js_err)?;
            while let Some(batch) = batches.next().await {
                writer.write(&batch.map_err(js_err)?).map_err(js_err)?;
            }
            writer.finish().map_err(js_err)?;
            writer.into_inner().map_err(js_err)
        })?
    }

    /// Low-level: an immutable [`TermDict`] handle on this store's term
    /// dictionary — the one door to code↔term translation (`decode`/`encode`
    /// of N-Triples term strings: `<iri>`, `_:blank`, `"lit"@lang`,
    /// `"lit"^^<dt>`, or `""` for the default graph). `undefined` short of
    /// core's code-read gate ([`code_read_snapshot`](CoreStore::code_read_snapshot):
    /// Dictionary layout, no append tail, resident dictionary). The handle
    /// keeps decoding correctly after the store is mutated, because it retains
    /// the dictionary its codes address.
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
/// already resolved; a future that did suspend surfaces as an error instead
/// of a hang.
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
/// N-Triples term columns (`{offsets, bytes}`) filled from
/// `shared_quad_chunks()`.
async fn match_payload(
    store: CoreStore,
    dict: Option<JsValue>,
    pattern: JsPattern,
) -> Result<JsValue, JsValue> {
    let matched = pattern.matched(&store).await?;
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
    // cached dictionary. The shared decode hands each term over in its
    // N-Triples spelling already (the default graph as the empty string, this
    // payload's vocabulary for it), so the bytes go straight into the
    // column — no parse into oxrdf terms, no re-render.
    let mut chunks = matched.shared_quad_chunks().map_err(js_err)?;
    // (offsets seeded with a leading 0, bytes) per s/p/o/g column.
    let mut cols: [(Vec<u32>, Vec<u8>); 4] = [
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
        (vec![0], Vec::new()),
    ];
    let mut n = 0u32;
    while let Some(chunk) = chunks.next().await {
        for row in chunk {
            let row = row.map_err(js_err)?;
            for (col, term) in cols.iter_mut().zip([&row.s, &row.p, &row.o, &row.g]) {
                col.1.extend_from_slice(term.as_bytes());
                col.0.push(col.1.len() as u32);
            }
            n += 1;
        }
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
