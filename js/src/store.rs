//! The `VortexRdfStore` and `TermDict` wasm bindings, and the columnar
//! payload construction behind `match`/`getQuads` (`match_payload`).

use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;

use futures::StreamExt;
use js_sys::{Object, Reflect};
use vortex_rdf_core::common::{formats::format_from_name, terms::parse_quads_from_reader};
use vortex_rdf_core::{
    DictSnapshot, LayoutStrategy, Result as CoreResult, VortexRdfStore as CoreStore, export_rdf,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::chunks::{CodeChunks, ProbeAnswers, finished_iterable, handle_iterable, probe_codes};
use crate::codes::{code_array, code_from_js, code_to_js};
use crate::error::{js_err, js_err_ctx};
use crate::ingest::{js_array_to_dictionary_array, js_array_to_quads, js_to_quad_stream};
use crate::options::parse_build_options;
use crate::probes::{batch_signal, chunk_probe, parse_probes, read_probe};
use crate::resolve_now;
use crate::terms::{JsPattern, quad_arg};

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
    // Dropped with this handle when it is freed: the iterators the store
    // hands out (`CodeChunks`, `ProbeAnswers`) hold a `Weak` of it and refuse
    // to pull once it is gone.
    alive: Rc<()>,
}

impl VortexRdfStore {
    fn wrap(inner: CoreStore) -> Self {
        Self {
            inner,
            dict_view: RefCell::new(None),
            alive: Rc::new(()),
        }
    }

    /// The store's dictionary as a snapshot, or `None` when the code path
    /// does not apply. Core's [`dict_reader`](CoreStore::dict_reader) is the
    /// one "codes are decodable" gate (Dictionary layout, no append tail — see
    /// its doc for why anything less decodes to wrong terms), and a snapshot
    /// needs the dictionary resident, as every dictionary in this build is.
    fn dict_snapshot(&self) -> Option<DictSnapshot> {
        self.inner.dict_reader()?.snapshot()
    }

    /// The dictionary for decoding a match's code columns, or `None` when
    /// the code path does not apply (see [`dict_snapshot`](Self::dict_snapshot));
    /// reads it declines fall back to the always-correct term path.
    fn code_path_dict(&self) -> Option<JsValue> {
        let snapshot = self.dict_snapshot()?;
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

/// An immutable handle on a store's term dictionary, decoding a term code to
/// its N-Triples string. Codes are `u64` and cross as JS numbers, checked
/// both ways (see [`crate::codes`]): exact up to 2^53 − 1, never rounded or
/// wrapped.
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
    /// Decode a term code, or `undefined` when it is out of range. A value
    /// that is no code — not a number, or a number that is not an integer
    /// from 0 to 2^53 − 1 — throws. Taken as a `JsValue`, so nothing is
    /// coerced: `null`, `[]` or `false` never decodes as code 0, nor `true`,
    /// `'1'` or `[1]` as code 1.
    #[wasm_bindgen(js_name = decode)]
    pub fn decode(&self, code: JsValue) -> Result<Option<String>, JsValue> {
        let number = code.as_f64().ok_or_else(|| {
            js_err(format!(
                "a term code is a number, an integer from 0 to 2^53 - 1 \
                 (Number.MAX_SAFE_INTEGER), got {}",
                code.js_typeof().as_string().unwrap_or_default()
            ))
        })?;
        let code = code_from_js(number).map_err(js_err)?;
        Ok(self.snapshot.decode(code))
    }

    /// Encode a term to its code (inverse of [`decode`](Self::decode)), or
    /// `undefined` when this dictionary does not hold the term. Tolerant of
    /// spelling: an IRI with or without angle brackets, a literal with escape
    /// variants, an `xsd:string` typing, an upper-case language tag, or
    /// `default`/`[]` for the default graph all find the stored form's code.
    /// Throws for a malformed term, and for a code past 2^53 − 1, which no JS
    /// number holds exactly.
    #[wasm_bindgen(js_name = encode)]
    pub fn encode(&self, term: &str) -> Result<Option<f64>, JsValue> {
        self.snapshot
            .encode(term)
            .map_err(js_err)?
            .map(code_to_js)
            .transpose()
            .map_err(js_err)
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
        let format = format_from_name(format_name).map_err(js_err)?;
        let config = parse_build_options(options)?;
        let quads_stream = parse_quads_from_reader(Cursor::new(input), format);
        let inner = CoreStore::from_quads(quads_stream, config.layout, config.indexes)
            .await
            .map_err(|e| js_err_ctx("Vortex build error", e))?;
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
        let inner = CoreStore::from_quads(quad_stream, config.layout, config.indexes)
            .await
            .map_err(|e| js_err_ctx("Vortex build error", e))?;
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
        let format = format_from_name(format_name).map_err(js_err)?;
        let mut buffer = Vec::new();
        export_rdf(self.inner.clone(), &mut buffer, format)
            .await
            .map_err(|e| js_err_ctx("RDF serialization error", e))?;
        String::from_utf8(buffer).map_err(|e| js_err_ctx("UTF-8 error", e))
    }

    /// Number of quads in the store. Returns synchronously: no wasm read path
    /// performs I/O, so there is nothing to await (see `resolve_now`).
    #[wasm_bindgen(skip_typescript)]
    pub fn size(&self) -> Result<usize, JsValue> {
        resolve_now(self.inner.size())?.map_err(js_err)
    }

    /// Whether the quad is in the store. Returns synchronously; throws on a
    /// malformed quad object (`addQuad`/`deleteQuad` reject it).
    #[wasm_bindgen(skip_typescript)]
    pub fn has(&self, quad_js: JsValue) -> Result<bool, JsValue> {
        let quad = quad_arg(&quad_js)?;
        resolve_now(self.inner.contains(&quad))?.map_err(js_err)
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
        let quad = quad_arg(&quad_js)?;
        self.mutate(async |s| s.add_quad(quad).await).await
    }

    #[wasm_bindgen(js_name = addQuads, skip_typescript)]
    pub async fn add_quads(&mut self, quads_js: js_sys::Array) -> Result<(), JsValue> {
        let quads = js_array_to_quads(quads_js)?;
        self.mutate(async |s| s.add_quads(quads).await).await
    }

    #[wasm_bindgen(js_name = deleteQuad, skip_typescript)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = quad_arg(&quad_js)?;
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
    /// selection alone — no term string is materialized. The `keep`, `limit`
    /// and `offset` of `options` narrow the match as for
    /// [`matchCodes`](Self::match_codes); a `limit` stops the count once that
    /// many rows are known to exist.
    #[wasm_bindgen(js_name = countQuads, skip_typescript)]
    pub fn count_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<usize, JsValue> {
        let probe = read_probe(
            JsPattern::parse(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        resolve_now(self.inner.count_probe(&probe))?.map_err(js_err)
    }

    /// [`countQuads`](Self::count_quads) for each of `probes`, one after
    /// another in input order, as an array of numbers. Every probe is parsed
    /// before any runs; the first that fails throws, and none after it runs.
    /// Runs on every layout.
    #[wasm_bindgen(js_name = countQuadsMany, skip_typescript)]
    pub fn count_quads_many(&self, probes: JsValue) -> Result<js_sys::Array, JsValue> {
        let probes = parse_probes(&probes)?;
        let counts = js_sys::Array::new();
        for probe in &probes {
            let count = resolve_now(self.inner.count_probe(probe))?.map_err(js_err)?;
            counts.push(&JsValue::from_f64(count as f64));
        }
        Ok(counts)
    }

    /// Low-level: resolve a pattern to the matched rows' raw term codes —
    /// `{ s, p, o, g }` as `Float64Array`s of exact integer codes plus
    /// `length` — with no term strings materialized; a code past 2^53 − 1
    /// throws. `null` unless the store's rows are code-addressable
    /// (Dictionary layout, no pending appends, resident dictionary); decode
    /// codes through [`termDict`](Self::term_dict). An invalid pattern term
    /// or option throws whether or not the rows are code-addressable.
    ///
    /// `options.keep` narrows the match inside the store, before any row is
    /// gathered: per position, a code set (a `Float64Array`, or an array of
    /// codes of any length but two) or a code range (`[lo, hi]`, the
    /// half-open codes `lo <= code < hi`). `options.offset` and
    /// `options.limit` then window the kept rows in base row order.
    #[wasm_bindgen(js_name = matchCodes, skip_typescript)]
    pub fn match_codes(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<JsValue, JsValue> {
        let probe = read_probe(
            JsPattern::parse(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        // Same gate as `code_path_dict`: without a dictionary snapshot the
        // codes would not resolve via `termDict`.
        if self.dict_snapshot().is_none() {
            return Ok(JsValue::NULL);
        }
        probe_codes(&self.inner, &probe)
    }

    /// Low-level: [`matchCodes`](Self::match_codes) in chunks — an
    /// `Iterable` whose items are `{ <column>: Float64Array, …, length }` of
    /// at most `options.batchRows` rows (8,192 by default), in base row
    /// order, holding the columns `options.columns` names (all four by
    /// default) — or `null` under the same gate as `matchCodes`. This call
    /// resolves the narrowed match (`keep`, `limit`, `offset`, as for
    /// `matchCodes`); each `next()` reads or gathers one chunk's rows. An
    /// aborted `options.signal` ends the iteration at the next `next()`, and
    /// one aborted before the call makes it return a finished iterable
    /// without resolving the match. `batchRows` 0, no column or an unknown
    /// column name throws.
    #[wasm_bindgen(js_name = matchCodeChunks, skip_typescript)]
    pub fn match_code_chunks(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<JsValue, JsValue> {
        let (probe, shape) = chunk_probe(
            JsPattern::parse(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        if self.dict_snapshot().is_none() {
            return Ok(JsValue::NULL);
        }
        if shape.signal.aborted {
            return Ok(finished_iterable());
        }
        let matched = resolve_now(self.inner.run_probe(&probe))?.map_err(js_err)?;
        let stream = matched
            .code_chunks(&shape.columns, shape.batch_rows)
            .map_err(js_err)?;
        let chunks = CodeChunks::new(stream, shape.columns, Rc::downgrade(&self.alive));
        Ok(handle_iterable(chunks, &shape.signal.value))
    }

    /// Low-level: [`matchCodes`](Self::match_codes) for each of `probes`, as
    /// an `Iterable` answering one probe per `next()` in input order. A probe
    /// is an array `[s, p, o, g]` of pattern terms or an object with keys
    /// `s`, `p`, `o`, `g`, `keep`, `limit` and `offset`. Every probe is
    /// parsed before any runs; the first that fails throws from its
    /// `next()`. On a store whose rows are not code-addressable every answer
    /// is `null` and no probe runs. An aborted `options.signal` ends the
    /// iteration at the next `next()`, and one aborted before the call makes
    /// it return a finished iterable.
    #[wasm_bindgen(js_name = matchCodesMany, skip_typescript)]
    pub fn match_codes_many(
        &self,
        probes: JsValue,
        options: Option<Object>,
    ) -> Result<JsValue, JsValue> {
        let probes = parse_probes(&probes)?;
        let signal = batch_signal(options.as_deref())?;
        if signal.aborted {
            return Ok(finished_iterable());
        }
        let store = self.dict_snapshot().map(|_| self.inner.clone());
        let answers = ProbeAnswers::new(store, probes, Rc::downgrade(&self.alive));
        Ok(handle_iterable(answers, &signal.value))
    }

    /// Low-level: an immutable [`TermDict`] handle on this store's term
    /// dictionary — the one door to code↔term translation (`decode`/`encode`
    /// of N-Triples term strings: `<iri>`, `_:blank`, `"lit"@lang`,
    /// `"lit"^^<dt>`, or `""` for the default graph). `undefined` short of
    /// core's [`dict_reader`](CoreStore::dict_reader) gate and a resident
    /// snapshot (Dictionary layout, no append tail, resident dictionary). The
    /// handle keeps decoding correctly after the store is mutated, because it
    /// retains the dictionary its codes address.
    #[wasm_bindgen(js_name = termDict, skip_typescript)]
    pub fn term_dict(&self) -> Option<TermDict> {
        let snapshot = self.dict_snapshot()?;
        Some(TermDict { snapshot })
    }
}

/// Resolve a pattern and pack the matched rows into the columnar payload the JS
/// lazy read model consumes. Shared by `match` and `getQuads`.
///
/// Dictionary layout (`dict` is `Some`) ships four code columns plus the
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

    // Code payload: code columns + the shared dictionary.
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
    let mut chunks = matched.shared_quad_chunks();
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

/// Set a matched view's four code columns on `payload` under `s`/`p`/`o`/`g`,
/// as `Float64Array`s of the codes ([`code_array`]), returning the row count
/// — or `None` when codes are not that view's vocabulary at all, in which
/// case nothing is set and the caller falls back to the term path.
async fn set_code_columns(payload: &Object, matched: &CoreStore) -> Result<Option<usize>, JsValue> {
    let Some(cols) = matched.code_columns_gathered().await.map_err(js_err)? else {
        return Ok(None);
    };
    for (name, col) in ["s", "p", "o", "g"].iter().zip(cols.iter()) {
        let codes = code_array(col.as_slice())?;
        Reflect::set(payload, &(*name).into(), &codes)?;
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
