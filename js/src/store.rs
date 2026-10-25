//! The `VortexRdfStore` and `TermDict` wasm bindings, and the columnar
//! payload construction behind `match`/`getQuads` (`match_payload`).
//!
//! Every `pub` method here is exported to JS under its `js_name`, or under
//! its own name without one. api.d.ts, the published type surface, documents
//! what each does; the docs here say only what the Rust side adds. The reads
//! are synchronous although core's are `async`: no wasm read path performs
//! I/O (see `resolve_now`), and only `match` defers its read, into a promise.

use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;

use futures::{FutureExt, Stream, StreamExt, stream};
use js_sys::{Array, Object, Reflect};
use vortex_rdf_core::common::{formats::format_from_name, terms::parse_quads_from_reader};
use vortex_rdf_core::{
    DictSnapshot, LayoutStrategy, Probe, QuadColumn, RawQuad, Result as CoreResult,
    VortexRdfStore as CoreStore, export_rdf,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::chunks::{
    CodeChunks, ProbeAnswers, code_columns, finished_iterable, handle_iterable, probe_codes,
};
use crate::codes::{code_from_js, code_to_js};
use crate::error::{js_err, js_err_ctx};
use crate::ingest::{
    js_array_to_dictionary_array, js_array_to_quads, js_array_to_raw_quads, rdfjs_stream_to_quads,
};
use crate::options::{BuildConfig, parse_build_options};
use crate::probes::{batch_signal, chunk_probe, parse_probes, read_probe};
use crate::resolve_now;
use crate::terms::{pattern_probe, quad_arg};

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

    /// Build a store from a quad stream under `config`, labelling a failure
    /// as the build's. `fromString` builds through here, and so does
    /// `fromQuads` but for a Dictionary-layout array, which goes through the
    /// interning sink.
    async fn build(
        quads: impl Stream<Item = CoreResult<RawQuad>> + Unpin + Send + 'static,
        config: BuildConfig,
    ) -> Result<Self, JsValue> {
        let inner = CoreStore::from_quads(quads, config.layout, config.indexes)
            .await
            .map_err(|e| js_err_ctx("Vortex build error", e))?;
        Ok(Self::wrap(inner))
    }

    /// The store's dictionary as a snapshot, or `None` when the code path
    /// does not apply. Core's [`dict_reader`](CoreStore::dict_reader) is the
    /// one "codes are decodable" gate (Dictionary layout, no append tail — see
    /// its doc for why anything less decodes to wrong terms), and a snapshot
    /// needs the dictionary resident, as every dictionary in this build is.
    fn dict_snapshot(&self) -> Option<DictSnapshot> {
        self.inner.dict_reader()?.snapshot()
    }

    /// The store's `LazyDict`, which decodes a match's code columns, built
    /// once over a snapshot and cached; `None` when the code path does not
    /// apply (see [`dict_snapshot`](Self::dict_snapshot)), and reads it
    /// declines fall back to the always-correct term path.
    ///
    /// The `LazyDict` holds a [`DictSnapshot`] and decodes each code the first
    /// time it is observed, interning the result — so a query pays one boundary
    /// crossing per *distinct* term it actually reads, and a query that only
    /// counts rows or compares terms by code pays none at all. Building it is
    /// O(1): nothing is flattened or copied up front. `makeDictView` only
    /// constructs it and never calls back into the store, so the cache can
    /// stay borrowed across that call.
    ///
    /// Because the snapshot is immutable, `LazyQuad`s handed out before a
    /// mutation keep decoding against the dictionary their codes address, even
    /// though `self.dict_view` is dropped so later reads pick up the new one.
    fn lazy_dict(&self) -> Option<JsValue> {
        let snapshot = self.dict_snapshot()?;
        let mut cached = self.dict_view.borrow_mut();
        let view = cached.get_or_insert_with(|| make_dict_view(TermDict { snapshot }));
        Some(view.clone())
    }
}

/// An immutable snapshot of a store's term dictionary, the JS `TermDict`. Its
/// codes cross as JS numbers, checked both ways by `crate::codes`.
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
    /// Takes the code as a `JsValue`, not an `f64`, so that nothing is
    /// coerced: `null`, `[]` or `false` never decodes as code 0, nor `true`,
    /// `'1'` or `[1]` as code 1.
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

    /// Core's [`DictSnapshot::encode`], whose code must also fit a JS number.
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
    pub fn empty() -> VortexRdfStore {
        VortexRdfStore::wrap(CoreStore::empty())
    }

    /// Takes ownership of the buffer wasm-bindgen marshalled from the caller's
    /// `Uint8Array`, so the load holds a single copy of the bytes.
    #[wasm_bindgen(js_name = fromBytes)]
    pub async fn from_bytes(bytes: Vec<u8>) -> Result<VortexRdfStore, JsValue> {
        let inner = CoreStore::from_bytes_owned(bytes).await.map_err(js_err)?;
        Ok(VortexRdfStore::wrap(inner))
    }

    #[wasm_bindgen(js_name = fromString)]
    pub async fn from_string(
        input: String,
        format_name: &str,
        options: JsValue,
    ) -> Result<VortexRdfStore, JsValue> {
        let format = format_from_name(format_name).map_err(js_err)?;
        let config = parse_build_options(options)?;
        Self::build(parse_quads_from_reader(Cursor::new(input), format), config).await
    }

    #[wasm_bindgen(js_name = fromQuads)]
    pub async fn from_quads(quads: JsValue, options: JsValue) -> Result<VortexRdfStore, JsValue> {
        let config = parse_build_options(options)?;
        if !Array::is_array(&quads) {
            return Self::build(rdfjs_stream_to_quads(quads)?, config).await;
        }
        let quads: Array = quads.unchecked_into();
        // Dictionary layout: push each decoded quad straight into the
        // interning sink so no `Vec<RawQuad>` of the whole array is ever built.
        if config.layout == LayoutStrategy::Dictionary {
            let built = js_array_to_dictionary_array(&quads, config.indexes)?;
            return Ok(Self::wrap(CoreStore::from_built(built).map_err(js_err)?));
        }
        let quads = js_array_to_raw_quads(&quads)?;
        Self::build(stream::iter(quads.into_iter().map(Ok)), config).await
    }

    pub fn layout(&self) -> String {
        // Core's Display: the canonical kebab-case name every frontend reports.
        self.inner.layout().to_string()
    }

    pub fn indexes(&self) -> Vec<String> {
        self.inner
            .indexes()
            .iter()
            .map(|index| index.to_string())
            .collect()
    }

    #[wasm_bindgen(js_name = toBytes)]
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

    #[wasm_bindgen(js_name = toRdf)]
    pub async fn to_rdf(&self, format_name: &str) -> Result<String, JsValue> {
        let format = format_from_name(format_name).map_err(js_err)?;
        let mut buffer = Vec::new();
        export_rdf(self.inner.clone(), &mut buffer, format)
            .await
            .map_err(|e| js_err_ctx("RDF serialization error", e))?;
        String::from_utf8(buffer).map_err(|e| js_err_ctx("UTF-8 error", e))
    }

    pub fn size(&self) -> Result<usize, JsValue> {
        resolve_now(self.inner.size())
    }

    pub fn has(&self, quad_js: JsValue) -> Result<bool, JsValue> {
        let quad = quad_arg(&quad_js)?;
        resolve_now(self.inner.contains(&quad))
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

    #[wasm_bindgen(js_name = addQuad)]
    pub async fn add_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = quad_arg(&quad_js)?;
        self.mutate(async |s| s.add_quad(quad).await).await
    }

    #[wasm_bindgen(js_name = addQuads)]
    pub async fn add_quads(&mut self, quads_js: js_sys::Array) -> Result<(), JsValue> {
        let quads = js_array_to_quads(&quads_js)?;
        self.mutate(async |s| s.add_quads(quads).await).await
    }

    #[wasm_bindgen(js_name = deleteQuad)]
    pub async fn delete_quad(&mut self, quad_js: JsValue) -> Result<(), JsValue> {
        let quad = quad_arg(&quad_js)?;
        self.mutate(async |s| s.delete_quad(&quad).await).await
    }

    /// The pattern resolves in a promise of its columnar payload
    /// (`match_payload`), which lazy-rdf.js's `makeLazyQuadStream` serves as
    /// the RDF/JS stream.
    #[wasm_bindgen(js_name = match)]
    pub fn match_pattern(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<JsValue, JsValue> {
        // Parse the pattern eagerly (cheap, synchronous) so only owned oxrdf
        // terms — not JsValues — are moved into the resolving future.
        let probe = pattern_probe(subject, predicate, object, graph)?;
        // Ensure the shared dictionary view synchronously (Dictionary layout);
        // it is not dependent on the matched rows and must be built off `self`.
        let dict = self.lazy_dict();
        let inner = self.inner.clone();
        // The read is synchronous like every other, but runs in the promise's
        // task, after `match` has returned: a failure is the stream's 'error'
        // event, not a throw.
        let promise = future_to_promise(async move { match_payload(&inner, dict, &probe) });
        Ok(make_lazy_quad_stream(&promise.into()))
    }

    /// The payload [`match`](Self::match_pattern) resolves (`match_payload`),
    /// built now and made lazy quads by lazy-rdf.js's `buildLazyQuads`.
    #[wasm_bindgen(js_name = getQuads)]
    pub fn get_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
    ) -> Result<js_sys::Array, JsValue> {
        let probe = pattern_probe(subject, predicate, object, graph)?;
        let payload = match_payload(&self.inner, self.lazy_dict(), &probe)?;
        Ok(build_lazy_quads(&payload))
    }

    #[wasm_bindgen(js_name = countQuads)]
    pub fn count_quads(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<usize, JsValue> {
        let probe = read_probe(
            pattern_probe(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        resolve_now(self.inner.count_probe(&probe))
    }

    /// Core's `count_many` joins the counts, which polls them in input order
    /// and stops at the first error; each completes on its first poll here
    /// (see `resolve_now`), so the probes run one after another and none after
    /// a failing one runs.
    #[wasm_bindgen(js_name = countQuadsMany)]
    pub fn count_quads_many(&self, probes: JsValue) -> Result<js_sys::Array, JsValue> {
        let counts = resolve_now(self.inner.count_many(&parse_probes(&probes)?))?;
        Ok(counts
            .into_iter()
            .map(|count| JsValue::from_f64(count as f64))
            .collect())
    }

    #[wasm_bindgen(js_name = matchCodes)]
    pub fn match_codes(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<JsValue, JsValue> {
        let probe = read_probe(
            pattern_probe(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        // Same gate as `lazy_dict`: without a dictionary snapshot the codes
        // would not resolve via `termDict`.
        if self.dict_snapshot().is_none() {
            return Ok(JsValue::NULL);
        }
        probe_codes(&self.inner, &probe)
    }

    #[wasm_bindgen(js_name = matchCodeChunks)]
    pub fn match_code_chunks(
        &self,
        subject: JsValue,
        predicate: JsValue,
        object: JsValue,
        graph: JsValue,
        options: Option<Object>,
    ) -> Result<JsValue, JsValue> {
        let (probe, shape) = chunk_probe(
            pattern_probe(subject, predicate, object, graph)?,
            options.as_deref(),
        )?;
        if self.dict_snapshot().is_none() {
            return Ok(JsValue::NULL);
        }
        if shape.signal.aborted {
            return Ok(finished_iterable());
        }
        let matched = resolve_now(self.inner.run_probe(&probe))?;
        let stream = matched
            .code_chunks(&shape.columns, shape.batch_rows)
            .map_err(js_err)?;
        let chunks = CodeChunks::new(stream, shape.columns, Rc::downgrade(&self.alive));
        Ok(handle_iterable(chunks, &shape.signal.value))
    }

    #[wasm_bindgen(js_name = matchCodesMany)]
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

    /// A [`TermDict`] over `dict_snapshot`, the gate of every code read.
    #[wasm_bindgen(js_name = termDict)]
    pub fn term_dict(&self) -> Option<TermDict> {
        let snapshot = self.dict_snapshot()?;
        Some(TermDict { snapshot })
    }
}

/// Resolve a pattern and pack the matched rows into the columnar payload the JS
/// lazy read model consumes. Shared by `match` and `getQuads`.
///
/// With the store's `LazyDict` (`dict` is `Some`), the payload is the matched
/// rows' code columns as `matchCodes` answers them ([`code_columns`]) plus
/// `dict`: no term strings are touched. Otherwise, or when codes are not the
/// matched view's vocabulary, it is the [`term_payload`].
fn match_payload(
    store: &CoreStore,
    dict: Option<JsValue>,
    probe: &Probe,
) -> Result<JsValue, JsValue> {
    let matched = resolve_now(store.run_probe(probe))?;
    if let Some(dict) = dict
        && let Some(payload) = code_columns(&matched)?
    {
        Reflect::set(&payload, &"dict".into(), &dict)?;
        return Ok(payload);
    }
    term_payload(&matched)
}

/// A matched view's rows as packed N-Triples term columns: `{ offsets, bytes }`
/// under `s`, `p`, `o` and `g`, plus `length`. The always-correct path, taken
/// whenever the rows cannot be described as codes against the store's cached
/// dictionary. The shared decode hands each term over in its N-Triples
/// spelling already (the default graph as the empty string, this payload's
/// vocabulary for it), so the bytes go straight into the column — no parse
/// into oxrdf terms, no re-render.
fn term_payload(matched: &CoreStore) -> Result<JsValue, JsValue> {
    // (offsets seeded with a leading 0, bytes) per column.
    let mut columns: [(Vec<u32>, Vec<u8>); 4] = std::array::from_fn(|_| (vec![0], Vec::new()));
    let mut chunks = matched.shared_quad_chunks();
    while let Some(chunk) = resolve_now(chunks.next().map(Ok))? {
        for row in chunk {
            let row = row.map_err(js_err)?;
            let terms = [&row.s, &row.p, &row.o, &row.g];
            for ((offsets, bytes), term) in columns.iter_mut().zip(terms) {
                bytes.extend_from_slice(term.as_bytes());
                offsets.push(bytes.len() as u32);
            }
        }
    }
    let payload = Object::new();
    for (column, (offsets, bytes)) in QuadColumn::ALL.iter().zip(&columns) {
        let packed = term_column(offsets, bytes)?;
        Reflect::set(&payload, &column.name().into(), &packed)?;
    }
    let rows = columns[0].0.len() - 1;
    Reflect::set(&payload, &"length".into(), &JsValue::from_f64(rows as f64))?;
    Ok(payload.into())
}

/// Pack one term column's offsets/bytes into a `{offsets, bytes}` JS object.
fn term_column(offsets: &[u32], bytes: &[u8]) -> Result<JsValue, JsValue> {
    let offs = js_sys::Uint32Array::new_with_length(offsets.len() as u32);
    offs.copy_from(offsets);
    let bys = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
    bys.copy_from(bytes);
    let column = Object::new();
    Reflect::set(&column, &"offsets".into(), &offs)?;
    Reflect::set(&column, &"bytes".into(), &bys)?;
    Ok(column.into())
}
