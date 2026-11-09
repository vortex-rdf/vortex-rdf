//! The wasm iterator handles behind `matchCodeChunks` (`CodeChunks`),
//! `matchCodesMany` (`ProbeAnswers`) and `match` (`QuadChunks`), the glue
//! that hands the first two to JS as a synchronous `Iterable` that is also
//! `Disposable` (`makeHandleIterable` in js-snippets/lazy-rdf.js), and the
//! packer of the term payloads `match` and `getQuads` hand to the lazy read
//! model (`TermColumns`). A handle's `next()` pulls one chunk or answers one
//! probe on the calling thread: no wasm read path performs I/O, so nothing is
//! spawned and a pull never suspends (see `resolve_now`). A handle reads
//! through its own reference to the store's rows, and refuses to pull once
//! the store's handle has been freed.

use std::rc::Weak;

use futures::TryStreamExt;
use js_sys::{Object, Reflect};
use vortex_rdf_core::{
    CodeChunkStream, LayoutStrategy, Probe, QuadColumn, RowChunkStream, SharedQuad, TermCode,
    VortexRdfStore as CoreStore,
};
use wasm_bindgen::prelude::*;

use crate::codes::code_array;
use crate::error::js_err;
use crate::probes::DEFAULT_BATCH_ROWS;
use crate::resolve_now;

#[wasm_bindgen(module = "/js-snippets/lazy-rdf.js")]
extern "C" {
    /// Wrap an iterator handle into a synchronous `Iterable` that is also
    /// `Disposable`: each `next()` pulls the handle once, unless `signal` has
    /// aborted, and the handle is freed at its end, at an error, on
    /// `return()` and on `[Symbol.dispose]`.
    #[wasm_bindgen(js_name = makeHandleIterable)]
    fn make_handle_iterable(handle: JsValue, signal: &JsValue) -> JsValue;
}

/// `handle` as the `Iterable` its call returns, ended early by `signal`
/// (`undefined` for none).
pub(crate) fn handle_iterable(handle: impl Into<JsValue>, signal: &JsValue) -> JsValue {
    make_handle_iterable(handle.into(), signal)
}

/// An `Iterable` that has already finished, with no handle under it: what
/// a call given an already aborted signal returns.
pub(crate) fn finished_iterable() -> JsValue {
    make_handle_iterable(JsValue::UNDEFINED, &JsValue::UNDEFINED)
}

/// The rows `probe` matches on `store` as their [`code_columns`], or `null`
/// when codes are not the matched view's vocabulary. Shared by `matchCodes`
/// and `matchCodesMany`.
pub(crate) fn probe_codes(store: &CoreStore, probe: &Probe) -> Result<JsValue, JsValue> {
    let matched = resolve_now(store.run_probe(probe))?;
    Ok(code_columns(&matched)?.unwrap_or(JsValue::NULL))
}

/// A matched view's rows as `{ s, p, o, g, length }`, four `Float64Array`s
/// of exact codes, or `None` when codes are not the view's vocabulary. The
/// answer of `matchCodes`, and with the store's `LazyDict` beside it the code
/// payload of `match` and `getQuads`.
pub(crate) fn code_columns(matched: &CoreStore) -> Result<Option<JsValue>, JsValue> {
    resolve_now(matched.code_columns_gathered())?
        .map(|columns| columns_object(&QuadColumn::ALL, &columns))
        .transpose()
}

/// The error a handle's `next()` throws once the store it reads is freed.
fn freed_store() -> JsValue {
    js_err("the VortexRdfStore this iterator reads has been freed")
}

/// The handle under `matchCodeChunks`' iterable: the match's code chunks,
/// one pulled per `next()`.
#[wasm_bindgen(skip_typescript)]
pub struct CodeChunks {
    /// The chunk stream; `None` once it has ended or failed.
    stream: Option<CodeChunkStream>,
    /// The columns of each chunk, in `(s, p, o, g)` order.
    columns: Vec<QuadColumn>,
    /// The liveness of the store's handle.
    alive: Weak<()>,
}

impl CodeChunks {
    pub(crate) fn new(stream: CodeChunkStream, columns: Vec<QuadColumn>, alive: Weak<()>) -> Self {
        CodeChunks {
            stream: Some(stream),
            columns,
            alive,
        }
    }
}

#[wasm_bindgen]
impl CodeChunks {
    /// The JS `next()`. The stream is dropped at its end and at the first
    /// error, a freed store's included, so every later call returns
    /// `undefined`.
    #[wasm_bindgen(js_name = next)]
    pub fn pull(&mut self) -> Result<JsValue, JsValue> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(JsValue::UNDEFINED);
        };
        let pulled = if self.alive.strong_count() == 0 {
            Err(freed_store())
        } else {
            next_chunk(stream, &self.columns)
        };
        if !matches!(pulled, Ok(Some(_))) {
            self.stream = None;
        }
        Ok(pulled?.unwrap_or(JsValue::UNDEFINED))
    }
}

/// The next chunk of `stream` as a chunk object, `None` at its end.
fn next_chunk(
    stream: &mut CodeChunkStream,
    columns: &[QuadColumn],
) -> Result<Option<JsValue>, JsValue> {
    resolve_now(stream.try_next())?
        .map(|chunk| columns_object(columns, &chunk))
        .transpose()
}

/// Code columns as `{ <column>: Float64Array, …, length }`, one copy per
/// column of `columns`: a chunk's, or all four of a match's.
fn columns_object(
    columns: &[QuadColumn],
    codes: &[impl AsRef<[TermCode]>],
) -> Result<JsValue, JsValue> {
    let object = Object::new();
    for (column, codes) in columns.iter().zip(codes) {
        let codes = code_array(codes.as_ref())?;
        Reflect::set(&object, &column.name().into(), &codes)?;
    }
    let rows = codes.first().map_or(0, |codes| codes.as_ref().len());
    Reflect::set(&object, &"length".into(), &JsValue::from_f64(rows as f64))?;
    Ok(object.into())
}

/// The handle under `matchCodesMany`' iterable: one probe answered per
/// `next()`.
#[wasm_bindgen(skip_typescript)]
pub struct ProbeAnswers {
    /// The store the probes run on; `None` when its rows have no codes, so
    /// that every probe answers `null` and none runs.
    store: Option<CoreStore>,
    /// The probes still to answer; empty once each has, or one has failed.
    probes: std::vec::IntoIter<Probe>,
    /// The liveness of the store's handle.
    alive: Weak<()>,
}

impl ProbeAnswers {
    pub(crate) fn new(store: Option<CoreStore>, probes: Vec<Probe>, alive: Weak<()>) -> Self {
        ProbeAnswers {
            store,
            probes: probes.into_iter(),
            alive,
        }
    }
}

#[wasm_bindgen]
impl ProbeAnswers {
    /// The JS `next()`. The probes are dropped at the first error, a freed
    /// store's included, so every later call returns `undefined`; once every
    /// probe has answered, a freed store no longer throws (a `CodeChunks`
    /// throws until it has seen its stream end).
    #[wasm_bindgen(js_name = next)]
    pub fn pull(&mut self) -> Result<JsValue, JsValue> {
        let Some(probe) = self.probes.next() else {
            return Ok(JsValue::UNDEFINED);
        };
        let answer = if self.alive.strong_count() == 0 {
            Err(freed_store())
        } else if let Some(store) = &self.store {
            probe_codes(store, &probe)
        } else {
            Ok(JsValue::NULL)
        };
        if answer.is_err() {
            self.probes = Vec::new().into_iter();
        }
        answer
    }
}

/// The handle under `match`'s stream (`makeLazyQuadStream` in
/// js-snippets/lazy-rdf.js): the match's rows in chunks of at most 8,192
/// (`DEFAULT_BATCH_ROWS`), one pulled per `next()`, each in the payload shape
/// `buildLazyQuads` takes, as `getQuads` builds it for the whole match.
/// Nothing is read until the first pull.
#[wasm_bindgen(skip_typescript)]
pub struct QuadChunks {
    /// The match's rows; `None` once they have ended or failed.
    rows: Option<MatchRows>,
    /// The liveness of the store's handle.
    alive: Weak<()>,
}

/// Where a [`QuadChunks`] reads its rows: the pattern until the first pull
/// resolves it, then the matched view's chunks, picked as Java's
/// `TermStream::open` picks them.
// One per `match` stream, never held in bulk: boxing the unresolved store and
// probe would cost every `match` an allocation for nothing. (The lint fires on
// 64-bit targets only; on wasm32 the variants are within its threshold.)
#[allow(clippy::large_enum_variant)]
enum MatchRows {
    /// The pattern, not resolved yet: the store as `match` saw it, the probe,
    /// and the store's `LazyDict` when codes are its rows' vocabulary.
    Unresolved {
        store: CoreStore,
        probe: Probe,
        dict: Option<JsValue>,
    },
    /// The view's code chunks, and the `LazyDict` that decodes them.
    Codes {
        dict: JsValue,
        chunks: CodeChunkStream,
    },
    /// A string layout's row chunks, and the view that decodes them.
    Rows {
        view: CoreStore,
        chunks: RowChunkStream,
    },
    /// The quads of a Dictionary view with an append tail, which core cannot
    /// chunk: read whole, taken [`DEFAULT_BATCH_ROWS`] at a time.
    Collected(std::vec::IntoIter<SharedQuad>),
}

impl QuadChunks {
    pub(crate) fn new(
        store: CoreStore,
        probe: Probe,
        dict: Option<JsValue>,
        alive: Weak<()>,
    ) -> Self {
        QuadChunks {
            rows: Some(MatchRows::Unresolved { store, probe, dict }),
            alive,
        }
    }
}

#[wasm_bindgen]
impl QuadChunks {
    /// The JS `next()`: the next chunk, or `undefined` at the end. The first
    /// call runs the probe. The rows are dropped at their end and at the
    /// first error, a freed store's included, so every later call returns
    /// `undefined`.
    #[wasm_bindgen(js_name = next)]
    pub fn pull(&mut self) -> Result<JsValue, JsValue> {
        let Some(rows) = self.rows.as_mut() else {
            return Ok(JsValue::UNDEFINED);
        };
        let pulled = if self.alive.strong_count() == 0 {
            Err(freed_store())
        } else {
            rows.next_payload()
        };
        if !matches!(pulled, Ok(Some(_))) {
            self.rows = None;
        }
        Ok(pulled?.unwrap_or(JsValue::UNDEFINED))
    }
}

impl MatchRows {
    /// The next chunk of rows as a payload, `None` at the end; the pattern is
    /// resolved first, and a chunk of no rows is skipped.
    fn next_payload(&mut self) -> Result<Option<JsValue>, JsValue> {
        loop {
            match self {
                MatchRows::Unresolved { store, probe, dict } => {
                    let view = resolve_now(store.run_probe(probe))?;
                    *self = MatchRows::of(view, dict.take())?;
                }
                MatchRows::Codes { dict, chunks } => {
                    let Some(codes) = resolve_now(chunks.try_next())? else {
                        return Ok(None);
                    };
                    if codes.first().is_some_and(|codes| !codes.is_empty()) {
                        // `match_payload`'s code payload, a chunk of it.
                        let payload = columns_object(&QuadColumn::ALL, &codes)?;
                        Reflect::set(&payload, &"dict".into(), dict)?;
                        return Ok(Some(payload));
                    }
                }
                MatchRows::Rows { view, chunks } => {
                    let Some(chunk) = resolve_now(chunks.try_next())? else {
                        return Ok(None);
                    };
                    let quads = view.decode_row_chunk(&chunk).map_err(js_err)?;
                    if !quads.is_empty() {
                        let mut columns = TermColumns::new();
                        for quad in &quads {
                            columns.push(quad);
                        }
                        return columns.into_payload().map(Some);
                    }
                }
                MatchRows::Collected(quads) => {
                    let mut columns = TermColumns::new();
                    for quad in quads.by_ref().take(DEFAULT_BATCH_ROWS) {
                        columns.push(&quad);
                    }
                    if columns.rows() == 0 {
                        return Ok(None);
                    }
                    return columns.into_payload().map(Some);
                }
            }
        }
    }

    /// The rows of `view`, a match just resolved: its code chunks when `dict`
    /// is the store's `LazyDict`, a string layout's row chunks, or else (a
    /// Dictionary view with an append tail) its quads read whole.
    fn of(view: CoreStore, dict: Option<JsValue>) -> Result<Self, JsValue> {
        if let Some(dict) = dict {
            let chunks = view
                .code_chunks(&QuadColumn::ALL, DEFAULT_BATCH_ROWS)
                .map_err(js_err)?;
            return Ok(MatchRows::Codes { dict, chunks });
        }
        if view.layout() != LayoutStrategy::Dictionary {
            let chunks = view.row_chunks(DEFAULT_BATCH_ROWS).map_err(js_err)?;
            return Ok(MatchRows::Rows { view, chunks });
        }
        let quads = resolve_now(view.shared_quads_vec())?;
        Ok(MatchRows::Collected(quads.into_iter()))
    }
}

/// Rows packed as N-Triples term columns, the term payload of `match` and
/// `getQuads`: `{ offsets, bytes }` under `s`, `p`, `o` and `g`, plus
/// `length`. Each term goes in as the shared decode hands it over, in its
/// N-Triples spelling already (the default graph as the empty string, this
/// payload's vocabulary for it), so the bytes go straight into the column:
/// no parse into oxrdf terms, no re-render.
pub(crate) struct TermColumns {
    /// Per column, `(s, p, o, g)`: the offsets, seeded with a leading 0, and
    /// the bytes.
    columns: [(Vec<u32>, Vec<u8>); 4],
}

impl TermColumns {
    pub(crate) fn new() -> Self {
        TermColumns {
            columns: std::array::from_fn(|_| (vec![0], Vec::new())),
        }
    }

    /// Append `row`'s four terms.
    pub(crate) fn push(&mut self, row: &SharedQuad) {
        let terms = [&row.s, &row.p, &row.o, &row.g];
        for ((offsets, bytes), term) in self.columns.iter_mut().zip(terms) {
            bytes.extend_from_slice(term.as_bytes());
            offsets.push(bytes.len() as u32);
        }
    }

    /// The rows pushed so far.
    pub(crate) fn rows(&self) -> usize {
        self.columns[0].0.len() - 1
    }

    /// The payload object, each column copied into JS.
    pub(crate) fn into_payload(self) -> Result<JsValue, JsValue> {
        let payload = Object::new();
        for (column, (offsets, bytes)) in QuadColumn::ALL.iter().zip(&self.columns) {
            let packed = term_column(offsets, bytes)?;
            Reflect::set(&payload, &column.name().into(), &packed)?;
        }
        let rows = JsValue::from_f64(self.rows() as f64);
        Reflect::set(&payload, &"length".into(), &rows)?;
        Ok(payload.into())
    }
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
