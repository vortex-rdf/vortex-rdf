//! The wasm iterator handles behind `matchCodeChunks` (`CodeChunks`) and
//! `matchCodesMany` (`ProbeAnswers`), and the glue that hands each to JS as a
//! synchronous `Iterable` that is also `Disposable` (`makeHandleIterable` in
//! js-snippets/lazy-rdf.js). A handle's `next()` pulls one chunk or answers
//! one probe on the calling thread: no wasm read path performs I/O, so
//! nothing is spawned and a pull never suspends (see `resolve_now`). A handle
//! reads through its own reference to the store's rows, and refuses to pull
//! once the store's handle has been freed.

use std::rc::Weak;

use futures::StreamExt;
use js_sys::{Object, Reflect};
use vortex_rdf_core::{CodeChunkStream, Probe, QuadColumn, TermCode, VortexRdfStore as CoreStore};
use wasm_bindgen::prelude::*;

use crate::codes::code_array;
use crate::error::js_err;
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

/// The rows `probe` matches on `store` as `{ s, p, o, g, length }`, four
/// `Float64Array`s of exact codes, or `null` when codes are not the matched
/// view's vocabulary. Shared by `matchCodes` and `matchCodesMany`.
pub(crate) fn probe_codes(store: &CoreStore, probe: &Probe) -> Result<JsValue, JsValue> {
    let matched = resolve_now(store.run_probe(probe))?.map_err(js_err)?;
    match resolve_now(matched.code_columns_gathered())?.map_err(js_err)? {
        Some(columns) => columns_object(&QuadColumn::ALL, &columns),
        None => Ok(JsValue::NULL),
    }
}

/// The error a handle's `next()` throws once the store it reads is freed.
fn freed_store() -> JsValue {
    js_err("the VortexRdfStore this iterator reads has been freed")
}

/// The chunks of a `matchCodeChunks` call: the match's code columns pulled
/// one chunk per `next()`, in base row order, each column a `Float64Array`
/// copy of the chunk's codes.
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
    /// The next chunk, `{ <column>: Float64Array, …, length }` holding the
    /// requested columns, or `undefined` once the match is exhausted. Throws
    /// once the store has been freed, for a failed read and for a code past
    /// 2^53 − 1; after the end or an error, every call returns `undefined`.
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
    match resolve_now(stream.next())? {
        Some(chunk) => columns_object(columns, &chunk.map_err(js_err)?).map(Some),
        None => Ok(None),
    }
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

/// The answers of a `matchCodesMany` call: one probe answered per `next()`,
/// in input order.
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
    /// The next probe's code columns, `{ s, p, o, g, length }` as for
    /// `matchCodes`, `null` when the store's rows have no codes, or
    /// `undefined` once every probe has answered. Throws once the store has
    /// been freed and for the first probe that fails; after the end or an
    /// error, every call returns `undefined`.
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
