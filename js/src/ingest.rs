//! Bulk quad ingestion across the wasm boundary: length-prefixed packed
//! buffers decoded with a linear cursor, and RDF/JS `Stream<Quad>` adaptation.

use futures::channel::mpsc;
use js_sys::{Array, Reflect};
use oxrdf::Quad;
use vortex_rdf_core::{
    BuiltArray, DictionaryQuadSink, Indexes, RawQuad, Result as CoreResult, VortexRdfError,
};
use wasm_bindgen::prelude::*;

use crate::error::js_err;
use crate::terms::{TermKind, graph, js_to_quad, object, predicate, subject};

#[wasm_bindgen(module = "/js-snippets/pack-quads.js")]
extern "C" {
    /// Flatten a range of an RDF/JS quad array into one length-prefixed byte
    /// buffer host-side, decoded by [`decode_packed`]. Throws on a malformed
    /// quad (hence `catch`).
    #[wasm_bindgen(js_name = packQuads, catch)]
    fn pack_quads(quads: &Array, start: u32, end: u32) -> Result<js_sys::Uint8Array, JsValue>;
}

/// Quads per `packQuads` call. The packed buffer is copied into linear memory
/// and stays live until its quads are decoded, so this bounds the ingest
/// transient to one chunk's term bytes.
const PACK_CHUNK: u32 = 1 << 16;

/// Decode a JS quad array in `PACK_CHUNK`-sized ranges, handing each quad to
/// `emit` as it is decoded.
fn js_array_decode(quads: &Array, mut emit: impl FnMut(Quad)) -> Result<(), JsValue> {
    let total = quads.length();
    let mut start = 0u32;
    while start < total {
        let end = (start + PACK_CHUNK).min(total);
        let packed = pack_quads(quads, start, end)?.to_vec();
        decode_packed(&packed, start as usize, &mut emit)?;
        start = end;
    }
    Ok(())
}

/// Every quad of a JS quad array, converted by `convert`, in one allocation.
fn js_array_collect<T>(quads: &Array, convert: impl Fn(Quad) -> T) -> Result<Vec<T>, JsValue> {
    let mut out = Vec::with_capacity(quads.length() as usize);
    js_array_decode(quads, |quad| out.push(convert(quad)))?;
    Ok(out)
}

/// Ingest form: quads as [`RawQuad`], which is what every builder consumes.
pub(crate) fn js_array_to_raw_quads(quads: &Array) -> Result<Vec<RawQuad>, JsValue> {
    js_array_collect(quads, |q| RawQuad::from_quad(&q))
}

/// Dictionary-layout ingest: decode each packed chunk straight into the
/// interning [`DictionaryQuadSink`] and build the array from it.
///
/// Unlike [`js_array_to_raw_quads`], nothing accumulates per quad but four
/// term codes — each `RawQuad`'s Strings die inside `push` — so the ingest
/// high-water holds one packed chunk plus one copy of every distinct term
/// instead of four owned Strings per quad.
pub(crate) fn js_array_to_dictionary_array(
    quads: &Array,
    indexes: Indexes,
) -> Result<BuiltArray, JsValue> {
    let mut sink = DictionaryQuadSink::new(indexes);
    js_array_decode(quads, |q| sink.push(RawQuad::from_quad(&q)))?;
    sink.finish().map_err(js_err)
}

/// Mutation form: quads as `oxrdf::Quad`, which `add_quads` needs because its
/// duplicate check goes through `match_pattern` on the parsed terms. Bounded by
/// the batch size, so the extra owned copy is not the ingest concern that
/// [`js_array_to_raw_quads`] avoids.
pub(crate) fn js_array_to_quads(quads: &Array) -> Result<Vec<Quad>, JsValue> {
    js_array_collect(quads, |q| q)
}

/// Decode the buffer [`pack_quads`] produced for the quads from array index
/// `first` on, handing each quad to `emit` as it is decoded. Each term is
/// built by its position's validated constructor in [`crate::terms`], the
/// ones the reflection reader [`js_to_quad`] builds through, so both decoders
/// accept the same terms; a term a constructor rejects is reported with its
/// quad's index in the array, as the packer reports the quads it rejects.
///
/// On the ingest path `emit` converts each quad as it is produced, so the
/// `oxrdf::Quad` is dropped inside the loop and at most one packed chunk's
/// terms are live at a time.
fn decode_packed(bytes: &[u8], first: usize, emit: &mut impl FnMut(Quad)) -> Result<(), JsValue> {
    struct Cursor<'a> {
        bytes: &'a [u8],
        pos: usize,
    }
    impl<'a> Cursor<'a> {
        /// The next `len` bytes.
        fn take(&mut self, len: usize) -> Result<&'a [u8], JsValue> {
            let end = self.pos + len;
            let taken = self
                .bytes
                .get(self.pos..end)
                .ok_or_else(|| js_err("Truncated quad buffer"))?;
            self.pos = end;
            Ok(taken)
        }
        fn u8(&mut self) -> Result<u8, JsValue> {
            Ok(self.take(1)?[0])
        }
        fn u32(&mut self) -> Result<u32, JsValue> {
            Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
        }
        fn str(&mut self) -> Result<&'a str, JsValue> {
            let len = self.u32()? as usize;
            std::str::from_utf8(self.take(len)?).map_err(|_| js_err("Invalid UTF-8 in quad buffer"))
        }
        /// One packed term — its tag, then (tags 0–4) its value and (tags
        /// 3–4) its extra — built by `build` with borrowed strings: `Ok(None)`
        /// for a tag that names no [`TermKind`] or a term `build` rejects.
        #[inline]
        fn term<U>(
            &mut self,
            build: impl FnOnce(TermKind, &'a str, &'a str) -> Option<U>,
        ) -> Result<Option<U>, JsValue> {
            let Some(kind) = TermKind::from_tag(self.u8()?) else {
                return Ok(None);
            };
            let value = if kind.has_value() { self.str()? } else { "" };
            let extra = if kind.has_extra() { self.str()? } else { "" };
            Ok(build(kind, value, extra))
        }
    }

    let invalid = |i: usize| js_err(format!("Invalid quad object at index {}", i));
    let mut cur = Cursor { bytes, pos: 0 };
    let n = cur.u32()? as usize;
    for i in first..first + n {
        let s = cur.term(subject)?.ok_or_else(|| invalid(i))?;
        let p = cur.term(predicate)?.ok_or_else(|| invalid(i))?;
        let o = cur.term(object)?.ok_or_else(|| invalid(i))?;
        let g = cur.term(graph)?.ok_or_else(|| invalid(i))?;
        emit(Quad::new(s, p, o, g));
    }
    Ok(())
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
pub(crate) fn rdfjs_stream_to_quads(
    stream_val: JsValue,
) -> Result<mpsc::UnboundedReceiver<CoreResult<RawQuad>>, JsValue> {
    let on = Reflect::get(&stream_val, &"on".into())
        .ok()
        .and_then(|f| f.dyn_into::<js_sys::Function>().ok())
        .ok_or_else(|| {
            js_err(
                "fromQuads expects an array of quads or an RDF/JS Stream \
                 (an object with an 'on' method)",
            )
        })?;

    let (tx, rx) = mpsc::unbounded::<CoreResult<RawQuad>>();

    let tx_data = tx.clone();
    let mut position = 0usize;
    let on_data = Closure::wrap(Box::new(move |quad_js: JsValue| {
        let item = js_to_quad(&quad_js)
            .as_ref()
            .map(RawQuad::from_quad)
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "Invalid quad object in RDF/JS stream at position {position}"
                ))
            });
        position += 1;
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
        .map_err(|_| js_err("Failed to attach a 'data' listener to the stream"))?;
    on.call2(&stream_val, &"error".into(), on_error.as_ref())
        .map_err(|_| js_err("Failed to attach an 'error' listener to the stream"))?;
    on.call2(&stream_val, &"end".into(), on_end.as_ref())
        .map_err(|_| js_err("Failed to attach an 'end' listener to the stream"))?;

    on_data.forget();
    on_error.forget();
    on_end.forget();

    Ok(rx)
}
