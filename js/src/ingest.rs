//! Bulk quad ingestion across the wasm boundary: length-prefixed packed
//! buffers (one boundary crossing per chunk instead of ~16–20 `Reflect` calls
//! per quad) and RDF/JS `Stream<Quad>` adaptation.

use futures::channel::mpsc;
use futures::{Stream, stream};
use js_sys::Reflect;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_rdf_core::error::{Result as CoreResult, VortexRdfError};
use vortex_rdf_core::store::{BuiltArray, RawQuad};
use vortex_rdf_core::{DictionaryQuadSink, Indexes};
use wasm_bindgen::prelude::*;

use crate::error::js_err;
use crate::terms::js_to_quad;

#[wasm_bindgen(module = "/js-snippets/pack-quads.js")]
extern "C" {
    /// Flatten an RDF/JS quad array into one length-prefixed byte buffer
    /// host-side, so bulk ingestion crosses the wasm boundary once instead of
    /// ~16–20 Reflect calls per quad. Decoded by [`packed_to_quads_into`].
    /// Throws
    /// on a malformed quad (hence `catch`).
    #[wasm_bindgen(js_name = packQuads, catch)]
    fn pack_quads(
        quads: &js_sys::Array,
        start: u32,
        end: u32,
    ) -> Result<js_sys::Uint8Array, JsValue>;
}

/// Quads per `packQuads` call. The packed buffer is copied into linear memory
/// and must stay live while the quads in it are decoded, so packing the whole
/// array at once put a second full copy of the dataset's term bytes on the wasm
/// high-water mark. Packing a range at a time bounds that to one chunk; the
/// boundary crossing it saves is per-chunk rather than per-quad either way.
const PACK_CHUNK: u32 = 1 << 16;

/// Decode a JS quad array in `PACK_CHUNK`-sized ranges, converting with `emit`.
fn js_array_decode<T>(
    quads: &js_sys::Array,
    mut emit: impl FnMut(Quad) -> T,
) -> Result<Vec<T>, JsValue> {
    let total = quads.length();
    let mut out: Vec<T> = Vec::with_capacity(total as usize);
    let mut start = 0u32;
    while start < total {
        let end = (start + PACK_CHUNK).min(total);
        let packed = pack_quads(quads, start, end)?;
        packed_to_quads_into(&packed.to_vec(), &mut emit, &mut out)?;
        start = end;
    }
    Ok(out)
}

/// Ingest form: quads as [`RawQuad`], which is what every builder consumes.
fn js_array_to_raw_quads(quads: js_sys::Array) -> Result<Vec<RawQuad>, JsValue> {
    js_array_decode(&quads, |q| RawQuad::from_quad(&q))
}

/// Dictionary-layout ingest: decode each packed chunk straight into the
/// interning [`DictionaryQuadSink`] and build the array from it.
///
/// Unlike [`js_array_to_raw_quads`], nothing accumulates per quad but four
/// u32 ids — each `RawQuad`'s Strings die inside `push` — so the ingest
/// high-water holds one packed chunk plus one copy of every distinct term
/// instead of four owned Strings per quad.
pub(crate) fn js_array_to_dictionary_array(
    quads: js_sys::Array,
    indexes: Indexes,
) -> Result<BuiltArray, JsValue> {
    let mut sink = DictionaryQuadSink::new(indexes);
    // `push` returns `()`, so the decode loop's collected results are a ZST
    // vector: nothing per quad is allocated.
    js_array_decode(&quads, |q| sink.push(RawQuad::from_quad(&q)))?;
    sink.finish().map_err(js_err)
}

/// Mutation form: quads as `oxrdf::Quad`, which `add_quads` needs because its
/// duplicate check goes through `match_pattern` on the parsed terms. Bounded by
/// the batch size, so the extra owned copy is not the ingest concern that
/// [`js_array_to_raw_quads`] avoids.
pub(crate) fn js_array_to_quads(quads: js_sys::Array) -> Result<Vec<Quad>, JsValue> {
    js_array_decode(&quads, |q| q)
}

/// Decode the buffer [`pack_quads`] produced back into owned quads. Term
/// construction/validation mirrors [`js_to_quad`]: IRIs and language tags are
/// validated, blank-node ids are taken as-is.
///
/// `emit` converts each decoded quad as it is produced, so on the ingest path
/// the `oxrdf::Quad` dies inside the loop. Collecting `Vec<Quad>` and
/// converting afterwards — which is what every builder did with it — held a
/// second owned copy of every term in the dataset live at once, and that copy
/// was a large part of the wasm ingest high-water mark.
fn packed_to_quads_into<T>(
    bytes: &[u8],
    emit: &mut impl FnMut(Quad) -> T,
    out: &mut Vec<T>,
) -> Result<(), JsValue> {
    struct Cursor<'a> {
        bytes: &'a [u8],
        pos: usize,
    }
    impl<'a> Cursor<'a> {
        fn u8(&mut self) -> Result<u8, JsValue> {
            let b = *self
                .bytes
                .get(self.pos)
                .ok_or_else(|| js_err("Truncated quad buffer"))?;
            self.pos += 1;
            Ok(b)
        }
        fn u32(&mut self) -> Result<u32, JsValue> {
            let end = self.pos + 4;
            let s = self
                .bytes
                .get(self.pos..end)
                .ok_or_else(|| js_err("Truncated quad buffer"))?;
            self.pos = end;
            Ok(u32::from_le_bytes(s.try_into().unwrap()))
        }
        fn str(&mut self) -> Result<&'a str, JsValue> {
            let len = self.u32()? as usize;
            let end = self.pos + len;
            let s = self
                .bytes
                .get(self.pos..end)
                .ok_or_else(|| js_err("Truncated quad buffer"))?;
            self.pos = end;
            std::str::from_utf8(s).map_err(|_| js_err("Invalid UTF-8 in quad buffer"))
        }
    }

    let invalid = |i: usize| js_err(format!("Invalid quad object at index {}", i));
    let mut cur = Cursor { bytes, pos: 0 };
    let n = cur.u32()? as usize;
    out.reserve(n);
    for i in 0..n {
        // subject: NamedNode | BlankNode
        let s = match cur.u8()? {
            0 => NamedOrBlankNode::NamedNode(NamedNode::new(cur.str()?).map_err(|_| invalid(i))?),
            1 => NamedOrBlankNode::BlankNode(oxrdf::BlankNode::new_unchecked(cur.str()?)),
            _ => return Err(invalid(i)),
        };
        // predicate: NamedNode
        let p = match cur.u8()? {
            0 => NamedNode::new(cur.str()?).map_err(|_| invalid(i))?,
            _ => return Err(invalid(i)),
        };
        // object: any term
        let o = match cur.u8()? {
            0 => Term::NamedNode(NamedNode::new(cur.str()?).map_err(|_| invalid(i))?),
            1 => Term::BlankNode(oxrdf::BlankNode::new_unchecked(cur.str()?)),
            2 => Term::Literal(oxrdf::Literal::new_simple_literal(cur.str()?)),
            3 => {
                let value = cur.str()?.to_owned();
                let lang = cur.str()?;
                Term::Literal(
                    oxrdf::Literal::new_language_tagged_literal(value, lang)
                        .map_err(|_| invalid(i))?,
                )
            }
            4 => {
                let value = cur.str()?.to_owned();
                let dt = NamedNode::new(cur.str()?).map_err(|_| invalid(i))?;
                Term::Literal(oxrdf::Literal::new_typed_literal(value, dt))
            }
            _ => return Err(invalid(i)),
        };
        // graph: NamedNode | BlankNode | DefaultGraph
        let g = match cur.u8()? {
            0 => GraphName::NamedNode(NamedNode::new(cur.str()?).map_err(|_| invalid(i))?),
            1 => GraphName::BlankNode(oxrdf::BlankNode::new_unchecked(cur.str()?)),
            5 => GraphName::DefaultGraph,
            _ => return Err(invalid(i)),
        };
        out.push(emit(Quad::new(s, p, o, g)));
    }
    Ok(())
}

/// A quad stream boxed for `build_array`, whichever of the two `fromQuads`
/// input shapes it came from.
pub(crate) type BoxedQuadStream = Box<dyn Stream<Item = CoreResult<RawQuad>> + Unpin + Send>;

/// Accept either shape `fromQuads` allows: a plain array (eagerly validated
/// and wrapped in `stream::iter`), or an RDF/JS `Stream<Quad>` (consumed
/// through its event-emitter interface).
pub(crate) fn js_to_quad_stream(value: JsValue) -> Result<BoxedQuadStream, JsValue> {
    if js_sys::Array::is_array(&value) {
        let quads = js_array_to_raw_quads(js_sys::Array::from(&value))?;
        let stream: BoxedQuadStream = Box::new(stream::iter(
            quads.into_iter().map(Ok::<RawQuad, VortexRdfError>),
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
            js_err(
                "fromQuads expects an array of quads or an RDF/JS Stream \
                 (an object with an 'on' method)",
            )
        })?;

    let (tx, rx) = mpsc::unbounded::<CoreResult<RawQuad>>();

    let tx_data = tx.clone();
    let on_data = Closure::wrap(Box::new(move |quad_js: JsValue| {
        let item = js_to_quad(quad_js)
            .as_ref()
            .map(RawQuad::from_quad)
            .ok_or_else(|| {
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
        .map_err(|_| js_err("Failed to attach a 'data' listener to the stream"))?;
    on.call2(&stream_val, &"error".into(), on_error.as_ref())
        .map_err(|_| js_err("Failed to attach an 'error' listener to the stream"))?;
    on.call2(&stream_val, &"end".into(), on_end.as_ref())
        .map_err(|_| js_err("Failed to attach an 'end' listener to the stream"))?;

    on_data.forget();
    on_error.forget();
    on_end.forget();

    Ok(Box::new(rx))
}
