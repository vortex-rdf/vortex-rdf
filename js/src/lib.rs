//! WebAssembly bindings for `vortex_rdf_core`: the RDF/JS-flavored
//! `VortexRdfStore` API plus the `serializeRdf`/`deserializeRdf` conversion
//! entry points. The lazy RDF/JS read model (LazyQuad/LazyTerm + stream) lives
//! in js-snippets/lazy-rdf.js and the bulk-ingest quad packer in
//! js-snippets/pack-quads.js, both copied verbatim into the generated pkg.

use wasm_bindgen::prelude::*;

mod error;
mod ingest;
mod options;
mod store;
mod terms;

pub use store::{ArrowFFI, TermDict, VortexRdfStore};

/// The hand-written TypeScript surface of this crate (the Rust items carry
/// `skip_typescript`), kept in its own file for real TS tooling.
#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = include_str!("api.d.ts");

/// Runs once as the module is instantiated (the generated `init` calls it):
/// a Rust panic then reports its message through `console.error` instead of
/// surfacing as an opaque `unreachable` trap.
#[wasm_bindgen(start)]
fn start() {
    console_error_panic_hook::set_once();
}

/// One-shot conversion: RDF text in, native-container bytes out. The store
/// entry points it delegates to define the semantics — including that the
/// bytes are a complete container carrying, under the Dictionary layout, the
/// term dictionary and index copies.
#[wasm_bindgen(js_name = serializeRdf, skip_typescript)]
pub async fn serialize_rdf(
    input: String,
    format_name: &str,
    options: JsValue,
) -> Result<Vec<u8>, JsValue> {
    VortexRdfStore::from_string(input, format_name, options)
        .await?
        .to_bytes()
        .await
}

/// One-shot conversion: native-container bytes in, RDF text out. `Vec<u8>`
/// for the same one-copy reason as [`VortexRdfStore::from_bytes`].
#[wasm_bindgen(js_name = deserializeRdf, skip_typescript)]
pub async fn deserialize_rdf(bytes: Vec<u8>, format_name: &str) -> Result<String, JsValue> {
    VortexRdfStore::from_bytes(bytes, JsValue::UNDEFINED)
        .await?
        .to_rdf(format_name)
        .await
}
