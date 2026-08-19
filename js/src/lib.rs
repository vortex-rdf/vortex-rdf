//! WebAssembly bindings for `vortex_rdf_core`: the RDF/JS-flavored
//! `VortexRdfStore` API plus the `rdf_to_vortex`/`vortex_to_rdf` conversion
//! entry points. The lazy RDF/JS read model (LazyQuad/LazyTerm + stream) lives
//! in js-snippets/lazy-rdf.js and the bulk-ingest quad packer in
//! js-snippets/pack-quads.js, both copied verbatim into the generated pkg.

use wasm_bindgen::prelude::*;

mod error;
mod ingest;
mod options;
mod store;
mod terms;

pub use store::{TermDict, VortexRdfStore};

/// The hand-written TypeScript surface of this crate (the Rust items carry
/// `skip_typescript`), kept in its own file for real TS tooling.
#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &'static str = include_str!("api.d.ts");

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

/// One-shot conversion: RDF text in, native-container bytes out. The store
/// entry points it delegates to define the semantics — including that the
/// bytes are a complete container carrying, under the Dictionary layout, the
/// term dictionary and index copies.
#[wasm_bindgen(skip_typescript)]
pub async fn rdf_to_vortex(
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
/// for the same reason as [`VortexRdfStore::from_bytes`]: the marshalled
/// buffer's ownership passes straight through to core, so the load never
/// holds two copies of the file bytes.
#[wasm_bindgen(skip_typescript)]
pub async fn vortex_to_rdf(vortex_bytes: Vec<u8>, format_name: &str) -> Result<String, JsValue> {
    VortexRdfStore::from_bytes(vortex_bytes)
        .await?
        .to_rdf(format_name)
        .await
}
