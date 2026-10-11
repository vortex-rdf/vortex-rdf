//! The native container codec: the `vortex-rdf.store.v2` grammar
//! (`container`), the write driver over it (`ser`), and read-side file
//! access (`read`). Textual RDF conversion lives outside this module:
//! parsing in `common::terms`, export in `store::export` (re-exported at the
//! crate root as `export_rdf`); the opened-store runtime handle lives
//! store-side in `store::native_file`.

pub(crate) mod container;
pub(crate) mod read;
/// Write-side native-container machinery. Compiled out of the no-file-io
/// native build, which has no consumer for the byte writer (reading via
/// `from_bytes` stays available); present natively behind `file-io` and on
/// wasm, whose bindings exchange file bytes. `container::write` is gated the
/// same way.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod ser;

#[cfg(feature = "file-io")]
pub use ser::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};
