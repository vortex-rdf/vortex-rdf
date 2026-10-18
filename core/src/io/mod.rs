//! The native container codec: the `vortex-rdf.store.v2` grammar
//! (`container`), the write driver over it (`write`), and read-side file
//! access (`read`). The public items are the two file writers,
//! `quads_stream_to_vortex_file` and `quads_stream_to_vortex_writer`, behind
//! `file-io`. Textual RDF conversion lives outside this module: parsing in
//! `common::terms`, export in `export_rdf`; the opened-store runtime handle
//! lives store-side in `store::persist::native_file`.

pub(crate) mod container;
pub(crate) mod read;
pub(crate) mod write;

#[cfg(feature = "file-io")]
pub use write::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};
