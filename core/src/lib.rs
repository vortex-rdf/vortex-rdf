//! A columnar RDF serialization format and queryable quad store, built on
//! [Vortex](https://docs.vortex.dev).
//!
//! Converts RDF quads (parsed from any format [`oxrdfio`] supports) into a
//! Vortex [`StructArray`](vortex_array::arrays::struct_::StructArray),
//! storable as a native-container `.vortex` file (or the same bytes in
//! memory), and queryable in place through [`VortexRdfStore`] without
//! decompressing or copying the underlying data. See the [repository README](https://github.com/vortex-rdf/vortex-rdf)
//! for the full architecture: column layouts, secondary indexes, and
//! ingestion builders.
//!
//! # Example
//!
//! ```
//! use futures::{executor::block_on, stream};
//! use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
//! use vortex_rdf_core::{LayoutStrategy, RawQuad, VortexRdfError, VortexRdfStore};
//!
//! block_on(async {
//!     let quad = Quad::new(
//!         NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap()),
//!         NamedNode::new("http://example.org/p").unwrap(),
//!         Term::Literal(Literal::new_simple_literal("hello")),
//!         GraphName::DefaultGraph,
//!     );
//!     // Builders consume `RawQuad` — terms already in the N-Triples form the
//!     // columns store. `parse_quads_from_reader` yields these directly.
//!     let quads = stream::iter(vec![Ok::<_, VortexRdfError>(RawQuad::from_quad(&quad))]);
//!
//!     // Sort the stream globally by (s, p, o, g) and adopt the result as a
//!     // queryable store (here: plain string columns, no secondary indexes).
//!     let store = VortexRdfStore::from_quads(quads, LayoutStrategy::Default, vec![])
//!         .await
//!         .unwrap();
//!
//!     // Pattern matching narrows a view over the store without copying data.
//!     let p = NamedNode::new("http://example.org/p").unwrap();
//!     let matched = store
//!         .match_pattern(None, Some(&p), None, None)
//!         .await
//!         .unwrap();
//!     assert_eq!(matched.size().await.unwrap(), 1);
//! });
//! ```

pub mod arrow;
pub mod common;
pub mod debug;
pub mod error;
pub mod io;
mod session;
/// The quad store: builders, layouts, indexes, matching and mutation.
pub mod store;

pub use arrow::{QuadColumn, TermEncoding};
pub use error::{Result, VortexRdfError};

pub use store::{
    BuiltArray, BuiltStream, ChunkStream, DictSnapshot, DictionaryQuadSink, IndexType, Indexes,
    Keep, LayoutStrategy, RawQuad, SharedQuad, SortedInMemoryBuilder, StoreParts,
    VortexArrayBuilder, VortexRdfStore, export_rdf,
};
// Compiled out on wasm along with the rest of the sorted-stream builder's
// out-of-core merge (see the module gate in `store::builders`).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use store::SortedStreamBuilder;

#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
use mimalloc::MiMalloc;
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Compiles the README's Rust snippets as doctests. The file-conversion
/// snippet uses the `file-io` entry points, so the hook follows that gate.
#[cfg(all(doctest, feature = "file-io"))]
#[doc = include_str!("../../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
mod tests;
