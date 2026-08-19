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
//! use vortex_rdf_core::{
//!     LayoutStrategy, RawQuad, SortedInMemoryBuilder, VortexArrayBuilder, VortexRdfError,
//!     VortexRdfStore,
//! };
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
//!     // Run the quad stream through a builder (here: sorted in memory by
//!     // (s, p, o, g), plain string columns, no secondary indexes), then adopt
//!     // its output as a queryable store.
//!     let built = SortedInMemoryBuilder::build_vortex_array(
//!         Box::new(quads),
//!         LayoutStrategy::Default,
//!         vec![],
//!     )
//!     .await
//!     .unwrap();
//!     let store = VortexRdfStore::from_built(built).unwrap();
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

pub mod common;
pub mod error;
pub mod io;
mod session;
pub mod store;

pub use error::VortexRdfError;

pub use store::{
    BuiltArray, DictSnapshot, DictionaryQuadSink, IndexType, Indexes, LayoutStrategy, RawQuad,
    SortedInMemoryBuilder, StoreParts, VortexArrayBuilder, VortexRdfStore,
};
// Compiled out on wasm along with the rest of the external-sort pipeline
// (see the module gate in `store::builders`).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use store::SortedStreamBuilder;

#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
use mimalloc::MiMalloc;
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(test)]
mod tests;
