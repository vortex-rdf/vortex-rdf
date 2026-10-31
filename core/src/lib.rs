//! A columnar RDF serialization format and queryable quad store, built on
//! [Vortex](https://docs.vortex.dev).
//!
//! Converts RDF quads (in any format [`oxrdfio`] parses) into a Vortex
//! [`StructArray`](vortex_array::arrays::struct_::StructArray), written as a
//! native-container `.vortex` file or the same bytes in memory, and queried in
//! place through [`VortexRdfStore`]. The
//! [repository README](https://github.com/vortex-rdf/vortex-rdf) describes the
//! column layouts, secondary indexes and builders.
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
//!     // Builders consume `RawQuad`s: terms in the N-Triples form the columns
//!     // store. `RawQuad::from_quad` and `parse_quads_from_reader` yield them;
//!     // `RawQuad::canonical` makes one from strings.
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

#[cfg(all(not(feature = "file-io"), not(target_arch = "wasm32")))]
compile_error!(
    "vortex-rdf-core supports two build configurations: native with `file-io` (on by default), \
     and wasm32-unknown-unknown without default features. Enable `file-io` for a native build."
);

pub mod columns;
pub mod common;
pub(crate) mod debug;
mod error;
pub mod io;
mod kernels;
mod session;
// The quad store. Its public items, like `error`'s, are re-exported below and
// reached at the crate root only.
mod store;

pub use error::{Result, VortexRdfError};

pub use store::{
    BuiltArray, CaseMap, CodeChunkStream, CodePattern, DictReader, DictSnapshot,
    DictionaryQuadSink, IndexComponentInfo, IndexType, Indexes, Keep, KindRanges, LayoutStrategy,
    LocatedCodes, NumOp, Number, Probe, QuadColumn, RawQuad, RowChunkStream, RowCountHint, RowId,
    SelectionKind, SharedQuad, SortOrder, SortedInMemoryBuilder, TermCode, TermPredicate,
    TextOptions, TextPredicate, Verdict, ViewStatistics, VortexArrayBuilder, VortexRdfStore,
    export_rdf,
};

/// The vortex session holding this crate's store layout, encodings and write
/// editions: hand it to vortex's own reader to open this crate's files, whose
/// root scans as the quad table.
pub fn vortex_session() -> &'static vortex_session::VortexSession {
    &session::VORTEX_SESSION
}

// The out-of-core builder needs a filesystem to spill to.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use store::SortedStreamBuilder;

#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
use mimalloc::MiMalloc;
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// The README's Rust snippets as doctests; the file-conversion snippet needs
/// `file-io`.
#[cfg(all(doctest, feature = "file-io"))]
#[doc = include_str!("../../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
mod tests;
