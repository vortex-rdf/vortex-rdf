pub mod vortex_rdf_store;
pub mod builders;
pub use builders::{
    VortexArrayBuilder,
    UnsortedInMemoryBuilder,
    SortedInMemoryBuilder,
    ChunkSortBuilder,
    GlobalSortBuilder,
    BuilderStrategy
};
pub use vortex_rdf_store::VortexRdfStore;

use vortex_array::ArrayRef;
use futures::Stream;
use oxrdf::Quad;
use crate::error::Result;

#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use vortex_array::expr::Expression;
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;


// Trait for stores that can provide quads
pub trait QuadStore {
    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>>;
}

/// Lazily-decoded quad source — either fully in-memory (IPC / mutation path)
/// or file-backed (lazy scan path, loaded via `from_file`).
#[derive(Clone)]
pub(crate) enum QuadsSource {
    /// All quads held in a Vortex StructArray in host memory.
    InMemory(ArrayRef),
    /// Quads remain on disk; scanned lazily on each `quads()` or `match_pattern()` call.
    #[cfg(feature = "file-io")]
    File {
        file: Arc<VortexFile>,
        /// Optional filter expression (built by `match_pattern`).
        filter: Option<Expression>,
    },
}