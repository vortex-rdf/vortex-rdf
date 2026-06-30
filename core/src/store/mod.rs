pub mod builders;
pub mod layout;
pub mod vortex_rdf_store;
pub use builders::{
    BuilderStrategy, ChunkSortBuilder, GlobalSortBuilder, SortedInMemoryBuilder,
    UnsortedInMemoryBuilder, VortexArrayBuilder,
};
pub use vortex_rdf_store::VortexRdfStore;

use crate::error::Result;
use futures::Stream;
use oxrdf::Quad;
use std::sync::Arc;
use vortex_array::ArrayRef;
use vortex_array::expr::Expression;
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
