pub mod vortex_rdf_store;

use futures::Stream;
use oxrdf::Quad;

// Re-export dictionary implementations for convenience
use crate::index::{SimpleDictionary, ChainedHash};
use crate::error::Result;

// Trait for stores that can provide quads
pub trait QuadStore {
    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>>;
}

// Re-export the main store
pub use vortex_rdf_store::VortexRdfStore;


// Type aliases for common store configurations
pub type SimpleDictionaryStore = VortexRdfStore<SimpleDictionary>;
pub type ChainedHashStore = VortexRdfStore<ChainedHash>;