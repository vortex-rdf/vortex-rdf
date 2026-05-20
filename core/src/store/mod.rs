pub mod cottas_vortex_store;
pub mod vortex_rdf_store;
use futures::Stream;
use oxrdf::Quad;
use crate::error::Result;
use crate::index::RdfDictionary;
use vortex_array::ArrayRef;


// Trait for stores that can provide quads
pub trait QuadStore {
    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>>;
}

// Re-export the main stores
pub use cottas_vortex_store::CottasVortexStore;
pub use vortex_rdf_store::VortexRdfStore;

// store_like.rs (or inside store mod)
pub trait VortexRdfStoreLike<Dict: RdfDictionary>: crate::store::QuadStore {
    fn dictionary(&self) -> &Dict;
    fn quads_array(&self) -> &ArrayRef;

    fn get_quads_array(&self) -> Result<ArrayRef> {
        Ok(self.quads_array().clone())
    }

    fn size(&self) -> usize {
        self.quads_array().len()
    }

    fn quads_stream(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>>;
}