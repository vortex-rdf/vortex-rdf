use crate::error::Result;
use crate::index::RdfDictionary;
use crate::store::layout::cottas::{CottasLayout, CottasLayoutBuilder, TripleOrdering};
use crate::store::layout::IndexBuilder;
use crate::store::vortex_rdf_store::VortexRdfStore;
use futures::Stream;
use oxrdf::Quad;
use vortex_array::ArrayRef;

pub type CottasVortexStore<Dict> = VortexRdfStore<Dict, CottasLayout>;

impl<Dict> VortexRdfStore<Dict, CottasLayout>
where
    Dict: RdfDictionary,
{
    const DEFAULT_ROW_GROUP_SIZE: usize = 1024;

    pub async fn build_spog_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        Self::build_ordered_vortex_index(
            quad_stream,
            TripleOrdering::SPO,
            Self::DEFAULT_ROW_GROUP_SIZE,
        )
        .await
    }

    pub async fn build_ordered_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
        ordering: TripleOrdering,
        row_group_size: usize,
    ) -> Result<ArrayRef> {
        IndexBuilder::build::<Dict, _>(
            quad_stream,
            CottasLayoutBuilder::new(ordering, row_group_size),
        )
        .await
    }
}
