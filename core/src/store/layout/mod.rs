use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use futures::Stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::sync::Arc;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::{ArrayRef, LEGACY_SESSION, VortexSessionExecute};

pub mod flat;
pub mod cottas;

pub fn filter_with_bool_mask(quads: &ArrayRef, mask: ArrayRef) -> Result<ArrayRef> {
    let mut ctx = LEGACY_SESSION.create_execution_ctx();

    let bool_arr = mask
        .execute::<BoolArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let canonical_mask = bool_arr.to_mask_fill_null_false(&mut ctx);

    quads
        .filter(canonical_mask)
        .map_err(VortexRdfError::Vortex)
}

pub trait RdfQuadLayout<Dict>: Sized + Clone + Send + Sync + 'static
where
    Dict: RdfDictionary,
{
    /// Human-readable / serialized layout identifier.
    const STORAGE_LAYOUT: &'static str;

    /// Create an empty physical quads array.
    fn empty_quads() -> Result<ArrayRef>;

    /// Build a Vortex root array from a stream of quads.
    async fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef>;

    /// Extract quads from the root Vortex array.
    fn extract_quads(root: &ArrayRef) -> Result<ArrayRef>;

    /// Decode physical quads into RDF quads.
    fn quads<'a>(
        dictionary: &'a Dict,
        quads: &'a ArrayRef,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'a>>;

    /// Find matching rows for a quad pattern.
    fn find_mask(
        dictionary: &Dict,
        quads: &ArrayRef,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>>;

    /// Add one quad.
    fn add_quad(
        dictionary: &mut Dict,
        quads: &ArrayRef,
        quad: Quad,
    ) -> Result<ArrayRef>;

    /// Delete one quad.
    fn delete_quad(
        dictionary: &Dict,
        quads: &ArrayRef,
        quad: &Quad,
    ) -> Result<ArrayRef>;

    /// Extra root fields for this layout, if any.
    ///
    /// Flat layout returns empty vec.
    /// COTTAS returns row_group_stats, file_metadata, etc.
    fn extra_root_fields(_dictionary: &Dict, _quads: &ArrayRef) -> Result<Vec<(Arc<str>, ArrayRef)>> {
        Ok(vec![])
    }
}

pub trait RdfQuadLayoutBuilder<Dict>
where
    Dict: RdfDictionary,
{
    fn ingest(&mut self, quad: &Quad, dictionary: &mut Dict) -> Result<()>;

    fn finalize(&mut self, dictionary: &mut Dict) -> Result<()>;

    fn build_quads(&self) -> Result<ArrayRef>;

    fn build_extra_root_fields(&self) -> Result<Vec<(Arc<str>, ArrayRef)>> {
        Ok(vec![])
    }
}

pub struct IndexBuilder;

impl IndexBuilder {
    pub async fn build<Dict, Builder>(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
        mut builder: Builder,
    ) -> Result<ArrayRef>
    where
        Dict: RdfDictionary,
        Builder: RdfQuadLayoutBuilder<Dict>,
    {
        use futures::StreamExt;
        use vortex_array::arrays::{ConstantArray, StructArray};
        use vortex_array::validity::Validity;
        use vortex_array::IntoArray;

        let mut dictionary = Dict::new();

        let quads: Vec<Quad> = quad_stream
            .collect::<Vec<Result<Quad>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Quad>>>()?;

        for quad in &quads {
            builder.ingest(quad, &mut dictionary)?;
        }

        builder.finalize(&mut dictionary)?;

        let quads_array = builder.build_quads()?;

        let dict_fields_raw = dictionary.to_vortex_array()?;

        let mut field_names: Vec<Arc<str>> = Vec::new();
        let mut field_arrays: Vec<ArrayRef> = Vec::new();

        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), 1).into_array());

        for (name, arr) in dict_fields_raw {
            field_names.push(name.into());
            field_arrays.push(crate::common::indexes::wrap_array_in_list(arr)?);
        }

        for (name, arr) in builder.build_extra_root_fields()? {
            field_names.push(name);
            field_arrays.push(arr);
        }

        field_names.push("quads".into());
        field_arrays.push(crate::common::indexes::wrap_array_in_list(quads_array)?);

        let root = StructArray::try_new(
            field_names.into(),
            field_arrays,
            1,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(root)
    }
}