//! In-memory construction of a [`VortexRdfStore`]: the builder, parts and
//! raw-quad constructors, the empty store and the assembly step they share.

use std::iter;
use std::sync::Arc;

use futures::Stream;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

use crate::error::{Result, VortexRdfError};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::store::builders::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::store::builders::SortedStreamBuilder;
use crate::store::builders::{BuiltArray, VortexArrayBuilder, build_parts_from_raws};
use crate::store::indexes::{IndexComponent, Indexes};
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{DictAccess, LayoutStrategy, ResolvedLayout};
use crate::store::view::selection::ViewSelection;
use crate::store::{
    QuadsSource, RawQuad, StoreParts, VortexRdfStore, array, next_generation, probes, resident,
    schema,
};

/// The layout an in-memory construction resolves to: Dictionary-resident
/// with a dictionary beside the rows, else by the array's dtype. A dict-less
/// Dictionary-layout array is rejected.
pub(super) fn resolved_layout(
    dict: Option<Arc<TermDictionary>>,
    dtype: &vortex_array::dtype::DType,
) -> Result<ResolvedLayout> {
    match dict {
        Some(dict) => Ok(ResolvedLayout::Dictionary(DictAccess::Resident(dict))),
        None => match LayoutStrategy::from_dtype(dtype) {
            LayoutStrategy::TypedObject => Ok(ResolvedLayout::TypedObject),
            LayoutStrategy::Dictionary => Err(VortexRdfError::Deserialization(
                "Dictionary-layout rows carry no dictionary; a bare code array cannot \
                 self-describe — construct through a builder (`from_built`) or open a \
                 serialized form that carries its dictionary component"
                    .to_string(),
            )),
            LayoutStrategy::Default => Ok(ResolvedLayout::Default),
        },
    }
}

/// The compressed-resident form of a built base and its components: integer
/// children re-encoded into probe-supported encodings
/// ([`with_compressed_int_children`](array::with_compressed_int_children)),
/// the base payload-wrapped for the zero-copy code-column path.
pub(super) fn compress_built_parts(
    base: ArrayRef,
    components: Vec<IndexComponent>,
) -> Result<(ArrayRef, Vec<IndexComponent>)> {
    let base = resident::with_compressed_int_children(base, true)?;
    let components = components
        .into_iter()
        .map(IndexComponent::into_compressed)
        .collect::<Result<Vec<_>>>()?;
    Ok((base, components))
}

impl VortexRdfStore {
    /// Build a store from a quad stream: the global `(s, p, o, g)` sort, the
    /// layout's columns, the requested indexes and the store assembly in one
    /// call. The builder is the out-of-core `SortedStreamBuilder` wherever a
    /// filesystem exists and `SortedInMemoryBuilder` on wasm; name one
    /// directly ([`from_built`](Self::from_built)) to override. The finished
    /// store is resident; `io::quads_stream_to_vortex_file` streams a dataset
    /// to a file instead.
    pub async fn from_quads(
        quads: impl Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<Self> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let built =
            SortedInMemoryBuilder::build_vortex_array(Box::new(quads), layout, indexes).await?;
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let built =
            SortedStreamBuilder::build_vortex_array(Box::new(quads), layout, indexes).await?;
        Self::from_built(built)
    }

    /// Rebuild a store from [`StoreParts`], the inverse of
    /// [`to_serializable_parts`](Self::to_serializable_parts). Dict-less
    /// Dictionary parts are refused. Integer children stay compressed
    /// wherever an encoded-search probe binds them and are decoded otherwise
    /// (`with_searchable_int_children`), components likewise. An `IsSorted`
    /// stamp on the `s` column is trusted as global `(s, p, o, g)` order; rows
    /// sorted by subject alone must not carry it.
    pub fn from_parts(parts: StoreParts) -> Result<Self> {
        let layout = resolved_layout(parts.dict, parts.array.dtype())?;
        let base = resident::with_searchable_int_children(parts.array)?;
        let components = parts
            .components
            .into_iter()
            .map(IndexComponent::into_searchable)
            .collect::<Result<Vec<_>>>()?;
        Self::assemble_resident(base, components, layout)
    }

    /// Build from a builder's output: the primary quad array plus the term
    /// dictionary and index components it carries, adopted as they are.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        let layout = resolved_layout(built.dict, built.array.dtype())?;
        let (base, components) = compress_built_parts(built.array, built.components)?;
        Self::assemble_resident(base, components, layout)
    }

    /// Assemble a store from a primary base and its components, as given;
    /// the index set follows the component roster, and the encoded-search
    /// probes are resolved now.
    pub(super) fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
    ) -> Result<Self> {
        let components: Arc<[IndexComponent]> = components.into();
        let indexes = crate::store::indexes::indexes_from_components(&components);
        let store_probes = probes::StructProbes::new();
        store_probes.warm(&base);
        for component in components.iter() {
            component.warm_probes();
        }
        Ok(Self {
            layout,
            indexes,
            generation: next_generation(),
            quads: QuadsSource::InMemory {
                base,
                selection: ViewSelection::all(),
                components,
                deleted: None,
                probes: store_probes,
                serve: None,
            },
            tail: None,
        })
    }

    /// Create an empty in-memory store with Default layout.
    pub fn empty() -> Self {
        // One empty string column serves all four fields.
        let e = array::make_string_array(iter::empty::<&str>());

        let quads = StructArray::try_new(
            FieldNames::from(schema::PRIMARY_COLUMNS),
            vec![e.clone(), e.clone(), e.clone(), e],
            0,
            Validity::NonNullable,
        )
        .expect("empty StructArray")
        .into_array();

        Self {
            layout: ResolvedLayout::Default,
            indexes: vec![],
            generation: next_generation(),
            quads: QuadsSource::InMemory {
                base: quads,
                selection: ViewSelection::all(),
                components: Arc::from(Vec::new()),
                deleted: None,
                probes: probes::StructProbes::new(),
                serve: None,
            },
            tail: None,
        }
    }

    /// Build a fresh owning in-memory store from raw quads under `strategy` —
    /// the shared back half of compaction (see [`build_parts_from_raws`]).
    /// `sorted` must be `true` only when `raws` is SPOG-sorted.
    pub(super) fn from_raw_quads(
        raws: &[RawQuad],
        strategy: LayoutStrategy,
        indexes: Indexes,
        sorted: bool,
    ) -> Result<Self> {
        let (base, components, dict) = build_parts_from_raws(raws, strategy, &indexes, sorted)?;
        let layout = resolved_layout(dict, base.dtype())?;
        // Compress like every other construction — a compacted store carries
        // the same resident form a freshly built one does.
        let (base, components) = compress_built_parts(base, components)?;
        Self::assemble_resident(base, components, layout)
    }
}
