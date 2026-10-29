//! In-memory construction of a [`VortexRdfStore`]: the constructors from a
//! quad stream, a builder's output and a store's serializable parts, the
//! empty store, and the layout, adoption and assembly steps they share.

use crate::error::{Result, VortexRdfError};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::store::builders::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::store::builders::SortedStreamBuilder;
use crate::store::builders::{BuiltArray, VortexArrayBuilder, build_struct_array};
use crate::store::indexes::{IndexComponent, Indexes};
use crate::store::layouts::{DictAccess, LayoutStrategy, ResolvedLayout};
use crate::store::zones::ZoneTable;
use crate::store::{
    Backend, MemoryBase, QuadsSource, RawQuad, VortexRdfStore, next_generation, probes, resident,
};

use std::sync::Arc;

use futures::Stream;

use vortex_array::ArrayRef;

/// The layout a store resolves to, built (`from_parts`, `from_built`) or
/// opened (`from_bytes`, `from_file`): a dictionary beside the rows (resident,
/// or left in its file child) makes it Dictionary, otherwise the rows' own
/// dtype decides. A dict-less Dictionary-layout dtype is refused: bare code
/// columns carry no way back to their terms.
pub(super) fn resolved_layout(
    dict: Option<DictAccess>,
    dtype: &vortex_array::dtype::DType,
) -> Result<ResolvedLayout> {
    match dict {
        Some(dict) => Ok(ResolvedLayout::Dictionary(dict)),
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

impl VortexRdfStore {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Build a store from a quad stream: the global `(s, p, o, g)` sort, the
    /// layout's columns, the requested indexes and the assembly in one call,
    /// through `SortedStreamBuilder` wherever a filesystem exists (so the sort
    /// is not bounded by memory) and `SortedInMemoryBuilder` on wasm. Both
    /// produce the same rows; to name one, pair its
    /// `VortexArrayBuilder::build_vortex_array` with
    /// [`from_built`](Self::from_built). The store is resident;
    /// `io::quads_stream_to_vortex_file` streams a dataset to a file instead.
    ///
    /// The quads must be canonical: build them with [`RawQuad::from_quad`],
    /// the parser ([`parse_quads_from_reader`]) or [`RawQuad::canonical`]. The
    /// builders intern and compare the spelling they are given, so a hand-built
    /// `RawQuad` in another spelling of the same terms is a different quad.
    ///
    /// [`parse_quads_from_reader`]: crate::common::terms::parse_quads_from_reader
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

    /// Rebuild a store from its serializable parts, the inverse of
    /// [`to_serializable_parts`](Self::to_serializable_parts) and the
    /// in-memory file open's adoption. Dict-less Dictionary parts are refused:
    /// bare code columns carry no way back to their terms. The base's and
    /// every component's integer children keep their encoding wherever an
    /// encoded-search probe binds them and are decoded otherwise
    /// (`with_searchable_int_children`).
    ///
    /// An `IsSorted` stamp on the `s` column is trusted as global
    /// `(s, p, o, g)` order, and the match fast paths search every bound role
    /// on it: rows sorted by subject alone must not carry it.
    ///
    /// `zones` are the base's code columns' zone bounds ([`ZoneTable`]): the
    /// zone maps of the file the parts were read from, or none.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_parts(parts: BuiltArray, zones: ZoneTable) -> Result<Self> {
        Self::adopt(
            parts,
            |base| Ok((resident::with_searchable_int_children(base)?, zones)),
            IndexComponent::into_searchable,
        )
    }

    /// Build a store from a builder's output: the primary quad array, the
    /// Dictionary layout's term dictionary and the requested indexes'
    /// components, adopted as they are. The store holds them compressed and
    /// resident: the base's code columns and each component's integer
    /// children are re-encoded into encodings the probes read in place
    /// (`with_compressed_int_children`), the base's also wrapped so the
    /// code-column read path decodes each once and is zero-copy after. The
    /// pass that compresses a code column also takes its zone bounds
    /// ([`ZoneTable`]).
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        Self::adopt(
            built,
            |base| {
                let mut zones = ZoneTable::default();
                let base = resident::with_compressed_int_children(base, true, Some(&mut zones))?;
                Ok((base, zones))
            },
            IndexComponent::into_compressed,
        )
    }

    /// The adoption step of [`from_parts`](Self::from_parts) and
    /// [`from_built`](Self::from_built): resolve the layout from `built`'s
    /// dictionary and dtype, put the base through `encode_base` (which also
    /// hands back the base's zone bounds) and every component through
    /// `encode_component`, and assemble the store.
    fn adopt(
        built: BuiltArray,
        encode_base: impl FnOnce(ArrayRef) -> Result<(ArrayRef, ZoneTable)>,
        encode_component: impl FnMut(IndexComponent) -> Result<IndexComponent>,
    ) -> Result<Self> {
        let layout = resolved_layout(built.dict.map(DictAccess::Resident), built.array.dtype())?;
        let (base, zones) = encode_base(built.array)?;
        let components = built
            .components
            .into_iter()
            .map(encode_component)
            .collect::<Result<Vec<_>>>()?;
        Self::assemble_resident(base, components, layout, zones)
    }

    /// Assemble an in-memory store from a primary base and its components,
    /// transforming neither: callers own the resident form of what they pass
    /// ([`adopt`](Self::adopt) compresses a build, or keeps an adopted store's
    /// wire encodings selectively).
    ///
    /// A Dictionary `layout` must hold its dictionary resident: an in-memory
    /// view decodes its rows without suspending, through that dictionary
    /// ([`in_memory_chunk`](Self::in_memory_chunk)).
    pub(super) fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
        zones: ZoneTable,
    ) -> Result<Self> {
        debug_assert!(
            !matches!(&layout, ResolvedLayout::Dictionary(access) if access.is_file_backed()),
            "an in-memory store's dictionary is resident"
        );
        let components: Arc<[IndexComponent]> = components.into();
        // The queryable index set follows the component roster, exactly as
        // the file path follows its child roster.
        let indexes = crate::store::indexes::indexes_from_components(&components);
        // Resolve the encoded-search probes at construction, so no query pays
        // the encoding-tree walk.
        let store_probes = probes::StructProbes::new();
        store_probes.warm(&base);
        store_probes.set_zones(&base, zones);
        for component in components.iter() {
            component.warm_probes();
        }
        Ok(Self {
            layout,
            indexes,
            generation: next_generation(),
            quads: QuadsSource::new(Backend::InMemory(MemoryBase {
                base,
                components,
                probes: store_probes,
                serve: None,
            })),
            tail: None,
        })
    }

    /// Create an empty in-memory store with Default layout.
    pub fn empty() -> Self {
        let base = build_struct_array(&[], LayoutStrategy::Default, false)
            .expect("the empty Default-layout chunk builds");
        Self::assemble_resident(
            base,
            Vec::new(),
            ResolvedLayout::Default,
            ZoneTable::default(),
        )
        .expect("an empty base assembles")
    }
}
