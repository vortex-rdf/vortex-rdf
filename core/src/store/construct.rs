//! In-memory construction of a [`VortexRdfStore`]: the constructors from a
//! quad stream, a builder's output, `StoreParts` and compaction's raw
//! quads, the empty store, and the layout, adoption and assembly steps they
//! share.

use crate::error::{Result, VortexRdfError};
#[cfg(any(feature = "file-io", test))]
use crate::store::StoreParts;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::store::builders::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::store::builders::SortedStreamBuilder;
use crate::store::builders::{
    BuiltArray, VortexArrayBuilder, build_parts_from_raws, build_struct_array,
};
use crate::store::indexes::{IndexComponent, Indexes};
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{DictAccess, LayoutStrategy, ResolvedLayout};
use crate::store::view::selection::ViewSelection;
use crate::store::{QuadsSource, RawQuad, VortexRdfStore, next_generation, probes, resident};

use std::sync::Arc;

use futures::Stream;

use vortex_array::ArrayRef;

/// The layout an in-memory construction (`from_parts`, `from_built`,
/// `from_bytes`, compaction's `from_raw_quads`) resolves to: a dictionary
/// held beside the rows makes it Dictionary-resident, otherwise the array's
/// own dtype decides. A dict-less Dictionary-layout array is rejected: bare
/// code columns carry no way back to their terms.
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

impl VortexRdfStore {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Build a store from a quad stream: the global `(s, p, o, g)` sort, the
    /// layout's columns, the requested indexes and the store assembly in one
    /// call. Callers who need neither a particular builder nor its
    /// intermediate [`BuiltArray`] should prefer this over pairing
    /// `VortexArrayBuilder::build_vortex_array` with
    /// [`from_built`](Self::from_built).
    ///
    /// The builder is picked by target: the out-of-core `SortedStreamBuilder`
    /// wherever a filesystem exists, so the sort itself is not bounded by
    /// memory, and `SortedInMemoryBuilder` on wasm, which has none. Both
    /// produce the same globally sorted rows; name one directly to override
    /// the choice.
    ///
    /// The finished store is resident either way. Writing a dataset larger
    /// than memory never has to materialize it — `io::quads_stream_to_vortex_file`
    /// streams the builder's chunks straight into the file writer instead.
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

    /// Rebuild a store from [`StoreParts`] — the inverse of
    /// [`to_serializable_parts`](Self::to_serializable_parts). A
    /// Dictionary-layout quad array must arrive with its dictionary beside
    /// it: dict-less Dictionary parts are refused (bare code columns carry no
    /// way back to their terms).
    ///
    /// This is the explicit resident adoption (the in-memory file open's): the
    /// base's integer children stay in their compressed form wherever the
    /// match fast paths can bind them through encoded search probes, and only
    /// children outside the probe's supported set are decoded to canonical
    /// primitives (see `with_searchable_int_children`); every index component
    /// is materialized into the same resident form, so its sorted probes bind
    /// directly too.
    ///
    /// The array's statistics are trusted as provenance: an `IsSorted` stamp
    /// on its `s` column asserts the rows are in global `(s, p, o, g)` order —
    /// the order every builder of this crate produces — and the match fast
    /// paths search every bound role on that basis. Rows sorted by subject
    /// alone must not carry it.
    #[cfg(any(feature = "file-io", test))]
    pub(crate) fn from_parts(parts: StoreParts) -> Result<Self> {
        Self::adopt(
            parts.built,
            resident::with_searchable_int_children,
            IndexComponent::into_searchable,
        )
    }

    /// Build from a builder's output: the primary quad array plus whatever the
    /// builder carries beside it — the Dictionary layout's term dictionary and
    /// the requested indexes' components, both adopted as they are. Nothing is
    /// re-derived or split out of the rows.
    ///
    /// The store holds them in the compressed-resident form: the base's code
    /// columns and each component's integer children are re-encoded into
    /// probe-supported encodings (see `with_compressed_int_children`), with
    /// the base additionally payload-wrapped so the code-column read path
    /// keeps its zero-copy fast path.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        Self::adopt(
            built,
            |base| resident::with_compressed_int_children(base, true),
            IndexComponent::into_compressed,
        )
    }

    /// Build a fresh owning in-memory store from raw quads under `strategy` —
    /// the shared back half of compaction (see [`build_parts_from_raws`]),
    /// adopted like any build ([`from_built`](Self::from_built)). `sorted`
    /// must be `true` only when `raws` is SPOG-sorted.
    pub(super) fn from_raw_quads(
        raws: &[RawQuad],
        strategy: LayoutStrategy,
        indexes: Indexes,
        sorted: bool,
    ) -> Result<Self> {
        Self::from_built(build_parts_from_raws(raws, strategy, &indexes, sorted)?)
    }

    /// The one adoption step of `from_parts` and
    /// [`from_built`](Self::from_built): resolve the layout from `built`'s
    /// dictionary and dtype, put the base through `encode_base` and every
    /// component through `encode_component`, and assemble the store.
    fn adopt(
        built: BuiltArray,
        encode_base: impl FnOnce(ArrayRef) -> Result<ArrayRef>,
        encode_component: impl FnMut(IndexComponent) -> Result<IndexComponent>,
    ) -> Result<Self> {
        let layout = resolved_layout(built.dict, built.array.dtype())?;
        let base = encode_base(built.array)?;
        let components = built
            .components
            .into_iter()
            .map(encode_component)
            .collect::<Result<Vec<_>>>()?;
        Self::assemble_resident(base, components, layout)
    }

    /// Assemble a store from an already-split primary base plus its
    /// components — the shared tail of every in-memory construction path.
    ///
    /// Callers own the resident form of what they pass: [`adopt`](Self::adopt)
    /// compresses a build and keeps an adopted store's wire encodings
    /// selectively (`with_searchable_int_children`); this assembler transforms
    /// nothing.
    ///
    /// A Dictionary `layout` must hold its dictionary resident: an in-memory
    /// view decodes its rows without suspending, through that dictionary
    /// ([`in_memory_chunk`](Self::in_memory_chunk)).
    pub(super) fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
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
        let base = build_struct_array(&[], LayoutStrategy::Default, false)
            .expect("the empty Default-layout chunk builds");
        Self::assemble_resident(base, Vec::new(), ResolvedLayout::Default)
            .expect("an empty base assembles")
    }
}
