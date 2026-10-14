//! In-memory construction of a [`VortexRdfStore`]: the constructors from a
//! quad stream, a builder's output, [`StoreParts`] and compaction's raw
//! quads, the empty store, and the layout, compression and assembly steps
//! they share.

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
    QuadsSource, RawQuad, StoreParts, VortexRdfStore, array, probes, resident, schema,
};

use std::iter;
use std::sync::Arc;

use futures::Stream;

use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

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

/// The compressed-resident form every in-memory construction produces: the
/// base's code columns and each component's integer children are
/// re-encoded into probe-supported encodings (see
/// [`with_compressed_int_children`](resident::with_compressed_int_children)),
/// with the base additionally payload-wrapped so the code-column read path
/// keeps its zero-copy fast path. Shared by the builder adoption
/// (`from_built`) and compaction's rebuild (`from_raw_quads`) — the two
/// places canonical built columns become a store.
fn compress_built_parts(
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
    /// This is the bindings' explicit resident adoption: the base's integer
    /// children stay in their compressed form wherever the match fast paths
    /// can bind them through encoded search probes, and only children outside
    /// the probe's supported set are decoded to canonical primitives (see
    /// `with_searchable_int_children`);
    /// every index component is materialized into the same resident form, so
    /// its sorted probes bind directly too.
    ///
    /// The array's statistics are trusted as provenance: an `IsSorted` stamp
    /// on its `s` column asserts the rows are in global `(s, p, o, g)` order —
    /// the order every builder of this crate produces — and the match fast
    /// paths search every bound role on that basis. Rows sorted by subject
    /// alone must not carry it.
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

    /// Build from a builder's output: the primary quad array plus whatever the
    /// builder carries beside it — the Dictionary layout's term dictionary and
    /// the requested indexes' components, both adopted as they are. Nothing is
    /// re-derived or split out of the rows.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        let layout = resolved_layout(built.dict, built.array.dtype())?;
        let (base, components) = compress_built_parts(built.array, built.components)?;
        Self::assemble_resident(base, components, layout)
    }

    /// Assemble a store from an already-split primary base plus its
    /// components — the shared tail of every in-memory construction path.
    ///
    /// Callers own the resident form of what they pass: construction sites
    /// compress first ([`compress_built_parts`]), the adoption site keeps
    /// wire encodings selectively
    /// ([`with_searchable_int_children`](resident::with_searchable_int_children));
    /// this assembler transforms nothing.
    pub(super) fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
    ) -> Result<Self> {
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
        // Build one empty string column and reuse it for all four fields —
        // they're all zero-length anyway, so there's nothing to distinguish.
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
