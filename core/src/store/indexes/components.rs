//! The persistence model of a secondary index: the *component*, which is the
//! one form index data ever takes.
//!
//! One [`IndexComponent`] is one child of a native store file (or its
//! in-memory twin): a set of rows under the child's own plain column names,
//! carrying the writer's sortedness provenance. Builders emit components
//! directly, beside primary-only quad rows; this module owns what happens to
//! them afterwards — assembling a child's rows ([`child_struct`]), adopting a
//! persisted one back into memory, and reading a persisted child's
//! implementation slug onto a component identity.
//!
//! The column *names* belong to the index modules, not here: each leaf
//! declares them once in its const [`ComponentIdentity`] table (reached through
//! [`IndexType::component_identities`]), and the loops below are parameterized by
//! those rows.

use std::sync::{Arc, OnceLock};

use vortex_array::arrays::StructArray;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::into_struct_array;

use super::{ALL_INDEX_TYPES, IndexType, Indexes};

/// The identity a persisted index child carries on the wire: its component
/// name and implementation slug. Each leaf module declares its identities as
/// a const table ([`IndexType::component_identities`]); the generic loops
/// here — the slug registry and the roster it builds — are parameterized by
/// these rows. The columns behind an identity are the leaf's business
/// ([`child_struct`] assembles them).
pub(crate) struct ComponentIdentity {
    /// The component name (`index:posg`, `index:ref-o`, …) — the same
    /// identity the persisted child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) slug: &'static str,
}

/// Everything the read side knows about a persisted component slug: the
/// identity row an in-memory [`IndexComponent`] adopts and the index type the
/// component makes queryable — see [`known_component`].
pub(crate) struct KnownComponent {
    /// The registry row carrying the component's name and implementation
    /// slug.
    pub(crate) identity: &'static ComponentIdentity,
    /// The index type owning the component.
    pub(crate) index: IndexType,
}

/// The single slug registry: what a persisted component's implementation slug
/// means to this version, or `None` for foreign slugs (skippable when
/// optional). A lookup over the per-leaf identity tables, so a new index (or a
/// renamed slug) is threaded through exactly one place — its
/// [`IndexType::component_identities`] row.
pub(crate) fn known_component(implementation: &str) -> Option<KnownComponent> {
    ALL_INDEX_TYPES.into_iter().find_map(|index| {
        index
            .component_identities()
            .iter()
            .find(|identity| identity.slug == implementation)
            .map(|identity| KnownComponent { identity, index })
    })
}

/// Every known index child holds exactly one row per quad; a mismatched child
/// means a corrupt or foreign file, and routing through it would return
/// silently wrong matches.
pub(crate) fn check_component_rows(name: &str, component_rows: u64, quad_rows: u64) -> Result<()> {
    if component_rows != quad_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "index component {} holds {} rows against {} quad rows",
            name, component_rows, quad_rows
        )));
    }
    Ok(())
}

/// Adopt a scanned persisted child as an in-memory [`IndexComponent`]:
/// `scanned` is the child's un-executed scan output, `sorted` the
/// descriptor's provenance. Canonicalization is *deferred* to the
/// component's first genuine use; the scan itself has already run, so this
/// form is safe over any segment source — it is how a file view lifts its
/// children for serialization. The row-count check runs here, eagerly: a
/// corrupt roster fails at adoption, not at first probe.
#[cfg(feature = "file-io")]
pub(crate) fn adopt_scanned_component(
    known: &KnownComponent,
    scanned: ArrayRef,
    sorted: bool,
    quad_rows: u64,
) -> Result<IndexComponent> {
    let rows = scanned.len() as u64;
    adopt_deferred(
        known,
        DeferredSource::Scanned(scanned),
        rows,
        sorted,
        quad_rows,
    )
}

/// Adopt a persisted child by its un-scanned reader — the fully deferred
/// form `from_bytes` uses: nothing of the child is read at open (the roster
/// row comes off the wire TOC alone), and scan plus canonicalization both
/// run on the component's first genuine use. The reader MUST sit over a
/// buffer-backed segment source (a `from_bytes` buffer): materialization is
/// synchronous, and only buffer-backed segment reads resolve without
/// pending. The row-count check reads the reader's own footer-known count,
/// so a corrupt roster still fails at open.
pub(crate) fn adopt_component_reader(
    known: &KnownComponent,
    reader: vortex_layout::LayoutReaderRef,
    sorted: bool,
    quad_rows: u64,
) -> Result<IndexComponent> {
    let rows = reader.row_count();
    adopt_deferred(
        known,
        DeferredSource::Reader(reader),
        rows,
        sorted,
        quad_rows,
    )
}

/// A deferred component over `source` — the shared tail of both adopters:
/// the row-count check against the quad rows, then the component under the
/// registry row's identity with a fresh probe cache.
fn adopt_deferred(
    known: &KnownComponent,
    source: DeferredSource,
    rows: u64,
    sorted: bool,
    quad_rows: u64,
) -> Result<IndexComponent> {
    check_component_rows(known.identity.name, rows, quad_rows)?;
    Ok(IndexComponent {
        name: known.identity.name,
        slug: known.identity.slug,
        rows: ComponentRows::Deferred(Arc::new(DeferredRows {
            source,
            cell: OnceLock::new(),
        })),
        sorted,
        probes: crate::store::view::probes::StructProbes::new(),
    })
}

/// One secondary-index component held in memory beside the store's primary
/// base: the in-memory twin of a native store file's index child, carrying
/// the same rows under the same child schema (plain column names — `s`, `p`,
/// `o`, `g`, `rid` for a copy family; `val`, `rid` for a reference family).
///
/// `rid` values address rows of the base the component was built against;
/// that is what keeps components valid across derived views (a
/// `RowSelection` narrows without renumbering) and what invalidates them on
/// any physical gather.
#[derive(Clone)]
pub(crate) struct IndexComponent {
    /// The component name (`index:posg`, `index:ref-o`, …) — the same
    /// identity the persisted child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) slug: &'static str,
    /// The component's rows: canonical from construction, or a deferred
    /// adoption canonicalized on first genuine use — reached through
    /// [`rows`](Self::rows).
    rows: ComponentRows,
    /// Whether the sort-key columns are GLOBALLY sorted — the writer's
    /// provenance, not an inspection: binary-search resolution is gated on
    /// this, and per-chunk-sorted data must never claim it (a false stamp
    /// corrupts query results; see the `sorted` field on the wire
    /// descriptor).
    pub(crate) sorted: bool,
    /// Lazily-resolved encoded-search probes over the component's columns,
    /// shared by every clone (probe resolution walks the encoding tree per
    /// call otherwise — the fixed cost of the resolvers' searches and the
    /// serve path's point reads). The rows are immutable once materialized,
    /// so the cache's array-identity guard holds for the component's
    /// lifetime; [`rebuilt`](Self::rebuilt) takes a fresh cache.
    probes: Arc<crate::store::view::probes::StructProbes>,
}

/// How an [`IndexComponent`] holds its rows.
#[derive(Clone)]
enum ComponentRows {
    /// Canonical from construction — a builder's emission, already one
    /// struct in child schema.
    Built(StructArray),
    /// Adopted from a serialized container without executing: the deferral
    /// state is `Arc`-shared so every clone of the component — and of the
    /// `Arc<[IndexComponent]>` roster the store's views share — sees one
    /// materialization.
    Deferred(Arc<DeferredRows>),
}

/// A deferred component's shared state: what remains to be run over the
/// persisted child, and the cell its one canonicalization lands in.
struct DeferredRows {
    source: DeferredSource,
    cell: OnceLock<StructArray>,
}

/// How much of the read pipeline a deferred component still owes.
enum DeferredSource {
    /// The scan already ran (holding its un-executed output — array metadata
    /// plus refcounts on the source's buffers); only canonicalization is
    /// deferred. Safe over any segment source.
    #[cfg(feature = "file-io")]
    Scanned(ArrayRef),
    /// Nothing ran: scan and canonicalization both defer. Only for readers
    /// over a buffer-backed segment source, whose scan resolves without
    /// pending — [`IndexComponent::rows`] drives it synchronously.
    Reader(vortex_layout::LayoutReaderRef),
}

impl IndexComponent {
    /// A component whose rows are already canonical in memory — the
    /// construction every builder's emission uses.
    pub(crate) fn built(
        name: &'static str,
        slug: &'static str,
        array: StructArray,
        sorted: bool,
    ) -> Self {
        Self {
            name,
            slug,
            rows: ComponentRows::Built(array),
            sorted,
            probes: crate::store::view::probes::StructProbes::new(),
        }
    }

    /// The component's rows, canonicalized to one struct in child schema —
    /// materializing a deferred adoption on first call. Two consumers racing
    /// on first touch may both run the pipeline, but the source is immutable
    /// so they canonicalize identical rows; whichever stores first wins and
    /// both read the stored struct — no lock is held across the run.
    pub(crate) fn rows(&self) -> Result<&StructArray> {
        use futures::FutureExt as _;

        match &self.rows {
            ComponentRows::Built(array) => Ok(array),
            ComponentRows::Deferred(deferred) => {
                if let Some(array) = deferred.cell.get() {
                    return Ok(array);
                }
                let scanned = match &deferred.source {
                    #[cfg(feature = "file-io")]
                    DeferredSource::Scanned(scanned) => scanned.clone(),
                    DeferredSource::Reader(reader) => {
                        // A buffer-backed scan's segment reads resolve
                        // synchronously (the `from_bytes` invariant every
                        // handle-free open already relies on), so the future
                        // completes on its first poll.
                        crate::io::read::scan_all_reader(reader.clone())
                            .now_or_never()
                            .unwrap_or_else(|| {
                                unreachable!(
                                    "a reader-deferred component only ever sits over a \
                                     buffer-backed segment source, whose scan resolves \
                                     synchronously"
                                )
                            })?
                    }
                };
                let mut ctx = VORTEX_SESSION.create_execution_ctx();
                let executed = scanned
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;
                Ok(deferred.cell.get_or_init(|| executed))
            }
        }
    }

    /// This component as a replayable native child write, its descriptor
    /// carrying the component's own sortedness provenance — the one way a
    /// component reaches the container writer, from a store's serialization
    /// and from a builder's chunk stream alike. Compiled only where a store
    /// can be written (file-io or wasm32).
    ///
    /// Writing is a genuine use: a deferred adoption materializes here, so
    /// the written child is byte-identical to an eagerly-adopted one's.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn to_write(&self) -> Result<crate::io::container::NativeComponentWrite> {
        use crate::io::container::{
            BufferedComponentSource, NativeComponentWrite, StoreComponentDescriptor,
            StoreComponentRole, default_child_strategy,
        };
        let array = self.rows()?.clone().into_array();
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: self.name.into(),
                role: StoreComponentRole::Index,
                implementation: self.slug.into(),
                version: 1,
                required: false,
                sorted: self.sorted,
                dtype: array.dtype().clone(),
            },
            Arc::new(
                BufferedComponentSource::try_new(vec![array]).map_err(VortexRdfError::Vortex)?,
            ),
            default_child_strategy(),
        )
        .map_err(VortexRdfError::Vortex)
    }

    /// Whether the rows have been canonicalized yet (a test-hook probe).
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn is_materialized(&self) -> bool {
        match &self.rows {
            ComponentRows::Built(_) => true,
            ComponentRows::Deferred(deferred) => deferred.cell.get().is_some(),
        }
    }

    /// This component with its rows materialized and passed through
    /// `transform`, held as canonical built rows under a fresh probe cache
    /// (the rebuilt rows are a different array, so the old cache's identity
    /// guard would only ever decline against them). Name, slug and
    /// sortedness provenance carry across unchanged.
    fn rebuilt(self, transform: impl FnOnce(ArrayRef) -> Result<ArrayRef>) -> Result<Self> {
        let rows = self.rows()?.clone().into_array();
        // The transforms only ever hand back a struct (their input was one),
        // so the downcast is a cast, not work.
        let array = into_struct_array(transform(rows)?)?;
        Ok(Self {
            rows: ComponentRows::Built(array),
            probes: crate::store::view::probes::StructProbes::new(),
            ..self
        })
    }

    /// This component in searchable form: rows materialized, integer
    /// children kept compressed wherever the encoded search probes bind them
    /// and decoded to canonical primitives otherwise (see
    /// [`array::with_searchable_int_children`]), so the sorted probes bind
    /// the value and `rid` columns directly instead of running the generic
    /// search kernel per call. The adoption step of a store loaded wholesale
    /// into memory.
    ///
    /// [`array::with_searchable_int_children`]: crate::store::array::with_searchable_int_children
    pub(crate) fn into_searchable(self) -> Result<Self> {
        self.rebuilt(crate::store::array::with_searchable_int_children)
    }

    /// This component with its integer children compressed into
    /// probe-supported encodings — the construction-side counterpart of
    /// [`into_searchable`](Self::into_searchable): a builder's canonical
    /// emission compresses here (see
    /// [`array::with_compressed_int_children`]). Components never serve code
    /// columns, so nothing ever needs their canonical form back; the sorted
    /// probes bind the compressed columns directly.
    ///
    /// [`array::with_compressed_int_children`]: crate::store::array::with_compressed_int_children
    pub(crate) fn into_compressed(self) -> Result<Self> {
        self.rebuilt(crate::store::array::with_compressed_int_children)
    }

    /// The cached encoded-search probe over the component's `column`, or
    /// `None` when the column's encoding declines (callers fall back to the
    /// per-call search). Resolving materializes a deferred component, exactly
    /// like [`rows`](Self::rows).
    pub(crate) fn probe(
        &self,
        column: &str,
    ) -> Option<Arc<vortex_rdf_encoded_search::OwnedSortedProbe>> {
        self.probes
            .by_name(self.rows().ok()?.as_ref(), column)
            .cloned()
    }

    /// The component's shared probe cache, for a serve plan that outlives
    /// this reference.
    pub(crate) fn probes_arc(&self) -> Arc<crate::store::view::probes::StructProbes> {
        Arc::clone(&self.probes)
    }

    /// Resolve this component's probes at construction, ahead of the first
    /// query — the component half of the eager resolution every in-memory
    /// construction does (see [`StructProbes::warm`]). A *deferred* component
    /// is left alone: materializing it here would undo the deferral that
    /// `from_bytes` adoption exists for.
    ///
    /// [`StructProbes::warm`]: crate::store::view::probes::StructProbes::warm
    pub(crate) fn warm_probes(&self) {
        if matches!(self.rows, ComponentRows::Deferred(_)) {
            return;
        }
        if let Ok(rows) = self.rows() {
            self.probes.warm(rows.as_ref());
        }
    }

    /// Look a component up by name, gated on its sortedness provenance — the
    /// shared front gate of the in-memory resolvers: absent and
    /// not-globally-sorted both mean the index declines (per-chunk-sorted
    /// rows are not binary-searchable).
    pub(crate) fn find_sorted<'a>(
        components: &'a [IndexComponent],
        name: &str,
    ) -> Option<&'a IndexComponent> {
        components.iter().find(|c| c.name == name && c.sorted)
    }
}

/// The index set a component roster implies, in declaration (preference)
/// order — the in-memory counterpart of reading a file's child roster.
pub(crate) fn indexes_from_components(components: &[IndexComponent]) -> Indexes {
    let mut indexes: Indexes = Vec::new();
    for component in components {
        if let Some(known) = known_component(component.slug)
            && !indexes.contains(&known.index)
        {
            indexes.push(known.index);
        }
    }
    indexes.sort_by_key(|index| index.preference_rank());
    indexes
}

/// Assemble a child's rows from its column arrays: `columns[i]` under the
/// `child_columns[i]` name, non-nullable throughout — the one construction
/// every builder's component emission goes through, in-memory and
/// out-of-core alike.
pub(crate) fn child_struct(
    child_columns: &[&'static str],
    columns: Vec<ArrayRef>,
    len: usize,
) -> Result<StructArray> {
    StructArray::try_new(
        child_columns
            .iter()
            .map(|n| FieldName::from(*n))
            .collect::<Vec<_>>()
            .into(),
        columns,
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
}

/// The child struct dtype under `child_columns` names — the dtype counterpart
/// of [`child_struct`], shared with the builders' direct child dtypes
/// (`copy_child_dtype`/`ref_child_dtype`) so a component's declared and built
/// shapes cannot drift. Compiled out on wasm with those builders.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn child_struct_dtype(
    child_columns: &[&'static str],
    field_dtypes: Vec<DType>,
) -> DType {
    use vortex_array::dtype::{Nullability, StructFields};
    DType::Struct(
        StructFields::new(
            child_columns
                .iter()
                .map(|n| (*n).into())
                .collect::<Vec<std::sync::Arc<str>>>()
                .into(),
            field_dtypes,
        ),
        Nullability::NonNullable,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::arrays::PrimitiveArray;

    #[test]
    fn check_component_rows_rejects_mismatch() {
        assert!(matches!(
            check_component_rows("index:ref-o", 3, 4),
            Err(VortexRdfError::Deserialization(_))
        ));
        assert!(check_component_rows("index:ref-o", 4, 4).is_ok());
    }

    /// A tiny built component under `slug`, for roster-shape tests.
    fn tiny(name: &'static str, slug: &'static str) -> IndexComponent {
        let column = PrimitiveArray::from_iter([0u32]).into_array();
        let rows = child_struct(&["val", "rid"], vec![column.clone(), column], 1).unwrap();
        IndexComponent::built(name, slug, rows, true)
    }

    #[test]
    fn indexes_from_components_orders_by_preference() {
        // Roster order is the wire order; the index set comes back in
        // preference order regardless, one entry per index.
        let roster = [
            tiny("index:ref-o", "secondary-by-reference/o"),
            tiny("index:posg", "secondary-by-copy/posg"),
            tiny("index:ref-p", "secondary-by-reference/p"),
        ];
        assert_eq!(
            indexes_from_components(&roster),
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
        );

        // A slug this version does not know implies no index.
        let foreign = [tiny("index:spog", "secondary-by-copy/spog")];
        assert!(indexes_from_components(&foreign).is_empty());
    }
}
