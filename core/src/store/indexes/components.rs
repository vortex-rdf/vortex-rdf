//! The persistence model of a secondary index: how its `_idx_*` row-space
//! columns become standalone *components* and back again.
//!
//! One [`IndexComponent`] is one child of a native store file (or its
//! in-memory twin): a set of rows under the child's own plain column names,
//! carrying the writer's sortedness provenance. This module owns both
//! directions of that mapping — splitting a builder's welded row space into
//! components ([`split_built_row_space`]), describing the children a
//! serializer should write ([`index_component_specs`]), and reading a
//! persisted child's implementation slug back onto a component identity.
//!
//! The column *names* on either side belong to the index modules, not here:
//! each leaf declares them once in its const [`ComponentRole`] table
//! (reached through [`IndexType::component_roles`]), and every loop below is
//! parameterized by those rows.

use std::sync::{Arc, OnceLock};

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::{DType, FieldName};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;

use super::{ALL_INDEX_TYPES, IndexType, Indexes, detect_indexes, strip_index_columns};

/// One persisted-child role of an index: the component's identity plus the
/// row-space columns it is assembled from. Each leaf module declares its
/// roles as a const table ([`IndexType::component_roles`]); the generic
/// loops here — spec push, row-space split, presence detection, the slug
/// registry — are parameterized by these rows instead of re-spelling the
/// scheme per leaf.
pub(crate) struct ComponentRole {
    /// The component name (`index:posg`, `index:ref-o`, …) — the same
    /// identity the persisted child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) slug: &'static str,
    /// The row-space `_idx_*` columns, in child-column order:
    /// `source_columns[i]` becomes the child's `child_columns[i]`.
    pub(crate) source_columns: &'static [&'static str],
    /// The child's own plain column names.
    pub(crate) child_columns: &'static [&'static str],
    /// The row-space column holding this role's leading sort key: the one
    /// the globally sorted emission stamps `IsSorted` and the split reads
    /// back as sortedness provenance.
    pub(crate) lead_source: &'static str,
}

/// Everything the read side knows about a persisted component slug: the role
/// row an in-memory [`IndexComponent`] adopts and the index type the
/// component makes queryable — see [`known_component`].
pub(crate) struct KnownComponent {
    /// The registry row carrying the component's identity and column scheme.
    pub(crate) role: &'static ComponentRole,
    /// The index type owning the component.
    pub(crate) index: IndexType,
}

/// The single slug registry: what a persisted component's implementation slug
/// means to this version, or `None` for foreign slugs (skippable when
/// optional). A lookup over the per-leaf role tables, so a new index (or a
/// renamed slug) is threaded through exactly one place — its
/// [`IndexType::component_roles`] row.
pub(crate) fn known_component(implementation: &str) -> Option<KnownComponent> {
    ALL_INDEX_TYPES.into_iter().find_map(|index| {
        index
            .component_roles()
            .iter()
            .find(|role| role.slug == implementation)
            .map(|role| KnownComponent { role, index })
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
/// children for serialization. The row-count check lives inside so every
/// adoption site applies it uniformly rather than each remembering it — and
/// it stays eager: a corrupt roster still fails at adoption, not at first
/// probe.
#[cfg(feature = "file-io")]
pub(crate) fn adopt_scanned_component(
    known: &KnownComponent,
    scanned: ArrayRef,
    sorted: bool,
    quad_rows: u64,
) -> Result<IndexComponent> {
    check_component_rows(known.role.name, scanned.len() as u64, quad_rows)?;
    Ok(IndexComponent {
        name: known.role.name,
        implementation: known.role.slug,
        rows: ComponentRows::Deferred(Arc::new(DeferredRows {
            source: DeferredSource::Scanned(scanned),
            cell: OnceLock::new(),
        })),
        sorted,
        probes: crate::store::probes::BaseProbes::new(),
    })
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
    check_component_rows(known.role.name, reader.row_count(), quad_rows)?;
    Ok(IndexComponent {
        name: known.role.name,
        implementation: known.role.slug,
        rows: ComponentRows::Deferred(Arc::new(DeferredRows {
            source: DeferredSource::Reader(reader),
            cell: OnceLock::new(),
        })),
        sorted,
        probes: crate::store::probes::BaseProbes::new(),
    })
}

/// One secondary-index component held in memory beside the store's primary
/// base: the in-memory twin of a native store file's index child, carrying
/// the same rows under the same child schema (plain column names — `s`, `p`,
/// `o`, `g`, `rid` for a copy family; `val`, `rid` for a reference role).
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
    pub(crate) implementation: &'static str,
    /// The component's rows: canonical from construction, or a deferred
    /// adoption canonicalized on first genuine use — reached through
    /// [`rows`](Self::rows)/[`as_array`](Self::as_array).
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
    /// lifetime; `into_resident` rebuilds the component and takes a fresh
    /// cache.
    probes: Arc<crate::store::probes::BaseProbes>,
}

/// How an [`IndexComponent`] holds its rows.
#[derive(Clone)]
enum ComponentRows {
    /// Canonical from construction — a builder's emission or a row-space
    /// split, already one struct in child schema.
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
    /// construction the builders and the row-space split use.
    pub(crate) fn built(
        name: &'static str,
        implementation: &'static str,
        array: StructArray,
        sorted: bool,
    ) -> Self {
        Self {
            name,
            implementation,
            rows: ComponentRows::Built(array),
            sorted,
            probes: crate::store::probes::BaseProbes::new(),
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
                        crate::io::native_file::scan_all_reader(reader.clone())
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

    /// The component's rows as an `ArrayRef` (an `Arc` bump past the first
    /// materialization). Consumed only by `ser`'s component write, gated the
    /// same way.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn as_array(&self) -> Result<ArrayRef> {
        Ok(self.rows()?.clone().into_array())
    }

    /// Whether the rows have been canonicalized yet — the laziness probe the
    /// deferral tests pin `from_bytes` on (those tests need `to_bytes`, hence
    /// the file-io bound).
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn is_materialized(&self) -> bool {
        match &self.rows {
            ComponentRows::Built(_) => true,
            ComponentRows::Deferred(deferred) => deferred.cell.get().is_some(),
        }
    }

    /// This component in resident form: rows materialized, integer children
    /// kept compressed wherever the encoded search probes bind them and
    /// decoded to canonical primitives otherwise (see
    /// [`array::with_searchable_int_children`]), so the sorted probes bind
    /// the value and `rid` columns directly instead of running the generic
    /// search kernel per call. The adoption step of a store loaded wholesale
    /// into memory; sortedness provenance is the component's own `sorted`
    /// field and carries across unchanged.
    ///
    /// [`array::with_searchable_int_children`]: crate::store::array::with_searchable_int_children
    pub(crate) fn into_resident(self) -> Result<Self> {
        use vortex_array::arrays::Struct;
        let rows = self.rows()?.clone().into_array();
        let canonical = crate::store::array::with_searchable_int_children(rows)?;
        // The helper only ever hands back a struct (its input was one), so
        // the downcast is a cast, not work.
        let array = match canonical.try_downcast::<Struct>() {
            Ok(array) => array,
            Err(other) => {
                let mut ctx = VORTEX_SESSION.create_execution_ctx();
                other
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?
            }
        };
        Ok(Self {
            rows: ComponentRows::Built(array),
            // The rebuilt rows are a different array; the old cache's
            // identity guard would only ever decline against them.
            probes: crate::store::probes::BaseProbes::new(),
            ..self
        })
    }

    /// This component with its integer children compressed into
    /// probe-supported encodings — the construction-side counterpart of
    /// [`into_resident`](Self::into_resident): a builder's canonical
    /// emission compresses here (see
    /// [`array::with_compressed_int_children`]), without the base's payload
    /// wrapper (components never serve code columns). The sorted probes bind
    /// the compressed columns directly.
    ///
    /// [`array::with_compressed_int_children`]: crate::store::array::with_compressed_int_children
    pub(crate) fn into_compressed(self) -> Result<Self> {
        use vortex_array::arrays::Struct;
        let rows = self.rows()?.clone().into_array();
        let compressed = crate::store::array::with_compressed_int_children(rows, false)?;
        let array = match compressed.try_downcast::<Struct>() {
            Ok(array) => array,
            Err(other) => {
                let mut ctx = VORTEX_SESSION.create_execution_ctx();
                other
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?
            }
        };
        Ok(Self {
            rows: ComponentRows::Built(array),
            probes: crate::store::probes::BaseProbes::new(),
            ..self
        })
    }

    /// The cached encoded-search probe over the component's `column`, or
    /// `None` when the column's encoding declines (callers fall back to the
    /// per-call search). Resolving materializes a deferred component, exactly
    /// like [`rows`](Self::rows).
    pub(crate) fn probe(
        &self,
        column: &str,
    ) -> Option<Arc<vortex_encoded_search::OwnedSortedProbe>> {
        let rows = self.rows().ok()?.clone().into_array();
        self.probes.by_name(&rows, column).cloned()
    }

    /// The component's shared probe cache, for a serve plan that outlives
    /// this reference.
    pub(crate) fn probes_arc(&self) -> Arc<crate::store::probes::BaseProbes> {
        Arc::clone(&self.probes)
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
        if let Some(known) = known_component(component.implementation)
            && !indexes.contains(&known.index)
        {
            indexes.push(known.index);
        }
    }
    indexes.sort_by_key(|index| index.preference_rank());
    indexes
}

/// Split a builder's welded row space into the store's model: the primary
/// quad array (index columns projected away) plus one [`IndexComponent`] per
/// persisted-child role the `_idx_*` columns assemble.
///
/// The builders keep emitting the welded form — it is also the streaming
/// write path's wire shape — and the store splits once at construction.
/// Sortedness is read off the welded columns' own `IsSorted` stamps, which
/// only the globally-sorted emission paths set (multi-chunk arrays lose
/// per-chunk stamps in canonicalization, correctly landing on `false`: a
/// concatenation of per-chunk sorts is not binary-searchable).
pub(crate) fn split_built_row_space(array: ArrayRef) -> Result<(ArrayRef, Vec<IndexComponent>)> {
    let detected = detect_indexes(array.dtype());
    if detected.is_empty() {
        return Ok((array, Vec::new()));
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = array
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut components = Vec::new();
    for index in &detected {
        for role in index.component_roles() {
            if let Some(component) = component_from_columns(&struct_arr, role)? {
                components.push(component);
            }
        }
    }
    let primary = strip_index_columns(struct_arr.into_array())?;
    Ok((primary, components))
}

/// Rebuild a role's component from row-space column arrays: relabel
/// `source_columns[i]` (shared, stats and all) to the child's
/// `child_columns[i]` names. `sorted` provenance comes from the lead
/// column's own stamp — set only by globally-sorted emission.
fn component_from_columns(
    struct_arr: &StructArray,
    role: &ComponentRole,
) -> Result<Option<IndexComponent>> {
    let mut arrays = Vec::with_capacity(role.source_columns.len());
    for column in role.source_columns {
        match struct_arr.unmasked_field_by_name(column) {
            Ok(a) => arrays.push(a.clone()),
            // A partial column set means this component was never built.
            Err(_) => return Ok(None),
        }
    }
    let sorted = role
        .source_columns
        .iter()
        .position(|c| *c == role.lead_source)
        .map(|i| crate::store::array::column_is_sorted(&arrays[i]))
        .unwrap_or(false);
    let array = child_struct(role.child_columns, arrays, struct_arr.len())?;
    Ok(Some(IndexComponent::built(
        role.name, role.slug, array, sorted,
    )))
}

/// Assemble a child's rows from its column arrays: `columns[i]` under the
/// `child_columns[i]` name, non-nullable throughout — the one construction
/// shared by the row-space split, the tee's chunk projection, and the
/// builders' direct child-chunk emissions.
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

/// One persisted child component of an index type: the descriptor written
/// into the native store root plus the role naming the row-space columns the
/// component is assembled from.
#[cfg(feature = "file-io")]
pub(crate) struct IndexComponentSpec {
    pub(crate) descriptor: crate::io::container::StoreComponentDescriptor,
    pub(crate) role: &'static ComponentRole,
}

/// The index components a row-space dtype implies, over every detected index
/// type — what the serializer turns into auxiliary children. `sorted` is the
/// writer's promise that the emitted columns are globally (not per-chunk)
/// sorted; it travels to the descriptors so a reader lifting the components
/// back into memory knows whether they are binary-searchable.
#[cfg(feature = "file-io")]
pub(crate) fn index_component_specs(
    dtype: &DType,
    sorted: bool,
) -> Result<Vec<IndexComponentSpec>> {
    use crate::io::container::{StoreComponentDescriptor, StoreComponentRole};
    let mut specs = Vec::new();
    for index in detect_indexes(dtype) {
        for role in index.component_roles() {
            specs.push(IndexComponentSpec {
                descriptor: StoreComponentDescriptor {
                    name: role.name.into(),
                    role: StoreComponentRole::Index,
                    implementation: role.slug.into(),
                    version: 1,
                    required: false,
                    sorted,
                    dtype: component_child_dtype(dtype, role)?,
                },
                role,
            });
        }
    }
    Ok(specs)
}

/// The child struct dtype a role's component assembles: the row-space
/// columns' dtypes under the child's own names.
#[cfg(feature = "file-io")]
fn component_child_dtype(dtype: &DType, role: &ComponentRole) -> Result<DType> {
    let DType::Struct(fields, _) = dtype else {
        return Err(VortexRdfError::Serialization(format!(
            "index components need a struct row space, got {dtype}"
        )));
    };
    let field_dtypes = role
        .source_columns
        .iter()
        .map(|n| {
            fields.field(n).ok_or_else(|| {
                VortexRdfError::Serialization(format!("row space misses index column {n}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(child_struct_dtype(role.child_columns, field_dtypes))
}

/// The child struct dtype under `child_columns` names — the dtype counterpart
/// of [`child_struct`], shared with the builders' direct child dtypes
/// (`copy_child_dtype`/`ref_child_dtype`) so the derived-from-row-space and
/// hand-built constructions cannot drift in shape.
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
