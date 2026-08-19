// The submodules are crate-private: every public item below is re-exported
// here (or at the crate root), so each has exactly one canonical public path.
pub(crate) mod array;
pub(crate) mod builders;
pub(crate) mod indexes;
pub(crate) mod layouts;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod probes;
pub(crate) mod scan;
pub(crate) mod schema;
pub(crate) mod selection;
pub(crate) mod source;

// [`VortexRdfStore`]'s impl clusters — the struct itself is defined below.
mod compaction;
mod matching;
mod mutation;
mod open;
mod rows;
mod serialize;
mod streaming;

pub use builders::{BuiltArray, BuiltStream, SortedInMemoryBuilder, VortexArrayBuilder};
// Compiled out on wasm along with the rest of the external-sort pipeline
// (see the module gate in `builders`).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use layouts::dictionary::DictSnapshot;
pub use layouts::dictionary::DictionaryQuadSink;
// `RawQuad` lives in `common` (it is pure RDF text — see that module's
// charter); this re-export keeps `store::RawQuad`, the path every builder
// consumer has always used.
pub use crate::common::quad::RawQuad;

pub(crate) use source::{QuadsSource, Tail};

use crate::error::{Result, VortexRdfError};
use layouts::dictionary::TermDictionary;
use layouts::{DictAccess, ResolvedLayout};
use selection::{RowSelection, ViewSelection};

use std::iter;
use std::sync::Arc;

use vortex_array::arrays::{StructArray, VarBinViewArray};
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// Columnar RDF quad storage backed by Vortex.
///
/// Stores quad terms according to the chosen layout strategy.
/// And applies indexes as chosen at build time.
#[derive(Clone)]
pub struct VortexRdfStore {
    /// The store's backing quad data, either an in-memory array or a lazily
    /// scanned Vortex file, together with the row selection, filters, and
    /// tombstones that define the rows visible through this store or view.
    quads: QuadsSource,
    /// The layout resolved against the backing array, carrying any state
    /// intrinsic to it (the Dictionary layout's term dictionary is loaded
    /// once at construction and propagated to derived stores, which may have
    /// lost the payload row through slicing/filtering).
    layout: ResolvedLayout,
    /// The secondary indexes whose columns this store's base schema carries,
    /// detected at construction (`detect_indexes`). Pattern matching plans
    /// index lookups against this set.
    ///
    /// Views derived through `match_pattern` keep their indexes: a view narrows
    /// a [`RowSelection`] over the base rather than rewriting rows, so the
    /// components' `rid` columns still address the base the ids were built
    /// against. Only physically gathering the rows — which renumbers them from
    /// zero, as [`compact_with_indexes`] does — invalidates those ids; it
    /// rebuilds the index set over the new order rather than carrying the old
    /// one across.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    indexes: Indexes,
    /// Rows appended since construction ([`add_quads`]), kept outside the base
    /// so appending never rewrites it — which is what lets the base's indexes
    /// and tombstones survive an append. `None` until something is appended.
    /// Queries run the base's fast paths plus a mask scan over the tail and
    /// union the two; [`compact_with_indexes`] folds the tail back in.
    ///
    /// [`add_quads`]: Self::add_quads
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    tail: Option<Tail>,
}

/// A store's serializable state, split the way the store holds it: the
/// primary quad rows, the in-memory index components describing them, and —
/// under the Dictionary layout — the term dictionary the rows' codes address.
/// Produced by [`VortexRdfStore::to_serializable_parts`] and adopted back by
/// [`VortexRdfStore::from_parts`] (the bindings' in-memory round-trip).
pub struct StoreParts {
    pub(crate) array: ArrayRef,
    pub(crate) components: Vec<crate::store::indexes::IndexComponent>,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The layout an in-memory construction resolves to: a dictionary held beside
/// the rows makes it Dictionary-resident, otherwise the array's own dtype
/// decides. A dict-less Dictionary-layout array is rejected: bare code
/// columns carry no way back to their terms — open the serialized form
/// (`from_file`/`from_bytes`), whose dictionary child carries the terms, or
/// construct through a builder (`from_built`).
fn resolved_layout(
    dict: Option<Arc<TermDictionary>>,
    dtype: &vortex_array::dtype::DType,
) -> Result<ResolvedLayout> {
    match dict {
        Some(dict) => Ok(ResolvedLayout::Dictionary(DictAccess::Resident(dict))),
        None => match LayoutStrategy::from_dtype(dtype) {
            LayoutStrategy::TypedObject => Ok(ResolvedLayout::TypedObject),
            LayoutStrategy::Dictionary => Err(VortexRdfError::Deserialization(
                "a bare Dictionary-layout array cannot self-describe; open its \
                 serialized form (`from_file`/`from_bytes`) or construct the store \
                 through a builder (`from_built`)"
                    .to_string(),
            )),
            LayoutStrategy::Default => Ok(ResolvedLayout::Default),
        },
    }
}

/// The compressed-resident form every in-memory construction produces: the
/// base's u32 code columns and each component's integer children are
/// re-encoded into probe-supported encodings (see
/// [`with_compressed_int_children`](array::with_compressed_int_children)),
/// with the base additionally payload-wrapped so the code-column read path
/// keeps its zero-copy fast path. Shared by the builder adoption
/// (`from_row_space`) and compaction's rebuild (`from_raw_quads`) — the two
/// places canonical built columns become a store.
fn compress_built_parts(
    base: ArrayRef,
    components: Vec<crate::store::indexes::IndexComponent>,
) -> Result<(ArrayRef, Vec<crate::store::indexes::IndexComponent>)> {
    let base = array::with_compressed_int_children(base, true)?;
    let components = components
        .into_iter()
        .map(crate::store::indexes::IndexComponent::into_compressed)
        .collect::<Result<Vec<_>>>()?;
    Ok((base, components))
}

impl VortexRdfStore {
    // ── constructors ─────────────────────────────────────────────────────────

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
    /// [`with_searchable_int_children`](array::with_searchable_int_children));
    /// every index component is materialized into the same resident form, so
    /// its sorted probes bind directly too.
    pub fn from_parts(parts: StoreParts) -> Result<Self> {
        let layout = resolved_layout(parts.dict, parts.array.dtype())?;
        let base = array::with_searchable_int_children(parts.array)?;
        let components = parts
            .components
            .into_iter()
            .map(crate::store::indexes::IndexComponent::into_resident)
            .collect::<Result<Vec<_>>>()?;
        Self::from_parts_internal(base, components, layout)
    }

    /// Test-only hook: whether every non-nullable integer child of an
    /// in-memory base is a canonical primitive — true only when adoption
    /// decoded everything (or the children were built canonical), so tests
    /// assert compression *retention* by negating it. Vacuously true for a
    /// file-backed store, whose base is not held in memory at all.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_base_int_children_canonical(&self) -> bool {
        use vortex_array::arrays::Struct;
        let QuadsSource::InMemory { base, .. } = &self.quads else {
            return true;
        };
        let Ok(struct_arr) = base.clone().try_downcast::<Struct>() else {
            return false;
        };
        debug_int_children_canonical(&struct_arr)
    }

    /// Test-only hook: whether one named child of an in-memory base is a
    /// canonical primitive — the per-child counterpart of
    /// [`debug_base_int_children_canonical`](Self::debug_base_int_children_canonical),
    /// so tests can pin retention of a specific encoding (e.g. a
    /// dictionary-encoded object column). `None` when the base has no such
    /// child or is not in memory.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_base_child_int_canonical(&self, name: &str) -> Option<bool> {
        use vortex_array::arrays::struct_::StructArrayExt;
        use vortex_array::arrays::{Primitive, Struct};
        let QuadsSource::InMemory { base, .. } = &self.quads else {
            return None;
        };
        let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
        let child = struct_arr.unmasked_field_by_name(name).ok()?;
        child.dtype().is_int().then(|| child.is::<Primitive>())
    }

    /// Test-only hook: whether the named in-memory component's rows hold
    /// canonical integer children — the component counterpart of
    /// [`debug_base_int_children_canonical`](Self::debug_base_int_children_canonical),
    /// so tests can pin that construction compresses components too. `None`
    /// when this store holds no such in-memory component.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_index_component_int_children_canonical(&self, name: &str) -> Option<bool> {
        let QuadsSource::InMemory { components, .. } = &self.quads else {
            return None;
        };
        let component = components.iter().find(|c| c.name == name)?;
        Some(debug_int_children_canonical(component.rows().ok()?))
    }

    /// Test-only hook: whether every sorted-stamped child of an in-memory
    /// base resolves an encoded search probe (see
    /// [`debug_sorted_children_probe_resolvable`]) — true for canonical and
    /// wire-encoded adoptions alike, false only when a bounds search on some
    /// child would fall through to the generic kernel. Vacuously true for a
    /// file-backed store.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_base_probe_resolvable(&self) -> bool {
        use vortex_array::arrays::Struct;
        let QuadsSource::InMemory { base, .. } = &self.quads else {
            return true;
        };
        let Ok(struct_arr) = base.clone().try_downcast::<Struct>() else {
            return false;
        };
        debug_sorted_children_probe_resolvable(&struct_arr)
    }

    /// Test-only hook: whether an in-memory base's `s` column carries the
    /// sorted stamp, so tests can assert the stamp survives adoption instead
    /// of only observing (order-insensitive) match results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_base_subject_sorted(&self) -> bool {
        match &self.quads {
            QuadsSource::InMemory { base, .. } => Self::base_subject_sorted(base),
            QuadsSource::File { .. } => false,
        }
    }

    /// Test-only hook: whether the dictionary was left in its child. A
    /// file-backed dictionary is only ever built around a resolved
    /// wire-chunk handle, so this is also the assertion that the child's
    /// layout shape was point-readable — a shape that declined opens
    /// resident instead.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_dict_file_backed(&self) -> bool {
        use crate::store::layouts::DictAccess;
        matches!(
            &self.layout,
            ResolvedLayout::Dictionary(DictAccess::FileBacked(_))
        )
    }

    /// Build from a builder's output: the quad array plus whatever the
    /// builder carries beside it — the Dictionary layout's term dictionary,
    /// and any index components emitted natively rather than as welded
    /// `_idx_*` columns (the out-of-core sorted strategy's), which are
    /// adopted directly. The direct construction path — no padding
    /// round-trip, no re-derivation, no re-split of pre-split components.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        Self::from_row_space(built.array, built.components, built.dict)
    }

    /// Adopt a (possibly welded) row-space array: split any `_idx_*` columns
    /// into in-memory [`IndexComponent`]s beside a pure primary base, append
    /// the components the caller already holds pre-split (a builder's native
    /// emission carries them beside a primary-only array), and derive the
    /// queryable index set from the resulting roster. The one place a
    /// builder's row space becomes the store's component model.
    ///
    /// [`IndexComponent`]: crate::store::indexes::IndexComponent
    fn from_row_space(
        array: ArrayRef,
        pre_split: Vec<crate::store::indexes::IndexComponent>,
        dict: Option<Arc<TermDictionary>>,
    ) -> Result<Self> {
        let (base, mut components) = crate::store::indexes::split_built_row_space(array)?;
        components.extend(pre_split);
        let layout = resolved_layout(dict, base.dtype())?;
        let (base, components) = compress_built_parts(base, components)?;
        Self::from_parts_internal(base, components, layout)
    }

    /// Assemble a store from an already-split primary base plus its
    /// components — the shared tail of every in-memory construction path.
    ///
    /// Callers own the resident form of what they pass: construction sites
    /// compress first ([`compress_built_parts`]), the adoption site keeps
    /// wire encodings selectively
    /// ([`with_searchable_int_children`](array::with_searchable_int_children));
    /// this assembler transforms nothing.
    fn from_parts_internal(
        base: ArrayRef,
        components: Vec<crate::store::indexes::IndexComponent>,
        layout: ResolvedLayout,
    ) -> Result<Self> {
        let components: Arc<[crate::store::indexes::IndexComponent]> = components.into();
        // The queryable index set follows the component roster, exactly as
        // the file path follows its child roster.
        let indexes = crate::store::indexes::indexes_from_components(&components);
        Ok(Self {
            layout,
            indexes,
            quads: QuadsSource::InMemory {
                base,
                selection: ViewSelection::all(),
                components,
                deleted: None,
                probes: probes::BaseProbes::new(),
                serve: None,
            },
            tail: None,
        })
    }

    /// Create an empty in-memory store with Default layout.
    pub fn empty() -> Self {
        // Build one empty string column and reuse it for all four fields —
        // they're all zero-length anyway, so there's nothing to distinguish.
        let e = VarBinViewArray::from_iter_str(iter::empty::<&str>()).into_array();

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
                probes: probes::BaseProbes::new(),
                serve: None,
            },
            tail: None,
        }
    }

    /// Derive a view over this store's base, narrowed to `selection`.
    ///
    /// The base and the index set carry over untouched: `selection` names base
    /// row ids, so nothing the indexes know has been invalidated, and the rows
    /// outside the selection remain reachable. The tail carries over as-is
    /// (its own selection is tail-local and untouched by a base narrowing);
    /// `match_pattern` narrows it separately.
    fn with_selection(&self, selection: RowSelection) -> Self {
        let quads = match &self.quads {
            QuadsSource::InMemory {
                base,
                components,
                deleted,
                probes,
                ..
            } => QuadsSource::InMemory {
                base: base.clone(),
                selection: ViewSelection::Exact(selection),
                // Shared, not rebuilt: the narrowed view's rows are still base
                // row ids, exactly what the components' `rid` columns address.
                components: Arc::clone(components),
                deleted: deleted.clone(),
                probes: Arc::clone(probes),
                // A re-selection breaks the serve plan's "row run is exactly the
                // selection" invariant, so it never carries across (as for File).
                serve: None,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter,
                deleted,
                ..
            } => QuadsSource::File {
                path: path.clone(),
                dict_max_resident_bytes: *dict_max_resident_bytes,
                file: file.clone(),
                filter: filter.clone(),
                selection: ViewSelection::Exact(selection),
                deleted: deleted.clone(),
                // A different selection breaks the serve plan's "filter selects
                // exactly the selection's rows" invariant, so it never carries
                // across a re-selection.
                serve: None,
            },
        };
        Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads,
            tail: self.tail.clone(),
        }
    }

    /// An empty view of this store: same base (and tail), selecting no row of
    /// either.
    ///
    /// Scans over it plan no work and `size()` answers 0 without touching the
    /// data. Indexes are dropped: chained matches on an empty view would
    /// otherwise run pointless lookups just to intersect with nothing.
    fn empty_view(&self) -> Self {
        let mut view = self.with_selection(RowSelection::empty());
        view.indexes = vec![];
        // Without the file backend the in-memory arm is the only variant.
        #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
        if let QuadsSource::InMemory { components, .. } = &mut view.quads {
            *components = Arc::from(Vec::new());
        }
        if let Some(tail) = &mut view.tail {
            tail.selection = RowSelection::empty();
        }
        view
    }

    /// The layout the tail's rows are stored in: the store's own, except under
    /// the Dictionary layout, where an appended term has no code in the sorted
    /// dictionary, so the tail holds Default-layout strings instead — patterns
    /// probe the base by code and the tail by string.
    fn tail_layout(&self) -> ResolvedLayout {
        match &self.layout {
            ResolvedLayout::Dictionary(_) => ResolvedLayout::Default,
            other => other.clone(),
        }
    }

    // ── ownership & compaction policy ────────────────────────────────────────

    /// The secondary indexes this store's schema carries.
    pub fn indexes(&self) -> &[IndexType] {
        &self.indexes
    }

    /// Number of physical rows in the append tail (including any tombstoned
    /// since they were appended); `0` when nothing has been appended or the
    /// tail has been compacted away.
    ///
    /// The tail is the store's only unindexed, unsorted region, so this is the
    /// number to watch when tuning compaction: `add_quads` folds it back into
    /// the base automatically once it crosses the thresholds — rewriting the
    /// source file for a file-backed store — and [`compact`](Self::compact)
    /// folds it on demand.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
    }

    /// This store as one that owns its rows and can be mutated — cheaply when it
    /// already is an owner, otherwise an independent, compacted copy.
    ///
    /// A view derived from `match_pattern` shares a base it does not own, so it
    /// cannot be mutated in place. This turns such a view into an owner by
    /// compacting, rebuilding its declared indexes
    /// ([`compact_with_indexes`]) so mutating a match result yields an
    /// independent store that is still indexed rather than one degraded to full
    /// scans. An owner is returned as a cheap clone, preserving its tombstones
    /// and indexes, so repeated in-place deletes stay cheap and keep their
    /// indexes.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn owned(&self) -> Result<Self> {
        if self.is_owner() {
            Ok(self.clone())
        } else {
            self.compact_with_indexes(self.indexes.clone()).await
        }
    }

    /// Whether this store owns its rows, rather than being a window onto
    /// someone else's.
    ///
    /// Only an owner may be mutated: a narrowed view's rows are a subset of a
    /// base it shares, so mutating it would either silently discard the rows
    /// outside the view or write through to data it doesn't own. A view that
    /// happens to select everything (an unconstrained `match_pattern`) covers
    /// exactly the same rows as the store it came from, so it counts as an
    /// owner — mutating it is indistinguishable from mutating that store.
    fn is_owner(&self) -> bool {
        // A narrowed tail marks a view just as a narrowed base does — a match
        // may cover all base rows yet only some of the tail's.
        let tail_owned = self
            .tail
            .as_ref()
            .is_none_or(|tail| matches!(tail.selection, RowSelection::All));
        tail_owned
            && match &self.quads {
                QuadsSource::InMemory { selection, .. } => selection.is_all(),
                #[cfg(feature = "file-io")]
                QuadsSource::File {
                    filter, selection, ..
                } => filter.is_none() && selection.is_all(),
            }
    }

    fn ensure_owner(&self, operation: &str) -> Result<()> {
        if self.is_owner() {
            return Ok(());
        }
        Err(VortexRdfError::Serialization(format!(
            "{operation} is not supported on a store derived from match_pattern: its rows are a \
             view onto a larger base, so mutating it would either silently drop the rows outside \
             the view or write through to data it does not own. Call owned() for an \
             independent copy to mutate, or call the mutation on the store the view came from."
        )))
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    pub fn layout(&self) -> LayoutStrategy {
        // Report the build-time strategy tag regardless of whether extra
        // state (like the Dictionary layout's term dictionary) is attached.
        self.layout.strategy()
    }

    /// An immutable handle on this store's term dictionary, or `None` when the
    /// store is not Dictionary-layout or the dictionary is not resident.
    ///
    /// Taking a snapshot is O(1) and retains only the dictionary — not the
    /// store, nor its quad columns. Crate-internal: the public accessor is
    /// [`code_read_snapshot`](Self::code_read_snapshot), which additionally
    /// gates on the codes this store's arrays serve being decodable.
    pub(crate) fn dictionary_snapshot(&self) -> Option<DictSnapshot> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) => {
                access.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
            }
            _ => None,
        }
    }

    /// An immutable handle on this store's term dictionary ([`DictSnapshot`]),
    /// gated on the codes this store's arrays serve being decodable against
    /// it — the one public dictionary accessor, and the one implementation of
    /// the code-read invariant, for every frontend that pairs a code-typed
    /// read
    /// ([`code_columns`](Self::code_columns)/[`code_columns_gathered`](Self::code_columns_gathered))
    /// with a decode handle. Taking a snapshot is O(1) and retains only the
    /// dictionary — not the store, nor its quad columns. Hand it to any
    /// consumer that holds term codes: see [`DictSnapshot`] for why the store
    /// itself is the wrong thing to decode against.
    ///
    /// `Some` requires all three of:
    /// - Dictionary layout — no other layout has codes at all;
    /// - an empty append tail — tail rows store terms as *strings* (an
    ///   appended term has no code in the sorted dictionary), so a tailed
    ///   view's rows are not fully code-addressable and gathering them
    ///   re-encodes against a fresh dictionary this handle would not be (see
    ///   [`get_quads_array`](Self::get_quads_array)'s contract);
    /// - a resident dictionary — a file-backed one has no in-memory snapshot
    ///   to hand out.
    ///
    /// Decoding codes against anything less than all three yields silently
    /// wrong terms, which is why bindings must take this gate rather than
    /// re-derive parts of it.
    pub fn code_read_snapshot(&self) -> Option<DictSnapshot> {
        if self.tail_len() != 0 {
            return None;
        }
        self.dictionary_snapshot()
    }

    /// Test-only: whether the named in-memory index component has
    /// canonicalized its rows yet (`None` when this store holds no such
    /// component) — how the deferral tests pin `from_bytes`' laziness. The
    /// tests need `to_bytes`, hence the file-io bound.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn index_component_materialized(&self, name: &str) -> Option<bool> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => components
                .iter()
                .find(|c| c.name == name)
                .map(|c| c.is_materialized()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => None,
        }
    }
}

/// Whether every non-nullable integer child of `struct_arr` is a canonical
/// primitive — the shared predicate behind the resident-adoption test hooks.
#[cfg(all(test, feature = "file-io"))]
fn debug_int_children_canonical(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::Primitive;
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        let int = child.dtype().is_int() && !child.dtype().is_nullable();
        !int || child.clone().try_downcast::<Primitive>().is_ok()
    })
}

/// Whether every sorted-stamped child of `struct_arr` binds an encoded
/// search probe — the property that keeps every bounds search off the
/// generic per-scalar kernel, whether the child is canonical or
/// wire-encoded. Unsorted children never take bounds searches, and their
/// equality path has the mask scan behind it, so they are not constrained.
#[cfg(all(test, feature = "file-io"))]
fn debug_sorted_children_probe_resolvable(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        !array::column_is_sorted(child)
            || vortex_encoded_search::SortedProbe::resolve(child).is_some()
    })
}

#[cfg(test)]
mod tests {
    use super::{StoreParts, VortexRdfStore};
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::struct_::StructArray;
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;

    /// A bare Dictionary-layout array cannot self-describe, and `from_parts`
    /// says so.
    #[test]
    fn test_from_parts_rejects_bare_dictionary_array() {
        let col = || Buffer::from_iter([1u32, 2, 3]).into_array();
        let array = StructArray::try_new(
            ["s", "p", "o", "g"].into(),
            vec![col(), col(), col(), col()],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let err = VortexRdfStore::from_parts(StoreParts {
            array,
            components: Vec::new(),
            dict: None,
        })
        .err()
        .expect("from_parts should fail");
        assert!(
            err.to_string().contains("from_built"),
            "unexpected error: {err}"
        );
    }
}
