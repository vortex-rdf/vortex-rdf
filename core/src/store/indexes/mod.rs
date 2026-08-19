//! Secondary-index vocabulary and the store's dispatch into it.
//!
//! This hub owns what is common to every index: the [`IndexType`] enum and
//! its exhaustive dispatch into the per-index modules (column emission,
//! schema detection, resolution), the resolution currency
//! (`IndexResolution`, `IndexedComponent`, the eager/lazy `ResolvedRowIds`
//! split and its `LazyRowIds` recipe) both backends answer in, the planners
//! that try a store's whole index set in preference order, and the row-space
//! helpers shared across index families.
//!
//! What belongs in a leaf instead: an index's column-name scheme, its sort
//! orders, and how it probes them (`secondary_by_copy`,
//! `secondary_by_reference`) — the hub never hardcodes a column name.
//! Three further clusters live beside it: `serve` (reading matched quads out
//! of an index's own columns), `components` (the persisted-child model and
//! the slug registry), and `row_space` (the `_idx_` column-name convention),
//! all re-exported here so callers see one `indexes::` surface.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::DType;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, lit, root, select};
use vortex_array::scalar::Scalar;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout};

pub(crate) mod components;
pub(crate) mod row_space;
pub(crate) mod secondary_by_copy;
pub(crate) mod secondary_by_reference;
pub(crate) mod serve;
#[cfg(feature = "file-io")]
pub(crate) mod tee;

pub(crate) use components::{
    ComponentRole, IndexComponent, KnownComponent, adopt_component_reader, indexes_from_components,
    known_component, split_built_row_space,
};
#[cfg(feature = "file-io")]
pub(crate) use components::{IndexComponentSpec, adopt_scanned_component, index_component_specs};
#[cfg(feature = "file-io")]
pub(crate) use row_space::primary_dtype;
pub(crate) use row_space::strip_index_columns;
#[cfg(feature = "file-io")]
pub(crate) use serve::FileServePlan;
pub(crate) use serve::InMemoryServePlan;

/// A secondary index that can be embedded alongside the primary quad columns.
///
/// Variant declaration order is the resolution preference order: pattern
/// matching tries each detected index in this order and takes the first that
/// doesn't decline (see `resolve_indexes_in_memory`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum IndexType {
    /// Appends two complete extra copies of the quad columns, each in its own
    /// sort order and each paired with the primary row IDs it permutes — the
    /// classic triple-store permutation indexes, giving predicate- and
    /// object-bound patterns the same sorted-column access path the primary
    /// (s, p, o, g) order gives subjects.
    ///
    /// Adds ten extra columns to the StructArray (`VarBin<Utf8>` term strings,
    /// or u32 codes under the Dictionary layout; `_idx_*_rid` always `u32`):
    /// - `_idx_posg_{s,p,o,g,rid}`: the quads sorted by (p, o, s, g)
    /// - `_idx_ospg_{s,p,o,g,rid}`: the quads sorted by (o, s, p, g)
    ///
    /// Predicate-bound patterns binary-search `_idx_posg_p`; a bound
    /// predicate **and** object prefix-search (p, o) in one probe, resolving
    /// both components; object-bound patterns binary-search `_idx_ospg_o`.
    /// The copies additionally let reads take the matching rows from a
    /// *contiguous* run of the copy columns — sliced or point-read in memory,
    /// scanned from the index child on a file — instead of scattering row-id
    /// reads across the primary columns. As with
    /// [`SecondaryByReference`](Self::SecondaryByReference), in-memory routing
    /// engages only when the lead value columns carry the `IsSorted` statistic
    /// (single-chunk builds, or the sorted builders' global emission).
    ///
    /// Costs roughly 2× the primary columns in extra storage — choose it over
    /// `SecondaryByReference` when predicate/object reads dominate.
    SecondaryByCopy,

    /// Builds sorted secondary indexes for both predicates **and** objects.
    ///
    /// Adds four extra columns to the StructArray:
    /// - `_idx_o_val`: object values sorted (`VarBin<Utf8>`; u32 codes under
    ///   the Dictionary layout)
    /// - `_idx_o_rid`: primary row IDs corresponding to each sorted object (`u32`)
    /// - `_idx_p_val`: predicate values sorted (`VarBin<Utf8>`; u32 codes under
    ///   the Dictionary layout)
    /// - `_idx_p_rid`: primary row IDs corresponding to each sorted predicate (`u32`)
    ///
    /// Enables binary-search routing in `match_pattern` for predicate-only and
    /// object-only patterns, avoiding full scans. Routing engages only when
    /// the value columns carry the `IsSorted` statistic, which builders stamp
    /// when the columns hold a globally sorted order (single-chunk builds, or
    /// the sorted builders' global emission).
    SecondaryByReference,
}

/// The canonical index name: kebab-case (`"secondary-by-copy"`,
/// `"secondary-by-reference"`), the same spelling the `clap` derive exposes
/// on the CLI — so every frontend reports one vocabulary and a value printed
/// by one can be parsed by another.
impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IndexType::SecondaryByCopy => "secondary-by-copy",
            IndexType::SecondaryByReference => "secondary-by-reference",
        })
    }
}

/// Accepts exactly the canonical kebab-case names
/// [`Display`](std::fmt::Display) emits — `"secondary-by-copy"`,
/// `"secondary-by-reference"` — the one vocabulary every frontend shares.
impl std::str::FromStr for IndexType {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "secondary-by-copy" => Ok(IndexType::SecondaryByCopy),
            "secondary-by-reference" => Ok(IndexType::SecondaryByReference),
            _ => Err(VortexRdfError::Deserialization(format!(
                "unknown index type {s:?}; expected \"secondary-by-copy\" or \
                 \"secondary-by-reference\""
            ))),
        }
    }
}

/// Every [`IndexType`], in declaration = preference order — what
/// [`detect_indexes`] scans and what [`IndexType::preference_rank`] indexes
/// into. Adding a variant fails to compile at that exhaustive match until the
/// variant is listed here too.
pub(crate) const ALL_INDEX_TYPES: [IndexType; 2] =
    [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];

impl IndexType {
    /// This variant's position in [`ALL_INDEX_TYPES`] — the resolution
    /// preference order, as a sort key.
    pub(crate) const fn preference_rank(self) -> usize {
        match self {
            IndexType::SecondaryByCopy => 0,
            IndexType::SecondaryByReference => 1,
        }
    }

    /// This index's persisted-child roles — the const table every generic
    /// loop is parameterized by: the spec push and row-space split
    /// (`components`), schema detection ([`Self::is_present`]), the slug
    /// registry ([`known_component`]), and the sorted-column roster
    /// ([`globally_sorted_columns`]). The exhaustive match is the
    /// compile-fail anchor for those loops: a new variant answers here once
    /// and flows into all of them.
    pub(crate) const fn component_roles(self) -> &'static [ComponentRole] {
        match self {
            IndexType::SecondaryByCopy => &secondary_by_copy::ROLES,
            IndexType::SecondaryByReference => &secondary_by_reference::ROLES,
        }
    }

    /// Whether this index needs the sorted builders' global two-pass
    /// emission path so its value columns are globally sorted.
    // Asked only by the external-sort builder, compiled out on wasm (see the
    // module gate in `store::builders`).
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) fn needs_global_sorted_emission(self) -> bool {
        match self {
            IndexType::SecondaryByCopy => true,
            IndexType::SecondaryByReference => true,
        }
    }

    /// Append the columns contributed by this index type to the given field
    /// name/array vectors, sorting the chunk's own quads.
    ///
    /// This is the single dispatch point for per-chunk index-column
    /// generation: adding a new `IndexType` variant only requires a new arm
    /// here delegating to its dedicated module.
    ///
    /// `start_row` is the global row ID of the first quad in `quads`, so
    /// per-chunk builders can emit row IDs that address the fully assembled
    /// array. An empty `quads` slice yields empty columns with the correct
    /// dtypes (used for empty-store schemas).
    ///
    /// `whole_dataset` marks the chunk as spanning the entire dataset, making
    /// the chunk-local sort a global order (stamped `IsSorted` for routing).
    pub(crate) fn append_columns(
        self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        quads: &[RawQuad],
        start_row: u32,
        whole_dataset: bool,
    ) {
        match self {
            IndexType::SecondaryByCopy => secondary_by_copy::append_columns(
                field_names,
                field_arrays,
                quads,
                start_row,
                whole_dataset,
            ),
            IndexType::SecondaryByReference => secondary_by_reference::append_columns(
                field_names,
                field_arrays,
                quads,
                start_row,
                whole_dataset,
            ),
        }
    }

    /// Append this index's columns for a Dictionary-layout chunk, where terms
    /// are already encoded as u32 codes. Sorted-dictionary codes preserve
    /// lexicographic order, so a code-based index is order-equivalent to its
    /// string-based counterpart while sorting integers instead of strings and
    /// storing 4 bytes per entry.
    ///
    /// `start_row` and `whole_dataset` have the same semantics as
    /// [`Self::append_columns`].
    pub(crate) fn append_dictionary_columns(
        self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        codes: &QuadCodes,
        start_row: u32,
        whole_dataset: bool,
    ) {
        match self {
            IndexType::SecondaryByCopy => secondary_by_copy::append_encoded_columns(
                field_names,
                field_arrays,
                codes,
                start_row,
                whole_dataset,
            ),
            IndexType::SecondaryByReference => secondary_by_reference::append_encoded_columns(
                field_names,
                field_arrays,
                codes,
                start_row,
                whole_dataset,
            ),
        }
    }

    /// Whether this index's columns are present in an array/file of `dtype`:
    /// every role's source columns, all of them — a partial set (e.g. after
    /// a lossy projection) must not count as a usable index.
    ///
    /// The query-side counterpart of `append_columns`: each index module
    /// still owns its column-name scheme — this just reads the names off its
    /// role table rather than the store probing hardcoded ones.
    pub(crate) fn is_present(self, dtype: &DType) -> bool {
        match dtype {
            DType::Struct(fields, _) => self.component_roles().iter().all(|role| {
                role.source_columns
                    .iter()
                    .all(|name| fields.names().iter().any(|n| n.as_ref() == *name))
            }),
            _ => false,
        }
    }

    /// Resolve this index against an in-memory base array, producing the exact
    /// base row ids for whichever pattern component it covers.
    ///
    /// Each index owns its own execution: it decides which pattern shapes it
    /// accelerates (e.g. `SecondaryByReference` declines when a subject is
    /// bound), chooses and probes its columns, and hands back the row ids to
    /// select — or declines, leaving the store to fall back to a scan. Like
    /// `append_columns`, the exhaustive match makes the compiler demand a
    /// query-side answer from every new index variant.
    pub(crate) fn resolve_in_memory(
        self,
        components: &[IndexComponent],
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution<InMemoryServePlan>> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_in_memory(components, layout, pattern, codes)
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_in_memory(components, pattern, codes)
            }
        }
    }

    /// Resolve this index against a file-backed store, producing the exact
    /// primary row ids for whichever pattern component it covers — the
    /// file-backed counterpart of [`Self::resolve_in_memory`], differing only
    /// in how the index reaches its columns (a pushed-down scan instead of an
    /// in-memory binary search).
    #[cfg(feature = "file-io")]
    pub(crate) async fn resolve_file(
        self,
        file: &crate::store::native_file::NativeStoreFile,
        layout: &ResolvedLayout,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<IndexResolution<FileServePlan>> {
        match self {
            IndexType::SecondaryByCopy => {
                secondary_by_copy::resolve_file(file, layout, pattern, codes).await
            }
            IndexType::SecondaryByReference => {
                secondary_by_reference::resolve_file(file, pattern, codes).await
            }
        }
    }
}

/// Which pattern component(s) an index lookup resolves. The resolved
/// components can be omitted from any residual filtering over the fetched
/// rows — the index's row ids already are exactly their matches.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum IndexedComponent {
    Predicate,
    Object,
    /// Both predicate and object at once — a prefix search of the
    /// (p, o, …)-sorted copy in [`IndexType::SecondaryByCopy`].
    PredicateObject,
}

impl IndexedComponent {
    /// The pattern with this (index-resolved) component cleared: what still
    /// needs checking against the rows the index returned.
    pub(crate) fn clear<'a>(self, pattern: QuadPattern<'a>) -> QuadPattern<'a> {
        match self {
            IndexedComponent::Predicate => QuadPattern {
                predicate: None,
                ..pattern
            },
            IndexedComponent::Object => QuadPattern {
                object: None,
                ..pattern
            },
            IndexedComponent::PredicateObject => QuadPattern {
                predicate: None,
                object: None,
                ..pattern
            },
        }
    }
}

/// The outcome of asking an index to resolve a quad pattern against a backend.
///
/// Both backends answer in the same currency — ascending, unique *base* row ids
/// — so the store folds either one into a [`RowSelection`] the same way. `Plan`
/// is the backend's serve-plan type ([`InMemoryServePlan`] for in-memory
/// resolutions, `FileServePlan` for file ones), so a resolution can only ever
/// hand the store a plan its own backend can execute.
///
/// [`RowSelection`]: crate::store::selection::RowSelection
// `Resolved` dwarfs the dataless `Declined`, but the enum is a transient
// per-match return value that is destructured immediately, never stored in
// bulk — boxing its fields would buy nothing but an allocation per query.
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexResolution<Plan> {
    /// The index does not accelerate this pattern: either its shape isn't one
    /// this index covers, or (in memory) its value column isn't in a usable
    /// sorted form. The caller falls back to its non-indexed path.
    Declined,
    /// The index applies and proved the pattern matches no row — the probed
    /// term is absent from the indexed column. The caller short-circuits to an
    /// empty result.
    Empty,
    /// The index resolved `resolves`, yielding exactly `row_ids`: an
    /// ascending, unique set of base row ids (eager, or lazily computed — see
    /// [`ResolvedRowIds`]). The caller narrows its selection to those ids and
    /// drops `resolves` from any residual filtering, since the ids already
    /// satisfy it.
    ///
    /// `serve` is the optional, index-agnostic *serving plan*: when the index
    /// also holds the matched quads clustered in its own columns, it hands back
    /// the backend's plan so the store can read them straight from there instead
    /// of gathering the primary columns by scattered row id — a contiguous file
    /// scan or, for an in-memory base, a plain array slice. An index that stores
    /// only back-references (no whole quads) leaves it `None`. It is a pure
    /// optimization — `row_ids` already resolve the pattern on their own.
    Resolved {
        row_ids: ResolvedRowIds,
        resolves: IndexedComponent,
        serve: Option<Plan>,
    },
}

/// How a resolution answers its row ids.
///
/// `Eager` is a resolution that had to compute its ids to answer at all (a
/// back-reference probe, or a copy resolution without a serving plan) and is
/// non-empty by construction — an empty scan short-circuits to
/// [`IndexResolution::Empty`] instead. `Lazy` rides only alongside a serve
/// plan: the plan answers reads straight from the index's own
/// columns, so the ids — a second pass over the same data — are deferred until
/// a consumer actually needs the selection (a count, a chained match, a
/// delete, a base-order gather). A lazy resolution may therefore materialize
/// to an *empty* id set; consumers reach it through the view's pending
/// selection, which handles that like any other narrow selection.
pub(crate) enum ResolvedRowIds {
    Eager(Buffer<u64>),
    Lazy(LazyRowIds),
}

/// The exact base row ids of a serve-attached index resolution, computed on
/// first need and shared across every clone of the view that carries them.
///
/// The serving plan makes the ids redundant for the dominant
/// match-then-iterate flow — for a file-backed store they cost a whole extra
/// pushed-down scan of the index child — so the resolution hands back the
/// *recipe* instead and whichever consumer first needs the selection runs it.
/// The result lands in a shared cell: later consumers (and view clones made
/// before materialization) read it back for free. Two consumers racing on
/// first need may both run the recipe, but the source is immutable so they
/// compute identical ids; whichever stores first wins and both return the
/// stored buffer — no lock is held across the computation.
#[derive(Clone)]
pub(crate) struct LazyRowIds {
    cell: Arc<OnceLock<Buffer<u64>>>,
    source: LazyRowIdSource,
}

/// Where a [`LazyRowIds`]' ids come from — mirroring the serve plans'
/// per-backend split ([`InMemoryServePlan`] / `FileServePlan`), holding
/// exactly what the eager path would have consumed at resolution time.
#[derive(Clone)]
enum LazyRowIdSource {
    /// In-memory: the rid-column slice of the component's matched run, decoded
    /// and sorted on demand ([`sorted_row_ids`]).
    Component(ArrayRef),
    /// File-backed: the rid-only pushed-down scan of the index child
    /// ([`scan_index_row_ids`]) the eager path would have run at match time.
    #[cfg(feature = "file-io")]
    IndexChild {
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        rid_column: &'static str,
    },
}

impl LazyRowIds {
    /// Test-only hook: whether the ids have actually been computed — so a
    /// test can pin that a read was answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Lazy ids over an in-memory component's matched rid run.
    pub(crate) fn from_component_run(rids: ArrayRef) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::Component(rids),
        }
    }

    /// Lazy ids scanned from a file's index child on first need.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_index_child_scan(
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        rid_column: &'static str,
    ) -> Self {
        Self {
            cell: Arc::new(OnceLock::new()),
            source: LazyRowIdSource::IndexChild {
                reader,
                constraints,
                rid_column,
            },
        }
    }

    /// How many rows the ids cover, when knowable without computing them: an
    /// in-memory run knows its width up front (so a count on a served match
    /// never decodes), a file child only after materialization.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        match &self.source {
            LazyRowIdSource::Component(rids) => Some(rids.len()),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } => self.cell.get().map(Buffer::len),
        }
    }

    /// The ids, computing (and caching) them on first call.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized(&self) -> Result<Buffer<u64>> {
        if let Some(ids) = self.cell.get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component(rids) => sorted_row_ids(rids.clone())?,
            LazyRowIdSource::IndexChild {
                reader,
                constraints,
                rid_column,
            } => scan_index_row_ids(reader.clone(), constraints, rid_column).await?,
        };
        Ok(self.cell.get_or_init(|| ids).clone())
    }

    /// The synchronous counterpart of [`materialized`](Self::materialized),
    /// for in-memory sources — a file child's ids take I/O, and every
    /// consumer of a file view's selection is already async.
    pub(crate) fn materialized_sync(&self) -> Result<Buffer<u64>> {
        if let Some(ids) = self.cell.get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component(rids) => sorted_row_ids(rids.clone())?,
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } => {
                unreachable!("an in-memory view only ever carries component-sourced pending ids")
            }
        };
        Ok(self.cell.get_or_init(|| ids).clone())
    }
}

/// Binary-search a component's sorted `column` for the `[lo, hi)` run of rows
/// equal to `native`, searched `within` a row range whose slice of the column
/// is itself sorted (the whole component, or a lead run for a prefix probe) —
/// the probe step shared by the in-memory resolvers.
///
/// `None` when the column is missing or the probe can't cast to its dtype
/// (the resolver declines and the store falls back to a mask scan); an empty
/// range when the probed term is absent from the data.
pub(crate) fn sorted_probe_run(
    rows: &StructArray,
    column: &'static str,
    native: &Scalar,
    within: Range<usize>,
) -> Result<Option<Range<usize>>> {
    use crate::store::array::search_sorted_bounds;

    let Ok(col) = rows.unmasked_field_by_name(column) else {
        return Ok(None);
    };
    let Ok(scalar) = native.cast(col.dtype()) else {
        return Ok(None);
    };
    let run = col.slice(within.clone()).map_err(VortexRdfError::Vortex)?;
    let (lo, hi) = search_sorted_bounds(&run, &scalar)?;
    Ok(Some(within.start + lo..within.start + hi))
}

/// The row ids of a located index-child run, read point-by-point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order — the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan); rids are unique by construction, so sorting alone suffices.
///
/// `rid_column` comes from the calling index — the hub names no index's
/// columns.
#[cfg(feature = "file-io")]
pub(crate) async fn rid_point_reads(
    file: &crate::store::native_file::NativeStoreFile,
    component: &str,
    rid_column: &str,
    range: Range<u64>,
) -> Result<Option<Buffer<u64>>> {
    let Some(chunks) = file.component_column_chunks(component, rid_column) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    let mut ids = Vec::with_capacity((range.end - range.start) as usize);
    for row in range {
        match chunks
            .value_at(row, &source, session)
            .await
            .map_err(VortexRdfError::Vortex)?
        {
            Some(rid) => ids.push(rid),
            None => return Ok(None),
        }
    }
    ids.sort_unstable();
    Ok(Some(Buffer::from(ids)))
}

/// [`sorted_probe_run`] through a component's cached probe when its column
/// resolves one (skipping the per-call slice + encoding-tree walk): a full
/// search on the whole column, or a windowed search inside a lead run —
/// window-only, exactly like the slice-then-search path. String-valued
/// components (whose probe scalars are not integers) and probe-declined
/// encodings fall back to the per-call search.
pub(crate) fn component_probe_run(
    component: &IndexComponent,
    column: &'static str,
    native: &Scalar,
    within: Option<Range<usize>>,
) -> Result<Option<Range<usize>>> {
    if let Some(owned) = component.probe(column)
        && let Ok(needle) = u64::try_from(native)
    {
        let (lo, hi) = match within {
            None => owned.bounds(needle),
            Some(range) => owned.bounds_in(range, needle),
        };
        return Ok(Some(lo..hi));
    }
    let rows = component.rows()?;
    let within = within.unwrap_or(0..rows.len());
    sorted_probe_run(rows, column, native, within)
}

/// Decode a row-id column into the ascending, unique `Buffer<u64>` every index
/// resolution answers in.
///
/// The whole column is cast and decoded at once rather than pulled one scalar
/// at a time — this is the dominant cost for a frequent term with many matches.
/// Sorting is required, not incidental: the ids come out in the index's own
/// order, and both `Selection::IncludeByIndex` and the selection algebra need
/// them ascending. They are unique by construction (each index row references
/// one quad row), so sorting alone suffices.
pub(crate) fn sorted_row_ids(row_id_column: ArrayRef) -> Result<Buffer<u64>> {
    use vortex_array::builtins::ArrayBuiltins;
    use vortex_array::dtype::{Nullability, PType};

    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let ids = row_id_column
        .cast(DType::Primitive(PType::U64, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .into_buffer::<u64>();

    // The freshly-executed buffer is normally uniquely owned, so the sort
    // runs in place with no copy; a shared buffer (someone else still holds
    // the execution's output) falls back to one copy.
    match ids.try_into_mut() {
        Ok(mut ids) => {
            ids.as_mut_slice().sort_unstable();
            Ok(ids.freeze())
        }
        Err(ids) => {
            let mut sorted = ids.as_slice().to_vec();
            sorted.sort_unstable();
            Ok(Buffer::from(sorted))
        }
    }
}

/// Scan `row_id_column` for the rows where every `(value_column, probe)`
/// equality holds, returning the primary row ids as an ascending, unique
/// buffer (the shape vortex's `Selection::IncludeByIndex` requires) — the
/// file-backed probe shared by the secondary indexes.
///
/// Each equality is a plain `eq`, the same encoding the serve-plan filter
/// uses: on Vortex 0.83 a binary `Eq` falsifies against the identical zone
/// min/max envelope as a `>= probe AND <= probe` range pair (see vortex's
/// `stats/rewrite/builtins.rs`), while the pair evaluates two conjuncts to
/// compute the same mask — measured, `eq` was never slower on this rid-scan
/// path. One shared shape also keeps both paths hitting the same per-conjunct
/// pruning caches. Output order is irrelevant (the ids are sorted
/// afterwards), so the scan may run unordered.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_index_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    value_constraints: &[(&'static str, Scalar)],
    row_id_column: &'static str,
) -> Result<Buffer<u64>> {
    let mut filter: Option<Expression> = None;
    for (column, probe) in value_constraints {
        let expr = eq(get_item(*column, root()), lit(probe.clone()));
        filter = Some(match filter.take() {
            Some(f) => and(f, expr),
            None => expr,
        });
    }
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = filter else {
        return Ok(Buffer::empty());
    };

    read_scanned_row_ids(
        rid_scan(reader, row_id_column).with_filter(filter),
        row_id_column,
    )
    .await
}

/// The row ids of a *located* index-child run — the rows a resolver bounded
/// by binary-searching the child's cached chunk probes — read by a rid-only
/// scan restricted to that range. The wide-run counterpart of
/// [`rid_point_reads`], for runs too large to read point by point.
///
/// The scan carries no filter: the location bounded exactly the rows the
/// constraints select, so re-testing the value columns would only re-read and
/// re-compare them. A resolver whose location covers only *some* of its
/// constraints must keep [`scan_index_row_ids`], whose filter tests them all.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_located_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    row_id_column: &'static str,
    range: Range<u64>,
) -> Result<Buffer<u64>> {
    read_scanned_row_ids(
        rid_scan(reader, row_id_column).with_row_range(range),
        row_id_column,
    )
    .await
}

/// A rid-only scan of an index child: just the row-id column, unordered
/// (callers sort the ids anyway). Restrictions — a filter, a row range — are
/// the caller's to add.
#[cfg(feature = "file-io")]
fn rid_scan(
    reader: vortex_layout::LayoutReaderRef,
    row_id_column: &'static str,
) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
    vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
        .with_projection(select([row_id_column], root()))
        .with_ordered(false)
}

/// Run a rid-only scan and decode its row-id column into the ascending,
/// unique buffer every index resolution answers in.
#[cfg(feature = "file-io")]
async fn read_scanned_row_ids(
    scan: vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    row_id_column: &'static str,
) -> Result<Buffer<u64>> {
    let arr = scan
        .into_array_stream()
        .map_err(VortexRdfError::Vortex)?
        .read_all()
        .await
        .map_err(VortexRdfError::Vortex)?;

    if arr.is_empty() {
        return Ok(Buffer::empty());
    }

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    sorted_row_ids(
        struct_arr
            .unmasked_field_by_name(row_id_column)
            .cloned()
            .map_err(VortexRdfError::Vortex)?,
    )
}

/// An eager file resolution off the rid-only pushed-down scan — the shared
/// tail of the file resolvers whenever no serving plan defers the ids (a
/// back-reference child, or a copy resolution that couldn't build its plan):
/// `Empty` when the scan proves the probe matches nothing.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_eager_from_scan(
    reader: vortex_layout::LayoutReaderRef,
    constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    resolves: IndexedComponent,
) -> Result<IndexResolution<FileServePlan>> {
    let row_ids = scan_index_row_ids(reader, constraints, rid_column).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves,
        serve: None,
    })
}

/// Index types whose columns are present in `dtype` — how a store discovers
/// its queryable indexes from an array or file schema at construction.
///
/// Iterates [`ALL_INDEX_TYPES`], so a new variant flows in here (and into the
/// resolvers) with no store changes once it is listed there and its
/// `component_roles`/`resolve_*` arms exist.
pub(crate) fn detect_indexes(dtype: &DType) -> Indexes {
    ALL_INDEX_TYPES
        .iter()
        .copied()
        .filter(|index| index.is_present(dtype))
        .collect()
}

/// Resolve the pattern against the configured indexes over an in-memory array,
/// returning the first index whose outcome isn't `Declined` (indexes are tried
/// in declaration = preference order). `Declined` when none apply, so the store
/// can fall back to a mask scan.
///
/// The plural `indexes` name marks this as the planner over the store's whole
/// index set; the singular [`IndexType::resolve_in_memory`] it calls resolves
/// one index.
pub(crate) fn resolve_indexes_in_memory(
    indexes: &[IndexType],
    components: &[IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    for index in indexes {
        match index.resolve_in_memory(components, layout, pattern, codes)? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// File-backed counterpart of [`resolve_indexes_in_memory`]: the first index
/// whose file resolution isn't `Declined`, in declaration (preference) order.
///
/// Whether the matched rows can additionally be *served* from the answering
/// index's own columns rides along inside the resolution itself
/// ([`IndexResolution::Resolved::serve`]), so the store never needs to know
/// which index answered.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_indexes_file(
    indexes: &[IndexType],
    file: &crate::store::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    for index in indexes {
        match index.resolve_file(file, layout, pattern, codes).await? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// True when any requested index type requires the sorted builders' global
/// two-pass emission pipeline. The plural name marks the planner over the whole
/// index set; the singular [`IndexType::needs_global_sorted_emission`] it folds
/// over answers for one index.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn indexes_need_global_sorted_emission(indexes: &[IndexType]) -> bool {
    unique_indexes(indexes)
        .into_iter()
        .any(IndexType::needs_global_sorted_emission)
}

/// The set of optional secondary indexes to embed in a store.
///
/// An empty `Indexes` means no secondary index columns are written (fastest
/// write, full-scan queries only). Use `vec![IndexType::SecondaryByReference]`
/// for the compact (value, row-id) predicate/object indexes, or
/// `vec![IndexType::SecondaryByCopy]` for the full sorted quad copies.
pub type Indexes = Vec<IndexType>;

/// The index value columns a globally sorted emission guarantees sorted,
/// across every index type — what the sorted builders (re-)stamp `IsSorted`
/// after canonicalization, which drops per-chunk stats. Each role's lead
/// sort-key column, read off the same tables the row-space split reads its
/// sortedness provenance from, so the re-stamp and the resolution gate can
/// never disagree about which columns matter; the fold over
/// [`ALL_INDEX_TYPES`] × [`IndexType::component_roles`] flows a new variant
/// in the moment it declares its roles.
pub(crate) fn globally_sorted_columns() -> impl Iterator<Item = &'static str> {
    ALL_INDEX_TYPES
        .into_iter()
        .flat_map(|index| index.component_roles().iter().map(|role| role.lead_source))
}

/// Deduplicate the requested indexes, preserving first-seen order, so a
/// repeated index (e.g. the same `--indexes` flag passed twice) cannot
/// produce duplicate columns in the schema.
pub(crate) fn unique_indexes(indexes: &[IndexType]) -> Vec<IndexType> {
    let mut seen: Vec<IndexType> = Vec::with_capacity(indexes.len());
    for &idx in indexes {
        if !seen.contains(&idx) {
            seen.push(idx);
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term};
    use vortex_array::dtype::{Nullability, StructFields};

    fn struct_dtype(names: &[&str]) -> DType {
        DType::Struct(
            StructFields::from_iter(
                names
                    .iter()
                    .map(|n| (*n, DType::Utf8(Nullability::NonNullable))),
            ),
            Nullability::NonNullable,
        )
    }

    #[test]
    fn detect_indexes_by_schema() {
        // All four columns present: the index is detected.
        let with_idx = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_o_val",
            "_idx_o_rid",
            "_idx_p_val",
            "_idx_p_rid",
        ]);
        assert_eq!(
            detect_indexes(&with_idx),
            vec![IndexType::SecondaryByReference]
        );

        // The ten copy-family columns mark the SecondaryByCopy index.
        let with_copy = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
            "_idx_ospg_s",
            "_idx_ospg_p",
            "_idx_ospg_o",
            "_idx_ospg_g",
            "_idx_ospg_rid",
        ]);
        assert_eq!(detect_indexes(&with_copy), vec![IndexType::SecondaryByCopy]);

        // Both index families in one schema: copy first (preference order).
        let with_both = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_o_val",
            "_idx_o_rid",
            "_idx_p_val",
            "_idx_p_rid",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
            "_idx_ospg_s",
            "_idx_ospg_p",
            "_idx_ospg_o",
            "_idx_ospg_g",
            "_idx_ospg_rid",
        ]);
        assert_eq!(
            detect_indexes(&with_both),
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
        );

        // No index columns: nothing detected.
        let without_idx = struct_dtype(&["s", "p", "o", "g"]);
        assert!(detect_indexes(&without_idx).is_empty());

        // A partial column set (e.g. after a lossy projection) must not
        // count as a usable index.
        let partial = struct_dtype(&["s", "p", "o", "g", "_idx_o_val", "_idx_o_rid"]);
        assert!(detect_indexes(&partial).is_empty());
        let partial_copy = struct_dtype(&[
            "s",
            "p",
            "o",
            "g",
            "_idx_posg_s",
            "_idx_posg_p",
            "_idx_posg_o",
            "_idx_posg_g",
            "_idx_posg_rid",
        ]);
        assert!(detect_indexes(&partial_copy).is_empty());

        // Non-struct dtypes carry no indexes.
        assert!(detect_indexes(&DType::Utf8(Nullability::NonNullable)).is_empty());
    }

    #[test]
    fn indexed_component_clear() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());

        let bound = QuadPattern::new(Some(&s), Some(&p), Some(&o), Some(&g));

        let r = IndexedComponent::Object.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_some() && r.object.is_none() && r.graph.is_some()
        );

        let r = IndexedComponent::Predicate.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_some() && r.graph.is_some()
        );

        let r = IndexedComponent::PredicateObject.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_none() && r.graph.is_some()
        );
    }
}
