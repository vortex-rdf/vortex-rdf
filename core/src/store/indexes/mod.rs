//! Secondary-index vocabulary and the store's dispatch into it: the
//! [`IndexType`] enum and its exhaustive dispatch into the per-index modules
//! (persisted-child identities, the children an in-memory build emits over
//! an [`IndexInput`], the probe a pattern shape chooses), the resolution
//! currency both backends answer in (`IndexResolution`, `ResolvedRoles`, the
//! eager/lazy `ResolvedRowIds` and its `LazyRowIds` recipe), and the
//! planners that try a store's indexes in preference order.
//!
//! A leaf (`copy`, `reference`) owns an index's column names, sort orders,
//! child builds and probe choice; the hub names no column but the primary
//! row id every child carries ([`COL_RID`]). Every probe runs through one
//! resolver per backend, `resolve` in memory and `file` on file. `serve`
//! (reading matched quads out of an index's own columns) and `components`
//! (the persisted-child model and the slug registry) are re-exported here,
//! so callers see one `indexes::` surface.

#[cfg(feature = "file-io")]
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::store::RawQuad;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout};
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::ChildReader;
use crate::store::schema::RowId;
use crate::store::view::selection::window_rows;
use resolve::IndexProbe;

pub(crate) mod components;
pub(crate) mod copy;
#[cfg(feature = "file-io")]
pub(crate) mod file;
pub(crate) mod reference;
mod resolve;
pub(crate) mod serve;

pub(crate) use components::{
    ComponentIdentity, IndexComponent, adopt_component, check_component_rows,
    indexes_from_components, known_component, run_order_row_ids, sorted_row_ids,
};
#[cfg(feature = "file-io")]
pub(crate) use file::{FileServePlan, read_located_rids, scan_index_row_ids};
pub(crate) use serve::{InMemoryServePlan, ServedRows};

/// The primary-row-id column every persisted index child carries beside its
/// own columns: the currency every resolution answers in, and the one column
/// name the index families share.
pub(crate) const COL_RID: &str = "rid";

/// The row id an indexed build gives its first row: 0, unless the tests'
/// `RowIdBase` hook offsets the ids (past `u32::MAX`, so a handful of quads
/// carry ids a 32-bit width cannot hold, or close to [`RowId::MAX`], so the
/// refusal of an id past the last one is reachable). Every reader takes the
/// base off again ([`base_row`]).
#[inline]
fn row_id_base() -> RowId {
    #[cfg(test)]
    if let Some(base) = crate::store::test_hooks::row_id_base() {
        return base;
    }
    0
}

/// The most quads an indexed build numbers: one per row id from the base up
/// to [`RowId::MAX`]. No store reaches it, but the builds count against it
/// with checked arithmetic ([`check_indexed_rows`], or row by row
/// [`next_row_id`]), so a row id is refused, never wrapped onto row 0.
#[inline]
fn row_limit() -> u64 {
    RowId::MAX - row_id_base()
}

/// `n` with thousands separators, as the refusal spells the limit.
fn with_separators(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A build's refusal of a store whose row ids would run past the last one;
/// `rows`, when known, is how many quads it would hold.
fn too_many_rows(rows: Option<u64>) -> VortexRdfError {
    VortexRdfError::Serialization(format!(
        "the store would exceed {} quads{}, the most a store with secondary indexes can \
         number: an index child records each row id as a u64, and no id is wrapped",
        with_separators(row_limit()),
        rows.map(|n| format!(" ({} quads)", with_separators(n)))
            .unwrap_or_default()
    ))
}

/// Refuses to index `rows` quads past the row limit: the check every
/// in-memory index build runs before it assigns a row id, which lets
/// [`row_id`] number the rows unchecked.
pub(crate) fn check_indexed_rows(rows: u64) -> Result<()> {
    if rows > row_limit() {
        return Err(too_many_rows(Some(rows)));
    }
    Ok(())
}

/// The id of the next row of an indexed build that has numbered `assigned`
/// rows so far, counting it: the checked increment of a build that numbers
/// rows as they stream past, without the count up front. The refusal comes
/// at the first row past the row limit, before any id wraps, and leaves the
/// count as it was. Compiled where its one caller, the out-of-core
/// builder, is.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn next_row_id(assigned: &mut u64) -> Result<RowId> {
    if *assigned >= row_limit() {
        return Err(too_many_rows(None));
    }
    // Below the limit, so neither sum overflows.
    let rid = row_id_base() + *assigned;
    *assigned += 1;
    Ok(rid)
}

/// Row `i`'s id as an index child records it, for `i` below a row count
/// [`check_indexed_rows`] admitted: the in-memory index builds' numbering.
/// The check comes first on every path that reaches here, so a sum past
/// [`RowId::MAX`] is a broken invariant, never a silent wrap.
pub(crate) fn row_id(i: usize) -> RowId {
    // A usize is at most 64 bits wide on every target: the cast is exact.
    let row = i as RowId;
    row_id_base()
        .checked_add(row)
        .expect("index builds refuse a store whose row ids would run past RowId::MAX")
}

/// The base row an index child's row id `rid` names: the id itself, less the
/// tests' `RowIdBase` offset under that hook (which must be the one the store
/// was built under).
#[inline]
pub(crate) fn base_row(rid: RowId) -> RowId {
    #[cfg(test)]
    {
        rid.checked_sub(row_id_base())
            .expect("a row id read under the RowIdBase hook was written under it")
    }
    #[cfg(not(test))]
    {
        rid
    }
}

/// The base row `row` as an `I` position, refused when `I` cannot hold it:
/// a row past what the index type addresses is past every row any base of
/// that width holds, so it is out of range, never narrowed onto another row.
/// [`row_index`] is the `usize` form every reader uses; this one is generic
/// so that the 32-bit case (wasm) is testable on any host.
pub(crate) fn index_of<I: TryFrom<RowId>>(row: RowId) -> Result<I> {
    I::try_from(row).map_err(|_| {
        VortexRdfError::Deserialization(format!(
            "an index child names row {row}, past the rows this platform addresses"
        ))
    })
}

/// The base row `row` as a `usize` position ([`index_of`]): exact on a
/// 64-bit target; on a 32-bit one, an error for a row past `usize::MAX`.
#[inline]
pub(crate) fn row_index(row: RowId) -> Result<usize> {
    index_of(row)
}

/// A secondary index, built as its own sorted children beside the primary
/// quad rows.
///
/// Variant declaration order is the resolution preference order: pattern
/// matching tries each index the store's component roster carries, in this
/// order, and takes the first that doesn't decline (see
/// `resolve_indexes_in_memory`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum IndexType {
    /// Two complete extra copies of the quad columns, each in its own sort
    /// order and paired with the primary row ids it permutes: the classic
    /// triple-store permutation indexes, giving predicate- and object-bound
    /// patterns the sorted-column access the primary (s, p, o, g) order gives
    /// subjects.
    ///
    /// Adds two children beside the quad rows, each a `{s, p, o, g, rid}`
    /// table (`VarBin<Utf8>` term strings, or u64 codes under the Dictionary
    /// layout; `rid` always a u64 [`RowId`]):
    /// - `index:posg`: the quads sorted by (p, o, s, g)
    /// - `index:ospg`: the quads sorted by (o, s, p, g)
    ///
    /// A bound predicate binary-searches `index:posg`'s `p` column, a bound
    /// predicate **and** object prefix-search (p, o) in one probe, and a
    /// bound object binary-searches `index:ospg`'s `o` column. Reads take the
    /// matching rows from a *contiguous* run of the copy columns instead of
    /// scattering row-id reads across the primary columns. Routing engages
    /// only on children their writer recorded as globally sorted, as every
    /// build here does.
    SecondaryByCopy,

    /// Sorted back-references for both predicates **and** objects.
    ///
    /// Adds two children beside the quad rows, each a `{val, rid}` table:
    /// - `index:ref-o`: object values sorted (`VarBin<Utf8>`; u64 codes under
    ///   the Dictionary layout), each paired with the primary row id (a u64
    ///   [`RowId`]) it came from
    /// - `index:ref-p`: the same for predicate values
    ///
    /// A pattern with a bound predicate or object and no bound subject
    /// binary-searches them instead of scanning (the object's child when both
    /// are bound). Routing engages only on children their writer recorded as
    /// globally sorted, as every build here does.
    SecondaryByReference,
}

/// The canonical index name: kebab-case (`"secondary-by-copy"`,
/// `"secondary-by-reference"`), the spelling the `clap` derive exposes on the
/// CLI, so every frontend reports one vocabulary and a value one prints
/// another parses.
impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IndexType::SecondaryByCopy => "secondary-by-copy",
            IndexType::SecondaryByReference => "secondary-by-reference",
        })
    }
}

/// Accepts exactly the canonical kebab-case names
/// [`Display`](std::fmt::Display) emits (`"secondary-by-copy"`,
/// `"secondary-by-reference"`), the one vocabulary every frontend shares. Any
/// other name is an [`InvalidInput`](VortexRdfError::InvalidInput) error.
impl std::str::FromStr for IndexType {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "secondary-by-copy" => Ok(IndexType::SecondaryByCopy),
            "secondary-by-reference" => Ok(IndexType::SecondaryByReference),
            _ => Err(VortexRdfError::InvalidInput(format!(
                "unknown index type {s:?}; expected \"secondary-by-copy\" or \
                 \"secondary-by-reference\""
            ))),
        }
    }
}

/// Every [`IndexType`], in declaration (preference) order: what the slug
/// registry scans, what the in-memory index set
/// ([`indexes_from_components`]) is filtered out of, and the order an
/// in-memory build emits the requested indexes' children in
/// ([`build_components`](crate::store::builders::build_components)).
pub(crate) const ALL_INDEX_TYPES: [IndexType; 2] =
    [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];

/// What an in-memory index build reads: the complete dataset in final row
/// order, as its quads' term strings or, under the Dictionary layout, as
/// their codes. A row's position in the input is its row id ([`row_id`]).
#[derive(Copy, Clone)]
pub(crate) enum IndexInput<'a> {
    /// The quads of a string layout (Default, TypedObject).
    Quads(&'a [RawQuad]),
    /// The Dictionary layout's codes.
    Codes(&'a QuadCodes),
}

impl IndexInput<'_> {
    pub(crate) fn rows(self) -> usize {
        match self {
            IndexInput::Quads(quads) => quads.len(),
            IndexInput::Codes(codes) => codes.s.len(),
        }
    }
}

impl IndexType {
    /// This index's persisted-child identities: the static table the generic
    /// loops are parameterized by, the slug registry ([`known_component`])
    /// and each leaf's component build. The exhaustive match makes a new
    /// variant answer here once for all of them.
    pub(crate) const fn component_identities(self) -> &'static [ComponentIdentity] {
        match self {
            IndexType::SecondaryByCopy => &copy::IDENTITIES,
            IndexType::SecondaryByReference => &reference::IDENTITIES,
        }
    }

    /// This index's persisted children over `input`, one per identity, in
    /// [`component_identities`](Self::component_identities) order, each
    /// globally sorted over the whole dataset and stamped so. The exhaustive
    /// match demands a build from every new variant.
    pub(crate) fn components(self, input: IndexInput<'_>) -> Result<Vec<IndexComponent>> {
        match self {
            IndexType::SecondaryByCopy => copy::components(input),
            IndexType::SecondaryByReference => reference::components(input),
        }
    }

    /// The probe this index runs for `pattern` (the child it searches, by
    /// which sort keys, what a hit resolves, and whether the child serves the
    /// matched quads), or `None` for a shape it does not accelerate
    /// (`SecondaryByReference` declines a bound subject, say), and the store
    /// tries the next index or falls back to a scan. A probe without keys
    /// bounds no run, so it is declined here, for both backends.
    ///
    /// The one step each index owns at query time: one resolver per backend
    /// executes every index's probe (`resolve::resolve_in_memory`,
    /// `file::resolve_file`). The exhaustive match demands a query-side
    /// answer from every new variant.
    fn choose<'a>(
        self,
        pattern: QuadPattern<'a>,
        layout: &ResolvedLayout,
    ) -> Option<IndexProbe<'a>> {
        let probe = match self {
            IndexType::SecondaryByCopy => copy::choose(pattern, layout),
            IndexType::SecondaryByReference => reference::choose(pattern),
        };
        probe.filter(|probe| !probe.keys.is_empty())
    }
}

/// Which pattern components an index lookup resolves; residual filtering
/// over the fetched rows omits them, as the index's row ids are exactly
/// their matches.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedRoles {
    Predicate,
    Object,
    /// Both predicate and object at once: a prefix search of the
    /// (p, o, …)-sorted copy in [`IndexType::SecondaryByCopy`].
    PredicateObject,
}

impl ResolvedRoles {
    /// The pattern with this (index-resolved) component cleared: what still
    /// needs checking against the rows the index returned.
    pub(crate) fn clear<'a>(self, pattern: QuadPattern<'a>) -> QuadPattern<'a> {
        match self {
            ResolvedRoles::Predicate => QuadPattern {
                predicate: None,
                ..pattern
            },
            ResolvedRoles::Object => QuadPattern {
                object: None,
                ..pattern
            },
            ResolvedRoles::PredicateObject => QuadPattern {
                predicate: None,
                object: None,
                ..pattern
            },
        }
    }
}

/// The outcome of asking an index to resolve a quad pattern on a backend.
///
/// Both backends answer in ascending, unique *base* row ids, which the store
/// folds into a [`RowSelection`] alike. `Plan` is the backend's serve-plan
/// type ([`InMemoryServePlan`] in memory, `FileServePlan` on file), so a
/// resolution can only hand the store a plan its own backend executes.
///
/// [`RowSelection`]: crate::store::view::selection::RowSelection
// `Resolved` dwarfs the dataless `Declined`, but the enum is a transient
// per-match return value that is destructured immediately and never stored
// in bulk.
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexResolution<Plan> {
    /// The index does not accelerate this pattern: either its shape isn't one
    /// this index covers, or (in memory) its value column isn't in a usable
    /// sorted form. The caller falls back to its non-indexed path.
    Declined,
    /// The index applies and proved the pattern matches no row: the probed
    /// term is absent from the indexed column. The caller short-circuits to an
    /// empty result.
    Empty,
    /// The index resolved `resolves`, yielding exactly `row_ids`: ascending,
    /// unique base row ids, eager or lazy ([`ResolvedRowIds`]). The caller
    /// narrows its selection to them and drops `resolves` from any residual
    /// filtering.
    ///
    /// `serve` is the optional *serving plan* of an index that holds the
    /// matched quads in its own columns, so the store reads them there (a
    /// contiguous file scan, or an array slice in memory) instead of
    /// gathering the primary columns by scattered row id; a back-reference
    /// index leaves it `None`. The plan only optimizes: `row_ids` resolve
    /// the pattern on their own.
    Resolved {
        row_ids: ResolvedRowIds,
        resolves: ResolvedRoles,
        serve: Option<Plan>,
    },
}

/// How a resolution answers its row ids.
///
/// `Eager`: the ids were computed at match time (the pushed-down scan an
/// unlocated back-reference probe needs, or a small located run's rid point
/// reads); non-empty by construction, as an empty scan answers
/// [`IndexResolution::Empty`]. Only the file resolver answers so: an
/// in-memory resolution is always `Lazy`, so a build without `file-io` has
/// no `Eager`.
///
/// `Lazy`: the ids are a recipe. The match leaves them pending beside a serve
/// plan it keeps, which reads from the index's own columns, or alone over a
/// located run of known width on a view built for a count or a window
/// (`IdsNeed::CountOrWindow`), whose width answers the count, and computes
/// them at once otherwise. Pending ids are computed when a consumer needs the
/// selection (a count the width cannot answer, a chained match, a delete, a
/// base-order gather), and may materialize to an *empty* set, which the
/// view's pending selection handles like any other narrow selection.
pub(crate) enum ResolvedRowIds {
    #[cfg(feature = "file-io")]
    Eager(Buffer<RowId>),
    Lazy(LazyRowIds),
}

/// The exact base row ids of an index resolution whose consumer may not need
/// them (a served one, or a located run that is only counted or windowed):
/// the *recipe*, run by the first consumer that needs the selection, its
/// result in a cell shared by every clone of the view that carries it. A
/// serve plan reads without the ids, which on file cost a whole extra scan
/// of the index child, and a located run's width counts without them.
///
/// Two consumers racing on first need may both run the recipe; the source is
/// immutable, so they compute the same ids, whichever stores first wins and
/// both return the stored buffer. No lock is held across the computation.
#[derive(Clone)]
pub(crate) struct LazyRowIds {
    /// The caches a match's ids fill, shared with every clone and the serve
    /// plan; `None` for a narrowed view's ids, which keep their own sort
    /// beside them ([`LazyRowIdSource::Unsorted`]) and so cost a narrowing
    /// no allocation.
    cells: Option<Arc<LazyCells>>,
    source: LazyRowIdSource,
}

/// What a [`LazyRowIds`] computes on first need, kept for every clone of the
/// view that carries it and, in memory, for its serve plan: the ids in base
/// order, and a component run's ids in the component's own order. A served
/// read takes the run order, decoded once; a consumer that needs base order
/// sorts once, from the run order when that is decoded.
#[derive(Default)]
pub(crate) struct LazyCells {
    sorted: OnceLock<Buffer<RowId>>,
    run_order: OnceLock<Buffer<RowId>>,
}

impl LazyCells {
    /// The ids of the component run `rids` (a rid-column slice), in the
    /// component's order, decoded on the first call ([`run_order_row_ids`])
    /// and kept: the one decode a served view's reads and its lazy ids
    /// share.
    pub(crate) fn run_order_of(&self, rids: &ArrayRef) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.run_order.get() {
            return Ok(ids.clone());
        }
        let ids = run_order_row_ids(rids.clone())?;
        Ok(self.run_order.get_or_init(|| ids).clone())
    }
}

/// Where a [`LazyRowIds`]' ids come from, split by backend as the serve plans
/// are ([`InMemoryServePlan`], `FileServePlan`), holding what the eager path
/// would have consumed at resolution time.
#[derive(Clone)]
enum LazyRowIdSource {
    /// In-memory: the rid-column slice of the component's matched run, decoded
    /// and sorted on demand ([`sorted_row_ids`]). `ascending` when the run's
    /// rids are already in row id order, so a window of the run can be cut
    /// before anything is decoded.
    Component { rids: ArrayRef, ascending: bool },
    /// A narrowed served view's ids, in any order (its index's), sorted into
    /// base order only on first need, into `sorted`: a cache each clone made
    /// before that copies rather than shares, as base-order consumers of a
    /// narrowed view are rare. `live` when they leave out every tombstoned
    /// row, so they count as they are.
    Unsorted {
        ids: Buffer<RowId>,
        live: bool,
        sorted: OnceLock<Buffer<RowId>>,
    },
    /// File-backed: the rid-only pushed-down scan of the index child
    /// ([`scan_index_row_ids`]) the eager path would have run at match time.
    /// The child binds through its file's memo under its own name, so the
    /// deferred scan binds with the same identity every plan and eager scan
    /// of this component uses (see `BoundExprMemo`). `located` is the run of
    /// child rows the constraints select, when the resolver located it:
    /// exactly the rows the scan keeps, so its width is the ids' count. It
    /// answers [`len_if_known`](LazyRowIds::len_if_known) and nothing else:
    /// a serving child's rids follow its own sort order, so no window of the
    /// run is a window of its ids.
    #[cfg(feature = "file-io")]
    IndexChild {
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        located: Option<Range<u64>>,
    },
    /// File-backed: a located run of an index child's rid column, read on
    /// first need through [`read_located_rids`]; its width is known up front,
    /// so a count reads none of it.
    #[cfg(feature = "file-io")]
    LocatedRun {
        file: Arc<crate::store::persist::native_file::NativeStoreFile>,
        child: ChildReader,
        range: Range<u64>,
    },
}

impl LazyRowIds {
    /// Test-only hook: whether the ids have been computed, so a test can pin
    /// a read answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.sorted_cell().get().is_some()
    }

    /// Where the ids sorted into base order are kept: a narrowed view's own
    /// cache, else the match's shared one.
    fn sorted_cell(&self) -> &OnceLock<Buffer<RowId>> {
        match (&self.source, &self.cells) {
            (LazyRowIdSource::Unsorted { sorted, .. }, _) => sorted,
            (_, Some(cells)) => &cells.sorted,
            (_, None) => unreachable!("a match's ids carry their caches"),
        }
    }

    /// The caches a match's ids share with the view's clones and its serve
    /// plan.
    fn shared_cells(&self) -> &Arc<LazyCells> {
        self.cells
            .as_ref()
            .expect("a match's ids carry their caches")
    }

    /// The caches these ids fill, for a serve plan reading the same run to
    /// share ([`LazyCells`]).
    pub(crate) fn cells(&self) -> Arc<LazyCells> {
        Arc::clone(self.shared_cells())
    }

    /// Lazy ids over an in-memory component's matched rid run, in the
    /// component's own order; `ascending` when those rids are already in row
    /// id order, as a reference component's are within one value (its rows
    /// are ordered by `(val, rid)`).
    pub(crate) fn from_component_run(rids: ArrayRef, ascending: bool) -> Self {
        Self {
            cells: Some(Arc::default()),
            source: LazyRowIdSource::Component { rids, ascending },
        }
    }

    /// Lazy ids of a narrowed served view: `ids` in any order, sorted into
    /// base order on first need. `live` when they leave out every tombstoned
    /// row.
    pub(crate) fn from_unsorted_ids(ids: Buffer<RowId>, live: bool) -> Self {
        Self {
            cells: None,
            source: LazyRowIdSource::Unsorted {
                ids,
                live,
                sorted: OnceLock::new(),
            },
        }
    }

    /// Lazy ids scanned from a file's index child on first need, counted by
    /// `located`, the constraints' run of child rows when the resolver
    /// located it.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_index_child_scan(
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        located: Option<Range<u64>>,
    ) -> Self {
        Self {
            cells: Some(Arc::default()),
            source: LazyRowIdSource::IndexChild {
                child,
                constraints,
                located,
            },
        }
    }

    /// Lazy ids over a located run of a file index child whose rids ascend
    /// within the run, as [`window`](Self::window) requires.
    #[cfg(feature = "file-io")]
    pub(crate) fn from_located_run(
        file: Arc<crate::store::persist::native_file::NativeStoreFile>,
        child: ChildReader,
        range: Range<u64>,
    ) -> Self {
        Self {
            cells: Some(Arc::default()),
            source: LazyRowIdSource::LocatedRun { file, child, range },
        }
    }

    /// The ids of rows `offset..offset + limit` of a run whose rids ascend,
    /// in base row order, read from the window's own rows and no others: an
    /// in-memory run's cut out of its rid column before they are decoded, a
    /// located file run's read from those rows of its child. A reference
    /// child's rows are ordered by `(val, rid)` (the order this crate's
    /// writers emit, docs/file-format.md §6), so within one value the rids
    /// ascend: row `k` of the run holds the run's `k`-th smallest rid, and a
    /// window of the run's rows is the same window of its ids in base order.
    /// A deep page costs its limit, and an empty window (no limit, or an
    /// offset at or past the run's end) reads nothing. `None` for any other
    /// source.
    pub(crate) async fn window(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Buffer<RowId>>> {
        let ids = match &self.source {
            LazyRowIdSource::Component {
                rids,
                ascending: true,
            } => match window_rows(offset, limit, rids.len() as u64) {
                Some(rows) => sorted_row_ids(rids.slice(rows.start as usize..rows.end as usize)?)?,
                None => Buffer::empty(),
            },
            #[cfg(feature = "file-io")]
            LazyRowIdSource::LocatedRun { file, child, range } => {
                match window_rows(offset, limit, range.end - range.start) {
                    Some(rows) => {
                        let rows = range.start + rows.start..range.start + rows.end;
                        read_located_rids(file, child, rows).await?
                    }
                    None => Buffer::empty(),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(ids))
    }

    /// How many rows the ids cover, when knowable without computing them: an
    /// in-memory run knows its width up front (so a count on a served match
    /// never decodes), and so does a located file run, whether its ids come
    /// from the run itself or from the child scan it bounds; an unlocated
    /// child scan and a pending residual compare only once their ids are
    /// known.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        match &self.source {
            LazyRowIdSource::Component { rids, .. } => Some(rids.len()),
            LazyRowIdSource::Unsorted { ids, .. } => Some(ids.len()),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { located, .. } => self
                .sorted_cell()
                .get()
                .map(Buffer::len)
                .or_else(|| located.as_ref().map(|run| (run.end - run.start) as usize)),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::LocatedRun { range, .. } => Some((range.end - range.start) as usize),
        }
    }

    /// The ids, when they take no I/O: an in-memory run's or a narrowed
    /// view's, computed (and cached) here as
    /// [`materialized`](Self::materialized) does, or a file child's once
    /// read. `None` for a file child's ids not read yet.
    pub(crate) fn ids_without_io(&self) -> Result<Option<Buffer<RowId>>> {
        match &self.source {
            LazyRowIdSource::Component { .. } | LazyRowIdSource::Unsorted { .. } => {
                self.materialized().map(Some)
            }
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } | LazyRowIdSource::LocatedRun { .. } => {
                Ok(self.sorted_cell().get().cloned())
            }
        }
    }

    /// The ids in their source's own order, when that takes no I/O: an
    /// in-memory run's in its component's order, decoded once and kept
    /// ([`LazyCells::run_order_of`]); a narrowed view's as they are. `None`
    /// for a file child's.
    pub(crate) fn run_order_ids(&self) -> Result<Option<Buffer<RowId>>> {
        match &self.source {
            LazyRowIdSource::Component { rids, .. } => {
                self.shared_cells().run_order_of(rids).map(Some)
            }
            LazyRowIdSource::Unsorted { ids, .. } => Ok(Some(ids.clone())),
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } | LazyRowIdSource::LocatedRun { .. } => Ok(None),
        }
    }

    /// How many of the ids are live under the tombstones `deleted`, counted
    /// without sorting them: a narrowed view's that leave every tombstone
    /// out are as many as they are; an in-memory run's, or a narrowed view's
    /// that may not, are read against the mask in their own order (base
    /// order where it is already kept). `None` for a file child's, whose
    /// count needs its ids read.
    pub(crate) fn live_len_without_sort(&self, deleted: &Mask) -> Result<Option<usize>> {
        let ids = match (&self.source, self.sorted_cell().get()) {
            (
                LazyRowIdSource::Unsorted {
                    ids, live: true, ..
                },
                _,
            ) => return Ok(Some(ids.len())),
            (LazyRowIdSource::Component { .. }, Some(sorted)) => sorted.clone(),
            _ => match self.run_order_ids()? {
                Some(ids) => ids,
                None => return Ok(None),
            },
        };
        observe!(LiveCount);
        Ok(Some(
            ids.iter()
                .filter(|&&id| !deleted.value(id as usize))
                .count(),
        ))
    }

    /// The ids, computed and cached on the first call; the awaiting form,
    /// which also runs a file child's deferred read.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized_async(&self) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.sorted_cell().get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            LazyRowIdSource::Component { .. } | LazyRowIdSource::Unsorted { .. } => {
                return self.materialized();
            }
            LazyRowIdSource::IndexChild {
                child, constraints, ..
            } => scan_index_row_ids(child, constraints).await?,
            LazyRowIdSource::LocatedRun { file, child, range } => {
                read_located_rids(file, child, range.clone()).await?
            }
        };
        observe!(IdsSorted(ids.len()));
        Ok(self.sorted_cell().get_or_init(|| ids).clone())
    }

    /// The ids, computed and cached on the first call; the synchronous form,
    /// for in-memory sources (a file child's ids take I/O, and every consumer
    /// of a file view's selection is async: see
    /// [`materialized_async`](Self::materialized_async)). A file child's ids
    /// that `materialized_async` has already read come from the cache, which
    /// is how the file match folds them; unread ones never reach here.
    pub(crate) fn materialized(&self) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.sorted_cell().get() {
            return Ok(ids.clone());
        }
        let ids = match &self.source {
            // A run already decoded in its component's order sorts a copy of
            // that, rather than decoding the rids again.
            LazyRowIdSource::Component { rids, .. } => match self.shared_cells().run_order.get() {
                Some(run_order) => components::sorted_copy(run_order)?,
                None => sorted_row_ids(rids.clone())?,
            },
            LazyRowIdSource::Unsorted { ids, .. } => components::sorted_copy(ids)?,
            #[cfg(feature = "file-io")]
            LazyRowIdSource::IndexChild { .. } | LazyRowIdSource::LocatedRun { .. } => {
                unreachable!(
                    "an in-memory view only ever carries a component's pending ids, and \
                     the file match reads a file run's ids before it folds them"
                )
            }
        };
        observe!(IdsSorted(ids.len()));
        Ok(self.sorted_cell().get_or_init(|| ids).clone())
    }
}

/// The pattern resolved against the configured indexes over an in-memory
/// base: the first outcome that isn't `Declined`, the indexes tried in
/// declaration (preference) order. `Declined` when none applies, and the
/// store falls back to a mask scan.
pub(crate) fn resolve_indexes_in_memory(
    indexes: &[IndexType],
    components: &[IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    for index in indexes {
        let Some(probe) = index.choose(pattern, layout) else {
            continue;
        };
        match resolve::resolve_in_memory(probe, components, codes)? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// The file counterpart of [`resolve_indexes_in_memory`]: the first file
/// resolution that isn't `Declined`, in declaration (preference) order. A
/// serve plan rides inside the resolution
/// ([`IndexResolution::Resolved::serve`]), so the store never needs to know
/// which index answered.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_indexes_file(
    indexes: &[IndexType],
    file: &Arc<crate::store::persist::native_file::NativeStoreFile>,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    for index in indexes {
        let Some(probe) = index.choose(pattern, layout) else {
            continue;
        };
        match file::resolve_file(probe, file, codes).await? {
            IndexResolution::Declined => continue,
            resolved => return Ok(resolved),
        }
    }
    Ok(IndexResolution::Declined)
}

/// The set of optional secondary indexes to embed in a store.
///
/// An empty `Indexes` writes no index children: the fastest write, and every
/// pattern without a bound subject scans. Use
/// `vec![IndexType::SecondaryByReference]` for the compact (value, row id)
/// predicate and object indexes, or `vec![IndexType::SecondaryByCopy]` for
/// the full sorted quad copies.
pub type Indexes = Vec<IndexType>;

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Term};

    #[test]
    fn slug_registry_covers_every_identity() {
        // Every declared identity's slug resolves back to its own index and
        // component name — the mapping a persisted child is read through.
        for index in ALL_INDEX_TYPES {
            for identity in index.component_identities() {
                let known = known_component(identity.slug).expect("declared slug is known");
                assert_eq!(known.index, index);
                assert_eq!(known.name, identity.name);
            }
        }

        // A slug this version does not implement is skippable, not fatal.
        assert!(known_component("secondary-by-copy/spog").is_none());
        assert!(known_component("").is_none());
    }

    #[test]
    fn resolved_roles_clear() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());

        let bound = QuadPattern::new(Some(&s), Some(&p), Some(&o), Some(&g));

        let r = ResolvedRoles::Object.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_some() && r.object.is_none() && r.graph.is_some()
        );

        let r = ResolvedRoles::Predicate.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_some() && r.graph.is_some()
        );

        let r = ResolvedRoles::PredicateObject.clear(bound);
        assert!(
            r.subject.is_some() && r.predicate.is_none() && r.object.is_none() && r.graph.is_some()
        );
    }
}
