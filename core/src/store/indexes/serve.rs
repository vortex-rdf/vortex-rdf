//! The index-agnostic *serving* path: reading a resolved view's quads out of
//! the answering index's own columns instead of gathering the primary columns
//! by scattered row id.
//!
//! An index builds a serve plan during resolution; the store executes it
//! without knowing which index produced it, so serving stays a uniform
//! capability of the store. It is the generic form of
//! what a permutation index (whole quads in a query-friendly order, e.g.
//! `IndexType::SecondaryByCopy`) can provide and a back-reference index (only
//! `(value, row-id)` pairs, e.g. `IndexType::SecondaryByReference`) cannot.
//!
//! The plans are typed by backend, because only the *acquisition* of the
//! matched columns differs: [`InMemoryServePlan`] slices the contiguous
//! matched run of an in-memory component, `FileServePlan` scans the index
//! child — its located run by row range, else with a pushed-down
//! term-equality filter. Each `QuadsSource` variant
//! carries exactly its own backend's plan type, so a view paired with the
//! other backend's plan is unrepresentable — and both decode through the
//! shared [`ServeDecode`] tail, so tombstone handling cannot drift between
//! them.
//!
//! Correctness never depends on a plan: it reproduces exactly the rows the
//! resolution's row ids name, so any operation that can't honor it (chained
//! matches, counting, materializing) simply ignores it and reads through the
//! row ids. The store keeps a plan only while the resolution is a view's sole
//! restriction — see `QuadsSource::File` / `QuadsSource::InMemory`. A plan is
//! also what licenses *deferring* those row ids (`LazyRowIds`): with a plan
//! attached, reads never touch them, so the resolution hands back a recipe
//! instead of scanning for them at match time.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::FieldNames;
#[cfg(feature = "file-io")]
use vortex_array::expr::{root, select};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
#[cfg(feature = "file-io")]
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{canonical_u32, into_struct_array};
use crate::store::arrow::QuadColumn;
use crate::store::indexes::{IndexComponent, LazyRowIds};
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::query::pushdown::Keep;
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::view::canonical::{LiveCanonical, decode_u32};
use crate::store::view::order::{SortOrder, keep_runs, sorted_within};
use crate::store::view::selection::point_sized;

/// The decode tail shared by both backend-typed serve plans: which of the
/// index's columns source each primary component, which carries the primary
/// row id, and the layout the projected columns decode through. Acquisition
/// differs per backend; everything after it lives here, once.
#[derive(Clone)]
struct ServeDecode {
    /// The source column for each primary `(s, p, o, g)` component, in that
    /// order — the index's own columns holding the whole quad.
    primary_columns: [&'static str; 4],
    /// The column giving each served row's primary row id, used to drop rows
    /// tombstoned since construction.
    rid_column: &'static str,
    /// The layout the projected source columns decode through (an index that
    /// stores whole terms decodes them as strings, or dictionary codes under
    /// the Dictionary layout).
    decode_layout: ResolvedLayout,
    /// The order the served rows come in — the family's sort order, when the
    /// component is globally sorted in it; `None` says nothing about it.
    key_order: Option<SortOrder>,
    /// How many of the order's leading keys the resolution fixed: constant
    /// over the run, the next key ascending within it.
    resolved: usize,
}

impl ServeDecode {
    /// Decode the `(s, p, o, g)` rows out of a chunk of the plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    fn decode_columns<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => T::decode(&self.decode_layout, &rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved with a dictionary scan.
    #[cfg(feature = "file-io")]
    async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => T::decode_async(&self.decode_layout, &rows).await,
            Err(e) => vec![Err(e)],
        }
    }

    /// The positions of a small run's live rows, for point reads through the
    /// component's cached probes. Tombstones are defined over primary row
    /// ids; the rid column says which primary row each served row mirrors,
    /// and only a tombstoned view pays for the liveness pass.
    ///
    /// The outer `None` declines: a run wider than
    /// [`POINT_GATHER_MAX_ROWS`], or a rid column whose encoding resolves no
    /// probe. The inner `None` means no tombstones — every position in
    /// `range` is live, so the caller iterates the range directly.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    fn live_positions(
        &self,
        array: &ArrayRef,
        range: &Range<usize>,
        probes: &crate::store::view::probes::StructProbes,
        deleted: Option<&Mask>,
    ) -> Option<Option<Vec<usize>>> {
        if !point_sized(range.len() as u64) {
            return None;
        }
        let Some(deleted) = deleted else {
            return Some(None);
        };
        let rid = probes.by_name(array, self.rid_column)?;
        Some(Some(
            range
                .clone()
                .filter(|&pos| !deleted.value(rid.value_at(pos) as usize))
                .collect(),
        ))
    }

    /// A small run's live rows as a primary-named `(s, p, o, g)` canonical
    /// struct, read point-by-point at the run's global positions through the
    /// component's cached probes — no slice, no per-call probe resolution.
    /// `Ok(None)` declines (a wide run, or a column — e.g. a string copy —
    /// whose encoding resolves no probe); the caller keeps the slice path.
    fn point_read_run_rows(
        &self,
        array: &ArrayRef,
        range: Range<usize>,
        probes: &crate::store::view::probes::StructProbes,
        deleted: Option<&Mask>,
    ) -> Result<Option<ArrayRef>> {
        let Some(live) = self.live_positions(array, &range, probes, deleted) else {
            return Ok(None);
        };
        let live: Vec<usize> = live.unwrap_or_else(|| range.collect());
        let mut children = Vec::with_capacity(4);
        for name in self.primary_columns {
            let Some(probe) = probes.by_name(array, name) else {
                return Ok(None);
            };
            let reads = live.iter().map(|&pos| probe.value_at(pos));
            let Some(child) = primitive_from_u64_reads(probe.array().dtype().as_ptype(), reads)
            else {
                return Ok(None);
            };
            children.push(child);
        }
        Ok(Some(
            StructArray::try_new(
                FieldNames::from(crate::store::schema::PRIMARY_COLUMNS),
                children,
                live.len(),
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
        ))
    }

    /// A chunk's live rows as a primary-named `(s, p, o, g)` struct: relabel the
    /// source columns, then drop any whose primary row id is tombstoned.
    fn chunk_rows(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = chunk
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let col = |name: &'static str| {
            struct_arr
                .unmasked_field_by_name(name)
                .cloned()
                .map_err(VortexRdfError::Vortex)
        };
        let [s, p, o, g] = self.primary_columns;
        let len = struct_arr.len();
        let rows = StructArray::try_new(
            FieldNames::from(crate::store::schema::PRIMARY_COLUMNS),
            vec![col(s)?, col(p)?, col(o)?, col(g)?],
            len,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        // Point-read the run through the component probes when it is small
        // enough (`gather_by_point_reads` gates on `POINT_GATHER_MAX_ROWS`);
        // otherwise decode the sliced columns.
        let rows = match crate::store::scan::gather::gather_by_point_reads(
            &rows,
            &crate::store::view::selection::RowSelection::Range(0..len as u64),
            None,
            None,
        )? {
            Some(canonical) => canonical,
            None => rows,
        };

        let Some(deleted) = deleted else {
            return Ok(rows);
        };
        // Tombstones are defined over primary row ids; the rid column says which
        // primary row each served row mirrors.
        let rid_col = col(self.rid_column)?
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let live = Mask::from_indices(
            len,
            rid_col
                .as_slice::<u32>()
                .iter()
                .enumerate()
                .filter(|&(_, &rid)| !deleted.value(rid as usize))
                .map(|(position, _)| position),
        );
        if live.all_true() {
            return Ok(rows);
        }
        rows.filter(live).map_err(VortexRdfError::Vortex)
    }
}

/// An index's serving plan for an in-memory view: the matched rows are the
/// contiguous `[start, end)` run of the index component's own array — the run
/// a binary search over its sorted lead column bounded — so `quads()` slices
/// them straight from the component (an `Arc` bump, no row-id gather) instead
/// of gathering the primary columns at scattered row ids.
///
/// The in-memory half of the serving path (see the module docs;
/// `FileServePlan` is the file-backed half). `QuadsSource::InMemory` carries
/// exactly this type, so an in-memory view can never hold a file plan.
#[derive(Clone)]
pub(crate) struct InMemoryServePlan {
    decode: ServeDecode,
    /// The index component's rows, in child schema.
    array: ArrayRef,
    range: Range<usize>,
    /// The component's shared probe cache, so a small run reads
    /// point-by-point at its global positions instead of slicing (a slice's
    /// probe would be re-resolved per call).
    probes: Arc<crate::store::view::probes::StructProbes>,
    /// The component's live canonical cache, which a wide run's code read
    /// slices (see [`code_columns`](Self::code_columns)).
    canonical: Arc<LiveCanonical>,
}

/// The fraction of its component a served run must cover for a wide code
/// read to decode the whole column into the component's live canonical cache
/// rather than the run alone: below it the run's own decode is the cheaper
/// read, and the cache stays cold unless some holder already keeps the column
/// alive.
pub(crate) const CACHE_RUN_FRACTION: usize = 32;

impl InMemoryServePlan {
    /// A plan serving the contiguous `range` of `component`'s rows.
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        component: &IndexComponent,
        range: Range<usize>,
        key_order: Option<SortOrder>,
        resolved: usize,
    ) -> Result<Self> {
        Ok(Self {
            decode: ServeDecode {
                primary_columns,
                rid_column,
                decode_layout,
                key_order,
                resolved,
            },
            array: component.rows()?.clone().into_array(),
            range,
            probes: component.probes_arc(),
            canonical: component.canonical_arc(),
        })
    }

    /// How a term-code constraint on `column` narrows the served run without
    /// leaving it: unchanged or empty when the column is one of the keys the
    /// resolution fixed (its one value is in `keep` or not); a sub-run when
    /// the column is the next key, or a later one that is ascending within
    /// the run because every key before it is constant there, found by
    /// binary search; declined otherwise — and for a code set whose runs do
    /// not coalesce into one, or whose searches would cost more than a pass
    /// over the run.
    pub(crate) fn narrow(&self, column: QuadColumn, keep: &Keep) -> Result<Narrow> {
        let Some(order) = self.decode.key_order else {
            return Ok(Narrow::Declined);
        };
        if self.range.is_empty() {
            return Ok(Narrow::Unchanged);
        }
        let name = self.decode.primary_columns[column.index()];
        let Some(probe) = self.probes.by_name(&self.array, name) else {
            return Ok(Narrow::Declined);
        };
        let position = order.position(column);
        if position < self.decode.resolved {
            let value = probe.value_at(self.range.start);
            let kept = u32::try_from(value).is_ok_and(|code| keep.contains(code));
            return Ok(if kept {
                Narrow::Unchanged
            } else {
                Narrow::Empty
            });
        }
        let ascending = position == self.decode.resolved
            || sorted_within(&self.array, &self.probes, order, self.range.clone(), column)
                == Some(true);
        if !ascending {
            return Ok(Narrow::Declined);
        }
        let Some(runs) = keep_runs(probe, self.range.clone(), keep) else {
            return Ok(Narrow::Declined);
        };
        Ok(match runs.as_slice() {
            [] => Narrow::Empty,
            [run] if *run == self.range => Narrow::Unchanged,
            [run] => Narrow::Run(run.clone()),
            _ => Narrow::Declined,
        })
    }

    /// This plan over `sub`, a sub-run of its run, with the deferred row ids
    /// of exactly those rows — what a view narrowed in place carries.
    pub(crate) fn restricted(&self, sub: Range<usize>) -> Result<(Self, LazyRowIds)> {
        let rids = into_struct_array(self.array.clone())?
            .unmasked_field_by_name(self.decode.rid_column)
            .map_err(VortexRdfError::Vortex)?
            .slice(sub.clone())
            .map_err(VortexRdfError::Vortex)?;
        Ok((
            Self {
                range: sub,
                ..self.clone()
            },
            LazyRowIds::from_component_run(rids),
        ))
    }

    /// The served rows' `u32` term codes for `columns`, in that order, read
    /// straight off the index component's own columns — the code-payload
    /// counterpart of [`decode`](Self::decode).
    ///
    /// A permutation index under the Dictionary layout already holds this
    /// view's codes, contiguously, in its own order; reading them here
    /// replaces materializing the resolution's row ids and gathering the
    /// primary columns at each one. Rows come back in the index's order, as
    /// [`decode`](Self::decode) already serves them.
    ///
    /// A point-sized run reads each code through the component's cached
    /// probes, decoding no column. A wider run slices each column's canonical
    /// form — the column itself when it is canonical, else the component's
    /// live canonical cache: decoded once, shared with every holder alive and
    /// freed with the last (kept for the store's lifetime when the cache is
    /// pinned), filled when the cache is pinned, the run covers at least
    /// 1/[`CACHE_RUN_FRACTION`] of the component or a holder already keeps
    /// the column alive; a narrower cold run decodes the run alone.
    /// Tombstoned rows are dropped through the rid column.
    ///
    /// `Ok(None)` declines to the caller's gather path: a non-Dictionary
    /// decode layout (the columns hold terms, not codes), or a point-sized
    /// run over a column whose encoding resolves no probe.
    pub(crate) fn code_columns(
        &self,
        columns: &[QuadColumn],
        deleted: Option<&Mask>,
    ) -> Result<Option<Vec<Buffer<u32>>>> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return Ok(None);
        }
        let names = columns
            .iter()
            .map(|column| self.decode.primary_columns[column.index()]);
        if point_sized(self.range.len() as u64) {
            let Some(live) =
                self.decode
                    .live_positions(&self.array, &self.range, &self.probes, deleted)
            else {
                return Ok(None);
            };
            let mut out = Vec::with_capacity(columns.len());
            for name in names {
                let Some(probe) = self.probes.by_name(&self.array, name) else {
                    return Ok(None);
                };
                out.push(match &live {
                    None => {
                        Buffer::from_iter(self.range.clone().map(|pos| probe.value_at(pos) as u32))
                    }
                    Some(live) => {
                        Buffer::from_iter(live.iter().map(|&pos| probe.value_at(pos) as u32))
                    }
                });
            }
            return Ok(Some(out));
        }
        let struct_arr = into_struct_array(self.array.clone())?;
        let child = |name: &str| {
            struct_arr
                .unmasked_field_by_name(name)
                .map_err(VortexRdfError::Vortex)
        };
        let live = match deleted {
            None => None,
            Some(deleted) => {
                let rids = decode_u32(&self.run_of(child(self.decode.rid_column)?)?, "rid")?;
                Some(
                    rids.as_slice()
                        .iter()
                        .enumerate()
                        .filter(|&(_, &rid)| !deleted.value(rid as usize))
                        .map(|(pos, _)| pos)
                        .collect::<Vec<usize>>(),
                )
            }
        };
        let mut out = Vec::with_capacity(columns.len());
        for (column, name) in columns.iter().zip(names) {
            let col = child(name)?;
            let codes = match canonical_u32(col) {
                Some(prim) => prim.into_buffer::<u32>().slice(self.range.clone()),
                None => match self.cached_column(column.index(), col)? {
                    Some(column) => column.slice(self.range.clone()),
                    None => decode_u32(&self.run_of(col)?, name)?,
                },
            };
            out.push(match &live {
                None => codes,
                Some(live) => Buffer::from_iter(live.iter().map(|&pos| codes.as_slice()[pos])),
            });
        }
        Ok(Some(out))
    }

    /// The served run: the component rows this plan covers.
    pub(crate) fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// The order the served rows come in, when the component is sorted.
    pub(crate) fn key_order(&self) -> Option<SortOrder> {
        self.decode.key_order
    }

    /// How many leading keys of that order the resolution fixed.
    pub(crate) fn resolved(&self) -> usize {
        self.decode.resolved
    }

    /// The component's rows, in child schema.
    pub(crate) fn rows(&self) -> ArrayRef {
        self.array.clone()
    }

    /// The component's probe cache.
    pub(crate) fn probes(&self) -> &crate::store::view::probes::StructProbes {
        &self.probes
    }

    /// The child columns sourcing `(s, p, o, g)`, in that order.
    pub(crate) fn primary_columns(&self) -> [&'static str; 4] {
        self.decode.primary_columns
    }

    /// `col` restricted to the served run.
    fn run_of(&self, col: &ArrayRef) -> Result<ArrayRef> {
        col.slice(self.range.clone())
            .map_err(VortexRdfError::Vortex)
    }

    /// The component's live canonical form of column `idx`, when a served
    /// read should go through it: already held by someone, or worth filling
    /// for a run this wide. `None` leaves the read to a decode of the run.
    fn cached_column(&self, idx: usize, col: &ArrayRef) -> Result<Option<Buffer<u32>>> {
        if let Some(column) = self.canonical.column_if_alive(idx) {
            return Ok(Some(column));
        }
        if self.canonical.is_pinned()
            || self.range.len().saturating_mul(CACHE_RUN_FRACTION) >= self.array.len()
        {
            return self.canonical.column(idx, col).map(Some);
        }
        Ok(None)
    }

    /// Decode the matched rows straight from the index component's rows:
    /// point reads at the run's global positions through the component's
    /// cached probes when the run is small, else slice the component to this
    /// plan's row run — either way decoding those columns as the primary
    /// `(s, p, o, g)`, replacing the row-id gather over the primaries.
    pub(crate) fn decode<T: ChunkDecode>(&self, deleted: Option<&Mask>) -> Vec<Result<T>> {
        match self.decode.point_read_run_rows(
            &self.array,
            self.range.clone(),
            &self.probes,
            deleted,
        ) {
            Ok(Some(rows)) => return T::decode(&self.decode.decode_layout, &rows),
            Ok(None) => {}
            Err(e) => return vec![Err(e)],
        }
        match self.array.slice(self.range.clone()) {
            Ok(rows) => self.decode.decode_columns(&rows, deleted),
            Err(e) => vec![Err(VortexRdfError::Vortex(e))],
        }
    }
}

/// An index's serving plan for a file-backed view: the matched rows are those
/// where every `(column, value)` term equality holds — a contiguous run of
/// the index child, which its sort order clusters — read by a scan of that
/// run when the resolution located it, else by a scan pushing the equalities
/// down as a zone-prunable filter, instead of scattering row-id reads across
/// the primary columns.
///
/// The file-backed half of the serving path (see the module docs;
/// [`InMemoryServePlan`] is the in-memory half). `QuadsSource::File` carries
/// exactly this type, so a file view can never hold an in-memory plan.
#[cfg(feature = "file-io")]
#[derive(Clone)]
pub(crate) struct FileServePlan {
    decode: ServeDecode,
    /// The index component child's cached layout reader.
    reader: vortex_layout::LayoutReaderRef,
    constraints: Vec<(&'static str, Scalar)>,
    /// The file handle's bind memo — the plan binds its projection and
    /// filter through it on FIRST READ, not at construction: a match-only
    /// call builds the plan without ever scanning through it, and must not
    /// pay for binds a count-only consumer will never use. The memo keys
    /// the bound trees by shape, so every plan for a repeated pattern
    /// carries the same identity and hits the child reader's
    /// identity-keyed caches (see `BoundExprMemo`).
    memo: Arc<crate::store::persist::native_file::BoundExprMemo>,
    /// The lazily bound (projection, filter) pair, shared across clones so
    /// the first reader's bind serves them all.
    bound: Arc<
        std::sync::OnceLock<(
            vortex_array::expr::BoundExpression,
            vortex_array::expr::BoundExpression,
        )>,
    >,
    /// The serving component's name, addressing its cached chunk probes on
    /// the file handle for point-read serving.
    component: &'static str,
    /// The child rows the constraints select, when the resolution located
    /// them by chunk probes — exactly the constrained rows, letting a small
    /// run be point-read and a wide one scanned by range
    /// ([`Self::located_run_scan`]) instead of filtered. `None` when
    /// unlocated (or when a constraint the location didn't cover would make
    /// the range over-approximate).
    row_range: Option<Range<u64>>,
}

/// How [`InMemoryServePlan::narrow`] answered a term-code constraint.
pub(crate) enum Narrow {
    /// Every served row is kept.
    Unchanged,
    /// No served row is kept.
    Empty,
    /// Exactly this sub-run of the served run is kept.
    Run(Range<usize>),
    /// The plan cannot say; the caller reads the rows.
    Declined,
}

/// The fewest rows a located run's scan split carries: below this the
/// per-split overhead (a spawned task, its segment requests, one decode
/// call) outweighs what spreading the decode buys.
#[cfg(feature = "file-io")]
const SERVE_SPLIT_MIN_ROWS: u64 = 1024;

/// Rows per split for a located run of `rows`: enough splits to hand every
/// worker a couple, never fewer than [`SERVE_SPLIT_MIN_ROWS`] rows each.
#[cfg(feature = "file-io")]
fn run_split_rows(rows: u64) -> usize {
    let workers = crate::io::read::available_parallelism() as u64;
    rows.div_ceil(2 * workers).max(SERVE_SPLIT_MIN_ROWS) as usize
}

#[cfg(feature = "file-io")]
impl FileServePlan {
    /// A plan serving a file's index columns by a pushed-down scan filtered to
    /// the rows where every `constraints` equality holds — or, over a located
    /// `row_range`, by point reads (a small run) or a range-restricted scan
    /// split across the workers (a wide one) — see
    /// [`Self::located_run_scan`].
    // The parameters are the plan itself: the column roles, the reader, the
    // constraints, and the bind memo.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        component: &'static str,
        row_range: Option<Range<u64>>,
        memo: Arc<crate::store::persist::native_file::BoundExprMemo>,
        key_order: Option<SortOrder>,
        resolved: usize,
    ) -> Self {
        Self {
            decode: ServeDecode {
                primary_columns,
                rid_column,
                decode_layout,
                key_order,
                resolved,
            },
            reader,
            constraints,
            memo,
            bound: Arc::new(std::sync::OnceLock::new()),
            component,
            row_range,
        }
    }

    /// The plan's (projection, filter), bound through the handle's memo on
    /// first use and shared across clones thereafter.
    fn bound_exprs(
        &self,
    ) -> Result<(
        vortex_array::expr::BoundExpression,
        vortex_array::expr::BoundExpression,
    )> {
        if let Some(bound) = self.bound.get() {
            return Ok(bound.clone());
        }
        let projection = select(self.projection(), root());
        // A serve plan always carries at least one constraint (the resolved
        // lead component), so the conjunction is never empty.
        let filter = super::row_ids::eq_conjunction(self.constraints.iter().cloned())
            .expect("a serve plan constrains at least one column");
        let scope = self.reader.dtype();
        let bound_projection = self
            .memo
            .bind(self.component, &projection, scope)
            .map_err(VortexRdfError::Vortex)?;
        let bound_filter = self
            .memo
            .bind(self.component, &filter, scope)
            .map_err(VortexRdfError::Vortex)?;
        Ok(self
            .bound
            .get_or_init(|| (bound_projection, bound_filter))
            .clone())
    }

    /// The serving component's name on the file handle.
    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The order the served rows come in, when the child is sorted.
    pub(crate) fn key_order(&self) -> Option<SortOrder> {
        self.decode.key_order
    }

    /// The located child-row range the constraints select, when known.
    pub(crate) fn row_range(&self) -> Option<Range<u64>> {
        self.row_range.clone()
    }

    /// The columns to project from the file to serve these rows: the four
    /// component sources plus the row-id column (for tombstones).
    pub(crate) fn projection(&self) -> [&'static str; 5] {
        let [s, p, o, g] = self.decode.primary_columns;
        [s, p, o, g, self.decode.rid_column]
    }

    /// A scan over the serving index child — where [`Self::projection`] and
    /// the plan's bound filter apply.
    fn child_scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// [`Self::child_scan`] with the plan's projection and filter — bound on
    /// first use — applied. The form the streaming reads consume for an
    /// unlocated run.
    pub(crate) fn projected_filtered_scan(
        &self,
    ) -> Result<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>> {
        let (projection, filter) = self.bound_exprs()?;
        Ok(self
            .child_scan()
            .with_projection(projection)
            .with_filter(filter))
    }

    /// A scan of the located run: [`Self::child_scan`] with the plan's
    /// projection, restricted to `row_range` and split by row count. `None`
    /// when the run is unlocated — [`Self::projected_filtered_scan`] answers
    /// then. The form the streaming reads consume for a wide located run.
    ///
    /// The scan spawns one task per split and the consumer decodes each
    /// chunk inside its task, so the split count is the decode's
    /// parallelism. The child's natural splits are its leaf chunks, which
    /// cluster a run into one split however wide it is; splitting the range
    /// by row count spreads the run's decode over the workers instead
    /// (`run_split_rows`). No filter rides along: the located range is
    /// exactly the constrained rows (the same fact `size` and the point
    /// reads rely on), so the term equalities would only re-read and
    /// re-compare the columns that bounded it.
    pub(crate) fn located_run_scan(
        &self,
    ) -> Result<Option<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>>> {
        let Some(range) = self.row_range.clone() else {
            return Ok(None);
        };
        let (projection, _) = self.bound_exprs()?;
        let split_rows = run_split_rows(range.end - range.start);
        Ok(Some(
            self.child_scan()
                .with_projection(projection)
                .with_row_range(range)
                .with_split_by(SplitBy::RowCount(split_rows)),
        ))
    }

    /// Decode the `(s, p, o, g)` rows out of a chunk of this plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    pub(crate) fn decode_columns<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        self.decode.decode_columns(chunk, deleted)
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved with a dictionary scan.
    pub(crate) async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        self.decode.decode_columns_async(chunk, deleted).await
    }

    /// A chunk of this plan's projected index columns as `(s, p, o, g)` rows
    /// with the rows tombstoned in `deleted` dropped — the code export's
    /// per-chunk step, ahead of the Arrow conversion.
    pub(crate) fn served_rows(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Result<ArrayRef> {
        self.decode.chunk_rows(chunk, deleted)
    }
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;

    #[test]
    fn run_split_rows_floor_and_arithmetic() {
        // Small runs never split below the per-split floor.
        assert_eq!(run_split_rows(100), SERVE_SPLIT_MIN_ROWS as usize);
        assert_eq!(run_split_rows(0), SERVE_SPLIT_MIN_ROWS as usize);

        // A wide run hands every worker a couple of splits.
        let rows = 1u64 << 20;
        let workers = crate::io::read::available_parallelism() as u64;
        let split = run_split_rows(rows);
        assert_eq!(
            split,
            rows.div_ceil(2 * workers).max(SERVE_SPLIT_MIN_ROWS) as usize
        );
        assert!(split as u64 >= SERVE_SPLIT_MIN_ROWS);
        assert!((rows as usize).div_ceil(split) as u64 <= 2 * workers + 1);
    }
}
