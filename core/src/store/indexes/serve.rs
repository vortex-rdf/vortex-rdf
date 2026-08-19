//! The index-agnostic *serving* path: reading a resolved view's quads out of
//! the answering index's own columns instead of gathering the primary columns
//! by scattered row id.
//!
//! An index builds a serve plan during resolution; the store executes it
//! without knowing which index produced it, so serving stays a uniform
//! capability rather than one index's special case. It is the generic form of
//! what a permutation index (whole quads in a query-friendly order, e.g.
//! `IndexType::SecondaryByCopy`) can provide and a back-reference index (only
//! `(value, row-id)` pairs, e.g. `IndexType::SecondaryByReference`) cannot.
//!
//! The plans are typed by backend, because only the *acquisition* of the
//! matched columns differs: [`InMemoryServePlan`] slices the contiguous
//! matched run of an in-memory component, `FileServePlan` scans the index
//! child with a pushed-down term-equality filter. Each `QuadsSource` variant
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

use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::FieldNames;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and, eq, get_item, lit, root};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::ResolvedLayout;

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
}

impl ServeDecode {
    /// Decode the `(s, p, o, g)` quads out of a chunk of the plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    fn decode_columns(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Vec<Result<Quad>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => self.decode_layout.decode_chunk(&rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved with a dictionary scan.
    #[cfg(feature = "file-io")]
    async fn decode_columns_async(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<Quad>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => self.decode_layout.decode_chunk_async(&rows).await,
            Err(e) => vec![Err(e)],
        }
    }

    /// A small run's live rows as a primary-named `(s, p, o, g)` canonical
    /// struct, read point-by-point at the run's global positions through the
    /// component's cached probes — no slice, no per-call probe resolution.
    /// `Ok(None)` declines (a wide run, or a column — e.g. a string copy —
    /// whose encoding resolves no probe); the caller keeps the slice path.
    fn rows_via_probes(
        &self,
        array: &ArrayRef,
        range: Range<usize>,
        probes: &crate::store::probes::BaseProbes,
        deleted: Option<&Mask>,
    ) -> Result<Option<ArrayRef>> {
        use vortex_array::dtype::PType;

        use crate::store::selection::POINT_GATHER_MAX_ROWS;

        if range.len() > POINT_GATHER_MAX_ROWS {
            return Ok(None);
        }
        // Tombstones are defined over primary row ids; the rid column says
        // which primary row each served row mirrors.
        let live: Vec<usize> = match deleted {
            None => range.collect(),
            Some(deleted) => {
                let Some(rid) = probes.by_name(array, self.rid_column) else {
                    return Ok(None);
                };
                range
                    .filter(|&pos| !deleted.value(rid.value_at(pos) as usize))
                    .collect()
            }
        };
        let mut children = Vec::with_capacity(4);
        for name in self.primary_columns {
            let Some(probe) = probes.by_name(array, name) else {
                return Ok(None);
            };
            let reads = live.iter().map(|&pos| probe.value_at(pos));
            let child = match probe.array().dtype().as_ptype() {
                PType::U8 => PrimitiveArray::from_iter(reads.map(|v| v as u8)).into_array(),
                PType::U16 => PrimitiveArray::from_iter(reads.map(|v| v as u16)).into_array(),
                PType::U32 => PrimitiveArray::from_iter(reads.map(|v| v as u32)).into_array(),
                PType::U64 => PrimitiveArray::from_iter(reads).into_array(),
                _ => return Ok(None),
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
        // A small served run over still-encoded component columns reads
        // faster point-by-point than through the per-column decode pipeline;
        // wide runs and non-probeable columns keep the vectorized path.
        let rows = match crate::store::selection::gather_by_point_reads(
            &rows,
            &crate::store::selection::RowSelection::Range(0..len as u64),
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
    probes: Arc<crate::store::probes::BaseProbes>,
}

impl InMemoryServePlan {
    /// A plan serving the contiguous `range` of an in-memory index
    /// component's rows.
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        array: ArrayRef,
        range: Range<usize>,
        probes: Arc<crate::store::probes::BaseProbes>,
    ) -> Self {
        Self {
            decode: ServeDecode {
                primary_columns,
                rid_column,
                decode_layout,
            },
            array,
            range,
            probes,
        }
    }

    /// The served rows' four `u32` term codes, read straight off the index
    /// component's own columns — the code-payload counterpart of
    /// [`decode`](Self::decode).
    ///
    /// A permutation index under the Dictionary layout already holds this
    /// view's codes, contiguously, in its own order; reading them here
    /// replaces materializing the resolution's row ids and gathering the
    /// primary columns at each one. Rows come back in the index's order, as
    /// [`decode`](Self::decode) already serves them.
    ///
    /// `None` declines to the caller's gather path: a run wider than
    /// [`POINT_GATHER_MAX_ROWS`], a non-Dictionary decode layout (the columns
    /// hold terms, not codes), or any column whose encoding resolves no
    /// probe.
    ///
    /// The width gate is the same trade [`rows_via_probes`] makes, and
    /// measurement puts it in the same place: a point read through the probe
    /// beats materializing row ids for the run, but per-element reads over a
    /// compressed column lose to a bulk gather over the base's canonical
    /// buffers once the run is large — reading a wide run here measured ~3x
    /// the gather it replaced.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::selection::POINT_GATHER_MAX_ROWS
    /// [`rows_via_probes`]: ServeDecode::rows_via_probes
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<u32>; 4]> {
        use crate::store::selection::POINT_GATHER_MAX_ROWS;

        if self.range.len() > POINT_GATHER_MAX_ROWS
            || !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_))
        {
            return None;
        }
        // Tombstones are defined over primary row ids; the rid column says
        // which primary row each served row mirrors. Only a tombstoned view
        // pays for the liveness pass.
        let live: Option<Vec<usize>> = match deleted {
            None => None,
            Some(deleted) => {
                let rid = self.probes.by_name(&self.array, self.decode.rid_column)?;
                Some(
                    self.range
                        .clone()
                        .filter(|&pos| !deleted.value(rid.value_at(pos) as usize))
                        .collect(),
                )
            }
        };
        let mut columns = Vec::with_capacity(4);
        for name in self.decode.primary_columns {
            let probe = self.probes.by_name(&self.array, name)?;
            columns.push(match &live {
                None => Buffer::from_iter(self.range.clone().map(|pos| probe.value_at(pos) as u32)),
                Some(live) => Buffer::from_iter(live.iter().map(|&pos| probe.value_at(pos) as u32)),
            });
        }
        let mut columns = columns.into_iter();
        Some([
            columns.next()?,
            columns.next()?,
            columns.next()?,
            columns.next()?,
        ])
    }

    /// Decode the matched quads straight from the index component's rows:
    /// point reads at the run's global positions through the component's
    /// cached probes when the run is small, else slice the component to this
    /// plan's row run — either way decoding those columns as the primary
    /// `(s, p, o, g)`, replacing the row-id gather over the primaries.
    pub(crate) fn decode(&self, deleted: Option<&Mask>) -> Vec<Result<Quad>> {
        match self
            .decode
            .rows_via_probes(&self.array, self.range.clone(), &self.probes, deleted)
        {
            Ok(Some(rows)) => return self.decode.decode_layout.decode_chunk(&rows),
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
/// where every `(column, value)` term equality holds, read by a pushed-down
/// scan of the index child (whose sort order clusters them into a contiguous,
/// zone-prunable run) instead of scattering row-id reads across the primary
/// columns.
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
    /// The serving component's name, addressing its cached chunk probes on
    /// the file handle for point-read serving.
    component: &'static str,
    /// The child rows the constraints select, when the resolution located
    /// them by chunk probes — exactly the constrained rows, letting a small
    /// run be point-read instead of scanned. `None` when unlocated (or when
    /// a constraint the location didn't cover would make the range
    /// over-approximate).
    row_range: Option<Range<u64>>,
}

#[cfg(feature = "file-io")]
impl FileServePlan {
    /// A plan serving a file's index columns by a pushed-down scan filtered to
    /// the rows where every `constraints` equality holds — or, over a located
    /// `row_range`, by point reads through the component's cached chunk
    /// probes.
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        component: &'static str,
        row_range: Option<Range<u64>>,
    ) -> Self {
        Self {
            decode: ServeDecode {
                primary_columns,
                rid_column,
                decode_layout,
            },
            reader,
            constraints,
            component,
            row_range,
        }
    }

    /// The serving component's name on the file handle.
    pub(crate) fn component(&self) -> &'static str {
        self.component
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
    /// [`Self::filter`] apply.
    pub(crate) fn file_scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// The filter selecting exactly the served rows within the index's columns
    /// — the conjunction of this plan's term equalities.
    pub(crate) fn filter(&self) -> Expression {
        let mut filter: Option<Expression> = None;
        for (column, value) in &self.constraints {
            let expr = eq(get_item(*column, root()), lit(value.clone()));
            filter = Some(match filter.take() {
                Some(f) => and(f, expr),
                None => expr,
            });
        }
        // A serve plan always carries at least one constraint (the resolved
        // lead component), so the conjunction is never empty.
        filter.expect("a serve plan constrains at least one column")
    }

    /// Decode the `(s, p, o, g)` quads out of a chunk of this plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    pub(crate) fn decode_columns(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<Quad>> {
        self.decode.decode_columns(chunk, deleted)
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved with a dictionary scan.
    pub(crate) async fn decode_columns_async(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<Quad>> {
        self.decode.decode_columns_async(chunk, deleted).await
    }
}
