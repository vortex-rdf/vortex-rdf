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
//! term-equality filter. Each backend of a view's source (`MemoryBase`,
//! `FileBase`) carries exactly its own plan type, so a view paired with the
//! other backend's plan is unrepresentable — and both decode through the
//! shared [`ServeDecode`] tail, so tombstone handling cannot drift between
//! them.
//!
//! Correctness never depends on a plan: it reproduces exactly the rows the
//! resolution's row ids name, so any operation that can't honor it (chained
//! matches, counting, materializing) simply ignores it and reads through the
//! row ids. The store keeps a plan only while the resolution is a view's sole
//! restriction — see `MemoryBase` / `FileBase`. A plan is also what licenses
//! *deferring* those row ids (`LazyRowIds`): with a plan attached, reads never
//! touch them, so the resolution hands back a recipe instead of scanning for
//! them at match time.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use super::COL_RID;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::probes::StructProbes;
use crate::store::schema::{PRIMARY_COLUMNS, RowId, TermCode};
use crate::store::view::selection::point_sized;

/// The decode tail shared by both backend-typed serve plans: the layout the
/// serving child's columns decode through. A serving child holds the whole
/// quad under the primary column names ([`PRIMARY_COLUMNS`]) beside the
/// primary row id ([`COL_RID`]), which drops rows tombstoned since
/// construction. Acquisition differs per backend; everything after it lives
/// here, once.
#[derive(Clone)]
pub(super) struct ServeDecode {
    /// The layout the projected source columns decode through (an index that
    /// stores whole terms decodes them as strings, or dictionary codes under
    /// the Dictionary layout).
    pub(super) decode_layout: ResolvedLayout,
}

impl ServeDecode {
    /// Decode the `(s, p, o, g)` rows out of a chunk of the plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    pub(super) fn decode_columns<T: ChunkDecode>(
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
    /// where each chunk's codes are resolved by reading the dictionary
    /// windows that hold them.
    #[cfg(feature = "file-io")]
    pub(super) async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => T::decode_async(&self.decode_layout, &rows).await,
            Err(e) => vec![Err(e)],
        }
    }

    /// A chunk's live rows as four code columns, `(s, p, o, g)` — the
    /// code-payload counterpart of [`decode_columns`](Self::decode_columns)
    /// for a Dictionary decode layout, whose columns hold codes.
    #[cfg(feature = "file-io")]
    pub(super) fn chunk_code_columns(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let rows = self
            .chunk_rows(chunk, deleted)?
            .execute::<StructArray>(&mut ctx)?;
        let [s, p, o, g] = PRIMARY_COLUMNS;
        let mut column = |name: &str| -> Result<Buffer<TermCode>> {
            Ok(
                crate::store::array::field_as::<PrimitiveArray>(&rows, name, &mut ctx)?
                    .into_buffer::<TermCode>(),
            )
        };
        Ok([column(s)?, column(p)?, column(o)?, column(g)?])
    }

    /// A chunk's live rows as a primary-named `(s, p, o, g)` struct: the
    /// chunk's quad columns without the row id, then any row whose primary
    /// row id is tombstoned dropped.
    fn chunk_rows(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx)?;
        let col = |name: &'static str| {
            struct_arr
                .unmasked_field_by_name(name)
                .cloned()
                .map_err(VortexRdfError::Vortex)
        };
        let [s, p, o, g] = PRIMARY_COLUMNS;
        let len = struct_arr.len();
        let rows = StructArray::try_new(
            FieldNames::from(PRIMARY_COLUMNS),
            vec![col(s)?, col(p)?, col(o)?, col(g)?],
            len,
            Validity::NonNullable,
        )?
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
        let rid_col = col(COL_RID)?.execute::<PrimitiveArray>(&mut ctx)?;
        let mut positions = Vec::with_capacity(len);
        for (position, &rid) in rid_col.as_slice::<RowId>().iter().enumerate() {
            if !deleted.value(super::row_index(super::base_row(rid))?) {
                positions.push(position);
            }
        }
        let live = Mask::from_indices(len, positions);
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
/// `FileServePlan` is the file-backed half). `MemoryBase` carries
/// exactly this type, so an in-memory view can never hold a file plan.
#[derive(Clone)]
pub(crate) struct InMemoryServePlan {
    decode: ServeDecode,
    /// The serving component's name (`index:posg`, `index:ospg`).
    component: &'static str,
    /// The index component's rows, in child schema.
    array: ArrayRef,
    range: Range<usize>,
    /// The component's shared probe cache, so a small run reads
    /// point-by-point at its global positions instead of slicing (a slice's
    /// probe would be re-resolved per call).
    probes: Arc<StructProbes>,
}

impl InMemoryServePlan {
    /// A plan serving the contiguous `range` of the rows `array` of the
    /// in-memory index component named `component`, decoded through
    /// `decode`.
    pub(super) fn new(
        decode: ServeDecode,
        component: &'static str,
        array: ArrayRef,
        range: Range<usize>,
        probes: Arc<StructProbes>,
    ) -> Self {
        Self {
            decode,
            component,
            array,
            range,
            probes,
        }
    }

    /// The serving component's name.
    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The positions of a small run's live rows, for point reads through the
    /// component's cached probes. Tombstones are defined over primary row
    /// ids; the rid column says which primary row each served row mirrors,
    /// and only a tombstoned view pays for the liveness pass.
    ///
    /// The outer `None` declines: a run wider than
    /// [`POINT_GATHER_MAX_ROWS`], or a rid column whose encoding resolves no
    /// probe. The inner `None` means no tombstones — every position in the
    /// run is live, so the caller iterates the run directly.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    fn live_positions(&self, deleted: Option<&Mask>) -> Option<Option<Vec<usize>>> {
        if !point_sized(self.range.len() as u64) {
            return None;
        }
        let Some(deleted) = deleted else {
            return Some(None);
        };
        let rid = self.probes.by_name(&self.array, COL_RID).flatten()?.probe();
        // A row id no `usize` holds declines to the slice path, whose
        // decode reports it.
        let mut live = Vec::with_capacity(self.range.len());
        for pos in self.range.clone() {
            let row = super::row_index(super::base_row(rid.value_at(pos))).ok()?;
            if !deleted.value(row) {
                live.push(pos);
            }
        }
        Some(Some(live))
    }

    /// A small run's live rows read point by point at the run's global
    /// positions through the component's cached probes — no slice: the four
    /// `(s, p, o, g)` columns, each as the `u64`s its probe reads, which are
    /// the child's codes (a code column is `u64`, and a probe reads every
    /// width as one). The one point read behind both the decoded and the
    /// code-payload serve. `None` declines (a wide run, or a column — e.g. a
    /// string copy — whose encoding resolves no probe); the caller keeps its
    /// slice or gather path.
    fn live_point_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<TermCode>; 4]> {
        let live = self.live_positions(deleted)?;
        let read = |name: &str| -> Option<Buffer<TermCode>> {
            let probe = self.probes.by_name(&self.array, name).flatten()?.probe();
            Some(match &live {
                None => Buffer::from_iter(self.range.clone().map(|pos| probe.value_at(pos))),
                Some(live) => Buffer::from_iter(live.iter().map(|&pos| probe.value_at(pos))),
            })
        };
        let [s, p, o, g] = PRIMARY_COLUMNS;
        Some([read(s)?, read(p)?, read(o)?, read(g)?])
    }

    /// A small run's live rows ([`live_point_columns`](Self::live_point_columns))
    /// as a primary-named `(s, p, o, g)` canonical struct. `Ok(None)`
    /// declines; the caller keeps the slice path.
    fn point_read_run_rows(&self, deleted: Option<&Mask>) -> Result<Option<ArrayRef>> {
        let Some(columns) = self.live_point_columns(deleted) else {
            return Ok(None);
        };
        let len = columns[0].len();
        Ok(Some(
            StructArray::try_new(
                FieldNames::from(PRIMARY_COLUMNS),
                Vec::from(columns.map(IntoArray::into_array)),
                len,
                Validity::NonNullable,
            )?
            .into_array(),
        ))
    }

    /// The served rows' four [`TermCode`]s, read straight off the index
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
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<TermCode>; 4]> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return None;
        }
        self.live_point_columns(deleted)
    }

    /// Decode the matched rows straight from the index component's rows:
    /// point reads at the run's global positions through the component's
    /// cached probes when the run is small, else slice the component to this
    /// plan's row run — either way decoding those columns as the primary
    /// `(s, p, o, g)`, replacing the row-id gather over the primaries.
    pub(crate) fn decode<T: ChunkDecode>(&self, deleted: Option<&Mask>) -> Vec<Result<T>> {
        match self.point_read_run_rows(deleted) {
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
