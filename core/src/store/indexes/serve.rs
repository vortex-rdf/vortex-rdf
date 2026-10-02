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
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::view::selection::point_sized;

/// The decode tail shared by both backend-typed serve plans: which of the
/// index's columns source each primary component, which carries the primary
/// row id, and the layout the projected columns decode through. Acquisition
/// differs per backend; everything after it lives here, once.
#[derive(Clone)]
pub(crate) struct ServeDecode {
    /// The source column for each primary `(s, p, o, g)` component, in that
    /// order — the index's own columns holding the whole quad.
    pub(super) primary_columns: [&'static str; 4],
    /// The column giving each served row's primary row id, used to drop rows
    /// tombstoned since construction.
    pub(super) rid_column: &'static str,
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
    /// where each chunk's codes are resolved with a dictionary scan.
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
        probes: &crate::store::probes::StructProbes,
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
        probes: &crate::store::probes::StructProbes,
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
    probes: Arc<crate::store::probes::StructProbes>,
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
        probes: Arc<crate::store::probes::StructProbes>,
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
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<u32>; 4]> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return None;
        }
        let live = self
            .decode
            .live_positions(&self.array, &self.range, &self.probes, deleted)?;
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
