//! The index-agnostic *serving* path: reading a resolved view's quads out of
//! the answering index's own columns instead of gathering the primary
//! columns by scattered row id. An index builds a serve plan during
//! resolution and the store executes it without knowing which index produced
//! it. A permutation index (whole quads in another order,
//! `IndexType::SecondaryByCopy`) can serve; a back-reference index
//! (`(value, row id)` pairs, `IndexType::SecondaryByReference`) cannot.
//!
//! The plans are typed by backend, as only the acquisition of the matched
//! columns differs: [`InMemoryServePlan`] slices the matched run of an
//! in-memory component, and `FileServePlan` (in `file.rs`) scans the index
//! child, its located run by row range, else under a pushed-down
//! term-equality filter. Each backend's base (`MemoryBase`, `FileBase`)
//! carries only its own plan type, and both plans decode through
//! [`ServeDecode`], so tombstone handling is shared.
//!
//! A plan reproduces exactly the rows the resolution's row ids name, so an
//! operation that cannot honor it (chained matches, counts,
//! materialization) reads through the ids instead. The store keeps a plan
//! only while the resolution is a view's sole restriction (see `MemoryBase`,
//! `FileBase`). A plan is what lets the resolution defer the ids
//! (`LazyRowIds`): reads through the plan never touch them.

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

/// The decode both serve plans share. A serving child holds the whole quad
/// under the primary column names ([`PRIMARY_COLUMNS`]) beside the primary
/// row id ([`COL_RID`]), through which the rows tombstoned since
/// construction are dropped.
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
    /// decode, for a file-backed dictionary: each chunk's codes are resolved
    /// by reading the dictionary windows that hold them.
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

    /// A chunk's live rows as four code columns, `(s, p, o, g)`: the
    /// code-payload counterpart of [`decode_columns`](Self::decode_columns)
    /// for a Dictionary decode layout, whose columns hold codes.
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
        // A small run is point-read (`gather_by_point_reads` gates on
        // `POINT_GATHER_MAX_ROWS`); a wider one decodes as sliced.
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
/// contiguous run `range` of the index component's own array, bounded by a
/// binary search over its sorted lead column, so `quads()` slices them from
/// the component (an `Arc` bump) or point-reads a small run, instead of
/// gathering the primary columns at scattered row ids. `MemoryBase` carries
/// only this type; `FileServePlan` (in `file.rs`) is the file-backed half.
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

    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The positions of a small run's live rows, for point reads through the
    /// component's cached probes. Tombstones are defined over primary row
    /// ids, and the rid column says which primary row each served row
    /// mirrors; only a tombstoned view pays for the liveness pass.
    ///
    /// The outer `None` declines: a run wider than
    /// [`POINT_GATHER_MAX_ROWS`], a rid column no probe resolves, or a row id
    /// no `usize` holds. The inner `None` means no tombstones: every position
    /// of the run is live.
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
    /// positions through the component's cached probes, with no slice: the
    /// four `(s, p, o, g)` columns, each as the `u64`s its probe reads, which
    /// are the child's codes (a code column is `u64`, and a probe reads every
    /// width as one). The point read behind both the decoded and the
    /// code-payload serve. `None` declines (a wide run, or a column no probe
    /// resolves, such as a string copy's); the caller keeps its slice or
    /// gather path.
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
        #[cfg(test)]
        serve_hooks::note_point_read();
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

    /// The served rows' four [`TermCode`] columns, read off the index
    /// component's own columns: the code-payload counterpart of
    /// [`decode`](Self::decode), in the index's order as `decode` serves
    /// them. Under the Dictionary layout a copy holds the view's codes
    /// contiguously, so no row id is materialized and no primary column
    /// gathered: a run of at most [`POINT_GATHER_MAX_ROWS`] rows is read
    /// point by point through the component's cached probes, and any other
    /// is the component sliced to the run, each column decoded whole.
    ///
    /// `None` declines to the caller's gather path: a non-Dictionary decode
    /// layout (its columns hold terms, not codes), or a slice that fails to
    /// decode (the gather path reports the error).
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<TermCode>; 4]> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return None;
        }
        if let Some(columns) = self.live_point_columns(deleted) {
            return Some(columns);
        }
        let rows = self.array.slice(self.range.clone()).ok()?;
        self.decode.chunk_code_columns(&rows, deleted).ok()
    }

    /// The matched rows decoded from the index component's own rows as the
    /// primary `(s, p, o, g)`: point reads at the run's global positions
    /// through the cached probes when the run is small, else the component
    /// sliced to the run.
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

/// Test hook: how many served in-memory runs this thread read point by point
/// at their global positions ([`InMemoryServePlan::point_read_run_rows`]), not
/// by slicing the component.
#[cfg(test)]
pub(crate) mod serve_hooks {
    use std::cell::Cell;

    thread_local! {
        static POINT_READS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one run read point by point.
    pub(super) fn note_point_read() {
        POINT_READS.with(|c| c.set(c.get() + 1));
    }

    /// The runs read point by point on this thread so far.
    pub(crate) fn point_reads() -> usize {
        POINT_READS.with(Cell::get)
    }
}
