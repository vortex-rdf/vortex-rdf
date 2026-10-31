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

use super::{COL_RID, LazyCells};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::probes::StructProbes;
use crate::store::schema::{PRIMARY_COLUMNS, RowId, TermCode};
use crate::store::view::selection::point_sized;
use vortex_rdf_encoded_search::SortedProbe;

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

    /// [`decode_columns_async`](Self::decode_columns_async) where it
    /// finishes without waiting ([`ChunkDecode::decode_now`]); `None` leaves
    /// the chunk to it.
    #[cfg(feature = "file-io")]
    pub(super) fn decode_columns_now<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Option<Vec<Result<T>>> {
        match self.chunk_rows(chunk, deleted) {
            Ok(rows) => T::decode_now(&self.decode_layout, &rows),
            Err(e) => Some(vec![Err(e)]),
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

    /// [`chunk_code_columns`](Self::chunk_code_columns) beside each live
    /// row's base row id ([`base_row`](super::base_row) of its row-id
    /// column), for a reader that puts the rows back in base order.
    #[cfg(feature = "file-io")]
    pub(super) fn chunk_code_columns_with_rids(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Result<([Buffer<TermCode>; 4], Buffer<RowId>)> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let rows = chunk.clone().execute::<StructArray>(&mut ctx)?;
        let mut column = |name: &str| -> Result<Buffer<u64>> {
            Ok(
                crate::store::array::field_as::<PrimitiveArray>(&rows, name, &mut ctx)?
                    .into_buffer::<u64>(),
            )
        };
        let [s, p, o, g] = PRIMARY_COLUMNS;
        let codes = [column(s)?, column(p)?, column(o)?, column(g)?];
        let base_rows: Buffer<RowId> = column(COL_RID)?
            .iter()
            .map(|&rid| super::base_row(rid))
            .collect();
        let Some(deleted) = deleted else {
            return Ok((codes, base_rows));
        };
        let mut live = Vec::with_capacity(base_rows.len());
        for (position, &row) in base_rows.iter().enumerate() {
            if !deleted.value(super::row_index(row)?) {
                live.push(position);
            }
        }
        if live.len() == base_rows.len() {
            return Ok((codes, base_rows));
        }
        let keep = |column: &Buffer<u64>| live.iter().map(|&i| column[i]).collect::<Buffer<u64>>();
        Ok((codes.each_ref().map(keep), keep(&base_rows)))
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

/// Rows a narrowed served view keeps of its plan's run, in the index's
/// order: each one's position in the serving component (in memory) or index
/// child (on file) beside the base row id it mirrors. A keep's result, and
/// the windows and partitions cut from it.
#[derive(Clone)]
pub(crate) struct ServedRows {
    pub(crate) positions: Buffer<u64>,
    pub(crate) rids: Buffer<RowId>,
}

impl ServedRows {
    /// How many rows these are.
    pub(crate) fn len(&self) -> usize {
        self.rids.len()
    }

    /// Rows `range` of these, sharing their buffers.
    pub(crate) fn slice(&self, range: Range<usize>) -> Self {
        Self {
            positions: self.positions.slice(range.clone()),
            rids: self.rids.slice(range),
        }
    }

    /// These rows less the `deleted` tombstones: themselves, shared, when
    /// nothing is tombstoned.
    pub(crate) fn live(&self, deleted: Option<&Mask>) -> Self {
        let Some(deleted) = deleted else {
            return self.clone();
        };
        let (positions, rids): (Vec<u64>, Vec<RowId>) = self
            .positions
            .iter()
            .zip(self.rids.iter())
            .filter(|&(_, &rid)| !deleted.value(rid as usize))
            .map(|(&position, &rid)| (position, rid))
            .unzip();
        Self {
            positions: Buffer::from(positions),
            rids: Buffer::from(rids),
        }
    }
}

/// An index's serving plan for an in-memory view: the matched rows are the
/// contiguous run `range` of the index component's own array, bounded by a
/// binary search over its sorted lead column, so `quads()` slices them from
/// the component (an `Arc` bump) or point-reads a small run, instead of
/// gathering the primary columns at scattered row ids. Every read of the
/// view follows the component's order. `MemoryBase` carries only this type;
/// `FileServePlan` (in `file.rs`) is the file-backed half.
#[derive(Clone)]
pub(crate) struct InMemoryServePlan {
    decode: ServeDecode,
    /// The serving component's name (`index:posg`, `index:ospg`).
    component: &'static str,
    /// The index component's rows, in child schema.
    array: ArrayRef,
    /// The rows of the component the match resolved: the run.
    run: Range<usize>,
    /// The rows of the component the view covers: the run, or the part of
    /// it a window cut.
    range: Range<usize>,
    /// The rows a keep kept of the range, in the component's order; when
    /// set, the view covers these and not the whole range.
    kept: Option<Arc<ServedRows>>,
    /// The run's slice of the component's rid column, the slice the view's
    /// lazy ids hold.
    rids: ArrayRef,
    /// The caches the view's lazy ids fill, shared, so the run's ids decode
    /// once for the plan's reads and the ids alike.
    cells: Arc<LazyCells>,
    /// The component's shared probe cache, so a small run reads
    /// point-by-point at its global positions instead of slicing (a slice's
    /// probe would be re-resolved per call).
    probes: Arc<StructProbes>,
}

impl InMemoryServePlan {
    /// A plan serving the contiguous `range` of the rows `array` of the
    /// in-memory index component named `component`, decoded through
    /// `decode`. `rids` is the run's slice of the component's rid column and
    /// `cells` the caches of the lazy ids built over it.
    pub(super) fn new(
        decode: ServeDecode,
        component: &'static str,
        array: ArrayRef,
        range: Range<usize>,
        rids: ArrayRef,
        probes: Arc<StructProbes>,
        cells: Arc<LazyCells>,
    ) -> Self {
        Self {
            decode,
            component,
            array,
            run: range.clone(),
            range,
            kept: None,
            rids,
            cells,
            probes,
        }
    }

    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The rows of the component the view covers: the run, or the part of
    /// it a window cut.
    pub(crate) fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// The rows a keep kept, when the view covers those and not its whole
    /// range.
    pub(crate) fn kept(&self) -> Option<&ServedRows> {
        self.kept.as_deref()
    }

    /// Whether a window or a keep narrowed the view below the run the match
    /// resolved.
    pub(crate) fn is_narrowed(&self) -> bool {
        self.kept.is_some() || self.range != self.run
    }

    /// This plan over `range`, a part of its own range: a window's of a view
    /// that keeps no rows.
    pub(crate) fn narrowed(&self, range: Range<usize>) -> Self {
        debug_assert!(self.kept.is_none());
        debug_assert!(self.range.start <= range.start && range.end <= self.range.end);
        Self {
            range,
            ..self.clone()
        }
    }

    /// This plan over `rows`, rows of its range: a keep's, or a window or
    /// partition of them.
    pub(crate) fn with_rows(&self, rows: ServedRows) -> Self {
        Self {
            kept: Some(Arc::new(rows)),
            ..self.clone()
        }
    }

    /// The view's live rows `admit` keeps, in the component's order: each
    /// row's position in the component and base row id offered to `admit`,
    /// the `deleted` tombstones skipped. What a keep of the view keeps.
    pub(crate) fn kept_rows(
        &self,
        deleted: Option<&Mask>,
        mut admit: impl FnMut(usize, RowId) -> bool,
    ) -> Result<ServedRows> {
        let rows = self
            .kept
            .as_ref()
            .map_or(self.range.len(), |kept| kept.len());
        let (mut positions, mut rids) = (Vec::with_capacity(rows), Vec::with_capacity(rows));
        let mut visit = |position: usize, rid: RowId| {
            if deleted.is_none_or(|d| !d.value(rid as usize)) && admit(position, rid) {
                positions.push(position as u64);
                rids.push(rid);
            }
        };
        match &self.kept {
            Some(kept) => {
                for (&position, &rid) in kept.positions.iter().zip(kept.rids.iter()) {
                    visit(position as usize, rid);
                }
            }
            None => {
                let run = self.run_rids()?;
                for (position, &rid) in self.range.clone().zip(&run[self.run_slots()]) {
                    visit(position, rid);
                }
            }
        }
        // A sparse keep gives back what it reserved past a quarter used.
        if positions.len() < positions.capacity() / 4 {
            positions.shrink_to_fit();
            rids.shrink_to_fit();
        }
        Ok(ServedRows {
            positions: Buffer::from(positions),
            rids: Buffer::from(rids),
        })
    }

    /// The serving component's `name` column through its cached probe, which
    /// reads it at the rows' positions; `None` when no probe resolves it.
    pub(crate) fn component_probe(&self, name: &str) -> Option<&SortedProbe<'_>> {
        Some(self.probes.by_name(&self.array, name).flatten()?.probe())
    }

    /// The run's base row ids in the component's order, decoded once into
    /// the caches the view's lazy ids share ([`LazyCells::run_order_of`]).
    pub(crate) fn run_rids(&self) -> Result<Buffer<RowId>> {
        self.cells.run_order_of(&self.rids)
    }

    /// The base row ids of the view's rows in the component's order, less
    /// the `deleted` tombstones: what a read gathers off the base in the
    /// index's order. A slice of the run's own ids, shared, when nothing is
    /// tombstoned.
    pub(crate) fn live_rids(&self, deleted: Option<&Mask>) -> Result<Buffer<RowId>> {
        if let Some(kept) = &self.kept {
            return Ok(kept.live(deleted).rids);
        }
        let rids = self.run_rids()?.slice(self.run_slots());
        Ok(match deleted {
            None => rids,
            Some(deleted) => rids
                .iter()
                .copied()
                .filter(|&rid| !deleted.value(rid as usize))
                .collect(),
        })
    }

    /// The view's live rows in the component's order: each one's position in
    /// the component beside its base row id, the `deleted` tombstones left
    /// out.
    pub(crate) fn live_rows(&self, deleted: Option<&Mask>) -> Result<(Vec<usize>, Vec<RowId>)> {
        if let Some(kept) = &self.kept {
            let live = kept.live(deleted);
            let positions = live.positions.iter().map(|&position| position as usize);
            return Ok((positions.collect(), live.rids.to_vec()));
        }
        let rids = self.run_rids()?;
        Ok(self
            .range
            .clone()
            .zip(rids[self.run_slots()].iter().copied())
            .filter(|&(_, rid)| deleted.is_none_or(|d| !d.value(rid as usize)))
            .unzip())
    }

    /// Where the view's range sits in the run: the slots of the run's ids
    /// it covers.
    fn run_slots(&self) -> Range<usize> {
        self.range.start - self.run.start..self.range.end - self.run.start
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
        if let Some(kept) = &self.kept {
            if !point_sized(kept.len() as u64) {
                return None;
            }
            let live = kept.live(deleted).positions;
            return Some(Some(
                live.iter().map(|&position| position as usize).collect(),
            ));
        }
        if !point_sized(self.range.len() as u64) {
            return None;
        }
        let Some(deleted) = deleted else {
            return Some(None);
        };
        let rid = self.probes.by_name(&self.array, COL_RID).flatten()?.probe();
        let mut rids = vec![0; self.range.len()];
        rid.read_into(self.range.start, &mut rids);
        // A row id no `usize` holds declines to the slice path, whose
        // decode reports it.
        let mut live = Vec::with_capacity(self.range.len());
        for (pos, rid) in self.range.clone().zip(rids) {
            let row = super::row_index(super::base_row(rid)).ok()?;
            if !deleted.value(row) {
                live.push(pos);
            }
        }
        Some(Some(live))
    }

    /// A small run's live rows read at the run's global positions through the
    /// component's cached probes, with no slice: the four `(s, p, o, g)`
    /// columns, each as the `u64`s its probe reads, which are the child's
    /// codes (a code column is `u64`, and a probe reads every width as one),
    /// each run of consecutive live positions in one pass. The point read
    /// behind both the decoded and the code-payload serve. `None` declines (a
    /// wide run, or a column no probe resolves, such as a string copy's); the
    /// caller keeps its slice or gather path.
    fn live_point_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<TermCode>; 4]> {
        let live = self.live_positions(deleted)?;
        let read = |name: &str| -> Option<Buffer<TermCode>> {
            let probe = self.probes.by_name(&self.array, name).flatten()?.probe();
            Some(match &live {
                None => {
                    let mut codes = vec![0; self.range.len()];
                    probe.read_into(self.range.start, &mut codes);
                    Buffer::from(codes)
                }
                Some(live) => {
                    let mut codes = vec![0; live.len()];
                    probe.read_at(live, &mut codes);
                    Buffer::from(codes)
                }
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
        observe!(ServePointRead);
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
        if let Some(kept) = &self.kept {
            let rows = self.kept_component_rows(kept, deleted).ok()?;
            return self.decode.chunk_code_columns(&rows, None).ok();
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
        if let Some(kept) = &self.kept {
            return match self.kept_component_rows(kept, deleted) {
                Ok(rows) => self.decode.decode_columns(&rows, None),
                Err(e) => vec![Err(e)],
            };
        }
        match self.array.slice(self.range.clone()) {
            Ok(rows) => self.decode.decode_columns(&rows, deleted),
            Err(e) => vec![Err(VortexRdfError::Vortex(e))],
        }
    }

    /// The component's rows at the kept rows' live positions, in their
    /// order: one take.
    fn kept_component_rows(&self, kept: &ServedRows, deleted: Option<&Mask>) -> Result<ArrayRef> {
        let positions = kept.live(deleted).positions;
        let indices = PrimitiveArray::new(positions, Validity::NonNullable).into_array();
        self.array.take(indices).map_err(VortexRdfError::Vortex)
    }
}
