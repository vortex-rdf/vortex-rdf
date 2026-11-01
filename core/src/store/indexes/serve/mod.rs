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
//! in-memory component, and [`FileServePlan`] (in `file.rs`) scans the index
//! child, its located run by row range, else under a pushed-down
//! term-equality filter. Each backend's base (`MemoryBase`, `FileBase`)
//! carries only its own plan type, and both plans decode through
//! [`ServeDecode`], so tombstone handling is shared.
//!
//! A plan reproduces exactly the view's rows: the run the resolution's row
//! ids name, or the part of it a window, keep, partition or chained match
//! narrowed it to, so an operation that cannot honor it (materialization in
//! base order) reads through the ids instead. A match keeps the plan only
//! while its resolution is the view's sole restriction; the narrowings of a
//! served view keep it, narrowed (see `MemoryBase`, `FileBase`). A plan is
//! what lets the resolution defer the ids (`LazyRowIds`): reads through the
//! plan never touch them.

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
use crate::store::SortOrder;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::schema::{PRIMARY_COLUMNS, RowId, TermCode};

#[cfg(feature = "file-io")]
mod file;
mod memory;

#[cfg(feature = "file-io")]
pub(crate) use file::{FileServePlan, PlanRead};
pub(crate) use memory::InMemoryServePlan;

/// The decode both serve plans share. A serving child holds the whole quad
/// under the primary column names ([`PRIMARY_COLUMNS`]) beside the primary
/// row id ([`COL_RID`]), through which the rows tombstoned since
/// construction are dropped.
#[derive(Clone)]
pub(crate) struct ServeDecode {
    /// The layout the projected source columns decode through (an index that
    /// stores whole terms decodes them as strings, or dictionary codes under
    /// the Dictionary layout).
    pub(super) decode_layout: ResolvedLayout,
    /// The order the plan's reads follow, which the copy that chose the run
    /// knows: `(p, o, s, g)` for an `index:posg` run whose terms leave the
    /// object unbound (`?p??`, `?p?g`); else base order, `(s, p, o, g)`, which
    /// every other served run's order reduces to (its bound columns lead the
    /// copy's sort).
    pub(super) order: SortOrder,
}

impl ServeDecode {
    /// Whether the decode reads codes off the index (the Dictionary layout's
    /// copies hold codes; the string layouts', terms).
    pub(crate) fn decodes_codes(&self) -> bool {
        matches!(self.decode_layout, ResolvedLayout::Dictionary(_))
    }

    /// Decode the `(s, p, o, g)` rows out of a chunk of the plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    pub(crate) fn decode_columns<T: ChunkDecode>(
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
    pub(crate) async fn decode_columns_async<T: ChunkDecode>(
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
    pub(crate) fn decode_columns_now<T: ChunkDecode>(
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
    pub(crate) fn chunk_code_columns(
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
    pub(crate) fn chunk_rows(&self, chunk: &ArrayRef, deleted: Option<&Mask>) -> Result<ArrayRef> {
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

/// Rows of a plan's run in the index's order: each one's position in the
/// serving component (in memory) or index child (on file) beside the base
/// row id it mirrors. As a narrowing keeps them (a keep's result, and the
/// windows and partitions cut from it) every row is live; as an unlocated
/// file run is first read, before its narrowing filters them
/// ([`ServedRows::live`]), tombstoned rows are among them.
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

    /// Each row's position beside its base row id, in order.
    pub(crate) fn positions_and_rids(&self) -> impl Iterator<Item = (u64, RowId)> + '_ {
        self.positions
            .iter()
            .copied()
            .zip(self.rids.iter().copied())
    }

    /// The kept `rows` (positions beside base row ids, in order), out of at
    /// most `capacity` offered: every narrowing that filters rows (a keep, a
    /// chained match, the tombstones of a range) collects what its filter
    /// leaves here. Each row is pushed once to buffers reserved for every
    /// row offered: two allocations, which a doubling buffer would spend on
    /// its growth. Keeping fewer than a quarter of them gives the rest back
    /// (two more): the buffers live as long as the narrowed view and every
    /// window of it.
    pub(crate) fn collect(rows: impl Iterator<Item = (u64, RowId)>, capacity: usize) -> Self {
        let (mut positions, mut rids) =
            (Vec::with_capacity(capacity), Vec::with_capacity(capacity));
        for (position, rid) in rows {
            positions.push(position);
            rids.push(rid);
        }
        if positions.len() < capacity / 4 {
            positions.shrink_to_fit();
            rids.shrink_to_fit();
        }
        Self {
            positions: Buffer::from(positions),
            rids: Buffer::from(rids),
        }
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
    #[cfg(feature = "file-io")]
    pub(crate) fn live(&self, deleted: Option<&Mask>) -> Self {
        let Some(deleted) = deleted else {
            return self.clone();
        };
        let live = self
            .positions_and_rids()
            .filter(|&(_, rid)| !deleted.value(rid as usize));
        Self::collect(live, self.len())
    }
}

/// The rows of its index a served view covers, in the index's order: what
/// both plans hold of a run, and what every narrowing of a served view cuts
/// or keeps. A match's plan covers its whole run; a narrowing's covers what
/// it cut or kept ([`InMemoryServePlan::is_narrowed`]).
#[derive(Clone)]
pub(crate) enum ServedSpan {
    /// Rows `range` of the index: a match's run, or the part of it a window
    /// or partition cut. Rows tombstoned since the index was built are among
    /// them, dropped as they are read, through their row ids.
    Range(Range<u64>),
    /// The rows a keep or a chained match kept, a window or partition of
    /// them, or an unlocated run's rows where they are not contiguous in the
    /// index: each one live.
    Kept(Arc<ServedRows>),
}

impl ServedSpan {
    /// How many rows the span covers, tombstoned ones in a range included.
    pub(crate) fn len(&self) -> usize {
        match self {
            ServedSpan::Range(range) => (range.end - range.start) as usize,
            ServedSpan::Kept(rows) => rows.len(),
        }
    }
}

/// A served in-memory view's live rows in its component, as a stream reads
/// them ([`InMemoryServePlan::live_component_rows`]).
pub(crate) enum ComponentRows {
    /// A run of the component with nothing kept or tombstoned.
    Range(Range<usize>),
    /// The live rows' positions in the component, in its order.
    Positions(Buffer<u64>),
}

impl ComponentRows {
    /// How many rows.
    pub(crate) fn len(&self) -> usize {
        match self {
            ComponentRows::Range(range) => range.len(),
            ComponentRows::Positions(positions) => positions.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The capacity of a uniquely owned buffer.
    fn capacity<T>(buffer: Buffer<T>) -> usize {
        match buffer.try_into_mut() {
            Ok(owned) => owned.capacity(),
            Err(_) => panic!("a kept buffer is uniquely owned"),
        }
    }

    /// A keep that admits few of a view's rows holds buffers sized to the rows
    /// it kept, not to the view it reserved for: they live as long as the kept
    /// view and every window of it.
    #[test]
    fn sparse_keeps_hold_only_their_rows() {
        let rows = || (0..10_000u64).map(|position| (position, position as RowId));
        let sparse = ServedRows::collect(
            rows().filter(|&(position, _)| position % 1_000 == 0),
            10_000,
        );
        assert_eq!(sparse.len(), 10);
        assert!(capacity(sparse.positions) < 10_000 / 4, "positions");
        assert!(capacity(sparse.rids) < 10_000 / 4, "row ids");
        let dense = ServedRows::collect(rows().filter(|&(position, _)| position % 2 == 0), 10_000);
        assert_eq!(dense.len(), 5_000);
        assert_eq!(dense.positions.as_slice()[..3], [0, 2, 4]);
    }
}
