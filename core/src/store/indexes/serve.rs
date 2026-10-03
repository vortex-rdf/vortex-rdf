//! Serving a resolved view's quads out of the answering index's own columns:
//! the decode tail both backends share and the in-memory plan over a
//! component's matched run.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::{FieldNames, PType};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::probes::StructProbes;
use crate::store::scan::gather::point_read_columns;
use crate::store::schema::PRIMARY_COLUMNS;
use crate::store::view::selection::point_sized;

/// How a serving index's child decodes: the child columns sourcing each
/// primary `(s, p, o, g)` component, the column carrying the primary row id
/// (tombstones are defined over primary row ids), and the layout the
/// projected columns decode through.
#[derive(Clone)]
pub(crate) struct ServeDecode {
    primary_columns: [&'static str; 4],
    rid_column: &'static str,
    decode_layout: ResolvedLayout,
}

impl ServeDecode {
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
    ) -> Self {
        Self {
            primary_columns,
            rid_column,
            decode_layout,
        }
    }

    /// The layout the served columns decode through.
    #[cfg(feature = "file-io")]
    pub(crate) fn layout(&self) -> &ResolvedLayout {
        &self.decode_layout
    }

    /// The child columns a file plan projects: the four component sources
    /// plus the row-id column.
    #[cfg(feature = "file-io")]
    pub(crate) fn projection(&self) -> [&'static str; 5] {
        let [s, p, o, g] = self.primary_columns;
        [s, p, o, g, self.rid_column]
    }

    /// The live positions of a point-sized run. The outer `None` declines: a
    /// run wider than `POINT_GATHER_MAX_ROWS`, or a rid column resolving no
    /// probe. The inner `None` means no tombstones: every position in `range`
    /// is live.
    fn live_positions(
        &self,
        array: &ArrayRef,
        range: &Range<usize>,
        probes: &StructProbes,
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

    /// A point-sized run's live rows as a primary-named `(s, p, o, g)` struct,
    /// read point by point through the component's cached probes. `Ok(None)`
    /// declines: a wide run, or a column resolving no probe.
    fn point_read_run_rows(
        &self,
        array: &ArrayRef,
        range: Range<usize>,
        probes: &StructProbes,
        deleted: Option<&Mask>,
    ) -> Result<Option<ArrayRef>> {
        let Some(live) = self.live_positions(array, &range, probes, deleted) else {
            return Ok(None);
        };
        let live: Vec<usize> = live.unwrap_or_else(|| range.collect());
        let Some(children) = point_read_columns(array, &self.primary_columns, &live, Some(probes))
        else {
            return Ok(None);
        };
        Ok(Some(primary_struct(into_arrays(children), live.len())?))
    }

    /// A chunk's live rows as a primary-named `(s, p, o, g)` struct: relabel
    /// the source columns, canonicalize a point-sized chunk by point reads
    /// when `canonicalize`, then drop rows whose primary row id is tombstoned.
    pub(crate) fn chunk_rows(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
        canonicalize: bool,
    ) -> Result<ArrayRef> {
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
        let mut rows = primary_struct(vec![col(s)?, col(p)?, col(o)?, col(g)?], len)?;
        if canonicalize
            && point_sized(len as u64)
            && let Some(children) =
                point_read_columns(&rows, &PRIMARY_COLUMNS, &(0..len).collect::<Vec<_>>(), None)
        {
            rows = primary_struct(into_arrays(children), len)?;
        }

        let Some(deleted) = deleted else {
            return Ok(rows);
        };
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

/// Point-read primitives as array references.
fn into_arrays(children: Vec<PrimitiveArray>) -> Vec<ArrayRef> {
    children.into_iter().map(IntoArray::into_array).collect()
}

/// `children` as a non-nullable struct under the primary column names.
fn primary_struct(children: Vec<ArrayRef>, len: usize) -> Result<ArrayRef> {
    Ok(StructArray::try_new(
        FieldNames::from(PRIMARY_COLUMNS),
        children,
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)?
    .into_array())
}

/// An index's serving plan for an in-memory view: the matched rows are the
/// contiguous `range` of the component's `array`, so reads slice or point-read
/// them instead of gathering the primary columns by row id.
#[derive(Clone)]
pub(crate) struct InMemoryServePlan {
    decode: ServeDecode,
    /// The component's rows, in child schema.
    array: ArrayRef,
    range: Range<usize>,
    /// The component's shared probe cache.
    probes: Arc<StructProbes>,
}

impl InMemoryServePlan {
    pub(crate) fn new(
        decode: ServeDecode,
        array: ArrayRef,
        range: Range<usize>,
        probes: Arc<StructProbes>,
    ) -> Self {
        Self {
            decode,
            array,
            range,
            probes,
        }
    }

    /// The served rows' four `u32` term codes, read off the component's own
    /// columns in the index's order. `None` declines: a run wider than
    /// `POINT_GATHER_MAX_ROWS`, a non-Dictionary decode layout, or a column
    /// resolving no probe.
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<u32>; 4]> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return None;
        }
        let live = self
            .decode
            .live_positions(&self.array, &self.range, &self.probes, deleted)?;
        let live: Vec<usize> = live.unwrap_or_else(|| self.range.clone().collect());
        let columns = point_read_columns(
            &self.array,
            &self.decode.primary_columns,
            &live,
            Some(&self.probes),
        )?;
        let mut columns = columns
            .into_iter()
            .map(|column| (column.ptype() == PType::U32).then(|| column.into_buffer::<u32>()));
        Some([
            columns.next()??,
            columns.next()??,
            columns.next()??,
            columns.next()??,
        ])
    }

    /// The matched rows decoded as primary `(s, p, o, g)` quads: point reads
    /// through the component's probes for a point-sized run, else the slice
    /// of the component's rows.
    pub(crate) fn decode<T: ChunkDecode>(&self, deleted: Option<&Mask>) -> Vec<Result<T>> {
        let rows = match self.decode.point_read_run_rows(
            &self.array,
            self.range.clone(),
            &self.probes,
            deleted,
        ) {
            Ok(Some(rows)) => Ok(rows),
            Ok(None) => self
                .array
                .slice(self.range.clone())
                .map_err(VortexRdfError::Vortex)
                .and_then(|rows| self.decode.chunk_rows(&rows, deleted, false)),
            Err(e) => Err(e),
        };
        match rows {
            Ok(rows) => T::decode(&self.decode.decode_layout, &rows),
            Err(e) => vec![Err(e)],
        }
    }
}
