//! Gathering a view's rows out of an in-memory base: the tombstone-aware
//! slice/take pipeline and the point-read path small selections take
//! through encoded search probes.

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, Struct, StructArray};
use vortex_array::dtype::PType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_mask::Mask;
use vortex_rdf_encoded_search::SortedProbe;

use crate::error::{Result, VortexRdfError};
use crate::store::probes::StructProbes;
use crate::store::view::selection::RowSelection;

/// Gather the rows of `base` that `selection` covers and `deleted` has not
/// tombstoned.
///
/// The single place the in-memory read paths turn a view into rows, so that
/// applying the tombstones cannot be forgotten by one of them: deletions are
/// deliberately kept out of the selection (see [`RowSelection::live_mask`]), so
/// a selection alone always over-reports.
pub(crate) fn gather_live(
    base: &ArrayRef,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    probes: Option<&StructProbes>,
) -> Result<ArrayRef> {
    if let Some(rows) = gather_by_point_reads(base, selection, deleted, probes)? {
        return Ok(rows);
    }
    let rows = selection.apply(base)?;
    let Some(deleted) = deleted else {
        return Ok(rows);
    };
    let live = selection.live_mask(deleted, base.len());
    if live.all_true() {
        return Ok(rows);
    }
    rows.filter(live).map_err(VortexRdfError::Vortex)
}

/// Rows of a small selection, read point-by-point through encoded search
/// probes into canonical output columns — the read-side counterpart of the
/// match paths' encoded probing, skipping the per-column slice/canonicalize
/// pipeline whose fixed cost dominates tiny reads on a compressed-resident
/// base. `None` declines: a wide or `All` selection, a non-struct base, or
/// any child (e.g. a string column) no probe resolves — the general pipeline
/// handles those. Also serves the index serving plans, which canonicalize
/// their small contiguous chunks through it (`Range(0..len)` over the sliced
/// component rows).
pub(crate) fn gather_by_point_reads(
    base: &ArrayRef,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    probes: Option<&StructProbes>,
) -> Result<Option<ArrayRef>> {
    let Some(live) = selection.point_sized_live_rows(deleted, base.len()) else {
        return Ok(None);
    };
    let live: Vec<usize> = live.into_iter().map(|i| i as usize).collect();
    let Some(fields) = base.dtype().as_struct_fields_opt() else {
        return Ok(None);
    };
    let names = fields.names().clone();
    let Some(children) = point_read_columns(base, names.iter().map(AsRef::as_ref), &live, probes)
    else {
        return Ok(None);
    };
    let rows =
        StructArray::try_new(names, children, live.len(), Validity::NonNullable)?.into_array();
    Ok(Some(rows))
}

/// The `names` columns of the struct `array`, in that order, each read point
/// by point at `positions` through its encoded-search probe into a canonical
/// primitive — the loop behind [`gather_by_point_reads`]: a small selection's
/// gather, and the serve tail's canonicalization of a small chunk
/// (`ServeDecode::chunk_rows`). `None` declines: a non-struct `array`, a name
/// it lacks, or a column (e.g. a string column) no probe resolves.
///
/// Each probe comes from `probes` when that cache holds `array`'s resolutions
/// (resolution walks the encoding tree, the fixed cost this path exists to
/// avoid) — a column it declined declines the read — and is resolved per
/// call otherwise (a transient base: a tail, served rows), the only case
/// that takes the struct apart for its children.
fn point_read_columns<'n>(
    array: &ArrayRef,
    names: impl IntoIterator<Item = &'n str>,
    positions: &[usize],
    probes: Option<&StructProbes>,
) -> Option<Vec<ArrayRef>> {
    let names = names.into_iter();
    let mut children = Vec::with_capacity(names.size_hint().0);
    let mut parts = None;
    for name in names {
        let column = match probes.and_then(|p| p.by_name(array, name)) {
            Some(cached) => {
                let cached = cached?;
                read_points(cached.probe(), cached.array(), positions)
            }
            None => {
                if parts.is_none() {
                    parts = Some(array.clone().try_downcast::<Struct>().ok()?);
                }
                let child = parts.as_ref()?.unmasked_field_by_name(name).ok()?;
                read_points(&SortedProbe::resolve(child)?, child, positions)
            }
        };
        children.push(column?);
    }
    Some(children)
}

/// `column` read point by point at `positions` through its `probe` into a
/// canonical primitive (see [`primitive_from_u64_reads`]).
fn read_points(probe: &SortedProbe, column: &ArrayRef, positions: &[usize]) -> Option<ArrayRef> {
    let reads = positions.iter().map(|&i| probe.value_at(i));
    primitive_from_u64_reads(column.dtype().as_ptype(), reads)
}

/// A primitive column of `ptype` built from point reads widened to `u64`.
/// `None` for any type the point-read paths do not produce (the caller
/// declines to its vectorized path).
pub(crate) fn primitive_from_u64_reads(
    ptype: PType,
    reads: impl Iterator<Item = u64>,
) -> Option<ArrayRef> {
    Some(match ptype {
        PType::U8 => PrimitiveArray::from_iter(reads.map(|v| v as u8)).into_array(),
        PType::U16 => PrimitiveArray::from_iter(reads.map(|v| v as u16)).into_array(),
        PType::U32 => PrimitiveArray::from_iter(reads.map(|v| v as u32)).into_array(),
        PType::U64 => PrimitiveArray::from_iter(reads).into_array(),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::arrays::{Primitive, VarBinViewArray};
    use vortex_buffer::Buffer;

    /// A five-row canonical u32 struct whose `s` column is the row id.
    fn u32_struct() -> ArrayRef {
        let s = Buffer::from_iter(0..5u32).into_array();
        let p = Buffer::from_iter((0..5u32).map(|i| i % 2)).into_array();
        StructArray::try_new(["s", "p"].into(), vec![s, p], 5, Validity::NonNullable)
            .unwrap()
            .into_array()
    }

    fn column_u32(rows: &ArrayRef, name: &str) -> Vec<u32> {
        let sa = rows.clone().try_downcast::<Struct>().unwrap();
        let col = sa.unmasked_field_by_name(name).unwrap().clone();
        col.try_downcast::<Primitive>()
            .unwrap()
            .as_slice::<u32>()
            .to_vec()
    }

    /// A point-sized id selection is read row by row with the tombstoned
    /// rows dropped from the gathered struct.
    #[test]
    fn gather_by_point_reads_drops_tombstones() {
        let base = u32_struct();
        let selection = RowSelection::Ids(Buffer::from_iter([0u64, 2, 4]));
        let deleted = Mask::from_indices(5, vec![2]);
        let rows = gather_by_point_reads(&base, &selection, Some(&deleted), None)
            .unwrap()
            .expect("a point-sized selection over probeable columns is served");
        assert_eq!(rows.len(), 2);
        assert_eq!(column_u32(&rows, "s"), vec![0, 4]);
        assert_eq!(column_u32(&rows, "p"), vec![0, 0]);
    }

    /// A child no probe resolves (a string column) declines the whole read —
    /// resolved per call without a cache, and as the cache's own decline
    /// (`Some(None)`) with the base's warmed one.
    #[test]
    fn gather_by_point_reads_declines_string_child() {
        let s = Buffer::from_iter(0..3u32).into_array();
        let o = VarBinViewArray::from_iter_str(["a", "b", "c"]).into_array();
        let base = StructArray::try_new(["s", "o"].into(), vec![s, o], 3, Validity::NonNullable)
            .unwrap()
            .into_array();
        let selection = RowSelection::Ids(Buffer::from_iter([0u64, 2]));
        assert!(
            gather_by_point_reads(&base, &selection, None, None)
                .unwrap()
                .is_none()
        );

        let probes = StructProbes::new();
        probes.warm(&base);
        assert!(matches!(probes.by_name(&base, "o"), Some(None)));
        assert!(
            gather_by_point_reads(&base, &selection, None, Some(&probes))
                .unwrap()
                .is_none()
        );
    }

    /// `All` is never point-sized, so it declines to the slice/take pipeline.
    #[test]
    fn gather_by_point_reads_declines_all() {
        let base = u32_struct();
        assert!(
            gather_by_point_reads(&base, &RowSelection::All, None, None)
                .unwrap()
                .is_none()
        );
    }
}
