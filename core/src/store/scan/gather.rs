//! Gathering a view's rows out of an in-memory base: the tombstone-aware
//! slice/take pipeline and the point-read path point-sized selections take
//! through encoded-search probes.

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

/// The rows of `base` that `selection` covers and `deleted` has not
/// tombstoned. Tombstones live outside the selection
/// ([`RowSelection::live_mask`]), so every in-memory read gathers through
/// here.
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

/// The live rows of a point-sized selection, read point by point through
/// encoded-search probes into canonical columns. `None` declines: a wide or
/// `All` selection, a non-struct base, or a child no probe resolves.
pub(crate) fn gather_by_point_reads(
    base: &ArrayRef,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    probes: Option<&StructProbes>,
) -> Result<Option<ArrayRef>> {
    let Some(live) = selection.point_sized_live_rows(deleted) else {
        return Ok(None);
    };
    let live: Vec<usize> = live.into_iter().map(|i| i as usize).collect();
    let Some(fields) = base.dtype().as_struct_fields_opt() else {
        return Ok(None);
    };
    let names = fields.names().clone();
    let name_strs: Vec<&str> = names.iter().map(|n| n.as_ref()).collect();
    let Some(children) = point_read_columns(base, &name_strs, &live, probes) else {
        return Ok(None);
    };
    let children: Vec<ArrayRef> = children.into_iter().map(IntoArray::into_array).collect();
    let rows = StructArray::try_new(names, children, live.len(), Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)?
        .into_array();
    Ok(Some(rows))
}

/// `names`' columns of the struct `array` at `positions`, each read point by
/// point through its encoded-search probe (the cached one in `probes` when it
/// resolves, else one resolved per call) into a canonical primitive. `None`
/// when `array` is not a struct, or a column resolves no probe or has a type
/// the reads cannot produce.
pub(crate) fn point_read_columns(
    array: &ArrayRef,
    names: &[&str],
    positions: &[usize],
    probes: Option<&StructProbes>,
) -> Option<Vec<PrimitiveArray>> {
    let struct_arr = array.clone().try_downcast::<Struct>().ok()?;
    let mut children = Vec::with_capacity(names.len());
    for name in names {
        let idx = struct_arr
            .names()
            .iter()
            .position(|n| n.as_ref() == *name)?;
        let child = struct_arr.unmasked_field_by_name(name).ok()?;
        let cached = probes.and_then(|p| p.child(array, idx));
        let local;
        let probe = match cached {
            Some(owned) => owned.probe(),
            None => {
                local = SortedProbe::resolve(child)?;
                &local
            }
        };
        let reads = positions.iter().map(|&i| probe.value_at(i));
        children.push(primitive_from_u64_reads(child.dtype().as_ptype(), reads)?);
    }
    Some(children)
}

/// A primitive column of `ptype` from point reads widened to `u64`; `None`
/// for a type the point-read paths do not produce.
pub(crate) fn primitive_from_u64_reads(
    ptype: PType,
    reads: impl Iterator<Item = u64>,
) -> Option<PrimitiveArray> {
    Some(match ptype {
        PType::U8 => PrimitiveArray::from_iter(reads.map(|v| v as u8)),
        PType::U16 => PrimitiveArray::from_iter(reads.map(|v| v as u16)),
        PType::U32 => PrimitiveArray::from_iter(reads.map(|v| v as u32)),
        PType::U64 => PrimitiveArray::from_iter(reads),
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
    /// rows dropped.
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

    /// A child no probe resolves (a string column) declines the whole read.
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
    }

    /// `All` is never point-sized.
    #[test]
    fn gather_by_point_reads_declines_all() {
        let base = u32_struct();
        assert!(
            gather_by_point_reads(&base, &RowSelection::All, None, None)
                .unwrap()
                .is_none()
        );
    }

    /// Named columns come back in the order asked, at the positions asked.
    #[test]
    fn point_read_columns_follow_names_and_positions() {
        let base = u32_struct();
        let columns = point_read_columns(&base, &["p", "s"], &[4, 1], None).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].as_slice::<u32>(), &[0, 1]);
        assert_eq!(columns[1].as_slice::<u32>(), &[4, 1]);
        assert!(point_read_columns(&base, &["nope"], &[0], None).is_none());
    }
}
