//! Gathering a view's rows out of an in-memory base: the tombstone-aware
//! slice/take pipeline and the point-read path small selections take
//! through encoded search probes.

use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::PType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_mask::Mask;

use crate::error::{Result, VortexRdfError};
use crate::store::view::probes::StructProbes;
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
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{Struct, StructArray};
    use vortex_rdf_encoded_search::SortedProbe;

    let Some(live) = selection.point_sized_live_rows(deleted) else {
        return Ok(None);
    };
    let live: Vec<usize> = live.into_iter().map(|i| i as usize).collect();
    let Ok(struct_arr) = base.clone().try_downcast::<Struct>() else {
        return Ok(None);
    };
    let names = struct_arr.names().clone();
    let mut children = Vec::with_capacity(names.len());
    for (idx, name) in names.iter().enumerate() {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return Ok(None);
        };
        // The store's cached probe first (resolution walks the encoding tree
        // — the fixed cost this path exists to avoid); a transient base
        // (tail, served rows) resolves per call.
        let cached = probes.and_then(|p| p.child(base, idx));
        let local;
        let probe = match cached {
            Some(owned) => owned.probe(),
            None => {
                let Some(resolved) = SortedProbe::resolve(child) else {
                    return Ok(None);
                };
                local = resolved;
                &local
            }
        };
        let reads = live.iter().map(|&i| probe.value_at(i));
        let Some(gathered) = primitive_from_u64_reads(child.dtype().as_ptype(), reads) else {
            return Ok(None);
        };
        children.push(gathered);
    }
    let rows = StructArray::try_new(names, children, live.len(), Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)?
        .into_array();
    Ok(Some(rows))
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
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{Primitive, Struct, StructArray, VarBinViewArray};
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
