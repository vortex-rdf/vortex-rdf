//! Gathering a view's rows out of an in-memory base: the tombstone-aware
//! slice/take pipeline, and the point-read path small selections take
//! through encoded-search probes.

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, Struct, StructArray};
use vortex_array::dtype::PType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_mask::Mask;
use vortex_rdf_encoded_search::SortedProbe;

use crate::error::{Result, VortexRdfError};
use crate::store::probes::StructProbes;
use crate::store::schema::RowId;
use crate::store::view::selection::{RowSelection, point_sized};

/// The rows of `base` that `selection` covers and `deleted` has not
/// tombstoned: the one place the in-memory reads turn a view into rows, so
/// none of them can forget the tombstones, which the selection never holds
/// (see [`RowSelection::live_mask`]).
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

/// The rows of `base` at the base row ids `ids`, in the order of `ids` (any
/// order, each id once, none tombstoned): point by point through `probes`
/// when they are few ([`point_sized`]), else one take. A served view's rows
/// read off the base in its index's order.
pub(crate) fn gather_rows_at(
    base: &ArrayRef,
    ids: &Buffer<RowId>,
    probes: Option<&StructProbes>,
) -> Result<ArrayRef> {
    if point_sized(ids.len() as u64)
        && let Some(fields) = base.dtype().as_struct_fields_opt()
    {
        let positions: Vec<usize> = ids.iter().map(|&id| id as usize).collect();
        let names = fields.names().clone();
        if let Some(children) =
            point_read_columns(base, names.iter().map(AsRef::as_ref), &positions, probes)
        {
            let rows =
                StructArray::try_new(names, children, positions.len(), Validity::NonNullable)?;
            return Ok(rows.into_array());
        }
    }
    let indices = PrimitiveArray::new(ids.clone(), Validity::NonNullable).into_array();
    base.take(indices).map_err(VortexRdfError::Vortex)
}

/// The live rows of a point-sized selection, read point by point through
/// encoded-search probes into canonical columns, skipping the per-column
/// slice/canonicalize pipeline whose fixed cost dominates tiny reads of a
/// compressed base. `None` declines to the general pipeline: a wide or `All`
/// selection, a non-struct base, or a child no probe resolves (a string
/// column, say). The serve plans canonicalize their small chunks through it
/// too (`Range(0..len)` over the sliced component rows).
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
/// primitive: the loop behind [`gather_by_point_reads`]. `None` declines: a
/// non-struct `array`, a name it lacks, or a column no probe resolves (a
/// string column, say).
///
/// A probe comes from `probes` when that cache holds `array`'s resolutions
/// (resolution walks the encoding tree, the fixed cost this path avoids), and
/// a column the cache declined declines the read; for a transient array (a
/// tail, served rows) it is resolved per call.
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
                use vortex_rdf_encoded_search::counters::{GATHER_DECODED, GATHER_ENCODED, note};
                if vortex_rdf_encoded_search::encoded_search_enabled() {
                    note(GATHER_ENCODED, child.len());
                    read_points(&SortedProbe::resolve(child)?, child, positions)
                } else {
                    note(GATHER_DECODED, child.len());
                    // VORTEX_RDF_ENCODED_SEARCH=0: decoded first, then read.
                    let decoded = crate::store::array::decoded_for_search(child).ok()?;
                    read_points(&SortedProbe::resolve(&decoded)?, &decoded, positions)
                }
            }
        };
        children.push(column?);
    }
    Some(children)
}

/// `column` read at `positions` through its `probe` into a canonical
/// primitive. Positions that hold consecutive runs are read a run in one pass
/// ([`SortedProbe::read_at`]), a `u64` column (every code column) taking the
/// reads as its buffer and a narrower one narrowing them; positions none of
/// which follow another are read point by point straight into the column
/// (see [`primitive_from_u64_reads`]).
fn read_points(probe: &SortedProbe, column: &ArrayRef, positions: &[usize]) -> Option<ArrayRef> {
    if !positions.windows(2).any(|pair| pair[1] == pair[0] + 1) {
        let reads = positions.iter().map(|&i| probe.value_at(i));
        return primitive_from_u64_reads(column.dtype().as_ptype(), reads);
    }
    let mut reads = vec![0; positions.len()];
    probe.read_at(positions, &mut reads);
    match column.dtype().as_ptype() {
        PType::U64 => Some(
            PrimitiveArray::new(vortex_buffer::Buffer::from(reads), Validity::NonNullable)
                .into_array(),
        ),
        ptype => primitive_from_u64_reads(ptype, reads.into_iter()),
    }
}

/// A primitive column of `ptype` built from point reads widened to `u64`.
/// The reads must be values of a `ptype` column, so narrowing them back is
/// lossless. `None` for any type the point-read paths do not produce (the
/// caller declines to its vectorized path).
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

        // Narrower columns come back at their own width: a u8 one (the
        // TypedObject layout's kind byte) and a u16 one equal their slices.
        let kind = Buffer::from_iter([2u8, 0, 1, 2, 3]).into_array();
        let wide = Buffer::from_iter([7u16, 300, 9, 65_535, 0]).into_array();
        let narrow = StructArray::try_new(
            ["k", "w"].into(),
            vec![kind, wide],
            5,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let rows = gather_by_point_reads(&narrow, &selection, Some(&deleted), None)
            .unwrap()
            .expect("u8 and u16 columns are read by point reads");
        let column = |name: &str| {
            let sa = rows.clone().try_downcast::<Struct>().unwrap();
            sa.unmasked_field_by_name(name)
                .unwrap()
                .clone()
                .try_downcast::<Primitive>()
                .unwrap()
        };
        assert_eq!(column("k").as_slice::<u8>(), &[2, 3]);
        assert_eq!(column("w").as_slice::<u16>(), &[7, 0]);
        // A type no point read produces (no probe reads a signed column).
        assert!(primitive_from_u64_reads(PType::I32, [1u64].into_iter()).is_none());
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
