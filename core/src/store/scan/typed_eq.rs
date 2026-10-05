//! Typed residual equality filtering: row-at-a-time compares over a base's
//! columns, slice loads for canonical u32 code columns, encoded point reads
//! for compressed integer columns, view-level compares for `Utf8` columns.
//! Anything else declines to the vectorized mask pipeline.

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinView, VarBinViewArray};
use vortex_array::scalar::Scalar;

use crate::store::view::selection::RowSelection;

/// A constraint's probe value, extracted from its `Scalar` once per scan.
enum Needle {
    Code(u32),
    Str(String),
}

impl Needle {
    fn from_scalar(scalar: &Scalar) -> Option<Self> {
        if let Ok(code) = u32::try_from(scalar) {
            return Some(Needle::Code(code));
        }
        Some(Needle::Str(scalar.as_utf8_opt()?.value()?.to_string()))
    }

    /// Every constraint's probe; `None` for an empty set or a value that is
    /// neither a u32 code nor a utf8 string.
    fn extract(eqs: &[(&'static str, Scalar)]) -> Option<Vec<Needle>> {
        if eqs.is_empty() {
            return None;
        }
        eqs.iter().map(|(_, s)| Needle::from_scalar(s)).collect()
    }
}

/// One equality constraint bound to a typed column view: a canonical
/// non-nullable u32 primitive compared by slice load, a non-nullable unsigned
/// integer column compared through an encoded-search probe, or a canonical
/// non-nullable `Utf8` `VarBinView` compared at the view level.
enum TypedEq<'a> {
    Code(PrimitiveArray, u32),
    CodeProbe(vortex_rdf_encoded_search::SortedProbe<'a>, u32),
    Str(StrEq<'a>),
}

/// A string equality over a canonical `VarBinView` column, compared at the
/// view level: length first, then the inline bytes or the referenced buffer
/// range.
struct StrEq<'a> {
    arr: VarBinViewArray,
    needle: &'a [u8],
}

impl StrEq<'_> {
    #[inline]
    fn matches(&self, i: usize) -> bool {
        let view = &self.arr.views()[i];
        if view.len() as usize != self.needle.len() {
            return false;
        }
        if view.is_inlined() {
            view.as_inlined().value() == self.needle
        } else {
            let r = view.as_view();
            let buf: &[u8] = self.arr.buffer(r.buffer_index as usize);
            &buf[r.as_range()] == self.needle
        }
    }
}

impl<'a> TypedEq<'a> {
    /// Bind one constraint to its column, or decline. A canonical u32 column
    /// binds by slice, as does a payload-wrapped one whose canonical form is
    /// materialized; `canonicalize` materializes one that is not. Any other
    /// non-nullable unsigned-integer column binds through an encoded-search
    /// probe when its encoding resolves one. Nullable columns decline.
    fn bind_col(col: &'a ArrayRef, needle: &'a Needle, canonicalize: bool) -> Option<TypedEq<'a>> {
        use crate::store::resident::{cached_u32_primitive, shared_u32_primitive};
        use vortex_array::dtype::DType;
        if col.dtype().is_nullable() {
            return None;
        }
        match needle {
            Needle::Code(code) => {
                if !col.dtype().is_unsigned_int() {
                    return None;
                }
                let canonical = match canonicalize {
                    true => shared_u32_primitive(col),
                    false => cached_u32_primitive(col),
                };
                if let Some(prim) = canonical {
                    return Some(TypedEq::Code(prim, *code));
                }
                let probe = vortex_rdf_encoded_search::SortedProbe::resolve(col)?;
                Some(TypedEq::CodeProbe(probe, *code))
            }
            Needle::Str(s) => {
                if !matches!(col.dtype(), DType::Utf8(_)) {
                    return None;
                }
                let arr = col.clone().try_downcast::<VarBinView>().ok()?;
                Some(TypedEq::Str(StrEq {
                    arr,
                    needle: s.as_bytes(),
                }))
            }
        }
    }

    /// Every constraint bound to its column, or `None` if any declines.
    fn bind(
        struct_arr: &'a StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &'a [Needle],
        canonicalize: bool,
    ) -> Option<Vec<TypedEq<'a>>> {
        let mut cols = Vec::with_capacity(eqs.len());
        for ((field, _), needle) in eqs.iter().zip(needles) {
            let col = struct_arr.unmasked_field_by_name(field).ok()?;
            cols.push(TypedEq::bind_col(col, needle, canonicalize)?);
        }
        Some(cols)
    }

    #[inline]
    fn matches(&self, i: usize) -> bool {
        match self {
            TypedEq::Code(prim, code) => prim.as_slice::<u32>()[i] == *code,
            TypedEq::CodeProbe(probe, code) => probe.value_at(i) == u64::from(*code),
            TypedEq::Str(s) => s.matches(i),
        }
    }

    /// The constraints as plain `(&[u32], u32)` pairs when every one is a
    /// slice-bound code compare, `None` otherwise.
    fn code_views<'b>(cols: &'b [TypedEq<'a>]) -> Option<Vec<(&'b [u32], u32)>> {
        cols.iter()
            .map(|c| match c {
                TypedEq::Code(prim, code) => Some((prim.as_slice::<u32>(), *code)),
                TypedEq::CodeProbe(..) | TypedEq::Str(..) => None,
            })
            .collect()
    }
}

/// The per-row test over a bound constraint set: the all-code form loops
/// over `(&[u32], u32)` pairs, the mixed form over the enum.
/// Selection size above which the typed row loop declines to the mask scan:
/// always for a lone constraint, and for any set binding a column through an
/// encoded-search probe.
const TYPED_EQ_MAX_ROWS: usize = 4_096;

/// The row ids of `selection` whose rows pass `matches`, in selection order.
/// One monomorphic loop per selection variant: the hot path of a residual
/// over an unindexed in-memory store.
fn filter_selected(
    selection: &RowSelection,
    base_len: usize,
    matches: impl Fn(usize) -> bool,
) -> Vec<u64> {
    match selection {
        RowSelection::All => (0..base_len as u64)
            .filter(|&i| matches(i as usize))
            .collect(),
        RowSelection::Range(r) => (r.start..r.end).filter(|&i| matches(i as usize)).collect(),
        RowSelection::Ids(ids) => ids
            .iter()
            .copied()
            .filter(|&i| matches(i as usize))
            .collect(),
    }
}

/// The base row ids inside `selection` satisfying every equality in `eqs`,
/// tested row by row through typed column views. `None` declines to the mask
/// scan: a constraint neither code nor string, a column no view binds, a
/// lone constraint or a probe-bound column over more than
/// [`TYPED_EQ_MAX_ROWS`] selected rows. A payload wrapper's canonical form
/// is materialized only when selected rows x constraints >= `base_len`.
pub(crate) fn typed_residual_ids(
    struct_arr: &StructArray,
    selection: &RowSelection,
    base_len: usize,
    eqs: &[(&'static str, Scalar)],
) -> Option<vortex_buffer::Buffer<u64>> {
    let needles = Needle::extract(eqs)?;
    let selected = selection.len(base_len);
    let wide = selected > TYPED_EQ_MAX_ROWS;
    if eqs.len() < 2 && wide {
        return None;
    }
    let canonicalize = selected.saturating_mul(eqs.len()) >= base_len;
    let cols = TypedEq::bind(struct_arr, eqs, &needles, canonicalize)?;
    if cols.iter().any(|c| matches!(c, TypedEq::CodeProbe(..))) && wide {
        return None;
    }
    let ids = match TypedEq::code_views(&cols) {
        Some(codes) => filter_selected(selection, base_len, |i| {
            codes.iter().all(|(s, c)| s[i] == *c)
        }),
        None => filter_selected(selection, base_len, |i| cols.iter().all(|c| c.matches(i))),
    };
    Some(vortex_buffer::Buffer::from_iter(ids))
}

/// The positions in `applied` (a flat canonical struct, or a chunked
/// accretion of them) satisfying every equality in `eqs`, in its own row
/// order; `None` on any other shape or a column no view binds.
pub(crate) fn typed_positions(
    applied: &ArrayRef,
    eqs: &[(&'static str, Scalar)],
) -> Option<Vec<usize>> {
    use vortex_array::arrays::chunked::ChunkedArrayExt;
    use vortex_array::arrays::{Chunked, Struct};
    let needles = Needle::extract(eqs)?;
    fn positions_of(
        sa: &StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &[Needle],
        offset: usize,
        out: &mut Vec<usize>,
    ) -> Option<()> {
        // Every row is tested, so a compressed column is read canonical.
        let cols = TypedEq::bind(sa, eqs, needles, true)?;
        let rows = 0..sa.len();
        match TypedEq::code_views(&cols) {
            Some(codes) => out.extend(
                rows.filter(|&i| codes.iter().all(|(s, c)| s[i] == *c))
                    .map(|i| offset + i),
            ),
            None => out.extend(
                rows.filter(|&i| cols.iter().all(|c| c.matches(i)))
                    .map(|i| offset + i),
            ),
        }
        Some(())
    }
    let mut out = Vec::new();
    if let Ok(sa) = applied.clone().try_downcast::<Struct>() {
        positions_of(&sa, eqs, &needles, 0, &mut out)?;
        return Some(out);
    }
    if let Ok(ch) = applied.clone().try_downcast::<Chunked>() {
        let mut offset = 0usize;
        for chunk in ch.chunks() {
            let sa = chunk.try_downcast::<Struct>().ok()?;
            positions_of(&sa, eqs, &needles, offset, &mut out)?;
            offset += sa.len();
        }
        return Some(out);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::view::selection::RowSelection;
    use vortex_array::arrays::{Primitive, SharedArray};
    use vortex_array::validity::Validity;
    use vortex_array::{IntoArray, VortexSessionExecute};
    use vortex_buffer::Buffer;

    /// A struct of {canonical u32 `p`, bit-packed u32 `o`}: one slice-bound
    /// and one probe-bound column.
    fn mixed_struct(n: u32) -> StructArray {
        let p = Buffer::from_iter((0..n).map(|i| i % 7)).into_array();
        let o_canonical = vortex_array::arrays::PrimitiveArray::from_iter((0..n).map(|i| i % 11));
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let o = vortex::encodings::fastlanes::bitpack_compress::bitpack_encode(
            &o_canonical,
            4,
            None,
            &mut ctx,
        )
        .unwrap()
        .into_array();
        assert!(
            o.clone().try_downcast::<Primitive>().is_err(),
            "fixture column must stay encoded"
        );
        StructArray::try_new(
            ["p", "o"].into(),
            vec![p, o],
            n as usize,
            Validity::NonNullable,
        )
        .unwrap()
    }

    /// The same struct with its encoded column payload-wrapped.
    fn wrapped_struct(n: u32) -> StructArray {
        let plain = mixed_struct(n);
        let p = plain.unmasked_field_by_name("p").unwrap().clone();
        let o = plain.unmasked_field_by_name("o").unwrap().clone();
        StructArray::try_new(
            ["p", "o"].into(),
            vec![p, SharedArray::new(o).into_array()],
            n as usize,
            Validity::NonNullable,
        )
        .unwrap()
    }

    fn eqs(pairs: &[(&'static str, u32)]) -> Vec<(&'static str, Scalar)> {
        pairs.iter().map(|&(f, v)| (f, Scalar::from(v))).collect()
    }

    /// An encoded column binds through the probe and filters like the
    /// canonical ground truth.
    #[test]
    fn probe_bound_column_filters_rows() {
        let sa = mixed_struct(1000);
        let eqs = eqs(&[("p", 3), ("o", 10)]);
        let ids = typed_residual_ids(&sa, &RowSelection::All, 1000, &eqs).unwrap();
        let want: Vec<u64> = (0..1000u64)
            .filter(|i| i % 7 == 3 && i % 11 == 10)
            .collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// A probe-bound constraint keeps the selection-size gate even beside a
    /// second constraint.
    #[test]
    fn probe_bound_column_declines_wide_selection() {
        let n = (TYPED_EQ_MAX_ROWS as u32) * 2;
        let sa = mixed_struct(n);
        let eqs = eqs(&[("p", 3), ("o", 10)]);
        assert!(typed_residual_ids(&sa, &RowSelection::All, n as usize, &eqs).is_none());
        let narrow = RowSelection::Range(10..90);
        let ids = typed_residual_ids(&sa, &narrow, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (10..90u64).filter(|i| i % 7 == 3 && i % 11 == 10).collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// A wide scan over a payload-wrapped column materializes the wrapper's
    /// canonical form and binds it as a slice.
    #[test]
    fn wrapped_column_materializes_for_wide_scan() {
        let n = (TYPED_EQ_MAX_ROWS as u32) * 2;
        let sa = wrapped_struct(n);
        let o = sa.unmasked_field_by_name("o").unwrap().clone();
        assert!(crate::store::resident::cached_u32_primitive(&o).is_none());

        let eqs = eqs(&[("p", 3), ("o", 10)]);
        let ids = typed_residual_ids(&sa, &RowSelection::All, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (0..n as u64)
            .filter(|i| i % 7 == 3 && i % 11 == 10)
            .collect();
        assert_eq!(ids.as_slice(), &want[..]);
        assert!(crate::store::resident::cached_u32_primitive(&o).is_some());
    }

    /// A narrow scan stays on point reads and leaves the wrapper compressed.
    #[test]
    fn wrapped_column_stays_compressed_for_narrow_scan() {
        let n = (TYPED_EQ_MAX_ROWS as u32) * 2;
        let sa = wrapped_struct(n);
        let o = sa.unmasked_field_by_name("o").unwrap().clone();

        let eqs = eqs(&[("p", 3), ("o", 10)]);
        let narrow = RowSelection::Range(10..90);
        let ids = typed_residual_ids(&sa, &narrow, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (10..90u64).filter(|i| i % 7 == 3 && i % 11 == 10).collect();
        assert_eq!(ids.as_slice(), &want[..]);
        assert!(crate::store::resident::cached_u32_primitive(&o).is_none());
    }

    /// A canonical non-u32 unsigned column binds through the probe.
    #[test]
    fn canonical_u8_column_binds() {
        let kind = Buffer::from_iter((0..100u32).map(|i| (i % 3) as u8)).into_array();
        let sa =
            StructArray::try_new(["k"].into(), vec![kind], 100, Validity::NonNullable).unwrap();
        let eqs = vec![("k", Scalar::from(2u8))];
        let ids = typed_residual_ids(&sa, &RowSelection::All, 100, &eqs).unwrap();
        let want: Vec<u64> = (0..100u64).filter(|i| i % 3 == 2).collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// A lone slice-bound constraint declines a wide selection and serves a
    /// narrow one.
    #[test]
    fn single_code_eq_declines_wide_selection() {
        let n = (TYPED_EQ_MAX_ROWS as u32) * 2;
        let sa = mixed_struct(n);
        let eqs = eqs(&[("p", 3)]);
        assert!(typed_residual_ids(&sa, &RowSelection::All, n as usize, &eqs).is_none());
        let ids = typed_residual_ids(&sa, &RowSelection::Range(0..100), n as usize, &eqs).unwrap();
        let want: Vec<u64> = (0..100u64).filter(|i| i % 7 == 3).collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// An id selection tests exactly the rows it names.
    #[test]
    fn id_selection_tests_named_rows_only() {
        let sa = mixed_struct(100);
        let eqs = eqs(&[("p", 3)]);
        let selection = RowSelection::Ids(Buffer::from_iter([3u64, 4, 10, 17, 99]));
        let ids = typed_residual_ids(&sa, &selection, 100, &eqs).unwrap();
        assert_eq!(ids.as_slice(), &[3u64, 10, 17]);
    }

    const LONG: &str = "a string longer than the twelve inline view bytes";

    /// A struct of {canonical u32 `p`, Utf8 `o`} mixing inlined and
    /// out-of-line strings.
    fn string_struct(n: u32) -> StructArray {
        let p = Buffer::from_iter((0..n).map(|i| i % 3)).into_array();
        let o = VarBinViewArray::from_iter_str((0..n).map(|i| match i % 4 {
            0 => "short",
            1 => LONG,
            _ => "other",
        }))
        .into_array();
        StructArray::try_new(
            ["p", "o"].into(),
            vec![p, o],
            n as usize,
            Validity::NonNullable,
        )
        .unwrap()
    }

    /// A Utf8 needle binds at the view level for inlined and out-of-line
    /// strings, alone and beside a code constraint.
    #[test]
    fn string_column_binds_at_view_level() {
        let sa = string_struct(200);
        let short = vec![("o", Scalar::from("short"))];
        let ids = typed_residual_ids(&sa, &RowSelection::All, 200, &short).unwrap();
        let want: Vec<u64> = (0..200u64).filter(|i| i % 4 == 0).collect();
        assert_eq!(ids.as_slice(), &want[..]);

        let long = vec![("o", Scalar::from(LONG))];
        let ids = typed_residual_ids(&sa, &RowSelection::All, 200, &long).unwrap();
        let want: Vec<u64> = (0..200u64).filter(|i| i % 4 == 1).collect();
        assert_eq!(ids.as_slice(), &want[..]);

        let mixed = vec![("p", Scalar::from(1u32)), ("o", Scalar::from(LONG))];
        let ids = typed_residual_ids(&sa, &RowSelection::All, 200, &mixed).unwrap();
        let want: Vec<u64> = (0..200u64).filter(|i| i % 3 == 1 && i % 4 == 1).collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// A nullable column declines even when its values are all valid.
    #[test]
    fn nullable_column_declines() {
        let p = PrimitiveArray::new(
            Buffer::from_iter((0..10u32).map(|i| i % 3)),
            Validity::AllValid,
        )
        .into_array();
        assert!(p.dtype().is_nullable());
        let sa = StructArray::try_new(["p"].into(), vec![p], 10, Validity::NonNullable).unwrap();
        let eqs = eqs(&[("p", 1)]);
        assert!(typed_residual_ids(&sa, &RowSelection::All, 10, &eqs).is_none());
    }

    /// A needle that is neither a code nor a string declines before any
    /// column is bound.
    #[test]
    fn non_code_non_string_needle_declines() {
        let sa = mixed_struct(10);
        let eqs = vec![("p", Scalar::from(true))];
        assert!(typed_residual_ids(&sa, &RowSelection::All, 10, &eqs).is_none());
    }

    fn code_struct(codes: &[u32]) -> ArrayRef {
        let p = Buffer::from_iter(codes.iter().copied()).into_array();
        StructArray::try_new(["p"].into(), vec![p], codes.len(), Validity::NonNullable)
            .unwrap()
            .into_array()
    }

    /// Positions over a chunked accretion are offset by the preceding
    /// chunks' rows; a chunk that is not a struct declines.
    #[test]
    fn typed_positions_offsets_across_chunks() {
        use vortex_array::arrays::ChunkedArray;
        let first = code_struct(&[0, 1, 2]);
        let second = code_struct(&[5, 7, 9]);
        let dtype = first.dtype().clone();
        let chunked = ChunkedArray::try_new(vec![first, second], dtype)
            .unwrap()
            .into_array();
        let eqs = eqs(&[("p", 7)]);
        assert_eq!(typed_positions(&chunked, &eqs), Some(vec![4]));

        let plain = Buffer::from_iter([7u32, 7]).into_array();
        let dtype = plain.dtype().clone();
        let non_struct = ChunkedArray::try_new(vec![plain], dtype)
            .unwrap()
            .into_array();
        assert!(typed_positions(&non_struct, &eqs).is_none());
    }
}
