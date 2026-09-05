//! Typed residual equality filtering: the row-at-a-time fast paths
//! `match_pattern` uses instead of the vectorized mask pipeline when every
//! residual constraint binds a typed column view — slice compares for
//! canonical u32 code columns, encoded point reads for compressed integer
//! columns, view-level string compares for the Default / TypedObject / tail
//! string columns. Anything else declines — as do wide selections over
//! encoded columns, where the vectorized pipeline wins — and the caller
//! falls back to the general mask-scan pipeline.

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinView, VarBinViewArray};
use vortex_array::scalar::Scalar;

use crate::store::view::selection::RowSelection;

/// A residual equality constraint's probe value, extracted from its `Scalar`
/// once per scan — not per chunk, the string extraction allocates.
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

    /// Extract every constraint's probe once; `None` if any is neither a u32
    /// code nor a utf8 string.
    fn extract(eqs: &[(&'static str, Scalar)]) -> Option<Vec<Needle>> {
        if eqs.is_empty() {
            return None;
        }
        eqs.iter().map(|(_, s)| Needle::from_scalar(s)).collect()
    }
}

/// One equality constraint bound to a concrete typed column view, for the
/// typed residual-filter fast paths. Three column shapes qualify: canonical
/// non-nullable u32 primitives (a built base's code columns, compared as
/// slice loads), non-nullable unsigned-integer columns whose
/// encoding resolves an encoded search probe (wire-encoded adoptions and
/// non-u32 widths like TypedObject's kind column, compared through per-row
/// point reads), and canonical non-nullable Utf8 `VarBinView`s (the Default /
/// TypedObject / tail string columns, compared at the view level). Anything
/// else — nullable, unsupported encodings, string-encoded columns — declines,
/// and the caller falls back to the general mask-scan pipeline.
enum TypedEq<'a> {
    Code(PrimitiveArray, u32),
    CodeProbe(vortex_rdf_encoded_search::SortedProbe<'a>, u32),
    Str(StrEq<'a>),
}

/// A string equality probe over a canonical `VarBinView` column, comparing at
/// the view level: length first (a u32 read from the 16-byte view struct —
/// which alone rejects almost every row), then the inline bytes or the
/// referenced buffer range. No per-row `ByteBuffer` is materialized, so a row
/// costs neither a slice nor a refcount bump.
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
    /// binds by slice; any other non-nullable unsigned-integer column binds
    /// through an encoded search probe when its encoding resolves. The mask
    /// scan covers what declines, at selection cost.
    fn bind_col(col: &'a ArrayRef, needle: &'a Needle) -> Option<TypedEq<'a>> {
        use crate::store::array::canonical_u32;
        use vortex_array::dtype::DType;
        if col.dtype().is_nullable() {
            return None;
        }
        match needle {
            Needle::Code(code) => {
                if !col.dtype().is_unsigned_int() {
                    return None;
                }
                if let Some(prim) = canonical_u32(col) {
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

    /// Bind every constraint to its typed column, or `None` if any declines.
    fn bind(
        struct_arr: &'a StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &'a [Needle],
    ) -> Option<Vec<TypedEq<'a>>> {
        let mut cols = Vec::with_capacity(eqs.len());
        for ((field, _), needle) in eqs.iter().zip(needles) {
            let col = struct_arr.unmasked_field_by_name(field).ok()?;
            cols.push(TypedEq::bind_col(col, needle)?);
        }
        Some(cols)
    }

    /// Row compare over the mixed enum. Sets whose every constraint is
    /// slice-bound (`TypedEq::Code`) are served by [`TypedEq::code_views`]
    /// instead and never reach here.
    #[inline]
    fn matches(&self, i: usize) -> bool {
        match self {
            TypedEq::Code(prim, code) => prim.as_slice::<u32>()[i] == *code,
            TypedEq::CodeProbe(probe, code) => probe.value_at(i) == u64::from(*code),
            TypedEq::Str(s) => s.matches(i),
        }
    }

    /// The all-code specialization: when every constraint is a u32 code
    /// compare (the Dictionary layout), the row loop over plain
    /// `(&[u32], u32)` pairs — slices hoisted once, borrowing from the bound
    /// constraints — is branch-free per constraint and vectorizes, which a
    /// loop over the mixed enum does not. `None` when any constraint is a
    /// string compare.
    fn code_views<'b>(cols: &'b [TypedEq<'a>]) -> Option<Vec<(&'b [u32], u32)>> {
        cols.iter()
            .map(|c| match c {
                TypedEq::Code(prim, code) => Some((prim.as_slice::<u32>(), *code)),
                TypedEq::CodeProbe(..) | TypedEq::Str(..) => None,
            })
            .collect()
    }
}

/// Selection size above which the typed row loop declines to the vectorized
/// mask scan: always for a lone constraint, and for any set that binds a
/// column through an encoded-search probe (see [`typed_residual_ids`]).
const TYPED_EQ_MAX_ROWS: usize = 4_096;

/// Typed residual filter over a store's base: when every residual equality
/// constraint targets a typed-comparable canonical column (see [`TypedEq`]),
/// test just the rows the selection covers with direct loads and return the
/// surviving base row ids. Returns `None` when any constraint cannot take the
/// typed path — the caller then falls back to the general mask scan, whose
/// per-call pipeline (slice through the optimizer, ConstantArray compares,
/// BoolArray canonicalization) is the fixed cost this path avoids. The base
/// counterpart of [`typed_positions`].
pub(crate) fn typed_residual_ids(
    struct_arr: &StructArray,
    selection: &RowSelection,
    base_len: usize,
    eqs: &[(&'static str, Scalar)],
) -> Option<vortex_buffer::Buffer<u64>> {
    // A lone constraint has no conjunction to short-circuit per row, so over
    // a wide selection it goes to the mask scan; two or more slice-bound
    // constraints take the typed loop at any width.
    let needles = Needle::extract(eqs)?;
    let selected = selection.len(base_len);
    let wide = selected > TYPED_EQ_MAX_ROWS;
    if eqs.len() < 2 && wide {
        return None;
    }
    let cols = TypedEq::bind(struct_arr, eqs, &needles)?;
    // A probe-bound column (no canonical form to reach for) is read per row,
    // so a wide selection over one goes to the mask scan.
    if cols.iter().any(|c| matches!(c, TypedEq::CodeProbe(..))) && wide {
        return None;
    }
    fn collect_ids(
        selection: &RowSelection,
        base_len: usize,
        matches_row: impl Fn(usize) -> bool,
    ) -> Vec<u64> {
        match selection {
            RowSelection::All => (0..base_len as u64)
                .filter(|&i| matches_row(i as usize))
                .collect(),
            RowSelection::Range(r) => (r.start..r.end)
                .filter(|&i| matches_row(i as usize))
                .collect(),
            RowSelection::Ids(ids) => ids
                .iter()
                .copied()
                .filter(|&i| matches_row(i as usize))
                .collect(),
        }
    }
    let ids: Vec<u64> = if let Some(codes) = TypedEq::code_views(&cols) {
        collect_ids(selection, base_len, |i| {
            codes.iter().all(|(s, c)| s[i] == *c)
        })
    } else {
        collect_ids(selection, base_len, |i| cols.iter().all(|c| c.matches(i)))
    };
    Some(vortex_buffer::Buffer::from_iter(ids))
}

/// The positions (in `applied`'s own row order) matching every constraint,
/// via the typed comparisons of [`TypedEq`] — the tail counterpart of
/// [`typed_residual_ids`]. Accepts a flat canonical struct or
/// a chunked accretion of them (the shape `VortexRdfStore::add_quads`
/// builds); `None` on any other shape, falling back to the mask pipeline.
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
        let cols = TypedEq::bind(sa, eqs, needles)?;
        if let Some(codes) = TypedEq::code_views(&cols) {
            out.extend(
                (0..sa.len())
                    .filter(|&i| codes.iter().all(|(s, c)| s[i] == *c))
                    .map(|i| offset + i),
            );
        } else {
            out.extend(
                (0..sa.len())
                    .filter(|&i| cols.iter().all(|c| c.matches(i)))
                    .map(|i| offset + i),
            );
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
    use vortex_array::arrays::Primitive;
    use vortex_array::validity::Validity;
    use vortex_array::{IntoArray, VortexSessionExecute};
    use vortex_buffer::Buffer;

    /// A struct of {canonical u32 `p`, bit-packed u32 `o`} — one slice-bound
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

    fn eqs(pairs: &[(&'static str, u32)]) -> Vec<(&'static str, Scalar)> {
        pairs.iter().map(|&(f, v)| (f, Scalar::from(v))).collect()
    }

    /// An encoded column binds through the probe and filters exactly like the
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

    /// A probe-bound constraint keeps the selection-size gate even with a
    /// second constraint beside it: wide selections decline to the mask scan.
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

    /// A canonical non-u32 unsigned column (TypedObject's kind byte) binds
    /// through the probe rather than declining.
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

    /// A lone slice-bound constraint declines a wide selection to the mask
    /// scan and serves a narrow one.
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

    const LONG: &str = "a string longer than the twelve inline view bytes";

    /// A struct of {canonical u32 `p`, Utf8 `o`} whose strings mix the
    /// inlined (<= 12 bytes) and out-of-line view forms.
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

    /// A Utf8 needle binds at the view level for both inlined and
    /// out-of-line strings, alone and beside a code constraint.
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

    /// A needle that is neither a code nor a string declines: `Needle`
    /// extraction answers `None` for it, before any column is bound.
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
