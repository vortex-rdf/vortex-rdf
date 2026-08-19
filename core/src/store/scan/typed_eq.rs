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

use crate::store::selection::RowSelection;

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
/// non-nullable u32 primitives (the Dictionary layout's code columns,
/// reached directly or through a payload wrapper's canonical cache, compared
/// as slice loads), non-nullable unsigned-integer columns whose
/// encoding resolves an encoded search probe (wire-encoded adoptions and
/// non-u32 widths like TypedObject's kind column, compared through per-row
/// point reads), and canonical non-nullable Utf8 `VarBinView`s (the Default /
/// TypedObject / tail string columns, compared at the view level). Anything
/// else — nullable, unsupported encodings, string-encoded columns — declines,
/// and the caller falls back to the general mask-scan pipeline.
enum TypedEq<'a> {
    Code(PrimitiveArray, u32),
    CodeProbe(vortex_encoded_search::SortedProbe<'a>, u32),
    Str(StrEq<'a>),
}

/// A string equality probe over a canonical `VarBinView` column, comparing at
/// the view level: length first (a u32 read from the 16-byte view struct —
/// which alone rejects almost every row), then the inline bytes or the
/// referenced buffer range. No per-row `ByteBuffer` is materialized —
/// profiling showed `bytes_at`'s slice/refcount traffic dominating tail scans.
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
    /// already materialized; `canonicalize` additionally materializes one that
    /// is not, which costs a pass over the whole column and is the caller's
    /// call to make. Any other non-nullable unsigned-integer column binds
    /// through an encoded search probe when its encoding resolves. The mask
    /// scan covers what declines, at selection cost.
    fn bind_col(col: &'a ArrayRef, needle: &'a Needle, canonicalize: bool) -> Option<TypedEq<'a>> {
        use crate::store::array::{cached_u32_primitive, shared_u32_primitive};
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
                let probe = vortex_encoded_search::SortedProbe::resolve(col)?;
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
        canonicalize: bool,
    ) -> Option<Vec<TypedEq<'a>>> {
        let mut cols = Vec::with_capacity(eqs.len());
        for ((field, _), needle) in eqs.iter().zip(needles) {
            let col = struct_arr.unmasked_field_by_name(field).ok()?;
            cols.push(TypedEq::bind_col(col, needle, canonicalize)?);
        }
        Some(cols)
    }

    /// The mixed/cold row compare. The per-row `as_slice` on the Code arm is
    /// deliberate: mixed code+string constraint sets do not occur in practice
    /// (Dictionary bases are all-code, tails and Default bases all-string),
    /// and the hot all-code case takes [`TypedEq::code_views`] instead.
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
    /// constraints — is branch-free per constraint and vectorizes;
    /// benchmarking showed a mixed-enum loop costing ~2× on full-column
    /// scans. `None` when any constraint is a string compare.
    fn code_views<'b>(cols: &'b [TypedEq<'a>]) -> Option<Vec<(&'b [u32], u32)>> {
        cols.iter()
            .map(|c| match c {
                TypedEq::Code(prim, code) => Some((prim.as_slice::<u32>(), *code)),
                TypedEq::CodeProbe(..) | TypedEq::Str(..) => None,
            })
            .collect()
    }
}

/// Selection size below which a *single* residual equality is better served by
/// the typed row-at-a-time loop than the vectorized mask pipeline (see
/// [`typed_residual_ids`]).
///
/// Above it the mask scan's SIMD comparison outruns the typed loop by enough to
/// repay slicing and canonicalizing the base; below it the query is nothing but
/// that fixed cost. The exact crossover depends on how many columns the store
/// carries — a `SecondaryByCopy` store pays it over 14 — so this sits an order
/// of magnitude under the narrowest measured crossover rather than at it.
const TYPED_SINGLE_EQ_MAX_ROWS: usize = 4_096;

/// Typed residual filter over a store's base: when every residual equality
/// constraint targets a typed-comparable canonical column (see [`TypedEq`]),
/// test just the rows the selection covers with direct loads and return the
/// surviving base row ids. Returns `None` when any constraint cannot take the
/// typed path — the caller then falls back to the general mask scan, whose
/// per-call pipeline (slice through the optimizer, ConstantArray compares,
/// BoolArray canonicalization) profiling showed dominating residual-bound
/// patterns. The base counterpart of [`typed_positions`].
pub(crate) fn typed_residual_ids(
    struct_arr: &StructArray,
    selection: &RowSelection,
    base_len: usize,
    eqs: &[(&'static str, Scalar)],
) -> Option<vortex_buffer::Buffer<u64>> {
    // With ≥2 slice-bound columns the typed residual wins outright: it
    // short-circuits the conjunction per row, which the vectorized pipeline
    // cannot.
    //
    // With a lone constraint there is nothing to short-circuit, and the
    // row-at-a-time `views()` access (an erased-array deref per row) loses
    // to SIMD `compare_views_constant` — profiling showed a single-column
    // `[S]` scan running ~3× slower typed. But that holds only while the
    // scan is what dominates. The mask pipeline's arguments cost the same
    // whatever the selection: `selection.apply` pushes a slice through the
    // array optimizer and `mask_for` canonicalizes the struct, over *every*
    // column the base carries. Once a fast path has already narrowed the
    // view to a handful of rows — a bound subject's binary search leaving
    // one residual term, the `SP` shape — that fixed cost is the entire
    // query, and it scales with the store's width: on a `SecondaryByCopy`
    // store (14 columns) `SP` cost 106 µs against `SPO`'s 8 µs, purely
    // because the second constraint let it take this path instead.
    //
    // So the rule is about selection size, not column count: keep the mask
    // scan while there are enough rows for SIMD to pay for the setup, take
    // the typed loop below that.
    let needles = Needle::extract(eqs)?;
    let selected = selection.len(base_len);
    let wide = selected > TYPED_SINGLE_EQ_MAX_ROWS;
    if eqs.len() < 2 && wide {
        return None;
    }
    // Reading a compressed column row-at-a-time costs a point read where a
    // canonical one costs a load, and materializing the payload wrapper's
    // canonical form costs one pass over the whole column. Take that pass
    // when this scan alone already reads as many values as the pass decodes:
    // it repays within the call, and the wrapper's cache — shared by every
    // view and clone of the base — makes every later scan a slice loop. A
    // narrow selection stays on point reads, so a store queried only through
    // its fast paths never materializes a second copy of its columns.
    let canonicalize = selected.saturating_mul(eqs.len()) >= base_len;
    let cols = TypedEq::bind(struct_arr, eqs, &needles, canonicalize)?;
    // What still binds through a probe is an encoding with no canonical form
    // to reach for — a wire-encoded adoption. Its per-row point read never
    // outruns the vectorized compare over a wide selection.
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
    if eqs.is_empty() {
        return None;
    }
    let needles = Needle::extract(eqs)?;
    fn positions_of(
        sa: &StructArray,
        eqs: &[(&'static str, Scalar)],
        needles: &[Needle],
        offset: usize,
        out: &mut Vec<usize>,
    ) -> Option<()> {
        // Every row of the chunk is tested, so a compressed column is always
        // worth reading through its canonical form.
        let cols = TypedEq::bind(sa, eqs, needles, true)?;
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
    use crate::store::selection::RowSelection;
    use vortex_array::arrays::{Primitive, SharedArray};
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

    /// The same struct with its encoded column payload-wrapped — the resident
    /// form a built store's base carries.
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
        let n = (TYPED_SINGLE_EQ_MAX_ROWS as u32) * 2;
        let sa = mixed_struct(n);
        let eqs = eqs(&[("p", 3), ("o", 10)]);
        assert!(typed_residual_ids(&sa, &RowSelection::All, n as usize, &eqs).is_none());
        let narrow = RowSelection::Range(10..90);
        let ids = typed_residual_ids(&sa, &narrow, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (10..90u64).filter(|i| i % 7 == 3 && i % 11 == 10).collect();
        assert_eq!(ids.as_slice(), &want[..]);
    }

    /// A wide scan over a payload-wrapped column materializes the wrapper's
    /// canonical form, binds it as a slice, and is served rather than
    /// declined to the mask pipeline.
    #[test]
    fn wrapped_column_materializes_for_wide_scan() {
        let n = (TYPED_SINGLE_EQ_MAX_ROWS as u32) * 2;
        let sa = wrapped_struct(n);
        let o = sa.unmasked_field_by_name("o").unwrap().clone();
        assert!(crate::store::array::cached_u32_primitive(&o).is_none());

        let eqs = eqs(&[("p", 3), ("o", 10)]);
        let ids = typed_residual_ids(&sa, &RowSelection::All, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (0..n as u64)
            .filter(|i| i % 7 == 3 && i % 11 == 10)
            .collect();
        assert_eq!(ids.as_slice(), &want[..]);
        assert!(crate::store::array::cached_u32_primitive(&o).is_some());
    }

    /// A scan reading far fewer values than materializing would decode stays
    /// on point reads, leaving the wrapper holding only its compressed form.
    #[test]
    fn wrapped_column_stays_compressed_for_narrow_scan() {
        let n = (TYPED_SINGLE_EQ_MAX_ROWS as u32) * 2;
        let sa = wrapped_struct(n);
        let o = sa.unmasked_field_by_name("o").unwrap().clone();

        let eqs = eqs(&[("p", 3), ("o", 10)]);
        let narrow = RowSelection::Range(10..90);
        let ids = typed_residual_ids(&sa, &narrow, n as usize, &eqs).unwrap();
        let want: Vec<u64> = (10..90u64).filter(|i| i % 7 == 3 && i % 11 == 10).collect();
        assert_eq!(ids.as_slice(), &want[..]);
        assert!(crate::store::array::cached_u32_primitive(&o).is_none());
    }

    /// A canonical non-u32 unsigned column (TypedObject's kind byte) now
    /// binds through the probe instead of declining.
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
}
