//! Low-level helpers over Vortex arrays: string-array construction, canonical
//! string-column access, sortedness statistics, sorted-column binary search,
//! the encoding of a store's integer children and the canonical accessors
//! that read them back, and mask conversion.
//!
//! The integer-children helpers are the two ends of the compressed-resident
//! form: [`with_compressed_int_children`] encodes a built store's code
//! columns into probe-supported encodings, [`with_searchable_int_children`]
//! keeps an adopted store's encodings wherever a probe binds them, and
//! [`shared_u32_primitive`] / [`cached_u32_primitive`] hand back the
//! canonical primitive a slice-bound read path needs — decoding into the
//! shared wrapper's cache, or only if some earlier read already did.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;

use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, VarBinViewArray};
use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Zero-cost row access into a canonical `VarBinView` string column: the
/// 16-byte views slice and the data buffers are resolved once, and each row's
/// bytes are then an inline read or a plain slice of the referenced buffer.
///
/// `bytes_at` instead materializes a refcounted `ByteBuffer` per row (two
/// atomic refcount ops plus alignment bookkeeping), which profiling showed
/// costing ~5% of every many-row decode; this reader is the loop-shaped
/// counterpart, sharing the access pattern of the typed residual filter's
/// `StrEq`.
pub(crate) struct StrColReader<'a> {
    arr: &'a VarBinViewArray,
    views: &'a [BinaryView],
}

impl<'a> StrColReader<'a> {
    pub(crate) fn new(arr: &'a VarBinViewArray) -> Self {
        Self {
            arr,
            views: arr.views(),
        }
    }

    #[inline]
    pub(crate) fn bytes_at(&self, i: usize) -> &'a [u8] {
        let view = &self.views[i];
        if view.is_inlined() {
            view.as_inlined().value()
        } else {
            let r = view.as_view();
            &self.arr.buffer(r.buffer_index as usize)[r.as_range()]
        }
    }

    #[inline]
    pub(crate) fn str_at(&self, i: usize) -> Result<&'a str> {
        buf_as_str(self.bytes_at(i))
    }
}

/// Build a Vortex string array (`VarBinView<Utf8>`, non-nullable) from string refs.
///
/// Values are copied once, directly into the array's buffer — no intermediate
/// owned `String` per value.
pub(crate) fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Build a nullable Vortex string array for optional fields (e.g. o_datatype, o_lang).
pub(crate) fn make_nullable_string_array(
    values: impl IntoIterator<Item = Option<String>>,
) -> ArrayRef {
    VarBinViewArray::from_iter_nullable_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on an array.
///
/// Only call when the array is sorted by construction: `match_pattern` trusts
/// this stat to binary-search the column, so a false stamp corrupts query
/// results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted — order is never assumed, only trusted
/// when explicitly recorded.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (`lo == hi` means the value is absent). Only meaningful on
/// columns [`column_is_sorted`] reports as sorted.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::arrays::Primitive;
    use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

    // Typed fast path: a canonical non-nullable u32 column (the Dictionary
    // layout's term codes). `partition_point` over the raw slice costs a few
    // dozen loads; the generic kernel below builds a fresh `ExecutionCtx` and
    // materializes a `Scalar` per probe, which profiling showed dominating
    // `match_pattern`'s fixed per-call cost.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(prim) = arr.clone().try_downcast::<Primitive>()
        && prim.ptype() == vortex_array::dtype::PType::U32
        && let Ok(code) = u32::try_from(probe)
    {
        let codes = prim.as_slice::<u32>();
        let lo = codes.partition_point(|&v| v < code);
        let hi = codes.partition_point(|&v| v <= code);
        return Ok((lo, hi));
    }

    // Encoded fast path: probe the compressed representation directly. Covers
    // wire-encoded columns (`from_bytes` adoption, sliced index runs) without
    // decoding them; declines fall through to the generic kernel.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(needle) = u64::try_from(probe)
        && let Some(encoded) = vortex_encoded_search::SortedProbe::resolve(arr)
    {
        return Ok(encoded.bounds(needle));
    }

    let index_of = |result: SearchResult| match result {
        SearchResult::Found(i) | SearchResult::NotFound(i) => i,
    };
    let lo = arr
        .search_sorted(probe, SearchSortedSide::Left)
        .map_err(VortexRdfError::Vortex)?;
    let hi = arr
        .search_sorted(probe, SearchSortedSide::Right)
        .map_err(VortexRdfError::Vortex)?;
    Ok((index_of(lo), index_of(hi)))
}

/// Keep a base struct's integer children (the Dictionary layout's term
/// codes, TypedObject's kind column) in their compressed form wherever the
/// match fast paths can bind them, decoding only the remainder to canonical
/// primitives — preserving each child's `IsSorted` stamp across any
/// re-encoding.
///
/// The per-row match fast paths — [`search_sorted_bounds`]' probes and the
/// typed residual loops — bind canonical primitives and any encoding an
/// encoded search probe resolves, so those children stay compressed at
/// adoption. A child outside the probe's supported set (e.g. dictionary
/// encoding) would pay a per-call fallback through the generic per-scalar
/// kernel on every match; decoding it once here keeps every fast path fast
/// for the store's lifetime. String children keep their encoded form: their
/// canonical `VarBinView` costs real memory, and the mask scan handles them
/// at selection cost. Serialization re-compresses every child through the
/// default write strategy, so the wire format is unaffected.
///
/// A nullable struct passes through untouched — the base schema is
/// non-nullable, and rebuilding a struct with validity is not this helper's
/// business.
pub(crate) fn with_searchable_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::validity::Validity;

    if rows.dtype().is_nullable() {
        return Ok(rows);
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = rows
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let names = struct_arr.names().clone();
    let mut children = Vec::with_capacity(names.len());
    let mut changed = false;
    for name in names.iter() {
        let child = struct_arr
            .unmasked_field_by_name(name.as_ref())
            .map_err(VortexRdfError::Vortex)?;
        let decode = child.dtype().is_int()
            && !child.dtype().is_nullable()
            && vortex_encoded_search::SortedProbe::resolve(child).is_none();
        if !decode {
            children.push(child.clone());
            continue;
        }
        let sorted = column_is_sorted(child);
        let canonical = child
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        if sorted {
            stamp_is_sorted(&canonical);
        }
        children.push(canonical);
        changed = true;
    }
    if !changed {
        return Ok(struct_arr.into_array());
    }
    let len = struct_arr.len();
    Ok(
        StructArray::try_new(names, children, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    )
}

/// Compress a built struct's u32 code columns into probe-supported
/// encodings, in place of the canonical primitives the builders emit —
/// the construction half of the store's compressed-resident form (the
/// adoption half is [`with_searchable_int_children`]).
///
/// The encoding is chosen from bounds the construction already knows —
/// Constant for a single-valued column, RunEnd for a sorted column with few
/// runs, BitPacked at the observed value width otherwise — never by a
/// sampling compressor: an external selector is free to pick encodings
/// outside the probe-supported set, which would put the column back on the
/// per-call generic-kernel fallback this crate's fast paths exist to avoid.
/// Sortedness stamps carry across; non-u32 and nullable children pass
/// through untouched.
///
/// `payload_lazy` wraps each compressed column in a `vortex.shared` lazy
/// wrapper: the match fast paths probe the compressed source through the
/// wrapper, while the code-column payload path materializes the canonical
/// primitive once into the wrapper's cache (`shared_u32_primitive`) and is
/// zero-copy on every later call. Pass it for the primary base — the only
/// array the payload path reads; components never serve payloads and skip
/// the wrapper.
pub(crate) fn with_compressed_int_children(rows: ArrayRef, payload_lazy: bool) -> Result<ArrayRef> {
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::{Primitive, SharedArray, StructArray};
    use vortex_array::validity::Validity;

    if rows.dtype().is_nullable() {
        return Ok(rows);
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = rows
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let names = struct_arr.names().clone();
    let mut children = Vec::with_capacity(names.len());
    for name in names.iter() {
        let child = struct_arr
            .unmasked_field_by_name(name.as_ref())
            .map_err(VortexRdfError::Vortex)?;
        let Ok(prim) = child.clone().try_downcast::<Primitive>() else {
            children.push(child.clone());
            continue;
        };
        if prim.ptype() != vortex_array::dtype::PType::U32 || child.dtype().is_nullable() {
            children.push(child.clone());
            continue;
        }
        let sorted = column_is_sorted(child);
        let mut encoded = compress_u32_column(&prim, sorted, &mut ctx)?;
        if payload_lazy && !encoded.is::<Primitive>() {
            encoded = SharedArray::new(encoded).into_array();
        }
        if sorted {
            stamp_is_sorted(&encoded);
        }
        children.push(encoded);
    }
    let len = struct_arr.len();
    Ok(
        StructArray::try_new(names, children, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    )
}

/// A base child as a canonical non-nullable u32 primitive, zero-copy where
/// one exists: a canonical column directly, a `vortex.shared` wrapper via its
/// one-way cache — the first call decodes the compressed source into the
/// cache, every later call is a refcount bump shared by all views over the
/// base. `None` for any other encoding (callers fall back to the gather
/// pipeline).
///
/// Decoding into the cache is one pass over the whole column, so callers that
/// only read a few rows take [`cached_u32_primitive`] instead.
pub(crate) fn shared_u32_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::Canonical;
    use vortex_array::arrays::shared::SharedArrayExt as _;
    use vortex_array::arrays::{Primitive, PrimitiveArray, Shared};

    if let Some(prim) = canonical_u32(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    let cached = shared
        .get_or_compute(|source| {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            source
                .clone()
                .execute::<PrimitiveArray>(&mut ctx)
                .map(Canonical::Primitive)
        })
        .ok()?;
    let prim = cached.try_downcast::<Primitive>().ok()?;
    (prim.ptype() == vortex_array::dtype::PType::U32).then_some(prim)
}

/// The non-decoding half of [`shared_u32_primitive`]: a base child's canonical
/// non-nullable u32 primitive when one already exists — the column itself, or
/// a `vortex.shared` wrapper whose one-way cache some earlier read already
/// filled. `None` when producing one would mean decoding the compressed
/// source, so a caller reading a handful of rows can prefer per-row point
/// reads over a whole-column pass.
pub(crate) fn cached_u32_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Shared;
    use vortex_array::arrays::shared::SharedArrayExt as _;

    if let Some(prim) = canonical_u32(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    canonical_u32(shared.current_array_ref())
}

/// A non-nullable canonical u32 primitive, or `None` for anything else — the
/// shape both `*_u32_primitive` accessors hand back.
fn canonical_u32(arr: &ArrayRef) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Primitive;

    if !arr.dtype().is_unsigned_int() || arr.dtype().is_nullable() {
        return None;
    }
    let prim = arr.clone().try_downcast::<Primitive>().ok()?;
    (prim.ptype() == vortex_array::dtype::PType::U32).then_some(prim)
}

/// One column's encoding choice; see [`with_compressed_int_children`].
fn compress_u32_column(
    prim: &vortex_array::arrays::PrimitiveArray,
    sorted: bool,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<ArrayRef> {
    use vortex::encodings::fastlanes::BitPacked;
    use vortex::encodings::runend::RunEnd;
    use vortex_array::arrays::ConstantArray;

    let values = prim.as_slice::<u32>();
    if values.is_empty() {
        return Ok(prim.clone().into_array());
    }
    let mut max = values[0];
    let mut min = values[0];
    let mut runs = 1usize;
    let mut prev = values[0];
    for &v in &values[1..] {
        if v > max {
            max = v;
        }
        if v < min {
            min = v;
        }
        if v != prev {
            runs += 1;
            prev = v;
        }
    }
    if min == max {
        return Ok(ConstantArray::new(min, values.len()).into_array());
    }
    if sorted && runs * 4 <= values.len() {
        let re = RunEnd::encode(prim.clone().into_array(), ctx).map_err(VortexRdfError::Vortex)?;
        return Ok(re.into_array());
    }
    let bit_width = (u32::BITS - max.leading_zeros()).max(1) as u8;
    if bit_width >= 32 {
        return Ok(prim.clone().into_array());
    }
    let packed = BitPacked::encode(&prim.clone().into_array(), bit_width, ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(packed.into_array())
}

/// Convert a boolean ArrayRef into a `vortex_mask::Mask` for use with `ArrayRef::filter`.
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    // Canonicalize to a concrete boolean array, then reinterpret its packed
    // bit buffer directly as a Mask (no per-bit conversion loop).
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let bool_arr = arr
        .execute::<BoolArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(Mask::from_buffer(bool_arr.into_bit_buffer()))
}

/// Borrow the bytes of a UTF-8 string column value as `&str` without copying.
///
/// **Trusted-input decode path**, the same argument as
/// [`parse_named_node`](crate::common::terms::parse_named_node):
/// every caller reads a column whose dtype is `Utf8`, and vortex validates that
/// invariant when the array is constructed — `VarBinViewData::validate` runs a
/// `from_utf8` over every view on IPC decode, and the file reader validates on
/// its own construction path. Re-validating here walks each term's bytes a
/// second time, which profiling showed at ~7% of every many-row decode
/// (`core::str::converts::from_utf8` under `decode_chunk`).
///
/// The check is kept as a `debug_assert`, so the test suite (which runs debug)
/// still fails loudly if a non-UTF-8 column ever reaches this, while release
/// builds skip the second walk. The `Result` is retained so the decode call
/// sites — which `?` on genuinely fallible neighbours — stay uniform.
#[inline]
pub(crate) fn buf_as_str(buf: &[u8]) -> Result<&str> {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "string column value is not valid UTF-8, but its dtype claims Utf8"
    );
    // SAFETY: `buf` is the bytes of a value in a `Utf8`-dtyped vortex column,
    // which vortex validates as UTF-8 when the array is constructed (see above).
    Ok(unsafe { std::str::from_utf8_unchecked(buf) })
}
