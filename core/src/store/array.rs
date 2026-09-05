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
use crate::store::schema;

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, ChunkedArray, Struct, StructArray, VarBinViewArray};
use vortex_array::dtype::DType;
use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
use vortex_array::{ArrayRef, Executable, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Zero-cost row access into a canonical `VarBinView` string column: the
/// 16-byte views slice and the data buffers are resolved once, and each row's
/// bytes are then an inline read or a plain slice of the referenced buffer.
///
/// Rows are read directly from the views slice and the referenced data
/// buffers, so a loop over the column allocates and refcounts nothing per
/// row.
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

    /// Row `i` as a `&str` view borrowed straight from the column buffers
    /// (zero-copy); a decoder's oxrdf constructors make the single owned
    /// copy.
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
/// On a base's `s` column the stamp asserts more than that column's order: it
/// is the witness that the whole base is in global `(s, p, o, g)` order — the
/// order every writer of this crate produces — and `match_pattern` trusts it
/// to binary-search each bound role inside the run the previous one bounded.
/// Only call it on rows sorted that way by construction: a false stamp
/// corrupts query results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted — order is never assumed, only trusted
/// when explicitly recorded. On a base's `s` column, `true` licenses the
/// prefix probe over the whole `(s, p, o, g)` order.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Whether a quad struct's `s` column carries the globally-sorted stamp —
/// the provenance a write records in the root's metadata and a reader gates
/// its subject binary search on. Every producer that stamps yields a
/// canonical `StructArray`, so anything that is not one reads as unsorted.
pub(crate) fn subject_sorted(rows: &ArrayRef) -> bool {
    rows.clone()
        .try_downcast::<Struct>()
        .ok()
        .and_then(|s| {
            s.unmasked_field_by_name(schema::COL_S)
                .ok()
                .map(column_is_sorted)
        })
        .unwrap_or(false)
}

/// Canonicalize `rows` to a struct and stamp its `s` column sorted when the
/// caller's provenance says the rows are globally `(s, p, o, g)`-sorted;
/// a pass-through when they are not.
pub(crate) fn with_subject_stamp(rows: ArrayRef, sorted: bool) -> Result<ArrayRef> {
    if !sorted {
        return Ok(rows);
    }
    let struct_arr = into_struct_array(rows)?;
    if let Ok(col) = struct_arr.unmasked_field_by_name(schema::COL_S) {
        stamp_is_sorted(col);
    }
    Ok(struct_arr.into_array())
}

/// Assemble per-chunk arrays into one array of `dtype`: an empty
/// `ChunkedArray` for no chunks, the chunk itself for exactly one, a
/// `ChunkedArray` otherwise.
pub(crate) fn chunked_or_single(mut chunks: Vec<ArrayRef>, dtype: DType) -> Result<ArrayRef> {
    match chunks.len() {
        1 => Ok(chunks.pop().expect("length checked above")),
        _ => Ok(ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array()),
    }
}

/// `arr` as a `StructArray`: a plain downcast when it already is one, else
/// an execution to the canonical struct.
pub(crate) fn into_struct_array(arr: ArrayRef) -> Result<StructArray> {
    match arr.try_downcast::<Struct>() {
        Ok(struct_arr) => Ok(struct_arr),
        Err(other) => {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            other
                .execute::<StructArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)
        }
    }
}

/// The named field of `struct_arr` executed to `T` (a canonical array type).
pub(crate) fn field_as<T: Executable>(
    struct_arr: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Result<T> {
    struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<T>(ctx)
        .map_err(VortexRdfError::Vortex)
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (`lo == hi` means the value is absent). Only meaningful on
/// columns [`column_is_sorted`] reports as sorted.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

    // Encoded fast path: probe the column's representation directly, canonical
    // or wire-encoded (`from_bytes` adoption, sliced index runs), without
    // decoding it; declines fall through to the generic kernel.
    if arr.dtype().is_unsigned_int()
        && !arr.dtype().is_nullable()
        && let Ok(needle) = u64::try_from(probe)
        && let Some(encoded) = vortex_rdf_encoded_search::SortedProbe::resolve(arr)
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

/// Decode the non-nullable integer children of a base struct that `decode`
/// selects into canonical primitives, keeping every other child as it is and
/// carrying each child's `IsSorted` stamp across the re-encoding.
///
/// A nullable struct passes through untouched — the base schema is
/// non-nullable, and rebuilding a struct with validity is not this helper's
/// business.
fn decode_int_children(rows: ArrayRef, decode: impl Fn(&ArrayRef) -> bool) -> Result<ArrayRef> {
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
        if !(child.dtype().is_int() && !child.dtype().is_nullable() && decode(child)) {
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

/// Keep an adopted base struct's integer children (the Dictionary layout's
/// term codes, TypedObject's kind column) in the encodings they arrived in
/// wherever the match fast paths can bind them, decoding only the remainder
/// to canonical primitives.
///
/// The per-row match fast paths — [`search_sorted_bounds`]' probes and the
/// typed residual loops — bind canonical primitives and any encoding an
/// encoded search probe resolves, so those children stay encoded at
/// adoption. A child outside the probe's supported set (e.g. dictionary
/// encoding) would pay a per-call fallback through the generic per-scalar
/// kernel on every match; decoding it once here keeps every fast path fast
/// for the store's lifetime. String children keep their encoded form: their
/// canonical `VarBinView` costs real memory, and the mask scan handles them
/// at selection cost. Serialization re-compresses every child through the
/// default write strategy, so the wire format is unaffected.
///
/// Code reads over the encoded children go through the base's live
/// canonical cache ([`LiveCanonical`](crate::store::view::canonical::LiveCanonical)).
pub(crate) fn with_searchable_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    decode_int_children(rows, |child| {
        vortex_rdf_encoded_search::SortedProbe::resolve(child).is_none()
    })
}

/// Hold a built base struct's integer children as flat canonical primitives.
/// The builders assemble anything over `DEFAULT_CHUNK_ROWS` rows into a
/// `ChunkedArray`, and the code-column read path serves a primary column
/// zero-copy only as one contiguous canonical buffer
/// ([`canonical_u32`]); already-canonical children pass through.
pub(crate) fn with_canonical_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::Primitive;
    decode_int_children(rows, |child| !child.is::<Primitive>())
}

/// Compress a built index component's u32 code columns into probe-supported
/// encodings, in place of the canonical primitives the builders emit.
///
/// Every encoding chosen here is one an encoded search probe resolves, so
/// the match fast paths keep binding the column; the choice is made from
/// bounds the construction already knows (Constant for single-valued, RunEnd
/// for sorted with few runs, BitPacked at the observed width).
/// Sortedness stamps carry across; non-u32 and nullable children pass
/// through untouched. A chunked column is compressed chunk by chunk (see
/// [`compress_u32_child`]) — the builders assemble anything over
/// `DEFAULT_CHUNK_ROWS` rows into a `ChunkedArray`, so without that every
/// build past one chunk would keep the canonical form.
///
/// Components are the columns nothing ever reads as a payload: serve plans
/// point-read them through the probes, so the compressed form costs no read
/// path anything. The primary base is held canonical instead
/// ([`with_canonical_int_children`]).
pub(crate) fn with_compressed_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::StructArray;
    use vortex_array::arrays::struct_::StructArrayExt;
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
        let sorted = column_is_sorted(child);
        let Some(encoded) = compress_u32_child(child, sorted, &mut ctx)? else {
            children.push(child.clone());
            continue;
        };
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

/// A base child as a canonical non-nullable u32 primitive when it is one —
/// the form a built base's code columns are held in, which every code read
/// serves zero-copy — and `None` for any other encoding (an adopted base's
/// wire encodings; callers decode through the base's live canonical cache
/// or gather).
pub(crate) fn canonical_u32(arr: &ArrayRef) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Primitive;

    if !arr.dtype().is_unsigned_int() || arr.dtype().is_nullable() {
        return None;
    }
    let prim = arr.clone().try_downcast::<Primitive>().ok()?;
    (prim.ptype() == vortex_array::dtype::PType::U32).then_some(prim)
}

/// One child column's compressed form, or `None` for a column this helper
/// leaves alone (non-u32, nullable, or an encoding that is already not a
/// canonical primitive).
///
/// A chunked column is compressed chunk by chunk and reassembled, which is
/// what makes the compressed-resident form reach builds above
/// `DEFAULT_CHUNK_ROWS` rows at all: the builders assemble anything larger
/// into a `ChunkedArray`, and a downcast straight to `Primitive` sees only
/// the wrapper. Per-chunk is also the natural granularity — the bounds pass
/// that picks the encoding is per-chunk regardless, and a chunk of a globally
/// sorted column is itself sorted, so the RunEnd choice stays valid.
fn compress_u32_child(
    child: &ArrayRef,
    sorted: bool,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<Option<ArrayRef>> {
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    use vortex_array::arrays::{Chunked, ChunkedArray, Primitive};

    if child.dtype().is_nullable() || !child.dtype().is_unsigned_int() {
        return Ok(None);
    }
    if let Some(chunked) = child.as_opt::<Chunked>() {
        let mut out = Vec::with_capacity(chunked.nchunks());
        let mut compressed_any = false;
        for chunk in chunked.chunks() {
            match compress_u32_child(&chunk, sorted, ctx)? {
                Some(encoded) => {
                    if sorted {
                        stamp_is_sorted(&encoded);
                    }
                    out.push(encoded);
                    compressed_any = true;
                }
                None => out.push(chunk),
            }
        }
        if !compressed_any {
            return Ok(None);
        }
        return Ok(Some(
            ChunkedArray::try_new(out, child.dtype().clone())
                .map_err(VortexRdfError::Vortex)?
                .into_array(),
        ));
    }
    let Ok(prim) = child.clone().try_downcast::<Primitive>() else {
        return Ok(None);
    };
    if prim.ptype() != vortex_array::dtype::PType::U32 {
        return Ok(None);
    }
    Ok(Some(compress_u32_column(&prim, sorted, ctx)?))
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
/// its own construction path. Re-validating here would walk each term's bytes
/// a second time on every decoded row.
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
