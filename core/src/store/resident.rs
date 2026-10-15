//! The encoding of a store's integer children and the canonical accessors
//! that read them back.
//!
//! The integer-children helpers are the two ends of the compressed-resident
//! form: [`with_compressed_int_children`] encodes a built store's code
//! columns into probe-supported encodings, [`with_searchable_int_children`]
//! keeps an adopted store's encodings wherever a probe binds them, and
//! [`shared_code_primitive`] / [`cached_code_primitive`] hand back the
//! canonical code primitive a slice-bound read path needs — decoding into the
//! shared wrapper's cache, or only if some earlier read already did.

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{column_is_sorted, stamp_is_sorted};
use crate::store::schema;
use crate::store::schema::CODE_PTYPE;

use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};

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
///
/// [`search_sorted_bounds`]: crate::store::array::search_sorted_bounds
pub(crate) fn with_searchable_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::PrimitiveArray;

    map_struct_children(rows, |child, ctx| {
        let decode = child.dtype().is_int()
            && !child.dtype().is_nullable()
            && vortex_rdf_encoded_search::SortedProbe::resolve(child).is_none();
        if !decode {
            return Ok(None);
        }
        let canonical = child.clone().execute::<PrimitiveArray>(ctx)?.into_array();
        if column_is_sorted(child) {
            stamp_is_sorted(&canonical);
        }
        Ok(Some(canonical))
    })
}

/// Compress a built struct's integer columns — the `u64` code columns
/// ([`TermCode`](schema::TermCode)) and row-id columns
/// ([`RowId`](schema::RowId)) — into probe-supported encodings, in place of
/// the canonical primitives the builders emit — the construction half of the
/// store's compressed-resident form (the adoption half is
/// [`with_searchable_int_children`]).
///
/// Every encoding chosen here is one an encoded search probe resolves, so
/// the match fast paths keep binding the column; the choice is made from
/// bounds the construction already knows (Constant for single-valued, RunEnd
/// for sorted with few runs, BitPacked at the observed width).
/// Sortedness stamps carry across; other widths and nullable children pass
/// through untouched. A chunked column is compressed chunk by chunk (see
/// [`compress_int_child`]) — the builders assemble anything over
/// `DEFAULT_CHUNK_ROWS` rows into a `ChunkedArray`, so without that every
/// build past one chunk would keep the canonical form.
///
/// `payload_lazy` wraps each compressed column in a `vortex.shared` lazy
/// wrapper: the match fast paths probe the compressed source through the
/// wrapper, while the code-column payload path materializes the canonical
/// primitive once into the wrapper's cache (`shared_code_primitive`) and is
/// zero-copy on every later call. Pass it for the primary base — the only
/// array the payload path reads; components never serve payloads and skip
/// the wrapper.
pub(crate) fn with_compressed_int_children(rows: ArrayRef, payload_lazy: bool) -> Result<ArrayRef> {
    use vortex_array::arrays::{Primitive, SharedArray};

    map_struct_children(rows, |child, ctx| {
        let sorted = column_is_sorted(child);
        let Some(mut encoded) = compress_int_child(child, sorted, ctx)? else {
            return Ok(None);
        };
        if payload_lazy && !encoded.is::<Primitive>() {
            encoded = SharedArray::new(encoded).into_array();
        }
        if sorted {
            stamp_is_sorted(&encoded);
        }
        Ok(Some(encoded))
    })
}

/// `rows` executed to a struct, each child replaced by the array `f` returns
/// for it or kept where `f` returns `None`; the executed struct itself when
/// `f` keeps every child. A nullable struct passes through untouched.
fn map_struct_children(
    rows: ArrayRef,
    mut f: impl FnMut(&ArrayRef, &mut ExecutionCtx) -> Result<Option<ArrayRef>>,
) -> Result<ArrayRef> {
    if rows.dtype().is_nullable() {
        return Ok(rows);
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = rows.execute::<StructArray>(&mut ctx)?;
    let names = struct_arr.names().clone();
    let mut children = Vec::with_capacity(names.len());
    let mut changed = false;
    for name in names.iter() {
        let child = struct_arr.unmasked_field_by_name(name.as_ref())?;
        match f(child, &mut ctx)? {
            Some(replacement) => {
                children.push(replacement);
                changed = true;
            }
            None => children.push(child.clone()),
        }
    }
    if !changed {
        return Ok(struct_arr.into_array());
    }
    let len = struct_arr.len();
    Ok(StructArray::try_new(names, children, len, Validity::NonNullable)?.into_array())
}

/// A base child as a canonical non-nullable code primitive
/// ([`TermCode`](schema::TermCode)), zero-copy where one exists: a canonical
/// column directly, a `vortex.shared` wrapper via its
/// one-way cache — the first call decodes the compressed source into the
/// cache, every later call is a refcount bump shared by all views over the
/// base. `None` for any other encoding (callers fall back to the gather
/// pipeline).
///
/// Decoding into the cache is one pass over the whole column, so callers that
/// only read a few rows take [`cached_code_primitive`] instead.
pub(crate) fn shared_code_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::Canonical;
    use vortex_array::arrays::shared::SharedArrayExt as _;
    use vortex_array::arrays::{Primitive, PrimitiveArray, Shared};

    if let Some(prim) = canonical_code(child) {
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
    (prim.ptype() == CODE_PTYPE).then_some(prim)
}

/// The non-decoding half of [`shared_code_primitive`]: a base child's
/// canonical non-nullable code primitive when one already exists — the column itself, or
/// a `vortex.shared` wrapper whose one-way cache some earlier read already
/// filled. `None` when producing one would mean decoding the compressed
/// source, so a caller reading a handful of rows can prefer per-row point
/// reads over a whole-column pass.
pub(crate) fn cached_code_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Shared;
    use vortex_array::arrays::shared::SharedArrayExt as _;

    if let Some(prim) = canonical_code(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    canonical_code(shared.current_array_ref())
}

/// A non-nullable canonical code primitive ([`TermCode`](schema::TermCode)),
/// or `None` for anything else — the shape both `*_code_primitive` accessors
/// hand back.
fn canonical_code(arr: &ArrayRef) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Primitive;

    if !arr.dtype().is_unsigned_int() || arr.dtype().is_nullable() {
        return None;
    }
    let prim = arr.clone().try_downcast::<Primitive>().ok()?;
    (prim.ptype() == CODE_PTYPE).then_some(prim)
}

/// One child column's compressed form, or `None` for a column this helper
/// leaves alone (not a `u64` code or row-id column, nullable, or an encoding
/// that is already not a canonical primitive).
///
/// A chunked column is compressed chunk by chunk and reassembled, which is
/// what makes the compressed-resident form reach builds above
/// `DEFAULT_CHUNK_ROWS` rows at all: the builders assemble anything larger
/// into a `ChunkedArray`, and a downcast straight to `Primitive` sees only
/// the wrapper. Per-chunk is also the natural granularity — the bounds pass
/// that picks the encoding is per-chunk regardless, and a chunk of a globally
/// sorted column is itself sorted, so the RunEnd choice stays valid.
fn compress_int_child(
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
            match compress_int_child(&chunk, sorted, ctx)? {
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
            ChunkedArray::try_new(out, child.dtype().clone())?.into_array(),
        ));
    }
    let Ok(prim) = child.clone().try_downcast::<Primitive>() else {
        return Ok(None);
    };
    // Neither a code nor a row id; both are u64 wide.
    if prim.ptype() != CODE_PTYPE && prim.ptype() != schema::ROW_ID_PTYPE {
        return Ok(None);
    }
    Ok(Some(compress_int_column(
        &prim,
        prim.as_slice::<u64>(),
        sorted,
        ctx,
    )?))
}

/// One column's encoding choice; see [`with_compressed_int_children`].
/// `values` is `prim`'s slice.
fn compress_int_column(
    prim: &vortex_array::arrays::PrimitiveArray,
    values: &[u64],
    sorted: bool,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<ArrayRef> {
    use vortex::encodings::fastlanes::BitPacked;
    use vortex::encodings::runend::RunEnd;
    use vortex_array::arrays::ConstantArray;

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
        let re = RunEnd::encode(prim.clone().into_array(), ctx)?;
        return Ok(re.into_array());
    }
    // At most 64, so the narrowing to u8 is exact.
    let bit_width = (u64::BITS - max.leading_zeros()).max(1) as u8;
    if u32::from(bit_width) >= u64::BITS {
        return Ok(prim.clone().into_array());
    }
    let packed = BitPacked::encode(&prim.clone().into_array(), bit_width, ctx)?;
    Ok(packed.into_array())
}
