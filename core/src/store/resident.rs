//! The compressed-resident form of a store's integer children: encoding a
//! built base's u32 code columns into probe-supported encodings, keeping an
//! adopted base's encodings wherever a probe binds them, and the canonical
//! u32 accessors that read a child back through the `vortex.shared` cache.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{column_is_sorted, stamp_is_sorted};

use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

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
            && vortex_rdf_encoded_search::SortedProbe::resolve(child).is_none();
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
        let sorted = column_is_sorted(child);
        let Some(mut encoded) = compress_u32_child(child, sorted, &mut ctx)? else {
            children.push(child.clone());
            continue;
        };
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
