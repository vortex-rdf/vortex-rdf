//! The compressed-resident form of a store's integer children: a built
//! base's u32 code columns encoded into probe-supported encodings, an
//! adopted base's encodings kept wherever a probe binds them, and the
//! canonical u32 accessors that read a child back through its
//! `vortex.shared` cache.

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{column_is_sorted, stamp_is_sorted};

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{Primitive, PrimitiveArray, SharedArray, StructArray};
use vortex_array::dtype::PType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};

/// Decode the non-nullable integer children of an adopted base that no
/// encoded-search probe resolves to canonical primitives; the rest keep
/// their encoding, `IsSorted` stamps carry across, a nullable struct passes
/// through.
pub(crate) fn with_searchable_int_children(rows: ArrayRef) -> Result<ArrayRef> {
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

/// Compress a built struct's u32 code columns into probe-supported encodings
/// (Constant for one value, RunEnd for a sorted column with few runs,
/// BitPacked at the observed width), chunk by chunk for a chunked column;
/// `IsSorted` stamps carry across, other children and a nullable struct pass
/// through. `payload_lazy` wraps each compressed column in a `vortex.shared`
/// wrapper whose cache [`shared_u32_primitive`] fills on the first payload
/// read; pass it for the primary base, never for components.
pub(crate) fn with_compressed_int_children(rows: ArrayRef, payload_lazy: bool) -> Result<ArrayRef> {
    map_struct_children(rows, |child, ctx| {
        let sorted = column_is_sorted(child);
        let Some(mut encoded) = compress_u32_child(child, sorted, ctx)? else {
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

/// `rows` executed to a struct with every child `f` answers `Some` for
/// replaced; a nullable struct passes through untouched.
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
    for name in names.iter() {
        let child = struct_arr.unmasked_field_by_name(name.as_ref())?;
        children.push(f(child, &mut ctx)?.unwrap_or_else(|| child.clone()));
    }
    let len = struct_arr.len();
    Ok(StructArray::try_new(names, children, len, Validity::NonNullable)?.into_array())
}

/// A base child as a canonical non-nullable u32 primitive: the column itself
/// when canonical, else a `vortex.shared` wrapper's cache, filled by the
/// first call (one pass over the column) and shared by every view over the
/// base. `None` for any other encoding.
pub(crate) fn shared_u32_primitive(child: &ArrayRef) -> Option<PrimitiveArray> {
    use vortex_array::Canonical;
    use vortex_array::arrays::Shared;
    use vortex_array::arrays::shared::SharedArrayExt as _;

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
    (prim.ptype() == PType::U32).then_some(prim)
}

/// [`shared_u32_primitive`] without decoding: the canonical u32 primitive
/// when the column is one or its `vortex.shared` cache is already filled,
/// else `None`.
pub(crate) fn cached_u32_primitive(child: &ArrayRef) -> Option<PrimitiveArray> {
    use vortex_array::arrays::Shared;
    use vortex_array::arrays::shared::SharedArrayExt as _;

    if let Some(prim) = canonical_u32(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    canonical_u32(shared.current_array_ref())
}

/// `arr` as a non-nullable canonical u32 primitive, `None` for anything
/// else.
fn canonical_u32(arr: &ArrayRef) -> Option<PrimitiveArray> {
    if !arr.dtype().is_unsigned_int() || arr.dtype().is_nullable() {
        return None;
    }
    let prim = arr.clone().try_downcast::<Primitive>().ok()?;
    (prim.ptype() == PType::U32).then_some(prim)
}

/// One child's compressed form, `None` for a child left alone (non-u32,
/// nullable, or not a canonical primitive). A chunked column is compressed
/// chunk by chunk, each chunk stamped sorted when `sorted`.
fn compress_u32_child(
    child: &ArrayRef,
    sorted: bool,
    ctx: &mut ExecutionCtx,
) -> Result<Option<ArrayRef>> {
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    use vortex_array::arrays::{Chunked, ChunkedArray};

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
            ChunkedArray::try_new(out, child.dtype().clone())?.into_array(),
        ));
    }
    let Ok(prim) = child.clone().try_downcast::<Primitive>() else {
        return Ok(None);
    };
    if prim.ptype() != PType::U32 {
        return Ok(None);
    }
    Ok(Some(compress_u32_column(&prim, sorted, ctx)?))
}

/// One u32 column's encoding: Constant when single-valued, RunEnd when
/// `sorted` with at most a quarter as many runs as values, BitPacked at the
/// observed width below 32 bits, else the primitive itself.
fn compress_u32_column(
    prim: &PrimitiveArray,
    sorted: bool,
    ctx: &mut ExecutionCtx,
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
        return Ok(RunEnd::encode(prim.clone().into_array(), ctx)?.into_array());
    }
    let bit_width = (u32::BITS - max.leading_zeros()).max(1) as u8;
    if bit_width >= 32 {
        return Ok(prim.clone().into_array());
    }
    Ok(BitPacked::encode(&prim.clone().into_array(), bit_width, ctx)?.into_array())
}
