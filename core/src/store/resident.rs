//! The compressed-resident form of a store's integer children:
//! [`with_compressed_int_children`] encodes a build's code and row-id columns
//! into encodings the probes read in place, [`with_searchable_int_children`]
//! keeps an adopted store's encodings wherever a probe binds them, and
//! [`shared_code_primitive`] / [`cached_code_primitive`] hand back the
//! canonical code primitive a slice-bound read path needs (decoding into the
//! shared wrapper's cache, or only when an earlier read already did).

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{column_is_sorted, stamp_is_sorted};
use crate::store::schema;
use crate::store::schema::CODE_PTYPE;
use crate::store::zones::{ZoneBuilder, ZoneTable};

use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::FieldName;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};

/// Keep a base struct's integer children (the Dictionary layout's term codes,
/// TypedObject's kind column) in their encoding wherever an encoded-search
/// probe binds them, and decode the rest to canonical primitives once,
/// keeping each child's `IsSorted` stamp.
///
/// The match fast paths ([`search_sorted_bounds`]' probes, the typed residual
/// loops) bind canonical primitives and every encoding a probe resolves; any
/// other encoding (e.g. dictionary encoding) would pay the generic per-scalar
/// kernel on every match. String children keep their encoding: their
/// canonical `VarBinView` costs real memory, and the mask scan handles them at
/// selection cost. Serialization re-compresses every child, so the wire format
/// is unaffected. A nullable struct (never a base) passes through untouched.
///
/// [`search_sorted_bounds`]: crate::store::array::search_sorted_bounds
#[cfg(feature = "file-io")]
pub(crate) fn with_searchable_int_children(rows: ArrayRef) -> Result<ArrayRef> {
    use vortex_array::arrays::PrimitiveArray;

    map_struct_children(rows, |_, child, ctx| {
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

/// Compress a built struct's `u64` columns (the codes,
/// [`TermCode`](schema::TermCode), and the row ids, [`RowId`](schema::RowId))
/// into encodings an encoded-search probe resolves, in place of the canonical
/// primitives the builders emit, so the match fast paths keep binding them:
/// the construction half of the compressed-resident form
/// ([`with_searchable_int_children`] is the adoption half). The choice comes
/// from bounds the build already knows (Constant for one value, RunEnd for a
/// sorted column with few runs, BitPacked at the observed width); sortedness
/// stamps carry across, and other widths and nullable children pass through
/// untouched. A chunked column is compressed chunk by chunk
/// ([`compress_int_child`]).
///
/// The pass that picks a column's encoding reads all its codes, and takes
/// the column's zone bounds on the way ([`ZoneBuilder`]): they come back
/// beside the struct ([`ZoneTable`]), for the primary base, whose residual
/// filter skips zones by them.
///
/// `payload_lazy` wraps each compressed column in a `vortex.shared` wrapper:
/// the match fast paths probe the compressed source through it, while the
/// code-column payload path decodes the canonical primitive once into its
/// cache (`shared_code_primitive`) and is zero-copy on every later call. Pass
/// it for the primary base, the only array the payload path reads.
pub(crate) fn with_compressed_int_children(
    rows: ArrayRef,
    payload_lazy: bool,
) -> Result<(ArrayRef, ZoneTable)> {
    use vortex_array::arrays::Primitive;
    use vortex_layout::arrays::shared::Shared;

    let mut zones = ZoneTable::default();
    let rows = map_struct_children(rows, |name, child, ctx| {
        let sorted = column_is_sorted(child);
        let mut bounds = ZoneBuilder::new();
        let Some(mut encoded) = compress_int_child(child, sorted, &mut bounds, ctx)? else {
            return Ok(None);
        };
        if let Some(column) = bounds.finish(child.len()) {
            zones.insert(name.clone(), column);
        }
        if payload_lazy && !encoded.is::<Primitive>() {
            encoded = Shared::new(encoded).into_array();
        }
        if sorted {
            stamp_is_sorted(&encoded);
        }
        Ok(Some(encoded))
    })?;
    Ok((rows, zones))
}

/// `rows` executed to a struct, each child replaced by the array `f` returns
/// for it or kept where `f` returns `None`; the executed struct itself when
/// `f` keeps every child. A nullable struct passes through untouched.
fn map_struct_children(
    rows: ArrayRef,
    mut f: impl FnMut(&FieldName, &ArrayRef, &mut ExecutionCtx) -> Result<Option<ArrayRef>>,
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
        match f(name, child, &mut ctx)? {
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
/// column directly, a `vortex.shared` wrapper through its one-way cache (the
/// first call decodes the compressed source into it, every later call is a
/// refcount bump shared by all views over the base). `None` for any other
/// encoding (callers fall back to the gather pipeline). Filling the cache is
/// one pass over the whole column, so a caller that reads a few rows takes
/// [`cached_code_primitive`] instead.
pub(crate) fn shared_code_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::Canonical;
    use vortex_array::arrays::{Primitive, PrimitiveArray};
    use vortex_layout::arrays::shared::{Shared, SharedArrayExt as _};

    if let Some(prim) = canonical_code(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    let cached = shared
        .get_or_compute(|source| {
            use vortex_rdf_encoded_search::counters::{SHARED_DECODED, note};
            note(SHARED_DECODED, source.len());
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
/// canonical code primitive when one already exists (the column itself, or a
/// `vortex.shared` wrapper whose cache an earlier read filled). `None` when
/// producing one would decode the compressed source, so a caller reading a
/// handful of rows can take per-row point reads over a whole-column pass.
pub(crate) fn cached_code_primitive(
    child: &ArrayRef,
) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_layout::arrays::shared::{Shared, SharedArrayExt as _};

    if let Some(prim) = canonical_code(child) {
        return Some(prim);
    }
    let shared = child.as_opt::<Shared>()?;
    canonical_code(shared.current_array_ref())
}

/// A non-nullable canonical code primitive ([`TermCode`](schema::TermCode)),
/// or `None` for anything else: the shape both `*_code_primitive` accessors
/// hand back.
fn canonical_code(arr: &ArrayRef) -> Option<vortex_array::arrays::PrimitiveArray> {
    use vortex_array::arrays::Primitive;

    if !arr.dtype().is_unsigned_int() || arr.dtype().is_nullable() {
        return None;
    }
    let prim = arr.clone().try_downcast::<Primitive>().ok()?;
    (prim.ptype() == CODE_PTYPE).then_some(prim)
}

/// One child column's compressed form, or `None` for a column left alone
/// (not a `u64` code or row-id column, nullable, or already not a canonical
/// primitive). A chunked column (any build past `DEFAULT_CHUNK_ROWS` rows) is
/// compressed chunk by chunk and reassembled; a chunk of a globally sorted
/// column is itself sorted, so the RunEnd choice stays valid per chunk.
/// `zones` sees the codes of every chunk compressed, in row order.
fn compress_int_child(
    child: &ArrayRef,
    sorted: bool,
    zones: &mut ZoneBuilder,
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
            match compress_int_child(&chunk, sorted, zones, ctx)? {
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
        zones,
        ctx,
    )?))
}

/// One column's encoding choice; see [`with_compressed_int_children`].
/// `values` is `prim`'s slice; `zones` folds in their bounds zone by zone, in
/// the pass that takes the column's own.
fn compress_int_column(
    prim: &vortex_array::arrays::PrimitiveArray,
    values: &[u64],
    sorted: bool,
    zones: &mut ZoneBuilder,
    ctx: &mut vortex_array::ExecutionCtx,
) -> Result<ArrayRef> {
    use vortex::encodings::fastlanes::BitPacked;
    use vortex::encodings::runend::RunEnd;
    use vortex_array::arrays::ConstantArray;

    if values.is_empty() {
        return Ok(prim.clone().into_array());
    }
    let (mut min, mut max) = (u64::MAX, u64::MIN);
    let mut runs = 1usize;
    let mut prev = values[0];
    let mut rest = values;
    while !rest.is_empty() {
        // Up to the end of the zone being filled.
        let (part, next) = rest.split_at(zones.room().min(rest.len()));
        let (mut lo, mut hi) = (u64::MAX, u64::MIN);
        for &v in part {
            if v > hi {
                hi = v;
            }
            if v < lo {
                lo = v;
            }
            if v != prev {
                runs += 1;
                prev = v;
            }
        }
        zones.push(part.len(), lo, hi);
        min = min.min(lo);
        max = max.max(hi);
        rest = next;
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
