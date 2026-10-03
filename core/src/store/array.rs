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

/// `rows` cut into slices of at most `batch_rows`; an empty array yields no
/// chunk.
pub(crate) fn rechunk(rows: ArrayRef, batch_rows: usize) -> Result<Vec<ArrayRef>> {
    let len = rows.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    if len <= batch_rows {
        return Ok(vec![rows]);
    }
    (0..len)
        .step_by(batch_rows)
        .map(|start| {
            rows.slice(start..(start + batch_rows).min(len))
                .map_err(VortexRdfError::Vortex)
        })
        .collect()
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
