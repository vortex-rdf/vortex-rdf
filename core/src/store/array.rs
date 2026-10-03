//! Helpers over Vortex arrays: string-column construction and zero-copy row
//! access, the sortedness stamps, struct and chunk assembly, sorted-column
//! binary search and mask conversion.

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::schema;

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, ChunkedArray, Struct, StructArray, VarBinViewArray};
use vortex_array::dtype::DType;
use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
use vortex_array::{ArrayRef, Executable, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Zero-copy row access into a canonical `VarBinView` string column: the
/// views slice and the data buffers are resolved once, and each row's bytes
/// are an inline read or a slice of the referenced buffer.
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

    /// Row `i` as a `&str` borrowed from the column buffers.
    #[inline]
    pub(crate) fn str_at(&self, i: usize) -> Result<&'a str> {
        buf_as_str(self.bytes_at(i))
    }
}

/// A non-nullable `VarBinView<Utf8>` array of `values`, each copied once
/// into the array's buffer.
pub(crate) fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on `arr`. On a base's `s` column the
/// stamp asserts global `(s, p, o, g)` order, which `match_pattern` trusts to
/// binary-search every bound prefix role; stamp only rows sorted that way by
/// construction, a false stamp corrupts matches.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// The `IsSorted` statistic of `arr`; absent counts as unsorted.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Whether a quad struct's `s` column carries the sorted stamp; anything
/// that is not a canonical `StructArray` reads as unsorted.
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

/// `rows` canonicalized to a struct with its `s` column stamped sorted when
/// `sorted`; a pass-through otherwise.
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

/// `chunks` as one array of `dtype`: the chunk itself for exactly one, a
/// `ChunkedArray` otherwise (empty for none).
pub(crate) fn chunked_or_single(mut chunks: Vec<ArrayRef>, dtype: DType) -> Result<ArrayRef> {
    match chunks.len() {
        1 => Ok(chunks.pop().expect("length checked above")),
        _ => Ok(ChunkedArray::try_new(chunks, dtype)?.into_array()),
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
        .map(|start| Ok(rows.slice(start..(start + batch_rows).min(len))?))
        .collect()
}

/// `arr` as a `StructArray`: a downcast when it is one, else an execution to
/// the canonical struct.
pub(crate) fn into_struct_array(arr: ArrayRef) -> Result<StructArray> {
    match arr.try_downcast::<Struct>() {
        Ok(struct_arr) => Ok(struct_arr),
        Err(other) => {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            Ok(other.execute::<StructArray>(&mut ctx)?)
        }
    }
}

/// The field `name` of `struct_arr` executed to `T`.
pub(crate) fn field_as<T: Executable>(
    struct_arr: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Result<T> {
    Ok(struct_arr
        .unmasked_field_by_name(name)?
        .clone()
        .execute::<T>(ctx)?)
}

/// The `[lo, hi)` run of rows equal to `probe` in a sorted column (`lo ==
/// hi` when absent); only meaningful on columns [`column_is_sorted`] reports
/// sorted. A non-nullable unsigned column is probed in its encoding when an
/// encoded-search probe resolves it.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

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
    let lo = arr.search_sorted(probe, SearchSortedSide::Left)?;
    let hi = arr.search_sorted(probe, SearchSortedSide::Right)?;
    Ok((index_of(lo), index_of(hi)))
}

/// A boolean array as a [`Mask`] over its bit buffer.
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    Ok(Mask::from_buffer(
        arr.execute::<BoolArray>(&mut ctx)?.into_bit_buffer(),
    ))
}

/// The bytes of a `Utf8`-dtyped column value as `&str` without
/// re-validation: vortex validates the column's UTF-8 when the array is
/// constructed, so the check here is a `debug_assert`. The `Result` keeps
/// the decode call sites uniform.
#[inline]
pub(crate) fn buf_as_str(buf: &[u8]) -> Result<&str> {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "string column value is not valid UTF-8, but its dtype claims Utf8"
    );
    // SAFETY: `buf` is a value of a `Utf8`-dtyped vortex column, validated
    // as UTF-8 when the array was constructed.
    Ok(unsafe { std::str::from_utf8_unchecked(buf) })
}
