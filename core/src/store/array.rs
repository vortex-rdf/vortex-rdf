//! Low-level helpers over Vortex arrays: string-array construction, canonical
//! string-column access, sortedness statistics, chunk and struct assembly
//! and slicing, sorted-column binary search, and mask conversion.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::schema;

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::arrays::{BoolArray, ChunkedArray, Struct, StructArray, VarBinViewArray};
use vortex_array::dtype::{DType, FieldName};
use vortex_array::expr::stats::{Precision, Stat, StatsProvider};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Executable, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

/// Row access into a canonical `VarBinView` string column: the views slice is
/// resolved once, and each row's bytes are an inline read or a slice of the
/// referenced data buffer, so a loop over the column allocates and refcounts
/// nothing per row.
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

    /// Row `i` as a `&str` borrowed from the column buffers, unchecked
    /// ([`trusted_str`]); a decoder's oxrdf constructors make the single owned
    /// copy.
    #[inline]
    pub(crate) fn str_at(&self, i: usize) -> &'a str {
        trusted_str(self.bytes_at(i))
    }
}

/// A non-nullable `VarBinView<Utf8>` array of `values`, each copied once,
/// straight into the array's buffer.
pub(crate) fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on an array.
///
/// On a base's `s` column the stamp asserts more than that column's order: it
/// is the witness that the whole base is in global `(s, p, o, g)` order (the
/// order every writer of this crate produces), and `match_pattern` trusts it
/// to binary-search each bound role inside the run the previous one bounded.
/// Only call it on rows sorted that way by construction: a false stamp
/// corrupts query results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    arr.statistics()
        .set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted: order is never assumed, only trusted when
/// recorded. On a base's `s` column, `true` licenses the prefix probe over
/// the whole `(s, p, o, g)` order.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Whether a quad struct's `s` column carries the globally sorted stamp: the
/// provenance a write records in the root's metadata and a reader gates its
/// subject binary search on. Every producer that stamps yields a canonical
/// `StructArray`, so anything that is not one reads as unsorted.
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
        _ => Ok(ChunkedArray::try_new(chunks, dtype)?.into_array()),
    }
}

/// `rows` cut into slices of at most `batch_rows` (> 0) rows, in row order,
/// each cut when the iterator reaches it: the array itself when it fits, no
/// slice for an empty array.
pub(crate) fn rechunk(
    rows: ArrayRef,
    batch_rows: usize,
) -> impl Iterator<Item = Result<ArrayRef>> + Send + 'static {
    let len = rows.len();
    let fits = len <= batch_rows;
    (0..len).step_by(batch_rows).map(move |start| {
        if fits {
            return Ok(rows.clone());
        }
        #[cfg(test)]
        rechunk_hooks::note()?;
        rows.slice(start..start.saturating_add(batch_rows).min(len))
            .map_err(VortexRdfError::Vortex)
    })
}

/// A non-nullable struct of `columns` under `names`, `columns[i]` named
/// `names[i]`, every column `len` rows long: the one construction of the
/// crate's named struct chunks, from a layout's primary columns to an index
/// child's rows and a point read's.
pub(crate) fn named_struct(
    names: &[&str],
    columns: Vec<ArrayRef>,
    len: usize,
) -> Result<StructArray> {
    let names: Vec<FieldName> = names.iter().map(|&n| FieldName::from(n)).collect();
    StructArray::try_new(names.into(), columns, len, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
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
        .unmasked_field_by_name(name)?
        .clone()
        .execute::<T>(ctx)
        .map_err(VortexRdfError::Vortex)
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (empty, at the insertion point, when the value is absent). Only
/// meaningful on columns [`column_is_sorted`] reports as sorted.
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
    let lo = arr.search_sorted(probe, SearchSortedSide::Left)?;
    let hi = arr.search_sorted(probe, SearchSortedSide::Right)?;
    Ok((index_of(lo), index_of(hi)))
}

/// A boolean array as a [`Mask`] for `ArrayRef::filter`: canonicalized, then
/// its packed bit buffer taken as the mask (no per-bit loop).
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let bool_arr = arr.execute::<BoolArray>(&mut ctx)?;
    Ok(Mask::from_buffer(bool_arr.into_bit_buffer()))
}

/// The bytes of a string column value as `&str`, borrowed and unchecked.
///
/// **Trusted-input decode path**, as
/// [`Trust::Stored`](crate::common::terms::Trust::Stored): `buf` must be a
/// value of a `Utf8` column of a store this crate wrote or the caller trusts.
/// Vortex does not validate every path to such bytes (an FSST chunk read from
/// a file is canonicalized unchecked), so a corrupt or crafted file is outside
/// this contract. Release builds skip the check, which would walk every
/// decoded term's bytes; debug builds keep it as a `debug_assert`, so the test
/// suite fails loudly if non-UTF-8 bytes ever reach it.
#[inline]
pub(crate) fn trusted_str(buf: &[u8]) -> &str {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "string column value is not valid UTF-8, but its dtype claims Utf8"
    );
    // SAFETY: the caller's precondition above: `buf` is a value of a `Utf8`
    // column of a store this crate wrote or the caller trusts.
    unsafe { std::str::from_utf8_unchecked(buf) }
}

/// Test hook: how many slices this thread's [`rechunk`] iterators cut, and a
/// guard that fails one of the next cuts.
#[cfg(test)]
pub(crate) mod rechunk_hooks {
    use std::cell::Cell;

    use crate::error::{Result, VortexRdfError};

    thread_local! {
        static CUTS: Cell<usize> = const { Cell::new(0) };
        static FAILING: Cell<Option<usize>> = const { Cell::new(None) };
    }

    /// Record one cut: an error, the cut's, when a [`FailingCut`] names it.
    pub(super) fn note() -> Result<()> {
        let cut = CUTS.with(|c| c.replace(c.get() + 1));
        if FAILING.with(Cell::get) == Some(cut) {
            return Err(VortexRdfError::Vortex(vortex_error::vortex_err!(
                "cut {cut} failed (test hook)"
            )));
        }
        Ok(())
    }

    /// The slices cut on this thread so far.
    pub(crate) fn cuts() -> usize {
        CUTS.with(Cell::get)
    }

    /// While alive, the cut this thread's `rechunk` iterators make `n` cuts
    /// from now (`0`: the next) fails. Dropping it restores the failure it
    /// replaced.
    pub(crate) struct FailingCut(Option<usize>);

    impl FailingCut {
        /// Fail the cut `n` cuts from now.
        pub(crate) fn after(n: usize) -> Self {
            let cut = cuts() + n;
            FailingCut(FAILING.with(|c| c.replace(Some(cut))))
        }
    }

    impl Drop for FailingCut {
        fn drop(&mut self) {
            FAILING.with(|c| c.set(self.0));
        }
    }
}
