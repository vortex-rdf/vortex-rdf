//! The zero-copy u64 column ([`U64Column`]) and its kernels: distinct
//! values, value counts, gathers and the equi-join, whose right side can be
//! built once and reused ([`CodeGroups`]). Each runs the core `columns`
//! function it wraps, GIL-released.

use std::os::raw::{c_int, c_void};

use pyo3::exceptions::{PyIndexError, PySystemError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use vortex_buffer::{Alignment, Buffer};
use vortex_rdf_core::columns;

use crate::JoinTooLarge;
use crate::u64s::extract_u64s;

/// One matched term-code column — or a kernel's row indices or counts —
/// exposed to Python zero-copy through the buffer protocol:
/// `memoryview(col).cast("Q")` views the Rust memory directly, one native-
/// endian u64 per value. Codes are u64 ([`TermCode`]), so every code crosses
/// whole, never narrowed. The column is read-only and owns (refcounts) its
/// backing buffer.
///
/// The column kernels ([`distinct`](Self::distinct),
/// [`value_counts`](Self::value_counts), [`take`](Self::take),
/// [`join_count`](Self::join_count), [`join_indices`](Self::join_indices),
/// [`groups`](Self::groups)) run GIL-released and preserve the orders a
/// query layer observes: first-seen order for distinct values, nested-loop
/// order for a join's pairs.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct U64Column {
    pub(crate) codes: Buffer<u64>,
}

impl From<Buffer<u64>> for U64Column {
    /// The column over `codes`, sharing their buffer.
    fn from(codes: Buffer<u64>) -> Self {
        U64Column { codes }
    }
}

#[pymethods]
impl U64Column {
    /// A column holding `values`: another `U64Column` (shared zero-copy), a
    /// buffer of u64 or non-negative i64 items or the raw byte view a column
    /// exports (one bulk copy), or any sequence of ints from 0 to 2**64 - 1.
    /// A buffer of other items raises `ValueError` (see [`extract_u64s`]).
    #[new]
    fn new(values: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(U64Column::from(extract_u64s(values)?))
    }

    fn __len__(&self) -> usize {
        self.codes.len()
    }

    fn __repr__(&self) -> String {
        format!("U64Column(len={})", self.codes.len())
    }

    /// The values from `start` up to, not including, `stop`, sharing this
    /// column's buffer rather than copying it; `IndexError` when `start` is
    /// past `stop` or `stop` past the end.
    fn slice(&self, start: usize, stop: usize) -> PyResult<U64Column> {
        let len = self.codes.len();
        if start > stop {
            return Err(PyIndexError::new_err(format!(
                "slice start {start} is past its stop {stop}"
            )));
        }
        if stop > len {
            return Err(PyIndexError::new_err(format!(
                "slice stop {stop} is out of range for a column of {len} values"
            )));
        }
        // Cut at a u64's alignment, which every value's offset meets
        // whatever alignment the shared buffer carries.
        let codes = self
            .codes
            .slice_with_alignment(start..stop, Alignment::of::<u64>());
        Ok(U64Column::from(codes))
    }

    /// The distinct values, each at its first occurrence, in that order.
    fn distinct(&self, py: Python<'_>) -> U64Column {
        let codes = py.detach(|| columns::distinct_first_seen(self.codes.as_slice()));
        U64Column::from(codes)
    }

    /// `(values, counts)`: the distinct values in first-seen order and how
    /// often each occurs.
    fn value_counts(&self, py: Python<'_>) -> (U64Column, U64Column) {
        let (values, counts) = py.detach(|| columns::value_counts(self.codes.as_slice()));
        (U64Column::from(values), U64Column::from(counts))
    }

    /// The values at `indices` (a `U64Column`, u64 buffer or int sequence,
    /// as [`extract_u64s`] reads them), in that order; an index past the end
    /// raises `IndexError`.
    fn take(&self, py: Python<'_>, indices: &Bound<'_, PyAny>) -> PyResult<U64Column> {
        let indices = extract_u64s(indices)?;
        let codes = py
            .detach(|| columns::take(self.codes.as_slice(), &indices))
            .map_err(|position| {
                PyIndexError::new_err(format!(
                    "take index {} is out of range for a column of {} values",
                    indices[position],
                    self.codes.len()
                ))
            })?;
        Ok(U64Column::from(codes))
    }

    /// The number of row pairs [`join_indices`](Self::join_indices) makes
    /// with `other` (a `U64Column`, u64 buffer or int sequence), counted
    /// without building them.
    fn join_count(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<usize> {
        let right = extract_u64s(other)?;
        Ok(py.detach(|| columns::join_count(self.codes.as_slice(), right.as_slice())))
    }

    /// The row pairs `(left_indices, right_indices)` where this column's
    /// value equals `other`'s (a `U64Column`, u64 buffer or int sequence) —
    /// an equi-join on the two columns as keys, in
    /// nested-loop order: this column's rows in order, each with its
    /// matches in `other` in their original order. Gather the joined
    /// columns with [`take`](Self::take). A join of more than `max_pairs`
    /// pairs raises `JoinTooLarge` before any pair is built.
    #[pyo3(signature = (other, *, max_pairs=None))]
    fn join_indices(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        max_pairs: Option<usize>,
    ) -> PyResult<(U64Column, U64Column)> {
        let right = extract_u64s(other)?;
        let (left_idx, right_idx) = py
            .detach(|| {
                columns::equi_join_indices(self.codes.as_slice(), right.as_slice(), max_pairs)
            })
            .map_err(|pairs| join_too_large(pairs, max_pairs))?;
        Ok((U64Column::from(left_idx), U64Column::from(right_idx)))
    }

    /// The rows where a run of equal rows starts, across this column and
    /// `others` (each a `U64Column`, u64 buffer or int sequence of this
    /// column's length): row 0, and every row whose value in some column
    /// differs from the row before. Gathered with [`take`](Self::take) they
    /// keep one row of each run — the distinct rows of a match whose equal
    /// rows are adjacent, as a union over graphs' triples are (see
    /// `columns::run_starts`). Columns of unequal lengths raise `ValueError`.
    #[pyo3(signature = (*others))]
    fn run_starts(&self, py: Python<'_>, others: &Bound<'_, PyTuple>) -> PyResult<U64Column> {
        let others = others
            .iter()
            .map(|other| extract_u64s(&other))
            .collect::<PyResult<Vec<_>>>()?;
        let mut columns = vec![self.codes.as_slice()];
        columns.extend(others.iter().map(|other| other.as_slice()));
        let starts = py
            .detach(|| columns::run_starts(&columns))
            .ok_or_else(|| PyValueError::new_err("run_starts columns differ in length"))?;
        Ok(U64Column::from(starts))
    }

    /// This column's rows grouped by value: the right side of
    /// [`join_indices`](Self::join_indices), built once and joined with any
    /// number of left columns (see [`CodeGroups`]).
    fn groups(&self, py: Python<'_>) -> CodeGroups {
        let groups = py.detach(|| columns::CodeGroups::build(self.codes.as_slice()));
        CodeGroups { groups }
    }

    /// Fills `view` over the raw u64 data; the exported buffer holds a
    /// reference to this object, so the memory outlives every memoryview.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let data = slf.get().codes.as_slice();
        let ret = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                data.as_ptr() as *mut c_void,
                std::mem::size_of_val(data) as ffi::Py_ssize_t,
                1, // read-only
                flags,
            )
        };
        if ret != 0 {
            return Err(PyErr::take(slf.py())
                .unwrap_or_else(|| PySystemError::new_err("PyBuffer_FillInfo failed")));
        }
        Ok(())
    }
}

/// `JoinTooLarge` for a join of `pairs` pairs refused by its `max_pairs`
/// cap: its `args` are the message, which names both, and `pairs`.
fn join_too_large(pairs: usize, max_pairs: Option<usize>) -> PyErr {
    let cap = max_pairs.unwrap_or_else(|| unreachable!("only a max_pairs cap refuses a join"));
    JoinTooLarge::new_err((
        format!("the join makes {pairs} pairs, more than its max_pairs of {cap}"),
        pairs,
    ))
}

/// A column's rows grouped by value ([`U64Column::groups`]): the right side
/// of an equi-join, built once and joined with any number of left columns,
/// so a join too large for one call runs as a loop over slices of its left
/// column ([`U64Column::slice`]). It holds one u64 per grouped row, plus a
/// slot per value of the column's span or per distinct value, until it is
/// dropped.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct CodeGroups {
    groups: columns::CodeGroups,
}

#[pymethods]
impl CodeGroups {
    /// The number of grouped rows.
    fn __len__(&self) -> usize {
        self.groups.len()
    }

    /// The number of row pairs [`join`](Self::join) makes with `column` (a
    /// `U64Column`, u64 buffer or int sequence), counted without building
    /// them.
    fn count(&self, py: Python<'_>, column: &Bound<'_, PyAny>) -> PyResult<usize> {
        let left = extract_u64s(column)?;
        Ok(py.detach(|| self.groups.count(left.as_slice())))
    }

    /// The row pairs `(left_indices, right_indices)` where `column`'s value
    /// (a `U64Column`, u64 buffer or int sequence) equals a grouped row's,
    /// in nested-loop order: `column`'s rows in order, each with its grouped
    /// rows in their column's order. The left indices count from `column`'s
    /// first row, so the joins of consecutive slices of a left column, each
    /// offset by its slice's start, concatenate to
    /// [`U64Column::join_indices`] of the whole column. A join of more than
    /// `max_pairs` pairs raises `JoinTooLarge` before any pair is built.
    #[pyo3(signature = (column, *, max_pairs=None))]
    fn join(
        &self,
        py: Python<'_>,
        column: &Bound<'_, PyAny>,
        max_pairs: Option<usize>,
    ) -> PyResult<(U64Column, U64Column)> {
        let left = extract_u64s(column)?;
        let (left_idx, right_idx) = py
            .detach(|| self.groups.join(left.as_slice(), max_pairs))
            .map_err(|pairs| join_too_large(pairs, max_pairs))?;
        Ok((U64Column::from(left_idx), U64Column::from(right_idx)))
    }

    /// The columns the join of `column` with the grouped rows gathers,
    /// filled while the pairs are found instead of through
    /// [`join`](Self::join)'s indices: for each pair in nested-loop order,
    /// each of `left_columns` (parallel to `column`) at the pair's left row,
    /// then each of `right_columns` (parallel to the grouped column) at its
    /// right row, one `U64Column` per column in that order. Each equals
    /// [`U64Column::take`] of its column at the matching index column of
    /// `join`. Every column is read as [`extract_u64s`] reads it; a column
    /// of the wrong length raises `ValueError`, and a join of more than
    /// `max_pairs` pairs raises `JoinTooLarge` before any output is built.
    #[pyo3(signature = (column, left_columns, right_columns, *, max_pairs=None))]
    fn gather<'py>(
        &self,
        py: Python<'py>,
        column: &Bound<'py, PyAny>,
        left_columns: Vec<Bound<'py, PyAny>>,
        right_columns: Vec<Bound<'py, PyAny>>,
        max_pairs: Option<usize>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let left = extract_u64s(column)?;
        let lefts = left_columns
            .iter()
            .map(extract_u64s)
            .collect::<PyResult<Vec<_>>>()?;
        let rights = right_columns
            .iter()
            .map(extract_u64s)
            .collect::<PyResult<Vec<_>>>()?;
        if let Some(bad) = lefts.iter().position(|c| c.len() != left.len()) {
            return Err(PyValueError::new_err(format!(
                "left column {bad} holds {} values, not one per row of the {} joined",
                lefts[bad].len(),
                left.len()
            )));
        }
        let grouped = self.groups.len();
        if let Some(bad) = rights.iter().position(|c| c.len() != grouped) {
            return Err(PyValueError::new_err(format!(
                "right column {bad} holds {} values, not one per row of the {grouped} grouped",
                rights[bad].len()
            )));
        }
        let outputs = py
            .detach(|| {
                let lefts: Vec<&[u64]> = lefts.iter().map(Buffer::as_slice).collect();
                let rights: Vec<&[u64]> = rights.iter().map(Buffer::as_slice).collect();
                self.groups
                    .gather(left.as_slice(), &lefts, &rights, max_pairs)
            })
            .map_err(|pairs| join_too_large(pairs, max_pairs))?;
        PyTuple::new(py, outputs.into_iter().map(U64Column::from))
    }
}
