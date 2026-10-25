//! The Dictionary-layout code path: matched rows as zero-copy `u64` term-code
//! columns ([`U64Column`]) plus a dictionary handle, mirroring the JS bindings' lazy payload
//! (`js/src/store.rs::match_payload`). A repeated code is decoded once while a
//! small cache still holds it, and no per-occurrence term string is built.

use std::ops::Range;
use std::os::raw::{c_int, c_void};

use pyo3::buffer::{ElementType, PyUntypedBuffer};
use pyo3::exceptions::{PyIndexError, PySystemError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use vortex_buffer::{Alignment, Buffer};
use vortex_rdf_core::VortexRdfError as CoreError;
use vortex_rdf_core::{CaseMap, DictReader, TermCode, TermPredicate, TextOptions, columns};

use crate::{JoinTooLarge, detached, py_err};

/// Buckets in the decode-sharing cache (see [`decode_slice`]).
/// A power of two, so the bucket index is a mask rather than a division; 256
/// entries keep the table at 4 KiB, small enough to stay cache-resident and to
/// zero cheaply for a short column.
const RECENT_BUCKETS: usize = 256;

/// Slot sentinel for an empty cache bucket. Code 0 is a legitimate term code,
/// so emptiness has to be carried by the slot rather than the code.
const NO_SLOT: usize = usize::MAX;

/// Bytes of text a [`PackedTerms`] reserves per position up front: a short
/// IRI or literal is longer, so the buffer grows only a few times.
const PACKED_TEXT_PER_POSITION: usize = 32;

/// The most a [`PackedTerms`] reserves up front however long the batch. The
/// buffer grows from there as terms are written, so a batch of mostly
/// out-of-range codes does not reserve text it never fills.
const PACKED_TEXT_RESERVE_MAX: usize = 1 << 20;

/// Terms decoded GIL-released, packed into one buffer and indexed by the
/// position of their code in the batch. A term is a range of `text`; a
/// position holding none (an out-of-range code) has no range. Positions may
/// be filled in any order.
struct PackedTerms {
    text: String,
    ranges: Vec<Option<Range<usize>>>,
}

impl PackedTerms {
    fn new(len: usize) -> Self {
        let reserve = len
            .saturating_mul(PACKED_TEXT_PER_POSITION)
            .min(PACKED_TEXT_RESERVE_MAX);
        Self {
            text: String::with_capacity(reserve),
            ranges: vec![None; len],
        }
    }

    /// Store position `i`'s term, copying its bytes into `text`.
    fn set(&mut self, i: usize, term: Option<&str>) {
        self.ranges[i] = term.map(|term| {
            let start = self.text.len();
            self.text.push_str(term);
            start..self.text.len()
        });
    }

    fn len(&self) -> usize {
        self.ranges.len()
    }

    fn get(&self, i: usize) -> Option<&str> {
        self.ranges[i].clone().map(|range| &self.text[range])
    }
}

/// An immutable handle on a store's term dictionary, resident or left in
/// its file. Decodes term codes to their N-Triples strings. A handle on a
/// file-backed dictionary keeps the file's mapping alive.
///
/// A file-backed handle answers every call by reading the dictionary child
/// (through a bounded cache of rebuilt windows), GIL released, on the
/// bindings' runtime; a resident one answers in place.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct TermDict {
    pub(crate) reader: DictReader,
}

/// Decodes `codes` through `reader` GIL-released into Python strings, a
/// repeated code sharing its string while the cache below still holds it.
/// `codes` is Rust-owned memory, never a view borrowed from a Python buffer,
/// which cannot cross the GIL release: callers pass a column's buffer or the
/// buffer [`extract_u64s`] returns.
///
/// A direct-mapped cache of `RECENT_BUCKETS` entries, indexed by the
/// code's low bits (`code & (RECENT_BUCKETS - 1)`), holds the
/// `(code, slot)` most recently decoded into each bucket; an occurrence
/// whose bucket holds its own code reuses that slot instead of
/// decompressing and allocating again. A hit
/// requires the stored code to match, so a collision — or a column holding
/// more live terms than there are buckets — costs only a re-decode and
/// never yields a wrong term.
///
/// Runs in two phases, because Python objects cannot be built while
/// detached from the interpreter. The decode loop runs GIL-released and
/// produces the distinct terms, packed into one buffer ([`PackedTerms`],
/// with no `String` per term), plus a per-occurrence index into them; the
/// GIL is then retaken to build one `PyString` per distinct term from a
/// slice of that buffer and clone a reference per occurrence. When
/// nothing was shared those indices are the identity, and the strings are
/// built straight from the decode order.
pub(crate) fn decode_slice(
    py: Python<'_>,
    reader: &DictReader,
    codes: &[TermCode],
) -> PyResult<Vec<Option<Py<PyString>>>> {
    // `slots[i]` indexes `decoded` for the i-th code, so the mapping from
    // occurrence to decoded term survives the GIL boundary as plain data.
    let (slots, decoded) = detached(py, || {
        let mut slots: Vec<usize> = Vec::with_capacity(codes.len());
        let mut distinct: Vec<TermCode> = Vec::new();
        let mut recent = [(0 as TermCode, NO_SLOT); RECENT_BUCKETS];
        for &code in codes {
            // The low bits pick the bucket: masked before the
            // narrowing, so they are the code's own.
            let bucket = (code & (RECENT_BUCKETS as TermCode - 1)) as usize;
            let (cached_code, cached_slot) = recent[bucket];
            let slot = if cached_slot != NO_SLOT && cached_code == code {
                cached_slot
            } else {
                let slot = distinct.len();
                distinct.push(code);
                // Overwrites whatever shared this bucket; a hit is
                // gated on the stored code, so a lost entry only
                // costs a re-decode.
                recent[bucket] = (code, slot);
                slot
            };
            slots.push(slot);
        }
        // One read of the distinct codes: a cursor over the resident terms,
        // or one batched read of the file-backed child.
        async move {
            let mut decoded = PackedTerms::new(distinct.len());
            reader
                .decode_many_with(&distinct, |i, term| decoded.set(i, term))
                .await?;
            Ok::<_, CoreError>((slots, decoded))
        }
    })?;

    let build = |i: usize| decoded.get(i).map(|term| PyString::new(py, term).unbind());

    // Nothing was shared, so `slots` is the identity: every string is used
    // exactly once and can be moved straight out, skipping the lookup table
    // and the per-occurrence refcount bump.
    if decoded.len() == slots.len() {
        return Ok((0..slots.len()).map(build).collect());
    }

    let interned: Vec<Option<Py<PyString>>> = (0..decoded.len()).map(build).collect();
    Ok(slots
        .into_iter()
        .map(|slot| interned[slot].as_ref().map(|s| s.clone_ref(py)))
        .collect())
}

#[pymethods]
impl TermDict {
    /// The N-Triples string for `code`, or `None` when the code is out of
    /// this dictionary's range. A code is an int from 0 to 2**64 - 1: a
    /// negative one, or one of 2**64 or more, raises `OverflowError`.
    fn decode(&self, py: Python<'_>, code: TermCode) -> PyResult<Option<String>> {
        // A resident dictionary answers in place, the GIL held: for one code
        // (or `encode`'s one term) that is cheaper than a detached call's GIL
        // release and `block_on`.
        if let Some(snapshot) = self.reader.snapshot() {
            return Ok(snapshot.decode(code));
        }
        detached(py, || self.reader.decode(code))
    }

    /// The code of the term `term`, or `None` when this dictionary does not
    /// hold it. The inverse of [`decode`](Self::decode), tolerant of
    /// spelling: an IRI with or without angle brackets, a literal with
    /// escape variants or an explicit `xsd:string` type, an upper-case
    /// language tag, and the default graph as `""`, `default` or `[]` all
    /// resolve to the stored form's code. A malformed term raises
    /// `ValueError`.
    fn encode(&self, py: Python<'_>, term: &str) -> PyResult<Option<TermCode>> {
        if let Some(snapshot) = self.reader.snapshot() {
            return snapshot.encode(term).map_err(py_err);
        }
        detached(py, || self.reader.encode(term))
    }

    /// [`encode`](Self::encode) over a sequence of terms, in order, in one
    /// GIL-released call. A malformed term raises `ValueError` before any
    /// lookup.
    fn encode_many(&self, py: Python<'_>, terms: Vec<String>) -> PyResult<Vec<Option<TermCode>>> {
        detached(py, || async {
            let refs: Vec<&str> = terms.iter().map(String::as_str).collect();
            self.reader.encode_many(&refs).await
        })
    }

    /// `kind` over the candidate `codes`: `(passed, undecided)`, both
    /// ascending subsets of `codes`; a candidate in neither fails. `codes`
    /// (a `U64Column`, a u64 buffer or any int sequence) must be sorted,
    /// unique and inside the dictionary, else `ValueError`. String kinds
    /// (`str_prefix`, `contains`, `strstarts`, `strends`, `regex`) test the
    /// term's text: by default rdflib's `string()` (string literals only);
    /// `as_str=True` reads SPARQL `STR(term)` instead, where a blank node, or
    /// a literal whose datatype rdflib normalizes, is undecided. `case`
    /// ("lower"/"upper") then wraps that text in Python's
    /// `str.lower()`/`str.upper()`, decided on ASCII text only: `contains`
    /// with `as_str=True, case="lower"` is `CONTAINS(LCASE(STR(?x)), c)`.
    /// The constant of `contains`/`strstarts`/`strends` is a strict
    /// N-Triples spelling (a malformed one is a `ValueError`); `regex` takes
    /// the pattern as `arg` and the SPARQL `flags` (`i`, `s` and `m` apply,
    /// any other letter is ignored), and decides a subset of patterns —
    /// every text is undecided for the others, and for one whose compiled
    /// program is large (counted repetitions that multiply). Only the
    /// dictionary windows holding candidates the kind ranges do not decide
    /// are read; no verdict is memoized. `lang_matches` is BCP 47 basic
    /// filtering, decided for `*` and for a range of ASCII letters, digits
    /// and hyphens; any other range, which rdflib may read differently
    /// (`en-*`, padding whitespace), leaves the language-tagged literals
    /// undecided. An unknown kind or an invalid argument or option raises
    /// `ValueError`.
    #[pyo3(signature = (kind, arg, codes, *, flags = "", case = None, as_str = false))]
    // The parameters are the Python signature.
    #[allow(clippy::too_many_arguments)]
    fn filter_codes(
        &self,
        py: Python<'_>,
        kind: &str,
        arg: &str,
        codes: &Bound<'_, PyAny>,
        flags: &str,
        case: Option<&str>,
        as_str: bool,
    ) -> PyResult<(U64Column, U64Column)> {
        let case = match case {
            None => None,
            Some("lower") => Some(CaseMap::Lower),
            Some("upper") => Some(CaseMap::Upper),
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "case must be None, \"lower\" or \"upper\", got {other:?}"
                )));
            }
        };
        let options = TextOptions {
            flags: flags.to_owned(),
            case,
            as_str,
        };
        let predicate = TermPredicate::parse_with(kind, arg, &options).map_err(py_err)?;
        // Like a keep's code set: whatever is no u64 code list is a bad value.
        let codes = extract_u64s(codes).map_err(|e| {
            PyValueError::new_err(format!(
                "codes is a U64Column, a u64 buffer or a sequence of non-negative ints below \
                 2**64: {e}"
            ))
        })?;
        let (passed, undecided) = detached(py, || {
            self.reader.filter_codes(&predicate, codes.as_slice())
        })?;
        Ok((U64Column { codes: passed }, U64Column { codes: undecided }))
    }

    /// The half-open code range `(lo, hi)` of the terms whose N-Triples
    /// spelling starts with `prefix`. Codes rank spellings in byte order,
    /// so a term kind (`"` for literals, `<` for IRIs, `_:` for blank nodes)
    /// and an IRI namespace (`<http://example.org/`) are each one range.
    fn prefix_range(&self, py: Python<'_>, prefix: &str) -> PyResult<(TermCode, TermCode)> {
        detached(py, || self.reader.prefix_range(prefix))
    }

    /// The code of the first term not below `term` in byte order — a
    /// present term's own code, where an absent one would sort, or the
    /// dictionary's size when every term is below it.
    fn lower_bound(&self, py: Python<'_>, term: &str) -> PyResult<TermCode> {
        detached(py, || self.reader.lower_bound(term))
    }

    /// Whether the terms are read from the store's file on demand (`True`)
    /// rather than held in memory.
    #[getter]
    fn file_backed(&self) -> bool {
        self.reader.is_file_backed()
    }

    /// Decode a batch of codes in one call, releasing the GIL for the whole
    /// batch.
    ///
    /// `codes` is preferably a [`U64Column`] (a column from `match_codes`
    /// passes directly, its buffer shared rather than copied) or a u64
    /// buffer (`memoryview(col).cast("Q")`, `array("Q", ...)`, a `uint64`
    /// NumPy array), read in one bulk copy with no per-element Python-int
    /// conversion; a non-negative `int64` buffer is read the same way, and a
    /// byte-typed buffer — the raw view a `U64Column` exports — is
    /// reinterpreted as native-endian u64s. A buffer of other items (a
    /// `cast("I")` view, a u32 array) raises `ValueError`. Any other sequence
    /// of ints still works, at one `PyLong` extraction per code.
    ///
    /// A repeated code usually yields the same Python string object, as
    /// [`decode_slice`] describes.
    fn decode_many(
        &self,
        py: Python<'_>,
        codes: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Option<Py<PyString>>>> {
        decode_slice(py, &self.reader, &extract_u64s(codes)?)
    }

    fn __len__(&self) -> usize {
        self.reader.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "TermDict(len={}, file_backed={})",
            self.reader.len(),
            if self.reader.is_file_backed() {
                "True"
            } else {
                "False"
            }
        )
    }
}

/// The u64 values of a Python object holding codes, indices or counts, as a
/// Rust-owned buffer. A [`U64Column`] shares its own buffer, uncopied. Any
/// other object exporting a buffer is read as one, in one copy, never
/// element by element, and only when its items are u64 values:
/// 8-byte unsigned integers (`memoryview(col).cast("Q")`, `array("Q", ...)`,
/// a `uint64` NumPy array), 8-byte signed integers none of which is negative
/// (an `int64` NumPy array), or raw bytes in multiples of 8 — the view a
/// `U64Column` itself exports — read as native-endian u64s. A buffer of any
/// other items raises `ValueError` naming the u64 view: above all a
/// `cast("I")` view of a column, which would split each code in two. An
/// object without a buffer is a sequence of ints, at one `PyLong` extraction
/// per element; an int that is negative or not below 2**64 raises
/// `OverflowError`. Nothing is narrowed or wrapped.
pub(crate) fn extract_u64s(obj: &Bound<'_, PyAny>) -> PyResult<Buffer<u64>> {
    if let Ok(column) = obj.cast::<U64Column>() {
        return Ok(column.get().codes.clone());
    }
    // SAFETY: `obj` is a live object and the GIL is held.
    if unsafe { ffi::PyObject_CheckBuffer(obj.as_ptr()) } == 1 {
        // An exporter that fills no strides (a ctypes array) is readable
        // through a memoryview, which fills them.
        let buf = PyUntypedBuffer::get(obj).map_err(|e| {
            PyValueError::new_err(format!(
                "this buffer cannot be read as u64 codes ({e}): pass memoryview(obj), or \
                 {U64_FORMS}"
            ))
        })?;
        return buffer_u64s(&buf).map(Buffer::from);
    }
    obj.extract::<Vec<u64>>().map(Buffer::from)
}

/// What a code argument may be, as the refusals of a buffer spell it.
const U64_FORMS: &str = "a U64Column, a buffer of 8-byte integers (memoryview(col).cast(\"Q\"), \
                         array(\"Q\"), a uint64 or non-negative int64 NumPy array) or raw bytes \
                         in multiples of 8";

/// How a code buffer's items read as u64 values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CodeItems {
    /// 8-byte unsigned integers.
    Unsigned,
    /// 8-byte signed integers, refused if any is negative.
    Signed,
    /// Raw bytes, read as native-endian u64s in groups of 8.
    Raw,
}

/// The u64 values of a buffer's items (see [`extract_u64s`]).
fn buffer_u64s(buf: &PyUntypedBuffer) -> PyResult<Vec<u64>> {
    let format = buf.format();
    let items = match (ElementType::from_format(format), buf.item_size()) {
        (ElementType::UnsignedInteger { bytes: 8 }, 8) => CodeItems::Unsigned,
        (ElementType::SignedInteger { bytes: 8 }, 8) => CodeItems::Signed,
        (ElementType::UnsignedInteger { bytes: 1 }, 1) => CodeItems::Raw,
        _ => {
            return Err(PyValueError::new_err(format!(
                "a buffer of {}-byte items (format '{}') holds no u64 codes: pass {U64_FORMS}",
                buf.item_size(),
                format.to_string_lossy()
            )));
        }
    };
    let len_bytes = buf.len_bytes();
    if !len_bytes.is_multiple_of(8) {
        // Only raw bytes can get here: 8-byte items fill whole u64s.
        return Err(PyValueError::new_err(format!(
            "a byte buffer of {len_bytes} bytes is not a whole number of u64 values"
        )));
    }
    let mut values = vec![0u64; len_bytes / 8];
    // SAFETY: a u64 has no invalid bit patterns and a u8's alignment divides
    // a u64's, so the values are writable as their `len_bytes` bytes.
    let bytes =
        unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), len_bytes) };
    copy_items(buf, bytes);
    let swap = items != CodeItems::Raw && foreign_byte_order(format.to_bytes());
    for value in &mut values {
        if swap {
            *value = value.swap_bytes();
        }
        if items == CodeItems::Signed && (*value as i64) < 0 {
            return Err(PyValueError::new_err(format!(
                "a buffer of signed 8-byte items holds {}: codes are non-negative",
                *value as i64
            )));
        }
    }
    Ok(values)
}

/// Whether a struct-module format's byte-order prefix names the byte order
/// this machine does not use (`<` little-endian, `>` and `!` big-endian;
/// `@`, `=` or none native).
fn foreign_byte_order(format: &[u8]) -> bool {
    match format.first() {
        Some(b'<') => cfg!(target_endian = "big"),
        Some(b'>' | b'!') => cfg!(target_endian = "little"),
        _ => false,
    }
}

/// Copies the buffer's items, in C order, into `out` (exactly
/// `buf.len_bytes()` long): one bulk copy when the buffer is C-contiguous,
/// item by item otherwise (a strided or indirect view).
fn copy_items(buf: &PyUntypedBuffer, out: &mut [u8]) {
    debug_assert_eq!(out.len(), buf.len_bytes());
    if buf.is_c_contiguous() {
        // SAFETY: a C-contiguous buffer is `len_bytes` readable bytes at
        // `buf_ptr`, kept alive by `buf`, and cannot overlap `out`, which
        // this function's caller owns.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.buf_ptr().cast::<u8>(), out.as_mut_ptr(), out.len());
        }
        return;
    }
    let shape = buf.shape();
    let item = buf.item_size();
    if out.is_empty() || item == 0 {
        return;
    }
    // Walk every index in C order: the last dimension varies fastest.
    let mut index = vec![0usize; shape.len()];
    for chunk in out.chunks_exact_mut(item) {
        // SAFETY: `index` is in bounds of `shape`, so `get_ptr` addresses a
        // whole item of `item` bytes inside the live buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.get_ptr(&index).cast::<u8>(),
                chunk.as_mut_ptr(),
                item,
            );
        }
        for d in (0..index.len()).rev() {
            index[d] += 1;
            if index[d] < shape[d] {
                break;
            }
            index[d] = 0;
        }
    }
}

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

#[pymethods]
impl U64Column {
    /// A column holding `values`: another `U64Column` (shared zero-copy), a
    /// buffer of u64 or non-negative i64 items or the raw byte view a column
    /// exports (one bulk copy), or any sequence of ints from 0 to 2**64 - 1.
    /// A buffer of other items raises `ValueError` (see [`extract_u64s`]).
    #[new]
    fn new(values: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(U64Column {
            codes: extract_u64s(values)?,
        })
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
        Ok(U64Column { codes })
    }

    /// The distinct values, each at its first occurrence, in that order.
    fn distinct(&self, py: Python<'_>) -> U64Column {
        let codes = py.detach(|| columns::distinct_first_seen(self.codes.as_slice()));
        U64Column { codes }
    }

    /// `(values, counts)`: the distinct values in first-seen order and how
    /// often each occurs.
    fn value_counts(&self, py: Python<'_>) -> (U64Column, U64Column) {
        let (values, counts) = py.detach(|| columns::value_counts(self.codes.as_slice()));
        (U64Column { codes: values }, U64Column { codes: counts })
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
        Ok(U64Column { codes })
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
        Ok((
            U64Column { codes: left_idx },
            U64Column { codes: right_idx },
        ))
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
        Ok((
            U64Column { codes: left_idx },
            U64Column { codes: right_idx },
        ))
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
        PyTuple::new(py, outputs.into_iter().map(|codes| U64Column { codes }))
    }
}
