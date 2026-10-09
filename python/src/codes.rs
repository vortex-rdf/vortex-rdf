//! The Dictionary-layout code path: matched rows as zero-copy `u64` term-code
//! columns ([`U64Column`]) plus a dictionary handle, mirroring the JS bindings' lazy payload
//! (`js/src/store.rs::match_payload`). Python decodes each distinct code once
//! and never materializes per-occurrence term strings.

use std::os::raw::{c_int, c_void};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PySystemError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyString;
use vortex_buffer::Buffer;
use vortex_rdf_core::VortexRdfError as CoreError;
use vortex_rdf_core::{CaseMap, DictReader, TermCode, TermPredicate, TextOptions, columns};

use crate::{RUNTIME, parse_err, store_err};

/// Buckets in the decode-sharing cache (see [`TermDict::decode_slice`]).
/// A power of two, so the bucket index is a mask rather than a division; 256
/// entries keep the table at 2 KiB, small enough to stay cache-resident and to
/// zero cheaply for a short column.
const RECENT_BUCKETS: usize = 256;

/// Slot sentinel for an empty cache bucket. Code 0 is a legitimate term code,
/// so emptiness has to be carried by the slot rather than the code.
const NO_SLOT: usize = usize::MAX;

/// An immutable handle on a store's term dictionary, resident or left in
/// its file. Decodes term codes to their N-Triples strings; safe to keep
/// across store mutations (the handle is frozen at creation).
///
/// A file-backed handle answers every call by reading the dictionary child,
/// GIL released, on the bindings' runtime; a resident one answers in place.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct TermDict {
    pub(crate) reader: DictReader,
}

/// A malformed term or predicate argument is a `ValueError`; anything else
/// (an I/O failure reading a file-backed dictionary) is a store error.
fn term_err(e: CoreError) -> PyErr {
    match e {
        CoreError::Deserialization(_) | CoreError::InvalidOperation(_) => parse_err(e),
        other => store_err(other),
    }
}

impl TermDict {
    /// Decodes `codes` GIL-released into one Python string per distinct
    /// code, sharing that object across every occurrence of the code. Callers
    /// must hand over codes copied out of any Python buffer: a borrowed
    /// buffer view cannot cross the GIL release.
    ///
    /// A direct-mapped cache of `RECENT_BUCKETS` entries, indexed by the
    /// code's low bits (`code & (RECENT_BUCKETS - 1)`), holds the `(code, slot)` most recently
    /// decoded into each bucket; an occurrence whose bucket holds its own code
    /// reuses that slot instead of decompressing and allocating again. A hit
    /// requires the stored code to match, so a collision — or a column holding
    /// more live terms than there are buckets — costs only a re-decode and
    /// never yields a wrong term.
    ///
    /// Runs in two phases, because Python objects cannot be built while
    /// detached from the interpreter. The decode loop runs GIL-released and
    /// produces the distinct terms plus a per-occurrence index into them; the
    /// GIL is then retaken to build one `PyString` per distinct term and clone
    /// a reference per occurrence. When nothing was shared those indices are
    /// the identity, and the strings are built straight from the decode order.
    pub(crate) fn decode_slice(
        &self,
        py: Python<'_>,
        codes: &[TermCode],
    ) -> PyResult<Vec<Option<Py<PyString>>>> {
        // `slots[i]` indexes `decoded` for the i-th code, so the mapping from
        // occurrence to decoded term survives the GIL boundary as plain data.
        let (slots, decoded) = py
            .detach(|| -> Result<_, CoreError> {
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
                // One cursor over the resident terms, or one batched read of
                // the file-backed child.
                let decoded = match self.reader.snapshot() {
                    Some(snapshot) => snapshot.decode_many(&distinct),
                    None => RUNTIME.block_on(self.reader.decode_many(&distinct))?,
                };
                Ok((slots, decoded))
            })
            .map_err(store_err)?;

        let build = |term: Option<String>| term.map(|t| PyString::new(py, &t).unbind());

        // Nothing was shared, so `slots` is the identity: every string is used
        // exactly once and can be moved straight out, skipping the lookup table
        // and the per-occurrence refcount bump.
        if decoded.len() == slots.len() {
            return Ok(decoded.into_iter().map(build).collect());
        }

        let interned: Vec<Option<Py<PyString>>> = decoded.into_iter().map(build).collect();
        Ok(slots
            .into_iter()
            .map(|slot| interned[slot].as_ref().map(|s| s.clone_ref(py)))
            .collect())
    }
}

#[pymethods]
impl TermDict {
    /// The N-Triples string for `code`, or `None` when the code is out of
    /// this dictionary's range. A code is an int from 0 to 2**64 - 1: a
    /// negative one, or one of 2**64 or more, raises `OverflowError`.
    fn decode(&self, py: Python<'_>, code: TermCode) -> PyResult<Option<String>> {
        if let Some(snapshot) = self.reader.snapshot() {
            return Ok(snapshot.decode(code));
        }
        py.detach(|| RUNTIME.block_on(self.reader.decode(code)))
            .map_err(term_err)
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
            return snapshot.encode_tolerant(term).map_err(term_err);
        }
        py.detach(|| RUNTIME.block_on(self.reader.encode(term)))
            .map_err(term_err)
    }

    /// [`encode`](Self::encode) over a sequence of terms, in order, in one
    /// GIL-released call. A malformed term raises `ValueError` before any
    /// lookup.
    fn encode_many(&self, py: Python<'_>, terms: Vec<String>) -> PyResult<Vec<Option<TermCode>>> {
        py.detach(|| {
            let refs: Vec<&str> = terms.iter().map(String::as_str).collect();
            RUNTIME.block_on(self.reader.encode_many(&refs))
        })
        .map_err(term_err)
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
    /// are read; nothing is memoized. `lang_matches` is BCP 47 basic
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
        let predicate = TermPredicate::parse_with(kind, arg, &options).map_err(term_err)?;
        // Like a keep's code set: whatever is no u64 code list is a bad value.
        let codes = code_buffer(codes).map_err(|e| {
            PyValueError::new_err(format!(
                "codes is a U64Column, a u64 buffer or a sequence of non-negative ints below \
                 2**64: {e}"
            ))
        })?;
        let (passed, undecided) = py
            .detach(|| match self.reader.snapshot() {
                Some(snapshot) => snapshot.filter_codes(&predicate, codes.as_slice()),
                None => RUNTIME.block_on(self.reader.filter_codes(&predicate, codes.as_slice())),
            })
            .map_err(term_err)?;
        Ok((U64Column { codes: passed }, U64Column { codes: undecided }))
    }

    /// The half-open code range `(lo, hi)` of the terms whose N-Triples
    /// spelling starts with `prefix`. Codes rank spellings in byte order,
    /// so a term kind (`"` for literals, `<` for IRIs, `_:` for blank nodes)
    /// and an IRI namespace (`<http://example.org/`) are each one range.
    fn prefix_range(&self, py: Python<'_>, prefix: &str) -> PyResult<(TermCode, TermCode)> {
        py.detach(|| RUNTIME.block_on(self.reader.prefix_range(prefix)))
            .map_err(term_err)
    }

    /// The code of the first term not below `term` in byte order — a
    /// present term's own code, where an absent one would sort, or the
    /// dictionary's size when every term is below it.
    fn lower_bound(&self, py: Python<'_>, term: &str) -> PyResult<TermCode> {
        py.detach(|| RUNTIME.block_on(self.reader.lower_bound(term)))
            .map_err(term_err)
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
    /// passes directly) or a u64 buffer (`memoryview(col).cast("Q")`,
    /// `array("Q", ...)`, a `uint64` NumPy array), read in one bulk copy
    /// with no per-element Python-int conversion; a u32 buffer is widened,
    /// and a byte-typed buffer — the raw view a `U64Column` exports — is
    /// reinterpreted as native-endian u64s. Any other sequence of ints still
    /// works, at one `PyLong` extraction per code.
    ///
    /// A repeated code yields the *same* Python string object; see
    /// [`decode_slice`](Self::decode_slice).
    fn decode_many(
        &self,
        py: Python<'_>,
        codes: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Option<Py<PyString>>>> {
        self.decode_slice(py, &extract_u64s(codes)?)
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

/// The u64 values of a Python object holding codes, indices or counts: a
/// [`U64Column`] (its buffer, copied), a u64 buffer
/// (`memoryview(col).cast("Q")`, `array("Q", ...)`, a `uint64` NumPy array)
/// read in one bulk copy, a u32 buffer widened, a byte-typed buffer — the
/// raw view a `U64Column` itself exports — reinterpreted as native-endian
/// u64s, or any other sequence of ints, at one `PyLong` extraction per
/// element. An int that is negative or not below 2**64 raises
/// `OverflowError`; nothing is narrowed or wrapped.
pub(crate) fn extract_u64s(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
    let py = obj.py();
    if let Ok(column) = obj.cast::<U64Column>() {
        return Ok(column.get().codes.as_slice().to_vec());
    }
    if let Ok(buf) = PyBuffer::<u64>::get(obj) {
        return buf.to_vec(py);
    }
    if let Ok(buf) = PyBuffer::<u32>::get(obj) {
        return Ok(buf.to_vec(py)?.into_iter().map(u64::from).collect());
    }
    if let Ok(buf) = PyBuffer::<u8>::get(obj) {
        let bytes = buf.to_vec(py)?;
        if !bytes.len().is_multiple_of(8) {
            return Err(PyValueError::new_err(format!(
                "byte buffer of {} bytes is not a whole number of u64 values",
                bytes.len()
            )));
        }
        return Ok(bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|b| u64::from_ne_bytes(*b))
            .collect());
    }
    obj.extract::<Vec<u64>>()
}

/// The u64 values of `obj` as a buffer: a `U64Column`'s own, shared
/// zero-copy, or [`extract_u64s`]'s copy of anything else.
fn code_buffer(obj: &Bound<'_, PyAny>) -> PyResult<Buffer<u64>> {
    if let Ok(column) = obj.cast::<U64Column>() {
        return Ok(column.get().codes.clone());
    }
    Ok(Buffer::from(extract_u64s(obj)?))
}

/// One matched term-code column — or a kernel's row indices or counts —
/// exposed to Python zero-copy through the buffer protocol:
/// `memoryview(col).cast("Q")` views the Rust memory directly, one native-
/// endian u64 per value. Codes are u64 ([`TermCode`]), so a dictionary of
/// more than 2**32 terms hands its codes across unchanged. The column is
/// read-only and owns (refcounts) its backing buffer.
///
/// The column kernels ([`distinct`](Self::distinct),
/// [`value_counts`](Self::value_counts), [`take`](Self::take),
/// [`join_indices`](Self::join_indices)) run GIL-released and preserve the
/// orders a query layer observes: first-seen order for distinct values,
/// nested-loop order for a join's pairs.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct U64Column {
    pub(crate) codes: Buffer<u64>,
}

#[pymethods]
impl U64Column {
    /// A column holding `values`: another `U64Column` (shared zero-copy), a
    /// u64 or u32 buffer or the raw byte view a column exports (one bulk
    /// copy), or any sequence of ints from 0 to 2**64 - 1.
    #[new]
    fn new(values: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(U64Column {
            codes: code_buffer(values)?,
        })
    }

    fn __len__(&self) -> usize {
        self.codes.len()
    }

    fn __repr__(&self) -> String {
        format!("U64Column(len={})", self.codes.len())
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

    /// The values at `indices` (a `U64Column`, u64 or u32 buffer or int
    /// sequence), in that order; an index past the end raises `IndexError`.
    fn take(&self, py: Python<'_>, indices: &Bound<'_, PyAny>) -> PyResult<U64Column> {
        let indices = extract_u64s(indices)?;
        let codes = py
            .detach(|| columns::take(self.codes.as_slice(), &indices))
            .map_err(|position| {
                pyo3::exceptions::PyIndexError::new_err(format!(
                    "take index {} is out of range for a column of {} values",
                    indices[position],
                    self.codes.len()
                ))
            })?;
        Ok(U64Column { codes })
    }

    /// The row pairs `(left_indices, right_indices)` where this column's
    /// value equals `other`'s (a `U64Column`, u64 or u32 buffer or int
    /// sequence) —
    /// an equi-join on the two columns as keys, in
    /// nested-loop order: this column's rows in order, each with its
    /// matches in `other` in their original order. Gather the joined
    /// columns with [`take`](Self::take).
    fn join_indices(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<(U64Column, U64Column)> {
        let right = U64Column::new(other)?.codes;
        let (left_idx, right_idx) =
            py.detach(|| columns::equi_join_indices(self.codes.as_slice(), right.as_slice()));
        Ok((
            U64Column { codes: left_idx },
            U64Column { codes: right_idx },
        ))
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
