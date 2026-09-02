//! The Dictionary-layout code path: matched rows as zero-copy `u32` term-code
//! columns plus a dictionary handle, mirroring the JS bindings' lazy payload
//! (`js/src/store.rs::match_payload`). Python decodes each distinct code once
//! and never materializes per-occurrence term strings.

use std::os::raw::{c_int, c_void};

use arrow_array::UInt32Array;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PySystemError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use vortex_buffer::Buffer;
use vortex_rdf_core::{DictSnapshot, TermPredicate};

use crate::arrow::array_capsules;
use crate::{parse_err, store_err};

/// Buckets in the decode-sharing cache (see [`TermDict::decode_slice`]).
/// A power of two, so the bucket index is a mask rather than a division; 256
/// entries keep the table at 2 KiB, small enough to stay cache-resident and to
/// zero cheaply for a short column.
const RECENT_BUCKETS: usize = 256;

/// Slot sentinel for an empty cache bucket. Code 0 is a legitimate term code,
/// so emptiness has to be carried by the slot rather than the code.
const NO_SLOT: u32 = u32::MAX;

/// An immutable handle on a store's term dictionary. Decodes term codes to
/// their N-Triples strings; safe to keep across store mutations (the snapshot
/// is frozen at creation).
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct TermDict {
    pub(crate) snapshot: DictSnapshot,
}

impl TermDict {
    /// Decodes `codes` GIL-released into one Python string per distinct
    /// code, sharing that object across every occurrence of the code. Callers
    /// must hand over codes copied out of any Python buffer: a borrowed
    /// buffer view cannot cross the GIL release.
    ///
    /// A direct-mapped cache of `RECENT_BUCKETS` entries, indexed by
    /// `code & (RECENT_BUCKETS - 1)`, holds the `(code, slot)` most recently
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
    pub(crate) fn decode_slice(&self, py: Python<'_>, codes: &[u32]) -> Vec<Option<Py<PyString>>> {
        // `slots[i]` indexes `decoded` for the i-th code, so the mapping from
        // occurrence to decoded term survives the GIL boundary as plain data.
        let (slots, decoded) = py.detach(|| {
            let mut slots: Vec<u32> = Vec::with_capacity(codes.len());
            let mut decoded: Vec<Option<String>> = Vec::new();
            let mut recent = [(0u32, NO_SLOT); RECENT_BUCKETS];
            for &code in codes {
                let bucket = (code as usize) & (RECENT_BUCKETS - 1);
                let (cached_code, cached_slot) = recent[bucket];
                let slot = if cached_slot != NO_SLOT && cached_code == code {
                    cached_slot
                } else {
                    let slot = decoded.len() as u32;
                    decoded.push(self.snapshot.decode(code));
                    // Overwrites whatever shared this bucket; a hit is
                    // gated on the stored code, so a lost entry only costs a
                    // re-decode.
                    recent[bucket] = (code, slot);
                    slot
                };
                slots.push(slot);
            }
            (slots, decoded)
        });

        let build = |term: Option<String>| term.map(|t| PyString::new(py, &t).unbind());

        // Nothing was shared, so `slots` is the identity: every string is used
        // exactly once and can be moved straight out, skipping the lookup table
        // and the per-occurrence refcount bump.
        if decoded.len() == slots.len() {
            return decoded.into_iter().map(build).collect();
        }

        let interned: Vec<Option<Py<PyString>>> = decoded.into_iter().map(build).collect();
        slots
            .into_iter()
            .map(|slot| interned[slot as usize].as_ref().map(|s| s.clone_ref(py)))
            .collect()
    }
}

#[pymethods]
impl TermDict {
    /// The N-Triples string for `code`, or `None` when the code is out of
    /// this dictionary's range.
    fn decode(&self, code: u32) -> Option<String> {
        self.snapshot.decode(code)
    }

    /// The code of the term string `term`, or `None` when this dictionary
    /// does not hold the term. The inverse of [`decode`](Self::decode):
    /// looked up as spelled, then — on a miss — in its canonical N-Triples
    /// form (an `xsd:string`-typed literal is a plain one, escapes
    /// normalize), so a caller's own rendering of a term still resolves.
    fn encode(&self, term: &str) -> Option<u32> {
        self.snapshot.encode(term)
    }

    /// [`encode`](Self::encode) for many terms in one GIL-released call, in
    /// input order.
    fn encode_many(&self, py: Python<'_>, terms: Vec<String>) -> Vec<Option<u32>> {
        py.detach(|| {
            let terms: Vec<&str> = terms.iter().map(String::as_str).collect();
            self.snapshot.encode_many(&terms)
        })
    }

    /// The first code whose term is `>= term` in byte order (`len(self)`
    /// when every term is smaller): codes are lexicographic ranks, so
    /// `lower_bound(a)..lower_bound(b)` is exactly the codes of the terms in
    /// `a..b`.
    fn lower_bound(&self, term: &str) -> u32 {
        self.snapshot.lower_bound(term)
    }

    /// The half-open code range `(lo, hi)` of the terms whose N-Triples
    /// spelling starts with `prefix` — an IRI namespace as `"<http://…/"`,
    /// a kind as its first byte (`'"'` literals, `"<"` IRIs, `"_:"` blank
    /// nodes). Pass it as `range(lo, hi)` to `match_codes(keep=...)`.
    fn prefix_range(&self, prefix: &str) -> (u32, u32) {
        self.snapshot.prefix_range(prefix)
    }

    /// The codes for which the term predicate `kind` with argument `arg` is
    /// definitely true, and the codes it cannot decide (the caller resolves
    /// those itself) — two ascending code columns; the remaining codes are
    /// definitely false. Kinds: `is_literal`, `is_iri`, `is_blank`
    /// (`arg` ignored), `datatype` (an IRI), `lang` (a tag, `""` for
    /// untagged), `lang_matches` (a language range), `str_prefix` (a
    /// string), `num_lt`/`num_le`/`num_gt`/`num_ge`/`num_eq`/`num_ne` (a
    /// numeric constant: an N-Triples literal spelling, or a bare number
    /// typed by its syntax). One pass over the dictionary per predicate,
    /// memoized for the dictionary's lifetime. A bad `kind` or `arg`
    /// raises `ValueError`.
    fn filter_codes(&self, py: Python<'_>, kind: &str, arg: &str) -> PyResult<(U32Column, U32Column)> {
        let predicate = TermPredicate::parse(kind, arg).map_err(parse_err)?;
        let (holds, unknown) = py
            .detach(|| self.snapshot.filter_codes(&predicate))
            .map_err(store_err)?;
        Ok((U32Column { codes: holds }, U32Column { codes: unknown }))
    }

    /// Decode a batch of codes in one call, releasing the GIL for the whole
    /// batch.
    ///
    /// `codes` is preferably a u32 buffer (`memoryview(col).cast("I")`,
    /// `array("I", ...)`, a `uint32` NumPy array), read in one bulk copy
    /// with no per-element Python-int conversion. A byte-typed buffer — the
    /// raw view a [`U32Column`] itself exports — is reinterpreted as
    /// native-endian u32s, so a column from `match_codes` passes directly.
    /// Any other sequence of ints still works, at one `PyLong` extraction
    /// per code.
    ///
    /// A repeated code yields the *same* Python string object; see
    /// [`decode_slice`](Self::decode_slice).
    fn decode_many(
        &self,
        py: Python<'_>,
        codes: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Option<Py<PyString>>>> {
        Ok(self.decode_slice(py, &codes_from_py(py, codes)?))
    }

    fn __len__(&self) -> usize {
        self.snapshot.len()
    }

    fn __repr__(&self) -> String {
        format!("TermDict(len={})", self.snapshot.len())
    }

    /// The Arrow PyCapsule interface: the whole dictionary as a `string_view`
    /// array whose element `i` is the term of code `i` — a code → term
    /// lookup table for `pyarrow.array(dictionary)`, `polars.Series(dictionary)`
    /// and friends. Built once per dictionary and shared by every export.
    /// `requested_schema` is accepted for protocol conformance and not
    /// applied.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let _ = requested_schema;
        let values = self.snapshot.to_arrow().map_err(store_err)?;
        array_capsules(py, values.as_ref())
    }
}

/// Term codes out of any Python value that carries them: a u32 buffer
/// (`memoryview(col).cast("I")`, `array("I", ...)`, a `uint32` NumPy array),
/// read in one bulk copy; a byte-typed buffer — the raw view a
/// [`U32Column`] exports — reinterpreted as native-endian u32s; or any
/// other sequence of ints, one `PyLong` extraction per code.
pub(crate) fn codes_from_py(py: Python<'_>, codes: &Bound<'_, PyAny>) -> PyResult<Vec<u32>> {
    if let Ok(buf) = PyBuffer::<u32>::get(codes) {
        return buf.to_vec(py);
    }
    if let Ok(buf) = PyBuffer::<u8>::get(codes) {
        let bytes = buf.to_vec(py)?;
        if !bytes.len().is_multiple_of(4) {
            return Err(PyValueError::new_err(format!(
                "byte buffer of {} bytes is not a whole number of u32 codes",
                bytes.len()
            )));
        }
        return Ok(bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| u32::from_ne_bytes(*b))
            .collect());
    }
    codes.extract::<Vec<u32>>()
}

/// One matched term-code column, exposed to Python zero-copy through the
/// buffer protocol: `memoryview(col).cast("I")` views the Rust memory
/// directly. The column is read-only and owns (refcounts) its backing buffer.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct U32Column {
    pub(crate) codes: Buffer<u32>,
}

#[pymethods]
impl U32Column {
    fn __len__(&self) -> usize {
        self.codes.len()
    }

    fn __repr__(&self) -> String {
        format!("U32Column(len={})", self.codes.len())
    }

    /// The Arrow PyCapsule interface: the column as a `uint32` array sharing
    /// this column's buffer — `pyarrow.array(col)`, `polars.Series(col)`
    /// view the same memory the buffer protocol exposes. `requested_schema`
    /// is accepted for protocol conformance and not applied.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let _ = requested_schema;
        let array = UInt32Array::new(self.codes.clone().into_arrow_scalar_buffer(), None);
        array_capsules(py, &array)
    }

    /// Fills `view` over the raw u32 data; the exported buffer holds a
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
