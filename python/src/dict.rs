//! The dictionary handle ([`TermDict`]) and the batch decode it shares with
//! the string matchers ([`decode_slice`]): a repeated code is decoded once
//! while a small cache still holds it, and no per-occurrence term string is
//! built. With the zero-copy code columns ([`U64Column`]) it mirrors the JS
//! bindings' lazy payload (`js/src/store.rs::match_payload`).

use std::ops::Range;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use vortex_rdf_core::VortexRdfError as CoreError;
use vortex_rdf_core::{CaseMap, DictReader, TermCode, TermPredicate, TextOptions};

use crate::kernels::U64Column;
use crate::u64s::extract_u64s;
use crate::{detached, py_err};

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
        Ok((U64Column::from(passed), U64Column::from(undecided)))
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
