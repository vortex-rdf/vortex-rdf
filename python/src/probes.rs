//! Probe and keep specifications from Python objects: the narrowing
//! keywords of `match_codes`/`count_quads` and the probe list of the
//! `*_many` calls. Everything is parsed before any evaluation, so a
//! malformed probe raises `ValueError` before the store is touched.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use vortex_rdf_core::{Keep, Probe, QuadColumn, TermCode};

use crate::py_err;
use crate::u64s::extract_u64s;

/// A probe from a pattern's four term spellings and its narrowing: the
/// keywords of `match_codes`/`count_quads`, or the keys of a dict probe.
/// `keep` is read by [`parse_keeps`]; absent or `None`, it keeps every row.
pub(crate) fn narrowed_probe(
    s: Option<&str>,
    p: Option<&str>,
    o: Option<&str>,
    g: Option<&str>,
    keep: Option<&Bound<'_, PyAny>>,
    limit: Option<usize>,
    offset: usize,
) -> PyResult<Probe> {
    let mut probe = Probe::parse(s, p, o, g).map_err(py_err)?;
    if let Some(keep) = keep.filter(|k| !k.is_none()) {
        probe.keeps = parse_keeps(keep)?;
    }
    probe.limit = limit;
    probe.offset = offset;
    Ok(probe)
}

/// Every probe of a `*_many` call, parsed before any evaluation.
pub(crate) fn parse_probes(probes: &Bound<'_, PyAny>) -> PyResult<Vec<Probe>> {
    probes
        .try_iter()?
        .map(|probe| parse_probe(&probe?))
        .collect()
}

/// A `Probe` from a pattern 4-tuple (or list) `(s, p, o, g)`, or a dict with
/// keys `s`, `p`, `o`, `g`, `keep`, `limit`, `offset` (each optional; any
/// other key is an error).
fn parse_probe(obj: &Bound<'_, PyAny>) -> PyResult<Probe> {
    let mut terms: [Option<String>; 4] = [None, None, None, None];
    let mut keep = None;
    let mut limit = None;
    let mut offset = 0usize;
    if let Ok(dict) = obj.cast::<PyDict>() {
        for (key, value) in dict.iter() {
            let key: String = key.extract().map_err(|_| {
                PyValueError::new_err(
                    "probe dict keys must be strings (s, p, o, g, keep, limit, offset)",
                )
            })?;
            if value.is_none() {
                continue;
            }
            if let Some(column) = QuadColumn::from_name(&key) {
                terms[column.index()] = Some(value.extract()?);
                continue;
            }
            match key.as_str() {
                "keep" => keep = Some(value),
                "limit" => limit = Some(extract_count(&value, "limit")?),
                "offset" => offset = extract_count(&value, "offset")?,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown probe key {other:?}: expected s, p, o, g, keep, limit or offset"
                    )));
                }
            }
        }
    } else {
        let items: Vec<Bound<'_, PyAny>> = if let Ok(tuple) = obj.cast::<PyTuple>() {
            tuple.iter().collect()
        } else if let Ok(list) = obj.cast::<PyList>() {
            list.iter().collect()
        } else {
            return Err(PyValueError::new_err(
                "a probe is a (s, p, o, g) tuple of optional term strings or a dict with keys \
                 s, p, o, g, keep, limit, offset",
            ));
        };
        if items.len() != 4 {
            return Err(PyValueError::new_err(format!(
                "a probe tuple has four positions (s, p, o, g), got {}",
                items.len()
            )));
        }
        for (slot, item) in terms.iter_mut().zip(&items) {
            if !item.is_none() {
                *slot = Some(item.extract()?);
            }
        }
    }
    let [s, p, o, g] = terms;
    narrowed_probe(
        s.as_deref(),
        p.as_deref(),
        o.as_deref(),
        g.as_deref(),
        keep.as_ref(),
        limit,
        offset,
    )
}

/// A non-negative row count (`limit`, `offset`).
fn extract_count(value: &Bound<'_, PyAny>, name: &str) -> PyResult<usize> {
    value
        .extract::<usize>()
        .map_err(|_| PyValueError::new_err(format!("{name} must be a non-negative integer")))
}

/// The keeps of a `keep` argument: a dict from position (`"s"`, `"p"`,
/// `"o"`, `"g"`, or 0–3) to a code set or range, applied in `(s, p, o, g)`
/// order.
fn parse_keeps(obj: &Bound<'_, PyAny>) -> PyResult<Vec<(QuadColumn, Keep)>> {
    let Ok(dict) = obj.cast::<PyDict>() else {
        return Err(PyValueError::new_err(
            "keep must be a dict mapping a position (\"s\", \"p\", \"o\", \"g\" or 0-3) to a \
             code set (U64Column, a u64 buffer or a list of ints) or a code range (a range \
             with step 1, or a 2-tuple (lo, hi), the half-open codes lo <= code < hi; a \
             2-tuple is never a set)",
        ));
    };
    let mut keeps: Vec<(QuadColumn, Keep)> = Vec::with_capacity(dict.len());
    for (position, spec) in dict.iter() {
        let column = parse_position(&position)?;
        if keeps.iter().any(|(c, _)| *c == column) {
            return Err(PyValueError::new_err(format!(
                "keep names position {:?} twice",
                column.name()
            )));
        }
        keeps.push((column, parse_keep(&spec)?));
    }
    keeps.sort_by_key(|(column, _)| column.index());
    Ok(keeps)
}

fn parse_position(position: &Bound<'_, PyAny>) -> PyResult<QuadColumn> {
    if let Ok(name) = position.extract::<String>() {
        return QuadColumn::from_name(&name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown keep position {name:?}: expected \"s\", \"p\", \"o\" or \"g\""
            ))
        });
    }
    if let Ok(index) = position.extract::<usize>() {
        return QuadColumn::from_index(index).ok_or_else(|| {
            PyValueError::new_err(format!("keep position {index} is out of range 0-3"))
        });
    }
    Err(PyValueError::new_err(
        "a keep position is \"s\", \"p\", \"o\", \"g\" or an int 0-3",
    ))
}

/// One keep: a `range` with step 1 or a 2-tuple `(lo, hi)` of ints is a code
/// range, the half-open codes `lo <= code < hi`; a `U64Column`, a u64 buffer
/// or any other sequence of ints is a code set, read by [`extract_u64s`] and
/// shared as it stands when it is already ascending and unique. A 2-tuple is
/// never a set.
fn parse_keep(spec: &Bound<'_, PyAny>) -> PyResult<Keep> {
    let py = spec.py();
    let range_type = py.import("builtins")?.getattr("range")?;
    let bounds = if spec.is_instance(&range_type)? {
        let step: i64 = spec.getattr("step")?.extract()?;
        if step != 1 {
            return Err(PyValueError::new_err("a keep range must have step 1"));
        }
        Some((spec.getattr("start")?, spec.getattr("stop")?))
    } else if let Ok(tuple) = spec.cast::<PyTuple>()
        && tuple.len() == 2
    {
        Some((tuple.get_item(0)?, tuple.get_item(1)?))
    } else {
        None
    };
    if let Some((lo, hi)) = bounds {
        return Ok(Keep::range(code_bound(&lo)?..code_bound(&hi)?));
    }
    let codes = extract_u64s(spec).map_err(|e| {
        PyValueError::new_err(format!(
            "a keep is a code set (U64Column, a u64 buffer or a list of non-negative ints below \
             2**64) or a code range (a range with step 1, or a 2-tuple (lo, hi), the half-open \
             codes lo <= code < hi; a 2-tuple is never a set): {e}"
        ))
    })?;
    Ok(Keep::from(codes))
}

/// A range bound as a code: a non-negative int below 2**64.
fn code_bound(value: &Bound<'_, PyAny>) -> PyResult<TermCode> {
    let bound: i128 = value.extract().map_err(|_| {
        PyValueError::new_err("a keep range bound must be an integer from 0 to 2**64 - 1")
    })?;
    TermCode::try_from(bound).map_err(|_| {
        PyValueError::new_err(format!(
            "keep range bound {bound} is outside the code range 0 to 2**64 - 1"
        ))
    })
}
