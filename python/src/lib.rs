use std::path::Path;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use std::sync::LazyLock;
use tokio::runtime::Runtime;
use vortex_rdf_core::common::utils::{parse_named_node, parse_subject, parse_term};
use vortex_rdf_core::io::{
    count_cottas_native_string_file, match_cottas_native_string_file_as_triples,
};

static PY_NATIVE_RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| Runtime::new().expect("failed to create Tokio runtime for vortex_rdf_native"));

#[pyfunction]
fn match_triples(
    path: String,
    subject_n3: Option<String>,
    predicate_n3: Option<String>,
    object_n3: Option<String>,
    layout: Option<String>,
) -> PyResult<Vec<(String, String, String)>> {
    let layout = layout.unwrap_or_else(|| "cottas-native-strings".to_string());

    if layout != "cottas-native-strings" {
        return Err(PyRuntimeError::new_err(format!(
            "Only cottas-native-strings is supported in the first binding version, got: {layout}"
        )));
    }

    let subject = subject_n3
        .as_deref()
        .map(parse_subject)
        .transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let predicate = predicate_n3
        .as_deref()
        .map(parse_named_node)
        .transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let object = object_n3
        .as_deref()
        .map(parse_term)
        .transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    PY_NATIVE_RUNTIME
        .block_on(match_cottas_native_string_file_as_triples(
            Path::new(&path),
            subject.as_ref(),
            predicate.as_ref(),
            object.as_ref(),
            None,
        ))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
fn count_triples(path: String, layout: Option<String>) -> PyResult<usize> {
    let layout = layout.unwrap_or_else(|| "cottas-native-strings".to_string());

    if layout != "cottas-native-strings" {
        return Err(PyRuntimeError::new_err(format!(
            "Only cottas-native-strings is supported, got: {layout}"
        )));
    }

    PY_NATIVE_RUNTIME
        .block_on(count_cottas_native_string_file(Path::new(&path)))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymodule]
fn vortex_rdf_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(match_triples, m)?)?;
    m.add_function(wrap_pyfunction!(count_triples, m)?)?;
    Ok(())
}
