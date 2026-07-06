use std::path::Path;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use std::sync::LazyLock;
use tokio::runtime::Runtime;
use vortex_rdf_core::common::utils::{parse_named_node, parse_subject, parse_term};
use vortex_rdf_core::io::{
    NativeIdsCountMode, count_cottas_native_ids_file_with_diagnostics_mode,
    count_cottas_native_string_file, match_cottas_native_file_as_triples,
    match_cottas_native_string_file_as_triples,
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

    match layout.as_str() {
        "cottas-native-strings" => PY_NATIVE_RUNTIME
            .block_on(match_cottas_native_string_file_as_triples(
                Path::new(&path),
                subject.as_ref(),
                predicate.as_ref(),
                object.as_ref(),
                None,
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string())),

        "cottas-native-ids" | "cottas-native" => PY_NATIVE_RUNTIME
            .block_on(match_cottas_native_file_as_triples(
                Path::new(&path),
                subject.as_ref(),
                predicate.as_ref(),
                object.as_ref(),
                None,
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string())),

        other => Err(PyRuntimeError::new_err(format!(
            "Unsupported native Vortex RDF layout: {other}"
        ))),
    }
}

#[pyfunction]
fn count_triples(path: String, layout: Option<String>) -> PyResult<usize> {
    let layout = layout.unwrap_or_else(|| "cottas-native-strings".to_string());

    match layout.as_str() {
        "cottas-native-strings" => PY_NATIVE_RUNTIME
            .block_on(count_cottas_native_string_file(Path::new(&path)))
            .map_err(|e| PyRuntimeError::new_err(e.to_string())),

        "cottas-native-ids" | "cottas-native" => {
            let diag = PY_NATIVE_RUNTIME
                .block_on(count_cottas_native_ids_file_with_diagnostics_mode(
                    Path::new(&path),
                    None,
                    None,
                    None,
                    None,
                    NativeIdsCountMode::RowsOnly,
                ))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            Ok(diag.count)
        }

        other => Err(PyRuntimeError::new_err(format!(
            "Unsupported native Vortex RDF layout: {other}"
        ))),
    }
}

#[pymodule]
fn vortex_rdf_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(match_triples, m)?)?;
    m.add_function(wrap_pyfunction!(count_triples, m)?)?;
    Ok(())
}
