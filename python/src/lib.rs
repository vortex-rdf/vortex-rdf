use std::path::Path;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use oxrdfio::RdfFormat;
use std::sync::LazyLock;
use std::time::Instant;
use tokio::runtime::Runtime;
use vortex_rdf_core::common::utils::{parse_named_node, parse_subject, parse_term};
use vortex_rdf_core::io::{
    NativeIdsCountMode, count_cottas_native_ids_file_with_diagnostics_mode,
    diagnose_cottas_native_term_windows,
    count_cottas_native_string_file, match_cottas_native_file_as_compact_triples,
    match_cottas_native_file_as_triples, match_cottas_native_file_as_triples_optimized,
    match_cottas_native_file_with_diagnostics, match_cottas_native_string_file_as_triples,
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
            .block_on(match_cottas_native_file_as_triples_optimized(
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

// VORTEX_RDF_COMPACT_PYTHON_RESULT_V1
#[pyfunction]
fn match_triples_compact(
    path: String,
    subject_n3: Option<String>,
    predicate_n3: Option<String>,
    object_n3: Option<String>,
    layout: Option<String>,
) -> PyResult<(Vec<String>, Vec<(u32, u32, u32)>)> {
    let layout = layout.unwrap_or_else(|| "cottas-native-ids".to_string());
    if !matches!(layout.as_str(), "cottas-native-ids" | "cottas-native") {
        return Err(PyRuntimeError::new_err("compact results require a native-ID layout"));
    }
    let subject = subject_n3.as_deref().map(parse_subject).transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let predicate = predicate_n3.as_deref().map(parse_named_node).transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let object = object_n3.as_deref().map(parse_term).transpose()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let batch = PY_NATIVE_RUNTIME.block_on(match_cottas_native_file_as_compact_triples(
        Path::new(&path), subject.as_ref(), predicate.as_ref(), object.as_ref(), None,
    )).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok((batch.terms, batch.rows))
}

#[pyfunction]
fn diagnose_match<'py>(
    py: Python<'py>,
    path: String,
    subject_n3: Option<String>,
    predicate_n3: Option<String>,
    object_n3: Option<String>,
    layout: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let layout = layout.unwrap_or_else(|| "cottas-native-ids".to_string());
    if !matches!(layout.as_str(), "cottas-native-ids" | "cottas-native") {
        return Err(PyRuntimeError::new_err(
            "diagnose_match currently supports cottas-native-ids only",
        ));
    }

    let parse_start = Instant::now();
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
    let binding_parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    // Measure only the optimized diagnostic path. Running the legacy path first
    // would alter warm state and make the diagnostic timings non-causal.
    // Measure the optimized indexed path independently. The resulting N-Triples
    // bytes are retained only to expose materialization cost and output size.
    let optimized_start = Instant::now();
    let mut rdf_bytes = Vec::<u8>::new();
    let diagnostics = PY_NATIVE_RUNTIME
        .block_on(match_cottas_native_file_with_diagnostics(
            Path::new(&path),
            subject.as_ref(),
            predicate.as_ref(),
            object.as_ref(),
            None,
            &mut rdf_bytes,
            RdfFormat::NTriples,
        ))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let optimized_binding_ms = optimized_start.elapsed().as_secs_f64() * 1000.0;

    let out = PyDict::new(py);
    out.set_item("binding_parse_ms", binding_parse_ms)?;
    out.set_item("optimized_binding_ms", optimized_binding_ms)?;
    out.set_item("optimized_rdf_bytes", rdf_bytes.len())?;
    out.set_item("core_total_ms", diagnostics.total_ms)?;
    out.set_item(
        "binding_overhead_ms",
        (optimized_binding_ms - diagnostics.total_ms).max(0.0),
    )?;
    out.set_item("term_lookup_ms", diagnostics.term_lookup_ms)?;
    out.set_item("bound_term_count", diagnostics.term_to_id_stats.len())?;
    out.set_item(
        "term_open_ms",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.open_ms)
            .sum::<f64>(),
    )?;
    out.set_item(
        "term_can_prune_ms",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.can_prune_ms)
            .sum::<f64>(),
    )?;
    out.set_item(
        "term_scan_build_ms",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.scan_build_ms)
            .sum::<f64>(),
    )?;
    out.set_item(
        "term_read_all_ms",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.read_all_ms)
            .sum::<f64>(),
    )?;
    out.set_item(
        "term_extract_ms",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.extract_ms)
            .sum::<f64>(),
    )?;
    out.set_item(
        "term_strategies",
        diagnostics
            .term_to_id_stats
            .iter()
            .map(|s| s.strategy.as_str())
            .collect::<Vec<_>>()
            .join("+"),
    )?;
    out.set_item("open_ms", diagnostics.open_ms)?;
    out.set_item("scan_build_ms", diagnostics.scan_build_ms)?;
    out.set_item("read_all_ms", diagnostics.read_all_ms)?;
    out.set_item("id_extract_ms", diagnostics.id_extract_ms)?;
    out.set_item("id_to_term_lookup_ms", diagnostics.id_to_term_lookup_ms)?;
    out.set_item("serialize_ms", diagnostics.serialize_ms)?;
    out.set_item("rows_out", diagnostics.rows_out)?;
    out.set_item("scan_batches", diagnostics.scan_batches)?;
    out.set_item("scan_rows_materialized", diagnostics.scan_rows_materialized)?;
    out.set_item(
        "subject_range_index_used",
        diagnostics.subject_range_index_used,
    )?;
    out.set_item("po_exact_index_used", diagnostics.po_rowgroup_index_used)?;
    out.set_item("po_candidate_ranges", diagnostics.po_candidate_ranges)?;
    out.set_item("po_candidate_rows", diagnostics.po_candidate_rows)?;
    out.set_item("unique_ids_requested", diagnostics.unique_ids_requested)?;
    out.set_item("unique_ids_loaded", diagnostics.unique_ids_loaded)?;
    out.set_item(
        "id_to_term_strategy",
        diagnostics.id_to_term_stats.strategy.clone(),
    )?;
    out.set_item(
        "id_to_term_open_ms",
        diagnostics.id_to_term_stats.open_files_ms,
    )?;
    out.set_item(
        "id_to_term_sort_dedup_ms",
        diagnostics.id_to_term_stats.sort_dedup_ms,
    )?;
    out.set_item(
        "id_to_term_read_ms",
        diagnostics.id_to_term_stats.blob_read_ms,
    )?;
    out.set_item(
        "id_to_term_requested",
        diagnostics.id_to_term_stats.requested_ids_unique,
    )?;
    out.set_item("id_to_term_loaded", diagnostics.id_to_term_stats.ids_loaded)?;
    out.set_item("access_index_strategy", diagnostics.access_index_strategy)?;
    out.set_item("access_index_lookup_ms", diagnostics.access_index_lookup_ms)?;
    out.set_item(
        "access_candidate_ranges",
        diagnostics.access_candidate_ranges,
    )?;
    out.set_item("access_candidate_rows", diagnostics.access_candidate_rows)?;
    out.set_item(
        "access_execution_strategy",
        diagnostics.access_execution_strategy,
    )?;
    out.set_item(
        "access_original_range_count",
        diagnostics.access_original_range_count,
    )?;
    out.set_item(
        "access_executed_scan_count",
        diagnostics.access_executed_scan_count,
    )?;
    out.set_item("access_selected_rows", diagnostics.access_selected_rows)?;
    Ok(out)
}

#[pyfunction]
#[pyo3(signature = (path, term, window_sizes=None, runs=5))]
fn diagnose_term_windows<'py>(
    py: Python<'py>,
    path: String,
    term: String,
    window_sizes: Option<Vec<usize>>,
    runs: usize,
) -> PyResult<Bound<'py, PyDict>> {
    let window_sizes = window_sizes.unwrap_or_else(|| vec![256, 512, 1024, 2048]);
    // The Python API accepts RDFLib/N3 syntax. Canonicalize through the same
    // oxrdf parser and Display representation used by production lookup.
    let canonical_term = parse_term(&term)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        .to_string();
    let diagnostics = PY_NATIVE_RUNTIME
        .block_on(diagnose_cottas_native_term_windows(
            Path::new(&path),
            &canonical_term,
            &window_sizes,
            runs,
        ))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let out = PyDict::new(py);
    out.set_item("term", diagnostics.term)?;
    out.set_item("term_preview", diagnostics.term_preview)?;
    out.set_item("dictionary_rows", diagnostics.dictionary_rows)?;
    out.set_item("discovered_row", diagnostics.discovered_row)?;
    out.set_item("expected_id", diagnostics.expected_id)?;
    out.set_item("discovery_open_ms", diagnostics.discovery_open_ms)?;
    out.set_item("discovery_read_ms", diagnostics.discovery_read_ms)?;
    out.set_item("discovery_extract_ms", diagnostics.discovery_extract_ms)?;
    let trials = pyo3::types::PyList::empty(py);
    for trial in diagnostics.trials {
        let item = PyDict::new(py);
        item.set_item("strategy", trial.strategy)?;
        item.set_item("window_rows", trial.window_rows)?;
        item.set_item("row_start", trial.row_start)?;
        item.set_item("row_end", trial.row_end)?;
        item.set_item("run", trial.run)?;
        item.set_item("open_ms", trial.open_ms)?;
        item.set_item("scan_build_ms", trial.scan_build_ms)?;
        item.set_item("read_all_ms", trial.read_all_ms)?;
        item.set_item("extract_ms", trial.extract_ms)?;
        item.set_item("total_ms", trial.total_ms)?;
        item.set_item("result_rows", trial.result_rows)?;
        item.set_item("found_id", trial.found_id)?;
        trials.append(item)?;
    }
    out.set_item("trials", trials)?;
    Ok(out)
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
    m.add_function(wrap_pyfunction!(match_triples_compact, m)?)?;
    m.add_function(wrap_pyfunction!(diagnose_match, m)?)?;
    m.add_function(wrap_pyfunction!(diagnose_term_windows, m)?)?;
    m.add_function(wrap_pyfunction!(count_triples, m)?)?;
    Ok(())
}
