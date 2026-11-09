use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex_rdf_core::common::formats::{detect_format, format_from_name};
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::io::quads_stream_to_vortex_file;
use vortex_rdf_core::{IndexType, LayoutStrategy};

use crate::{detached, py_err};

/// Serialize an RDF file into a `.vortex` store file.
///
/// The options after the two paths are keyword-only. `format` is an RDF
/// format name ("ntriples", "nquads", "turtle", ...); when omitted it is
/// detected from the input file extension. `layout` and `indexes` take
/// core's canonical kebab-case strategy names (the spelling
/// `VortexRdfStore.layout()` reports); `layout` defaults to "dictionary",
/// the default shared by the JS bindings and the CLI. `indexes` lists the
/// secondary index components to build into the file ("secondary-by-copy",
/// "secondary-by-reference"). Rows are always written in global
/// (s, p, o, g) order, through the out-of-core sort that bounds peak memory
/// below the dataset size.
#[pyfunction]
#[pyo3(signature = (
    input_path,
    output_path,
    *,
    format=None,
    layout="dictionary",
    indexes=Vec::new(),
))]
pub fn serialize_rdf(
    py: Python<'_>,
    input_path: PathBuf,
    output_path: PathBuf,
    format: Option<&str>,
    layout: &str,
    indexes: Vec<String>,
) -> PyResult<()> {
    let layout: LayoutStrategy = layout.parse().map_err(py_err)?;
    let indexes = indexes
        .iter()
        .map(|name| name.parse::<IndexType>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(py_err)?;
    let format = match format {
        Some(name) => format_from_name(name).map_err(py_err)?,
        None => detect_format(Some(input_path.as_path())).ok_or_else(|| {
            PyValueError::new_err(
                "could not detect RDF format from the input file extension; pass format=",
            )
        })?,
    };

    detached(py, || async {
        let file = std::fs::File::open(&input_path)?;
        let quads = parse_quads_from_reader(file, format);
        quads_stream_to_vortex_file(quads, &output_path, layout, indexes).await
    })
}
