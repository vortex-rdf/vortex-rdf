use std::path::{Path, PathBuf};

use futures::StreamExt;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use pyo3::prelude::*;
use vortex_rdf_core::{
    VortexRdfStore,
    common::utils::{parse_graph_name, parse_named_node, parse_subject, parse_term},
    index::{ChainedHash, RdfDictionary, SimpleDictionary},
    io::CottasNativeStringStore,
    store::layout::{RdfQuadLayout, cottas::CottasLayout, flat::FlatLayout},
};

enum StoreKind {
    SimpleFlat(VortexRdfStore<SimpleDictionary, FlatLayout>),
    ChainedFlat(VortexRdfStore<ChainedHash, FlatLayout>),
    SimpleCottas(VortexRdfStore<SimpleDictionary, CottasLayout>),
    ChainedCottas(VortexRdfStore<ChainedHash, CottasLayout>),
    NativeStrings(CottasNativeStringStore),
}

#[pyclass]
pub struct VortexRdfFile {
    runtime: tokio::runtime::Runtime,
    inner: StoreKind,
}

#[pymethods]
impl VortexRdfFile {
    #[pyo3(signature = (subject = None, predicate = None, object = None, graph = None))]
    pub fn quads(
        &self,
        subject: Option<String>,
        predicate: Option<String>,
        object: Option<String>,
        graph: Option<String>,
    ) -> PyResult<Vec<(String, String, String, String)>> {
        let subject = parse_optional_subject(subject)?;
        let predicate = parse_optional_predicate(predicate)?;
        let object = parse_optional_object(object)?;
        let graph = parse_optional_graph(graph)?;

        self.runtime.block_on(async {
            match &self.inner {
                StoreKind::SimpleFlat(store) => {
                    collect_generic(
                        store,
                        subject.as_ref(),
                        predicate.as_ref(),
                        object.as_ref(),
                        graph.as_ref(),
                    )
                    .await
                }

                StoreKind::ChainedFlat(store) => {
                    collect_generic(
                        store,
                        subject.as_ref(),
                        predicate.as_ref(),
                        object.as_ref(),
                        graph.as_ref(),
                    )
                    .await
                }

                StoreKind::SimpleCottas(store) => {
                    collect_generic(
                        store,
                        subject.as_ref(),
                        predicate.as_ref(),
                        object.as_ref(),
                        graph.as_ref(),
                    )
                    .await
                }

                StoreKind::ChainedCottas(store) => {
                    collect_generic(
                        store,
                        subject.as_ref(),
                        predicate.as_ref(),
                        object.as_ref(),
                        graph.as_ref(),
                    )
                    .await
                }

                StoreKind::NativeStrings(store) => store
                    .match_rows(
                        subject.as_ref(),
                        predicate.as_ref(),
                        object.as_ref(),
                        graph.as_ref(),
                    )
                    .await
                    .map_err(to_py_runtime_err),
            }
        })
    }
}

async fn collect_quads<Dict, Layout>(
    path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> PyResult<Vec<(String, String, String, String)>>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    let store = VortexRdfStore::<Dict, Layout>::from_file(path)
        .await
        .map_err(to_py_runtime_err)?;

    let filtered = store
        .match_pattern(subject, predicate, object, graph)
        .await
        .map_err(to_py_runtime_err)?;

    let mut stream = filtered.quads().map_err(to_py_runtime_err)?;

    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let quad = item.map_err(to_py_runtime_err)?;

        rows.push((
            quad.subject.to_string(),
            quad.predicate.to_string(),
            quad.object.to_string(),
            quad.graph_name.to_string(),
        ));
    }

    Ok(rows)
}

fn parse_optional_subject(value: Option<String>) -> PyResult<Option<NamedOrBlankNode>> {
    value
        .map(|v| parse_subject(&v).map_err(to_py_value_err))
        .transpose()
}

fn parse_optional_predicate(value: Option<String>) -> PyResult<Option<NamedNode>> {
    value
        .map(|v| parse_named_node(&v).map_err(to_py_value_err))
        .transpose()
}

fn parse_optional_object(value: Option<String>) -> PyResult<Option<Term>> {
    value
        .map(|v| parse_term(&v).map_err(to_py_value_err))
        .transpose()
}

fn parse_optional_graph(value: Option<String>) -> PyResult<Option<GraphName>> {
    value
        .map(|v| parse_graph_name(&v).map_err(to_py_value_err))
        .transpose()
}

fn to_py_runtime_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}

fn to_py_value_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(err.to_string())
}

async fn collect_generic<Dict, Layout>(
    store: &VortexRdfStore<Dict, Layout>,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> PyResult<Vec<(String, String, String, String)>>
where
    Dict: RdfDictionary,
    Layout: RdfQuadLayout<Dict>,
{
    let filtered = store
        .match_pattern(subject, predicate, object, graph)
        .await
        .map_err(to_py_runtime_err)?;

    let mut stream = filtered.quads().map_err(to_py_runtime_err)?;

    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let quad = item.map_err(to_py_runtime_err)?;

        rows.push((
            quad.subject.to_string(),
            quad.predicate.to_string(),
            quad.object.to_string(),
            quad.graph_name.to_string(),
        ));
    }

    Ok(rows)
}

#[pymodule]
fn vortex_rdf_python(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<VortexRdfFile>()?;
    Ok(())
}
