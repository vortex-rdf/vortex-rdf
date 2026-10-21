//! Fixtures shared by the conformance tests and the example: a modular quad
//! set, a store on each backend, and the registration of a store view or
//! persisted child as a DataFusion table through vortex's own integration —
//! the path an external planner takes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef, UInt64Type};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream;
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_arrow::ArrowSessionExt as _;
use vortex_datafusion::v2::VortexTable;
use vortex_rdf_core::vortex_scan::DataSourceRef;
use vortex_rdf_core::{
    IndexType, LayoutStrategy, QuadColumn, RawQuad, TermCode, VortexRdfStore, vortex_session,
};

/// `n` quads over `n` subjects, `p_mod` predicates, `o_mod` literal objects
/// and `graphs` graphs (the default graph first).
pub fn modular_quads(n: usize, p_mod: usize, o_mod: usize, graphs: usize) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            let graph = match i % graphs {
                0 => GraphName::DefaultGraph,
                k => GraphName::NamedNode(
                    NamedNode::new(format!("http://example.org/g{k}")).unwrap(),
                ),
            };
            Quad::new(
                NamedOrBlankNode::NamedNode(
                    NamedNode::new(format!("http://example.org/s{i:05}")).unwrap(),
                ),
                NamedNode::new(format!("http://example.org/p{}", i % p_mod)).unwrap(),
                Term::Literal(Literal::new_simple_literal(format!("o{}", i % o_mod))),
                graph,
            )
        })
        .collect()
}

/// An in-memory store over `quads`.
pub async fn memory_store(
    quads: &[Quad],
    layout: LayoutStrategy,
    indexes: Vec<IndexType>,
) -> VortexRdfStore {
    let raws: Vec<_> = quads.iter().map(|q| Ok(RawQuad::from_quad(q))).collect();
    VortexRdfStore::from_quads(stream::iter(raws), layout, indexes)
        .await
        .expect("build store")
}

/// `quads` written to `store.vortex` under `dir`: the file a file-backed
/// store opens, mapped (`from_file`) or loaded whole
/// (`from_file_in_memory`).
pub async fn store_file(
    dir: &Path,
    quads: &[Quad],
    layout: LayoutStrategy,
    indexes: Vec<IndexType>,
) -> PathBuf {
    let path = dir.join("store.vortex");
    let raws: Vec<_> = quads.iter().map(|q| Ok(RawQuad::from_quad(q))).collect();
    vortex_rdf_core::io::quads_stream_to_vortex_file(stream::iter(raws), &path, layout, indexes)
        .await
        .expect("write store file");
    path
}

/// The Arrow schema a source's dtype maps to — the one to register the
/// table with, so DataFusion's schema and the scan's batches agree (vortex
/// maps `Utf8` to `Utf8View`).
pub fn arrow_schema(source: &DataSourceRef) -> SchemaRef {
    Arc::new(
        vortex_session()
            .arrow()
            .to_arrow_schema(source.dtype())
            .expect("a struct dtype"),
    )
}

/// A DataFusion context that keeps a single partition, so a scan's row
/// order survives into the results.
pub fn context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1))
}

/// `source` registered as the table `name` through vortex's table provider.
pub fn register(ctx: &SessionContext, name: &str, source: DataSourceRef) {
    let schema = arrow_schema(&source);
    ctx.register_table(
        name,
        Arc::new(VortexTable::new(source, vortex_session().clone(), schema)),
    )
    .expect("register table");
}

/// The batches `sql` produces.
pub async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute")
}

/// Every row of `sql` as [`TermCode`] columns (all result columns must be
/// `UInt64`).
pub async fn rows_u64(ctx: &SessionContext, sql: &str) -> Vec<Vec<TermCode>> {
    let mut rows = Vec::new();
    for batch in query(ctx, sql).await {
        let columns: Vec<&[TermCode]> = (0..batch.num_columns())
            .map(|c| {
                batch
                    .column(c)
                    .as_primitive::<UInt64Type>()
                    .values()
                    .as_ref()
            })
            .collect();
        for i in 0..batch.num_rows() {
            rows.push(columns.iter().map(|col| col[i]).collect());
        }
    }
    rows
}

/// The strings of `sql`'s single string column.
pub async fn strings(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in query(ctx, sql).await {
        let column = batch.column(0);
        if let Some(views) = column.as_string_view_opt() {
            out.extend(views.iter().map(|v| v.expect("non-null").to_string()));
        } else {
            let strs = column.as_string::<i32>();
            out.extend(strs.iter().map(|v| v.expect("non-null").to_string()));
        }
    }
    out
}

/// The one `i64` value `sql` produces (a `COUNT`).
pub async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = query(ctx, sql).await;
    let batch = batches.iter().find(|b| b.num_rows() > 0).expect("one row");
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

/// The filter vortex's scan node applies in `sql`'s physical plan — what
/// follows `filter=` on the plan's `VortexScanSource` line — or `None` when
/// no filter reached the scan.
pub async fn scan_filter(ctx: &SessionContext, sql: &str) -> Option<String> {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let scan = text
        .lines()
        .find(|line| line.contains("VortexScanSource"))
        .expect("a vortex scan node");
    scan.split_once("filter=")
        .map(|(_, filter)| filter.to_string())
}

/// A view's codes in base row order, through the Arrow-free export.
pub async fn view_codes(view: &VortexRdfStore) -> Vec<Vec<TermCode>> {
    use futures::StreamExt as _;
    let mut stream = view.code_chunks(&QuadColumn::ALL, 1 << 16).expect("codes");
    let mut rows = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        for i in 0..chunk[0].len() {
            rows.push(chunk.iter().map(|b| b[i]).collect());
        }
    }
    rows
}
