//! A store view as a DataFusion table — the starting point for an engine
//! built on vortex-rdf: build (or open) a store, narrow it, register the
//! view and its index child, and query them with SQL over term codes.
//!
//! Run with `cargo run --manifest-path interface-tests/Cargo.toml --example
//! datafusion_table`.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use vortex_arrow::ArrowSessionExt as _;
use vortex_datafusion::v2::VortexTable;
use vortex_rdf_core::{IndexType, LayoutStrategy, QuadColumn, vortex_session};
use vortex_rdf_interface_tests::{memory_store, modular_quads};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = memory_store(
        &modular_quads(1_000, 4, 5, 3),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let dict = store.dict_reader().expect("a Dictionary-layout store");

    // Narrow the store natively first (a predicate pattern, served from the
    // index), then hand the view to DataFusion as a table of u32 codes.
    let p1 = oxrdf::NamedNode::new("http://example.org/p1")?;
    let view = store.match_pattern(None, Some(&p1), None, None).await?;
    let ctx = SessionContext::new();
    for (name, source) in [
        ("quads", store.data_source().await?),
        ("p1", view.data_source().await?),
        (
            "posg",
            store
                .component_data_source("index:posg")?
                .expect("the by-copy index child"),
        ),
    ] {
        let schema = Arc::new(vortex_session().arrow().to_arrow_schema(source.dtype())?);
        ctx.register_table(
            name,
            Arc::new(VortexTable::new(source, vortex_session().clone(), schema)),
        )?;
    }

    // A term-level constraint becomes a code range through the dictionary.
    let (lo, hi) = dict.prefix_range("<http://example.org/s001").await?;
    let batches = ctx
        .sql(&format!(
            "SELECT s, o FROM p1 WHERE s >= {lo} AND s < {hi} ORDER BY s LIMIT 5"
        ))
        .await?
        .collect()
        .await?;
    for batch in &batches {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::UInt32Type;
        let s = batch.column(0).as_primitive::<UInt32Type>();
        let o = batch.column(1).as_primitive::<UInt32Type>();
        let codes: Vec<u32> = s.values().iter().chain(o.values().iter()).copied().collect();
        let terms = dict.decode_many(&codes).await?;
        for i in 0..batch.num_rows() {
            println!(
                "{} {} {}",
                terms[i].as_deref().unwrap_or("?"),
                p1,
                terms[batch.num_rows() + i].as_deref().unwrap_or("?")
            );
        }
    }
    let _ = QuadColumn::ALL;
    Ok(())
}
