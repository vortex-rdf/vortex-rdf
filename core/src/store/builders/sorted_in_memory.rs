//! The [`SortedInMemoryBuilder`] strategy: hold the whole dataset and sort it
//! once by (s, p, o, g). Holding everything is what earns the global
//! sortedness the build claims (the `s` column's `IsSorted` stamp, and the
//! index children [`build_components`] builds over the sorted dataset), at
//! O(dataset) memory; the out-of-core `sorted_stream` gives the same guarantee
//! without it.
//!
//! Quads are unique and terms canonical in what this builder emits: adjacent
//! repeats are dropped right after the sort, before any column or index child
//! is built, so the rows, the row ids and every child describe the same
//! deduplicated dataset, and terms are interned by their canonical spelling.

use super::{BuiltArray, VortexArrayBuilder, build_components, build_struct_array};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::indexes::{IndexInput, Indexes};
use crate::store::layouts::LayoutStrategy;
use crate::store::layouts::dictionary::ingest::{InterningQuadBuilder, finish_interned};

use crate::debug;
use futures::{Stream, StreamExt};

/// The fully in-memory, globally sorted builder, the only one on
/// wasm32-unknown-unknown: it sorts every quad by (s, p, o, g) in memory,
/// builds the primary columns, and builds every requested index child over
/// the same sorted dataset, so each child is globally sorted and
/// binary-searchable.
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();

        // One StructArray of primary columns, each requested index's child
        // built beside it over the same sorted dataset. The Dictionary layout
        // interns terms as the stream drains, so the sort runs over 32-byte
        // coded rows and no `Vec<RawQuad>` (four owned Strings per quad)
        // accumulates.
        let built = if layout == LayoutStrategy::Dictionary {
            let interner = InterningQuadBuilder::from_stream(quad_stream).await?;
            finish_interned(interner, &indexes)?
        } else {
            let quads = ingest_and_sort(quad_stream).await?;
            BuiltArray {
                array: build_struct_array(&quads, layout, true)?,
                components: build_components(&indexes, IndexInput::Quads(&quads))?,
                dict: None,
            }
        };
        log::debug!(
            "[SortedInMemoryBuilder] Built {} quads in {:?}",
            built.array.len(),
            debug::elapsed(start)
        );
        Ok(built)
    }
}

/// Ingest the full quad stream and sort it globally by (s, p, o, g), keeping
/// each distinct quad once: equal quads are adjacent after the sort.
async fn ingest_and_sort(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
) -> Result<Vec<RawQuad>> {
    let mut quads: Vec<RawQuad> = Vec::new();
    while let Some(res) = quads_in.next().await {
        quads.push(res?);
    }
    log::debug!("[SortedInMemoryBuilder] Read {} quads", quads.len());

    let sort_start = debug::timer();
    quads.sort_unstable();
    let ingested = quads.len();
    quads.dedup();
    log::debug!(
        "[SortedInMemoryBuilder] Sorted quads in {:?}, dropped {} repeats",
        debug::elapsed(sort_start),
        ingested - quads.len()
    );

    Ok(quads)
}
