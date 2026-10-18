//! The [`SortedInMemoryBuilder`] strategy: hold the whole dataset, sort it
//! once by (s, p, o, g), and emit chunks as windows of that single order.
//!
//! Holding everything at once is what earns the global sortedness this
//! builder claims: the `s` column's `IsSorted` stamp, and the index children
//! it builds through [`build_components`] over the sorted dataset. The cost
//! is O(dataset) memory; the out-of-core strategy with the same guarantee is
//! [`sorted_stream`](super::sorted_stream). Only this file's ordering
//! discipline lives here — the emission machinery it drives belongs to
//! [`builders`](super).
//!
//! Quads are unique and terms are canonical in what this builder emits. The
//! sort is followed by dropping adjacent repeats, before any column or index
//! child is built, so the rows, the row ids and every child describe the
//! same deduplicated dataset. Terms are interned by the spelling the parser
//! gave them, which is the one canonical spelling of an RDF term.

use super::{BuiltArray, VortexArrayBuilder, build_components, build_struct_array};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::indexes::{IndexInput, Indexes};
use crate::store::layouts::LayoutStrategy;
use crate::store::layouts::dictionary::ingest::{InterningQuadBuilder, finish_interned};

use crate::debug;
use futures::{Stream, StreamExt};

/// Fully in-memory, globally sorted Vortex RDF Array Builder.
///
/// Sorts all quads in memory by (s, p, o, g), builds the primary columns, and
/// builds every requested index child (by copy and by reference) over the
/// same sorted dataset, so each child is globally sorted and
/// binary-searchable.
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();

        // Build a single contiguous StructArray of primary columns, with each
        // requested index's child built beside it over the same sorted
        // dataset: both the `s` column and every child are then globally
        // sorted.
        //
        // Dictionary layout interns terms as the stream drains, so the sort
        // runs over 32-byte coded rows and no `Vec<RawQuad>` (four owned
        // Strings per quad) ever accumulates.
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
