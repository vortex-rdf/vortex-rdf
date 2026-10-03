//! The [`SortedStreamBuilder`] strategy: spill sorted runs to temporary
//! files, then K-way merge them into one global (s, p, o, g) order. The run
//! format is `spill`'s; the index families' side of the merge is
//! `stream_indexes`'.

use super::spill::{RunSpiller, TempRunsGuard};
use super::stream_indexes::{merge_feeding_indexes, merger_components};
use super::{
    BuiltArray, BuiltStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder, build_struct_array,
    chunk_stream,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, with_subject_stamp};
use crate::store::indexes::{IndexComponent, Indexes, unique_indexes};
use crate::store::layouts::dictionary::{TermCodeMap, TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};

use crate::debug;
use futures::{Stream, StreamExt, TryStreamExt};
use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;

/// Out-of-core globally sorted builder: sorted runs are spilled to disk and
/// K-way merged into fixed-size chunks, so peak memory is bounded by the
/// chunk size. With a secondary index requested, the quad merge runs eagerly
/// (row ids are assigned by the merge) while each index family's
/// `(value, row id)` entries are spilled as sorted runs; each family then
/// streams its child off its own merger beside the lazily re-read quad
/// chunks.
pub struct SortedStreamBuilder;

impl VortexArrayBuilder for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        build_array(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS).await
    }

    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS, None).await
    }
}

/// The chunk stream materialized into one array with its `s` column stamped
/// sorted, and the streamed index children materialized as in-memory
/// components. `chunk_size` is a test parameter; `build_vortex_array` passes
/// `DEFAULT_CHUNK_ROWS`.
pub(crate) async fn build_array(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltArray> {
    let start = debug::timer();
    let BuiltStream {
        dtype,
        chunks,
        components: writes,
        dict,
        ..
    } = build_chunk_stream(quad_stream, layout, indexes, chunk_size, None).await?;
    let chunks: Vec<ArrayRef> = chunks.try_collect().await?;
    let mut components = Vec::with_capacity(writes.len());
    for write in &writes {
        components.push(IndexComponent::from_write(write).await?);
    }
    let array = with_subject_stamp(chunked_or_single(chunks, dtype)?, true)?;
    log::debug!(
        "[SortedStreamBuilder] Materialized {} quads in {:?}",
        array.len(),
        debug::elapsed(start)
    );
    Ok(BuiltArray {
        array,
        components,
        dict,
    })
}

/// How merged quads become primary chunks: the string layouts' columns, or
/// u32 codes against the dictionary collected during ingest.
enum Encoding {
    Strings(LayoutStrategy),
    Codes {
        dict: Arc<TermDictionary>,
        code_map: Arc<TermCodeMap>,
    },
}

impl Encoding {
    fn strategy(&self) -> LayoutStrategy {
        match self {
            Encoding::Strings(layout) => *layout,
            Encoding::Codes { .. } => LayoutStrategy::Dictionary,
        }
    }

    /// `quads`, a window of the global merge, as one chunk with `s` stamped
    /// sorted.
    fn chunk(&self, quads: &[RawQuad]) -> Result<ArrayRef> {
        match self {
            Encoding::Strings(layout) => build_struct_array(quads, *layout, true),
            Encoding::Codes { code_map, .. } => dictionary::build_chunk(quads, code_map, true),
        }
    }

    fn dict(&self) -> Option<Arc<TermDictionary>> {
        match self {
            Encoding::Strings(_) => None,
            Encoding::Codes { dict, .. } => Some(Arc::clone(dict)),
        }
    }
}

/// External merge sort producing a lazily evaluated stream of sorted chunks.
/// Ingest to sorted runs on disk runs to completion before this returns.
/// Without indexes the K-way merge then produces chunks as the consumer
/// polls; with them the merge itself runs eagerly (it assigns the row ids
/// the index runs carry) and only chunk emission stays lazy. The temp run
/// files are removed when the stream is dropped. `spill_dir` pins where the
/// runs land; `None` takes [`TempRunsGuard::create`]'s default. `chunk_size`
/// is a test parameter; `build_vortex_stream` passes `DEFAULT_CHUNK_ROWS`.
pub(crate) async fn build_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
    spill_dir: Option<&Path>,
) -> Result<BuiltStream> {
    let start = debug::timer();
    let guard = Arc::new(TempRunsGuard::create("sorted_stream", spill_dir)?);
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);
    let mut spiller = RunSpiller::<RawQuad>::new(guard.path(), "quads", chunk_size);
    let mut ingested = 0usize;
    while let Some(res) = quads_in.next().await {
        let raw = res?;
        if let Some(b) = dict_builder.as_mut() {
            b.insert_quad(&raw);
        }
        spiller.push(raw)?;
        ingested += 1;
    }
    let merger = spiller.into_merger()?;
    let encoding = match dict_builder {
        Some(builder) => {
            let (dict, code_map) = builder.finish()?;
            Encoding::Codes {
                dict: Arc::new(dict),
                code_map: Arc::new(code_map),
            }
        }
        None => Encoding::Strings(layout),
    };
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads into {} runs in {:?} (dictionary={})",
        ingested,
        merger.run_count(),
        debug::elapsed(start),
        matches!(encoding, Encoding::Codes { .. })
    );

    let unique = unique_indexes(&indexes);
    let (merger, components) = if unique.is_empty() {
        (merger, Vec::new())
    } else {
        match &encoding {
            Encoding::Codes { code_map, .. } => {
                let codes = Arc::clone(code_map);
                let (merged, mergers) = merge_feeding_indexes(
                    merger,
                    guard.path(),
                    chunk_size,
                    &unique,
                    move |term| dictionary::code_of(&codes, term),
                )?;
                (
                    merged,
                    merger_components(mergers, chunk_size, &guard, true)?,
                )
            }
            Encoding::Strings(_) => {
                let (merged, mergers) =
                    merge_feeding_indexes(merger, guard.path(), chunk_size, &unique, |term| {
                        Ok(term.to_string())
                    })?;
                (
                    merged,
                    merger_components(mergers, chunk_size, &guard, false)?,
                )
            }
        }
    };

    let dict = encoding.dict();
    let strategy = encoding.strategy();
    let (dtype, chunks) = chunk_stream(
        (merger, guard),
        chunk_size,
        move |(merger, _guard), n| {
            let batch = merger.next_batch(n)?;
            if batch.is_empty() {
                return Ok(None);
            }
            encoding.chunk(&batch).map(Some)
        },
        || build_struct_array(&[], strategy, false),
    )?;
    Ok(BuiltStream::sorted(dtype, chunks, components, dict))
}
