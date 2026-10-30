//! The [`SortedStreamBuilder`] strategy: an external merge sort over term
//! ids. Every term is interned as it arrives ([`TermInterner`]), each quad
//! kept as four `u32` ids; full buffers are sorted in their terms' order and
//! spilled as fixed-width runs ([`runs`](super::runs)), then K-way merged back
//! into one global (s, p, o, g) order through the terms' ranks. It gives the
//! in-memory builder's guarantee (globally sorted `s` and index children)
//! with the dataset on disk as 16 bytes a quad; the requested indexes merge
//! from their own spilled `(ranks, row id)` runs and stream out as
//! components, never materialized whole
//! ([`stream_indexes`](super::stream_indexes)).
//!
//! Quads are unique and terms canonical in what this builder emits. A term is
//! interned by its canonical spelling, so equal ids are equal terms, and the
//! quad runs are spilled *distinct*: a quad that arrives more than once, in
//! one run or in several, is merged out once. Row ids are assigned by that
//! merge, after the repeats are gone, so every index family's records
//! describe the deduplicated rows and are unique, each carrying a row id of
//! its own; the families are spilled and merged as they are.
//!
//! The distinct terms are what this builder holds for the dataset, under
//! every layout: the interner during ingest, then the Dictionary layout's
//! term dictionary or, under the string layouts, the sorted terms the ranks
//! are spelled back from.

use super::interner::TermInterner;
use super::runs::QuadSpiller;
use super::spill::TempRunsGuard;
use super::stream_indexes::merge_feeding_indexes;
use super::{
    BuildSizes, BuiltArray, BuiltStream, ChunkStream, VortexArrayBuilder, build_struct_array,
};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, with_subject_stamp};
use crate::store::indexes::{IndexComponent, Indexes};
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary};
use crate::store::layouts::{LayoutStrategy, dictionary};
use crate::store::schema::TermCode;

use crate::debug;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::dtype::DType;

/// The out-of-core, globally sorted builder: an external merge sort, so a
/// dataset larger than memory builds. Sorted runs of term ids are spilled to
/// disk (`build_vortex_array` spills under `VORTEX_RDF_SPILL_DIR`, else the
/// OS temp dir), then K-way merged into fixed-size chunks.
///
/// With any secondary index requested, a first merge assigns the row ids
/// while each index family's `(ranks, row id)` entries are spilled as sorted
/// runs; the quad chunks then come off a second merge over the same runs,
/// beside each family's child streaming off its own merger. Without indexes
/// one lazy merge pass emits chunks as the consumer polls.
pub struct SortedStreamBuilder;

impl VortexArrayBuilder for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        build_array(quad_stream, layout, indexes, BuildSizes::DEFAULT).await
    }
}

/// The chunk stream materialized into one array, its `s` column stamped
/// sorted (every chunk is a window of the global merge, and assembling chunks
/// loses the per-chunk stats `match_pattern` gates its binary searches on),
/// and each streamed index child materialized as an in-memory component
/// ([`IndexComponent::from_stream`]).
pub(crate) async fn build_array(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    sizes: BuildSizes,
) -> Result<BuiltArray> {
    let start = debug::timer();
    let BuiltStream {
        dtype,
        chunks,
        components: children,
        dict,
        ..
    } = build_chunk_stream(quad_stream, layout, indexes, sizes, None).await?;
    let chunks: Vec<ArrayRef> = chunks.try_collect().await?;
    let mut components = Vec::with_capacity(children.len());
    for child in children {
        components.push(IndexComponent::from_stream(child).await?);
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

/// How merged rank quads become primary chunks: codes into the term
/// dictionary built from the ranked terms, or the string layout's columns
/// with each rank spelled back.
enum Encoding {
    Codes {
        dict: Arc<TermDictionary>,
    },
    Strings {
        layout: LayoutStrategy,
        terms: Arc<[Box<str>]>,
    },
}

impl Encoding {
    /// `layout`'s encoding of the build's terms, `sorted` in rank order:
    /// moved into the Dictionary layout's term dictionary, or kept to spell
    /// the ranks back.
    fn new(layout: LayoutStrategy, sorted: Vec<Box<str>>) -> Result<Self> {
        Ok(match layout {
            LayoutStrategy::Dictionary => Encoding::Codes {
                dict: Arc::new(TermDictionary::from_sorted_column(
                    VarBinViewArray::from_iter_str(sorted),
                )?),
            },
            layout => Encoding::Strings {
                layout,
                terms: Arc::from(sorted),
            },
        })
    }

    fn strategy(&self) -> LayoutStrategy {
        match self {
            Encoding::Strings { layout, .. } => *layout,
            Encoding::Codes { .. } => LayoutStrategy::Dictionary,
        }
    }

    /// `ranks`, a non-empty window of the global merge, as one chunk with
    /// `s` stamped sorted.
    fn chunk(&self, ranks: &[[u32; 4]]) -> Result<ArrayRef> {
        match self {
            Encoding::Codes { dict } => {
                let n = ranks.len();
                let mut codes = QuadCodes::with_capacity(n);
                for &[s, p, o, g] in ranks {
                    codes.push([
                        code_of_rank(dict, s)?,
                        code_of_rank(dict, p)?,
                        code_of_rank(dict, o)?,
                        code_of_rank(dict, g)?,
                    ]);
                }
                dictionary::build_code_chunk(&codes, 0..n, true)
            }
            Encoding::Strings { layout, terms } => {
                let quads = ranks
                    .iter()
                    .map(|&[s, p, o, g]| {
                        Ok(RawQuad {
                            s: term_of_rank(terms, s)?,
                            p: term_of_rank(terms, p)?,
                            o: term_of_rank(terms, o)?,
                            g: term_of_rank(terms, g)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                build_struct_array(&quads, *layout, true)
            }
        }
    }

    /// The dictionary that rides beside the chunks for the serializer.
    fn dict(&self) -> Option<Arc<TermDictionary>> {
        match self {
            Encoding::Strings { .. } => None,
            Encoding::Codes { dict } => Some(Arc::clone(dict)),
        }
    }
}

/// A term rank past the build's terms: only a corrupt spill record names one.
fn rank_error(rank: u32, terms: usize) -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "spill record names term rank {rank}, past the {terms} terms"
    ))
}

/// The dictionary code of the term of `rank`.
fn code_of_rank(dict: &TermDictionary, rank: u32) -> Result<TermCode> {
    let at = rank as usize;
    if at < dict.len() {
        Ok(dict.code_at(at))
    } else {
        Err(rank_error(rank, dict.len()))
    }
}

/// The spelling of the term of `rank`.
fn term_of_rank(terms: &[Box<str>], rank: u32) -> Result<String> {
    terms
        .get(rank as usize)
        .map(|term| term.to_string())
        .ok_or_else(|| rank_error(rank, terms.len()))
}

/// External merge sort into a lazily evaluated stream of sorted chunks, each
/// distinct quad once. Ingest into sorted runs completes before this returns:
/// no sorted output exists before all input is seen. Without secondary
/// indexes the K-way merge then builds chunks only as the consumer polls, at
/// a peak memory of the distinct terms plus a run buffer and a chunk; with
/// them a first merge runs to the end (it assigns the row ids the index runs
/// carry) and the chunks come lazily off a second. The run files are removed
/// when the stream is dropped.
///
/// `spill` is the output file's name and the directory to spill beside: a
/// build that writes a file passes its temp file's directory, beside the file
/// it writes or replaces, so the runs share the output's volume, in a run
/// directory named after the output. `None` takes
/// [`TempRunsGuard::create`]'s default.
pub(crate) async fn build_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    sizes: BuildSizes,
    spill: Option<(&OsStr, &Path)>,
) -> Result<BuiltStream> {
    let start = debug::timer();
    let (output, base) = spill.unzip();
    let guard = Arc::new(TempRunsGuard::create(output, base)?);

    let mut terms = TermInterner::new();
    let mut spiller = QuadSpiller::new(guard.path(), sizes.run_rows);
    let mut ingested = 0usize;
    while let Some(res) = quads_in.next().await {
        let ids = terms.intern_quad(res?)?;
        spiller.push(ids, &terms)?;
        ingested += 1;
    }
    let spilled = spiller.finish(&terms)?;
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads ({} distinct terms) into {} runs in {:?}",
        ingested,
        terms.len(),
        spilled.run_count(),
        debug::elapsed(start)
    );

    let freeze = debug::timer();
    let frozen = terms.finish();
    let rank_of = Arc::new(frozen.rank_of);
    let encoding = Encoding::new(layout, frozen.sorted)?;
    // A build that spilled nothing sorts its one run here, by rank.
    let runs = spilled.ranked(&rank_of)?;
    log::debug!(
        "[SortedStreamBuilder] Ranked {} terms in {:?}",
        rank_of.len(),
        debug::elapsed(freeze)
    );

    // Any requested index means a first pass over the merge: the index
    // children are globally sorted, which needs the row ids the merge
    // assigns before their entries can be sorted. Only the families the
    // requested types need are spilled, each once however often requested.
    let components = if indexes.is_empty() {
        Vec::new()
    } else {
        match &encoding {
            Encoding::Codes { dict } => {
                let dict = Arc::clone(dict);
                merge_feeding_indexes(&runs, &rank_of, &guard, sizes, &indexes, move |rank| {
                    code_of_rank(&dict, rank)
                })?
            }
            Encoding::Strings { terms, .. } => {
                let terms = Arc::clone(terms);
                merge_feeding_indexes(&runs, &rank_of, &guard, sizes, &indexes, move |rank| {
                    term_of_rank(&terms, rank)
                })?
            }
        }
    };

    let merger = runs.merger(&rank_of)?;
    let dict = encoding.dict();
    let strategy = encoding.strategy();
    let (dtype, chunks) = chunk_stream(
        (merger, guard),
        sizes.chunk_rows,
        move |(merger, _guard), n| {
            let batch = merger.next_batch(n)?;
            if batch.is_empty() {
                return Ok(None);
            }
            encoding.chunk(&batch).map(Some)
        },
        || build_struct_array(&[], strategy, false),
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components,
        quads_sorted: true,
        dict,
    })
}

/// The eager-first-chunk-then-unfold emission: `next` yields the next chunk
/// of at most `chunk_size` rows off `source`, `None` once exhausted, and
/// `empty` supplies the schema-carrying chunk of an empty dataset. The first
/// chunk is built before returning so the dtype is known up front; the rest
/// are built as polled.
fn chunk_stream<S: Send + 'static>(
    mut source: S,
    chunk_size: usize,
    mut next: impl FnMut(&mut S, usize) -> Result<Option<ArrayRef>> + Send + 'static,
    empty: impl FnOnce() -> Result<ArrayRef>,
) -> Result<(DType, ChunkStream)> {
    let first = match next(&mut source, chunk_size)? {
        Some(chunk) => chunk,
        None => empty()?,
    };
    let dtype = first.dtype().clone();
    let rest = stream::unfold((source, next), move |(mut source, mut next)| async move {
        match next(&mut source, chunk_size) {
            Ok(None) => None,
            Ok(Some(chunk)) => Some((Ok(chunk), (source, next))),
            Err(e) => Some((Err(e.into()), (source, next))),
        }
    });
    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}
