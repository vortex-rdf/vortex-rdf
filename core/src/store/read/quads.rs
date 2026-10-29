//! Quad streaming: the chunk-granularity decode stream behind `quads`,
//! `quads_vec`, their shared-term twins and the raw export stream, one
//! pipeline for every row representation ([`ChunkDecode`]).

use futures::future;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;

use crate::error::Result;
use crate::store::layouts::ChunkDecode;
use crate::store::{Backend, MemoryBase, RawQuad, SharedQuad, Tail, VortexRdfStore};

#[cfg(feature = "file-io")]
use std::sync::Arc;

#[cfg(feature = "file-io")]
use super::deferred;
#[cfg(feature = "file-io")]
use super::rows::BaseRead;
#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
#[cfg(feature = "file-io")]
use crate::io::read::{available_parallelism, inline_splits, spawned_splits};
#[cfg(feature = "file-io")]
use crate::store::FileBase;
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::layouts::ResolvedLayout;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter::SplitMasks;
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads;
#[cfg(feature = "file-io")]
use crate::store::scan::read_plan::MaskedRead;
#[cfg(feature = "file-io")]
use futures::FutureExt as _;
#[cfg(feature = "file-io")]
use futures::future::BoxFuture;
#[cfg(feature = "file-io")]
use vortex_array::ArrayRef;
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

/// A file view's rows as decoded chunks.
#[cfg(feature = "file-io")]
type Chunks<T> = BoxStream<'static, Vec<Result<T>>>;

/// How a read takes a view's rows: the three ways the readers above ask
/// for them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum QuadRead {
    /// Streamed, a served view's rows read off its serve plan's run
    /// (`quads`, `shared_quad_chunks`).
    Served,
    /// Streamed in base row order, a serve plan never consulted (the raw
    /// export).
    BaseOrder,
    /// Collected whole and served as [`Served`](Self::Served) is (`quads_vec`,
    /// `shared_quads_vec`): a one-shot read, which on file reads a pending
    /// filter's masks first.
    Collected,
}

impl QuadRead {
    /// Whether a serve plan, when the view carries one, answers the read.
    fn serves(self) -> bool {
        self != QuadRead::BaseOrder
    }
}

impl VortexRdfStore {
    // ── quads streaming ───────────────────────────────────────────────────────

    /// Stream every quad this view covers, one at a time: the base's rows (in
    /// the serving index's order for a served view, else in base row order),
    /// then the tail's live rows. Nothing is read until the first poll, and a
    /// failure to set the read up arrives as the stream's first item.
    pub fn quads(&self) -> Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_> {
        Box::new(
            self.chunk_stream::<Quad>(QuadRead::Served)
                .flat_map(stream::iter),
        )
    }

    /// Every quad this view covers, in [`quads`](Self::quads)' order, in one
    /// exactly-sized `Vec`: the decoded chunks are collected first, so the
    /// total is known before a quad is moved. Prefer it to
    /// `quads().try_collect()`, which grows its `Vec` one quad at a time.
    pub async fn quads_vec(&self) -> Result<Vec<Quad>> {
        self.decoded_vec::<Quad>().await
    }

    /// Every quad this view covers as [`SharedQuad`]s in one exactly-sized
    /// `Vec`, the rows and order of [`quads_vec`](Self::quads_vec): each
    /// distinct term of a chunk is decoded once and shared by reference count
    /// across the rows repeating it, with no per-row parse into oxrdf terms.
    pub async fn shared_quads_vec(&self) -> Result<Vec<SharedQuad>> {
        self.decoded_vec::<SharedQuad>().await
    }

    /// The chunk stream of [`SharedQuad`]s behind
    /// [`shared_quads_vec`](Self::shared_quads_vec): each item is one decoded
    /// chunk (a scan split, a read by id, a file view's tail, or a whole
    /// in-memory view), for a consumer that converts whole batches without
    /// per-quad stream overhead. A chunk's error, or a failure to set the
    /// read up, arrives as a one-element `vec![Err(..)]`.
    pub fn shared_quad_chunks(
        &self,
    ) -> Box<dyn Stream<Item = Vec<Result<SharedQuad>>> + Unpin + Send + '_> {
        Box::new(self.chunk_stream::<SharedQuad>(QuadRead::Served))
    }

    /// The raw-text counterpart of
    /// [`shared_quad_chunks`](Self::shared_quad_chunks): chunks of this view's
    /// quads as [`RawQuad`]s, every term in the N-Triples form the columns
    /// store, which the N-Triples/N-Quads export writes verbatim (no oxrdf
    /// parse and re-escape per term, as [`quads`](Self::quads) pays).
    ///
    /// Rows come in base row order: a serve plan, which orders rows by its
    /// index, is not consulted, so a served view's pending selection
    /// materializes and its rows are read as an unserved view's are.
    pub(in crate::store) fn raw_quad_chunks(&self) -> BoxStream<'_, Vec<Result<RawQuad>>> {
        self.chunk_stream::<RawQuad>(QuadRead::BaseOrder)
    }

    /// Collect every decoded chunk, then flatten into one exactly-sized `Vec`.
    /// An in-memory view is one chunk ([`in_memory_chunk`](Self::in_memory_chunk)),
    /// taken as it decodes, with no chunk stream around it; a file view's
    /// chunks are read as a one-shot read reads them (see `file_chunks`).
    async fn decoded_vec<T: ChunkDecode>(&self) -> Result<Vec<T>> {
        match &self.quads.backend {
            Backend::InMemory(memory) => {
                let chunk = self.in_memory_chunk::<T>(memory, QuadRead::Collected)?;
                flattened(chunk.len(), [chunk])
            }
            #[cfg(feature = "file-io")]
            Backend::File(_) => {
                let chunks: Vec<Vec<Result<T>>> =
                    self.chunk_stream::<T>(QuadRead::Collected).collect().await;
                flattened(chunks.iter().map(Vec::len).sum(), chunks)
            }
        }
    }

    /// The one chunk stream behind every read above, built on the first poll,
    /// a build failure as one error chunk: in memory the view's one chunk
    /// ([`in_memory_chunk`](Self::in_memory_chunk)), decoded without
    /// suspending; on file the chunks of `file_chunks`, read as `mode` asks.
    fn chunk_stream<T: ChunkDecode>(&self, mode: QuadRead) -> BoxStream<'_, Vec<Result<T>>> {
        #[cfg(test)]
        chunk_stream_hooks::note();
        match &self.quads.backend {
            Backend::InMemory(memory) => stream::once(future::lazy(move |_| {
                self.in_memory_chunk::<T>(memory, mode)
            }))
            .map(|chunk| chunk.unwrap_or_else(|e| vec![Err(e)]))
            .boxed(),
            #[cfg(feature = "file-io")]
            Backend::File(base) => deferred(self.file_chunks::<T>(base, mode), |e| vec![Err(e)]),
        }
    }

    /// An in-memory view's rows as one decoded chunk, `memory` its base, the
    /// tail's live rows appended, tombstones applied, without suspending: the
    /// rows are in memory and an in-memory store's dictionary is resident.
    ///
    /// When `mode` serves, a served view reads its plan's run, a slice or
    /// point reads of the component's run. Every other read is the base's rows
    /// in base row order, gathered over the exact selection
    /// ([`in_memory_base_rows`](Self::in_memory_base_rows)).
    fn in_memory_chunk<T: ChunkDecode>(
        &self,
        memory: &MemoryBase,
        mode: QuadRead,
    ) -> Result<Vec<Result<T>>> {
        let mut quads = match &memory.serve {
            // A served view decodes the matched quads from the index
            // component's contiguous run instead of gathering the primary
            // columns at scattered row ids; tombstones apply through the
            // run's rid column, and the selection may stay pending (the plan
            // never needs it).
            Some(plan) if mode.serves() => plan.decode::<T>(self.quads.deleted.as_ref()),
            _ => T::decode(&self.layout, &self.in_memory_base_rows(memory)?),
        };
        // Tail rows are in memory and few: they follow the base's.
        quads.extend(self.tail.as_ref().map_or_else(Vec::new, Tail::decode));
        Ok(quads)
    }

    /// A file view's rows as decoded chunks: the base's, then the tail's live
    /// rows as one more chunk. Tombstones are applied on every path.
    ///
    /// When `mode` serves, a served view reads its plan's run: point reads for a
    /// point-sized located run, a row-count-split scan for a wide one, the
    /// plan's filter scan for an unlocated one. Every other read is the base's
    /// rows in base row order, over the exact selection
    /// ([`base_read`](Self::base_read)): rows read by id
    /// ([`ExactRead`](crate::store::scan::read_plan::ExactRead)) are one chunk, and the
    /// restricted scan streams split by split. A collected read of a pending
    /// filter the reads by id leave to the scan reads the filter's masks
    /// first, then the rows they keep ([`masked_chunks`](Self::masked_chunks)),
    /// and reads a window's kept masks the same way.
    #[cfg(feature = "file-io")]
    async fn file_chunks<T: ChunkDecode>(
        &self,
        base: &FileBase,
        mode: QuadRead,
    ) -> Result<Chunks<T>> {
        let FileBase {
            file, serve: plan, ..
        } = base;
        let deleted = &self.quads.deleted;
        // Tail rows are in memory and few: decode them eagerly, to be appended
        // after whatever the base yields.
        let tail: Vec<Result<T>> = self.tail.as_ref().map_or_else(Vec::new, Tail::decode);
        // A served view streams from the answering index's own columns,
        // where the matching rows are one contiguous run of zones the scan
        // prunes to, rather than scattering row-id reads across the file.
        // The plan's filter selects exactly the rows this view's selection
        // names (see `FileServePlan`); tombstones apply through its rid
        // column.
        if let Some(plan) = plan
            && mode.serves()
        {
            // A small located run reads point by point, as one chunk.
            if let Some(read) = plan.point_run(file)? {
                let chunk = match read.await {
                    Ok(rows) => {
                        plan.decode_columns_async::<T>(&rows, deleted.as_ref())
                            .await
                    }
                    Err(e) => vec![Err(e)],
                };
                return Ok(with_tail(stream::iter([chunk]), tail));
            }
            // Any other run streams the plan's scan (see
            // `FileServePlan::scan`).
            let decoder = ServedDecoder {
                plan: plan.clone(),
                deleted: deleted.clone(),
            };
            return self.scan_chunks(plan.scan()?, decoder, tail);
        }
        if mode == QuadRead::Collected
            && let Some(masks) = &base.masks
        {
            return self
                .masked_chunks(file, masks, base.filter.as_ref(), tail)
                .await;
        }
        let primary = self.layout.strategy().primary_column_names();
        let (scope, read) = self.file_read(base, primary).await?;
        if mode == QuadRead::Collected
            && let Some(masks) = scope.masks_first(&read).await?
        {
            return self.masked_chunks(file, &masks, scope.filter, tail).await;
        }
        match scope.rows(read).await? {
            // Rows read by id decode as one chunk, with no scan machinery.
            BaseRead::Rows(rows) => Ok(with_tail(
                stream::iter([T::decode_async(&self.layout, &rows).await]),
                tail,
            )),
            BaseRead::Scan(scan) => self.scan_chunks(*scan, self.layout.clone(), tail),
        }
    }

    /// A one-shot read's chunks of the rows `masks` keep (a pending filter's
    /// split masks), then the tail's chunk, read as [`MaskedRead`] chooses:
    /// point by point as one chunk, read inline, or else one chunk per split
    /// with a match, taken at its ids or projected at its mask. Those split
    /// reads are spawned onto the tokio runtime whenever there is more than
    /// one and a runtime is current, whatever the row-scan driver's split
    /// limit: the read is collected whole, so many splits' reads overlap
    /// their decodes, where a single split, read inline, spares a spawn's
    /// latency. Each chunk decodes
    /// inside its split's task under a resident dictionary and on the stream
    /// under a file-backed one, as [`scan_chunks`](Self::scan_chunks)
    /// decodes. A point read a chunk declines is taken.
    #[cfg(feature = "file-io")]
    async fn masked_chunks<T: ChunkDecode>(
        &self,
        file: &Arc<NativeStoreFile>,
        masks: &SplitMasks,
        filter: Option<&vortex_array::expr::Expression>,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        let primary = self.layout.strategy().primary_column_names();
        // PROTO: the columns the filter pins are not read, but filled with
        // their code.
        let pinned: Arc<[(String, u64)]> = filter
            .and_then(crate::store::scan::file_exprs::eq_code_pairs)
            .unwrap_or_default()
            .into();
        let read_columns: Vec<&'static str> = primary
            .iter()
            .copied()
            .filter(|c| !pinned.iter().any(|(name, _)| name == c))
            .collect();
        let read = MaskedRead::of(file, masks)?;
        if read == MaskedRead::Points {
            let ids = masks.first_ids(usize::MAX);
            if let Some(rows) = file_reads::file_point_rows(file, &read_columns, None, ids).await? {
                let rows = file_reads::with_pinned(rows, primary, &pinned)?;
                let chunk = T::decode_async(&self.layout, &rows).await;
                return Ok(with_tail(stream::iter([chunk]), tail));
            }
        }
        let take = read != MaskedRead::Project;
        let (_, tasks) = file_reads::masked_split_tasks(file, &read_columns, masks, take)?;
        let tasks: Vec<file_reads::SplitRead> = tasks
            .into_iter()
            .map(|task| {
                let pinned = Arc::clone(&pinned);
                async move {
                    let rows = task.await?;
                    rows.map(|rows| {
                        file_reads::with_pinned(rows, primary, &pinned)
                            .map_err(|e| vortex_error::vortex_err!("{e}"))
                    })
                    .transpose()
                }
                .boxed()
            })
            .collect();
        let spawn = tasks.len() > 1 && tokio::runtime::Handle::try_current().is_ok();
        let window = 4 * available_parallelism();
        let layout = self.layout.clone();
        let file_backed_dictionary =
            matches!(&self.layout, ResolvedLayout::Dictionary(access) if access.is_file_backed());
        let chunks: Chunks<T> = if file_backed_dictionary {
            let rows = if spawn {
                spawned_splits(tasks, window).left_stream()
            } else {
                inline_splits(tasks).right_stream()
            };
            rows.filter_map(|split| future::ready(split.transpose()))
                .then(move |chunk| match chunk {
                    Ok(chunk) => ChunkDecoder::<T>::decode_async(&layout, chunk),
                    Err(e) => future::ready(vec![Err(VortexRdfError::Vortex(e))]).boxed(),
                })
                .boxed()
        } else {
            let decoding: Vec<_> = tasks
                .into_iter()
                .map(|task| {
                    let layout = layout.clone();
                    async move {
                        task.await
                            .map(|rows| rows.map(|rows| T::decode(&layout, &rows)))
                    }
                })
                .collect();
            let rows = if spawn {
                spawned_splits(decoding, window).left_stream()
            } else {
                inline_splits(decoding).right_stream()
            };
            rows.filter_map(|split| future::ready(split.transpose()))
                .map(|chunk| chunk.unwrap_or_else(|e| vec![Err(VortexRdfError::Vortex(e))]))
                .boxed()
        };
        Ok(with_tail(chunks, tail))
    }

    /// `scan`'s chunks decoded through `decoder`, then the tail's chunk; a
    /// chunk's read error is one error chunk. The decode runs inside the
    /// scan's split tasks (its map function), concurrently across the
    /// runtime's workers, except under a file-backed dictionary: that decode
    /// awaits a read of the dictionary windows holding the chunk's codes, so
    /// it runs on the scan's stream.
    #[cfg(feature = "file-io")]
    fn scan_chunks<T: ChunkDecode>(
        &self,
        scan: ScanBuilder<ArrayRef>,
        decoder: impl ChunkDecoder<T>,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        #[cfg(test)]
        crate::store::scan::file_scans::scan_hooks::note();
        let file_backed_dictionary =
            matches!(&self.layout, ResolvedLayout::Dictionary(access) if access.is_file_backed());
        let chunks: Chunks<T> = if file_backed_dictionary {
            scan.into_stream()?
                .then(move |chunk| match chunk {
                    Ok(chunk) => decoder.decode_async(chunk),
                    Err(e) => future::ready(vec![Err(VortexRdfError::Vortex(e))]).boxed(),
                })
                .boxed()
        } else {
            scan.map(move |chunk| Ok(decoder.decode(&chunk)))
                .into_stream()?
                .map(|chunk| match chunk {
                    Ok(rows) => rows,
                    Err(e) => vec![Err(VortexRdfError::Vortex(e))],
                })
                .boxed()
        };
        Ok(with_tail(chunks, tail))
    }
}

/// The `total` rows of `chunks`, in order, moved into one exactly-sized `Vec`;
/// a row's error is the result.
fn flattened<T>(total: usize, chunks: impl IntoIterator<Item = Vec<Result<T>>>) -> Result<Vec<T>> {
    let mut rows = Vec::with_capacity(total);
    for chunk in chunks {
        for row in chunk {
            rows.push(row?);
        }
    }
    Ok(rows)
}

/// `chunks` followed by the tail's one chunk.
#[cfg(feature = "file-io")]
fn with_tail<T: Send + 'static>(
    chunks: impl Stream<Item = Vec<Result<T>>> + Send + 'static,
    tail: Vec<Result<T>>,
) -> Chunks<T> {
    chunks.chain(stream::iter([tail])).boxed()
}

/// How a file read's scanned chunks decode into `T` rows: synchronously
/// inside the scan's split tasks, or awaiting a file-backed dictionary.
#[cfg(feature = "file-io")]
trait ChunkDecoder<T>: Clone + Send + Sync + 'static {
    /// `chunk`'s rows, decoded without suspending.
    fn decode(&self, chunk: &ArrayRef) -> Vec<Result<T>>;
    /// `chunk`'s rows, resolving a file-backed dictionary's codes with a read
    /// of its child.
    fn decode_async(&self, chunk: ArrayRef) -> BoxFuture<'static, Vec<Result<T>>>;
}

/// Chunks of the quad table's primary columns.
#[cfg(feature = "file-io")]
impl<T: ChunkDecode> ChunkDecoder<T> for ResolvedLayout {
    fn decode(&self, chunk: &ArrayRef) -> Vec<Result<T>> {
        T::decode(self, chunk)
    }

    fn decode_async(&self, chunk: ArrayRef) -> BoxFuture<'static, Vec<Result<T>>> {
        let layout = self.clone();
        async move { T::decode_async(&layout, &chunk).await }.boxed()
    }
}

/// Chunks of a serving index child's projected columns, the view's
/// tombstones applied through the child's rid column.
#[cfg(feature = "file-io")]
#[derive(Clone)]
struct ServedDecoder {
    plan: FileServePlan,
    deleted: Option<Mask>,
}

#[cfg(feature = "file-io")]
impl<T: ChunkDecode> ChunkDecoder<T> for ServedDecoder {
    fn decode(&self, chunk: &ArrayRef) -> Vec<Result<T>> {
        self.plan.decode_columns::<T>(chunk, self.deleted.as_ref())
    }

    fn decode_async(&self, chunk: ArrayRef) -> BoxFuture<'static, Vec<Result<T>>> {
        let served = self.clone();
        async move {
            served
                .plan
                .decode_columns_async::<T>(&chunk, served.deleted.as_ref())
                .await
        }
        .boxed()
    }
}

/// Test hook: how many chunk streams this thread's reads built (materializing
/// an in-memory view builds none).
#[cfg(test)]
pub(crate) mod chunk_stream_hooks {
    use std::cell::Cell;

    thread_local! {
        static STREAMS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one chunk stream.
    pub(super) fn note() {
        STREAMS.with(|c| c.set(c.get() + 1));
    }

    /// The chunk streams built on this thread so far.
    pub(crate) fn streams() -> usize {
        STREAMS.with(Cell::get)
    }
}
