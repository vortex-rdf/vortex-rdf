//! Quad streaming: the chunk-granularity decode stream behind `quads`,
//! `quads_vec`, their shared-term twins and the N-Triples/N-Quads export's
//! stream, one pipeline for every row representation ([`ChunkDecode`]).

use futures::future;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;

use crate::common::quad::NQuadLines;
use crate::error::Result;
use crate::store::layouts::ChunkDecode;
use crate::store::{Backend, MemoryBase, SharedQuad, Tail, VortexRdfStore};

#[cfg(feature = "file-io")]
use super::deferred;
#[cfg(feature = "file-io")]
use super::file_plan::{CollectedPlan, FileReadScope, StreamedPlan};
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
use crate::store::scan::file_reads::{self, MaskedRows};
#[cfg(feature = "file-io")]
use crate::store::scan::file_scans::{SPLITS_IN_FLIGHT_PER_CORE, ScanDriver};
#[cfg(feature = "file-io")]
use crate::store::scan::read_plan::ExactRead;
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

    /// This view's chunks as N-Triples/N-Quads lines ([`NQuadLines`]), each
    /// chunk's rows written straight from its columns, every term in the
    /// N-Triples form they store (no oxrdf parse and re-escape per term, as
    /// [`quads`](Self::quads) pays): what the N-Triples/N-Quads export
    /// writes.
    ///
    /// Rows come in base row order: a serve plan, which orders rows by its
    /// index, is not consulted, so a served view's pending selection
    /// materializes and its rows are read as an unserved view's are.
    pub(in crate::store) fn nquad_line_chunks(&self) -> BoxStream<'_, Vec<Result<NQuadLines>>> {
        self.chunk_stream::<NQuadLines>(QuadRead::BaseOrder)
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
    /// rows in base row order, read as the file read's plan has it
    /// ([`file_plan`](crate::store::read::file_plan)), one-shot for a
    /// collected read and streamed otherwise: a filter's kept rows
    /// ([`masked_chunks`](Self::masked_chunks)), or a stream's split by split
    /// ([`decoded_split_reads`](Self::decoded_split_reads)); rows read by id
    /// ([`ExactRead`](crate::store::scan::read_plan::ExactRead)) as one chunk,
    /// and the restricted scan split by split
    /// ([`exact_chunks`](Self::exact_chunks)).
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
        let primary = self.layout.strategy().primary_column_names();
        if mode == QuadRead::Collected {
            return match self.collected_plan(base, primary).await? {
                CollectedPlan::Masked(masked) => self.masked_chunks(&masked, tail).await,
                CollectedPlan::Exact(scope, read) => self.exact_chunks(scope, read, tail).await,
            };
        }
        match self.streamed_plan(base, primary).await? {
            // Each split's mask evaluated as the stream reaches it, under the
            // row-scan driver.
            StreamedPlan::SplitReads(reads) => {
                let spawn = ScanDriver::for_row_scan(reads.len()) == ScanDriver::Spawned;
                Ok(self.decoded_split_reads(reads, spawn, tail))
            }
            StreamedPlan::Exact(scope, read) => self.exact_chunks(scope, read, tail).await,
        }
    }

    /// The chunks of `read` over `scope`, then the tail's chunk: rows read
    /// by id decode as one chunk, with no scan machinery; the restricted scan
    /// decodes split by split ([`scan_chunks`](Self::scan_chunks)).
    #[cfg(feature = "file-io")]
    async fn exact_chunks<T: ChunkDecode>(
        &self,
        scope: FileReadScope<'_>,
        read: ExactRead,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        match scope.rows(read).await? {
            BaseRead::Rows(rows) => Ok(with_tail(
                stream::iter([T::decode_async(&self.layout, &rows).await]),
                tail,
            )),
            BaseRead::Scan(scan) => self.scan_chunks(*scan, self.layout.clone(), tail),
        }
    }

    /// A one-shot read's chunks of the rows `masked` keeps (a filter's split
    /// masks), then the tail's chunk: point by point as one chunk, read
    /// inline, or else one chunk per split with a match
    /// ([`MaskedRows::split_reads`]). Those split reads are spawned onto the
    /// tokio runtime whenever there is more than one and a runtime is
    /// current, whatever the row-scan driver's split limit: the read is
    /// collected whole, so many splits' reads overlap their decodes, where a
    /// single split, read inline, spares a spawn's latency. Each chunk
    /// decodes inside its split's read
    /// ([`decoded_split_reads`](Self::decoded_split_reads)).
    #[cfg(feature = "file-io")]
    async fn masked_chunks<T: ChunkDecode>(
        &self,
        masked: &MaskedRows<'_>,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        if let Some(rows) = masked.points().await? {
            let chunk = T::decode_async(&self.layout, &rows).await;
            return Ok(with_tail(stream::iter([chunk]), tail));
        }
        let reads = masked.split_reads()?;
        let spawn = reads.len() > 1 && tokio::runtime::Handle::try_current().is_ok();
        Ok(self.decoded_split_reads(reads, spawn, tail))
    }

    /// The rows of split `reads` (one per split, in split order) decoded into
    /// chunks of `T`, then the tail's chunk: the reads spawned onto the
    /// runtime when `spawn`, [`SPLITS_IN_FLIGHT_PER_CORE`] per core in
    /// flight, or polled inline. Each chunk decodes inside its split's read,
    /// so spawned splits decode on the runtime's workers, a file-backed
    /// dictionary's awaiting its window reads there; a file-backed chunk of
    /// fewer than [`TASK_DECODE_MIN_ROWS`] rows decodes on the stream.
    #[cfg(feature = "file-io")]
    fn decoded_split_reads<T: ChunkDecode>(
        &self,
        tasks: Vec<file_reads::SplitRead>,
        spawn: bool,
        tail: Vec<Result<T>>,
    ) -> Chunks<T> {
        let window = SPLITS_IN_FLIGHT_PER_CORE * available_parallelism();
        let file_backed_dictionary =
            matches!(&self.layout, ResolvedLayout::Dictionary(access) if access.is_file_backed());
        let decoding: Vec<_> = tasks
            .into_iter()
            .map(|task| {
                let layout = self.layout.clone();
                async move {
                    let Some(rows) = task.await? else {
                        return Ok(None);
                    };
                    let chunk = if !file_backed_dictionary {
                        SplitChunk::Decoded(T::decode(&layout, &rows))
                    } else if rows.len() < TASK_DECODE_MIN_ROWS {
                        SplitChunk::Undecoded(rows)
                    } else {
                        SplitChunk::Decoded(T::decode_async(&layout, &rows).await)
                    };
                    Ok(Some(chunk))
                }
            })
            .collect();
        let rows = if spawn {
            spawned_splits(decoding, window).left_stream()
        } else {
            inline_splits(decoding).right_stream()
        };
        let layout = self.layout.clone();
        let chunks = rows
            .filter_map(|split| future::ready(split.transpose()))
            .then(move |chunk| finished_on_stream(&layout, chunk))
            .boxed();
        with_tail(chunks, tail)
    }

    /// `scan`'s chunks decoded through `decoder`, then the tail's chunk; a
    /// chunk's read error is one error chunk. The decode runs inside the
    /// scan's split tasks (its map function), concurrently across the
    /// runtime's workers. Those tasks cannot await, so under a file-backed
    /// dictionary they decode a chunk only where its window reads complete
    /// when first polled ([`ChunkDecoder::decode_now`]), as a mapped file's
    /// always do; a chunk whose reads would wait, or of fewer than
    /// [`TASK_DECODE_MIN_ROWS`] rows, passes on undecoded and decodes on the
    /// scan's stream.
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
            let in_task = decoder.clone();
            scan.map(move |chunk| {
                if chunk.len() < TASK_DECODE_MIN_ROWS {
                    return Ok(SplitChunk::Undecoded(chunk));
                }
                Ok(match in_task.decode_now(&chunk) {
                    Some(rows) => SplitChunk::Decoded(rows),
                    None => SplitChunk::Undecoded(chunk),
                })
            })
            .into_stream()?
            .then(move |chunk| finished_on_stream(&decoder, chunk))
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

/// The fewest rows a file-backed Dictionary chunk has for its split task to
/// decode it. A smaller chunk gains little from a worker and would decode
/// there against windows out of that core's cache, so it decodes on the
/// stream, on the thread that reads it.
#[cfg(feature = "file-io")]
const TASK_DECODE_MIN_ROWS: usize = 1024;

/// A chunk as a split task hands it on: decoded, or left for the stream to
/// decode (a file-backed dictionary's, when its reads would wait or it is
/// small).
#[cfg(feature = "file-io")]
enum SplitChunk<T> {
    Decoded(Vec<Result<T>>),
    Undecoded(ArrayRef),
}

/// `chunk` finished on the stream: decoded rows as they are, an undecoded
/// chunk decoded through `decoder` (awaiting a file-backed dictionary's
/// reads), an error as one error chunk.
#[cfg(feature = "file-io")]
fn finished_on_stream<T: ChunkDecode, D: ChunkDecoder<T>>(
    decoder: &D,
    chunk: vortex_error::VortexResult<SplitChunk<T>>,
) -> impl Future<Output = Vec<Result<T>>> + Send + use<T, D> {
    match chunk {
        Ok(SplitChunk::Decoded(rows)) => future::ready(rows).left_future(),
        Ok(SplitChunk::Undecoded(chunk)) => {
            #[cfg(test)]
            stream_decode_hooks::note(chunk.len());
            decoder.decode_async(chunk).right_future()
        }
        Err(e) => future::ready(vec![Err(VortexRdfError::Vortex(e))]).left_future(),
    }
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
    /// `chunk`'s rows where [`decode_async`](Self::decode_async) finishes
    /// without waiting ([`ChunkDecode::decode_now`]); `None` leaves the
    /// chunk to it.
    fn decode_now(&self, chunk: &ArrayRef) -> Option<Vec<Result<T>>>;
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

    fn decode_now(&self, chunk: &ArrayRef) -> Option<Vec<Result<T>>> {
        T::decode_now(self, chunk)
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

    fn decode_now(&self, chunk: &ArrayRef) -> Option<Vec<Result<T>>> {
        self.plan
            .decode_columns_now::<T>(chunk, self.deleted.as_ref())
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

/// Test hook: the row counts of the chunks this thread's streams decoded
/// themselves, their split tasks having left them undecoded.
#[cfg(all(test, feature = "file-io"))]
pub(crate) mod stream_decode_hooks {
    use std::cell::RefCell;

    thread_local! {
        static LEFT: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    }

    /// Record a chunk of `rows` rows decoded on a stream.
    pub(super) fn note(rows: usize) {
        LEFT.with_borrow_mut(|left| left.push(rows));
    }

    /// The row counts of the chunks decoded on this thread's streams so
    /// far, in order.
    pub(crate) fn left() -> Vec<usize> {
        LEFT.with_borrow(Vec::clone)
    }
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use crate::store::layouts::dictionary::{DictAccess, FileBackedDict};
    use crate::{LayoutStrategy, export_rdf};

    /// The rows, shared rows and N-Quads lines of `store`, each read
    /// whole, and the row counts of the chunks behind them that their
    /// streams decoded.
    async fn read_whole(
        store: &VortexRdfStore,
    ) -> ((Vec<Quad>, Vec<SharedQuad>, Vec<u8>), Vec<usize>) {
        let before = stream_decode_hooks::left().len();
        let quads = store.quads_vec().await.unwrap();
        let shared = store.shared_quads_vec().await.unwrap();
        let mut lines = Vec::new();
        export_rdf(store.clone(), &mut lines, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        let left = stream_decode_hooks::left().split_off(before);
        ((quads, shared, lines), left)
    }

    /// The chunks of the file at `path`, in scan order, by row count.
    async fn chunk_rows(path: &std::path::Path) -> Vec<usize> {
        let native = crate::tests::open_mapped(path).await;
        let chunks: Vec<ArrayRef> = native
            .scan()
            .unwrap()
            .into_stream()
            .unwrap()
            .map(|chunk| chunk.unwrap())
            .collect()
            .await;
        chunks.iter().map(|chunk| chunk.len()).collect()
    }

    /// A file-backed Dictionary store's chunks decode inside their split
    /// tasks: over a mapped file every window read completes when first
    /// polled, so no chunk of [`TASK_DECODE_MIN_ROWS`] rows or more is left
    /// to the stream, for the whole store, for a match read by its filter's
    /// masks a split at a time and for one an index serves by a scan of its
    /// run, and the rows are those of the store loaded in memory. A smaller
    /// chunk is left to the stream, and so is every chunk over dictionary
    /// reads that wait, decoded the same.
    #[tokio::test]
    async fn file_backed_chunks_decode_in_their_split_tasks() {
        // Each whole read is three: the rows, the shared rows, the export.
        for count in [600, 3000] {
            let quads = crate::tests::modular_quads(count, 7, 50);
            let (_dir, path) =
                crate::tests::write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
            assert_eq!(chunk_rows(&path).await, [count]);
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            let (want, _) = read_whole(&loaded).await;

            let mapped = VortexRdfStore::from_file(&path).await.unwrap();
            assert!(mapped.dict_reader().unwrap().is_file_backed());
            let (got, left) = read_whole(&mapped).await;
            assert!(got == want, "{count} mapped rows");
            let small: &[usize] = if count < TASK_DECODE_MIN_ROWS {
                &[count; 3]
            } else {
                &[]
            };
            assert_eq!(left, small, "{count} mapped rows");

            let native = crate::tests::open_mapped(&path).await;
            let waiting_dict = FileBackedDict::open_with_waiting_reads(&native)
                .await
                .unwrap()
                .expect("the written dictionary child must be window-searchable");
            let mut waiting = mapped.clone();
            waiting.layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(waiting_dict));
            let (got, left) = read_whole(&waiting).await;
            assert!(got == want, "{count} waiting rows");
            assert_eq!(left, [count; 3], "{count} waiting rows");
        }

        // A predicate's rows over a store without an index: its filter's
        // masks first, then the rows they keep, a split's read at a time,
        // which the export takes for 86 rows too (the one-shot reads take
        // so few point by point, as one chunk on the stream's thread).
        let p = oxrdf::NamedNode::new("http://example.org/p1").unwrap();
        for (count, modulus, small) in [(4000, 3, None), (600, 7, Some(86))] {
            let quads = crate::tests::modular_quads(count, modulus, 50);
            let (_dir, path) =
                crate::tests::write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            let loaded = loaded
                .match_pattern(None, Some(&p), None, None)
                .await
                .unwrap();
            let (want, _) = read_whole(&loaded).await;
            let mapped = VortexRdfStore::from_file(&path).await.unwrap();
            let filtered = mapped
                .match_pattern(None, Some(&p), None, None)
                .await
                .unwrap();
            assert!(matches!(
                filtered.quads.file(),
                Some(FileBase {
                    filter: crate::store::FileFilter::Pending(_),
                    serve: None,
                    ..
                })
            ));
            let (got, left) = read_whole(&filtered).await;
            assert!(got == want, "{count} filtered rows");
            assert_eq!(left, Vec::from_iter(small), "{count} filtered rows");
        }

        // The same rows as an index's run, too wide to point-read, scanned
        // as a chunk of 1,024 rows and one of 309.
        let quads = crate::tests::modular_quads(4000, 3, 50);
        let (_dir, path) = crate::tests::write_store_file(
            quads,
            LayoutStrategy::Dictionary,
            vec![crate::IndexType::SecondaryByCopy],
        )
        .await;
        let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
        let loaded = loaded
            .match_pattern(None, Some(&p), None, None)
            .await
            .unwrap();
        let (want, _) = read_whole(&loaded).await;
        let mapped = VortexRdfStore::from_file(&path).await.unwrap();
        let served = mapped
            .match_pattern(None, Some(&p), None, None)
            .await
            .unwrap();
        assert!(matches!(
            served.quads.file(),
            Some(FileBase { serve: Some(_), .. })
        ));
        let (got, left) = read_whole(&served).await;
        assert!(got == want, "served rows");
        assert!(
            !left.is_empty() && left.iter().all(|&rows| rows < TASK_DECODE_MIN_ROWS),
            "served chunks left to the stream: {left:?}"
        );
    }
}
