//! Quad streaming: the chunk-granularity decode stream behind `quads`,
//! `quads_vec`, their shared-term twins and the raw export stream — one
//! pipeline for every row representation ([`ChunkDecode`]).

use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;

use crate::error::Result;
use crate::store::layouts::ChunkDecode;
use crate::store::scan::gather::gather_live;
use crate::store::{QuadsSource, RawQuad, SharedQuad, Tail, VortexRdfStore};

#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
#[cfg(feature = "file-io")]
use crate::store::layouts::ResolvedLayout;
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads;
#[cfg(feature = "file-io")]
use crate::store::view::selection::point_sized;
#[cfg(feature = "file-io")]
use futures::FutureExt as _;
#[cfg(feature = "file-io")]
use futures::future::{self, BoxFuture};
#[cfg(feature = "file-io")]
use vortex_array::ArrayRef;
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

/// A view's rows as decoded chunks.
type Chunks<T> = BoxStream<'static, Vec<Result<T>>>;

impl VortexRdfStore {
    // ── quads streaming ───────────────────────────────────────────────────────

    /// Stream every quad this view covers, one at a time: the base's rows (in
    /// the serving index's order for a served view, else in base row order),
    /// then the tail's live rows. Nothing is read until the first poll, and a
    /// failure to set the read up arrives as the stream's first item.
    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        Ok(Box::new(
            self.chunk_stream::<Quad>(true).flat_map(stream::iter),
        ))
    }

    /// Every quad this view covers, materialized into one exactly-sized
    /// `Vec`.
    ///
    /// Prefer this over `quads().try_collect()` when the whole result is
    /// wanted: `try_collect` grows its `Vec` one quad at a time, with the
    /// doubling reallocation copies that implies. Collecting the decoded
    /// chunks first makes the total length known before a single quad is
    /// moved.
    pub async fn quads_vec(&self) -> Result<Vec<Quad>> {
        self.decoded_vec::<Quad>().await
    }

    /// Every quad this view covers as [`SharedQuad`]s, materialized into one
    /// exactly-sized `Vec` — the same rows in the same order as
    /// [`quads_vec`](Self::quads_vec), with each term decoded once per
    /// distinct term of a chunk and shared by reference count across the
    /// rows repeating it, and no per-row parse into oxrdf terms.
    pub async fn shared_quads_vec(&self) -> Result<Vec<SharedQuad>> {
        self.decoded_vec::<SharedQuad>().await
    }

    /// The chunk-granularity stream of [`SharedQuad`]s behind
    /// [`shared_quads_vec`](Self::shared_quads_vec): each item is one decoded
    /// chunk (a scan split, a read by id, the in-memory base, or the tail),
    /// so a consumer converting whole batches — a binding interning each
    /// distinct term once — takes them without per-quad stream overhead. A
    /// chunk-level error, or a failure to set the read up, arrives as a
    /// one-element `vec![Err(..)]`.
    pub fn shared_quad_chunks(
        &self,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<SharedQuad>>> + Unpin + Send + '_>> {
        Ok(Box::new(self.chunk_stream::<SharedQuad>(true)))
    }

    /// The raw-text counterpart of
    /// [`shared_quad_chunks`](Self::shared_quad_chunks): each item is one
    /// chunk of this view's quads decoded to [`RawQuad`]s — every term in the
    /// exact N-Triples string form the columns store. The N-Triples/N-Quads
    /// export fast path streams these and writes the strings verbatim,
    /// skipping the oxrdf parse → re-escape round trip [`quads`](Self::quads)
    /// pays per term.
    ///
    /// A serve plan is deliberately not consulted here: it reorders rows for
    /// read locality, and this stream reads every view in base row order — a
    /// served view's pending selection materializes, and its rows are read as
    /// an unserved view's are.
    pub(in crate::store) fn raw_quad_chunks(&self) -> BoxStream<'_, Vec<Result<RawQuad>>> {
        self.chunk_stream::<RawQuad>(false)
    }

    /// Collect every decoded chunk, then flatten into one exactly-sized `Vec`.
    async fn decoded_vec<T: ChunkDecode>(&self) -> Result<Vec<T>> {
        let chunks: Vec<Vec<Result<T>>> = self.chunk_stream::<T>(true).collect().await;
        let total = chunks.iter().map(Vec::len).sum();
        let mut rows = Vec::with_capacity(total);
        for chunk in chunks {
            for row in chunk {
                rows.push(row?);
            }
        }
        Ok(rows)
    }

    /// The one chunk stream behind every read above:
    /// [`decoded_chunks`](Self::decoded_chunks) built on the first poll, a
    /// build failure as one error chunk. `serve` says whether a serve plan,
    /// when the view carries one, answers the read.
    fn chunk_stream<T: ChunkDecode>(&self, serve: bool) -> BoxStream<'_, Vec<Result<T>>> {
        stream::once(self.decoded_chunks::<T>(serve))
            .flat_map(|built| match built {
                Ok(chunks) => chunks,
                Err(e) => stream::iter([vec![Err(e)]]).boxed(),
            })
            .boxed()
    }

    /// This view's rows as decoded chunks: the base's, then the tail's live
    /// rows as one more chunk. Tombstones are applied on every path.
    ///
    /// With `serve`, a served view reads its plan's run: in memory a slice or
    /// point reads of the component's run; on file point reads for a
    /// point-sized located run, a row-count-split scan for a wide one, the
    /// plan's filter scan for an unlocated one. Every other read is in base
    /// row order, over exact row ids (a served view's pending selection
    /// materializes first): in memory the selected rows are gathered into one
    /// chunk; on file a selection the reads by id serve
    /// ([`ExactRead`](file_reads::ExactRead)) is one chunk, and anything else
    /// streams the restricted scan split by split.
    async fn decoded_chunks<T: ChunkDecode>(&self, serve: bool) -> Result<Chunks<T>> {
        // Tail rows are in memory and few: decode them eagerly, to be appended
        // after whatever the base yields.
        let tail: Vec<Result<T>> = self.tail.as_ref().map_or_else(Vec::new, Tail::decode);
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                serve: plan,
                ..
            } => {
                // The data is already in memory: the base decodes as one
                // chunk, the tail's rows appended to it.
                let mut quads = match plan {
                    // A served view reads the matched quads straight from the
                    // copy family's contiguous run in `base` (a plain slice),
                    // instead of gathering the primary columns at scattered row
                    // ids; tombstones are applied through the rid column. The
                    // selection may still be pending — the plan never needs it.
                    Some(plan) if serve => plan.decode::<T>(deleted.as_ref()),
                    _ => {
                        let materialized;
                        let selection = match plan {
                            // Base row order needs exact row ids: a served
                            // match's pending selection materializes here.
                            Some(_) => {
                                materialized = selection.materialized()?;
                                &materialized
                            }
                            // Without a plan the selection is exact (pending
                            // ids ride alongside one, or on a view that is only
                            // counted or windowed, never streamed).
                            None => selection.expect_exact(),
                        };
                        let rows = gather_live(base, selection, deleted.as_ref(), Some(probes))?;
                        T::decode_async(&self.layout, &rows).await
                    }
                };
                quads.extend(tail);
                Ok(stream::iter([quads]).boxed())
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                serve: plan,
                ..
            } => {
                // A served view streams from the answering index's own columns
                // instead: the index clusters the matching rows into a
                // contiguous run of zones, so the scan prunes to those rather
                // than scattering row-id reads across the whole file. The plan's
                // filter selects exactly the rows this view's selection names
                // (see `FileServePlan`); tombstones are applied through its rid
                // column. The store executes the plan without knowing which
                // index produced it.
                if let Some(plan) = plan
                    && serve
                {
                    // A small located run reads point-by-point through the
                    // component's cached chunk probes — one chunk, no scan
                    // machinery; the filter scan handles the rare mid-fetch
                    // decline.
                    if let Some(range) = plan.row_range()
                        && point_sized(range.end - range.start)
                    {
                        let scan = plan.projected_filtered_scan()?;
                        let projection = plan.projection();
                        let point = file_reads::component_point_chunk(
                            file,
                            plan.component(),
                            &projection,
                            range,
                        );
                        let chunk = match file_reads::point_rows_or_scan(point, scan).await {
                            Ok(rows) => {
                                plan.decode_columns_async::<T>(&rows, deleted.as_ref())
                                    .await
                            }
                            Err(e) => vec![Err(e)],
                        };
                        return Ok(with_tail(stream::iter([chunk]), tail));
                    }
                    // A wide located run scans exactly its rows, split by
                    // row count so the decode spreads over the runtime's
                    // workers; an unlocated one keeps the pushed-down filter
                    // scan (see `FileServePlan::located_run_scan`).
                    let scan = match plan.located_run_scan()? {
                        Some(scan) => scan,
                        None => plan.projected_filtered_scan()?,
                    };
                    let decoder = ServedDecoder {
                        plan: plan.clone(),
                        deleted: deleted.clone(),
                    };
                    return self.scan_chunks(scan, decoder, tail);
                }
                let materialized;
                let selection = match plan {
                    // Base row order needs exact row ids: a served match's
                    // pending selection materializes here (running the
                    // deferred index-child read once, cached on the view).
                    Some(_) => {
                        materialized = selection.materialized_async().await?;
                        &materialized
                    }
                    // Without a plan the selection is exact (pending ids ride
                    // alongside one, or on a view that is only counted or
                    // windowed, never streamed).
                    None => selection.expect_exact(),
                };
                // An exact selection the reads by id serve (see `ExactRead`) —
                // point reads through the file's cached chunk probes, or takes
                // at its ids — decodes as one chunk, with no scan machinery.
                // Anything else, and a read by id that declines (an
                // unprobeable chunk), streams the restricted scan: primary
                // columns only, with the pending filter and the selection
                // applied, tombstoned rows excluded.
                let columns = self.layout.strategy().primary_column_names();
                let chunk = match Self::exact_rows_by_id(
                    file,
                    columns,
                    filter.as_ref(),
                    selection,
                    deleted.as_ref(),
                )
                .await
                {
                    Ok(Some(rows)) => T::decode_async(&self.layout, &rows).await,
                    Ok(None) => {
                        let scan = self.restricted_file_scan(
                            file,
                            filter.as_ref(),
                            selection,
                            deleted.as_ref(),
                        )?;
                        return self.scan_chunks(scan, self.layout.clone(), tail);
                    }
                    Err(e) => vec![Err(e)],
                };
                Ok(with_tail(stream::iter([chunk]), tail))
            }
        }
    }

    /// `scan`'s chunks decoded through `decoder`, then the tail's chunk. The
    /// decode runs inside the scan's split tasks (via the scan's map
    /// function), so decoding runs concurrently across the runtime's workers
    /// instead of serially at the stream consumer — except under a
    /// file-backed dictionary, whose chunk decode awaits a read of the
    /// dictionary windows holding the chunk's codes, so it runs after the
    /// scan's stream. A chunk's read error is one error chunk.
    #[cfg(feature = "file-io")]
    fn scan_chunks<T: ChunkDecode>(
        &self,
        scan: ScanBuilder<ArrayRef>,
        decoder: impl ChunkDecoder<T>,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        #[cfg(test)]
        file_reads::scan_hooks::note();
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
