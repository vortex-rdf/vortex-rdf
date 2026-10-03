//! Quad streaming: the chunk-granularity decode stream behind `quads`,
//! `quads_vec`, their shared-term twins and the raw export stream.

use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;

use crate::error::Result;
use crate::store::layouts::ChunkDecode;
use crate::store::scan::gather::gather_live;
use crate::store::{QuadsSource, RawQuad, SharedQuad, VortexRdfStore};

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

    /// Stream every quad this view covers, one at a time: base rows (in the
    /// serving index's order for a served view, else base row order), then
    /// the tail's live rows. Nothing is read until the first poll.
    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        Ok(Box::new(
            self.chunk_stream::<Quad>(true).flat_map(stream::iter),
        ))
    }

    /// Every quad this view covers in one exactly-sized `Vec`.
    pub async fn quads_vec(&self) -> Result<Vec<Quad>> {
        self.decoded_vec::<Quad>().await
    }

    /// Every quad this view covers as [`SharedQuad`]s in one exactly-sized
    /// `Vec`: the same rows in the same order as [`quads_vec`](Self::quads_vec),
    /// each distinct term of a chunk decoded once and shared by reference
    /// count, with no per-row parse into oxrdf terms.
    pub async fn shared_quads_vec(&self) -> Result<Vec<SharedQuad>> {
        self.decoded_vec::<SharedQuad>().await
    }

    /// The chunk-granularity stream behind
    /// [`shared_quads_vec`](Self::shared_quads_vec): each item is one decoded
    /// chunk (a scan split, the in-memory base, or the tail). A chunk-level
    /// error arrives as a one-element `vec![Err(..)]`.
    pub fn shared_quad_chunks(
        &self,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<SharedQuad>>> + Unpin + Send + '_>> {
        Ok(Box::new(self.chunk_stream::<SharedQuad>(true)))
    }

    /// The raw-text counterpart of
    /// [`shared_quad_chunks`](Self::shared_quad_chunks): chunks of
    /// [`RawQuad`]s, every term in the N-Triples string form the columns
    /// store. A serve plan is not consulted: every view streams in base row
    /// order.
    pub(in crate::store) fn raw_quad_chunks(
        &self,
    ) -> Box<dyn Stream<Item = Vec<Result<RawQuad>>> + Unpin + Send + '_> {
        Box::new(self.chunk_stream::<RawQuad>(false))
    }

    /// Every decoded chunk collected, then flattened into one exactly-sized
    /// `Vec`.
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

    /// [`decoded_chunks`](Self::decoded_chunks) built on first poll; a build
    /// failure is one error chunk.
    fn chunk_stream<T: ChunkDecode>(
        &self,
        serve: bool,
    ) -> impl Stream<Item = Vec<Result<T>>> + Unpin + Send + '_ {
        stream::once(self.decoded_chunks::<T>(serve))
            .flat_map(|built| match built {
                Ok(chunks) => chunks,
                Err(e) => stream::iter([vec![Err(e)]]).boxed(),
            })
            .boxed()
    }

    /// This view's rows as decoded chunks: the base's, then the tail's.
    ///
    /// In memory the base is one chunk, read through the serve plan when
    /// `serve` and one is attached, else gathered in base row order (a pending
    /// selection materializes). On file a served view reads its plan's run
    /// (point reads for a point-sized located run, a row-count-split scan for
    /// a wide one, the plan's filter scan for an unlocated one); an unserved
    /// one reads a point-sized selection point by point and streams a scan's
    /// splits otherwise. Tombstones are applied on every path.
    async fn decoded_chunks<T: ChunkDecode>(&self, serve: bool) -> Result<Chunks<T>> {
        let tail: Vec<Result<T>> = self
            .tail
            .as_ref()
            .map_or_else(Vec::new, |tail| tail.decode());
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                serve: plan,
                ..
            } => {
                let mut quads = match plan {
                    Some(plan) if serve => plan.decode::<T>(deleted.as_ref()),
                    _ => {
                        let rows = gather_live(
                            base,
                            &selection.materialized()?,
                            deleted.as_ref(),
                            Some(probes),
                        )?;
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
                if let Some(plan) = plan
                    && serve
                {
                    let decoder = ServedDecoder {
                        plan: plan.clone(),
                        deleted: deleted.clone(),
                    };
                    if let Some(range) = plan.row_range()
                        && point_sized(range.end - range.start)
                    {
                        // The filter scan covers a chunk declining the probe.
                        let scan = plan.projected_filtered_scan()?;
                        let point = plan.point_chunk(file);
                        let chunk = match file_reads::point_rows_or_scan(point, scan).await {
                            Ok(rows) => decoder.decode_async(rows).await,
                            Err(e) => vec![Err(e)],
                        };
                        return Ok(with_tail(stream::iter([chunk]), tail));
                    }
                    let scan = match plan.located_run_scan()? {
                        Some(scan) => scan,
                        None => plan.projected_filtered_scan()?,
                    };
                    return self.scan_chunks(scan, decoder, tail);
                }
                let exact = selection.materialized_async().await?;
                if exact.is_point_sized() {
                    let rows = self.base_selected_rows().await?;
                    let chunk = T::decode_async(&self.layout, &rows).await;
                    return Ok(with_tail(stream::iter([chunk]), tail));
                }
                let scan = file_reads::restricted_scan(
                    file,
                    self.layout.strategy().primary_column_names(),
                    filter.as_ref(),
                    &exact,
                    deleted.as_ref(),
                )?;
                self.scan_chunks(scan, self.layout.clone(), tail)
            }
        }
    }

    /// `scan`'s chunks decoded through `decoder`, then the tail. Under a
    /// file-backed dictionary the decode awaits a dictionary read per chunk,
    /// so it runs after the scan's stream; otherwise it runs inside the scan's
    /// split tasks. A chunk's read error is one error chunk.
    #[cfg(feature = "file-io")]
    fn scan_chunks<T: ChunkDecode>(
        &self,
        scan: ScanBuilder<ArrayRef>,
        decoder: impl ChunkDecoder<T>,
        tail: Vec<Result<T>>,
    ) -> Result<Chunks<T>> {
        let file_backed_dictionary =
            matches!(&self.layout, ResolvedLayout::Dictionary(access) if access.is_file_backed());
        let chunks: Chunks<T> = if file_backed_dictionary {
            scan.into_stream()
                .map_err(VortexRdfError::Vortex)?
                .then(move |chunk| match chunk {
                    Ok(chunk) => decoder.decode_async(chunk),
                    Err(e) => future::ready(vec![Err(VortexRdfError::Vortex(e))]).boxed(),
                })
                .boxed()
        } else {
            scan.map(move |chunk| Ok(decoder.decode(&chunk)))
                .into_stream()
                .map_err(VortexRdfError::Vortex)?
                .map(|chunk| match chunk {
                    Ok(quads) => quads,
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

/// Decodes one scanned chunk into `T` rows.
#[cfg(feature = "file-io")]
trait ChunkDecoder<T>: Clone + Send + Sync + 'static {
    fn decode(&self, chunk: &ArrayRef) -> Vec<Result<T>>;
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
/// tombstones applied by rid.
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
