//! Quad streaming: the chunk-granularity decode stream behind `quads`,
//! `quads_vec` and their shared-term twins.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::scan::gather::gather_live;
#[cfg(feature = "file-io")]
use crate::store::selection::point_sized;
use crate::store::{QuadsSource, RawQuad, SharedQuad};

#[cfg(feature = "file-io")]
use futures::FutureExt as _;
#[cfg(feature = "file-io")]
use futures::future::{self, BoxFuture};
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;

use vortex_array::ArrayRef;
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;

use super::VortexRdfStore;

impl VortexRdfStore {
    // ── quads streaming ───────────────────────────────────────────────────────

    /// Stream every quad this view covers, one at a time.
    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        Ok(Box::new(
            self.decoded_chunks::<Quad>()?.flat_map(stream::iter),
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
    /// chunk (a scan split, the in-memory base, or the tail), so a consumer
    /// converting whole batches — a binding interning each distinct term
    /// once — takes them without per-quad stream overhead. A chunk-level
    /// scan error arrives as a one-element `vec![Err(..)]`.
    pub fn shared_quad_chunks(
        &self,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<SharedQuad>>> + Unpin + Send + 'static>> {
        self.decoded_chunks::<SharedQuad>()
    }

    /// Collect every decoded chunk, then flatten into one exactly-sized `Vec`.
    async fn decoded_vec<T: ChunkDecode>(&self) -> Result<Vec<T>> {
        let chunks: Vec<Vec<Result<T>>> = self.decoded_chunks::<T>()?.collect().await;
        let total = chunks.iter().map(Vec::len).sum();
        let mut rows = Vec::with_capacity(total);
        for chunk in chunks {
            for row in chunk {
                rows.push(row?);
            }
        }
        Ok(rows)
    }

    /// The decode-granularity stream behind [`quads`](Self::quads),
    /// [`quads_vec`](Self::quads_vec) and their shared-term twins: each item
    /// is one decoded chunk (a scan split, the in-memory base, or the tail),
    /// so consumers that want whole batches can take them without paying
    /// per-quad stream overhead. A chunk-level scan error arrives as a
    /// one-element `vec![Err(..)]`. The stream owns everything it reads
    /// (decoded rows, or a scan over the file handle), so it outlives the
    /// store handle it was taken from.
    fn decoded_chunks<T: ChunkDecode>(
        &self,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<T>>> + Unpin + Send + 'static>> {
        let layout = self.layout.clone();
        // Tail rows are in memory and few: decode them eagerly, to be appended
        // after whatever the base yields.
        let tail_quads: Vec<Result<T>> = match &self.tail {
            None => vec![],
            Some(tail) => T::decode(&self.tail_layout(), &tail.live_rows()?),
        };
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                serve,
                ..
            } => {
                // The data is already in memory. Decode the base rows up front,
                // then hand back a simple iterator wrapped as a stream.
                let mut quads = match serve {
                    // A served view reads the matched quads straight from the
                    // copy family's contiguous run in `base` (a plain slice),
                    // instead of gathering the primary columns at scattered row
                    // ids; tombstones are applied through the rid column. The
                    // selection may still be pending — the plan never needs it.
                    Some(serve) => serve.decode::<T>(deleted.as_ref()),
                    // Without a plan the selection is exact (pending ids only
                    // ever ride alongside one).
                    None => T::decode(
                        &layout,
                        &gather_live(
                            base,
                            selection.expect_exact(),
                            deleted.as_ref(),
                            Some(probes),
                        )?,
                    ),
                };
                quads.extend(tail_quads);
                Ok(Box::new(stream::iter([quads])))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                serve,
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
                if let Some(serve) = serve {
                    // A small located run reads point-by-point through the
                    // component's cached chunk probes — one async item, no
                    // scan machinery; the moved-in filter scan handles the
                    // rare mid-fetch decline.
                    if let Some(range) = serve.row_range()
                        && point_sized(range.end - range.start)
                    {
                        let scan = serve.projected_filtered_scan()?;
                        let serve = serve.clone();
                        let deleted = deleted.clone();
                        let file = std::sync::Arc::clone(file);
                        let chunk = async move {
                            let projection = serve.projection();
                            let point = file_scan::component_point_chunk(
                                &file,
                                serve.component(),
                                &projection,
                                range,
                            );
                            match file_scan::point_rows_or_scan(point, scan).await {
                                Ok(rows) => {
                                    serve
                                        .decode_columns_async::<T>(&rows, deleted.as_ref())
                                        .await
                                }
                                Err(e) => vec![Err(e)],
                            }
                        };
                        return Ok(Box::new(
                            stream::once(chunk)
                                .chain(stream::iter([tail_quads]))
                                .boxed(),
                        ));
                    }
                    let serve = serve.clone();
                    let deleted = deleted.clone();
                    // A wide located run scans exactly its rows, split by
                    // row count so the decode below spreads over the
                    // runtime's workers; an unlocated one keeps the
                    // pushed-down filter scan (see
                    // `FileServePlan::located_run_scan`).
                    let scan = match serve.located_run_scan()? {
                        Some(scan) => scan,
                        None => serve.projected_filtered_scan()?,
                    };
                    let (sync_serve, sync_deleted) = (serve.clone(), deleted.clone());
                    return self.scan_chunks(
                        scan,
                        move |chunk| sync_serve.decode_columns::<T>(&chunk, sync_deleted.as_ref()),
                        move |chunk| {
                            let serve = serve.clone();
                            let deleted = deleted.clone();
                            async move {
                                serve
                                    .decode_columns_async::<T>(&chunk, deleted.as_ref())
                                    .await
                            }
                            .boxed()
                        },
                        tail_quads,
                    );
                }
                // A tiny exact selection materializes point-by-point through
                // the file's cached chunk probes — one async item, no scan
                // machinery. Candidacy (small selection, eq-shaped filter) is
                // checked synchronously; the fallback scan moves into the
                // future for the rare mid-fetch decline (an unprobeable
                // chunk), which reads the same few rows through the scan.
                let exact = selection.expect_exact();
                if exact.is_point_sized()
                    && filter
                        .as_ref()
                        .is_none_or(|f| file_scan::eq_code_pairs(f).is_some())
                {
                    let scan =
                        self.restricted_file_scan(file, filter.as_ref(), exact, deleted.as_ref())?;
                    let file = std::sync::Arc::clone(file);
                    let columns = self.layout.strategy().primary_column_names();
                    let filter = filter.clone();
                    let selection = exact.clone();
                    let deleted = deleted.clone();
                    let chunk = async move {
                        let point = file_scan::file_point_rows(
                            &file,
                            columns,
                            filter.as_ref(),
                            &selection,
                            deleted.as_ref(),
                        );
                        match file_scan::point_rows_or_scan(point, scan).await {
                            Ok(rows) => T::decode_async(&layout, &rows).await,
                            Err(e) => vec![Err(e)],
                        }
                    };
                    return Ok(Box::new(
                        stream::once(chunk)
                            .chain(stream::iter([tail_quads]))
                            .boxed(),
                    ));
                }
                // Same restriction setup as `base_selected_rows`: primary
                // columns only, with any pending filter/selection applied
                // (tombstoned rows excluded). Without a serve plan the
                // selection is exact (pending ids only ever ride alongside
                // one).
                let scan = self.restricted_file_scan(
                    file,
                    filter.as_ref(),
                    selection.expect_exact(),
                    deleted.as_ref(),
                )?;
                let sync_layout = layout.clone();
                self.scan_chunks(
                    scan,
                    move |chunk| T::decode(&sync_layout, &chunk),
                    move |chunk| {
                        let layout = layout.clone();
                        async move { T::decode_async(&layout, &chunk).await }.boxed()
                    },
                    tail_quads,
                )
            }
        }
    }

    /// The raw-text counterpart of
    /// [`shared_quad_chunks`](Self::shared_quad_chunks): each
    /// item is one chunk of this view's quads decoded to [`RawQuad`]s — every
    /// term in the exact N-Triples string form the columns store. The
    /// N-Triples/N-Quads export fast path streams these and writes the
    /// strings verbatim, skipping the oxrdf parse → re-escape round trip
    /// [`quads`](Self::quads) pays per term.
    ///
    /// A serve plan is deliberately not consulted here: it reorders rows for
    /// read locality, and this stream's consumers are order-insignificant
    /// line formats — the restricted scan answers every file view with the
    /// same quads in base row order.
    pub(super) async fn raw_quad_chunks(
        &self,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<RawQuad>>> + Unpin + Send + '_>> {
        // Tail rows are in memory and few: decode them eagerly, to be
        // appended after whatever the base yields. Under the Dictionary
        // layout the tail already holds N-Triples strings (Default layout),
        // so this never needs the async dictionary path.
        let tail_raws: Vec<Result<RawQuad>> = match &self.tail {
            None => vec![],
            Some(tail) => raw_chunk(&self.tail_layout(), &tail.live_rows()?),
        };
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                ..
            } => {
                // This stream reads in base row order, which only exact row
                // ids can produce — a served match's pending selection
                // materializes here.
                let rows = gather_live(
                    base,
                    &selection.materialized()?,
                    deleted.as_ref(),
                    Some(probes),
                )?;
                // `base_raw_quads` routes through the async decode when the
                // dictionary is file-backed, exactly like the other
                // raws-consuming paths (serialization, compaction).
                let mut raws: Vec<Result<RawQuad>> = match self.base_raw_quads(&rows).await {
                    Ok(raws) => raws.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                };
                raws.extend(tail_raws);
                Ok(Box::new(stream::iter([raws])))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                // As for the in-memory arm: base row order needs the exact
                // ids, so a pending selection materializes (running the
                // deferred index-child scan once, cached on the view).
                let selection = selection.materialized_async().await?;
                let scan =
                    self.restricted_file_scan(file, filter.as_ref(), &selection, deleted.as_ref())?;
                let layout = self.layout.clone();
                let sync_layout = layout.clone();
                self.scan_chunks(
                    scan,
                    move |chunk| raw_chunk(&sync_layout, &chunk),
                    move |chunk| {
                        let layout = layout.clone();
                        async move {
                            match layout.raw_quads_async(&chunk).await {
                                Ok(raws) => raws.into_iter().map(Ok).collect(),
                                Err(e) => vec![Err(e)],
                            }
                        }
                        .boxed()
                    },
                    tail_raws,
                )
            }
        }
    }

    /// The decoded chunk stream of `scan`, through `sync` when every chunk
    /// decodes in memory and through `r#async` when the dictionary is
    /// file-backed — each chunk's codes then resolve with a scan of their
    /// own, so the decode must await (see [`scan_chunk_stream_async`]).
    #[cfg(feature = "file-io")]
    fn scan_chunks<T: Send + 'static>(
        &self,
        scan: ScanBuilder<ArrayRef>,
        sync: impl Fn(ArrayRef) -> Vec<Result<T>> + Send + Sync + 'static,
        r#async: impl FnMut(ArrayRef) -> BoxFuture<'static, Vec<Result<T>>> + Send + 'static,
        tail: Vec<Result<T>>,
    ) -> Result<Box<dyn Stream<Item = Vec<Result<T>>> + Unpin + Send + 'static>> {
        let file_backed_dictionary =
            matches!(&self.layout, ResolvedLayout::Dictionary(access) if access.is_file_backed());
        if file_backed_dictionary {
            scan_chunk_stream_async(scan, r#async, tail)
        } else {
            scan_chunk_stream(scan, sync, tail)
        }
    }
}

/// One chunk decoded to raw quads through `layout`, a chunk-level decode
/// error carried as a one-element error chunk (the same convention as
/// `VortexRdfStore::decoded_chunks`).
fn raw_chunk(layout: &ResolvedLayout, rows: &ArrayRef) -> Vec<Result<RawQuad>> {
    match layout.raw_quads(rows) {
        Ok(raws) => raws.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    }
}

/// The boxed, error-mapped, tail-chained stream every file pipeline in
/// `VortexRdfStore::decoded_chunks` (and its raw twin) returns. `decode` runs
/// inside the scan's spawned split tasks (via the scan's map function), so
/// decoding runs concurrently across the runtime's workers instead of
/// serially at the stream consumer. Each polled item is one decoded chunk; a
/// per-chunk scan error becomes a one-element error chunk.
#[cfg(feature = "file-io")]
fn scan_chunk_stream<T: Send + 'static>(
    scan: ScanBuilder<ArrayRef>,
    decode: impl Fn(ArrayRef) -> Vec<Result<T>> + Send + Sync + 'static,
    tail_quads: Vec<Result<T>>,
) -> Result<Box<dyn Stream<Item = Vec<Result<T>>> + Unpin + Send + 'static>> {
    let stream = scan
        .map(move |chunk| Ok(decode(chunk)))
        .into_stream()
        .map_err(VortexRdfError::Vortex)?;
    let chunk_stream = stream
        .map(|chunk_res| match chunk_res {
            Err(e) => vec![Err(VortexRdfError::Vortex(e))],
            Ok(quads) => quads,
        })
        .chain(stream::iter([tail_quads]));
    Ok(Box::new(chunk_stream))
}

/// The async-decode counterpart of [`scan_chunk_stream`], for reads whose
/// decode must itself await — a file-backed dictionary resolves each chunk's
/// codes with a scan of its own, so the decode runs after the chunk stream,
/// not inside the scan's sync map function.
#[cfg(feature = "file-io")]
fn scan_chunk_stream_async<T: Send + 'static>(
    scan: ScanBuilder<ArrayRef>,
    mut decode: impl FnMut(ArrayRef) -> BoxFuture<'static, Vec<Result<T>>> + Send + 'static,
    tail_quads: Vec<Result<T>>,
) -> Result<Box<dyn Stream<Item = Vec<Result<T>>> + Unpin + Send + 'static>> {
    let stream = scan.into_stream().map_err(VortexRdfError::Vortex)?;
    let chunk_stream = stream
        .then(move |chunk_res| match chunk_res {
            Err(e) => future::ready(vec![Err(VortexRdfError::Vortex(e))]).boxed(),
            Ok(chunk) => decode(chunk),
        })
        .boxed()
        .chain(stream::iter([tail_quads]));
    Ok(Box::new(chunk_stream))
}
