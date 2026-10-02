//! Chunked, Arrow-free exports of a view's rows — the stream a query planner
//! reads a [`VortexRdfStore`] view through when it wants columns rather than
//! decoded quads: struct chunks in the layout's own dtype
//! ([`row_chunks`](VortexRdfStore::row_chunks)) or `u32` code columns
//! ([`code_chunks`](VortexRdfStore::code_chunks)).
//!
//! Every chunk holds at most `batch_rows` rows. Rows come in **base row
//! order** with the view's narrowing applied — the pattern, keeps, windows
//! and tombstones — and for the string layouts the append tail's live rows
//! follow the base, as [`quads`](VortexRdfStore::quads) orders them. A served
//! match (a view answered from an index copy) materializes its row ids
//! first, so the order is the base's, never the index's. The streams are
//! lazy: nothing is gathered or scanned until the first poll, and a
//! file-backed view streams the scan's chunks as they arrive.

use std::future::Future;

use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute as _};
use vortex_buffer::Buffer;

use super::VortexRdfStore;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::array::{field_as, into_struct_array};
use crate::store::schema::QuadColumn;

/// A view's rows as struct chunks in the layout's native dtype, each of at
/// most the requested `batch_rows`.
pub type RowChunkStream = BoxStream<'static, Result<ArrayRef>>;

/// A view's rows as `u32` code columns — one buffer per requested column, in
/// the requested order — each chunk of at most the requested `batch_rows`.
pub type CodeChunkStream = BoxStream<'static, Result<Vec<Buffer<u32>>>>;

impl VortexRdfStore {
    /// The rows this view selects as struct chunks in the layout's primary
    /// dtype — `u32` codes under the Dictionary layout, strings otherwise —
    /// in base row order, each chunk holding at most `batch_rows` rows (a
    /// file-backed view's chunks follow the file's natural splits, cut to the
    /// cap; an in-memory view's are exactly `batch_rows` but the last).
    /// Tombstoned rows are skipped and, under a string layout, the append
    /// tail's live rows come last.
    ///
    /// Errors: `batch_rows == 0`; a Dictionary-layout view with a non-empty
    /// tail (its tail holds strings whose terms have no code — `compact` the
    /// store first).
    pub fn row_chunks(&self, batch_rows: usize) -> Result<RowChunkStream> {
        check_batch(batch_rows)?;
        if self.layout.strategy() == super::LayoutStrategy::Dictionary && self.tail_len() != 0 {
            self.ensure_code_view("row_chunks")?;
        }
        let store = self.clone();
        Ok(lazy(
            async move { store.row_chunk_source(batch_rows).await },
        ))
    }

    /// The rows this view selects as `u32` term-code columns, one buffer per
    /// column of `columns` in that order, in base row order, each chunk
    /// holding at most `batch_rows` rows. The chunked twin of
    /// [`code_columns_gathered`](Self::code_columns_gathered), with the same
    /// vocabulary: the codes of the store's dictionary
    /// ([`dict_reader`](Self::dict_reader) decodes them).
    ///
    /// In memory the buffers are zero-copy slices of the base's columns;
    /// on file the scan projects only `columns`. Errors: `batch_rows == 0`,
    /// no columns, or a view whose rows are not code-addressable (a string
    /// layout, or a Dictionary view with a non-empty tail).
    pub fn code_chunks(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        check_batch(batch_rows)?;
        if columns.is_empty() {
            return Err(VortexRdfError::InvalidOperation(
                "code_chunks needs at least one column".to_string(),
            ));
        }
        self.ensure_code_view("code_chunks")?;
        let columns = columns.to_vec();
        let store = self.clone();
        Ok(lazy(async move {
            store.code_chunk_source(&columns, batch_rows).await
        }))
    }

    /// The chunk stream behind [`row_chunks`](Self::row_chunks), built once
    /// the stream is first polled.
    async fn row_chunk_source(&self, batch_rows: usize) -> Result<RowChunkStream> {
        let tail = self.tail_chunks(batch_rows)?;
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                let rows = self.base_selected_rows().await?;
                let mut chunks = rechunk(rows, batch_rows)?;
                chunks.extend(tail);
                Ok(stream::iter(chunks.into_iter().map(Ok)).boxed())
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                // A point-sized selection reads row by row through the
                // file's chunk probes (see `base_selected_rows`) instead of
                // decoding whole chunks of the scan.
                if selection.is_point_sized() {
                    let rows = self.base_selected_rows().await?;
                    let mut chunks = rechunk(rows, batch_rows)?;
                    chunks.extend(tail);
                    return Ok(stream::iter(chunks.into_iter().map(Ok)).boxed());
                }
                let scan =
                    self.restricted_file_scan(file, filter.as_ref(), &selection, deleted.as_ref())?;
                Ok(rechunked_scan(scan, batch_rows)?
                    .chain(stream::iter(tail.into_iter().map(Ok)))
                    .boxed())
            }
        }
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built once
    /// the stream is first polled.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                // Zero-copy: the requested columns are slices of the base's
                // own `u32` buffers (or a gather of them for an id selection).
                if let Some(all) = self.base_code_columns() {
                    let picked: Vec<Buffer<u32>> =
                        columns.iter().map(|c| all[c.index()].clone()).collect();
                    let rows = picked[0].len();
                    let chunks: Vec<Vec<Buffer<u32>>> = (0..rows)
                        .step_by(batch_rows)
                        .map(|start| {
                            let end = (start + batch_rows).min(rows);
                            picked.iter().map(|b| b.slice(start..end)).collect()
                        })
                        .collect();
                    return Ok(stream::iter(chunks.into_iter().map(Ok)).boxed());
                }
                // A base whose columns the shared cache cannot hand out as
                // `u32` primitives: go through the row chunks and extract.
                let rows = self.base_selected_rows().await?;
                let chunks = rechunk(rows, batch_rows)?;
                Ok(stream::iter(chunks.into_iter().map(move |c| extract_codes(c, &names))).boxed())
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                if selection.is_point_sized() {
                    let rows = self.base_selected_rows().await?;
                    let chunks = rechunk(rows, batch_rows)?;
                    return Ok(stream::iter(
                        chunks.into_iter().map(move |c| extract_codes(c, &names)),
                    )
                    .boxed());
                }
                let scan = Self::restricted_file_scan_projected(
                    file,
                    &names,
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )?;
                Ok(rechunked_scan(scan, batch_rows)?
                    .map(move |chunk| extract_codes(chunk?, &names))
                    .boxed())
            }
        }
    }

    /// The tail's live rows cut to `batch_rows` (none without a tail).
    fn tail_chunks(&self, batch_rows: usize) -> Result<Vec<ArrayRef>> {
        match &self.tail {
            Some(tail) => rechunk(tail.live_rows()?, batch_rows),
            None => Ok(Vec::new()),
        }
    }
}

/// `Err` for a zero batch size.
fn check_batch(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "a chunk stream needs batch_rows > 0".to_string(),
        ));
    }
    Ok(())
}

/// A stream that builds its source on first poll — the view's gather or
/// scan runs then, not when the stream is requested — and yields the
/// build's error as its single item when the build fails.
fn lazy<T: Send + 'static>(
    build: impl Future<Output = Result<BoxStream<'static, Result<T>>>> + Send + 'static,
) -> BoxStream<'static, Result<T>> {
    stream::once(build)
        .flat_map(|built| match built {
            Ok(s) => s,
            Err(e) => stream::iter([Err(e)]).boxed(),
        })
        .boxed()
}

/// `rows` cut into slices of at most `batch_rows`; an empty array yields no
/// chunk.
pub(super) fn rechunk(rows: ArrayRef, batch_rows: usize) -> Result<Vec<ArrayRef>> {
    let len = rows.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    if len <= batch_rows {
        return Ok(vec![rows]);
    }
    (0..len)
        .step_by(batch_rows)
        .map(|start| {
            rows.slice(start..(start + batch_rows).min(len))
                .map_err(VortexRdfError::Vortex)
        })
        .collect()
}

/// The scan's chunk stream with every chunk cut to `batch_rows`, a chunk's
/// read error carried as one error item.
#[cfg(feature = "file-io")]
fn rechunked_scan(
    scan: vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    batch_rows: usize,
) -> Result<RowChunkStream> {
    let chunks = scan.into_stream().map_err(VortexRdfError::Vortex)?;
    Ok(chunks
        .flat_map(move |chunk| match chunk {
            Ok(chunk) => match rechunk(chunk, batch_rows) {
                Ok(pieces) => stream::iter(pieces.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter([Err(e)]).boxed(),
            },
            Err(e) => stream::iter([Err(VortexRdfError::Vortex(e))]).boxed(),
        })
        .boxed())
}

/// The `u32` buffers of `names`' columns in one struct chunk.
fn extract_codes(chunk: ArrayRef, names: &[&'static str]) -> Result<Vec<Buffer<u32>>> {
    let struct_arr = into_struct_array(chunk)?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    names
        .iter()
        .map(|name| {
            Ok(field_as::<PrimitiveArray>(&struct_arr, name, &mut ctx)?.into_buffer::<u32>())
        })
        .collect()
}
