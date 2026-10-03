//! Chunked exports of a view's rows: struct chunks in the layout's dtype
//! ([`row_chunks`](VortexRdfStore::row_chunks)) or `u32` code columns
//! ([`code_chunks`](VortexRdfStore::code_chunks)). Chunks hold at most
//! `batch_rows` rows and come in base row order with the view's narrowing
//! and tombstones applied (a served match materializes its row ids first);
//! under a string layout the tail's live rows follow the base. The streams
//! build on first poll.

use std::future::Future;

use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute as _};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::VortexRdfStore;
use crate::store::array::{field_as, into_struct_array, rechunk};
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads;
use crate::store::schema::QuadColumn;

/// A view's rows as struct chunks in the layout's dtype, each of at most the
/// requested `batch_rows`.
pub type RowChunkStream = BoxStream<'static, Result<ArrayRef>>;

/// A view's rows as `u32` code columns, one buffer per requested column in
/// the requested order, each chunk of at most the requested `batch_rows`.
pub type CodeChunkStream = BoxStream<'static, Result<Vec<Buffer<u32>>>>;

impl VortexRdfStore {
    /// The rows this view selects as struct chunks in the layout's primary
    /// dtype (`u32` codes under the Dictionary layout, strings otherwise), in
    /// base row order, each chunk of at most `batch_rows` rows (a file view's
    /// chunks follow the file's splits cut to the cap; an in-memory view's are
    /// exactly `batch_rows` but the last). Tombstoned rows are skipped; under
    /// a string layout the tail's live rows come last. Errors: `batch_rows ==
    /// 0`; a Dictionary view with a non-empty tail (`compact` the store
    /// first).
    pub fn row_chunks(&self, batch_rows: usize) -> Result<RowChunkStream> {
        check_batch(batch_rows)?;
        self.ensure_no_dictionary_tail("row_chunks")?;
        let store = self.clone();
        Ok(lazy(
            async move { store.row_chunk_source(batch_rows).await },
        ))
    }

    /// The rows this view selects as `u32` code columns, one buffer per
    /// column of `columns` in that order, in base row order, each chunk of at
    /// most `batch_rows` rows; the codes are the store's dictionary's
    /// ([`dict_reader`](Self::dict_reader) decodes them). In memory the
    /// buffers are zero-copy slices of the base's columns; on file the scan
    /// projects only `columns`. Errors: `batch_rows == 0`, no columns, or a
    /// view whose rows are not code-addressable (a string layout, or a
    /// Dictionary view with a non-empty tail).
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

    /// The chunk stream behind [`row_chunks`](Self::row_chunks), built on
    /// first poll.
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
                // A point-sized selection reads point by point through the
                // chunk probes.
                if selection.is_point_sized() {
                    let rows = self.base_selected_rows().await?;
                    let mut chunks = rechunk(rows, batch_rows)?;
                    chunks.extend(tail);
                    return Ok(stream::iter(chunks.into_iter().map(Ok)).boxed());
                }
                let scan = file_reads::restricted_scan(
                    file,
                    self.layout.strategy().primary_column_names(),
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )?;
                Ok(rechunked_scan(scan, batch_rows)?
                    .chain(stream::iter(tail.into_iter().map(Ok)))
                    .boxed())
            }
        }
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built on
    /// first poll.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                // Slices of the base's own `u32` buffers (a gather for an id
                // selection).
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
                // Columns the shared cache cannot hand out as `u32`
                // primitives: extracted from the row chunks.
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
                let scan = file_reads::restricted_scan(
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

/// A stream built from `build` on first poll; a build failure is the
/// stream's single item.
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
