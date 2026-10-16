//! Chunked, Arrow-free exports of a view's rows — the streams a query
//! planner reads a [`VortexRdfStore`] view through when it wants columns
//! rather than decoded quads: struct chunks in the layout's own dtype
//! ([`row_chunks`](VortexRdfStore::row_chunks)) or [`TermCode`] columns
//! ([`code_chunks`](VortexRdfStore::code_chunks)).
//!
//! Every chunk holds at most `batch_rows` rows. Rows come in base row order
//! with the view's narrowing applied — the pattern, keeps, windows and
//! tombstones — and under a string layout the append tail's live rows follow
//! the base, as [`quads`](VortexRdfStore::quads) orders them. A served match
//! (a view an index answered) materializes its row ids first, so the order is
//! the base's, never the index's. The streams are lazy: nothing is gathered
//! or read until the first poll. A file-backed view reads an exact selection
//! by id where `ExactRead` picks a read by id, and otherwise streams its
//! scan's chunks through the row-scan driver.

use std::future::Future;

use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute as _};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{field_as, into_struct_array, rechunk};
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads::stream_rows;
use crate::store::schema::{QuadColumn, TermCode};
use crate::store::{QuadsSource, VortexRdfStore};

/// A view's rows as struct chunks in the layout's primary dtype, each of at
/// most the requested `batch_rows` rows.
pub type RowChunkStream = BoxStream<'static, Result<ArrayRef>>;

/// A view's rows as [`TermCode`] columns — one buffer per requested column,
/// in the requested order — each chunk of at most the requested `batch_rows`
/// rows.
pub type CodeChunkStream = BoxStream<'static, Result<Vec<Buffer<TermCode>>>>;

impl VortexRdfStore {
    /// The rows this view selects as struct chunks in the layout's primary
    /// dtype — [`TermCode`] columns under the Dictionary layout, strings
    /// otherwise — in base row order, each chunk holding at most `batch_rows`
    /// rows (a scanned file view's chunks follow the scan's splits, cut to
    /// the cap). Tombstoned rows are skipped and, under a string layout, the
    /// append tail's live rows come last.
    ///
    /// Errors: `batch_rows == 0`; a Dictionary-layout view with a non-empty
    /// tail (its tail holds strings whose terms have no code — `compact` the
    /// store first).
    pub fn row_chunks(&self, batch_rows: usize) -> Result<RowChunkStream> {
        check_batch(batch_rows)?;
        self.ensure_no_dictionary_tail("row_chunks")?;
        let store = self.clone();
        Ok(lazy(
            async move { store.row_chunk_source(batch_rows).await },
        ))
    }

    /// The rows this view selects as [`TermCode`] columns, one buffer per
    /// column of `columns` in that order, in base row order, each chunk
    /// holding at most `batch_rows` rows. The chunked twin of
    /// [`code_columns_gathered`](Self::code_columns_gathered), with the same
    /// vocabulary: the codes of the store's dictionary
    /// ([`dict_reader`](Self::dict_reader) decodes them).
    ///
    /// In memory the buffers are slices of the base's own code columns —
    /// zero-copy for a contiguous selection without tombstones. On file the
    /// scan, and a read by id with no filter pending, read only `columns`; a
    /// read by id under a pending filter reads every primary column. Errors:
    /// `batch_rows == 0`, no columns, or a view whose rows are not
    /// code-addressable (a string layout, or a Dictionary view with a
    /// non-empty tail).
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

    /// The chunk stream behind [`row_chunks`](Self::row_chunks), built when
    /// the stream is first polled.
    async fn row_chunk_source(&self, batch_rows: usize) -> Result<RowChunkStream> {
        let tail = self.tail_chunks(batch_rows)?;
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                let mut chunks = rechunk(self.base_selected_rows().await?, batch_rows)?;
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
                // Base row order needs the exact ids, so a pending selection
                // materializes here.
                let selection = selection.materialized_async().await?;
                if let Some(rows) = Self::exact_rows_by_id(
                    file,
                    self.layout.strategy().primary_column_names(),
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )
                .await?
                {
                    let mut chunks = rechunk(rows, batch_rows)?;
                    chunks.extend(tail);
                    return Ok(stream::iter(chunks.into_iter().map(Ok)).boxed());
                }
                let scan =
                    self.restricted_file_scan(file, filter.as_ref(), &selection, deleted.as_ref())?;
                Ok(rechunked(stream_rows(scan)?, batch_rows)
                    .chain(stream::iter(tail.into_iter().map(Ok)))
                    .boxed())
            }
        }
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built when
    /// the stream is first polled.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                // The requested columns are the base's own code buffers,
                // sliced per chunk.
                if let Some(all) = self.base_code_columns() {
                    let picked: Vec<Buffer<TermCode>> =
                        columns.iter().map(|c| all[c.index()].clone()).collect();
                    let rows = picked[0].len();
                    let chunks: Vec<Result<Vec<Buffer<TermCode>>>> = (0..rows)
                        .step_by(batch_rows)
                        .map(|start| {
                            let end = start.saturating_add(batch_rows).min(rows);
                            Ok(picked.iter().map(|b| b.slice(start..end)).collect())
                        })
                        .collect();
                    return Ok(stream::iter(chunks).boxed());
                }
                // A base whose code columns the shared cache cannot hand out:
                // the gathered rows, cut to the cap, their codes read per
                // chunk.
                let chunks = rechunk(self.base_selected_rows().await?, batch_rows)?;
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
                // A read by id checks a pending filter on the columns it
                // reads, so under one it reads every primary column; the
                // requested ones are picked out of each chunk by name.
                let by_id_columns = match filter {
                    Some(_) => self.layout.strategy().primary_column_names(),
                    None => &names[..],
                };
                if let Some(rows) = Self::exact_rows_by_id(
                    file,
                    by_id_columns,
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )
                .await?
                {
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
                Ok(rechunked(stream_rows(scan)?, batch_rows)
                    .map(move |chunk| extract_codes(chunk?, &names))
                    .boxed())
            }
        }
    }

    /// The tail's live rows cut to `batch_rows`; none without a tail.
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

/// A stream that builds its source on first poll — the view's gather or read
/// runs then, not when the stream is requested — and yields the build's
/// error as its one item when the build fails.
fn lazy<T: Send + 'static>(
    build: impl Future<Output = Result<BoxStream<'static, Result<T>>>> + Send + 'static,
) -> BoxStream<'static, Result<T>> {
    stream::once(build)
        .flat_map(|built| match built {
            Ok(chunks) => chunks,
            Err(e) => stream::iter([Err(e)]).boxed(),
        })
        .boxed()
}

/// `chunks` with every chunk cut to `batch_rows`, a chunk's error carried as
/// one error item.
#[cfg(feature = "file-io")]
fn rechunked(chunks: BoxStream<'static, Result<ArrayRef>>, batch_rows: usize) -> RowChunkStream {
    chunks
        .flat_map(move |chunk| {
            let pieces: Vec<Result<ArrayRef>> =
                match chunk.and_then(|chunk| rechunk(chunk, batch_rows)) {
                    Ok(pieces) => pieces.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                };
            stream::iter(pieces)
        })
        .boxed()
}

/// The [`TermCode`] buffers of the `names` columns of one struct chunk, in
/// that order.
pub(in crate::store) fn extract_codes(
    chunk: ArrayRef,
    names: &[&str],
) -> Result<Vec<Buffer<TermCode>>> {
    let struct_arr = into_struct_array(chunk)?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    names
        .iter()
        .map(|name| {
            Ok(field_as::<PrimitiveArray>(&struct_arr, name, &mut ctx)?.into_buffer::<TermCode>())
        })
        .collect()
}
