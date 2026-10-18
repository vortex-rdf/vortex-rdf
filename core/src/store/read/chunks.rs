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
//! or read until the first poll. The base's rows come through the one
//! base-row seam (`base_read`): a file-backed view reads an exact selection
//! by id where `ExactRead` picks a read by id, and otherwise streams its
//! scan's chunks through the row-scan driver.

use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute as _};
use vortex_buffer::Buffer;

use super::deferred;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::VortexRdfStore;
use crate::store::array::{field_as, into_struct_array, rechunk};
use crate::store::schema::{QuadColumn, TermCode};

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
    /// Errors: `batch_rows == 0` is invalid input; a Dictionary-layout view
    /// with a non-empty tail (its tail holds strings whose terms have no code
    /// — `compact` the store first) an invalid operation.
    pub fn row_chunks(&self, batch_rows: usize) -> Result<RowChunkStream> {
        check_batch(batch_rows)?;
        self.ensure_no_dictionary_tail("row_chunks")?;
        let store = self.clone();
        Ok(deferred(
            async move { store.row_chunk_source(batch_rows).await },
            Err,
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
    /// `batch_rows == 0` or no columns (invalid input), or a view whose rows
    /// are not code-addressable (an invalid operation: a string layout, or a
    /// Dictionary view with a non-empty tail).
    pub fn code_chunks(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        check_batch(batch_rows)?;
        if columns.is_empty() {
            return Err(VortexRdfError::InvalidInput(
                "code_chunks needs at least one column".to_string(),
            ));
        }
        self.ensure_code_view("code_chunks")?;
        let columns = columns.to_vec();
        let store = self.clone();
        Ok(deferred(
            async move { store.code_chunk_source(&columns, batch_rows).await },
            Err,
        ))
    }

    /// The chunk stream behind [`row_chunks`](Self::row_chunks), built when
    /// the stream is first polled: the base's rows, then the tail's.
    async fn row_chunk_source(&self, batch_rows: usize) -> Result<RowChunkStream> {
        let tail = self.tail_chunks(batch_rows)?;
        let base = self
            .base_read(self.layout.strategy().primary_column_names())
            .await?;
        Ok(base
            .chunks(batch_rows)?
            .chain(stream::iter(tail.into_iter().map(Ok)))
            .boxed())
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built when
    /// the stream is first polled.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        // In memory the requested columns are the base's own code buffers,
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
        // A file view, or a base whose code columns the shared cache cannot
        // hand out: its rows, cut to the cap, their codes read per chunk by
        // name.
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        let base = self.base_read(&names).await?;
        Ok(base
            .chunks(batch_rows)?
            .map(move |chunk| extract_codes(chunk?, &names))
            .boxed())
    }

    /// The tail's live rows cut to `batch_rows`; none without a tail.
    fn tail_chunks(&self, batch_rows: usize) -> Result<Vec<ArrayRef>> {
        match &self.tail {
            Some(tail) => rechunk(tail.live_rows()?, batch_rows),
            None => Ok(Vec::new()),
        }
    }
}

/// The [`InvalidInput`](VortexRdfError::InvalidInput) error for a zero batch
/// size.
fn check_batch(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 {
        return Err(VortexRdfError::InvalidInput(
            "a chunk stream needs batch_rows > 0".to_string(),
        ));
    }
    Ok(())
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
