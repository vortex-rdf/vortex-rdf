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
//! or read until the first poll, and then one chunk at a time. In memory a
//! contiguous selection without tombstones is sliced from the base, and any
//! other is cut into windows of live ids, each gathered when its chunk is
//! pulled. On file a selection `ExactRead` takes by id is taken one window
//! of ids per pull; any other read goes through the one base-row seam
//! (`base_read`): the few rows `ExactRead` reads point by point, read at
//! once, or the scan's chunks, streamed through the row-scan driver.

use std::ops::Range;
use std::sync::Arc;

use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute as _};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use super::deferred;
use super::rows::{gather_codes, gather_column};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::VortexRdfStore;
use crate::store::array::{field_as, into_struct_array, rechunk};
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads::{ExactRead, take_file_rows};
use crate::store::scan::gather::gather_live;
use crate::store::schema::{QuadColumn, RowId, TermCode};
use crate::store::view::selection::RowSelection;

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
    /// In memory a contiguous selection without tombstones is sliced from
    /// the base's own code columns, zero-copy, and any other selection has
    /// only `columns` gathered from them, one window of live ids per chunk.
    /// On file a selection taken by id is taken one chunk's ids per pull; the
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
            .base_chunks(self.layout.strategy().primary_column_names(), batch_rows)
            .await?;
        Ok(base.chain(stream::iter(tail.into_iter().map(Ok))).boxed())
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built when
    /// the stream is first polled.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        // In memory over the base's own code columns: a contiguous run is
        // sliced and a window of live ids gathered, one chunk per pull.
        if let Some(prims) = self.base_code_primitives() {
            return Ok(match self.memory_rows(batch_rows)? {
                MemoryRows::Run(run) => {
                    let picked: Vec<Buffer<TermCode>> = columns
                        .iter()
                        .map(|c| {
                            prims[c.index()]
                                .clone()
                                .into_buffer::<TermCode>()
                                .slice(run.clone())
                        })
                        .collect();
                    let rows = run.len();
                    stream::iter((0..rows).step_by(batch_rows).map(move |start| {
                        #[cfg(test)]
                        run_hooks::note();
                        let end = start.saturating_add(batch_rows).min(rows);
                        Ok(picked.iter().map(|b| b.slice(start..end)).collect())
                    }))
                    .boxed()
                }
                MemoryRows::Windows(windows) => {
                    // Every column asked for: the four gathered in one pass;
                    // a subset: only its columns.
                    let every = QuadColumn::ALL.iter().all(|c| columns.contains(c));
                    let picked: Vec<usize> = columns.iter().map(|c| c.index()).collect();
                    stream::iter(windows.map(move |ids| {
                        #[cfg(test)]
                        gather_hooks::note();
                        Ok(if every {
                            let codes = gather_codes(&prims, &ids);
                            picked.iter().map(|&k| codes[k].clone()).collect()
                        } else {
                            picked
                                .iter()
                                .map(|&k| gather_column(&prims[k], &ids))
                                .collect()
                        })
                    }))
                    .boxed()
                }
            });
        }
        // A file view, or a base whose code columns the shared cache cannot
        // hand out: its rows, chunked, their codes read per chunk by name.
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        let base = self.base_chunks(&names, batch_rows).await?;
        Ok(base.map(move |chunk| extract_codes(chunk?, &names)).boxed())
    }

    /// The base rows this view covers, without the tail, in chunks of at
    /// most `batch_rows` rows of at least the `columns` (primary column
    /// names). In memory a window of live ids ([`MemoryRows::Windows`]) is
    /// gathered per pull, and on file a selection `ExactRead` takes by id is
    /// taken one window per pull ([`take_chunks`](Self::take_chunks)); any
    /// other read is [`base_read`](Self::base_read)'s, cut to the cap.
    async fn base_chunks(
        &self,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<RowChunkStream> {
        if let Some(memory) = self.quads.in_memory()
            && let MemoryRows::Windows(windows) = self.memory_rows(batch_rows)?
        {
            let base = memory.base.clone();
            let probes = Arc::clone(&memory.probes);
            return Ok(stream::iter(windows.map(move |ids| {
                #[cfg(test)]
                gather_hooks::note();
                gather_live(&base, &RowSelection::Ids(ids), None, Some(&probes))
            }))
            .boxed());
        }
        #[cfg(feature = "file-io")]
        if let Some(chunks) = self.take_chunks(columns, batch_rows).await? {
            return Ok(chunks);
        }
        self.base_read(columns).await?.chunks(batch_rows)
    }

    /// How this in-memory view's base rows are cut into chunks of at most
    /// `batch_rows` rows: a contiguous selection without tombstones is a
    /// [`MemoryRows::Run`], and any other is cut into windows of live ids. A
    /// served view's pending selection materializes here.
    fn memory_rows(&self, batch_rows: usize) -> Result<MemoryRows> {
        let base_len = self.quads.base_len();
        Ok(
            match (self.quads.selection.materialized()?, &self.quads.deleted) {
                (RowSelection::All, None) => MemoryRows::Run(0..base_len),
                (RowSelection::Range(range), None) => {
                    MemoryRows::Run(range.start as usize..range.end as usize)
                }
                (selection, deleted) => MemoryRows::Windows(live_id_windows(
                    selection,
                    deleted.clone(),
                    base_len,
                    batch_rows,
                )),
            },
        )
    }

    /// A file view's base rows when `ExactRead` takes its exact selection by
    /// id: the live ids cut into windows of at most `batch_rows`, each taken
    /// ([`take_file_rows`]) when its chunk is pulled. `None` in memory, and
    /// for a selection read point by point or by the scan, which
    /// [`base_read`](Self::base_read) reads.
    #[cfg(feature = "file-io")]
    async fn take_chunks(
        &self,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<Option<RowChunkStream>> {
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        let (selection, by_id_columns) = self.file_read_scope(base, columns).await?;
        let ExactRead::Take(ids) = Self::exact_read(
            &base.file,
            by_id_columns,
            base.filter.as_ref(),
            &selection,
            self.quads.deleted.as_ref(),
        )?
        else {
            return Ok(None);
        };
        let file = Arc::clone(&base.file);
        let columns: Arc<[&'static str]> = Arc::from(by_id_columns);
        Ok(Some(
            stream::iter(id_windows(Buffer::from(ids), batch_rows).enumerate())
                .then(move |(k, window)| {
                    let (file, columns) = (Arc::clone(&file), Arc::clone(&columns));
                    async move {
                        // A take declines only an empty id list, and no
                        // window is empty: a decline is a broken invariant.
                        take_file_rows(&file, &columns, &window)
                            .await
                            .and_then(|rows| {
                                rows.ok_or_else(|| {
                                    VortexRdfError::Vortex(vortex_error::vortex_err!(
                                        "the take of id window {k} ({} ids from base row {}) \
                                         declined",
                                        window.len(),
                                        window.first().copied().unwrap_or_default()
                                    ))
                                })
                            })
                    }
                })
                .boxed(),
        ))
    }

    /// The tail's live rows cut to `batch_rows`; none without a tail.
    fn tail_chunks(&self, batch_rows: usize) -> Result<Vec<ArrayRef>> {
        match &self.tail {
            Some(tail) => rechunk(tail.live_rows()?, batch_rows),
            None => Ok(Vec::new()),
        }
    }
}

/// An in-memory view's base rows as the chunk sources cut them.
enum MemoryRows {
    /// A contiguous run of base rows, none tombstoned: each chunk is a slice
    /// of the base, nothing gathered.
    Run(Range<usize>),
    /// The view's live row ids in windows of at most the batch size, in base
    /// row order, each built when the iterator reaches it: each chunk is one
    /// window, gathered.
    Windows(IdWindows),
}

/// Windows of base row ids, a chunk's rows each.
type IdWindows = Box<dyn Iterator<Item = Buffer<RowId>> + Send>;

/// `ids` cut into consecutive windows of at most `batch_rows`, each a slice
/// of it.
fn id_windows(
    ids: Buffer<RowId>,
    batch_rows: usize,
) -> impl Iterator<Item = Buffer<RowId>> + Send + 'static {
    let len = ids.len();
    (0..len)
        .step_by(batch_rows)
        .map(move |start| ids.slice(start..start.saturating_add(batch_rows).min(len)))
}

/// The live ids of `selection` over a base of `base_len` rows, the `deleted`
/// tombstones skipped, in windows of at most `batch_rows`: an id list without
/// tombstones sliced, any other selection walked and filtered one window at a
/// time, so no id list is built beyond the selection's own.
fn live_id_windows(
    selection: RowSelection,
    deleted: Option<Mask>,
    base_len: usize,
    batch_rows: usize,
) -> IdWindows {
    match (selection, deleted) {
        (RowSelection::Ids(ids), None) => Box::new(id_windows(ids, batch_rows)),
        (RowSelection::All, deleted) => filtered_windows(0..base_len as RowId, deleted, batch_rows),
        (RowSelection::Range(range), deleted) => filtered_windows(range, deleted, batch_rows),
        (RowSelection::Ids(ids), deleted) => {
            filtered_windows((0..ids.len()).map(move |k| ids[k]), deleted, batch_rows)
        }
    }
}

/// The ids of `candidates` the `deleted` tombstones leave live, in windows
/// of at most `batch_rows`, each collected when the iterator reaches it.
fn filtered_windows(
    candidates: impl Iterator<Item = RowId> + Send + 'static,
    deleted: Option<Mask>,
    batch_rows: usize,
) -> IdWindows {
    let mut live =
        candidates.filter(move |&id| deleted.as_ref().is_none_or(|d| !d.value(id as usize)));
    Box::new(std::iter::from_fn(move || {
        let mut window = Vec::with_capacity(live.size_hint().1.unwrap_or(0).min(batch_rows));
        window.extend(live.by_ref().take(batch_rows));
        (!window.is_empty()).then(|| Buffer::from(window))
    }))
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

/// Test hook: how many windows of live ids this thread's chunk streams
/// gathered from an in-memory base, one per pulled chunk.
#[cfg(test)]
pub(crate) mod gather_hooks {
    use std::cell::Cell;

    thread_local! {
        static GATHERS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one window gathered.
    pub(crate) fn note() {
        GATHERS.with(|c| c.set(c.get() + 1));
    }

    /// The windows gathered on this thread so far.
    pub(crate) fn gathers() -> usize {
        GATHERS.with(Cell::get)
    }
}

/// Test hook: how many chunks this thread's code-chunk streams cut from an
/// in-memory run's columns, one per cut.
#[cfg(test)]
pub(crate) mod run_hooks {
    use std::cell::Cell;

    thread_local! {
        static CUTS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one chunk cut.
    pub(crate) fn note() {
        CUTS.with(|c| c.set(c.get() + 1));
    }

    /// The chunks cut on this thread so far.
    pub(crate) fn cuts() -> usize {
        CUTS.with(Cell::get)
    }
}
