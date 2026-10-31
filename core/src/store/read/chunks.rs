//! Chunked exports of a view's rows, for a planner that wants columns rather
//! than decoded quads: struct chunks in the layout's dtype
//! ([`row_chunks`](VortexRdfStore::row_chunks)) or [`TermCode`] columns
//! ([`code_chunks`](VortexRdfStore::code_chunks)).
//!
//! Chunks hold at most `batch_rows` rows with the view's narrowing and
//! tombstones applied, in the view's order: a served in-memory view's in its
//! index's order, as [`quads`](VortexRdfStore::quads) reads it, gathered off
//! the base at the run's row ids, one window of them per chunk, the ids never
//! sorted; any other view's in base row order (a served file view's row ids
//! sorted into it first). Under a string layout the tail's live rows follow
//! the base, as `quads` orders them. Nothing is read until the first poll. In
//! memory a contiguous selection without tombstones is sliced from the base,
//! and any other is cut into windows of live ids, each gathered when its
//! chunk is pulled. On file a selection `ExactRead` takes by id is taken one window of
//! ids per pull; a pending filter it leaves to the masks is evaluated a split
//! at a time as the stream reaches the split, only the requested columns read
//! at the rows the split's mask keeps; any other read goes through the one
//! base-row seam (`base_read`): the few rows `ExactRead` reads point by
//! point, read at once, or the scan's chunks, streamed through the row-scan
//! driver.

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
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::MemoryBase;
use crate::store::array::{field_as, into_struct_array, rechunk};
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
use crate::store::indexes::InMemoryServePlan;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
#[cfg(feature = "file-io")]
use crate::store::read::file_plan::StreamedPlan;
use crate::store::scan::gather::{gather_live, gather_rows_at};
#[cfg(feature = "file-io")]
use crate::store::scan::{
    file_reads::take_file_rows, file_scans::stream_split_reads, read_plan::ExactRead,
};
use crate::store::schema::{QuadColumn, RowId, TermCode};
use crate::store::view::selection::{RowSelection, SelectedRows};
use crate::store::{SharedQuad, VortexRdfStore};

/// A view's rows as struct chunks in the layout's primary dtype, each of at
/// most the requested `batch_rows` rows.
pub type RowChunkStream = BoxStream<'static, Result<ArrayRef>>;

/// A view's rows as [`TermCode`] columns, one buffer per requested column in
/// the requested order, each chunk of at most the requested `batch_rows`
/// rows.
pub type CodeChunkStream = BoxStream<'static, Result<Vec<Buffer<TermCode>>>>;

impl VortexRdfStore {
    /// The rows this view selects as struct chunks in the layout's primary
    /// dtype ([`TermCode`] columns under the Dictionary layout, strings
    /// otherwise), in the view's order (a served in-memory view's in its
    /// index's order, any other in base row order: see the [module
    /// docs](self)), each of at most `batch_rows` rows (a
    /// file view read split by split, scanned or by its filter's masks, has
    /// its chunks follow the splits, cut to the cap).
    /// Tombstoned rows are skipped; under a string layout the tail's live
    /// rows come last. Nothing is read until the first poll, and a read error
    /// is a stream item.
    ///
    /// Errors: `batch_rows == 0` (invalid input); a Dictionary view with a
    /// non-empty tail, whose strings have no code (an invalid operation:
    /// `compact` the store first).
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
    /// column of `columns` in that order, in the view's order (as
    /// [`row_chunks`](Self::row_chunks)), each chunk of at
    /// most `batch_rows` rows: the chunked twin of
    /// [`code_columns_gathered`](Self::code_columns_gathered), in the codes of
    /// the store's dictionary ([`dict_reader`](Self::dict_reader) decodes
    /// them). Nothing is read until the first poll, and a read error is a
    /// stream item.
    ///
    /// In memory, over code columns the shared cache hands out as primitives,
    /// a contiguous selection without tombstones is sliced zero-copy, and any
    /// other has only `columns` gathered, one window of live ids per chunk (a
    /// served view's ids in its index's order);
    /// columns the cache cannot hand out (chunked or wire-encoded) are read
    /// as row chunks and decoded chunk by chunk. On file a selection taken by
    /// id is taken one chunk's ids per pull, and a pending filter the reads
    /// by id leave to the scan is read by its masks first, a split at a time
    /// as the stream reaches it. The masked reads and the scan read only
    /// `columns` at the rows they keep (a pending filter's columns are read
    /// to evaluate it), as does a read by id with no filter pending; one
    /// under a pending filter reads every primary column. Errors:
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

    /// The rows of one of this store's [`row_chunks`](Self::row_chunks) as
    /// [`SharedQuad`]s, decoded as [`shared_quads_vec`](Self::shared_quads_vec)
    /// decodes a chunk: each distinct term of the chunk once, shared by the
    /// rows repeating it. For the string layouts (Default and TypedObject); a
    /// Dictionary store's chunks hold codes, which
    /// [`code_chunks`](Self::code_chunks) reads and
    /// [`dict_reader`](Self::dict_reader) decodes (an invalid operation here).
    pub fn decode_row_chunk(&self, chunk: &ArrayRef) -> Result<Vec<SharedQuad>> {
        if let ResolvedLayout::Dictionary(_) = self.layout {
            return Err(VortexRdfError::InvalidOperation(
                "decode_row_chunk decodes string layouts: read a Dictionary store's rows with \
                 code_chunks and decode them with dict_reader"
                    .into(),
            ));
        }
        SharedQuad::decode(&self.layout, chunk)
            .into_iter()
            .collect()
    }

    /// The chunk stream behind [`row_chunks`](Self::row_chunks), built when
    /// the stream is first polled: the base's rows, then the tail's.
    async fn row_chunk_source(&self, batch_rows: usize) -> Result<RowChunkStream> {
        let tail = self.tail_chunks(batch_rows)?;
        let primary = self.layout.strategy().primary_column_names();
        let base = match self.served_rows(primary, batch_rows)? {
            Some(base) => base,
            None => self.base_chunks(primary, batch_rows).await?,
        };
        Ok(base.chain(stream::iter(tail)).boxed())
    }

    /// The chunk stream behind [`code_chunks`](Self::code_chunks), built when
    /// the stream is first polled.
    async fn code_chunk_source(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        // A served view, in its index's order.
        if let Some(chunks) = self.served_code_chunks(columns, batch_rows)? {
            return Ok(chunks);
        }
        // In memory over the base's own code columns: a contiguous run is
        // sliced and a window of live ids gathered, one chunk per pull. Only
        // the columns asked for are decoded into their shared caches.
        if let Some((all, asked)) = self.base_code_sources(columns) {
            return Ok(match self.memory_rows(batch_rows)? {
                MemoryRows::Run(run) => {
                    let picked: Vec<Buffer<TermCode>> = asked
                        .into_iter()
                        .map(|prim| prim.into_buffer::<TermCode>().slice(run.clone()))
                        .collect();
                    let rows = run.len();
                    stream::iter((0..rows).step_by(batch_rows).map(move |start| {
                        observe!(RunCut);
                        let end = start.saturating_add(batch_rows).min(rows);
                        Ok(picked.iter().map(|b| b.slice(start..end)).collect())
                    }))
                    .boxed()
                }
                MemoryRows::Windows(windows) => gathered_code_chunks(all, asked, columns, windows),
            });
        }
        // A file view, or a base whose code columns the shared cache cannot
        // hand out: its rows, chunked, their codes read per chunk by name.
        let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
        let base = self.base_chunks(&names, batch_rows).await?;
        Ok(base.map(move |chunk| extract_codes(chunk?, &names)).boxed())
    }

    /// A served view's code chunks in its index's order. On file, the plan's
    /// child chunks ([`FileServePlan::child_chunks`]), their live rows' codes
    /// cut to `batch_rows`. In memory, the base's code columns gathered at
    /// the view's row ids as the index keeps them
    /// ([`InMemoryServePlan::live_rids`]), one window of ids per chunk, the
    /// run's ids decoded once and never sorted; where the shared cache cannot
    /// hand the code columns out, the [served rows](Self::served_rows)' codes,
    /// read per chunk. `None` for a view no plan serves.
    ///
    /// [`FileServePlan::child_chunks`]: crate::store::indexes::FileServePlan::child_chunks
    fn served_code_chunks(
        &self,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<Option<CodeChunkStream>> {
        #[cfg(feature = "file-io")]
        if let Some((base, plan)) = self.served_file_plan() {
            observe!(ServedRunChunks);
            let picked: Vec<usize> = columns.iter().map(|c| c.index()).collect();
            let deleted = self.quads.deleted.clone();
            let chunks = plan.child_chunks(Arc::clone(&base.file))?;
            let plan = plan.clone();
            return Ok(Some(
                chunks
                    .flat_map(move |chunk| {
                        let codes =
                            chunk.and_then(|chunk| plan.chunk_codes(&chunk, deleted.as_ref()));
                        stream::iter(match codes {
                            Ok(codes) => code_slices(codes, &picked, batch_rows),
                            Err(e) => vec![Err(e)],
                        })
                    })
                    .boxed(),
            ));
        }
        let Some((_, plan)) = self.served_memory_plan() else {
            return Ok(None);
        };
        let Some((all, asked)) = self.base_code_sources(columns) else {
            let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
            return Ok(self
                .served_rows(&names, batch_rows)?
                .map(|rows| rows.map(move |chunk| extract_codes(chunk?, &names)).boxed()));
        };
        let ids = plan.live_rids(self.quads.deleted.as_ref())?;
        observe!(ServedRunChunks);
        Ok(Some(gathered_code_chunks(
            all,
            asked,
            columns,
            id_windows(ids, batch_rows),
        )))
    }

    /// A served view's base rows, at least `columns` (primary column names),
    /// in its index's order. On file, the plan's child chunks, their live
    /// rows as the primary columns, cut to `batch_rows` (not for TypedObject,
    /// whose primary dtype its copies' string objects do not have). In
    /// memory, the base's rows at the view's row ids as the index keeps them,
    /// one window of ids per chunk, each read when its chunk is pulled.
    /// `None` for a view no plan serves.
    fn served_rows(
        &self,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<Option<RowChunkStream>> {
        #[cfg(feature = "file-io")]
        if let Some((base, plan)) = self.served_file_plan() {
            if self.layout.strategy() == crate::store::LayoutStrategy::TypedObject {
                return Ok(None);
            }
            observe!(ServedRunChunks);
            let deleted = self.quads.deleted.clone();
            let chunks = plan.child_chunks(Arc::clone(&base.file))?;
            let plan = plan.clone();
            return Ok(Some(
                chunks
                    .flat_map(move |chunk| {
                        let rows = chunk
                            .and_then(|chunk| plan.chunk_primary_rows(&chunk, deleted.as_ref()));
                        stream::iter(match rows {
                            Ok(rows) => rechunk(rows, batch_rows).collect(),
                            Err(e) => vec![Err(e)],
                        })
                    })
                    .boxed(),
            ));
        }
        let Some((memory, plan)) = self.served_memory_plan() else {
            return Ok(None);
        };
        let ids = plan.live_rids(self.quads.deleted.as_ref())?;
        let base = projected(&memory.base, columns)?;
        let probes = Arc::clone(&memory.probes);
        observe!(ServedRunChunks);
        Ok(Some(
            stream::iter(id_windows(ids, batch_rows).map(move |ids| {
                observe!(Gather {
                    width: base.dtype().as_struct_fields_opt().map(|f| f.nfields())
                });
                gather_rows_at(&base, &ids, Some(&probes))
            }))
            .boxed(),
        ))
    }

    /// This file view's base beside the serve plan its chunk streams read in
    /// its index's order: the plan reads exactly the view's rows, its
    /// constraints covering any residual term the view keeps as a filter for
    /// the base's reads. `None` for a view no plan serves (or while a
    /// [`LegacyChunkPaths`](crate::store::test_hooks::LegacyChunkPaths) guard
    /// sends every stream down the base-order paths).
    #[cfg(feature = "file-io")]
    fn served_file_plan(&self) -> Option<(&FileBase, &FileServePlan)> {
        #[cfg(test)]
        if crate::store::test_hooks::legacy_chunk_paths() {
            return None;
        }
        let base = self.quads.file()?;
        Some((base, base.serve.as_ref()?))
    }

    /// This in-memory view's base beside the serve plan its chunk streams
    /// read in its index's order; `None` for a view no in-memory plan serves
    /// (or while a [`LegacyChunkPaths`](crate::store::test_hooks::LegacyChunkPaths)
    /// guard sends every stream down the base-order paths).
    fn served_memory_plan(&self) -> Option<(&MemoryBase, &InMemoryServePlan)> {
        #[cfg(test)]
        if crate::store::test_hooks::legacy_chunk_paths() {
            return None;
        }
        let memory = self.quads.in_memory()?;
        Some((memory, memory.serve.as_ref()?))
    }

    /// The base's code primitives an in-memory code read of `columns` takes
    /// its codes from: all four when every column is asked for (gathered in
    /// one pass), beside the asked ones in the order asked. `None` when the
    /// shared cache cannot hand one out (chunked or wire-encoded), or on
    /// file.
    fn base_code_sources(
        &self,
        columns: &[QuadColumn],
    ) -> Option<(Option<[PrimitiveArray; 4]>, Vec<PrimitiveArray>)> {
        let every = QuadColumn::ALL.iter().all(|c| columns.contains(c));
        let all = if every {
            Some(self.base_code_primitives()?)
        } else {
            None
        };
        let asked = match &all {
            Some(all) => columns.iter().map(|c| all[c.index()].clone()).collect(),
            None => columns
                .iter()
                .map(|&c| self.base_code_primitive(c))
                .collect::<Option<_>>()?,
        };
        Some((all, asked))
    }

    /// The base rows this view covers, without the tail, in chunks of at
    /// most `batch_rows` rows of at least the `columns` (primary column
    /// names). In memory a window of live ids ([`MemoryRows::Windows`]) is
    /// gathered per pull; a file view's read is chosen once
    /// ([`file_base_chunks`](Self::file_base_chunks)); any other read is
    /// [`base_read`](Self::base_read)'s, cut to the cap.
    async fn base_chunks(
        &self,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<RowChunkStream> {
        if let Some(memory) = self.quads.in_memory()
            && let MemoryRows::Windows(windows) = self.memory_rows(batch_rows)?
        {
            // Only the columns asked for are gathered.
            let base = projected(&memory.base, columns)?;
            let probes = Arc::clone(&memory.probes);
            return Ok(stream::iter(windows.map(move |ids| {
                observe!(Gather {
                    width: base.dtype().as_struct_fields_opt().map(|f| f.nfields())
                });
                gather_live(&base, &RowSelection::Ids(ids), None, Some(&probes))
            }))
            .boxed());
        }
        #[cfg(feature = "file-io")]
        if let Some(base) = self.quads.file() {
            return self.file_base_chunks(base, columns, batch_rows).await;
        }
        self.base_read(columns).await?.chunks(batch_rows)
    }

    /// How this in-memory view's base rows are cut into chunks of at most
    /// `batch_rows` rows: a contiguous selection without tombstones is a
    /// [`MemoryRows::Run`], and any other is cut into windows of live ids. A
    /// served view's pending selection materializes here.
    fn memory_rows(&self, batch_rows: usize) -> Result<MemoryRows> {
        let base_len = self.quads.base_len();
        let selection = self.quads.selection.materialized()?;
        if self.quads.deleted.is_none()
            && let SelectedRows::Run(run) = selection.as_rows(base_len)
        {
            return Ok(MemoryRows::Run(run));
        }
        Ok(MemoryRows::Windows(live_id_windows(
            &selection,
            self.quads.deleted.clone(),
            base_len,
            batch_rows,
        )))
    }

    /// A file view's base chunks, read as a stream's plan has it
    /// ([`StreamedPlan`]): a pending filter the reads by id leave to the
    /// masks split by split as the stream reaches them, only `columns` read
    /// at the rows each split's mask keeps; a selection taken by id cut into
    /// windows of at most `batch_rows` live ids, each taken
    /// ([`take_file_rows`]) when its chunk is pulled; a point or scan read
    /// [`FileReadScope::rows`](crate::store::read::file_plan::FileReadScope::rows)'s.
    /// Every chunk is cut to the cap.
    #[cfg(feature = "file-io")]
    async fn file_base_chunks(
        &self,
        base: &FileBase,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<RowChunkStream> {
        let (scope, read) = match self.streamed_plan(base, columns).await? {
            StreamedPlan::SplitReads(reads) => {
                return Ok(stream_split_reads(reads)
                    .flat_map(move |rows| {
                        stream::iter(match rows {
                            Ok(rows) => rechunk(rows, batch_rows).collect(),
                            Err(e) => vec![Err(e)],
                        })
                    })
                    .boxed());
            }
            StreamedPlan::Exact(scope, read) => (scope, read),
        };
        let ids = match read {
            ExactRead::Take(ids) => ids,
            read => return scope.rows(read).await?.chunks(batch_rows),
        };
        let file = Arc::clone(&base.file);
        let columns: Arc<[&'static str]> = Arc::from(scope.by_id_columns);
        // Each take reads the columns a window's filter left free and fills
        // the ones it pinned ([`FileFilter::pins`](crate::store::FileFilter::pins)).
        let pins = scope.pins.clone();
        let read_columns: Arc<[&'static str]> = Arc::from(pins.columns_to_read(&columns));
        Ok(stream::iter(id_windows(ids, batch_rows).enumerate())
            .then(move |(k, window)| {
                let (file, columns) = (Arc::clone(&file), Arc::clone(&columns));
                let (pins, read_columns) = (pins.clone(), Arc::clone(&read_columns));
                async move {
                    // A take declines only an empty id list, and no
                    // window is empty: a decline is a broken invariant.
                    let rows = take_file_rows(&file, &read_columns, &window)
                        .await?
                        .ok_or_else(|| {
                            VortexRdfError::Vortex(vortex_error::vortex_err!(
                                "the take of id window {k} ({} ids from base row {}) declined",
                                window.len(),
                                window.first().copied().unwrap_or_default()
                            ))
                        })?;
                    pins.fill_rows(rows, &columns)
                }
            })
            .boxed())
    }

    /// The tail's live rows cut to `batch_rows`, each slice cut when it is
    /// reached; none without a tail.
    fn tail_chunks(
        &self,
        batch_rows: usize,
    ) -> Result<impl Iterator<Item = Result<ArrayRef>> + Send + 'static> {
        let rows = self
            .tail
            .as_ref()
            .map(|tail| tail.live_rows())
            .transpose()?;
        Ok(rows
            .into_iter()
            .flat_map(move |rows| rechunk(rows, batch_rows)))
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

/// The `picked` columns of `codes` cut into slices of at most `batch_rows`
/// rows, sharing their buffers: a served file chunk's code chunks.
#[cfg(feature = "file-io")]
fn code_slices(
    codes: [Buffer<TermCode>; 4],
    picked: &[usize],
    batch_rows: usize,
) -> Vec<Result<Vec<Buffer<TermCode>>>> {
    let rows = codes[0].len();
    (0..rows)
        .step_by(batch_rows)
        .map(|start| {
            let end = start.saturating_add(batch_rows).min(rows);
            Ok(picked.iter().map(|&k| codes[k].slice(start..end)).collect())
        })
        .collect()
}

/// Code chunks of `columns`, one per window of base row ids, each gathered
/// when its chunk is pulled: from `all` four primitives in one pass when
/// every column is asked for, else from the `asked` ones alone.
fn gathered_code_chunks(
    all: Option<[PrimitiveArray; 4]>,
    asked: Vec<PrimitiveArray>,
    columns: &[QuadColumn],
    windows: impl Iterator<Item = Buffer<RowId>> + Send + 'static,
) -> CodeChunkStream {
    let picked: Vec<usize> = columns.iter().map(|c| c.index()).collect();
    stream::iter(windows.map(move |ids| {
        observe!(Gather { width: None });
        Ok(match &all {
            // Every column asked for: the four gathered in one pass.
            Some(all) => {
                let codes = gather_codes(all, &ids);
                picked.iter().map(|&k| codes[k].clone()).collect()
            }
            // A subset: only its columns.
            None => asked.iter().map(|prim| gather_column(prim, &ids)).collect(),
        })
    }))
    .boxed()
}

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
    selection: &RowSelection,
    deleted: Option<Mask>,
    base_len: usize,
    batch_rows: usize,
) -> IdWindows {
    match (selection.as_rows(base_len), deleted) {
        (SelectedRows::Ids(ids), None) => Box::new(id_windows(ids.clone(), batch_rows)),
        (SelectedRows::Run(run), deleted) => {
            filtered_windows(run.start as RowId..run.end as RowId, deleted, batch_rows)
        }
        (SelectedRows::Ids(ids), deleted) => {
            let ids = ids.clone();
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

/// `base`, a struct of primary columns, cut to `columns` (each once, in the
/// order first asked for) without copying them; a base of any other shape as
/// it is.
fn projected(base: &ArrayRef, columns: &[&'static str]) -> Result<ArrayRef> {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::Struct;
    use vortex_array::dtype::FieldName;

    let Ok(struct_arr) = base.clone().try_downcast::<Struct>() else {
        return Ok(base.clone());
    };
    let mut names: Vec<FieldName> = Vec::with_capacity(columns.len());
    for &name in columns {
        if !names.iter().any(|held| held.as_ref() == name) {
            names.push(FieldName::from(name));
        }
    }
    Ok(struct_arr.project(&names)?.into_array())
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
