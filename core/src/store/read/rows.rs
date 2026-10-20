//! The row/code read surface: the one base-row seam every row reader goes
//! through ([`BaseRead`]), code columns, and the selected-rows plumbing the
//! serialization and compaction paths share.

#[cfg(feature = "file-io")]
use std::borrow::Cow;

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::RawQuad;
use crate::store::array::{rechunk, with_subject_stamp};
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::read::chunks::{RowChunkStream, extract_codes};
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads::{self, ExactRead};
use crate::store::scan::gather::gather_live;
use crate::store::schema::{self, RowId, TermCode};
use crate::store::view::selection::RowSelection;
#[cfg(feature = "file-io")]
use crate::store::view::selection::ViewSelection;
use crate::store::{Backend, MemoryBase};

use futures::stream::{self, StreamExt};
use vortex_array::ArrayRef;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;

#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, root, select};
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

use crate::store::VortexRdfStore;

/// A view's base rows as [`base_read`](VortexRdfStore::base_read) reads
/// them: already read, or the scan that reads them.
pub(in crate::store) enum BaseRead {
    /// The rows, gathered in memory or read from the file by id.
    Rows(ArrayRef),
    /// The restricted scan of the file, to run split by split.
    #[cfg(feature = "file-io")]
    Scan(Box<ScanBuilder<ArrayRef>>),
}

impl BaseRead {
    /// The rows as one array, the scan run to completion.
    pub(in crate::store) async fn collect(self) -> Result<ArrayRef> {
        match self {
            BaseRead::Rows(rows) => Ok(rows),
            #[cfg(feature = "file-io")]
            BaseRead::Scan(scan) => file_reads::read_all_rows(*scan).await,
        }
    }

    /// The rows in chunks of at most `batch_rows` rows, in row order: slices
    /// of the rows, or the scan's chunks each cut to the cap, every slice cut
    /// when its chunk is pulled; a chunk's read error, or a slice's, carried
    /// as its error item.
    pub(in crate::store) fn chunks(self, batch_rows: usize) -> Result<RowChunkStream> {
        match self {
            BaseRead::Rows(rows) => Ok(stream::iter(rechunk(rows, batch_rows)).boxed()),
            #[cfg(feature = "file-io")]
            BaseRead::Scan(scan) => Ok(file_reads::stream_rows(*scan)?
                .flat_map(move |chunk| match chunk {
                    Ok(chunk) => stream::iter(rechunk(chunk, batch_rows)).left_stream(),
                    Err(e) => stream::iter([Err(e)]).right_stream(),
                })
                .boxed()),
        }
    }
}

/// The codes at `ids` of each of the four code primitives `prims`, in the
/// order of `ids`: one gather per column.
pub(in crate::store) fn gather_codes(
    prims: &[PrimitiveArray; 4],
    ids: &[RowId],
) -> [Buffer<TermCode>; 4] {
    #[cfg(test)]
    column_hooks::note(true);
    prims.each_ref().map(|prim| {
        let slice = prim.as_slice::<TermCode>();
        Buffer::from_iter(ids.iter().map(|&i| slice[i as usize]))
    })
}

pub(in crate::store) fn gather_column(prim: &PrimitiveArray, ids: &[RowId]) -> Buffer<TermCode> {
    #[cfg(test)]
    column_hooks::note(false);
    let slice = prim.as_slice::<TermCode>();
    Buffer::from_iter(ids.iter().map(|&i| slice[i as usize]))
}

impl VortexRdfStore {
    /// The rows this view selects as four [`TermCode`] columns (`s`, `p`,
    /// `o`, `g`), with no row read: off the answering index's own columns
    /// when the view's serve plan covers them, else from the base's canonical
    /// code primitives ([`base_code_columns`](Self::base_code_columns)). A
    /// builder-compressed column behind a `vortex.shared` wrapper qualifies:
    /// its primitive is decoded once into the wrapper's cache
    /// (`shared_code_primitive`) and shared zero-copy from then on.
    ///
    /// `None` for a non-Dictionary layout, a non-empty tail (its strings are
    /// not in the dictionary), a file-backed source, or base columns the
    /// cache cannot hand out as non-nullable code primitives (chunked or
    /// wire-encoded); callers then read the rows, as
    /// [`code_columns_gathered`](Self::code_columns_gathered) does after
    /// trying this.
    pub(crate) fn code_columns(&self) -> Option<[Buffer<TermCode>; 4]> {
        if !self.is_code_view() {
            return None;
        }
        // The answering index's columns hold this view's codes as one
        // contiguous run: no deferred row ids to compute, no scattered
        // gather over the primaries.
        if let Some(plan) = &self.quads.in_memory()?.serve
            && let Some(columns) = plan.code_columns(self.quads.deleted.as_ref())
        {
            return Some(columns);
        }
        self.base_code_columns()
    }

    /// [`code_columns`](Self::code_columns) without the served path: the
    /// view's codes from the base's own columns in base row order, whatever
    /// answered the match (the order [`code_chunks`](Self::code_chunks)
    /// yields). A served match's pending selection materializes here. `None`
    /// under the same conditions as `code_columns`.
    pub(crate) fn base_code_columns(&self) -> Option<[Buffer<TermCode>; 4]> {
        if !self.is_code_view() {
            return None;
        }
        let prims = self.base_code_primitives()?;
        // A gather by row id needs the exact selection: a served match's
        // deferred decode and sort run here.
        let selection = self.quads.selection.materialized().ok()?;
        // Without tombstones a contiguous selection shares the base's buffers
        // (a `Buffer` slice is a refcount bump) and an id list is a
        // branch-free gather; only tombstoned views test liveness per row.
        let Some(deleted) = &self.quads.deleted else {
            return Some(match &selection {
                RowSelection::All => prims.map(|prim| prim.into_buffer::<TermCode>()),
                RowSelection::Range(r) => prims.map(|prim| {
                    prim.into_buffer::<TermCode>()
                        .slice(r.start as usize..r.end as usize)
                }),
                RowSelection::Ids(ids) => gather_codes(&prims, ids),
            });
        };
        let base_len = self.quads.base_len();
        let live = |i: usize| !deleted.value(i);
        let column = |prim: &PrimitiveArray| -> Buffer<TermCode> {
            let slice = prim.as_slice::<TermCode>();
            match &selection {
                RowSelection::All => {
                    Buffer::from_iter((0..base_len).filter(|&i| live(i)).map(|i| slice[i]))
                }
                RowSelection::Range(r) => Buffer::from_iter(
                    (r.start as usize..r.end as usize)
                        .filter(|&i| live(i))
                        .map(|i| slice[i]),
                ),
                RowSelection::Ids(ids) => Buffer::from_iter(
                    ids.iter()
                        .map(|&i| i as usize)
                        .filter(|&i| live(i))
                        .map(|i| slice[i]),
                ),
            }
        };
        Some(prims.each_ref().map(column))
    }

    /// The base's `s`, `p`, `o` and `g` code columns as canonical
    /// primitives: what [`base_code_columns`](Self::base_code_columns) slices
    /// or gathers. A builder-compressed column behind a `vortex.shared`
    /// wrapper is decoded once into the wrapper's cache
    /// (`shared_code_primitive`). `None` on file, or for a base column the
    /// cache cannot hand out as a non-nullable code primitive (chunked or
    /// wire-encoded).
    pub(in crate::store) fn base_code_primitives(&self) -> Option<[PrimitiveArray; 4]> {
        use vortex_array::arrays::Struct;
        let base = &self.quads.in_memory()?.base;
        let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
        let primitive = |name: &str| {
            crate::store::resident::shared_code_primitive(
                struct_arr.unmasked_field_by_name(name).ok()?,
            )
        };
        let [s, p, o, g] = schema::PRIMARY_COLUMNS;
        Some([primitive(s)?, primitive(p)?, primitive(o)?, primitive(g)?])
    }

    /// The rows this view selects as four [`TermCode`] columns (`s`, `p`,
    /// `o`, `g`): the `code_columns` fast path when it answers, a served
    /// file view's codes off the answering index's run while its ids are
    /// deferred, else the view's base rows read as every row reader reads
    /// them (`base_read`), one code column taken per role, so a code view of
    /// any shape answers. `None` only where codes are not the store's
    /// vocabulary: a non-Dictionary layout, or a non-empty tail, whose terms
    /// the dictionary lacks. The bindings' code-column reads go through here.
    pub async fn code_columns_gathered(&self) -> Result<Option<[Buffer<TermCode>; 4]>> {
        if let Some(columns) = self.code_columns() {
            return Ok(Some(columns));
        }
        if !self.is_code_view() {
            return Ok(None);
        }
        // Codes off the index's run: no row ids computed, no gather.
        #[cfg(feature = "file-io")]
        if let ViewSelection::Pending(_) = &self.quads.selection
            && let Some(FileBase {
                serve: Some(plan), ..
            }) = self.quads.file()
            && let Some(columns) = plan.code_columns(self.quads.deleted.as_ref()).await?
        {
            return Ok(Some(columns));
        }
        let rows = self
            .base_read(&schema::PRIMARY_COLUMNS)
            .await?
            .collect()
            .await?;
        let codes = extract_codes(rows, &schema::PRIMARY_COLUMNS)?;
        Ok(Some(
            codes
                .try_into()
                .expect("one code buffer per primary column"),
        ))
    }

    /// The base rows this view covers, without the tail, in base row order,
    /// as [`BaseRead`] carries them: at least the `columns` (primary column
    /// names), each found by name. Every row reader reads the base through
    /// here; the served fast paths read the answering index instead.
    ///
    /// The read is over the exact selection: a served view's pending
    /// selection materializes here, since its plan orders the rows by the
    /// index; a pending one without a plan sits only on a view built to be
    /// counted or windowed, which is never read (`expect_exact`). In memory
    /// the live selected rows are gathered, every primary column
    /// ([`in_memory_base_rows`](Self::in_memory_base_rows)). On file a
    /// selection the reads by id serve is read by id (`exact_rows_by_id`),
    /// through every primary column while a filter is pending (the read
    /// checks it on the columns it reads); anything else, or a read by id
    /// that declines, is the restricted scan of `columns`
    /// (`restricted_file_scan`).
    #[cfg_attr(not(feature = "file-io"), allow(unused_variables))]
    pub(in crate::store) async fn base_read(&self, columns: &[&'static str]) -> Result<BaseRead> {
        match &self.quads.backend {
            Backend::InMemory(memory) => Ok(BaseRead::Rows(self.in_memory_base_rows(memory)?)),
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                let (selection, by_id_columns) = self.file_read_scope(base, columns).await?;
                let (file, filter) = (&base.file, base.filter.as_ref());
                let deleted = self.quads.deleted.as_ref();
                match Self::exact_rows_by_id(file, by_id_columns, filter, &selection, deleted)
                    .await?
                {
                    Some(rows) => Ok(BaseRead::Rows(rows)),
                    None => Ok(BaseRead::Scan(Box::new(Self::restricted_file_scan(
                        file, columns, filter, &selection, deleted,
                    )?))),
                }
            }
        }
    }

    /// What a read of this file view's `base` rows covers: its exact
    /// selection, a served view's pending one materialized here (the plan
    /// orders the rows by the index, and the read needs base order), and the
    /// columns a read by id reads, every primary column while a filter is
    /// pending (the read checks it on the columns it reads), else `columns`.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn file_read_scope<'a>(
        &'a self,
        base: &FileBase,
        columns: &'a [&'static str],
    ) -> Result<(Cow<'a, RowSelection>, &'a [&'static str])> {
        let selection = match base.serve {
            // Runs the deferred index-child read once, cached on the view.
            Some(_) => Cow::Owned(self.quads.selection.materialized_async().await?),
            None => Cow::Borrowed(self.quads.selection.expect_exact()),
        };
        let by_id_columns = match base.filter {
            Some(_) => self.layout.strategy().primary_column_names(),
            None => columns,
        };
        Ok((selection, by_id_columns))
    }

    /// An in-memory view's live base rows in base row order, every primary
    /// column, `memory` its base: its exact selection (see
    /// [`base_read`](Self::base_read)) gathered with the tombstones dropped,
    /// point by point through the base's probes when the selection is small.
    pub(in crate::store) fn in_memory_base_rows(&self, memory: &MemoryBase) -> Result<ArrayRef> {
        let materialized;
        let selection = match &memory.serve {
            Some(_) => {
                materialized = self.quads.selection.materialized()?;
                &materialized
            }
            None => self.quads.selection.expect_exact(),
        };
        gather_live(
            &memory.base,
            selection,
            self.quads.deleted.as_ref(),
            Some(&memory.probes),
        )
    }

    /// The base rows this view covers, without the tail, as one array: the
    /// [`base_read`](Self::base_read) read to completion, with the subject
    /// stamp restored. A read keeps base row order, so the base's
    /// `(s, p, o, g)` provenance (an in-memory base's stamp, the file's
    /// recorded `quads_sorted`) carries to the rows; the gather and a
    /// multi-chunk read drop the stamp, and without it a re-serialization
    /// would write the rows as `quads_sorted:false` and every later reader
    /// would lose the subject binary search.
    pub(in crate::store) async fn base_selected_rows(&self) -> Result<ArrayRef> {
        let rows = self
            .base_read(self.layout.strategy().primary_column_names())
            .await?
            .collect()
            .await?;
        with_subject_stamp(rows, self.quads.subject_sorted())
    }

    /// The rows of `columns` at an exact file `selection` with `filter`
    /// pending and `deleted` tombstoned, read by id as [`ExactRead`] chooses
    /// ([`exact_read`](Self::exact_read), then
    /// [`read_exact`](Self::read_exact)). `None` when `ExactRead` leaves the
    /// selection to the scan, or a read by id declines; the caller then
    /// scans.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn exact_rows_by_id(
        file: &NativeStoreFile,
        columns: &[&'static str],
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<Option<ArrayRef>> {
        let read = Self::exact_read(file, columns, filter, selection, deleted)?;
        Self::read_exact(file, columns, filter, read).await
    }

    /// How the rows of `columns` at an exact file `selection`, with `filter`
    /// pending and `deleted` tombstoned, are read ([`ExactRead::of`]).
    #[cfg(feature = "file-io")]
    pub(in crate::store) fn exact_read(
        file: &NativeStoreFile,
        columns: &[&'static str],
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<ExactRead> {
        ExactRead::of(file, columns, filter, selection, deleted)
    }

    /// The rows of `columns` that `read` reads by id, with `filter` pending:
    /// point by point through the file's cached chunk probes
    /// ([`file_reads::file_point_rows`]), or taken at the ids
    /// ([`file_reads::take_file_rows`]). `None` for [`ExactRead::Scan`], or a
    /// read by id that declines; the caller then scans.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn read_exact(
        file: &NativeStoreFile,
        columns: &[&'static str],
        filter: Option<&Expression>,
        read: ExactRead,
    ) -> Result<Option<ArrayRef>> {
        match read {
            ExactRead::Points(rows) => {
                file_reads::file_point_rows(file, columns, filter, rows).await
            }
            ExactRead::Take(ids) => file_reads::take_file_rows(file, columns, &ids).await,
            ExactRead::Scan => Ok(None),
        }
    }

    /// The scan every unserved file read starts from: a projection of
    /// `columns` (primary column names, in the order the chunks carry them;
    /// index columns never surface), the view's pushed-down filter for the
    /// pattern components no index resolved, and its row selection, with
    /// tombstoned rows excluded, for those one did. The scan decodes the
    /// projected columns and the columns the filter references.
    #[cfg(feature = "file-io")]
    fn restricted_file_scan(
        file: &NativeStoreFile,
        columns: &[&'static str],
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<ScanBuilder<ArrayRef>> {
        let mut scan = file.scan()?;
        // Filters and projections bind against the scan's scope (the root
        // dtype), so it is read before the projection replaces it. Binding
        // through the handle's memo keeps one identity per repeated shape
        // (see `BoundExprMemo`).
        let scope = scan.dtype()?;
        let memo = file.bound_exprs();
        scan = scan.with_projection(memo.bind(
            file_reads::QUAD_SCOPE,
            &select(columns, root()),
            &scope,
        )?);
        if let Some(f) = filter {
            scan = scan.with_filter(memo.bind(file_reads::QUAD_SCOPE, f, &scope)?);
        }
        Ok(selection.restrict_scan(scan, deleted))
    }

    /// Every live quad this view covers, decoded to raw N-Triples term strings
    /// (a file-backed dictionary lifted resident for the decode), base rows
    /// first in base row order, then the tail's live rows, and how many of
    /// them came from the base.
    pub(in crate::store) async fn live_raw_quads(&self) -> Result<(Vec<RawQuad>, usize)> {
        #[cfg(test)]
        crate::store::test_hooks::note_gather();
        let base = self.base_selected_rows().await?;
        let mut raws = self.layout.raw_quads_async(&base).await?;
        let base_rows = raws.len();
        if let Some(tail) = &self.tail {
            raws.extend(tail.raw_quads()?);
        }
        Ok((raws, base_rows))
    }
}

/// Test hook: how many gathers this thread made from code primitives, by
/// path: all four columns in one pass ([`gather_codes`]), or one column
/// ([`gather_column`]).
#[cfg(test)]
pub(crate) mod column_hooks {
    use std::cell::Cell;

    thread_local! {
        static FOUR: Cell<usize> = const { Cell::new(0) };
        static ONE: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one gather, of all four columns or of one.
    pub(super) fn note(four: bool) {
        let cell = if four { &FOUR } else { &ONE };
        cell.with(|c| c.set(c.get() + 1));
    }

    /// The gathers on this thread so far: `(of all four columns, of one)`.
    pub(crate) fn gathers() -> (usize, usize) {
        (FOUR.with(Cell::get), ONE.with(Cell::get))
    }
}
