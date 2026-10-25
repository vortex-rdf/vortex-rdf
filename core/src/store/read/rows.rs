//! The row/code read surface: the one base-row seam every row reader goes
//! through ([`BaseRead`]), code columns, and the selected-rows plumbing the
//! serialization and compaction paths share.

#[cfg(feature = "file-io")]
use std::borrow::Cow;
#[cfg(feature = "file-io")]
use std::sync::Arc;

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::RawQuad;
use crate::store::array::{rechunk, with_subject_stamp};
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::read::chunks::{RowChunkStream, extract_codes};
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter::{self, SplitMasks};
use crate::store::scan::gather::gather_live;
#[cfg(feature = "file-io")]
use crate::store::scan::read_plan::{ExactRead, MaskedRead};
#[cfg(feature = "file-io")]
use crate::store::scan::{file_exprs, file_reads, file_scans};
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
            BaseRead::Scan(scan) => file_scans::read_all_rows(*scan).await,
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
            BaseRead::Scan(scan) => Ok(file_scans::stream_rows(*scan)?
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
    /// any shape answers. A file view's pending filter of code equalities
    /// that a read by id cannot serve is evaluated to its split masks first,
    /// and the rows they keep are read point by point, by one take, or by
    /// projecting each split at its mask, whichever their density makes
    /// cheapest. `None` only where codes are not the store's vocabulary: a
    /// non-Dictionary layout, or a non-empty tail, whose terms the dictionary
    /// lacks. The bindings' code-column reads go through here.
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
        // On file the read is the one `base_read` makes, decided once; a
        // point-sized read by id fills the code buffers straight from the
        // chunk probes, with the same rows and filter checks, and a pending
        // filter the reads by id leave to the scan is evaluated to its masks
        // first, the rows they keep read after. A window's kept masks are
        // read the same way, the filter not run again.
        #[cfg(feature = "file-io")]
        if let Backend::File(base) = &self.quads.backend {
            if let Some(masks) = &base.masks {
                let primary = self.layout.strategy().primary_column_names();
                return Self::masked_codes(&base.file, primary, masks)
                    .await
                    .map(Some);
            }
            let (scope, read) = self.file_read(base, &schema::PRIMARY_COLUMNS).await?;
            if let Some(masks) = scope.masks_first(&read).await? {
                return Self::masked_codes(scope.file, scope.by_id_columns, &masks)
                    .await
                    .map(Some);
            }
            let rows = match read {
                ExactRead::Points(rows) => {
                    if let Some(codes) = file_reads::file_point_codes(
                        scope.file,
                        scope.by_id_columns,
                        scope.filter,
                        rows,
                    )
                    .await?
                        && let Some(codes) = primary_order(scope.by_id_columns, codes)
                    {
                        return Ok(Some(codes));
                    }
                    // Declined as the row read would decline: the scan reads.
                    BaseRead::Scan(Box::new(scope.scan()?))
                }
                read => scope.rows(read).await?,
            }
            .collect()
            .await?;
            return primary_codes(rows).map(Some);
        }
        let rows = self
            .base_read(&schema::PRIMARY_COLUMNS)
            .await?
            .collect()
            .await?;
        primary_codes(rows).map(Some)
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
    /// selection the reads by id serve is read by id, through every primary
    /// column while a filter is pending (the read checks it on the columns it
    /// reads); anything else, or a read by id that declines, is the restricted
    /// scan of `columns` ([`FileReadScope::rows`]).
    #[cfg_attr(not(feature = "file-io"), allow(unused_variables))]
    pub(in crate::store) async fn base_read(&self, columns: &[&'static str]) -> Result<BaseRead> {
        match &self.quads.backend {
            Backend::InMemory(memory) => Ok(BaseRead::Rows(self.in_memory_base_rows(memory)?)),
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                let (scope, read) = self.file_read(base, columns).await?;
                scope.rows(read).await
            }
        }
    }

    /// What a read of this file view's `base` rows covers, and the
    /// [`ExactRead`] that reads them, chosen once: its exact selection, a
    /// served view's pending one materialized here (the plan orders the rows
    /// by the index, and the read needs base order); the columns a read by id
    /// reads, every primary column while a filter is pending (the read checks
    /// it on the columns it reads), else `columns`; and the filter still to
    /// run. A view whose window kept its filter's masks ([`FileBase::masks`])
    /// covers the ids the masks keep, with no filter to run.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn file_read<'a>(
        &'a self,
        base: &'a FileBase,
        columns: &'a [&'static str],
    ) -> Result<(FileReadScope<'a>, ExactRead)> {
        let (selection, by_id_columns, filter) = match &base.masks {
            Some(masks) => {
                let ids = Buffer::from(masks.first_ids(usize::MAX));
                (Cow::Owned(RowSelection::from_ids(ids)), columns, None)
            }
            None => {
                let selection = match base.serve {
                    // Runs the deferred index-child read once, cached on the view.
                    Some(_) => Cow::Owned(self.quads.selection.materialized_async().await?),
                    None => Cow::Borrowed(self.quads.selection.expect_exact()),
                };
                let filter = base.filter.as_ref();
                let by_id_columns = match filter {
                    Some(_) => self.layout.strategy().primary_column_names(),
                    None => columns,
                };
                (selection, by_id_columns, filter)
            }
        };
        let scope = FileReadScope {
            file: &base.file,
            columns,
            by_id_columns,
            filter,
            selection,
            deleted: self.quads.deleted.as_ref(),
        };
        let read = ExactRead::of(
            scope.file,
            scope.by_id_columns,
            scope.filter,
            &scope.selection,
            scope.deleted,
        )?;
        Ok((scope, read))
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

    /// The `(s, p, o, g)` code columns of the rows `masks` keep (a pending
    /// filter's split masks, in file order), `columns` the layout's primary
    /// columns, read as [`MaskedRead`] chooses with no filter left to check:
    /// point by point straight into the code buffers, by one take through the
    /// chunk probes, or each split projected at its mask. A read by id that a
    /// chunk declines falls to the next.
    #[cfg(feature = "file-io")]
    async fn masked_codes(
        file: &Arc<NativeStoreFile>,
        columns: &[&'static str],
        masks: &SplitMasks,
    ) -> Result<[Buffer<TermCode>; 4]> {
        let read = MaskedRead::of(file, masks)?;
        if read != MaskedRead::Project {
            let ids = masks.first_ids(usize::MAX);
            if read == MaskedRead::Points
                && let Some(codes) =
                    file_reads::file_point_codes(file, columns, None, ids.clone()).await?
                && let Some(codes) = primary_order(columns, codes)
            {
                return Ok(codes);
            }
            if let Some(rows) = file_reads::take_file_rows(file, columns, &ids).await?
                && let Some(codes) = primary_order(columns, extract_codes(rows, columns)?)
            {
                return Ok(codes);
            }
        }
        let rows = file_reads::masked_projection(file, &schema::PRIMARY_COLUMNS, masks).await?;
        primary_codes(rows)
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

/// A file view's base read, as [`VortexRdfStore::file_read`] scopes it: the
/// file, the columns the scan projects, the columns a read by id reads, the
/// filter still to run, the exact selection and the tombstones. The
/// [`ExactRead`] chosen for it travels beside it, so its id list moves into
/// the read that takes it.
#[cfg(feature = "file-io")]
pub(in crate::store) struct FileReadScope<'a> {
    pub(in crate::store) file: &'a Arc<NativeStoreFile>,
    /// The primary columns the scan projects, in the order its chunks carry
    /// them.
    pub(in crate::store) columns: &'a [&'static str],
    /// The columns a read by id reads: every primary column while a filter
    /// is pending, which the read checks on the columns it reads.
    pub(in crate::store) by_id_columns: &'a [&'static str],
    pub(in crate::store) filter: Option<&'a Expression>,
    pub(in crate::store) selection: Cow<'a, RowSelection>,
    pub(in crate::store) deleted: Option<&'a Mask>,
}

#[cfg(feature = "file-io")]
impl FileReadScope<'_> {
    /// The base rows `read` reads by id: point by point through the file's
    /// cached chunk probes ([`file_reads::file_point_rows`]), or taken at the
    /// ids ([`file_reads::take_file_rows`]). [`ExactRead::Scan`] and
    /// [`ExactRead::Masks`], which a streaming reader scans, and a read by id
    /// that declines are the restricted scan ([`scan`](Self::scan)).
    pub(in crate::store) async fn rows(&self, read: ExactRead) -> Result<BaseRead> {
        let rows = match read {
            ExactRead::Points(rows) => {
                file_reads::file_point_rows(self.file, self.by_id_columns, self.filter, rows)
                    .await?
            }
            ExactRead::Take(ids) => {
                file_reads::take_file_rows(self.file, self.by_id_columns, &ids).await?
            }
            ExactRead::Scan | ExactRead::Masks => None,
        };
        Ok(match rows {
            Some(rows) => BaseRead::Rows(rows),
            None => BaseRead::Scan(Box::new(self.scan()?)),
        })
    }

    /// The pending filter's masks over the selection, tombstones cleared,
    /// every split evaluated in file order as a window evaluates them, when
    /// `read` leaves the filter to the restricted scan
    /// ([`ExactRead::Masks`]): the first step of a one-shot read. `None` for
    /// any other read.
    pub(in crate::store) async fn masks_first(
        &self,
        read: &ExactRead,
    ) -> Result<Option<SplitMasks>> {
        match (read, self.filter) {
            (ExactRead::Masks, Some(filter)) => Ok(Some(
                file_filter::split_masks(self.file, filter, &self.selection, self.deleted).await?,
            )),
            _ => Ok(None),
        }
    }

    /// The scan every unserved file read starts from: a projection of the
    /// scope's columns (index columns never surface), the view's pushed-down
    /// filter for the pattern components no index resolved, and its row
    /// selection, with tombstoned rows excluded, for those one did. The scan
    /// decodes the projected columns and the columns the filter references.
    pub(in crate::store) fn scan(&self) -> Result<ScanBuilder<ArrayRef>> {
        let mut scan = self.file.scan()?;
        // Filters and projections bind against the scan's scope (the root
        // dtype), so it is read before the projection replaces it. Binding
        // through the handle's memo keeps one identity per repeated shape
        // (see `BoundExprMemo`).
        let scope = scan.dtype()?;
        let memo = self.file.bound_exprs();
        scan = scan.with_projection(memo.bind(
            file_exprs::QUAD_SCOPE,
            &select(self.columns, root()),
            &scope,
        )?);
        if let Some(f) = self.filter {
            #[cfg(test)]
            restricted_scan_hooks::note_filtered();
            scan = scan.with_filter(memo.bind(file_exprs::QUAD_SCOPE, f, &scope)?);
        }
        Ok(self.selection.restrict_scan(scan, self.deleted))
    }
}

/// The `(s, p, o, g)` code columns of a read of the primary columns.
fn primary_codes(rows: ArrayRef) -> Result<[Buffer<TermCode>; 4]> {
    Ok(extract_codes(rows, &schema::PRIMARY_COLUMNS)?
        .try_into()
        .expect("one code buffer per primary column"))
}

/// `codes`, one buffer per column of `columns`, as the `(s, p, o, g)` code
/// columns; `None` when a primary column is missing.
#[cfg(feature = "file-io")]
fn primary_order(
    columns: &[&str],
    mut codes: Vec<vortex_buffer::Buffer<TermCode>>,
) -> Option<[vortex_buffer::Buffer<TermCode>; 4]> {
    let mut take = |name: &str| {
        let idx = columns.iter().position(|c| *c == name)?;
        Some(std::mem::take(&mut codes[idx]))
    };
    Some([
        take(schema::COL_S)?,
        take(schema::COL_P)?,
        take(schema::COL_O)?,
        take(schema::COL_G)?,
    ])
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

/// Test hook: how many restricted file scans this thread built with a filter
/// pushed down ([`FileReadScope::scan`]), each evaluating that filter split by
/// split as it is read.
#[cfg(all(test, feature = "file-io"))]
pub(crate) mod restricted_scan_hooks {
    use std::cell::Cell;

    thread_local! {
        static FILTERED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one restricted scan built with a filter.
    pub(super) fn note_filtered() {
        FILTERED.set(FILTERED.get() + 1);
    }

    /// The restricted scans built with a filter on this thread so far.
    pub(crate) fn filtered() -> usize {
        FILTERED.get()
    }
}
