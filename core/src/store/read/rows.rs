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
    /// of the rows, or the scan's chunks each cut to the cap, a chunk's read
    /// error carried as one error item.
    pub(in crate::store) fn chunks(self, batch_rows: usize) -> Result<RowChunkStream> {
        match self {
            BaseRead::Rows(rows) => {
                Ok(stream::iter(rechunk(rows, batch_rows)?.into_iter().map(Ok)).boxed())
            }
            #[cfg(feature = "file-io")]
            BaseRead::Scan(scan) => Ok(file_reads::stream_rows(*scan)?
                .flat_map(move |chunk| {
                    let pieces: Vec<Result<ArrayRef>> =
                        match chunk.and_then(|chunk| rechunk(chunk, batch_rows)) {
                            Ok(pieces) => pieces.into_iter().map(Ok).collect(),
                            Err(e) => vec![Err(e)],
                        };
                    stream::iter(pieces)
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

/// The codes at `ids` of one code primitive, `prim`, in the order of `ids`:
/// [`gather_codes`] for a single column.
pub(in crate::store) fn gather_column(prim: &PrimitiveArray, ids: &[RowId]) -> Buffer<TermCode> {
    #[cfg(test)]
    column_hooks::note(false);
    let slice = prim.as_slice::<TermCode>();
    Buffer::from_iter(ids.iter().map(|&i| slice[i as usize]))
}

impl VortexRdfStore {
    /// The rows this view selects, as four [`TermCode`] columns (`s`, `p`,
    /// `o`, `g`) — read off the answering index's own columns when the view
    /// carries a serve plan that covers them, else gathered directly from the
    /// base's canonical primitive slices.
    ///
    /// `None` whenever codes cannot be served both cheaply and correctly:
    /// a non-Dictionary layout, a non-empty append tail (its strings are not
    /// in the cached dictionary), a file-backed source, or base columns not
    /// reachable as canonical non-nullable code primitives (e.g. chunked or
    /// wire-compressed). Callers fall back to reading the rows (`base_read`).
    ///
    /// A builder-compressed column behind a `vortex.shared` wrapper still
    /// qualifies: its canonical primitive is materialized once into the
    /// wrapper's one-way cache (`shared_code_primitive`) and shared zero-copy
    /// by every later call and every view over the base — the payload path
    /// pays a first-touch decode instead of losing the buffer-sharing fast
    /// path.
    ///
    /// Serving codes off the base's buffers skips the slice-gather-canonicalize
    /// pipeline that [`code_columns_gathered`](Self::code_columns_gathered) —
    /// the bindings' entry point — otherwise runs; that method tries this fast
    /// path first.
    pub(crate) fn code_columns(&self) -> Option<[Buffer<TermCode>; 4]> {
        if !self.is_code_view() {
            return None;
        }
        // Served fast path: the answering index's own columns already hold
        // this view's codes as one contiguous run, so reading them there
        // costs neither the row-id materialization this view deferred at
        // match time nor a scattered gather over the primaries.
        if let Some(plan) = &self.quads.in_memory()?.serve
            && let Some(columns) = plan.code_columns(self.quads.deleted.as_ref())
        {
            return Some(columns);
        }
        self.base_code_columns()
    }

    /// [`code_columns`](Self::code_columns) without the served fast path: the
    /// view's codes gathered from the base's own columns in base row order,
    /// whatever answered the match — the order
    /// [`code_chunks`](Self::code_chunks) yields. A served match's pending
    /// selection materializes here. `None` under the same conditions as
    /// `code_columns`, or when a base column is not a canonical code
    /// primitive the shared cache can hand out.
    pub(crate) fn base_code_columns(&self) -> Option<[Buffer<TermCode>; 4]> {
        if !self.is_code_view() {
            return None;
        }
        let prims = self.base_code_primitives()?;
        // No plan (or a plan that declined): codes are gathered by row id, so
        // a served match's pending selection materializes here (the in-memory
        // decode+sort it deferred at match time).
        let selection = self.quads.selection.materialized().ok()?;
        // Contiguous, tombstone-free selections share the base's buffers
        // zero-copy (a `Buffer` slice is a refcount bump); a tombstone-free id
        // list is a branch-free gather; only tombstoned views pay a
        // per-element liveness test.
        let Some(deleted) = &self.quads.deleted else {
            return Some(match &selection {
                RowSelection::All => prims.map(|prim| prim.into_buffer::<TermCode>()),
                RowSelection::Range(r) => prims.map(|prim| {
                    prim.into_buffer::<TermCode>()
                        .slice(r.start as usize..r.end as usize)
                }),
                // An index-resolved match without deletes — the bindings'
                // common payload shape.
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
    /// wrapper is materialized once into the wrapper's cache
    /// (`shared_code_primitive`). `None` on file, or for a base column that
    /// is not a canonical non-nullable code primitive the cache can hand out
    /// (e.g. chunked or wire-compressed).
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

    /// The rows this view selects as four [`TermCode`] columns, gathering
    /// them when `code_columns`' zero-copy fast path
    /// does not apply.
    ///
    /// The fallback reads the view's base rows as every row reader does
    /// (`base_read`: gathered in memory, read by id or scanned on file), then
    /// takes one code column per role, so a file-backed store, a narrowed view
    /// whose base columns are chunked, or any other non-canonical shape still
    /// answers codes. Only the cases where codes are not the store's
    /// vocabulary at all yield `None`: a non-Dictionary layout, or a non-empty
    /// append tail (whose terms are absent from the cached dictionary, so its
    /// codes would address a different one).
    ///
    /// This is the payload path behind the bindings' code-column reads; they
    /// call it instead of re-implementing the gather.
    pub async fn code_columns_gathered(&self) -> Result<Option<[Buffer<TermCode>; 4]>> {
        if let Some(columns) = self.code_columns() {
            return Ok(Some(columns));
        }
        if !self.is_code_view() {
            return Ok(None);
        }
        // A served file view whose row ids are still deferred reads its codes
        // off the answering index's run instead of computing the ids and
        // gathering the primary rows at them.
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
    /// index; any other selection is exact already, a pending one without a
    /// plan sitting only on a view built to be counted or windowed, which is
    /// never read (`expect_exact`). In memory the live selected rows are
    /// gathered, every primary column
    /// ([`in_memory_base_rows`](Self::in_memory_base_rows)). On file a
    /// selection the reads by id serve is read by id (`exact_rows_by_id`) —
    /// through every primary column while a filter is pending, which the read
    /// checks on the columns it reads — and anything else, or a read by id
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
    /// stamp.
    ///
    /// A read keeps base row order and this view only narrows it, so the
    /// base's `(s, p, o, g)` provenance (an in-memory base's stamp, the file's
    /// recorded `quads_sorted`) carries to the rows. The gather and the
    /// multi-chunk read lose the stat, so it is restored here — without it, a
    /// re-serialization would demote the rows to `quads_sorted:false` and
    /// every later reader would lose the subject binary search.
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

    /// The scan every unserved file read starts from: the `columns` only —
    /// primary column names, in the order the chunks carry them, so the scan
    /// decodes no column the caller does not read (index columns are internal
    /// and never surfaced) — with the restrictions the view accumulated via
    /// `match_pattern` applied: a pushed-down filter for the components no
    /// index resolved, and the row selection (with tombstoned rows excluded)
    /// for those it did.
    #[cfg(feature = "file-io")]
    fn restricted_file_scan(
        file: &NativeStoreFile,
        columns: &[&'static str],
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<ScanBuilder<ArrayRef>> {
        let mut scan = file.scan()?;
        // The scan's scope (the quad-source root dtype) is what filters and
        // projections bind against — read it before the projection replaces
        // it. Binding goes through the handle's memo so a repeated shape
        // keeps one identity (see `BoundExprMemo`).
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
    /// (a file-backed dictionary lifted resident for the decode) — base rows
    /// first (in view order), then the tail's live rows — and how many of
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
