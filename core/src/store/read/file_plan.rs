//! How a file view's base rows are read, decided once from the view's state:
//! what a read covers ([`FileReadScope`]: the selection, the columns, the
//! filter still to run, the codes every selected row holds) and the plan
//! that reads it, one for a read collected whole and one for a stream
//! ([`CollectedPlan`], [`StreamedPlan`]). Every file read of a view's base
//! goes through here; a served view's plan reads the index child instead.
//!
//! The policy, in one place:
//!
//! - The masks a window kept of the view's filter ([`FileFilter::Kept`]): a
//!   one-shot read reads the rows they keep ([`MaskedRows`]); a stream reads
//!   their ids, as it reads any exact ids.
//! - A pending filter the reads by id leave to the masks
//!   ([`ExactRead::Masks`]): a one-shot read evaluates every split's mask
//!   first, in file order, then reads the rows the masks keep; a stream
//!   evaluates each split's mask when it reaches the split, then reads the
//!   rows that mask keeps ([`file_reads::masks_first_split_reads`]).
//! - Anything else is the [`ExactRead`]: rows by id, or the restricted scan
//!   ([`FileReadScope::rows`]).

use std::borrow::Cow;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::expr::{root, select};
use vortex_buffer::Buffer;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::Mask;

use crate::error::Result;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::read::chunks::extract_codes;
use crate::store::read::rows::BaseRead;
use crate::store::scan::file_exprs::{self, CodeEqs, PushedFilter};
use crate::store::scan::file_filter;
use crate::store::scan::file_reads::{self, MaskedRows, SplitRead};
use crate::store::scan::read_plan::ExactRead;
use crate::store::schema::TermCode;
use crate::store::view::selection::RowSelection;
use crate::store::{FileBase, FileFilter, VortexRdfStore};

/// How a one-shot read (collected whole: `quads_vec`, `shared_quads_vec`,
/// `code_columns_gathered`, a whole-base read) takes a file view's base rows.
pub(in crate::store) enum CollectedPlan<'a> {
    /// The rows a filter's split masks keep: masks a window kept, or a
    /// pending filter's evaluated over every split first.
    Masked(MaskedRows<'a>),
    /// Rows by id, or the restricted scan.
    Exact(FileReadScope<'a>, ExactRead),
}

/// How a stream takes a file view's base rows.
pub(in crate::store) enum StreamedPlan<'a> {
    /// A pending filter's masks evaluated a split at a time as the stream
    /// reaches the split, each split's kept rows read after its mask.
    SplitReads(Vec<SplitRead>),
    /// Rows by id (a window's kept masks read by their ids), or the
    /// restricted scan.
    Exact(FileReadScope<'a>, ExactRead),
}

impl VortexRdfStore {
    /// The plan of a one-shot read of `columns` (primary column names) of
    /// this file view's `base` rows: the rows the masks a window kept keep;
    /// a pending filter the reads by id leave to the masks evaluated over
    /// every split first, then the rows its masks keep; else the
    /// [`ExactRead`].
    pub(in crate::store) async fn collected_plan<'a>(
        &'a self,
        base: &'a FileBase,
        columns: &'a [&'static str],
    ) -> Result<CollectedPlan<'a>> {
        if let Some(masks) = base.filter.kept_masks() {
            let masks = Arc::clone(masks);
            let masked = MaskedRows::of(&base.file, columns, masks, base.filter.pins())?;
            return Ok(CollectedPlan::Masked(masked));
        }
        let (scope, read) = self.file_read(base, columns).await?;
        if let (ExactRead::Masks, Some(filter)) = (&read, scope.filter) {
            let masks =
                file_filter::split_masks(scope.file, filter, &scope.selection, scope.deleted)
                    .await?;
            let pins = scope.filter_eqs();
            let masked = MaskedRows::of(scope.file, columns, Arc::new(masks), pins)?;
            return Ok(CollectedPlan::Masked(masked));
        }
        Ok(CollectedPlan::Exact(scope, read))
    }

    /// The plan of a stream of `columns` (primary column names) of this file
    /// view's `base` rows: a pending filter the reads by id leave to the
    /// masks read a split at a time, each split's mask evaluated when the
    /// stream reaches it; else the [`ExactRead`], which reads the ids the
    /// masks a window kept keep.
    pub(in crate::store) async fn streamed_plan<'a>(
        &'a self,
        base: &'a FileBase,
        columns: &'a [&'static str],
    ) -> Result<StreamedPlan<'a>> {
        let (scope, read) = self.file_read(base, columns).await?;
        if let (ExactRead::Masks, Some(filter)) = (&read, scope.filter) {
            let reads = file_reads::masks_first_split_reads(
                scope.file,
                filter,
                &scope.selection,
                scope.deleted,
                columns,
            )?;
            return Ok(StreamedPlan::SplitReads(reads));
        }
        Ok(StreamedPlan::Exact(scope, read))
    }

    /// What a read of this file view's `base` rows covers, and the
    /// [`ExactRead`] that reads them, chosen once: its exact selection, a
    /// served view's pending one materialized here (the plan orders the rows
    /// by the index, and the read needs base order); the columns a read by id
    /// reads, every primary column while a filter is pending (the read checks
    /// it on the columns it reads), else `columns`; and the filter still to
    /// run. A view whose window kept its filter's masks ([`FileFilter::Kept`])
    /// covers the ids the masks keep, with no filter to run.
    async fn file_read<'a>(
        &'a self,
        base: &'a FileBase,
        columns: &'a [&'static str],
    ) -> Result<(FileReadScope<'a>, ExactRead)> {
        let (selection, by_id_columns, filter) = match &base.filter {
            FileFilter::Kept { masks, .. } => {
                let ids = Buffer::from(masks.first_ids(usize::MAX));
                (Cow::Owned(RowSelection::from_ids(ids)), columns, None)
            }
            filter => {
                let selection = match base.serve {
                    // Runs the deferred index-child read once, cached on the view.
                    Some(_) => Cow::Owned(self.quads.selection.materialized_async().await?),
                    None => Cow::Borrowed(self.quads.selection.expect_exact()),
                };
                let filter = filter.to_evaluate();
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
            pins: base.filter.pins(),
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
}

/// A file view's base read, as [`VortexRdfStore::file_read`] scopes it: the
/// file, the columns the scan projects, the columns a read by id reads, the
/// filter still to run, the exact selection, the tombstones, and the codes
/// every selected row holds. The [`ExactRead`] chosen for it travels beside
/// it, so its id list moves into the read that takes it.
pub(in crate::store) struct FileReadScope<'a> {
    pub(in crate::store) file: &'a Arc<NativeStoreFile>,
    /// The primary columns the scan projects, in the order its chunks carry
    /// them.
    pub(in crate::store) columns: &'a [&'static str],
    /// The columns a read by id reads: every primary column while a filter
    /// is pending, which the read checks on the columns it reads.
    pub(in crate::store) by_id_columns: &'a [&'static str],
    pub(in crate::store) filter: Option<&'a PushedFilter>,
    pub(in crate::store) selection: Cow<'a, RowSelection>,
    pub(in crate::store) deleted: Option<&'a Mask>,
    /// The codes every selected row holds by a filter a window kept the
    /// masks of or resolved ([`FileFilter::pins`]): a read by id reads the
    /// columns they leave free and fills them, as
    /// [`CodeEqs::for_points`] has it.
    pub(in crate::store) pins: &'a CodeEqs,
}

impl FileReadScope<'_> {
    /// The base rows `read` reads by id: point by point through the file's
    /// cached chunk probes ([`file_reads::file_point_rows`]), or taken at the
    /// ids ([`file_reads::take_file_rows`]). [`ExactRead::Scan`], a read by
    /// id that declines, and [`ExactRead::Masks`] (which the plans read by
    /// the masks first) are the restricted scan ([`scan`](Self::scan)).
    pub(in crate::store) async fn rows(&self, read: ExactRead) -> Result<BaseRead> {
        let rows = match read {
            ExactRead::Points(rows) => {
                let pins = self.pins.for_points(rows.len());
                let columns = pins.columns_to_read(self.by_id_columns);
                file_reads::file_point_rows(self.file, &columns, self.filter, rows)
                    .await?
                    .map(|rows| pins.fill_rows(rows, self.by_id_columns))
                    .transpose()?
            }
            ExactRead::Take(ids) => {
                let columns = self.pins.columns_to_read(self.by_id_columns);
                file_reads::take_file_rows(self.file, &columns, &ids)
                    .await?
                    .map(|rows| self.pins.fill_rows(rows, self.by_id_columns))
                    .transpose()?
            }
            ExactRead::Scan | ExactRead::Masks => None,
        };
        Ok(match rows {
            Some(rows) => BaseRead::Rows(rows),
            None => BaseRead::Scan(Box::new(self.scan()?)),
        })
    }

    /// The code equalities of the pending filter, which every row its masks
    /// keep holds: what a read of those rows fills.
    fn filter_eqs(&self) -> &CodeEqs {
        self.filter
            .and_then(PushedFilter::eqs)
            .unwrap_or(CodeEqs::NONE)
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
            scan = scan.with_filter(memo.bind(file_exprs::QUAD_SCOPE, f.expr(), &scope)?);
        }
        Ok(self.selection.restrict_scan(scan, self.deleted))
    }
}

/// The `(s, p, o, g)` code columns of the rows `masked` keeps, `masked` a
/// read of the primary columns: point by point straight into the code
/// buffers, or the rows read whole ([`MaskedRows::collected`]), each column
/// the equalities pin filled with its code.
pub(in crate::store) async fn masked_codes(
    masked: &MaskedRows<'_>,
) -> Result<[Buffer<TermCode>; 4]> {
    if let Some(codes) = masked.point_codes().await? {
        return Ok(codes);
    }
    let read_columns = masked.read_columns();
    let codes = extract_codes(masked.collected().await?, &read_columns)?;
    masked
        .pins()
        .fill_codes(&read_columns, codes, masked.kept())
}

/// Test hook: how many restricted file scans this thread built with a filter
/// pushed down ([`FileReadScope::scan`]), each evaluating that filter split by
/// split as it is read.
#[cfg(test)]
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
