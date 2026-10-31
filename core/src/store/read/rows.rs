//! The row/code read surface: the one base-row seam every row reader goes
//! through ([`BaseRead`]), code columns, and the selected-rows plumbing the
//! serialization and compaction paths share.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::RawQuad;
use crate::store::array::{rechunk, with_subject_stamp};
use crate::store::read::chunks::{RowChunkStream, extract_codes};
#[cfg(feature = "file-io")]
use crate::store::read::file_plan::{CollectedPlan, masked_codes};
use crate::store::scan::gather::gather_live;
#[cfg(feature = "file-io")]
use crate::store::scan::read_plan::ExactRead;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_reads, file_scans};
use crate::store::schema::{self, QuadColumn, RowId, TermCode};
use crate::store::view::selection::SelectedRows;
#[cfg(feature = "file-io")]
use crate::store::view::selection::ViewSelection;
use crate::store::{Backend, MemoryBase};

use futures::stream::{self, StreamExt};
use vortex_array::ArrayRef;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;

#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;

use crate::observe::observe;
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
/// order of `ids`: one gather per column. Called column by column rather
/// than mapped over the columns, so that each gather stays a loop of its
/// own: mapped, it can be inlined into the array map's iteration, at 9 %
/// more instructions.
pub(in crate::store) fn gather_codes(
    prims: &[PrimitiveArray; 4],
    ids: &[RowId],
) -> [Buffer<TermCode>; 4] {
    observe!(CodeGather { all_four: true });
    let [s, p, o, g] = prims;
    [
        gather(s, ids),
        gather(p, ids),
        gather(o, ids),
        gather(g, ids),
    ]
}

pub(in crate::store) fn gather_column(prim: &PrimitiveArray, ids: &[RowId]) -> Buffer<TermCode> {
    observe!(CodeGather { all_four: false });
    gather(prim, ids)
}

/// The codes of `prim` at `ids`, in the order of `ids`.
fn gather(prim: &PrimitiveArray, ids: &[RowId]) -> Buffer<TermCode> {
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
        if let Some(plan) = &self.quads.in_memory()?.serve {
            let deleted = self.quads.deleted.as_ref();
            // A window of a served view: the base's code columns gathered at
            // its row ids, in the index's order.
            if plan.is_narrowed()
                && let Some(prims) = self.base_code_primitives()
                && let Ok(ids) = plan.live_rids(deleted)
            {
                return Some(gather_codes(&prims, &ids));
            }
            // The answering index's columns hold this view's codes as one
            // contiguous run: no deferred row ids to compute, no scattered
            // gather over the primaries.
            if let Some(columns) = plan.code_columns(deleted) {
                return Some(columns);
            }
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
        let rows = selection.as_rows(self.quads.base_len());
        let Some(deleted) = &self.quads.deleted else {
            return Some(match rows {
                SelectedRows::Run(run) => {
                    prims.map(|prim| prim.into_buffer::<TermCode>().slice(run.clone()))
                }
                SelectedRows::Ids(ids) => gather_codes(&prims, ids),
            });
        };
        let live = |i: usize| !deleted.value(i);
        let column = |prim: &PrimitiveArray| -> Buffer<TermCode> {
            let slice = prim.as_slice::<TermCode>();
            match &rows {
                SelectedRows::Run(run) => {
                    Buffer::from_iter(run.clone().filter(|&i| live(i)).map(|i| slice[i]))
                }
                SelectedRows::Ids(ids) => Buffer::from_iter(
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
        let [s, p, o, g] = QuadColumn::ALL;
        Some([
            self.base_code_primitive(s)?,
            self.base_code_primitive(p)?,
            self.base_code_primitive(o)?,
            self.base_code_primitive(g)?,
        ])
    }

    /// The base's `column` code column as a canonical primitive, as
    /// [`base_code_primitives`](Self::base_code_primitives) reads each of
    /// the four: a builder-compressed column is decoded into its own shared
    /// cache, the other columns' caches left as they are.
    pub(in crate::store) fn base_code_primitive(
        &self,
        column: QuadColumn,
    ) -> Option<PrimitiveArray> {
        use vortex_array::arrays::Struct;
        let base = &self.quads.in_memory()?.base;
        let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
        crate::store::resident::shared_code_primitive(
            struct_arr.unmasked_field_by_name(column.name()).ok()?,
        )
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
        if let ViewSelection::Pending { .. } = &self.quads.selection
            && let Some(FileBase {
                serve: Some(plan), ..
            }) = self.quads.file()
            && let Some(columns) = plan.code_columns(self.quads.deleted.as_ref()).await?
        {
            return Ok(Some(columns));
        }
        // On file the read is a one-shot read's plan (`collected_plan`): a
        // filter's kept rows read straight into the code buffers, point by
        // point or read whole; a point-sized read by id fills the code
        // buffers straight from the chunk probes, with the same rows and
        // filter checks; anything else the rows `base_read` reads.
        #[cfg(feature = "file-io")]
        if let Backend::File(base) = &self.quads.backend {
            let (scope, read) = match self.collected_plan(base, &schema::PRIMARY_COLUMNS).await? {
                CollectedPlan::Masked(masked) => return masked_codes(&masked).await.map(Some),
                CollectedPlan::Exact(scope, read) => (scope, read),
            };
            let rows = match read {
                ExactRead::Points(rows) => {
                    let pins = scope.pins.for_points(rows.len());
                    let read_columns = pins.columns_to_read(scope.by_id_columns);
                    if let Some(codes) =
                        file_reads::file_point_codes(scope.file, &read_columns, scope.filter, rows)
                            .await?
                    {
                        // The read columns in `(s, p, o, g)` order, the pinned
                        // ones filled: every primary column is read or pinned.
                        let kept = codes.first().map_or(0, Buffer::len);
                        return pins.fill_codes(&read_columns, codes, kept).map(Some);
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
    /// as [`BaseRead`] carries them, read whole: at least the `columns`
    /// (primary column names), each found by name. The whole-base reads
    /// (compaction, serialization) read the base through here; the served
    /// fast paths read the answering index instead.
    ///
    /// The read is over the exact selection: a served view's pending
    /// selection materializes here, since its plan orders the rows by the
    /// index; a pending one without a plan sits only on a view built to be
    /// counted or windowed, which is never read (`expect_exact`). In memory
    /// the live selected rows are gathered, every primary column
    /// ([`in_memory_base_rows`](Self::in_memory_base_rows)). On file it is a
    /// one-shot read's plan ([`collected_plan`](Self::collected_plan)): a
    /// filter's kept rows, a window's masks or a pending filter's evaluated
    /// first; rows by id, through every primary column while a filter is
    /// pending (the read checks it on the columns it reads); anything else,
    /// or a read by id that declines, the restricted scan of `columns`
    /// ([`FileReadScope::rows`](crate::store::read::file_plan::FileReadScope::rows)).
    #[cfg_attr(not(feature = "file-io"), allow(unused_variables))]
    pub(in crate::store) async fn base_read(&self, columns: &[&'static str]) -> Result<BaseRead> {
        match &self.quads.backend {
            Backend::InMemory(memory) => Ok(BaseRead::Rows(self.in_memory_base_rows(memory)?)),
            #[cfg(feature = "file-io")]
            Backend::File(base) => match self.collected_plan(base, columns).await? {
                CollectedPlan::Masked(masked) => Ok(BaseRead::Rows(masked.rows().await?)),
                CollectedPlan::Exact(scope, read) => scope.rows(read).await,
            },
        }
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

    /// Every live quad this view covers, decoded to raw N-Triples term strings
    /// (a file-backed dictionary lifted resident for the decode), base rows
    /// first in base row order, then the tail's live rows, and how many of
    /// them came from the base.
    pub(in crate::store) async fn live_raw_quads(&self) -> Result<(Vec<RawQuad>, usize)> {
        observe!(RawQuadsGathered);
        let base = self.base_selected_rows().await?;
        let mut raws = self.layout.raw_quads_async(&base).await?;
        let base_rows = raws.len();
        if let Some(tail) = &self.tail {
            raws.extend(tail.raw_quads()?);
        }
        Ok((raws, base_rows))
    }
}

/// The `(s, p, o, g)` code columns of a read of the primary columns.
fn primary_codes(rows: ArrayRef) -> Result<[Buffer<TermCode>; 4]> {
    Ok(extract_codes(rows, &schema::PRIMARY_COLUMNS)?
        .try_into()
        .expect("one code buffer per primary column"))
}
