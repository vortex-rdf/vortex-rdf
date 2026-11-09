//! How a served view's rows are read in its index's order, decided once per
//! read: an in-memory view's for its chunk streams and its one-shot code read
//! alike ([`MemoryRead`]), a file view's for its chunk streams beside it
//! ([`ServedRead`]); and the code and row chunks each way yields.

use std::sync::Arc;

#[cfg(feature = "file-io")]
use futures::stream::BoxStream;
use futures::stream::{self, StreamExt};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

#[cfg(feature = "file-io")]
use super::chunks::cut_each;
use super::chunks::{
    CodeChunkStream, RowChunkStream, gathered_code_chunks, gathered_row_chunks, id_windows,
};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::store::array::batch_ranges;
use crate::store::indexes::InMemoryServePlan;
#[cfg(feature = "file-io")]
use crate::store::indexes::{FileServePlan, PlanRead};
use crate::store::resident::CodeAccess;
use crate::store::schema::{QuadColumn, RowId};
#[cfg(feature = "file-io")]
use crate::store::{
    LayoutStrategy, array::rechunk, persist::native_file::NativeStoreFile,
    scan::file_reads::take_file_rows, scan::file_scans::stream_rows, schema::TermCode,
};
use crate::store::{MemoryBase, VortexRdfStore};

/// What a read of an in-memory served view takes at once, which decides
/// where it takes the rows from ([`VortexRdfStore::memory_read`]).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadShape {
    /// A chunk at a time: the row and code chunk streams.
    Chunks,
    /// The view's codes in one piece: [`code_columns`](VortexRdfStore::code_columns).
    Whole,
}

/// Where a read takes an in-memory served view's rows from, in its index's
/// order, decided once per read ([`VortexRdfStore::memory_read`]) for chunk
/// streams and one-shot code reads alike.
pub(super) enum MemoryRead<'a> {
    /// The base gathered at the view's live row ids in the index's order
    /// (the ids a narrowing of the view holds, else the plan's): a base whose
    /// code columns decode once ([`CodeAccess::Resident`]), or a string
    /// layout's.
    Gather {
        memory: &'a MemoryBase,
        ids: Buffer<RowId>,
    },
    /// The serving component's own columns at the view's live rows (the
    /// `deleted` tombstones left out): a Dictionary base whose code columns
    /// are encoded ([`CodeAccess::Encoded`]), which a gather at the view's
    /// scattered ids would decode again, or a whole read of a view that
    /// holds no ids, which reads the plan's run once rather than computing
    /// them.
    Component {
        plan: &'a InMemoryServePlan,
        deleted: Option<&'a Mask>,
    },
}

/// How a chunk stream takes a served view's rows in its index's order,
/// decided once per stream ([`VortexRdfStore::served_read`]).
pub(super) enum ServedRead<'a> {
    /// In memory, as a chunk read takes the rows ([`MemoryRead`]).
    Memory(MemoryRead<'a>),
    /// On file, the plan's rows as it reads them ([`FileServePlan::read`]),
    /// the view's `deleted` tombstones dropped.
    #[cfg(feature = "file-io")]
    File {
        file: &'a Arc<NativeStoreFile>,
        plan: &'a FileServePlan,
        deleted: Option<&'a Mask>,
    },
}

impl VortexRdfStore {
    /// How this view's chunk streams read it in its index's order: the one
    /// place a stream asks whether and how a plan serves the view. On file
    /// the plan reads its own rows ([`FileServePlan::read`]); in memory, as
    /// [`memory_read`](Self::memory_read) decides for a chunk read. `None`
    /// for a view no plan serves.
    pub(super) fn served_read(&self) -> Result<Option<ServedRead<'_>>> {
        #[cfg(feature = "file-io")]
        if let Some(base) = self.quads.file() {
            return Ok(base.serve.as_ref().map(|plan| ServedRead::File {
                file: &base.file,
                plan,
                deleted: self.quads.deleted.as_ref(),
            }));
        }
        Ok(self.memory_read(ReadShape::Chunks)?.map(ServedRead::Memory))
    }

    /// Where a read of `shape` takes this in-memory served view's rows from,
    /// in its index's order: the one place an in-memory read, a chunk stream
    /// or the one-shot code read, asks whether and how a plan serves the
    /// view. A Dictionary view reads the serving component's columns when
    /// its base's code columns are encoded, and in a whole read of a view
    /// that holds no ids (the plan's run read once, its ids never computed);
    /// any other read gathers the base at the view's ids, those a narrowing
    /// holds, else the plan's. `None` for a view no in-memory plan serves.
    #[inline]
    pub(super) fn memory_read(&self, shape: ReadShape) -> Result<Option<MemoryRead<'_>>> {
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        let deleted = self.quads.deleted.as_ref();
        let held = self.quads.selection.held_index_order();
        if plan.decoder().decodes_codes()
            && (memory.code_access == CodeAccess::Encoded
                || (shape == ReadShape::Whole && held.is_none()))
        {
            return Ok(Some(MemoryRead::Component { plan, deleted }));
        }
        let ids = match held {
            Some(ids) => ids.clone(),
            None => plan.live_rids(deleted)?,
        };
        Ok(Some(MemoryRead::Gather { memory, ids }))
    }
}

impl ServedRead<'_> {
    /// The view's code chunks of `columns`, `batch_rows` rows each, in its
    /// index's order: in memory the base's code columns gathered at each
    /// window of its ids, or each window of its live rows read off the
    /// component; on file each batch of points read straight into code
    /// buffers, or each scanned chunk's codes, tombstoned rows dropped.
    pub(super) fn code_chunks(
        self,
        store: &VortexRdfStore,
        columns: &[QuadColumn],
        batch_rows: usize,
    ) -> Result<CodeChunkStream> {
        observe!(ServedRunChunks);
        let picked: Arc<[usize]> = columns.iter().map(|c| c.index()).collect();
        Ok(match self {
            ServedRead::Memory(MemoryRead::Gather { ids, .. }) => {
                // A base whose code columns decode once hands them out.
                let (all, asked) = store.base_code_sources(columns).ok_or_else(|| {
                    VortexRdfError::Vortex(vortex_error::vortex_err!(
                        "the base's code columns failed to decode"
                    ))
                })?;
                gathered_code_chunks(all, asked, columns, id_windows(ids, batch_rows))
            }
            ServedRead::Memory(MemoryRead::Component { plan, deleted }) => {
                let rows = plan.live(deleted)?;
                let plan = plan.clone();
                stream::iter(batch_ranges(rows.len(), batch_rows))
                    .map(move |window| {
                        plan.window_code_columns(&rows, window)
                            .map(|codes| picked.iter().map(|&k| codes[k].clone()).collect())
                    })
                    .boxed()
            }
            #[cfg(feature = "file-io")]
            ServedRead::File {
                file,
                plan,
                deleted,
            } => {
                let (read, deleted) = plan.read(deleted)?;
                let (plan, file, deleted) = (plan.clone(), Arc::clone(file), deleted.cloned());
                let codes = match read {
                    // Kept rows, or a point-sized run: each batch's codes
                    // point by point, straight into code buffers.
                    PlanRead::Points(positions) => {
                        point_batches(positions, batch_rows, move |batch| {
                            let (plan, file, deleted) =
                                (plan.clone(), Arc::clone(&file), deleted.clone());
                            async move { plan.codes_at(&file, &batch, deleted.as_ref()).await }
                        })
                    }
                    PlanRead::Scan(scan) => stream_rows(scan)?
                        .map(move |chunk| {
                            chunk.and_then(|chunk| {
                                plan.decoder().chunk_code_columns(&chunk, deleted.as_ref())
                            })
                        })
                        .boxed(),
                };
                cut_each(codes, move |codes| {
                    code_slices(codes, Arc::clone(&picked), batch_rows)
                })
            }
        })
    }

    /// The view's base rows of at least `columns` (primary column names),
    /// `batch_rows` rows each, in its index's order: in memory the base's
    /// rows gathered at each window of its ids, or each window of its live
    /// rows read off the component; on file each of the plan's chunks as the
    /// primary columns, tombstoned rows dropped, or for a TypedObject
    /// `store` each batch of its ids taken from the base.
    pub(super) async fn row_chunks(
        self,
        #[cfg_attr(not(feature = "file-io"), allow(unused_variables))] store: &VortexRdfStore,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<RowChunkStream> {
        observe!(ServedRunChunks);
        Ok(match self {
            ServedRead::Memory(MemoryRead::Gather { memory, ids }) => {
                gathered_row_chunks(memory, columns, id_windows(ids, batch_rows))?
            }
            ServedRead::Memory(MemoryRead::Component { plan, deleted }) => {
                let rows = plan.live(deleted)?;
                let plan = plan.clone();
                stream::iter(batch_ranges(rows.len(), batch_rows))
                    .map(move |window| plan.window_rows(&rows, window))
                    .boxed()
            }
            #[cfg(feature = "file-io")]
            ServedRead::File {
                file,
                plan,
                deleted,
            } => {
                // A TypedObject view's copies hold string objects, not the
                // layout's typed ones.
                if store.layout.strategy() == LayoutStrategy::TypedObject {
                    return typed_rows(plan, file, deleted, columns, batch_rows).await;
                }
                let (read, deleted) = plan.read(deleted)?;
                let (plan, file, deleted) = (plan.clone(), Arc::clone(file), deleted.cloned());
                // Kept rows, or a point-sized run: each batch's rows point by
                // point; else the plan's scan, split by split.
                let chunks = match read {
                    PlanRead::Points(positions) => {
                        let plan = plan.clone();
                        point_batches(positions, batch_rows, move |batch| {
                            let (plan, file) = (plan.clone(), Arc::clone(&file));
                            async move { plan.rows_at(&file, &batch).await }
                        })
                    }
                    PlanRead::Scan(scan) => stream_rows(scan)?,
                };
                let rows = chunks.map(move |chunk| {
                    chunk.and_then(|chunk| plan.decoder().chunk_rows(&chunk, deleted.as_ref()))
                });
                cut_each(rows, move |rows| rechunk(rows, batch_rows))
            }
        })
    }
}

/// A served TypedObject file view's base rows in its index's order, at least
/// `columns`: each batch of `batch_rows` of the view's ids in the index's
/// order ([`FileServePlan::live_rows`], the `deleted` tombstones left out)
/// taken from the base ([`file_rows_in_order`]), one batch a chunk.
#[cfg(feature = "file-io")]
async fn typed_rows(
    plan: &FileServePlan,
    file: &Arc<NativeStoreFile>,
    deleted: Option<&Mask>,
    columns: &[&'static str],
    batch_rows: usize,
) -> Result<RowChunkStream> {
    let ids = plan.live_rows(file, deleted).await?.rids;
    let (file, columns): (_, Arc<[&'static str]>) = (Arc::clone(file), Arc::from(columns));
    Ok(stream::iter(id_windows(ids, batch_rows))
        .then(move |window| {
            let (file, columns) = (Arc::clone(&file), Arc::clone(&columns));
            async move { file_rows_in_order(&file, &columns, &window).await }
        })
        .boxed())
}

/// The plan's rows at `positions`, `batch_rows` of them a batch, each batch
/// read by `read` when it is pulled: a served file stream's point reads.
#[cfg(feature = "file-io")]
fn point_batches<T, Fut>(
    positions: Buffer<u64>,
    batch_rows: usize,
    read: impl Fn(Buffer<u64>) -> Fut + Send + 'static,
) -> BoxStream<'static, Result<T>>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    stream::iter(batch_ranges(positions.len(), batch_rows))
        .then(move |batch| read(positions.slice(batch)))
        .boxed()
}

/// The base's `columns` at the base row ids `ids` (any order, each once, not
/// empty), in the order of `ids`: the ids sorted beside their slots, taken
/// from the file ([`take_file_rows`]), the rows put back in the ids' order.
#[cfg(feature = "file-io")]
async fn file_rows_in_order(
    file: &NativeStoreFile,
    columns: &[&'static str],
    ids: &[RowId],
) -> Result<vortex_array::ArrayRef> {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::validity::Validity;

    let mut slots: Vec<usize> = (0..ids.len()).collect();
    slots.sort_unstable_by_key(|&slot| ids[slot]);
    let sorted: Vec<RowId> = slots.iter().map(|&slot| ids[slot]).collect();
    let rows = take_file_rows(file, columns, &sorted)
        .await?
        .ok_or_else(|| {
            VortexRdfError::Vortex(vortex_error::vortex_err!(
                "the take of {} served ids declined",
                ids.len()
            ))
        })?;
    let mut back = vec![0u64; ids.len()];
    for (taken, &slot) in slots.iter().enumerate() {
        back[slot] = taken as u64;
    }
    rows.take(PrimitiveArray::new(Buffer::from(back), Validity::NonNullable).into_array())
        .map_err(VortexRdfError::Vortex)
}

/// The `picked` columns of `codes` cut into slices of at most `batch_rows`
/// rows, sharing their buffers, each cut when it is reached: a served file
/// chunk's code chunks.
#[cfg(feature = "file-io")]
fn code_slices(
    codes: [Buffer<TermCode>; 4],
    picked: Arc<[usize]>,
    batch_rows: usize,
) -> impl Iterator<Item = Result<Vec<Buffer<TermCode>>>> + Send + 'static {
    batch_ranges(codes[0].len(), batch_rows).map(move |rows| {
        Ok(picked
            .iter()
            .map(|&k| codes[k].slice(rows.clone()))
            .collect())
    })
}
