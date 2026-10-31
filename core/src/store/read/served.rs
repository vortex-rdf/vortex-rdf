//! A served view's chunk streams, in its index's order: how they read the
//! view's rows, decided once per stream ([`ServedRead`]), and the code and
//! row chunks each way yields.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use vortex_buffer::Buffer;
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

use super::chunks::{
    CodeChunkStream, RowChunkStream, batch_ranges, gathered_code_chunks, id_windows, projected,
};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::store::indexes::{ComponentRows, InMemoryServePlan};
#[cfg(feature = "file-io")]
use crate::store::indexes::{FileServePlan, PlanRead};
use crate::store::resident::CodeAccess;
use crate::store::scan::gather::gather_rows_at;
use crate::store::schema::{QuadColumn, RowId};
#[cfg(feature = "file-io")]
use crate::store::{
    LayoutStrategy, array::rechunk, persist::native_file::NativeStoreFile,
    scan::file_reads::take_file_rows, scan::file_scans::stream_rows, schema::TermCode,
};
use crate::store::{MemoryBase, VortexRdfStore};

/// How a served view's chunk streams read its rows in its index's order,
/// decided once per stream ([`VortexRdfStore::served_read`]).
// Built once per stream and matched at once: boxing the file read's scan
// would cost every served file stream an allocation for nothing.
#[allow(clippy::large_enum_variant)]
pub(super) enum ServedRead<'a> {
    /// In memory, the base gathered at the view's live row ids in the
    /// index's order (the ids a narrowing of the view holds, else the
    /// plan's): a base whose code columns decode once
    /// ([`CodeAccess::Resident`]), or a string layout's.
    Gather {
        memory: &'a MemoryBase,
        ids: Buffer<RowId>,
    },
    /// In memory, the serving component's own columns, a window of the
    /// view's live rows at a time: a Dictionary base whose code columns are
    /// encoded ([`CodeAccess::Encoded`]), which a gather at the view's
    /// scattered ids would decode again per chunk.
    Component {
        plan: &'a InMemoryServePlan,
        rows: ComponentRows,
    },
    /// On file, the plan's rows as it reads them ([`PlanRead`]), beside the
    /// tombstones each chunk's decode still drops.
    #[cfg(feature = "file-io")]
    File {
        file: &'a Arc<NativeStoreFile>,
        plan: &'a FileServePlan,
        read: PlanRead,
        deleted: Option<Mask>,
    },
    /// On file, a TypedObject view's rows: its copies hold string objects,
    /// not the layout's typed ones, so each batch of the view's ids in the
    /// index's order ([`FileServePlan::live_rows`]) is taken from the base.
    #[cfg(feature = "file-io")]
    FileBase {
        file: &'a Arc<NativeStoreFile>,
        plan: &'a FileServePlan,
        deleted: Option<Mask>,
    },
}

impl VortexRdfStore {
    /// How this view's chunk streams read it in its index's order: the one
    /// place a stream asks whether and how a plan serves the view. `None`
    /// for a view no plan serves (or while a
    /// [`LegacyChunkPaths`](crate::store::test_hooks::LegacyChunkPaths)
    /// guard sends every stream down the base-order paths).
    pub(super) fn served_read(&self) -> Result<Option<ServedRead<'_>>> {
        #[cfg(test)]
        if crate::store::test_hooks::legacy_chunk_paths() {
            return Ok(None);
        }
        let deleted = self.quads.deleted.as_ref();
        #[cfg(feature = "file-io")]
        if let Some(base) = self.quads.file() {
            let Some(plan) = &base.serve else {
                return Ok(None);
            };
            observe!(ServedRunChunks);
            if self.layout.strategy() == LayoutStrategy::TypedObject {
                return Ok(Some(ServedRead::FileBase {
                    file: &base.file,
                    plan,
                    deleted: deleted.cloned(),
                }));
            }
            let read = plan.read()?;
            let deleted = read.tombstones(deleted).cloned();
            return Ok(Some(ServedRead::File {
                file: &base.file,
                plan,
                read,
                deleted,
            }));
        }
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        observe!(ServedRunChunks);
        if plan.decoder().decodes_codes() && memory.code_access() == CodeAccess::Encoded {
            let rows = plan.live_component_rows(deleted)?;
            return Ok(Some(ServedRead::Component { plan, rows }));
        }
        let ids = match self.quads.selection.held_index_order() {
            Some(ids) => ids.clone(),
            None => plan.live_rids(deleted)?,
        };
        Ok(Some(ServedRead::Gather { memory, ids }))
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
        let picked: Vec<usize> = columns.iter().map(|c| c.index()).collect();
        Ok(match self {
            ServedRead::Gather { ids, .. } => {
                // A base whose code columns decode once hands them out.
                let (all, asked) = store.base_code_sources(columns).ok_or_else(|| {
                    VortexRdfError::Vortex(vortex_error::vortex_err!(
                        "the base's code columns failed to decode"
                    ))
                })?;
                gathered_code_chunks(all, asked, columns, id_windows(ids, batch_rows))
            }
            ServedRead::Component { plan, rows } => {
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
                read,
                deleted,
            } => {
                let (plan, file) = (plan.clone(), Arc::clone(file));
                let codes = match read {
                    // Kept rows, or a point-sized run: each batch's codes
                    // point by point, straight into code buffers.
                    PlanRead::Kept(positions) | PlanRead::Points(positions) => {
                        stream::iter(batch_ranges(positions.len(), batch_rows))
                            .then(move |batch| {
                                let (plan, file, deleted) =
                                    (plan.clone(), Arc::clone(&file), deleted.clone());
                                let batch = positions.slice(batch);
                                async move {
                                    match plan.point_codes(&file, &batch, deleted.as_ref()).await? {
                                        Some(codes) => Ok(codes),
                                        None => plan.decoder().chunk_code_columns(
                                            &plan.rows_at(&file, &batch).await?,
                                            deleted.as_ref(),
                                        ),
                                    }
                                }
                            })
                            .boxed()
                    }
                    PlanRead::Scan(scan) => stream_rows(scan)?
                        .map(move |chunk| {
                            chunk.and_then(|chunk| {
                                plan.decoder().chunk_code_columns(&chunk, deleted.as_ref())
                            })
                        })
                        .boxed(),
                };
                codes
                    .flat_map(move |codes| {
                        stream::iter(match codes {
                            Ok(codes) => code_slices(codes, &picked, batch_rows),
                            Err(e) => vec![Err(e)],
                        })
                    })
                    .boxed()
            }
            #[cfg(feature = "file-io")]
            ServedRead::FileBase { .. } => {
                return Err(VortexRdfError::InvalidOperation(
                    "a TypedObject store's rows hold terms, not codes".into(),
                ));
            }
        })
    }

    /// The view's base rows of at least `columns` (primary column names),
    /// `batch_rows` rows each, in its index's order: in memory the base's
    /// rows gathered at each window of its ids, or each window of its live
    /// rows read off the component; on file each of the plan's chunks as the
    /// primary columns, tombstoned rows dropped, or for TypedObject each
    /// batch of its ids taken from the base.
    pub(super) fn row_chunks(
        self,
        columns: &[&'static str],
        batch_rows: usize,
    ) -> Result<RowChunkStream> {
        Ok(match self {
            ServedRead::Gather { memory, ids } => {
                let base = projected(&memory.base, columns)?;
                let probes = Arc::clone(&memory.probes);
                stream::iter(id_windows(ids, batch_rows).map(move |ids| {
                    observe!(Gather {
                        width: base.dtype().as_struct_fields_opt().map(|f| f.nfields())
                    });
                    gather_rows_at(&base, &ids, Some(&probes))
                }))
                .boxed()
            }
            ServedRead::Component { plan, rows } => {
                let plan = plan.clone();
                stream::iter(batch_ranges(rows.len(), batch_rows))
                    .map(move |window| plan.window_rows(&rows, window))
                    .boxed()
            }
            #[cfg(feature = "file-io")]
            ServedRead::File {
                file,
                plan,
                read,
                deleted,
            } => {
                let chunks = plan.read_chunks(read, Arc::clone(file), batch_rows)?;
                let plan = plan.clone();
                chunks
                    .flat_map(move |chunk| {
                        let rows = chunk
                            .and_then(|chunk| plan.decoder().chunk_rows(&chunk, deleted.as_ref()));
                        stream::iter(match rows {
                            Ok(rows) => rechunk(rows, batch_rows).collect(),
                            Err(e) => vec![Err(e)],
                        })
                    })
                    .boxed()
            }
            #[cfg(feature = "file-io")]
            ServedRead::FileBase {
                file,
                plan,
                deleted,
            } => typed_rows(plan.clone(), Arc::clone(file), deleted, columns, batch_rows),
        })
    }
}

/// A served TypedObject file view's base rows in its index's order, at least
/// `columns`: each batch of `batch_rows` of the view's ids in the index's
/// order ([`FileServePlan::live_rows`], the `deleted` tombstones left out)
/// taken from the base ([`file_rows_in_order`]), one batch a chunk.
#[cfg(feature = "file-io")]
fn typed_rows(
    plan: FileServePlan,
    file: Arc<NativeStoreFile>,
    deleted: Option<Mask>,
    columns: &[&'static str],
    batch_rows: usize,
) -> RowChunkStream {
    let columns: Arc<[&'static str]> = Arc::from(columns);
    stream::once(async move {
        let rows = plan.live_rows(&file, deleted.as_ref()).await?;
        Ok::<_, VortexRdfError>((rows.rids, file))
    })
    .flat_map(move |read| match read {
        Ok((ids, file)) => {
            let columns = Arc::clone(&columns);
            stream::iter(id_windows(ids, batch_rows))
                .then(move |window| {
                    let (file, columns) = (Arc::clone(&file), Arc::clone(&columns));
                    async move { file_rows_in_order(&file, &columns, &window).await }
                })
                .left_stream()
        }
        Err(e) => stream::iter([Err(e)]).right_stream(),
    })
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
/// rows, sharing their buffers: a served file chunk's code chunks.
#[cfg(feature = "file-io")]
fn code_slices(
    codes: [Buffer<TermCode>; 4],
    picked: &[usize],
    batch_rows: usize,
) -> Vec<Result<Vec<Buffer<TermCode>>>> {
    batch_ranges(codes[0].len(), batch_rows)
        .map(|rows| {
            Ok(picked
                .iter()
                .map(|&k| codes[k].slice(rows.clone()))
                .collect())
        })
        .collect()
}
