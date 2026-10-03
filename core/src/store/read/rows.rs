//! Rows and code columns: a view's gathered rows, its `u32` code columns,
//! and the raw-quad plumbing serialization and compaction share.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, field_as, subject_sorted, with_subject_stamp};
use crate::store::layouts::{ResolvedLayout, dictionary};
#[cfg(feature = "file-io")]
use crate::store::scan::file_reads;
use crate::store::scan::gather::gather_live;
use crate::store::schema;
use crate::store::view::selection::RowSelection;

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    /// Base live rows then tail rows as one array of primary columns; no
    /// index components ride along. A Dictionary view with a tail is
    /// re-encoded against a fresh dictionary, so its codes are not the
    /// store's: decode such a view through [`quads`](Self::quads), or
    /// serialize it through
    /// [`to_serializable_parts`](Self::to_serializable_parts), which returns
    /// the dictionary beside the rows. The string layouts chunk base and tail
    /// together in the base's own vocabulary.
    pub(crate) async fn selected_rows(&self) -> Result<ArrayRef> {
        let base = self.base_selected_rows().await?;
        let Some(tail) = &self.tail else {
            return Ok(base);
        };
        match &self.layout {
            ResolvedLayout::Dictionary(_) => {
                let (raws, _) = self.merged_raw_quads(&base).await?;
                if raws.is_empty() {
                    return dictionary::empty_struct();
                }
                let code_map = dictionary::code_map(dictionary::sorted_unique_terms(&raws));
                // Appended rows break the base's subject sort.
                dictionary::build_chunk(&raws, &code_map, false)
            }
            _ => {
                let tail_rows = tail.live_rows()?;
                let dtype = base.dtype().clone();
                chunked_or_single(vec![base, tail_rows], dtype)
            }
        }
    }

    /// The view's rows as four `u32` code columns `(s, p, o, g)`: off the
    /// serving index's own columns (in the index's order) when a plan covers
    /// them, else gathered from the base's canonical primitives in base row
    /// order. `None` when the view is not code-addressable
    /// ([`is_code_view`](Self::is_code_view)), is file-backed, or a base
    /// column is not reachable as a non-nullable `u32` primitive (chunked or
    /// wire-compressed); callers fall back to `selected_rows`.
    pub(crate) fn code_columns(&self) -> Option<[Buffer<u32>; 4]> {
        if !self.is_code_view() {
            return None;
        }
        #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
        let QuadsSource::InMemory { deleted, serve, .. } = &self.quads else {
            return None;
        };
        if let Some(plan) = serve
            && let Some(columns) = plan.code_columns(deleted.as_ref())
        {
            return Some(columns);
        }
        self.base_code_columns()
    }

    /// [`code_columns`](Self::code_columns) without the served path: the
    /// codes gathered from the base's own columns in base row order (a
    /// pending selection materializes). Contiguous, tombstone-free
    /// selections share the base's buffers zero-copy. `None` for a
    /// file-backed view or a column the shared cache cannot hand out as a
    /// `u32` primitive. Callers gate on [`is_code_view`](Self::is_code_view).
    pub(crate) fn base_code_columns(&self) -> Option<[Buffer<u32>; 4]> {
        use vortex_array::arrays::Struct;
        #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
        let QuadsSource::InMemory {
            base,
            selection,
            deleted,
            ..
        } = &self.quads
        else {
            return None;
        };
        let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
        let mut prims: Vec<PrimitiveArray> = Vec::with_capacity(4);
        for name in schema::PRIMARY_COLUMNS {
            let col = struct_arr.unmasked_field_by_name(name).ok()?;
            prims.push(crate::store::resident::shared_u32_primitive(col)?);
        }
        let selection = selection.materialized().ok()?;
        let column = |prim: &PrimitiveArray| -> Buffer<u32> {
            match (&selection, deleted) {
                (RowSelection::All, None) => prim.clone().into_buffer::<u32>(),
                (RowSelection::Range(r), None) => prim
                    .clone()
                    .into_buffer::<u32>()
                    .slice(r.start as usize..r.end as usize),
                (RowSelection::Ids(ids), None) => {
                    let slice = prim.as_slice::<u32>();
                    Buffer::from_iter(ids.iter().map(|&i| slice[i as usize]))
                }
                (selection, Some(deleted)) => {
                    let slice = prim.as_slice::<u32>();
                    let live = |i: usize| !deleted.value(i);
                    match selection {
                        RowSelection::All => Buffer::from_iter(
                            (0..base.len()).filter(|&i| live(i)).map(|i| slice[i]),
                        ),
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
                }
            }
        };
        Some([
            column(&prims[0]),
            column(&prims[1]),
            column(&prims[2]),
            column(&prims[3]),
        ])
    }

    /// The view's rows as four `u32` code columns `(s, p, o, g)`, gathered
    /// through `selected_rows` when the zero-copy path declines. A served
    /// view's codes come off the index copy in the index's order; every other
    /// view's in base row order, the order [`code_chunks`](Self::code_chunks)
    /// promises. `None` for a view that is not code-addressable: a string
    /// layout, or a Dictionary view with a non-empty append tail.
    pub async fn code_columns_gathered(&self) -> Result<Option<[Buffer<u32>; 4]>> {
        if !self.is_code_view() {
            return Ok(None);
        }
        if let Some(columns) = self.code_columns() {
            return Ok(Some(columns));
        }
        let rows = self.selected_rows().await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let column = |name: &str, ctx: &mut vortex_array::ExecutionCtx| -> Result<Buffer<u32>> {
            Ok(field_as::<PrimitiveArray>(&struct_arr, name, ctx)?.into_buffer::<u32>())
        };
        Ok(Some([
            column(schema::COL_S, &mut ctx)?,
            column(schema::COL_P, &mut ctx)?,
            column(schema::COL_O, &mut ctx)?,
            column(schema::COL_G, &mut ctx)?,
        ]))
    }

    /// The base rows this view covers, without the tail: gathered in memory,
    /// or read from the file with the pending filter and selection applied (a
    /// point-sized selection through the chunk probes, anything wider by
    /// scan). A pending selection materializes. The result carries the
    /// base's subject-sorted stamp, which the take/filter kernels drop.
    pub(in crate::store) async fn base_selected_rows(&self) -> Result<ArrayRef> {
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                ..
            } => {
                let selection = selection.materialized()?;
                match (&selection, deleted) {
                    (RowSelection::All, None) => Ok(base.clone()),
                    _ => {
                        let rows = gather_live(base, &selection, deleted.as_ref(), Some(probes))?;
                        with_subject_stamp(rows, subject_sorted(base))
                    }
                }
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                let columns = self.layout.strategy().primary_column_names();
                let scan = file_reads::restricted_scan(
                    file,
                    columns,
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )?;
                let arr = file_reads::point_rows_or_scan(
                    file_reads::file_point_rows(
                        file,
                        columns,
                        filter.as_ref(),
                        &selection,
                        deleted.as_ref(),
                    ),
                    scan,
                )
                .await?;
                with_subject_stamp(arr, file.quads_sorted())
            }
        }
    }

    /// `base` decoded to raw quads followed by the tail's live rows, and how
    /// many of the result came from the base.
    pub(in crate::store) async fn merged_raw_quads(
        &self,
        base: &ArrayRef,
    ) -> Result<(Vec<RawQuad>, usize)> {
        let mut raws = self.layout.raw_quads_async(base).await?;
        let base_rows = raws.len();
        if let Some(tail) = &self.tail {
            raws.extend(tail.raw_quads()?);
        }
        Ok((raws, base_rows))
    }

    /// Every live quad this view covers as raw N-Triples term strings: base
    /// rows first, in view order, then tail rows.
    pub(in crate::store) async fn live_raw_quads(&self) -> Result<Vec<RawQuad>> {
        let base = self.base_selected_rows().await?;
        Ok(self.merged_raw_quads(&base).await?.0)
    }
}
