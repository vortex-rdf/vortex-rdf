//! The row/code read surface: sizes, gathered rows, code columns, and the
//! selected-rows plumbing the serialization and compaction paths share.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::RawQuad;
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{LayoutStrategy, ResolvedLayout, dictionary};
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::schema;
use crate::store::selection::{RowSelection, ViewSelection, gather_live};

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{ChunkedArray, PrimitiveArray, StructArray};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;

#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, root, select};
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

use super::VortexRdfStore;

impl VortexRdfStore {
    /// Number of quads in the store.
    ///
    /// For a file-backed store with a pending `match_pattern` filter, this
    /// counts matching rows from the filter masks alone — only the columns the
    /// filter references are read, and no rows are projected or decoded.
    /// `file.row_count()` alone would report the unfiltered total.
    pub async fn size(&self) -> Result<usize> {
        let base = match &self.quads {
            // In-memory patterns resolve to exact row ids at match time —
            // or, for a served match, to a pending run whose width is known
            // without decoding it — so the selection alone knows the answer
            // and no rows are touched. Deletions are only counted out, never
            // gathered.
            QuadsSource::InMemory {
                base,
                selection,
                deleted: None,
                ..
            } => match selection {
                ViewSelection::Exact(selection) => selection.len(base.len()),
                ViewSelection::Pending(lazy) => match lazy.len_if_known() {
                    Some(len) => len,
                    None => lazy.materialized_sync()?.len(),
                },
            },
            // Tombstones ask liveness per selected row, so a pending
            // selection materializes — a count is one of the consumers the
            // deferred ids exist for.
            QuadsSource::InMemory {
                base,
                selection,
                deleted: Some(deleted),
                ..
            } => selection
                .materialized_sync()?
                .live_mask(deleted, base.len())
                .true_count(),
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                // A count needs the selection itself — the serve plan cannot
                // answer it — so a served match's deferred index-child scan
                // runs here, once, and is cached on the view.
                let selection = selection.materialized().await?;
                match filter {
                    // No filter pending: the selection is exact, minus whatever
                    // the tombstones have removed from it.
                    None => match deleted {
                        None => selection.len(file.row_count() as usize),
                        Some(d) => selection
                            .live_mask(d, file.row_count() as usize)
                            .true_count(),
                    },
                    // A filter is pending: its selectivity is unknown ahead of
                    // time, so the rows actually have to be evaluated (with the
                    // tombstoned rows excluded before counting).
                    Some(f) => {
                        file_scan::count_matching_rows(file, f, &selection, deleted.as_ref())
                            .await?
                    }
                }
            }
        };
        // The tail's contribution: its selection is always exact (tail matches
        // are resolved eagerly), minus its own tombstones.
        let tail = self.tail.as_ref().map_or(0, |tail| match &tail.deleted {
            None => tail.selection.len(tail.rows.len()),
            Some(deleted) => tail
                .selection
                .live_mask(deleted, tail.rows.len())
                .true_count(),
        });
        Ok(base + tail)
    }

    /// Gather the rows this view selects into a single in-memory StructArray
    /// of primary columns only — index components never ride in the returned
    /// array (in memory they live beside the base, on disk as index children;
    /// serialize through [`to_serializable_parts`](Self::to_serializable_parts)
    /// to get them beside the rows).
    ///
    /// On a Dictionary view with a non-empty append tail the returned codes
    /// address a *fresh* dictionary (the tail's terms are not in the cached
    /// one) that this method cannot hand out — decode such views through
    /// [`quads`](Self::quads)/[`quads_vec`](Self::quads_vec), or serialize
    /// them via [`to_serializable_parts`](Self::to_serializable_parts), which
    /// returns the dictionary beside the rows. Without a tail — tombstoned or
    /// not — Dictionary codes always address the cached dictionary
    /// ([`code_read_snapshot`](Self::code_read_snapshot) hands it out).
    pub async fn get_quads_array(&self) -> Result<ArrayRef> {
        self.selected_rows().await
    }

    /// The rows this view selects, base and tail combined — the rows-only
    /// counterpart of serialization's `selected_parts`: no index components
    /// are materialized, rebuilt, or split off, because none ride in the
    /// result.
    ///
    /// Without a tail the base's selected live rows are the whole answer, in
    /// the store's own vocabulary: under the Dictionary layout a tombstone
    /// never re-encodes, so the codes stay addressed to the cached dictionary
    /// the caller can actually obtain. With a tail the layouts diverge:
    /// - a Dictionary view must re-encode base and tail together against a
    ///   fresh dictionary (the tail's terms have no codes in the cached one —
    ///   see [`get_quads_array`](Self::get_quads_array)'s contract);
    /// - every other layout stores the tail in the base's own vocabulary, so
    ///   the two chunk together with no decode at all.
    async fn selected_rows(&self) -> Result<ArrayRef> {
        let base = self.base_selected_rows().await?;
        let Some(tail) = &self.tail else {
            return Ok(base);
        };
        let tail_rows = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref(), None)?;
        match &self.layout {
            ResolvedLayout::Dictionary(_) => {
                let mut raws = self.base_raw_quads(&base).await?;
                raws.extend(ResolvedLayout::Default.raw_quads(&tail_rows)?);
                if raws.is_empty() {
                    return dictionary::empty_struct(&[]);
                }
                let (dict, id_map) = TermDictionary::from_quads_with_map(&raws)?;
                // Appended rows break the base's subject sort, and no index
                // set rides along: the chunk is the primary columns alone.
                dictionary::build_chunk(&raws, &dict, &id_map, &[], 0, false, true)
            }
            _ => {
                // The tail is a second chunk in the base's own vocabulary —
                // no raws decode, no rebuild.
                let dtype = base.dtype().clone();
                Ok(ChunkedArray::try_new(vec![base, tail_rows], dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array())
            }
        }
    }

    /// The rows this view selects, as four `u32` term-code columns (`s`, `p`,
    /// `o`, `g`) — read off the answering index's own columns when the view
    /// carries a serve plan that covers them, else gathered directly from the
    /// base's canonical primitive slices.
    ///
    /// `None` whenever codes cannot be served both cheaply and correctly:
    /// a non-Dictionary layout, a non-empty append tail (its strings are not
    /// in the cached dictionary), a file-backed source, or base columns not
    /// reachable as canonical non-nullable u32 primitives (e.g. chunked or
    /// wire-compressed). Callers fall back to [`Self::get_quads_array`].
    ///
    /// A builder-compressed column behind a `vortex.shared` wrapper still
    /// qualifies: its canonical primitive is materialized once into the
    /// wrapper's one-way cache (`shared_u32_primitive`) and shared zero-copy
    /// by every later call and every view over the base — the payload path
    /// pays a first-touch decode instead of losing the buffer-sharing fast
    /// path.
    ///
    /// This is the payload path behind the JS bindings' `match`/`getQuads`:
    /// serving codes off the base's buffers skips the per-call
    /// slice-gather-canonicalize pipeline those calls otherwise pay.
    pub fn code_columns(&self) -> Option<[Buffer<u32>; 4]> {
        use vortex_array::arrays::Struct;
        if self.layout.strategy() != LayoutStrategy::Dictionary || self.tail_len() != 0 {
            return None;
        }
        // Without `file-io` the InMemory variant is the only one, making this
        // pattern irrefutable — which is fine, not a bug.
        #[allow(irrefutable_let_patterns)]
        let QuadsSource::InMemory {
            base,
            selection,
            deleted,
            serve,
            ..
        } = &self.quads
        else {
            return None;
        };
        // Served fast path: the answering index's own columns already hold
        // this view's codes as one contiguous run, so reading them there
        // costs neither the row-id materialization this view deferred at
        // match time nor a scattered gather over the primaries.
        if let Some(plan) = serve
            && let Some(columns) = plan.code_columns(deleted.as_ref())
        {
            return Some(columns);
        }
        let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
        let mut prims: Vec<PrimitiveArray> = Vec::with_capacity(4);
        for name in schema::PRIMARY_COLUMNS {
            let col = struct_arr.unmasked_field_by_name(name).ok()?;
            prims.push(crate::store::array::shared_u32_primitive(col)?);
        }
        // No plan (or a plan that declined): codes are gathered by row id, so
        // a served match's pending selection materializes here (the in-memory
        // decode+sort it deferred at match time).
        let selection = selection.materialized_sync().ok()?;
        // Contiguous, tombstone-free selections share the base's buffers
        // zero-copy (a `Buffer` slice is a refcount bump); a tombstone-free id
        // list is a branch-free gather; only tombstoned views pay a
        // per-element liveness test.
        let column = |prim: &PrimitiveArray| -> Buffer<u32> {
            match (&selection, deleted) {
                (RowSelection::All, None) => prim.clone().into_buffer::<u32>(),
                (RowSelection::Range(r), None) => prim
                    .clone()
                    .into_buffer::<u32>()
                    .slice(r.start as usize..r.end as usize),
                // An index-resolved match without deletes — the bindings'
                // common payload shape.
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

    /// The rows this view selects as four `u32` term-code columns, gathering
    /// them when [`code_columns`](Self::code_columns)' zero-copy fast path
    /// does not apply.
    ///
    /// The fallback is the full read pipeline —
    /// [`get_quads_array`](Self::get_quads_array), canonicalize, then one
    /// primitive column per role — so a file-backed store, a narrowed view
    /// whose base columns are chunked, or any other non-canonical shape still
    /// answers codes. Only the cases where codes are not the store's
    /// vocabulary at all yield `None`: a non-Dictionary layout, or a non-empty
    /// append tail (whose terms are absent from the cached dictionary, so its
    /// codes would address a different one).
    ///
    /// This is the payload path behind the bindings' code-column reads; they
    /// call it instead of re-implementing the gather.
    pub async fn code_columns_gathered(&self) -> Result<Option<[Buffer<u32>; 4]>> {
        if let Some(columns) = self.code_columns() {
            return Ok(Some(columns));
        }
        if self.layout.strategy() != LayoutStrategy::Dictionary || self.tail_len() != 0 {
            return Ok(None);
        }
        let rows = self.selected_rows().await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let column = |name: &str, ctx: &mut vortex_array::ExecutionCtx| -> Result<Buffer<u32>> {
            let col = struct_arr
                .unmasked_field_by_name(name)
                .map_err(VortexRdfError::Vortex)?;
            let prim = col
                .clone()
                .execute::<PrimitiveArray>(ctx)
                .map_err(VortexRdfError::Vortex)?;
            Ok(prim.into_buffer::<u32>())
        };
        Ok(Some([
            column(schema::COL_S, &mut ctx)?,
            column(schema::COL_P, &mut ctx)?,
            column(schema::COL_O, &mut ctx)?,
            column(schema::COL_G, &mut ctx)?,
        ]))
    }

    /// The base rows this view covers (gathered in memory, or scanned from the
    /// file with the pending filter and selection applied) — without the tail.
    pub(super) async fn base_selected_rows(&self) -> Result<ArrayRef> {
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                ..
            } => {
                // A base-order gather needs exact row ids (a serve plan
                // reorders rows), so a served match's pending selection
                // materializes here.
                let selection = selection.materialized_sync()?;
                match (&selection, deleted) {
                    // The whole base, nothing deleted: hand back the array as
                    // it stands (pure primary columns — index copies live in
                    // `self.components`, not the base).
                    (RowSelection::All, None) => Ok(base.clone()),
                    // Anything narrower: gather the live selected rows. A
                    // gather preserves row order (selections are ascending,
                    // tombstones only drop rows), so the base's subject
                    // sortedness carries to the result — but the filter/take
                    // kernels do not propagate the stat, so restore it from
                    // the base's own provenance.
                    _ => {
                        let rows = gather_live(base, &selection, deleted.as_ref(), Some(probes))?;
                        Self::with_subject_stamp(rows, Self::base_subject_sorted(base))
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
                // Same materialization as above — the scan reads in file row
                // order, which only the exact ids can restrict.
                let selection = selection.materialized().await?;
                // A tiny exact selection reads point-by-point through the
                // file's cached chunk probes, skipping the scan machinery and
                // its whole-leaf decodes; anything it declines scans below.
                if let Some(rows) = file_scan::file_point_rows(
                    file,
                    &self.layout.primary_column_names(),
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                )
                .await?
                {
                    return Self::with_subject_stamp(rows, file.quads_sorted());
                }
                let scan =
                    self.restricted_file_scan(file, filter.as_ref(), &selection, deleted.as_ref())?;
                // Execute the scan and materialize every matching row into a
                // single in-memory array.
                let arr = scan
                    .into_array_stream()
                    .map_err(VortexRdfError::Vortex)?
                    .read_all()
                    .await
                    .map_err(VortexRdfError::Vortex)?;
                // A scan preserves file row order and this view only narrows
                // it, so the file's recorded quads_sorted provenance carries
                // to the materialized rows; the multi-chunk read loses any
                // per-leaf stats, so restore the stamp explicitly — without
                // it, a re-serialization would demote the file to
                // quads_sorted:false and every later reader would lose the
                // subject binary search.
                Self::with_subject_stamp(arr, file.quads_sorted())
            }
        }
    }

    /// The scan every unserved file read starts from: the layout's primary
    /// columns only (index columns are internal and never surfaced), with the
    /// restrictions the view accumulated via `match_pattern` applied — a
    /// pushed-down filter for the components no index resolved, and the row
    /// selection (with tombstoned rows excluded) for those it did.
    #[cfg(feature = "file-io")]
    pub(super) fn restricted_file_scan(
        &self,
        file: &crate::store::native_file::NativeStoreFile,
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<ScanBuilder<ArrayRef>> {
        let proj = self.layout.primary_column_names();
        let mut scan = file
            .scan()
            .map_err(VortexRdfError::Vortex)?
            .with_projection(select(proj, root()));
        if let Some(f) = filter {
            scan = scan.with_filter(f.clone());
        }
        Ok(selection.restrict_scan(scan, deleted))
    }

    /// Whether an in-memory base's `s` column carries the sorted stamp.
    pub(super) fn base_subject_sorted(base: &ArrayRef) -> bool {
        base.clone()
            .try_downcast::<vortex_array::arrays::Struct>()
            .ok()
            .and_then(|s| {
                s.unmasked_field_by_name(schema::COL_S)
                    .ok()
                    .map(crate::store::array::column_is_sorted)
            })
            .unwrap_or(false)
    }

    /// Canonicalize `rows` and restore the subject sorted stamp when the
    /// caller's provenance says the rows are globally `s`-sorted. A no-op
    /// pass-through when they are not.
    pub(super) fn with_subject_stamp(rows: ArrayRef, sorted: bool) -> Result<ArrayRef> {
        if !sorted {
            return Ok(rows);
        }
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        if let Ok(col) = struct_arr.unmasked_field_by_name(schema::COL_S) {
            crate::store::array::stamp_is_sorted(col);
        }
        Ok(struct_arr.into_array())
    }

    /// The base rows decoded to raw quads, through the async path when the
    /// dictionary is file-backed (only possible on a file-io build).
    pub(super) async fn base_raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        #[cfg(feature = "file-io")]
        {
            self.layout.raw_quads_async(rows).await
        }
        #[cfg(not(feature = "file-io"))]
        {
            self.layout.raw_quads(rows)
        }
    }

    /// Every live quad this view covers, decoded to raw N-Triples term strings
    /// — base rows first (in view order), then tail rows.
    pub(super) async fn live_raw_quads(&self) -> Result<Vec<RawQuad>> {
        let mut raws = self
            .base_raw_quads(&self.base_selected_rows().await?)
            .await?;
        if let Some(tail) = &self.tail {
            let rows = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref(), None)?;
            raws.extend(self.tail_layout().raw_quads(&rows)?);
        }
        Ok(raws)
    }
}
