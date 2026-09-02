//! The row/code read surface: sizes, gathered rows, code columns, and the
//! selected-rows plumbing the serialization and compaction paths share.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, field_as, subject_sorted, with_subject_stamp};
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{LayoutStrategy, ResolvedLayout, dictionary};
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::scan::gather::gather_live;
use crate::store::schema;
use crate::store::selection::{RowSelection, ViewSelection};

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;

#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, root, select};
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
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
                    None => lazy.materialized()?.len(),
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
                .materialized()?
                .live_mask(deleted, base.len())
                .true_count(),
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                serve,
                ..
            } => {
                // A located serve plan knows the width of the child run it
                // serves — exactly the constrained rows — so a pending
                // selection over one counts from the plan, without the
                // deferred index-child scan the selection itself would run.
                // Tombstones are defined over primary row ids the plan does
                // not hold, and a pending filter's selectivity is unknown, so
                // either sends the count through the selection.
                let located = match (selection, serve, filter, deleted) {
                    (ViewSelection::Pending(_), Some(plan), None, None) => plan.row_range(),
                    _ => None,
                };
                if let Some(range) = located {
                    (range.end - range.start) as usize
                } else {
                    // A count needs the selection itself, so a served match's
                    // deferred index-child scan runs here, once, and is
                    // cached on the view.
                    let selection = selection.materialized_async().await?;
                    match filter {
                        // No filter pending: the selection is exact, minus
                        // whatever the tombstones have removed from it.
                        None => match deleted {
                            None => selection.len(file.row_count() as usize),
                            Some(d) => selection
                                .live_mask(d, file.row_count() as usize)
                                .true_count(),
                        },
                        // A filter is pending: its selectivity is unknown
                        // ahead of time, so the rows actually have to be
                        // evaluated (with the tombstoned rows excluded before
                        // counting).
                        Some(f) => {
                            file_scan::count_matching_rows(file, f, &selection, deleted.as_ref())
                                .await?
                        }
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

    /// The rows this view selects, base and tail combined, as a single
    /// in-memory array of primary columns only — the rows-only counterpart
    /// of serialization's `selected_parts`: no index components are
    /// materialized, rebuilt, or split off, because none ride in the result
    /// (in memory they live beside the base, on disk as index children;
    /// serialize through [`to_serializable_parts`](Self::to_serializable_parts)
    /// to get them beside the rows).
    ///
    /// Without a tail the base's selected live rows are the whole answer, in
    /// the store's own vocabulary: under the Dictionary layout a tombstone
    /// never re-encodes, so the codes stay addressed to the cached dictionary
    /// ([`code_read_snapshot`](Self::code_read_snapshot) hands it out). With
    /// a tail the layouts diverge:
    /// - a Dictionary view must re-encode base and tail together against a
    ///   *fresh* dictionary (the tail's terms have no codes in the cached one)
    ///   that this method cannot hand out — decode such views through
    ///   [`quads`](Self::quads)/[`quads_vec`](Self::quads_vec), or serialize
    ///   them via [`to_serializable_parts`](Self::to_serializable_parts),
    ///   which returns the dictionary beside the rows;
    /// - every other layout stores the tail in the base's own vocabulary, so
    ///   the two chunk together with no decode at all.
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
                let (_, code_map) = TermDictionary::from_quads_with_map(&raws)?;
                // Appended rows break the base's subject sort, and no index
                // set rides along: the chunk is the primary columns alone.
                dictionary::build_chunk(&raws, &code_map, false)
            }
            _ => {
                // The tail is a second chunk in the base's own vocabulary —
                // no raws decode, no rebuild.
                let tail_rows = tail.live_rows()?;
                let dtype = base.dtype().clone();
                chunked_or_single(vec![base, tail_rows], dtype)
            }
        }
    }

    /// The rows this view selects, as four `u32` term-code columns (`s`, `p`,
    /// `o`, `g`) — read off the answering index's own columns when the view
    /// carries a serve plan that covers them, else off the base's canonical
    /// columns (see [`select_codes`]).
    ///
    /// `None` whenever codes cannot be served this way: a non-Dictionary
    /// layout, a non-empty append tail (its strings are not in the cached
    /// dictionary), a file-backed source, or base columns that are not
    /// canonical non-nullable u32 primitives — the wire encodings an adopted
    /// base keeps, which [`code_columns_shared`](Self::code_columns_shared)
    /// serves through the live canonical cache.
    pub(crate) fn code_columns(&self) -> Option<[Buffer<u32>; 4]> {
        use vortex_array::arrays::Struct;
        if self.layout.strategy() != LayoutStrategy::Dictionary || self.tail_len() != 0 {
            return None;
        }
        // Without `file-io`, InMemory is the only variant.
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
        let mut columns: Vec<Buffer<u32>> = Vec::with_capacity(4);
        for name in schema::PRIMARY_COLUMNS {
            let col = struct_arr.unmasked_field_by_name(name).ok()?;
            columns.push(crate::store::array::canonical_u32(col)?.into_buffer::<u32>());
        }
        // No plan (or a plan that declined): codes are gathered by row id, so
        // a served match's pending selection materializes here (the in-memory
        // decode+sort it deferred at match time).
        let selection = selection.materialized().ok()?;
        Some(select_codes(&columns, &selection, deleted.as_ref()))
    }

    /// [`code_columns`](Self::code_columns), extended to an encoded base
    /// through its live canonical cache
    /// ([`LiveCanonical`](crate::store::canonical::LiveCanonical)): a
    /// contiguous, tombstone-free selection wider than a point read decodes
    /// each column once — shared with every holder alive, freed with the
    /// last — and hands out slices of it; an id list or a tombstoned view
    /// gathers from the decoded columns only while some holder keeps them
    /// alive.
    ///
    /// `None` leaves the rest to the gather pipeline: point-sized selections
    /// (point reads through the probes) and gathers over columns nobody
    /// holds (a `take` over the encoded base) — neither decodes a whole
    /// column for a few rows.
    pub(crate) fn code_columns_shared(&self) -> Result<Option<[Buffer<u32>; 4]>> {
        if let Some(columns) = self.code_columns() {
            return Ok(Some(columns));
        }
        if self.layout.strategy() != LayoutStrategy::Dictionary || self.tail_len() != 0 {
            return Ok(None);
        }
        #[allow(irrefutable_let_patterns)]
        let QuadsSource::InMemory {
            base,
            selection,
            deleted,
            canonical,
            ..
        } = &self.quads
        else {
            return Ok(None);
        };
        let selection = selection.materialized()?;
        if selection.is_point_sized() {
            return Ok(None);
        }
        let contiguous =
            matches!(selection, RowSelection::All | RowSelection::Range(_)) && deleted.is_none();
        let struct_arr = crate::store::array::into_struct_array(base.clone())?;
        let mut columns: Vec<Buffer<u32>> = Vec::with_capacity(4);
        for (idx, name) in schema::PRIMARY_COLUMNS.iter().enumerate() {
            let column = if contiguous {
                let col = struct_arr
                    .unmasked_field_by_name(name)
                    .map_err(VortexRdfError::Vortex)?;
                canonical.column(idx, col)?
            } else {
                let Some(column) = canonical.column_if_alive(idx) else {
                    return Ok(None);
                };
                column
            };
            columns.push(column);
        }
        Ok(Some(select_codes(&columns, &selection, deleted.as_ref())))
    }

    /// The rows this view selects as four `u32` term-code columns, gathering
    /// them when neither [`code_columns`](Self::code_columns) nor the live
    /// canonical cache ([`code_columns_shared`](Self::code_columns_shared))
    /// applies.
    ///
    /// The fallback is the full read pipeline — `selected_rows` (point reads
    /// through the probes for a point-sized selection, a `take` over the base
    /// otherwise), canonicalize, then one primitive column per role — so a
    /// file-backed store, a narrow view over an encoded base, or any other
    /// shape still answers codes. Only the cases where codes are not the
    /// store's vocabulary at all yield `None`: a non-Dictionary layout, or a
    /// non-empty append tail (whose terms are absent from the cached
    /// dictionary, so its codes would address a different one).
    ///
    /// This is the payload path behind the bindings' code-column reads; they
    /// call it instead of re-implementing the gather.
    pub async fn code_columns_gathered(&self) -> Result<Option<[Buffer<u32>; 4]>> {
        if let Some(columns) = self.code_columns_shared()? {
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
            Ok(field_as::<PrimitiveArray>(&struct_arr, name, ctx)?.into_buffer::<u32>())
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
                let selection = selection.materialized()?;
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
                // Same materialization as above — the scan reads in file row
                // order, which only the exact ids can restrict.
                let selection = selection.materialized_async().await?;
                // A tiny exact selection reads point-by-point through the
                // file's cached chunk probes, skipping the scan machinery and
                // its whole-leaf decodes; anything it declines runs the scan.
                let scan =
                    self.restricted_file_scan(file, filter.as_ref(), &selection, deleted.as_ref())?;
                let arr = file_scan::point_rows_or_scan(
                    file_scan::file_point_rows(
                        file,
                        self.layout.strategy().primary_column_names(),
                        filter.as_ref(),
                        &selection,
                        deleted.as_ref(),
                    ),
                    scan,
                )
                .await?;
                // A scan preserves file row order and this view only narrows
                // it, so the file's recorded quads_sorted provenance carries
                // to the materialized rows; the multi-chunk read loses any
                // per-leaf stats, so restore the stamp explicitly — without
                // it, a re-serialization would demote the file to
                // quads_sorted:false and every later reader would lose the
                // subject binary search.
                with_subject_stamp(arr, file.quads_sorted())
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
        self.restricted_file_scan_projected(
            file,
            filter,
            selection,
            deleted,
            self.layout.strategy().primary_column_names(),
        )
    }

    /// [`restricted_file_scan`](Self::restricted_file_scan) reading only
    /// `columns` — a subset of the primary columns, in the caller's order —
    /// so a consumer that needs fewer columns never decodes the rest.
    #[cfg(feature = "file-io")]
    pub(super) fn restricted_file_scan_projected(
        &self,
        file: &crate::store::native_file::NativeStoreFile,
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
        columns: &[&str],
    ) -> Result<ScanBuilder<ArrayRef>> {
        let proj: Vec<vortex_array::dtype::FieldName> =
            columns.iter().map(|c| vortex_array::dtype::FieldName::from(*c)).collect();
        let mut scan = file.scan().map_err(VortexRdfError::Vortex)?;
        // The scan's scope (the quad-source root dtype) is what filters and
        // projections bind against — read it before the projection replaces
        // it. Binding goes through the handle's memo so a repeated shape
        // keeps one identity (see `BoundExprMemo`).
        let scope = scan.dtype().map_err(VortexRdfError::Vortex)?;
        let memo = file.bound_exprs();
        scan = scan.with_projection(
            memo.bind(file_scan::QUAD_SCOPE, &select(proj, root()), &scope)
                .map_err(VortexRdfError::Vortex)?,
        );
        if let Some(f) = filter {
            scan = scan.with_filter(
                memo.bind(file_scan::QUAD_SCOPE, f, &scope)
                    .map_err(VortexRdfError::Vortex)?,
            );
        }
        Ok(selection.restrict_scan(scan, deleted))
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

    /// The given base rows decoded to raw quads, followed by the tail's live
    /// rows, and how many of the result came from the base.
    pub(super) async fn merged_raw_quads(&self, base: &ArrayRef) -> Result<(Vec<RawQuad>, usize)> {
        let mut raws = self.base_raw_quads(base).await?;
        let base_rows = raws.len();
        if let Some(tail) = &self.tail {
            raws.extend(self.tail_layout().raw_quads(&tail.live_rows()?)?);
        }
        Ok((raws, base_rows))
    }

    /// Every live quad this view covers, decoded to raw N-Triples term strings
    /// — base rows first (in view order), then tail rows.
    pub(super) async fn live_raw_quads(&self) -> Result<Vec<RawQuad>> {
        let base = self.base_selected_rows().await?;
        Ok(self.merged_raw_quads(&base).await?.0)
    }
}

/// The codes `selection` picks out of four canonical columns, tombstones
/// dropped: a contiguous, tombstone-free selection is a slice of each column
/// (a refcount bump — the buffers stay shared with whoever holds the
/// columns), a tombstone-free id list is a branch-free gather, and only
/// tombstoned views pay a per-element liveness test.
fn select_codes(
    columns: &[Buffer<u32>],
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> [Buffer<u32>; 4] {
    let column = |column: &Buffer<u32>| -> Buffer<u32> {
        match (selection, deleted) {
            (RowSelection::All, None) => column.clone(),
            (RowSelection::Range(r), None) => column.slice(r.start as usize..r.end as usize),
            (RowSelection::Ids(ids), None) => {
                let slice = column.as_slice();
                Buffer::from_iter(ids.iter().map(|&i| slice[i as usize]))
            }
            (selection, Some(deleted)) => {
                let slice = column.as_slice();
                let live = |i: usize| !deleted.value(i);
                match selection {
                    RowSelection::All => Buffer::from_iter(
                        (0..slice.len()).filter(|&i| live(i)).map(|i| slice[i]),
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
    [
        column(&columns[0]),
        column(&columns[1]),
        column(&columns[2]),
        column(&columns[3]),
    ]
}
