//! Narrowings of a served view: a [`window`](VortexRdfStore::window),
//! [`partitions`](VortexRdfStore::partitions), a
//! [`keep`](VortexRdfStore::keep) or a pattern chained on a view a by-copy
//! index serves cut or filter its rows in the index's order ("index order,
//! filtered") and stay served, so the narrowed view reads off the index too.
//! Each is written once over [`Served`], the view's plan beside its base: the
//! one place a narrowing reads through a backend (the view's live rows, the
//! index's columns at them) and builds the narrowed view.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use super::keep::Keep;
use crate::error::Result;
use crate::store::array::bool_array_to_mask;
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
use crate::store::indexes::{InMemoryServePlan, LazyRowIds, ServedRows, ServedSpan};
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::scan::residual::mask_for;
use crate::store::schema::{QuadColumn, RowId, TermCode};
use crate::store::view::selection::{RowSelection, ViewSelection, window_rows};
#[cfg(feature = "file-io")]
use crate::store::{FileBase, SortOrder, read::chunks::extract_codes};
use crate::store::{MemoryBase, QuadsSource, VortexRdfStore};

/// A served view's plan beside its base, with the rows of the index it
/// covers located: what every narrowing of the view reads and builds
/// through.
pub(in crate::store::query) struct Served<'a> {
    quads: &'a QuadsSource,
    /// The rows of the index the view covers, which a narrowing cuts or
    /// filters.
    span: ServedSpan,
    backend: Backend<'a>,
}

enum Backend<'a> {
    Memory {
        memory: &'a MemoryBase,
        plan: &'a InMemoryServePlan,
    },
    #[cfg(feature = "file-io")]
    File {
        base: &'a FileBase,
        plan: &'a FileServePlan,
    },
}

/// A served view's live rows, as a window or partition cuts them.
enum LiveRows {
    /// A range with nothing tombstoned: every row of it live, unlisted.
    Range(Range<u64>),
    /// A range's live rows under tombstones, listed.
    Spanned(ServedRows),
    /// Kept rows, each live.
    Kept(ServedRows),
}

/// What a window or partition (or a keep or chained match) leaves of a
/// served view's rows: the span its plan covers, beside the row ids the
/// narrowed view holds, in the index's order.
enum Cut {
    /// A range with nothing tombstoned, whose ids its backend names (a slice
    /// of the run's in memory, read off the child on first need on file).
    Range(Range<u64>),
    /// Listed live rows: the range a tombstoned range's cut spans, or kept
    /// rows, beside their ids.
    Listed {
        span: ServedSpan,
        rids: Buffer<RowId>,
    },
}

impl LiveRows {
    fn len(&self) -> usize {
        match self {
            LiveRows::Range(range) => (range.end - range.start) as usize,
            LiveRows::Spanned(rows) | LiveRows::Kept(rows) => rows.len(),
        }
    }

    /// Rows `piece` (not empty) of these, in their order: a part of the
    /// range; under tombstones the range from the first of the piece's live
    /// rows to its last (the tombstoned rows inside dropped at read through
    /// their row ids); or a slice of the kept rows.
    fn cut(&self, piece: Range<usize>) -> Cut {
        debug_assert!(!piece.is_empty());
        match self {
            LiveRows::Range(range) => {
                Cut::Range(range.start + piece.start as u64..range.start + piece.end as u64)
            }
            LiveRows::Spanned(rows) => {
                let rows = rows.slice(piece);
                let (first, last) = (rows.positions[0], rows.positions[rows.len() - 1]);
                Cut::Listed {
                    span: ServedSpan::Range(first..last + 1),
                    rids: rows.rids,
                }
            }
            LiveRows::Kept(rows) => Cut::kept(rows.slice(piece)),
        }
    }
}

impl Cut {
    /// Kept rows, each live.
    fn kept(rows: ServedRows) -> Self {
        let rids = rows.rids.clone();
        Cut::Listed {
            span: ServedSpan::Kept(Arc::new(rows)),
            rids,
        }
    }

    /// How many base rows the narrowed view holds.
    fn len(&self) -> usize {
        match self {
            Cut::Range(range) => (range.end - range.start) as usize,
            Cut::Listed { rids, .. } => rids.len(),
        }
    }
}

impl<'a> Served<'a> {
    /// The view `quads` selects, when a plan serves it, with the rows its
    /// plan covers: on file an unlocated run is located first, read once by
    /// row index ([`located_span`](FileServePlan::located_span)). `None` for
    /// a view no plan serves.
    pub(in crate::store::query) async fn of(quads: &'a QuadsSource) -> Result<Option<Self>> {
        #[cfg(feature = "file-io")]
        if let Some(base) = quads.file() {
            let Some(plan) = &base.serve else {
                return Ok(None);
            };
            let span = plan.located_span(quads.deleted.as_ref()).await?;
            return Ok(Some(Served {
                quads,
                span,
                backend: Backend::File { base, plan },
            }));
        }
        let Some(memory) = quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        Ok(Some(Served {
            quads,
            span: plan.span(),
            backend: Backend::Memory { memory, plan },
        }))
    }

    /// [`of`](Self::of) a view a narrowing left part of its run (a window,
    /// keep, partition or chained match of a served view), whose rows are
    /// known without reading: what a pattern chained on the view filters.
    pub(in crate::store::query) fn of_narrowed(quads: &'a QuadsSource) -> Option<Self> {
        #[cfg(feature = "file-io")]
        if let Some(base) = quads.file() {
            let plan = base.serve.as_ref().filter(|plan| plan.is_narrowed())?;
            return Some(Served {
                quads,
                span: plan.span()?,
                backend: Backend::File { base, plan },
            });
        }
        let memory = quads.in_memory()?;
        let plan = memory.serve.as_ref().filter(|plan| plan.is_narrowed())?;
        Some(Served {
            quads,
            span: plan.span(),
            backend: Backend::Memory { memory, plan },
        })
    }

    fn deleted(&self) -> Option<&'a Mask> {
        self.quads.deleted.as_ref()
    }

    /// The view's live rows, unlisted where they are a range with nothing
    /// tombstoned.
    async fn live(&self) -> Result<LiveRows> {
        Ok(match (&self.span, self.deleted()) {
            (ServedSpan::Kept(rows), _) => LiveRows::Kept((**rows).clone()),
            (ServedSpan::Range(range), None) => LiveRows::Range(range.clone()),
            (ServedSpan::Range(_), Some(_)) => LiveRows::Spanned(self.live_rows().await?),
        })
    }

    /// The view's live rows listed, each with its base row id: the kept
    /// rows, or a range's (in memory its run's ids, on file its rid column)
    /// less the tombstones.
    async fn live_rows(&self) -> Result<ServedRows> {
        match &self.backend {
            Backend::Memory { plan, .. } => plan.live_rows(self.deleted()),
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => plan.live_rows(&base.file, self.deleted()).await,
        }
    }

    /// The index's `columns` at its rows `positions` (ascending on file, not
    /// empty), in that order.
    async fn columns_at(
        &self,
        columns: &[&'static str],
        positions: &Buffer<u64>,
    ) -> Result<ArrayRef> {
        match &self.backend {
            Backend::Memory { plan, .. } => plan.columns_at(columns, positions),
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => plan.columns_at(&base.file, columns, positions).await,
        }
    }

    /// The view's live rows whose `column` code `admits`, in the index's
    /// order: what a keep keeps. In memory each row's code is read where it
    /// lies (the base's code cache at its row id, else the component's probe
    /// at its position); on file the column is read over the range, or at
    /// the kept rows. `None` for an in-memory column neither reads.
    async fn admitted(
        &self,
        store: &VortexRdfStore,
        column: QuadColumn,
        admits: impl Fn(TermCode) -> bool,
    ) -> Result<Option<ServedRows>> {
        let deleted = self.deleted();
        match &self.backend {
            Backend::Memory { plan, .. } => {
                if let Some(codes) = store.base_code_primitive(column) {
                    let codes = codes.as_slice::<TermCode>();
                    return Ok(Some(
                        plan.kept_rows(deleted, |_, rid| admits(codes[rid as usize]))?,
                    ));
                }
                let Some(codes) = plan.component_probe(column.name()) else {
                    return Ok(None);
                };
                Ok(Some(plan.kept_rows(deleted, |position, _| {
                    admits(codes.value_at(position as usize))
                })?))
            }
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => {
                use crate::store::indexes::{COL_RID, base_row};

                let name = column.name();
                Ok(Some(match &self.span {
                    // Kept rows are live: only `admits` tests them.
                    ServedSpan::Kept(kept) => {
                        let read = plan
                            .columns_at(&base.file, &[name], &kept.positions)
                            .await?;
                        let codes = extract_codes(read, &[name])?.remove(0);
                        let mut codes = codes.iter();
                        ServedRows::collect(kept.positions_and_rids(), kept.len(), |_, _| {
                            codes.next().is_some_and(|&code| admits(code))
                        })
                    }
                    ServedSpan::Range(range) => {
                        let mut columns = plan
                            .range_columns(&base.file, range.clone(), &[name, COL_RID])
                            .await?;
                        let (rids, codes) = (columns.remove(1), columns.remove(0));
                        let rows = range.clone().zip(rids.iter().map(|&rid| base_row(rid)));
                        let mut codes = codes.iter();
                        ServedRows::collect(rows, codes.len(), |_, rid| {
                            // Every row's code is passed, tombstoned or not.
                            let admitted = codes.next().is_some_and(|&code| admits(code));
                            admitted && deleted.is_none_or(|d| !d.value(rid as usize))
                        })
                    }
                }))
            }
        }
    }

    /// The view narrowed to `cut`, served: its plan over the cut's span, its
    /// ids the cut's in the index's order, which leave out every tombstone
    /// (a narrowing takes live rows); on file every narrowed row passes the
    /// view's filter, so its counts and reads take the plan's rows, the
    /// filter's pinned codes kept for reads by id.
    fn narrowed(&self, cut: Cut) -> Result<QuadsSource> {
        Ok(match &self.backend {
            Backend::Memory { memory, plan } => {
                let (span, rids) = match cut {
                    Cut::Range(range) => {
                        let rids = plan.range_rids(range.clone())?;
                        (ServedSpan::Range(range), rids)
                    }
                    Cut::Listed { span, rids } => (span, rids),
                };
                let ids = LazyRowIds::from_unsorted_ids(rids, true);
                self.quads.in_memory_with(
                    memory,
                    ViewSelection::pending(ids),
                    Some(plan.with_span(span)),
                )
            }
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => {
                let (span, ids) = match cut {
                    Cut::Range(range) => {
                        // The run's rids ascend where the index's order
                        // within it is base order.
                        let ascending = plan.read_order() == Some(SortOrder::Spog);
                        let ids = LazyRowIds::from_located_run(
                            Arc::clone(&base.file),
                            plan.child().clone(),
                            range.clone(),
                            ascending,
                        );
                        (ServedSpan::Range(range), ids)
                    }
                    Cut::Listed { span, rids } => (span, LazyRowIds::from_unsorted_ids(rids, true)),
                };
                self.quads
                    .file_with(
                        base,
                        base.filter.clone(),
                        ViewSelection::pending(ids),
                        Some(plan.with_span(span)),
                    )
                    .with_filter_resolved()
            }
        })
    }
}

impl VortexRdfStore {
    /// The window of a served view, itself served: rows
    /// `offset..offset + limit` of the view's live rows in its index's
    /// order, a part of its range (whose ids a slice of the run's in memory,
    /// read on first need from those rows of the child's rid column on file)
    /// or a slice of its kept rows, their ids kept unsorted
    /// ([`LazyRowIds::from_unsorted_ids`]); under tombstones the cut counts
    /// live rows. The tail is windowed with what the base did not fill, as
    /// [`window_cutting`](Self::window_cutting) windows it. A window that
    /// holds no base row selects none, with no plan. `None` for a view no
    /// plan serves.
    pub(in crate::store) async fn served_window(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let Some(served) = Served::of(&self.quads).await? else {
            return Ok(None);
        };
        let live = served.live().await?;
        let base_live = live.len();
        let (quads, base_taken) = match window_rows(offset, limit, base_live as u64) {
            Some(rows) => {
                let cut = live.cut(rows.start as usize..rows.end as usize);
                let taken = cut.len();
                (served.narrowed(cut)?, taken)
            }
            None => (
                self.quads
                    .with_selection(ViewSelection::Exact(RowSelection::empty())),
                0,
            ),
        };
        let tail = self.windowed_tail(offset, limit, base_live, base_taken);
        Ok(Some(self.derived(quads, tail)))
    }

    /// A served view cut into at most `n` served windows of
    /// `ceil(rows / n)` of its live rows each, in its index's order, as
    /// [`served_window`](Self::served_window) cuts one, the tail with the
    /// last. A view whose base holds no live row is its own one partition.
    /// `None` for a view no plan serves.
    pub(in crate::store) async fn served_partitions(&self, n: usize) -> Result<Option<Vec<Self>>> {
        let Some(served) = Served::of(&self.quads).await? else {
            return Ok(None);
        };
        let live = served.live().await?;
        let rows = live.len();
        if rows == 0 {
            return Ok(Some(vec![self.clone()]));
        }
        let size = rows.div_ceil(n);
        let pieces: Vec<Range<usize>> = (0..rows)
            .step_by(size)
            .map(|start| start..(start + size).min(rows))
            .collect();
        let last = pieces.len() - 1;
        pieces
            .into_iter()
            .enumerate()
            .map(|(i, piece)| {
                let quads = served.narrowed(live.cut(piece))?;
                Ok(self.derived(quads, self.partition_tail(i == last)))
            })
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    /// [`keep`](Self::keep) of a served view, served too: the view's live
    /// rows tested in its index's order against `column`'s codes
    /// ([`Served::admitted`]), the admitted ones kept on the plan with their
    /// row ids unsorted. None admitted is the empty view. `None` for a view
    /// no plan serves, or an in-memory one whose column neither the base's
    /// code cache nor the component's probes read.
    pub(in crate::store::query) async fn served_keep(
        &self,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<Option<Self>> {
        let Some(served) = Served::of(&self.quads).await? else {
            return Ok(None);
        };
        if served.span.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        let test = keep.test(served.span.len());
        let Some(kept) = served
            .admitted(self, column, |code| test.admits(code))
            .await?
        else {
            return Ok(None);
        };
        if kept.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        let quads = served.narrowed(Cut::kept(kept))?;
        Ok(Some(self.derived(quads, self.tail.clone())))
    }

    /// `pattern` matched over the base of a view a narrowing left part of a
    /// served run (`served`): each bound role's probe value, as the copy's
    /// columns spell it (`codes`), compared with the copy's column at the
    /// view's live rows in the index's order (the component's columns in
    /// memory; on file the child's, point by point or over the rows they
    /// span), the matching rows kept on the plan with their row ids
    /// unsorted. A term the layout proves absent, or no row matching, gives
    /// the empty view; a pattern binding nothing, the view as it is. The tail
    /// is left as it is: [`match_pattern_for`](Self::match_pattern_for)
    /// matches it.
    pub(in crate::store::query) async fn served_chain(
        &self,
        served: Served<'_>,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
    ) -> Result<Self> {
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        for term in pattern.bound_roles() {
            let Some(value) = codes.probe_scalar(term)? else {
                return Ok(self.empty_view());
            };
            eqs.push((term.role().name(), value));
        }
        if eqs.is_empty() {
            return Ok(self.clone());
        }
        let rows = served.live_rows().await?;
        if rows.len() == 0 {
            return Ok(self.empty_view());
        }
        let names: Vec<&'static str> = eqs.iter().map(|(name, _)| *name).collect();
        let read = served.columns_at(&names, &rows.positions).await?;
        let holding = bool_array_to_mask(mask_for(&read, &eqs)?)?;
        let mut row = 0;
        let kept = ServedRows::collect(rows.positions_and_rids(), rows.len(), |_, _| {
            row += 1;
            holding.value(row - 1)
        });
        if kept.len() == 0 {
            return Ok(self.empty_view());
        }
        let quads = served.narrowed(Cut::kept(kept))?;
        Ok(self.derived(quads, self.tail.clone()))
    }
}
