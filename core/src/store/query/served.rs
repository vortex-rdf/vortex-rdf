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
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::array::bool_array_to_mask;
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
use crate::store::indexes::{InMemoryServePlan, LazyRowIds, LiveRows, ServedRows, ServedSpan};
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::read::chunks::batch_ranges;
use crate::store::scan::residual::mask_for;
use crate::store::schema::{QuadColumn, RowId, TermCode};
use crate::store::view::selection::{RowSelection, ViewSelection, window_rows};
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

/// What a window or partition (or a keep or chained match) leaves of a
/// served view's rows: the span its plan covers, beside the row ids the
/// narrowed view holds, in the index's order.
enum Cut {
    /// A range with nothing tombstoned, its ids its plan's (a slice of the
    /// run's in memory; on file read off the child on first need).
    Range(Range<u64>),
    /// Listed live rows: the range a tombstoned range's cut spans, or kept
    /// rows, beside their ids.
    Listed {
        span: ServedSpan,
        rids: Buffer<RowId>,
    },
}

impl Cut {
    /// Rows `piece` (not empty) of the `live` rows, in their order: a part
    /// of the range; under tombstones the range from the first of the piece's
    /// live rows to its last (the tombstoned rows inside dropped at read
    /// through their row ids); or a slice of the kept rows.
    #[inline]
    fn of(live: &LiveRows, piece: Range<usize>) -> Self {
        debug_assert!(!piece.is_empty());
        match live {
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

    /// Kept rows, each live.
    #[inline]
    fn kept(rows: ServedRows) -> Self {
        let rids = rows.rids.clone();
        Cut::Listed {
            span: ServedSpan::Kept(Arc::new(rows)),
            rids,
        }
    }

    /// How many base rows the narrowed view holds.
    #[inline]
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
        if let Some((served, _)) = Self::in_memory(quads) {
            return Ok(Some(served));
        }
        #[cfg(feature = "file-io")]
        return Self::file(quads).await;
        #[cfg(not(feature = "file-io"))]
        Ok(None)
    }

    /// [`of`](Self::of) an in-memory view, which reads nothing, beside its
    /// plan.
    #[inline]
    fn in_memory(quads: &'a QuadsSource) -> Option<(Self, &'a InMemoryServePlan)> {
        let memory = quads.in_memory()?;
        let plan = memory.serve.as_ref()?;
        let served = Served {
            quads,
            span: plan.span(),
            backend: Backend::Memory { memory, plan },
        };
        Some((served, plan))
    }

    /// [`of`](Self::of) a file view.
    #[cfg(feature = "file-io")]
    async fn file(quads: &'a QuadsSource) -> Result<Option<Self>> {
        let Some(base) = quads.file() else {
            return Ok(None);
        };
        let Some(plan) = &base.serve else {
            return Ok(None);
        };
        let span = plan.located_span(quads.deleted.as_ref()).await?;
        Ok(Some(Served {
            quads,
            span,
            backend: Backend::File { base, plan },
        }))
    }

    /// An in-memory served view beside its live rows, known at once: what
    /// its windows and partitions cut, with nothing awaited.
    #[inline]
    fn in_memory_live(quads: &'a QuadsSource) -> Result<Option<(Self, LiveRows)>> {
        let Some((served, plan)) = Self::in_memory(quads) else {
            return Ok(None);
        };
        let live = plan.live(served.deleted())?;
        Ok(Some((served, live)))
    }

    /// A file served view beside its live rows: its run located, and a
    /// tombstoned range's rid column read.
    #[cfg(feature = "file-io")]
    async fn file_live(quads: &'a QuadsSource) -> Result<Option<(Self, LiveRows)>> {
        let Some(served) = Self::file(quads).await? else {
            return Ok(None);
        };
        let live = served.live().await?;
        Ok(Some((served, live)))
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

    /// The view's live rows as its plan lists them ([`LiveRows`]): what its
    /// windows and partitions cut.
    #[cfg(feature = "file-io")]
    async fn live(&self) -> Result<LiveRows> {
        match &self.backend {
            Backend::Memory { plan, .. } => plan.live(self.deleted()),
            Backend::File { base, plan } => plan.live(&base.file, self.deleted()).await,
        }
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
            Backend::File { base, plan } => plan
                .kept_rows(&base.file, deleted, column.name(), admits)
                .await
                .map(Some),
        }
    }

    /// The view narrowed to `cut`, served: its plan over the cut's span, its
    /// ids the cut's in the index's order, which leave out every tombstone
    /// (a narrowing takes live rows).
    // Inlined, so a narrowing of a view known to be in memory (its windows)
    // builds the view in place, the file arm left out.
    #[inline(always)]
    fn narrowed(&self, cut: Cut) -> Result<QuadsSource> {
        match &self.backend {
            Backend::Memory { memory, plan } => self.narrowed_in_memory(memory, plan, cut),
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => Ok(self.narrowed_file(base, plan, cut)),
        }
    }

    /// [`narrowed`](Self::narrowed) in memory: a range's ids a slice of the
    /// run's.
    #[inline(always)]
    fn narrowed_in_memory(
        &self,
        memory: &MemoryBase,
        plan: &InMemoryServePlan,
        cut: Cut,
    ) -> Result<QuadsSource> {
        let (span, rids) = match cut {
            Cut::Range(range) => {
                let rids = plan.range_rids(range.clone())?;
                (ServedSpan::Range(range), rids)
            }
            Cut::Listed { span, rids } => (span, rids),
        };
        let ids = LazyRowIds::from_unsorted_ids(rids, true);
        Ok(self.quads.in_memory_with(
            memory,
            ViewSelection::pending(ids),
            Some(plan.with_span(span)),
        ))
    }

    /// [`narrowed`](Self::narrowed) on file: a range's ids read on first
    /// need off those rows of the child's rid column. Every narrowed row
    /// passes the view's filter, so its counts and reads take the plan's
    /// rows, the filter's pinned codes kept for reads by id.
    #[cfg(feature = "file-io")]
    fn narrowed_file(&self, base: &FileBase, plan: &FileServePlan, cut: Cut) -> QuadsSource {
        let (span, ids) = match cut {
            Cut::Range(range) => {
                let ids = plan.range_ids(&base.file, range.clone());
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
}

impl VortexRdfStore {
    /// The window of a view an in-memory plan serves, itself served, as
    /// [`served_window_of`](Self::served_window_of) cuts it; nothing is
    /// awaited. `None` for a view no in-memory plan serves.
    #[inline]
    pub(in crate::store) fn served_window_in_memory(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let Some((served, live)) = Served::in_memory_live(&self.quads)? else {
            return Ok(None);
        };
        self.served_window_of(&served, &live, offset, limit)
            .map(Some)
    }

    /// The window of a view a file plan serves, itself served, as
    /// [`served_window_of`](Self::served_window_of) cuts it, an unlocated
    /// run located first. `None` for a view no file plan serves.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn served_window_file(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Self>> {
        let Some((served, live)) = Served::file_live(&self.quads).await? else {
            return Ok(None);
        };
        self.served_window_of(&served, &live, offset, limit)
            .map(Some)
    }

    /// The window of a served view (`served`, its `live` rows), itself
    /// served: rows `offset..offset + limit` of its live rows in its index's
    /// order, a part of its range (whose ids a slice of the run's in memory,
    /// read on first need from those rows of the child's rid column on file)
    /// or a slice of its kept rows, their ids kept unsorted
    /// ([`LazyRowIds::from_unsorted_ids`]); under tombstones the cut counts
    /// live rows. The tail is windowed with what the base did not fill, as
    /// [`window_cutting`](Self::window_cutting) windows it. A window that
    /// holds no base row selects none, with no plan.
    // Inlined into both shells, so the in-memory one builds its view in
    // place: the hottest narrowing, on views of a few rows.
    #[inline(always)]
    fn served_window_of(
        &self,
        served: &Served<'_>,
        live: &LiveRows,
        offset: usize,
        limit: usize,
    ) -> Result<Self> {
        let base_live = live.len();
        let cut = window_rows(offset, limit, base_live as u64)
            .map(|rows| Cut::of(live, rows.start as usize..rows.end as usize));
        let base_taken = cut.as_ref().map_or(0, Cut::len);
        let tail = self.windowed_tail(offset, limit, base_live, base_taken);
        let quads = match cut {
            Some(cut) => served.narrowed(cut)?,
            None => self
                .quads
                .with_selection(ViewSelection::Exact(RowSelection::empty())),
        };
        Ok(self.derived(quads, tail))
    }

    /// A view an in-memory plan serves cut into served windows, as
    /// [`served_partitions_of`](Self::served_partitions_of) cuts it; nothing
    /// is awaited. `None` for a view no in-memory plan serves, or with no
    /// live row.
    pub(in crate::store) fn served_partitions_in_memory(
        &self,
        n: usize,
    ) -> Result<Option<Vec<Self>>> {
        let Some((served, live)) = Served::in_memory_live(&self.quads)? else {
            return Ok(None);
        };
        self.served_partitions_of(&served, &live, n)
    }

    /// A view a file plan serves cut into served windows, as
    /// [`served_partitions_of`](Self::served_partitions_of) cuts it, an
    /// unlocated run located first. `None` for a view no file plan serves,
    /// or with no live row.
    #[cfg(feature = "file-io")]
    pub(in crate::store) async fn served_partitions_file(
        &self,
        n: usize,
    ) -> Result<Option<Vec<Self>>> {
        let Some((served, live)) = Served::file_live(&self.quads).await? else {
            return Ok(None);
        };
        self.served_partitions_of(&served, &live, n)
    }

    /// A served view (`served`, its `live` rows) cut into at most `n` served
    /// windows of `ceil(rows / n)` of its live rows each, in its index's
    /// order, as [`served_window_of`](Self::served_window_of) cuts one, the
    /// tail with the last. `None` for a view whose base holds no live row,
    /// which is cut as an unserved view is: there are no live rows to cut in
    /// the index's order.
    fn served_partitions_of(
        &self,
        served: &Served<'_>,
        live: &LiveRows,
        n: usize,
    ) -> Result<Option<Vec<Self>>> {
        let rows = live.len();
        if rows == 0 {
            return Ok(None);
        }
        let size = rows.div_ceil(n);
        let count = rows.div_ceil(size);
        let mut parts = Vec::with_capacity(count);
        for (i, piece) in batch_ranges(rows, size).enumerate() {
            let quads = served.narrowed(Cut::of(live, piece))?;
            parts.push(self.derived(quads, self.partition_tail(i + 1 == count)));
        }
        Ok(Some(parts))
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
        let holding = rows
            .positions_and_rids()
            .zip(0..)
            .filter_map(|(row, i)| holding.value(i).then_some(row));
        let kept = ServedRows::collect(holding, rows.len());
        if kept.len() == 0 {
            return Ok(self.empty_view());
        }
        let quads = served.narrowed(Cut::kept(kept))?;
        Ok(self.derived(quads, self.tail.clone()))
    }
}
