//! Narrowings of a served view: a [`window`](VortexRdfStore::window),
//! [`partitions`](VortexRdfStore::partitions), a
//! [`keep`](VortexRdfStore::keep) or a pattern chained on a view a by-copy
//! index serves cut or filter its rows in the index's order ("index order,
//! filtered") and stay served, so the narrowed view reads off the index too.
//! Each is written once over [`Served`], the view's plan beside its base: the
//! one place a narrowing reads through a backend (the view's live rows, the
//! index's columns at them) and builds the narrowed view.

use vortex_array::ArrayRef;
use vortex_array::scalar::Scalar;
use vortex_mask::Mask;

use super::keep::Keep;
use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::array::batch_ranges;
use crate::store::array::bool_array_to_mask;
#[cfg(feature = "file-io")]
use crate::store::indexes::FileServePlan;
use crate::store::indexes::{InMemoryServePlan, LazyRowIds, LiveRows, ServedRows};
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::scan::residual::mask_for;
use crate::store::schema::{QuadColumn, TermCode};
use crate::store::view::selection::{RowSelection, ViewSelection, window_rows};
use crate::store::{MemoryBase, QuadsSource, VortexRdfStore};

/// A served view's plan beside its base: what every narrowing of the view
/// reads and builds through.
pub(in crate::store::query) struct Served<'a> {
    quads: &'a QuadsSource,
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

impl<'a> Served<'a> {
    /// The view `quads` selects, when a plan serves it; `None` for a view no
    /// plan serves. Nothing is read.
    pub(in crate::store::query) fn of(quads: &'a QuadsSource) -> Option<Self> {
        if let Some((served, _)) = Self::in_memory(quads) {
            return Some(served);
        }
        #[cfg(feature = "file-io")]
        if let Some((served, ..)) = Self::file(quads) {
            return Some(served);
        }
        None
    }

    /// [`of`](Self::of) an in-memory view, beside its plan.
    #[inline]
    fn in_memory(quads: &'a QuadsSource) -> Option<(Self, &'a InMemoryServePlan)> {
        let memory = quads.in_memory()?;
        let plan = memory.serve.as_ref()?;
        let backend = Backend::Memory { memory, plan };
        Some((Served { quads, backend }, plan))
    }

    /// [`of`](Self::of) a file view, beside its base and plan.
    #[cfg(feature = "file-io")]
    fn file(quads: &'a QuadsSource) -> Option<(Self, &'a FileBase, &'a FileServePlan)> {
        let base = quads.file()?;
        let plan = base.serve.as_ref()?;
        let backend = Backend::File { base, plan };
        Some((Served { quads, backend }, base, plan))
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
        let Some((served, base, plan)) = Self::file(quads) else {
            return Ok(None);
        };
        let live = plan.live(&base.file, served.deleted()).await?;
        Ok(Some((served, live)))
    }

    /// Whether a narrowing left the view part of its run (a window, keep,
    /// partition or chained match of a served view): what a pattern chained
    /// on the view filters in the index's order.
    pub(in crate::store::query) fn is_narrowed(&self) -> bool {
        match &self.backend {
            Backend::Memory { plan, .. } => plan.is_narrowed(),
            #[cfg(feature = "file-io")]
            Backend::File { plan, .. } => plan.is_narrowed(),
        }
    }

    fn deleted(&self) -> Option<&'a Mask> {
        self.quads.deleted.as_ref()
    }

    /// How many rows of the index the view covers, tombstoned ones in a
    /// range included: on file, an unlocated run located first, read once by
    /// row index ([`located_span`](FileServePlan::located_span)).
    async fn span_len(&self) -> Result<usize> {
        Ok(match &self.backend {
            Backend::Memory { plan, .. } => plan.span_len(),
            #[cfg(feature = "file-io")]
            Backend::File { plan, .. } => plan.located_span(self.deleted()).await?.len(),
        })
    }

    /// The view's live rows where `holds` holds of the index's `columns`
    /// there, in the index's order: the columns read once over the rows the
    /// view covers (the plans' `rows_where`). What a chained pattern keeps.
    async fn rows_where(
        &self,
        columns: &[&'static str],
        holds: impl FnOnce(&ArrayRef) -> Result<Mask>,
    ) -> Result<ServedRows> {
        match &self.backend {
            Backend::Memory { plan, .. } => plan.rows_where(self.deleted(), columns, holds),
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => {
                plan.rows_where(&base.file, self.deleted(), columns, holds)
                    .await
            }
        }
    }

    /// The view's live rows whose `column` code `admits`, in the index's
    /// order: what a keep keeps. In memory each row's code is read where it
    /// lies (the base's code cache at its row id, else the component's probe
    /// at its position); on file the column is read over the range, or at
    /// the kept rows. `None` for an in-memory column neither reads.
    async fn admitted(
        &self,
        column: QuadColumn,
        admits: impl Fn(TermCode) -> bool,
    ) -> Result<Option<ServedRows>> {
        let deleted = self.deleted();
        match &self.backend {
            Backend::Memory { memory, plan } => {
                if let Some(codes) = memory.base_code_primitive(column) {
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

    /// The view narrowed to `rows` (some of its live rows), served: its plan
    /// over the span they cover, its ids theirs in the index's order (a
    /// range's its plan's: a slice of the run's in memory, read on first need
    /// from those rows of the child's rid column on file), each live.
    // Inlined, so a narrowing of a view known to be in memory (its windows)
    // builds the view in place, the file arm left out.
    #[inline(always)]
    fn narrowed(&self, rows: LiveRows) -> Result<QuadsSource> {
        match &self.backend {
            Backend::Memory { memory, plan } => {
                let (span, ids) = rows.into_span(|range| {
                    Ok(LazyRowIds::from_unsorted_ids(plan.range_rids(range)?))
                })?;
                let plan = Some(plan.with_span(span));
                Ok(self
                    .quads
                    .in_memory_with(memory, ViewSelection::pending(ids), plan))
            }
            // Every narrowed row passes the view's filter, so its counts and
            // reads take the plan's rows, the filter's pinned codes kept for
            // reads by id.
            #[cfg(feature = "file-io")]
            Backend::File { base, plan } => {
                let (span, ids) = rows.into_span(|range| Ok(plan.range_ids(&base.file, range)))?;
                let (filter, plan) = (base.filter.clone(), Some(plan.with_span(span)));
                Ok(self
                    .quads
                    .file_with(base, filter, ViewSelection::pending(ids), plan)
                    .with_filter_resolved())
            }
        }
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
    /// ([`LazyRowIds::from_unsorted_ids`]); under tombstones the window counts
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
            .map(|rows| live.slice(rows.start as usize..rows.end as usize));
        let base_taken = cut.as_ref().map_or(0, LiveRows::len);
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
            let quads = served.narrowed(live.slice(piece))?;
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
        let Some(served) = Served::of(&self.quads) else {
            return Ok(None);
        };
        let rows = served.span_len().await?;
        if rows == 0 {
            return Ok(Some(self.empty_view()));
        }
        let test = keep.test(rows);
        let Some(kept) = served.admitted(column, |code| test.admits(code)).await? else {
            return Ok(None);
        };
        self.served_kept(&served, kept).map(Some)
    }

    /// `pattern` matched over the base of a view a narrowing left part of a
    /// served run (`served`): each bound role's probe value, as the copy's
    /// columns spell it (`codes`), compared with the copy's columns over the
    /// rows the view covers, read once ([`Served::rows_where`]: the
    /// component's in memory, the child's on file), the matching live rows
    /// kept on the plan with their row ids unsorted. A term the layout proves absent, or no row matching, gives
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
        let names: Vec<&'static str> = eqs.iter().map(|(name, _)| *name).collect();
        let kept = served
            .rows_where(&names, |read| bool_array_to_mask(mask_for(read, &eqs)?))
            .await?;
        self.served_kept(&served, kept)
    }

    /// The view `served` narrowed to the `kept` rows (a keep's or a chained
    /// match's), its tail as it is; the empty view when none was kept.
    fn served_kept(&self, served: &Served<'_>, kept: ServedRows) -> Result<Self> {
        if kept.len() == 0 {
            return Ok(self.empty_view());
        }
        let quads = served.narrowed(LiveRows::Kept(kept))?;
        Ok(self.derived(quads, self.tail.clone()))
    }
}
