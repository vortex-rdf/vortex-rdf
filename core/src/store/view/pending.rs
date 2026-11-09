//! A view's selection left pending as a residual compare ([`PendingResidual`],
//! [`ViewSelection::Residual`]): in memory, the equalities no prefix search or
//! index answered, over a view built only to be counted or windowed short of
//! its rows (`IdsNeed::CountOrWindow`), compared only as far as a count or a
//! window needs ([`ResidualCompare`]). Where the base keeps zone maps, a count
//! or a window goes zone by zone, from the zones' counts where they settle
//! them; with zones off, or without zone maps, the selection is compared in
//! pieces, a zone's rows first and four times as many each after. The
//! bindings' ASK (`count_probe` at a limit of one) and `LIMIT` (`run_probe`
//! with a limit) over a pattern no index serves stop at the first zone or
//! piece that holds their rows, instead of comparing every row the pattern's
//! other roles leave.
//!
//! [`ViewSelection::Residual`]: super::selection::ViewSelection::Residual

use std::sync::OnceLock;

use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
use crate::observe::observe;
use crate::store::MemoryBase;
use crate::store::read::live::Live;
use crate::store::scan::residual::{Classes, ResidualCompare, ResidualTable};
use crate::store::schema::RowId;
use crate::store::view::selection::{RowSelection, SelectedRows, window_rows};

/// A residual compare left pending: the rows of `selection` (of `memory`'s
/// base) where every equality of `eqs` holds, walked as `classes` say
/// ([`ResidualCompare`]): whole by a consumer that needs every match
/// ([`materialized`](Self::materialized)), or part by part by a count or a
/// window, only as far as it needs ([`count_capped`](Self::count_capped),
/// [`window`](Self::window)). The matches, once every one is known, are
/// kept for every clone of the view that holds the compare.
pub(crate) struct PendingResidual {
    memory: MemoryBase,
    selection: RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
    classes: Classes,
    ids: OnceLock<Buffer<RowId>>,
}

impl PendingResidual {
    /// The compare of `eqs` over `selection`, of `memory`'s base, left
    /// pending, walked as `classes` say.
    pub(crate) fn new(
        memory: MemoryBase,
        selection: RowSelection,
        eqs: Vec<(&'static str, Scalar)>,
        classes: Classes,
    ) -> Self {
        observe!(ResidualDeferred);
        Self {
            memory,
            selection,
            eqs,
            classes,
            ids: OnceLock::new(),
        }
    }

    /// The walk of the compare, `deleted` tombstoned.
    fn compare<'a>(&'a self, deleted: Option<&'a Mask>) -> ResidualCompare<'a> {
        ResidualCompare {
            table: ResidualTable::base(&self.memory),
            selection: &self.selection,
            eqs: &self.eqs,
            classes: &self.classes,
            deleted,
            t: None,
        }
    }

    /// Whether a count or a window of `want` rows walks pieces that reach
    /// every selected row: it compares the whole selection at once and
    /// keeps the matches, as no piece could stop it short.
    fn reaches_every_row(&self, want: usize) -> bool {
        matches!(self.classes, Classes::Pieces { .. })
            && want >= self.selection.len(self.memory.base.len())
    }

    /// Every match for a count or a window that reaches every selected row
    /// ([`reaches_every_row`](Self::reaches_every_row)): the whole selection
    /// compared as one piece, unless the matches are kept.
    fn every_piece(&self) -> Result<Buffer<RowId>> {
        if self.ids.get().is_none() {
            observe!(ResidualPiece(self.selection.len(self.memory.base.len())));
        }
        self.materialized()
    }

    /// Every match: the whole selection compared at once on first need, the
    /// matches kept.
    pub(crate) fn materialized(&self) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.ids.get() {
            return Ok(ids.clone());
        }
        let matched = self.compare(None).rows()?;
        let ids = match matched.as_rows(self.memory.base.len()) {
            SelectedRows::Run(run) => (run.start as RowId..run.end as RowId).collect(),
            SelectedRows::Ids(ids) => ids.clone(),
        };
        Ok(self.ids.get_or_init(|| ids).clone())
    }

    /// How many live rows match (`deleted` tombstoned): by zones, with no
    /// match materialized; in pieces, every match compared once and kept.
    pub(crate) fn count(&self, deleted: Option<&Mask>) -> Result<usize> {
        if self.ids.get().is_none() && matches!(self.classes, Classes::Zoned(_)) {
            return self.compare(deleted).count(None);
        }
        Ok(count_live(&self.materialized()?, deleted))
    }

    /// How many rows match, once every one is known.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        self.ids.get().map(Buffer::len)
    }

    /// `min(live matches, limit)`, `deleted` tombstoned: walked part by part
    /// only until `limit` live rows are counted, or from the kept matches.
    pub(crate) fn count_capped(&self, limit: usize, deleted: Option<&Mask>) -> Result<usize> {
        if self.ids.get().is_none() && !self.reaches_every_row(limit) {
            return Ok(self.compare(deleted).count(Some(limit))?.min(limit));
        }
        Ok(count_live(&self.every_piece()?, deleted).min(limit))
    }

    /// The live rows `offset..offset + limit` of the matches (`deleted`
    /// tombstoned), in base row order, beside the base's live matches as far
    /// as they are known ([`Live`]): walked part by part only until the
    /// window's last row, or taken from the kept matches.
    pub(crate) fn window(
        &self,
        offset: usize,
        limit: usize,
        deleted: Option<&Mask>,
    ) -> Result<(Buffer<RowId>, Live)> {
        if self.ids.get().is_none() && !self.reaches_every_row(offset.saturating_add(limit)) {
            return self.compare(deleted).window(offset, limit);
        }
        let live = live_ids(&self.every_piece()?, deleted);
        let rows = window_rows(offset, limit, live.len() as u64)
            .map_or(0..0, |rows| rows.start as usize..rows.end as usize);
        Ok((live.slice(rows), Live::Exact(live.len())))
    }

    /// Test-only hook: whether every match has been computed, so a test can
    /// pin a read answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.ids.get().is_some()
    }
}

/// The live rows among `ids`, in order.
fn live_ids(ids: &Buffer<RowId>, deleted: Option<&Mask>) -> Buffer<RowId> {
    match deleted {
        None => ids.clone(),
        Some(deleted) => ids
            .iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize))
            .collect(),
    }
}

/// How many of `ids` are not tombstoned in `deleted`.
fn count_live(ids: &[RowId], deleted: Option<&Mask>) -> usize {
    match deleted {
        None => ids.len(),
        Some(deleted) => ids
            .iter()
            .filter(|&&id| !deleted.value(id as usize))
            .count(),
    }
}
