//! A view's selection left pending as a residual compare
//! ([`PendingResidual`], [`ViewSelection::Residual`]): in memory, the
//! equalities no prefix search or index answered, over a view built only to
//! be counted or windowed short of its rows (`IdsNeed::CountOrWindow`),
//! compared over the view's selection in pieces, a zone's rows first and four
//! times as many each after, only as far as a capped count or a window
//! needs. The bindings' ASK (`count_probe` at a limit of one) and `LIMIT`
//! (`run_probe` with a limit) over a pattern no index serves stop at the
//! first piece that holds their rows, instead of comparing every row the
//! pattern's other roles leave.
//!
//! [`ViewSelection::Residual`]: super::selection::ViewSelection::Residual

use std::ops::Range;
use std::sync::OnceLock;

use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
use crate::io::zone_map::ZONE_ROWS;
use crate::store::MemoryBase;
use crate::store::scan::residual::residual_rows;
use crate::store::schema::RowId;
use crate::store::view::selection::{RowSelection, SelectedRows, window_rows};

/// The rows of a pending compare's first piece: a zone's. It is above the
/// typed residual's 4,096-row cap, so every piece but a selection's last is
/// compared by the mask scan, never by the per-row probe reads the typed loop
/// makes over a probe-bound column (a store loaded from a file or from
/// bytes), which cost many times a mask scan's row.
pub(crate) const FIRST_PIECE_ROWS: usize = ZONE_ROWS;

/// Each piece after the first holds this many times the rows of the one
/// before: a count or a window whose rows come early stops after a piece of
/// about their size, and a compare that runs to the end takes a handful of
/// pieces.
const PIECE_GROWTH: usize = 4;

/// A residual compare left pending: the rows of `selection` (of `memory`'s
/// base) where every equality of `eqs` holds, compared as the eager compare
/// ([`residual_rows`]) compares them, whole by a consumer that needs every
/// match ([`materialized`](Self::materialized)), or piece by piece by a
/// count or a window, only as far as it needs
/// ([`count_capped`](Self::count_capped), [`window`](Self::window)). The
/// matches, once every piece has been compared, are kept for every clone of
/// the view that holds the compare.
pub(crate) struct PendingResidual {
    memory: MemoryBase,
    selection: RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
    ids: OnceLock<Buffer<RowId>>,
}

/// What the first pieces of a [`PendingResidual`] matched.
struct ResidualPrefix {
    /// The matching base rows of the pieces compared, ascending, tombstoned
    /// rows included.
    ids: Buffer<RowId>,
    /// How many of `ids` are live.
    live: usize,
    /// Whether every piece was compared, so that `ids` are every match.
    complete: bool,
}

impl ResidualPrefix {
    /// Every match, `ids`, their live rows counted under `deleted`.
    fn whole(ids: Buffer<RowId>, deleted: Option<&Mask>) -> Self {
        let live = count_live(&ids, deleted);
        Self {
            ids,
            live,
            complete: true,
        }
    }

    /// The live rows among the matches, in base row order.
    fn live_ids(&self, deleted: Option<&Mask>) -> Buffer<RowId> {
        match deleted {
            None => self.ids.clone(),
            Some(deleted) => self
                .ids
                .iter()
                .copied()
                .filter(|&id| !deleted.value(id as usize))
                .collect(),
        }
    }
}

impl PendingResidual {
    /// The compare of `eqs` over `selection`, of `memory`'s base, left
    /// pending.
    pub(crate) fn new(
        memory: MemoryBase,
        selection: RowSelection,
        eqs: Vec<(&'static str, Scalar)>,
    ) -> Self {
        #[cfg(test)]
        residual_hooks::note_deferred();
        Self {
            memory,
            selection,
            eqs,
            ids: OnceLock::new(),
        }
    }

    /// Every match: the whole selection compared at once on first need, the
    /// matches kept.
    pub(crate) fn materialized(&self) -> Result<Buffer<RowId>> {
        if let Some(ids) = self.ids.get() {
            return Ok(ids.clone());
        }
        let matched = residual_rows(&self.memory, self.selection.clone(), &self.eqs, None)?;
        let ids = match matched.as_rows(self.memory.base.len()) {
            SelectedRows::Run(run) => (run.start as RowId..run.end as RowId).collect(),
            SelectedRows::Ids(ids) => ids.clone(),
        };
        Ok(self.ids.get_or_init(|| ids).clone())
    }

    /// How many rows match, once every piece has been compared.
    pub(crate) fn len_if_known(&self) -> Option<usize> {
        self.ids.get().map(Buffer::len)
    }

    /// `min(live matches, limit)`, `deleted` tombstoned, the pieces compared
    /// only until `limit` live rows are found.
    pub(crate) fn count_capped(&self, limit: usize, deleted: Option<&Mask>) -> Result<usize> {
        Ok(self.prefix(limit, deleted)?.live.min(limit))
    }

    /// The live rows `offset..offset + limit` of the matches (`deleted`
    /// tombstoned), in base row order, the pieces compared only until the
    /// window's last row is found, beside the base's live rows as far as
    /// they are known: every one once every piece was compared, else the
    /// ones found, at least `offset + limit`.
    pub(crate) fn window(
        &self,
        offset: usize,
        limit: usize,
        deleted: Option<&Mask>,
    ) -> Result<(Buffer<RowId>, usize)> {
        let prefix = self.prefix(offset.saturating_add(limit), deleted)?;
        let live = prefix.live_ids(deleted);
        let rows = window_rows(offset, limit, live.len() as u64)
            .map_or(0..0, |rows| rows.start as usize..rows.end as usize);
        Ok((live.slice(rows), live.len()))
    }

    /// Test-only hook: whether every match has been computed, so a test can
    /// pin a read answered without them.
    #[cfg(test)]
    pub(crate) fn debug_materialized(&self) -> bool {
        self.ids.get().is_some()
    }

    /// What the first pieces match, at least `want` live rows under
    /// `deleted` when there are as many ([`compare_prefix`](Self::compare_prefix));
    /// every match once they are known, as a prefix that compared every
    /// piece leaves them.
    fn prefix(&self, want: usize, deleted: Option<&Mask>) -> Result<ResidualPrefix> {
        if let Some(ids) = self.ids.get() {
            return Ok(ResidualPrefix::whole(ids.clone(), deleted));
        }
        let prefix = self.compare_prefix(want, deleted)?;
        if prefix.complete {
            self.ids.get_or_init(|| prefix.ids.clone());
        }
        Ok(prefix)
    }

    /// The matches of the selection's first pieces, compared in base row
    /// order until at least `want` of them are live (not tombstoned in
    /// `deleted`) or the selection ends: [`FIRST_PIECE_ROWS`] rows first,
    /// [`PIECE_GROWTH`] times as many each piece after, and the whole
    /// selection as one piece when it holds no more than `want` rows, as no
    /// count or window of `want` rows could stop short of it.
    fn compare_prefix(&self, want: usize, deleted: Option<&Mask>) -> Result<ResidualPrefix> {
        let base_len = self.memory.base.len();
        let rows = self.selection.as_rows(base_len);
        let total = self.selection.len(base_len);
        let mut ids: Vec<RowId> = Vec::new();
        let mut live = 0;
        let mut start = 0;
        let mut piece = if total <= want {
            total
        } else {
            FIRST_PIECE_ROWS
        };
        while start < total && live < want {
            let end = start.saturating_add(piece).min(total);
            #[cfg(test)]
            residual_hooks::note_piece(end - start);
            let matched = residual_rows(&self.memory, rows_at(&rows, start..end), &self.eqs, None)?;
            let found = ids.len();
            match matched.as_rows(base_len) {
                SelectedRows::Run(run) => ids.extend(run.start as RowId..run.end as RowId),
                SelectedRows::Ids(matches) => ids.extend_from_slice(matches),
            }
            live += count_live(&ids[found..], deleted);
            start = end;
            piece = piece.saturating_mul(PIECE_GROWTH);
        }
        Ok(ResidualPrefix {
            ids: Buffer::from(ids),
            live,
            complete: start == total,
        })
    }
}

/// The selected rows at `positions`, their places in the selection's order,
/// as a selection of their own.
fn rows_at(rows: &SelectedRows<'_>, positions: Range<usize>) -> RowSelection {
    match rows {
        SelectedRows::Run(run) => RowSelection::Range(
            (run.start + positions.start) as u64..(run.start + positions.end) as u64,
        ),
        SelectedRows::Ids(ids) => RowSelection::Ids(ids.slice(positions)),
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

/// Test hook: the compares this thread left pending, and the rows of each
/// piece it compared, in order.
#[cfg(test)]
pub(crate) mod residual_hooks {
    use std::cell::{Cell, RefCell};

    thread_local! {
        static DEFERRED: Cell<usize> = const { Cell::new(0) };
        static PIECES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    }

    /// Record one compare left pending.
    pub(super) fn note_deferred() {
        DEFERRED.set(DEFERRED.get() + 1);
    }

    /// Record one piece of `rows` rows compared.
    pub(super) fn note_piece(rows: usize) {
        PIECES.with_borrow_mut(|pieces| pieces.push(rows));
    }

    /// The compares left pending on this thread so far.
    pub(crate) fn deferred() -> usize {
        DEFERRED.get()
    }

    /// The rows of each piece compared on this thread since the last call,
    /// in order.
    pub(crate) fn take_pieces() -> Vec<usize> {
        PIECES.with_borrow_mut(std::mem::take)
    }
}
