//! A residual compare left pending ([`PendingResidual`]): in memory, the
//! equalities no prefix search or index answered, over a view built only to
//! be counted or windowed short of its rows (`IdsNeed::CountOrWindow`),
//! compared over the view's selection in pieces, a zone's rows first and four
//! times as many each after, only as far as a capped count or a window
//! needs. The bindings' ASK (`count_probe` at a limit of one) and `LIMIT`
//! (`run_probe` with a limit) over a pattern no index serves stop at the
//! first piece that holds their rows, instead of comparing every row the
//! pattern's other roles leave.

use std::ops::Range;

use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
use crate::io::zone_map::ZONE_ROWS;
use crate::store::schema::RowId;
use crate::store::view::selection::{RowSelection, SelectedRows};
use crate::store::{IdsNeed, MemoryBase, VortexRdfStore};

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
/// ([`VortexRdfStore::residual_rows`]) compares them, whole by a consumer
/// that needs every match ([`all_ids`](Self::all_ids)), or piece by piece by
/// a count or a window, only as far as it needs ([`prefix`](Self::prefix)).
pub(crate) struct PendingResidual {
    memory: MemoryBase,
    selection: RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
}

/// What the first pieces of a [`PendingResidual`] matched.
pub(crate) struct ResidualPrefix {
    /// The matching base rows of the pieces compared, ascending, tombstoned
    /// rows included.
    pub(crate) ids: Buffer<RowId>,
    /// How many of `ids` are live.
    pub(crate) live: usize,
    /// Whether every piece was compared, so that `ids` are every match.
    pub(crate) complete: bool,
}

impl ResidualPrefix {
    /// Every match, `ids`, their live rows counted under `deleted`.
    pub(crate) fn whole(ids: Buffer<RowId>, deleted: Option<&Mask>) -> Self {
        let live = count_live(&ids, deleted);
        Self {
            ids,
            live,
            complete: true,
        }
    }

    /// The live rows among the matches, in base row order.
    pub(crate) fn live_ids(&self, deleted: Option<&Mask>) -> Buffer<RowId> {
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
    /// Whether a compare over `rows` selected rows is left pending for a
    /// caller whose use of the view is `need`: only a count or a window that
    /// needs fewer rows than the selection holds can stop early, and only
    /// over more rows than the first piece holds.
    pub(crate) fn defers(need: IdsNeed, rows: usize) -> bool {
        matches!(need, IdsNeed::CountOrWindow { reach } if reach < rows) && rows > FIRST_PIECE_ROWS
    }

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
        }
    }

    /// Every match, the whole selection compared at once.
    pub(crate) fn all_ids(&self) -> Result<Buffer<RowId>> {
        let matched =
            VortexRdfStore::residual_rows(&self.memory, self.selection.clone(), &self.eqs, None)?;
        Ok(match matched.as_rows(self.memory.base.len()) {
            SelectedRows::Run(run) => (run.start as RowId..run.end as RowId).collect(),
            SelectedRows::Ids(ids) => ids.clone(),
        })
    }

    /// The matches of the selection's first pieces, compared in base row
    /// order until at least `want` of them are live (not tombstoned in
    /// `deleted`) or the selection ends: [`FIRST_PIECE_ROWS`] rows first,
    /// [`PIECE_GROWTH`] times as many each piece after, and the whole
    /// selection as one piece when it holds no more than `want` rows, as no
    /// count or window of `want` rows could stop short of it.
    pub(crate) fn prefix(&self, want: usize, deleted: Option<&Mask>) -> Result<ResidualPrefix> {
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
            let matched = VortexRdfStore::residual_rows(
                &self.memory,
                rows_at(&rows, start..end),
                &self.eqs,
                None,
            )?;
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
