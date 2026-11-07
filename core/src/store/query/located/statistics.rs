//! A located run's statistics: the file's footer bounds settle a checked
//! slot for the whole file, then the zone maps class the run's zones
//! against the slots left, asked once, when a read first needs them.

use super::run::*;
use crate::error::Result;
use crate::io::zone_map::{ZONE_ROWS, eq_on_bounds, footer_bounds};
use crate::io::zone_set::{ZoneCheck, ZoneSet};
use crate::observe::observe;
use crate::store::persist::native_file::{FileTable, NativeStoreFile};
use crate::store::schema::PRIMARY_COLUMNS;
use std::sync::Arc;
use vortex_rdf_encoded_search::counters;

/// Where a run asks its file's statistics when a read needs them
/// ([`LocatedRun::ask_statistics`]): the file, and the table whose zones
/// class the run's positions (a sorted table, or a reference child, whose
/// own `g` is the one column it holds a zone map of).
pub(super) struct PendingStatistics {
    pub(super) file: Arc<NativeStoreFile>,
    pub(super) table: FileTable,
}

impl LocatedRun {
    /// Asks the file's statistics for the run's checked slots
    /// ([`decide_by_statistics`]) if they are still to ask; once asked, or
    /// with none to ask, it does nothing.
    pub(super) async fn ask_statistics(&mut self) -> Result<()> {
        let Some(PendingStatistics { file, table }) = self.pending.take() else {
            return Ok(());
        };
        // Boxed: its state would otherwise enlarge every read's future.
        Box::pin(decide_by_statistics(&file, table, self)).await
    }
}

/// The statistics' decisions on `run`, located in `table` (a sorted table's
/// gap run, or a reference child's run checked against the graph), in
/// ladder order: the footer's exact bounds of each checked slot's column
/// settle it for the whole file ([`eq_on_bounds`]), leaving the run empty
/// where no row holds the slot's code and dropping a slot every row holds;
/// the zone maps then class the run's zones against the slots left
/// ([`run_zones`]). The quad table's footer speaks for an index child's
/// columns, which hold its values.
pub(super) async fn decide_by_statistics(
    file: &NativeStoreFile,
    table: FileTable,
    run: &mut LocatedRun,
) -> Result<()> {
    observe!(StatisticsAsked);
    let rows = run.positions();
    let mut unsettled = Vec::new();
    for &slot in &run.checked {
        let code = run.codes[slot].expect("a checked slot is bound");
        let footer = footer_bounds(file, PRIMARY_COLUMNS[slot])
            .filter(|_| file.row_count() > 0)
            .and_then(|(min, max)| eq_on_bounds(min, max, code));
        match footer {
            Some(false) => {
                note(counters::ZONE_COLUMN, rows.end - rows.start);
                run.clear_rows();
                run.checked.clear();
                return Ok(());
            }
            Some(true) => note(counters::ZONE_COLUMN, rows.end - rows.start),
            None => unsettled.push(slot),
        }
    }
    run.checked = unsettled;
    if !run.checked.is_empty() {
        run.zones = run_zones(file, table, run).await?;
    }
    Ok(())
}

/// The zone checks of `run`'s checked slots, each named by its slot, over
/// `table`, the table `run`'s positions are in ([`ZoneSet`]): its checked
/// columns' zone maps, read once per file ([`ZoneMaps`]). `None` when a
/// checked column has no zone map (a reference child keeps `g`'s alone), for
/// the run to check every row.
///
/// [`ZoneMaps`]: crate::store::persist::native_file::ZoneMaps
pub(super) async fn run_zones(
    file: &NativeStoreFile,
    table: FileTable,
    run: &LocatedRun,
) -> Result<Option<ZoneSet>> {
    let maps = file.zone_maps();
    let mut checks = Vec::with_capacity(run.checked.len());
    for &slot in &run.checked {
        let Some(zones) = maps.get(table, slot).await? else {
            return Ok(None);
        };
        checks.push(ZoneCheck {
            bit: slot as u32,
            code: run.codes[slot].expect("a checked slot is bound"),
            zones,
        });
    }
    Ok(ZoneSet::new(checks, 0))
}

/// The longest gap run checked row by row: [`GAP_ROWS`], or what a test
/// forced on this thread.
pub(super) fn gap_rows() -> u64 {
    #[cfg(test)]
    if let Some(rows) = crate::store::test_hooks::forced_gap_rows() {
        return rows;
    }
    GAP_ROWS
}

/// Rows of a run whose rows are checked row by row (a prefix run with bound
/// slots past it that the statistics do not decide, or a reference run with
/// other slots bound) up to which it answers; a longer one declines, for the
/// caller's other paths, as a window of it checks every row to count them.
/// A run the zone maps decide is not capped.
pub(super) const GAP_ROWS: u64 = 1 << 23;

/// The fewest rows of a gap run for it to ask the file's statistics before
/// its rows (a zone's): a shorter run reads its rows faster than the footer
/// and the zone maps decide them, about 1 against 45 µs for a one-row probe.
pub(super) const STATISTICS_ROWS: u64 = ZONE_ROWS as u64;
