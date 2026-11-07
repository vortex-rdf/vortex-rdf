//! A located run's statistics: the file's footer bounds settle a checked
//! slot for the whole file, then the zone maps class the run's zones
//! against the slots left, asked once, when a read first needs them.

use super::run::{Check, LocatedRun};
use crate::counters::{self, note};
use crate::error::Result;
use crate::observe::observe;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::schema::PRIMARY_COLUMNS;
use crate::zone_stats::rules::zones_enabled;
use crate::zone_stats::zone_map::{ZONE_ROWS, eq_on_bounds, footer_bounds};
use crate::zone_stats::zone_set::{ZoneCheck, ZoneSet};
use std::sync::Arc;

/// What a located run knows of its file's statistics.
pub(super) enum Statistics {
    /// To ask, on the first read that needs them: the run's file, whose
    /// footer and zone maps answer for the run's table.
    Unasked(Arc<NativeStoreFile>),
    /// Asked: the zones class the run's positional checks.
    Zoned(ZoneSet),
    /// Nothing left to class: asked, with no zones left (the footer settled
    /// the checks, or a checked column has no zone map), or never to ask (a
    /// run with no check or one by row id, zones off, or fewer positions
    /// than a zone holds).
    Settled,
}

/// `run`, if it answers its pattern, its statistics set. A run whose checks
/// the zones may decide (at least one, and every one positional) of a
/// zone's positions or more asks them with zones on: when a read needs them,
/// so a first page its first block fills asks none, or at once when it is
/// longer than [`gap_rows`], as whether it answers depends on what they
/// decide. A shorter run reads its few rows faster than they answer (a
/// join's one-row probe: 1 against 45 µs). A run answers with no check, its
/// checks decided by zones, or within [`gap_rows`]; a longer one, still
/// checked row by row, declines (`None`), as without zones.
pub(super) async fn admit(
    mut run: LocatedRun,
    file: &Arc<NativeStoreFile>,
) -> Result<Option<LocatedRun>> {
    let decidable = !run.checks.is_empty() && run.checks.iter().all(Check::is_positional);
    if decidable && zones_enabled() && run.len() >= STATISTICS_ROWS {
        run.statistics = Statistics::Unasked(Arc::clone(file));
        if run.len() > gap_rows() {
            run.ask_statistics().await?;
        }
    }
    let answers = run.checks.is_empty() || run.zones().is_some() || run.len() <= gap_rows();
    Ok(answers.then_some(run))
}

impl LocatedRun {
    /// Asks the file's statistics for the run's checks
    /// ([`decide_by_statistics`]) if they are still to ask; once asked, or
    /// with none to ask, it does nothing.
    pub(super) async fn ask_statistics(&mut self) -> Result<()> {
        let file = match &self.statistics {
            Statistics::Unasked(file) => Arc::clone(file),
            Statistics::Zoned(_) | Statistics::Settled => return Ok(()),
        };
        self.statistics = Statistics::Settled;
        // Boxed: its state would otherwise enlarge every read's future.
        Box::pin(decide_by_statistics(&file, self)).await
    }

    /// The zones that class the run's positional checks, once asked.
    pub(super) fn zones(&self) -> Option<&ZoneSet> {
        match &self.statistics {
            Statistics::Zoned(zones) => Some(zones),
            Statistics::Unasked(_) | Statistics::Settled => None,
        }
    }
}

/// The statistics' decisions on `run` (a sorted table's gap run, or a
/// reference child's run checked against the graph), in ladder order: the
/// footer's exact bounds of each check's column settle it for the whole
/// file ([`eq_on_bounds`]), leaving the run empty where no row holds the
/// check's code and dropping a check every row holds; the zone maps then
/// class the run's zones against the checks left ([`run_zones`]). The quad
/// table's footer speaks for an index child's columns, which hold its
/// values.
pub(super) async fn decide_by_statistics(
    file: &NativeStoreFile,
    run: &mut LocatedRun,
) -> Result<()> {
    observe!(StatisticsAsked);
    let rows = run.positions();
    let mut settled = Vec::new();
    for (at, check) in run.checks.iter().enumerate() {
        let footer = footer_bounds(file, PRIMARY_COLUMNS[check.slot])
            .filter(|_| file.row_count() > 0)
            .and_then(|(min, max)| eq_on_bounds(min, max, check.code));
        match footer {
            Some(false) => {
                note(counters::ZONE_COLUMN, rows.end - rows.start);
                run.clear_rows();
                run.checks.clear();
                return Ok(());
            }
            Some(true) => {
                note(counters::ZONE_COLUMN, rows.end - rows.start);
                settled.push(at);
            }
            None => {}
        }
    }
    for at in settled.into_iter().rev() {
        run.checks.remove(at);
    }
    if !run.checks.is_empty()
        && let Some(zones) = run_zones(file, run).await?
    {
        run.statistics = Statistics::Zoned(zones);
    }
    Ok(())
}

/// The zone checks of `run`'s checks, each named by its slot, over the
/// run's table ([`ZoneSet`]): its positional checks' columns' zone maps,
/// read once per file ([`ZoneMaps`]). `None` when a check reads by row id
/// or a checked column has no zone map, for the run to check every row.
///
/// [`ZoneMaps`]: crate::zone_stats::file_maps::ZoneMaps
pub(super) async fn run_zones(file: &NativeStoreFile, run: &LocatedRun) -> Result<Option<ZoneSet>> {
    let (maps, table) = (file.zone_maps(), run.table.file_table());
    let mut checks = Vec::with_capacity(run.checks.len());
    for check in &run.checks {
        if !check.is_positional() {
            return Ok(None);
        }
        let Some(zones) = maps.get(table, check.slot).await? else {
            return Ok(None);
        };
        checks.push(ZoneCheck {
            bit: check.slot as u32,
            code: check.code,
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
