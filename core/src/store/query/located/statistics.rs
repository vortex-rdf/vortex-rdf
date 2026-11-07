//! A located run's statistics: the file's footer bounds settle a checked
//! slot for the whole file, then the zone maps class the run's zones
//! against the slots left, asked once, when a read first needs them.

use super::plan::REFERENCES;
use super::run::*;
use crate::error::Result;
use crate::io::zone_map::ColumnZones;
use crate::io::zone_map::{ZONE_ROWS, eq_on_bounds, footer_bounds};
use crate::io::zone_set::{ZoneCheck, ZoneSet};
use crate::observe::observe;
use crate::store::persist::native_file::LocatedTables;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::schema::PRIMARY_COLUMNS;
use std::sync::Arc;
use vortex_rdf_encoded_search::counters;

/// Where a run asks its file's statistics when a read needs them
/// ([`LocatedRun::ask_statistics`]): the file, and the table whose zones
/// class the run's positions.
pub(super) struct PendingStatistics {
    pub(super) file: Arc<NativeStoreFile>,
    pub(super) target: StatisticsTarget,
}

/// The table whose zone maps decide a run's checks.
#[derive(Clone, Copy)]
pub(super) enum StatisticsTarget {
    /// Sorted table `table` (`component` names it, `None` the quad table).
    Table {
        table: usize,
        component: Option<&'static str>,
    },
    /// Reference index `index` of [`REFERENCES`]: its child's own `g`, the
    /// one check of a run of it decided by zones.
    Reference { index: usize },
}

impl LocatedRun {
    /// Asks the file's statistics for the run's checked slots
    /// ([`decide_by_statistics`]) if they are still to ask; once asked, or
    /// with none to ask, it does nothing.
    pub(super) async fn ask_statistics(&mut self) -> Result<()> {
        let Some(PendingStatistics { file, target }) = self.pending.take() else {
            return Ok(());
        };
        let tables = file
            .built_located_tables()
            .expect("a located run's file built its located tables");
        let source = self.source.clone();
        // Boxed: its state would otherwise enlarge every read's future.
        Box::pin(decide_by_statistics(&file, tables, target, self, &source)).await
    }
}

/// The statistics' decisions on `run`, located in `target` (a sorted
/// table's gap run, or a reference child's run checked against the graph),
/// in ladder order: the footer's exact bounds of each checked slot's column
/// settle it for the whole file ([`eq_on_bounds`]), leaving the run empty
/// where no row holds the slot's code and dropping a slot every row holds;
/// the zone maps then class the run's zones against the slots left
/// ([`run_zones`]). The quad table's footer speaks for an index child's
/// columns, which hold its values.
///
/// [`eq_on_bounds`]: crate::io::zone_map::eq_on_bounds
pub(super) async fn decide_by_statistics(
    file: &NativeStoreFile,
    tables: &LocatedTables,
    target: StatisticsTarget,
    run: &mut LocatedRun,
    source: &Source,
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
        run.zones = run_zones(file, tables, target, run, source).await?;
    }
    Ok(())
}

/// The zone checks of `run`'s checked slots, each named by its slot, over
/// the table `run` is located in ([`ZoneSet`]): a sorted table's checked
/// columns, or a reference child's own `g`. `None` when a checked column has
/// no zone map, for the run to check every row.
pub(super) async fn run_zones(
    file: &NativeStoreFile,
    tables: &LocatedTables,
    target: StatisticsTarget,
    run: &LocatedRun,
    source: &Source,
) -> Result<Option<ZoneSet>> {
    let mut checks = Vec::with_capacity(run.checked.len());
    for &slot in &run.checked {
        let zones = match target {
            StatisticsTarget::Table { table, component } => {
                column_zones(file, tables, table, component, slot, source).await?
            }
            StatisticsTarget::Reference { index } if slot == GRAPH_SLOT => {
                reference_zones(file, tables, index, source).await?
            }
            StatisticsTarget::Reference { .. } => None,
        };
        let Some(zones) = zones else {
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

/// The zone map of reference index `index`'s child's own `g`, with its
/// zones' recorded graphs, read on first use and kept with the file's
/// located tables; `None` for a child without one.
pub(super) async fn reference_zones(
    file: &NativeStoreFile,
    tables: &LocatedTables,
    index: usize,
    source: &Source,
) -> Result<Option<Arc<ColumnZones>>> {
    let cell = &tables.reference_zones[index];
    kept_zones(cell, file, Some(REFERENCES[index].0), "g", source).await
}

/// The zone map of column `slot` of sorted table `table` (`component` names
/// it, `None` the quad table), read on first use and kept with the file:
/// the quad table's in the file's own maps
/// ([`quad_zone_maps`](NativeStoreFile::quad_zone_maps)),
/// a copy index's with its located tables; `None` for a column without exact
/// zone bounds.
pub(super) async fn column_zones(
    file: &NativeStoreFile,
    tables: &LocatedTables,
    table: usize,
    component: Option<&str>,
    slot: usize,
    source: &Source,
) -> Result<Option<Arc<ColumnZones>>> {
    if component.is_none() {
        return file.quad_zone_maps().get(slot).await;
    }
    let cell = &tables.zones[table - 1][slot];
    kept_zones(cell, file, component, PRIMARY_COLUMNS[slot], source).await
}

/// The zone map of column `column` of the located table `component` names,
/// kept in `cell`: read on first use, `None` for a column without exact
/// zone bounds.
pub(super) async fn kept_zones(
    cell: &std::sync::OnceLock<Option<Arc<ColumnZones>>>,
    file: &NativeStoreFile,
    component: Option<&str>,
    column: &str,
    source: &Source,
) -> Result<Option<Arc<ColumnZones>>> {
    use ColumnZones;

    if let Some(zones) = cell.get() {
        return Ok(zones.clone());
    }
    observe!(ZoneMapRead);
    let zones = match file.table_column_layout(component, column) {
        Some(column) => ColumnZones::read(&column, source).await?.map(Arc::new),
        None => None,
    };
    Ok(cell.get_or_init(|| zones).clone())
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
