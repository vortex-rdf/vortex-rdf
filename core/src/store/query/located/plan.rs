//! Choosing the located run that answers a pattern of codes: an exact run
//! of a sorted table's key prefix, the run of its longest bound prefix (the
//! gap run) checked against the slots past it, or a bound `p`'s or `o`'s
//! run in its reference index, whichever is shorter and answers.

use super::run::*;
use super::statistics::*;
use super::{CodePattern, locate_enabled};
use crate::error::Result;
use crate::observe::observe;
use crate::stats_rules::zones_enabled;
use crate::store::VortexRdfStore;
use crate::store::persist::native_file::{FileTable, LocatedTables};
use crate::store::schema::PRIMARY_COLUMNS;
use crate::store::schema::TermCode;
use std::ops::Range;
use std::sync::Arc;
use vortex_session::VortexSession;

/// The sorted tables whose key prefix a pattern's bound slots may be: the
/// quad table and the copy indexes, each with its key order as
/// `(s, p, o, g)` positions.
const TABLES: [(FileTable, [usize; 4]); 3] = [
    (FileTable::Quads, [0, 1, 2, 3]),
    (FileTable::Posg, [1, 2, 0, 3]),
    (FileTable::Ospg, [2, 0, 1, 3]),
];

impl VortexRdfStore {
    /// The located run answering `pattern` ([`locate_codes`](Self::locate_codes)
    /// says when one does), found by binary search; `None` when none does.
    pub(super) async fn located_run(&self, pattern: CodePattern) -> Result<Option<LocatedRun>> {
        if !locate_enabled() {
            return Ok(None);
        }
        if !self.is_code_view() || self.quads.deleted.is_some() || !self.quads.selection.is_all() {
            return Ok(None);
        }
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        if base.filter.over_selection().is_some() || base.serve.is_some() {
            return Ok(None);
        }
        let file = &base.file;
        let (source, session) = (file.segment_source(), file.session());
        let codes = pattern.slots();
        let bound = codes.iter().filter(|c| c.is_some()).count();
        if bound == 0 {
            return Ok(None);
        }
        let tables = file.located_tables(|| {
            LocatedTables::new(
                TABLES.map(|(table, _)| {
                    let handle = |column: &str| match table.component() {
                        None => file.column_chunks(column),
                        Some(name) => file.component_column_chunks(name, column),
                    };
                    Some([
                        handle(PRIMARY_COLUMNS[0])?,
                        handle(PRIMARY_COLUMNS[1])?,
                        handle(PRIMARY_COLUMNS[2])?,
                        handle(PRIMARY_COLUMNS[3])?,
                    ])
                }),
                REFERENCES.map(|(table, _)| {
                    let name = table.component()?;
                    Some([
                        file.component_column_chunks(name, "val")?,
                        file.component_column_chunks(name, "rid")?,
                    ])
                }),
                REFERENCES.map(|(table, _)| file.component_column_chunks(table.component()?, "g")),
            )
        });
        // The table whose key order starts with the most bound slots, when
        // none starts with all of them: a bound graph after a free slot.
        let mut gap: Option<(usize, usize, &Columns)> = None;
        for (table, ((file_table, order), columns)) in TABLES.iter().zip(&tables.sorted).enumerate()
        {
            let prefix = order.iter().take_while(|&&c| codes[c].is_some()).count();
            if *file_table == FileTable::Quads && !file.quads_sorted() {
                continue;
            }
            let Some(columns) = columns else {
                continue;
            };
            if prefix != bound {
                if prefix > 0 && gap.is_none_or(|(_, longest, _)| prefix > longest) {
                    gap = Some((table, prefix, columns));
                }
                continue;
            }
            let Some(found) = locate_run(columns, order, prefix, &codes, &source, session).await?
            else {
                continue;
            };
            let table = RunTable::Sorted {
                columns: columns.clone(),
                table: *file_table,
            };
            return Ok(Some(LocatedRun::new(
                codes,
                found,
                table,
                Vec::new(),
                source,
                session.clone(),
            )));
        }
        // The bound reference slots' runs, each searched once and only for a
        // step that uses it: the gap step weighs the runs of the slots past
        // its prefix (a slot in the prefix holds every row of the gap run,
        // so its run is never shorter), and the lookup by reference, reached
        // when the gap step does not answer, reads the rest too.
        let past_prefix = |slot: usize| {
            gap.is_none_or(|(table, prefix, _)| !TABLES[table].1[..prefix].contains(&slot))
        };
        let mut runs = reference_runs(codes, tables, past_prefix, &source, session).await?;
        // The run of the longest bound prefix, its rows checked against the
        // bound slots past it, in the table's order, unless a reference run
        // answers in its place: one strictly shorter and within the cap on a
        // run checked row by row, as a hub subject's many quads give way to
        // its rare object's few (`s?o`).
        if let Some((table, prefix, columns)) = gap {
            let order = &TABLES[table].1;
            let shorter_reference = |found: &Range<u64>| {
                runs.iter().flatten().any(|run| {
                    let rows = run.end - run.start;
                    rows < found.end - found.start && rows <= gap_rows()
                })
            };
            if let Some(found) = locate_run(columns, order, prefix, &codes, &source, session)
                .await?
                .filter(|found| !shorter_reference(found))
            {
                let checks = order[prefix..]
                    .iter()
                    .filter_map(|&slot| {
                        let code = codes[slot]?;
                        let column = columns[slot].clone();
                        let kind = CheckKind::Positional { column };
                        Some(Check { slot, code, kind })
                    })
                    .collect();
                let table = RunTable::Sorted {
                    columns: columns.clone(),
                    table: TABLES[table].0,
                };
                let mut located =
                    LocatedRun::new(codes, found, table, checks, source.clone(), session.clone());
                // A run of a zone or more asks the file's statistics; a
                // shorter one reads its few rows faster than they answer
                // (a join's one-row probe: 1 against 45 µs). A run within
                // the cap asks them when a read needs them, so a first page
                // its first block fills asks none; a longer one asks now,
                // as whether it is capped depends on what they decide.
                if zones_enabled() && located.len() >= STATISTICS_ROWS {
                    located.pending = Some(PendingStatistics {
                        file: Arc::clone(file),
                    });
                    if located.len() > gap_rows() {
                        located.ask_statistics().await?;
                    }
                }
                // A run still checked row by row is capped, as without zones.
                if located.checks.is_empty()
                    || located.zones.is_some()
                    || located.len() <= gap_rows()
                {
                    return Ok(Some(located));
                }
            }
            // The prefix's slots' reference runs, for the lookup by reference.
            let rest = reference_runs(codes, tables, |slot| !past_prefix(slot), &source, session);
            for (run, found) in runs.iter_mut().zip(rest.await?) {
                *run = run.take().or(found);
            }
        }
        let Some(mut located) = by_reference(codes, tables, runs, source, session) else {
            return Ok(None);
        };
        // A run checked against the graph alone, its child holding each
        // row's graph (its one check a positional one), asks the statistics
        // as a table run does: its child's own `g` zones decide it.
        if !located.checks.is_empty()
            && located.checks.iter().all(Check::is_positional)
            && zones_enabled()
            && located.len() >= STATISTICS_ROWS
        {
            located.pending = Some(PendingStatistics {
                file: Arc::clone(file),
            });
            if located.len() > gap_rows() {
                located.ask_statistics().await?;
            }
        }
        // A reference run still checked row by row is capped.
        if located.checks.is_empty() || located.zones.is_some() || located.len() <= gap_rows() {
            return Ok(Some(located));
        }
        Ok(None)
    }
}

/// The reference indexes, each `(val, rid)` sorted by `val` then `rid`: a
/// run of one code's `val` lists, ascending, the quad-table rows holding it
/// at the `(s, p, o, g)` position given here.
pub(super) const REFERENCES: [(FileTable, usize); 2] = [(FileTable::RefP, 1), (FileTable::RefO, 2)];

/// The run of each bound reference slot's code that `wanted` names, in its
/// reference index's `val`, by [`REFERENCES`] index: `None` where the slot
/// is free or not wanted, the file holds no such index, or a leaf declines
/// the search.
pub(super) async fn reference_runs(
    codes: [Option<TermCode>; 4],
    tables: &LocatedTables,
    wanted: impl Fn(usize) -> bool,
    source: &Source,
    session: &VortexSession,
) -> Result<[Option<Range<u64>>; 2]> {
    let mut runs: [Option<Range<u64>>; 2] = [None, None];
    for (i, &(_, slot)) in REFERENCES.iter().enumerate() {
        let (Some(code), Some([val, _]), true) = (codes[slot], &tables.references[i], wanted(slot))
        else {
            continue;
        };
        observe!(ReferenceSearched { slot });
        // Boxed: a search's state would otherwise enlarge every lookup's
        // future, the sorted tables' too.
        runs[i] = Box::pin(val.bounds(code, source, session)).await?;
    }
    Ok(runs)
}

/// [`located_run`](VortexRdfStore::located_run) through a reference index:
/// the shortest of `runs` ([`reference_runs`]) gives the quad-table rows in
/// order, all of them, or, with other slots bound, those that hold them,
/// checked: `g` in place on the child's own `g` where it holds one, the
/// other slots by row id in the quad table, `p` or `o` against the other
/// reference's run where it was located. The caller caps a run its
/// statistics leave checked row by row at [`GAP_ROWS`]. A bound subject's
/// own run is the gap step's, which reads it unless a shorter reference run
/// answers in its place.
pub(super) fn by_reference(
    codes: [Option<TermCode>; 4],
    tables: &LocatedTables,
    runs: [Option<Range<u64>>; 2],
    source: Source,
    session: &VortexSession,
) -> Option<LocatedRun> {
    let quads = tables.sorted[0].as_ref()?;
    let (index, run) = (0..2)
        .filter_map(|i| Some((i, runs[i].clone()?)))
        .min_by_key(|(_, run)| run.end - run.start)?;
    let [_, rid] = tables.references[index].as_ref()?;
    let (table, key) = REFERENCES[index];
    // The graph in place, on the child's own `g`, first.
    let own_graph = codes[GRAPH_SLOT].zip(tables.reference_graphs[index].clone());
    let in_place = own_graph.is_some();
    let mut checks: Vec<Check> = own_graph
        .map(|(code, column)| Check {
            slot: GRAPH_SLOT,
            code,
            kind: CheckKind::Positional { column },
        })
        .into_iter()
        .collect();
    for (slot, code) in codes.into_iter().enumerate() {
        let Some(code) = code else {
            continue;
        };
        if slot == key || (slot == GRAPH_SLOT && in_place) {
            continue;
        }
        let other = (0..2).filter(|&i| i != index).find_map(|i| {
            let (Some(run), Some([_, rid])) = (&runs[i], &tables.references[i]) else {
                return None;
            };
            (REFERENCES[i].1 == slot).then(|| OtherReference {
                rid: rid.clone(),
                run: run.clone(),
            })
        });
        let kind = CheckKind::ById { other };
        checks.push(Check { slot, code, kind });
    }
    let table = RunTable::Reference {
        rid: rid.clone(),
        quads: quads.clone(),
        table,
    };
    Some(LocatedRun::new(
        codes,
        run,
        table,
        checks,
        source,
        session.clone(),
    ))
}

/// The run of a sorted table's rows holding `codes` in the first `prefix`
/// slots of its key `order` (`(s, p, o, g)` positions): the lead key's
/// bounds, narrowed key by key inside the run until it empties. `None` when a
/// chunk declines its probe.
pub(super) async fn locate_run(
    columns: &Columns,
    order: &[usize; 4],
    prefix: usize,
    codes: &[Option<TermCode>; 4],
    source: &Source,
    session: &VortexSession,
) -> Result<Option<Range<u64>>> {
    let lead = order[0];
    let Some(mut run) = columns[lead]
        .bounds(codes[lead].expect("a bound lead key"), source, session)
        .await?
    else {
        return Ok(None);
    };
    for &key in &order[1..prefix] {
        if run.is_empty() {
            break;
        }
        let code = codes[key].expect("a bound key");
        let Some(narrowed) = columns[key].bounds_in(run, code, source, session).await? else {
            return Ok(None);
        };
        run = narrowed;
    }
    Ok(Some(run))
}
