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
use crate::store::persist::native_file::LocatedTables;
use crate::store::schema::PRIMARY_COLUMNS;
use crate::store::schema::TermCode;
use std::ops::Range;
use std::sync::Arc;
use vortex_session::VortexSession;

/// The sorted tables whose key prefix a pattern's bound slots may be: the
/// quad table (`None`) and the copy indexes, each with its key order as
/// `(s, p, o, g)` positions.
const TABLES: [(Option<&str>, [usize; 4]); 3] = [
    (None, [0, 1, 2, 3]),
    (Some("index:posg"), [1, 2, 0, 3]),
    (Some("index:ospg"), [2, 0, 1, 3]),
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
                TABLES.map(|(component, _)| {
                    let handle = |column: &str| match component {
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
                REFERENCES.map(|(name, _)| {
                    Some([
                        file.component_column_chunks(name, "val")?,
                        file.component_column_chunks(name, "rid")?,
                    ])
                }),
                REFERENCES.map(|(name, _)| file.component_column_chunks(name, "g")),
            )
        });
        let run = |columns: Columns, rows, checked| LocatedRun {
            codes,
            columns,
            rows,
            checked,
            source: source.clone(),
            session: session.clone(),
            zones: None,
            pending: None,
        };
        // The table whose key order starts with the most bound slots, when
        // none starts with all of them: a bound graph after a free slot.
        let mut gap: Option<(usize, usize)> = None;
        for (table, ((component, order), columns)) in TABLES.iter().zip(&tables.sorted).enumerate()
        {
            let prefix = order.iter().take_while(|&&c| codes[c].is_some()).count();
            if component.is_none() && !file.quads_sorted() {
                continue;
            }
            let Some(columns) = columns else {
                continue;
            };
            if prefix != bound {
                if prefix > 0 && gap.is_none_or(|(_, longest)| prefix > longest) {
                    gap = Some((table, prefix));
                }
                continue;
            }
            let Some(found) = locate_run(columns, order, prefix, &codes, &source, session).await?
            else {
                continue;
            };
            return Ok(Some(run(
                columns.clone(),
                RunRows::Table(found),
                Vec::new(),
            )));
        }
        // The bound reference slots' runs, each searched once and only for a
        // step that uses it: the gap step weighs the runs of the slots past
        // its prefix (a slot in the prefix holds every row of the gap run,
        // so its run is never shorter), and the lookup by reference, reached
        // when the gap step does not answer, reads the rest too.
        let past_prefix = |slot: usize| {
            gap.is_none_or(|(table, prefix)| !TABLES[table].1[..prefix].contains(&slot))
        };
        let mut runs = reference_runs(codes, tables, past_prefix, &source, session).await?;
        // The run of the longest bound prefix, its rows checked against the
        // bound slots past it, in the table's order, unless a reference run
        // answers in its place: one strictly shorter and within the cap on a
        // run checked row by row, as a hub subject's many quads give way to
        // its rare object's few (`s?o`).
        if let Some((table, prefix)) = gap {
            let columns = tables.sorted[table].as_ref().expect("a table with columns");
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
                let checked: Vec<usize> = order[prefix..]
                    .iter()
                    .copied()
                    .filter(|&slot| codes[slot].is_some())
                    .collect();
                let mut located = run(columns.clone(), RunRows::Table(found), checked);
                // A run of a zone or more asks the file's statistics; a
                // shorter one reads its few rows faster than they answer
                // (a join's one-row probe: 1 against 45 µs). A run within
                // the cap asks them when a read needs them, so a first page
                // its first block fills asks none; a longer one asks now,
                // as whether it is capped depends on what they decide.
                if zones_enabled() && located.len() >= STATISTICS_ROWS {
                    located.pending = Some(PendingStatistics {
                        file: Arc::clone(file),
                        target: StatisticsTarget::Table {
                            table,
                            component: TABLES[table].0,
                        },
                    });
                    if located.len() > gap_rows() {
                        located.ask_statistics().await?;
                    }
                }
                // A run still checked row by row is capped, as without zones.
                if located.checked.is_empty()
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
        let Some((columns, rows, checked, index)) = by_reference(codes, tables, runs) else {
            return Ok(None);
        };
        let mut located = run(columns, rows, checked);
        // A run checked against the graph alone, its child holding each
        // row's graph, asks the statistics as a table run does: its child's
        // own `g` zones decide it.
        if matches!(&located.rows, RunRows::Reference { graph: Some(_), .. })
            && located.checked == [GRAPH_SLOT]
            && zones_enabled()
            && located.len() >= STATISTICS_ROWS
        {
            located.pending = Some(PendingStatistics {
                file: Arc::clone(file),
                target: StatisticsTarget::Reference { index },
            });
            if located.len() > gap_rows() {
                located.ask_statistics().await?;
            }
        }
        // A reference run still checked row by row is capped.
        if located.checked.is_empty() || located.zones.is_some() || located.len() <= gap_rows() {
            return Ok(Some(located));
        }
        Ok(None)
    }
}

/// The reference indexes, each `(val, rid)` sorted by `val` then `rid`: a
/// run of one code's `val` lists, ascending, the quad-table rows holding it
/// at the `(s, p, o, g)` position given here.
pub(super) const REFERENCES: [(&str, usize); 2] = [("index:ref-p", 1), ("index:ref-o", 2)];

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
/// checked: against the other reference's run for `p` or `o`, by reading the
/// quad table for `s`, and for `g` by the child's own `g` where it holds one,
/// else the quad table's. Answers the table the rows are read from (the quad
/// table), where they are, the slots checked, and the reference index; the
/// caller caps a run its statistics leave checked row by row at
/// [`GAP_ROWS`]. A bound subject's own run is the gap step's, which reads it
/// unless a shorter reference run answers in its place.
pub(super) fn by_reference(
    codes: [Option<TermCode>; 4],
    tables: &LocatedTables,
    runs: [Option<Range<u64>>; 2],
) -> Option<(Columns, RunRows, Vec<usize>, usize)> {
    let quads = tables.sorted[0].as_ref()?;
    let index = (0..2)
        .filter(|&i| runs[i].is_some())
        .min_by_key(|&i| runs[i].as_ref().map(|run| run.end - run.start))?;
    let run = runs[index].clone().expect("a located run");
    let [_, rid] = tables.references[index]
        .as_ref()
        .expect("a reference that located a run");
    let key = REFERENCES[index].1;
    let checked: Vec<usize> = (0..4)
        .filter(|&slot| slot != key && codes[slot].is_some())
        .collect();
    let other = (0..2).filter(|&i| i != index).find_map(|i| {
        let slot = REFERENCES[i].1;
        let (Some(run), Some([_, rid])) = (&runs[i], &tables.references[i]) else {
            return None;
        };
        checked.contains(&slot).then(|| OtherReference {
            slot,
            rid: rid.clone(),
            run: run.clone(),
        })
    });
    let rows = RunRows::Reference {
        rid: rid.clone(),
        run,
        other,
        graph: codes[GRAPH_SLOT].and(tables.reference_graphs[index].clone()),
    };
    Some((quads.clone(), rows, checked, index))
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
