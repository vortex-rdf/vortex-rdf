//! Reading a located run: its rows a block at a time, each block checked
//! as it is read (zone by zone where the statistics class them), its
//! windows, counts and streams.

use super::LocatedCodes;
use super::statistics::*;
use crate::CodeChunkStream;
use crate::error::Result;
use crate::observe::observe;
use crate::store::scan::column_reads::{leaves_holding, listed_in, read_codes, take_codes};
use crate::store::schema::TermCode;
use std::ops::Range;
use std::sync::Arc;
use vortex_buffer::Buffer;
use vortex_layout::segments::SegmentSource;
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_session::VortexSession;

use crate::io::zone_set::{ZoneClass, ZonePart, ZoneSet};
use crate::store::read::live::WindowCursor;
use vortex_rdf_encoded_search::counters;

/// One located run's zone decision of `rows` rows at counter site `site`
/// (`VORTEX_RDF_COUNTERS=1`).
pub(super) fn note(site: usize, rows: u64) {
    counters::note(site, rows as usize);
}

/// A column's chunk-probe handle.
pub(super) type Chunks = Arc<ColumnChunks>;

/// A table's `(s, p, o, g)` chunk-probe handles.
pub(super) type Columns = [Chunks; 4];

/// The segments a file's reads fetch.
pub(super) type Source = Arc<dyn SegmentSource>;

/// The run answering a pattern, as [`located_run`](VortexRdfStore::located_run)
/// finds it: the table its rows are read from, where they are, and the
/// bound slots each is checked against. It holds handles to the file's
/// columns and nothing on the store, so a window or a stream of it reads only
/// what it needs, and a stream of it outlives the store handle that found it.
pub(super) struct LocatedRun {
    pub(super) codes: [Option<TermCode>; 4],
    /// The `(s, p, o, g)` columns of the table the rows are read from: a
    /// sorted table's, or the quad table's for a reference run.
    pub(super) columns: Columns,
    pub(super) rows: RunRows,
    /// The bound slots the run does not pin, checked at each of its rows in
    /// this order.
    pub(super) checked: Vec<usize>,
    pub(super) source: Source,
    pub(super) session: VortexSession,
    /// The zone checks of the run's checked slots, each named by its slot
    /// ([`ZoneSet`]), over the table the run's positions are in, when zone
    /// maps decide them ([`run_zones`]); `None` checks every row.
    pub(super) zones: Option<ZoneSet>,
    /// The file statistics still to ask for the run's checked slots, until a
    /// read needs them ([`ask_statistics`](Self::ask_statistics)); `None`
    /// once asked, or when the run asks none.
    pub(super) pending: Option<PendingStatistics>,
}

/// Where a [`LocatedRun`]'s rows are.
pub(super) enum RunRows {
    /// The table's rows `run`, contiguous.
    Table(Range<u64>),
    /// The quad-table rows a reference run lists, ascending: `rid`'s values
    /// at `run`. `other` is the other reference's run, when it located the
    /// code of a checked slot.
    Reference {
        rid: Chunks,
        run: Range<u64>,
        other: Option<OtherReference>,
        /// The reference child's own `g`, when the graph is checked and the
        /// child holds each row's graph: the check reads it at the run's
        /// positions, contiguous, instead of the quad table at its rows.
        graph: Option<Chunks>,
    },
}

/// The other reference's run of a [`RunRows::Reference`], which lists the
/// rows holding the code of the checked slot `slot`, in ascending order.
pub(super) struct OtherReference {
    pub(super) slot: usize,
    pub(super) rid: Chunks,
    pub(super) run: Range<u64>,
}

/// A block's rows of the table a [`LocatedRun`] reads: a contiguous run of
/// it, or the ascending rows a reference run and the checks kept.
pub(super) enum Rows {
    Run(Range<u64>),
    Ids(Vec<u64>),
}

impl Rows {
    pub(super) fn is_empty(&self) -> bool {
        match self {
            Rows::Run(run) => run.is_empty(),
            Rows::Ids(ids) => ids.is_empty(),
        }
    }
}

impl LocatedRun {
    /// The positions of the run's rows in the table it is located in: a
    /// sorted table's rows, or a reference child's.
    pub(super) fn positions(&self) -> Range<u64> {
        match &self.rows {
            RunRows::Table(run) | RunRows::Reference { run, .. } => run.clone(),
        }
    }

    /// The run, emptied: no row of it matches.
    pub(super) fn clear_rows(&mut self) {
        match &mut self.rows {
            RunRows::Table(run) | RunRows::Reference { run, .. } => *run = run.start..run.start,
        }
    }

    /// The column the check of `slot` reads at the run's positions: a
    /// reference run's own `g` for the graph, else the table's.
    pub(super) fn checked_column(&self, slot: usize) -> &Chunks {
        match &self.rows {
            RunRows::Reference {
                graph: Some(graph), ..
            } if slot == GRAPH_SLOT => graph,
            _ => &self.columns[slot],
        }
    }

    /// The run's rows, before their checks.
    pub(super) fn len(&self) -> u64 {
        match &self.rows {
            RunRows::Table(run) | RunRows::Reference { run, .. } => run.end - run.start,
        }
    }

    /// The rows at positions `at` of the run (not empty, within its
    /// [`len`](Self::len)) that the checks keep, in the run's order: with no
    /// slot checked, a table run's own rows or the rows a reference run lists
    /// there.
    pub(super) async fn block(&self, at: Range<u64>) -> Result<Rows> {
        observe!(LocatedBlock(at.end - at.start));
        let (source, session) = (&self.source, &self.session);
        match &self.rows {
            RunRows::Table(run) => {
                let rows = run.start + at.start..run.start + at.end;
                if self.checked.is_empty() {
                    return Ok(Rows::Run(rows));
                }
                let Some(zones) = &self.zones else {
                    let every = self
                        .checked
                        .iter()
                        .fold(0u32, |bits, &slot| bits | 1 << slot);
                    return Ok(Rows::Ids(self.checked_rows(rows, every).await?));
                };
                // Zone by zone: none keeps nothing, all keeps every row
                // unread, a mixed zone checks the slots it leaves unsettled.
                let mut kept = Vec::new();
                for ZonePart { zone, rows } in zones.parts(rows) {
                    match zones.class(zone) {
                        ZoneClass::None => note(counters::ZONE_NONE, rows.end - rows.start),
                        ZoneClass::All => {
                            note(counters::ZONE_ALL, rows.end - rows.start);
                            kept.extend(rows);
                        }
                        ZoneClass::Mixed { unsettled } => {
                            kept.extend(self.checked_rows(rows, unsettled).await?);
                        }
                    }
                }
                Ok(Rows::Ids(kept))
            }
            RunRows::Reference {
                rid,
                run,
                other,
                graph,
            } => {
                let rows = run.start + at.start..run.start + at.end;
                // A run checked against the graph alone, decided zone by
                // zone over its child's positions: none keeps nothing, all
                // keeps every row id unread, a mixed zone reads its graphs.
                if let (Some(zones), Some(graph)) = (&self.zones, graph) {
                    debug_assert_eq!(
                        self.checked,
                        [GRAPH_SLOT],
                        "a reference child's zones decide its graph alone"
                    );
                    let mut kept = Vec::new();
                    for ZonePart { zone, rows } in zones.parts(rows) {
                        match zones.class(zone) {
                            ZoneClass::None => note(counters::ZONE_NONE, rows.end - rows.start),
                            ZoneClass::All => {
                                note(counters::ZONE_ALL, rows.end - rows.start);
                                kept.extend(read_codes(rid, rows, source, session).await?.iter());
                            }
                            ZoneClass::Mixed { .. } => {
                                kept.extend(self.graph_rows(rid, graph, rows).await?);
                            }
                        }
                    }
                    return Ok(Rows::Ids(kept));
                }
                // The child's own graphs check the graph first, on the
                // block's own contiguous rows.
                let own = graph
                    .as_ref()
                    .filter(|_| self.checked.contains(&GRAPH_SLOT));
                let mut kept = match own {
                    Some(graph) => self.graph_rows(rid, graph, rows).await?,
                    None => read_codes(rid, rows, source, session).await?.to_vec(),
                };
                for &slot in &self.checked {
                    if kept.is_empty() {
                        break;
                    }
                    if slot == GRAPH_SLOT && own.is_some() {
                        continue;
                    }
                    // A slot with a located reference run keeps the rows that
                    // run lists too: both lists ascend, so each row is
                    // searched in what is left of the other's `rid`s, which
                    // sit together in one index. Reading the quad table costs
                    // a leaf for each leaf the rows reach, searching the other
                    // run a search per row: rows spread one or two to a leaf
                    // (a large file) are searched, rows bunched in few leaves
                    // read, as are rows whose search a leaf declines.
                    if let Some(other) = other.as_ref().filter(|other| other.slot == slot)
                        && kept.len()
                            < SEARCHES_PER_LEAF * leaves_holding(&self.columns[slot], &kept)
                        && let Some(listed) =
                            listed_in(&kept, &other.rid, other.run.clone(), source, session).await?
                    {
                        kept = listed;
                        continue;
                    }
                    kept = self.holding(slot, kept).await?;
                }
                Ok(Rows::Ids(kept))
            }
        }
    }

    /// The table rows `rows` (contiguous) holding the code of every checked
    /// slot in `slots` (bits `1 << slot`), checked in [`checked`](Self::checked)
    /// order: the first slot read over the rows, still contiguous, each
    /// other one at the rows the slots before it kept.
    pub(super) async fn checked_rows(&self, rows: Range<u64>, slots: u32) -> Result<Vec<u64>> {
        debug_assert!(
            matches!(self.rows, RunRows::Table(_)),
            "checked rows are a table run's: a reference run lists quad-table rows"
        );
        let mut order = self
            .checked
            .iter()
            .copied()
            .filter(|&slot| slots & 1 << slot != 0);
        let Some(first) = order.next() else {
            return Ok(rows.collect());
        };
        observe!(CheckRead(rows.end - rows.start));
        note(counters::CHECK_READ, rows.end - rows.start);
        let code = self.codes[first].expect("a checked slot is bound");
        let values = read_codes(
            &self.columns[first],
            rows.clone(),
            &self.source,
            &self.session,
        )
        .await?;
        let mut kept: Vec<u64> = rows
            .zip(values.iter())
            .filter(|&(_, &value)| value == code)
            .map(|(row, _)| row)
            .collect();
        for slot in order {
            if kept.is_empty() {
                break;
            }
            kept = self.holding(slot, kept).await?;
        }
        Ok(kept)
    }

    /// The matches of the run's positions `part` (a zone's, one of
    /// `zones`'): none for a zone of class none, its rows for one of class
    /// all, and for a mixed zone with one slot unsettled there its recorded
    /// count of the slot's code when the part is the whole zone
    /// ([`ZoneSet::recorded_in`]), else its rows counted on the checked
    /// column's encoding ([`ColumnChunks::count_eq`]); a mixed zone with more
    /// slots unsettled (a sorted table's) is checked.
    ///
    /// [`ColumnChunks::count_eq`]: ColumnChunks::count_eq
    pub(super) async fn zone_count(&self, zones: &ZoneSet, part: &ZonePart) -> Result<u64> {
        let rows = part.rows.clone();
        Ok(match zones.class(part.zone) {
            ZoneClass::None => {
                note(counters::ZONE_NONE, rows.end - rows.start);
                0
            }
            ZoneClass::All => {
                note(counters::ZONE_ALL, rows.end - rows.start);
                rows.end - rows.start
            }
            ZoneClass::Mixed { unsettled } if unsettled.count_ones() == 1 => {
                if let Some(matches) = zones.recorded_in(unsettled.trailing_zeros(), part) {
                    observe!(ZoneRecorded(rows.end - rows.start));
                    note(counters::ZONE_RECORDED, rows.end - rows.start);
                    return Ok(matches);
                }
                let slot = unsettled.trailing_zeros() as usize;
                let code = self.codes[slot].expect("a checked slot is bound");
                observe!(ZoneCounted(rows.end - rows.start));
                note(counters::ZONE_COUNTED, rows.end - rows.start);
                self.checked_column(slot)
                    .count_eq(rows, code, &self.source, &self.session)
                    .await?
            }
            ZoneClass::Mixed { unsettled } => {
                self.checked_rows(rows, unsettled).await?.len() as u64
            }
        })
    }

    /// [`window_of_matches`](Self::window_of_matches) of a run decided by
    /// zones: the zone holding the window's first match found by the zones'
    /// counts ([`zone_count`](Self::zone_count)), the window read from there
    /// a zone at a time, and the count, when `whole`, finished by the
    /// counts of the zones past it.
    pub(super) async fn zoned_window(
        &self,
        zones: &ZoneSet,
        offset: usize,
        limit: Option<usize>,
        whole: bool,
    ) -> Result<(u64, Rows)> {
        let run = &self.positions();
        let parts: Vec<ZonePart> = zones.parts(run.clone()).collect();
        let mut cursor = WindowCursor::new(offset, limit);
        // Until the zone holding the window's first match, each zone is
        // counted and skipped by its count, unread: those before the window,
        // and at its start those counted empty.
        let mut counting = true;
        let mut at = 0;
        while at < parts.len() && !cursor.is_full() {
            let rows = &parts[at].rows;
            if counting {
                let n = self.zone_count(zones, &parts[at]).await?;
                if cursor.skips(n as usize) {
                    at += 1;
                    continue;
                }
                counting = false;
            }
            let kept = match self
                .block(rows.start - run.start..rows.end - run.start)
                .await?
            {
                Rows::Ids(kept) => kept,
                Rows::Run(rows) => rows.collect(),
            };
            cursor.take(&kept);
            at += 1;
        }
        let (window, live) = cursor.finish(at == parts.len());
        let mut total = live.at_least() as u64;
        if whole {
            for part in &parts[at..] {
                total += self.zone_count(zones, part).await?;
            }
        }
        Ok((total, Rows::Ids(window)))
    }

    /// The row ids `rid` lists at a reference child's positions `at` whose
    /// graph, the child's own `g` there, is the run's graph code.
    pub(super) async fn graph_rows(
        &self,
        rid: &Chunks,
        graph: &Chunks,
        at: Range<u64>,
    ) -> Result<Vec<u64>> {
        let (source, session) = (&self.source, &self.session);
        let code = self.codes[GRAPH_SLOT].expect("a checked graph is bound");
        observe!(ReferenceGraphRead(at.end - at.start));
        observe!(CheckRead(at.end - at.start));
        note(counters::CHECK_READ, at.end - at.start);
        let rids = read_codes(rid, at.clone(), source, session).await?;
        let graphs = read_codes(graph, at, source, session).await?;
        Ok(rids
            .iter()
            .zip(graphs.iter())
            .filter(|&(_, &g)| g == code)
            .map(|(&row, _)| row)
            .collect())
    }

    /// Of `rows` (ascending), those holding the checked `slot`'s code, read
    /// at them.
    pub(super) async fn holding(&self, slot: usize, rows: Vec<u64>) -> Result<Vec<u64>> {
        observe!(CheckTaken {
            slot,
            rows: rows.len() as u64
        });
        let code = self.codes[slot].expect("a checked slot is bound");
        let values = take_codes(&self.columns[slot], &rows, &self.source, &self.session).await?;
        Ok(rows
            .into_iter()
            .zip(values.iter())
            .filter(|&(_, &value)| value == code)
            .map(|(row, _)| row)
            .collect())
    }

    /// The `(s, p, o, g)` code columns of a block's `rows`: a bound slot is
    /// its code repeated, a free one read from its chunk leaves
    /// ([`read_codes`], [`take_codes`]).
    pub(super) async fn columns(&self, rows: &Rows) -> Result<[Buffer<TermCode>; 4]> {
        let len = match rows {
            Rows::Run(run) => (run.end - run.start) as usize,
            Rows::Ids(ids) => ids.len(),
        };
        let (source, session) = (&self.source, &self.session);
        let mut out: [Buffer<TermCode>; 4] = Default::default();
        for (slot, column) in out.iter_mut().enumerate() {
            *column = match (self.codes[slot], rows) {
                (Some(code), _) => vortex_buffer::BufferMut::full(code, len).freeze(),
                (None, Rows::Run(run)) => {
                    read_codes(&self.columns[slot], run.clone(), source, session).await?
                }
                (None, Rows::Ids(ids)) => {
                    take_codes(&self.columns[slot], ids, source, session).await?
                }
            };
        }
        Ok(out)
    }

    /// The window `offset..offset + limit` (`None`: to the last) of the
    /// run's matches, beside their number: with no slot checked the run's
    /// length, and only the window's rows read; otherwise the whole run is
    /// counted, zone by zone where the statistics decide it and a block at a
    /// time where they do not, and only the rows in the window are held.
    pub(super) async fn window(
        &mut self,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<LocatedCodes> {
        self.ask_statistics().await?;
        let (total, rows) = self.window_of_matches(offset, limit, true).await?;
        let columns = self.columns(&rows).await?;
        Ok(LocatedCodes { total, columns })
    }

    /// The `(s, p, o, g)` code columns of [`window`](Self::window)'s rows,
    /// without its count: the run checked from its start only as far as the
    /// window's last row. A first window that the run's first block
    /// ([`FIRST_GAP_BLOCK`] rows) fills is read from that block, row by row,
    /// before the file's statistics are asked, which cost more than the
    /// block (70 against 15 µs); a window the block leaves short asks them.
    pub(super) async fn window_rows(
        &mut self,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        if self.pending.is_some()
            && offset == 0
            && let Some(limit) = limit.filter(|&limit| limit as u64 <= FIRST_GAP_BLOCK)
        {
            let mut kept = match self.block(0..FIRST_GAP_BLOCK.min(self.len())).await? {
                Rows::Ids(kept) => kept,
                Rows::Run(rows) => rows.collect(),
            };
            if kept.len() >= limit {
                kept.truncate(limit);
                return self.columns(&Rows::Ids(kept)).await;
            }
        }
        self.ask_statistics().await?;
        let (_, rows) = self.window_of_matches(offset, limit, false).await?;
        self.columns(&rows).await
    }

    /// The rows of the window `offset..offset + limit` of the run's matches,
    /// beside the number of matches the reads counted: all of them, with no
    /// slot checked or with `whole`; otherwise those up to the window's last
    /// row, where the checks stop. A run the statistics decide goes zone by
    /// zone ([`zoned_window`](Self::zoned_window)), skipping the zones before
    /// the window by their counts, from their classes and recorded values,
    /// unread; any other checked run is
    /// read a block at a time, [`FIRST_GAP_BLOCK`] rows first and each next
    /// block twice as many, up to [`GAP_BLOCK`]: a window near the run's
    /// start reads little of it.
    pub(super) async fn window_of_matches(
        &self,
        offset: usize,
        limit: Option<usize>,
        whole: bool,
    ) -> Result<(u64, Rows)> {
        if let Some(zones) = &self.zones {
            return self.zoned_window(zones, offset, limit, whole).await;
        }
        let len = self.len();
        if self.checked.is_empty() {
            let w = window_of(len, offset, limit);
            let rows = match w.is_empty() {
                // A count's window, or one past the end, reads nothing.
                true => Rows::Ids(Vec::new()),
                false => self.block(w).await?,
            };
            return Ok((len, rows));
        }
        let mut cursor = WindowCursor::new(offset, limit);
        let (mut at, mut block) = (0, FIRST_GAP_BLOCK);
        while at < len {
            if !whole && cursor.is_full() {
                break;
            }
            let next = at.saturating_add(block).min(len);
            let kept = match self.block(at..next).await? {
                Rows::Ids(kept) => kept,
                Rows::Run(rows) => rows.collect(),
            };
            cursor.take(&kept);
            at = next;
            block = (block * 2).min(GAP_BLOCK);
        }
        let (window, live) = cursor.finish(at >= len);
        Ok((live.at_least() as u64, Rows::Ids(window)))
    }

    /// The run's matches as chunks of their four code columns, a block of
    /// `batch_rows` rows of the run read per chunk (blocks keeping no row
    /// skipped), so a chunk holds at most `batch_rows` rows and every row of
    /// the run is read once; the stream holds the run and its position in it.
    pub(super) fn chunks(self, batch_rows: usize) -> CodeChunkStream {
        use futures::StreamExt as _;

        let batch = batch_rows as u64;
        futures::stream::try_unfold((self, 0u64), move |(mut run, mut at)| async move {
            run.ask_statistics().await?;
            let len = run.len();
            while at < len {
                let next = at.saturating_add(batch).min(len);
                let rows = run.block(at..next).await?;
                at = next;
                if rows.is_empty() {
                    continue;
                }
                let columns = run.columns(&rows).await?;
                return Ok(Some((Vec::from(columns), (run, at))));
            }
            Ok(None)
        })
        .boxed()
    }
}

/// The graph's slot in `(s, p, o, g)`.
pub(super) const GRAPH_SLOT: usize = 3;

/// How many searches of a reference run's `rid`s cost about as much as
/// reaching one more quad-table leaf to read a slot there (about 250 ns
/// against 4 µs per leaf on WatDiv 100M's reference store, warm).
pub(super) const SEARCHES_PER_LEAF: usize = 16;

/// Rows of a checked run a window reads in its first block.
pub(super) const FIRST_GAP_BLOCK: u64 = 1 << 12;

/// Rows of a checked run a window reads per block at most.
pub(super) const GAP_BLOCK: u64 = 1 << 16;

/// The positions `offset..offset + limit` of `total` matches, clamped to
/// them (`None`: to the last).
pub(super) fn window_of(total: u64, offset: usize, limit: Option<usize>) -> Range<u64> {
    let from = (offset as u64).min(total);
    let to = limit.map_or(total, |limit| from.saturating_add(limit as u64).min(total));
    from..to
}
