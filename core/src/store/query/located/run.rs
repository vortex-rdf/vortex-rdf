//! Reading a located run: its rows a block at a time, each block checked
//! as it is read (zone by zone where the statistics class them), its
//! windows, counts and streams.

use super::LocatedCodes;
use super::statistics::Statistics;
use crate::CodeChunkStream;
use crate::error::Result;
use crate::observe::observe;
use crate::store::scan::column_reads::{leaves_holding, listed_in, read_codes, take_codes};
use crate::store::schema::TermCode;
use crate::zone_stats::file_maps::FileTable;
use std::ops::Range;
use std::sync::Arc;
use vortex_buffer::Buffer;
use vortex_layout::segments::SegmentSource;
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_session::VortexSession;

use crate::counters::{self, note};
use crate::store::read::live::WindowCursor;
use crate::zone_stats::zone_set::{ZoneClass, ZonePart, ZoneSet};

/// A column's chunk-probe handle.
pub(super) type Chunks = Arc<ColumnChunks>;

/// A table's `(s, p, o, g)` chunk-probe handles.
pub(super) type Columns = [Chunks; 4];

/// The segments a file's reads fetch.
pub(super) type Source = Arc<dyn SegmentSource>;

/// The run answering a pattern, as [`located_run`](VortexRdfStore::located_run)
/// finds it: its positions in a table ([`RunTable`]) and the checks a
/// position passes to match ([`Check`]). It holds handles to the file's
/// columns and nothing on the store, so a window or a stream of it reads only
/// what it needs, and a stream of it outlives the store handle that found it.
pub(super) struct LocatedRun {
    /// The pattern's bound codes, `(s, p, o, g)`: a bound slot's column of
    /// the run's rows is its code repeated.
    pub(super) codes: [Option<TermCode>; 4],
    /// The run's positions in its table.
    positions: Range<u64>,
    pub(super) table: RunTable,
    /// The checks a position passes to match, in order: those reading a
    /// column of the run's own table first, then those by row id.
    pub(super) checks: Vec<Check>,
    source: Source,
    session: VortexSession,
    /// What the run knows of its file's statistics: to ask, until a read
    /// needs them ([`ask_statistics`](Self::ask_statistics)); the zones of
    /// its positional checks over its table once asked, where zone maps
    /// decide them ([`run_zones`]); or nothing left to class, and every row
    /// checked.
    pub(super) statistics: Statistics,
}

/// The table a [`LocatedRun`]'s positions are in.
pub(super) enum RunTable {
    /// A sorted table: the positions are its rows, which its `(s, p, o, g)`
    /// columns read.
    Sorted { columns: Columns, table: FileTable },
    /// A reference child: `rid` lists, ascending, the quad-table row at each
    /// position, which the quad table's `(s, p, o, g)` columns (`quads`,
    /// whether or not it is sorted) read.
    Reference {
        rid: Chunks,
        quads: Columns,
        table: FileTable,
    },
}

impl RunTable {
    /// The columns the run's rows are read from: the sorted table's at its
    /// positions, the quad table's at a reference run's row ids.
    fn columns(&self) -> &Columns {
        match self {
            RunTable::Sorted { columns, .. } => columns,
            RunTable::Reference { quads, .. } => quads,
        }
    }

    /// The table the positions are in, whose zone maps class them.
    pub(super) fn file_table(&self) -> FileTable {
        match self {
            RunTable::Sorted { table, .. } | RunTable::Reference { table, .. } => *table,
        }
    }
}

/// A check of a [`LocatedRun`]'s rows: the slot's code against its column.
pub(super) struct Check {
    pub(super) slot: usize,
    pub(super) code: TermCode,
    pub(super) kind: CheckKind,
}

/// Where a [`Check`] reads its slot.
pub(super) enum CheckKind {
    /// A column of the run's own table at its positions (a sorted table's
    /// column, or a reference child's own `g`): the run's zones decide it.
    Positional { column: Chunks },
    /// The quad table's column at the row ids the positions list; `other`,
    /// the other reference index's run of the code, lists those rows where
    /// searching it costs less.
    ById { other: Option<OtherReference> },
}

impl Check {
    /// Whether the check reads a column of the run's own table.
    pub(super) fn is_positional(&self) -> bool {
        matches!(self.kind, CheckKind::Positional { .. })
    }
}

/// The other reference index's run of a by-id [`Check`]'s code, which lists
/// the rows holding it, in ascending order.
pub(super) struct OtherReference {
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

    /// The rows as ascending ids, a run's listed out.
    fn into_ids(self) -> Vec<u64> {
        match self {
            Rows::Run(run) => run.collect(),
            Rows::Ids(ids) => ids,
        }
    }
}

impl LocatedRun {
    /// The run at `positions` of `table`, its rows passing `checks`, read
    /// through `source`; no statistics asked yet.
    pub(super) fn new(
        codes: [Option<TermCode>; 4],
        positions: Range<u64>,
        table: RunTable,
        checks: Vec<Check>,
        source: Source,
        session: VortexSession,
    ) -> Self {
        Self {
            codes,
            positions,
            table,
            checks,
            source,
            session,
            statistics: Statistics::Settled,
        }
    }

    /// The run's positions in its table.
    pub(super) fn positions(&self) -> Range<u64> {
        self.positions.clone()
    }

    /// The run, emptied: no row of it matches.
    pub(super) fn clear_rows(&mut self) {
        self.positions = self.positions.start..self.positions.start;
    }

    /// The run's rows, before their checks.
    pub(super) fn len(&self) -> u64 {
        self.positions.end - self.positions.start
    }

    /// The bits (`1 << slot`) of the run's positional checks.
    fn positional_bits(&self) -> u32 {
        self.checks
            .iter()
            .filter(|check| check.is_positional())
            .fold(0, |bits, check| bits | 1 << check.slot)
    }

    /// The rows at positions `at` of the run (not empty, within its
    /// [`len`](Self::len)) that the checks keep, in the run's order: the
    /// positional checks zone by zone where zones class them (none keeps
    /// nothing, all keeps every row unread, a mixed zone checks the slots it
    /// leaves unsettled), then the checks by row id. With no check, a sorted
    /// run's own rows.
    pub(super) async fn block(&self, at: Range<u64>) -> Result<Rows> {
        observe!(LocatedBlock(at.end - at.start));
        let span = self.positions.start + at.start..self.positions.start + at.end;
        if self.checks.is_empty() && matches!(self.table, RunTable::Sorted { .. }) {
            return Ok(Rows::Run(span));
        }
        let mut rows = match self.zones() {
            None => self.checked_rows(span, self.positional_bits()).await?,
            Some(zones) => {
                let mut rows = Vec::new();
                for ZonePart { zone, rows: part } in zones.parts(span) {
                    match zones.class(zone) {
                        ZoneClass::None => note(counters::ZONE_NONE, part.end - part.start),
                        ZoneClass::All => {
                            note(counters::ZONE_ALL, part.end - part.start);
                            match &self.table {
                                RunTable::Sorted { .. } => rows.extend(part),
                                RunTable::Reference { rid, .. } => rows.extend(
                                    read_codes(rid, part, &self.source, &self.session)
                                        .await?
                                        .iter(),
                                ),
                            }
                        }
                        ZoneClass::Mixed { unsettled } => {
                            rows.extend(self.checked_rows(part, unsettled).await?);
                        }
                    }
                }
                rows
            }
        };
        for check in &self.checks {
            if rows.is_empty() {
                break;
            }
            if let CheckKind::ById { other } = &check.kind {
                rows = self.by_id(check, other.as_ref(), rows).await?;
            }
        }
        Ok(Rows::Ids(rows))
    }

    /// The rows at the positions of `span` (contiguous) that pass every
    /// positional check in `slots` (bits `1 << slot`), in check order: the
    /// first read over the span, still contiguous, each other one at the
    /// positions the checks before it kept. A sorted run's rows are those
    /// positions; a reference run's, the row ids `rid` lists there, read
    /// over the span beside its check (its child's own `g`, the one column it
    /// holds) and picked in the same pass.
    async fn checked_rows(&self, span: Range<u64>, slots: u32) -> Result<Vec<u64>> {
        let checks: Vec<(&Check, &Chunks)> = self
            .checks
            .iter()
            .filter_map(|check| match &check.kind {
                CheckKind::Positional { column } if slots & 1 << check.slot != 0 => {
                    Some((check, column))
                }
                _ => None,
            })
            .collect();
        let ids = match &self.table {
            RunTable::Sorted { .. } => None,
            RunTable::Reference { rid, .. } => {
                Some(read_codes(rid, span.clone(), &self.source, &self.session).await?)
            }
        };
        let Some(&(first, column)) = checks.first() else {
            return Ok(match ids {
                Some(ids) => ids.to_vec(),
                None => span.collect(),
            });
        };
        if ids.is_some() {
            observe!(ReferenceGraphRead(span.end - span.start));
        }
        observe!(CheckRead(span.end - span.start));
        note(counters::CHECK_READ, span.end - span.start);
        let values = read_codes(column, span.clone(), &self.source, &self.session).await?;
        let start = span.start;
        if let (Some(ids), [_]) = (&ids, checks.as_slice()) {
            return Ok(ids
                .iter()
                .zip(values.iter())
                .filter(|&(_, &value)| value == first.code)
                .map(|(&id, _)| id)
                .collect());
        }
        let mut kept: Vec<u64> = span
            .zip(values.iter())
            .filter(|&(_, &value)| value == first.code)
            .map(|(position, _)| position)
            .collect();
        for &(check, column) in &checks[1..] {
            if kept.is_empty() {
                break;
            }
            kept = self.holding(column, check, kept).await?;
        }
        Ok(match ids {
            None => kept,
            Some(ids) => kept
                .iter()
                .map(|&position| ids[(position - start) as usize])
                .collect(),
        })
    }

    /// Of a reference run's row ids `rows` (ascending), those holding
    /// `check`'s code. Where `other`, the other reference's run of the code,
    /// is located, the rows it lists too: both lists ascend, so each row is
    /// searched in what is left of the other's `rid`s, which sit together in
    /// one index. Reading the quad table costs a leaf for each leaf the rows
    /// reach, searching the other run a search per row: rows spread one or
    /// two to a leaf (a large file) are searched, rows bunched in few leaves
    /// read, as are rows whose search a leaf declines.
    async fn by_id(
        &self,
        check: &Check,
        other: Option<&OtherReference>,
        rows: Vec<u64>,
    ) -> Result<Vec<u64>> {
        let column = &self.table.columns()[check.slot];
        if let Some(other) = other
            && rows.len() < SEARCHES_PER_LEAF * leaves_holding(column, &rows)
            && let Some(listed) = listed_in(
                &rows,
                &other.rid,
                other.run.clone(),
                &self.source,
                &self.session,
            )
            .await?
        {
            return Ok(listed);
        }
        self.holding(column, check, rows).await
    }

    /// The matches of the run's positions `part` (a zone's, one of
    /// `zones`'): none for a zone of class none, its rows for one of class
    /// all, and for a mixed zone with one positional check unsettled there
    /// its recorded count of the check's code when the part is the whole
    /// zone ([`ZoneSet::recorded_in`]), else its rows counted on the
    /// check's column's encoding ([`ColumnChunks::count_eq`]); a mixed zone
    /// with more checks unsettled is checked.
    ///
    /// [`ColumnChunks::count_eq`]: ColumnChunks::count_eq
    pub(super) async fn zone_count(&self, zones: &ZoneSet, part: &ZonePart) -> Result<u64> {
        let rows = part.rows.clone();
        let unsettled = match zones.class(part.zone) {
            ZoneClass::None => {
                note(counters::ZONE_NONE, rows.end - rows.start);
                return Ok(0);
            }
            ZoneClass::All => {
                note(counters::ZONE_ALL, rows.end - rows.start);
                return Ok(rows.end - rows.start);
            }
            ZoneClass::Mixed { unsettled } => unsettled,
        };
        let single = (unsettled.count_ones() == 1)
            .then(|| self.positional(unsettled.trailing_zeros() as usize))
            .flatten();
        if let Some((check, column)) = single {
            if let Some(matches) = zones.recorded_in(unsettled.trailing_zeros(), part) {
                observe!(ZoneRecorded(rows.end - rows.start));
                note(counters::ZONE_RECORDED, rows.end - rows.start);
                return Ok(matches);
            }
            observe!(ZoneCounted(rows.end - rows.start));
            note(counters::ZONE_COUNTED, rows.end - rows.start);
            return Ok(column
                .count_eq(rows, check.code, &self.source, &self.session)
                .await?);
        }
        Ok(self.checked_rows(rows, unsettled).await?.len() as u64)
    }

    /// The positional check of `slot`, with its column.
    fn positional(&self, slot: usize) -> Option<(&Check, &Chunks)> {
        self.checks.iter().find_map(|check| match &check.kind {
            CheckKind::Positional { column } if check.slot == slot => Some((check, column)),
            _ => None,
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
            let kept = self
                .block(rows.start - run.start..rows.end - run.start)
                .await?
                .into_ids();
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

    /// Of `rows` (ascending), those holding `check`'s code in `column`, read
    /// at them.
    async fn holding(&self, column: &Chunks, check: &Check, rows: Vec<u64>) -> Result<Vec<u64>> {
        observe!(CheckTaken {
            slot: check.slot,
            rows: rows.len() as u64
        });
        let values = take_codes(column, &rows, &self.source, &self.session).await?;
        Ok(rows
            .into_iter()
            .zip(values.iter())
            .filter(|&(_, &value)| value == check.code)
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
                    read_codes(&self.table.columns()[slot], run.clone(), source, session).await?
                }
                (None, Rows::Ids(ids)) => {
                    take_codes(&self.table.columns()[slot], ids, source, session).await?
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
        if matches!(self.statistics, Statistics::Unasked(_))
            && offset == 0
            && let Some(limit) = limit.filter(|&limit| limit as u64 <= FIRST_GAP_BLOCK)
        {
            let mut kept = self
                .block(0..FIRST_GAP_BLOCK.min(self.len()))
                .await?
                .into_ids();
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
        if let Some(zones) = self.zones() {
            return self.zoned_window(zones, offset, limit, whole).await;
        }
        let len = self.len();
        if self.checks.is_empty() {
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
            let kept = self.block(at..next).await?.into_ids();
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
