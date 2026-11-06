//! The residual compare over an in-memory base: what the statistics settle
//! of a pattern's leftover equalities ([`ask_statistics`]: the base
//! columns' array statistics, then their zone maps, classing the zones a
//! selection touches once, [`ResidualZones`]), the rows of a selection
//! where every equality left holds ([`residual_rows`]: zone by zone, "none"
//! zones skipped, "all" zones taken and mixed ones compared, through the
//! typed fast path or the mask scan),
//! and the mask of an array's rows holding them ([`mask_for`]), which the
//! tail's match compares with too. Functions of the base, with no store
//! state: the match narrows a selection with them, and a residual compare
//! left pending runs them piece by piece
//! ([`PendingResidual`](crate::store::view::pending::PendingResidual)).

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::StructArray;
use vortex_array::arrays::constant::ConstantArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;
use web_time::Instant;

use crate::debug;
use crate::error::Result;
use crate::io::zone_map::{ColumnZones, ZONE_ROWS, eq_on_bounds};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::MemoryBase;
use crate::store::array::{bool_array_to_mask, into_struct_array, stamped_bounds, unsigned_value};
use crate::store::scan::typed_eq::typed_residual_ids_reading;
use crate::store::schema::{PRIMARY_COLUMNS, RowId};
use crate::store::view::selection::{RowSelection, SelectedRows};

/// Whether a residual over a selection of `rows` rows asks the statistics
/// before it compares: past one zone's rows, as below them the compare
/// costs less than the statistics' answer.
pub(crate) fn asks_statistics(rows: usize) -> bool {
    rows > ZONE_ROWS
}

/// What the statistics settle of a residual before its rows are compared
/// ([`ask_statistics`]): the first two levels of the statistics ladder in
/// memory, the columns' array statistics and then their zones.
pub(crate) enum Asked {
    /// No base row holds an equality's code: the array statistics.
    Nothing,
    /// Every equality holds on every base row: the array statistics.
    Everything,
    /// The equalities the array statistics leave, in order, and the zones
    /// the selection touches classed against them, where their columns
    /// have zone maps ([`ResidualZones`]).
    Open {
        eqs: Vec<(&'static str, Scalar)>,
        zones: Option<ResidualZones>,
    },
}

/// The statistics a residual over `selection` (of `memory`'s base) asks
/// once, each equality of `eqs` by the bounds' rule of `x = code`
/// ([`eq_on_bounds`]): first against its column's exact bounds, the array
/// statistics ([`stamped_bounds`]), then the equalities left against their
/// columns' zone maps and `g`'s recorded graphs ([`ResidualZones`]).
pub(crate) fn ask_statistics(
    memory: &MemoryBase,
    selection: &RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
) -> Result<Asked> {
    let base = &memory.base;
    let struct_arr = into_struct_array(base.clone())?;
    let mut open = Vec::with_capacity(eqs.len());
    let mut zoned = Vec::new();
    for (field, value) in eqs {
        let slot = PRIMARY_COLUMNS.iter().position(|column| *column == field);
        let (Some(slot), Some(code)) = (slot, unsigned_value(&value)) else {
            open.push((field, value));
            continue;
        };
        let bounds = struct_arr
            .unmasked_field_by_name(field)
            .ok()
            .and_then(stamped_bounds);
        match bounds.and_then(|(min, max)| eq_on_bounds(min, max, code)) {
            Some(false) => return Ok(Asked::Nothing),
            Some(true) => continue,
            None => {}
        }
        if let Some(zones) = memory.zones.get(slot) {
            zoned.push(ZonedEq {
                eq: open.len(),
                code,
                zones: Arc::clone(zones),
            });
        }
        open.push((field, value));
    }
    if open.is_empty() {
        return Ok(Asked::Everything);
    }
    let zones = ResidualZones::new(zoned, open.len(), selection, base.len());
    Ok(Asked::Open { eqs: open, zones })
}

/// How a residual reads a zone's selected rows, by its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Treatment {
    /// Skipped: no row holds every code.
    Skip,
    /// Taken uncompared: every row holds every code.
    Take,
    /// Compared on the equalities whose bits are set (by place in the
    /// residual's): those of the columns the zone leaves unsettled, and
    /// those no zone map speaks for.
    Compare(u32),
}

/// An equality of a residual whose column has a zone map: its place among
/// the residual's equalities, its code, and the map.
struct ZonedEq {
    eq: usize,
    code: u64,
    zones: Arc<ColumnZones>,
}

/// A residual's zones, classed once against its equalities: each zone its
/// selection touches is "none" (a column's zone cannot hold its code), "all"
/// (every column's zone holds only its code) or mixed, by the bounds' rule
/// of `x = c` and `g`'s recorded graphs ([`ColumnZones::eq_holds`]), and
/// kept as how its rows are read ([`Treatment`]). Built with the residual
/// ([`ask_statistics`]); a pending residual keeps it for every piece.
pub(crate) struct ResidualZones {
    /// The equalities whose columns have zone maps, of one zone geometry.
    columns: Vec<ZonedEq>,
    /// The first zone classed.
    first: u64,
    /// How the selected rows of each zone from `first` on, through the
    /// selection's last, are read.
    treatments: Vec<Treatment>,
    /// Every equality's bit, for a zone outside the classed ones.
    every: u32,
}

impl ResidualZones {
    /// The classes of the zones `selection` (of a base of `base_len` rows)
    /// touches, against `zoned`, of the residual's `eqs` equalities. `None`
    /// without a zoned equality, for zone maps of different geometries, for
    /// a selection of no row, or past 32 equalities.
    fn new(
        zoned: Vec<ZonedEq>,
        eqs: usize,
        selection: &RowSelection,
        base_len: usize,
    ) -> Option<Self> {
        let span = span(&selection.as_rows(base_len))?;
        let first_map = &zoned.first()?.zones;
        let (zone_len, zones) = (first_map.zone_len(), first_map.zones());
        if eqs > 32
            || zoned
                .iter()
                .any(|c| c.zones.zone_len() != zone_len || c.zones.zones() != zones)
        {
            return None;
        }
        let every = every_eq(eqs);
        let unzoned = zoned.iter().fold(every, |bits, c| bits & !(1 << c.eq));
        let first = span.start / zone_len;
        let end = span.end.div_ceil(zone_len).min(zones);
        observe!(ZonesClassed(end.saturating_sub(first)));
        let treatments = (first..end)
            .map(|zone| {
                let (mut none, mut unsettled) = (false, 0u32);
                for column in &zoned {
                    match column.zones.eq_holds(zone, column.code) {
                        Some(false) => none = true,
                        Some(true) => {}
                        None => unsettled |= 1 << column.eq,
                    }
                }
                match (none, unsettled | unzoned) {
                    (true, _) => Treatment::Skip,
                    (false, 0) => Treatment::Take,
                    (false, bits) => Treatment::Compare(bits),
                }
            })
            .collect();
        Some(Self {
            columns: zoned,
            first,
            treatments,
            every,
        })
    }

    /// Rows per zone, the last zone's excepted.
    fn zone_len(&self) -> u64 {
        self.columns[0].zones.zone_len()
    }

    /// The base's rows in zone `zone`.
    fn zone_rows(&self, zone: u64) -> Range<u64> {
        self.columns[0].zones.zone_rows(zone)
    }

    /// How zone `zone`'s selected rows are read: as its class says, a zone
    /// outside the classed ones compared on every equality.
    fn treatment(&self, zone: u64) -> Treatment {
        zone.checked_sub(self.first)
            .and_then(|at| self.treatments.get(at as usize))
            .copied()
            .unwrap_or(Treatment::Compare(self.every))
    }

    /// The zones `rows` touches, in order, each with its rows within
    /// `rows` and its treatment.
    fn zones_of(&self, rows: Range<u64>) -> impl Iterator<Item = (u64, Range<u64>, Treatment)> {
        let zone_len = self.zone_len();
        let zones = match rows.is_empty() {
            true => 0..0,
            false => rows.start / zone_len..(rows.end - 1) / zone_len + 1,
        };
        zones.map(move |zone| {
            let zone_rows = self.zone_rows(zone);
            let part = zone_rows.start.max(rows.start)..zone_rows.end.min(rows.end);
            (zone, part, self.treatment(zone))
        })
    }

    /// `rows` cut at zone boundaries, each part with its zone's
    /// treatment, adjacent parts of one treatment joined, in order.
    fn segments(&self, rows: Range<u64>) -> Vec<(Range<u64>, Treatment)> {
        let mut segments: Vec<(Range<u64>, Treatment)> = Vec::new();
        for (_, part, treatment) in self.zones_of(rows) {
            match segments.last_mut() {
                Some((run, joined)) if *joined == treatment && run.end == part.start => {
                    run.end = part.end;
                }
                _ => segments.push((part, treatment)),
            }
        }
        segments
    }

    /// The rows of zone `zone` holding equality `eq`'s code (by place in
    /// the residual's), as its column's zone recorded them; `None` where it
    /// records none or no zone map speaks for the equality.
    fn recorded_count(&self, eq: usize, zone: u64) -> Option<u64> {
        let column = self.columns.iter().find(|column| column.eq == eq)?;
        column.zones.recorded_count(zone, column.code)
    }
}

/// What a zone's selected rows hold of a residual's matches, had as cheaply
/// as they can be ([`zone_matches`]).
enum ZoneMatches {
    /// Their live matches, counted without reading them.
    Counted(usize),
    /// Their matches, compared (tombstoned rows among them).
    Read(RowSelection),
}

/// The matches of zone `zone`'s selected rows `part`, by its treatment:
/// none in a "none" zone, the live rows of an "all" zone, a mixed zone's
/// counted unread where it can be ([`unread_plan`]), else compared as part
/// of a residual reading `work` rows.
#[allow(clippy::too_many_arguments)]
fn zone_matches(
    memory: &MemoryBase,
    zones: &ResidualZones,
    zone: u64,
    part: &RowSelection,
    eqs: &[(&'static str, Scalar)],
    treatment: Treatment,
    deleted: Option<&Mask>,
    work: usize,
) -> Result<ZoneMatches> {
    let base_len = memory.base.len();
    Ok(match treatment {
        Treatment::Skip => ZoneMatches::Counted(0),
        Treatment::Take => {
            observe!(ResidualZoneTaken(part.len(base_len) as u64));
            ZoneMatches::Counted(live_rows(part, deleted, base_len))
        }
        Treatment::Compare(bits) => {
            match unread_plan(memory, zones, zone, part, eqs, bits, deleted) {
                Some(plan) => ZoneMatches::Counted(unread_count(plan)),
                None => ZoneMatches::Read(compared(
                    memory,
                    part.clone(),
                    &picked(eqs, bits),
                    work,
                    None,
                )?),
            }
        }
    })
}

/// The matches of a zone's selected rows `part` by its treatment, read:
/// none, every row, or those the compare keeps (tombstoned rows among
/// them), as part of a residual reading `work` rows.
fn matched_rows(
    memory: &MemoryBase,
    part: RowSelection,
    eqs: &[(&'static str, Scalar)],
    treatment: Treatment,
    work: usize,
) -> Result<RowSelection> {
    match treatment {
        Treatment::Skip => Ok(RowSelection::empty()),
        Treatment::Take => Ok(part),
        Treatment::Compare(bits) => compared(memory, part, &picked(eqs, bits), work, None),
    }
}

/// Whether the zones a residual compares, in `runs` runs holding
/// `compare_rows` of its `selected` rows, are compared as one selection
/// instead: several runs holding more than half of it. As the zones-off
/// route's `kept_runs` judges zone runs, comparing every row once costs
/// less than comparing such runs apart.
fn compares_whole(runs: usize, compare_rows: usize, selected: usize) -> bool {
    runs > 1 && compare_rows.saturating_mul(2) > selected
}

/// How many live rows of `selection` (of `memory`'s base, `deleted`
/// tombstoned) match every equality of `eqs`, by the residual's zone
/// classes and with no match materialized, stopping once `cap` are counted:
/// the count, or at least `cap`. A full count ([`full_count`]) plans every
/// zone first; a capped one goes zone by zone in base order
/// ([`zone_matches`]), as it may stop at its first.
pub(crate) fn zoned_count(
    memory: &MemoryBase,
    selection: &RowSelection,
    eqs: &[(&'static str, Scalar)],
    zones: &ResidualZones,
    deleted: Option<&Mask>,
    cap: usize,
) -> Result<usize> {
    let base_len = memory.base.len();
    let rows = selection.as_rows(base_len);
    let Some(span) = span(&rows) else {
        return Ok(0);
    };
    let work = selection.len(base_len);
    if cap == usize::MAX {
        return full_count(memory, selection, &rows, span, eqs, zones, deleted);
    }
    let mut count = 0;
    for (zone, run, treatment) in zones.zones_of(span) {
        if count >= cap {
            break;
        }
        let part = part_of(&rows, run);
        if part.is_empty(base_len) {
            continue;
        }
        count += match zone_matches(memory, zones, zone, &part, eqs, treatment, deleted, work)? {
            ZoneMatches::Counted(matches) => matches,
            ZoneMatches::Read(matched) => live_rows(&matched, deleted, base_len),
        };
    }
    Ok(count)
}

/// [`zoned_count`] of every match. Each zone is planned first: skipped,
/// taken, counted unread ([`unread_plan`]), or compared, adjacent zones
/// compared on the same equalities joined into one run. Runs to compare
/// that [`compares_whole`] judges fragmented compare the whole selection
/// once; otherwise the takes and unread counts are summed and each run
/// compared.
fn full_count(
    memory: &MemoryBase,
    selection: &RowSelection,
    rows: &SelectedRows<'_>,
    span: Range<u64>,
    eqs: &[(&'static str, Scalar)],
    zones: &ResidualZones,
    deleted: Option<&Mask>,
) -> Result<usize> {
    let base_len = memory.base.len();
    let selected = selection.len(base_len);
    let (mut taken, mut unread) = (Vec::new(), Vec::new());
    let mut runs: Vec<(Range<u64>, u32)> = Vec::new();
    for (zone, run, treatment) in zones.zones_of(span) {
        let part = part_of(rows, run.clone());
        if part.is_empty(base_len) {
            continue;
        }
        match treatment {
            Treatment::Skip => {}
            Treatment::Take => taken.push(part),
            Treatment::Compare(bits) => {
                match unread_plan(memory, zones, zone, &part, eqs, bits, deleted) {
                    Some(plan) => unread.push(plan),
                    None => match runs.last_mut() {
                        Some((last, joined)) if *joined == bits && last.end == run.start => {
                            last.end = run.end;
                        }
                        _ => runs.push((run, bits)),
                    },
                }
            }
        }
    }
    let compare_rows: usize = runs
        .iter()
        .map(|(run, _)| part_of(rows, run.clone()).len(base_len))
        .sum();
    if compares_whole(runs.len(), compare_rows, selected) {
        let matched = compared(memory, selection.clone(), eqs, selected, None)?;
        return Ok(live_rows(&matched, deleted, base_len));
    }
    let mut count = 0;
    for part in taken {
        observe!(ResidualZoneTaken(part.len(base_len) as u64));
        count += live_rows(&part, deleted, base_len);
    }
    count += unread.into_iter().map(unread_count).sum::<usize>();
    for (run, bits) in runs {
        let part = part_of(rows, run);
        let matched = compared(memory, part, &picked(eqs, bits), selected, None)?;
        count += live_rows(&matched, deleted, base_len);
    }
    Ok(count)
}

/// The live matches `offset..offset + limit` of `selection` (of `memory`'s
/// base, `deleted` tombstoned) in base row order, by the residual's zone
/// classes, beside the live matches as far as they are known: every one
/// when the window reaches the last, else at least `offset + limit`. The
/// zones before the window are counted ([`zone_matches`]), unread where
/// they can be; the matches are read from the zone holding the window's
/// first, a zone at a time, until the window's last.
pub(crate) fn zoned_window(
    memory: &MemoryBase,
    selection: &RowSelection,
    eqs: &[(&'static str, Scalar)],
    zones: &ResidualZones,
    offset: usize,
    limit: usize,
    deleted: Option<&Mask>,
) -> Result<(vortex_buffer::Buffer<RowId>, usize)> {
    let base_len = memory.base.len();
    let rows = selection.as_rows(base_len);
    let Some(span) = span(&rows) else {
        return Ok((vortex_buffer::Buffer::empty(), 0));
    };
    let want = offset.saturating_add(limit);
    let work = selection.len(base_len);
    // The live matches before the window's zone, and those read from it on.
    let (mut before, mut found) = (0usize, Vec::<RowId>::new());
    for (zone, run, treatment) in zones.zones_of(span) {
        let part = part_of(&rows, run);
        if part.is_empty(base_len) {
            continue;
        }
        if found.is_empty() {
            // A zone is counted only to be skipped: where the window starts
            // at the zone's first match, its matches are read at once.
            let counted = match before < offset {
                true => zone_matches(memory, zones, zone, &part, eqs, treatment, deleted, work)?,
                false => {
                    ZoneMatches::Read(matched_rows(memory, part.clone(), eqs, treatment, work)?)
                }
            };
            let matched = match counted {
                ZoneMatches::Counted(matches) if before + matches <= offset => {
                    before += matches;
                    continue;
                }
                ZoneMatches::Counted(_) => matched_rows(memory, part, eqs, treatment, work)?,
                ZoneMatches::Read(matched) => matched,
            };
            let live = live_ids(&matched, deleted, base_len);
            if before + live.len() <= offset {
                before += live.len();
                continue;
            }
            found = live;
        } else {
            let matched = matched_rows(memory, part, eqs, treatment, work)?;
            found.extend(live_ids(&matched, deleted, base_len));
        }
        if before + found.len() >= want {
            break;
        }
    }
    let start = offset.saturating_sub(before).min(found.len());
    let end = start.saturating_add(limit).min(found.len());
    Ok((
        vortex_buffer::Buffer::from_iter(found[start..end].iter().copied()),
        before + found.len(),
    ))
}

/// How a mixed zone's matches are counted without reading them
/// ([`unread_plan`]).
enum Unread {
    /// The zone's recorded count of the code, for a zone of `rows` rows.
    Recorded { rows: u64, matches: u64 },
    /// The column's encoding, through the store's probe of it, counted
    /// over the contiguous selected rows `run` for `code`.
    Encoded {
        column: Arc<vortex_rdf_encoded_search::OwnedSortedProbe>,
        code: u64,
        run: Range<usize>,
    },
}

/// How mixed zone `zone`'s selected rows `part` are counted unread, where
/// its one equality left to compare (`bits`) allows and no selected row is
/// tombstoned: the zone's recorded count of the code when the selection
/// holds the whole zone ([`ColumnZones::recorded_count`]), else the
/// column's encoding counted over the selected rows when they are
/// contiguous (`count_eq`). `None` when neither applies.
fn unread_plan(
    memory: &MemoryBase,
    zones: &ResidualZones,
    zone: u64,
    part: &RowSelection,
    eqs: &[(&'static str, Scalar)],
    bits: u32,
    deleted: Option<&Mask>,
) -> Option<Unread> {
    let base_len = memory.base.len();
    if bits.count_ones() != 1 || deleted.is_some_and(|d| tombstoned(part, d, base_len) > 0) {
        return None;
    }
    let eq = bits.trailing_zeros() as usize;
    let (field, value) = &eqs[eq];
    let code = unsigned_value(value)?;
    let zone_rows = zones.zone_rows(zone);
    let rows = zone_rows.end - zone_rows.start;
    if part.len(base_len) as u64 == rows
        && let Some(matches) = zones.recorded_count(eq, zone)
    {
        return Some(Unread::Recorded { rows, matches });
    }
    let SelectedRows::Run(run) = part.as_rows(base_len) else {
        return None;
    };
    let column = Arc::clone(memory.probes.by_name(&memory.base, field).flatten()?);
    Some(Unread::Encoded { column, code, run })
}

/// The matches `plan` counts.
fn unread_count(plan: Unread) -> usize {
    match plan {
        Unread::Recorded { rows, matches } => {
            // The zone's rows feed the test event alone.
            let _ = rows;
            observe!(ZoneRecorded(rows));
            matches as usize
        }
        Unread::Encoded { column, code, run } => {
            observe!(ZoneCounted(run.len() as u64));
            column.probe().count_eq(run.start, run.len(), code)
        }
    }
}

/// The equalities of `eqs` whose bits `bits` sets, in order.
fn picked(eqs: &[(&'static str, Scalar)], bits: u32) -> Vec<(&'static str, Scalar)> {
    eqs.iter()
        .enumerate()
        .filter(|(i, _)| bits & (1 << i) != 0)
        .map(|(_, eq)| eq.clone())
        .collect()
}

/// How many of `selection`'s rows `deleted` tombstones.
fn tombstoned(selection: &RowSelection, deleted: &Mask, base_len: usize) -> usize {
    match selection.as_rows(base_len) {
        SelectedRows::Run(run) => deleted.slice(run).true_count(),
        SelectedRows::Ids(ids) => ids.iter().filter(|&&id| deleted.value(id as usize)).count(),
    }
}

/// `selection`'s rows `deleted` leaves live, in order.
fn live_ids(selection: &RowSelection, deleted: Option<&Mask>, base_len: usize) -> Vec<RowId> {
    let mut ids = Vec::with_capacity(selection.len(base_len));
    extend_rows(&mut ids, selection, base_len);
    if let Some(deleted) = deleted {
        ids.retain(|&id| !deleted.value(id as usize));
    }
    ids
}

/// How many of `selection`'s rows `deleted` leaves live.
fn live_rows(selection: &RowSelection, deleted: Option<&Mask>, base_len: usize) -> usize {
    selection.len(base_len) - deleted.map_or(0, |d| tombstoned(selection, d, base_len))
}

/// One bit for each of `eqs` equalities.
fn every_eq(eqs: usize) -> u32 {
    if eqs >= 32 { u32::MAX } else { (1 << eqs) - 1 }
}

/// The base rows from a selection's first to its last, `None` for a
/// selection of no row.
fn span(rows: &SelectedRows<'_>) -> Option<Range<u64>> {
    match rows {
        SelectedRows::Run(run) if !run.is_empty() => Some(run.start as u64..run.end as u64),
        SelectedRows::Ids(ids) => Some(*ids.first()?..*ids.last()? + 1),
        SelectedRows::Run(_) => None,
    }
}

/// The selected rows within `run`, as a selection of their own.
fn part_of(rows: &SelectedRows<'_>, run: Range<u64>) -> RowSelection {
    match rows {
        SelectedRows::Run(_) => RowSelection::Range(run),
        SelectedRows::Ids(ids) => {
            let start = ids.partition_point(|&id| id < run.start);
            let end = ids.partition_point(|&id| id < run.end);
            RowSelection::Ids(ids.slice(start..end))
        }
    }
}

/// Append `selection`'s rows to `ids`, in order.
fn extend_rows(ids: &mut Vec<RowId>, selection: &RowSelection, base_len: usize) {
    match selection.as_rows(base_len) {
        SelectedRows::Run(run) => ids.extend(run.start as RowId..run.end as RowId),
        SelectedRows::Ids(rows) => ids.extend_from_slice(rows),
    }
}

/// The rows of `selection` (of `memory`'s base) where every equality of
/// `eqs` holds. With the residual's zone classes (`zones`), zone by zone:
/// "none" zones skipped, the rows of "all" zones taken uncompared, and
/// mixed zones compared on the equalities they leave unsettled. Without
/// them (zones off, or no equality's column has a zone map), the zones
/// whose bounds cannot hold a code are skipped, as before the classes, and
/// the rest compared: the typed fast path or the mask scan.
pub(crate) fn residual_rows(
    memory: &MemoryBase,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
    zones: Option<&ResidualZones>,
    t: Option<Instant>,
) -> Result<RowSelection> {
    if let Some(zones) = zones {
        return zoned_rows(memory, selection, eqs, zones, t);
    }
    let MemoryBase { base, zones, .. } = memory;
    // The zones whose code bounds cannot hold a constrained code are
    // skipped, as a file's zone maps skip them. Every row or a range has
    // the zone runs left inside it compared run by run, each a slice of
    // the base, with no id list of the rows left (those runs are many
    // where a code's zones scatter); an id list keeps only its ids in them.
    let mut selection = selection;
    if asks_statistics(selection.len(base.len()))
        && let Some(runs) = zones.kept_runs(eqs, base.len())?
    {
        observe!(ZoneNarrowed);
        debug::step!(t, "[match_pattern] In-memory narrowed by zone bounds");
        match selection.clip_runs(&runs) {
            Some(runs) => {
                let ids = runs_matching(base, &runs, eqs)?;
                return Ok(RowSelection::from_ids(vortex_buffer::Buffer::from(ids)));
            }
            None => selection = selection.intersect_runs(&runs),
        }
        if selection.is_empty(base.len()) {
            return Ok(selection);
        }
    }
    let work = selection.len(base.len());
    compared(memory, selection, eqs, work, t)
}

/// [`residual_rows`] by the residual's zone classes.
fn zoned_rows(
    memory: &MemoryBase,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
    zones: &ResidualZones,
    t: Option<Instant>,
) -> Result<RowSelection> {
    let base_len = memory.base.len();
    let rows = selection.as_rows(base_len);
    let Some(span) = span(&rows) else {
        return Ok(selection);
    };
    let segments = zones.segments(span);
    if segments
        .iter()
        .all(|(_, treatment)| *treatment == Treatment::Take)
    {
        observe!(ResidualZoneTaken(selection.len(base_len) as u64));
        debug::step!(t, "[match_pattern] In-memory residual: every zone taken");
        return Ok(selection);
    }
    let selected = selection.len(base_len);
    let compare: Vec<usize> = segments
        .iter()
        .filter(|(_, treatment)| matches!(treatment, Treatment::Compare(_)))
        .map(|(run, _)| part_of(&rows, run.clone()).len(base_len))
        .collect();
    if compares_whole(compare.len(), compare.iter().sum(), selected) {
        return compared(memory, selection.clone(), eqs, selected, t);
    }
    if segments
        .iter()
        .any(|(_, treatment)| *treatment == Treatment::Skip)
    {
        observe!(ZoneNarrowed);
        debug::step!(t, "[match_pattern] In-memory narrowed by zone classes");
    }
    let mut ids: Vec<RowId> = Vec::new();
    for (run, treatment) in segments {
        let part = part_of(&rows, run);
        if part.is_empty(base_len) {
            continue;
        }
        match treatment {
            Treatment::Skip => {}
            Treatment::Take => {
                observe!(ResidualZoneTaken(part.len(base_len) as u64));
                extend_rows(&mut ids, &part, base_len);
            }
            Treatment::Compare(bits) => {
                let matched = compared(memory, part, &picked(eqs, bits), selected, t)?;
                extend_rows(&mut ids, &matched, base_len);
            }
        }
    }
    Ok(RowSelection::from_ids(vortex_buffer::Buffer::from(ids)))
}

/// The rows of `selection` where every equality of `eqs` holds, compared:
/// the typed fast path, else the mask scan. `selection` is part of a
/// residual reading `work` rows in all, which decides whether the compare
/// decodes its columns ([`typed_residual_ids_reading`]).
fn compared(
    memory: &MemoryBase,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
    work: usize,
    t: Option<Instant>,
) -> Result<RowSelection> {
    let MemoryBase { base, probes, .. } = memory;
    let struct_arr = into_struct_array(base.clone())?;
    observe!(ResidualCompared(selection.len(base.len()) as u64));
    // Typed fast path: residual equalities over canonical code columns
    // (Dictionary layout) become slice loops yielding exact base ids,
    // with no slice/compare/mask pipeline, and a probe-bound column reads
    // through the store's own probe. Only the selection's rows are
    // tested, so the ids are already the intersection.
    Ok(
        match typed_residual_ids_reading(
            &struct_arr,
            &selection,
            base.len(),
            eqs,
            Some((base, probes)),
            work,
        ) {
            Some(ids) => {
                debug::step!(
                    t,
                    "[match_pattern] In-memory narrowed by typed residual scan"
                );
                RowSelection::Ids(ids)
            }
            None => {
                let mask = mask_for(&selection.apply(base)?, eqs)?;
                let narrowed = selection.refine(&bool_array_to_mask(mask)?);
                debug::step!(t, "[match_pattern] In-memory narrowed by mask scan");
                narrowed
            }
        },
    )
}

/// The base rows of `runs` that satisfy `eqs`, ascending: each run's
/// slice of the base compared on the constrained columns alone
/// ([`mask_for`]).
fn runs_matching(
    base: &ArrayRef,
    runs: &[std::ops::Range<u64>],
    eqs: &[(&'static str, Scalar)],
) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for run in runs {
        observe!(ResidualCompared(run.end - run.start));
        let rows = base.slice(run.start as usize..run.end as usize)?;
        match bool_array_to_mask(mask_for(&rows, eqs)?)?.bit_buffer() {
            vortex_mask::AllOr::All => ids.extend(run.clone()),
            vortex_mask::AllOr::None => {}
            vortex_mask::AllOr::Some(bits) => {
                bits.for_each_set_index(|i| ids.push(run.start + i as u64));
            }
        }
    }
    Ok(ids)
}

/// A boolean mask, one bit per row of `array` in its own order, of the
/// rows that satisfy `eqs`: the per-column equalities a pattern compiled
/// to under the layout of `array`'s rows (the store's for the base, the
/// tail's for the tail; see [`Tail::layout_for`](crate::store::Tail::layout_for)). The mask is
/// positional, so a caller holding a view maps it back to base row ids
/// with [`RowSelection::refine`]. The fallback of the typed comparison
/// paths: callers pass in the constraints they compiled to try those.
pub(crate) fn mask_for(array: &ArrayRef, eqs: &[(&'static str, Scalar)]) -> Result<ArrayRef> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = array.clone().execute::<StructArray>(&mut ctx)?;

    // One equality comparison per constrained column, ANDed.
    let mut mask: Option<ArrayRef> = None;
    for (field, value) in eqs {
        let col = struct_arr.unmasked_field_by_name(field)?;
        // Cast to the column's dtype, so a numeric column such as
        // `o_kind` compares against a scalar of its type and nullability.
        let scalar = value.cast(col.dtype())?;
        let rhs = ConstantArray::new(scalar, col.len()).into_array();
        let m = col.binary(rhs, Operator::Eq)?;
        mask = Some(match mask.take() {
            Some(prev) => prev.binary(m, Operator::And)?,
            None => m,
        });
    }
    Ok(match mask {
        Some(mask) => mask,
        // An unconstrained pattern matches every row. Both callers gate on
        // a non-empty constraint set, so the fold always starts.
        None => ConstantArray::new(Scalar::from(true), struct_arr.len()).into_array(),
    })
}
