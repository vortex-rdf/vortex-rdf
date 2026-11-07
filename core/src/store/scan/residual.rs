//! The residual compare over an in-memory table ([`ResidualCompare`]): the
//! equalities of a pattern no prefix search or index answered, over a
//! selection of a table's rows, the base's or an index component's. The
//! statistics settle what they can first ([`ask_statistics`]: the base
//! columns' whole bounds, then their zones, which their zone maps keep, and
//! against which the zones a selection touches are classed, [`Classes`]). Then one walk counts the
//! matches ([`count`](ResidualCompare::count)), takes a window of them
//! ([`window`](ResidualCompare::window)), or finds every one
//! ([`rows`](ResidualCompare::rows)):
//! - zone by zone where the zones are classed: "none" zones skipped, "all"
//!   zones taken, mixed ones counted unread where a record or the column's
//!   encoding counts them, else compared;
//! - in growing pieces where they are not (zones off, or no zone map), each
//!   piece compared, skipping the zones whose bounds rule a code out where
//!   the base keeps their zone maps.
//!
//! A compare is the typed fast path or the mask scan over the base, and the
//! graph column's encoding over a component. The mask of an array's rows
//! holding the equalities ([`mask_for`]) is the tail's match's compare too.
//! These are functions of the table, with no store state. The match narrows
//! a selection with them, and two callers walk them only as far as they
//! need: a residual compare left pending
//! ([`PendingResidual`](crate::store::view::pending::PendingResidual)) and an
//! index run's graph check
//! ([`GraphCheck`](crate::store::indexes::graph_check::GraphCheck)).

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::constant::ConstantArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_mask::Mask;
use web_time::Instant;

use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::MemoryBase;
use crate::store::array::{bool_array_to_mask, into_struct_array, unsigned_value};
use crate::store::probes::StructProbes;
use crate::store::read::live::{Live, WindowCursor};
use crate::store::scan::pieces::{FIRST_PIECE_ROWS, growing_pieces};
use crate::store::scan::typed_eq::typed_residual_ids_reading;
use crate::store::schema::{RowId, primary_slot};
use crate::store::view::selection::{RowSelection, SelectedRows};
use crate::zone_stats::zone_map::{ZONE_ROWS, eq_on_bounds};
use crate::zone_stats::zone_set::{ZoneCheck, ZoneClass, ZonePart, ZoneSet};

/// Whether a residual over a selection of `rows` rows asks the statistics
/// before it compares: past one zone's rows, as below them the compare
/// costs less than the statistics' answer.
pub(crate) fn asks_statistics(rows: usize) -> bool {
    rows > ZONE_ROWS
}

/// What the statistics settle of a residual before its rows are compared
/// ([`ask_statistics`]): the columns' whole bounds, then their zones.
pub(crate) enum Asked {
    /// No base row holds an equality's code: the columns' bounds.
    Nothing,
    /// Every equality holds on every base row: the columns' bounds.
    Everything,
    /// The equalities the columns' bounds leave, in order, and how the
    /// zones the selection touches are classed against them ([`Classes`]).
    Open {
        eqs: Vec<(&'static str, Scalar)>,
        classes: Classes,
    },
}

/// The statistics a residual over `selection` (of `memory`'s base) asks
/// once, each equality of `eqs` by the bounds' rule of `x = code`
/// ([`eq_on_bounds`]): first against its column's exact bounds, which its
/// zone map keeps ([`ColumnZones::column_bounds`]), then the equalities left
/// against their columns' zones and `g`'s recorded graphs
/// ([`Classes::Zoned`]); where those decline, the pieces of a residual whose
/// statistics are not asked ([`unasked_classes`]). A column without a zone
/// map has neither level.
///
/// [`ColumnZones::column_bounds`]: crate::zone_stats::zone_map::ColumnZones::column_bounds
pub(crate) fn ask_statistics(
    memory: &MemoryBase,
    selection: &RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
) -> Result<Asked> {
    let base = &memory.base;
    let mut open = Vec::with_capacity(eqs.len());
    let mut zoned = Vec::new();
    for (field, value) in eqs {
        let (Some(slot), Some(code)) = (primary_slot(field), unsigned_value(&value)) else {
            open.push((field, value));
            continue;
        };
        let zones = memory.zones.get(slot);
        let bounds = zones.and_then(|zones| zones.column_bounds());
        match bounds.and_then(|(min, max)| eq_on_bounds(min, max, code)) {
            Some(false) => return Ok(Asked::Nothing),
            Some(true) => continue,
            None => {}
        }
        if let Some(zones) = zones {
            zoned.push(ZoneCheck {
                bit: open.len() as u32,
                code,
                zones: Arc::clone(zones),
            });
        }
        open.push((field, value));
    }
    if open.is_empty() {
        return Ok(Asked::Everything);
    }
    let classes = Classes::zoned(zoned, open.len(), selection, base.len())
        .unwrap_or_else(|| unasked_classes(memory, &open));
    Ok(Asked::Open { eqs: open, classes })
}

/// The classes of a residual over `memory`'s base whose statistics are not
/// asked (zones off, or a selection of no more than a zone's rows): growing
/// pieces, whose compares skip the zones the bounds of `eqs`' columns rule
/// out where the base keeps their zone maps ([`ZoneSet::bounds_only`]), as
/// before the zone classes.
pub(crate) fn unasked_classes(memory: &MemoryBase, eqs: &[(&'static str, Scalar)]) -> Classes {
    let checks: Vec<ZoneCheck> = eqs
        .iter()
        .enumerate()
        .filter_map(|(at, (field, value))| {
            Some(ZoneCheck {
                bit: u32::try_from(at).ok()?,
                code: unsigned_value(value)?,
                zones: Arc::clone(memory.zones.get(primary_slot(field)?)?),
            })
        })
        .collect();
    let unzoned = checks
        .iter()
        .fold(every_eq(eqs.len()), |bits, check| bits & !(1 << check.bit));
    Classes::Pieces {
        bounds: ZoneSet::new(checks, unzoned).map(ZoneSet::bounds_only),
    }
}

/// How a residual's selection is walked ([`ask_statistics`],
/// [`unasked_classes`]).
pub(crate) enum Classes {
    /// Zone by zone: each equality whose column has a zone map is a check,
    /// named by its place among the residual's equalities, the rest
    /// unzoned, so a zone's class ([`ZoneClass`]) is how its selected rows
    /// are read: skipped ("none", a column's zone cannot hold its code),
    /// taken uncompared ("all", every column's zone holds only its code),
    /// or compared on the equalities it leaves unsettled.
    Zoned(ZoneSet),
    /// In growing pieces of the selection ([`growing_pieces`]), each
    /// compared on every equality, none counted unread: zones off, or no
    /// equality's column with a zone map. Where `bounds` holds the
    /// equalities' checks by their bounds alone, a piece past a zone's rows
    /// skips the zones they rule out.
    Pieces { bounds: Option<ZoneSet> },
}

impl Classes {
    /// The checks `zoned` of a residual's `eqs` equalities, over the zones
    /// `selection` (of a base of `base_len` rows) touches. `None` without a
    /// zoned equality, for zone maps of different geometries, for a
    /// selection of no row, or past 32 equalities.
    fn zoned(
        zoned: Vec<ZoneCheck>,
        eqs: usize,
        selection: &RowSelection,
        base_len: usize,
    ) -> Option<Self> {
        let _span = span(&selection.as_rows(base_len))?;
        if eqs > 32 {
            return None;
        }
        let unzoned = zoned
            .iter()
            .fold(every_eq(eqs), |bits, check| bits & !(1 << check.bit));
        let set = ZoneSet::new(zoned, unzoned)?;
        observe!(ZonesClassed(
            _span.end.div_ceil(set.zone_len()).min(set.zones()) - _span.start / set.zone_len()
        ));
        Some(Self::Zoned(set))
    }
}

/// The table a residual compares: its rows, a struct of columns (the
/// base's, or an index component's), and the encoded-search probes over
/// them, resolved for those rows ([`StructProbes`]).
#[derive(Clone, Copy)]
pub(crate) struct ResidualTable<'a> {
    pub(crate) rows: &'a ArrayRef,
    pub(crate) probes: &'a StructProbes,
    /// Whether the rows are an index component's: compared on their one
    /// column's encoding ([`component_compared`]) and observed apart from
    /// the base's.
    pub(crate) component: bool,
}

impl<'a> ResidualTable<'a> {
    /// `memory`'s base.
    pub(crate) fn base(memory: &'a MemoryBase) -> Self {
        Self {
            rows: &memory.base,
            probes: &memory.probes,
            component: false,
        }
    }
}

/// A residual compare: the rows of `selection` (of `table`'s) where every
/// equality of `eqs` holds, walked as `classes` say, the rows `deleted`
/// tombstones left out of its live counts and windows.
pub(crate) struct ResidualCompare<'a> {
    pub(crate) table: ResidualTable<'a>,
    pub(crate) selection: &'a RowSelection,
    pub(crate) eqs: &'a [(&'static str, Scalar)],
    pub(crate) classes: &'a Classes,
    pub(crate) deleted: Option<&'a Mask>,
    /// The match's timer, for its debug steps.
    pub(crate) t: Option<Instant>,
}

/// A part of a residual's selection read as one: a zone's selected rows, or
/// a piece's.
struct Part {
    /// The selected rows of the part.
    selected: RowSelection,
    /// How they are read.
    class: ZoneClass,
    /// The zone they lie in, its rows within the selection's span, when the
    /// classes are zones'.
    zone: Option<ZonePart>,
}

impl ResidualCompare<'_> {
    /// The table's rows.
    fn len(&self) -> usize {
        self.table.rows.len()
    }

    /// The selection's rows, as a run or ascending ids.
    fn selected_rows(&self) -> SelectedRows<'_> {
        self.selection.as_rows(self.len())
    }

    /// How many rows the selection holds: the residual's work, which picks
    /// whether a compare decodes its columns ([`typed_residual_ids_reading`]).
    fn work(&self) -> usize {
        self.selection.len(self.len())
    }

    /// How many matches `matched` (selected rows) holds that are live.
    fn live(&self, matched: &RowSelection) -> usize {
        live_rows(matched, self.deleted, self.len())
    }

    /// The parts of the selection's `span`, in table order: its zones' rows
    /// with their classes, or growing pieces of it, a zone's rows first and
    /// four times as many each after, or every row when no more than `want`
    /// are selected, as no count or window of `want` rows could stop short
    /// of them.
    fn parts(&self, span: Range<u64>, want: usize) -> Box<dyn Iterator<Item = Part> + '_> {
        let rows = self.selected_rows();
        let len = self.len();
        match self.classes {
            Classes::Zoned(set) => Box::new(set.parts(span).filter_map(move |zone| {
                let selected = part_of(&rows, zone.rows.clone());
                (!selected.is_empty(len)).then(|| Part {
                    selected,
                    class: set.class(zone.zone),
                    zone: Some(zone),
                })
            })),
            Classes::Pieces { .. } => {
                let total = self.work();
                let first = if total <= want {
                    total
                } else {
                    FIRST_PIECE_ROWS
                };
                let every = every_eq(self.eqs.len());
                Box::new(
                    growing_pieces(std::iter::once(0..total as u64), first as u64).map(
                        move |(_, piece)| Part {
                            selected: rows_at(&rows, piece.start as usize..piece.end as usize),
                            class: ZoneClass::Mixed { unsettled: every },
                            zone: None,
                        },
                    ),
                )
            }
        }
    }

    /// How many live rows match, stopping once `cap` are counted (`None`:
    /// every one): the count, or at least `cap` and never more than the
    /// count. A capped count goes part by part, each counted unread where
    /// it can be ([`unread`](Self::unread)) and compared otherwise, as it may
    /// stop at its first. A full count by zones plans every zone first
    /// ([`full_count`](Self::full_count)); one in pieces compares the whole
    /// selection once.
    pub(crate) fn count(&self, cap: Option<usize>) -> Result<usize> {
        let Some(span) = span(&self.selected_rows()) else {
            return Ok(0);
        };
        match (cap, self.classes) {
            (None, Classes::Zoned(set)) => self.full_count(set, span),
            (None, Classes::Pieces { .. }) => Ok(self.live(&self.rows()?)),
            (Some(cap), _) => {
                let mut count = 0;
                for part in self.parts(span, cap) {
                    if count >= cap {
                        break;
                    }
                    count += match self.unread(&part) {
                        Some(matches) => matches,
                        None => self.live(&self.matched(&part)?),
                    };
                }
                Ok(count)
            }
        }
    }

    /// [`count`](Self::count) of every match, by zones. Each zone is
    /// planned first: skipped, taken, counted unread ([`unread_plan`]), or
    /// compared, adjacent zones compared on the same equalities joined into
    /// one run. Runs to compare that [`compares_whole`] judges fragmented
    /// compare the whole selection once; otherwise the takes and unread
    /// counts are summed and each run compared.
    fn full_count(&self, set: &ZoneSet, span: Range<u64>) -> Result<usize> {
        let rows = self.selected_rows();
        let (len, selected) = (self.len(), self.work());
        let (mut taken, mut unread) = (Vec::new(), Vec::new());
        let mut runs: Vec<(Range<u64>, u32)> = Vec::new();
        for zone in set.parts(span) {
            let part = part_of(&rows, zone.rows.clone());
            if part.is_empty(len) {
                continue;
            }
            match set.class(zone.zone) {
                ZoneClass::None => {}
                ZoneClass::All => taken.push(part),
                ZoneClass::Mixed { unsettled } => {
                    match self.unread_plan(set, &zone, &part, unsettled) {
                        Some(plan) => unread.push(plan),
                        None => match runs.last_mut() {
                            Some((last, joined))
                                if *joined == unsettled && last.end == zone.rows.start =>
                            {
                                last.end = zone.rows.end;
                            }
                            _ => runs.push((zone.rows, unsettled)),
                        },
                    }
                }
            }
        }
        let compare_rows: usize = runs
            .iter()
            .map(|(run, _)| part_of(&rows, run.clone()).len(len))
            .sum();
        if compares_whole(runs.len(), compare_rows, selected) {
            let every = every_eq(self.eqs.len());
            return Ok(self.live(&self.compare(self.selection.clone(), every)?));
        }
        let mut count = 0;
        for part in taken {
            self.observe_taken(&part);
            count += self.live(&part);
        }
        count += unread.into_iter().map(unread_count).sum::<usize>();
        for (run, bits) in runs {
            count += self.live(&self.compare(part_of(&rows, run), bits)?);
        }
        Ok(count)
    }

    /// The live matches `offset..offset + limit`, in table order, beside the
    /// live matches as far as they are known ([`Live`]): every one when the
    /// window reaches the last. Each part is counted unread first where it
    /// can be, so a part counted empty is never read and the parts before
    /// the window are skipped by their counts; the matches are read from the
    /// part holding the window's first, a part at a time, until the window
    /// is full. A window of no rows reads nothing.
    pub(crate) fn window(&self, offset: usize, limit: usize) -> Result<(Buffer<RowId>, Live)> {
        let Some(span) = span(&self.selected_rows()) else {
            return Ok((Buffer::empty(), Live::Exact(0)));
        };
        let mut cursor = WindowCursor::new(offset, Some(limit));
        let mut ended = true;
        for part in self.parts(span, offset.saturating_add(limit)) {
            if cursor.is_full() {
                ended = false;
                break;
            }
            match self.unread(&part) {
                Some(0) => continue,
                Some(matches) if cursor.skips(matches) => continue,
                _ => {}
            }
            let matched = self.matched(&part)?;
            cursor.take(&live_ids(&matched, self.deleted, self.len()));
        }
        let (ids, live) = cursor.finish(ended);
        Ok((Buffer::from(ids), live))
    }

    /// Every match, in table order, tombstoned rows among them: zone by zone
    /// ([`rows_by`](Self::rows_by)), or in pieces the whole selection
    /// compared once, skipping the zones its bounds rule out where it may.
    pub(crate) fn rows(&self) -> Result<RowSelection> {
        match self.classes {
            Classes::Zoned(set) => self.rows_by(set, self.selection.clone()),
            Classes::Pieces { bounds } => self.pieces_compared(bounds, self.selection.clone()),
        }
    }

    /// A part's matches counted unread, where a part of zones' classes can
    /// be: none in a "none" zone, the live rows of an "all" zone, a mixed
    /// zone's from its record or its column's encoding ([`unread_plan`]).
    /// `None` for a part that must be compared, and for every piece.
    fn unread(&self, part: &Part) -> Option<usize> {
        let (Classes::Zoned(set), Some(zone)) = (self.classes, &part.zone) else {
            return None;
        };
        match part.class {
            ZoneClass::None => Some(0),
            ZoneClass::All => {
                self.observe_taken(&part.selected);
                Some(self.live(&part.selected))
            }
            ZoneClass::Mixed { unsettled } => self
                .unread_plan(set, zone, &part.selected, unsettled)
                .map(unread_count),
        }
    }

    /// How a mixed zone's selected rows `part` are counted unread, where its
    /// one equality left to compare (`bits`) allows and no selected row is
    /// tombstoned: the zone's recorded count of the code when the selection
    /// holds the whole zone ([`ZoneSet::recorded_in`]), else the column's
    /// encoding counted over the selected rows when they are contiguous
    /// (`count_eq`). `None` when neither applies.
    fn unread_plan(
        &self,
        set: &ZoneSet,
        zone: &ZonePart,
        part: &RowSelection,
        bits: u32,
    ) -> Option<Unread> {
        let len = self.len();
        if bits.count_ones() != 1 || self.deleted.is_some_and(|d| tombstoned(part, d, len) > 0) {
            return None;
        }
        let eq = bits.trailing_zeros();
        let (field, value) = &self.eqs[eq as usize];
        let code = unsigned_value(value)?;
        let whole = set.zone_rows(zone.zone);
        let rows = whole.end - whole.start;
        if part.len(len) as u64 == rows
            && let Some(matches) = set.recorded_in(
                eq,
                &ZonePart {
                    zone: zone.zone,
                    rows: whole,
                },
            )
        {
            return Some(Unread::Recorded { rows, matches });
        }
        let SelectedRows::Run(run) = part.as_rows(len) else {
            return None;
        };
        let column = Arc::clone(
            self.table
                .probes
                .by_name(self.table.rows, field)
                .flatten()?,
        );
        Some(Unread::Encoded { column, code, run })
    }

    /// A part's matches, read: none, every row, or those the compare keeps
    /// (tombstoned rows among them).
    fn matched(&self, part: &Part) -> Result<RowSelection> {
        match (part.class, self.classes) {
            (ZoneClass::None, _) => Ok(RowSelection::empty()),
            (ZoneClass::All, _) => Ok(part.selected.clone()),
            (ZoneClass::Mixed { .. }, Classes::Pieces { bounds }) => {
                observe!(ResidualPiece(part.selected.len(self.len())));
                self.pieces_compared(bounds, part.selected.clone())
            }
            (ZoneClass::Mixed { unsettled }, Classes::Zoned(_)) => {
                self.compare(part.selected.clone(), unsettled)
            }
        }
    }

    /// The matches of `selection` (a piece, or the whole selection) on every
    /// equality: by the zones `bounds` leaves past a zone's rows, as before
    /// the zone classes, else compared whole.
    fn pieces_compared(
        &self,
        bounds: &Option<ZoneSet>,
        selection: RowSelection,
    ) -> Result<RowSelection> {
        match bounds {
            Some(set) if asks_statistics(selection.len(self.len())) => self.rows_by(set, selection),
            _ => self.compare(selection, every_eq(self.eqs.len())),
        }
    }

    /// The matches of `selection`, zone by zone by `set`'s classes: "none"
    /// zones skipped, the rows of "all" zones taken uncompared, and mixed
    /// zones compared on the equalities they leave unsettled, adjacent zones
    /// of one class compared as one run, or the whole selection compared
    /// once where those runs are fragmented ([`compares_whole`]).
    fn rows_by(&self, set: &ZoneSet, selection: RowSelection) -> Result<RowSelection> {
        let len = self.len();
        let rows = selection.as_rows(len);
        let Some(span) = span(&rows) else {
            return Ok(selection);
        };
        let mut segments: Vec<(Range<u64>, ZoneClass)> = Vec::new();
        for zone in set.parts(span) {
            let class = set.class(zone.zone);
            match segments.last_mut() {
                Some((run, joined)) if *joined == class && run.end == zone.rows.start => {
                    run.end = zone.rows.end;
                }
                _ => segments.push((zone.rows, class)),
            }
        }
        if segments.iter().all(|(_, class)| *class == ZoneClass::All) {
            self.observe_taken(&selection);
            if !self.table.component {
                debug::step!(
                    self.t,
                    "[match_pattern] In-memory residual: every zone taken"
                );
            }
            return Ok(selection);
        }
        let selected = selection.len(len);
        let compare: Vec<usize> = segments
            .iter()
            .filter(|(_, class)| matches!(class, ZoneClass::Mixed { .. }))
            .map(|(run, _)| part_of(&rows, run.clone()).len(len))
            .collect();
        if compares_whole(compare.len(), compare.iter().sum(), selected) {
            return self.compare(selection.clone(), every_eq(self.eqs.len()));
        }
        if !self.table.component && segments.iter().any(|(_, class)| *class == ZoneClass::None) {
            observe!(ZoneNarrowed);
            debug::step!(self.t, "[match_pattern] In-memory narrowed by zone classes");
        }
        let mut ids: Vec<RowId> = Vec::new();
        for (run, class) in segments {
            let part = part_of(&rows, run);
            if part.is_empty(len) {
                continue;
            }
            match class {
                ZoneClass::None => {}
                ZoneClass::All => {
                    self.observe_taken(&part);
                    extend_rows(&mut ids, &part, len);
                }
                ZoneClass::Mixed { unsettled } => {
                    let matched = self.compare(part, unsettled)?;
                    extend_rows(&mut ids, &matched, len);
                }
            }
        }
        Ok(RowSelection::from_ids(Buffer::from(ids)))
    }

    /// The rows of `selection` where the equalities `bits` names hold,
    /// compared: on a component's column encoding, else by the base's typed
    /// fast path or mask scan.
    fn compare(&self, selection: RowSelection, bits: u32) -> Result<RowSelection> {
        let eqs = picked(self.eqs, bits);
        if self.table.component {
            return component_compared(self.table, selection, &eqs);
        }
        compared(self.table, selection, &eqs, self.work(), self.t)
    }

    /// The test event of a part taken uncompared, of the base's rows.
    fn observe_taken(&self, _part: &RowSelection) {
        if !self.table.component {
            observe!(ResidualZoneTaken(_part.len(self.len()) as u64));
        }
    }
}

/// Whether the zones a residual compares, in `runs` runs holding
/// `compare_rows` of its `selected` rows, are compared as one selection
/// instead: several runs holding more than half of it, as comparing every
/// row once costs less than comparing such runs apart.
fn compares_whole(runs: usize, compare_rows: usize, selected: usize) -> bool {
    runs > 1 && compare_rows.saturating_mul(2) > selected
}

/// How a mixed zone's matches are counted without reading them
/// ([`ResidualCompare::unread`]).
enum Unread {
    /// The zone's recorded count of the code, for a zone of `rows` rows.
    Recorded { rows: u64, matches: u64 },
    /// The column's encoding, through the table's probe of it, counted over
    /// the contiguous selected rows `run` for `code`.
    Encoded {
        column: Arc<vortex_rdf_encoded_search::OwnedSortedProbe>,
        code: u64,
        run: Range<usize>,
    },
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

/// Append `selection`'s rows to `ids`, in order.
fn extend_rows(ids: &mut Vec<RowId>, selection: &RowSelection, base_len: usize) {
    match selection.as_rows(base_len) {
        SelectedRows::Run(run) => ids.extend(run.start as RowId..run.end as RowId),
        SelectedRows::Ids(rows) => ids.extend_from_slice(rows),
    }
}

/// The rows of `selection` (of `table`'s base) where every equality of `eqs`
/// holds, compared: the typed fast path, else the mask scan. `selection` is
/// part of a residual reading `work` rows in all, which decides whether the
/// compare decodes its columns ([`typed_residual_ids_reading`]).
fn compared(
    table: ResidualTable<'_>,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
    work: usize,
    t: Option<Instant>,
) -> Result<RowSelection> {
    let len = table.rows.len();
    let struct_arr = into_struct_array(table.rows.clone())?;
    observe!(ResidualCompared(selection.len(len) as u64));
    // Typed fast path: residual equalities over canonical code columns
    // (Dictionary layout) become slice loops yielding exact base ids,
    // with no slice/compare/mask pipeline, and a probe-bound column reads
    // through the store's own probe. Only the selection's rows are
    // tested, so the ids are already the intersection.
    Ok(
        match typed_residual_ids_reading(
            &struct_arr,
            &selection,
            len,
            eqs,
            Some((table.rows, table.probes)),
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
                let mask = mask_for(&selection.apply(table.rows)?, eqs)?;
                let narrowed = selection.refine(&bool_array_to_mask(mask)?);
                debug::step!(t, "[match_pattern] In-memory narrowed by mask scan");
                narrowed
            }
        },
    )
}

/// The rows of `selection` (of an index component's `table`) holding the one
/// equality of `eqs`, ascending: found on its column's encoding through the
/// component's probe (`eq_offsets`, a dictionary's codes against the code),
/// or decoded and compared. A run bound to a graph checks its rows this way,
/// contiguous inside the component; any other shape takes the base's
/// compare.
fn component_compared(
    table: ResidualTable<'_>,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
) -> Result<RowSelection> {
    let len = table.rows.len();
    let (SelectedRows::Run(run), [(field, value)]) = (selection.as_rows(len), eqs) else {
        return compared(table, selection, eqs, len, None);
    };
    let Some(code) = unsigned_value(value) else {
        return compared(table, selection, eqs, len, None);
    };
    observe!(ComponentChecked(run.len() as u64));
    let mut offsets = Vec::new();
    match table.probes.by_name(table.rows, field).flatten() {
        Some(probe) => probe
            .probe()
            .eq_offsets(run.start, run.len(), code, &mut offsets),
        None => {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let values = into_struct_array(table.rows.clone())?
                .unmasked_field_by_name(field)?
                .slice(run.clone())?
                .execute::<PrimitiveArray>(&mut ctx)?;
            offsets.extend(
                values
                    .as_slice::<u64>()
                    .iter()
                    .enumerate()
                    .filter(|&(_, &value)| value == code)
                    .map(|(at, _)| at),
            );
        }
    }
    Ok(RowSelection::from_ids(
        offsets
            .into_iter()
            .map(|at| (run.start + at) as RowId)
            .collect(),
    ))
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
