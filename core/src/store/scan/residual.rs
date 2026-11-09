//! The residual compare over an in-memory table ([`ResidualCompare`]): the
//! equalities of a pattern no prefix search or index answered, over a
//! selection of a table's rows, the base's or an index component's. The
//! statistics settle what they can first ([`ask_statistics`]): the columns'
//! whole bounds, then the zones the selection touches, classed against their
//! zone maps ([`Classes`]). One walk then counts the matches
//! ([`count`](ResidualCompare::count)), takes a window of them
//! ([`window`](ResidualCompare::window)) or finds every one
//! ([`rows`](ResidualCompare::rows)):
//! - zone by zone where the zones are classed: "none" zones skipped, "all"
//!   zones taken, mixed ones counted unread from a record or the column's
//!   encoding where they can be, else compared;
//! - in growing pieces where they are not (zones off, or no zone map), each
//!   piece compared, skipping the zones whose bounds rule a code out.
//!
//! A compare is the typed fast path or the mask scan over the base, and the
//! graph column's encoding over a component; the mask of an array's rows
//! holding the equalities ([`mask_for`]) is the tail's match's compare too.
//! Nothing here holds store state: the match narrows a selection with these,
//! and a residual left pending
//! ([`PendingResidual`](crate::store::view::pending::PendingResidual)) and an
//! index run's graph check
//! ([`GraphCheck`](crate::store::indexes::graph_check::GraphCheck)) walk only
//! as far as they need.

use std::convert::Infallible;
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
use crate::zone_stats::quad_zones::QuadZones;
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
/// ([`eq_on_bounds`]): first against its column's whole bounds, which its
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
    let mut open = Vec::with_capacity(eqs.len());
    let mut zoned = Vec::new();
    for (field, value) in eqs {
        if let Some(check) = zone_check(&memory.zones, open.len(), field, &value) {
            let settled = check
                .zones
                .column_bounds()
                .and_then(|(min, max)| eq_on_bounds(min, max, check.code));
            match settled {
                Some(false) => return Ok(Asked::Nothing),
                Some(true) => continue,
                None => zoned.push(check),
            }
        }
        open.push((field, value));
    }
    if open.is_empty() {
        return Ok(Asked::Everything);
    }
    let classes = Classes::zoned(zoned, open.len(), selection, memory.base.len())
        .unwrap_or_else(|| unasked_classes(memory, &open));
    Ok(Asked::Open { eqs: open, classes })
}

/// The classes of a residual over `memory`'s base whose statistics are not
/// asked (zones off, or a selection of no more than a zone's rows): growing
/// pieces, whose compares skip the zones the bounds of `eqs`' columns rule
/// out where the base keeps their zone maps ([`ZoneSet::bounds_only`]).
pub(crate) fn unasked_classes(memory: &MemoryBase, eqs: &[(&'static str, Scalar)]) -> Classes {
    let checks: Vec<ZoneCheck> = eqs
        .iter()
        .enumerate()
        .filter_map(|(at, (field, value))| zone_check(&memory.zones, at, field, value))
        .collect();
    let unzoned = unzoned(eqs.len(), &checks);
    Classes::Pieces {
        bounds: ZoneSet::new(checks, unzoned).map(ZoneSet::bounds_only),
    }
}

/// The zone check of the equality `field = value`, the `bit`th of a
/// residual's, against the zone map `zones` keeps of its column: `None` for
/// a column without one, a value that is no code, or a bit past a
/// [`ZoneClass`]'s 32.
fn zone_check(zones: &QuadZones, bit: usize, field: &str, value: &Scalar) -> Option<ZoneCheck> {
    Some(ZoneCheck {
        bit: u32::try_from(bit).ok().filter(|&bit| bit < u32::BITS)?,
        code: unsigned_value(value)?,
        zones: Arc::clone(zones.get(primary_slot(field)?)?),
    })
}

/// The bits of the `eqs` equalities no check of `checks` speaks for.
fn unzoned(eqs: usize, checks: &[ZoneCheck]) -> u32 {
    checks
        .iter()
        .fold(every_eq(eqs), |bits, check| bits & !(1 << check.bit))
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
        if eqs > u32::BITS as usize {
            return None;
        }
        let unzoned = unzoned(eqs, &zoned);
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

/// A part of a residual's selection read as one, by a walk that may stop
/// short ([`ResidualCompare::parts`]).
enum Part<'a> {
    /// A zone's selected rows, with the zone's class against `set`.
    Zone {
        set: &'a ZoneSet,
        zone: ZonePart,
        selected: RowSelection,
        class: ZoneClass,
    },
    /// A piece's selected rows, compared on every equality
    /// ([`ResidualCompare::pieces_compared`]).
    Piece {
        bounds: &'a Option<ZoneSet>,
        selected: RowSelection,
    },
}

/// One step of a walk over a residual's whole selection
/// ([`ResidualCompare::steps`]): the selected rows of adjacent zones read
/// as one. `C` is how a mixed zone is counted unread: [`Unread`] in a
/// count, never in a walk for the rows ([`Infallible`]).
enum Step<C> {
    /// No row matches: skipped.
    Skip,
    /// Every row matches: taken uncompared.
    Take,
    /// A mixed zone's matches counted unread.
    Unread(C),
    /// Compared on the equalities `bits` names.
    Compare(u32),
}

impl<C> Step<C> {
    /// Whether this step and `next`, of the zone after it, are one step:
    /// both skip, both take, or both compare on the same equalities.
    fn joins(&self, next: &Self) -> bool {
        match (self, next) {
            (Step::Skip, Step::Skip) | (Step::Take, Step::Take) => true,
            (Step::Compare(bits), Step::Compare(other)) => bits == other,
            _ => false,
        }
    }
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
    fn parts(&self, span: Range<u64>, want: usize) -> Box<dyn Iterator<Item = Part<'_>> + '_> {
        let rows = self.selected_rows();
        let len = self.len();
        match self.classes {
            Classes::Zoned(set) => Box::new(set.parts(span).filter_map(move |zone| {
                let selected = part_of(&rows, zone.rows.clone());
                (!selected.is_empty(len)).then(|| Part::Zone {
                    set,
                    class: set.class(zone.zone),
                    zone,
                    selected,
                })
            })),
            Classes::Pieces { bounds } => {
                let total = self.work();
                let first = if total <= want {
                    total
                } else {
                    FIRST_PIECE_ROWS
                };
                Box::new(
                    growing_pieces(std::iter::once(0..total as u64), first as u64).map(
                        move |(_, piece)| Part::Piece {
                            bounds,
                            selected: rows_at(&rows, piece.start as usize..piece.end as usize),
                        },
                    ),
                )
            }
        }
    }

    /// The walk over the selected rows `rows` within `span`, zone by zone
    /// of `set`, in table order: a zone with no selected row is skipped, and
    /// any other is a step by its class ([`Step`]), a mixed zone's chosen by
    /// `mixed` from the zone, its selected rows and its unsettled bits.
    /// Adjacent zones of one step are joined, across zones with no selected
    /// row, so a run of zones compared on the same equalities is compared
    /// once.
    fn steps<C>(
        &self,
        set: &ZoneSet,
        rows: &SelectedRows<'_>,
        span: Range<u64>,
        mut mixed: impl FnMut(&ZonePart, &RowSelection, u32) -> Step<C>,
    ) -> Vec<(Range<u64>, Step<C>)> {
        let len = self.len();
        let mut steps: Vec<(Range<u64>, Step<C>)> = Vec::new();
        for zone in set.parts(span) {
            let selected = part_of(rows, zone.rows.clone());
            if selected.is_empty(len) {
                if let Some((run, _)) = steps.last_mut() {
                    run.end = zone.rows.end;
                }
                continue;
            }
            let step = match set.class(zone.zone) {
                ZoneClass::None => Step::Skip,
                ZoneClass::All => Step::Take,
                ZoneClass::Mixed { unsettled } => mixed(&zone, &selected, unsettled),
            };
            match steps.last_mut() {
                Some((run, last)) if run.end == zone.rows.start && last.joins(&step) => {
                    run.end = zone.rows.end;
                }
                _ => steps.push((zone.rows, step)),
            }
        }
        steps
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

    /// [`count`](Self::count) of every match, by zones: the walk's steps
    /// ([`steps`](Self::steps)), a mixed zone counted unread where it can be
    /// ([`unread_plan`](Self::unread_plan)), summed; or the whole selection
    /// compared once where the compares are fragmented ([`compares_whole`]).
    fn full_count(&self, set: &ZoneSet, span: Range<u64>) -> Result<usize> {
        let rows = self.selected_rows();
        let steps = self.steps(set, &rows, span, |zone, selected, bits| {
            match self.unread_plan(set, zone, selected, bits) {
                Some(plan) => Step::Unread(plan),
                None => Step::Compare(bits),
            }
        });
        if compares_whole(&rows, &steps, self.work(), self.len()) {
            let every = every_eq(self.eqs.len());
            return Ok(self.live(&self.compare(self.selection.clone(), every)?));
        }
        let mut count = 0;
        for (run, step) in steps {
            count += match step {
                Step::Skip => 0,
                Step::Take => {
                    let part = part_of(&rows, run);
                    self.observe_taken(&part);
                    self.live(&part)
                }
                Step::Unread(plan) => unread_count(plan),
                Step::Compare(bits) => self.live(&self.compare(part_of(&rows, run), bits)?),
            };
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

    /// A part's matches counted unread, where a zone's can be: none in a
    /// "none" zone, the live rows of an "all" zone, a mixed zone's from its
    /// record or its column's encoding ([`unread_plan`](Self::unread_plan)).
    /// `None` for a zone that must be compared, and for every piece.
    fn unread(&self, part: &Part<'_>) -> Option<usize> {
        let Part::Zone {
            set,
            zone,
            selected,
            class,
        } = part
        else {
            return None;
        };
        match *class {
            ZoneClass::None => Some(0),
            ZoneClass::All => {
                self.observe_taken(selected);
                Some(self.live(selected))
            }
            ZoneClass::Mixed { unsettled } => self
                .unread_plan(set, zone, selected, unsettled)
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
    fn matched(&self, part: &Part<'_>) -> Result<RowSelection> {
        match part {
            Part::Zone {
                class: ZoneClass::None,
                ..
            } => Ok(RowSelection::empty()),
            Part::Zone {
                class: ZoneClass::All,
                selected,
                ..
            } => Ok(selected.clone()),
            Part::Zone {
                class: ZoneClass::Mixed { unsettled },
                selected,
                ..
            } => self.compare(selected.clone(), *unsettled),
            Part::Piece { bounds, selected } => {
                observe!(ResidualPiece(selected.len(self.len())));
                self.pieces_compared(bounds, selected.clone())
            }
        }
    }

    /// The matches of `selection` (a piece, or the whole selection) on every
    /// equality: by the zones `bounds` leaves past a zone's rows, else
    /// compared whole.
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

    /// The matches of `selection`, zone by zone by `set`'s classes: the
    /// walk's steps ([`steps`](Self::steps)), "none" zones skipped, "all"
    /// zones taken uncompared and the rest compared on the equalities they
    /// leave unsettled; or the whole selection compared once where the
    /// compares are fragmented ([`compares_whole`]).
    fn rows_by(&self, set: &ZoneSet, selection: RowSelection) -> Result<RowSelection> {
        let len = self.len();
        let rows = selection.as_rows(len);
        let Some(span) = span(&rows) else {
            return Ok(selection);
        };
        let steps = self.steps(set, &rows, span, |_, _, bits| {
            Step::<Infallible>::Compare(bits)
        });
        if steps.iter().all(|(_, step)| matches!(step, Step::Take)) {
            self.observe_taken(&selection);
            if !self.table.component {
                debug::step!(
                    self.t,
                    "[match_pattern] In-memory residual: every zone taken"
                );
            }
            return Ok(selection);
        }
        if compares_whole(&rows, &steps, selection.len(len), len) {
            return self.compare(selection, every_eq(self.eqs.len()));
        }
        if !self.table.component && steps.iter().any(|(_, step)| matches!(step, Step::Skip)) {
            observe!(ZoneNarrowed);
            debug::step!(self.t, "[match_pattern] In-memory narrowed by zone classes");
        }
        let mut ids: Vec<RowId> = Vec::new();
        for (run, step) in steps {
            let part = part_of(&rows, run);
            match step {
                Step::Skip => {}
                Step::Take => {
                    self.observe_taken(&part);
                    extend_rows(&mut ids, &part, len);
                }
                Step::Unread(never) => match never {},
                Step::Compare(bits) => extend_rows(&mut ids, &self.compare(part, bits)?, len),
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

/// Whether the compares among `steps`, over the `selected` rows of `rows`
/// (of a table of `len` rows), are one compare of the whole selection
/// instead: several, holding more than half of it, as comparing every row
/// once costs less than comparing such runs apart.
fn compares_whole<C>(
    rows: &SelectedRows<'_>,
    steps: &[(Range<u64>, Step<C>)],
    selected: usize,
    len: usize,
) -> bool {
    let compares: Vec<usize> = steps
        .iter()
        .filter(|(_, step)| matches!(step, Step::Compare(_)))
        .map(|(run, _)| part_of(rows, run.clone()).len(len))
        .collect();
    compares.len() > 1 && compares.iter().sum::<usize>().saturating_mul(2) > selected
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

/// The equalities of `eqs` whose bits `bits` sets, in order; an equality
/// past the 32 bits, which no zone settles, is always picked.
fn picked(eqs: &[(&'static str, Scalar)], bits: u32) -> Vec<(&'static str, Scalar)> {
    eqs.iter()
        .enumerate()
        .filter(|(i, _)| {
            1u32.checked_shl(*i as u32)
                .is_none_or(|bit| bits & bit != 0)
        })
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
