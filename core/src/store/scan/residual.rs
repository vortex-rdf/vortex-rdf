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

use vortex_array::arrays::StructArray;
use vortex_array::arrays::constant::ConstantArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use web_time::Instant;

use crate::debug;
use crate::error::Result;
use crate::io::zone_map::{ZONE_ROWS, bounds_prove};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::stats_rules::eq_proofs;
use crate::store::MemoryBase;
use crate::store::array::{bool_array_to_mask, into_struct_array, stamped_bounds, unsigned_value};
use crate::store::query::zone_decider::{CheckedColumn, ZoneClass, ZoneDecider};
use crate::store::scan::typed_eq::typed_residual_ids;
use crate::store::schema::{CODE_DTYPE, PRIMARY_COLUMNS, RowId};
use crate::store::view::selection::{RowSelection, SelectedRows};

/// Whether a residual over a selection of `rows` rows asks the statistics
/// before it compares: past one zone's rows, as below them the compare
/// costs less than the statistics' answer.
pub(crate) fn asks_statistics(rows: usize) -> bool {
    rows > ZONE_ROWS
}

/// What the statistics settle of a residual before its rows are compared
/// ([`ask_statistics`]): the ladder's first two levels (spec §3).
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
/// once, each equality of `eqs` by Vortex's proofs of `x = code`
/// ([`eq_proofs`]): first against its column's exact bounds, the array
/// statistics ([`stamped_bounds`], [`bounds_prove`]), then the equalities
/// left against their columns' zone maps ([`ResidualZones`]), the same
/// proofs pruning them.
pub(crate) fn ask_statistics(
    memory: &MemoryBase,
    selection: &RowSelection,
    eqs: Vec<(&'static str, Scalar)>,
) -> Result<Asked> {
    let base = &memory.base;
    let struct_arr = into_struct_array(base.clone())?;
    let rows = base.len() as u64;
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
        let zones = memory.zones.get(slot);
        if bounds.is_none() && zones.is_none() {
            open.push((field, value));
            continue;
        }
        let proofs = eq_proofs(&CODE_DTYPE, code)?;
        if bounds_prove(bounds, proofs.none.as_ref(), rows)? {
            return Ok(Asked::Nothing);
        }
        if bounds_prove(bounds, proofs.all.as_ref(), rows)? {
            continue;
        }
        if let Some(zones) = zones {
            let column = CheckedColumn {
                slot,
                code,
                zones: zones.clone(),
                proofs,
            };
            zoned.push((open.len(), column));
        }
        open.push((field, value));
    }
    if open.is_empty() {
        return Ok(Asked::Everything);
    }
    let zones = ResidualZones::new(zoned, open.len(), selection, base.len())?;
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

/// A residual's zones, classed once against its equalities: each zone its
/// selection touches is "none", "all" or mixed ([`ZoneClass`]), as
/// [`ZoneDecider`] finds on the zone maps of the columns the equalities
/// bind (with `g`'s recorded graphs). Built with the residual
/// ([`ask_statistics`]); a pending residual keeps it for every piece.
pub(crate) struct ResidualZones {
    decider: ZoneDecider,
    /// The first zone classed.
    first: u64,
    /// The class of each zone from `first` on, through the selection's last.
    classes: Vec<ZoneClass>,
    /// The equality (by place in the residual's) on each slot with a zone
    /// map.
    by_slot: [Option<usize>; 4],
    /// The equalities no zone map speaks for, one bit each: compared in
    /// every zone that is not "none".
    unzoned: u32,
}

impl ResidualZones {
    /// The classes of the zones `selection` (of a base of `base_len` rows)
    /// touches, against `zoned`: the checked columns of the residual's
    /// equalities that have zone maps, each with its equality's place among
    /// the residual's `eqs` equalities. `None` without such a column, for a
    /// selection of no row, or past 32 equalities.
    fn new(
        zoned: Vec<(usize, CheckedColumn)>,
        eqs: usize,
        selection: &RowSelection,
        base_len: usize,
    ) -> Result<Option<Self>> {
        let Some(span) = span(&selection.as_rows(base_len)) else {
            return Ok(None);
        };
        if eqs > 32 {
            return Ok(None);
        }
        let mut by_slot = [None; 4];
        let mut unzoned = every_eq(eqs);
        let mut columns = Vec::with_capacity(zoned.len());
        for (index, column) in zoned {
            by_slot[column.slot] = Some(index);
            unzoned &= !(1 << index);
            columns.push(column);
        }
        let Some(decider) = ZoneDecider::new(columns) else {
            return Ok(None);
        };
        let zone_len = decider.zone_len();
        let first = span.start / zone_len;
        let classes = decider.classify(first..span.end.div_ceil(zone_len))?;
        Ok(Some(Self {
            decider,
            first,
            classes,
            by_slot,
            unzoned,
        }))
    }

    /// How zone `zone`'s selected rows are read: as its class says, a zone
    /// outside the classed ones compared on every equality.
    fn treatment(&self, zone: u64) -> Treatment {
        let class = zone
            .checked_sub(self.first)
            .and_then(|at| self.classes.get(at as usize));
        let zoned = self
            .by_slot
            .iter()
            .flatten()
            .fold(0, |bits, i| bits | 1 << i);
        match class {
            None => Treatment::Compare(self.unzoned | zoned),
            Some(ZoneClass::None) => Treatment::Skip,
            Some(ZoneClass::All) if self.unzoned == 0 => Treatment::Take,
            Some(ZoneClass::All) => Treatment::Compare(self.unzoned),
            Some(ZoneClass::Mixed { unsettled }) => {
                let unsettled = (0..4)
                    .filter(|slot| unsettled & (1 << slot) != 0)
                    .filter_map(|slot| self.by_slot[slot])
                    .fold(0, |bits, i| bits | 1 << i);
                Treatment::Compare(self.unzoned | unsettled)
            }
        }
    }

    /// `rows` cut at zone boundaries, each part with its zone's
    /// treatment, adjacent parts of one treatment joined, in order.
    fn segments(&self, rows: Range<u64>) -> Vec<(Range<u64>, Treatment)> {
        let mut segments: Vec<(Range<u64>, Treatment)> = Vec::new();
        if rows.is_empty() {
            return segments;
        }
        let zone_len = self.decider.zone_len();
        for zone in rows.start / zone_len..=(rows.end - 1) / zone_len {
            let zone_rows = self.decider.zone_rows(zone);
            let part = zone_rows.start.max(rows.start)..zone_rows.end.min(rows.end);
            let treatment = self.treatment(zone);
            match segments.last_mut() {
                Some((run, joined)) if *joined == treatment && run.end == part.start => {
                    run.end = part.end;
                }
                _ => segments.push((part, treatment)),
            }
        }
        segments
    }
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
    compared(memory, selection, eqs, t)
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
                let unsettled: Vec<_> = eqs
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| bits & (1 << i) != 0)
                    .map(|(_, eq)| eq.clone())
                    .collect();
                let matched = compared(memory, part, &unsettled, t)?;
                extend_rows(&mut ids, &matched, base_len);
            }
        }
    }
    Ok(RowSelection::from_ids(vortex_buffer::Buffer::from(ids)))
}

/// The rows of `selection` where every equality of `eqs` holds, compared:
/// the typed fast path, else the mask scan.
fn compared(
    memory: &MemoryBase,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
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
        match typed_residual_ids(
            &struct_arr,
            &selection,
            base.len(),
            eqs,
            Some((base, probes)),
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
