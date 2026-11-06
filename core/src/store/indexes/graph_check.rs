//! The graph check of an in-memory index run bound to a graph
//! ([`GraphCheck`]): the run's rows are contiguous in its component, so the
//! component's own graph column checks them zone by zone, as a file's located
//! run checks its rows, in place of the graph compared at the base rows the
//! run lists, scattered over the base.
//!
//! Each zone the run touches is decided by the component's zone map of its
//! graph column ([`ColumnZones::eq_holds`]): no row holds the graph, every row
//! does, or some may. A mixed zone is counted from its recorded graphs when
//! the run holds the whole zone, else on the graph column's encoding
//! (`count_eq`), else by comparing its graphs; its matching rows are found by
//! comparing them. A window of a run whose ids ascend skips the zones before
//! it by their counts and reads only the zones it spans.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::{Buffer, BufferMut};
use vortex_rdf_encoded_search::OwnedSortedProbe;

use super::components::{IndexComponent, run_order_row_ids};
use super::get_or_try_init;
use crate::error::Result;
use crate::io::zone_map::ColumnZones;
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::schema::{COL_G, RowId};

/// A run's check of its bound graph inside its component: the run's rows of
/// the component, the graph's code, and the component's graph column with
/// its zone map ([`IndexComponent::graph_zones`]).
pub(crate) struct GraphCheck {
    /// The run's rows of the component.
    rows: Range<usize>,
    /// The bound graph's code.
    code: u64,
    /// The zone map of the component's graph column, with each zone's
    /// recorded graphs.
    zones: Arc<ColumnZones>,
    /// The run's slice of the component's graph column.
    graphs: ArrayRef,
    /// The component's encoded-search probe over its graph column, when its
    /// encoding resolves one: what counts a part of a zone and reads its
    /// graphs in place.
    probe: Option<Arc<OwnedSortedProbe>>,
    /// How many of the run's rows in each zone it touches hold the graph, in
    /// run order, counted once and kept.
    counts: OnceLock<Arc<[usize]>>,
}

/// What a zone's rows of a checked run hold, by the zone map alone.
enum Zone {
    /// No row of the zone holds the graph.
    None,
    /// Every row of the zone holds it.
    All,
    /// Some rows may.
    Mixed,
}

impl GraphCheck {
    /// The check of the graph `code` over `component`'s rows `rows` (a run,
    /// not empty), whose graph column's zone map is `zones`.
    pub(crate) fn new(
        component: &IndexComponent,
        rows: Range<usize>,
        code: u64,
        zones: Arc<ColumnZones>,
    ) -> Result<Self> {
        use vortex_array::arrays::struct_::StructArrayExt as _;

        let graphs = component
            .rows()?
            .unmasked_field_by_name(COL_G)?
            .slice(rows.clone())?;
        Ok(Self {
            rows,
            code,
            zones,
            graphs,
            probe: component.probe(COL_G),
            counts: OnceLock::new(),
        })
    }

    /// The zones the run touches, in run order, each with the run's rows in
    /// it (component rows).
    fn parts(&self) -> impl Iterator<Item = (u64, Range<usize>)> + '_ {
        let zone_len = self.zones.zone_len() as usize;
        let first = self.rows.start / zone_len;
        let last = (self.rows.end - 1) / zone_len;
        (first..=last).map(move |zone| {
            let start = (zone * zone_len).max(self.rows.start);
            let end = ((zone + 1) * zone_len).min(self.rows.end);
            (zone as u64, start..end)
        })
    }

    /// What zone `zone` holds of the graph, by its bounds and recorded graphs.
    fn zone(&self, zone: u64) -> Zone {
        match self.zones.eq_holds(zone, self.code) {
            Some(false) => Zone::None,
            Some(true) => Zone::All,
            None => Zone::Mixed,
        }
    }

    /// How many of the run's rows hold the graph.
    pub(crate) fn count(&self) -> Result<usize> {
        Ok(self.counts()?.iter().sum())
    }

    /// `min(matches, limit)` at least: the zones counted in run order only
    /// until `limit` matches are ([`zone_count`](Self::zone_count)), so a
    /// capped count stops at the zone that meets its cap. Every match where
    /// the counts are kept, or fewer than `limit` match.
    pub(crate) fn count_capped(&self, limit: usize) -> Result<usize> {
        if let Some(counts) = self.counts.get() {
            return Ok(counts.iter().sum());
        }
        let mut counted = 0;
        for (zone, part) in self.parts() {
            if counted >= limit {
                break;
            }
            counted += self.zone_count(zone, &part)?;
        }
        Ok(counted)
    }

    /// How many of the run's rows in each zone it touches hold the graph,
    /// counted on the first call and kept ([`zone_count`](Self::zone_count)).
    fn counts(&self) -> Result<Arc<[usize]>> {
        get_or_try_init(&self.counts, || {
            self.parts()
                .map(|(zone, part)| self.zone_count(zone, &part))
                .collect()
        })
    }

    /// How many of the run's rows `part` of zone `zone` hold the graph: none
    /// or every row by the zone map, a mixed zone from its record when the
    /// run holds it whole, else on the graph column's encoding, else by
    /// comparing its graphs.
    fn zone_count(&self, zone: u64, part: &Range<usize>) -> Result<usize> {
        match self.zone(zone) {
            Zone::None => Ok(0),
            Zone::All => Ok(part.len()),
            Zone::Mixed => self.mixed_count(zone, part),
        }
    }

    /// How many of the run's rows `part` of mixed zone `zone` hold the graph:
    /// the zone's recorded count when `part` is the whole zone, else counted
    /// on the graph column's encoding, else compared.
    fn mixed_count(&self, zone: u64, part: &Range<usize>) -> Result<usize> {
        let whole = self.zones.zone_rows(zone);
        if (whole.start, whole.end) == (part.start as u64, part.end as u64)
            && let Some(matches) = self.zones.recorded_count(zone, self.code)
        {
            observe!(ZoneRecorded(part.len() as u64));
            return Ok(matches as usize);
        }
        if let Some(probe) = &self.probe {
            observe!(ZoneCounted(part.len() as u64));
            return Ok(probe.probe().count_eq(part.start, part.len(), self.code));
        }
        Ok(self.compared(part)?.len())
    }

    /// The offsets in `part` (component rows) of the rows whose graph is the
    /// bound one, ascending: found on the graph column's encoding through
    /// its probe (`eq_offsets`, a dictionary's codes against the graph's
    /// code), or decoded and compared.
    fn compared(&self, part: &Range<usize>) -> Result<Vec<usize>> {
        observe!(ComponentChecked(part.len() as u64));
        let mut offsets = Vec::new();
        match &self.probe {
            Some(probe) => {
                probe
                    .probe()
                    .eq_offsets(part.start, part.len(), self.code, &mut offsets);
            }
            None => {
                let mut ctx = VORTEX_SESSION.create_execution_ctx();
                let graphs = self
                    .graphs
                    .slice(part.start - self.rows.start..part.end - self.rows.start)?
                    .execute::<PrimitiveArray>(&mut ctx)?;
                offsets.extend(
                    graphs
                        .as_slice::<u64>()
                        .iter()
                        .enumerate()
                        .filter(|&(_, &graph)| graph == self.code)
                        .map(|(at, _)| at),
                );
            }
        }
        Ok(offsets)
    }

    /// The ids of the run's rows `part` of zone `zone` that hold the graph,
    /// in run order, `rids` being the run's slice of the rid column: every
    /// row's in a zone of the graph alone, the compared rows' in a mixed one.
    fn part_ids(&self, rids: &ArrayRef, zone: u64, part: &Range<usize>) -> Result<Buffer<RowId>> {
        let in_run = part.start - self.rows.start..part.end - self.rows.start;
        match self.zone(zone) {
            Zone::None => Ok(Buffer::empty()),
            Zone::All => run_order_row_ids(rids.slice(in_run)?),
            Zone::Mixed => {
                let offsets = self.compared(part)?;
                if offsets.is_empty() {
                    return Ok(Buffer::empty());
                }
                let ids = run_order_row_ids(rids.slice(in_run)?)?;
                Ok(offsets.into_iter().map(|at| ids[at]).collect())
            }
        }
    }

    /// The ids of the run's rows that hold the graph, in run order, `rids`
    /// being the run's slice of the rid column: a zone at a time
    /// ([`part_ids`](Self::part_ids)), the zones of the graph alone decoded
    /// whole and the mixed ones' compared rows picked, and a zone holding
    /// none of the graph's rows (by the zone map, or counted so) neither read
    /// nor decoded.
    pub(crate) fn ids(&self, rids: &ArrayRef) -> Result<Buffer<RowId>> {
        let counts = self.counts.get();
        let mut ids = BufferMut::with_capacity(counts.map_or(0, |counts| counts.iter().sum()));
        for (at, (zone, part)) in self.parts().enumerate() {
            if counts.is_some_and(|counts| counts[at] == 0) {
                continue;
            }
            ids.extend_from_slice(&self.part_ids(rids, zone, &part)?);
        }
        Ok(ids.freeze())
    }

    /// The ids of the run's matches `offset..offset + limit`, in run order,
    /// `rids` being the run's slice of the rid column, beside the run's
    /// matches as far as they are known: every one when the window reaches
    /// the run's last zone, else at least `offset + limit`. The zones are
    /// counted in run order only until the window fills (their kept counts
    /// where a count took them), those before the window skipped by their
    /// counts, and only the zones it spans read, those counted empty
    /// excepted.
    pub(crate) fn window(
        &self,
        rids: &ArrayRef,
        offset: usize,
        limit: usize,
    ) -> Result<(Buffer<RowId>, usize)> {
        if limit == 0 {
            return Ok((Buffer::empty(), self.count()?));
        }
        let kept = self.counts.get();
        let mut ids = BufferMut::with_capacity(limit);
        let mut before = 0;
        for (at, (zone, part)) in self.parts().enumerate() {
            if ids.len() == limit {
                return Ok((ids.freeze(), before));
            }
            let count = match kept {
                Some(counts) => counts[at],
                None => self.zone_count(zone, &part)?,
            };
            if count == 0 {
                continue;
            }
            if before + count <= offset {
                before += count;
                continue;
            }
            let matched = self.part_ids(rids, zone, &part)?;
            let from = offset.saturating_sub(before);
            let take = (limit - ids.len()).min(matched.len().saturating_sub(from));
            ids.extend_from_slice(&matched[from..from + take]);
            before += count;
        }
        Ok((ids.freeze(), before))
    }
}
