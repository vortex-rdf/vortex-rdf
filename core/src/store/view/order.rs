//! The orders a view's rows can be in, and the searches a sorted column
//! admits: whether a column is ascending over a run, and the runs of that
//! column a term-code constraint keeps.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_rdf_encoded_search::OwnedSortedProbe;

use crate::error::{Result, VortexRdfError};
use crate::store::array::subject_sorted;
use crate::store::arrow::QuadColumn;
use crate::store::query::pushdown::Keep;
use crate::store::view::probes::StructProbes;
use crate::store::{QuadsSource, VortexRdfStore};

/// The order a run of rows is sorted in: the four quad columns, most
/// significant first. The base is [`SPOG`](Self::SPOG); a by-copy index
/// family holds the same quads in [`POSG`](Self::POSG) or
/// [`OSPG`](Self::OSPG).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SortOrder([QuadColumn; 4]);

impl SortOrder {
    /// The base's order: `(s, p, o, g)`.
    pub const SPOG: Self = Self([QuadColumn::S, QuadColumn::P, QuadColumn::O, QuadColumn::G]);
    /// The `index:posg` copy family's order.
    pub const POSG: Self = Self([QuadColumn::P, QuadColumn::O, QuadColumn::S, QuadColumn::G]);
    /// The `index:ospg` copy family's order.
    pub const OSPG: Self = Self([QuadColumn::O, QuadColumn::S, QuadColumn::P, QuadColumn::G]);

    /// The columns, most significant first.
    pub fn columns(&self) -> &[QuadColumn; 4] {
        &self.0
    }

    /// `column`'s rank in the order: 0 for the most significant key.
    pub fn position(&self, column: QuadColumn) -> usize {
        self.0
            .iter()
            .position(|c| *c == column)
            .expect("every quad column has a rank")
    }
}

impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|c| c.name()).collect();
        f.write_str(&names.join(","))
    }
}

impl std::str::FromStr for SortOrder {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        let columns: Vec<QuadColumn> = s
            .split(',')
            .map(|name| name.trim().parse())
            .collect::<Result<_>>()?;
        let order: [QuadColumn; 4] = columns.try_into().map_err(|_| {
            VortexRdfError::InvalidOperation(format!(
                "a sort order names the four quad columns once each, got {s:?}"
            ))
        })?;
        if QuadColumn::ALL.iter().any(|c| !order.contains(c)) {
            return Err(VortexRdfError::InvalidOperation(format!(
                "a sort order names the four quad columns once each, got {s:?}"
            )));
        }
        Ok(Self(order))
    }
}

impl VortexRdfStore {
    /// The order every read of this view yields its rows in, when the view
    /// knows it: the base's `(s, p, o, g)` for a view over the sorted base —
    /// whole, a range, or an ascending gather of it — and the answering
    /// index's own order for a served match. `None` under an append tail
    /// (its rows come last, unsorted), on a base without the sorted stamp,
    /// and wherever a served child is not known to be sorted; a consumer
    /// may then assume nothing.
    pub fn sort_order(&self) -> Option<SortOrder> {
        if self.tail_len() != 0 {
            return None;
        }
        match &self.quads {
            QuadsSource::InMemory { base, serve, .. } => match serve {
                Some(plan) => plan.key_order(),
                None => subject_sorted(base).then_some(SortOrder::SPOG),
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, serve, .. } => match serve {
                Some(plan) => plan.key_order(),
                None => file.quads_sorted().then_some(SortOrder::SPOG),
            },
        }
    }
}

/// Whether `column` is ascending over `range` of `rows` sorted in `order`:
/// every key before it in the order is constant over the range, read at
/// the range's two ends through `probes`. `None` when a preceding column
/// resolves no probe.
pub(crate) fn sorted_within(
    rows: &ArrayRef,
    probes: &StructProbes,
    order: SortOrder,
    range: Range<usize>,
    column: QuadColumn,
) -> Option<bool> {
    if range.len() < 2 {
        return Some(true);
    }
    for key in &order.columns()[..order.position(column)] {
        let probe = probes.by_name(rows, key.name())?;
        if probe.value_at(range.start) != probe.value_at(range.end - 1) {
            return Some(false);
        }
    }
    Some(true)
}

/// The runs of `range` — over a column that is ascending there, searched
/// through its `probe` — whose codes `keep` admits, ascending and disjoint.
/// A code range is two lower bounds; a code set is one bounds search per
/// code, worth it only while those searches are cheaper than reading the
/// range once, which is when `None` leaves the read to a linear pass.
pub(crate) fn keep_runs(
    probe: &OwnedSortedProbe,
    range: Range<usize>,
    keep: &Keep,
) -> Option<Vec<Range<usize>>> {
    match keep {
        Keep::Range(lo, hi) => {
            let start = probe.bounds_in(range.clone(), u64::from(*lo)).0;
            let end = probe.bounds_in(range, u64::from(*hi)).0;
            let mut runs = Vec::with_capacity(1);
            if start < end {
                runs.push(start..end);
            }
            Some(runs)
        }
        Keep::Set(codes) => {
            let searches = codes
                .len()
                .saturating_mul(range.len().max(2).ilog2() as usize + 1);
            if searches >= range.len() {
                return None;
            }
            let mut runs: Vec<Range<usize>> = Vec::new();
            for &code in codes.as_slice() {
                let (lo, hi) = probe.bounds_in(range.clone(), u64::from(code));
                if lo == hi {
                    continue;
                }
                match runs.last_mut() {
                    Some(last) if last.end == lo => last.end = hi,
                    _ => runs.push(lo..hi),
                }
            }
            Some(runs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_name_and_rank_their_columns() {
        for order in [SortOrder::SPOG, SortOrder::POSG, SortOrder::OSPG] {
            assert_eq!(order.to_string().parse::<SortOrder>().unwrap(), order);
        }
        assert_eq!(SortOrder::POSG.to_string(), "p,o,s,g");
        assert_eq!(SortOrder::POSG.position(QuadColumn::S), 2);
        assert!("s,p,o".parse::<SortOrder>().is_err());
        assert!("s,p,o,o".parse::<SortOrder>().is_err());
    }
}
