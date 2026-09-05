//! What a planner may assume about a view before reading it.

use crate::error::Result;
use crate::store::array::subject_sorted;
use crate::store::arrow::QuadColumn;
use crate::store::view::order::{SortOrder, sorted_within};
use crate::store::view::selection::RowSelection;
use crate::store::{LayoutStrategy, QuadsSource, VortexRdfStore};

/// The facts about a view a planner can use without reading its rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewStatistics {
    /// The exact live row count.
    pub rows: usize,
    /// The order the rows come in, when known ([`VortexRdfStore::sort_order`]).
    pub sort_order: Option<SortOrder>,
    /// Per column in `(s, p, o, g)` order, an inclusive envelope of the
    /// term codes the column holds, where one is known without a read: the
    /// two ends of a sorted run, or the one value of a key a match fixed.
    /// Codes are lexicographic ranks, so an envelope is also a term range.
    pub code_bounds: [Option<(u32, u32)>; 4],
    /// The store's dictionary size — the number of distinct terms any column
    /// can hold — under the Dictionary layout.
    pub distinct_terms: Option<usize>,
}

impl VortexRdfStore {
    /// This view's [`ViewStatistics`]: the row count is exact (a count, no
    /// row decoded); the envelopes are read at a run's two ends through the
    /// base's or the answering index's probes, and stay `None` wherever the
    /// rows would have to be read to know — a file view, a view under a
    /// tail, a column that is not sorted over the selection.
    pub async fn statistics(&self) -> Result<ViewStatistics> {
        let rows = self.size().await?;
        Ok(ViewStatistics {
            rows,
            sort_order: self.sort_order(),
            code_bounds: if rows == 0 {
                [None; 4]
            } else {
                self.code_bounds()?
            },
            distinct_terms: self.dictionary_snapshot().map(|dict| dict.len()),
        })
    }

    /// The code envelopes of an in-memory Dictionary view without a tail.
    fn code_bounds(&self) -> Result<[Option<(u32, u32)>; 4]> {
        let mut bounds = [None; 4];
        if self.layout.strategy() != LayoutStrategy::Dictionary || self.tail_len() != 0 {
            return Ok(bounds);
        }
        // Without `file-io`, InMemory is the only variant.
        #[allow(irrefutable_let_patterns)]
        let QuadsSource::InMemory {
            base,
            selection,
            probes,
            serve,
            ..
        } = &self.quads
        else {
            return Ok(bounds);
        };
        // The keys a served match fixed are one value; its next key spans the
        // run's two ends. A base-order selection spans its first and last
        // row on `s`, and on a later column while every column before it is
        // constant between them.
        let (rows, order, resolved, first, last) = match serve {
            Some(plan) => {
                let Some(order) = plan.key_order() else {
                    return Ok(bounds);
                };
                let run = plan.range();
                (plan.rows(), order, plan.resolved(), run.start, run.end - 1)
            }
            None => {
                if !subject_sorted(base) {
                    return Ok(bounds);
                }
                let (first, last) = match selection.materialized()? {
                    RowSelection::All => (0, base.len() - 1),
                    RowSelection::Range(range) => (range.start as usize, range.end as usize - 1),
                    RowSelection::Ids(ids) => (
                        ids.as_slice()[0] as usize,
                        ids.as_slice()[ids.len() - 1] as usize,
                    ),
                };
                (base.clone(), SortOrder::SPOG, 0, first, last)
            }
        };
        let names = match serve {
            Some(plan) => plan.primary_columns(),
            None => QuadColumn::ALL.map(QuadColumn::name),
        };
        let struct_probes = match serve {
            Some(plan) => plan.probes(),
            None => probes.as_ref(),
        };
        for (position, column) in order.columns().iter().enumerate() {
            let sorted = position < resolved
                || sorted_within(&rows, struct_probes, order, first..last + 1, *column)
                    == Some(true);
            if !sorted {
                break;
            }
            let Some(probe) = struct_probes.by_name(&rows, names[column.index()]) else {
                break;
            };
            let (lo, hi) = (probe.value_at(first), probe.value_at(last));
            bounds[column.index()] = Some((lo as u32, hi as u32));
        }
        Ok(bounds)
    }
}
