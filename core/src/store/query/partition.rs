//! Splitting a view into partitions: disjoint views, in view order, that
//! together cover its rows — the currency of a parallel scan.

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use crate::error::Result;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{QuadsSource, VortexRdfStore};

/// `range` cut into `count` consecutive pieces of as equal a size as the
/// division allows, the remainder spread one row at a time over the first.
fn cut(range: Range<u64>, count: usize) -> Vec<Range<u64>> {
    let len = range.end - range.start;
    let (each, extra) = (len / count as u64, len % count as u64);
    let mut start = range.start;
    (0..count as u64)
        .map(|i| {
            let end = start + each + u64::from(i < extra);
            let piece = start..end;
            start = end;
            piece
        })
        .collect()
}

impl VortexRdfStore {
    /// This view as exactly `count` disjoint views in its order, together
    /// covering its rows — trailing ones may be empty. A contiguous
    /// selection is cut into consecutive ranges, an id list into consecutive
    /// slices of it, and a served run into consecutive sub-runs that keep
    /// their plan and their deferred ids; a file view's pushed-down filter
    /// rides with every partition. The append tail, whose rows every read
    /// yields last, rides with the last partition alone.
    pub async fn partitions(&self, count: NonZeroUsize) -> Result<Vec<Self>> {
        let count = count.get();
        if count == 1 {
            return Ok(vec![self.clone()]);
        }
        let sources: Vec<QuadsSource> = match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                components,
                deleted,
                probes,
                canonical,
                serve,
            } => match (selection, serve) {
                // A served run: sub-runs under the plan restricted to each,
                // their ids still deferred.
                (ViewSelection::Pending(_), Some(plan)) => {
                    let run = plan.range();
                    cut(run.start as u64..run.end as u64, count)
                        .into_iter()
                        .map(|sub| {
                            let (plan, ids) =
                                plan.restricted(sub.start as usize..sub.end as usize)?;
                            Ok(QuadsSource::InMemory {
                                base: base.clone(),
                                selection: ViewSelection::Pending(ids),
                                components: Arc::clone(components),
                                deleted: deleted.clone(),
                                probes: Arc::clone(probes),
                                canonical: Arc::clone(canonical),
                                serve: Some(plan),
                            })
                        })
                        .collect::<Result<_>>()?
                }
                _ => exact_parts(&selection.materialized()?, base.len() as u64, count)
                    .into_iter()
                    .map(|part| self.quads.with_selection(ViewSelection::Exact(part)))
                    .collect(),
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file, selection, ..
            } => exact_parts(
                &selection.materialized_async().await?,
                file.row_count(),
                count,
            )
            .into_iter()
            .map(|part| self.quads.with_selection(ViewSelection::Exact(part)))
            .collect(),
        };
        let last = sources.len() - 1;
        Ok(sources
            .into_iter()
            .enumerate()
            .map(|(i, quads)| Self {
                layout: self.layout.clone(),
                indexes: self.indexes.clone(),
                quads,
                tail: self.tail.as_ref().map(|tail| {
                    if i == last {
                        tail.clone()
                    } else {
                        tail.with_selection(RowSelection::empty())
                    }
                }),
            })
            .collect())
    }
}

/// `selection` over a base of `rows` rows cut into `count` consecutive
/// selections.
fn exact_parts(selection: &RowSelection, rows: u64, count: usize) -> Vec<RowSelection> {
    match selection {
        RowSelection::All => cut(0..rows, count)
            .into_iter()
            .map(RowSelection::Range)
            .collect(),
        RowSelection::Range(range) => cut(range.clone(), count)
            .into_iter()
            .map(RowSelection::Range)
            .collect(),
        RowSelection::Ids(ids) => cut(0..ids.len() as u64, count)
            .into_iter()
            .map(|piece| RowSelection::Ids(ids.slice(piece.start as usize..piece.end as usize)))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cut_spreads_the_remainder_over_the_first_pieces() {
        assert_eq!(cut(0..10, 3), vec![0..4, 4..7, 7..10]);
        assert_eq!(cut(5..7, 4), vec![5..6, 6..7, 7..7, 7..7]);
        assert_eq!(cut(3..3, 2), vec![3..3, 3..3]);
    }
}
