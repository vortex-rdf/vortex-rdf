//! Planner-facing metadata about a view, answered without reading: row count
//! and its certainty, sort order, code bounds, the store's index children,
//! partitions, and the identity of the data behind the view.

use std::num::NonZeroUsize;

use vortex_mask::Mask;

use crate::error::Result;
use crate::store::QuadsSource;
use crate::store::VortexRdfStore;
use crate::store::indexes::IndexType;
use crate::store::indexes::copy::CopyFamily;
use crate::store::schema::QuadColumn;
use crate::store::view::selection::{RowSelection, ViewSelection};

/// A sort order of quad rows, by the columns compared first to last.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// `(s, p, o, g)`: the base's order in every store built by this crate.
    Spog,
    /// `(p, o, s, g)`: the `index:posg` child's order.
    Posg,
    /// `(o, s, p, g)`: the `index:ospg` child's order.
    Ospg,
}

/// How many rows a view holds: exactly, or at most while a pushed-down file
/// filter has rows still to test or a served file run is unread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowCountHint {
    /// The row count, when known without reading.
    pub exact: Option<usize>,
    /// A count the view can never exceed (`exact` when known).
    pub upper_bound: usize,
}

impl RowCountHint {
    /// A known count.
    pub fn exact(rows: usize) -> Self {
        Self {
            exact: Some(rows),
            upper_bound: rows,
        }
    }

    /// A bound only.
    pub fn at_most(rows: usize) -> Self {
        Self {
            exact: None,
            upper_bound: rows,
        }
    }
}

/// The shape of a view's selection over its base rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionKind {
    /// Every base row (an unrefined view).
    All,
    /// One contiguous run of base rows: a subject prefix search or a zone-map
    /// envelope.
    Range,
    /// An explicit list of base rows: an index lookup or a mask scan.
    Ids,
    /// An index run whose base row ids have not been read yet (a served
    /// match); reading the view materializes them.
    PendingIndexRun,
}

/// What a view promises about its rows before reading them. The row order
/// described is the one [`row_chunks`](VortexRdfStore::row_chunks),
/// [`code_chunks`](VortexRdfStore::code_chunks) and
/// [`data_source`](VortexRdfStore::data_source) produce: base row order,
/// whatever answered the match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewStatistics {
    /// The row count, exact or bounded.
    pub rows: RowCountHint,
    /// The order the rows come out in, when it is a known sort: `Spog` for a
    /// view over a globally sorted base.
    pub sort_order: Option<SortOrder>,
    /// Per column (`s`, `p`, `o`, `g`), the inclusive code range the view's
    /// rows are known to lie in (a bound column's single code, a subject
    /// run's first and last code), or `None`. Only the Dictionary layout has
    /// codes.
    pub code_bounds: [Option<(u32, u32)>; 4],
    /// The shape of the view's base row selection.
    pub selection: SelectionKind,
    /// The index child that answered the match and serves `quads()` in its
    /// own order, if any.
    pub served_component: Option<&'static str>,
    /// Whether a pushed-down file filter is still to be evaluated; then the
    /// row count is a bound and every read runs the filter.
    pub pending_filter: bool,
    /// Rows tombstoned in the base (never in the count).
    pub tombstones: usize,
    /// Live rows in the append tail, which follow the base.
    pub tail_rows: usize,
    /// Whether the base is read from a file.
    pub file_backed: bool,
    /// The [`generation`](VortexRdfStore::generation) of the data.
    pub generation: u64,
}

/// One of a store's index children.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexComponentInfo {
    /// The child's component name (`index:posg`, `index:ref-o`, …), what
    /// [`component_data_source`](VortexRdfStore::component_data_source)
    /// takes.
    pub name: &'static str,
    /// The index the child belongs to.
    pub index: IndexType,
    /// The child's row order, when it is a quad order (`index:posg`,
    /// `index:ospg`); the reference children are sorted `{val, rid}` pairs.
    pub sort_order: Option<SortOrder>,
    /// Whether the child's sort keys are globally sorted (the writer's
    /// provenance), which makes it binary-searchable.
    pub sorted: bool,
    /// The child's row count, when known without reading it.
    pub rows: Option<usize>,
    /// Whether the child's rows are canonical in memory (otherwise they are
    /// read from the file, or adopted on first use).
    pub resident: bool,
}

impl VortexRdfStore {
    /// Identity of the data behind this view. Every view derived from one
    /// store (`match_pattern`, `keep`, `window`, `partitions`) carries its
    /// generation; a mutation, a `compact` and a fresh open or build produce
    /// a new one. Equal generations hold the same rows, codes and tail, so a
    /// consumer can key cached plans or statistics on it.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// What this view promises about its rows before reading them (see
    /// [`ViewStatistics`]). No I/O: a count that would need a scan is a
    /// bound, a code range that would need a read is `None`.
    pub fn view_statistics(&self) -> ViewStatistics {
        let tail_rows = self.tail_size();
        let rows = match (self.quads.live_len_if_known(), self.quads.has_filter()) {
            (Some(live), false) => RowCountHint::exact(live + tail_rows),
            (Some(live), true) => RowCountHint::at_most(live + tail_rows),
            (None, _) => RowCountHint::at_most(self.quads.base_len() + tail_rows),
        };
        let selection = match self.quads.view_selection() {
            ViewSelection::Exact(RowSelection::All) => SelectionKind::All,
            ViewSelection::Exact(RowSelection::Range(_)) => SelectionKind::Range,
            ViewSelection::Exact(RowSelection::Ids(_)) => SelectionKind::Ids,
            ViewSelection::Pending(_) => SelectionKind::PendingIndexRun,
        };
        let code_bounds = if self.layout.strategy() == crate::store::LayoutStrategy::Dictionary {
            self.code_bounds()
        } else {
            [None; 4]
        };
        ViewStatistics {
            rows,
            sort_order: self.quads.subject_sorted().then_some(SortOrder::Spog),
            code_bounds,
            selection,
            served_component: self.quads.served_component(),
            pending_filter: self.quads.has_filter(),
            tombstones: self.quads.deleted().map_or(0, Mask::true_count),
            tail_rows,
            file_backed: self.quads.is_file_backed(),
            generation: self.generation,
        }
    }

    /// The code bounds of a Dictionary view: in memory the subject column's
    /// first and last codes over a contiguous, tombstone-free run of a
    /// sorted base; on file the single code of each pushed-down equality.
    fn code_bounds(&self) -> [Option<(u32, u32)>; 4] {
        let mut bounds: [Option<(u32, u32)>; 4] = [None; 4];
        match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                ..
            } => {
                let base_len = base.len();
                if self.quads.subject_sorted()
                    && deleted.is_none()
                    && let ViewSelection::Exact(sel) = selection
                    && let Some(range) = match sel {
                        RowSelection::All if base_len > 0 => Some(0..base_len as u64),
                        RowSelection::Range(r) if r.end > r.start => Some(r.clone()),
                        _ => None,
                    }
                    && let Some(probe) = probes.by_name(base, QuadColumn::S.name())
                    && let (Ok(lo), Ok(hi)) = (
                        u32::try_from(probe.value_at(range.start as usize)),
                        u32::try_from(probe.value_at(range.end as usize - 1)),
                    )
                {
                    bounds[QuadColumn::S.index()] = Some((lo, hi));
                }
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { filter, .. } => {
                if let Some(filter) = filter
                    && let Some(pairs) = crate::store::scan::file_reads::eq_code_pairs(filter)
                {
                    for (column, code) in pairs {
                        if let (Some(column), Ok(code)) =
                            (QuadColumn::from_name(&column), u32::try_from(code))
                        {
                            bounds[column.index()] = Some((code, code));
                        }
                    }
                }
            }
        }
        bounds
    }

    /// The store's index children (see [`IndexComponentInfo`]), in the order
    /// they are held: the tables
    /// [`component_data_source`](Self::component_data_source) serves beside
    /// the quads.
    pub fn index_components(&self) -> Vec<IndexComponentInfo> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => components
                .iter()
                .filter_map(|c| {
                    Some(IndexComponentInfo {
                        name: c.identity.name,
                        index: IndexType::of_component(c.identity.name)?,
                        sort_order: sort_order_of(c.identity.name),
                        sorted: c.sorted,
                        rows: c.len_if_resident(),
                        resident: true,
                    })
                })
                .collect(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file
                .components()
                .iter()
                .filter_map(|descriptor| {
                    let known = crate::store::indexes::component_named(&descriptor.name)?;
                    let name = known.identity.name;
                    let rows = file.child_reader(name).ok().flatten().map(|child| {
                        usize::try_from(child.reader.row_count()).unwrap_or(usize::MAX)
                    });
                    Some(IndexComponentInfo {
                        name,
                        index: known.index,
                        sort_order: sort_order_of(name),
                        sorted: descriptor.sorted,
                        rows,
                        resident: false,
                    })
                })
                .collect(),
        }
    }

    /// This view cut into at most `n` views over disjoint, contiguous pieces
    /// of its base rows, in base row order, so reading the partitions in
    /// turn reads the view. Each partition keeps the view's pushed-down
    /// filter and tombstones and evaluates them on its own rows; the append
    /// tail goes with the last partition. A served match materializes its
    /// row ids first. Sizes are about equal before filtering, not after.
    pub async fn partitions(&self, n: NonZeroUsize) -> Result<Vec<Self>> {
        let n = n.get();
        if n == 1 {
            return Ok(vec![self.clone()]);
        }
        let pieces = self
            .quads
            .materialized_selection()
            .await?
            .split(n, self.quads.base_len());
        if pieces.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let last = pieces.len() - 1;
        Ok(pieces
            .into_iter()
            .enumerate()
            .map(|(i, piece)| {
                let tail = self.tail.as_ref().map(|tail| {
                    if i == last {
                        tail.clone()
                    } else {
                        tail.with_selection(RowSelection::empty())
                    }
                });
                self.derived(self.quads.with_selection(ViewSelection::Exact(piece)), tail)
            })
            .collect())
    }
}

/// The quad order of a copy child, `None` for every other child.
fn sort_order_of(name: &str) -> Option<SortOrder> {
    CopyFamily::of_component(name).map(CopyFamily::sort_order)
}
