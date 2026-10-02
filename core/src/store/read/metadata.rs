//! Planner-facing metadata about a view: how many rows it holds and how
//! surely, the order its rows come out in, what its codes are bounded by,
//! which of the store's index children exist, how to split it into
//! partitions, and what identifies the data behind it — everything a query
//! engine asks before it reads, answered without reading.

use std::num::NonZeroUsize;

use crate::error::Result;
use crate::store::QuadsSource;
use crate::store::VortexRdfStore;
use crate::store::array::subject_sorted;
use crate::store::indexes::IndexType;
use crate::store::schema::QuadColumn;
use crate::store::view::selection::{RowSelection, ViewSelection};

/// A sort order of quad rows, by the columns compared first to last.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// `(s, p, o, g)` — the base's order in every store built by this crate.
    Spog,
    /// `(p, o, s, g)` — the `index:posg` child's order.
    Posg,
    /// `(o, s, p, g)` — the `index:ospg` child's order.
    Ospg,
}

/// How many rows a view holds: exactly, when nothing but an in-memory
/// gather is pending, or at most, while a pushed-down file filter has rows
/// still to test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowCountHint {
    /// The row count, when it is known without reading.
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
    /// One contiguous run of base rows — a subject prefix search or a
    /// zone-map envelope.
    Range,
    /// An explicit list of base rows — an index lookup or a mask scan.
    Ids,
    /// An index run whose base row ids have not been read yet (a served
    /// match); reading the view materializes them.
    PendingIndexRun,
}

/// What a view promises about its rows before reading them. The row order
/// described is the one [`row_chunks`](VortexRdfStore::row_chunks),
/// [`code_chunks`](VortexRdfStore::code_chunks) and
/// [`data_source`](VortexRdfStore::data_source) produce — base row order,
/// whatever answered the match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewStatistics {
    /// The row count, exact or bounded.
    pub rows: RowCountHint,
    /// The order the rows come out in, when it is a known sort: `Spog` for
    /// a view over a globally sorted base.
    pub sort_order: Option<SortOrder>,
    /// Per column (`s`, `p`, `o`, `g`), the inclusive code range the view's
    /// rows are known to lie in — a bound column's single code, a subject
    /// run's first and last code — or `None` when nothing is known. Only
    /// the Dictionary layout has codes.
    pub code_bounds: [Option<(u32, u32)>; 4],
    /// The shape of the view's base row selection.
    pub selection: SelectionKind,
    /// The index child that answered the match and will serve a decode in
    /// its own order (`quads`), if any.
    pub served_component: Option<&'static str>,
    /// Whether a pushed-down file filter is still to be evaluated — then
    /// the row count is a bound and every read runs the filter.
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
    /// The child's component name (`index:posg`, `index:ref-o`, …) — what
    /// [`component_data_source`](VortexRdfStore::component_data_source)
    /// takes.
    pub name: &'static str,
    /// The index the child belongs to.
    pub index: IndexType,
    /// The child's row order, when it is a quad order (`index:posg`,
    /// `index:ospg`); the reference children are sorted `{val, rid}` pairs.
    pub sort_order: Option<SortOrder>,
    /// Whether the child's sort keys are globally sorted (the writer's
    /// provenance), which is what makes it binary-searchable.
    pub sorted: bool,
    /// The child's row count, when known without reading it.
    pub rows: Option<usize>,
    /// Whether the child's rows are canonical in memory (otherwise they are
    /// read from the file, or adopted on first use).
    pub resident: bool,
}

impl VortexRdfStore {
    /// Identity of the data behind this view. Every view derived from one
    /// store — by `match_pattern`, `keep`, `window`, `partitions` — carries
    /// its generation; a mutation (`add_quads`, `delete_quad`), a `compact`
    /// and a fresh open or build produce a new one. Two stores with equal
    /// generations hold the same rows, codes and tail, so a consumer can key
    /// cached plans or statistics on it.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// What this view promises about its rows before reading them (see
    /// [`ViewStatistics`]). No I/O: a count that would need a scan is a
    /// bound, a code range that would need a read is `None`.
    pub fn view_statistics(&self) -> ViewStatistics {
        let tail_rows = self.tail_size();
        let is_dictionary = self.layout.strategy() == crate::store::LayoutStrategy::Dictionary;
        let mut code_bounds: [Option<(u32, u32)>; 4] = [None; 4];
        let (
            rows,
            sort_order,
            selection,
            served_component,
            pending_filter,
            tombstones,
            file_backed,
        ) = match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                deleted,
                probes,
                serve,
                ..
            } => {
                let base_len = base.len();
                let tombstones = deleted.as_ref().map_or(0, |m| m.true_count());
                let (kind, exact) = match selection {
                    ViewSelection::Exact(sel) => (
                        selection_kind(sel),
                        Some(live_rows(sel, deleted.as_ref(), base_len)),
                    ),
                    ViewSelection::Pending(lazy) => (
                        SelectionKind::PendingIndexRun,
                        match deleted {
                            None => lazy.len_if_known(),
                            // Live ids of a run with tombstones: read
                            // the run (in memory, no I/O).
                            Some(deleted) => lazy.materialized().ok().map(|ids| {
                                ids.iter().filter(|&&i| !deleted.value(i as usize)).count()
                            }),
                        },
                    ),
                };
                let rows = match exact {
                    Some(n) => RowCountHint::exact(n + tail_rows),
                    None => RowCountHint::at_most(base_len + tail_rows),
                };
                let sorted = subject_sorted(base);
                // A contiguous, tombstone-free run of a sorted base: the
                // subject column's first and last codes bound the view.
                if is_dictionary
                    && sorted
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
                    code_bounds[QuadColumn::S.index()] = Some((lo, hi));
                }
                (
                    rows,
                    sorted.then_some(SortOrder::Spog),
                    kind,
                    serve.as_ref().map(|_| served_in_memory(self)),
                    false,
                    tombstones,
                    false,
                )
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                serve,
                ..
            } => {
                let base_len = file.row_count() as usize;
                let tombstones = deleted.as_ref().map_or(0, |m| m.true_count());
                let (kind, selected) = match selection {
                    ViewSelection::Exact(sel) => (
                        selection_kind(sel),
                        Some(live_rows(sel, deleted.as_ref(), base_len)),
                    ),
                    ViewSelection::Pending(lazy) => (
                        SelectionKind::PendingIndexRun,
                        // A run's width is known from its range; its
                        // live count only once the ids are read.
                        if deleted.is_none() {
                            lazy.len_if_known().or_else(|| {
                                serve
                                    .as_ref()
                                    .and_then(|plan| plan.row_range())
                                    .map(|r| usize::try_from(r.end - r.start).unwrap_or(usize::MAX))
                            })
                        } else {
                            None
                        },
                    ),
                };
                let rows = match (selected, filter) {
                    (Some(n), None) => RowCountHint::exact(n + tail_rows),
                    (Some(n), Some(_)) => RowCountHint::at_most(n + tail_rows),
                    (None, _) => RowCountHint::at_most(base_len + tail_rows),
                };
                // The pushed-down equalities bound their columns exactly.
                if is_dictionary
                    && let Some(f) = filter
                    && let Some(pairs) = crate::store::scan::file_scan::eq_code_pairs(f)
                {
                    for (column, code) in pairs {
                        if let (Some(column), Ok(code)) =
                            (QuadColumn::from_name(&column), u32::try_from(code))
                        {
                            code_bounds[column.index()] = Some((code, code));
                        }
                    }
                }
                (
                    rows,
                    file.quads_sorted().then_some(SortOrder::Spog),
                    kind,
                    serve.as_ref().map(|plan| plan.component()),
                    filter.is_some(),
                    tombstones,
                    true,
                )
            }
        };
        ViewStatistics {
            rows,
            sort_order,
            code_bounds,
            selection,
            served_component,
            pending_filter,
            tombstones,
            tail_rows,
            file_backed,
            generation: self.generation,
        }
    }

    /// The store's index children (see [`IndexComponentInfo`]), in the
    /// order they are held — the tables
    /// [`component_data_source`](Self::component_data_source) serves beside
    /// the quads.
    pub fn index_components(&self) -> Vec<IndexComponentInfo> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => components
                .iter()
                .filter_map(|c| {
                    let index = index_of_slug(c.slug)?;
                    Some(IndexComponentInfo {
                        name: c.name,
                        index,
                        sort_order: sort_order_of(c.name),
                        sorted: c.sorted,
                        rows: c.len_if_resident(),
                        resident: true,
                    })
                })
                .collect(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => {
                file.components()
                    .iter()
                    .filter_map(|descriptor| {
                        let (index, name) = crate::store::indexes::ALL_INDEX_TYPES
                            .iter()
                            .find_map(|index| {
                                index
                                    .component_identities()
                                    .iter()
                                    .find(|identity| identity.name == descriptor.name)
                                    .map(|identity| (*index, identity.name))
                            })?;
                        let rows = file
                            .component_reader(name)
                            .ok()
                            .flatten()
                            .map(|(_, reader)| {
                                usize::try_from(reader.row_count()).unwrap_or(usize::MAX)
                            });
                        Some(IndexComponentInfo {
                            name,
                            index,
                            sort_order: sort_order_of(name),
                            sorted: descriptor.sorted,
                            rows,
                            resident: false,
                        })
                    })
                    .collect()
            }
        }
    }

    /// This view cut into at most `n` views over disjoint, contiguous pieces
    /// of its base rows — in base row order, so reading the partitions in
    /// turn reads the view — for an engine that scans in parallel. Each
    /// partition keeps the view's pushed-down filter and tombstones and
    /// evaluates them on its own rows only; the append tail goes with the
    /// last partition. A served match materializes its row ids first.
    /// Sizes are about equal before filtering, not after.
    pub async fn partitions(&self, n: NonZeroUsize) -> Result<Vec<Self>> {
        let n = n.get();
        if n == 1 {
            return Ok(vec![self.clone()]);
        }
        let pieces = match &self.quads {
            QuadsSource::InMemory {
                base, selection, ..
            } => selection.materialized()?.split(n, base.len()),
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file, selection, ..
            } => selection
                .materialized_async()
                .await?
                .split(n, file.row_count() as usize),
        };
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
                let quads = match &self.quads {
                    QuadsSource::InMemory {
                        base,
                        components,
                        deleted,
                        probes,
                        ..
                    } => QuadsSource::InMemory {
                        base: base.clone(),
                        selection: ViewSelection::Exact(piece),
                        components: std::sync::Arc::clone(components),
                        deleted: deleted.clone(),
                        probes: std::sync::Arc::clone(probes),
                        serve: None,
                    },
                    #[cfg(feature = "file-io")]
                    QuadsSource::File {
                        path,
                        dict_max_resident_bytes,
                        file,
                        filter,
                        deleted,
                        ..
                    } => QuadsSource::File {
                        path: path.clone(),
                        dict_max_resident_bytes: *dict_max_resident_bytes,
                        file: file.clone(),
                        filter: filter.clone(),
                        selection: ViewSelection::Exact(piece),
                        deleted: deleted.clone(),
                        serve: None,
                    },
                };
                Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    generation: self.generation,
                    quads,
                    tail,
                }
            })
            .collect())
    }
}

/// The in-memory serve plan's component: the by-copy index is the only one
/// that serves, and its plan reads from the family it resolved.
fn served_in_memory(store: &VortexRdfStore) -> &'static str {
    // An in-memory serve plan is always a by-copy family's run; which one
    // is not recorded on the plan, so report the index.
    let _ = store;
    "index:secondary-by-copy"
}

fn selection_kind(selection: &RowSelection) -> SelectionKind {
    match selection {
        RowSelection::All => SelectionKind::All,
        RowSelection::Range(_) => SelectionKind::Range,
        RowSelection::Ids(_) => SelectionKind::Ids,
    }
}

/// The rows an exact selection covers minus its tombstones.
fn live_rows(
    selection: &RowSelection,
    deleted: Option<&vortex_mask::Mask>,
    base_len: usize,
) -> usize {
    match deleted {
        None => selection.len(base_len),
        Some(deleted) => selection.live_mask(deleted, base_len).true_count(),
    }
}

fn sort_order_of(name: &str) -> Option<SortOrder> {
    match name {
        "index:posg" => Some(SortOrder::Posg),
        "index:ospg" => Some(SortOrder::Ospg),
        _ => None,
    }
}

/// The index an implementation slug (`secondary-by-copy/posg`, …) belongs
/// to.
fn index_of_slug(slug: &str) -> Option<IndexType> {
    slug.split('/').next()?.parse().ok()
}
