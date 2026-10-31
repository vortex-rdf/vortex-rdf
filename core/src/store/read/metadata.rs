//! Planner-facing metadata about a view, answered without reading its rows:
//! how many it holds and how surely, the order they come out in, the codes
//! that bound them, the store's index children, the view's partitions, and
//! the identity of the data behind it.

use std::num::NonZeroUsize;

use vortex_mask::Mask;

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::indexes::copy::CopyFamily;
use crate::store::indexes::{ComponentIdentity, IndexType};
use crate::store::read::count::BaseRows;
use crate::store::schema::{QuadColumn, TermCode};
use crate::store::view::selection::{RowSelection, SelectedRows, ViewSelection};
use crate::store::{Backend, LayoutStrategy, MemoryBase, VortexRdfStore};

/// A sort order of quad rows, by the columns compared first to last.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// `(s, p, o, g)`: the base's order in every store this crate builds.
    Spog,
    /// `(p, o, s, g)`: the `index:posg` child's order.
    Posg,
    /// `(o, s, p, g)`: the `index:ospg` child's order.
    Ospg,
}

/// How many rows a view holds: exactly, or at most while a pushed-down file
/// filter has rows still to test, or while a file view's index run has row
/// ids unread that its width cannot count (the run is not located, or
/// tombstones are to be counted out).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowCountHint {
    /// The row count, when it is known without reading.
    pub exact: Option<usize>,
    /// A count the view never exceeds (`exact`, when that is known).
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
    /// One contiguous run of base rows: a subject prefix search or a
    /// zone-map envelope.
    Range,
    /// An explicit list of base rows: an index lookup or a mask scan.
    Ids,
    /// An index run whose base row ids are not read yet (a served match);
    /// reading the view materializes them.
    PendingIndexRun,
}

/// What a view promises about its rows before they are read, for the order
/// [`row_chunks`](VortexRdfStore::row_chunks) and
/// [`code_chunks`](VortexRdfStore::code_chunks) produce: the base's rows in
/// base row order, whatever answered the match, then the tail's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewStatistics {
    /// The row count, base and tail, exact or bounded.
    pub rows: RowCountHint,
    /// The order the view's rows come out in, when it is a known sort:
    /// `Spog` for a view over a globally sorted base that reads no tail row.
    /// A view that reads any has none, since the tail's rows follow the
    /// base's in append order.
    pub sort_order: Option<SortOrder>,
    /// Per column (`s`, `p`, `o`, `g`), the inclusive code range the view's
    /// base rows are known to lie in (a pushed-down equality's code, a
    /// subject run's first and last code), or `None` when nothing is known
    /// without reading. Only the Dictionary layout has codes, and only in the
    /// base.
    pub code_bounds: [Option<(TermCode, TermCode)>; 4],
    /// The shape of the view's selection over its base rows.
    pub selection: SelectionKind,
    /// The index child that answered the match and decodes
    /// [`quads`](VortexRdfStore::quads) in its own order, if any.
    pub served_component: Option<&'static str>,
    /// Whether a pushed-down file filter is still to be evaluated: then the
    /// row count is a bound, and every read runs the filter. A filter a
    /// window has evaluated over every row, finding it cuts none, is not
    /// pending: its matches are counted exactly and read without running it
    /// again.
    pub pending_filter: bool,
    /// Rows tombstoned in the base (never counted).
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
    /// The child's component name (`index:posg`, `index:ref-o`, …).
    pub name: &'static str,
    /// The index the child belongs to.
    pub index: IndexType,
    /// The child's row order when it is a quad order (`index:posg`,
    /// `index:ospg`); the reference children are sorted `{val, rid}` pairs.
    pub sort_order: Option<SortOrder>,
    /// Whether the child's sort keys are globally sorted (the writer's
    /// provenance), which is what makes it binary-searchable.
    pub sorted: bool,
    /// The child's row count: one row per quad of the base.
    pub rows: usize,
}

impl VortexRdfStore {
    /// The identity of the data behind this view. Every view derived from a
    /// store (by `match_pattern`, `keep`, `window` or `partitions`) carries
    /// its generation; a build, an open, `add_quads`, `delete_matching` and
    /// `compact` each take a new one from one process-wide counter (an
    /// `add_quads` that adds nothing returns the store as it is). Views of
    /// one generation select from the same base rows, codes and tail, so a
    /// consumer can key what it caches about the data on it; different
    /// generations promise nothing. A generation is never persisted.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// What this view promises about its rows before they are read (see
    /// [`ViewStatistics`]), without I/O: a count that would need a scan is a
    /// bound, a code range that would need a read is `None`. It is not a pure
    /// read in one case: to count an in-memory pending run's live rows under
    /// tombstones, it decodes the run's row ids, cached in the run's shared
    /// `LazyRowIds` for the view and every clone of it; that decode is the
    /// one way it fails.
    pub fn view_statistics(&self) -> Result<ViewStatistics> {
        let tail_rows = self.tail_size();
        // A filter whose masks a window kept is evaluated: the masks count
        // the base's live rows, an explicit set of them.
        let evaluated = self.quads.evaluated_len();
        let filtered = self.quads.has_filter() && evaluated.is_none();
        let selection = &self.quads.selection;
        // The base's live rows when known without I/O; else at most the
        // selection's rows before tombstones (a pending run's width), or the
        // base's.
        let base = self.quads.base_rows(|_| true)?;
        Ok(ViewStatistics {
            rows: row_count_hint(base, filtered, tail_rows),
            sort_order: (self.quads.subject_sorted() && tail_rows == 0).then_some(SortOrder::Spog),
            code_bounds: self.code_bounds(),
            selection: match evaluated {
                Some(_) => SelectionKind::Ids,
                None => selection_kind(selection),
            },
            served_component: self.quads.served_component(),
            pending_filter: filtered,
            tombstones: self.quads.deleted.as_ref().map_or(0, Mask::true_count),
            tail_rows,
            file_backed: self.quads.is_file_backed(),
            generation: self.generation,
        })
    }

    /// The [`code_bounds`](ViewStatistics::code_bounds) of this view: only a
    /// Dictionary layout has codes, and only in the base.
    fn code_bounds(&self) -> [Option<(TermCode, TermCode)>; 4] {
        let mut code_bounds = [None; 4];
        if self.layout.strategy() != LayoutStrategy::Dictionary {
            return code_bounds;
        }
        match &self.quads.backend {
            Backend::InMemory(MemoryBase { base, probes, .. }) => {
                let base_len = base.len();
                // A contiguous, tombstone-free run of a sorted base: codes are
                // sorted ranks, so the subject column's first and last codes
                // bound the view's.
                if self.quads.subject_sorted()
                    && self.quads.deleted.is_none()
                    && let ViewSelection::Exact(sel) = &self.quads.selection
                    && let SelectedRows::Run(run) = sel.as_rows(base_len)
                    && !run.is_empty()
                    && let Some(owned) = probes.by_name(base, QuadColumn::S.name()).flatten()
                {
                    let probe = owned.probe();
                    code_bounds[QuadColumn::S.index()] =
                        Some((probe.value_at(run.start), probe.value_at(run.end - 1)));
                }
            }
            #[cfg(feature = "file-io")]
            Backend::File(FileBase { filter, .. }) => {
                // The pushed-down equalities bound their columns exactly.
                if let Some(eqs) = filter.over_selection().and_then(|f| f.eqs()) {
                    for (column, code) in eqs.iter() {
                        if let Some(column) = QuadColumn::from_name(column.as_ref()) {
                            code_bounds[column.index()] = Some((code, code));
                        }
                    }
                }
            }
        }
        code_bounds
    }

    /// The store's index children (see [`IndexComponentInfo`]), in the order
    /// the store holds them, each classified by its implementation slug when
    /// the store was built or opened: a child under a slug this version does
    /// not know is not listed.
    pub fn index_components(&self) -> Result<Vec<IndexComponentInfo>> {
        let info = |identity: &ComponentIdentity, sorted: bool, rows: usize| IndexComponentInfo {
            name: identity.name,
            index: identity.index,
            sort_order: sort_order_of(identity.name),
            sorted,
            rows,
        };
        Ok(match &self.quads.backend {
            Backend::InMemory(MemoryBase {
                base, components, ..
            }) => components
                .iter()
                .map(|c| info(c.identity, c.sorted, base.len()))
                .collect(),
            #[cfg(feature = "file-io")]
            Backend::File(FileBase { file, .. }) => file
                .index_children()
                .iter()
                .map(|child| info(child.identity, child.sorted, file.row_count() as usize))
                .collect(),
        })
    }

    /// This view cut into at most `n` views over disjoint, contiguous pieces
    /// of its base rows, in base row order, so reading the partitions in
    /// turn reads the view. Each keeps the view's pushed-down filter and
    /// tombstones and applies them to its own rows; the append tail goes with
    /// the last, so over a sorted base every partition but the last keeps the
    /// `Spog` [`sort_order`](ViewStatistics::sort_order), and the last keeps
    /// it only when the view reads no tail row. For `n` = 1 the one partition
    /// is the view itself, its serve plan and pending selection included; for
    /// a larger `n`, a served match's row ids are read first and no partition
    /// keeps the serve plan. Pieces are about equal before filtering, not
    /// after; a view selecting no base row is its own one partition.
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

/// The count of a view whose base holds `base` rows, with a filter still to
/// test them when `filtered`, and `tail_rows` tail rows after them: exact
/// when the live base rows are counted and no filter is pending, else at
/// most their [`upper_bound`](BaseRows::upper_bound) plus the tail's.
fn row_count_hint(base: BaseRows, filtered: bool, tail_rows: usize) -> RowCountHint {
    let upper_bound = base.upper_bound() + tail_rows;
    match (base.live, filtered) {
        (Some(_), false) => RowCountHint::exact(upper_bound),
        _ => RowCountHint::at_most(upper_bound),
    }
}

fn selection_kind(selection: &ViewSelection) -> SelectionKind {
    match selection {
        ViewSelection::Exact(RowSelection::All) => SelectionKind::All,
        ViewSelection::Exact(RowSelection::Range(_)) => SelectionKind::Range,
        ViewSelection::Exact(RowSelection::Ids(_)) => SelectionKind::Ids,
        // A pending residual sits only on a view built to be counted or
        // windowed, which no caller holds.
        ViewSelection::Pending { .. } | ViewSelection::Residual(_) => {
            SelectionKind::PendingIndexRun
        }
    }
}

/// The quad order of a copy child, `None` for every other child.
fn sort_order_of(name: &str) -> Option<SortOrder> {
    [
        (CopyFamily::Posg, SortOrder::Posg),
        (CopyFamily::Ospg, SortOrder::Ospg),
    ]
    .into_iter()
    .find_map(|(family, order)| (family.identity().name == name).then_some(order))
}
