//! Planner-facing metadata about a view, answered without reading its rows:
//! how many it holds and how surely, the order they come out in, the codes
//! that bound them, the store's index children, the view's partitions, and
//! the identity of the data behind it.

use std::num::NonZeroUsize;
use std::ops::Range;

use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::indexes::copy::CopyFamily;
use crate::store::indexes::{ComponentIdentity, IndexType, LazyRowIds};
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
    /// An index run whose base row ids are not read yet: a served match, or
    /// a window, keep, partition or chained match of one, whose reads take
    /// its rows off the index. Reading such a view in base order, or reading
    /// a view built only to be counted or windowed, materializes them.
    PendingIndexRun,
}

/// What a view promises about its rows before they are read: how many, the
/// order every read of them follows (a served view's base rows in its
/// index's order, any other view's in base row order, then the tail's), the
/// code ranges they lie in, and how the view selects them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewStatistics {
    /// The row count, base and tail, exact or bounded.
    pub rows: RowCountHint,
    /// The order the view's rows come out in, when it is a known sort: the
    /// order every read follows ([`quads`](VortexRdfStore::quads),
    /// [`row_chunks`](VortexRdfStore::row_chunks),
    /// [`code_chunks`](VortexRdfStore::code_chunks), the exports). `Posg` for
    /// a view the `index:posg` copy serves with the object unbound (`?p??`,
    /// and on file `?p?g`) and for its windows, keeps, partitions and chained
    /// matches; `Spog` for any other view over a globally sorted base (every
    /// base this crate writes), a served one's index order being base order
    /// within its bound terms. A served file view whose index child is not
    /// globally sorted has none, and neither has a view that reads any tail
    /// row, since the tail's rows follow the base's in append order.
    pub sort_order: Option<SortOrder>,
    /// Per column (`s`, `p`, `o`, `g`), the inclusive code range the view's
    /// base rows are known to lie in (a pushed-down equality's code, a
    /// subject run's first and last code), or `None` when nothing is known
    /// without reading. Only the Dictionary layout has codes, and only in the
    /// base.
    pub code_bounds: [Option<(TermCode, TermCode)>; 4],
    /// The shape of the view's selection over its base rows.
    pub selection: SelectionKind,
    /// The index child that answered the match and serves every read of the
    /// view, in its own order (see [`sort_order`](Self::sort_order)), if
    /// any. A window, keep, partition or chained match of a served view is
    /// served by the same child.
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
            sort_order: (tail_rows == 0).then(|| self.quads.read_order()).flatten(),
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
    /// of its rows, in the view's order, so reading the partitions in turn
    /// reads the view. A view a by-copy index serves (a match, or a window,
    /// keep or chained match of one) is cut in its index's order into served
    /// windows of about equal live rows, as [`window`](Self::window) cuts
    /// one (on file an unlocated run is read once by row index first), the
    /// tail with the last: every partition keeps the view's
    /// [`sort_order`](ViewStatistics::sort_order), the last only when the
    /// view reads no tail row. Any other view is cut in base row order: each
    /// keeps the view's pushed-down filter and tombstones and applies them to
    /// its own rows, its pieces about equal before filtering, not after; the
    /// append tail goes with the last, so over a sorted base every partition
    /// but the last keeps the `Spog` sort order, and the last keeps it only
    /// when the view reads no tail row. For `n` = 1 the one partition is the
    /// view itself, its serve plan and pending selection included; a view
    /// selecting no base row is its own one partition.
    pub async fn partitions(&self, n: NonZeroUsize) -> Result<Vec<Self>> {
        let n = n.get();
        if n == 1 {
            return Ok(vec![self.clone()]);
        }
        if let Some(parts) = self.served_partitions(n)? {
            return Ok(parts);
        }
        #[cfg(feature = "file-io")]
        if let Some(parts) = self.served_file_partitions(n).await? {
            return Ok(parts);
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

impl VortexRdfStore {
    /// A view an in-memory plan serves cut into at most `n` served windows
    /// of `ceil(rows / n)` of its live rows each, in its index's order, the
    /// tail with the last; each window a part of the plan's range, or a slice
    /// of the rows a keep kept, with its row ids kept unsorted, as
    /// [`served_window_in_memory`](Self::served_window_in_memory) builds one.
    /// `None` for a view no in-memory plan serves, or one whose base holds no
    /// live row.
    fn served_partitions(&self, n: usize) -> Result<Option<Vec<Self>>> {
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        // The view's live rows: the rows a keep kept, or the live positions
        // under tombstones, or else the range's.
        let deleted = self.quads.deleted.as_ref();
        let (kept, positions, rids) = match (plan.kept(), deleted) {
            (Some(kept), deleted) => {
                let kept = kept.live(deleted);
                let rids = kept.rids.clone();
                (Some(kept), None, rids)
            }
            (None, None) => (None, None, plan.live_rids(None)?),
            (None, Some(deleted)) => {
                let (positions, rids) = plan.live_rows(Some(deleted))?;
                (None, Some(positions), Buffer::from(rids))
            }
        };
        let rows = rids.len();
        if rows == 0 {
            return Ok(None);
        }
        let size = rows.div_ceil(n);
        let range = plan.range();
        let pieces: Vec<Range<usize>> = (0..rows)
            .step_by(size)
            .map(|start| start..(start + size).min(rows))
            .collect();
        let last = pieces.len() - 1;
        Ok(Some(
            pieces
                .into_iter()
                .enumerate()
                .map(|(i, piece)| {
                    let narrowed = match (&kept, &positions) {
                        (Some(kept), _) => plan.with_rows(kept.slice(piece.clone())),
                        (None, None) => {
                            plan.narrowed(range.start + piece.start..range.start + piece.end)
                        }
                        (None, Some(positions)) => {
                            plan.narrowed(positions[piece.start]..positions[piece.end - 1] + 1)
                        }
                    };
                    let ids = LazyRowIds::from_unsorted_ids(rids.slice(piece), true);
                    let quads = self.quads.in_memory_with(
                        memory,
                        ViewSelection::pending(ids),
                        Some(narrowed),
                    );
                    let tail = self.tail.as_ref().map(|tail| {
                        if i == last {
                            tail.clone()
                        } else {
                            tail.with_selection(RowSelection::empty())
                        }
                    });
                    self.derived(quads, tail)
                })
                .collect(),
        ))
    }
}

impl VortexRdfStore {
    /// [`served_partitions`](Self::served_partitions) of a view a file plan
    /// serves over a located run: at most `n` served windows of its live
    /// rows in its index's order, each a part of the located range whose ids
    /// read on first need from those rows of the child's rid column, or,
    /// under tombstones, over kept rows or over an unlocated run (read once by
    /// row index), a slice of its live rows with their ids unsorted. The tail
    /// goes with the last. `None` for a view no file plan serves, or one
    /// whose base holds no live row.
    #[cfg(feature = "file-io")]
    async fn served_file_partitions(&self, n: usize) -> Result<Option<Vec<Self>>> {
        use std::sync::Arc;

        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        let Some(plan) = &base.serve else {
            return Ok(None);
        };
        // An unlocated run is located first, read once by row index.
        let plan = &plan.located().await?;
        let deleted = self.quads.deleted.as_ref();
        let range = plan.located_range();
        // The live rows: the located range whole, or listed (the kept rows,
        // or a located range's under tombstones).
        let listed = match (plan.kept(), &range, deleted) {
            (None, Some(_), None) => None,
            _ => Some(plan.live_rows(&base.file, deleted).await?),
        };
        let rows = match (&listed, &range) {
            (Some(listed), _) => listed.len(),
            (None, Some(range)) => (range.end - range.start) as usize,
            (None, None) => unreachable!("a located plan's rows are listed or ranged"),
        };
        if rows == 0 {
            return Ok(None);
        }
        let size = rows.div_ceil(n);
        let pieces: Vec<Range<usize>> = (0..rows)
            .step_by(size)
            .map(|start| start..(start + size).min(rows))
            .collect();
        let last = pieces.len() - 1;
        Ok(Some(
            pieces
                .into_iter()
                .enumerate()
                .map(|(i, piece)| {
                    let (narrowed, ids) = match (&listed, &range) {
                        (Some(listed), _) => {
                            let rows = listed.slice(piece);
                            let ids = LazyRowIds::from_unsorted_ids(rows.rids.clone(), true);
                            let narrowed = match (plan.kept(), &range) {
                                (None, Some(_)) => {
                                    let first = rows.positions[0];
                                    plan.narrowed(first..rows.positions[rows.len() - 1] + 1)
                                }
                                _ => plan.with_rows(rows),
                            };
                            (narrowed, ids)
                        }
                        (None, Some(range)) => {
                            let start = range.start + piece.start as u64;
                            let cut = start..range.start + piece.end as u64;
                            let ids = LazyRowIds::from_located_run(
                                Arc::clone(&base.file),
                                plan.child().clone(),
                                cut.clone(),
                            );
                            (plan.narrowed(cut), ids)
                        }
                        (None, None) => unreachable!("a located plan's rows are listed or ranged"),
                    };
                    let quads = self
                        .quads
                        .file_with(
                            base,
                            base.filter.clone(),
                            ViewSelection::pending(ids),
                            Some(narrowed),
                        )
                        // Every narrowed row passes the filter: its counts and reads take
                        // the plan's rows, the filter's pinned codes kept for reads by id.
                        .with_filter_resolved();
                    let tail = self.tail.as_ref().map(|tail| {
                        if i == last {
                            tail.clone()
                        } else {
                            tail.with_selection(RowSelection::empty())
                        }
                    });
                    self.derived(quads, tail)
                })
                .collect(),
        ))
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
