//! Narrowing a view beyond a pattern: [`Keep`] (the rows whose code in one
//! column is in a set or range), windows, and the empty view. Every
//! narrowing is a derived view that composes with
//! [`match_pattern`](VortexRdfStore::match_pattern) in either order and reads
//! through the same paths.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;

use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::store::QuadsSource;
use crate::store::array::{column_is_sorted, into_struct_array};
use crate::store::probes::StructProbes;
use crate::store::resident::cached_u32_primitive;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_filter, file_reads};
use crate::store::schema::{self, QuadColumn};
use crate::store::view::selection::{RowSelection, ViewSelection};

use crate::store::VortexRdfStore;

/// Which term codes a [`keep`](VortexRdfStore::keep) admits in a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Any of these codes; ascending and unique, as [`Keep::set`] builds
    /// them.
    Set(Buffer<u32>),
    /// Any code in the half-open range `lo..hi`. Codes rank the dictionary's
    /// spellings in byte order, so a term kind or an IRI namespace is one
    /// such range (see `DictReader::prefix_range`).
    Range(u32, u32),
}

/// A `Keep::Set` of up to this many codes is pushed to a file scan as an
/// `OR` of equalities (each a zone-prunable conjunct); larger sets go as one
/// `list_contains` conjunct.
#[cfg(feature = "file-io")]
const KEEP_SET_OR_MAX: usize = 32;

/// A `Keep::Set` wider than this is not pushed to the scan as an expression
/// at all: the column is read for the view's rows and tested in memory.
#[cfg(feature = "file-io")]
const KEEP_SET_FILTER_MAX: usize = 4_096;

impl Keep {
    /// The keep admitting exactly `codes` (any order, repeats folded).
    pub fn set(codes: impl IntoIterator<Item = u32>) -> Self {
        let mut codes: Vec<u32> = codes.into_iter().collect();
        codes.sort_unstable();
        codes.dedup();
        Keep::Set(Buffer::from(codes))
    }

    /// The keep admitting every code in `range`.
    pub fn range(range: Range<u32>) -> Self {
        Keep::Range(range.start, range.end.max(range.start))
    }

    /// Whether the keep admits no code at all.
    pub fn is_empty(&self) -> bool {
        match self {
            Keep::Set(codes) => codes.is_empty(),
            Keep::Range(lo, hi) => hi <= lo,
        }
    }

    /// Whether `code` is admitted.
    pub fn admits(&self, code: u32) -> bool {
        match self {
            Keep::Set(codes) => codes.as_slice().binary_search(&code).is_ok(),
            Keep::Range(lo, hi) => (*lo..*hi).contains(&code),
        }
    }

    /// The keep compiled for a row loop.
    fn test(&self) -> KeepTest<'_> {
        match self {
            Keep::Range(lo, hi) => KeepTest::Range(*lo, *hi),
            Keep::Set(codes) => {
                let codes = codes.as_slice();
                let (Some(&lo), Some(&hi)) = (codes.first(), codes.last()) else {
                    return KeepTest::Range(0, 0);
                };
                let span = (hi - lo) as usize + 1;
                // A bitmap while the span is within 8 bits per member.
                if span <= codes.len().saturating_mul(8) {
                    let mut bits = vec![0u64; span.div_ceil(64)];
                    for &code in codes {
                        let bit = (code - lo) as usize;
                        bits[bit / 64] |= 1u64 << (bit % 64);
                    }
                    KeepTest::Bitmap { lo, hi, bits }
                } else {
                    KeepTest::Sorted(codes)
                }
            }
        }
    }
}

/// A [`Keep`] as a per-row test: a range compare, a bitmap over a dense set's
/// span, or a binary search of a sparse one.
enum KeepTest<'a> {
    Range(u32, u32),
    Bitmap { lo: u32, hi: u32, bits: Vec<u64> },
    Sorted(&'a [u32]),
}

impl KeepTest<'_> {
    #[inline]
    fn admits(&self, code: u32) -> bool {
        match self {
            KeepTest::Range(lo, hi) => (*lo..*hi).contains(&code),
            KeepTest::Bitmap { lo, hi, bits } => {
                if code < *lo || code > *hi {
                    return false;
                }
                let bit = (code - lo) as usize;
                bits[bit / 64] & (1u64 << (bit % 64)) != 0
            }
            KeepTest::Sorted(codes) => codes.binary_search(&code).is_ok(),
        }
    }
}

/// The code at `row` of a base column, read without decoding the column:
/// its canonical primitive when one is already materialized, else the
/// store's cached encoded-search probe.
enum CodeReader<'a> {
    Slice(&'a [u32]),
    Probe(&'a vortex_rdf_encoded_search::OwnedSortedProbe),
}

impl CodeReader<'_> {
    #[inline]
    fn code_at(&self, row: usize) -> u32 {
        match self {
            CodeReader::Slice(slice) => slice[row],
            CodeReader::Probe(probe) => probe.value_at(row) as u32,
        }
    }
}

impl VortexRdfStore {
    // ── narrowing beyond a pattern ────────────────────────────────────────────

    /// An empty view of this store: the same base and tail with no row of
    /// either selected, no indexes, no serve plan.
    pub(crate) fn empty_view(&self) -> Self {
        let tail = self
            .tail
            .as_ref()
            .map(|tail| tail.with_selection(RowSelection::empty()));
        Self {
            indexes: vec![],
            ..self.derived(self.quads.emptied(), tail)
        }
    }

    /// Narrow this view to the rows whose code in `column` the keep admits
    /// (the rows a `VALUES` block or a term predicate selects; see
    /// `DictReader::filter_codes`). Composes with
    /// [`match_pattern`](Self::match_pattern) in either order; an empty keep
    /// is the empty view. Requires the Dictionary layout with an empty append
    /// tail, `InvalidOperation` otherwise. In memory: a binary search when the
    /// selected rows are a run of the sorted base that `column` orders, else
    /// one pass over the selected rows of the column. On file: a range or set
    /// conjunct on the pushed-down filter, pruned by zone maps; a very wide set
    /// is tested in memory. A served view's pending ids materialize first.
    pub async fn keep(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        self.ensure_code_view("keep")?;
        if keep.is_empty() {
            return Ok(self.empty_view());
        }
        match &self.quads {
            QuadsSource::InMemory { .. } => self.keep_in_memory(column, keep),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => self.keep_file(column, keep).await,
        }
    }

    /// [`keep`](Self::keep) for several columns at once, applied in the order
    /// given (each over the previous one's result).
    pub async fn keep_many(&self, keeps: &[(QuadColumn, Keep)]) -> Result<Self> {
        let mut view = self.clone();
        for (column, keep) in keeps {
            view = view.keep(*column, keep).await?;
        }
        Ok(view)
    }

    /// The view's rows after skipping `offset` and taking at most `limit`:
    /// base rows in base row order, then the tail's. A pending file filter is
    /// evaluated in file order only as far as the window reaches. The serve
    /// plan is dropped, so a served view's rows come back in base order.
    /// Available on every layout.
    pub async fn window(&self, offset: usize, limit: usize) -> Result<Self> {
        let t = debug::timer();
        let selection = self.quads.materialized_selection().await?;
        let (windowed, base_live) = self.base_window(&selection, offset, limit).await?;
        let base_taken = windowed.len(self.quads.base_len());
        let quads = self
            .quads
            .with_selection(ViewSelection::Exact(windowed))
            .without_filter();
        // Whatever the base could not fill falls to the tail.
        let tail = self.tail.as_ref().map(|tail| {
            tail.window(
                offset.saturating_sub(base_live),
                limit.saturating_sub(base_taken),
            )
        });
        log::debug!(
            "[window] offset {offset} limit {limit}: {base_taken} base rows at {:?}",
            debug::elapsed(t)
        );
        Ok(self.derived(quads, tail))
    }

    /// The base's window and its live row count. A pending file filter is
    /// evaluated only as far as the window reaches, so fewer rows than asked
    /// means the base is exhausted and their count is its live size.
    async fn base_window(
        &self,
        selection: &RowSelection,
        offset: usize,
        limit: usize,
    ) -> Result<(RowSelection, usize)> {
        let base_len = self.quads.base_len();
        let deleted = self.quads.deleted();
        #[cfg(feature = "file-io")]
        if let (Some(file), Some(filter)) = (self.quads.file(), self.quads.filter()) {
            let want = offset.saturating_add(limit);
            let found =
                file_filter::first_matching_rows(file, filter, selection, deleted, want).await?;
            let live = found.len();
            return Ok((
                RowSelection::from_ids(found).window(offset, limit, None, base_len),
                live,
            ));
        }
        Ok((
            selection.window(offset, limit, deleted, base_len),
            selection.live_len(deleted, base_len),
        ))
    }

    /// The in-memory backend of [`keep`](Self::keep).
    fn keep_in_memory(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        let t = debug::timer();
        #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
        let QuadsSource::InMemory {
            base,
            selection,
            probes,
            ..
        } = &self.quads
        else {
            unreachable!("keep routes only InMemory sources here");
        };
        let selection = selection.materialized()?;
        let base_len = base.len();
        if selection.is_empty(base_len) {
            return Ok(self.empty_view());
        }
        let struct_arr = into_struct_array(base.clone())?;
        let narrowed =
            match Self::keep_sorted_run(&struct_arr, base, probes, &selection, column, keep) {
                Some(narrowed) => {
                    log::debug!(
                        "[keep] {:?} narrowed by binary search inside a sorted run at {:?}",
                        column,
                        debug::elapsed(t)
                    );
                    narrowed
                }
                None => {
                    let narrowed =
                        Self::keep_by_pass(&struct_arr, base, probes, &selection, column, keep)?;
                    log::debug!(
                        "[keep] {:?} narrowed by a pass over {} selected rows at {:?}",
                        column,
                        selection.len(base_len),
                        debug::elapsed(t)
                    );
                    narrowed
                }
            };
        if narrowed.is_empty(base_len) {
            return Ok(self.empty_view());
        }
        Ok(self.derived(
            self.quads.with_selection(ViewSelection::Exact(narrowed)),
            self.tail.clone(),
        ))
    }

    /// The keep by binary search, when the selection is a run of the sorted
    /// base that `column` orders (every earlier column of the `(s, p, o, g)`
    /// order is constant over the run). A range keep is two lower bounds; a
    /// small set is one bounded sub-run per code. `None` declines to the row
    /// pass.
    fn keep_sorted_run(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Option<RowSelection> {
        let RowSelection::Range(range) = selection else {
            return None;
        };
        let s_col = struct_arr.unmasked_field_by_name(schema::COL_S).ok()?;
        if !column_is_sorted(s_col) {
            return None;
        }
        let run = range.start as usize..range.end as usize;
        if run.is_empty() {
            return None;
        }
        for earlier in &QuadColumn::ALL[..column.index()] {
            let probe = probes.by_name(base, earlier.name())?;
            if probe.value_at(run.start) != probe.value_at(run.end - 1) {
                return None;
            }
        }
        let probe = probes.by_name(base, column.name())?;
        let width = run.len();
        match keep {
            Keep::Range(lo, hi) => {
                let (start, _) = probe.bounds_in(run.clone(), u64::from(*lo));
                let (end, _) = probe.bounds_in(start..run.end, u64::from(*hi));
                Some(RowSelection::Range(start as u64..end as u64))
            }
            Keep::Set(codes) => {
                // One bounded search per code while that beats a pass.
                let searches = codes.len().saturating_mul(width.ilog2() as usize + 1);
                if searches >= width {
                    return None;
                }
                let mut ids: Vec<u64> = Vec::new();
                let mut from = run.start;
                for &code in codes.as_slice() {
                    let (lo, hi) = probe.bounds_in(from..run.end, u64::from(code));
                    ids.extend(lo as u64..hi as u64);
                    from = hi;
                    if from >= run.end {
                        break;
                    }
                }
                Some(RowSelection::Ids(Buffer::from(ids)))
            }
        }
    }

    /// The keep by one pass over the selected rows of `column`, read in place
    /// (its canonical primitive when materialized, else the cached probe), or
    /// decoded for the selected rows only when neither serves.
    fn keep_by_pass(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<RowSelection> {
        let col = struct_arr
            .unmasked_field_by_name(column.name())
            .map_err(VortexRdfError::Vortex)?;
        let test = keep.test();
        let cached = cached_u32_primitive(col);
        let reader = match (&cached, probes.by_name(base, column.name())) {
            (Some(prim), _) => Some(CodeReader::Slice(prim.as_slice::<u32>())),
            (None, Some(probe)) => Some(CodeReader::Probe(probe)),
            (None, None) => None,
        };
        let ids: Vec<u64> = match reader {
            Some(reader) => {
                let admit = |row: u64| test.admits(reader.code_at(row as usize));
                match selection {
                    RowSelection::All => (0..base.len() as u64).filter(|&r| admit(r)).collect(),
                    RowSelection::Range(r) => (r.start..r.end).filter(|&r| admit(r)).collect(),
                    RowSelection::Ids(ids) => ids.iter().copied().filter(|&r| admit(r)).collect(),
                }
            }
            None => {
                // The selected rows only, decoded once.
                use vortex_array::VortexSessionExecute as _;
                use vortex_array::arrays::PrimitiveArray;
                let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
                let prim = selection
                    .apply(col)?
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;
                return Ok(selection
                    .clone()
                    .refine(&keep_mask(prim.as_slice::<u32>(), &test)));
            }
        };
        Ok(RowSelection::from_ids(Buffer::from(ids)))
    }

    /// The file backend of [`keep`](Self::keep): the keep as a conjunct on
    /// the view's pushed-down filter (a range as two comparisons, a set as an
    /// `OR` of equalities or a `list_contains`), narrowed by zone-map
    /// pruning; a set too wide for an expression is tested in memory over the
    /// column read for the view's rows.
    #[cfg(feature = "file-io")]
    async fn keep_file(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        use vortex_array::expr::and;

        let t = debug::timer();
        let QuadsSource::File {
            file,
            filter,
            selection,
            ..
        } = &self.quads
        else {
            unreachable!("keep routes only File sources here");
        };
        let row_count = file.row_count() as usize;
        let selection = selection.materialized_async().await?;
        if selection.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        let (filter, selection) = match keep_conjunct(column, keep) {
            Some(conjunct) => {
                // Zone maps may bound the conjunct's rows (a namespace range
                // in a sorted column).
                let selection = match file_filter::row_range_from_pruning(file, &conjunct).await? {
                    Some(range) => selection.intersect_range(range),
                    None => selection,
                };
                let filter = match filter {
                    Some(existing) => and(existing.clone(), conjunct),
                    None => conjunct,
                };
                log::debug!(
                    "[keep] {:?} pushed to the file scan as a conjunct at {:?}",
                    column,
                    debug::elapsed(t)
                );
                (Some(filter), selection)
            }
            None => {
                // Too wide a set for an expression: the pending filter
                // resolves to exact rows, the column is read for them and
                // tested in memory.
                let selection = match filter {
                    Some(f) => {
                        let matched =
                            file_filter::matching_file_rows(file, Some(f), &selection).await?;
                        RowSelection::All.refine(&matched)
                    }
                    None => selection,
                };
                let codes = file_reads::read_column_codes(file, column.name(), &selection).await?;
                let mask = keep_mask(&codes, &keep.test());
                log::debug!(
                    "[keep] {:?} tested in memory over {} file rows at {:?}",
                    column,
                    codes.len(),
                    debug::elapsed(t)
                );
                (None, selection.refine(&mask))
            }
        };
        if selection.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        Ok(self.derived(
            self.quads
                .file_with(filter, ViewSelection::Exact(selection), None),
            self.tail.clone(),
        ))
    }
}

/// The positions of `codes` the keep admits, as a mask over them.
fn keep_mask(codes: &[u32], test: &KeepTest<'_>) -> vortex_mask::Mask {
    vortex_mask::Mask::from_indices(
        codes.len(),
        (0..codes.len()).filter(|&i| test.admits(codes[i])),
    )
}

/// The keep as a filter conjunct over the quad scan's root: a range as
/// `col >= lo AND col < hi`, a set as an `OR` of equalities up to
/// [`KEEP_SET_OR_MAX`] codes or one `list_contains` up to
/// [`KEEP_SET_FILTER_MAX`]; `None` beyond that.
#[cfg(feature = "file-io")]
fn keep_conjunct(column: QuadColumn, keep: &Keep) -> Option<vortex_array::expr::Expression> {
    use std::sync::Arc;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::expr::{and, eq, get_item, gt_eq, list_contains, lit, lt, or_collect, root};
    use vortex_array::scalar::Scalar;

    let col = || get_item(column.name(), root());
    match keep {
        Keep::Range(lo, hi) => Some(and(gt_eq(col(), lit(*lo)), lt(col(), lit(*hi)))),
        Keep::Set(codes) if codes.len() <= KEEP_SET_OR_MAX => {
            or_collect(codes.iter().map(|&code| eq(col(), lit(code))))
        }
        Keep::Set(codes) if codes.len() <= KEEP_SET_FILTER_MAX => {
            let list = Scalar::list(
                Arc::new(DType::Primitive(PType::U32, Nullability::NonNullable)),
                codes.iter().map(|&code| Scalar::from(code)).collect(),
                Nullability::NonNullable,
            );
            Some(list_contains(lit(list), col()))
        }
        Keep::Set(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_set_sorts_and_dedups() {
        let keep = Keep::set([5, 1, 3, 3, 1]);
        assert_eq!(keep, Keep::Set(Buffer::from(vec![1u32, 3, 5])));
        assert!(keep.admits(3) && !keep.admits(4));
        assert!(Keep::set([]).is_empty());
        assert!(Keep::range(4..4).is_empty());
        let (lo, hi) = (9, 4);
        assert!(Keep::range(lo..hi).is_empty());
        assert!(Keep::range(2..5).admits(4) && !Keep::range(2..5).admits(5));
    }

    #[test]
    fn keep_test_matches_admits() {
        let dense = Keep::set((100..140).step_by(3));
        let sparse = Keep::set([1, 1_000, 1_000_000]);
        let range = Keep::range(10..20);
        for keep in [&dense, &sparse, &range] {
            let test = keep.test();
            for code in (0..1_100).chain([999_999, 1_000_000, 1_000_001]) {
                assert_eq!(test.admits(code), keep.admits(code), "{keep:?} at {code}");
            }
        }
        assert!(matches!(dense.test(), KeepTest::Bitmap { .. }));
        assert!(matches!(sparse.test(), KeepTest::Sorted(_)));
    }
}
