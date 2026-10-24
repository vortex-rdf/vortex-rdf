//! Narrowing a view beyond a pattern: [`Keep`] (the rows whose code in one
//! column is in a set or range) and windows, the restrictions a query engine
//! pushes below a pattern (`VALUES`, a `FILTER` on a term predicate,
//! `LIMIT`/`OFFSET`), and the empty view that a keep or a match selecting
//! nothing returns. The capped counts behind `ASK` live in `read/count.rs`.
//!
//! Every narrowing is a derived view that composes with
//! [`match_pattern`](VortexRdfStore::match_pattern) in either order and reads
//! through the same paths (`size`, `code_columns_gathered`, `quads`). Keeps
//! apply after the pattern and never as scan expressions: in memory as a
//! binary search inside a sorted run or a pass over the column's selected
//! rows, on file as the located runs of a sorted subject column or else a
//! pass over the column streamed for the view's rows. A window that cannot
//! cut a row is the view as it is; any other over a file view with a pending
//! filter evaluates its splits in file order and stops at the first split
//! that fills it, and one that finds it cuts no row keeps the view, the
//! filter's evaluation with it.

use std::ops::Range;
#[cfg(feature = "file-io")]
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;
use vortex_rdf_encoded_search::SortedProbe;

use crate::debug;
use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::array::{column_is_sorted, into_struct_array};
use crate::store::probes::StructProbes;
use crate::store::resident::cached_code_primitive;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_filter, file_reads};
use crate::store::schema::{self, QuadColumn, RowId, TermCode};
use crate::store::view::selection::{RowSelection, ViewSelection, point_sized};
use crate::store::{Backend, MemoryBase};

use crate::store::VortexRdfStore;

/// Which term codes a [`keep`](VortexRdfStore::keep) admits in a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Any of these codes; ascending and unique, as [`Keep::set`] builds
    /// them.
    Set(Buffer<TermCode>),
    /// Any code in the half-open range `lo..hi`. Codes rank the dictionary's
    /// spellings in byte order, so a term kind or an IRI namespace is one
    /// such range (see `DictReader::prefix_range`).
    Range(TermCode, TermCode),
}

impl Keep {
    /// The keep admitting exactly `codes` (any order, repeats folded).
    pub fn set(codes: impl IntoIterator<Item = TermCode>) -> Self {
        let mut codes: Vec<TermCode> = codes.into_iter().collect();
        codes.sort_unstable();
        codes.dedup();
        Keep::Set(Buffer::from(codes))
    }

    /// The keep admitting every code in `range`.
    pub fn range(range: Range<TermCode>) -> Self {
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
    pub fn admits(&self, code: TermCode) -> bool {
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
                // A bitmap, a bit per code of the span, while the span is
                // within 8 bits per member. A span wider than a `usize` (on a
                // 32-bit target) takes the sorted test, not a wrapped bitmap.
                let span = usize::try_from(hi - lo)
                    .ok()
                    .filter(|&span| span < codes.len().saturating_mul(8));
                match span {
                    Some(span) => {
                        let mut bits = vec![0u64; (span + 1).div_ceil(64)];
                        for &code in codes {
                            // At most `span`, so it fits a `usize`.
                            let bit = (code - lo) as usize;
                            bits[bit / 64] |= 1u64 << (bit % 64);
                        }
                        KeepTest::Bitmap { lo, hi, bits }
                    }
                    None => KeepTest::Sorted(codes),
                }
            }
        }
    }
}

/// A [`Keep`] as a per-row test: a range compare, a bitmap over a dense set's
/// span, or a binary search of a sparse one.
enum KeepTest<'a> {
    Range(TermCode, TermCode),
    Bitmap {
        lo: TermCode,
        hi: TermCode,
        bits: Vec<u64>,
    },
    Sorted(&'a [TermCode]),
}

impl KeepTest<'_> {
    #[inline]
    fn admits(&self, code: TermCode) -> bool {
        match self {
            KeepTest::Range(lo, hi) => (*lo..*hi).contains(&code),
            KeepTest::Bitmap { lo, hi, bits } => {
                if code < *lo || code > *hi {
                    return false;
                }
                // `lo <= code <= hi`, a span the bitmap was sized for.
                let bit = (code - lo) as usize;
                bits[bit / 64] & (1u64 << (bit % 64)) != 0
            }
            KeepTest::Sorted(codes) => codes.binary_search(&code).is_ok(),
        }
    }
}

impl VortexRdfStore {
    // ── narrowing beyond a pattern ────────────────────────────────────────────

    /// Narrow this view to the rows whose code in `column` the keep admits
    /// (the rows a `VALUES` block or a term predicate selects; see
    /// `DictReader::filter_codes`), inside the store rather than over
    /// gathered columns. Composes with [`match_pattern`] in either order and
    /// reads through the same paths; an empty keep is the empty view.
    ///
    /// Requires the Dictionary layout with an empty append tail (codes are
    /// the store's vocabulary only then, as for
    /// [`dict_reader`](Self::dict_reader)); anything else is an
    /// `InvalidOperation` error.
    ///
    /// In memory the keep is a binary search when the selected rows are a
    /// run of the sorted base that `column` orders (a bound prefix of the
    /// `(s, p, o, g)` order, as a subject-bound match leaves, or every row of
    /// a base sorted by subject for a keep on `s`), else one pass over the
    /// column's selected rows: read in place from a canonical column, else a
    /// piece of a chunk at a time, decoded and compared where the selected
    /// rows are dense and read row by row through the column's probe where
    /// they are sparse or point-sized. On file, a keep on
    /// the subject column of a store sorted by it is located: the rows it
    /// admits are runs of that column, found in the mapped leaf metadata and
    /// intersected with the view's rows, so nothing is read and a pending
    /// filter stays pending (a set of more than one code per sixteen rows of
    /// the view streams instead). Any other keep resolves the view's rows,
    /// streams the column for them and tests the keep in memory, never as a
    /// scan expression. A pending view's deferred row ids (a served match's,
    /// or a run held for a count) materialize first.
    ///
    /// [`match_pattern`]: Self::match_pattern
    pub async fn keep(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        self.ensure_code_view("keep")?;
        if keep.is_empty() {
            return Ok(self.empty_view());
        }
        match &self.quads.backend {
            Backend::InMemory(memory) => self.keep_in_memory(memory, column, keep),
            #[cfg(feature = "file-io")]
            Backend::File(base) => self.keep_file(base, column, keep).await,
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

    /// The view's rows after skipping `offset` and taking at most `limit`.
    /// Available on every layout.
    ///
    /// A window that cannot cut a row is the view as it is, its serve plan,
    /// pending filter and row order included: one with no `offset` whose
    /// `limit` is at least the view's row count, or a bound on it, known
    /// without reading (the `rows.upper_bound` of
    /// [`view_statistics`](Self::view_statistics)). With no `offset`, a
    /// served file view whose run's width is unknown reads the run's ids
    /// first, kept on the view for the window below, and is the view as it
    /// is when they fit. A file view whose reads take a pending filter of
    /// code equalities by its masks first is never the view as it is: its
    /// window evaluates the filter whatever the bound, as below, and so finds
    /// whether it cuts.
    ///
    /// Any other window is a position range over base order: base rows in
    /// base row order, then the tail's. Its serve plan is dropped, so a
    /// served view's rows come back in base order, not the index's. A
    /// pending file filter is resolved only as far as the window reaches
    /// (its splits evaluated in file order, stopping at the first that fills
    /// the window), so `LIMIT` over a filtered scan costs the matching prefix
    /// of the file. A window with no `offset` whose filter matches fewer rows
    /// than its limit finds that it cuts no base row: the view keeps its rows,
    /// known exactly from then on, its reads and counts taking them from the
    /// filter's evaluation, which is never run again, and its tail is
    /// windowed to the rest of the limit.
    pub async fn window(&self, offset: usize, limit: usize) -> Result<Self> {
        let t = debug::timer();
        let WindowCheck::MayCut {
            base_live,
            masks_first,
        } = self.window_check(offset, limit)?
        else {
            debug::step!(t, "[window] limit {limit} cuts no row: the view as it is");
            return Ok(self.clone());
        };
        self.window_cut(offset, limit, base_live, masks_first).await
    }

    /// [`window`](Self::window) past its check ([`window_check`](Self::window_check)),
    /// which found the window may cut a row; `base_live` is the base's live
    /// row count when the check counted it, and `masks_first` whether it
    /// found the view's reads take a pending filter by the masks first.
    #[cfg_attr(not(feature = "file-io"), allow(unused_variables))]
    pub(in crate::store) async fn window_cut(
        &self,
        offset: usize,
        limit: usize,
        base_live: Option<usize>,
        masks_first: bool,
    ) -> Result<Self> {
        let t = debug::timer();
        // A served file run whose width is unknown until its ids are read
        // reads them into the view's shared cell, where any window below
        // takes them from, and is the view as it is when they fit.
        #[cfg(feature = "file-io")]
        let base_live = if offset == 0
            && let Some(FileBase { serve: Some(_), .. }) = self.quads.file()
            && let ViewSelection::Pending(lazy) = &self.quads.selection
            && lazy.len_if_known().is_none()
        {
            lazy.materialized_async().await?;
            let WindowCheck::MayCut { base_live, .. } = self.window_check(offset, limit)? else {
                debug::step!(t, "[window] limit {limit} cuts no row of the served run");
                return Ok(self.clone());
            };
            base_live
        } else {
            base_live
        };
        #[cfg(test)]
        narrow_hooks::note_window();
        #[cfg(feature = "file-io")]
        if offset == 0
            && let Some(windowed) = self.filter_window(limit, masks_first).await?
        {
            debug::step!(t, "[window] limit {limit} over the filter's masks");
            return Ok(windowed);
        }
        // A located run held pending for this window reads only the window's
        // own rows, and its width is the base's live size; any other view
        // windows its materialized selection.
        let (windowed, base_live) = match self.quads.located_window(offset, limit).await? {
            Some(located) => located,
            None => {
                let selection = self.quads.materialized_selection().await?;
                self.base_window(&selection, offset, limit, base_live)
                    .await?
            }
        };
        let base_taken = windowed.len(self.quads.base_len());
        // The window is a position range over base order with the filter it
        // resolved applied, so the view carries exact ids, no filter and no
        // plan.
        let quads = self
            .quads
            .with_selection(ViewSelection::Exact(windowed))
            .without_filter();
        // Whatever the base could not fill falls to the tail: the offset it
        // did not consume, the limit it did not take.
        let tail = self.tail.as_ref().map(|tail| {
            tail.window(
                offset.saturating_sub(base_live),
                limit.saturating_sub(base_taken),
            )
        });
        debug::step!(
            t,
            "[window] offset {offset} limit {limit}: {base_taken} base rows"
        );
        Ok(self.derived(quads, tail))
    }

    /// Whether a window of `limit` rows after `offset` can cut a row of this
    /// view, known without reading one. It cannot with no offset and a limit
    /// at least the `rows.upper_bound` of
    /// [`view_statistics`](Self::view_statistics), the base's rows plus the
    /// tail's live rows, both from [`QuadsSource::base_rows`](crate::store::QuadsSource::base_rows):
    /// the selected rows (a pending run's width, or every base row while that
    /// is unknown), tested first, which decodes nothing, then, only under
    /// tombstones and when those do not fit, their live rows where known
    /// without I/O, which in memory decodes a pending run's ids, counted once
    /// for the window to take. A view whose window kept its filter's masks
    /// knows the base's live rows exactly from them. A view pending without
    /// a serve plan always may cut: its window takes its rows into exact
    /// ids, the rows and order of the plain match, and where
    /// [`QuadsSource::located_window`](crate::store::QuadsSource::located_window)
    /// answers (no tombstones, an in-memory run whose rids ascend, a file run
    /// with no filter) it reads them without the run's other ids. So does a
    /// file view whose reads take its pending filter by the masks first
    /// ([`reads_masks_first`](Self::reads_masks_first)), whatever its bound:
    /// its window evaluates the filter, which its reads would evaluate
    /// anyway, so its one-shot reads take the rows from that evaluation, its
    /// streams read the matches' ids, and its statistics are exact. That test
    /// runs after the bound's, which costs less, but under tombstones before
    /// it, as the bound may count live rows such a window never takes.
    pub(in crate::store) fn window_check(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<WindowCheck> {
        if offset != 0 || self.quads.is_pending_without_plan() {
            return Ok(WindowCheck::MayCut {
                base_live: None,
                masks_first: false,
            });
        }
        #[cfg(feature = "file-io")]
        let tombstoned = self.quads.deleted.is_some();
        #[cfg(feature = "file-io")]
        if tombstoned && self.reads_masks_first()? {
            return Ok(WindowCheck::MayCut {
                base_live: None,
                masks_first: true,
            });
        }
        let tail = self.tail_size();
        let fits = |base: usize| base.saturating_add(tail) <= limit;
        let base = self.quads.base_rows(|selected| !fits(selected))?;
        if !fits(base.upper_bound()) {
            return Ok(WindowCheck::MayCut {
                base_live: base.live,
                masks_first: false,
            });
        }
        #[cfg(feature = "file-io")]
        if !tombstoned && self.reads_masks_first()? {
            return Ok(WindowCheck::MayCut {
                base_live: base.live,
                masks_first: true,
            });
        }
        Ok(WindowCheck::CannotCut)
    }

    /// Whether this view's one-shot reads take its pending filter by the
    /// filter's masks first ([`ExactRead::Masks`](file_reads::ExactRead::Masks)):
    /// a file view with no serve plan and no masks a window kept, an exact
    /// selection, and a filter of code equalities over columns with chunk
    /// probes, at a selection the reads by id decline. The choice those
    /// reads make, made here without reading.
    #[cfg(feature = "file-io")]
    pub(in crate::store) fn reads_masks_first(&self) -> Result<bool> {
        let (
            Some(FileBase {
                file,
                filter: Some(filter),
                serve: None,
                masks: None,
            }),
            ViewSelection::Exact(selection),
        ) = (self.quads.file(), &self.quads.selection)
        else {
            return Ok(false);
        };
        let primary = self.layout.strategy().primary_column_names();
        file_reads::ExactRead::reads_masks(file, primary, Some(filter), selection)
    }

    /// `window(0, limit)` of a file view with a pending filter, an exact
    /// selection and no serve plan: the filter's masks evaluated in file
    /// order up to the split that brings the matches to `limit`
    /// ([`file_filter::split_masks_until`]). `None` for any other view.
    ///
    /// With `limit` matches the window is their ids, in base order, with no
    /// filter and the tail cut off. With fewer it cuts no base row: the view
    /// keeps its filter with the masks kept for its reads where the masks are
    /// smaller than the matches' ids ([`masks_smaller_than_ids`]) and the
    /// filter is one the reads by id check ([`file_reads::point_readable`],
    /// known with `masks_first`: the window's check found the view's reads
    /// take the filter by its masks first), else holds those ids with no
    /// filter, and its tail is windowed to the rest of the limit.
    #[cfg(feature = "file-io")]
    async fn filter_window(&self, limit: usize, masks_first: bool) -> Result<Option<Self>> {
        let (Some(base), ViewSelection::Exact(selection), Some((file, filter))) = (
            self.quads.file(),
            &self.quads.selection,
            self.quads.pending_filter(),
        ) else {
            return Ok(None);
        };
        if base.serve.is_some() {
            return Ok(None);
        }
        let deleted = self.quads.deleted.as_ref();
        let masks = file_filter::split_masks_until(file, filter, selection, deleted, limit).await?;
        let found: usize = masks.iter().map(|(mask, _)| mask.true_count()).sum();
        let primary = self.layout.strategy().primary_column_names();
        let keep_masks = found < limit
            && masks_smaller_than_ids(&masks, found)
            && (masks_first || file_reads::point_readable(file, primary, Some(filter)));
        let (quads, tail_limit) = if keep_masks {
            (
                self.quads.with_evaluated_masks(Arc::new(masks)),
                limit - found,
            )
        } else {
            let taken = found.min(limit);
            let ids = Buffer::from(file_filter::first_ids(&masks, taken));
            let selection = ViewSelection::Exact(RowSelection::from_ids(ids));
            (
                self.quads.file_with(base, None, selection, None),
                limit - taken,
            )
        };
        let tail = self.tail.as_ref().map(|tail| tail.window(0, tail_limit));
        Ok(Some(self.derived(quads, tail)))
    }

    /// The base's window over its exact `selection`, and the base's live row
    /// count as far as the window needs it: a position range over the
    /// selection, stepping over tombstones, with the selection's live row
    /// count (`live`, when the window's check counted it); or, under a
    /// pending file filter, the window of the first matches, evaluated only
    /// as far as the window reaches, with their count (at most
    /// `offset + limit`).
    async fn base_window(
        &self,
        selection: &RowSelection,
        offset: usize,
        limit: usize,
        live: Option<usize>,
    ) -> Result<(RowSelection, usize)> {
        let base_len = self.quads.base_len();
        let deleted = self.quads.deleted.as_ref();
        #[cfg(feature = "file-io")]
        if let Some((file, filter)) = self.quads.pending_filter() {
            // Only the matches the window can reach are evaluated; fewer than
            // asked means the base is exhausted and their count is its live
            // size.
            let want = offset.saturating_add(limit);
            let found =
                file_filter::first_matching_rows(file, filter, selection, deleted, want).await?;
            let live = if found.len() < want {
                found.len()
            } else {
                want
            };
            return Ok((
                RowSelection::Ids(found).window(offset, limit, None, base_len),
                live,
            ));
        }
        let live = live.unwrap_or_else(|| selection.live_len(deleted, base_len));
        Ok((selection.window(offset, limit, deleted, base_len), live))
    }

    /// An empty view of this store: the same base and tail, selecting no row
    /// of either, so scans plan no work and `size()` answers 0 without
    /// touching the data. Indexes and components are dropped, so chained
    /// matches run no lookups, and no serve plan carries over.
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

    /// The in-memory backend of [`keep`](Self::keep), `memory` the view's
    /// base.
    fn keep_in_memory(&self, memory: &MemoryBase, column: QuadColumn, keep: &Keep) -> Result<Self> {
        let t = debug::timer();
        let MemoryBase { base, probes, .. } = memory;
        // A pending view's deferred ids are needed now: the keep narrows exact
        // row ids, and a serve plan's run no longer describes its result.
        let selection = self.quads.selection.materialized()?;
        let base_len = base.len();
        if selection.is_empty(base_len) {
            return Ok(self.empty_view());
        }
        let struct_arr = into_struct_array(base.clone())?;
        let narrowed =
            match Self::keep_sorted_run(&struct_arr, base, probes, &selection, column, keep) {
                Some(narrowed) => {
                    #[cfg(test)]
                    narrow_hooks::note_sorted_run_keep();
                    debug::step!(
                        t,
                        "[keep] {:?} narrowed by binary search inside a sorted run",
                        column
                    );
                    narrowed
                }
                None => {
                    let narrowed =
                        Self::keep_by_pass(&struct_arr, base, probes, &selection, column, keep)?;
                    debug::step!(
                        t,
                        "[keep] {:?} narrowed by a pass over {} selected rows",
                        column,
                        selection.len(base_len)
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
    /// order is constant over the run), every row of the base counting as
    /// one run. A range keep is two lower bounds; a small set is one bounded
    /// sub-run per code. `None` declines to the row pass.
    fn keep_sorted_run(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Option<RowSelection> {
        let run = match selection {
            RowSelection::All => 0..base.len(),
            RowSelection::Range(range) => range.start as usize..range.end as usize,
            RowSelection::Ids(_) => return None,
        };
        let s_col = struct_arr.unmasked_field_by_name(schema::COL_S).ok()?;
        if !column_is_sorted(s_col) {
            return None;
        }
        if run.is_empty() {
            return None;
        }
        for earlier in &QuadColumn::ALL[..column.index()] {
            let probe = probes.by_name(base, earlier.name()).flatten()?.probe();
            if probe.value_at(run.start) != probe.value_at(run.end - 1) {
                return None;
            }
        }
        let probe = probes.by_name(base, column.name()).flatten()?.probe();
        let width = run.len();
        match keep {
            Keep::Range(lo, hi) => {
                let (start, _) = probe.bounds_in(run.clone(), *lo);
                let (end, _) = probe.bounds_in(start..run.end, *hi);
                Some(RowSelection::Range(start as u64..end as u64))
            }
            Keep::Set(codes) => {
                // One bounded search per code while that beats a pass, in
                // probe reads: a point-sized run's pass reads each of its
                // rows through the probe, a wider run's decodes them, a
                // probe read costing `KEEP_DECODE_ROWS_PER_ID` decoded rows.
                let searches = codes.len().saturating_mul(width.ilog2() as usize + 1);
                let pass = if point_sized(width as u64) {
                    width
                } else {
                    width / KEEP_DECODE_ROWS_PER_ID
                };
                if searches >= pass {
                    return None;
                }
                let mut ids: Vec<u64> = Vec::new();
                let mut from = run.start;
                for &code in codes.as_slice() {
                    let (lo, hi) = probe.bounds_in(from..run.end, code);
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

    /// The keep by one pass over the selected rows of `column`: read in place
    /// from its canonical primitive when one is materialized, else through
    /// its cached probe a piece at a time ([`keep_by_pieces`]); a column
    /// neither serves is decoded for the selected rows only.
    fn keep_by_pass(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<RowSelection> {
        use vortex_array::VortexSessionExecute as _;
        use vortex_array::arrays::PrimitiveArray;

        let col = struct_arr.unmasked_field_by_name(column.name())?;
        let test = keep.test();
        let base_len = base.len();
        if let Some(prim) = cached_code_primitive(col) {
            let codes = prim.as_slice::<TermCode>();
            let kept = selection.filter_rows(base_len, |row| test.admits(codes[row]));
            return Ok(survivors(selection, base_len, kept));
        }
        if let Some(probe) = probes.by_name(base, column.name()).flatten() {
            return keep_by_pieces(
                col,
                probe.probe(),
                selection,
                &test,
                base_len,
                KEEP_PIECE_ROWS,
            );
        }
        // The selected rows only, decoded once.
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let prim = selection.apply(col)?.execute::<PrimitiveArray>(&mut ctx)?;
        Ok(selection
            .clone()
            .refine(&keep_mask(prim.as_slice::<TermCode>(), &test)))
    }

    /// The file backend of [`keep`](Self::keep). On a file sorted by subject a
    /// keep on `s` is first located (see [`keep_located`](Self::keep_located)):
    /// the rows it admits are runs of the sorted column, found in the mapped
    /// leaf metadata, so the selection narrows to them and a pending filter
    /// stays pending. Every other keep, and any the location declines, is
    /// tested in memory: the view's rows resolve to exact ids (a pending
    /// filter evaluated, deferred row ids materialized), the column is
    /// streamed for them and each code is tested. A keep never becomes a
    /// filter expression: its code list is per query, and every distinct
    /// expression would pin new entries in vortex's identity-keyed reader
    /// caches.
    #[cfg(feature = "file-io")]
    async fn keep_file(&self, base: &FileBase, column: QuadColumn, keep: &Keep) -> Result<Self> {
        let t = debug::timer();
        let FileBase { file, filter, .. } = base;
        let row_count = file.row_count() as usize;
        let selection = self.quads.selection.materialized_async().await?;
        if selection.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        let located = match column {
            QuadColumn::S => Self::keep_located(file, &selection, keep, row_count).await?,
            _ => None,
        };
        let (filter, narrowed) = match located {
            Some(narrowed) => {
                debug::step!(
                    t,
                    "[keep] {:?} located as runs of the sorted column",
                    column
                );
                (filter.clone(), narrowed)
            }
            None => {
                let selection = match filter {
                    Some(f) => RowSelection::All
                        .refine(&file_filter::matching_file_rows(file, Some(f), &selection).await?),
                    None => selection,
                };
                let test = keep.test();
                let positions =
                    file_reads::column_positions(file, column.name(), &selection, |code| {
                        test.admits(code)
                    })
                    .await?;
                let selected = selection.len(row_count);
                let narrowed = selection.refine_positions(positions, selected);
                debug::step!(
                    t,
                    "[keep] {:?} tested in memory over {} file rows",
                    column,
                    selected
                );
                (None, narrowed)
            }
        };
        if narrowed.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        Ok(self.derived(
            self.quads
                .file_with(base, filter, ViewSelection::Exact(narrowed), None),
            self.tail.clone(),
        ))
    }

    /// A keep on the subject column by located runs: in a file sorted by `s`
    /// the rows with `lo <= s < hi` are one run, and each of a set's codes one
    /// run, found by binary search over the mapped column's leaf metadata, so
    /// no column is read, no expression bound, and a pending filter stays
    /// pending beside the narrowed selection. The runs intersect `selection`,
    /// which stays a range wherever the admitted rows are one run of it.
    ///
    /// `None` declines to the stream: the file is not sorted by `s`, a chunk
    /// has no probe, or the set is too large to look up code by code (see
    /// [`set_is_located`]).
    #[cfg(feature = "file-io")]
    async fn keep_located(
        file: &crate::store::persist::native_file::NativeStoreFile,
        selection: &RowSelection,
        keep: &Keep,
        row_count: usize,
    ) -> Result<Option<RowSelection>> {
        Ok(match keep {
            Keep::Range(lo, hi) => file_reads::locate_subject_code_range(file, *lo..*hi)
                .await?
                .map(|run| selection.clone().intersect_range(run)),
            Keep::Set(codes) if set_is_located(codes.len(), selection.len(row_count)) => {
                file_reads::locate_subject_code_runs(file, codes.as_slice())
                    .await?
                    .map(|runs| selection.clone().intersect_runs(&runs))
            }
            Keep::Set(_) => None,
        })
    }
}

/// Whether split masks that keep `found` rows take less memory than those
/// rows' ids: a mask keeping some but not all of its range's rows holds a bit
/// a row of that range, a mask keeping all or none of them holds nothing,
/// and an id is 64 bits.
#[cfg(feature = "file-io")]
fn masks_smaller_than_ids(masks: &[(vortex_mask::Mask, Range<u64>)], found: usize) -> bool {
    let bits: u64 = masks
        .iter()
        .filter(|(mask, _)| mask.values().is_some())
        .map(|(_, range)| range.end - range.start)
        .sum();
    (found as u64).saturating_mul(64) >= bits
}

/// The positions of `codes` the keep admits, as a mask over them.
fn keep_mask(codes: &[TermCode], test: &KeepTest<'_>) -> vortex_mask::Mask {
    vortex_mask::Mask::from_indices(
        codes.len(),
        (0..codes.len()).filter(|&i| test.admits(codes[i])),
    )
}

/// The selection a pass leaves when `kept`, ascending, are the rows of
/// `selection` (out of `base_len`) the keep admits: `selection` itself when
/// it admits every one, else the kept ids (the canonical empty selection for
/// none).
fn survivors(selection: &RowSelection, base_len: usize, kept: Vec<RowId>) -> RowSelection {
    if !kept.is_empty() && kept.len() == selection.len(base_len) {
        return selection.clone();
    }
    RowSelection::from_ids(Buffer::from(kept))
}

/// Rows of one chunk of a column the in-memory pass decodes at once
/// ([`keep_by_pieces`]): a longer chunk is decoded a piece at a time, so the
/// codes decoded at once stay bounded whatever the chunk's length.
const KEEP_PIECE_ROWS: usize = 1 << 16;

/// Selected rows a piece holds at most for the pass to read them one by one
/// through the column's probe whatever their density: so few probe reads
/// cost less than a decode's fixed part (a slice, an execution, a buffer).
const KEEP_POINT_IDS_PER_PIECE: usize = 16;

/// Rows a decode reads per selected row, at most, for a piece to be decoded
/// and compared rather than read row by row through the column's probe: one
/// probe read costs as much as decoding many rows, so ids sparser than this
/// are cheaper read in place.
const KEEP_DECODE_ROWS_PER_ID: usize = 16;

/// Whether a piece with `selected` selected rows among the `span` rows from
/// its first selected row to its last is decoded and compared rather than
/// read row by row through the probe: more than
/// [`KEEP_POINT_IDS_PER_PIECE`] selected rows, at least one per
/// [`KEEP_DECODE_ROWS_PER_ID`] rows of the span.
fn decodes_piece(selected: usize, span: usize) -> bool {
    selected > KEEP_POINT_IDS_PER_PIECE && selected.saturating_mul(KEEP_DECODE_ROWS_PER_ID) >= span
}

/// The rows of `selection` (out of `base_len`) whose code in `column` the
/// keep's `test` admits, as the selection they leave ([`survivors`]): the
/// pass over a column with a probe and no canonical primitive. A point-sized
/// selection is read row by row through `probe`. Any other is taken a piece
/// at a time, a piece being at most `piece_rows` rows of one of the column's
/// chunks ([`code_chunks`]): a piece whose selected rows are dense
/// ([`decodes_piece`]) has the rows from its first selected row to its last
/// decoded at once and compared, any other reads its selected rows one by
/// one through `probe`. A piece's decoded codes are dropped before the next
/// piece is read.
fn keep_by_pieces(
    column: &ArrayRef,
    probe: &SortedProbe<'_>,
    selection: &RowSelection,
    test: &KeepTest<'_>,
    base_len: usize,
    piece_rows: usize,
) -> Result<RowSelection> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    if selection.is_point_sized() {
        #[cfg(test)]
        narrow_hooks::note_probed_rows(selection.len(base_len));
        let kept = selection.filter_rows(base_len, |row| test.admits(probe.value_at(row)));
        return Ok(survivors(selection, base_len, kept));
    }
    // The selected rows: every row of one stretch, or an ascending id list.
    let (stretch, listed) = match selection {
        RowSelection::All => (0..base_len, None),
        RowSelection::Range(range) => {
            let end = usize::try_from(range.end).map_or(base_len, |end| end.min(base_len));
            let start = usize::try_from(range.start).map_or(end, |start| start.min(end));
            (start..end, None)
        }
        RowSelection::Ids(ids) => (0..base_len, Some(ids.as_slice())),
    };
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let mut kept: Vec<RowId> = Vec::new();
    // The first id of `listed` no piece has taken yet.
    let mut next = 0;
    'chunks: for (chunk_start, chunk) in code_chunks(column) {
        let chunk_end = chunk_start + chunk.len();
        for start in (chunk_start..chunk_end).step_by(piece_rows) {
            if start >= stretch.end || listed.is_some_and(|ids| next == ids.len()) {
                break 'chunks;
            }
            let end = chunk_end.min(start.saturating_add(piece_rows));
            let rows = match listed {
                None => PieceRows::Stretch(stretch.start.max(start)..stretch.end.min(end)),
                Some(ids) => {
                    let taken = ids[next..].partition_point(|&id| id < end as RowId);
                    next += taken;
                    PieceRows::Ids(&ids[next - taken..next])
                }
            };
            let span = rows.span();
            if span.is_empty() {
                continue;
            }
            if decodes_piece(rows.selected(), span.len()) {
                #[cfg(test)]
                narrow_hooks::note_decoded_piece();
                let codes = chunk
                    .slice(span.start - chunk_start..span.end - chunk_start)?
                    .execute::<PrimitiveArray>(&mut ctx)?;
                rows.admit_decoded(&mut kept, test, codes.as_slice::<TermCode>(), span.start);
            } else {
                #[cfg(test)]
                narrow_hooks::note_probed_rows(rows.selected());
                rows.admit_probed(&mut kept, test, probe);
            }
        }
    }
    Ok(survivors(selection, base_len, kept))
}

/// One piece's selected rows: every row of a stretch, or a run of the
/// selection's ascending id list.
enum PieceRows<'a> {
    Stretch(Range<usize>),
    Ids(&'a [RowId]),
}

impl PieceRows<'_> {
    /// How many rows are selected.
    fn selected(&self) -> usize {
        match self {
            PieceRows::Stretch(rows) => rows.len(),
            PieceRows::Ids(ids) => ids.len(),
        }
    }

    /// The rows from the first selected to the last, which a decode reads;
    /// empty when none is selected.
    fn span(&self) -> Range<usize> {
        match self {
            PieceRows::Stretch(rows) => rows.clone(),
            PieceRows::Ids(ids) => match (ids.first(), ids.last()) {
                (Some(&first), Some(&last)) => first as usize..last as usize + 1,
                _ => 0..0,
            },
        }
    }

    /// Push onto `kept` the selected rows whose code the keep's `test`
    /// admits, reading it from `codes`, the decoded codes of the rows from
    /// `first` on.
    fn admit_decoded(
        &self,
        kept: &mut Vec<RowId>,
        test: &KeepTest<'_>,
        codes: &[TermCode],
        first: usize,
    ) {
        match self {
            PieceRows::Stretch(_) => kept.extend(
                codes
                    .iter()
                    .enumerate()
                    .filter(|&(_, &code)| test.admits(code))
                    .map(|(i, _)| (first + i) as RowId),
            ),
            PieceRows::Ids(ids) => kept.extend(
                ids.iter()
                    .copied()
                    .filter(|&id| test.admits(codes[id as usize - first])),
            ),
        }
    }

    /// Push onto `kept` the selected rows whose code the keep's `test`
    /// admits, reading it row by row through `probe`.
    fn admit_probed(&self, kept: &mut Vec<RowId>, test: &KeepTest<'_>, probe: &SortedProbe<'_>) {
        match self {
            PieceRows::Stretch(rows) => kept.extend(
                rows.clone()
                    .filter(|&row| test.admits(probe.value_at(row)))
                    .map(|row| row as RowId),
            ),
            PieceRows::Ids(ids) => kept.extend(
                ids.iter()
                    .copied()
                    .filter(|&id| test.admits(probe.value_at(id as usize))),
            ),
        }
    }
}

/// A code column's chunks, each beside the base row it starts at: a chunked
/// array's own chunks, any other array as one chunk. A `vortex.shared`
/// wrapper is looked through to the array it holds, so a piece decoded from
/// it never fills the wrapper's cache, which would keep the whole column
/// decoded for the base's lifetime.
fn code_chunks(column: &ArrayRef) -> Vec<(usize, ArrayRef)> {
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    use vortex_array::arrays::shared::SharedArrayExt as _;
    use vortex_array::arrays::{Chunked, Shared};

    let source = match column.as_opt::<Shared>() {
        Some(shared) => shared.current_array_ref().clone(),
        None => column.clone(),
    };
    if !source.is::<Chunked>() {
        return vec![(0, source)];
    }
    let chunked = source.as_::<Chunked>();
    let offsets = chunked.chunk_offset_values();
    (0..chunked.nchunks())
        .map(|i| (offsets[i], chunked.chunk(i).clone()))
        .collect()
}

/// The selected rows one code of a subject set keep is worth when choosing
/// between looking the set up and streaming the column: a lookup costs two
/// binary searches over the mapped chunk metadata per code, whatever the
/// selection, the stream a little per selected row plus a fixed scan, and the
/// two meet near this many rows per code.
#[cfg(feature = "file-io")]
const LOCATED_SET_ROWS_PER_CODE: usize = 16;

/// Whether a set of `codes` codes on the subject column is looked up code by
/// code over a selection of `rows` rows rather than streamed: while it has at
/// most one code per [`LOCATED_SET_ROWS_PER_CODE`] rows, rounded up, so at
/// least one code over any non-empty selection.
#[cfg(feature = "file-io")]
fn set_is_located(codes: usize, rows: usize) -> bool {
    codes <= rows.div_ceil(LOCATED_SET_ROWS_PER_CODE)
}

/// What a window's check ([`window_check`](VortexRdfStore::window_check))
/// found.
pub(in crate::store) enum WindowCheck {
    /// The window cannot cut a row: it is the view as it is.
    CannotCut,
    /// The window may cut a row. `base_live` is the base's live row count
    /// when the check counted it, which the window takes instead of
    /// counting again; `masks_first` is whether the check found the view's
    /// reads take its pending filter by the masks first, which tells the
    /// window the reads by id can check that filter.
    MayCut {
        base_live: Option<usize>,
        masks_first: bool,
    },
}

/// Test hook: how many of this thread's in-memory keeps were answered by
/// binary search inside a sorted run (`keep_sorted_run`), not by a pass
/// over the selected rows; how many pieces of a column the passes decoded,
/// and how many selected rows they read through a probe instead
/// (`keep_by_pieces`); and how many windows this thread built.
#[cfg(test)]
pub(crate) mod narrow_hooks {
    use std::cell::Cell;

    thread_local! {
        static SORTED_RUN_KEEPS: Cell<usize> = const { Cell::new(0) };
        static DECODED_PIECES: Cell<usize> = const { Cell::new(0) };
        static PROBED_ROWS: Cell<usize> = const { Cell::new(0) };
        static WINDOWS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one keep answered inside a sorted run.
    pub(super) fn note_sorted_run_keep() {
        SORTED_RUN_KEEPS.with(|c| c.set(c.get() + 1));
    }

    /// The keeps answered inside a sorted run on this thread so far.
    pub(crate) fn sorted_run_keeps() -> usize {
        SORTED_RUN_KEEPS.with(Cell::get)
    }

    /// Record one piece of a column a keep's pass decoded.
    pub(super) fn note_decoded_piece() {
        DECODED_PIECES.set(DECODED_PIECES.get() + 1);
    }

    /// The pieces this thread's keeps decoded so far.
    pub(crate) fn decoded_pieces() -> usize {
        DECODED_PIECES.get()
    }

    /// Record `rows` selected rows a keep's pass read one by one through a
    /// probe.
    pub(super) fn note_probed_rows(rows: usize) {
        PROBED_ROWS.set(PROBED_ROWS.get() + rows);
    }

    /// The selected rows this thread's keeps read through a probe so far.
    pub(crate) fn probed_rows() -> usize {
        PROBED_ROWS.get()
    }

    /// Record one window built: a call of
    /// [`window`](crate::store::VortexRdfStore::window) past the checks that
    /// find it is the view as it is.
    pub(super) fn note_window() {
        WINDOWS.set(WINDOWS.get() + 1);
    }

    /// The windows built on this thread so far.
    pub(crate) fn windows() -> usize {
        WINDOWS.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use vortex::encodings::runend::RunEnd;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::validity::Validity;
    use vortex_array::{IntoArray, VortexSessionExecute as _};

    use crate::store::array::{chunked_or_single, stamp_is_sorted};
    use crate::store::resident::with_compressed_int_children;

    #[test]
    fn keep_set_sorts_and_dedups() {
        let keep = Keep::set([5, 1, 3, 3, 1]);
        assert_eq!(keep, Keep::Set(Buffer::from(vec![1u64, 3, 5])));
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

    /// A set is looked up while it has at most one code per sixteen rows of
    /// the selection, rounded up.
    #[test]
    fn located_sets_are_bounded_by_the_selection() {
        assert!(set_is_located(1, 1));
        assert!(set_is_located(1, 16));
        assert!(!set_is_located(2, 16));
        assert!(set_is_located(2, 17));
        assert!(set_is_located(500_000, 8_000_000));
        assert!(!set_is_located(500_001, 8_000_000));
        assert!(!set_is_located(1, 0));
    }

    /// A piece is decoded past sixteen selected rows, at one or more per
    /// sixteen rows of its span.
    #[test]
    fn dense_pieces_are_decoded() {
        assert!(!decodes_piece(16, 16));
        assert!(decodes_piece(17, 17));
        assert!(decodes_piece(17, 272));
        assert!(!decodes_piece(17, 273));
        assert!(decodes_piece(4_096, KEEP_PIECE_ROWS));
        assert!(!decodes_piece(4_095, KEEP_PIECE_ROWS));
    }

    /// The code columns of a `rows`-row base: `s` ascending in runs of four,
    /// `p` cycling through thirteen codes, `o` through a thousand and `g`
    /// through eight in ten-row blocks.
    fn base_codes(rows: u64) -> [Vec<TermCode>; 4] {
        [
            (0..rows).map(|i| i / 4).collect(),
            (0..rows).map(|i| (i * 7) % 13).collect(),
            (0..rows).map(|i| (i * 31 + i / 3) % 1_000).collect(),
            (0..rows).map(|i| (i / 10) % 8).collect(),
        ]
    }

    /// `codes` as an in-memory base of `chunk_rows`-row chunks (one array
    /// when that is every row) in the encodings a build and an adoption
    /// leave: `s`, `p` and `o` compressed by the build's pass (`s`, stamped
    /// sorted, run-end; `p` and `o` bit-packed) behind its shared wrapper,
    /// and `g` run-end encoded chunk by chunk and left bare, as an adopted
    /// file's column is; its probes warmed.
    fn encoded_base(
        codes: &[Vec<TermCode>; 4],
        chunk_rows: usize,
    ) -> (StructArray, ArrayRef, Arc<StructProbes>) {
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let mut children = Vec::with_capacity(codes.len());
        for (index, values) in codes.iter().enumerate() {
            let mut chunks = Vec::new();
            for part in values.chunks(chunk_rows) {
                let chunk = PrimitiveArray::from_iter(part.iter().copied()).into_array();
                chunks.push(if index == QuadColumn::G.index() {
                    RunEnd::encode(chunk, &mut ctx).unwrap().into_array()
                } else {
                    chunk
                });
            }
            let child = chunked_or_single(chunks, dtype.clone()).unwrap();
            if index == QuadColumn::S.index() {
                stamp_is_sorted(&child);
            }
            children.push(child);
        }
        let rows = StructArray::try_new(
            ["s", "p", "o", "g"].into(),
            children,
            codes[0].len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let base = with_compressed_int_children(rows, true).unwrap();
        let probes = StructProbes::new();
        probes.warm(&base);
        (into_struct_array(base.clone()).unwrap(), base, probes)
    }

    /// The base rows `selection` covers out of `base_len`, ascending.
    fn rows_of(selection: &RowSelection, base_len: usize) -> Vec<u64> {
        selection.filter_rows(base_len, |_| true)
    }

    /// The per-row pass's answer: the selected rows whose code in `column`,
    /// read through the column's probe, `keep` admits, ascending.
    fn per_row_pass(
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Vec<u64> {
        let probe = probes
            .by_name(base, column.name())
            .flatten()
            .expect("the column has a probe")
            .probe();
        let test = keep.test();
        selection.filter_rows(base.len(), |row| test.admits(probe.value_at(row)))
    }

    /// The selections the pass is checked over, each beside its name, out of
    /// 6,000 rows in 2,000-row chunks: every row, a range across two chunk
    /// boundaries, a point-sized range, and id lists dense (seven rows in
    /// eight), sparse (one in 23), clustered (each chunk's first 300 rows
    /// amid one in 29) and point-sized (one in 40).
    fn pass_selections() -> Vec<(&'static str, RowSelection)> {
        let ids = |admit: &dyn Fn(u64) -> bool| {
            RowSelection::Ids(Buffer::from_iter((0..6_000u64).filter(|&i| admit(i))))
        };
        vec![
            ("every row", RowSelection::All),
            ("a range", RowSelection::Range(1_500..4_700)),
            ("a point-sized range", RowSelection::Range(1_990..2_100)),
            ("dense ids", ids(&|i: u64| !i.is_multiple_of(8))),
            ("sparse ids", ids(&|i: u64| i.is_multiple_of(23))),
            (
                "clustered ids",
                ids(&|i: u64| i % 2_000 < 300 || i.is_multiple_of(29)),
            ),
            ("point-sized ids", ids(&|i: u64| i.is_multiple_of(40))),
        ]
    }

    /// Over every row of a base sorted by subject (an `All` selection), a
    /// keep on `s` is a binary search, a range as one run of the column and a
    /// small set as a run per code, each equal to the per-row pass; a set
    /// with more searches than the base has rows, and a keep on `p`, which
    /// the base's subjects do not hold constant, decline to the pass.
    #[test]
    fn subject_keep_over_every_row_is_a_search() {
        let codes = base_codes(6_000);
        let (struct_arr, base, probes) = encoded_base(&codes, 2_000);
        let all = RowSelection::All;
        let search = |column: QuadColumn, keep: &Keep| {
            VortexRdfStore::keep_sorted_run(&struct_arr, &base, &probes, &all, column, keep)
        };
        let last = codes[0][5_999];
        for keep in [
            Keep::range(0..last + 1),
            Keep::range(10..900),
            Keep::range(700..701),
            Keep::range(last + 1..last + 9),
            Keep::set([0, 3, 499, 500, 1_001, last]),
        ] {
            let narrowed =
                search(QuadColumn::S, &keep).unwrap_or_else(|| panic!("{keep:?} is searched"));
            assert_eq!(
                rows_of(&narrowed, 6_000),
                per_row_pass(&base, &probes, &all, QuadColumn::S, &keep),
                "{keep:?}"
            );
        }
        assert!(matches!(
            search(QuadColumn::S, &Keep::range(10..900)),
            Some(RowSelection::Range(_))
        ));
        // A thousand codes take thirteen steps each, past what a pass costs.
        let wide = Keep::set((0..1_000).map(|i| i * 3 / 2));
        assert!(search(QuadColumn::S, &wide).is_none());
        assert!(search(QuadColumn::P, &Keep::range(0..5)).is_none());
    }

    /// The pass over columns read through their probes (bit-packed and
    /// run-end chunks behind the build's shared wrapper, bare run-end
    /// chunks, and each as one unchunked array) equals the per-row pass for
    /// every keep over every selection of [`pass_selections`], with pieces
    /// shorter than the chunks too, so a chunk is decoded in several; and no
    /// decode fills a shared wrapper's cache.
    #[test]
    fn piecewise_pass_equals_the_per_row_pass() {
        const ROWS: usize = 6_000;
        let codes = base_codes(ROWS as u64);
        let selections = pass_selections();
        for chunk_rows in [2_000, ROWS] {
            let (struct_arr, base, probes) = encoded_base(&codes, chunk_rows);
            for column in QuadColumn::ALL {
                let values = &codes[column.index()];
                let col = struct_arr.unmasked_field_by_name(column.name()).unwrap();
                let probe = probes
                    .by_name(&base, column.name())
                    .flatten()
                    .unwrap()
                    .probe();
                let top = values.iter().max().unwrap() + 1;
                for keep in [
                    Keep::range(0..top),
                    Keep::range(0..top / 2),
                    Keep::range(top / 3..top / 3 + 1),
                    Keep::set((0..top).step_by(2)),
                    Keep::set([1, top / 2, top - 1]),
                    Keep::range(top..top + 5),
                ] {
                    let test = keep.test();
                    let brute = |selection: &RowSelection| -> Vec<u64> {
                        rows_of(selection, ROWS)
                            .into_iter()
                            .filter(|&row| keep.admits(values[row as usize]))
                            .collect()
                    };
                    assert_eq!(
                        per_row_pass(&base, &probes, &RowSelection::All, column, &keep),
                        brute(&RowSelection::All),
                        "{chunk_rows}-row chunks, {column:?} {keep:?}: the per-row pass"
                    );
                    for (name, selection) in &selections {
                        let tag = format!("{chunk_rows}-row chunks, {column:?} {keep:?}, {name}");
                        let want = brute(selection);
                        let narrowed = VortexRdfStore::keep_by_pass(
                            &struct_arr,
                            &base,
                            &probes,
                            selection,
                            column,
                            &keep,
                        )
                        .unwrap();
                        assert_eq!(rows_of(&narrowed, ROWS), want, "{tag}");
                        for piece_rows in [500, 96] {
                            let narrowed =
                                keep_by_pieces(col, probe, selection, &test, ROWS, piece_rows)
                                    .unwrap();
                            assert_eq!(
                                rows_of(&narrowed, ROWS),
                                want,
                                "{tag}, {piece_rows}-row pieces"
                            );
                        }
                    }
                }
            }
            for column in ["s", "p", "o"] {
                let col = struct_arr.unmasked_field_by_name(column).unwrap();
                assert!(
                    cached_code_primitive(col).is_none(),
                    "{chunk_rows}-row chunks, {column}: the shared cache stays empty"
                );
            }
        }
    }

    /// The gate: every row and a range are decoded a piece at a time, one
    /// piece a chunk or several, with no row read through the probe; a sparse
    /// id list (one row in 23), a point-sized range, dense as it is, and a
    /// point-sized id set read every selected row through the probe and
    /// decode nothing; a list dense in each chunk's first 300 rows and sparse
    /// elsewhere decodes the first piece of each chunk and probes the rest.
    /// The column's shared cache stays empty.
    #[test]
    fn the_pass_decodes_dense_pieces_and_probes_sparse_ones() {
        use super::narrow_hooks::{decoded_pieces, probed_rows};
        const ROWS: usize = 6_000;
        let codes = base_codes(ROWS as u64);
        let (struct_arr, base, probes) = encoded_base(&codes, 2_000);
        let col = struct_arr.unmasked_field_by_name("p").unwrap();
        let probe = probes.by_name(&base, "p").flatten().unwrap().probe();
        let keep = Keep::range(0..7);
        let test = keep.test();
        let run = |selection: &RowSelection, piece_rows: usize| {
            let (pieces, probed) = (decoded_pieces(), probed_rows());
            keep_by_pieces(col, probe, selection, &test, ROWS, piece_rows).unwrap();
            (decoded_pieces() - pieces, probed_rows() - probed)
        };
        let ids = |admit: &dyn Fn(u64) -> bool| -> Vec<u64> {
            (0..ROWS as u64).filter(|&i| admit(i)).collect()
        };
        assert_eq!(
            run(&RowSelection::All, KEEP_PIECE_ROWS),
            (3, 0),
            "a chunk a piece"
        );
        assert_eq!(run(&RowSelection::All, 500), (12, 0), "four pieces a chunk");
        assert_eq!(
            run(&RowSelection::Range(1_500..4_700), KEEP_PIECE_ROWS),
            (3, 0),
            "the chunks a range touches"
        );
        let sparse = ids(&|i: u64| i.is_multiple_of(23));
        assert_eq!(
            run(
                &RowSelection::Ids(Buffer::from(sparse.clone())),
                KEEP_PIECE_ROWS
            ),
            (0, sparse.len()),
            "sparse ids"
        );
        assert_eq!(
            run(&RowSelection::Range(2_000..2_200), KEEP_PIECE_ROWS),
            (0, 200),
            "a point-sized range"
        );
        let point = ids(&|i: u64| i.is_multiple_of(40));
        assert_eq!(
            run(
                &RowSelection::Ids(Buffer::from(point.clone())),
                KEEP_PIECE_ROWS
            ),
            (0, point.len()),
            "a point-sized id set"
        );
        let clustered = ids(&|i: u64| i % 2_000 < 300 || i.is_multiple_of(29));
        let in_first_pieces = clustered.iter().filter(|&&i| i % 2_000 < 500).count();
        assert_eq!(
            run(&RowSelection::Ids(Buffer::from(clustered.clone())), 500),
            (3, clustered.len() - in_first_pieces),
            "clustered ids"
        );
        assert!(
            cached_code_primitive(col).is_none(),
            "the shared cache stays empty"
        );
    }
}
