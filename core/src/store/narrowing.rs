//! Narrowing a view beyond a pattern: keeping only the rows whose code in
//! one column falls in a set or range ([`Keep`]), windowing the rows, and
//! capped counts — the restrictions a query engine pushes below a pattern
//! (`VALUES`, `FILTER` on a term predicate, `LIMIT`/`OFFSET`, `ASK`).
//!
//! Every narrowing is an ordinary derived view: it composes with
//! [`match_pattern`](VortexRdfStore::match_pattern) in either order and is
//! read through the same paths (`size`, `code_columns_gathered`, `quads`).
//! Keeps are applied *after* the pattern, never through the pattern
//! compiler and never as scan expressions: in memory as a binary search
//! inside a sorted run or a pass over the selected rows of the column, on
//! file as the located runs of a sorted subject column or else a pass over
//! the column streamed for the view's rows. Windows and capped counts over a
//! file view with a pending filter evaluate its splits in file order and stop
//! at the first split that completes them.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;

use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::store::array::{cached_code_primitive, column_is_sorted, into_struct_array};
use crate::store::layouts::LayoutStrategy;
use crate::store::probes::StructProbes;
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::schema::{self, QuadColumn, TermCode};
use crate::store::selection::{RowSelection, ViewSelection};
use crate::store::{QuadsSource, Tail};

use super::VortexRdfStore;

/// Which term codes a [`keep`](VortexRdfStore::keep) admits in a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Any of these codes — ascending and unique, as [`Keep::set`] builds
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
                // A bitmap costs a bit per code of the span; worth it while
                // the span is within 8 bits per member (a byte each). A span
                // wider than a `usize` (on a 32-bit target) takes the sorted
                // test rather than a wrapped bitmap.
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

/// The code at `row` of a base column, read without decoding the column:
/// its canonical primitive when one is already materialized, else the
/// store's cached encoded-search probe.
enum CodeReader<'a> {
    Slice(&'a [TermCode]),
    Probe(&'a vortex_rdf_encoded_search::OwnedSortedProbe),
}

impl CodeReader<'_> {
    #[inline]
    fn code_at(&self, row: usize) -> TermCode {
        match self {
            CodeReader::Slice(slice) => slice[row],
            // A probe reads every width as a u64 — exactly a code.
            CodeReader::Probe(probe) => probe.value_at(row),
        }
    }
}

impl VortexRdfStore {
    // ── narrowing beyond a pattern ────────────────────────────────────────────

    /// Narrow this view to the rows whose code in `column` the keep admits —
    /// the rows a `VALUES` block or a term predicate (see
    /// `DictReader::filter_codes`) selects, applied inside the store instead
    /// of over gathered columns. Composes with [`match_pattern`] in either
    /// order and reads through the same paths; an empty keep is the empty
    /// view.
    ///
    /// Requires the Dictionary layout with an empty append tail (codes are
    /// the store's vocabulary only then, as for
    /// [`code_read_snapshot`](Self::code_read_snapshot)); anything else is an
    /// `InvalidOperation` error.
    ///
    /// In memory the keep is a binary search when the selected rows are a
    /// run of the sorted base that `column` orders (a bound prefix of the
    /// `(s, p, o, g)` order, as a subject-bound match leaves), else one pass
    /// over the selected rows of the column read in place. On file, a keep on
    /// the subject column of a store sorted by it is located: the rows it
    /// admits are runs of that column, found in the mapped leaf metadata and
    /// intersected with the view's rows, so nothing is read and a pending
    /// filter stays pending (a set of more than one code per sixteen rows of
    /// the view is streamed instead). Any other keep resolves the view's
    /// rows, streams the column for them through the mapping and tests the
    /// keep in memory — never pushed to the scan as an expression. A pending
    /// view's deferred row ids (a served match's, or a run held for a count)
    /// materialize first.
    ///
    /// [`match_pattern`]: Self::match_pattern
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

    /// The view's rows after skipping `offset` and taking at most `limit`, in
    /// the order the view reads them — base rows in base row order, then the
    /// tail's. A file view with a pending filter resolves it only as far as
    /// the window reaches: its splits are evaluated in file order and the
    /// evaluation stops at the first that fills the window, so `LIMIT` over a
    /// filtered scan costs the matching prefix of the file, not all of it.
    /// Available on every layout.
    ///
    /// A served view's rows come back in base order, not the index's (the
    /// plan is dropped: a window is a position range over base order, which
    /// the index's order does not share).
    pub async fn window(&self, offset: usize, limit: usize) -> Result<Self> {
        let t = debug::timer();
        let (quads, base_live, base_taken) = match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                components,
                deleted,
                probes,
                ..
            } => {
                let selection = selection.materialized()?;
                let live = match deleted {
                    None => selection.len(base.len()),
                    Some(deleted) => selection.live_mask(deleted, base.len()).true_count(),
                };
                let windowed = selection.window(offset, limit, deleted.as_ref(), base.len());
                let taken = windowed.len(base.len());
                (
                    QuadsSource::InMemory {
                        base: base.clone(),
                        selection: ViewSelection::Exact(windowed),
                        components: std::sync::Arc::clone(components),
                        deleted: deleted.clone(),
                        probes: std::sync::Arc::clone(probes),
                        serve: None,
                    },
                    live,
                    taken,
                )
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let row_count = file.row_count() as usize;
                // A located run held pending for this window: only the window's
                // own rows are read, and the run's width is its live size.
                let located = match (selection, filter, deleted) {
                    (ViewSelection::Pending(lazy), None, None) => match lazy.len_if_known() {
                        Some(live) => lazy
                            .window_async(offset, limit)
                            .await?
                            .map(|ids| (ids, live)),
                        None => None,
                    },
                    _ => None,
                };
                let (windowed, live) = if let Some((ids, live)) = located {
                    // The ids are the window itself.
                    let windowed = if ids.is_empty() {
                        RowSelection::empty()
                    } else {
                        RowSelection::Ids(ids)
                    };
                    (windowed, live)
                } else {
                    let selection = selection.materialized_async().await?;
                    match filter {
                        None => {
                            let live = match deleted {
                                None => selection.len(row_count),
                                Some(deleted) => {
                                    selection.live_mask(deleted, row_count).true_count()
                                }
                            };
                            (
                                selection.window(offset, limit, deleted.as_ref(), row_count),
                                live,
                            )
                        }
                        Some(filter) => {
                            // Only the matches the window can reach are
                            // evaluated; fewer than asked means the base is
                            // exhausted and their count is its live size.
                            let want = offset.saturating_add(limit);
                            let found = file_scan::first_matching_rows(
                                file,
                                filter,
                                &selection,
                                deleted.as_ref(),
                                want,
                            )
                            .await?;
                            let live = if found.len() < want {
                                found.len()
                            } else {
                                want
                            };
                            (
                                RowSelection::Ids(found).window(offset, limit, None, row_count),
                                live,
                            )
                        }
                    }
                };
                let taken = windowed.len(row_count);
                (
                    QuadsSource::File {
                        path: path.clone(),
                        file: file.clone(),
                        filter: None,
                        selection: ViewSelection::Exact(windowed),
                        deleted: deleted.clone(),
                        serve: None,
                    },
                    live,
                    taken,
                )
            }
        };
        // Whatever the base could not fill falls to the tail: the offset it
        // did not consume, the limit it did not take.
        let tail = self.tail.as_ref().map(|tail| {
            let tail_offset = offset.saturating_sub(base_live);
            let tail_limit = limit.saturating_sub(base_taken);
            Tail {
                rows: tail.rows.clone(),
                selection: tail.selection.window(
                    tail_offset,
                    tail_limit,
                    tail.deleted.as_ref(),
                    tail.rows.len(),
                ),
                deleted: tail.deleted.clone(),
            }
        });
        log::debug!(
            "[window] offset {offset} limit {limit}: {base_taken} base rows at {:?}",
            debug::elapsed(t)
        );
        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads,
            tail,
        })
    }

    /// `min(size, limit)`, stopping as soon as `limit` rows are known to
    /// exist: a file view with a pending filter evaluates its splits in file
    /// order and stops at the first that reaches the cap (an in-memory view
    /// knows its size without reading rows). Available on every layout.
    pub async fn size_capped(&self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let base = match &self.quads {
            QuadsSource::InMemory { .. } => self.base_size().await?,
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                serve,
                ..
            } => match filter {
                None => self.base_size().await?,
                Some(filter) => {
                    // A filter never rides with a served plan; with one the
                    // selection is already exact, so this never scans an
                    // index child to count.
                    debug_assert!(serve.is_none());
                    let selection = selection.materialized_async().await?;
                    file_scan::count_matching_rows_capped(
                        file,
                        filter,
                        &selection,
                        deleted.as_ref(),
                        limit,
                    )
                    .await?
                }
            },
        };
        if base >= limit {
            return Ok(limit);
        }
        Ok((base + self.tail_size()).min(limit))
    }

    /// Whether the view holds any quad — [`size_capped`](Self::size_capped)
    /// of one, so a filtered file view reads only up to its first match.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.size_capped(1).await? > 0)
    }

    /// Err unless this view's rows are code-addressable: the Dictionary
    /// layout with an empty append tail.
    fn ensure_code_view(&self, operation: &str) -> Result<()> {
        if self.layout.strategy() != LayoutStrategy::Dictionary {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs the Dictionary layout: this store's {:?} layout stores terms \
                 as strings, which have no codes to keep",
                self.layout.strategy()
            )));
        }
        if self.tail_len() != 0 {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs an empty append tail: the {} appended rows hold terms the \
                 dictionary has no codes for; compact() the store first",
                self.tail_len()
            )));
        }
        Ok(())
    }

    /// The in-memory backend of [`keep`](Self::keep).
    fn keep_in_memory(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        let t = debug::timer();
        // Without `file-io`, InMemory is the only variant.
        #[allow(irrefutable_let_patterns)]
        let QuadsSource::InMemory {
            base,
            selection,
            components,
            deleted,
            probes,
            ..
        } = &self.quads
        else {
            unreachable!("keep routes only InMemory sources here");
        };
        // A pending view's deferred ids are needed now: the keep narrows exact
        // row ids, and a serve plan's run no longer describes its result.
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
        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: QuadsSource::InMemory {
                base: base.clone(),
                selection: ViewSelection::Exact(narrowed),
                components: std::sync::Arc::clone(components),
                deleted: deleted.clone(),
                probes: std::sync::Arc::clone(probes),
                serve: None,
            },
            tail: self.tail.clone(),
        })
    }

    /// The keep by binary search, when the selection is a run of the sorted
    /// base that `column` orders: every column before it in the `(s, p, o,
    /// g)` order is constant over the run (so the run is sorted by this
    /// one). A range keep is two lower bounds; a small set is one bounded
    /// sub-run per code. `None` declines to the row pass.
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
                let (start, _) = probe.bounds_in(run.clone(), *lo);
                let (end, _) = probe.bounds_in(start..run.end, *hi);
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

    /// The keep by one pass over the selected rows of `column`, read in
    /// place (its canonical primitive when materialized, else the cached
    /// probe) — or, for a column neither serves, decoded for the selected
    /// rows only.
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
        let cached = cached_code_primitive(col);
        let reader = match (&cached, probes.by_name(base, column.name())) {
            (Some(prim), _) => Some(CodeReader::Slice(prim.as_slice::<TermCode>())),
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
                let codes = prim.as_slice::<TermCode>();
                let positions = (0..codes.len()).filter(|&i| test.admits(codes[i]));
                let mask = vortex_mask::Mask::from_indices(codes.len(), positions);
                return Ok(selection.clone().refine(&mask));
            }
        };
        Ok(if ids.is_empty() {
            RowSelection::empty()
        } else {
            RowSelection::Ids(Buffer::from(ids))
        })
    }

    /// The file backend of [`keep`](Self::keep). On a file sorted by subject a
    /// keep on `s` is first located (see [`keep_located`](Self::keep_located)):
    /// the rows it admits are runs of the sorted column, found in the mapped
    /// leaf metadata, so the selection narrows to them and a pending filter
    /// stays pending. Every other keep — and any the location declines — is
    /// tested in memory: the view's rows are resolved to exact ids (a pending
    /// filter evaluated, deferred row ids materialized), the column is
    /// streamed for them through the mapping and each code is tested. A keep
    /// never becomes a filter expression: its code list is per query, and
    /// every distinct expression would pin new entries in Vortex's
    /// identity-keyed reader caches.
    #[cfg(feature = "file-io")]
    async fn keep_file(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        let t = debug::timer();
        let QuadsSource::File {
            path,
            file,
            filter,
            selection,
            deleted,
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
        let located = match column {
            QuadColumn::S => Self::keep_located(file, &selection, keep, row_count).await?,
            _ => None,
        };
        let (filter, narrowed) = match located {
            Some(narrowed) => {
                log::debug!(
                    "[keep] {:?} located as runs of the sorted column at {:?}",
                    column,
                    debug::elapsed(t)
                );
                (filter.clone(), narrowed)
            }
            None => {
                let selection = match filter {
                    Some(f) => RowSelection::All
                        .refine(&file_scan::matching_file_rows(file, Some(f), &selection).await?),
                    None => selection,
                };
                let test = keep.test();
                let positions =
                    file_scan::column_positions(file, column.name(), &selection, |code| {
                        test.admits(code)
                    })
                    .await?;
                let selected = selection.len(row_count);
                let narrowed = selection.refine_positions(positions, selected);
                log::debug!(
                    "[keep] {:?} tested in memory over {} file rows at {:?}",
                    column,
                    selected,
                    debug::elapsed(t)
                );
                (None, narrowed)
            }
        };
        if narrowed.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: QuadsSource::File {
                path: path.clone(),
                file: file.clone(),
                filter,
                selection: ViewSelection::Exact(narrowed),
                deleted: deleted.clone(),
                serve: None,
            },
            tail: self.tail.clone(),
        })
    }

    /// A keep on the subject column served by located runs: in a file sorted
    /// by `s` the rows with `lo <= s < hi` are one run, and a set's codes one
    /// run each, found by binary search over the mapped column's leaf
    /// metadata — no column is read, no expression bound, and a pending
    /// filter is left pending beside the narrowed selection. The runs
    /// intersect `selection`, which stays a range wherever the admitted rows
    /// are one run of it.
    ///
    /// `None` declines to the stream: the file is not sorted by `s`, a chunk
    /// has no probe, or the set is too large to look up code by code (see
    /// [`set_is_located`]).
    #[cfg(feature = "file-io")]
    async fn keep_located(
        file: &crate::store::native_file::NativeStoreFile,
        selection: &RowSelection,
        keep: &Keep,
        row_count: usize,
    ) -> Result<Option<RowSelection>> {
        Ok(match keep {
            Keep::Range(lo, hi) => file_scan::locate_subject_code_range(file, *lo..*hi)
                .await?
                .map(|run| selection.clone().intersect_range(run)),
            Keep::Set(codes) if set_is_located(codes.len(), selection.len(row_count)) => {
                file_scan::locate_subject_code_runs(file, codes.as_slice())
                    .await?
                    .map(|runs| selection.clone().intersect_runs(&runs))
            }
            Keep::Set(_) => None,
        })
    }
}

/// How many selected rows a code of a set keep on the subject column is worth
/// when deciding between looking the set up and streaming the column: a
/// lookup costs two binary searches over the mapped chunk metadata per code,
/// whatever the selection, while the stream costs a little per selected row
/// plus a fixed scan, and the two meet near this many rows per code.
#[cfg(feature = "file-io")]
const LOCATED_SET_ROWS_PER_CODE: usize = 16;

/// Whether a set of `codes` codes on the subject column is looked up code by
/// code over a selection of `rows` rows rather than streamed: while it has no
/// more than one code per [`LOCATED_SET_ROWS_PER_CODE`] rows, so the work
/// stays a fraction of the stream's and a set that is large relative to its
/// selection streams as every other keep does. Always at least one code.
#[cfg(feature = "file-io")]
fn set_is_located(codes: usize, rows: usize) -> bool {
    codes <= rows.div_ceil(LOCATED_SET_ROWS_PER_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    #[cfg(feature = "file-io")]
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
}
