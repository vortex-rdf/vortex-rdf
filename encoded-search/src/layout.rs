//! File-side probing behind the `layout` feature: one column's flat chunk
//! leaves located inside a layout tree ([`flat_leaves`]), then global bounds
//! queries and point reads that fetch and probe only the leaves a search
//! touches. A fetch reads the leaf's whole segment and rebuilds the array in
//! its wire encoding, metadata over the segment buffers with nothing
//! decompressed ([`fetch_flat`]); each fetched leaf is resolved into an
//! [`OwnedSortedProbe`] once and cached.
//!
//! A column the writer dictionary-encoded at the layout level (a
//! `vortex.dict` node: a values leaf beside a codes subtree) is probed
//! through its codes leaves, each composed with the dictionary's values into
//! a dictionary array: the probe bisects the decoded values, so the order the
//! writer gave the codes in never matters. The values leaf is fetched once
//! and shared by every codes leaf beneath it; a run of dictionaries (the
//! writer opens a new one when one outgrows its constraints) is a chunked
//! layout of such nodes.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::{Dict, PrimitiveArray};
use vortex_array::dtype::DType;
use vortex_array::serde::SerializedArray;
use vortex_array::{Array, ArrayRef, IntoArray, VortexSessionExecute};
use vortex_error::{VortexExpect as _, VortexResult};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::dict::Dict as DictLayout;
use vortex_layout::layouts::flat::{Flat, FlatLayout};
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};
use vortex_session::VortexSession;

use crate::{OwnedSortedProbe, SortedProbe};

/// A dictionary layout's values leaf, shared by every codes leaf beneath it:
/// fetched and reconstructed in its wire encoding on first use, then cached.
struct DictValues {
    layout: FlatLayout,
    cell: OnceLock<ArrayRef>,
}

impl DictValues {
    /// The values array, fetched through `source` on first use.
    async fn array(
        &self,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        if let Some(values) = self.cell.get() {
            return Ok(values.clone());
        }
        let values = fetch_flat(&self.layout, source, session).await?;
        let _ = self.cell.set(values);
        Ok(self
            .cell
            .get()
            .vortex_expect("values cell was just populated")
            .clone())
    }
}

/// One flat chunk leaf, never empty: its layout, its first row in the
/// column, the dictionary its values index into (when it holds a
/// `vortex.dict` layout's codes), and its fetched, resolved probe (filled on
/// first use; `None` when the chunk's encoding declines resolution).
struct ChunkLeaf {
    layout: FlatLayout,
    row_offset: u64,
    row_count: u64,
    dict: Option<Arc<DictValues>>,
    cell: OnceLock<Option<OwnedSortedProbe>>,
    /// The chunk's last value, read once from its probe: the chunk bisection
    /// compares against it, and a probe's `value_at` can cost a search of its
    /// own (a RunEnd one searches its run ends).
    last: OnceLock<u64>,
}

/// The flat chunk leaves of one non-nullable unsigned-integer column of a
/// struct layout, addressable for global bounds queries and point reads.
///
/// [`Self::lower_bound`] and [`Self::bounds`] require the column to be sorted
/// ascending across the whole file, a caller contract as for
/// [`SortedProbe`](crate::SortedProbe). [`Self::value_at`] is exact
/// regardless of sort order. Rows are `u64` column positions.
pub struct ColumnChunks {
    chunks: Vec<ChunkLeaf>,
    row_count: u64,
    dtype: DType,
}

impl ColumnChunks {
    /// Walks `root` (a struct layout) to `field`'s child and takes its
    /// [`flat_leaves`]: the column's values, or the codes of the dictionary
    /// layouts encoding it, each decoding through its dictionary's values
    /// leaf. Returns `None` when the shape or the field's dtype (non-nullable
    /// unsigned integer) is unsupported.
    pub fn from_struct_layout(root: &LayoutRef, field: &str) -> Option<Self> {
        root.as_opt::<StructLayout>()?;
        let column = (0..root.nslots()).find_map(|i| {
            matches!(root.slot_type(i), Some(LayoutChildType::Field(ref name)) if name.as_ref() == field)
                .then(|| root.slot(i).ok().flatten())
                .flatten()
        })?;
        let dtype = column.dtype().clone();
        if !dtype.is_unsigned_int() || dtype.is_nullable() {
            return None;
        }

        let row_count = column.row_count();
        let mut chunks: Vec<ChunkLeaf> = Vec::new();
        for leaf in flat_leaves(&column)? {
            // A dictionary's codes leaves are adjacent and name its one
            // values leaf (one segment): they share one cache of it.
            let prev = chunks.last().and_then(|prev| prev.dict.as_ref());
            let dict = leaf.dict_values.map(|layout| match prev {
                Some(shared) if shared.layout.segment_id() == layout.segment_id() => {
                    Arc::clone(shared)
                }
                _ => Arc::new(DictValues {
                    layout,
                    cell: OnceLock::new(),
                }),
            });
            chunks.push(ChunkLeaf {
                row_offset: leaf.row_offset,
                row_count: leaf.layout.row_count(),
                layout: leaf.layout,
                dict,
                cell: OnceLock::new(),
                last: OnceLock::new(),
            });
        }
        Some(Self {
            chunks,
            row_count,
            dtype,
        })
    }

    /// Number of rows in the column.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The column's dtype (a non-nullable unsigned integer, by construction).
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    /// Number of flat chunk leaves the column is cut into.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// How many chunk leaves have been fetched so far: those whose probe was
    /// resolved and those whose encoding declined it.
    #[doc(hidden)]
    pub fn fetched_chunks(&self) -> usize {
        self.chunks
            .iter()
            .filter(|chunk| chunk.cell.get().is_some())
            .count()
    }

    /// Exact global `[lo, hi)` of `needle` in the column: the
    /// [`lower_bound`](Self::lower_bound)s of `needle` and of `needle + 1`
    /// (the row count when `needle` is `u64::MAX`). Requires the sorted
    /// contract. `Ok(None)` when a needed chunk's encoding declines the
    /// probe, for the caller to scan instead.
    pub async fn bounds(
        &self,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<Range<u64>>> {
        // The first chunk whose last value reaches the needle holds its first
        // row. A run that ends inside that chunk is bounded by one search of
        // the chunk; one that reaches its end may continue into the next
        // chunks, whose bound the global search finds.
        let Some(idx) = self.first_chunk_reaching(needle, source, session).await? else {
            return Ok(None);
        };
        if idx == self.chunks.len() {
            return Ok(Some(self.row_count..self.row_count));
        }
        let Some(probe) = self.chunk_probe(idx, source, session).await? else {
            return Ok(None);
        };
        let leaf = &self.chunks[idx];
        let (lo, hi) = probe.probe().bounds(needle);
        let lower = leaf.row_offset + lo as u64;
        if (hi as u64) < leaf.row_count {
            return Ok(Some(lower..leaf.row_offset + hi as u64));
        }
        let upper = match needle.checked_add(1) {
            Some(next) => self.lower_bound(next, source, session).await?,
            None => Some(self.row_count),
        };
        Ok(upper.map(|upper| lower..upper.max(lower)))
    }

    /// Exact global row of the first value `>= needle` (the row count when
    /// every value is below it), fetching at most the chunks a binary search
    /// over the chunks' last values touches (cached thereafter). Requires the
    /// sorted contract. `Ok(None)` when a needed chunk's encoding declines
    /// the probe.
    pub async fn lower_bound(
        &self,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<u64>> {
        // The first chunk whose last value reaches the needle holds it.
        let Some(lo) = self.first_chunk_reaching(needle, source, session).await? else {
            return Ok(None);
        };
        if lo == self.chunks.len() {
            return Ok(Some(self.row_count));
        }
        let Some(probe) = self.chunk_probe(lo, source, session).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.chunks[lo].row_offset + probe.probe().lower_bound(needle) as u64,
        ))
    }

    /// Index of the first chunk whose last value is `>= needle` (the chunk
    /// count when none is), by bisection over the chunks' cached last values.
    /// `Ok(None)` when a needed chunk's encoding declines the probe.
    async fn first_chunk_reaching(
        &self,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<usize>> {
        let (mut lo, mut hi) = (0usize, self.chunks.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(last) = self.chunk_last(mid, source, session).await? else {
                return Ok(None);
            };
            if last < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(Some(lo))
    }

    /// Chunk `idx`'s last value, read from its probe on first use; `None`
    /// when its encoding declines the probe.
    async fn chunk_last(
        &self,
        idx: usize,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<u64>> {
        if let Some(&last) = self.chunks[idx].last.get() {
            return Ok(Some(last));
        }
        let Some(probe) = self.chunk_probe(idx, source, session).await? else {
            return Ok(None);
        };
        let probe = probe.probe();
        let last = probe.value_at(probe.len() - 1);
        Ok(Some(*self.chunks[idx].last.get_or_init(|| last)))
    }

    /// [`Self::bounds`] restricted to `range`, in absolute rows, under the
    /// window contract of [`SortedProbe::bounds_in`](crate::SortedProbe::bounds_in):
    /// only the window must be sorted ascending, and rows outside it are never
    /// read. A window inside one chunk leaf is searched through that leaf's
    /// probe; a wider one picks the leaf holding each bound by the leaves'
    /// last values (rows of the window for every leaf but its last), then
    /// searches inside that leaf. Fetches only the chunks those searches touch
    /// (cached thereafter). `Ok(None)` when a needed chunk's encoding declines.
    ///
    /// # Panics
    /// Panics if `range.end > self.row_count()`.
    pub async fn bounds_in(
        &self,
        range: Range<u64>,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<Range<u64>>> {
        assert!(range.end <= self.row_count, "window out of bounds");
        if range.is_empty() {
            return Ok(Some(range.start..range.start));
        }
        // The leaf holding the lower bound is searched for both bounds at
        // once; only a run that reaches the end of that leaf's part of the
        // window needs its upper bound searched past it.
        let Some(leaf) = self
            .window_leaf(range.clone(), needle, source, session)
            .await?
        else {
            return Ok(None);
        };
        let bounds = leaf.bounds(needle);
        if bounds.end < leaf.rows().end || bounds.end == range.end {
            return Ok(Some(bounds));
        }
        let upper = match needle.checked_add(1) {
            Some(next) => match self
                .window_leaf(bounds.end..range.end, next, source, session)
                .await?
            {
                Some(leaf) => leaf.bounds(next).start,
                None => return Ok(None),
            },
            None => range.end,
        };
        Ok(Some(bounds.start..upper))
    }

    /// The part of the non-empty `window` in the leaf
    /// holding its first row whose value is `>= needle`, under the window
    /// contract: the first leaf whose last row in the window reaches the
    /// needle, which for every leaf but the window's last is the leaf's own
    /// last row. `Ok(None)` when a needed chunk's encoding declines.
    async fn window_leaf(
        &self,
        window: Range<u64>,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<LeafRows<'_>>> {
        let first = self
            .chunks
            .partition_point(|c| c.row_offset + c.row_count <= window.start);
        let last = self
            .chunks
            .partition_point(|c| c.row_offset + c.row_count < window.end)
            .max(first);
        let (mut lo, mut hi) = (first, last);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(value) = self.chunk_last(mid, source, session).await? else {
                return Ok(None);
            };
            if value < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let leaf = &self.chunks[lo];
        let Some(probe) = self.chunk_probe(lo, source, session).await? else {
            return Ok(None);
        };
        let start = window.start.max(leaf.row_offset) - leaf.row_offset;
        let end = window.end.min(leaf.row_offset + leaf.row_count) - leaf.row_offset;
        Ok(Some(LeafRows {
            probe: probe.probe(),
            row_offset: leaf.row_offset,
            window: start as usize..end as usize,
        }))
    }

    /// The row past the end of the chunk leaf holding `row`, from the layout
    /// alone: walking a run leaf by leaf, each piece is one [`Self::leaf_rows`].
    ///
    /// # Panics
    /// Panics if `row >= self.row_count()`.
    pub fn leaf_end(&self, row: u64) -> u64 {
        assert!(row < self.row_count, "row {row} out of bounds");
        let leaf = &self.chunks[self
            .chunks
            .partition_point(|c| c.row_offset + c.row_count <= row)];
        leaf.row_offset + leaf.row_count
    }

    /// The rows `rows` (absolute, non-empty) as a [`LeafRows`] of the one
    /// chunk leaf holding them all, whose probe is fetched once: searches and
    /// reads inside a located run then touch that leaf only, where each
    /// [`Self::bounds_in`] step or [`Self::value_at`] finds its chunk again.
    /// `Ok(None)` when the rows span leaves or the leaf's encoding declines.
    ///
    /// # Panics
    /// Panics if `rows` is empty or ends past `self.row_count()`.
    pub async fn leaf_rows(
        &self,
        rows: Range<u64>,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<LeafRows<'_>>> {
        assert!(
            rows.start < rows.end && rows.end <= self.row_count,
            "rows {rows:?} out of bounds"
        );
        let idx = self
            .chunks
            .partition_point(|c| c.row_offset + c.row_count <= rows.start);
        let leaf = &self.chunks[idx];
        if rows.end > leaf.row_offset + leaf.row_count {
            return Ok(None);
        }
        let Some(probe) = self.chunk_probe(idx, source, session).await? else {
            return Ok(None);
        };
        let local = |row: u64| (row - leaf.row_offset) as usize;
        Ok(Some(LeafRows {
            probe: probe.probe(),
            row_offset: leaf.row_offset,
            window: local(rows.start)..local(rows.end),
        }))
    }

    /// Exact value at global `row`, fetching (and caching) only the chunk
    /// that holds it. Needs no sort order. `Ok(None)` when that chunk's
    /// encoding declines the probe.
    ///
    /// # Panics
    /// Panics if `row >= self.row_count()`.
    pub async fn value_at(
        &self,
        row: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<u64>> {
        assert!(row < self.row_count, "row {row} out of bounds");
        let idx = self
            .chunks
            .partition_point(|c| c.row_offset + c.row_count <= row)
            .min(self.chunks.len() - 1);
        let Some(probe) = self.chunk_probe(idx, source, session).await? else {
            return Ok(None);
        };
        // The local row is below the leaf's row count, which its fetch
        // checked fits a `usize`.
        Ok(Some(
            probe
                .probe()
                .value_at((row - self.chunks[idx].row_offset) as usize),
        ))
    }

    /// The values at `rows` (ascending), as one array of the column's dtype:
    /// each leaf holding some of them takes them from its array in the wire
    /// encoding (the take kernels decode only what they touch), and only
    /// those leaves are fetched and cached. `Ok(None)` when a leaf the rows
    /// reach declines its probe.
    ///
    /// # Panics
    /// Panics if a row is `>= self.row_count()`.
    pub async fn take(
        &self,
        rows: &[u64],
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<ArrayRef>> {
        use vortex_array::arrays::{ChunkedArray, PrimitiveArray};

        let mut parts = Vec::new();
        let mut rest = rows;
        let mut idx = 0;
        while let Some(&first) = rest.first() {
            assert!(first < self.row_count, "row {first} out of bounds");
            idx += self.chunks[idx..].partition_point(|c| c.row_offset + c.row_count <= first);
            let leaf = &self.chunks[idx];
            let inside = rest.partition_point(|&row| row < leaf.row_offset + leaf.row_count);
            let Some(probe) = self.chunk_probe(idx, source, session).await? else {
                return Ok(None);
            };
            let offsets =
                PrimitiveArray::from_iter(rest[..inside].iter().map(|&row| row - leaf.row_offset));
            parts.push(probe.array().take(offsets.into_array())?);
            rest = &rest[inside..];
        }
        Ok(Some(match parts.len() {
            1 => parts.pop().vortex_expect("one part"),
            _ => ChunkedArray::try_new(parts, self.dtype.clone())?.into_array(),
        }))
    }

    /// The chunk's resolved probe, fetched through `source` and resolved on
    /// first use; `None` when its encoding declines. A codes leaf is composed
    /// with its dictionary's values before resolution, so the probe reads
    /// the column's values.
    async fn chunk_probe(
        &self,
        idx: usize,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<&OwnedSortedProbe>> {
        let leaf = &self.chunks[idx];
        if leaf.cell.get().is_none() {
            let array = fetch_flat(&leaf.layout, source, session).await?;
            let array = match &leaf.dict {
                None => array,
                Some(dict) => {
                    let values = dict.array(source, session).await?;
                    Array::<Dict>::try_new(array, values)?.into_array()
                }
            };
            // VORTEX_RDF_ENCODED_SEARCH=0: the leaf is decoded first and its decoded values searched.
            let array = if crate::encoded_search_enabled()
                || !array.dtype().is_unsigned_int()
                || array.dtype().is_nullable()
            {
                array
            } else {
                let mut ctx = session.create_execution_ctx();
                array.execute::<PrimitiveArray>(&mut ctx)?.into_array()
            };
            let _ = leaf.cell.set(OwnedSortedProbe::resolve(array));
        }
        Ok(leaf
            .cell
            .get()
            .vortex_expect("chunk cell was just populated")
            .as_ref())
    }
}

/// A row window of a [`ColumnChunks`] column inside one chunk leaf, from
/// [`ColumnChunks::leaf_rows`]: bounds searches within the window and point
/// reads through the leaf's probe, in the column's absolute rows.
pub struct LeafRows<'a> {
    probe: &'a SortedProbe<'a>,
    row_offset: u64,
    window: Range<usize>,
}

impl LeafRows<'_> {
    /// The window's rows, absolute.
    pub fn rows(&self) -> Range<u64> {
        self.row_offset + self.window.start as u64..self.row_offset + self.window.end as u64
    }

    /// The rows of the window equal to `needle`, absolute, under the window
    /// contract of [`SortedProbe::bounds_in`]: only the window must be sorted
    /// ascending.
    pub fn bounds(&self, needle: u64) -> Range<u64> {
        let (lo, hi) = self.probe.bounds_in(self.window.clone(), needle);
        self.row_offset + lo as u64..self.row_offset + hi as u64
    }

    /// The value at absolute `row`.
    ///
    /// # Panics
    /// Panics if `row` is outside the window.
    pub fn value_at(&self, row: u64) -> u64 {
        assert!(self.rows().contains(&row), "row {row} outside the window");
        self.probe.value_at((row - self.row_offset) as usize)
    }
}

/// Reports the column's shape and how much of it has been fetched: a fetched
/// chunk has already cost a segment read.
impl std::fmt::Debug for ColumnChunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnChunks")
            .field("row_count", &self.row_count)
            .field("dtype", &self.dtype)
            .field("chunks", &self.chunk_count())
            .field(
                "dictionary_coded",
                &self.chunks.iter().filter(|c| c.dict.is_some()).count(),
            )
            .field("fetched", &self.fetched_chunks())
            .finish()
    }
}

/// One flat leaf of a column's layout tree, as [`flat_leaves`] finds it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FlatLeaf {
    /// The leaf: one segment holding the array of its rows.
    pub layout: FlatLayout,
    /// The column row of the leaf's first row.
    pub row_offset: u64,
    /// When the leaf holds a `vortex.dict` layout's codes rather than the
    /// column's values: that dictionary's flat values leaf, the one every
    /// codes leaf beneath the dictionary names.
    pub dict_values: Option<FlatLayout>,
}

/// The flat leaves holding `column`'s rows, in row order: through zoned
/// wrappers, the chunks of a chunked layout (each at its own offset), and a
/// dictionary layout's codes child, whose leaves name the dictionary's flat
/// values leaf. A flat leaf of zero rows is left out, so a chunked layout
/// with an empty chunk yields fewer leaves than it has chunks. `None` for any
/// other shape, including a dictionary whose values are not one flat leaf and
/// a dictionary nested inside another's codes.
pub fn flat_leaves(column: &LayoutRef) -> Option<Vec<FlatLeaf>> {
    let mut leaves = Vec::new();
    collect_leaves(column.clone(), 0, None, &mut leaves)?;
    Some(leaves)
}

/// [`flat_leaves`]' walk of `node`, whose first row is the column's row
/// `offset`, into `out`; `dict_values` is the values leaf of the dictionary
/// whose codes `node` holds.
fn collect_leaves(
    node: LayoutRef,
    offset: u64,
    dict_values: Option<&FlatLayout>,
    out: &mut Vec<FlatLeaf>,
) -> Option<()> {
    let node = unwrap_zoned(node)?;
    if let Some(flat) = node.as_opt::<Flat>() {
        if flat.row_count() > 0 {
            out.push(FlatLeaf {
                layout: flat.clone(),
                row_offset: offset,
                dict_values: dict_values.cloned(),
            });
        }
        return Some(());
    }
    if node.is::<ChunkedLayout>() {
        for i in 0..node.nslots() {
            let Some(LayoutChildType::Chunk((_, chunk_offset))) = node.slot_type(i) else {
                return None;
            };
            let child = node.slot(i).ok().flatten()?;
            collect_leaves(child, offset + chunk_offset, dict_values, out)?;
        }
        return Some(());
    }
    if node.is::<DictLayout>() {
        if dict_values.is_some() {
            return None;
        }
        let values = unwrap_zoned(node.slot(0).ok().flatten()?)?;
        let values = values.as_opt::<Flat>()?;
        let codes = node.slot(1).ok().flatten()?;
        return collect_leaves(codes, offset, Some(values), out);
    }
    None
}

/// Fetch a flat leaf's segment and rebuild its array in the wire encoding:
/// metadata over the segment buffers, nothing decompressed. `session` must
/// register the encodings the leaf was written with.
///
/// # Errors
/// The segment request's error; the decode's, for a segment that does not
/// hold the leaf's array or an encoding `session` does not register; and a
/// row count past what a `usize` holds.
pub async fn fetch_flat(
    flat: &FlatLayout,
    source: &Arc<dyn SegmentSource>,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    let segment = source.request(flat.segment_id()).await?;
    let parts = match flat.array_tree().cloned() {
        Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment)?,
        None => SerializedArray::try_from(segment)?,
    };
    let row_count = usize::try_from(flat.row_count())?;
    parts.decode(flat.dtype(), row_count, flat.array_ctx(), session)
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.slot(0).ok().flatten()?;
    }
    Some(node)
}
