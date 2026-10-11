//! File-side probing behind the `layout` feature: locate one column's flat
//! chunk leaves inside a layout tree, then answer global bounds queries and
//! point reads by fetching and probing only the chunks a search touches.
//!
//! A chunk fetch reads the leaf's whole segment and reconstructs the array in
//! its wire encoding (`SerializedArray::decode` rebuilds metadata over the
//! segment buffers — it does not decompress); each fetched chunk is resolved
//! into an [`OwnedSortedProbe`] once and cached, so repeated queries probe
//! without further reads or resolution.
//!
//! A column the writer dictionary-encoded at the layout level (a `vortex.dict`
//! node: a values leaf beside a codes subtree) is probed through its codes
//! leaves: each is composed with the dictionary's values into a dictionary
//! array, which the probe resolves like any other — its dictionary node reads
//! and bisects the decoded values, so the order the writer assigned codes in
//! never matters. The values leaf is fetched once and shared by every codes
//! leaf beneath it; a run of dictionaries (the writer opens a new one when a
//! dictionary outgrows its constraints) is a chunked layout of such nodes.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::Dict;
use vortex_array::dtype::DType;
use vortex_array::serde::SerializedArray;
use vortex_array::{Array, ArrayRef, IntoArray};
use vortex_error::{VortexExpect as _, VortexResult};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::dict::Dict as DictLayout;
use vortex_layout::layouts::flat::{Flat, FlatLayout};
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};
use vortex_session::VortexSession;

use crate::OwnedSortedProbe;

/// A dictionary layout's values leaf, shared by every codes leaf beneath it:
/// fetched and reconstructed in its wire encoding on first use, then cached.
struct DictValues {
    layout: LayoutRef,
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
        let flat = self
            .layout
            .as_opt::<Flat>()
            .vortex_expect("dictionary values leaves are validated flat at construction");
        let values = fetch_flat(flat, self.layout.row_count(), source, session).await?;
        let _ = self.cell.set(values);
        Ok(self
            .cell
            .get()
            .vortex_expect("values cell was just populated")
            .clone())
    }
}

/// One flat chunk leaf: its layout, logical position, the dictionary its
/// values index into (when it holds a `vortex.dict` layout's codes rather
/// than the column's values), and the fetched, resolved probe (filled on
/// first use; `None` when the chunk's encoding declines resolution).
struct ChunkLeaf {
    layout: LayoutRef,
    row_offset: u64,
    row_count: u64,
    dict: Option<Arc<DictValues>>,
    cell: OnceLock<Option<OwnedSortedProbe>>,
}

/// The flat chunk leaves of one non-nullable unsigned-integer column of a
/// struct layout, addressable for global bounds queries and point reads.
///
/// [`Self::bounds`] requires the column to be sorted ascending across the
/// whole file — a caller contract, exactly as for
/// [`SortedProbe`](crate::SortedProbe). [`Self::value_at`] is exact
/// regardless of sort order.
pub struct ColumnChunks {
    chunks: Vec<ChunkLeaf>,
    row_count: u64,
    dtype: DType,
}

impl ColumnChunks {
    /// Walks `root` (a struct layout) to `field`'s chunk leaves: the field
    /// child, through any zoned wrappers, then flat leaves — directly, under
    /// a chunked layout, or as the codes of a dictionary layout (whose flat
    /// values leaf they decode through), itself possibly one of a chunked run
    /// of dictionaries. Returns `None` when the shape or the field's dtype
    /// (non-nullable unsigned integer) is unsupported.
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
        let mut chunks = Vec::new();
        collect_leaves(column, 0, None, &mut chunks)?;
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

    /// Exact global `[lo, hi)` of `needle` in the column, fetching at most
    /// the chunks a binary search over chunk extremes touches (cached
    /// thereafter). Requires the sorted contract. `Ok(None)` when a needed
    /// chunk's encoding declines the probe — the caller falls back to its
    /// scan path.
    pub async fn bounds(
        &self,
        needle: u64,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<Range<u64>>> {
        if self.chunks.is_empty() {
            return Ok(Some(0..0));
        }

        // First chunk whose last value is >= needle.
        let (mut lo, mut hi) = (0usize, self.chunks.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(probe) = self.chunk_probe(mid, source, session).await? else {
                return Ok(None);
            };
            if probe.value_at(probe.len() - 1) < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let lower = if lo == self.chunks.len() {
            self.row_count
        } else {
            let Some(probe) = self.chunk_probe(lo, source, session).await? else {
                return Ok(None);
            };
            self.chunks[lo].row_offset + probe.lower_bound(needle) as u64
        };

        // First chunk whose first value is > needle.
        let (mut lo2, mut hi2) = (0usize, self.chunks.len());
        while lo2 < hi2 {
            let mid = lo2 + (hi2 - lo2) / 2;
            let Some(probe) = self.chunk_probe(mid, source, session).await? else {
                return Ok(None);
            };
            if probe.value_at(0) <= needle {
                lo2 = mid + 1;
            } else {
                hi2 = mid;
            }
        }
        let upper = if lo2 == 0 {
            0
        } else {
            let Some(probe) = self.chunk_probe(lo2 - 1, source, session).await? else {
                return Ok(None);
            };
            self.chunks[lo2 - 1].row_offset + probe.upper_bound(needle) as u64
        };

        Ok(Some(lower..upper.max(lower)))
    }

    /// [`Self::bounds`] restricted to `range`, in absolute rows, under the
    /// window contract of [`SortedProbe::bounds_in`](crate::SortedProbe::bounds_in):
    /// only the window must be sorted ascending, and rows outside it are never
    /// read. Fetches only the chunks the bisection touches (cached
    /// thereafter). `Ok(None)` when a needed chunk's encoding declines.
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
        // partition_point over the window through async point reads.
        let partition =
            async |mut lo: u64, mut hi: u64, upper: bool| -> VortexResult<Option<u64>> {
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let Some(v) = self.value_at(mid, source, session).await? else {
                        return Ok(None);
                    };
                    if v < needle || (upper && v == needle) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                Ok(Some(lo))
            };
        let Some(lo) = partition(range.start, range.end, false).await? else {
            return Ok(None);
        };
        let Some(hi) = partition(lo, range.end, true).await? else {
            return Ok(None);
        };
        Ok(Some(lo..hi))
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
        Ok(Some(
            probe.value_at((row - self.chunks[idx].row_offset) as usize),
        ))
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
            let flat = leaf
                .layout
                .as_opt::<Flat>()
                .vortex_expect("chunk leaves are validated flat at construction");
            let array = fetch_flat(flat, leaf.row_count, source, session).await?;
            let array = match &leaf.dict {
                None => array,
                Some(dict) => {
                    let values = dict.array(source, session).await?;
                    Array::<Dict>::try_new(array, values)?.into_array()
                }
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

/// Collect the flat leaves under `node`, whose first row is the column's
/// absolute row `offset`, into `out` in row order: through zoned wrappers,
/// the chunks of a chunked layout (each at its own offset within the node),
/// and a dictionary layout's codes child — whose leaves decode through the
/// dictionary's flat values leaf, `dict`. `None` for any other shape,
/// including a dictionary nested inside another's codes.
fn collect_leaves(
    node: LayoutRef,
    offset: u64,
    dict: Option<Arc<DictValues>>,
    out: &mut Vec<ChunkLeaf>,
) -> Option<()> {
    let node = unwrap_zoned(node)?;
    if node.is::<Flat>() {
        let row_count = node.row_count();
        if row_count > 0 {
            out.push(ChunkLeaf {
                layout: node,
                row_offset: offset,
                row_count,
                dict,
                cell: OnceLock::new(),
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
            collect_leaves(child, offset + chunk_offset, dict.clone(), out)?;
        }
        return Some(());
    }
    if node.is::<DictLayout>() {
        if dict.is_some() {
            return None;
        }
        let values = unwrap_zoned(node.slot(0).ok().flatten()?)?;
        if !values.is::<Flat>() {
            return None;
        }
        let codes = node.slot(1).ok().flatten()?;
        let dict = Arc::new(DictValues {
            layout: values,
            cell: OnceLock::new(),
        });
        return collect_leaves(codes, offset, Some(dict), out);
    }
    None
}

/// Fetch a flat leaf's segment and rebuild its array in the wire encoding —
/// metadata over the segment buffers, nothing decompressed.
async fn fetch_flat(
    flat: &FlatLayout,
    row_count: u64,
    source: &Arc<dyn SegmentSource>,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    let segment = source.request(flat.segment_id()).await?;
    let parts = match flat.array_tree().cloned() {
        Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment)?,
        None => SerializedArray::try_from(segment)?,
    };
    let row_count = usize::try_from(row_count).vortex_expect("leaf row count must fit in usize");
    parts.decode(flat.dtype(), row_count, flat.array_ctx(), session)
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.slot(0).ok().flatten()?;
    }
    Some(node)
}
