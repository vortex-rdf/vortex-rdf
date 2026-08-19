//! File-side probing behind the `layout` feature: locate one column's flat
//! chunk leaves inside a layout tree, then answer global bounds queries and
//! point reads by fetching and probing only the chunks a search touches.
//!
//! A chunk fetch reads the leaf's whole segment and reconstructs the array in
//! its wire encoding (`SerializedArray::decode` rebuilds metadata over the
//! segment buffers — it does not decompress); each fetched chunk is resolved
//! into an [`OwnedSortedProbe`] once and cached, so repeated queries probe
//! without further reads or resolution.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::dtype::DType;
use vortex_array::serde::SerializedArray;
use vortex_error::{VortexExpect as _, VortexResult};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};
use vortex_session::VortexSession;

use crate::OwnedSortedProbe;

/// One flat chunk leaf: its layout, logical position, and the fetched,
/// resolved probe (filled on first use; `None` when the chunk's encoding
/// declines resolution).
struct ChunkSpec {
    layout: LayoutRef,
    row_offset: u64,
    row_count: u64,
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
    chunks: Vec<ChunkSpec>,
    row_count: u64,
    dtype: DType,
}

impl ColumnChunks {
    /// Walks `root` (a struct layout) to `field`'s chunk leaves: the field
    /// child, through any zoned wrappers, then either a chunked layout of
    /// flat leaves or a single flat leaf. Returns `None` when the shape or
    /// the field's dtype (non-nullable unsigned integer) is unsupported.
    pub fn from_struct_layout(root: &LayoutRef, field: &str) -> Option<Self> {
        root.as_opt::<StructLayout>()?;
        let column = (0..root.nchildren()).find_map(|i| {
            matches!(root.child_type(i), LayoutChildType::Field(ref name) if name.as_ref() == field)
                .then(|| root.child(i).ok())
                .flatten()
        })?;
        let dtype = column.dtype().clone();
        if !dtype.is_unsigned_int() || dtype.is_nullable() {
            return None;
        }

        let data = unwrap_zoned(column)?;
        let row_count = data.row_count();
        let mut chunks = Vec::new();
        if data.is::<Flat>() {
            chunks.push(ChunkSpec {
                layout: data,
                row_offset: 0,
                row_count,
                cell: OnceLock::new(),
            });
        } else if data.is::<ChunkedLayout>() {
            for i in 0..data.nchildren() {
                let LayoutChildType::Chunk((_, row_offset)) = data.child_type(i) else {
                    return None;
                };
                let leaf = unwrap_zoned(data.child(i).ok()?)?;
                let chunk_rows = leaf.row_count();
                if chunk_rows == 0 {
                    continue;
                }
                if !leaf.is::<Flat>() {
                    return None;
                }
                chunks.push(ChunkSpec {
                    layout: leaf,
                    row_offset,
                    row_count: chunk_rows,
                    cell: OnceLock::new(),
                });
            }
        } else {
            return None;
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

    /// [`Self::bounds`] restricted to `range`, in absolute rows. Only the
    /// window must be sorted ascending — probes point-read through
    /// [`Self::value_at`], so rows outside the window are never consulted (a
    /// prefix probe searches a second key inside a lead run of a column that
    /// is not sorted as a whole). Fetches only the chunks the bisection
    /// touches (cached thereafter). `Ok(None)` when a needed chunk's encoding
    /// declines.
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
    /// first use; `None` when its encoding declines.
    async fn chunk_probe(
        &self,
        idx: usize,
        source: &Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<Option<&OwnedSortedProbe>> {
        let spec = &self.chunks[idx];
        if spec.cell.get().is_none() {
            let flat = spec
                .layout
                .as_opt::<Flat>()
                .vortex_expect("chunk leaves are validated flat at construction");
            let segment = source.request(flat.segment_id()).await?;
            let parts = match flat.array_tree().cloned() {
                Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment)?,
                None => SerializedArray::try_from(segment)?,
            };
            let row_count =
                usize::try_from(spec.row_count).vortex_expect("chunk row count must fit in usize");
            let array = parts.decode(flat.dtype(), row_count, flat.array_ctx(), session)?;
            let _ = spec.cell.set(OwnedSortedProbe::resolve(array));
        }
        Ok(spec
            .cell
            .get()
            .vortex_expect("chunk cell was just populated")
            .as_ref())
    }
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.child(0).ok()?;
    }
    Some(node)
}
