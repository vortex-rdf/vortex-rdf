//! The file-backed arm of the Dictionary layout's residency axis: a term
//! dictionary left in its serialized child, read on demand. Probes and
//! decodes point-read the child's wire-encoded chunk leaves
//! ([`TermChunks`]), so a dictionary whose child cannot be point-read is not
//! file-backed at all — [`store::open`](crate::store::open) hands that shape
//! to the resident arm instead. The policy enum choosing between this and
//! the resident form is [`DictAccess`](super::access::DictAccess); the whole
//! module only compiles with `file-io`, since without a file there is
//! nothing to leave the terms in.

use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::expr::{root, select};
use vortex_array::serde::SerializedArray;
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};

use crate::error::{Result, VortexRdfError};
use crate::io::container::DICT_COMPONENT_NAME;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, buf_as_str};
use crate::store::native_file::NativeStoreFile;
use crate::store::selection::POINT_GATHER_MAX_ROWS;

use super::check_code;
use super::term_dict::{
    COL_DICT_TERM, ChunkCursor, ProbeCache, TermChunk, TermDictionary, chunk_of,
};

/// The dictionary child's flat chunk leaves, fetched on demand in their wire
/// encoding and kept for the store's lifetime — the string sibling of the
/// quad columns' chunk-probe handles on `NativeStoreFile`. A fetched leaf
/// stays FSST when it arrived FSST (a row read decompresses one value) and
/// is canonicalized once otherwise. The term column is globally sorted (wire
/// contract), so term → code probes binary-search rows through per-row
/// reads, touching only the chunks the bisection crosses; code → term reads
/// decode exactly the probed rows.
pub(crate) struct TermChunks {
    /// The leaves, in row order.
    specs: Vec<ChunkSpec>,
    /// `starts[i]` = global row of leaf i's first term; ascending.
    starts: Vec<usize>,
    /// Terms in the column.
    row_count: u64,
    /// Where the leaves' segments are fetched from.
    source: Arc<dyn SegmentSource>,
}

/// One flat term-chunk leaf and its fetched form (filled on first use).
struct ChunkSpec {
    layout: LayoutRef,
    rows: u64,
    cell: OnceLock<TermChunk>,
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.slot(0).ok().flatten()?;
    }
    Some(node)
}

impl TermChunks {
    /// Walks the dictionary child's layout to its term column's chunk
    /// leaves: the field child, through any zoned wrappers, then a chunked
    /// layout of flat leaves or a single flat leaf. `None` when the shape is
    /// anything else — the caller keeps the scan paths.
    pub(crate) fn resolve(dict: &LayoutRef, source: Arc<dyn SegmentSource>) -> Option<Self> {
        dict.as_opt::<StructLayout>()?;
        let column = (0..dict.nslots()).find_map(|i| {
            matches!(dict.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
                .then(|| dict.slot(i).ok().flatten())
                .flatten()
        })?;
        let data = unwrap_zoned(column)?;
        let row_count = data.row_count();
        // Codes are u32 by construction; an empty child has nothing to
        // point-read and an oversized one cannot be a term column.
        if row_count == 0 || row_count > u64::from(u32::MAX) {
            return None;
        }
        let mut specs = Vec::new();
        let mut starts = Vec::new();
        if data.is::<Flat>() {
            specs.push(ChunkSpec {
                layout: data,
                rows: row_count,
                cell: OnceLock::new(),
            });
            starts.push(0);
        } else if data.is::<ChunkedLayout>() {
            for i in 0..data.nslots() {
                let Some(LayoutChildType::Chunk((_, row_offset))) = data.slot_type(i) else {
                    return None;
                };
                let leaf = unwrap_zoned(data.slot(i).ok().flatten()?)?;
                let rows = leaf.row_count();
                if rows == 0 {
                    continue;
                }
                if !leaf.is::<Flat>() {
                    return None;
                }
                specs.push(ChunkSpec {
                    layout: leaf,
                    rows,
                    cell: OnceLock::new(),
                });
                starts.push(usize::try_from(row_offset).ok()?);
            }
        } else {
            return None;
        }
        Some(Self {
            specs,
            starts,
            row_count,
            source,
        })
    }

    /// The chunk holding global `row`, and the row local to it.
    fn locate(&self, row: u64) -> (usize, usize) {
        chunk_of(&self.starts, row as usize)
    }

    /// The fetched form of chunk `idx`, read and adopted on first use. The
    /// segment read reconstructs the wire encoding (no decompression);
    /// concurrent first reads may race to build, and the loser's copy is
    /// dropped.
    async fn chunk(&self, idx: usize) -> Result<&TermChunk> {
        let spec = &self.specs[idx];
        if spec.cell.get().is_none() {
            let flat = spec
                .layout
                .as_opt::<Flat>()
                .expect("term chunk leaves are validated flat at construction");
            let segment = self
                .source
                .request(flat.segment_id())
                .await
                .map_err(VortexRdfError::Vortex)?;
            let parts = match flat.array_tree().cloned() {
                Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment),
                None => SerializedArray::try_from(segment),
            }
            .map_err(VortexRdfError::Vortex)?;
            let rows = usize::try_from(spec.rows).expect("chunk row count must fit in usize");
            let array = parts
                .decode(flat.dtype(), rows, flat.array_ctx(), &VORTEX_SESSION)
                .map_err(VortexRdfError::Vortex)?;
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let _ = spec.cell.set(TermChunk::from_wire(array, &mut ctx)?);
        }
        Ok(spec
            .cell
            .get()
            .expect("the chunk was just initialized above"))
    }

    /// The term bytes at `row`, read through `cursors` — one lazily built
    /// cursor per touched chunk, so repeated reads in one call reuse the
    /// cursor's decode scratch.
    async fn term_bytes<'s, 'c>(
        &'s self,
        cursors: &'c mut [Option<ChunkCursor<'s>>],
        row: u64,
    ) -> Result<&'c [u8]> {
        let (idx, local) = self.locate(row);
        if cursors[idx].is_none() {
            cursors[idx] = Some(self.chunk(idx).await?.cursor());
        }
        Ok(cursors[idx]
            .as_mut()
            .expect("the cursor was just initialized above")
            .bytes_at(local))
    }

    /// Term → code: a binary search over per-row reads — the async twin of
    /// `TermDictionary::search`, the same three-way compare per step that
    /// returns as soon as the probe hits.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<u32>> {
        let needle = term.as_bytes();
        let mut cursors: Vec<Option<ChunkCursor<'_>>> =
            (0..self.specs.len()).map(|_| None).collect();
        let (mut lo, mut hi) = (0u64, self.row_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.term_bytes(&mut cursors, mid).await?.cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Some(mid as u32)),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// Code → term for each of `codes` (in-bounds, the caller's contract),
    /// reading exactly the probed rows.
    pub(crate) async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Arc<str>>> {
        let mut cursors: Vec<Option<ChunkCursor<'_>>> =
            (0..self.specs.len()).map(|_| None).collect();
        let mut out = Vec::with_capacity(codes.len());
        for &code in codes {
            let bytes = self.term_bytes(&mut cursors, u64::from(code)).await?;
            out.push(Arc::from(buf_as_str(bytes)?));
        }
        Ok(out)
    }
}

/// A term dictionary left in its layout child, with no term held resident:
/// term → code probes and code → term decodes read the sorted `_dict_term`
/// column on demand.
///
/// `reader` is the dictionary child's layout reader (the native store root's
/// `dictionary` component), so a term's code is its child row. Probes and
/// small decodes point-read the wire-encoded chunk leaves through
/// [`TermChunks`], with probe answers memoized in a [`ProbeCache`]; a wide
/// decode instead scans the row indices it wants through `reader`.
#[derive(Clone)]
pub(crate) struct FileBackedDict {
    /// The dictionary child's layout reader (child-local row coordinates).
    reader: vortex_layout::LayoutReaderRef,
    /// Number of terms.
    len: u64,
    /// term → code memo, shared across clones (every derived view of a store
    /// probes the same immutable dictionary).
    probes: Arc<ProbeCache>,
    /// Wire-chunk point-read handle, shared across clones — the dictionary
    /// analogue of the quad columns' cached chunk probes.
    chunks: Arc<TermChunks>,
    /// The wide-batch scan's term projection, bound once per handle — a
    /// fresh bind per call would miss the reader's identity-keyed caches
    /// (see `BoundExprMemo` on the store file handle) and grow them per
    /// call. Shared across clones like the reader whose caches it keys.
    projection: Arc<OnceLock<vortex_array::expr::BoundExpression>>,
}

impl FileBackedDict {
    /// A file-backed dictionary over the child `reader` reads, point-read
    /// through `chunks`; the term count is the reader's row count.
    pub(crate) fn new(reader: vortex_layout::LayoutReaderRef, chunks: TermChunks) -> Self {
        Self {
            len: reader.row_count(),
            reader,
            probes: Arc::new(ProbeCache::new()),
            chunks: Arc::new(chunks),
            projection: Arc::new(OnceLock::new()),
        }
    }

    /// The file-backed form of `native`'s dictionary child: its cached
    /// reader plus the wire-chunk handle resolved off the same child. `None`
    /// when the file has no dictionary component or the child's layout
    /// shape cannot be point-read (the caller then lifts the dictionary
    /// resident).
    pub(crate) fn open(native: &NativeStoreFile) -> Result<Option<Self>> {
        let Some((_, reader)) = native
            .component_reader(DICT_COMPONENT_NAME)
            .map_err(VortexRdfError::Vortex)?
        else {
            return Ok(None);
        };
        let Some(child) = native
            .component_layout(DICT_COMPONENT_NAME)
            .map_err(VortexRdfError::Vortex)?
        else {
            return Ok(None);
        };
        Ok(TermChunks::resolve(&child, native.segment_source())
            .map(|chunks| Self::new(reader, chunks)))
    }

    /// A scan over the dictionary child — the reader-level equivalent of
    /// `file.scan()`.
    fn scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// Term → code: a point-read binary search of the chunk leaves, memoized.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<u32>> {
        if let Some(memo) = self.probes.get(term) {
            return Ok(memo);
        }
        let code = self.chunks.encode(term).await?;
        self.probes.put(term, code);
        Ok(code)
    }

    /// Code → term for reconstruction: resolve `codes` (ascending, unique)
    /// to their term strings — the dictionary's code → string seam. Batches
    /// of at most [`POINT_GATHER_MAX_ROWS`] codes are point-read through the
    /// chunk leaves; wider ones are read with one row-index scan of the
    /// child.
    pub(crate) async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Arc<str>>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(&max) = codes.last() {
            check_code(max, usize::try_from(self.len).unwrap_or(usize::MAX))?;
        }
        if codes.len() <= POINT_GATHER_MAX_ROWS {
            return self.chunks.decode_many(codes).await;
        }
        let rows: vortex_buffer::Buffer<u64> = codes.iter().map(|&code| code as u64).collect();
        let rows = vortex_scan::strict_sorted_buffer::StrictSortedBuffer::try_new(rows)
            .map_err(VortexRdfError::Vortex)?;
        let projection = match self.projection.get() {
            Some(bound) => bound.clone(),
            None => {
                let bound = select([COL_DICT_TERM], root())
                    .bind(self.reader.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                self.projection.get_or_init(|| bound).clone()
            }
        };
        let arr = crate::store::scan::file_scan::read_all_rows(
            self.scan()
                .with_row_indices(rows)
                .with_projection(projection),
        )
        .await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = arr
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let col = struct_arr
            .unmasked_field_by_name(COL_DICT_TERM)
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        if col.len() != codes.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "Dictionary row-index scan returned {} rows for {} codes",
                col.len(),
                codes.len()
            )));
        }
        let reader = StrColReader::new(&col);
        (0..col.len())
            .map(|i| reader.str_at(i).map(Arc::from))
            .collect()
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::access::DictAccess::ensure_resident
    pub(crate) async fn lift_resident(&self) -> Result<TermDictionary> {
        TermDictionary::from_child_reader(
            self.reader.clone(),
            super::term_dict::DictForm::AsWritten,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The compression windows survive serialization as the child's chunk
    /// leaves: a windowed dictionary's written child resolves one leaf per
    /// window, and a `FileBackedDict` over it probes correctly across all of
    /// them.
    #[tokio::test]
    async fn windowed_dict_child_chunk_leaves() {
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;

        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let bytes = crate::tests::write_dict_only_store(&d).await;

        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = NativeStoreFile::try_new(file).unwrap();
        let fbd = FileBackedDict::open(&native)
            .unwrap()
            .expect("the written dictionary child must be point-readable");

        // One chunk leaf per compression window, none merged, none re-cut.
        assert_eq!(fbd.chunks.specs.len(), 6);

        // Probes across every window: interior, first-of-window,
        // last-of-window, and absent.
        for (i, term) in terms.iter().enumerate().step_by(97) {
            assert_eq!(fbd.encode(term).await.unwrap(), Some(i as u32), "{term}");
        }
        for boundary in (0..600).step_by(100) {
            assert_eq!(
                fbd.encode(&terms[boundary]).await.unwrap(),
                Some(boundary as u32)
            );
            assert_eq!(
                fbd.encode(&terms[boundary + 99]).await.unwrap(),
                Some((boundary + 99) as u32)
            );
        }
        assert_eq!(fbd.encode("<http://zzz>").await.unwrap(), None);
    }
}
