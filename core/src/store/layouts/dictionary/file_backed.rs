//! A term dictionary left in its serialized child and read on demand:
//! probes and small decodes point-read the child's flat chunk leaves, wide
//! decodes scan it. Requires a point-readable child (one flat leaf, or a
//! chunked layout of flat leaves) with 1 <= row_count <= u32::MAX. File-io
//! only.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::OnceCell;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::VarBinViewArray;
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
use crate::io::read::available_parallelism;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, buf_as_str};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::view::selection::POINT_GATHER_MAX_ROWS;

use super::predicates::{KindRanges, Scanned, TermPredicate};
use super::storage::{COL_DICT_TERM, ChunkCursor, TermChunk, check_code, chunk_of, term_column};
use super::term_dict::{
    DictMemos, TermDictionary, VerdictSets, next_dictionary_id, prefix_range_from,
    prefix_successor, tolerant_fallback,
};

/// The dictionary child's flat chunk leaves, fetched on demand in their wire
/// encoding and kept for the store's lifetime. The term column is globally
/// sorted (wire contract): term → code bisects the rows through per-row
/// reads, code → term decodes exactly the probed rows.
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

/// One flat leaf and its fetched form, filled on first use.
struct ChunkSpec {
    layout: LayoutRef,
    rows: u64,
    cell: OnceCell<TermChunk>,
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.slot(0).ok().flatten()?;
    }
    Some(node)
}

impl TermChunks {
    /// The term column's leaves under the dictionary child `dict`: the field
    /// child, through any zoned wrappers, then a chunked layout of flat
    /// leaves or one flat leaf. `None` for any other shape, an empty child,
    /// or more than `u32::MAX` rows.
    pub(crate) fn resolve(dict: &LayoutRef, source: Arc<dyn SegmentSource>) -> Option<Self> {
        dict.as_opt::<StructLayout>()?;
        let column = (0..dict.nslots()).find_map(|i| {
            matches!(dict.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
                .then(|| dict.slot(i).ok().flatten())
                .flatten()
        })?;
        let data = unwrap_zoned(column)?;
        let row_count = data.row_count();
        if row_count == 0 || row_count > u64::from(u32::MAX) {
            return None;
        }
        let leaves: Vec<(LayoutRef, u64)> = if data.is::<Flat>() {
            vec![(data, 0)]
        } else if data.is::<ChunkedLayout>() {
            let mut leaves = Vec::with_capacity(data.nslots());
            for i in 0..data.nslots() {
                let Some(LayoutChildType::Chunk((_, row_offset))) = data.slot_type(i) else {
                    return None;
                };
                leaves.push((data.slot(i).ok().flatten()?, row_offset));
            }
            leaves
        } else {
            return None;
        };
        let mut specs = Vec::with_capacity(leaves.len());
        let mut starts = Vec::with_capacity(leaves.len());
        for (leaf, row_offset) in leaves {
            let leaf = unwrap_zoned(leaf)?;
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
                cell: OnceCell::new(),
            });
            starts.push(usize::try_from(row_offset).ok()?);
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

    /// One unopened cursor per chunk.
    fn cursors(&self) -> Vec<Option<ChunkCursor<'_>>> {
        (0..self.specs.len()).map(|_| None).collect()
    }

    /// Chunk `idx` in its wire encoding, fetched and adopted on first use;
    /// concurrent first reads wait for one fetch.
    async fn chunk(&self, idx: usize) -> Result<&TermChunk> {
        let spec = &self.specs[idx];
        spec.cell
            .get_or_try_init(|| async {
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
                TermChunk::from_wire(array, &mut ctx)
            })
            .await
    }

    /// The term bytes at `row`, through `cursors`: one cursor per touched
    /// chunk, built on first use.
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

    /// The row of the first term not below `needle` in byte order (the row
    /// count when every term is below it), read through `cursors`.
    async fn lower_bound_in<'s>(
        &'s self,
        cursors: &mut [Option<ChunkCursor<'s>>],
        needle: &[u8],
    ) -> Result<u32> {
        let (mut lo, mut hi) = (0u64, self.row_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.term_bytes(cursors, mid).await? < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo as u32)
    }

    /// Term → code: the lower bound and one equality probe.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<u32>> {
        let needle = term.as_bytes();
        let mut cursors = self.cursors();
        let lo = self.lower_bound_in(&mut cursors, needle).await?;
        let hit = u64::from(lo) < self.row_count
            && self.term_bytes(&mut cursors, u64::from(lo)).await? == needle;
        Ok(hit.then_some(lo))
    }

    /// The row of the first term not below `needle` in byte order; the row
    /// count when every term is below it.
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<u32> {
        self.lower_bound_in(&mut self.cursors(), needle).await
    }

    /// The terms of `codes` (in bounds), reading exactly those rows.
    pub(crate) async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Arc<str>>> {
        let mut cursors = self.cursors();
        let mut out = Vec::with_capacity(codes.len());
        for &code in codes {
            let bytes = self.term_bytes(&mut cursors, u64::from(code)).await?;
            out.push(Arc::from(buf_as_str(bytes)?));
        }
        Ok(out)
    }
}

/// A term dictionary left in its layout child: term → code probes and code →
/// term decodes read the sorted `_dict_term` column on demand, a term's code
/// being its child row. Clones share one state.
#[derive(Clone)]
pub(crate) struct FileBackedDict(Arc<Inner>);

struct Inner {
    /// Identity of the dictionary (see `DictReader::dictionary_id`).
    id: u64,
    /// The dictionary child's layout reader, in child-local rows.
    reader: vortex_layout::LayoutReaderRef,
    /// Number of terms.
    len: u64,
    /// Wire-chunk point reads.
    chunks: TermChunks,
    /// The wide-batch scan's term projection, bound once.
    projection: OnceLock<vortex_array::expr::BoundExpression>,
    /// term → code, kind ranges and partition memos.
    memos: DictMemos,
}

impl FileBackedDict {
    /// A file-backed dictionary over the child `reader` reads, point-read
    /// through `chunks`.
    pub(crate) fn new(reader: vortex_layout::LayoutReaderRef, chunks: TermChunks) -> Self {
        Self(Arc::new(Inner {
            id: next_dictionary_id(),
            len: reader.row_count(),
            reader,
            chunks,
            projection: OnceLock::new(),
            memos: DictMemos::default(),
        }))
    }

    /// This dictionary's identity.
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        usize::try_from(self.0.len).unwrap_or(usize::MAX)
    }

    /// The file-backed form of `native`'s dictionary child; `None` when the
    /// file has no dictionary component or the child's layout cannot be
    /// point-read.
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

    /// A scan over the dictionary child.
    fn scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.0.reader.clone(),
        )
    }

    /// Term → code: a point-read binary search of the leaves, memoized.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<u32>> {
        if let Some(memo) = self.0.memos.encode.get(term) {
            return Ok(memo);
        }
        let code = self.0.chunks.encode(term).await?;
        self.0.memos.encode.put(term, code);
        Ok(code)
    }

    /// The terms of `codes`, which must be ascending, unique and in bounds:
    /// at most [`POINT_GATHER_MAX_ROWS`] codes are point-read through the
    /// leaves, more are read with one row-index scan of the child.
    pub(crate) async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Arc<str>>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(&max) = codes.last() {
            check_code(max, self.len())?;
        }
        if codes.len() <= POINT_GATHER_MAX_ROWS {
            return self.0.chunks.decode_many(codes).await;
        }
        let rows: vortex_buffer::Buffer<u64> = codes.iter().map(|&code| code as u64).collect();
        let rows = vortex_scan::strict_sorted_buffer::StrictSortedBuffer::try_new(rows)
            .map_err(VortexRdfError::Vortex)?;
        let projection = self.term_projection()?;
        let arr = crate::store::scan::file_scan::read_all_rows(
            self.scan()
                .with_row_indices(rows)
                .with_projection(projection),
        )
        .await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let col = term_column(arr, &mut ctx)?
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

    /// The child scan's term projection, bound once.
    fn term_projection(&self) -> Result<vortex_array::expr::BoundExpression> {
        if let Some(bound) = self.0.projection.get() {
            return Ok(bound.clone());
        }
        let bound = select([COL_DICT_TERM], root())
            .bind(self.0.reader.dtype())
            .map_err(VortexRdfError::Vortex)?;
        Ok(self.0.projection.get_or_init(|| bound).clone())
    }

    /// The terms of `codes` in any order, repeats allowed; an out-of-range
    /// code decodes to `None`.
    pub(crate) async fn decode_many_any(&self, codes: &[u32]) -> Result<Vec<Option<String>>> {
        let len = self.len();
        let mut distinct: Vec<u32> = codes
            .iter()
            .copied()
            .filter(|&code| (code as usize) < len)
            .collect();
        distinct.sort_unstable();
        distinct.dedup();
        let terms = self.decode_many(&distinct).await?;
        Ok(codes
            .iter()
            .map(|code| {
                distinct
                    .binary_search(code)
                    .ok()
                    .map(|i| terms[i].to_string())
            })
            .collect())
    }

    /// [`encode`](Self::encode) of `term`, then of its canonical spelling
    /// when that differs. Malformed input is an error.
    pub(crate) async fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        if let Some(code) = self.encode(term).await? {
            return Ok(Some(code));
        }
        match tolerant_fallback(term)? {
            Some(canonical) => self.encode(&canonical).await,
            None => Ok(None),
        }
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order,
    /// with the reads overlapped.
    pub(crate) async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        futures::stream::iter(terms.iter().map(|term| self.encode_tolerant(term)))
            .buffered(available_parallelism().max(4))
            .try_collect()
            .await
    }

    /// The code of the first term not below `needle` in byte order; the term
    /// count when every term is below it.
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<u32> {
        self.0.chunks.lower_bound(needle).await
    }

    /// The codes of the terms spelled with `prefix`.
    pub(crate) async fn prefix_range(&self, prefix: &str) -> Result<Range<u32>> {
        let lo = self.lower_bound(prefix.as_bytes()).await?;
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => Some(self.lower_bound(&successor).await?),
            None => None,
        };
        Ok(prefix_range_from(lo, hi, self.len() as u32))
    }

    /// The code ranges of the term kinds, computed once.
    pub(crate) async fn kind_ranges(&self) -> Result<KindRanges> {
        if let Some(kinds) = self.0.memos.kinds.get() {
            return Ok(kinds.clone());
        }
        let first_is_empty = self.0.len > 0
            && self
                .0
                .chunks
                .decode_many(&[0])
                .await?
                .first()
                .is_some_and(|term| term.is_empty());
        let kinds = KindRanges::new(
            first_is_empty,
            self.prefix_range("\"").await?,
            self.prefix_range("<").await?,
            self.prefix_range("_:").await?,
            self.len() as u32,
        );
        Ok(self.0.memos.kinds.get_or_init(|| kinds).clone())
    }

    /// The `(true, unknown)` partition of the codes by `predicate`: the scan
    /// range is read from the child in row order, one chunk at a time.
    /// Memoized.
    pub(crate) async fn filter_codes(&self, predicate: &TermPredicate) -> Result<VerdictSets> {
        if let Some(sets) = self.0.memos.predicates.get(predicate) {
            return Ok(sets);
        }
        let kinds = self.kind_ranges().await?;
        let partition = predicate.partition();
        let mut scanned = Scanned::default();
        if let Some(range) = partition.scan(&kinds).filter(|range| !range.is_empty()) {
            let tasks = self
                .scan()
                .with_row_range(u64::from(range.start)..u64::from(range.end))
                .with_projection(self.term_projection()?)
                .build()
                .map_err(VortexRdfError::Vortex)?;
            // `buffered` keeps the splits in row order, so codes are assigned
            // by counting rows.
            let mut chunks = futures::stream::iter(tasks).buffered(available_parallelism());
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let mut code = range.start;
            while let Some(chunk) = chunks.next().await {
                let Some(chunk) = chunk.map_err(VortexRdfError::Vortex)? else {
                    continue;
                };
                let col = term_column(chunk, &mut ctx)?
                    .execute::<VarBinViewArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;
                let reader = StrColReader::new(&col);
                for i in 0..col.len() {
                    scanned.visit(predicate, code, reader.str_at(i));
                    code += 1;
                }
            }
            if code != range.end {
                return Err(VortexRdfError::Deserialization(format!(
                    "Dictionary range scan returned {} rows for {} codes",
                    code - range.start,
                    range.end - range.start
                )));
            }
        }
        let mut true_ranges = Vec::new();
        for prefix in partition.true_prefixes() {
            true_ranges.push(self.prefix_range(&prefix).await?);
        }
        let sets = Arc::new(partition.assemble(&kinds, scanned, true_ranges));
        self.0.memos.predicates.put(predicate, Arc::clone(&sets));
        Ok(sets)
    }

    /// The whole dictionary lifted resident, with one term-column scan.
    pub(crate) async fn lift_resident(&self) -> Result<TermDictionary> {
        TermDictionary::from_child_reader(self.0.reader.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A windowed dictionary's written child resolves one leaf per window,
    /// and a `FileBackedDict` over it probes correctly across all of them.
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
        assert_eq!(fbd.0.chunks.specs.len(), 6);

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
