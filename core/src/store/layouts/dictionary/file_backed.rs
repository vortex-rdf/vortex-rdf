//! The file-backed arm of the Dictionary layout's residency axis: a term
//! dictionary left in its serialized child, read on demand. Probes and
//! decodes point-read the child's wire-encoded chunk leaves
//! ([`TermChunks`]), so a dictionary whose child cannot be point-read is not
//! file-backed at all — [`store::open`](crate::store::open) hands that shape
//! to the resident arm instead. The policy enum choosing between this and
//! the resident form is [`DictAccess`](super::access::DictAccess); the whole
//! module only compiles with `file-io`, since without a file there is
//! nothing to leave the terms in.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use futures::{StreamExt as _, TryStreamExt as _};
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

use crate::common::terms::canonical_spelling;
use crate::error::{Result, VortexRdfError};
use crate::io::container::DICT_COMPONENT_NAME;
use crate::io::read::available_parallelism;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, buf_as_str};
use crate::store::native_file::NativeStoreFile;
use crate::store::selection::POINT_GATHER_MAX_ROWS;

use super::check_code;
use super::predicates::{KindRanges, Scanned, TermPredicate};
use super::term_dict::{
    COL_DICT_TERM, ChunkCursor, PredicateMemo, ProbeCache, TermChunk, TermDictionary, VerdictSets,
    chunk_of, prefix_successor,
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

    /// The row of the first term not below `needle` in byte order (the row
    /// count when every term is below it) — the async twin of
    /// `TermDictionary::lower_bound`.
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<u32> {
        let mut cursors: Vec<Option<ChunkCursor<'_>>> =
            (0..self.specs.len()).map(|_| None).collect();
        let (mut lo, mut hi) = (0u64, self.row_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.term_bytes(&mut cursors, mid).await? < needle {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo as u32)
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
    /// The kind ranges, computed on first use (a few probes), shared across
    /// clones.
    kinds: Arc<OnceLock<KindRanges>>,
    /// Memo for [`filter_codes`](Self::filter_codes), shared across clones —
    /// a partition is a scan of the child, the one read worth keeping.
    predicates: Arc<PredicateMemo>,
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
            kinds: Arc::new(OnceLock::new()),
            predicates: Arc::new(PredicateMemo::new()),
        }
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        usize::try_from(self.len).unwrap_or(usize::MAX)
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
        let projection = self.term_projection()?;
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

    /// The child scan's term projection, bound once per handle (see the
    /// field).
    fn term_projection(&self) -> Result<vortex_array::expr::BoundExpression> {
        match self.projection.get() {
            Some(bound) => Ok(bound.clone()),
            None => {
                let bound = select([COL_DICT_TERM], root())
                    .bind(self.reader.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                Ok(self.projection.get_or_init(|| bound).clone())
            }
        }
    }

    /// Code → term for `codes` in any order, repeats allowed, out-of-range
    /// codes decoding to `None`: the distinct in-range codes are read once
    /// through [`decode_many`](Self::decode_many) and scattered back.
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

    /// The async twin of `TermDictionary::encode_tolerant`.
    pub(crate) async fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        if let Some(code) = self.encode(term).await? {
            return Ok(Some(code));
        }
        let canonical = canonical_spelling(term)?;
        if canonical == term {
            return Ok(None);
        }
        self.encode(&canonical).await
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order,
    /// with the lookups' chunk reads overlapped.
    pub(crate) async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        futures::stream::iter(terms.iter().map(|term| self.encode_tolerant(term)))
            .buffered(available_parallelism().max(4))
            .try_collect()
            .await
    }

    /// The async twin of `TermDictionary::lower_bound`.
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<u32> {
        self.chunks.lower_bound(needle).await
    }

    /// The async twin of `TermDictionary::prefix_range`.
    pub(crate) async fn prefix_range(&self, prefix: &str) -> Result<Range<u32>> {
        let lo = self.lower_bound(prefix.as_bytes()).await?;
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => self.lower_bound(&successor).await?,
            None => self.len() as u32,
        };
        Ok(lo..hi.max(lo))
    }

    /// The async twin of `TermDictionary::kind_ranges`, computed once per
    /// handle.
    pub(crate) async fn kind_ranges(&self) -> Result<KindRanges> {
        if let Some(kinds) = self.kinds.get() {
            return Ok(kinds.clone());
        }
        let default_graph = if self.len > 0 {
            let first = self.chunks.decode_many(&[0]).await?;
            first
                .first()
                .is_some_and(|term| term.is_empty())
                .then_some(0)
        } else {
            None
        };
        let kinds = KindRanges {
            default_graph,
            literals: self.prefix_range("\"").await?,
            iris: self.prefix_range("<").await?,
            blanks: self.prefix_range("_:").await?,
            len: self.len() as u32,
        };
        Ok(self.kinds.get_or_init(|| kinds).clone())
    }

    /// The async twin of `TermDictionary::filter_codes`: the predicate's
    /// scan range is read from the child in row order, one chunk at a
    /// time, and evaluated as it streams — a file-backed dictionary never
    /// holds more than a chunk of terms decoded.
    pub(crate) async fn filter_codes(&self, predicate: &TermPredicate) -> Result<VerdictSets> {
        let key = predicate.to_string();
        if let Some(sets) = self.predicates.get(&key) {
            return Ok(sets);
        }
        let kinds = self.kind_ranges().await?;
        let plan = predicate.scan_plan(&kinds);
        let mut scanned = Scanned::default();
        if let Some(range) = plan.scan.filter(|range| !range.is_empty()) {
            let tasks = self
                .scan()
                .with_row_range(u64::from(range.start)..u64::from(range.end))
                .with_projection(self.term_projection()?)
                .build()
                .map_err(VortexRdfError::Vortex)?;
            // `buffered` yields the splits in row order, so codes are
            // assigned by counting rows as they stream.
            let mut chunks = futures::stream::iter(tasks).buffered(available_parallelism());
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let mut code = range.start;
            while let Some(chunk) = chunks.next().await {
                let Some(chunk) = chunk.map_err(VortexRdfError::Vortex)? else {
                    continue;
                };
                let struct_arr = chunk
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;
                let col = struct_arr
                    .unmasked_field_by_name(COL_DICT_TERM)
                    .map_err(VortexRdfError::Vortex)?
                    .clone()
                    .execute::<VarBinViewArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;
                let reader = StrColReader::new(&col);
                for i in 0..col.len() {
                    match reader.str_at(i) {
                        Ok(spelling) => scanned.visit(predicate, code, spelling),
                        Err(_) => scanned.unknown.push(code),
                    }
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
        let mut true_ranges = Vec::with_capacity(plan.true_prefixes.len());
        for prefix in &plan.true_prefixes {
            true_ranges.push(self.prefix_range(prefix).await?);
        }
        let sets = Arc::new(predicate.assemble(&kinds, scanned, &true_ranges));
        self.predicates.put(key, Arc::clone(&sets));
        Ok(sets)
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::access::DictAccess::ensure_resident
    pub(crate) async fn lift_resident(&self) -> Result<TermDictionary> {
        TermDictionary::from_child_reader(self.reader.clone()).await
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

    /// The dictionary child's term column carries exact window bounds: one
    /// zone per FSST window whose `vortex.min()`/`vortex.max()` are the
    /// window's first and last terms; the component is stamped version 2.
    #[tokio::test]
    async fn dict_child_zone_maps_hold_window_bounds() {
        use crate::io::container::{DICT_COMPONENT_NAME, DICT_VERSION};
        use crate::store::array::StrColReader;
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;
        use vortex_layout::LayoutChildType;
        use vortex_layout::layouts::zoned::Zoned;

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
        let descriptor = native
            .components()
            .iter()
            .find(|c| c.name == DICT_COMPONENT_NAME)
            .unwrap();
        assert_eq!(descriptor.version, DICT_VERSION);

        let child = native
            .component_layout(DICT_COMPONENT_NAME)
            .unwrap()
            .unwrap();
        let column = (0..child.nslots())
            .find_map(|i| {
                matches!(child.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
                    .then(|| child.slot(i).ok().flatten())
                    .flatten()
            })
            .unwrap();
        let zoned = column
            .as_opt::<Zoned>()
            .expect("the term column is zone-mapped");
        assert_eq!(zoned.nzones(), 6);
        let names: Vec<String> = super::super::term_dict::window_bound_aggregates()
            .iter()
            .map(ToString::to_string)
            .collect();
        for name in &names {
            assert!(zoned.present_aggregates().contains(name), "{name}");
        }
        let zones = column.slot(1).unwrap().unwrap();
        let reader = zones
            .new_reader(
                "zones".into(),
                native.segment_source(),
                &VORTEX_SESSION,
                &Default::default(),
            )
            .unwrap();
        let table = crate::io::read::scan_all_reader(reader).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let table = table.execute::<StructArray>(&mut ctx).unwrap();
        let maxes = table
            .unmasked_field_by_name(&names[0])
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        let mins = table
            .unmasked_field_by_name(&names[1])
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        for w in 0..6 {
            assert_eq!(StrColReader::new(&mins).str_at(w).unwrap(), terms[w * 100]);
            assert_eq!(
                StrColReader::new(&maxes).str_at(w).unwrap(),
                terms[w * 100 + 99]
            );
        }
    }
}
