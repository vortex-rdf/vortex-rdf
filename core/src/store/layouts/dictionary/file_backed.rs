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
use vortex_array::stream::ArrayStreamExt as _;
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, buf_as_str};
use crate::store::selection::POINT_GATHER_MAX_ROWS;

use super::term_dict::{
    ChunkCursor, ProbeCache, TERM_FIELD, TermChunk, TermDictionary, dict_from_reader,
};

/// The dictionary child's flat chunk leaves, fetched on demand in their wire
/// encoding and kept for the store's lifetime — the string sibling of the
/// quad columns' chunk-probe handles on `NativeStoreFile`. A fetched leaf
/// stays FSST when it arrived FSST (a row read decompresses one value) and
/// is canonicalized once otherwise. The term column is globally sorted (wire
/// contract), so term→ID probes binary-search rows through per-row reads,
/// touching only the chunks the bisection crosses; ID→term reads decode
/// exactly the probed rows.
pub(crate) struct TermChunks {
    specs: Vec<ChunkSpec>,
    row_count: u64,
    source: Arc<dyn SegmentSource>,
}

/// One flat term-chunk leaf and its fetched form (filled on first use).
struct ChunkSpec {
    layout: LayoutRef,
    row_offset: u64,
    rows: u64,
    cell: OnceLock<TermChunk>,
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.child(0).ok()?;
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
        let column = (0..dict.nchildren()).find_map(|i| {
            matches!(dict.child_type(i), LayoutChildType::Field(ref n) if n.as_ref() == TERM_FIELD)
                .then(|| dict.child(i).ok())
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
        if data.is::<Flat>() {
            specs.push(ChunkSpec {
                layout: data,
                row_offset: 0,
                rows: row_count,
                cell: OnceLock::new(),
            });
        } else if data.is::<ChunkedLayout>() {
            for i in 0..data.nchildren() {
                let LayoutChildType::Chunk((_, row_offset)) = data.child_type(i) else {
                    return None;
                };
                let leaf = unwrap_zoned(data.child(i).ok()?)?;
                let rows = leaf.row_count();
                if rows == 0 {
                    continue;
                }
                if !leaf.is::<Flat>() {
                    return None;
                }
                specs.push(ChunkSpec {
                    layout: leaf,
                    row_offset,
                    rows,
                    cell: OnceLock::new(),
                });
            }
        } else {
            return None;
        }
        Some(Self {
            specs,
            row_count,
            source,
        })
    }

    /// The chunk holding global `row`, and the row local to it.
    fn locate(&self, row: u64) -> (usize, usize) {
        let idx = self
            .specs
            .partition_point(|s| s.row_offset + s.rows <= row)
            .min(self.specs.len() - 1);
        (idx, (row - self.specs[idx].row_offset) as usize)
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

    /// Term→ID: a binary search over per-row reads.
    pub(crate) async fn get_id(&self, term: &str) -> Result<Option<u32>> {
        let needle = term.as_bytes();
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
        if lo == self.row_count {
            return Ok(None);
        }
        Ok((self.term_bytes(&mut cursors, lo).await? == needle).then_some(lo as u32))
    }

    /// ID→terms for `codes` (in-bounds, the caller's contract), reading
    /// exactly the probed rows.
    pub(crate) async fn resolve_terms(&self, codes: &[u32]) -> Result<Vec<String>> {
        let mut cursors: Vec<Option<ChunkCursor<'_>>> =
            (0..self.specs.len()).map(|_| None).collect();
        let mut out = Vec::with_capacity(codes.len());
        for &code in codes {
            let bytes = self.term_bytes(&mut cursors, u64::from(code)).await?;
            out.push(buf_as_str(bytes)?.to_owned());
        }
        Ok(out)
    }
}

/// A term dictionary left in its layout child: term→ID probes and ID→term
/// decodes read the sorted `_dict_term` column on demand instead of holding
/// all terms resident.
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
}

impl FileBackedDict {
    pub(crate) fn new(
        reader: vortex_layout::LayoutReaderRef,
        len: u64,
        chunks: TermChunks,
    ) -> Self {
        Self {
            reader,
            len,
            probes: Arc::new(ProbeCache::new()),
            chunks: Arc::new(chunks),
        }
    }

    /// A scan over the dictionary child — the reader-level equivalent of
    /// `file.scan()`.
    fn scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// Term→ID: a point-read binary search of the chunk leaves, memoized.
    pub(crate) async fn get_id(&self, term: &str) -> Result<Option<u32>> {
        if let Some(memo) = self.probes.get(term) {
            return Ok(memo);
        }
        let code = self.chunks.get_id(term).await?;
        self.probes.put(term, code);
        Ok(code)
    }

    /// ID→terms for reconstruction: resolve `codes` (ascending, unique) to
    /// their term strings — the dictionary's code→string seam. The
    /// layout-side chunk decode (`ResolvedLayout::decode_chunk_async`)
    /// resolves a chunk's distinct codes through this. Small batches
    /// point-read the chunk leaves; wide ones run a single row-index scan,
    /// whose bulk decode wins once most of a leaf is wanted anyway.
    pub(crate) async fn resolve_terms(&self, codes: &[u32]) -> Result<Vec<String>> {
        if codes.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(&max) = codes.last()
            && max as u64 >= self.len
        {
            return Err(VortexRdfError::Deserialization(format!(
                "Term code {} out of dictionary bounds ({})",
                max, self.len
            )));
        }
        if codes.len() <= POINT_GATHER_MAX_ROWS {
            return self.chunks.resolve_terms(codes).await;
        }
        let rows: vortex_buffer::Buffer<u64> = codes.iter().map(|&code| code as u64).collect();
        let arr = self
            .scan()
            .with_row_indices(rows)
            .with_projection(select([TERM_FIELD], root()))
            .into_array_stream()
            .map_err(VortexRdfError::Vortex)?
            .read_all()
            .await
            .map_err(VortexRdfError::Vortex)?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = arr
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let col = struct_arr
            .unmasked_field_by_name(TERM_FIELD)
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
            .map(|i| reader.str_at(i).map(str::to_string))
            .collect()
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::access::DictAccess::ensure_resident
    pub(crate) async fn load_resident(&self) -> Result<TermDictionary> {
        dict_from_reader(self.reader.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;
    use vortex_array::validity::Validity;

    /// The compression windows survive serialization as the child's chunk
    /// leaves: a windowed dictionary's written child resolves one leaf per
    /// window, and a `FileBackedDict` over it probes correctly across all of
    /// them.
    #[tokio::test]
    async fn windowed_dict_child_chunk_leaves() {
        use crate::io::container;
        use vortex_array::stream::ArrayStreamAdapter;
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;

        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let len = d.len() as u64;

        // A minimal native file: a one-row quad child plus the dictionary.
        let quads = StructArray::try_new(
            ["s", "p", "o", "g"].into(),
            (0..4)
                .map(|_| vortex_buffer::Buffer::from_iter([0u32]).into_array())
                .collect::<Vec<_>>(),
            1,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let dtype = quads.dtype().clone();
        let stream = ArrayStreamAdapter::new(
            dtype,
            Box::pin(futures::stream::once(async move { Ok(quads) })),
        );
        let mut bytes: Vec<u8> = Vec::new();
        container::write_store(
            &VORTEX_SESSION,
            &mut bytes,
            stream,
            container::default_child_strategy(),
            false,
            vec![crate::io::ser::dict_component(&d).unwrap()],
        )
        .await
        .unwrap();

        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = crate::store::native_file::NativeStoreFile::try_new(file).unwrap();
        let (_, reader) = native
            .component_reader(container::DICT_COMPONENT_NAME)
            .unwrap()
            .expect("the dictionary child must be present");
        let typed = native
            .footer()
            .layout()
            .as_::<container::RdfStoreLayoutVTable>();
        let (_, dict_child) = container::store_component(typed, container::DICT_COMPONENT_NAME)
            .unwrap()
            .expect("the dictionary child must be present");
        let chunks = TermChunks::resolve(&dict_child, native.segment_source())
            .expect("the written dictionary child must be point-readable");

        // One chunk leaf per compression window, none merged, none re-cut.
        assert_eq!(chunks.specs.len(), 6);
        let fbd = FileBackedDict::new(reader, len, chunks);

        // Probes across every window: interior, first-of-window,
        // last-of-window, and absent.
        for (i, term) in terms.iter().enumerate().step_by(97) {
            assert_eq!(fbd.get_id(term).await.unwrap(), Some(i as u32), "{term}");
        }
        for boundary in (0..600).step_by(100) {
            assert_eq!(
                fbd.get_id(&terms[boundary]).await.unwrap(),
                Some(boundary as u32)
            );
            assert_eq!(
                fbd.get_id(&terms[boundary + 99]).await.unwrap(),
                Some((boundary + 99) as u32)
            );
        }
        assert_eq!(fbd.get_id("<http://zzz>").await.unwrap(), None);
    }
}
