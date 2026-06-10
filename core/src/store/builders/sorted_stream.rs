use crate::io::VORTEX_SESSION;
use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use super::{VortexArrayBuilder, EncodedQuad, assemble_chunks};

use std::path::Path;
use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind};
use web_time::{SystemTime, UNIX_EPOCH, Instant};
use std::collections::BinaryHeap;
use futures::{Stream, StreamExt};
use oxrdf::Quad;

use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::validity::Validity;

/// An out-of-core, scalable sorted stream Vortex RDF Array Builder.
///
/// This builder is designed to process extremely large datasets that exceed available system memory.
///
/// ### Serialization Phases:
/// 1. **Ingestion & Runs Generation**:
///    Reads quads from the stream in memory-bounded batches (e.g., 500k quads).
///    Maps terms to unique numeric IDs using zero-copy bulk ingestion via `get_or_insert_bulk`.
///    Accumulates quads in memory up to a `chunk_size` limit (e.g., 1M), sorts them locally,
///    and serializes the sorted run to a temporary file on disk.
/// 2. **External Merge Sort (K-Way Merge)**:
///    Uses a min-heap (BinaryHeap) to merge all sorted run files from disk in linear time
///    and strictly bounded $O(1)$ memory.
/// 3. **Thin Chunking**:
///    As quads are merged from the heap, they are accumulated into memory-bounded chunks.
///    Each chunk builds its own sorted secondary permutation indexes locally to preserve the
///    out-of-core memory scalability property.
/// 4. **De-duplicated Root-Level Dictionary**:
///    Assembles the thin chunks into a single unified `ChunkedArray`.
///    Appends the specialized dictionary columns exactly **once** at the root level of the final `StructArray`
///    to eliminate redundant dictionary copies and minimize file size.
pub struct SortedStreamBuilder;

/// Min-heap item used for the K-way merge sort.
struct HeapItem {
    quad: EncodedQuad,
    reader_idx: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.quad == other.quad
    }
}

// Custom Ord implementation for min-heap (reversing comparison order).
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.quad.cmp(&self.quad)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Helper function to write a sorted run of encoded quads to disk.
fn write_run(path: &Path, quads: &[EncodedQuad]) -> Result<()> {
    let file = File::create(path)
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);
    for q in quads {
        bincode::serialize_into(&mut writer, q)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }
    Ok(())
}

/// Helper structure to stream quads back from a sorted run file on disk.
struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        Ok(Self { reader: BufReader::new(file) })
    }

    /// Read the next quad from the run file. Returns `None` on End-Of-File.
    fn next_quad(&mut self) -> Result<Option<EncodedQuad>> {
        match bincode::deserialize_from(&mut self.reader) {
            Ok(q) => Ok(Some(q)),
            Err(e) => {
                if let bincode::ErrorKind::Io(ref io_err) = *e {
                    if io_err.kind() == ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                }
                Err(VortexRdfError::Deserialization(e.to_string()))
            }
        }
    }
}

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let start = Instant::now();
        let mut dictionary = Dict::new();
        
        // Generate a unique temporary directory to store sorted run files.
        let id = uuid::Uuid::new_v4();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = std::env::current_dir()
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
            .join("target")
            .join(format!("tmp_vortex_sorted_stream_{}_{}", now, id));
        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

        let chunk_size = 500_000;
        let mut buffer = Vec::with_capacity(chunk_size);
        let mut run_paths = Vec::new();
        let mut total_ingested = 0;

        let batch_size = 500_000;
        let mut quad_batch = Vec::with_capacity(batch_size);

        // Helper closure to process and dictionary-encode a batch of quads.
        let process_batch = |batch: &mut Vec<Quad>, dict: &mut Dict, buf: &mut Vec<EncodedQuad>, total: &mut usize| {
            if batch.is_empty() {
                return;
            }
            *total += batch.len();
            let mut batch_terms = Vec::with_capacity(batch.len() * 4);
            for quad in batch.iter() {
                batch_terms.push(quad.subject.to_string());
                batch_terms.push(quad.predicate.to_string());
                batch_terms.push(quad.object.to_string());
                batch_terms.push(quad.graph_name.to_string());
            }
            let term_refs: Vec<&str> = batch_terms.iter().map(|s| s.as_str()).collect();
            let dict_encode_start = Instant::now();
            let ids = dict.get_or_insert_bulk(&term_refs);
            let dict_duration = dict_encode_start.elapsed();
            log::debug!("[SortedStreamBuilder] Dictionary encoding of {} terms completed in {:?}", term_refs.len(), dict_duration);

            for i in 0..batch.len() {
                buf.push(EncodedQuad {
                    s: ids[i * 4],
                    p: ids[i * 4 + 1],
                    o: ids[i * 4 + 2],
                    g: ids[i * 4 + 3],
                });
            }
            batch.clear();
        };

        // ── Phase 1: Ingest, dictionary-encode, and write sorted runs ──
        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quad_batch.push(res?);

            // Process and encode quads in memory-bounded batches to optimize RAM consumption.
            if quad_batch.len() >= batch_size {
                process_batch(&mut quad_batch, &mut dictionary, &mut buffer, &mut total_ingested);

                // Once chunk size limit is reached, sort locally and serialize to a run file.
                if buffer.len() >= chunk_size {
                    buffer.sort_unstable();
                    let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
                    write_run(&run_path, &buffer)?;
                    log::debug!("[SortedStreamBuilder] Wrote sorted run {} of size {} to disk", run_paths.len(), buffer.len());
                    run_paths.push(run_path);
                    buffer.clear();
                }
            }
        }

        // Flush any remaining elements in the batch.
        process_batch(&mut quad_batch, &mut dictionary, &mut buffer, &mut total_ingested);

        // Flush the final run to disk.
        if !buffer.is_empty() {
            buffer.sort_unstable();
            let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
            write_run(&run_path, &buffer)?;
            log::debug!("[SortedStreamBuilder] Wrote final sorted run {} of size {} to disk", run_paths.len(), buffer.len());
            run_paths.push(run_path);
            buffer.clear();
        }

        log::debug!("[SortedStreamBuilder] Ingested and dictionary-encoded {} quads", total_ingested);
        log::debug!("[SortedStreamBuilder] External merge sort - sorted and serialized runs");

        // Serialize the completed RdfDictionary to Vortex.
        let dict_vortex_start = Instant::now();
        let dict_fields = dictionary.to_vortex_array()?;
        log::debug!("[SortedStreamBuilder] Serialized dictionary to Vortex in {:?}", dict_vortex_start.elapsed());

        // ── Phase 2: K-Way external merge sort using a min-heap ──
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(RunReader::new(path)?);
        }

        let mut heap = BinaryHeap::new();
        for (i, r) in readers.iter_mut().enumerate() {
            if let Some(q) = r.next_quad()? {
                heap.push(HeapItem { quad: q, reader_idx: i });
            }
        }

        // ── Phase 3: Thin chunking & local secondary indexing during merge ──
        let mut chunks = Vec::new();
        let mut chunk_s = Vec::with_capacity(chunk_size);
        let mut chunk_p = Vec::with_capacity(chunk_size);
        let mut chunk_o = Vec::with_capacity(chunk_size);
        let mut chunk_g = Vec::with_capacity(chunk_size);
        
        let mut global_idx = 0u32;
        let mut total_rows = 0;

        // Nested helper to build and index a thin chunk locally in constant memory.
        let mut flush_chunk = |s_ids: &mut Vec<u32>, p_ids: &mut Vec<u32>, o_ids: &mut Vec<u32>, g_ids: &mut Vec<u32>, start_idx: u32| -> Result<()> {
            let n = s_ids.len();
            if n == 0 {
                return Ok(());
            }

            // Build local secondary sorted Object index.
            let mut o_index: Vec<(u32, u32)> = o_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, o_id)| (o_id, start_idx + local_idx as u32))
                .collect();
            o_index.sort_unstable_by_key(|pair| pair.0);
            let idx_o_val: Vec<u32> = o_index.iter().map(|pair| pair.0).collect();
            let idx_o_rid: Vec<u32> = o_index.iter().map(|pair| pair.1).collect();

            // Build local secondary sorted Predicate index.
            let mut p_index: Vec<(u32, u32)> = p_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, p_id)| (p_id, start_idx + local_idx as u32))
                .collect();
            p_index.sort_unstable_by_key(|pair| pair.0);
            let idx_p_val: Vec<u32> = p_index.iter().map(|pair| pair.0).collect();
            let idx_p_rid: Vec<u32> = p_index.iter().map(|pair| pair.1).collect();

            let field_names: Vec<Arc<str>> = vec![
                "s".into(), 
                "p".into(), 
                "o".into(), 
                "g".into(),
                "_idx_o_val".into(),
                "_idx_o_rid".into(),
                "_idx_p_val".into(),
                "_idx_p_rid".into(),
            ];

            let field_arrays: Vec<ArrayRef> = vec![
                PrimitiveArray::from_iter(s_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(p_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(o_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(g_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(idx_o_val).into_array(),
                PrimitiveArray::from_iter(idx_o_rid).into_array(),
                PrimitiveArray::from_iter(idx_p_val).into_array(),
                PrimitiveArray::from_iter(idx_p_rid).into_array(),
            ];

            let struct_chunk = StructArray::try_new(
                field_names.into(),
                field_arrays,
                n,
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array();

            log::debug!("[SortedStreamBuilder] Sorted, indexed, and flushed thin chunk {} of size {} starting at index {}", chunks.len(), n, start_idx);
            chunks.push(struct_chunk);
            Ok(())
        };

        // Merge quads from run readers and write chunks.
        while let Some(item) = heap.pop() {
            chunk_s.push(item.quad.s);
            chunk_p.push(item.quad.p);
            chunk_o.push(item.quad.o);
            chunk_g.push(item.quad.g);

            if chunk_s.len() >= chunk_size {
                let start_idx = global_idx;
                global_idx += chunk_s.len() as u32;
                total_rows += chunk_s.len();
                flush_chunk(&mut chunk_s, &mut chunk_p, &mut chunk_o, &mut chunk_g, start_idx)?;
            }

            let r_idx = item.reader_idx;
            if let Some(next_q) = readers[r_idx].next_quad().map_err(|e| VortexRdfError::Deserialization(e.to_string()))? {
                heap.push(HeapItem { quad: next_q, reader_idx: r_idx });
            }
        }

        // Flush any remaining elements.
        if !chunk_s.is_empty() {
            let start_idx = global_idx;
            total_rows += chunk_s.len();
            flush_chunk(&mut chunk_s, &mut chunk_p, &mut chunk_o, &mut chunk_g, start_idx)?;
        }

        // Clean up temporary run files.
        for path in &run_paths {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir(&temp_dir);

        // ── Phase 4: Root-level dictionary construction ──
        let assembled = assemble_chunks(chunks)?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let assembled_struct = assembled.clone().execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let mut field_names: Vec<Arc<str>> = vec![
            "s".into(), "p".into(), "o".into(), "g".into(),
            "_idx_o_val".into(), "_idx_o_rid".into(),
            "_idx_p_val".into(), "_idx_p_rid".into(),
        ];

        let mut field_arrays: Vec<ArrayRef> = vec![
            assembled_struct.unmasked_field_by_name("s").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("p").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("o").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("g").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("_idx_o_val").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("_idx_o_rid").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("_idx_p_val").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("_idx_p_rid").map_err(VortexRdfError::Vortex)?.clone(),
        ];

        // Attach store type identifier and dictionary arrays exactly once at the root level.
        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), total_rows).into_array());

        for (name, arr) in &dict_fields {
            field_names.push(format!("_dict_{}", name).into());
            field_arrays.push(indexes::array_as_dict_column(arr.clone(), total_rows)?);
        }

        let final_struct = StructArray::try_new(
            field_names.into(),
            field_arrays,
            total_rows,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        log::debug!("[SortedStreamBuilder] External merge sort serialization complete. Total rows: {}", total_rows);
        log::debug!("[SortedStreamBuilder] Completed serialization of {} quads in {:?}", total_rows, start.elapsed());

        Ok(final_struct)
    }
}
