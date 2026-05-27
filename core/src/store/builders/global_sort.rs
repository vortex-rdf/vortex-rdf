use std::sync::Arc;
use std::time::Instant;
use futures::{Stream, StreamExt};
use vortex_array::{ArrayRef, IntoArray};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::validity::Validity;
use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use oxrdf::Quad;
use super::{VortexArrayBuilder, EncodedQuad, assemble_chunks};

pub struct GlobalSortBuilder;

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

struct RunReader {
    reader: std::io::BufReader<std::fs::File>,
    current: Option<EncodedQuad>,
}

impl RunReader {
    fn new(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(file);
        let current = Self::read_one(&mut reader)?;
        Ok(Self { reader, current })
    }

    fn read_one(reader: &mut std::io::BufReader<std::fs::File>) -> std::io::Result<Option<EncodedQuad>> {
        use std::io::Read;
        let mut buf = [0u8; 16];
        match reader.read_exact(&mut buf) {
            Ok(()) => {
                let s = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                let p = u32::from_le_bytes(buf[4..8].try_into().unwrap());
                let o = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                let g = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                Ok(Some(EncodedQuad { s, p, o, g }))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn next_quad(&mut self) -> std::io::Result<Option<EncodedQuad>> {
        let prev = self.current;
        self.current = Self::read_one(&mut self.reader)?;
        Ok(prev)
    }
}

fn write_run(path: &std::path::Path, buffer: &[EncodedQuad]) -> Result<()> {
    use std::io::Write;
    let file = std::fs::File::create(path).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    let mut writer = std::io::BufWriter::new(file);
    for quad in buffer {
        writer.write_all(&quad.s.to_le_bytes()).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        writer.write_all(&quad.p.to_le_bytes()).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        writer.write_all(&quad.o.to_le_bytes()).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        writer.write_all(&quad.g.to_le_bytes()).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }
    writer.flush().map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    Ok(())
}

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for GlobalSortBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let start_dict = Instant::now();
        let mut dictionary = Dict::new();

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp_dir = std::env::current_dir()
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
            .join("target")
            .join(format!("tmp_vortex_sort_{}_{}", now, id));
        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

        let chunk_size = 500_000;
        let mut buffer = Vec::with_capacity(chunk_size);
        let mut run_paths = Vec::new();

        let batch_size = 100_000;
        let mut quad_batch = Vec::with_capacity(batch_size);

        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quad_batch.push(res?);

            if quad_batch.len() >= batch_size {
                let mut batch_terms = Vec::with_capacity(quad_batch.len() * 4);
                for quad in &quad_batch {
                    batch_terms.push(quad.subject.to_string());
                    batch_terms.push(quad.predicate.to_string());
                    batch_terms.push(quad.object.to_string());
                    batch_terms.push(quad.graph_name.to_string());
                }
                let term_refs: Vec<&str> = batch_terms.iter().map(|s| s.as_str()).collect();
                let ids = dictionary.get_or_insert_bulk(&term_refs);

                for i in 0..quad_batch.len() {
                    buffer.push(EncodedQuad {
                        s: ids[i * 4],
                        p: ids[i * 4 + 1],
                        o: ids[i * 4 + 2],
                        g: ids[i * 4 + 3],
                    });
                }
                quad_batch.clear();

                if buffer.len() >= chunk_size {
                    buffer.sort_unstable();
                    let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
                    write_run(&run_path, &buffer)?;
                    run_paths.push(run_path);
                    buffer.clear();
                }
            }
        }

        if !quad_batch.is_empty() {
            let mut batch_terms = Vec::with_capacity(quad_batch.len() * 4);
            for quad in &quad_batch {
                batch_terms.push(quad.subject.to_string());
                batch_terms.push(quad.predicate.to_string());
                batch_terms.push(quad.object.to_string());
                batch_terms.push(quad.graph_name.to_string());
            }
            let term_refs: Vec<&str> = batch_terms.iter().map(|s| s.as_str()).collect();
            let ids = dictionary.get_or_insert_bulk(&term_refs);

            for i in 0..quad_batch.len() {
                buffer.push(EncodedQuad {
                    s: ids[i * 4],
                    p: ids[i * 4 + 1],
                    o: ids[i * 4 + 2],
                    g: ids[i * 4 + 3],
                });
            }
        }

        if !buffer.is_empty() {
            buffer.sort_unstable();
            let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
            write_run(&run_path, &buffer)?;
            run_paths.push(run_path);
            buffer.clear();
        }

        log::debug!(
            "[GlobalSortBuilder] Run generation complete. Created {} runs in {:?}",
            run_paths.len(),
            start_dict.elapsed()
        );

        let dict_fields = dictionary.to_vortex_array()?;

        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(RunReader::new(path).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?);
        }

        let mut heap = std::collections::BinaryHeap::new();
        for reader_idx in 0..readers.len() {
            if let Some(quad) = readers[reader_idx].next_quad().map_err(|e| VortexRdfError::Deserialization(e.to_string()))? {
                heap.push(HeapItem { quad, reader_idx });
            }
        }

        let mut chunks = Vec::new();
        let mut chunk_s = Vec::with_capacity(chunk_size);
        let mut chunk_p = Vec::with_capacity(chunk_size);
        let mut chunk_o = Vec::with_capacity(chunk_size);
        let mut chunk_g = Vec::with_capacity(chunk_size);
        
        let mut global_idx = 0u32;
        let mut total_rows = 0;

        let mut flush_chunk = |s_ids: &mut Vec<u32>, p_ids: &mut Vec<u32>, o_ids: &mut Vec<u32>, g_ids: &mut Vec<u32>, start_idx: u32| -> Result<()> {
            let n = s_ids.len();
            if n == 0 {
                return Ok(());
            }

            let mut o_index: Vec<(u32, u32)> = o_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, o_id)| (o_id, start_idx + local_idx as u32))
                .collect();
            o_index.sort_unstable_by_key(|pair| pair.0);

            let idx_o_val: Vec<u32> = o_index.iter().map(|pair| pair.0).collect();
            let idx_o_rid: Vec<u32> = o_index.iter().map(|pair| pair.1).collect();

            let mut p_index: Vec<(u32, u32)> = p_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, p_id)| (p_id, start_idx + local_idx as u32))
                .collect();
            p_index.sort_unstable_by_key(|pair| pair.0);

            let idx_p_val: Vec<u32> = p_index.iter().map(|pair| pair.0).collect();
            let idx_p_rid: Vec<u32> = p_index.iter().map(|pair| pair.1).collect();

            let mut field_names: Vec<Arc<str>> = vec![
                "s".into(), 
                "p".into(), 
                "o".into(), 
                "g".into(),
                "_idx_o_val".into(),
                "_idx_o_rid".into(),
                "_idx_p_val".into(),
                "_idx_p_rid".into(),
            ];

            let mut field_arrays: Vec<ArrayRef> = vec![
                PrimitiveArray::from_iter(s_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(p_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(o_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(g_ids.drain(..)).into_array(),
                PrimitiveArray::from_iter(idx_o_val).into_array(),
                PrimitiveArray::from_iter(idx_o_rid).into_array(),
                PrimitiveArray::from_iter(idx_p_val).into_array(),
                PrimitiveArray::from_iter(idx_p_rid).into_array(),
            ];

            field_names.push("store_type".into());
            field_arrays.push(ConstantArray::new(Dict::store_type(), n).into_array());

            for (name, arr) in &dict_fields {
                field_names.push(format!("_dict_{}", name).into());
                field_arrays.push(indexes::array_as_dict_column(arr.clone(), n)?);
            }

            let struct_chunk = StructArray::try_new(
                field_names.into(),
                field_arrays,
                n,
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array();

            chunks.push(struct_chunk);
            Ok(())
        };

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

        if !chunk_s.is_empty() {
            let start_idx = global_idx;
            total_rows += chunk_s.len();
            flush_chunk(&mut chunk_s, &mut chunk_p, &mut chunk_o, &mut chunk_g, start_idx)?;
        }

        for path in &run_paths {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir(&temp_dir);

        log::debug!("[GlobalSortBuilder] External merge sort serialization complete. Total rows: {}", total_rows);

        assemble_chunks(chunks)
    }
}
