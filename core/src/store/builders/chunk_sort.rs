use crate::io::VORTEX_SESSION;
use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use super::{VortexArrayBuilder, EncodedQuad, assemble_chunks};

use std::sync::Arc;
use web_time::Instant;
use futures::{Stream, StreamExt};
use oxrdf::Quad;

use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::validity::Validity;

/// A memory-efficient builder strategy that sorts quads locally within individual chunks.
///
/// This strategy is highly effective for large datasets:
/// * Ingests and encodes terms using memory-bounded batching (e.g., 500k batches) via `get_or_insert_bulk`.
/// * Partitions data into independent chunks (e.g., 1M quads).
/// * Each chunk is sorted locally in memory, and local secondary permutation indices (`_idx_o_*`, `_idx_p_*`)
///   are constructed relative to that chunk.
/// * In order to avoid repeating/duplicating the serialized dictionary in each chunk, the builder keeps
///   individual chunks thin, merges them via `assemble_chunks`, and attaches the dictionary columns
///   exactly **once** at the root level of the final `StructArray`.
pub struct ChunkSortBuilder;

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for ChunkSortBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let start = Instant::now();
        let mut dictionary = Dict::new();
        let chunk_size = 500_000;
        let mut buffer = Vec::with_capacity(chunk_size);
        
        let batch_size = 500_000;
        let mut quad_batch = Vec::with_capacity(batch_size);

        // Helper closure to process and dictionary-encode a batch of quads.
        let process_batch = |batch: &mut Vec<Quad>, dict: &mut Dict, buf: &mut Vec<EncodedQuad>| {
            if batch.is_empty() {
                return;
            }
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
            log::debug!("[ChunkSortBuilder] Dictionary encoding of {} terms completed in {:?}", term_refs.len(), dict_duration);

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

        // ── Ingest and dictionary-encode quads in bounded batches ──
        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quad_batch.push(res?);

            if quad_batch.len() >= batch_size {
                process_batch(&mut quad_batch, &mut dictionary, &mut buffer);
            }
        }

        // Flush remaining elements in the batch.
        process_batch(&mut quad_batch, &mut dictionary, &mut buffer);

        log::debug!("[ChunkSortBuilder] Ingested and dictionary-encoded {} quads", buffer.len());

        // Serialize the completed RdfDictionary to Vortex.
        let dict_vortex_start = Instant::now();
        let dict_fields = dictionary.to_vortex_array()?;
        log::debug!("[ChunkSortBuilder] Serialized dictionary to Vortex in {:?}", dict_vortex_start.elapsed());

        let mut chunks = Vec::new();

        // ── Process and sort quads locally inside each chunk slice ──
        let build_chunks_start = Instant::now();
        let mut start_idx = 0u32;
        let mut total_len = 0;
        for chunk_slice in buffer.chunks_mut(chunk_size) {
            let chunk_start = Instant::now();
            let n = chunk_slice.len();
            total_len += n;
            chunk_slice.sort_unstable();

            let mut s_ids = Vec::with_capacity(n);
            let mut p_ids = Vec::with_capacity(n);
            let mut o_ids = Vec::with_capacity(n);
            let mut g_ids = Vec::with_capacity(n);

            for quad in chunk_slice {
                s_ids.push(quad.s);
                p_ids.push(quad.p);
                o_ids.push(quad.o);
                g_ids.push(quad.g);
            }

            // Build local secondary Object index.
            let mut o_index: Vec<(u32, u32)> = o_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, o_id)| (o_id, start_idx + local_idx as u32))
                .collect();
            o_index.sort_unstable_by_key(|pair| pair.0);
            let idx_o_val: Vec<u32> = o_index.iter().map(|pair| pair.0).collect();
            let idx_o_rid: Vec<u32> = o_index.iter().map(|pair| pair.1).collect();

            // Build local secondary Predicate index.
            let mut p_index: Vec<(u32, u32)> = p_ids.iter().copied()
                .enumerate()
                .map(|(local_idx, p_id)| (p_id, start_idx + local_idx as u32))
                .collect();
            p_index.sort_unstable_by_key(|pair| pair.0);
            let idx_p_val: Vec<u32> = p_index.iter().map(|pair| pair.0).collect();
            let idx_p_rid: Vec<u32> = p_index.iter().map(|pair| pair.1).collect();

            let field_names: Vec<Arc<str>> = vec![
                "s".into(), "p".into(), "o".into(), "g".into(),
                "_idx_o_val".into(), "_idx_o_rid".into(),
                "_idx_p_val".into(), "_idx_p_rid".into(),
            ];

            let field_arrays: Vec<ArrayRef> = vec![
                PrimitiveArray::from_iter(s_ids).into_array(),
                PrimitiveArray::from_iter(p_ids).into_array(),
                PrimitiveArray::from_iter(o_ids).into_array(),
                PrimitiveArray::from_iter(g_ids).into_array(),
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

            log::debug!("[ChunkSortBuilder] Sorted, indexed, and flushed thin chunk {} of size {} starting at index {} in {:?}", chunks.len(), n, start_idx, chunk_start.elapsed());
            chunks.push(struct_chunk);
            start_idx += n as u32;
        }

        // Assemble the thin chunks.
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

        // ── Root-level dictionary construction ──
        // Attach store type identifier and dictionary arrays exactly once at the root level.
        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), total_len).into_array());

        for (name, arr) in &dict_fields {
            field_names.push(format!("_dict_{}", name).into());
            field_arrays.push(indexes::array_as_dict_column(arr.clone(), total_len)?);
        }

        let final_struct = StructArray::try_new(
            field_names.into(),
            field_arrays,
            total_len,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        log::debug!("[ChunkSortBuilder] Built and chunk-sorted {} chunks in {:?}", assembled_struct.len(), build_chunks_start.elapsed());
        log::debug!("[ChunkSortBuilder] Completed serialization of {} quads in {:?}", buffer.len(), start.elapsed());

        Ok(final_struct)
    }
}
