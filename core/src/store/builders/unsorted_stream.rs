use crate::io::VORTEX_SESSION;
use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use super::{VortexArrayBuilder, EncodedQuad, assemble_chunks};

use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use web_time::{SystemTime, UNIX_EPOCH, Instant};
use futures::{Stream, StreamExt};
use oxrdf::Quad;

use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::validity::Validity;

/// An out-of-core, memory-efficient unsorted stream Vortex RDF Array Builder.
///
/// This builder is designed to process extremely large datasets that exceed available system memory
/// without sorting the input quads.
///
/// ### Serialization Phases:
/// 1. **Ingestion & Disk Flush**:
///    Reads quads from the stream in memory-bounded batches (e.g., 500k quads).
///    Maps terms to unique numeric IDs using zero-copy bulk ingestion via `get_or_insert_bulk`.
///    Writes the unsorted encoded quads directly to a temporary file on disk.
/// 2. **Thin Chunking**:
///    Reads the quads back from the temporary file and groups them into chunks.
///    Each chunk builds its struct array without any sorting or secondary indexes.
/// 3. **De-duplicated Root-Level Dictionary**:
///    Assembles the thin chunks into a single unified `ChunkedArray`.
///    Appends the specialized dictionary columns exactly **once** at the root level of the final `StructArray`
///    to eliminate redundant dictionary copies and minimize file size.
pub struct UnsortedStreamBuilder;

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for UnsortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let start = Instant::now();
        let mut dictionary = Dict::new();
        
        // Generate a unique temporary directory to store the unsorted quads file.
        let id = uuid::Uuid::new_v4();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = std::env::current_dir()
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
            .join("target")
            .join(format!("tmp_vortex_unsorted_stream_{}_{}", now, id));
        std::fs::create_dir_all(&temp_dir)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

        let temp_file_path = temp_dir.join("quads.bin");
        let file = File::create(&temp_file_path)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        let mut writer = BufWriter::new(file);

        let batch_size = 500_000;
        let mut quad_batch = Vec::with_capacity(batch_size);
        let mut total_ingested = 0;

        // Helper closure to process, dictionary-encode, and write a batch of quads.
        let process_batch = |batch: &mut Vec<Quad>, dict: &mut Dict, writer: &mut BufWriter<File>, total: &mut usize| -> Result<()> {
            if batch.is_empty() {
                return Ok(());
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
            log::debug!("[UnsortedStreamBuilder] Dictionary encoding of {} terms completed in {:?}", term_refs.len(), dict_duration);

            for i in 0..batch.len() {
                let eq = EncodedQuad {
                    s: ids[i * 4],
                    p: ids[i * 4 + 1],
                    o: ids[i * 4 + 2],
                    g: ids[i * 4 + 3],
                };
                bincode::serialize_into(&mut *writer, &eq)
                    .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
            }
            batch.clear();
            Ok(())
        };

        // ── Phase 1: Ingest, dictionary-encode, and write to disk ──
        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quad_batch.push(res?);

            if quad_batch.len() >= batch_size {
                process_batch(&mut quad_batch, &mut dictionary, &mut writer, &mut total_ingested)?;
            }
        }
        process_batch(&mut quad_batch, &mut dictionary, &mut writer, &mut total_ingested)?;
        writer.flush().map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        drop(writer);

        log::debug!("[UnsortedStreamBuilder] Ingested and dictionary-encoded {} quads", total_ingested);

        // Serialize the completed RdfDictionary to Vortex.
        let dict_vortex_start = Instant::now();
        let dict_fields = dictionary.to_vortex_array()?;
        log::debug!("[UnsortedStreamBuilder] Serialized dictionary to Vortex in {:?}", dict_vortex_start.elapsed());

        // ── Phase 2: Thin chunking ──
        let file = File::open(&temp_file_path)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        let mut reader = BufReader::new(file);

        let mut chunks = Vec::new();
        let chunk_size = 500_000;
        let mut total_rows = 0;

        loop {
            let mut chunk_s = Vec::with_capacity(chunk_size);
            let mut chunk_p = Vec::with_capacity(chunk_size);
            let mut chunk_o = Vec::with_capacity(chunk_size);
            let mut chunk_g = Vec::with_capacity(chunk_size);

            for _ in 0..chunk_size {
                match bincode::deserialize_from::<_, EncodedQuad>(&mut reader) {
                    Ok(q) => {
                        chunk_s.push(q.s);
                        chunk_p.push(q.p);
                        chunk_o.push(q.o);
                        chunk_g.push(q.g);
                    }
                    Err(e) => {
                        if let bincode::ErrorKind::Io(ref io_err) = *e {
                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                                break;
                            }
                        }
                        return Err(VortexRdfError::Deserialization(e.to_string()));
                    }
                }
            }

            let n = chunk_s.len();
            if n == 0 {
                break;
            }
            total_rows += n;

            let field_names: Vec<Arc<str>> = vec![
                "s".into(), "p".into(), "o".into(), "g".into(),
            ];

            let field_arrays: Vec<ArrayRef> = vec![
                PrimitiveArray::from_iter(chunk_s).into_array(),
                PrimitiveArray::from_iter(chunk_p).into_array(),
                PrimitiveArray::from_iter(chunk_o).into_array(),
                PrimitiveArray::from_iter(chunk_g).into_array(),
            ];

            let struct_chunk = StructArray::try_new(
                field_names.into(),
                field_arrays,
                n,
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array();

            log::debug!("[UnsortedStreamBuilder] Flushed thin chunk {} of size {}", chunks.len(), n);
            chunks.push(struct_chunk);
        }

        // Clean up temporary files.
        let _ = std::fs::remove_file(&temp_file_path);
        let _ = std::fs::remove_dir(&temp_dir);

        // ── Phase 3: Root-level dictionary construction ──
        let assembled = assemble_chunks(chunks)?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let assembled_struct = assembled.clone().execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let mut field_names: Vec<Arc<str>> = vec![
            "s".into(), "p".into(), "o".into(), "g".into(),
        ];

        let mut field_arrays: Vec<ArrayRef> = vec![
            assembled_struct.unmasked_field_by_name("s").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("p").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("o").map_err(VortexRdfError::Vortex)?.clone(),
            assembled_struct.unmasked_field_by_name("g").map_err(VortexRdfError::Vortex)?.clone(),
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

        log::debug!("[UnsortedStreamBuilder] Stream serialization complete. Total rows: {}", total_rows);
        log::debug!("[UnsortedStreamBuilder] Completed serialization of {} quads in {:?}", total_rows, start.elapsed());

        Ok(final_struct)
    }
}
