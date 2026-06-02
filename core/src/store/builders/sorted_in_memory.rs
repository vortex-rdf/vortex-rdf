use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use super::{VortexArrayBuilder, EncodedQuad};

use oxrdf::Quad;
use web_time::Instant;
use std::sync::Arc;
use futures::{Stream, StreamExt};

use vortex_array::{ArrayRef, IntoArray};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::validity::Validity;


/// A fully in-memory, globally sorted Vortex RDF Array Builder.
///
/// This builder eager-loads all quads from the stream into RAM and is optimized for datasets
/// that fit comfortably within system memory.
///
/// ### Serialization Phases:
/// 1. **Eager Ingestion**: Reads all quads from the stream directly into a flat in-memory vector.
/// 2. **Single Bulk Dictionary Insertion**: Collects all RDF terms and encodes them in a single,
///    highly optimized bulk call to `get_or_insert_bulk`. This completely avoids re-allocating
///    and re-compressing underlying Vortex structures.
/// 3. **Global In-Memory Sort**: Sorts all encoded quads globally in memory according to the canonical order.
/// 4. **Global Secondary Indexing**: Constructs flat, global sorted permutation indices (`_idx_o_*`, `_idx_p_*`)
///    for the entire dataset at once, avoiding any chunking overhead or chunk assembly stages.
/// 5. **Single-Dict StructArray Assembly**: Builds a single flat, unified `StructArray` with the specialized
///    dictionary columns attached directly at the root level.
pub struct SortedInMemoryBuilder;

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let start = Instant::now();
        let mut dictionary = Dict::new();
        
        // ── Phase 1: Ingest all quads into memory ──
        let mut quads = Vec::new();
        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quads.push(res?);
        }
        log::debug!("[SortedInMemoryBuilder] Read {} quads", quads.len());

        // ── Phase 2: Single bulk dictionary insertion ──
        let dict_start = Instant::now();
        let mut terms = Vec::with_capacity(quads.len() * 4);
        for quad in &quads {
            terms.push(quad.subject.to_string());
            terms.push(quad.predicate.to_string());
            terms.push(quad.object.to_string());
            terms.push(quad.graph_name.to_string());
        }
        let term_refs: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
        let ids = dictionary.get_or_insert_bulk(&term_refs);

        let mut encoded = Vec::with_capacity(quads.len());
        for i in 0..quads.len() {
            encoded.push(EncodedQuad {
                s: ids[i * 4],
                p: ids[i * 4 + 1],
                o: ids[i * 4 + 2],
                g: ids[i * 4 + 3],
            });
        }
        log::debug!("[SortedInMemoryBuilder] Built dictionary in {:?}", dict_start.elapsed());

        // ── Phase 3: Global in-memory sort ──
        let sort_start = Instant::now();
        encoded.sort_unstable();
        log::debug!("[SortedInMemoryBuilder] Sorted quads in {:?}", sort_start.elapsed());

        // Serialize the completed RdfDictionary to Vortex.
        let dict_vortex_start = Instant::now();
        let dict_fields = dictionary.to_vortex_array()?;
        log::debug!("[SortedInMemoryBuilder] Serialized dictionary to Vortex in {:?}", dict_vortex_start.elapsed());

        // ── Phase 4: Construct global arrays and sorted secondary permutation indexes ──
        let build_array_start = Instant::now();
        let n = encoded.len();
        let mut s_ids = Vec::with_capacity(n);
        let mut p_ids = Vec::with_capacity(n);
        let mut o_ids = Vec::with_capacity(n);
        let mut g_ids = Vec::with_capacity(n);

        for quad in &encoded {
            s_ids.push(quad.s);
            p_ids.push(quad.p);
            o_ids.push(quad.o);
            g_ids.push(quad.g);
        }

        // Build global secondary sorted Object index.
        let mut o_index: Vec<(u32, u32)> = o_ids.iter().copied()
            .enumerate()
            .map(|(idx, o_id)| (o_id, idx as u32))
            .collect();
        o_index.sort_unstable_by_key(|pair| pair.0);
        let idx_o_val: Vec<u32> = o_index.iter().map(|pair| pair.0).collect();
        let idx_o_rid: Vec<u32> = o_index.iter().map(|pair| pair.1).collect();

        // Build global secondary sorted Predicate index.
        let mut p_index: Vec<(u32, u32)> = p_ids.iter().copied()
            .enumerate()
            .map(|(idx, p_id)| (p_id, idx as u32))
            .collect();
        p_index.sort_unstable_by_key(|pair| pair.0);
        let idx_p_val: Vec<u32> = p_index.iter().map(|pair| pair.0).collect();
        let idx_p_rid: Vec<u32> = p_index.iter().map(|pair| pair.1).collect();

        let mut field_names: Vec<Arc<str>> = vec![
            "s".into(), "p".into(), "o".into(), "g".into(),
            "_idx_o_val".into(), "_idx_o_rid".into(),
            "_idx_p_val".into(), "_idx_p_rid".into(),
        ];

        let mut field_arrays: Vec<ArrayRef> = vec![
            PrimitiveArray::from_iter(s_ids).into_array(),
            PrimitiveArray::from_iter(p_ids).into_array(),
            PrimitiveArray::from_iter(o_ids).into_array(),
            PrimitiveArray::from_iter(g_ids).into_array(),
            PrimitiveArray::from_iter(idx_o_val).into_array(),
            PrimitiveArray::from_iter(idx_o_rid).into_array(),
            PrimitiveArray::from_iter(idx_p_val).into_array(),
            PrimitiveArray::from_iter(idx_p_rid).into_array(),
        ];

        // ── Phase 5: Assembly of flat unified StructArray ──
        // Attach store type identifier and dictionary arrays exactly once at the root level.
        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), n).into_array());

        for (name, arr) in &dict_fields {
            field_names.push(format!("_dict_{}", name).into());
            field_arrays.push(indexes::array_as_dict_column(arr.clone(), n)?);
        }

        let struct_array = StructArray::try_new(
            field_names.into(),
            field_arrays,
            n,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        log::debug!("[SortedInMemoryBuilder] Constructed StructArray and global indexes in {:?}", build_array_start.elapsed());
        log::debug!("[SortedInMemoryBuilder] Completed serialization of {} quads in {:?}", n, start.elapsed());

        Ok(struct_array)
    }
}
