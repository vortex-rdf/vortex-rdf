use crate::error::{Result, VortexRdfError};
use crate::common::indexes;
use crate::index::RdfDictionary;
use super::{VortexArrayBuilder, EncodedQuad};

use std::sync::Arc;
use web_time::Instant;
use futures::{Stream, StreamExt};
use oxrdf::Quad;

use vortex_array::{ArrayRef, IntoArray};
use vortex_array::arrays::{PrimitiveArray, StructArray, ConstantArray};
use vortex_array::validity::Validity;

/// A fully in-memory, unsorted Vortex RDF Array Builder.
///
/// This is the simplest and fastest builder strategy:
/// * Eagerly reads all quads from the stream directly into a flat in-memory vector.
/// * Performs exactly one single highly optimized bulk insertion call to `get_or_insert_bulk` for RDF terms.
/// * Assembles a single flat, unified `StructArray` directly from the encoded quads without any sorting or indexing.
pub struct UnsortedInMemoryBuilder;

impl<Dict: RdfDictionary> VortexArrayBuilder<Dict> for UnsortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> Result<ArrayRef> {
        let mut dictionary = Dict::new();
        let start = Instant::now();
        
        // ── Phase 1: Ingest all quads into memory ──
        let mut quads = Vec::new();
        let mut pinned_stream = Box::pin(quad_stream);
        while let Some(res) = pinned_stream.next().await {
            quads.push(res?);
        }
        log::debug!("[UnsortedInMemoryBuilder] Read {} quads", quads.len());

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
        log::debug!("[UnsortedInMemoryBuilder] Built dictionary in {:?}", dict_start.elapsed());

        // Serialize the completed RdfDictionary to Vortex.
        let dict_vortex_start = Instant::now();
        let dict_fields = dictionary.to_vortex_array()?;
        log::debug!("[UnsortedInMemoryBuilder] Serialized dictionary to Vortex in {:?}", dict_vortex_start.elapsed());

        // ── Phase 3: Construct flat arrays and assemble StructArray ──
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

        let mut field_names: Vec<Arc<str>> = vec![
            "s".into(), "p".into(), "o".into(), "g".into(),
        ];

        let mut field_arrays: Vec<ArrayRef> = vec![
            PrimitiveArray::from_iter(s_ids).into_array(),
            PrimitiveArray::from_iter(p_ids).into_array(),
            PrimitiveArray::from_iter(o_ids).into_array(),
            PrimitiveArray::from_iter(g_ids).into_array(),
        ];

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

        log::debug!("[UnsortedInMemoryBuilder] Constructed StructArray in {:?}", build_array_start.elapsed());
        log::debug!("[UnsortedInMemoryBuilder] Completed serialization of {} quads in {:?}", n, start.elapsed());

        Ok(struct_array)
    }
}
