use crate::error::Result;
use crate::index::RdfDictionary;
use crate::common::utils;
use crate::store::builders::{VortexArrayBuilder, UnsortedInMemoryBuilder};
use crate::store::QuadsSource;

use std::sync::Arc;
use std::time::Instant;
use std::io::Cursor;
use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, NamedNode, Quad, NamedOrBlankNode, Term};

use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::arrays::{
    BoolArray, ChunkedArray, ConstantArray, PrimitiveArray, StructArray,
};
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::{ArrayRef, IntoArray, LEGACY_SESSION, VortexSessionExecute};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::expr::{Expression, root, select, and, eq, get_item, lit};
use vortex_array::dtype::DType;
use vortex_array::stream::ArrayStreamExt;

/// Unified VortexRdfStore that works with any RdfDictionary implementation.
/// Implements zero-copy, highly compressed, and scan-optimized RDF storage.
///
/// ### StructArray Schema Layout
///
/// The store serialized structure is a flat N-row Vortex `StructArray` representing all RDF quads
/// along with (optional) sorted secondary indexes and dictionary data.
/// This unified layout allows taking advantage of the Vortex Scan API 
/// for push-down filtering and binary search routing leveraging Vortex zone maps metadata.
///
/// #### 1. Core Primary Columns
/// * **`s`**: Subject ID (`u32`). Maps to the dictionary values.
/// * **`p`**: Predicate ID (`u32`). Maps to the dictionary values.
/// * **`o`**: Object ID (`u32`). Maps to the dictionary values.
/// * **`g`**: Graph ID (`u32`). Maps to the dictionary values.
///
/// #### 2. (Optional) Sorted Secondary Indexes (Object & Predicate Routing)
/// To support fast pattern matching without full scans and redundant full-data replicas (permutations),
/// the store embeds secondary lookup indices sorted independently by field value:
///
/// * **Object Index**:
///   * **`_idx_o_val`**: Object ID values (`u32`) sorted in ascending order.
///   * **`_idx_o_rid`**: Global row IDs (`u32`) indicating the primary row index matching the object ID.
///
/// * **Predicate Index**:
///   * **`_idx_p_val`**: Predicate ID values (`u32`) sorted in ascending order.
///   * **`_idx_p_rid`**: Global row IDs (`u32`) indicating the primary row index matching the predicate ID.
///
/// When matching patterns like `(None, None, Some(Object), None)`, 
/// the store scans `_idx_o_val` to locate all matched row IDs from `_idx_o_rid`, 
/// taking only those specific primary quad rows.
///
/// #### 3. Dictionary & Metadata Projections
/// * **`store_type`**: Constant column holding the dictionary/index identifier (e.g., `simple-dictionary`, `chained_hash`).
/// * **`_dict_*`**: Specialized columns representing serialized internal structures of the dictionary.
pub struct VortexRdfStore<Dict: RdfDictionary> {
    pub dictionary: Dict,
    quads: QuadsSource,
}

// ─── impl VortexRdfStore ─────────────────────────────────────────────────────

impl<Dict: RdfDictionary> VortexRdfStore<Dict> {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Load from a flat N-row Vortex struct array.
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        let vortex_struct = vortex_array.clone().execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        // Load the specialized RdfDictionary from the serialization structure.
        let dictionary = Dict::from_vortex_array(&vortex_array)?;

        // Verify and retrieve the flat root-level fields: s, p, o, g.
        let get = |name: &str| -> Result<ArrayRef> {
            vortex_struct.unmasked_field_by_name(name)
                .map(|f| f.clone())
                .map_err(|_| VortexRdfError::Deserialization(
                    format!("Field '{}' not found in new-format struct", name)
                ))
        };
        
        let quads = StructArray::from_fields(&[
            ("s", get("s")?),
            ("p", get("p")?),
            ("o", get("o")?),
            ("g", get("g")?)
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(Self { dictionary, quads: QuadsSource::InMemory(quads) })
    }

    /// Create an empty in-memory store.
    pub fn empty() -> Self {
        let quads = StructArray::from_fields(&[
            ("s", PrimitiveArray::from_iter(Vec::<u32>::new()).into_array()),
            ("p", PrimitiveArray::from_iter(Vec::<u32>::new()).into_array()),
            ("o", PrimitiveArray::from_iter(Vec::<u32>::new()).into_array()),
            ("g", PrimitiveArray::from_iter(Vec::<u32>::new()).into_array()),
        ]).unwrap().into_array();

        Self {
            dictionary: Dict::new(),
            quads: QuadsSource::InMemory(quads),
        }
    }

    /// Load from a Vortex file lazily.
    /// Parses only the necessary dictionary columns eagerly and leaves
    /// [s,p,o,g] and [_idx_*] columns on disk for streaming scan projection.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        // Eagerly open the file session.
        let file = Arc::new(de::open_vortex_file(path).await?);
        
        let dict_start = Instant::now();
        // Dynamically project only the internal dictionary indexing tables.
        let mut proj_fields: Vec<Arc<str>> = Vec::new();
        for name in Dict::vortex_field_names() {
            proj_fields.push(format!("_dict_{}", name).into());
        }

        // Perform file projection scanning to eagerly retrieve the dictionary mappings.
        let dict_array = file
            .scan().map_err(VortexRdfError::Vortex)?
            .with_projection(select(proj_fields, root()))
            .into_array_stream().map_err(VortexRdfError::Vortex)?
            .read_all().await.map_err(VortexRdfError::Vortex)?;
        log::debug!("[VortexRdfStore::from_file] Dict load took {:?}", dict_start.elapsed());

        let dictionary = Dict::from_vortex_array(&dict_array)?;

        Ok(Self {
            dictionary,
            quads: QuadsSource::File { file, filter: None },
        })
    }

    /// Create a new store with the given quads (and associated dictionary).
    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self { 
            dictionary: self.dictionary.clone(), 
            quads: QuadsSource::InMemory(quads) 
        })
    }

    /// Load from IPC bytes (always in-memory, flat format).
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = Cursor::new(bytes);
        let vortex_array = de::array_from_ipc_reader(cursor)?;
        Self::new(vortex_array)
    }
}

impl<Dict: RdfDictionary> VortexRdfStore<Dict> {
    // ── serialization ─────────────────────────────────────────────────────────

    /// Build the new Vortex file format using the default UnsortedInMemoryBuilder by default.
    pub async fn build_vortex_array(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> Result<ArrayRef> {
        Self::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream).await
    }

    /// Build the new Vortex file format with a specified builder.
    pub async fn build_vortex_array_with_builder<B: VortexArrayBuilder<Dict>>(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> Result<ArrayRef> {
        B::build_vortex_array(Box::new(quad_stream)).await
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        match &self.quads {
            QuadsSource::InMemory(arr) => Ok(arr.clone()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter } => {
                let mut scan = file.scan().map_err(VortexRdfError::Vortex)?;
                scan = scan.with_projection(select(["s", "p", "o", "g"], root()));
                if let Some(expr) = filter {
                    scan = scan.with_filter(expr.clone());
                }
                let stream = scan.into_array_stream().map_err(VortexRdfError::Vortex)?;
                let array = futures::executor::block_on(async {
                    stream.read_all().await
                }).map_err(VortexRdfError::Vortex)?;
                Ok(array)
            }
        }
    }

    pub fn size(&self) -> usize {
        match &self.quads {
            QuadsSource::InMemory(arr) => arr.len(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter } => {
                if filter.is_some() {
                    0 // unknown until scanned
                } else {
                    file.row_count() as usize
                }
            }
        }
    }

    // ── quads streaming ───────────────────────────────────────────────────────

    /// Stream all quads out of the store.
    /// Dynamically matches in-memory arrays or lazy file-backed scanning strategies.
    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        match &self.quads {
            QuadsSource::InMemory(quads_arr) => {
                // Batch-decode: execute values once for the whole scan
                let values = self.dictionary.values_view()?;
                let chunk = quads_arr.clone();
                let quads = utils::decode_chunk(&chunk, &values);
                Ok(Box::new(stream::iter(quads)))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter } => {
                let values = Arc::new(self.dictionary.values_view()?);
                // Dynamically project only the core s, p, o, g quad fields.
                let mut scan = file.scan().map_err(VortexRdfError::Vortex)?
                    .with_projection(select(["s", "p", "o", "g"], root()));

                // Inject lazy scan filters if built by pattern matching.
                if let Some(expr) = filter {
                    scan = scan.with_filter(expr.clone());
                }

                let array_stream = scan.into_array_stream()
                    .map_err(VortexRdfError::Vortex)?;

                // Decode loaded pages lazily.
                let quad_stream = array_stream.flat_map(move |chunk_res| {
                    let values = values.clone();
                    let quads = match chunk_res {
                        Err(e) => vec![Err(VortexRdfError::Vortex(e))],
                        Ok(chunk) => utils::decode_chunk(&chunk, &values),
                    };
                    stream::iter(quads)
                });

                Ok(Box::new(quad_stream))
            }
        }
    }

    // ── pattern matching ──────────────────────────────────────────────────────

    /// Internal helper — build a boolean mask over the in-memory quads array.
    fn find_mask(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        let quads_struct = self.get_quads_array()?.execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let fields = quads_struct.unmasked_fields();

        let mut mask: Option<ArrayRef> = None;
        let mut combine = |new: ArrayRef| -> Result<()> {
            mask = Some(match mask.take() {
                Some(m) => m.binary(new, Operator::And).map_err(VortexRdfError::Vortex)?,
                None => new,
            });
            Ok(())
        };

        let patterns = [
            (subject.map(|s| s.to_string()), 0usize, "Subject"),
            (predicate.map(|p| p.to_string()), 1, "Predicate"),
            (object.map(|o| o.to_string()), 2, "Object"),
            (graph.map(|g| g.to_string()), 3, "Graph"),
        ];

        let total_len = self.size();
        for (term_opt, col_idx, label) in patterns {
            if let Some(term_str) = term_opt {
                let t = Instant::now();
                if let Some(id) = self.dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();
                    let scalar = Scalar::from(id).cast(col.dtype()).map_err(VortexRdfError::Vortex)?;
                    let rhs = ConstantArray::new(scalar, col.len()).into_array();
                    let col_mask = col.binary(rhs, Operator::Eq).map_err(VortexRdfError::Vortex)?;
                    log::debug!("[find_mask] {} took {:?}", label, t.elapsed());
                    combine(col_mask)?;
                } else {
                    // Quick-prune: term not present in vocabulary, match returns 0 rows.
                    return Ok(Some(ConstantArray::new(false, total_len).into_array()));
                }
            }
        }
        Ok(mask)
    }

    /// Build a Vortex filter `Expression` for the file-backed path.
    #[cfg(feature = "file-io")]
    fn build_file_filter(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<Expression>> {
        let patterns: [(&str, Option<String>); 4] = [
            ("s", subject.map(|s| s.to_string())),
            ("p", predicate.map(|p| p.to_string())),
            ("o", object.map(|o| o.to_string())),
            ("g", graph.map(|g| g.to_string())),
        ];

        let mut filter: Option<Expression> = None;

        for (field, term_opt) in patterns {
            if let Some(term_str) = term_opt {
                if let Some(id) = self.dictionary.get_id(&term_str) {
                    let expr = eq(get_item(field, root()), lit(id));
                    filter = Some(match filter.take() {
                        Some(f) => and(f, expr),
                        None => expr,
                    });
                } else {
                    // Term not in dictionary → guaranteed no results.
                    return Ok(Some(lit(false)));
                }
            }
        }
        Ok(filter)
    }

    /// Filters quads based on subject, predicate, object, and graph patterns.
    /// In memory, creates standard Boolean masks.
    /// For files, builds zero-cost lazy projection filter expressions.
    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let start = Instant::now();

        match &self.quads {
            // ── in-memory: existing boolean-mask approach ─────────────────
            QuadsSource::InMemory(_) => {
                let mask = self.find_mask(subject, predicate, object, graph)?;
                if let Some(m) = mask {
                    let quads_arr = self.get_quads_array()?;
                    let mut ctx = LEGACY_SESSION.create_execution_ctx();
                    let bool_arr = m.execute::<BoolArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                    let canonical = bool_arr.to_mask_fill_null_false(&mut ctx);
                    let filtered = quads_arr.filter(canonical).map_err(VortexRdfError::Vortex)?;
                    log::debug!("[match_pattern] In-memory filter took {:?}", start.elapsed());
                    self.with_quads(filtered)
                } else {
                    Ok(Self { dictionary: self.dictionary.clone(), quads: self.quads.clone() })
                }
            }

            // ── file-backed: build filter expression, return lazy store ───
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => {
                let has_indices = if let DType::Struct(fields, _) = file.dtype() {
                    fields.names().iter().any(|n| n.as_ref() == "_idx_o_val")
                } else {
                    false
                };

                if has_indices {
                    if subject.is_none() && object.is_some() {
                        let obj = object.unwrap();
                        if let Some(o_id) = self.dictionary.get_id(&obj.to_string()) {
                            let mut ctx = LEGACY_SESSION.create_execution_ctx();
                            
                            // 1. Scan sorted _idx_o_val to get matching _idx_o_rid
                            let scan = file.scan()
                                .map_err(VortexRdfError::Vortex)?
                                .with_projection(select(["_idx_o_rid"], root()))
                                .with_filter(eq(get_item("_idx_o_val", root()), lit(o_id)));
                            
                            let array = scan.into_array_stream().map_err(VortexRdfError::Vortex)?
                                .read_all().await.map_err(VortexRdfError::Vortex)?;
                            
                            let struct_arr = array.execute::<StructArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                            let rid_col = struct_arr.unmasked_field_by_name("_idx_o_rid")
                                .map_err(|_| VortexRdfError::Deserialization("Missing _idx_o_rid column".into()))?;
                            let rid_prim = rid_col.clone().execute::<PrimitiveArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                            let rids = rid_prim.as_slice::<u32>();

                            if rids.is_empty() {
                                return Ok(Self::empty());
                            }

                            // 2. Fetch matching quads
                            let chunk_size = 500_000;
                            let mut chunk_matches: std::collections::HashMap<usize, Vec<u32>> = std::collections::HashMap::new();
                            for &rid in rids {
                                let chunk_idx = (rid as usize) / chunk_size;
                                let local_rid = rid % (chunk_size as u32);
                                chunk_matches.entry(chunk_idx).or_default().push(local_rid);
                            }

                            let primary_scan = file.scan().map_err(VortexRdfError::Vortex)?
                                .with_projection(select(["s", "p", "o", "g"], root()));
                            let mut stream = primary_scan.into_array_stream().map_err(VortexRdfError::Vortex)?;

                            let mut chunks = Vec::new();
                            let mut chunk_idx = 0;
                            while let Some(chunk_res) = stream.next().await {
                                let chunk = chunk_res.map_err(VortexRdfError::Vortex)?;
                                if let Some(local_rids) = chunk_matches.get(&chunk_idx) {
                                    let struct_chunk = chunk.execute::<StructArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                                    let indices = PrimitiveArray::from_iter(local_rids.clone()).into_array();
                                    let taken = struct_chunk.take(indices).map_err(VortexRdfError::Vortex)?;
                                    chunks.push(taken);
                                }
                                chunk_idx += 1;
                            }

                            let quads_arr = if chunks.len() == 1 {
                                chunks.remove(0)
                            } else {
                                let dtype = chunks[0].dtype().clone();
                                ChunkedArray::try_new(chunks, dtype)
                                    .map_err(VortexRdfError::Vortex)?
                                    .into_array()
                            };

                            let taken_store = Self {
                                dictionary: self.dictionary.clone(),
                                quads: QuadsSource::InMemory(quads_arr),
                            };

                            log::debug!("[match_pattern] Object index routing completed in {:?}", start.elapsed());
                            return Box::pin(taken_store.match_pattern(subject, predicate, None, graph)).await;
                        } else {
                            return Ok(Self::empty());
                        }
                    } else if subject.is_none() && object.is_none() && predicate.is_some() {
                        let pred = predicate.unwrap();
                        if let Some(p_id) = self.dictionary.get_id(&pred.to_string()) {
                            let mut ctx = LEGACY_SESSION.create_execution_ctx();
                            
                            // 1. Scan sorted _idx_p_val to get matching _idx_p_rid
                            let scan = file.scan()
                                .map_err(VortexRdfError::Vortex)?
                                .with_projection(select(["_idx_p_rid"], root()))
                                .with_filter(eq(get_item("_idx_p_val", root()), lit(p_id)));
                            
                            let array = scan.into_array_stream().map_err(VortexRdfError::Vortex)?
                                .read_all().await.map_err(VortexRdfError::Vortex)?;
                            
                            let struct_arr = array.execute::<StructArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                            let rid_col = struct_arr.unmasked_field_by_name("_idx_p_rid")
                                .map_err(|_| VortexRdfError::Deserialization("Missing _idx_p_rid column".into()))?;
                            let rid_prim = rid_col.clone().execute::<PrimitiveArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                            let rids = rid_prim.as_slice::<u32>();

                            if rids.is_empty() {
                                return Ok(Self::empty());
                            }

                            // 2. Fetch matching quads
                            let chunk_size = 500_000;
                            let mut chunk_matches: std::collections::HashMap<usize, Vec<u32>> = std::collections::HashMap::new();
                            for &rid in rids {
                                let chunk_idx = (rid as usize) / chunk_size;
                                let local_rid = rid % (chunk_size as u32);
                                chunk_matches.entry(chunk_idx).or_default().push(local_rid);
                            }

                            let primary_scan = file.scan().map_err(VortexRdfError::Vortex)?
                                .with_projection(select(["s", "p", "o", "g"], root()));
                            let mut stream = primary_scan.into_array_stream().map_err(VortexRdfError::Vortex)?;

                            let mut chunks = Vec::new();
                            let mut chunk_idx = 0;
                            while let Some(chunk_res) = stream.next().await {
                                let chunk = chunk_res.map_err(VortexRdfError::Vortex)?;
                                if let Some(local_rids) = chunk_matches.get(&chunk_idx) {
                                    let struct_chunk = chunk.execute::<StructArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
                                    let indices = PrimitiveArray::from_iter(local_rids.clone()).into_array();
                                    let taken = struct_chunk.take(indices).map_err(VortexRdfError::Vortex)?;
                                    chunks.push(taken);
                                }
                                chunk_idx += 1;
                            }

                            let quads_arr = if chunks.len() == 1 {
                                chunks.remove(0)
                            } else {
                                let dtype = chunks[0].dtype().clone();
                                ChunkedArray::try_new(chunks, dtype)
                                    .map_err(VortexRdfError::Vortex)?
                                    .into_array()
                            };

                            let taken_store = Self {
                                dictionary: self.dictionary.clone(),
                                quads: QuadsSource::InMemory(quads_arr),
                            };

                            log::debug!("[match_pattern] Predicate index routing completed in {:?}", start.elapsed());
                            return Box::pin(taken_store.match_pattern(subject, None, object, graph)).await;
                        } else {
                            return Ok(Self::empty());
                        }
                    }
                }

                // Fallback to primary scan
                let filter = self.build_file_filter(subject, predicate, object, graph)?;
                log::debug!("[match_pattern] File filter expr built in {:?}", start.elapsed());
                Ok(Self {
                    dictionary: self.dictionary.clone(),
                    quads: QuadsSource::File {
                        file: file.clone(),
                        filter,
                    },
                })
            }
        }
    }

    // ── mutations (in-memory only) ────────────────────────────────────────────

    /// Append a single quad to the store.
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let old_quads = self.get_quads_array()?; // errors for file-backed
        let mut new_dict = self.dictionary.clone();

        let s_id = new_dict.get_or_insert(&quad.subject.to_string());
        let p_id = new_dict.get_or_insert(&quad.predicate.to_string());
        let o_id = new_dict.get_or_insert(&quad.object.to_string());
        let g_id = new_dict.get_or_insert(&quad.graph_name.to_string());

        let new_row = StructArray::from_fields(&[
            ("s", PrimitiveArray::from_iter(vec![s_id]).into_array()),
            ("p", PrimitiveArray::from_iter(vec![p_id]).into_array()),
            ("o", PrimitiveArray::from_iter(vec![o_id]).into_array()),
            ("g", PrimitiveArray::from_iter(vec![g_id]).into_array()),
        ]).map_err(VortexRdfError::Vortex)?.into_array();

        let combined = ChunkedArray::try_new(
            vec![old_quads.clone(), new_row],
            old_quads.dtype().clone(),
        ).map_err(VortexRdfError::Vortex)?.into_array();

        Ok(Self { dictionary: new_dict, quads: QuadsSource::InMemory(combined) })
    }

    /// Delete a matching quad from the store.
    pub async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        let mask = self.find_mask(
            Some(&quad.subject), Some(&quad.predicate),
            Some(&quad.object), Some(&quad.graph_name),
        )?;

        if let Some(m) = mask {
            let inverse = m.not().map_err(VortexRdfError::Vortex)?;
            let mut ctx = LEGACY_SESSION.create_execution_ctx();
            let bool_arr = inverse.execute::<BoolArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
            let canonical = bool_arr.to_mask_fill_null_false(&mut ctx);
            let quads_arr = self.get_quads_array()?;
            let filtered = quads_arr.filter(canonical).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered)
        } else {
            Ok(Self { dictionary: self.dictionary.clone(), quads: self.quads.clone() })
        }
    }
}

// Implement QuadStore trait for VortexRdfStore
impl<D: RdfDictionary> crate::store::QuadStore for VortexRdfStore<D> {
    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.quads()
    }
}
