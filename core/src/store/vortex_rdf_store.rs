use crate::error::{Result, VortexRdfError};
use crate::io::de;
use crate::index::RdfDictionary;
use crate::common::{utils, indexes};

use std::sync::Arc;
use std::time::Instant;
use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, NamedNode, Quad, NamedOrBlankNode, Term};

use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::arrays::{
    BoolArray, ChunkedArray, ConstantArray, PrimitiveArray, StructArray, VarBinViewArray,
};
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, LEGACY_SESSION, VortexSessionExecute};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::expr::{Expression, root, select, and, eq, get_item, lit};
use vortex_file::VortexFile;

/// Lazily-decoded quad source — either fully in-memory (IPC / mutation path)
/// or file-backed (lazy scan path, loaded via `from_file`).
enum QuadsSource {
    /// All quads held in a Vortex StructArray in host memory.
    InMemory(ArrayRef),
    /// Quads remain on disk; scanned lazily on each `quads()` or `match_pattern()` call.
    #[cfg(feature = "file-io")]
    File {
        file: Arc<VortexFile>,
        /// Optional filter expression (built by `match_pattern`).
        filter: Option<Expression>,
    },
}

impl Clone for QuadsSource {
    fn clone(&self) -> Self {
        match self {
            QuadsSource::InMemory(arr) => QuadsSource::InMemory(arr.clone()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter } => QuadsSource::File {
                file: file.clone(),
                filter: filter.clone(),
            },
        }
    }
}

/// Unified VortexRdfStore that works with any RdfDictionary implementation.
/// Implements zero-copy, highly compressed, and scan-optimized RDF storage.
pub struct VortexRdfStore<Dict: RdfDictionary> {
    pub dictionary: Dict,
    quads: QuadsSource,
}

// ─── internal quad decoder ────────────────────────────

/// Decode a single chunk `ArrayRef` (a StructArray with fields s,p,o,g)
/// into `Quad`s using the pre-decoded `values` view.
fn decode_chunk(chunk: &ArrayRef, values: &VarBinViewArray) -> Vec<Result<Quad>> {
    // 1. Establish an execution context to resolve/evaluate the compressed Vortex arrays.
    let mut ctx = LEGACY_SESSION.create_execution_ctx();
    
    // 2. Evaluate and canonicalize the chunk array into a standard StructArray.
    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    // 3. Extract subject, predicate, object, and graph ID columns using flat primitive extractor.
    let s_ids = match utils::extract_flat_primitive_column(&struct_arr, 0) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let p_ids = match utils::extract_flat_primitive_column(&struct_arr, 1) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let o_ids = match utils::extract_flat_primitive_column(&struct_arr, 2) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let g_ids = match utils::extract_flat_primitive_column(&struct_arr, 3) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };

    // 4. Iterate over each row in the chunk to decode ID sequences into RDF Terms and Quads.
    (0..s_ids.len()).map(|i| {
        // Retrieve the u32 index for each field and cast to usize.
        let s_id = s_ids.as_slice::<u32>()[i] as usize;
        let p_id = p_ids.as_slice::<u32>()[i] as usize;
        let o_id = o_ids.as_slice::<u32>()[i] as usize;
        let g_id = g_ids.as_slice::<u32>()[i] as usize;

        // Perform zero-copy dictionary lookup to get raw string representation of each term.
        let s_b = values.bytes_at(s_id); let s_s = String::from_utf8_lossy(s_b.as_ref());
        let p_b = values.bytes_at(p_id); let p_s = String::from_utf8_lossy(p_b.as_ref());
        let o_b = values.bytes_at(o_id); let o_s = String::from_utf8_lossy(o_b.as_ref());
        let g_b = values.bytes_at(g_id); let g_s = String::from_utf8_lossy(g_b.as_ref());

        // Parse the serialized term strings back into structural RDF Term types.
        let s_term = utils::get_as_term(&s_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid subject ID {s_id}")))?;
        let p_term = utils::get_as_term(&p_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid predicate ID {p_id}")))?;
        let o_term = utils::get_as_term(&o_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid object ID {o_id}")))?;

        // Map the graph string to the appropriate structural GraphName.
        let g_name = if g_s.is_empty() || g_s == "[]" {
            GraphName::DefaultGraph
        } else {
            match utils::get_as_term(&g_s) {
                Some(Term::NamedNode(n)) => GraphName::NamedNode(n),
                Some(Term::BlankNode(b)) => GraphName::BlankNode(b),
                _ => GraphName::DefaultGraph,
            }
        };

        // Construct standard structural components, validating subject and predicate constraints.
        let subject = match s_term {
            Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
            Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b),
            _ => return Err(VortexRdfError::Deserialization("Invalid subject type".into())),
        };
        let predicate = match p_term {
            Term::NamedNode(n) => n,
            _ => return Err(VortexRdfError::Deserialization("Invalid predicate type".into())),
        };

        // Assemble and return the complete structural RDF Quad.
        Ok(Quad::new(subject, predicate, o_term, g_name))
    }).collect()
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
    /// [s,p,o,g] columns on disk for streaming scan projection.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        use vortex_array::stream::ArrayStreamExt;

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

    /// Load from IPC bytes (always in-memory, flat format).
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = std::io::Cursor::new(bytes);
        let vortex_array = de::array_from_ipc_reader(cursor)?;
        Self::new(vortex_array)
    }

    // ── serialization ─────────────────────────────────────────────────────────

    /// Build the new Vortex file format:
    /// ```text
    /// StructArray(len=N) {
    ///   s, p, o, g:    PrimitiveArray<u32>(N)   ← root-level, zone-map prunable
    ///   store_type:    ConstantArray<Utf8>(N)
    ///   _dict_values:  DictArray(codes=zeros(N), values=ListArray(1, VarBinaryArray))
    ///   _dict_*:       DictArray(codes=zeros(N), values=ListArray(1, *_arr))
    /// }
    /// ```
    pub async fn build_vortex_array(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> Result<ArrayRef> {
        let start_dict = Instant::now();
        let mut dictionary = Dict::new();

        // 1. Collect stream results.
        // TODO: check if it's better to process in chunks of quads and serialize each chunk separately.
        //       This would enable for parallel processing of chunks and reduce peak memory usage.
        // TODO: investigate ordering for indexed-structured storage + sub-indexes.
        let quads: Vec<Quad> = quad_stream
            .collect::<Vec<Result<Quad>>>().await
            .into_iter().collect::<Result<Vec<Quad>>>()?;
        let n = quads.len();
        log::debug!("[build_vortex_array] Collected {} quads in {:?}", n, start_dict.elapsed());

        // 2. Extract bulk term strings for dictionary insertion.
        let mut term_strings = Vec::with_capacity(n * 4);
        for q in &quads {
            term_strings.push(q.subject.to_string());
            term_strings.push(q.predicate.to_string());
            term_strings.push(q.object.to_string());
            term_strings.push(q.graph_name.to_string());
        }
        
        // 3. Populate dictionary mapping to obtain stable term IDs.
        let all_ids = dictionary.get_or_insert_bulk(
            &term_strings.iter().map(|s| s.as_str()).collect::<Vec<_>>()
        );

        let mut s_ids = Vec::with_capacity(n);
        let mut p_ids = Vec::with_capacity(n);
        let mut o_ids = Vec::with_capacity(n);
        let mut g_ids = Vec::with_capacity(n);
        for i in 0..n {
            s_ids.push(all_ids[i * 4]);
            p_ids.push(all_ids[i * 4 + 1]);
            o_ids.push(all_ids[i * 4 + 2]);
            g_ids.push(all_ids[i * 4 + 3]);
        }

        // 4. Construct the serializable forms of the dictionary tables.
        let dict_fields = dictionary.to_vortex_array()?;

        // 5. Build flat N-row StructArray fields.
        let mut field_names: Vec<Arc<str>> = vec![
            "s".into(), 
            "p".into(), 
            "o".into(), 
            "g".into()
        ];
        let mut field_arrays: Vec<ArrayRef> = vec![
            PrimitiveArray::from_iter(s_ids).into_array(),
            PrimitiveArray::from_iter(p_ids).into_array(),
            PrimitiveArray::from_iter(o_ids).into_array(),
            PrimitiveArray::from_iter(g_ids).into_array(),
        ];

        // store_type as ConstantArray(N)
        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), n).into_array());

        // Dict fields as DictionaryArray(codes=all-zeros, values=ListArray(1, dict_arr))
        // O(N) write, RLE compresses codes to ~16 bytes, dict stored exactly once.
        for (name, arr) in dict_fields {
            field_names.push(format!("_dict_{}", name).into());
            field_arrays.push(indexes::array_as_dict_column(arr, n)?);
        }

        let vortex_array = StructArray::try_new(
            field_names.into(),
            field_arrays,
            n,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        log::debug!("[build_vortex_array] Total build took {:?}", start_dict.elapsed());
        Ok(vortex_array)
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        match &self.quads {
            QuadsSource::InMemory(arr) => Ok(arr.clone()),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => Err(VortexRdfError::Deserialization(
                "get_quads_array not supported for file-backed store".into()
            )),
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

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self { dictionary: self.dictionary.clone(), quads: QuadsSource::InMemory(quads) })
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
                let quads = decode_chunk(&chunk, &values);
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
                        Ok(chunk) => decode_chunk(&chunk, &values),
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
