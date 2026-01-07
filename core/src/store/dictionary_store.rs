use crate::error::{Result, VortexRdfError};
use crate::io::de;
use crate::store::VortexRdfStore;
use crate::common::utils;

use std::collections::HashMap;
use std::time::Instant;


use futures::{Stream, StreamExt};
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};

use vortex::compute::{and, compare, filter, Operator};
use vortex_array::arrays::{
    ChunkedArray, 
    ConstantArray, 
    ListArray, 
    PrimitiveArray, 
    StructArray, 
    VarBinViewArray,
};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};
use vortex_dtype::{DType, Nullability};
use vortex_mask::Mask;
use vortex_fsst::{fsst_compress, fsst_train_compressor};


pub struct DictionaryStore {
    pub vortex_array: ArrayRef,
    pub dictionary: Dictionary,
    pub quads: ArrayRef,
}

impl DictionaryStore {
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        let dictionary = Dictionary::from_vortex_array(&vortex_array)?;
        let quads = utils::extract_vortex_struct_field(
            &vortex_array.to_struct(),
            1,
            "quads"
        )?;
        Ok(Self {
            vortex_array,
            dictionary,
            quads,
        })
    }

    pub fn empty() -> Self {
        let dictionary = Dictionary::new();
        let dict_arr = VarBinViewArray::from_iter_str::<String, _>(vec![]).into_array();
        
        let s = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let p = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let o = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let g = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        
        let quads = StructArray::from_fields(&[
            ("s", s),
            ("p", p),
            ("o", o),
            ("g", g)
        ])
        .unwrap()
        .into_array();

        let dict_offsets = PrimitiveArray::from_iter(vec![0i32, 0]).into_array();
        let dict_list = ListArray::try_new(
            dict_arr,
            dict_offsets,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();

        let quads_offsets = PrimitiveArray::from_iter(vec![0i32, 0]).into_array();
        let quads_list = ListArray::try_new(
            quads.clone(),
            quads_offsets.clone(),
            Validity::NonNullable
        )
        .unwrap()
        .into_array();

        let store_type = ConstantArray::new("dictionary", 1).into_array();

        let vortex_array = StructArray::try_new(
            ["dictionary", "quads", "store_type"].into(),
            vec![dict_list, quads_list, store_type],
            1,
            Validity::NonNullable
        )
        .unwrap()
        .into_array();

        Self {
            vortex_array,
            dictionary,
            quads,
        }
    }

    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        // VortexFile::open handles opening the file from path
        let vortex_array = de::load_vortex_file_ref(path.as_ref().to_path_buf()).await?;
        Self::new(vortex_array)
    }

    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = std::io::Cursor::new(bytes);
        let vortex_array = de::array_from_reader(cursor)?;
        Self::new(vortex_array)
    }

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        Ok(self.quads.clone())
    }

    /// Internal helper to find row indices matching a pattern
    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let quads_struct = self.quads.to_struct();
        let fields = quads_struct.fields();

        let mut mask: Option<ArrayRef> = None;

        let mut combine_mask = |new_mask: ArrayRef| -> Result<()> {
            if let Some(m) = mask.take() {
                mask = Some(and(&m, &new_mask).map_err(VortexRdfError::Vortex)?);
            } else {
                mask = Some(new_mask);
            }
            Ok(())
        };

        let patterns = [
            (subject.map(|s| s.to_string()), 0, "Subject"),
            (predicate.map(|p| p.to_string()), 1, "Predicate"),
            (object.map(|o| o.to_string()), 2, "Object"),
            (graph.map(|g| g.to_string()), 3, "Graph"),
        ];

        for (term_opt, col_idx, label) in patterns {
            if let Some(term_str) = term_opt {
                let start = Instant::now();
                if let Some(id) = self.dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();
                    let scalar = vortex_scalar::Scalar::from(id)
                        .cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;

                    // Use statistics to prune matching
                    let column_mask = self.compare_with_pruning(col, &scalar)?;
                    log::debug!("[DictionaryStore::find_mask] {} comparison took {:?}", label, start.elapsed());
                    combine_mask(column_mask)?;
                } else {
                    // ID not in dictionary means no possible match
                    return Ok(Some(ConstantArray::new(false, self.quads.len()).into_array()));
                }
            }
        }

        Ok(mask)
    }

    fn compare_with_pruning(&self, col: &ArrayRef, scalar: &vortex_scalar::Scalar) -> Result<ArrayRef> {
        compare(
            col,
            &ConstantArray::new(scalar.clone(), col.len()).into_array(),
            Operator::Eq,
        )
        .map_err(VortexRdfError::Vortex)
    }

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self {
            vortex_array: self.with_quads_array(quads.clone())?,
            dictionary: self.dictionary.clone(),
            quads,
        })
    }

    fn with_quads_array(&self, quads: ArrayRef) -> Result<ArrayRef> {
        let vortex_struct = self.vortex_array.to_struct();
        let dict_list_ref = vortex_struct.fields().get(0).unwrap();
        let dict_list = dict_list_ref.to_listview();
        let dict_elements = dict_list.elements();
        self.with_dict_and_quads(dict_elements.clone(), quads)
    }

    fn with_dict_and_quads(&self, dict: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let dict_offsets =
            PrimitiveArray::from_iter(vec![0i32, dict.len() as i32])
                .into_array();
        let dict_list = ListArray::try_new(
            dict,
            dict_offsets,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        self.with_dict_and_quads_array(dict_list, quads)
    }

    fn with_dict_and_quads_array(&self, dict_list: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let quads_offsets = PrimitiveArray::from_iter(vec![0i32, quads.len() as i32])
            .into_array();
        let quads_list = ListArray::try_new(
                quads,
                quads_offsets.clone(),
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array();

        let new_dict_struct = StructArray::try_new(
            ["dictionary", "quads"].into(),
            vec![dict_list, quads_list],
            quads_offsets.len() - 1,
            Validity::NonNullable
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        Ok(new_dict_struct)
    }
}

impl VortexRdfStore for DictionaryStore {
    async fn build_vortex_index(
        mut quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> Result<ArrayRef> {
        let start_dict = Instant::now();
        let mut dictionary = Dictionary::new();
        let mut s_ids = Vec::new();
        let mut p_ids = Vec::new();
        let mut o_ids = Vec::new();
        let mut g_ids = Vec::new();

        while let Some(quad_res) = quad_stream.next().await {
            let quad = quad_res?;
            s_ids.push(dictionary.get_or_insert(&quad.subject.to_string()));
            p_ids.push(dictionary.get_or_insert(&quad.predicate.to_string()));
            o_ids.push(dictionary.get_or_insert(&quad.object.to_string()));
            g_ids.push(dictionary.get_or_insert(&quad.graph_name.to_string()));
        }

        log::debug!("[DictionaryStore::build_vortex_index] Dictionary building took {:?}", start_dict.elapsed());
        
        // Encode quad IDs into a StructArray
        let start_encode = Instant::now();
        let s_arr = PrimitiveArray::from_iter(s_ids).into_array();
        let p_arr = PrimitiveArray::from_iter(p_ids).into_array();
        let o_arr = PrimitiveArray::from_iter(o_ids).into_array();
        let g_arr = PrimitiveArray::from_iter(g_ids).into_array();

        let quads_flat = StructArray::from_fields(&[
            ("s", s_arr),
            ("p", p_arr),
            ("o", o_arr),
            ("g", g_arr),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        log::debug!("[DictionaryStore::build_vortex_index] Vortex encoding for quads took {:?}", start_encode.elapsed());

        // 3. Compress the dictionary
        // Use FSST compression for the dictionary
        let start_fsst = Instant::now();
        let dict_arr = dictionary.fsst_encode()?;
        log::debug!("[DictionaryStore::build_vortex_index] FSST compression for Dictionary took {:?}", start_fsst.elapsed());

        // 4. Wrap everything into a top-level StructArray BUT using ListArrays for flexibility
        // Use the ListArray trick to put arrays of different lengths into a single StructArray
        let start_list = Instant::now();
        let dict_offsets =
            PrimitiveArray::from_iter(vec![0i32, dict_arr.len() as i32])
                .into_array();
        let dict_list = ListArray::try_new(
            dict_arr,
            dict_offsets,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        let quads_offsets = PrimitiveArray::from_iter(vec![0i32, quads_flat.len() as i32])
            .into_array();
        let quads_list = ListArray::try_new(
            quads_flat,
            quads_offsets.clone(),
            Validity::NonNullable
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        // Add store type metadata field
        let store_type = ConstantArray::new("dictionary", 1).into_array();

        let vortex_array = StructArray::try_new(
            ["dictionary", "quads", "store_type"].into(),
            vec![dict_list, quads_list, store_type],
            quads_offsets.len() - 1,
            Validity::NonNullable
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        log::debug!("[DictionaryStore::build_vortex_index] Top-level Vortex StructArray creation took {:?}", start_list.elapsed());

        Ok(vortex_array)
    }


    fn size(&self) -> usize {
        self.get_quads_array().map(|a| a.len()).unwrap_or(0)
    }

    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        let quads_start = Instant::now();
        let quads_struct = self.get_quads_array()?.to_struct();
        let fields = quads_struct.fields();

        let s_ids = fields.get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
            .clone()
            .to_primitive();
        let p_ids = fields.get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
            .clone()
            .to_primitive();
        let o_ids = fields.get(2)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
            .clone()
            .to_primitive();
        let g_ids = fields.get(3)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
            .clone()
            .to_primitive();
        log::debug!("[DictionaryStore::quads] Quads struct extraction took {:?}", quads_start.elapsed());

        let len = s_ids.len();

        let iter = (0..len).map(move |i| {
            let s_id = s_ids.as_slice::<u32>()[i];
            let p_id = match p_ids.ptype() {
                vortex_dtype::PType::U16 => p_ids.as_slice::<u16>()[i] as u32,
                vortex_dtype::PType::U32 => p_ids.as_slice::<u32>()[i],
                _ => {
                    // This shouldn't happen if we validated above, but let's handle it
                    // To avoid returning Result in every field access.
                    0 // Fallback
                }
            };
            let o_id = o_ids.as_slice::<u32>()[i];
            let g_id = g_ids.as_slice::<u32>()[i];

            let s_term = self.dictionary.get_term(s_id)
                .ok_or_else(|| VortexRdfError::Deserialization(format!("S ID {} not in dictionary", s_id)))?;
            let p_term = self.dictionary.get_term(p_id)
                .ok_or_else(|| VortexRdfError::Deserialization(format!("P ID {} not in dictionary", p_id)))?;
            let o_term = self.dictionary.get_term(o_id)
                .ok_or_else(|| VortexRdfError::Deserialization(format!("O ID {} not in dictionary", o_id)))?;
            let g_name = self.dictionary.get_graph_name(g_id)
                .ok_or_else(|| VortexRdfError::Deserialization(format!("G ID {} not in dictionary", g_id)))?;

            let subject = match s_term {
                Term::NamedNode(n) => Subject::NamedNode(n),
                Term::BlankNode(b) => Subject::BlankNode(b),
                _ => return Err(VortexRdfError::Deserialization("Invalid subject type".to_string())),
            };

            let predicate = match p_term {
                Term::NamedNode(n) => n,
                _ => return Err(VortexRdfError::Deserialization("Invalid predicate type".to_string())),
            };

            Ok(Quad::new(subject, predicate, o_term, g_name))
        });

        Ok(Box::new(futures::stream::iter(iter)))
    }

    async fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let start = Instant::now();
        // TODO: check if can use vortex-scan for better performance on large datasets.
        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
            let mask_start = Instant::now();
            let quads_array_ref = self.get_quads_array()?;
            let canonical = m.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Mask must be boolean".to_string(),
                    ))
                }
            };
            log::debug!("[DictionaryStore::match_pattern] Mask creation took {:?}", mask_start.elapsed());
            let filter_start = Instant::now();
            let filtered_quads = filter(&quads_array_ref, &canonical_mask)
                .map_err(VortexRdfError::Vortex)?;
            log::debug!("[DictionaryStore::match_pattern] Filtering compute operation took {:?}", filter_start.elapsed());
            let _self = self.with_quads(filtered_quads);
            log::debug!("[DictionaryStore::match_pattern] Pattern matching took overall {:?}", start.elapsed());
            _self
        } else {
            Ok(Self {
                vortex_array: self.vortex_array.clone(),
                dictionary: self.dictionary.clone(),
                quads: self.quads.clone(),
            })
        }
    }

    async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let mut new_dict = self.dictionary.clone();

        let s_id = new_dict.get_or_insert(&quad.subject.to_string());
        let p_id = new_dict.get_or_insert(&quad.predicate.to_string());
        let o_id = new_dict.get_or_insert(&quad.object.to_string());
        let g_id = new_dict.get_or_insert(&quad.graph_name.to_string());

        // Encode quad IDs into a StructArray
        let s_arr = PrimitiveArray::from_iter(vec![s_id]).into_array();
        let p_arr = PrimitiveArray::from_iter(vec![p_id]).into_array();
        let o_arr = PrimitiveArray::from_iter(vec![o_id]).into_array();
        let g_arr = PrimitiveArray::from_iter(vec![g_id]).into_array();

        let new_row = StructArray::from_fields(&[
            ("s", s_arr),
            ("p", p_arr),
            ("o", o_arr),
            ("g", g_arr)
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        let old_quads = self.get_quads_array()?;

        // Use vortex ChunkedArray for efficient zero-copy concatenation
        let combined_quads = ChunkedArray::try_new(
            vec![old_quads, new_row],
            self.get_quads_array()?.dtype().clone(),
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        // Re-encode dictionary
        let dict_arr = new_dict.fsst_encode()?;

        let combined_quads = combined_quads;
        Ok(Self {
            vortex_array: self.with_dict_and_quads(dict_arr, combined_quads.clone())?,
            dictionary: new_dict,
            quads: combined_quads,
        })
    }

    async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        let mask = self.find_mask(
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )?;

        if let Some(m) = mask {
            // We want rows where mask is FALSE
            let inverse_mask_array = compare(
                &m,
                &ConstantArray::new(true, m.len()).into_array(),
                Operator::NotEq,
            )
            .map_err(VortexRdfError::Vortex)?;

            let canonical = inverse_mask_array.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Inverse mask must be boolean".to_string(),
                    ))
                }
            };

            let quads_array_ref = self.get_quads_array()?;
            let filtered_quads = filter(&quads_array_ref, &canonical_mask)
            .map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                vortex_array: self.vortex_array.clone(),
                dictionary: self.dictionary.clone(),
                quads: self.quads.clone(),
            })
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Dictionary {
    pub terms: Vec<String>,
    pub term_to_id: HashMap<String, u32>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_vortex_array(vortex_array: &ArrayRef) -> Result<Self> {
        let start = Instant::now();
        let vortex_struct = vortex_array.to_struct();
        log::debug!("[Dictionary::from_vortex_array] Build vortex struct took {:?}", start.elapsed());
        
        let dict_array_ref = utils::extract_vortex_struct_field(&vortex_struct, 0, "dictionary")?;

        let start_vortex_extraction = Instant::now();
        let dict_varbin = dict_array_ref.to_varbinview();
        log::debug!("[Dictionary::from_vortex_array] Vortex extraction took {:?}", start_vortex_extraction.elapsed());

        let loop_start = Instant::now();
        let mut dictionary = Dictionary::new();
        for i in 0..dict_varbin.len() {
            let bytes = dict_varbin.bytes_at(i);
            let s = String::from_utf8_lossy(&bytes).into_owned();
            dictionary.terms.push(s.clone());
            dictionary.term_to_id.insert(s.clone(), i as u32);
        }
        log::debug!("[Dictionary::from_vortex_array] HashMap build took {:?}", loop_start.elapsed());
        
        Ok(dictionary)
    }

    pub fn get_or_insert(&mut self, term_str: &str) -> u32 {
        if let Some(&id) = self.term_to_id.get(term_str) {
            id
        } else {
            let id = self.terms.len() as u32;
            let term_string = term_str.to_string();
            self.terms.push(term_string.clone());
            self.term_to_id.insert(term_string, id);
            id
        }
    }

    pub fn get_id(&self, term_str: &str) -> Option<u32> {
        self.term_to_id.get(term_str).copied()
    }

    pub fn get_term(&self, id: u32) -> Option<Term> {
        let s = self.terms.get(id as usize)?;
        utils::get_as_term(&s)
    }

    pub fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let s = self.terms.get(id as usize)?;
        if s.is_empty() || s == "[]" {
            Some(GraphName::DefaultGraph)
        } else {
            match utils::get_as_term(&s) {
                Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
                Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
                _ => Some(GraphName::DefaultGraph), // Fallback
            }
        }
    }

    pub fn fsst_encode(&self) -> Result<ArrayRef> {
        let dict_raw = VarBinViewArray::from_iter(
            self.terms.iter().map(|s: &String| Some(s.as_str())),
            DType::Utf8(Nullability::NonNullable),
        );

        // Apply FSST compression to the dictionary table
        if dict_raw.len() > 0 {
            let compressor = fsst_train_compressor(&dict_raw);
            Ok(fsst_compress(dict_raw, &compressor).into_array())
        } else {
            Ok(dict_raw.into_array())
        }
    }
}

