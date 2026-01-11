use crate::error::{Result, VortexRdfError};
use crate::store::VortexRdfStore;
use crate::io::de;
use crate::common::utils;

use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use std::path::Path;
use std::time::Instant;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use futures::{Stream, StreamExt, stream};

use vortex::compute::{and, compare, filter, Operator};
use vortex_array::arrays::{ChunkedArray, ConstantArray, ListArray, PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};
use vortex_dtype::{DType, Nullability, PType};
use vortex_mask::Mask;
use vortex_scalar::Scalar;

#[derive(Clone)]
pub struct ChainedHashStore {
    // Chained Hash Index for RDF Terms
    pub buckets: ArrayRef, // PrimitiveArray<i32>
    pub next: ArrayRef,    // PrimitiveArray<i32>
    pub values: ArrayRef,  // VarBinViewArray

    // Data
    pub quads: ArrayRef,  // StructArray {s, p, o, g}
}

impl ChainedHashStore {
    const BUCKET_SIZE: usize = 1_000_003; // Prime number

    pub fn new(vortex_index: ArrayRef) -> Result<Self> {
        let start = Instant::now();
        let vortex_struct = vortex_index.to_struct();

        // Field 0 is the store_type
        let values = utils::extract_vortex_struct_field(&vortex_struct, 1, "dictionary")?;
        let quads = utils::extract_vortex_struct_field(&vortex_struct, 2, "quads")?;
        let buckets_arr = utils::extract_vortex_struct_field(&vortex_struct, 3, "buckets")?;
        let next = utils::extract_vortex_struct_field(&vortex_struct, 4, "next")?;

        let buckets = buckets_arr;
        log::debug!("[chained_hash_store::new] Vortex index destructuring took {:?}", start.elapsed());
        
        Ok(Self {
            buckets,
            next,
            values,
            quads,
        })
    }

    pub fn build_joint_array(
        values: ArrayRef,
        quads: ArrayRef,
        buckets: ArrayRef,
        next: ArrayRef,
    ) -> Result<ArrayRef> {
        let buckets_arr = buckets;
        
        let values_list = Self::wrap_in_list(values)?;
        let quads_list = Self::wrap_in_list(quads)?;
        let buckets_list = Self::wrap_in_list(buckets_arr)?;
        let next_list = Self::wrap_in_list(next)?;

        let store_type = ConstantArray::new("chained-hash", 1).into_array();

        let vortex_array = StructArray::from_fields(&[
            ("store_type", store_type),
            ("dictionary", values_list),
            ("quads", quads_list),
            ("buckets", buckets_list),
            ("next", next_list),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(vortex_array)
    }

    fn wrap_in_list(array: ArrayRef) -> Result<ArrayRef> {
        let offsets = PrimitiveArray::from_iter(vec![0i32, array.len() as i32]).into_array();
        let list = ListArray::try_new(array, offsets, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        Ok(list)
    }

    pub fn empty() -> Self {
        let buckets = PrimitiveArray::from_iter(vec![-1i32; Self::BUCKET_SIZE])
            .into_array();
        let next = PrimitiveArray::from_iter(Vec::<i32>::new())
            .into_array();
        let values = VarBinViewArray::from_iter_str::<String, _>(vec![])
            .into_array();
        
        let s = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let p = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let o = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let g = PrimitiveArray::from_iter(Vec::<u32>::new()).into_array();
        let quads = StructArray::from_fields(&[
            ("s", s),
            ("p", p),
            ("o", o),
            ("g", g),
        ])
        .unwrap()
        .into_array();

        Self {
            buckets,
            next,
            values,
            quads,
        }
    }

    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        // VortexFile::open handles opening the file from path
        let vortex_array = de::load_vortex_file_ref(path.as_ref().to_path_buf()).await?;
        Self::new(vortex_array)
    }

    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = std::io::Cursor::new(bytes);
        let vortex_array = de::array_from_reader(cursor)?;

        Self::new(vortex_array)
    }

    fn hash(s: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        (hasher.finish() as usize) % Self::BUCKET_SIZE
    }

    fn get_or_insert(
        &self,
        term: &str,
        buckets: &mut Vec<i32>,
        new_values: &mut Vec<String>,
        new_next: &mut Vec<i32>,
    ) -> Result<u32> {
        let h = Self::hash(term);
        
        // 1. Check if expected in Main Arrays
        // Traverse existing chain in Arrays
        let mut row = buckets[h];
        
        // We walk manually here to support hopping between Arrays and Vectors
        while row != -1 {
            let idx = row as usize;
            let base_len = self.values.len();

            if idx >= base_len {
                // In Delta
                let d_idx = idx - base_len;
                if new_values[d_idx] == term {
                    return Ok(row as u32);
                }
                row = new_next[d_idx];
            } else {
                // In Store
                let bytes = self.values.to_varbinview().bytes_at(idx);
                if bytes.as_slice() == term.as_bytes() {
                    return Ok(row as u32);
                }
                
                let next_prim = self.next.to_primitive();
                let data = next_prim.as_slice::<i32>();
                row = data[idx];
            }
        }

        // 2. Not found -> Insert
        let new_row_id = (self.values.len() + new_values.len()) as i32;
        new_values.push(term.to_string());
        
        // Point new item to OLD head
        let old_head = buckets[h];
        new_next.push(old_head);
        
        // Update head to NEW item
        buckets[h] = new_row_id;

        Ok(new_row_id as u32)
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        let h = Self::hash(term_str);
        let bucket_idx = h;
        let mut curr = self.buckets.to_primitive()
            .scalar_at(bucket_idx)
            .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
            .ok()?
            .as_primitive()
            .typed_value::<i32>()?;
            
        let values_varbin = self.values.to_varbinview();
        let next_prim = self.next.to_primitive();
        
        while curr != -1 {
            let idx = curr as usize;
            let bytes = values_varbin.bytes_at(idx);
            if bytes.as_ref() == term_str.as_bytes() {
                return Some(idx as u32);
            }
            curr = next_prim.scalar_at(idx)
                .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
                .ok()?
                .as_primitive()
                .typed_value::<i32>()?;
        }
        None
    }

    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let quads_struct = self.quads.to_struct();
        let fields = quads_struct.fields(); // s, p, o, g

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
                if let Some(id) = self.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();
                    let scalar = Scalar::from(id).cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;

                    let col_mask = self.compare_with_pruning(col, &scalar)?;
                    log::debug!("[ChainedHashStore::find_mask] {} comparison took {:?}", label, start.elapsed());
                    combine_mask(col_mask)?;
                } else {
                    return Ok(Some(ConstantArray::new(false, self.quads.len()).into_array()));
                }
            }
        }

        Ok(mask)
    }

    fn compare_with_pruning(
        &self, col: &ArrayRef, 
        scalar: &Scalar
    ) -> Result<ArrayRef> {
        compare(
                col,
                &ConstantArray::new(scalar.clone(), col.len()).into_array(),
                Operator::Eq,
            )
            .map_err(VortexRdfError::Vortex)
    }
}

impl VortexRdfStore for ChainedHashStore {
    async fn build_vortex_index(
        mut quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> Result<ArrayRef> {
        let start = Instant::now();
        // Chained hash index
        let mut buckets = vec![-1i32; Self::BUCKET_SIZE];
        let mut values_vec = Vec::new();
        let mut next_vec = Vec::new();

        // Quad term IDs
        let mut s_ids = Vec::new();
        let mut p_ids = Vec::new();
        let mut o_ids = Vec::new();
        let mut g_ids = Vec::new();
        
        let base_store = Self::empty();
        log::debug!("[ChainedHashStore::build_vortex_index] Initial vectors setup took {:?}", start.elapsed());
        
        let quad_start = Instant::now();
        while let Some(quad_res) = quad_stream.next().await {
            let quad = quad_res?;
            let s_id = base_store.get_or_insert(
                &quad.subject.to_string(),
                &mut buckets,
                &mut values_vec,
                &mut next_vec
            )?;
            let p_id = base_store.get_or_insert(
                &quad.predicate.to_string(),
                &mut buckets,
                &mut values_vec,
                &mut next_vec
            )?;
            let o_id = base_store.get_or_insert(
                &quad.object.to_string(),
                &mut buckets,
                &mut values_vec,
                &mut next_vec
            )?;
            let g_id = base_store.get_or_insert(
                &quad.graph_name.to_string(),
                &mut buckets,
                &mut values_vec,
                &mut next_vec
            )?;
            
            s_ids.push(s_id);
            p_ids.push(p_id);
            o_ids.push(o_id);
            g_ids.push(g_id);
        }
        log::debug!("[ChainedHashStore::build_vortex_index] Quad processing and index building took {:?}", quad_start.elapsed());

        let vortex_start = Instant::now();
        // Vortex arrays for chained hash index
        let values_arr = VarBinViewArray::from_iter_str::<String, _>(values_vec).into_array();
        let next_arr = PrimitiveArray::from_iter(next_vec).into_array();
        let buckets_arr = PrimitiveArray::from_iter(buckets).into_array();

        // Vortex arrays for quads
        let s_arr = PrimitiveArray::from_iter(s_ids).into_array();
        let p_arr = PrimitiveArray::from_iter(p_ids).into_array();
        let o_arr = PrimitiveArray::from_iter(o_ids).into_array();
        let g_arr = PrimitiveArray::from_iter(g_ids).into_array();
        let quads_array = StructArray::from_fields(&[
            ("s", s_arr),
            ("p", p_arr),
            ("o", o_arr),
            ("g", g_arr),
        ]).map_err(VortexRdfError::Vortex)?.into_array();

        let vortex_array = Self::build_joint_array(values_arr, quads_array, buckets_arr, next_arr)?;
        log::debug!("[ChainedHashStore::build_vortex_index] Vortex array building took {:?}", vortex_start.elapsed());
        
        Ok(vortex_array)
    }

    fn size(&self) -> usize {
        self.quads.len()
    }

    async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let mut buckets = self.buckets
        .to_primitive()
            .as_slice::<i32>()
            .to_vec();
        let mut new_values = Vec::new();
        let mut new_next = Vec::new();

        let s_id = self.get_or_insert(
            &quad.subject.to_string(),
            &mut buckets,
            &mut new_values,
            &mut new_next
        )?;
        let p_id = self.get_or_insert(
            &quad.predicate.to_string(),
            &mut buckets,
            &mut new_values,
            &mut new_next
        )?;
        let o_id = self.get_or_insert(
            &quad.object.to_string(),
            &mut buckets,
            &mut new_values,
            &mut new_next
        )?;
        let g_id = self.get_or_insert(
            &quad.graph_name.to_string(),
            &mut buckets,
            &mut new_values,
            &mut new_next
        )?;

        // Append Arrays
        let val_chunk = VarBinViewArray::from_iter(
            new_values.iter().map(Some), 
            DType::Utf8(Nullability::NonNullable)
        ).into_array();
        let next_chunk = PrimitiveArray::from_iter(new_next).into_array();

        // 1. Values
        let combined_values = if val_chunk.len() > 0 {
            ChunkedArray::try_new(
                vec![self.values.clone(), val_chunk], 
                self.values.dtype().clone()
            ).map_err(VortexRdfError::Vortex)?.into_array()
        } else {
            self.values.clone()
        };

        // 2. Next
        let combined_next = if next_chunk.len() > 0 {
            ChunkedArray::try_new(
                vec![self.next.clone(), next_chunk],
                self.next.dtype().clone()
            ).map_err(VortexRdfError::Vortex)?.into_array()
        } else {
            self.next.clone()
        };

        // 3. Quads
        let s_arr = PrimitiveArray::from_iter(vec![s_id]).into_array();
        let p_arr = PrimitiveArray::from_iter(vec![p_id]).into_array();
        let o_arr = PrimitiveArray::from_iter(vec![o_id]).into_array();
        let g_arr = PrimitiveArray::from_iter(vec![g_id]).into_array();
        
        let quad_chunk = StructArray::from_fields(&[
            ("s", s_arr),
            ("p", p_arr),
            ("o", o_arr),
            ("g", g_arr),
        ]).map_err(VortexRdfError::Vortex)?.into_array();

        let combined_quads = ChunkedArray::try_new(
            vec![self.quads.clone(), quad_chunk],
            self.quads.dtype().clone()
        ).map_err(VortexRdfError::Vortex)?.into_array();

        let buckets_arr = PrimitiveArray::from_iter(buckets).into_array();

        Ok(Self {
            buckets: buckets_arr,
            next: combined_next,
            values: combined_values,
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
            let inverse_mask_array = compare(
                &m,
                &ConstantArray::new(true, m.len()).into_array(),
                Operator::NotEq,
            )
            .map_err(VortexRdfError::Vortex)?;

            let canonical = inverse_mask_array.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => return Err(VortexRdfError::Deserialization("Inverse mask must be boolean".into())),
            };

            let filtered_quads = filter(&self.quads, &canonical_mask)
                .map_err(VortexRdfError::Vortex)?;

            Ok(Self {
                buckets: self.buckets.clone(),
                next: self.next.clone(),
                values: self.values.clone(),
                quads: filtered_quads,
            })
        } else {
            Ok(self.clone())
        }
    }

    async fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
         let mask = self.find_mask(subject, predicate, object, graph)?;
         
         if let Some(m) = mask {
            let canonical = m.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => return Err(VortexRdfError::Deserialization("Mask must be boolean".into())),
            };

            let filtered_quads = filter(&self.quads, &canonical_mask)
                .map_err(VortexRdfError::Vortex)?;
            
            Ok(Self {
                buckets: self.buckets.clone(),
                next: self.next.clone(),
                values: self.values.clone(),
                quads: filtered_quads,
            })
         } else {
             Ok(self.clone())
         }
    }

    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        let quads_start = Instant::now();
        let quads_struct = self.quads.to_struct();
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
        
        let values_varbin = self.values.to_varbinview();
        
        log::debug!("[ChainedHashStore::quads] Quads struct extraction took {:?}", quads_start.elapsed());

        let len = s_ids.len();

        let iter = (0..len).map(move |i| {
            let s_id = s_ids.as_slice::<u32>()[i];
            let p_id = p_ids.as_slice::<u32>()[i];
            let o_id = o_ids.as_slice::<u32>()[i];
            let g_id = g_ids.as_slice::<u32>()[i];

            // Helper to get term from view
            let get_term_from_view = |id: u32| -> Result<Term> {
                let bytes = values_varbin.bytes_at(id as usize);
                let s = String::from_utf8_lossy(bytes.as_ref()).to_string();
                utils::get_as_term(&s)
                    .ok_or_else(|| VortexRdfError::Deserialization(format!("ID {} invalid term", id)))
            };

            let get_graph_name_from_view = |id: u32| -> Result<GraphName> {
                 let bytes = values_varbin.bytes_at(id as usize);
                 let s = String::from_utf8_lossy(bytes.as_ref()).to_string();
                 if s.is_empty() || s == "[]" {
                     Ok(GraphName::DefaultGraph)
                 } else {
                     match utils::get_as_term(&s) {
                         Some(Term::NamedNode(n)) => Ok(GraphName::NamedNode(n)),
                         Some(Term::BlankNode(b)) => Ok(GraphName::BlankNode(b)),
                         _ => Ok(GraphName::DefaultGraph), 
                     }
                 }
            };

            let s_term = get_term_from_view(s_id)?;
            let p_term = get_term_from_view(p_id)?;
            let o_term = get_term_from_view(o_id)?;
            let g_name = get_graph_name_from_view(g_id)?;

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

        Ok(Box::new(stream::iter(iter)))
    }
}


