use crate::error::Result;
use crate::index::RdfDictionary;
use crate::common::{utils, indexes};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use oxrdf::{GraphName, Term};

use vortex::array::builders::{ArrayBuilder, VarBinViewBuilder, PrimitiveBuilder};

use vortex_array::ArrayRef;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray, StructArray};
use vortex_array::{IntoArray, ToCanonical};
use vortex_dtype::{DType, Nullability, PType};

/// Chained hash dictionary implementation
#[derive(Clone)]
pub struct ChainedHash {
    pub buckets: ArrayRef,  // PrimitiveArray<i32>
    pub next: ArrayRef,     // PrimitiveArray<i32>
    pub values: ArrayRef,   // VarBinViewArray
}

impl ChainedHash {
    const BUCKET_SIZE: usize = 1_000_003; // Prime number

    fn hash(s: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        (hasher.finish() as usize) % Self::BUCKET_SIZE
    }

    /// Get ID from ArrayRef-based representation (for lookups during queries)
    pub fn get_id_from_arrays(
        term_str: &str,
        buckets_arr: &ArrayRef,
        next_arr: &ArrayRef,
        values_arr: &ArrayRef,
    ) -> Option<u32> {
        let h = Self::hash(term_str);
        let bucket_idx = h;
        
        let mut curr = buckets_arr.to_primitive()
            .scalar_at(bucket_idx)
            .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
            .ok()?
            .as_primitive()
            .typed_value::<i32>()?;
            
        let values_varbin = values_arr.to_varbinview();
        let next_prim = next_arr.to_primitive();
        
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
}

impl RdfDictionary for ChainedHash {
    fn new() -> Self {
        Self {
            buckets: PrimitiveArray::from_iter(vec![-1i32; Self::BUCKET_SIZE]).into_array(),
            next: PrimitiveArray::from_iter(Vec::<i32>::new()).into_array(),
            values: VarBinViewArray::from_iter_str::<String, _>(vec![]).into_array(),
        }
    }

    fn from_vortex_array(dict_array_ref: &ArrayRef) -> Result<Self> {
        let start = Instant::now();
        
        // The dictionary might be wrapped in a ListArray, or it might be the Struct directly
        // Try to unwrap as ListArray first, if that fails, use it directly
        // The dictionary might be a Struct (new format), ListArray (old format), or direct array
        let id = dict_array_ref.encoding().id();
        log::debug!("[ChainedHash::from_vortex_array] Encoding ID: {}", id);

        let dict_struct = if id.as_ref() == "vortex.struct" {
            dict_array_ref.to_struct()
        } else if let Some(s) = dict_array_ref.as_any().downcast_ref::<StructArray>() {
             s.clone()
        } else if id.as_ref() == "vortex.listview" {
            // It's a ListArray, unwrap it
            let dict_list = dict_array_ref.to_listview();
            // children()[1] is the values for ListArray
            let dict_struct_array = dict_list.children()[1].clone();
            dict_struct_array.to_struct()
        } else {
            // Use directly (though for ChainedHash it should be a struct)
            dict_array_ref.to_struct()
        };
        
        // Extract the three fields from the dictionary struct
        // These might be ListArrays or the actual data arrays directly
        let values_field = utils::extract_vortex_struct_field(&dict_struct, "values")?;
        let buckets_field = utils::extract_vortex_struct_field(&dict_struct, "buckets")?;
        let next_field = utils::extract_vortex_struct_field(&dict_struct, "next")?;
        
        // Unwrap each field if it's a ListArray, otherwise use directly
        let values = if values_field.encoding().id().as_ref() == "vortex.listview" {
            values_field.to_listview().children()[1].clone()
        } else {
            values_field
        };
        
        let buckets = if buckets_field.encoding().id().as_ref() == "vortex.listview" {
            buckets_field.to_listview().children()[1].clone()
        } else {
            buckets_field
        };
        
        let next = if next_field.encoding().id().as_ref() == "vortex.listview" {
            next_field.to_listview().children()[1].clone()
        } else {
            next_field
        };
        
        log::debug!("[ChainedHash::from_vortex_array] Reconstruction took {:?}", start.elapsed());
        
        Ok(Self {
            buckets,
            next,
            values,
        })
    }

    fn get_or_insert(&mut self, term_str: &str) -> u32 {
        //let _start = Instant::now();
        let h = Self::hash(term_str);
        
        // Traverse chain to check if term exists
        let buckets_prim = self.buckets.to_primitive();
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let mut row = buckets_slice[h];
        
        let values_varbin = self.values.to_varbinview();
        let next_prim = self.next.to_primitive();
        let next_slice = next_prim.as_slice::<i32>();
        
        while row != -1 {
            let idx = row as usize;
            let bytes = values_varbin.bytes_at(idx);
            if bytes.as_ref() == term_str.as_bytes() {
                //log::debug!("[ChainedHash::get_or_insert] Found term in {:?}", start.elapsed());
                return row as u32;
            }
            row = next_slice[idx];
        }
        
        // Not found -> Insert
        let new_row_id = self.values.len() as i32;
        
        // Build new values array by appending
        let mut values_builder = VarBinViewBuilder::with_capacity(
            values_varbin.dtype().clone(),
            values_varbin.len() + 1
        );
        values_builder.extend_from_array(&self.values);
        values_builder.append_value(term_str.as_bytes());
        self.values = values_builder.finish_into_varbinview().into_array();
        
        // Update next array - append old_head using builder
        let old_head = buckets_slice[h];
        let mut next_builder = PrimitiveBuilder::<i32>::with_capacity(
            Nullability::NonNullable,
            next_slice.len() + 1
        );
        next_builder.extend_from_array(&self.next);
        next_builder.append_value(old_head);
        self.next = next_builder.finish().into_array();
        
        // Update buckets array - modify bucket[h]
        // Note: For modifying a single element, to_vec() is more efficient than builder iteration
        let mut buckets_vec = buckets_slice.to_vec();
        buckets_vec[h] = new_row_id;
        self.buckets = PrimitiveArray::from_iter(buckets_vec).into_array();
        
        //log::debug!("[ChainedHash::get_or_insert] Inserted term in {:?}", start.elapsed());
        new_row_id as u32
    }

    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32> {
        let mut ids = Vec::with_capacity(terms.len());
        
        // First pass: collect existing IDs and new terms
        let mut new_terms: Vec<&str> = Vec::new();
        let mut new_term_hashes: Vec<usize> = Vec::new();
        
        let buckets_prim = self.buckets.to_primitive();
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let values_varbin = self.values.to_varbinview();
        let next_prim = self.next.to_primitive();
        let next_slice = next_prim.as_slice::<i32>();
        
        for &term_str in terms {
            let h = Self::hash(term_str);
            let mut row = buckets_slice[h];
            let mut found = false;
            
            // Check if term exists
            while row != -1 {
                let idx = row as usize;
                let bytes = values_varbin.bytes_at(idx);
                if bytes.as_ref() == term_str.as_bytes() {
                    ids.push(row as u32);
                    found = true;
                    break;
                }
                row = next_slice[idx];
            }
            
            if !found {
                // Mark for insertion
                new_terms.push(term_str);
                new_term_hashes.push(h);
                ids.push(u32::MAX); // Placeholder, will be replaced
            }
        }
        
        // If no new terms, return early
        if new_terms.is_empty() {
            return ids;
        }
        
        // Second pass: bulk insert new terms using builders
        let last_known_id = self.values.len() as u32;
        
        // Build new values array
        let mut values_builder = VarBinViewBuilder::with_capacity(
            values_varbin.dtype().clone(),
            values_varbin.len() + new_terms.len()
        );
        values_builder.extend_from_array(&self.values);
        
        for &term in &new_terms {
            values_builder.append_value(term.as_bytes());
        }
        
        self.values = values_builder.finish_into_varbinview().into_array();
        
        // Build new next array
        let mut next_builder = PrimitiveBuilder::<i32>::with_capacity(
            Nullability::NonNullable,
            next_slice.len() + new_terms.len()
        );
        next_builder.extend_from_array(&self.next);
        
        // Build new buckets array with updates
        let mut buckets_vec = buckets_slice.to_vec();
        
        // Update buckets and next for each new term
        for (i, &h) in new_term_hashes.iter().enumerate() {
            let new_id = last_known_id + i as u32;
            let old_head = buckets_vec[h];
            next_builder.append_value(old_head);
            buckets_vec[h] = new_id as i32;
        }
        
        self.next = next_builder.finish().into_array();
        self.buckets = PrimitiveArray::from_iter(buckets_vec).into_array();
        
        // Third pass: replace placeholders with actual IDs
        let mut new_term_idx = 0;
        for id in &mut ids {
            if *id == u32::MAX {
                *id = last_known_id + new_term_idx;
                new_term_idx += 1;
            }
        }
        
        ids
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        let h = Self::hash(term_str);
        let buckets_prim = self.buckets.to_primitive();
        let mut curr = buckets_prim.as_slice::<i32>()[h];
        
        let values_varbin = self.values.to_varbinview();
        let next_prim = self.next.to_primitive();
        let next_slice = next_prim.as_slice::<i32>();
        
        while curr != -1 {
            let idx = curr as usize;
            let bytes = values_varbin.bytes_at(idx);
            if bytes.as_ref() == term_str.as_bytes() {
                return Some(idx as u32);
            }
            curr = next_slice[idx];
        }
        None
    }

    fn get_term(&self, id: u32) -> Option<Term> {
        let values_varbin = self.values.to_varbinview();
        if (id as usize) >= values_varbin.len() {
            return None;
        }
        let bytes = values_varbin.bytes_at(id as usize);
        let s = String::from_utf8_lossy(bytes.as_ref());
        utils::get_as_term(&s)
    }

    fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let values_varbin = self.values.to_varbinview();
        if (id as usize) >= values_varbin.len() {
            return None;
        }
        let bytes = values_varbin.bytes_at(id as usize);
        let s = String::from_utf8_lossy(bytes.as_ref());
        if s.is_empty() || s == "[]" {
            Some(GraphName::DefaultGraph)
        } else {
            match utils::get_as_term(&s) {
                Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
                Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
                _ => Some(GraphName::DefaultGraph),
            }
        }
    }

    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>> {        
        Ok(vec![
            ("values".to_string(), self.values.clone()),
            ("buckets".to_string(), self.buckets.clone()),
            ("next".to_string(), self.next.clone()),
        ])
    }

    fn store_type() -> &'static str {
        indexes::IndexType::ChainedHash.as_str()
    }
}
