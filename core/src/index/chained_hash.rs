use crate::common::{indexes, utils};
use crate::error::Result;
use crate::index::RdfDictionary;

use oxrdf::{GraphName, Term};
use vortex::VortexSessionDefault;
use vortex_session::VortexSession;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use vortex_array::builders::{ArrayBuilder, VarBinViewBuilder, PrimitiveBuilder};
use vortex_array::arrays::listview::ListViewArrayExt;

use vortex_array::ArrayRef;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray, StructArray, ListViewArray};
use vortex_array::{IntoArray, VortexSessionExecute};
use vortex_array::dtype::{DType, Nullability, PType};

/// Chained hash dictionary implementation
#[derive(Clone)]
pub struct ChainedHash {
    pub buckets: ArrayRef, // PrimitiveArray<i32>
    pub next: ArrayRef,    // PrimitiveArray<i32>
    pub values: ArrayRef,  // VarBinViewArray
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
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        let mut curr = buckets_arr.clone().execute::<PrimitiveArray>(&mut ctx).ok()?
            .execute_scalar(bucket_idx, &mut ctx).ok()?
            .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
            .ok()?
            .as_primitive()
            .typed_value::<i32>()?;

        let values_varbin = values_arr.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
        let next_prim = next_arr.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;

        while curr != -1 {
            let idx = curr as usize;
            let bytes = values_varbin.bytes_at(idx);
            if bytes.as_ref() == term_str.as_bytes() {
                return Some(idx as u32);
            }
            curr = next_prim.execute_scalar(idx, &mut ctx)
                .ok()?
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
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        // The dictionary might be wrapped in a ListArray, or it might be the Struct directly
        // Try to unwrap as ListArray first, if that fails, use it directly
        let id = dict_array_ref.encoding_id();
        log::debug!("[ChainedHash::from_vortex_array] Encoding ID: {}", id);

        let dict_struct = if id.as_ref() == "vortex.listview" {
            // It's a ListArray, unwrap it
            let dict_list = dict_array_ref.clone().execute::<ListViewArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?;
            let dict_struct_array = dict_list.elements().clone();
            dict_struct_array.execute::<StructArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?
        } else {
            dict_array_ref.clone().execute::<StructArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?
        };

        // Extract the three fields from the dictionary struct
        let values_field = utils::extract_vortex_struct_field(&dict_struct, "values")?;
        let buckets_field = utils::extract_vortex_struct_field(&dict_struct, "buckets")?;
        let next_field = utils::extract_vortex_struct_field(&dict_struct, "next")?;

        // Unwrap each field if it's a ListArray, otherwise use directly
        let values = if values_field.encoding_id().as_ref() == "vortex.listview" {
            values_field.clone().execute::<ListViewArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?
                .elements().clone()
        } else {
            values_field
        };

        let buckets = if buckets_field.encoding_id().as_ref() == "vortex.listview" {
            buckets_field.clone().execute::<ListViewArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?
                .elements().clone()
        } else {
            buckets_field
        };

        let next = if next_field.encoding_id().as_ref() == "vortex.listview" {
            next_field.clone().execute::<ListViewArray>(&mut ctx)
                .map_err(crate::error::VortexRdfError::Vortex)?
                .elements().clone()
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
        let h = Self::hash(term_str);
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        // Traverse chain to check if term exists
        let buckets_prim = self.buckets.clone().execute::<PrimitiveArray>(&mut ctx).expect("buckets must be primitive");
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let mut row = buckets_slice[h];

        let values_varbin = self.values.clone().execute::<VarBinViewArray>(&mut ctx).expect("values must be varbinview");
        let next_prim = self.next.clone().execute::<PrimitiveArray>(&mut ctx).expect("next must be primitive");
        let next_slice = next_prim.as_slice::<i32>();

        while row != -1 {
            let idx = row as usize;
            let bytes = values_varbin.bytes_at(idx);
            if bytes.as_ref() == term_str.as_bytes() {
                return row as u32;
            }
            row = next_slice[idx];
        }

        // Not found -> Insert
        let new_row_id = self.values.len() as i32;

        // Build new values array by appending
        let mut values_builder = VarBinViewBuilder::with_capacity(
            values_varbin.dtype().clone(),
            values_varbin.len() + 1,
        );
        values_builder.extend_from_array(&self.values);
        values_builder.append_value(term_str.as_bytes());
        self.values = values_builder.finish_into_varbinview().into_array();

        // Update next array - append old_head using builder
        let old_head = buckets_slice[h];
        let mut next_builder =
            PrimitiveBuilder::<i32>::with_capacity(Nullability::NonNullable, next_slice.len() + 1);
        next_builder.extend_from_array(&self.next);
        next_builder.append_value(old_head);
        self.next = next_builder.finish().into_array();

        // Update buckets array - modify bucket[h]
        let mut buckets_vec = buckets_slice.to_vec();
        buckets_vec[h] = new_row_id;
        self.buckets = PrimitiveArray::from_iter(buckets_vec).into_array();

        new_row_id as u32
    }

    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32> {
        let mut ids = Vec::with_capacity(terms.len());
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        // First pass: collect existing IDs and new terms
        let mut new_terms: Vec<&str> = Vec::new();
        let mut new_term_hashes: Vec<usize> = Vec::new();

        let buckets_prim = self.buckets.clone().execute::<PrimitiveArray>(&mut ctx).expect("buckets must be primitive");
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let values_varbin = self.values.clone().execute::<VarBinViewArray>(&mut ctx).expect("values must be varbinview");
        let next_prim = self.next.clone().execute::<PrimitiveArray>(&mut ctx).expect("next must be primitive");
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
            values_varbin.len() + new_terms.len(),
        );
        values_builder.extend_from_array(&self.values);

        for &term in &new_terms {
            values_builder.append_value(term.as_bytes());
        }

        self.values = values_builder.finish_into_varbinview().into_array();

        // Build new next array
        let mut next_builder = PrimitiveBuilder::<i32>::with_capacity(
            Nullability::NonNullable,
            next_slice.len() + new_terms.len(),
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
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let buckets_prim = self.buckets.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;
        let mut curr = buckets_prim.as_slice::<i32>()[h];

        let values_varbin = self.values.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
        let next_prim = self.next.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;
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
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let values_varbin = self.values.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
        if (id as usize) >= values_varbin.len() {
            return None;
        }
        let bytes = values_varbin.bytes_at(id as usize);
        let s = String::from_utf8_lossy(bytes.as_ref());
        utils::get_as_term(&s)
    }

    fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let values_varbin = self.values.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
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
