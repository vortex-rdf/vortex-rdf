use crate::common::{indexes::IndexType, utils};
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use std::time::Instant;
use std::collections::HashMap;
use oxrdf::{GraphName, Term};
use vortex::VortexSessionDefault;
use vortex_session::VortexSession;

use vortex_array::builders::{ArrayBuilder, VarBinViewBuilder, PrimitiveBuilder};
use vortex_array::ArrayRef;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray, StructArray};
use vortex_array::{IntoArray, LEGACY_SESSION, VortexSessionExecute};
use vortex_array::dtype::Nullability;
use vortex_fsst::{fsst_compress, fsst_train_compressor};
use vortex_btrblocks::BtrBlocksCompressor;

/// Chained hash dictionary implementation.
///
/// All three arrays (`values`, `buckets`, `next`) are kept as opaque `ArrayRef`s
/// — i.e. in their Vortex-compressed form in memory — rather than being decoded
/// eagerly. This means:
///
/// * `get_id` / `get_term` execute the relevant array on demand (cheap for
///   sparse access such as a single pattern lookup or a few matching rows).
/// * For bulk access (`quads()` full scan), callers should call `values_view()`
///   once to get a decoded `VarBinViewArray` and then use `bytes_at` per row,
///   avoiding the per-call execute + context-creation overhead.
///
/// Keeping `values` as an `ArrayRef` means the FSST-compressed string data
/// stays compressed between accesses, using significantly less memory than
/// `SimpleDictionary`'s plain `Vec<String>`.
#[derive(Clone)]
pub struct ChainedHash {
    pub buckets: ArrayRef, // PrimitiveArray<i32>
    pub next: ArrayRef,    // PrimitiveArray<i32>
    pub values: ArrayRef,  // VarBinViewArray
}

impl ChainedHash {
    const BUCKET_SIZE: usize = 1_000_003; // Prime number for hash distribution

    /// Deterministic FNV-1a hash — produces identical bucket indices across
    /// process runs (unlike `DefaultHasher` which is randomly seeded).
    fn hash(s: &str) -> usize {
        const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
        const FNV_PRIME: u64 = 1_099_511_628_211;
        let mut h = FNV_OFFSET;
        for byte in s.bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        (h as usize) % Self::BUCKET_SIZE
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

    /// Deserializes the ChainedHash tables from a Vortex StructArray.
    fn from_vortex_array(dict_array_ref: &ArrayRef) -> Result<Self> {
        let start = Instant::now();
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        let dict_struct = dict_array_ref.clone().execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let values  = utils::extract_dictionary_column(&dict_struct, "_dict_values")?;
        let buckets_raw = utils::extract_dictionary_column(&dict_struct, "_dict_buckets")?;
        let next_raw    = utils::extract_dictionary_column(&dict_struct, "_dict_next")?;

        // Eagerly decompress/evaluate buckets and next to flat PrimitiveArrays
        let buckets = buckets_raw.execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        let next = next_raw.execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();

        log::debug!(
            "[ChainedHash::from_vortex_array] Reconstruction took {:?}",
            start.elapsed()
        );

        Ok(Self { buckets, next, values })
    }

    fn get_or_insert(&mut self, term_str: &str) -> u32 {
        let h = Self::hash(term_str);
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        // Traverse chain to check if term exists
        let buckets_prim = self.buckets.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .expect("buckets must be primitive");
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let mut row = buckets_slice[h];

        let values_varbin = self.values.clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .expect("values must be varbinview");
        let next_prim = self.next.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .expect("next must be primitive");
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

        // Build new values array by appending.
        let mut values_builder = VarBinViewBuilder::with_capacity(
            values_varbin.dtype().clone(),
            values_varbin.len() + 1,
        );
        values_builder.extend_from_array(&self.values);
        values_builder.append_value(term_str.as_bytes());
        self.values = values_builder.finish_into_varbinview().into_array();

        // Update next array - append old_head using builder.
        let old_head = buckets_slice[h];
        let mut next_builder =
            PrimitiveBuilder::<i32>::with_capacity(Nullability::NonNullable, next_slice.len() + 1);
        next_builder.extend_from_array(&self.next);
        next_builder.append_value(old_head);
        self.next = next_builder.finish().into_array();

        // Update buckets array - modify bucket[h].
        let mut buckets_vec = buckets_slice.to_vec();
        buckets_vec[h] = new_row_id;
        self.buckets = PrimitiveArray::from_iter(buckets_vec).into_array();

        new_row_id as u32
    }

    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32> {
        let mut ids = Vec::with_capacity(terms.len());
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        let buckets_prim = self.buckets.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .expect("buckets must be primitive");
        let buckets_slice = buckets_prim.as_slice::<i32>();
        let values_varbin = self.values.clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .expect("values must be varbinview");
        let next_prim = self.next.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .expect("next must be primitive");
        let next_slice = next_prim.as_slice::<i32>();

        let last_known_id = self.values.len() as u32;

        // Track new terms first seen in this batch: maps term string -> assigned new ID.
        // This is needed because `buckets_slice` / `next_slice` are frozen snapshots of
        // the pre-call state, so repeated occurrences of the same term within the batch
        // would otherwise all fail the chain-walk and each be inserted as a distinct entry
        // with a different ID — causing query misses for all but one of those quads.
        let mut batch_new_terms: HashMap<&str, u32> = HashMap::new();
        let mut new_terms: Vec<&str> = Vec::new();
        let mut new_term_hashes: Vec<usize> = Vec::new();

        for &term_str in terms {
            let h = Self::hash(term_str);
            let mut row = buckets_slice[h];
            let mut found = false;

            // Check existing (pre-call) hash table
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
                // Check if this term was already encountered in the current batch
                if let Some(&existing_id) = batch_new_terms.get(term_str) {
                    ids.push(existing_id);
                } else {
                    // Brand-new term: assign the next available ID
                    let new_id = last_known_id + new_terms.len() as u32;
                    batch_new_terms.insert(term_str, new_id);
                    new_terms.push(term_str);
                    new_term_hashes.push(h);
                    ids.push(new_id);
                }
            }
        }

        // If no new terms, return early
        if new_terms.is_empty() {
            return ids;
        }

        // Bulk-insert new terms using builders
        let mut values_builder = VarBinViewBuilder::with_capacity(
            values_varbin.dtype().clone(),
            values_varbin.len() + new_terms.len(),
        );
        values_builder.extend_from_array(&self.values);
        for &term in &new_terms {
            values_builder.append_value(term.as_bytes());
        }
        self.values = values_builder.finish_into_varbinview().into_array();

        let mut next_builder = PrimitiveBuilder::<i32>::with_capacity(
            Nullability::NonNullable,
            next_slice.len() + new_terms.len(),
        );
        next_builder.extend_from_array(&self.next);

        let mut buckets_vec = buckets_slice.to_vec();
        for (i, &h) in new_term_hashes.iter().enumerate() {
            let new_id = last_known_id + i as u32;
            let old_head = buckets_vec[h];
            next_builder.append_value(old_head);
            buckets_vec[h] = new_id as i32;
        }

        self.next = next_builder.finish().into_array();
        self.buckets = PrimitiveArray::from_iter(buckets_vec).into_array();

        ids
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        let h = Self::hash(term_str);
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        let buckets_prim = self.buckets.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .ok()?;
        let mut curr = buckets_prim.as_slice::<i32>()[h];

        let values_varbin = self.values.clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .ok()?;
        let next_prim = self.next.clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .ok()?;
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
        let values_varbin = self
            .values
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .ok()?;
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
        let values_varbin = self
            .values
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .ok()?;
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

    fn values_view(&self) -> Result<VarBinViewArray> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        self.values.clone().execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)
    }

    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        let dict_raw = self.values.clone().execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let dict_arr = if dict_raw.len() > 0 {
            // Train compressor and execute FSST on the dictionary payload.
            let compressor = fsst_train_compressor(&dict_raw);
            let len = dict_raw.len();
            let dtype = dict_raw.dtype().clone();
            fsst_compress(
                dict_raw,
                len,
                &dtype,
                &compressor,
                &mut ctx,
            ).into_array()
        } else {
            dict_raw.into_array()
        };

        let btr_compressor = BtrBlocksCompressor::default();
        let buckets_compressed = btr_compressor.compress(&self.buckets, &mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let next_compressed = btr_compressor.compress(&self.next, &mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        Ok(vec![
            ("values".to_string(), dict_arr),
            ("buckets".to_string(), buckets_compressed),
            ("next".to_string(), next_compressed),
        ])
    }

    fn store_type() -> &'static str {
        IndexType::ChainedHash.as_str()
    }

    fn vortex_field_names() -> &'static [&'static str] {
        &["values", "buckets", "next"]
    }
}
