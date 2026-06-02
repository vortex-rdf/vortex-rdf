use crate::io::VORTEX_LIGHT_SESSION;
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use crate::common::{utils, indexes::IndexType};

use std::collections::HashMap;
use std::sync::Arc;
use web_time::Instant;
use oxrdf::{GraphName, Term};

use vortex_array::ArrayRef;
use vortex_array::arrays::{StructArray, VarBinViewArray};
use vortex_array::{IntoArray, VortexSessionExecute};
use vortex_array::dtype::{DType, Nullability};
use vortex_fsst::{fsst_compress, fsst_train_compressor};

/// Internal shared structures of the simple dictionary
#[derive(Debug, Clone, Default)]
struct SimpleDictionaryInner {
    terms: Vec<String>,
    term_to_id: HashMap<String, u32>,
}

/// Simple dictionary implementation using a flat Vec and HashMap for fast bi-directional lookups.
/// Uses an Arc-wrapped inner struct to make clones O(1) during query pattern matching.
#[derive(Debug, Clone, Default)]
pub struct SimpleDictionary {
    inner: Arc<SimpleDictionaryInner>,
}

impl RdfDictionary for SimpleDictionary {
    fn new() -> Self {
        Self::default()
    }

    /// Deserializes the dictionary mappings directly from a Vortex StructArray.
    fn from_vortex_array(array_ref: &ArrayRef) -> Result<Self> {
        let start = Instant::now();
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_array = array_ref.clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let dict_array = utils::extract_dictionary_column(&struct_array, "_dict_values")?;

        let dict_varbin = dict_array.execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        log::debug!("[SimpleDictionary::from_vortex_array] Extraction took {:?}", start.elapsed());

        let loop_start = Instant::now();
        let mut dictionary = SimpleDictionary::new();
        // Decode bytes and insert sequentially into host memory lookup maps.
        for i in 0..dict_varbin.len() {
            let bytes = dict_varbin.bytes_at(i);
            let s = String::from_utf8_lossy(&bytes).into_owned();
            dictionary.get_or_insert(&s);
        }
        log::debug!("[SimpleDictionary::from_vortex_array] HashMap build took {:?}", loop_start.elapsed());

        Ok(dictionary)
    }

    fn get_or_insert(&mut self, term_str: &str) -> u32 {
        let inner = Arc::make_mut(&mut self.inner);
        if let Some(&id) = inner.term_to_id.get(term_str) {
            id
        } else {
            let id = inner.terms.len() as u32;
            let term_string = term_str.to_string();
            inner.terms.push(term_string.clone());
            inner.term_to_id.insert(term_string, id);
            id
        }
    }

    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32> {
        let inner = Arc::make_mut(&mut self.inner);
        let mut ids = Vec::with_capacity(terms.len());
        
        for &term_str in terms {
            if let Some(&id) = inner.term_to_id.get(term_str) {
                ids.push(id);
            } else {
                let id = inner.terms.len() as u32;
                let term_string = term_str.to_string();
                inner.terms.push(term_string.clone());
                inner.term_to_id.insert(term_string, id);
                ids.push(id);
            }
        }
        
        ids
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        self.inner.term_to_id.get(term_str).copied()
    }

    fn get_term(&self, id: u32) -> Option<Term> {
        let s = self.inner.terms.get(id as usize)?;
        utils::get_as_term(s)
    }

    fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let s = self.inner.terms.get(id as usize)?;
        if s.is_empty() || s == "[]" {
            Some(GraphName::DefaultGraph)
        } else {
            match utils::get_as_term(s) {
                Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
                Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
                _ => Some(GraphName::DefaultGraph), // Fallback
            }
        }
    }

    fn values_view(&self) -> crate::error::Result<VarBinViewArray> {
        Ok(VarBinViewArray::from_iter(
            self.inner.terms.iter().map(|s: &String| Some(s.as_str())),
            DType::Utf8(Nullability::NonNullable),
        ))
    }

    /// Serializes the dictionary table into a flat list of (name, array) fields.
    /// Employs standard FSST (Fast Static String Table) compression for optimal storage.
    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>> {
        let dict_raw = VarBinViewArray::from_iter(
            self.inner.terms.iter().map(|s: &String| Some(s.as_str())),
            DType::Utf8(Nullability::NonNullable),
        );

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
                &mut VORTEX_LIGHT_SESSION.create_execution_ctx(),
            ).into_array()
        } else {
            dict_raw.into_array()
        };
        
        Ok(vec![("values".to_string(), dict_arr)])
    }

    fn store_type() -> &'static str {
        IndexType::SimpleDictionary.as_str()
    }

    fn vortex_field_names() -> &'static [&'static str] {
        &["values"]
    }
}
