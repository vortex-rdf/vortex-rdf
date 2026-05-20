use crate::error::Result;
use crate::index::RdfDictionary;
use crate::common::{utils, indexes::IndexType};

use std::collections::HashMap;
use std::time::Instant;
use oxrdf::{GraphName, Term};

use vortex_array::ArrayRef;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::{IntoArray, LEGACY_SESSION, VortexSessionExecute};
use vortex_array::dtype::{DType, Nullability};
use vortex_fsst::{fsst_compress, fsst_train_compressor};

/// Simple dictionary implementation using a Vec and HashMap
#[derive(Debug, Clone, Default)]
pub struct SimpleDictionary {
    pub terms: Vec<String>,
    pub term_to_id: HashMap<String, u32>,
}

impl RdfDictionary for SimpleDictionary {
    fn new() -> Self {
        Self::default()
    }

    fn from_vortex_array(array_ref: &ArrayRef) -> Result<Self> {
        let start = Instant::now();
        
        // The input is the top-level StructArray which contains "dictionary" field.
        // We need to extract it.
        let mut ctx = LEGACY_SESSION.create_execution_ctx();
        let struct_array = array_ref.clone().execute::<vortex_array::arrays::StructArray>(&mut ctx)
            .map_err(crate::error::VortexRdfError::Vortex)?;
        let dict_array = utils::extract_vortex_struct_field(&struct_array, "dictionary")?;
        
        // It's already unwrapped by extract_vortex_struct_field if it was a list
        let dict_varbin = dict_array.clone().execute::<VarBinViewArray>(&mut ctx)
            .map_err(crate::error::VortexRdfError::Vortex)?;
        
        log::debug!("[SimpleDictionary::from_vortex_array] Vortex extraction took {:?}", start.elapsed());

        let loop_start = Instant::now();
        let mut dictionary = SimpleDictionary::new();
        for i in 0..dict_varbin.len() {
            let bytes = dict_varbin.bytes_at(i);
            let s = String::from_utf8_lossy(&bytes).into_owned();
            dictionary.get_or_insert(&s);
        }
        log::debug!("[SimpleDictionary::from_vortex_array] HashMap build took {:?}", loop_start.elapsed());
        
        Ok(dictionary)
    }

    fn get_or_insert(&mut self, term_str: &str) -> u32 {
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

    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32> {
        let mut ids = Vec::with_capacity(terms.len());
        
        for &term_str in terms {
            if let Some(&id) = self.term_to_id.get(term_str) {
                ids.push(id);
            } else {
                let id = self.terms.len() as u32;
                let term_string = term_str.to_string();
                self.terms.push(term_string.clone());
                self.term_to_id.insert(term_string, id);
                ids.push(id);
            }
        }
        
        ids
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        self.term_to_id.get(term_str).copied()
    }

    fn get_term(&self, id: u32) -> Option<Term> {
        let s = self.terms.get(id as usize)?;
        utils::get_as_term(&s)
    }

    fn get_graph_name(&self, id: u32) -> Option<GraphName> {
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

    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>> {
        let dict_raw = VarBinViewArray::from_iter(
            self.terms.iter().map(|s: &String| Some(s.as_str())),
            DType::Utf8(Nullability::NonNullable),
        );

        let dict_arr = if dict_raw.len() > 0 {
            // Apply FSST compression to the dictionary table
            let compressor = fsst_train_compressor(&dict_raw);
            let len = dict_raw.len();
            let dtype = dict_raw.dtype().clone();
            fsst_compress(
                dict_raw,
                len,
                &dtype,
                &compressor,
                &mut vortex_array::LEGACY_SESSION.create_execution_ctx(),
            ).into_array()
        } else {
            dict_raw.into_array()
        };
        
        Ok(vec![("dictionary".to_string(), dict_arr)])
    }

    fn store_type() -> &'static str {
        IndexType::SimpleDictionary.as_str()
    }
}
