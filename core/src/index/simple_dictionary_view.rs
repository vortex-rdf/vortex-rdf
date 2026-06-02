use crate::common::utils;
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;

use oxrdf::{GraphName, Term};
use std::time::Instant;
use vortex::dtype::{DType, Nullability};

use vortex::VortexSessionDefault;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::{StructArray, VarBinViewArray};
use vortex_session::VortexSession;

#[derive(Debug, Clone)]
pub struct SimpleDictionaryView {
    terms: Vec<String>,
}

impl SimpleDictionaryView {
    pub fn from_dictionary_sidecar_root(array_ref: &ArrayRef) -> Result<Self> {
        let start = Instant::now();

        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();

        let struct_array = array_ref
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let dict_array = utils::extract_vortex_struct_field(&struct_array, "values")?;

        let dict_varbin = dict_array
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let mut terms = Vec::with_capacity(dict_varbin.len());

        for i in 0..dict_varbin.len() {
            let bytes = dict_varbin.bytes_at(i);
            terms.push(String::from_utf8_lossy(&bytes).into_owned());
        }
        log::debug!(
            "[SimpleDictionaryView::from_dictionary_sidecar_root] Simple Dictionary created in: {:?}",
            start.elapsed()
        );

        Ok(Self { terms })
    }

    pub fn get_id(&self, term_str: &str) -> Option<u32> {
        self.terms
            .binary_search_by(|probe| probe.as_str().cmp(term_str))
            .ok()
            .map(|idx| idx as u32)
    }

    pub fn get_term_str(&self, id: u32) -> Option<&str> {
        self.terms.get(id as usize).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}
impl RdfDictionary for SimpleDictionaryView {
    fn new() -> Self {
        Self { terms: Vec::new() }
    }

    fn from_vortex_array(vortex_array: &ArrayRef) -> Result<Self> {
        Self::from_dictionary_sidecar_root(vortex_array)
    }

    fn get_or_insert(&mut self, _term_str: &str) -> u32 {
        panic!("SimpleDictionaryView is read-only and does not support insertion")
    }

    fn get_or_insert_bulk(&mut self, _terms: &[&str]) -> Vec<u32> {
        panic!("SimpleDictionaryView is read-only and does not support bulk insertion")
    }

    fn get_id(&self, term_str: &str) -> Option<u32> {
        SimpleDictionaryView::get_id(self, term_str)
    }

    fn get_term(&self, id: u32) -> Option<Term> {
        let s = self.get_term_str(id)?;
        utils::get_as_term(s)
    }

    fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let s = self.get_term_str(id)?;

        if s.is_empty() || s == "[]" {
            Some(GraphName::DefaultGraph)
        } else {
            match utils::get_as_term(s) {
                Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
                Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
                _ => Some(GraphName::DefaultGraph),
            }
        }
    }

    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>> {
        Err(VortexRdfError::InvalidOperation(
            "SimpleDictionaryView is read-only and cannot be serialized directly".to_string(),
        ))
    }

    fn store_type() -> &'static str {
        "simple-dictionary-view"
    }

    fn values_view(&self) -> crate::error::Result<VarBinViewArray> {
        Ok(VarBinViewArray::from_iter(
            self.terms.iter().map(|s: &String| Some(s.as_str())),
            DType::Utf8(Nullability::NonNullable),
        ))
    }

    fn vortex_field_names() -> &'static [&'static str] {
        &["values"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_to_id_works() {
        let view = SimpleDictionaryView {
            terms: vec![
                "<http://example.org/a>".to_string(),
                "<http://example.org/b>".to_string(),
                "<http://example.org/c>".to_string(),
            ],
        };

        assert_eq!(view.get_id("<http://example.org/a>"), Some(0));
        assert_eq!(view.get_id("<http://example.org/b>"), Some(1));
        assert_eq!(view.get_id("<http://example.org/c>"), Some(2));
        assert_eq!(view.get_id("<http://example.org/missing>"), None);
    }

    #[test]
    fn id_to_term_str_works() {
        let view = SimpleDictionaryView {
            terms: vec![
                "<http://example.org/a>".to_string(),
                "<http://example.org/b>".to_string(),
            ],
        };

        assert_eq!(view.get_term_str(0), Some("<http://example.org/a>"));
        assert_eq!(view.get_term_str(1), Some("<http://example.org/b>"));
        assert_eq!(view.get_term_str(2), None);
    }

    #[test]
    fn binary_search_lookup_works_for_sorted_terms() {
        let view = SimpleDictionaryView {
            terms: vec![
                "<http://example.org/a>".to_string(),
                "<http://example.org/b>".to_string(),
                "<http://example.org/c>".to_string(),
            ],
        };

        assert_eq!(view.get_id("<http://example.org/a>"), Some(0));
        assert_eq!(view.get_id("<http://example.org/b>"), Some(1));
        assert_eq!(view.get_id("<http://example.org/c>"), Some(2));
        assert_eq!(view.get_id("<http://example.org/missing>"), None);
    }
}
