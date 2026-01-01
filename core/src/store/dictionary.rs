use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Term};
use std::collections::HashMap;
use vortex_array::ToCanonical;
use vortex_array::ArrayRef;
use vortex_dtype::{DType, Nullability, PType};
use crate::error::{Result, VortexRdfError};

#[derive(Debug, Clone, Default)]
pub struct Dictionary {
    pub terms: Vec<String>,
    pub term_to_id: HashMap<String, u32>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_root(root: &ArrayRef) -> Result<Self> {
        let root_struct = root.to_struct();
        let dict_list_ref = root_struct
            .fields()
            .get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing dictionary field".to_string()))?
            .clone();

        let dict_list = dict_list_ref.to_listview();
        let dict_offset = dict_list.offsets().scalar_at(0).cast(&DType::Primitive(PType::I32, Nullability::NonNullable)).map_err(VortexRdfError::Vortex)?.as_primitive().typed_value::<i32>().ok_or_else(|| VortexRdfError::Deserialization("Missing dictionary offset".to_string()))? as usize;
        let dict_size = dict_list.sizes().scalar_at(0).cast(&DType::Primitive(PType::I32, Nullability::NonNullable)).map_err(VortexRdfError::Vortex)?.as_primitive().typed_value::<i32>().ok_or_else(|| VortexRdfError::Deserialization("Missing dictionary size".to_string()))? as usize;
        let dict_array_ref = dict_list
            .elements()
            .slice(dict_offset..dict_offset + dict_size);

        let dict_varbin = dict_array_ref.to_varbinview();

        let mut dictionary = Dictionary::new();
        for i in 0..dict_varbin.len() {
            let bytes = dict_varbin.bytes_at(i);
            let s = String::from_utf8_lossy(&bytes).into_owned();
            dictionary.terms.push(s.clone());
            dictionary.term_to_id.insert(s.clone(), i as u32);
        }
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
        // Use oxrdf parser to reconstruct the term from N-Triples string
        if s.starts_with('<') {
            Some(Term::NamedNode(
                NamedNode::new(s.trim_matches(|c| c == '<' || c == '>')).ok()?,
            ))
        } else if s.starts_with("_:") {
            Some(Term::BlankNode(
                BlankNode::new(s.trim_start_matches("_:")).ok()?,
            ))
        } else if s.starts_with('"') {
            // Very basic literal parsing for now
            if s.contains("^^") {
                let parts: Vec<&str> = s.split("^^").collect();
                let val = parts[0].trim_matches('"');
                let dt = parts[1].trim_matches(|c| c == '<' || c == '>');
                Some(Term::Literal(Literal::new_typed_literal(
                    val,
                    NamedNode::new(dt).ok()?,
                )))
            } else if s.contains('@') {
                let last_at = s.rfind('@')?;
                let val = s[..last_at].trim_matches('"');
                let lang = &s[last_at + 1..];
                Some(Term::Literal(
                    Literal::new_language_tagged_literal(val, lang).ok()?,
                ))
            } else {
                Some(Term::Literal(Literal::new_simple_literal(
                    s.trim_matches('"'),
                )))
            }
        } else if s.is_empty() {
            None // Used for Default Graph in some contexts, but get_graph_name handles it
        } else {
            None
        }
    }

    pub fn get_graph_name(&self, id: u32) -> Option<GraphName> {
        let s = self.terms.get(id as usize)?;
        if s.is_empty() || s == "[]" {
            Some(GraphName::DefaultGraph)
        } else {
            match self.get_term(id) {
                Some(Term::NamedNode(n)) => Some(GraphName::NamedNode(n)),
                Some(Term::BlankNode(b)) => Some(GraphName::BlankNode(b)),
                _ => Some(GraphName::DefaultGraph), // Fallback
            }
        }
    }
}
