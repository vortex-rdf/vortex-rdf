use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term};
pub use oxrdfio::RdfFormat;
use oxrdfio::{RdfParser, RdfSerializer};
use std::collections::HashMap;
use std::io::{Read, Write};
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};

pub mod de;
pub mod error;
pub mod ser;

pub use error::VortexRdfError;

#[derive(Debug, Clone, Default)]
pub struct Dictionary {
    pub terms: Vec<String>,
    pub term_to_id: HashMap<String, u32>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_root(root: &ArrayRef) -> error::Result<Self> {
        let root_struct = root.to_struct();
        let dict_list_ref = root_struct
            .fields()
            .get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing dictionary field".to_string()))?
            .clone();

        let dict_list = dict_list_ref.to_listview();
        let dict_offset = dict_list.offsets().to_primitive().as_slice::<i32>()[0] as usize;
        let dict_size = dict_list.sizes().to_primitive().as_slice::<i32>()[0] as usize;
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

    pub fn get_or_insert_subject(&mut self, subject: &Subject) -> u32 {
        self.get_or_insert(&subject.to_string())
    }

    pub fn get_or_insert_named_node(&mut self, node: &NamedNode) -> u32 {
        self.get_or_insert(&node.to_string())
    }

    pub fn get_or_insert_term(&mut self, term: &Term) -> u32 {
        self.get_or_insert(&term.to_string())
    }

    pub fn get_or_insert_graph(&mut self, graph: &GraphName) -> u32 {
        self.get_or_insert(&graph.to_string())
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

    pub fn get_id(&self, term: &Term) -> Option<u32> {
        self.term_to_id.get(&term.to_string()).copied()
    }

    pub fn get_graph_id(&self, graph: &GraphName) -> Option<u32> {
        self.term_to_id.get(&graph.to_string()).copied()
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

pub struct VortexRdfStore {
    pub root: ArrayRef,
    pub dictionary: Dictionary,
    #[allow(dead_code)]
    mmap: Option<memmap2::Mmap>,
}

impl VortexRdfStore {
    pub fn new(root: ArrayRef) -> error::Result<Self> {
        let dictionary = Dictionary::from_root(&root)?;
        Ok(Self {
            root,
            dictionary,
            mmap: None,
        })
    }

    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> error::Result<Self> {
        let file = std::fs::File::open(path)?;
        // SAFETY: Memory mapping is used for zero-copy access to large datasets.
        // We assume the file is not concurrently modified or truncated by another process,
        // which could otherwise lead to undefined behavior (e.g., SIGBUS).
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        let mut store = Self::from_bytes(&mmap)?;
        store.mmap = Some(mmap);
        Ok(store)
    }

    pub fn from_bytes(bytes: &[u8]) -> error::Result<Self> {
        let array = de::array_from_reader(std::io::Cursor::new(bytes))?;
        Self::new(array)
    }

    pub fn get_quads_array(&self) -> error::Result<ArrayRef> {
        let root_struct = self.root.to_struct();
        let quads_list_ref = root_struct
            .fields()
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing quads field".to_string()))?
            .clone();

        let quads_list = quads_list_ref.to_listview();
        let quads_offset = quads_list.offsets().to_primitive().as_slice::<i32>()[0] as usize;
        let quads_size = quads_list.sizes().to_primitive().as_slice::<i32>()[0] as usize;
        Ok(quads_list
            .elements()
            .slice(quads_offset..quads_offset + quads_size))
    }

    pub fn size(&self) -> usize {
        self.get_quads_array().map(|a| a.len()).unwrap_or(0)
    }

    pub fn quads(&self) -> error::Result<impl Iterator<Item = error::Result<Quad>>> {
        de::decode_quads_stream(self.root.clone())
    }

    /// Internal helper to find row indices matching a pattern
    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> error::Result<Option<ArrayRef>> {
        use vortex::compute::{and, compare, Operator};

        let quads_array_ref = self.get_quads_array()?;
        let quads_struct = quads_array_ref.to_struct();
        let fields = quads_struct.fields();

        let mut mask: Option<ArrayRef> = None;

        let mut combine_mask = |new_mask: ArrayRef| -> error::Result<()> {
            if let Some(m) = mask.take() {
                mask = Some(and(&m, &new_mask).map_err(VortexRdfError::Vortex)?);
            } else {
                mask = Some(new_mask);
            }
            Ok(())
        };

        if let Some(s) = subject {
            let id = self.dictionary.term_to_id.get(&s.to_string()).copied();
            if let Some(sid) = id {
                let col = fields.get(0).unwrap();
                let scalar = vortex_scalar::Scalar::from(sid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &vortex_array::arrays::ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                combine_mask(col_mask)?;
            } else {
                // If term not in dict, mask is all false
                let col = fields.get(0).unwrap();
                return Ok(Some(
                    vortex_array::arrays::ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(p) = predicate {
            let id = self.dictionary.term_to_id.get(&p.to_string()).copied();
            if let Some(pid) = id {
                let col = fields.get(1).unwrap();
                let scalar = vortex_scalar::Scalar::from(pid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &vortex_array::arrays::ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(1).unwrap();
                return Ok(Some(
                    vortex_array::arrays::ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(o) = object {
            let id = self.dictionary.term_to_id.get(&o.to_string()).copied();
            if let Some(oid) = id {
                let col = fields.get(2).unwrap();
                let scalar = vortex_scalar::Scalar::from(oid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &vortex_array::arrays::ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(2).unwrap();
                return Ok(Some(
                    vortex_array::arrays::ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(g) = graph {
            let id = self.dictionary.term_to_id.get(&g.to_string()).copied();
            if let Some(gid) = id {
                let col = fields.get(3).unwrap();
                let scalar = vortex_scalar::Scalar::from(gid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &vortex_array::arrays::ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(3).unwrap();
                return Ok(Some(
                    vortex_array::arrays::ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        Ok(mask)
    }

    pub fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> error::Result<Self> {
        use vortex::compute::filter;

        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
            let quads_array_ref = self.get_quads_array()?;
            let canonical = m.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => vortex_mask::Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Mask must be boolean".to_string(),
                    ))
                }
            };
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                root: self.root.clone(),
                dictionary: self.dictionary.clone(),
                mmap: None,
            })
        }
    }

    pub fn add_quad(&self, quad: Quad) -> error::Result<Self> {
        let mut new_dict = self.dictionary.clone();
        let s_id = new_dict.get_or_insert_subject(&quad.subject);
        let p_id = new_dict.get_or_insert_named_node(&quad.predicate);
        let o_id = new_dict.get_or_insert_term(&quad.object);
        let g_id = new_dict.get_or_insert_graph(&quad.graph_name);

        let new_row = ser::encode_quad_ids(vec![s_id], vec![p_id], vec![o_id], vec![g_id])?;
        let old_quads = self.get_quads_array()?;

        // Use vortex ChunkedArray for efficient zero-copy concatenation
        let combined_quads = vortex_array::arrays::ChunkedArray::try_new(
            vec![old_quads, new_row],
            self.get_quads_array()?.dtype().clone(),
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        // Re-encode dictionary
        let dict_arr = ser::encode_dictionary(&new_dict)?;

        Ok(Self {
            root: self.with_dict_and_quads(dict_arr, combined_quads)?,
            dictionary: new_dict,
            mmap: None,
        })
    }

    pub fn delete_quad(&self, quad: &Quad) -> error::Result<Self> {
        use vortex::compute::{compare, filter, Operator};

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
                &vortex_array::arrays::ConstantArray::new(true, m.len()).into_array(),
                Operator::NotEq,
            )
            .map_err(VortexRdfError::Vortex)?;

            let canonical = inverse_mask_array.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => vortex_mask::Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Inverse mask must be boolean".to_string(),
                    ))
                }
            };

            let quads_array_ref = self.get_quads_array()?;
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                root: self.root.clone(),
                dictionary: self.dictionary.clone(),
                mmap: None,
            })
        }
    }

    fn with_quads(&self, quads: ArrayRef) -> error::Result<Self> {
        Ok(Self {
            root: self.with_quads_root(quads)?,
            dictionary: self.dictionary.clone(),
            mmap: None,
        })
    }

    fn with_quads_root(&self, quads: ArrayRef) -> error::Result<ArrayRef> {
        let root_struct = self.root.to_struct();
        let dict_list = root_struct.fields().get(0).unwrap().clone();
        self.with_dict_and_quads_root(dict_list, quads)
    }

    fn with_dict_and_quads(&self, dict: ArrayRef, quads: ArrayRef) -> error::Result<ArrayRef> {
        use vortex_array::arrays::ListArray;
        use vortex_array::validity::Validity;

        let dict_offsets =
            vortex_array::arrays::PrimitiveArray::from_iter(vec![0i32, dict.len() as i32])
                .into_array();
        let dict_list = ListArray::try_new(dict, dict_offsets, Validity::NonNullable)?.into_array();

        self.with_dict_and_quads_root(dict_list, quads)
    }

    fn with_dict_and_quads_root(&self, dict_list: ArrayRef, quads: ArrayRef) -> error::Result<ArrayRef> {
        use vortex_array::arrays::ListArray;
        use vortex_array::validity::Validity;

        let quads_offsets =
            vortex_array::arrays::PrimitiveArray::from_iter(vec![0i32, quads.len() as i32])
                .into_array();
        let quads_list =
            ListArray::try_new(quads, quads_offsets, Validity::NonNullable)?.into_array();

        let new_root = vortex_array::arrays::StructArray::from_fields(&[
            ("dictionary", dict_list),
            ("quads", quads_list),
        ])?
        .into_array();
        Ok(new_root)
    }
}

pub fn quads_to_vortex_writer<I, W>(quads: I, writer: W) -> error::Result<()>
where
    I: IntoIterator<Item = Quad>,
    W: Write,
{
    let array = ser::encode_quads(quads)?;
    ser::write_array_to_ipc(array, writer)
}

pub fn quads_to_vortex<I>(quads: I) -> error::Result<Vec<u8>>
where
    I: IntoIterator<Item = Quad>,
{
    let mut buffer = Vec::new();
    quads_to_vortex_writer(quads, &mut buffer)?;
    Ok(buffer)
}

pub fn vortex_to_quads(bytes: &[u8]) -> error::Result<Vec<Quad>> {
    let array = de::array_from_reader(std::io::Cursor::new(bytes))?;
    de::decode_quads(array)
}

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub fn serialize<R: Read, W: Write>(reader: R, writer: W, format: RdfFormat) -> error::Result<()> {
    let parser = RdfParser::from_format(format);
    let quads_iter = parser
        .for_reader(reader)
        .map(|res| res.map_err(|e| error::VortexRdfError::Serialization(e.to_string())));

    itertools::process_results(quads_iter, |iter| quads_to_vortex_writer(iter, writer))?
}

/// High-level function to deserialize Vortex-RDF data from a reader directly to an RDF writer.
pub fn deserialize<R: Read, W: Write>(
    reader: R,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let array = de::array_from_reader(reader)?;
    let quads_stream = de::decode_quads_stream(array)?;

    let mut serializer = RdfSerializer::from_format(format).for_writer(writer);
    for quad_res in quads_stream {
        let quad = quad_res?;
        serializer
            .serialize_quad(&quad)
            .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    }
    serializer
        .finish()
        .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{GraphName, Literal, NamedNode, Quad, Subject, Term};

    #[test]
    fn test_roundtrip_quad() {
        let s = Subject::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("hello"));
        let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let quad = Quad::new(s, p, o, g);
        let quads = vec![quad.clone()];

        let vortex_bytes = quads_to_vortex(quads).expect("Serialization failed");
        let decoded_quads = vortex_to_quads(&vortex_bytes).expect("Deserialization failed");

        assert_eq!(1, decoded_quads.len());
        assert_eq!(
            quad.subject.to_string(),
            decoded_quads[0].subject.to_string()
        );
    }

    #[test]
    fn test_match_pattern() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;

        let s2 = Subject::NamedNode(NamedNode::new("http://example.org/s2").unwrap());
        let p2 = NamedNode::new("http://example.org/p2").unwrap();
        let o2 = Term::Literal(Literal::new_simple_literal("o2"));
        let g2 = GraphName::DefaultGraph;

        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());
        let q2 = Quad::new(s2.clone(), p2.clone(), o2.clone(), g2.clone());

        let quads = vec![q1.clone(), q2.clone()];
        let vortex_bytes = quads_to_vortex(quads).unwrap();
        let store = VortexRdfStore::from_bytes(&vortex_bytes).unwrap();

        // Match ?s <p1> ?o ?g
        let filtered = store.match_pattern(None, Some(&p1), None, None).unwrap();
        assert_eq!(filtered.size(), 1);

        let mut results = filtered.quads().unwrap();
        let res_q = results.next().unwrap().unwrap();
        assert_eq!(res_q.subject.to_string(), s1.to_string());

        // Match ?s <non-existent> ?o ?g
        let p3 = NamedNode::new("http://example.org/p3").unwrap();
        let empty = store.match_pattern(None, Some(&p3), None, None).unwrap();
        assert_eq!(empty.size(), 0);
    }

    #[test]
    fn test_add_delete_quad() {
        let s1 = Subject::NamedNode(NamedNode::new("http://example.org/s1").unwrap());
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("o1"));
        let g1 = GraphName::DefaultGraph;
        let q1 = Quad::new(s1.clone(), p1.clone(), o1.clone(), g1.clone());

        let store = VortexRdfStore::from_bytes(&quads_to_vortex(vec![]).unwrap()).unwrap();
        assert_eq!(store.size(), 0);

        // Add quad
        let store = store.add_quad(q1.clone()).unwrap();
        assert_eq!(store.size(), 1);

        // Delete quad
        let store = store.delete_quad(&q1).unwrap();
        assert_eq!(store.size(), 0);
    }
}
