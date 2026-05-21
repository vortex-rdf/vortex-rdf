use crate::error::{Result, VortexRdfError};
use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_array::arrays::listview::{ListViewArray, ListViewArrayExt};
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, VortexSessionExecute};

pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("Invalid NamedNode '{}': {}", s, e)))
}

pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    let s = s.trim_start_matches("_:");
    BlankNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("Invalid BlankNode '{}': {}", s, e)))
}

pub fn parse_subject(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node(s)?))
    }
}

pub fn parse_term(s: &str) -> Result<Term> {
    if s.starts_with('_') {
        Ok(Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        // Simple literal parsing for now
        let val = s.trim_matches('"');
        Ok(Term::Literal(Literal::new_simple_literal(val)))
    } else {
        Ok(Term::NamedNode(parse_named_node(s)?))
    }
}

pub fn parse_graph_name(s: &str) -> Result<GraphName> {
    if s.is_empty() || s == "default" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node(s)?))
    }
}

pub fn get_as_term(s: &str) -> Option<Term> {
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
        None
    } else {
        None
    }
}

pub fn parse_quads_from_reader<R: std::io::Read + Send + 'static>(
    reader: R,
    format: RdfFormat,
) -> impl Stream<Item = Result<Quad>> {
    let parser = RdfParser::from_format(format);
    let iter = parser
        .for_reader(reader)
        .map(|x| x.map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e))));
    stream::iter(iter)
}

/*
 This function unpacks a certain Vortex ListArray from a Vortex StructArray.
 It assumes that the ListArray has been packed as a single element array.
*/
pub fn extract_vortex_struct_field(vortex_struct: &StructArray, name: &str) -> Result<ArrayRef> {
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::dtype::{DType, Nullability, PType};
    let start = std::time::Instant::now();
    let session = VortexSession::default(); // default session (registries etc.)
    let mut ctx = session.create_execution_ctx(); // execution ctx

    let list_ref = vortex_struct
        .unmasked_field_by_name(name)
        .map_err(|_| {
            VortexRdfError::Deserialization(format!("Field '{}' not found in struct", name))
        })?
        .clone();

    let list = list_ref
        .clone()
        .execute::<ListViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let offset = list
        .offsets()
        .execute_scalar(0, &mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .as_primitive()
        .typed_value::<i32>()
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!("Missing offset for field '{}'", name))
        })? as usize;

    let size = list
        .sizes()
        .execute_scalar(0, &mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .as_primitive()
        .typed_value::<i32>()
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!("Missing size for field '{}'", name))
        })? as usize;

    log::debug!(
        "[utils::extract_vortex_struct_field] Extracting Vortex struct field '{}' took {:?}",
        name,
        start.elapsed()
    );
    let sliced = list
        .elements()
        .slice(offset..offset + size)
        .map_err(VortexRdfError::Vortex)?;
    Ok(sliced)
}

pub fn extract_vortex_struct_field_optional(
    vortex_struct: &StructArray,
    name: &str,
) -> Option<ArrayRef> {
    extract_vortex_struct_field(vortex_struct, name).ok()
}

/*
* Test functions for benchmarks
*/

// Generate a stream of RDF quads for benchmarking
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<Quad>> {
    const EX: &str = "http://example.org/";

    stream::iter((0..size).map(|i| {
        let subject =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50)));
        let graph = GraphName::NamedNode(NamedNode::new_unchecked(format!("{}graph", EX)));

        Ok(Quad::new(subject, predicate, object, graph))
    }))
}

#[derive(Debug)]
pub struct VortexFileSanity {
    pub file_len: u64,
    pub start_magic: [u8; 4],
    pub end_magic: [u8; 4],
    pub version: u16,
    pub postscript_len: u16,
}
