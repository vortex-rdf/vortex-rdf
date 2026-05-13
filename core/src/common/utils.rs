use oxrdf::{
    Quad, 
    Term,
    NamedNode,
    BlankNode,
    Subject,
    Literal,
    GraphName
};
use oxrdfio::{RdfFormat, RdfParser};
use futures::{stream, Stream};
use crate::error::{VortexRdfError, Result};
use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
use vortex_dtype::{DType, Nullability, PType};
use vortex_array::ToCanonical;

pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid NamedNode '{}': {}", s, e)
        ))
}

pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    let s = s.trim_start_matches("_:");
    BlankNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid BlankNode '{}': {}", s, e)
        ))
}

pub fn parse_subject(s: &str) -> Result<Subject> {
    if s.starts_with("_:") {
        Ok(Subject::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(Subject::NamedNode(parse_named_node(s)?))
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
    let iter = parser.for_reader(reader).map(|x| {
        x.map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e)))
    });
    stream::iter(iter)
}

/*
 This function unpacks a certain Vortex ListArray from a Vortex StructArray.
 It assumes that the ListArray has been packed as a single element array.
*/
pub fn extract_vortex_struct_field(
    vortex_struct: &StructArray,
    name: &str
) -> Result<ArrayRef> {
    let start = std::time::Instant::now();
    
    // Find index by name
    let names = vortex_struct.names();
    let idx = names.iter().position(|n| n.as_ref() == name)
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Field '{}' not found in struct", name)))?;

    let fields = vortex_struct.fields();
    let list_ref = fields.get(idx)
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Missing field '{}'", name)))?
        .clone();

    let list = list_ref.to_listview();
    
    let offset = list.offsets().scalar_at(0)
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .as_primitive()
        .typed_value::<i32>()
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Missing offset for field '{}'", name)))? as usize;
        
    let size = list.sizes().scalar_at(0)
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .as_primitive()
        .typed_value::<i32>()
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Missing size for field '{}'", name)))? as usize;
    
    log::debug!("[utils::extract_vortex_struct_field] Extracting Vortex struct field '{}' took {:?}", name, start.elapsed());
    Ok(list.elements().slice(offset..offset + size))
}

/*
* Test functions for benchmarks
*/

// Generate a stream of RDF quads for benchmarking
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<Quad>> {
    const EX: &str = "http://example.org/";
    
    stream::iter((0..size).map(|i| {
        let subject = Subject::NamedNode(
            NamedNode::new_unchecked(format!("{}subject/{}", EX, i))
        );
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(
            NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50))
        );
        let graph = GraphName::NamedNode(
            NamedNode::new_unchecked(format!("{}graph", EX))
        );
        
        Ok(Quad::new(subject, predicate, object, graph))
    }))
}