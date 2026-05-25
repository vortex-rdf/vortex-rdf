use oxrdf::{
    Quad, 
    Term,
    NamedNode,
    BlankNode,
    NamedOrBlankNode,
    Literal,
    GraphName
};
use oxrdfio::{RdfFormat, RdfParser};
use futures::{stream, Stream};
use crate::error::{VortexRdfError, Result};
use vortex_array::{LEGACY_SESSION, VortexSessionExecute};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::primitive::PrimitiveArray;

/// Parses a string representation of an RDF named node (URI), stripping optional `<` and `>` boundaries.
pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    // Trim any wrapping angle brackets commonly used in N-Triples/Turtle notation.
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid NamedNode '{}': {}", s, e)
        ))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix if present.
pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    // Trim the standard RDF blank node prefix '_:'.
    let s = s.trim_start_matches("_:");
    BlankNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid BlankNode '{}': {}", s, e)
        ))
}

/// Parses an RDF subject node, which can either be a NamedNode (URI) or a BlankNode.
pub fn parse_subject(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node(s)?))
    }
}

/// Parses an arbitrary RDF term (blank node, literal, or named node) from its string form.
pub fn parse_term(s: &str) -> Result<Term> {
    if s.starts_with('_') {
        Ok(Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        // Simple literal string parsing.
        // TODO: Add support for multi-line literals
        let val = s.trim_matches('"');
        Ok(Term::Literal(Literal::new_simple_literal(val)))
    } else {
        Ok(Term::NamedNode(parse_named_node(s)?))
    }
}

/// Parses an RDF graph name, which can be the default graph, a named node, or a blank node.
pub fn parse_graph_name(s: &str) -> Result<GraphName> {
    if s.is_empty() || s == "default" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node(s)?))
    }
}

/// Reconstructs a full structural oxrdf `Term` from its raw serialized string representation.
/// Handles URIs, Blank Nodes, simple literals, language-tagged literals, and typed literals.
pub fn get_as_term(s: &str) -> Option<Term> {
    if s.starts_with('<') {
        // Parse named nodes / URIs.
        Some(Term::NamedNode(
            NamedNode::new(s.trim_matches(|c| c == '<' || c == '>')).ok()?,
        ))
    } else if s.starts_with("_:") {
        // Parse blank nodes.
        Some(Term::BlankNode(
            BlankNode::new(s.trim_start_matches("_:")).ok()?,
        ))
    } else if s.starts_with('"') {
        // Parse literals (simple, typed, or language-tagged).
        if s.contains("^^") {
            // Typed literal: "value"^^<datatype>
            let parts: Vec<&str> = s.split("^^").collect();
            let val = parts[0].trim_matches('"');
            let dt = parts[1].trim_matches(|c| c == '<' || c == '>');
            Some(Term::Literal(Literal::new_typed_literal(
                val,
                NamedNode::new(dt).ok()?,
            )))
        } else if s.contains('@') {
            // Language-tagged literal: "value"@lang
            let last_at = s.rfind('@')?;
            let val = s[..last_at].trim_matches('"');
            let lang = &s[last_at + 1..];
            Some(Term::Literal(
                Literal::new_language_tagged_literal(val, lang).ok()?,
            ))
        } else {
            // Simple plain string literal: "value"
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

/// Parses a stream of RDF quads from any reader using the specified RDF format (Turtle, N-Triples, etc.).
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

/// Retrieve and evaluate/canonicalize a root-level flat column within a StructArray.
/// Extracts a direct column by index, resolving any structural compression in the process.
pub fn extract_flat_primitive_column(
    vortex_struct: &StructArray,
    idx: usize,
) -> Result<PrimitiveArray> {
    // 1. Create a legacy session context for executing/evaluating the possibly compressed array field.
    let mut ctx = LEGACY_SESSION.create_execution_ctx();
    
    // 2. Fetch the unmasked field array ref at the target index.
    let col = vortex_struct.unmasked_field(idx);
    
    // 3. Resolve the array reference into a canonical flat PrimitiveArray of integers.
    col.clone().execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)
}

/*
* Test functions for benchmarks
*/

/// Helper function to generate a stream of mock RDF quads for benchmark and test workflows.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<Quad>> {
    const EX: &str = "http://example.org/";
    
    stream::iter((0..size).map(|i| {
        let subject = NamedOrBlankNode::NamedNode(
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