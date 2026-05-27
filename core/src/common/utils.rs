use crate::error::{Result, VortexRdfError};
use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};
use vortex_array::{ArrayRef, LEGACY_SESSION, VortexSessionExecute};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::primitive::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_array::arrays::listview::{ListViewArray, ListViewArrayExt};

/// Parses a string representation of an RDF named node (URI), stripping optional `<` and `>` boundaries.
pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    // Trim any wrapping angle brackets commonly used in N-Triples/Turtle notation.
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("Invalid NamedNode '{}': {}", s, e)))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix if present.
pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    // Trim the standard RDF blank node prefix '_:'.
    let s = s.trim_start_matches("_:");
    BlankNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("Invalid BlankNode '{}': {}", s, e)))
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
    let iter = parser
        .for_reader(reader)
        .map(|x| x.map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e))));
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

/// Decode a single chunk `ArrayRef` (a StructArray with fields s,p,o,g)
/// into `Quad`s using the pre-decoded `values` view.
pub fn decode_chunk(chunk: &ArrayRef, values: &VarBinViewArray) -> Vec<Result<Quad>> {
    // 1. Establish an execution context to resolve/evaluate the compressed Vortex arrays.
    let mut ctx = LEGACY_SESSION.create_execution_ctx();
    
    // 2. Evaluate and canonicalize the chunk array into a standard StructArray.
    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    // 3. Extract subject, predicate, object, and graph ID columns using flat primitive extractor.
    let s_ids = match extract_flat_primitive_column(&struct_arr, 0) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let p_ids = match extract_flat_primitive_column(&struct_arr, 1) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let o_ids = match extract_flat_primitive_column(&struct_arr, 2) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };
    let g_ids = match extract_flat_primitive_column(&struct_arr, 3) {
        Ok(ids) => ids,
        Err(e) => return vec![Err(e)],
    };

    // 4. Iterate over each row in the chunk to decode ID sequences into RDF Terms and Quads.
    (0..s_ids.len()).map(|i| {
        // Retrieve the u32 index for each field and cast to usize.
        let s_id = s_ids.as_slice::<u32>()[i] as usize;
        let p_id = p_ids.as_slice::<u32>()[i] as usize;
        let o_id = o_ids.as_slice::<u32>()[i] as usize;
        let g_id = g_ids.as_slice::<u32>()[i] as usize;

        // Perform zero-copy dictionary lookup to get raw string representation of each term.
        let s_b = values.bytes_at(s_id); let s_s = String::from_utf8_lossy(s_b.as_ref());
        let p_b = values.bytes_at(p_id); let p_s = String::from_utf8_lossy(p_b.as_ref());
        let o_b = values.bytes_at(o_id); let o_s = String::from_utf8_lossy(o_b.as_ref());
        let g_b = values.bytes_at(g_id); let g_s = String::from_utf8_lossy(g_b.as_ref());

        // Parse the serialized term strings back into structural RDF Term types.
        let s_term = get_as_term(&s_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid subject ID {s_id}")))?;
        let p_term = get_as_term(&p_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid predicate ID {p_id}")))?;
        let o_term = get_as_term(&o_s)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid object ID {o_id}")))?;

        // Map the graph string to the appropriate structural GraphName.
        let g_name = if g_s.is_empty() || g_s == "[]" {
            GraphName::DefaultGraph
        } else {
            match get_as_term(&g_s) {
                Some(Term::NamedNode(n)) => GraphName::NamedNode(n),
                Some(Term::BlankNode(b)) => GraphName::BlankNode(b),
                _ => GraphName::DefaultGraph,
            }
        };

        // Construct standard structural components, validating subject and predicate constraints.
        let subject = match s_term {
            Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
            Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b),
            _ => return Err(VortexRdfError::Deserialization("Invalid subject type".into())),
        };
        let predicate = match p_term {
            Term::NamedNode(n) => n,
            _ => return Err(VortexRdfError::Deserialization("Invalid predicate type".into())),
        };

        // Assemble and return the complete structural RDF Quad.
        Ok(Quad::new(subject, predicate, o_term, g_name))
    }).collect()
}

/// Extract and decode a dictionary column from a serialized StructArray.
/// E.g., extracts "_dict_values" and decodes any dictionary-encoded wrappers around it.
pub fn extract_dictionary_column(
    dict_struct: &StructArray,
    key: &str,
) -> Result<ArrayRef> {
    let arr = dict_struct.unmasked_field_by_name(key)
        .map_err(|_| VortexRdfError::Deserialization(
            format!("Field '{}' not found in dict struct", key)
        ))?;
    crate::common::indexes::array_from_dict_column(arr)
}

pub fn extract_vortex_struct_field_optional(
    vortex_struct: &StructArray,
    name: &str,
) -> Option<ArrayRef> {
    extract_vortex_struct_field(vortex_struct, name).ok()
}

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

/*
* Test functions for benchmarks
*/

/// Helper function to generate a stream of mock RDF quads for benchmark and test workflows.
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
