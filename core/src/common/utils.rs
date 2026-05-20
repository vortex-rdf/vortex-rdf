use crate::error::{Result, VortexRdfError};
use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term};
use oxrdfio::{RdfFormat, RdfParser};
use vortex_array::ArrayRef;
use vortex_array::ToCanonical;
use vortex_array::arrays::StructArray;
use vortex_dtype::{DType, Nullability, PType};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

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
        .map_err(|e| VortexRdfError::Deserialization(format!("Invalid BlankNode '{}': {}", s, e)))
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
    let start = std::time::Instant::now();

    // Find index by name
    let names = vortex_struct.names();
    let idx = names
        .iter()
        .position(|n| n.as_ref() == name)
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!("Field '{}' not found in struct", name))
        })?;

    let fields = vortex_struct.fields();
    let list_ref = fields
        .get(idx)
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Missing field '{}'", name)))?
        .clone();

    let list = list_ref.to_listview();

    let offset = list
        .offsets()
        .scalar_at(0)
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .as_primitive()
        .typed_value::<i32>()
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!("Missing offset for field '{}'", name))
        })? as usize;

    let size = list
        .sizes()
        .scalar_at(0)
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
    Ok(list.elements().slice(offset..offset + size))
}

pub fn extract_vortex_struct_field_optional(
    vortex_struct: &StructArray,
    name: &str,
) -> Option<ArrayRef> {
    let names = vortex_struct.names();
    let idx = names.iter().position(|n| n.as_ref() == name)?;
    let fields = vortex_struct.fields();
    let list_ref = fields.get(idx)?.clone();
    let list = list_ref.to_listview();

    let offset = list
        .offsets()
        .scalar_at(0)
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .ok()?;
    let offset = offset.as_primitive().typed_value::<i32>()? as usize;

    let size = list
        .sizes()
        .scalar_at(0)
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .ok()?;
    let size = size.as_primitive().typed_value::<i32>()? as usize;

    Some(list.elements().slice(offset..offset + size))
}

/*
* Test functions for benchmarks
*/

// Generate a stream of RDF quads for benchmarking
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<Quad>> {
    const EX: &str = "http://example.org/";

    stream::iter((0..size).map(|i| {
        let subject = Subject::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
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

pub fn check_vtxf_sanity(path: impl AsRef<Path>) -> std::io::Result<VortexFileSanity> {
    const MAGIC: [u8; 4] = *b"VTXF";
    const EOF_SIZE: u64 = 8;

    let path = path.as_ref();
    let mut f = File::open(path)?;
    let file_len = f.metadata()?.len();

    if file_len < 4 + EOF_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("File too small to be a Vortex file: {file_len} bytes"),
        ));
    }

    // Read start magic
    let mut start_magic = [0u8; 4];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut start_magic)?;

    // Read EOF marker (last 8 bytes): [u16 version][u16 postscript_len][u32 magic]
    let mut eof = [0u8; 8];
    f.seek(SeekFrom::End(-(EOF_SIZE as i64)))?;
    f.read_exact(&mut eof)?;

    let version = u16::from_le_bytes([eof[0], eof[1]]);
    let postscript_len = u16::from_le_bytes([eof[2], eof[3]]);
    let end_magic = [eof[4], eof[5], eof[6], eof[7]];

    // Basic checks from spec
    if start_magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid Vortex start magic: {:?} (expected {:?})",
                start_magic, MAGIC
            ),
        ));
    }

    if end_magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid Vortex end magic: {:?} (expected {:?})",
                end_magic, MAGIC
            ),
        ));
    }

    // Postscript is located immediately before the EOF marker, and its length is u16.
    // Spec guarantees postscript length won't exceed 65528 bytes. We'll sanity-check bounds. 【1-01ea59】
    let ps_len_u64 = postscript_len as u64;
    if ps_len_u64 > 65528 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Postscript length too large: {}", postscript_len),
        ));
    }
    if ps_len_u64 + EOF_SIZE > file_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "Postscript length {} + EOF {} exceeds file length {}",
                ps_len_u64, EOF_SIZE, file_len
            ),
        ));
    }
    Ok(VortexFileSanity {
        file_len,
        start_magic,
        end_magic,
        version,
        postscript_len,
    })
}