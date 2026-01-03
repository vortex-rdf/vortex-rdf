use vortex_array::ArrayRef;
use vortex_dtype::{DType, Nullability, PType};
use vortex_scalar::Scalar;
use vortex_array::ToCanonical;
use crate::error::{VortexRdfError, Result};

pub fn quads_array_from_vortex_array(vortex_array: ArrayRef) -> Result<ArrayRef> {
    let start = std::time::Instant::now();
    let vortex_struct = vortex_array.to_struct();
    let quads_list_ref = vortex_struct
        .fields()
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing quads field".to_string()))?
        .clone();
    
    let quads_list_view = quads_list_ref.to_listview();
    let offsets_scalar = quads_list_view.offsets().scalar_at(0);
    let sizes_scalar = quads_list_view.sizes().scalar_at(0);
    
    let quads_offset: usize = offsets_scalar
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)
        .and_then(
            |scalar: Scalar| scalar
                .as_primitive()
                .typed_value::<i32>()
                .ok_or_else(|| VortexRdfError::Deserialization("Missing quads offset".to_string()))
        )
        .map(|offset| offset as usize)?;
    let quads_size: usize = sizes_scalar
        .cast(&DType::Primitive(PType::I32, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)
        .and_then(
            |scalar: Scalar| scalar
                .as_primitive()
                .typed_value::<i32>()
                .ok_or_else(|| VortexRdfError::Deserialization("Missing quads size".to_string()))
        )
        .map(|size| size as usize)?;
    let quads_array = quads_list_view
        .elements()
        .slice(quads_offset..quads_offset + quads_size);
    log::debug!("[utils::quads_array_from_vortex_array] Quads array extraction took {:?}", start.elapsed());
    Ok(quads_array)
}

pub fn parse_named_node(s: &str) -> Result<oxrdf::NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    oxrdf::NamedNode::new(s).map_err(|e| VortexRdfError::Deserialization(format!("Invalid NamedNode '{}': {}", s, e)))
}

pub fn parse_blank_node(s: &str) -> Result<oxrdf::BlankNode> {
    let s = s.trim_start_matches("_:");
    oxrdf::BlankNode::new(s).map_err(|e| VortexRdfError::Deserialization(format!("Invalid BlankNode '{}': {}", s, e)))
}

pub fn parse_subject(s: &str) -> Result<oxrdf::Subject> {
    if s.starts_with("_:") {
        Ok(oxrdf::Subject::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(oxrdf::Subject::NamedNode(parse_named_node(s)?))
    }
}

pub fn parse_term(s: &str) -> Result<oxrdf::Term> {
    if s.starts_with('_') {
        Ok(oxrdf::Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        // Simple literal parsing for now
        let val = s.trim_matches('"');
        Ok(oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(val)))
    } else {
        Ok(oxrdf::Term::NamedNode(parse_named_node(s)?))
    }
}

pub fn parse_graph_name(s: &str) -> Result<oxrdf::GraphName> {
    if s.is_empty() || s == "default" {
        Ok(oxrdf::GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(oxrdf::GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(oxrdf::GraphName::NamedNode(parse_named_node(s)?))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum, Debug)]
pub enum Format {
    /// N-Triples (.nt)
    #[value(name = "nt")]
    NTriples,
    /// N-Quads (.nq)
    #[value(name = "nq")]
    NQuads,
    /// Turtle (.ttl)
    #[value(name = "ttl")]
    Turtle,
    /// TriG (.trig)
    #[value(name = "trig")]
    TriG,
    /// RDF/XML (.rdf, .xml)
    #[value(name = "rdf")]
    RdfXml,
    /// JSON-LD (.jsonld, .json)
    #[value(name = "jsonld")]
    JsonLd,
    /// N3 (.n3)
    #[value(name = "n3")]
    N3,
}

impl From<Format> for oxrdfio::RdfFormat {
    fn from(f: Format) -> Self {
        match f {
            Format::NTriples => oxrdfio::RdfFormat::NTriples,
            Format::NQuads => oxrdfio::RdfFormat::NQuads,
            Format::Turtle => oxrdfio::RdfFormat::Turtle,
            Format::TriG => oxrdfio::RdfFormat::TriG,
            Format::RdfXml => oxrdfio::RdfFormat::RdfXml,
            Format::JsonLd => oxrdfio::RdfFormat::JsonLd {
                profile: Default::default(),
            },
            Format::N3 => oxrdfio::RdfFormat::N3,
        }
    }
}

pub fn detect_format(path: &Option<std::path::PathBuf>) -> Option<oxrdfio::RdfFormat> {
    let path = path.as_ref()?;
    let ext = path.extension()?.to_str()?;
    oxrdfio::RdfFormat::from_extension(ext)
}