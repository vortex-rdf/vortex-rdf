use anyhow::{anyhow, Context, Result};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
use clap::{Parser, ValueEnum};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{stdin, stdout, Read, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Vortex-RDF CLI: Convert between RDF and Vortex-RDF format"
)]
struct Cli {
    /// Action to perform
    #[arg(value_enum)]
    action: Action,

    /// Input file path (defaults to stdin)
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Output file path (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// RDF Format (auto-detected from file extension if not provided)
    #[arg(short, long, value_enum)]
    format: Option<Format>,

    /// Subject pattern
    #[arg(long)]
    subject: Option<String>,

    /// Predicate pattern
    #[arg(long)]
    predicate: Option<String>,

    /// Object pattern
    #[arg(long)]
    object: Option<String>,

    /// Graph pattern
    #[arg(long)]
    graph: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Action {
    /// Convert from RDF to Vortex-RDF
    Serialize,
    /// Convert from Vortex-RDF to RDF
    Deserialize,
    /// Filter Vortex-RDF store by pattern
    Match,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Format {
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

impl From<Format> for RdfFormat {
    fn from(f: Format) -> Self {
        match f {
            Format::NTriples => RdfFormat::NTriples,
            Format::NQuads => RdfFormat::NQuads,
            Format::Turtle => RdfFormat::Turtle,
            Format::TriG => RdfFormat::TriG,
            Format::RdfXml => RdfFormat::RdfXml,
            Format::JsonLd => RdfFormat::JsonLd {
                profile: Default::default(),
            },
            Format::N3 => RdfFormat::N3,
        }
    }
}

fn detect_format(path: &Option<PathBuf>) -> Option<RdfFormat> {
    let path = path.as_ref()?;
    let ext = path.extension()?.to_str()?;
    RdfFormat::from_extension(ext)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.action {
        Action::Serialize => {
            let format = cli
                .format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&cli.input))
                .ok_or_else(|| {
                    anyhow!("Could not detect RDF format. Please specify it with --format")
                })?;

            let mut reader: Box<dyn Read> = match &cli.input {
                Some(p) => Box::new(File::open(p).context("Failed to open input file")?),
                None => Box::new(stdin()),
            };

            let writer: Box<dyn Write> = match &cli.output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            vortex_rdf_core::serialize(&mut reader, writer, format)
                .context("Failed to serialize to Vortex")?;
        }
        Action::Deserialize => {
            let format = cli
                .format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&cli.output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &cli.output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            let reader: Box<dyn Read> = match &cli.input {
                Some(p) => Box::new(File::open(p).context("Failed to open input file")?),
                None => Box::new(stdin()),
            };

            vortex_rdf_core::deserialize(reader, writer, format)
                .context("Failed to deserialize from Vortex")?;
        }
        Action::Match => {
            let input_path = cli
                .input
                .as_ref()
                .ok_or_else(|| anyhow!("Input file is required for match action"))?;

            let store = vortex_rdf_core::VortexRdfStore::from_file(input_path)
                .context("Failed to load Vortex store via mmap")?;

            let mut subject = None;
            if let Some(s) = &cli.subject {
                subject = Some(parse_subject(s)?);
            }

            let mut predicate = None;
            if let Some(p) = &cli.predicate {
                predicate = Some(parse_named_node(p)?);
            }

            let mut object = None;
            if let Some(o) = &cli.object {
                object = Some(parse_term(o)?);
            }

            let mut graph = None;
            if let Some(g) = &cli.graph {
                graph = Some(parse_graph_name(g)?);
            }

            let filtered = store
                .match_pattern(
                    subject.as_ref(),
                    predicate.as_ref(),
                    object.as_ref(),
                    graph.as_ref(),
                )
                .context("Failed to apply match pattern")?;

            let format = cli
                .format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&cli.output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &cli.output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            // If output is .vortex, we should serialize as Vortex
            if let Some(p) = &cli.output {
                if p.extension().map(|e| e == "vortex").unwrap_or(false) {
                    vortex_rdf_core::ser::write_array_to_ipc(filtered.root, writer)
                        .context("Failed to write Vortex output")?;
                    return Ok(());
                }
            }

            // Otherwise default to RDF
            let mut serializer = oxrdfio::RdfSerializer::from_format(format).for_writer(writer);
            for quad_res in filtered.quads()? {
                let quad = quad_res?;
                serializer
                    .serialize_quad(&quad)
                    .map_err(|e| anyhow!("Serialization error: {}", e))?;
            }
            serializer
                .finish()
                .map_err(|e| anyhow!("Serialization finish error: {}", e))?;
        }
    }

    Ok(())
}

fn parse_named_node(s: &str) -> Result<oxrdf::NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    oxrdf::NamedNode::new(s).map_err(|e| anyhow!("Invalid NamedNode '{}': {}", s, e))
}

fn parse_blank_node(s: &str) -> Result<oxrdf::BlankNode> {
    let s = s.trim_start_matches("_:");
    oxrdf::BlankNode::new(s).map_err(|e| anyhow!("Invalid BlankNode '{}': {}", s, e))
}

fn parse_subject(s: &str) -> Result<oxrdf::Subject> {
    if s.starts_with("_:") {
        Ok(oxrdf::Subject::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(oxrdf::Subject::NamedNode(parse_named_node(s)?))
    }
}

fn parse_term(s: &str) -> Result<oxrdf::Term> {
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

fn parse_graph_name(s: &str) -> Result<oxrdf::GraphName> {
    if s.is_empty() || s == "default" {
        Ok(oxrdf::GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(oxrdf::GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(oxrdf::GraphName::NamedNode(parse_named_node(s)?))
    }
}

