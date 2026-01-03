use anyhow::{anyhow, Context, Result};
use mimalloc::MiMalloc;
use futures::StreamExt;
use log::{debug, info};

use clap::{Parser, ValueEnum};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{stdin, stdout, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use vortex_rdf_core::{deserialize, serialize, VortexRdfStore};
use vortex_rdf_core::utils::*;
use vortex_rdf_core::io::*;

/*
 As indicated by vortex docs:
 https://docs.rs/vortex/latest/vortex/index.html#performance-optimization
*/
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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

    /// Output file path (required for serialize action, defaults to stdout for others)
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.action {
        Action::Serialize => {
            let start = Instant::now();
            let format = cli
                .format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&cli.input))
                .ok_or_else(|| {
                    anyhow!("Could not detect RDF format. Please specify it with --format")
                })?;

            let reader: Box<dyn Read + Send> = match &cli.input {
                Some(p) => Box::new(File::open(p).context("Failed to open input file")?),
                None => Box::new(stdin()),
            };

            let output_path = cli.output.as_ref()
                .ok_or_else(|| anyhow!("Output file is required for serialize action"))?;
            let writer = tokio::fs::File::create(output_path)
                .await
                .context("Failed to create output file")?;
            serialize(reader, writer, format)
                .await
                .context("Failed to serialize to Vortex")?;
            info!("Serialization took {:?}", start.elapsed());
        }
        Action::Deserialize => {
            let start = Instant::now();
            let format = cli
                .format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&cli.output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &cli.output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            match &cli.input {
                Some(p) => {
                     // From file path
                     deserialize(p.clone(), writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
                None => {
                    // From stdin (read to buffer first)
                    let mut buffer = Vec::new();
                    stdin().read_to_end(&mut buffer).context("Failed to read from stdin")?;
                    let buffer = vortex::buffer::Buffer::from(buffer);
                    deserialize(buffer, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
            }
            info!("Deserialization took {:?}", start.elapsed());
        }
        Action::Match => {
            let start = Instant::now();
            let input_path = cli
                .input
                .as_ref()
                .ok_or_else(|| anyhow!("Input file is required for match action"))?;

            let load_start = Instant::now();
            let store = VortexRdfStore::from_file(input_path)
                .await
                .context("Failed to load Vortex-RDF store from file")?;
            debug!("Loading vortex file to store took {:?}", load_start.elapsed());

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

            let match_start = Instant::now();
            let filtered = store
                .match_pattern(
                    subject.as_ref(),
                    predicate.as_ref(),
                    object.as_ref(),
                    graph.as_ref(),
                )
                .await
                .context("Failed to apply match pattern")?;
            debug!("Applying match pattern took {:?}", match_start.elapsed());

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
                    let writer = tokio::fs::File::create(p)
                        .await
                        .context("Failed to create output file for Vortex")?;
                    
                    // Re-serialize the quads to rebuild the dictionary.
                    // The filtered store currently shares the original (likely large) dictionary.
                    // By decoding and re-encoding, we create a fresh, minimal dictionary containing
                    // only the terms actually used in the filtered results.
                    // For some reason directly re-writing to a file is much slower.
                    let start_re_ser = std::time::Instant::now();
                    
                    let new_array = ser::encode_quads(filtered.quads()?).await?;
                    debug!("Re-serialization took {:?}", start_re_ser.elapsed());

                    ser::write_array_to_vortex(new_array, writer)
                        .await
                        .context("Failed to write Vortex output")?;
                    info!("Matching took {:?}", start.elapsed());
                    return Ok(());
                }
            }

            // Otherwise default to RDF
            let mut serializer = oxrdfio::RdfSerializer::from_format(format).for_writer(writer);
            let mut quads_stream = filtered.quads()?;
            while let Some(quad_res) = quads_stream.next().await {
                let quad = quad_res?;
                serializer
                    .serialize_quad(&quad)
                    .map_err(|e| anyhow!("Serialization error: {}", e))?;
            }
            serializer
                .finish()
                .map_err(|e| anyhow!("Serialization finish error: {}", e))?;
            info!("Matching took {:?}", start.elapsed());
        }
    }

    Ok(())
}
