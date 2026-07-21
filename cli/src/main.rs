use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use log::{debug, info};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{Read, Write, stdin, stdout};
use std::path::PathBuf;
use std::time::Instant;

use tokio::fs::File as TokioFile;

use vortex_rdf_core::common::formats::{Format, detect_format};
use vortex_rdf_core::common::utils::{
    parse_graph_name, parse_named_node, parse_quads_from_reader, parse_subject, parse_term,
};
use vortex_rdf_core::{
    BuilderStrategy, IndexType, LayoutStrategy, SortedInMemoryBuilder, SortedStreamBuilder,
    UnsortedStreamBuilder, VortexRdfStore,
    io::{deserialize, quads_stream_to_vortex_writer_with_builder},
};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Vortex-RDF CLI: Convert between RDF and Vortex-RDF format"
)]
struct Cli {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Convert from RDF to Vortex-RDF
    Serialize {
        /// Column layout strategy
        #[arg(long, value_enum, default_value = "default")]
        layout: LayoutStrategy,

        /// Secondary indexes to build (can be specified multiple times)
        #[arg(long, value_enum)]
        indexes: Vec<IndexType>,

        /// Input file path (defaults to stdin)
        #[arg(short, long)]
        input: Option<PathBuf>,

        /// Output file path (required)
        #[arg(short, long)]
        output: PathBuf,

        /// RDF Format (auto-detected from file extension if not provided)
        #[arg(short, long, value_enum)]
        format: Option<Format>,

        /// Builder strategy to use when serializing (defaults to unsorted-stream)
        #[arg(short, long, value_enum, default_value = "unsorted-stream")]
        builder_strategy: BuilderStrategy,
    },
    /// Convert from Vortex-RDF to RDF
    Deserialize {
        /// Input file path (defaults to stdin)
        #[arg(short, long)]
        input: Option<PathBuf>,

        /// Output file path (defaults to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// RDF Format (auto-detected from file extension if not provided)
        #[arg(short, long, value_enum)]
        format: Option<Format>,
    },
    /// Filter Vortex-RDF store by pattern
    Match {
        /// Input file path (required)
        #[arg(short, long)]
        input: PathBuf,

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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.action {
        Action::Serialize {
            layout,
            indexes,
            input,
            output,
            format,
            builder_strategy,
        } => {
            let start = Instant::now();
            let format = format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&input))
                .ok_or_else(|| {
                    anyhow!("Could not detect RDF format. Please specify it with --format")
                })?;

            let reader: Box<dyn Read + Send> = match &input {
                Some(p) => Box::new(File::open(p).context("Failed to open input file")?),
                None => Box::new(stdin()),
            };
            let writer = TokioFile::create(&output)
                .await
                .context("Failed to create output file")?;
            let quads_stream = parse_quads_from_reader(reader, format);

            // Chunks are streamed into the Vortex writer as they are built;
            // streaming-capable builders never materialize the full dataset.
            match builder_strategy {
                BuilderStrategy::UnsortedStream => {
                    quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
                        quads_stream,
                        writer,
                        layout,
                        indexes,
                    )
                    .await
                }
                BuilderStrategy::SortedInMemory => {
                    quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
                        quads_stream,
                        writer,
                        layout,
                        indexes,
                    )
                    .await
                }
                BuilderStrategy::SortedStream => {
                    quads_stream_to_vortex_writer_with_builder::<SortedStreamBuilder, _, _>(
                        quads_stream,
                        writer,
                        layout,
                        indexes,
                    )
                    .await
                }
            }
            .context("Failed to serialize to Vortex")?;
            info!("Fully serialized to Vortex-RDF in {:?}", start.elapsed());
        }

        Action::Deserialize {
            input,
            output,
            format,
        } => {
            let start = Instant::now();
            let format = format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            match &input {
                Some(path) => {
                    let store = VortexRdfStore::from_file(path)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    deserialize(store, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
                None => {
                    let mut buffer = Vec::new();
                    stdin()
                        .read_to_end(&mut buffer)
                        .context("Failed to read from stdin")?;
                    let store = VortexRdfStore::from_bytes(&buffer)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    deserialize(store, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
            }
            info!("Deserialization took {:?}", start.elapsed());
        }

        Action::Match {
            input,
            output,
            format,
            subject,
            predicate,
            object,
            graph,
        } => {
            let start = Instant::now();

            let subject_node = subject.as_deref().map(parse_subject).transpose()?;
            let predicate_node = predicate.as_deref().map(parse_named_node).transpose()?;
            let object_node = object.as_deref().map(parse_term).transpose()?;
            let graph_node = graph.as_deref().map(parse_graph_name).transpose()?;

            let output_format = format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            let is_vortex = input.extension().map(|e| e == "vortex").unwrap_or(false);

            if is_vortex {
                let load_start = Instant::now();
                let store = VortexRdfStore::from_file(&input)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                debug!("Opened Vortex file in {:?}", load_start.elapsed());

                let match_start = Instant::now();
                let filtered = store
                    .match_pattern(
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                    )
                    .await
                    .context("Failed to match pattern")?;
                debug!("Applying match pattern took {:?}", match_start.elapsed());

                deserialize(filtered, writer, output_format)
                    .await
                    .context("Failed to deserialize filtered results")?;
            } else {
                let load_start = Instant::now();
                let input_format = format
                    .map(RdfFormat::from)
                    .or_else(|| detect_format(&Some(input.clone())))
                    .ok_or_else(|| {
                        anyhow!("Could not detect RDF format. Please specify it with --format")
                    })?;

                let reader = Box::new(File::open(&input).context("Failed to open input file")?);
                let quads_stream = parse_quads_from_reader(reader, input_format);

                let arr = VortexRdfStore::build_vortex_array(quads_stream).await?;
                let store = VortexRdfStore::new(arr).map_err(|e| anyhow::anyhow!(e))?;
                debug!("Vortex store built in {:?}", load_start.elapsed());

                let match_start = Instant::now();
                let filtered = store
                    .match_pattern(
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                    )
                    .await
                    .context("Failed to match pattern")?;
                debug!("Applying match pattern took {:?}", match_start.elapsed());

                deserialize(filtered, writer, output_format)
                    .await
                    .context("Failed to deserialize filtered results")?;
            }

            info!("Full matching operation took {:?}", start.elapsed());
        }
    }

    Ok(())
}
