use anyhow::{anyhow, Context, Result};
use log::{debug, info};
use clap::{Parser, Subcommand};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{stdin, stdout, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use tokio::fs::{File as TokioFile};

use vortex::buffer::Buffer;


use vortex_rdf_core::{
    io::{serialize, deserialize, load_vortex_file_ref, load_vortex_file_path},
    index::{SimpleDictionary, ChainedHash},
    VortexRdfStore
};
use vortex_rdf_core::common::formats::{Format, detect_format};
use vortex_rdf_core::common::indexes::{IndexType, detect_index_type};
use vortex_rdf_core::common::utils::{
    parse_subject,
    parse_named_node,
    parse_term,
    parse_graph_name,
    parse_quads_from_reader
};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Vortex-RDF CLI: Convert between RDF and Vortex-RDF format"
)]
struct Cli {
    /// Action to perform
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Convert from RDF to Vortex-RDF
    Serialize {
        #[arg(long, value_enum)]
        index_type: IndexType,
        
        /// Input file path (defaults to stdin)
        #[arg(short, long)]
        input: Option<PathBuf>,
        
        /// Output file path (required)
        #[arg(short, long)]
        output: PathBuf,
        
        /// RDF Format (auto-detected from file extension if not provided)
        #[arg(short, long, value_enum)]
        format: Option<Format>,
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

        /// Index type for non-Vortex input
        #[arg(short, long, value_enum)]
        index_type: Option<IndexType>,
        
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
        Action::Serialize { index_type, input, output, format } => {
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
            
            let vortex_array = match index_type {
                IndexType::SimpleDictionary 
                    => VortexRdfStore::<SimpleDictionary>::build_vortex_index(quads_stream).await?,
                IndexType::ChainedHash 
                    => VortexRdfStore::<ChainedHash>::build_vortex_index(quads_stream).await?,
            };

            serialize(vortex_array, writer)
                .await
                .context("Failed to serialize to Vortex")?;
            info!("Fully serialized to Vortex-RDF in {:?}", start.elapsed());
        }
        Action::Deserialize { input, output, format } => {
            let start = Instant::now();
            let format = format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            let vortex_index = match &input {
                Some(p) => load_vortex_file_path(p)
                    .await
                    .context("Failed to read Vortex index from file")?,
                None => {
                    let mut buffer = Vec::new();
                    stdin().read_to_end(&mut buffer).context("Failed to read from stdin")?;
                    load_vortex_file_ref(Buffer::from(buffer))
                        .await
                        .context("Failed to read Vortex index from buffer")?
                }
            };
            
            let detect_start = Instant::now();
            match detect_index_type(&vortex_index) {
                IndexType::SimpleDictionary => {
                    log::debug!("[cli::deserialize] Dictionary index detected in {:?}", detect_start.elapsed());
                    let store = VortexRdfStore::<SimpleDictionary>::new(vortex_index)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    deserialize(store, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                },
                IndexType::ChainedHash => {
                    log::debug!("[cli::deserialize] ChainedHash index detected in {:?}", detect_start.elapsed());
                    let store = VortexRdfStore::<ChainedHash>::new(vortex_index)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    deserialize(store, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
            };
            info!("Deserialization took {:?}", start.elapsed());
        }
        Action::Match { 
            input, 
            output, 
            format, 
            index_type, 
            subject, 
            predicate, 
            object, 
            graph 
        } => {
            let start = Instant::now();

            // 1. Prepare Filter Terms
            let subject_node = if let Some(s) = &subject { Some(parse_subject(s)?) } else { None };
            let predicate_node = if let Some(p) = &predicate { Some(parse_named_node(p)?) } else { None };
            let object_node = if let Some(o) = &object { Some(parse_term(o)?) } else { None };
            let graph_node = if let Some(g) = &graph { Some(parse_graph_name(g)?) } else { None };

            // 2. Prepare/Load Vortex Array and Text IndexType
            let is_vortex_file = input.extension().map(|e| e == "vortex").unwrap_or(false);
            
            let (vortex_array, resolved_index_type) = if is_vortex_file {
                let load_start = Instant::now();
                let arr = load_vortex_file_path(&input)
                    .await
                    .context("Failed to read Vortex index from file")?;
                debug!("Vortex index reference created in {:?}", load_start.elapsed());
                let detect_start = Instant::now();
                let t = detect_index_type(&arr);
                debug!("Detected index type {:?} in {:?}", t, detect_start.elapsed());
                (arr, t)
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

                let t = index_type.unwrap_or(IndexType::SimpleDictionary);
                
                let arr = match t {
                    IndexType::SimpleDictionary => VortexRdfStore::<SimpleDictionary>::build_vortex_index(quads_stream).await?,
                    IndexType::ChainedHash => VortexRdfStore::<ChainedHash>::build_vortex_index(quads_stream).await?,
                };
                debug!("Vortex index created in {:?}", load_start.elapsed());
                (arr, t)
            };

            // 3. Prepare Output Writer
            let output_format = format
                .map(RdfFormat::from)
                .or_else(|| detect_format(&output))
                .unwrap_or(RdfFormat::NQuads);

            let writer: Box<dyn Write> = match &output {
                Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                None => Box::new(stdout()),
            };

            // 4. Branch, Match, and Describe
            // We must perform matching inside the arms because `store` types are different.
            
            let match_start = Instant::now();
            match resolved_index_type {
                IndexType::SimpleDictionary => {
                    let store = VortexRdfStore::<SimpleDictionary>::new(vortex_array)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    debug!("DictionaryStore instance created in {:?}", start.elapsed());
                    
                    let filtered = store.match_pattern(
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                    ).await.context("Failed to match pattern")?;
                    debug!("Applying match pattern took {:?}", match_start.elapsed());

                    deserialize(filtered, writer, output_format)
                        .await
                        .context("Failed to deserialize filtered results")?;
                },
                IndexType::ChainedHash => {
                    let store = VortexRdfStore::<ChainedHash>::new(vortex_array)
                         .map_err(|e| anyhow::anyhow!(e))?;
                    debug!("ChainedHashStore instance created in {:?}", start.elapsed());
                    
                    let filtered = store.match_pattern(
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                    ).await.context("Failed to match pattern")?;
                     debug!("Applying match pattern took {:?}", match_start.elapsed());

                    deserialize(filtered, writer, output_format)
                        .await
                        .context("Failed to deserialize filtered results")?;
                }
            }
            
            info!("Full matching operation took {:?}", start.elapsed());
        }
    }

    Ok(())
}
