use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use log::{debug, info};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{Read, Write, stdin, stdout};
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use vortex::VortexSessionDefault;
use vortex_array::ExecutionCtx;
use vortex_rdf_core::io::match_cottas_native_file;
use vortex_rdf_core::store::layout::cottas::CottasLayout;

use tokio::fs::File as TokioFile;

use vortex::buffer::Buffer;

use vortex::session::VortexSession;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_rdf_core::{
    VortexRdfStore,
    common::formats::{Format, detect_format},
    common::indexes::{IndexType, detect_index_type},
    common::utils::{
        parse_graph_name, parse_named_node, parse_quads_from_reader, parse_subject, parse_term,
    },
    index::{ChainedHash, SimpleDictionary},
    io::{
        CottasNativeConfig, deserialize, load_vortex_file_path, load_vortex_file_ref, serialize,
        serialize_cottas_native_file,
    },
    store::layout::flat::FlatLayout,
};

use vortex_array::Canonical;
    io::{serialize, deserialize, load_vortex_file_ref, open_vortex_file},
    index::{SimpleDictionary, ChainedHash},
    VortexRdfStore,
    BuilderStrategy,
    UnsortedInMemoryBuilder,
    SortedInMemoryBuilder,
    ChunkSortBuilder,
    GlobalSortBuilder,
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

        #[arg(long, value_enum, default_value_t = StoreLayout::Default)]
        storage_layout: StoreLayout,

        /// Input file path (defaults to stdin)
        #[arg(short, long)]
        input: Option<PathBuf>,

        /// Output file path (required)
        #[arg(short, long)]
        output: PathBuf,

        /// RDF Format (auto-detected from file extension if not provided)
        #[arg(short, long, value_enum)]
        format: Option<Format>,

        /// Builder strategy to use when serializing (defaults to unsorted-in-memory)
        #[arg(short, long, value_enum, default_value = "unsorted-in-memory")]
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

        /// Index type for non-Vortex input
        #[arg(short = 't', long, value_enum)]
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

        /// Storage layout for generated Vortex index
        #[arg(long, value_enum)]
        storage_layout: Option<StoreLayout>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
#[clap(rename_all = "kebab_case")]
enum StoreLayout {
    Default,
    CottasSpog,
    CottasNative,
}

fn detect_storage_layout(
    vortex_array: &vortex_array::ArrayRef,
    ctx: &mut ExecutionCtx,
) -> anyhow::Result<StoreLayout> {
    let canonical = vortex_array
        .clone()
        .execute::<Canonical>(ctx)
        .context("Failed to execute canonical conversion")?;

    let vortex_struct = match canonical {
        Canonical::Struct(s) => s,
        _ => return Ok(StoreLayout::Default),
    };
    if let Ok(field_ref) = vortex_struct.unmasked_field_by_name("storage_layout") {
        if let Ok(scalar) = field_ref.execute_scalar(0, ctx) {
            let value = format!("{}", scalar);
            if value.contains("cottas-spog") {
                return Ok(StoreLayout::CottasSpog);
            }
        }
    }
    Ok(StoreLayout::Default)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    let session = VortexSession::default(); // default session (registries etc.)
    let mut ctx = session.create_execution_ctx(); // execution ctx

    match cli.action {
        Action::Serialize {
            index_type,
            storage_layout,
            input,
            output,
            format,
       , builder_strategy } => {
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
            let mut writer = TokioFile::create(&output)
                .await
                .context("Failed to create output file")?;
            let quads_stream = parse_quads_from_reader(reader, format);

            if storage_layout == StoreLayout::CottasNative {
                if index_type != IndexType::SimpleDictionary {
                    return Err(anyhow!(
                        "cottas-native currently supports only --index-type simple-dictionary. COTTAS benefits from ordered dictionary IDs and sorted row groups. Hash-based IDs do not preserve useful lexical locality for min/max zone-map pruning,"
                    ));
                }

                serialize_cottas_native_file::<SimpleDictionary, _>(
                    quads_stream,
                    &output,
                    CottasNativeConfig::default(),
                )
                .await
                .context("Failed to serialize native COTTAS Vortex file")?;

                info!(
                    "Serialized to native COTTAS Vortex-RDF in {:?}",
                    start.elapsed()
                );

                return Ok(());
            }

            let vortex_array = match storage_layout {
                StoreLayout::Default => match index_type {
                    IndexType::SimpleDictionary => {
                        VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_index(
                            quads_stream,
                        )
                        .await?
                    }
                    IndexType::ChainedHash => {
                        VortexRdfStore::<ChainedHash, FlatLayout>::build_vortex_index(quads_stream)
                            .await?
                    }
                },
                StoreLayout::CottasSpog => match index_type {
                    IndexType::SimpleDictionary => {
                        VortexRdfStore::<SimpleDictionary, CottasLayout>::build_vortex_index(
                            quads_stream,
                        )
                        .await?
                    }
                    IndexType::ChainedHash => {
                        VortexRdfStore::<ChainedHash, CottasLayout>::build_vortex_index(
                            quads_stream,
                        )
                        .await?
                    }
                },
                StoreLayout::CottasNative => unreachable!("handled above"),
            };

            serialize(vortex_array, &mut writer)
                .await
                .context("Failed to serialize to Vortex")?;
            info!("Fully serialized to Vortex-RDF in {:?}", start.elapsed());
            writer
                .flush()
                .await
                .context("Failed to flush output file")?;

            writer
                .sync_all()
                .await
                .context("Failed to sync output file")?;

            drop(writer); // Ensure file is closed before we check metadata
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
                    let file = open_vortex_file(path)
                        .await
                        .context("Failed to open Vortex file lazily")?;
                    
                    use vortex_array::stream::ArrayStreamExt;
                    let store_type_array: vortex_array::ArrayRef = file.scan()
                        .context("Failed to scan Vortex file")?
                        .with_projection(vortex_array::expr::select(["store_type"], vortex_array::expr::root()))
                        .into_array_stream()
                        .context("Failed to get array stream")?
                        .read_all()
                        .await
                        .context("Failed to read store_type column")?;
                    
                    let resolved_index_type = detect_index_type(&store_type_array);
                    log::debug!("[cli::deserialize] Dictionary index detected in {:?}", start.elapsed());

                    match resolved_index_type {
                        IndexType::SimpleDictionary => {
                            let store = VortexRdfStore::<SimpleDictionary>::from_file(path)
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }
                        IndexType::ChainedHash => {
                            let store = VortexRdfStore::<ChainedHash>::from_file(path)
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }
                    }
                }
                None => {
                    let mut buffer = Vec::new();
                    stdin()
                        .read_to_end(&mut buffer)
                        .context("Failed to read from stdin")?;
                    load_vortex_file_ref(Buffer::from(buffer))
                        .await
                        .context("Failed to read Vortex index from buffer")?
                }
            };

            let detect_start = Instant::now();
            match detect_index_type(&vortex_index) {
                IndexType::SimpleDictionary => {
                    log::debug!(
                        "[cli::deserialize] Dictionary index detected in {:?}",
                        detect_start.elapsed()
                    );
                    let store = VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_index)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    deserialize(store, writer, format)
                        .await
                        .context("Failed to deserialize from Vortex")?;
                }
                IndexType::ChainedHash => {
                    log::debug!(
                        "[cli::deserialize] ChainedHash index detected in {:?}",
                        detect_start.elapsed()
                    );
                    let store = VortexRdfStore::<ChainedHash, FlatLayout>::new(vortex_index)
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
            graph,
            storage_layout,
        } => {
            let start = Instant::now();

            // 1. Prepare Filter Terms
            let subject_node = if let Some(s) = &subject {
                Some(parse_subject(s)?)
            } else {
                None
            };
            let predicate_node = if let Some(p) = &predicate {
                Some(parse_named_node(p)?)
            } else {
                None
            };
            let object_node = if let Some(o) = &object {
                Some(parse_term(o)?)
            } else {
                None
            };
            let graph_node = if let Some(g) = &graph {
                Some(parse_graph_name(g)?)
            } else {
                None
            };

            // 2. Prepare/Load Vortex Array and Text IndexType
            let is_vortex_file = input.extension().map(|e| e == "vortex").unwrap_or(false);

            if is_vortex_file && storage_layout == Some(StoreLayout::CottasNative) {
                let output_format = format
                    .map(RdfFormat::from)
                    .or_else(|| detect_format(&output))
                    .unwrap_or(RdfFormat::NQuads);

                let writer: Box<dyn Write> = match &output {
                    Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                    None => Box::new(stdout()),
                };

                let native_match_start = Instant::now();

                let resolved_index_type = index_type.unwrap_or(IndexType::SimpleDictionary);

                if resolved_index_type != IndexType::SimpleDictionary {
                    return Err(anyhow!(
                        "cottas-native currently supports only --index-type simple-dictionary"
                    ));
                }

                match_cottas_native_file(
                    &input,
                    subject_node.as_ref(),
                    predicate_node.as_ref(),
                    object_node.as_ref(),
                    graph_node.as_ref(),
                    writer,
                    output_format,
                )
                .await
                .context("Failed to match native COTTAS file")?;

                info!(
                    "Native COTTAS matching operation took {:?}",
                    native_match_start.elapsed()
                );

                return Ok(());
            }

            let (vortex_array, resolved_index_type, resolved_storage_layout) = if is_vortex_file {
                let load_start = Instant::now();
                let file = open_vortex_file(&input)
                    .await
                    .context("Failed to open Vortex file lazily")?;
                use vortex_array::stream::ArrayStreamExt;
                let store_type_array: vortex_array::ArrayRef = file.scan()
                    .context("Failed to scan Vortex file")?
                    .with_projection(vortex_array::expr::select(["store_type"], vortex_array::expr::root()))
                    .into_array_stream()
                    .context("Failed to get array stream")?
                    .read_all()
                    .await
                    .context("Failed to read Vortex index from file")?;
                debug!(
                    "Vortex index reference created in {:?}",
                    load_start.elapsed()
                );
                let detect_start = Instant::now();
                let t = detect_index_type(&arr);
                let resolved_layout = detect_storage_layout(&arr, &mut ctx)?;
                debug!(
                    "Detected index type {:?} and storage layout {:?} in {:?}",
                    t,
                    resolved_layout,
                    detect_start.elapsed()
                );
                (arr, t, resolved_layout)
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
                let layout = storage_layout.unwrap_or(StoreLayout::Default);
                let arr = match layout {
                    StoreLayout::Default => match t {
                        IndexType::SimpleDictionary => {
                            VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_index(
                                quads_stream,
                            )
                            .await?
                        }
                        IndexType::ChainedHash => {
                            VortexRdfStore::<ChainedHash, FlatLayout>::build_vortex_index(
                                quads_stream,
                            )
                            .await?
                        }
                    },
                    StoreLayout::CottasSpog => match t {
                        IndexType::SimpleDictionary => {
                            VortexRdfStore::<SimpleDictionary, CottasLayout>::build_vortex_index(
                                quads_stream,
                            )
                            .await?
                        }
                        IndexType::ChainedHash => {
                            VortexRdfStore::<ChainedHash, CottasLayout>::build_vortex_index(
                                quads_stream,
                            )
                            .await?
                        }
                    },
                    StoreLayout::CottasNative => unreachable!("handled in Serialize arm"),
                };
                debug!("Vortex index created in {:?}", load_start.elapsed());
                (arr, t, layout)
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
            match (resolved_index_type, resolved_storage_layout) {
                (IndexType::SimpleDictionary, StoreLayout::Default) => {
                    let store = VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    debug!("DictionaryStore instance created in {:?}", start.elapsed());

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
                (IndexType::SimpleDictionary, StoreLayout::CottasSpog) => {
                    let store = VortexRdfStore::<SimpleDictionary, CottasLayout>::new(vortex_array)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    debug!(
                        "CottasDictionaryStore instance created in {:?}",
                        start.elapsed()
                    );

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
                },
                IndexType::ChainedHash => {
                    let store = if is_vortex_file {
                        VortexRdfStore::<ChainedHash>::from_file(&input)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        VortexRdfStore::<ChainedHash>::new(vortex_array.expect("Must have vortex_array for in-memory store"))
                             .map_err(|e| anyhow::anyhow!(e))?
                    };
                    debug!("ChainedHashStore instance created in {:?}", start.elapsed());

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
                (IndexType::ChainedHash, StoreLayout::CottasSpog) => {
                    let store = VortexRdfStore::<ChainedHash, CottasLayout>::new(vortex_array)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    debug!(
                        "CottasChainedHashStore instance created in {:?}",
                        start.elapsed()
                    );

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
                (IndexType::SimpleDictionary, StoreLayout::CottasNative) => {
                    unreachable!("handled above");
                }
                (IndexType::ChainedHash, StoreLayout::CottasNative) => {
                    unreachable!("handled above");
                }
            }
        }
    }

    Ok(())
}
