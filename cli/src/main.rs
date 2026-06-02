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

use tokio::fs::File as TokioFile;

use vortex::buffer::Buffer;

use vortex::session::VortexSession;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_rdf_core::{
    BuilderStrategy, ChunkSortBuilder, GlobalSortBuilder, SortedInMemoryBuilder,
    UnsortedInMemoryBuilder, VortexRdfStore,
    common::formats::{Format, detect_format},
    common::indexes::{IndexType, detect_index_type},
    common::utils::{
        parse_graph_name, parse_named_node, parse_quads_from_reader, parse_subject, parse_term,
    },
    index::{ChainedHash, SimpleDictionary},
    io::{
        CottasNativeConfig, CottasNativeStringConfig, CottasVortexCompressionProfile, deserialize,
        load_vortex_file_ref, match_cottas_native_file, match_cottas_native_string_file,
        open_vortex_file, serialize, serialize_cottas_native_file,
        serialize_cottas_native_string_file,
    },
    store::layout::{cottas::CottasLayout, flat::FlatLayout},
};

use vortex_array::Canonical;

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

        /// Vortex compression profile for native COTTAS string files
        #[arg(long, value_enum, default_value_t = CompressionProfile::Balanced)]
        compression_profile: CompressionProfile,
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
enum CompressionProfile {
    Balanced,
    Compact,
}

impl From<CompressionProfile> for CottasVortexCompressionProfile {
    fn from(value: CompressionProfile) -> Self {
        match value {
            CompressionProfile::Balanced => CottasVortexCompressionProfile::Balanced,
            CompressionProfile::Compact => CottasVortexCompressionProfile::Compact,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
#[clap(rename_all = "kebab_case")]
enum StoreLayout {
    Default,
    CottasSpog,
    CottasNative,
    CottasNativeStrings,
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

async fn build_with_strategy<Dict, Layout>(
    quads_stream: impl futures::Stream<Item = vortex_rdf_core::error::Result<oxrdf::Quad>>
    + Unpin
    + Send
    + 'static,
    builder_strategy: BuilderStrategy,
) -> vortex_rdf_core::error::Result<vortex_array::ArrayRef>
where
    Dict: vortex_rdf_core::index::RdfDictionary,
    Layout: vortex_rdf_core::store::layout::RdfQuadLayout<Dict>,
{
    match builder_strategy {
        BuilderStrategy::UnsortedInMemory => {
            VortexRdfStore::<Dict, Layout>::build_vortex_array_with_builder::<
                UnsortedInMemoryBuilder,
            >(quads_stream)
            .await
        }
        BuilderStrategy::SortedInMemory => {
            VortexRdfStore::<Dict, Layout>::build_vortex_array_with_builder::<
                SortedInMemoryBuilder,
            >(quads_stream)
            .await
        }
        BuilderStrategy::ChunkSort => {
            VortexRdfStore::<Dict, Layout>::build_vortex_array_with_builder::<
                ChunkSortBuilder,
            >(quads_stream)
            .await
        }
        BuilderStrategy::GlobalSort => {
            VortexRdfStore::<Dict, Layout>::build_vortex_array_with_builder::<
                GlobalSortBuilder,
            >(quads_stream)
            .await
        }
    }
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
            builder_strategy,
            compression_profile,
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
            let mut writer = TokioFile::create(&output)
                .await
                .context("Failed to create output file")?;
            let quads_stream = parse_quads_from_reader(reader, format);

            if storage_layout == StoreLayout::CottasNativeStrings {
                serialize_cottas_native_string_file(
                    quads_stream,
                    &output,
                    CottasNativeStringConfig {
                        compression_profile: compression_profile.into(),
                        ..CottasNativeStringConfig::default()
                    },
                )
                .await
                .context("Failed to serialize native string COTTAS Vortex file")?;

                info!(
                    "Serialized to native string COTTAS Vortex-RDF in {:?}",
                    start.elapsed()
                );

                return Ok(());
            }

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
                        build_with_strategy::<SimpleDictionary, FlatLayout>(
                            quads_stream,
                            builder_strategy,
                        )
                        .await?
                    }
                    IndexType::ChainedHash => {
                        build_with_strategy::<ChainedHash, FlatLayout>(
                            quads_stream,
                            builder_strategy,
                        )
                        .await?
                    }
                },

                StoreLayout::CottasSpog => match index_type {
                    IndexType::SimpleDictionary => {
                        build_with_strategy::<SimpleDictionary, CottasLayout>(
                            quads_stream,
                            builder_strategy,
                        )
                        .await?
                    }
                    IndexType::ChainedHash => {
                        build_with_strategy::<ChainedHash, CottasLayout>(
                            quads_stream,
                            builder_strategy,
                        )
                        .await?
                    }
                },

                StoreLayout::CottasNative => unreachable!("handled above"),
                StoreLayout::CottasNativeStrings => unreachable!("handled above"),
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

                    let store_type_array: vortex_array::ArrayRef = file
                        .scan()
                        .context("Failed to scan Vortex file")?
                        .with_projection(vortex_array::expr::select(
                            ["store_type"],
                            vortex_array::expr::root(),
                        ))
                        .into_array_stream()
                        .context("Failed to get array stream")?
                        .read_all()
                        .await
                        .context("Failed to read store_type column")?;

                    let resolved_index_type = detect_index_type(&store_type_array);

                    let resolved_storage_layout =
                        detect_storage_layout(&store_type_array, &mut ctx)
                            .unwrap_or(StoreLayout::Default);

                    log::debug!(
                        "[cli::deserialize] Detected index {:?}, layout {:?}",
                        resolved_index_type,
                        resolved_storage_layout
                    );

                    match (resolved_index_type, resolved_storage_layout) {
                        (IndexType::SimpleDictionary, StoreLayout::Default) => {
                            let store =
                                VortexRdfStore::<SimpleDictionary, FlatLayout>::from_file(path)
                                    .await
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::ChainedHash, StoreLayout::Default) => {
                            let store = VortexRdfStore::<ChainedHash, FlatLayout>::from_file(path)
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::SimpleDictionary, StoreLayout::CottasSpog) => {
                            let store =
                                VortexRdfStore::<SimpleDictionary, CottasLayout>::from_file(path)
                                    .await
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::ChainedHash, StoreLayout::CottasSpog) => {
                            let store =
                                VortexRdfStore::<ChainedHash, CottasLayout>::from_file(path)
                                    .await
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (_, StoreLayout::CottasNative) => {
                            return Err(anyhow!(
                                "cottas-native deserialization is not handled by generic VortexRdfStore"
                            ));
                        }
                        (_, StoreLayout::CottasNativeStrings) => {
                            return Err(anyhow!(
                                "cottas-native-strings deserialization is not handled by generic VortexRdfStore yet"
                            ));
                        }
                    }
                }

                None => {
                    let mut buffer = Vec::new();

                    stdin()
                        .read_to_end(&mut buffer)
                        .context("Failed to read from stdin")?;

                    let vortex_array = load_vortex_file_ref(Buffer::from(buffer))
                        .await
                        .context("Failed to read Vortex index from buffer")?;

                    let resolved_index_type = detect_index_type(&vortex_array);
                    let resolved_storage_layout = detect_storage_layout(&vortex_array, &mut ctx)
                        .unwrap_or(StoreLayout::Default);

                    match (resolved_index_type, resolved_storage_layout) {
                        (IndexType::SimpleDictionary, StoreLayout::Default) => {
                            let store =
                                VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::ChainedHash, StoreLayout::Default) => {
                            let store =
                                VortexRdfStore::<ChainedHash, FlatLayout>::new(vortex_array)
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::SimpleDictionary, StoreLayout::CottasSpog) => {
                            let store =
                                VortexRdfStore::<SimpleDictionary, CottasLayout>::new(vortex_array)
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (IndexType::ChainedHash, StoreLayout::CottasSpog) => {
                            let store =
                                VortexRdfStore::<ChainedHash, CottasLayout>::new(vortex_array)
                                    .map_err(|e| anyhow::anyhow!(e))?;

                            deserialize(store, writer, format)
                                .await
                                .context("Failed to deserialize from Vortex")?;
                        }

                        (_, StoreLayout::CottasNative) => {
                            return Err(anyhow!(
                                "cottas-native deserialization from stdin is not supported here"
                            ));
                        }
                        (_, StoreLayout::CottasNativeStrings) => {
                            return Err(anyhow!(
                                "cottas-native-strings deserialization from stdin is not supported here"
                            ));
                        }
                    }
                }
            }

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

            if is_vortex_file && storage_layout == Some(StoreLayout::CottasNativeStrings) {
                let output_format = format
                    .map(RdfFormat::from)
                    .or_else(|| detect_format(&output))
                    .unwrap_or(RdfFormat::NQuads);

                let writer: Box<dyn Write> = match &output {
                    Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
                    None => Box::new(stdout()),
                };

                let native_match_start = Instant::now();

                match_cottas_native_string_file(
                    &input,
                    subject_node.as_ref(),
                    predicate_node.as_ref(),
                    object_node.as_ref(),
                    graph_node.as_ref(),
                    writer,
                    output_format,
                )
                .await
                .context("Failed to match native string COTTAS file")?;

                info!(
                    "Native string COTTAS matching operation took {:?}",
                    native_match_start.elapsed()
                );

                return Ok(());
            }

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

                let arr = load_vortex_file_ref(Buffer::from(std::fs::read(&input)?))
                    .await
                    .context("Failed to read Vortex index from file")?;

                debug!("Vortex index loaded in {:?}", load_start.elapsed());

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
                            VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array(
                                quads_stream,
                            )
                            .await?
                        }
                        IndexType::ChainedHash => {
                            VortexRdfStore::<ChainedHash, FlatLayout>::build_vortex_array(
                                quads_stream,
                            )
                            .await?
                        }
                    },
                    StoreLayout::CottasSpog => match t {
                        IndexType::SimpleDictionary => {
                            VortexRdfStore::<SimpleDictionary, CottasLayout>::build_vortex_array(
                                quads_stream,
                            )
                            .await?
                        }
                        IndexType::ChainedHash => {
                            VortexRdfStore::<ChainedHash, CottasLayout>::build_vortex_array(
                                quads_stream,
                            )
                            .await?
                        }
                    },
                    StoreLayout::CottasNative => unreachable!("handled in Serialize arm"),
                    StoreLayout::CottasNativeStrings => unreachable!("handled above"),
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
                    let store = if is_vortex_file {
                        VortexRdfStore::<SimpleDictionary, FlatLayout>::from_file(&input)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                            .map_err(|e| anyhow::anyhow!(e))?
                    };
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
                    let store = if is_vortex_file {
                        VortexRdfStore::<SimpleDictionary, CottasLayout>::from_file(&input)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        VortexRdfStore::<SimpleDictionary, CottasLayout>::new(vortex_array)
                            .map_err(|e| anyhow::anyhow!(e))?
                    };

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
                (IndexType::ChainedHash, StoreLayout::Default) => {
                    let store = if is_vortex_file {
                        VortexRdfStore::<ChainedHash, FlatLayout>::from_file(&input)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        VortexRdfStore::<ChainedHash, FlatLayout>::new(vortex_array)
                            .map_err(|e| anyhow::anyhow!(e))?
                    };

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
                    let store = if is_vortex_file {
                        VortexRdfStore::<ChainedHash, CottasLayout>::from_file(&input)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        VortexRdfStore::<ChainedHash, CottasLayout>::new(vortex_array)
                            .map_err(|e| anyhow::anyhow!(e))?
                    };

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
                (_, StoreLayout::CottasNativeStrings) => {
                    unreachable!("handled above");
                }
            }
        }
    }

    Ok(())
}
