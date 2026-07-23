use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use log::{debug, info};
use oxrdfio::RdfFormat;

use std::{
    fs::File,
    io::{Read, Write, stdin, stdout},
    path::PathBuf,
    time::Instant,
};

use tokio::{fs::File as TokioFile, io::AsyncWriteExt};

use vortex::{VortexSessionDefault, buffer::Buffer, session::VortexSession};
use vortex_array::{
    Canonical, ExecutionCtx, VortexSessionExecute, arrays::struct_::StructArrayExt,
};

use vortex_rdf_core::{
    BuilderStrategy, ChunkSortBuilder, GlobalSortBuilder, SortedInMemoryBuilder,
    UnsortedInMemoryBuilder, VortexRdfStore,
    common::formats::{Format, detect_format},
    common::indexes::{IndexType, detect_index_type},
    common::utils::{
        parse_graph_name, parse_named_node, parse_quads_from_reader, parse_subject, parse_term,
    },
    deserialize,
    index::{ChainedHash, SimpleDictionary},
    io::{
        CottasNativeConfig, CottasNativeStringConfig, CottasVortexCompressionProfile,
        NativeIdsCountMode, NativeStringCountMode, build_cottas_native_o_exact_ranges_index,
        build_cottas_native_po_predicate_partitions_v2, build_cottas_native_subject_range_index,
        build_cottas_native_term_directory, count_cottas_native_ids_file_with_diagnostics_mode,
        count_cottas_native_string_file_with_diagnostics_mode, load_vortex_file_ref,
        match_cottas_native_file, match_cottas_native_file_with_diagnostics,
        match_cottas_native_string_file, match_cottas_native_string_file_with_diagnostics,
        open_vortex_file, rebuild_cottas_native_term_dictionary, serialize,
        serialize_cottas_native_file, serialize_cottas_native_string_file,
    },
    store::layout::{
        cottas::{CottasLayout, TripleOrdering},
        flat::FlatLayout,
    },
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

        /// Vortex compression profile for native COTTAS string files
        #[arg(long, value_enum, default_value_t = CompressionProfile::Balanced)]
        compression_profile: CompressionProfile,

        #[arg(long, default_value = "SPO")]
        ordering: TripleOrdering,
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

        /// Write match diagnostics JSON to this file
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Build native subject range index sidecar for an existing cottas-native-ids file
    #[command(name = "build-native-subject-index")]
    BuildNativeSubjectIndex {
        /// Input .vortex file path
        #[arg(short, long)]
        input: PathBuf,
        /// Write build diagnostics JSON to this file
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Build the compact predicate partition sidecar for an existing PO v2 directory
    #[command(name = "build-native-po-partitions")]
    BuildNativePoPartitions {
        /// Input cottas-native-ids .vortex data file
        #[arg(short, long)]
        input: PathBuf,
        /// Write build diagnostics JSON to this file
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Build the typed object-only exact-range index for an existing native-ID file
    #[command(name = "build-native-object-index")]
    BuildNativeObjectIndex {
        #[arg(short, long)]
        input: PathBuf,
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Rebuild only the Vortex term-to-ID dictionary from the existing
    /// Vortex ID-to-term component. Triples and native indexes are untouched.
    #[command(name = "rebuild-native-term-dictionary")]
    RebuildNativeTermDictionary {
        #[arg(short, long)]
        input: PathBuf,
        /// Rows per sorted term zone. Smaller values improve point lookup at
        /// the cost of modestly larger layout/statistics metadata.
        #[arg(long, default_value_t = 1_024)]
        row_group_size: usize,
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Build only the sparse lexical directory from the existing term-to-ID component.
    #[command(name = "build-native-term-directory")]
    BuildNativeTermDirectory {
        #[arg(short, long)]
        input: PathBuf,
        #[arg(long, default_value_t = 512)]
        fence_rows: usize,
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,
    },
    /// Count rows matching a pattern without RDF result serialization
    Count {
        /// Input file path required
        #[arg(short, long)]
        input: PathBuf,

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

        /// Storage layout
        #[arg(long, value_enum)]
        storage_layout: StoreLayout,

        /// Write count diagnostics JSON to this file
        #[arg(long)]
        diagnostics_out: Option<PathBuf>,

        /// Count diagnostic mode
        #[arg(long, value_enum, default_value_t = CountMode::NativeFilter)]
        mode: CountMode,
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
enum CountMode {
    NativeFilter,
    ManualEq,
    DecodeOnly,
    ExecuteOnly,
    RowsOnly,
}

impl From<CountMode> for NativeStringCountMode {
    fn from(value: CountMode) -> Self {
        match value {
            CountMode::NativeFilter => NativeStringCountMode::NativeFilter,
            CountMode::ManualEq => NativeStringCountMode::ManualEq,
            CountMode::DecodeOnly => NativeStringCountMode::DecodeOnly,
            CountMode::ExecuteOnly => NativeStringCountMode::ExecuteOnly,
            CountMode::RowsOnly => NativeStringCountMode::RowsOnly,
        }
    }
}

fn to_native_ids_count_mode(mode: CountMode) -> NativeIdsCountMode {
    match mode {
        CountMode::NativeFilter => NativeIdsCountMode::NativeFilter,
        CountMode::ManualEq => NativeIdsCountMode::ManualEq,
        CountMode::ExecuteOnly => NativeIdsCountMode::ExecuteOnly,
        CountMode::RowsOnly => NativeIdsCountMode::RowsOnly,

        // Native IDs do not need a separate string decode-only path.
        // For primitive u32 IDs, decode-only is equivalent to manual equality
        // when a bound term exists.
        CountMode::DecodeOnly => NativeIdsCountMode::ManualEq,
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
#[clap(rename_all = "kebab_case")]
enum StoreLayout {
    Default,
    CottasSpog,

    #[value(alias = "cottas-native")]
    CottasNativeIds,

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
            ordering,
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
                let config = CottasNativeStringConfig {
                    compression_profile: compression_profile.into(),
                    ordering: ordering,
                    ..CottasNativeStringConfig::default()
                };
                serialize_cottas_native_string_file(quads_stream, &output, config)
                    .await
                    .context("Failed to serialize native string COTTAS Vortex file")?;

                info!(
                    "Serialized to native string COTTAS Vortex-RDF in {:?}",
                    start.elapsed()
                );

                return Ok(());
            }

            if storage_layout == StoreLayout::CottasNativeIds {
                if index_type != IndexType::SimpleDictionary {
                    return Err(anyhow!(
                        "cottas-native currently supports only --index-type simple-dictionary. COTTAS benefits from ordered dictionary IDs and sorted row groups. Hash-based IDs do not preserve useful lexical locality for min/max zone-map pruning,"
                    ));
                }

                let config = CottasNativeConfig {
                    ordering,
                    compression_profile: compression_profile.into(),
                    ..CottasNativeConfig::default()
                };

                serialize_cottas_native_file::<SimpleDictionary, _>(quads_stream, &output, config)
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

                StoreLayout::CottasNativeIds => unreachable!("handled above"),
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

                        (_, StoreLayout::CottasNativeIds) => {
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

                        (_, StoreLayout::CottasNativeIds) => {
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
        Action::BuildNativeSubjectIndex {
            input,
            diagnostics_out,
        } => {
            let start = Instant::now();
            if !input.extension().map(|e| e == "vortex").unwrap_or(false) {
                return Err(anyhow!(
                    "build-native-subject-index expects a .vortex input file"
                ));
            }
            let stats = build_cottas_native_subject_range_index(&input)
                .await
                .context("Failed to build native subject range index")?;
            let stats_json = serde_json::to_vec_pretty(&stats)
                .context("Failed to serialize subject range index build diagnostics JSON")?;
            if let Some(diag_path) = &diagnostics_out {
                tokio::fs::write(diag_path, &stats_json)
                    .await
                    .context("Failed to write subject range index diagnostics JSON file")?;
            }
            stdout()
                .write_all(&stats_json)
                .context("Failed to write subject range index diagnostics to stdout")?;
            stdout()
                .write_all(
                    b"
",
                )
                .context("Failed to write trailing newline")?;
            info!(
                "Built native subject range index for {:?} in {:?}",
                input,
                start.elapsed()
            );
            return Ok(());
        }
        Action::BuildNativePoPartitions {
            input,
            diagnostics_out,
        } => {
            let start = Instant::now();
            if !input.extension().map(|e| e == "vortex").unwrap_or(false) {
                return Err(anyhow!(
                    "build-native-po-partitions expects a .vortex input file"
                ));
            }
            let stats = build_cottas_native_po_predicate_partitions_v2(&input)
                .await
                .context("Failed to build native PO predicate partitions")?;
            let stats_json = serde_json::to_vec_pretty(&stats)
                .context("Failed to serialize PO partition build diagnostics JSON")?;
            if let Some(diag_path) = &diagnostics_out {
                tokio::fs::write(diag_path, &stats_json)
                    .await
                    .context("Failed to write PO partition diagnostics JSON file")?;
            }
            stdout()
                .write_all(&stats_json)
                .context("Failed to write PO partition diagnostics to stdout")?;
            stdout()
                .write_all(
                    b"
",
                )
                .context("Failed to write trailing newline")?;
            info!(
                "Built native PO predicate partitions for {:?} in {:?}",
                input,
                start.elapsed()
            );
            return Ok(());
        }
        Action::BuildNativeObjectIndex {
            input,
            diagnostics_out,
        } => {
            if !input.extension().map(|e| e == "vortex").unwrap_or(false) {
                return Err(anyhow!(
                    "build-native-object-index expects a .vortex input file"
                ));
            }
            let stats = build_cottas_native_o_exact_ranges_index(&input)
                .await
                .context("Failed to build native object index")?;
            let json = serde_json::to_vec_pretty(&stats)
                .context("Failed to serialize object-index diagnostics")?;
            if let Some(path) = diagnostics_out {
                tokio::fs::write(path, &json)
                    .await
                    .context("Failed to write object-index diagnostics")?;
            }
            stdout().write_all(&json)?;
            stdout().write_all(
                b"
",
            )?;
            return Ok(());
        }
        Action::RebuildNativeTermDictionary {
            input,
            row_group_size,
            diagnostics_out,
        } => {
            if !input
                .extension()
                .map(|value| value == "vortex")
                .unwrap_or(false)
            {
                return Err(anyhow!(
                    "rebuild-native-term-dictionary expects a .vortex input file"
                ));
            }
            let stats = rebuild_cottas_native_term_dictionary(&input, row_group_size)
                .await
                .context("Failed to rebuild Vortex term dictionary")?;
            let json = serde_json::to_vec_pretty(&stats)
                .context("Failed to serialize dictionary rebuild diagnostics")?;
            if let Some(path) = diagnostics_out {
                tokio::fs::write(path, &json)
                    .await
                    .context("Failed to write dictionary rebuild diagnostics")?;
            }
            stdout().write_all(&json)?;
            stdout().write_all(b"\n")?;
            return Ok(());
        }
        Action::BuildNativeTermDirectory {
            input,
            fence_rows,
            diagnostics_out,
        } => {
            if !input
                .extension()
                .map(|value| value == "vortex")
                .unwrap_or(false)
            {
                return Err(anyhow!(
                    "build-native-term-directory expects a .vortex input file"
                ));
            }
            if fence_rows == 0 {
                return Err(anyhow!("--fence-rows must be greater than zero"));
            }
            let stats = build_cottas_native_term_directory(&input, fence_rows)
                .await
                .context("Failed to build sparse native term directory")?;
            let json = serde_json::to_vec_pretty(&stats)
                .context("Failed to serialize term directory diagnostics")?;
            if let Some(path) = diagnostics_out {
                tokio::fs::write(path, &json)
                    .await
                    .context("Failed to write term directory diagnostics")?;
            }
            stdout().write_all(&json)?;
            stdout().write_all(b"\n")?;
            return Ok(());
        }
        Action::Count {
            input,
            subject,
            predicate,
            object,
            graph,
            storage_layout,
            mode,
            diagnostics_out,
        } => {
            let count_start = Instant::now();

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

            let is_vortex_file = input.extension().map(|e| e == "vortex").unwrap_or(false);
            if !is_vortex_file {
                return Err(anyhow!("count currently expects a .vortex input file"));
            }

            match storage_layout {
                StoreLayout::CottasNativeStrings => {
                    let diag = count_cottas_native_string_file_with_diagnostics_mode(
                        &input,
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                        mode.into(),
                    )
                    .await
                    .context("Failed to count native string COTTAS file")?;

                    let diag_json = serde_json::to_vec_pretty(&diag)
                        .context("Failed to serialize native string count diagnostics JSON")?;

                    if let Some(diag_path) = &diagnostics_out {
                        tokio::fs::write(diag_path, &diag_json)
                            .await
                            .context("Failed to write count diagnostics JSON file")?;
                    }

                    stdout()
                        .write_all(&diag_json)
                        .context("Failed to write count diagnostics to stdout")?;
                    stdout()
                        .write_all(b"\n")
                        .context("Failed to write trailing newline")?;

                    info!(
                        "Native string COTTAS count-only operation took {:?}",
                        count_start.elapsed()
                    );

                    return Ok(());
                }
                StoreLayout::CottasNativeIds => {
                    let diag = count_cottas_native_ids_file_with_diagnostics_mode(
                        &input,
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                        to_native_ids_count_mode(mode),
                    )
                    .await
                    .context("Failed to count native ID COTTAS file")?;

                    let diag_json = serde_json::to_vec_pretty(&diag)
                        .context("Failed to serialize native ID count diagnostics JSON")?;

                    if let Some(diag_path) = &diagnostics_out {
                        tokio::fs::write(diag_path, &diag_json)
                            .await
                            .context("Failed to write native ID count diagnostics JSON file")?;
                    }

                    stdout()
                        .write_all(&diag_json)
                        .context("Failed to write native ID count diagnostics to stdout")?;
                    stdout()
                        .write_all(b"\n")
                        .context("Failed to write trailing newline")?;

                    info!(
                        "Native ID COTTAS count-only operation took {:?}",
                        count_start.elapsed()
                    );

                    return Ok(());
                }
                other => {
                    return Err(anyhow!(
                        "count is currently implemented only for --storage-layout cottas-native-strings and cottas-native-ids, got {:?}",
                        other
                    ));
                }
            }
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
            diagnostics_out,
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

                if let Some(diag_path) = &diagnostics_out {
                    let diag = match_cottas_native_string_file_with_diagnostics(
                        &input,
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                        writer,
                        output_format,
                    )
                    .await
                    .context("Failed to match native string COTTAS file with diagnostics")?;

                    let diag_json = serde_json::to_vec_pretty(&diag)
                        .context("Failed to serialize native string diagnostics JSON")?;

                    tokio::fs::write(diag_path, diag_json)
                        .await
                        .context("Failed to write diagnostics JSON file")?;

                    info!(
                        "Native string COTTAS matching operation took {:?} (diagnostics written to {:?})",
                        native_match_start.elapsed(),
                        diag_path
                    );
                } else {
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
                }

                return Ok(());
            }

            if is_vortex_file && storage_layout == Some(StoreLayout::CottasNativeIds) {
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

                if let Some(diag_path) = &diagnostics_out {
                    let diag = match_cottas_native_file_with_diagnostics(
                        &input,
                        subject_node.as_ref(),
                        predicate_node.as_ref(),
                        object_node.as_ref(),
                        graph_node.as_ref(),
                        writer,
                        output_format,
                    )
                    .await
                    .context("Failed to match native COTTAS file with diagnostics")?;

                    let diag_json = serde_json::to_vec_pretty(&diag)
                        .context("Failed to serialize native COTTAS diagnostics JSON")?;

                    tokio::fs::write(diag_path, diag_json)
                        .await
                        .context("Failed to write diagnostics JSON file")?;

                    info!(
                        "Native COTTAS matching operation took {:?} (diagnostics written to {:?})",
                        native_match_start.elapsed(),
                        diag_path
                    );
                } else {
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
                }

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
                    StoreLayout::CottasNativeIds => unreachable!("handled in Serialize arm"),
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
                (IndexType::SimpleDictionary, StoreLayout::CottasNativeIds) => {
                    unreachable!("handled above");
                }
                (IndexType::ChainedHash, StoreLayout::CottasNativeIds) => {
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
