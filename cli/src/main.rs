use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use oxrdfio::RdfFormat;
use std::path::PathBuf;

use vortex_rdf_core::common::formats::{format_from_name, supported_format_names};
use vortex_rdf_core::{IndexType, LayoutStrategy};

mod commands;

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
    Serialize(SerializeArgs),
    /// Convert from Vortex-RDF to RDF
    Deserialize(DeserializeArgs),
    /// Filter Vortex-RDF store by pattern
    Match(MatchArgs),
}

#[derive(Args)]
struct SerializeArgs {
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

    /// RDF format name, e.g. "ttl" or "nquads" (auto-detected from the input
    /// file extension if not provided)
    #[arg(short, long, value_parser = parse_format)]
    format: Option<RdfFormat>,
}

#[derive(Args)]
struct DeserializeArgs {
    /// Input file path (defaults to stdin)
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Output file path (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// RDF format name, e.g. "ttl" or "nquads" (auto-detected from the
    /// output file extension if not provided, falling back to N-Quads)
    #[arg(short, long, value_parser = parse_format)]
    format: Option<RdfFormat>,
}

#[derive(Args)]
struct MatchArgs {
    /// Input file path (required)
    #[arg(short, long)]
    input: PathBuf,

    /// Output file path (defaults to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// RDF format of a non-Vortex input (auto-detected from the input file
    /// extension if not provided; ignored for .vortex inputs)
    #[arg(long, value_parser = parse_format)]
    input_format: Option<RdfFormat>,

    /// RDF format for the exported matches (auto-detected from the output
    /// file extension if not provided, falling back to N-Quads)
    #[arg(long, value_parser = parse_format)]
    output_format: Option<RdfFormat>,

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

/// clap value parser over core's format-name table, so the CLI accepts the
/// same spellings as every other binding ("ttl"/"turtle", "xml"/"rdfxml", …)
/// and its rejection message quotes the same list the parser accepts.
fn parse_format(name: &str) -> Result<RdfFormat, String> {
    format_from_name(name).ok_or_else(|| {
        format!(
            "unsupported RDF format {:?} (expected one of: {})",
            name,
            supported_format_names().join(", ")
        )
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.action {
        Action::Serialize(args) => commands::serialize::run(args).await,
        Action::Deserialize(args) => commands::deserialize::run(args).await,
        Action::Match(args) => commands::matching::run(args).await,
    }
}
