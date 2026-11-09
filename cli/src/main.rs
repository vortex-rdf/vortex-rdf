use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use oxrdfio::RdfFormat;
use std::path::PathBuf;

use vortex_rdf_core::common::formats::format_from_name;
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
    #[arg(long, value_enum, default_value = "dictionary")]
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
    #[arg(short, long, value_parser = format_from_name)]
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
    #[arg(short, long, value_parser = format_from_name)]
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
    #[arg(long, value_parser = format_from_name)]
    input_format: Option<RdfFormat>,

    /// RDF format for the exported matches (auto-detected from the output
    /// file extension if not provided, falling back to N-Quads)
    #[arg(long, value_parser = format_from_name)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    /// Every format flag reads core's format names, in any case, and refuses
    /// an unknown one with core's message, which lists the accepted names.
    #[test]
    fn format_flags_take_core_format_names() {
        let parse = |args: &[&str]| {
            Cli::try_parse_from(std::iter::once("vortex-rdf-cli").chain(args.iter().copied()))
        };
        for args in [
            &["serialize", "-o", "out.vortex", "-f", "nope"][..],
            &["deserialize", "-f", "nope"],
            &["match", "-i", "in.vortex", "--input-format", "nope"],
            &["match", "-i", "in.vortex", "--output-format", "nope"],
        ] {
            let error = parse(args)
                .err()
                .expect("an unknown format name is refused");
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{args:?}");
            assert!(
                error.to_string().contains(
                    "Invalid input: unknown RDF format \"nope\"; expected one of: ntriples, nt,"
                ),
                "{args:?}: {error}"
            );
        }
        let Action::Deserialize(args) = parse(&["deserialize", "-f", "TTL"]).unwrap().action else {
            panic!("not the deserialize action");
        };
        assert_eq!(args.format, Some(RdfFormat::Turtle));
    }
}
