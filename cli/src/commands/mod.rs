//! The three subcommand bodies, one file per [`crate::Action`] arm (`match`
//! in [`matching`], as `match` is a keyword), and what they share: the input
//! format rule of `serialize` and `match` ([`input_format`]), and the output
//! format rule and writer of `deserialize` and `match` ([`output_format`],
//! [`export_to_output`]).

use anyhow::{Context, Result, anyhow};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{BufWriter, Write, stdout};
use std::path::Path;

use vortex_rdf_core::common::formats::detect_format;
use vortex_rdf_core::{VortexRdfStore, export_rdf};

pub mod deserialize;
pub mod matching;
pub mod serialize;

/// The format an RDF input is read in: `explicit` when given, else the one
/// the extension of the input `path` names. Without either (stdin, or an
/// extension that names no format) the error asks for the `flag` that sets
/// it.
fn input_format(explicit: Option<RdfFormat>, path: Option<&Path>, flag: &str) -> Result<RdfFormat> {
    explicit
        .or_else(|| detect_format(path))
        .ok_or_else(|| anyhow!("Could not detect RDF format. Please specify it with {flag}"))
}

/// The format an exported document is written in: `explicit` when given, else
/// the one the extension of the output `path` names, else N-Quads, which is
/// also what a document written to stdout (no `path`) gets.
fn output_format(explicit: Option<RdfFormat>, path: Option<&Path>) -> RdfFormat {
    explicit
        .or_else(|| detect_format(path))
        .unwrap_or(RdfFormat::NQuads)
}

/// Exports `store` as `format` to the file at `path`, creating it, or to stdout
/// when there is no `path`. A failed export is reported with the context
/// `failure`, whether a write or the final flush failed.
///
/// Core writes each quad as several small writes, so the output goes through a
/// [`BufWriter`], which hands the file or stdout one write per buffer. A
/// `BufWriter` dropped with data in it discards the error of its last write, so
/// the final flush is explicit and its error is returned.
async fn export_to_output(
    store: VortexRdfStore,
    path: Option<&Path>,
    format: RdfFormat,
    failure: &'static str,
) -> Result<()> {
    let output: Box<dyn Write> = match path {
        Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
        None => Box::new(stdout().lock()),
    };
    let mut writer = BufWriter::new(output);
    export_rdf(store, &mut writer, format)
        .await
        .context(failure)?;
    writer.flush().context(failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both format rules take the explicit format, then the one the
    /// extension names. Without either, an output is N-Quads and an input is
    /// an error that names the flag to pass.
    #[test]
    fn format_rules_take_the_explicit_format_then_the_extension() {
        // (explicit format, path, output format, input format or `None` for
        // the error)
        let rows = [
            // An explicit format wins over what the extension names.
            (
                Some(RdfFormat::Turtle),
                Some("out.nt"),
                RdfFormat::Turtle,
                Some(RdfFormat::Turtle),
            ),
            (
                Some(RdfFormat::Turtle),
                None,
                RdfFormat::Turtle,
                Some(RdfFormat::Turtle),
            ),
            // Without one, the extension names the format.
            (
                None,
                Some("out.nt"),
                RdfFormat::NTriples,
                Some(RdfFormat::NTriples),
            ),
            (
                None,
                Some("out.nq"),
                RdfFormat::NQuads,
                Some(RdfFormat::NQuads),
            ),
            (
                None,
                Some("dir/out.ttl"),
                RdfFormat::Turtle,
                Some(RdfFormat::Turtle),
            ),
            // No path (stdin or stdout), no extension, or an extension that
            // names no format.
            (None, None, RdfFormat::NQuads, None),
            (None, Some("out"), RdfFormat::NQuads, None),
            (None, Some("out.parquet"), RdfFormat::NQuads, None),
        ];
        for (explicit, path, output, input) in rows {
            let at = path.map(Path::new);
            assert_eq!(
                output_format(explicit, at),
                output,
                "output: explicit {explicit:?}, path {path:?}"
            );
            match input_format(explicit, at, "--input-format") {
                Ok(format) => assert_eq!(Some(format), input, "input: path {path:?}"),
                Err(error) => {
                    assert_eq!(input, None, "input: path {path:?}: {error}");
                    assert_eq!(
                        error.to_string(),
                        "Could not detect RDF format. Please specify it with --input-format"
                    );
                }
            }
        }
    }
}
