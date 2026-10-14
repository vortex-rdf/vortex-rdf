//! The three subcommand bodies, one file per [`crate::Action`] arm; `main.rs`
//! keeps only the clap definitions and the dispatch. The `match` arm lives in
//! [`matching`] because `match` is a keyword. What `deserialize` and `match`
//! share for their output is the format rule [`output_format`] and the writer
//! [`export_to_output`].

use anyhow::{Context, Result};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{BufWriter, Write, stdout};
use std::path::Path;

use vortex_rdf_core::common::formats::detect_format;
use vortex_rdf_core::{VortexRdfStore, export_rdf};

pub mod deserialize;
pub mod matching;
pub mod serialize;

/// The format an exported document is written in: `explicit` when given, else
/// the one the extension of the output `path` names, else N-Quads — which is
/// also what a document written to stdout (no `path`) gets.
fn output_format(explicit: Option<RdfFormat>, path: Option<&Path>) -> RdfFormat {
    explicit
        .or_else(|| detect_format(path))
        .unwrap_or(RdfFormat::NQuads)
}

/// Exports `store` as `format` to the file at `path`, creating it, or to stdout
/// when there is no `path`. A failed export is reported with the context
/// `failure`.
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
    writer.flush().context("Failed to flush the output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_format_takes_the_explicit_format_then_the_extension_then_n_quads() {
        // (explicit format, output path, resolved format)
        let rows = [
            // An explicit format wins over what the extension names.
            (Some(RdfFormat::Turtle), Some("out.nt"), RdfFormat::Turtle),
            (Some(RdfFormat::Turtle), None, RdfFormat::Turtle),
            // Without one, the extension names the format.
            (None, Some("out.nt"), RdfFormat::NTriples),
            (None, Some("out.nq"), RdfFormat::NQuads),
            (None, Some("dir/out.ttl"), RdfFormat::Turtle),
            // No path (stdout), no extension, or an extension that names no
            // format: N-Quads.
            (None, None, RdfFormat::NQuads),
            (None, Some("out"), RdfFormat::NQuads),
            (None, Some("out.parquet"), RdfFormat::NQuads),
        ];
        for (explicit, path, expected) in rows {
            assert_eq!(
                output_format(explicit, path.map(Path::new)),
                expected,
                "explicit {explicit:?}, path {path:?}"
            );
        }
    }
}
