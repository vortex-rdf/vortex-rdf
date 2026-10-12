//! The three subcommand bodies, one file per [`crate::Action`] arm; `main.rs`
//! keeps only the clap definitions and the dispatch. The `match` arm lives in
//! [`matching`] because `match` is a keyword. The format rule that `deserialize`
//! and `match` share for their output is [`output_format`].

use oxrdfio::RdfFormat;
use std::path::Path;

use vortex_rdf_core::common::formats::detect_format;

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
