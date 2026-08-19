//! Resolving an [`RdfFormat`] from what a user supplies: a file path or a
//! format name. The two entry points below are the only places a format is
//! named rather than passed, so every binding's format argument funnels
//! through them and accepts the same spellings.

use oxrdfio::RdfFormat;

/// Infer the RDF format from a path's extension. `None` when there is no
/// path, no extension, or the extension names no format oxrdfio knows — the
/// caller then needs an explicit format.
pub fn detect_format(path: &Option<std::path::PathBuf>) -> Option<RdfFormat> {
    let path = path.as_ref()?;
    let ext = path.extension()?.to_str()?;
    RdfFormat::from_extension(ext)
}

/// Parse a user-facing RDF format name — case-insensitive, accepting the
/// common aliases (`"ntriples"`, `"ttl"`, `"xml"`, …) — into an
/// [`RdfFormat`]. `None` for an unrecognized name. The name table behind
/// every string-typed format parameter (the JS bindings' `RdfFormatName`).
pub fn format_from_name(name: &str) -> Option<RdfFormat> {
    Some(match name.to_lowercase().as_str() {
        "nt" | "ntriples" => RdfFormat::NTriples,
        "nq" | "nquads" => RdfFormat::NQuads,
        "ttl" | "turtle" => RdfFormat::Turtle,
        "trig" => RdfFormat::TriG,
        "n3" => RdfFormat::N3,
        "rdf" | "rdfxml" | "xml" => RdfFormat::RdfXml,
        "jsonld" => RdfFormat::JsonLd {
            profile: Default::default(),
        },
        _ => return None,
    })
}

/// Every name [`format_from_name`] accepts, long spelling before its short
/// aliases — the single list "unsupported format" error messages quote, so a
/// binding's message cannot drift from the parser's table above. Kept
/// adjacent to that match; a name belongs here exactly when it has an arm
/// there (the test suite asserts each listed name parses).
pub fn supported_format_names() -> &'static [&'static str] {
    &[
        "ntriples", "nt", "nquads", "nq", "turtle", "ttl", "trig", "n3", "rdfxml", "rdf", "xml",
        "jsonld",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    // The alias table itself (every listed name parses, Display agreement,
    // unknown names) is pinned centrally in `crate::tests::names`; this
    // covers the mapping and its `None` arms.
    #[test]
    fn detect_format_maps_extensions_and_declines_the_rest() {
        let path = |p: &str| Some(std::path::PathBuf::from(p));
        assert_eq!(detect_format(&path("data.nt")), Some(RdfFormat::NTriples));
        assert_eq!(detect_format(&path("data.nq")), Some(RdfFormat::NQuads));
        assert_eq!(
            detect_format(&path("dir/data.ttl")),
            Some(RdfFormat::Turtle)
        );
        assert_eq!(detect_format(&path("data.trig")), Some(RdfFormat::TriG));
        assert_eq!(detect_format(&path("data.rdf")), Some(RdfFormat::RdfXml));
        // No path, no extension, or an extension naming no format: `None`,
        // so the caller asks for an explicit format.
        assert_eq!(detect_format(&None), None);
        assert_eq!(detect_format(&path("data")), None);
        assert_eq!(detect_format(&path("data.parquet")), None);
    }

    #[test]
    fn format_from_name_accepts_aliases_case_insensitively() {
        assert_eq!(format_from_name("NTriples"), Some(RdfFormat::NTriples));
        assert_eq!(format_from_name("nq"), Some(RdfFormat::NQuads));
        assert_eq!(format_from_name("ttl"), Some(RdfFormat::Turtle));
        assert_eq!(format_from_name("TRIG"), Some(RdfFormat::TriG));
        assert_eq!(format_from_name("n3"), Some(RdfFormat::N3));
        assert_eq!(format_from_name("xml"), Some(RdfFormat::RdfXml));
        assert!(matches!(
            format_from_name("jsonld"),
            Some(RdfFormat::JsonLd { .. })
        ));
        assert_eq!(format_from_name("csv"), None);
    }
}
