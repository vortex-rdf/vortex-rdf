//! Resolving an [`RdfFormat`] from what a user supplies: a file path or a
//! format name. The two entry points below are the only places a format is
//! named rather than passed, so every binding's format argument funnels
//! through them, accepts the same spellings and refuses the rest with the
//! same error.

use oxrdfio::{JsonLdProfileSet, RdfFormat};

use crate::error::{Result, VortexRdfError};

/// Every accepted format name with the format it denotes, each long spelling
/// before its aliases.
const FORMAT_NAMES: [(&str, RdfFormat); 12] = [
    ("ntriples", RdfFormat::NTriples),
    ("nt", RdfFormat::NTriples),
    ("nquads", RdfFormat::NQuads),
    ("nq", RdfFormat::NQuads),
    ("turtle", RdfFormat::Turtle),
    ("ttl", RdfFormat::Turtle),
    ("trig", RdfFormat::TriG),
    ("n3", RdfFormat::N3),
    ("rdfxml", RdfFormat::RdfXml),
    ("rdf", RdfFormat::RdfXml),
    ("xml", RdfFormat::RdfXml),
    (
        "jsonld",
        RdfFormat::JsonLd {
            profile: JsonLdProfileSet::empty(),
        },
    ),
];

/// Infer the RDF format from a path's extension. `None` when there is no
/// path, no extension, or the extension names no format oxrdfio knows — the
/// caller then needs an explicit format.
pub fn detect_format(path: Option<&std::path::Path>) -> Option<RdfFormat> {
    let ext = path?.extension()?.to_str()?;
    RdfFormat::from_extension(ext)
}

/// Parse a user-facing RDF format name — case-insensitive, accepting the
/// common aliases (`"ntriples"`, `"ttl"`, `"xml"`, …) — into an
/// [`RdfFormat`]. An unrecognized name is an
/// [`InvalidInput`](VortexRdfError::InvalidInput) error that lists every
/// accepted name. The name table behind every string-typed format parameter:
/// the JS bindings' `RdfFormatName`, Python's `format=` and the CLI's format
/// flags.
pub fn format_from_name(name: &str) -> Result<RdfFormat> {
    let spelling = name.to_lowercase();
    FORMAT_NAMES
        .iter()
        .find(|(accepted, _)| *accepted == spelling)
        .map(|(_, format)| *format)
        .ok_or_else(|| {
            VortexRdfError::InvalidInput(format!(
                "unknown RDF format {name:?}; expected one of: {}",
                supported_format_names().join(", ")
            ))
        })
}

/// Every name [`format_from_name`] accepts, long spelling before its aliases
/// — the list its error quotes.
pub(crate) fn supported_format_names() -> &'static [&'static str] {
    const NAMES: [&str; FORMAT_NAMES.len()] = {
        let mut names = [""; FORMAT_NAMES.len()];
        let mut i = 0;
        while i < names.len() {
            names[i] = FORMAT_NAMES[i].0;
            i += 1;
        }
        names
    };
    &NAMES
}

#[cfg(test)]
mod tests {
    use super::*;

    // tests/names.rs asserts every supported_format_names() entry parses and
    // pins the refusal's message; this pins the extension mapping,
    // case-insensitivity, and the arms that decline.
    #[test]
    fn detect_format_maps_extensions_and_declines_the_rest() {
        let path = |p: &str| detect_format(Some(std::path::Path::new(p)));
        assert_eq!(path("data.nt"), Some(RdfFormat::NTriples));
        assert_eq!(path("data.nq"), Some(RdfFormat::NQuads));
        assert_eq!(path("dir/data.ttl"), Some(RdfFormat::Turtle));
        assert_eq!(path("data.trig"), Some(RdfFormat::TriG));
        assert_eq!(path("data.rdf"), Some(RdfFormat::RdfXml));
        // No path, no extension, or an extension naming no format: `None`,
        // so the caller asks for an explicit format.
        assert_eq!(detect_format(None), None);
        assert_eq!(path("data"), None);
        assert_eq!(path("data.parquet"), None);
    }

    #[test]
    fn format_from_name_accepts_aliases_case_insensitively() {
        for (name, format) in [
            ("NTriples", RdfFormat::NTriples),
            ("nq", RdfFormat::NQuads),
            ("ttl", RdfFormat::Turtle),
            ("TRIG", RdfFormat::TriG),
            ("n3", RdfFormat::N3),
            ("xml", RdfFormat::RdfXml),
        ] {
            assert_eq!(format_from_name(name).unwrap(), format, "{name}");
        }
        assert!(matches!(
            format_from_name("jsonld"),
            Ok(RdfFormat::JsonLd { .. })
        ));
        assert!(matches!(
            format_from_name("csv"),
            Err(VortexRdfError::InvalidInput(_))
        ));
    }
}
