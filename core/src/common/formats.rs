//! Resolving an [`RdfFormat`] from a file path or a format name; every
//! binding's format argument funnels through these two entry points.

use oxrdfio::{JsonLdProfileSet, RdfFormat};

/// Every accepted format name with its format: the long spelling first,
/// then its aliases.
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

/// The RDF format a path's extension names; `None` without a path, an
/// extension, or a format oxrdfio knows for it.
pub fn detect_format(path: Option<&std::path::Path>) -> Option<RdfFormat> {
    let ext = path?.extension()?.to_str()?;
    RdfFormat::from_extension(ext)
}

/// The format a user-facing name denotes, case-insensitively, aliases
/// included (`"ntriples"`, `"nt"`, `"ttl"`, `"xml"`, …); `None` for an
/// unrecognized name.
pub fn format_from_name(name: &str) -> Option<RdfFormat> {
    let name = name.to_lowercase();
    FORMAT_NAMES
        .iter()
        .find(|(spelling, _)| *spelling == name)
        .map(|(_, format)| *format)
}

/// Every name [`format_from_name`] accepts, long spelling before its
/// aliases; the list "unsupported format" errors quote.
pub fn supported_format_names() -> &'static [&'static str] {
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

    // tests/names.rs asserts every supported_format_names() entry parses;
    // this pins the extension mapping, case-insensitivity, and the None arms.
    #[test]
    fn detect_format_maps_extensions_and_declines_the_rest() {
        let path = |p: &str| detect_format(Some(std::path::Path::new(p)));
        assert_eq!(path("data.nt"), Some(RdfFormat::NTriples));
        assert_eq!(path("data.nq"), Some(RdfFormat::NQuads));
        assert_eq!(path("dir/data.ttl"), Some(RdfFormat::Turtle));
        assert_eq!(path("data.trig"), Some(RdfFormat::TriG));
        assert_eq!(path("data.rdf"), Some(RdfFormat::RdfXml));
        // No path, no extension, or an extension naming no format: `None`.
        assert_eq!(detect_format(None), None);
        assert_eq!(path("data"), None);
        assert_eq!(path("data.parquet"), None);
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
