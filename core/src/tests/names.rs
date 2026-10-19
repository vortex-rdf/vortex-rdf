//! The frontend vocabulary: canonical strategy/index/format names — the only
//! spellings `FromStr` accepts — the quad column names, and the agreement
//! between the hand-written `Display` impls and the clap derive.

use crate::common::formats::{format_from_name, supported_format_names};
use crate::{IndexType, LayoutStrategy, QuadColumn, VortexRdfError};

#[test]
fn canonical_names_round_trip() {
    // Display emits the clap-derive kebab spelling; FromStr accepts it back.
    for (layout, name) in [
        (LayoutStrategy::Default, "default"),
        (LayoutStrategy::TypedObject, "typed-object"),
        (LayoutStrategy::Dictionary, "dictionary"),
    ] {
        assert_eq!(layout.to_string(), name);
        assert_eq!(name.parse::<LayoutStrategy>().unwrap(), layout);
    }
    for (index, name) in [
        (IndexType::SecondaryByCopy, "secondary-by-copy"),
        (IndexType::SecondaryByReference, "secondary-by-reference"),
    ] {
        assert_eq!(index.to_string(), name);
        assert_eq!(name.parse::<IndexType>().unwrap(), index);
    }
}

/// A quad column's name and position each give it back; the names are the
/// schema's `s p o g`, and any other name or a fifth position names none.
#[test]
fn quad_column_names_round_trip() {
    let names: Vec<&str> = QuadColumn::ALL.iter().map(|column| column.name()).collect();
    assert_eq!(names, ["s", "p", "o", "g"]);
    for column in QuadColumn::ALL {
        assert_eq!(QuadColumn::from_name(column.name()), Some(column));
        assert_eq!(QuadColumn::from_index(column.index()), Some(column));
    }
    assert_eq!(QuadColumn::from_index(4), None);
    assert_eq!(QuadColumn::from_name("x"), None);
}

/// Parsing is strict: only the canonical kebab-case names are accepted.
/// PascalCase, snake_case and short forms are rejected rather than folded
/// into a canonical arm, and so are names that match nothing at all. Every
/// refusal is invalid input.
#[test]
fn non_canonical_spellings_are_rejected() {
    let layout = |name: &str| name.parse::<LayoutStrategy>().unwrap_err();
    let index = |name: &str| name.parse::<IndexType>().unwrap_err();
    for err in [
        layout("TypedObject"),
        layout("Dictionary"),
        index("SecondaryByCopy"),
        index("SecondaryByReference"),
        layout("typed_object"),
        // Names that match nothing at all.
        layout("columnar"),
        index("btree"),
    ] {
        assert!(matches!(err, VortexRdfError::InvalidInput(_)), "{err:?}");
    }

    // The error names the canonical vocabulary.
    assert!(layout("Dictionary").to_string().contains("\"dictionary\""));
    assert!(
        layout("typed_object")
            .to_string()
            .contains("\"typed-object\"")
    );
}

/// The hand-written `Display` impls must agree with the spelling the clap
/// derive exposes on the CLI — the derive is generated, so this pins the pair
/// together (runs only under the `clap` feature the CLI enables).
#[cfg(feature = "clap")]
#[test]
fn display_matches_clap_value_names() {
    use clap::ValueEnum;

    for layout in LayoutStrategy::value_variants() {
        assert_eq!(
            layout.to_string(),
            layout.to_possible_value().unwrap().get_name()
        );
    }
    for index in IndexType::value_variants() {
        assert_eq!(
            index.to_string(),
            index.to_possible_value().unwrap().get_name()
        );
    }
}

/// Every listed format name parses, and an unknown name is invalid input
/// whose message quotes the name as given and lists exactly those names.
#[test]
fn supported_format_names_all_parse() {
    let names = supported_format_names();
    assert!(!names.is_empty());
    for name in names {
        assert!(
            format_from_name(name).is_ok(),
            "{name} is listed as supported but does not parse"
        );
    }
    let err = format_from_name("CSV").unwrap_err();
    assert!(matches!(err, VortexRdfError::InvalidInput(_)), "{err:?}");
    assert_eq!(
        err.to_string(),
        "Invalid input: unknown RDF format \"CSV\"; expected one of: ntriples, nt, nquads, nq, \
         turtle, ttl, trig, n3, rdfxml, rdf, xml, jsonld"
    );
}
