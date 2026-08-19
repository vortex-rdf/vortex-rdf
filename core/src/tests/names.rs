//! The frontend vocabulary: canonical strategy/index/format names — the only
//! spellings `FromStr` accepts — and the agreement between the hand-written
//! `Display` impls and the clap derive.

use crate::common::formats::{format_from_name, supported_format_names};
use crate::{IndexType, LayoutStrategy};

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

/// Parsing is strict: only the canonical kebab-case names are accepted, so
/// the retired binding spellings (PascalCase, underscores, short forms) must
/// be rejected, not silently folded into a canonical arm.
#[test]
fn non_canonical_spellings_are_rejected() {
    // Retired JS PascalCase.
    assert!("TypedObject".parse::<LayoutStrategy>().is_err());
    assert!("Dictionary".parse::<LayoutStrategy>().is_err());
    assert!("SecondaryByCopy".parse::<IndexType>().is_err());
    assert!("SecondaryByReference".parse::<IndexType>().is_err());

    // Retired Python underscore and short forms.
    assert!("typed_object".parse::<LayoutStrategy>().is_err());

    // The error names the canonical vocabulary.
    let err = "Dictionary".parse::<LayoutStrategy>().unwrap_err();
    assert!(err.to_string().contains("\"dictionary\""));
    let err = "typed_object".parse::<LayoutStrategy>().unwrap_err();
    assert!(err.to_string().contains("\"typed-object\""));
}

#[test]
fn unknown_names_error() {
    assert!("columnar".parse::<LayoutStrategy>().is_err());
    assert!("btree".parse::<IndexType>().is_err());
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

#[test]
fn supported_format_names_all_parse() {
    let names = supported_format_names();
    assert!(!names.is_empty());
    for name in names {
        assert!(
            format_from_name(name).is_some(),
            "{name} is listed as supported but does not parse"
        );
    }
}
