//! The `match` command's pattern flags: each filters on its own position, and
//! a malformed one is refused under the hint for that flag.

mod common;

use common::{Scratch, TRIPLE, cli, flag, run};
use std::path::Path;
use std::process::Output;

const IN_G1: &str = "<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> <http://example.org/g1> .\n";
const IN_DEFAULT: &str = "<http://example.org/s2> <http://example.org/p2> \"o2\"@en .\n";

/// `match -i input name value`.
fn matching(input: &Path, name: &str, value: &str) -> Output {
    cli(
        &[
            Path::new("match"),
            &flag("-i"),
            input,
            &flag(name),
            Path::new(value),
        ],
        &[],
    )
}

/// Each flag binds its own position: a bare IRI or an angle-bracket one, a
/// language-tagged literal, a named graph and the default graph.
#[test]
fn each_flag_filters_its_own_position() {
    let scratch = Scratch::new("pattern-flags");
    let rdf = scratch.file("data.nq", &format!("{IN_G1}{IN_DEFAULT}"));
    let store = scratch.path("data.vortex");
    assert!(run("serialize", &rdf, &store).status.success());
    for (name, value, want) in [
        ("--subject", "http://example.org/s2", IN_DEFAULT),
        ("--predicate", "<http://example.org/p1>", IN_G1),
        ("--object", "\"o2\"@en", IN_DEFAULT),
        ("--graph", "http://example.org/g1", IN_G1),
        ("--graph", "default", IN_DEFAULT),
    ] {
        let result = matching(&store, name, value);
        assert!(result.status.success(), "{name} {value}: {result:?}");
        assert_eq!(
            String::from_utf8(result.stdout).unwrap(),
            want,
            "{name} {value}"
        );
    }
}

/// A malformed flag reports its hint, caused by the position core names.
#[test]
fn a_malformed_flag_is_refused_under_its_own_hint() {
    let scratch = Scratch::new("pattern-flags-refused");
    let rdf = scratch.file("data.nt", TRIPLE);
    for (name, value, hint, cause) in [
        (
            "--subject",
            "bad iri",
            "Invalid --subject pattern (expected <iri>, an IRI, or _:blank)",
            "Invalid input: pattern subject: invalid IRI \"bad iri\": ",
        ),
        (
            "--predicate",
            "bad iri",
            "Invalid --predicate pattern (expected <iri> or an IRI)",
            "Invalid input: pattern predicate: invalid IRI \"bad iri\": ",
        ),
        (
            "--object",
            "\"unterminated",
            "Invalid --object pattern (expected <iri>, _:blank, \"literal\", \
             \"literal\"@lang, or \"literal\"^^<datatype>)",
            "Invalid input: pattern object: malformed literal ",
        ),
        (
            "--graph",
            "bad iri",
            "Invalid --graph pattern (expected <iri>, _:blank, or default)",
            "Invalid input: pattern graph: invalid IRI \"bad iri\": ",
        ),
    ] {
        let result = matching(&rdf, name, value);
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success(), "{name}: {result:?}");
        assert!(result.stdout.is_empty(), "{name}: {result:?}");
        assert!(
            stderr.starts_with(&format!("Error: {hint}\n\nCaused by:\n    {cause}")),
            "{name}: {stderr}"
        );
    }
}
