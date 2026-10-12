//! `deserialize` and `match` open their input before they create their output
//! file: an input that cannot be read leaves an existing output file as it
//! was.

mod common;

use common::{Scratch, TRIPLE, cli, flag, run};
use std::fs;
use std::path::Path;
use std::process::Output;

const KEPT: &str = "the previous output\n";

#[test]
fn an_unreadable_input_leaves_the_output_file_alone() {
    let scratch = Scratch::new("refused");
    let garbage = scratch.file("garbage.vortex", "this is not a store");
    let missing = scratch.path("missing.vortex");
    let rdf = scratch.file("data.nt", TRIPLE);
    let rdf_missing = scratch.path("missing.nt");
    let output = scratch.file("out.nq", KEPT);

    let refused = |what: &str, result: Output| {
        assert!(!result.status.success(), "{what} should fail");
        assert_eq!(
            fs::read_to_string(&output).unwrap(),
            KEPT,
            "{what} touched the output file"
        );
    };
    for (action, input) in [
        ("deserialize", &garbage),
        ("deserialize", &missing),
        ("match", &garbage),
        ("match", &missing),
        ("match", &rdf_missing),
    ] {
        refused(&format!("{action} {input:?}"), run(action, input, &output));
    }
    // The bytes of a file that is no store, piped in as the input.
    refused(
        "deserialize on stdin",
        cli(
            &[Path::new("deserialize"), &flag("-o"), &output],
            &fs::read(&garbage).unwrap(),
        ),
    );

    // The same output path is written when the input is good.
    let store = scratch.path("data.vortex");
    assert!(run("serialize", &rdf, &store).status.success());
    for action in ["deserialize", "match"] {
        fs::write(&output, KEPT).unwrap();
        let result = run(action, &store, &output);
        assert!(result.status.success(), "{action}: {result:?}");
        assert!(
            fs::read_to_string(&output)
                .unwrap()
                .contains("http://example.org/s"),
            "{action} wrote nothing"
        );
    }
}
