//! How `deserialize` and `match` treat their output file. They open their
//! input before they create it: an input that cannot be read leaves an
//! existing output file as it was. And an output that cannot be written is
//! reported, not swallowed.

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

/// `/dev/full` opens for writing and fails every write with ENOSPC. The export
/// is one line, which a buffered writer holds until its final flush: the error
/// of that flush must still end the command in failure.
#[cfg(target_os = "linux")]
#[test]
fn an_output_that_cannot_be_written_is_reported() {
    let scratch = Scratch::new("unwritable");
    let rdf = scratch.file("data.nt", TRIPLE);
    let store = scratch.path("data.vortex");
    assert!(run("serialize", &rdf, &store).status.success());

    for action in ["deserialize", "match"] {
        let result = run(action, &store, Path::new("/dev/full"));
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success(), "{action} succeeded: {result:?}");
        assert!(
            stderr.contains("No space left on device"),
            "{action} did not report the write error: {stderr}"
        );
    }
}
