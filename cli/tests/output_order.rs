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
    let directory = scratch.path("a-directory.vortex");
    fs::create_dir(&directory).unwrap();
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
        result
    };
    for (action, input) in [
        ("deserialize", &garbage),
        ("deserialize", &missing),
        ("deserialize", &directory),
        ("match", &garbage),
        ("match", &missing),
        ("match", &directory),
        ("match", &rdf_missing),
    ] {
        let result = refused(&format!("{action} {input:?}"), run(action, input, &output));
        if input == &directory {
            let stderr = String::from_utf8_lossy(&result.stderr);
            assert!(
                stderr.contains("is a directory"),
                "{action} should say the input is a directory: {stderr}"
            );
        }
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

/// `/dev/full` opens for writing and fails every write with ENOSPC. A
/// one-line export is held by the buffered writer until its final flush; a
/// longer one overflows the writer's 8 KiB buffer, so a write fails first.
/// Either way the command fails with its own failure text and the write
/// error.
#[cfg(target_os = "linux")]
#[test]
fn an_output_that_cannot_be_written_is_reported() {
    let scratch = Scratch::new("unwritable");
    let long: String = (0..400)
        .map(|i| {
            format!("<http://example.org/s{i}> <http://example.org/p> <http://example.org/o> .\n")
        })
        .collect();
    for (name, text) in [("one-line", TRIPLE), ("long", long.as_str())] {
        let rdf = scratch.file(&format!("{name}.nt"), text);
        let store = scratch.path(&format!("{name}.vortex"));
        assert!(run("serialize", &rdf, &store).status.success());

        for (action, failure) in [
            ("deserialize", "Failed to export RDF from Vortex"),
            ("match", "Failed to export filtered results"),
        ] {
            let result = run(action, &store, Path::new("/dev/full"));
            let stderr = String::from_utf8_lossy(&result.stderr);
            assert!(
                !result.status.success(),
                "{name} {action} succeeded: {result:?}"
            );
            assert!(
                stderr.contains(failure) && stderr.contains("No space left on device"),
                "{name} {action} did not report {failure:?} and the write error: {stderr}"
            );
        }
    }
}
