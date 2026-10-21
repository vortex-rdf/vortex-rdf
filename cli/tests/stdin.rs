//! `serialize` and `deserialize` without `-i` read their input from standard
//! input.

mod common;

use common::{Scratch, TRIPLE, cli, flag, run};
use std::fs;
use std::path::Path;

#[test]
fn deserialize_reads_a_store_piped_on_stdin() {
    let scratch = Scratch::new("stdin");
    let rdf = scratch.file("data.nt", TRIPLE);
    let store = scratch.path("data.vortex");
    assert!(run("serialize", &rdf, &store).status.success());

    let result = cli(&[Path::new("deserialize")], &fs::read(&store).unwrap());
    assert!(result.status.success(), "{result:?}");
    assert_eq!(String::from_utf8(result.stdout).unwrap(), TRIPLE);
}

#[test]
fn serialize_reads_rdf_piped_on_stdin() {
    let scratch = Scratch::new("serialize-stdin");
    let store = scratch.path("out.vortex");
    let result = cli(
        &[
            Path::new("serialize"),
            &flag("-f"),
            Path::new("nt"),
            &flag("-o"),
            &store,
        ],
        TRIPLE.as_bytes(),
    );
    assert!(result.status.success(), "{result:?}");

    let printed = cli(&[Path::new("deserialize"), &flag("-i"), &store], &[]);
    assert!(printed.status.success(), "{printed:?}");
    assert_eq!(String::from_utf8(printed.stdout).unwrap(), TRIPLE);
}
