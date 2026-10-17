//! `deserialize` without `-i` reads its store from standard input.

mod common;

use common::{Scratch, TRIPLE, cli, run};
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
