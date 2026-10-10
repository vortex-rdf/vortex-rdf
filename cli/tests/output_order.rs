//! `deserialize` and `match` open their input before they create their output
//! file: an input that cannot be read leaves an existing output file as it
//! was, instead of truncating it first.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const KEPT: &str = "the previous output\n";
const TRIPLE: &str = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n";

/// A scratch directory removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("vortex-rdf-cli-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn file(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn cli(args: &[&Path]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_vortex-rdf-cli"))
        .args(args)
        .output()
        .unwrap()
}

fn flag(name: &str) -> PathBuf {
    PathBuf::from(name)
}

fn run(action: &str, input: &Path, output: &Path) -> Output {
    cli(&[Path::new(action), &flag("-i"), input, &flag("-o"), output])
}

#[test]
fn an_unreadable_input_leaves_the_output_file_alone() {
    let scratch = Scratch::new("refused");
    let garbage = scratch.file("garbage.vortex", "this is not a store");
    let missing = scratch.path("missing.vortex");
    let rdf = scratch.file("data.nt", TRIPLE);
    let rdf_missing = scratch.path("missing.nt");
    let output = scratch.file("out.nq", KEPT);

    for (action, input) in [
        ("deserialize", &garbage),
        ("deserialize", &missing),
        ("match", &garbage),
        ("match", &missing),
        ("match", &rdf_missing),
    ] {
        let result = run(action, input, &output);
        assert!(!result.status.success(), "{action} {input:?} should fail");
        assert_eq!(
            fs::read_to_string(&output).unwrap(),
            KEPT,
            "{action} {input:?} touched the output file"
        );
    }

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
