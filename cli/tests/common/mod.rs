//! What the CLI's integration tests share: a scratch directory, the RDF they
//! write into it, and a runner for the built binary.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// One default-graph triple, spelled as both N-Triples and N-Quads write it.
pub const TRIPLE: &str = "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n";

/// A scratch directory removed when dropped.
pub struct Scratch(PathBuf);

impl Scratch {
    pub fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("vortex-rdf-cli-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn file(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Runs the binary with `args`, feeding `stdin` to its standard input.
pub fn cli(args: &[&Path], stdin: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_vortex-rdf-cli"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

/// A flag spelled as a path, so it can sit beside the paths in `cli`'s
/// arguments.
pub fn flag(name: &str) -> PathBuf {
    PathBuf::from(name)
}

/// `action -i input -o output`, with nothing on standard input.
pub fn run(action: &str, input: &Path, output: &Path) -> Output {
    cli(
        &[Path::new(action), &flag("-i"), input, &flag("-o"), output],
        &[],
    )
}
