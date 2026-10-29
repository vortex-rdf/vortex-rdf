//! `cargo run -p vortex-rdf-java --example fixtures -- <dir>`: the stores the Java tests open, one per layout
//! and index set (`dictionary.vortex`, `copy.vortex`, `reference.vortex`, `default.vortex`), and `oracle.tsv`,
//! their quads as four tab-separated N-Triples spellings per line (the default graph is an empty field).

#[path = "../tests/common/mod.rs"]
mod common;

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("usage: fixtures <dir>"));
    std::fs::create_dir_all(&dir).expect("create the fixture directory");
    let quads = common::quads(150);
    for built in common::builds() {
        common::write(&dir, &built, &quads);
    }
    let oracle: String = quads.iter().map(|q| q.join("\t") + "\n").collect();
    std::fs::write(dir.join("oracle.tsv"), oracle).expect("write the oracle");
}
