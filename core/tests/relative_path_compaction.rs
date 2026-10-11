//! A store opened by a relative path compacts over the file it opened, whatever
//! the working directory has become since: it keeps the absolute path of the
//! open, in both access modes.
//!
//! This is its own test binary because the working directory is process-wide.
#![cfg(unix)]

use futures::stream;
use oxrdf::{GraphName, Literal, NamedNode, Quad};
use vortex_rdf_core::io::quads_stream_to_vortex_file;
use vortex_rdf_core::{IndexType, LayoutStrategy, RawQuad, VortexRdfStore};

fn raw_quads(n: usize) -> Vec<RawQuad> {
    (0..n)
        .map(|i| RawQuad {
            s: format!("<http://example.org/subject/{i:06}>"),
            p: "<http://example.org/predicate>".to_string(),
            o: format!("\"value {i:06}\""),
            g: String::new(),
        })
        .collect()
}

/// `n` quads past those of `raw_quads`, enough to cross the auto-compaction
/// floor in one append.
fn appended(first: usize, n: usize) -> Vec<Quad> {
    (first..first + n)
        .map(|i| {
            Quad::new(
                NamedNode::new(format!("http://example.org/subject/{i:06}")).unwrap(),
                NamedNode::new("http://example.org/predicate").unwrap(),
                Literal::new_simple_literal(format!("value {i:06}")),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

fn names(directory: &std::path::Path) -> Vec<std::ffi::OsString> {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}

#[tokio::test]
async fn a_relative_path_compacts_over_its_own_file_after_a_chdir() {
    const BASE: usize = 4;
    const APPENDED: usize = 4_200;

    let home = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let path = home.path().join("store.vortex");
    quads_stream_to_vortex_file(
        stream::iter(raw_quads(BASE).into_iter().map(Ok)),
        &path,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();

    let original = std::env::current_dir().unwrap();
    std::env::set_current_dir(home.path()).unwrap();
    let mapped = VortexRdfStore::from_file("store.vortex").await.unwrap();
    let loaded = VortexRdfStore::from_file_in_memory("store.vortex")
        .await
        .unwrap();
    std::env::set_current_dir(elsewhere.path()).unwrap();

    // An append past the auto-compaction floor folds the tail in.
    let compacted = mapped.add_quads(appended(BASE, APPENDED)).await.unwrap();
    assert_eq!(compacted.tail_len(), 0, "the append must compact");
    assert_eq!(compacted.size().await.unwrap(), BASE + APPENDED);
    let rebuilt = loaded.add_quads(appended(BASE, APPENDED)).await.unwrap();
    assert_eq!(rebuilt.tail_len(), 0, "the append must compact");
    assert_eq!(rebuilt.size().await.unwrap(), BASE + APPENDED);

    // The file the stores opened took the compaction ...
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.size().await.unwrap(), BASE + APPENDED);
    assert_eq!(
        names(home.path()),
        vec![std::ffi::OsString::from("store.vortex")]
    );
    // ... and nothing was written where the process has moved to.
    let strays = names(elsewhere.path());
    std::env::set_current_dir(original).unwrap();
    assert!(
        strays.is_empty(),
        "files created in the new directory: {strays:?}"
    );
}
