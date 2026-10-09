//! Rebuilding a store file while a live store has it memory-mapped.
//!
//! A file-backed store reads the pages of the file it mapped. A writer that
//! truncates and rewrites that same file pulls those pages out from under
//! it: the live store's next read faults past the new end of file (SIGBUS)
//! or decodes the new file's bytes as if they were the old ones. A writer
//! that builds beside the path and renames the finished file over it leaves
//! the old inode alive for the mapping, so the live store keeps answering
//! from the data it opened.
//!
//! This is its own test binary because, on a writer that rewrites in place,
//! the failure is the whole process dying of SIGBUS, which would take every
//! other test of a shared binary down with it.
#![cfg(target_os = "linux")]

use futures::stream;
use vortex_rdf_core::io::quads_stream_to_vortex_file;
use vortex_rdf_core::{IndexType, LayoutStrategy, RawQuad, VortexRdfStore};

/// `n` quads whose terms all carry `tag`, so a row read back says which
/// build it came from.
fn quads(tag: &str, n: usize) -> Vec<RawQuad> {
    (0..n)
        .map(|i| RawQuad {
            s: format!("<http://example.org/{tag}/subject/{i:06}>"),
            p: format!("<http://example.org/{tag}/predicate/{}>", i % 7),
            o: format!("\"{tag} value {i:06}\""),
            g: String::new(),
        })
        .collect()
}

async fn build(path: &std::path::Path, tag: &str, n: usize) {
    quads_stream_to_vortex_file(
        stream::iter(quads(tag, n).into_iter().map(Ok)),
        path,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_rebuild_over_a_mapped_store_leaves_the_open_store_readable() {
    // The old file is much larger than its replacement, so an in-place
    // rewrite leaves most of the old mapping beyond the new end of file.
    const OLD: usize = 20_000;
    const NEW: usize = 50;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");

    build(&path, "old", OLD).await;
    let live = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(live.size().await.unwrap(), OLD);

    build(&path, "new", NEW).await;

    // The store opened before the rebuild still answers from the file it
    // mapped: a full scan, and a match served by its reference index.
    let rows = live.quads_vec().await.unwrap();
    assert_eq!(rows.len(), OLD);
    assert!(
        rows.iter()
            .all(|quad| quad.subject.to_string().contains("/old/")),
        "the live store must keep reading the old data"
    );
    let predicate = oxrdf::NamedNode::new("http://example.org/old/predicate/3").unwrap();
    let matched = live
        .match_pattern(None, Some(&predicate), None, None)
        .await
        .unwrap();
    assert_eq!(
        matched.size().await.unwrap(),
        (0..OLD).filter(|i| i % 7 == 3).count()
    );

    // A store opened after the rebuild sees the new data.
    let fresh = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(fresh.size().await.unwrap(), NEW);
    assert!(
        fresh
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .all(|quad| quad.subject.to_string().contains("/new/"))
    );

    // Only the store file is left beside the path.
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(names, vec![std::ffi::OsString::from("store.vortex")]);
}
