//! Writing a store file is all-or-nothing: the bytes land in a sibling temp
//! file that is renamed over the target only once it is complete, so a write
//! that fails leaves no partial file and the previous store byte-identical,
//! and a successful one replaces the old file by rename (the old inode stays
//! alive for anything that has it mapped; `tests/replace_mapped_store.rs`
//! pins that against a live mapping in a process of its own).

use super::*;
use oxrdfio::RdfFormat;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Two valid triples, then a line no N-Triples parser accepts: the stream
/// fails after quads were already ingested.
const VALID_THEN_MALFORMED: &str = "\
<http://example.org/s1> <http://example.org/p> \"one\" .\n\
<http://example.org/s2> <http://example.org/p> \"two\" .\n\
<http://example.org/s3> <http://example.org/p> not-a-term .\n";

fn malformed_stream()
-> impl futures::Stream<Item = crate::error::Result<crate::store::RawQuad>> + Unpin + Send + 'static
{
    crate::common::terms::parse_quads_from_reader(
        Cursor::new(VALID_THEN_MALFORMED.as_bytes()),
        RdfFormat::NTriples,
    )
}

/// The directory's entries, sorted: what a test compares to say nothing but
/// the expected files is there.
fn entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    entries
}

const BOTH_INDEXES: [IndexType; 2] = [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];

/// A build whose input fails mid-stream — after valid quads — writes nothing
/// at a fresh path, under every layout, with and without indexes: no store
/// file and no temp file.
#[tokio::test]
async fn test_failed_build_leaves_nothing_at_a_fresh_path() {
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        for indexes in [vec![], BOTH_INDEXES.to_vec()] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.vortex");
            let error = crate::io::quads_stream_to_vortex_file(
                malformed_stream(),
                &path,
                layout,
                indexes.clone(),
            )
            .await
            .expect_err("a malformed line must fail the write");
            assert!(
                matches!(error, VortexRdfError::Deserialization(_)),
                "{layout:?}/{indexes:?}: {error}"
            );
            assert_eq!(
                entries(dir.path()),
                Vec::<PathBuf>::new(),
                "{layout:?}/{indexes:?}: a failed write must leave no store file and no temp file"
            );
        }
    }
}

/// When `path` already holds a store, a write that fails leaves that store
/// byte-identical, still openable, with no sibling beside it.
#[tokio::test]
async fn test_failed_build_keeps_the_existing_store_byte_identical() {
    let (dir, path) = write_store_file(
        modular_quads(12, 3, 4),
        LayoutStrategy::Dictionary,
        BOTH_INDEXES.to_vec(),
    )
    .await;
    let before = std::fs::read(&path).unwrap();

    crate::io::quads_stream_to_vortex_file(
        malformed_stream(),
        &path,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect_err("a malformed line must fail the write");

    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "the previous store must survive a failed rebuild untouched"
    );
    assert_eq!(entries(dir.path()), vec![path.clone()]);
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.size().await.unwrap(), 12);
    assert_eq!(reopened.layout(), LayoutStrategy::Dictionary);
}

/// A successful write over an existing store replaces it whole — new layout,
/// new rows, new indexes — and leaves only the store file in the directory.
#[tokio::test]
async fn test_successful_write_replaces_the_existing_store() {
    let (dir, path) = write_store_file(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let replacement = graph_modular_quads(
        5,
        3,
        2,
        2,
        &[
            GraphName::DefaultGraph,
            GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
        ],
    );
    crate::io::quads_stream_to_vortex_file(
        quad_stream(replacement.clone()),
        &path,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();

    assert_eq!(entries(dir.path()), vec![path.clone()]);
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(reopened.layout(), LayoutStrategy::Dictionary);
    assert_eq!(reopened.indexes(), &[IndexType::SecondaryByReference]);
    assert_eq!(view_strings(&reopened).await, quad_strings(&replacement));
}

/// An input that records whether anyone read it: an empty stream that flips
/// the flag the first time it is polled.
fn recording_stream(
    polled: Arc<AtomicBool>,
) -> impl futures::Stream<Item = crate::error::Result<crate::store::RawQuad>> + Unpin + Send + 'static
{
    futures::stream::poll_fn(move |_| {
        polled.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(None)
    })
}

/// A path that cannot take a store is reported before the input is read: a
/// missing directory fails at once, not after the whole ingest, sort and
/// dictionary have run.
#[tokio::test]
async fn test_a_missing_directory_fails_before_the_input_is_read() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("store.vortex");
        let polled = Arc::new(AtomicBool::new(false));

        let error = crate::io::quads_stream_to_vortex_file(
            recording_stream(polled.clone()),
            &path,
            layout,
            vec![],
        )
        .await
        .expect_err("there is nowhere to put the store");

        assert!(
            matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
            "{layout:?}: {error}"
        );
        assert!(
            !polled.load(Ordering::SeqCst),
            "{layout:?}: the input was read before the output path was known to take a store"
        );
        assert_eq!(entries(dir.path()), Vec::<PathBuf>::new(), "{layout:?}");
    }
}

/// A directory at `path` is refused up front too (it could only fail at the
/// rename, after the build), and is left exactly as it was.
#[tokio::test]
async fn test_a_directory_at_the_path_is_refused_before_the_input_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("keep.txt"), b"keep").unwrap();
    let polled = Arc::new(AtomicBool::new(false));

    let error = crate::io::quads_stream_to_vortex_file(
        recording_stream(polled.clone()),
        &path,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .expect_err("a directory cannot be replaced by a file");

    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::IsADirectory),
        "{error}"
    );
    assert!(
        !polled.load(Ordering::SeqCst),
        "the input was read although the path is a directory"
    );
    assert_eq!(entries(dir.path()), vec![path.clone()]);
    assert_eq!(entries(&path), vec![path.join("keep.txt")]);
}

/// A rename that fails after the whole store was written — the path became a
/// directory meanwhile, which the up-front check could not know — leaves the
/// directory untouched and removes the temp file.
#[tokio::test]
async fn test_a_rename_failing_after_the_write_removes_the_temp_file() {
    use crate::io::ser::write_store_atomically;
    use vortex_io::VortexWrite as _;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    let appears = path.clone();

    let error = write_store_atomically(&path, |mut writer| async move {
        writer.write_all(b"a whole store".to_vec()).await?;
        std::fs::create_dir(&appears).unwrap();
        std::fs::write(appears.join("keep.txt"), b"keep").unwrap();
        Ok(())
    })
    .await
    .expect_err("a directory cannot be replaced by a file");
    assert!(matches!(error, VortexRdfError::Io(_)), "{error}");

    assert_eq!(entries(dir.path()), vec![path.clone()]);
    assert_eq!(entries(&path), vec![path.join("keep.txt")]);
}

// ─── The shared writer: failure after the temp file exists ─────────────

/// A write that fails once the temp file holds bytes — at a fresh path, and
/// over an existing store — removes the temp file and leaves `path` as it
/// was: absent, or the previous store's exact bytes.
#[tokio::test]
async fn test_write_failing_after_the_temp_file_exists_removes_it() {
    use crate::io::ser::write_store_atomically;
    use vortex_io::VortexWrite as _;

    for existing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.vortex");
        if existing {
            std::fs::write(&path, b"the previous store").unwrap();
        }
        let before = entries(dir.path());
        let watched = dir.path().to_path_buf();
        let expected_before = before.clone();

        let error = write_store_atomically(&path, |mut writer| async move {
            writer.write_all(b"half a store".to_vec()).await?;
            // The temp file is a sibling of `path`, and it is there now.
            let during = entries(&watched);
            assert_eq!(during.len(), before.len() + 1, "{during:?}");
            let temp = during.iter().find(|p| !before.contains(p)).unwrap();
            assert!(
                temp.extension().is_some_and(|e| e == "tmp"),
                "a temp file must not look like a store: {temp:?}"
            );
            Err(VortexRdfError::Serialization("the write failed".into()))
        })
        .await
        .expect_err("the closure's error must come back");
        assert!(matches!(error, VortexRdfError::Serialization(_)), "{error}");

        assert_eq!(entries(dir.path()), expected_before, "existing={existing}");
        if existing {
            assert_eq!(std::fs::read(&path).unwrap(), b"the previous store");
        }
    }
}

/// Dropping the write while it is in flight — a cancelled task, a timeout —
/// removes the temp file too, and `path` is untouched.
#[tokio::test]
async fn test_dropping_the_write_removes_the_temp_file() {
    use crate::io::ser::write_store_atomically;
    use vortex_io::VortexWrite as _;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    std::fs::write(&path, b"the previous store").unwrap();

    let stalled = write_store_atomically(&path, |mut writer| async move {
        writer.write_all(b"half a store".to_vec()).await?;
        std::future::pending::<()>().await;
        Ok(())
    });
    tokio::time::timeout(std::time::Duration::from_millis(300), stalled)
        .await
        .expect_err("the stalled write must still be running when it is dropped");

    assert_eq!(entries(dir.path()), vec![path.clone()]);
    assert_eq!(std::fs::read(&path).unwrap(), b"the previous store");
}

/// The temp file is created in `path`'s directory under a name that cannot
/// be mistaken for a store, and a successful write leaves only `path`.
#[tokio::test]
async fn test_temp_file_is_a_sibling_and_gone_after_success() {
    use crate::io::ser::write_store_atomically;
    use vortex_io::VortexWrite as _;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    let watched = dir.path().to_path_buf();

    write_store_atomically(&path, |mut writer| async move {
        writer.write_all(b"a store".to_vec()).await?;
        writer.shutdown().await?;
        let during = entries(&watched);
        assert_eq!(during.len(), 1, "{during:?}");
        assert_eq!(during[0].parent(), Some(watched.as_path()));
        assert_ne!(during[0].file_name(), Some("store.vortex".as_ref()));
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(entries(dir.path()), vec![path.clone()]);
    assert_eq!(std::fs::read(&path).unwrap(), b"a store");
}
