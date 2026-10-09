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

// ─── Links and permissions (Unix) ──────────────────────────────────────

/// Replacing a store keeps what the old file was set up as: a symbolic link
/// at the path (a `current -> versions/v3.vortex` setup) still points at the
/// replaced file, and the permission bits carry over. Owner, ACLs and
/// extended attributes are not preserved. A store the process cannot write
/// (a read-only `0444` file) is never replaced: a read-only store signals
/// that it should not be overwritten.
#[cfg(unix)]
mod links_and_permissions {
    use super::*;
    use std::os::unix::fs::{PermissionsExt as _, symlink};

    const MODE_MASK: u32 = 0o7777;

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & MODE_MASK
    }

    fn is_link(path: &Path) -> bool {
        std::fs::symlink_metadata(path)
            .unwrap()
            .file_type()
            .is_symlink()
    }

    async fn rebuild(path: &Path, quads: Vec<Quad>) -> crate::error::Result<()> {
        crate::io::quads_stream_to_vortex_file(
            quad_stream(quads),
            path,
            LayoutStrategy::Default,
            vec![],
        )
        .await
    }

    /// `dir/versions/v3.vortex` holding twelve quads, and `dir/current` a
    /// relative link to it.
    async fn versioned_store() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let versions = dir.path().join("versions");
        std::fs::create_dir(&versions).unwrap();
        let v3 = versions.join("v3.vortex");
        crate::io::quads_stream_to_vortex_file(
            quad_stream(modular_quads(12, 3, 4)),
            &v3,
            LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .unwrap();
        let current = dir.path().join("current");
        symlink("versions/v3.vortex", &current).unwrap();
        (dir, v3, current)
    }

    /// A store written through a link replaces the file the link points to
    /// and leaves the link as it was; nothing else is left beside either.
    #[tokio::test]
    async fn test_a_symlink_to_a_store_keeps_its_link() {
        let (dir, v3, current) = versioned_store().await;

        rebuild(&current, modular_quads(5, 2, 2)).await.unwrap();

        assert!(is_link(&current), "the link must survive the rebuild");
        assert_eq!(
            std::fs::read_link(&current).unwrap(),
            PathBuf::from("versions/v3.vortex")
        );
        assert!(!is_link(&v3));
        for path in [&current, &v3] {
            let store = VortexRdfStore::from_file(path).await.unwrap();
            assert_eq!(store.size().await.unwrap(), 5, "{path:?}");
            assert_eq!(store.layout(), LayoutStrategy::Default, "{path:?}");
        }
        assert_eq!(
            entries(dir.path()),
            vec![current.clone(), dir.path().join("versions")]
        );
        assert_eq!(entries(&dir.path().join("versions")), vec![v3]);
    }

    /// Absolute targets and chains of links are followed to the file at the
    /// end, and every link on the way stays.
    #[tokio::test]
    async fn test_a_chain_of_symlinks_is_followed_to_the_end() {
        let (dir, v3, current) = versioned_store().await;
        std::fs::remove_file(&current).unwrap();
        symlink(&v3, &current).unwrap(); // absolute
        let latest = dir.path().join("latest");
        symlink("current", &latest).unwrap(); // a link to a link

        rebuild(&latest, modular_quads(7, 2, 2)).await.unwrap();

        assert!(is_link(&latest) && is_link(&current) && !is_link(&v3));
        assert_eq!(
            std::fs::read_link(&latest).unwrap(),
            PathBuf::from("current")
        );
        assert_eq!(std::fs::read_link(&current).unwrap(), v3);
        assert_eq!(
            VortexRdfStore::from_file(&v3)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            7
        );
    }

    /// A link to a file that is not there yet is written through, as creating
    /// the file always did.
    #[tokio::test]
    async fn test_a_dangling_symlink_gets_its_target_created() {
        let dir = tempfile::tempdir().unwrap();
        let versions = dir.path().join("versions");
        std::fs::create_dir(&versions).unwrap();
        let next = dir.path().join("next");
        symlink("versions/v4.vortex", &next).unwrap();

        rebuild(&next, modular_quads(6, 2, 2)).await.unwrap();

        assert!(is_link(&next));
        let v4 = versions.join("v4.vortex");
        assert!(!is_link(&v4));
        assert_eq!(
            VortexRdfStore::from_file(&next)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            6
        );
        assert_eq!(entries(&versions), vec![v4]);
    }

    /// A link that ends at a directory, or never ends, is refused before the
    /// input is read, and nothing is replaced.
    #[tokio::test]
    async fn test_a_symlink_to_a_directory_or_a_loop_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("target");
        std::fs::create_dir(&target_dir).unwrap();
        let to_dir = dir.path().join("to-dir");
        symlink("target", &to_dir).unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        symlink("b", &a).unwrap();
        symlink("a", &b).unwrap();

        for (path, what) in [(&to_dir, "to a directory"), (&a, "loop")] {
            let polled = Arc::new(AtomicBool::new(false));
            let error = crate::io::quads_stream_to_vortex_file(
                recording_stream(polled.clone()),
                path,
                LayoutStrategy::Default,
                vec![],
            )
            .await
            .expect_err(what);
            assert!(matches!(error, VortexRdfError::Io(_)), "{what}: {error}");
            assert!(!polled.load(Ordering::SeqCst), "{what}: the input was read");
            assert!(is_link(path), "{what}: the link was replaced");
        }
        assert_eq!(entries(&target_dir), Vec::<PathBuf>::new());
    }

    /// Compaction rewrites the store file through the same writer, so a store
    /// opened through a link is compacted in place and the link stays.
    #[tokio::test]
    async fn test_compacting_a_store_opened_through_a_symlink_keeps_the_link() {
        let (_dir, v3, current) = versioned_store().await;
        let store = VortexRdfStore::from_file(&current).await.unwrap();
        let extra = make_quad(
            "http://example.org/s99",
            "http://example.org/p0",
            "object 9",
            GraphName::DefaultGraph,
        );

        let compacted = store
            .add_quad(extra)
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();

        assert!(is_link(&current) && !is_link(&v3));
        assert_eq!(compacted.size().await.unwrap(), 13);
        assert_eq!(
            VortexRdfStore::from_file(&v3)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            13
        );
    }

    /// The new store has the permission bits of the one it replaces, however
    /// restrictive or permissive they are.
    #[tokio::test]
    async fn test_a_rebuild_keeps_the_permission_bits_of_the_store_it_replaces() {
        for mode in [0o600, 0o640, 0o664, 0o755] {
            let (_dir, path) =
                write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();

            rebuild(&path, modular_quads(5, 2, 2)).await.unwrap();

            assert_eq!(mode_of(&path), mode, "{mode:o}");
            assert_eq!(
                VortexRdfStore::from_file(&path)
                    .await
                    .unwrap()
                    .size()
                    .await
                    .unwrap(),
                5
            );
        }
    }

    /// Through a link, the permissions are those of the file it ends at.
    #[tokio::test]
    async fn test_a_rebuild_through_a_symlink_keeps_the_targets_permissions() {
        let (_dir, v3, current) = versioned_store().await;
        std::fs::set_permissions(&v3, std::fs::Permissions::from_mode(0o640)).unwrap();

        rebuild(&current, modular_quads(5, 2, 2)).await.unwrap();

        // v3 is the file that was replaced (it holds the new five quads), and
        // it kept its bits.
        assert_eq!(
            VortexRdfStore::from_file(&v3)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            5
        );
        assert_eq!(mode_of(&v3), 0o640);
    }

    /// A path with no store yet gets the permissions any new file does.
    #[tokio::test]
    async fn test_a_fresh_path_gets_the_default_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("plain");
        std::fs::File::create(&plain).unwrap();
        let path = dir.path().join("store.vortex");

        rebuild(&path, modular_quads(5, 2, 2)).await.unwrap();

        assert_eq!(mode_of(&path), mode_of(&plain));
    }

    /// While the replacement is being written it already has the old store's
    /// permissions, so a private store is never copied into a more readable
    /// temp file.
    #[tokio::test]
    async fn test_the_temp_file_has_the_old_permissions_while_it_is_written() {
        use crate::io::ser::write_store_atomically;
        use vortex_io::VortexWrite as _;

        let (dir, path) =
            write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let watched = dir.path().to_path_buf();
        let store = path.clone();

        write_store_atomically(&path, |mut writer| async move {
            writer.write_all(b"half a store".to_vec()).await?;
            let temp = entries(&watched)
                .into_iter()
                .find(|entry| *entry != store)
                .expect("the temp file exists while the store is written");
            assert_eq!(mode_of(&temp), 0o600);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(mode_of(&path), 0o600);
    }

    /// Make `path` read-only, and say whether the process can write it anyway
    /// (root and `CAP_DAC_OVERRIDE` bypass the mode): the refusal can only be
    /// observed where the mode is honoured.
    fn read_only_is_honoured(path: &Path) -> bool {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444)).unwrap();
        let bypassed = std::fs::OpenOptions::new().write(true).open(path).is_ok();
        if bypassed {
            eprintln!("skipped: this process can write a 0444 file (root?)");
        }
        !bypassed
    }

    /// A store the process cannot write is never replaced, as `File::create`
    /// never replaced one: the rebuild is refused with `PermissionDenied`
    /// naming the path, before any input is read, and the store stays
    /// byte-identical with nothing left beside it.
    #[tokio::test]
    async fn test_a_store_the_process_cannot_write_is_never_replaced() {
        let (dir, path) =
            write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
        if !read_only_is_honoured(&path) {
            return;
        }
        let before = std::fs::read(&path).unwrap();
        let polled = Arc::new(AtomicBool::new(false));

        let error = crate::io::quads_stream_to_vortex_file(
            recording_stream(polled.clone()),
            &path,
            LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .expect_err("a read-only store must not be replaced");

        let VortexRdfError::Io(io_error) = &error else {
            panic!("expected an I/O error, got {error:?}");
        };
        assert_eq!(
            io_error.kind(),
            std::io::ErrorKind::PermissionDenied,
            "{error}"
        );
        assert!(
            error.to_string().contains(&format!("{path:?}")),
            "the error must name the path: {error}"
        );
        assert!(
            !polled.load(Ordering::SeqCst),
            "the input was read although the store cannot be replaced"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(mode_of(&path), 0o444);
        assert_eq!(entries(dir.path()), vec![path.clone()]);
    }

    /// Through a link, the file the link ends at is the one probed: a link to
    /// a read-only store is refused and left as it was, and so is the store.
    #[tokio::test]
    async fn test_a_symlink_to_a_store_the_process_cannot_write_is_refused() {
        let (dir, v3, current) = versioned_store().await;
        if !read_only_is_honoured(&v3) {
            return;
        }
        let before = std::fs::read(&v3).unwrap();

        let error = rebuild(&current, modular_quads(5, 2, 2))
            .await
            .expect_err("a read-only store must not be replaced");

        assert!(
            matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
            "{error}"
        );
        assert!(
            error.to_string().contains(&format!("{current:?}")),
            "the error must name the path it was asked to write: {error}"
        );
        assert!(is_link(&current));
        assert_eq!(std::fs::read(&v3).unwrap(), before);
        assert_eq!(entries(&dir.path().join("versions")), vec![v3]);
    }

    /// Compaction rewrites the store file through the same writer, so a store
    /// file the process cannot write is not compacted either: the compaction
    /// fails and the file is left as it was.
    #[tokio::test]
    async fn test_compaction_leaves_a_store_the_process_cannot_write_alone() {
        let (dir, path) =
            write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
        if !read_only_is_honoured(&path) {
            return;
        }
        let before = std::fs::read(&path).unwrap();
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let extra = make_quad(
            "http://example.org/s99",
            "http://example.org/p0",
            "object 9",
            GraphName::DefaultGraph,
        );

        let error = store
            .add_quad(extra)
            .await
            .unwrap()
            .compact()
            .await
            .err()
            .expect("a read-only store file must not be rewritten");

        assert!(
            matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(entries(dir.path()), vec![path.clone()]);
    }
}
