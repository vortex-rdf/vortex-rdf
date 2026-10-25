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

/// A link to a file that is not there yet is written through: the file is
/// created where the link points.
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

/// A temp file that stands in for an existing store is born private
/// (`0o600`), whatever the umask would give a new file, so no byte of a
/// private store is ever in a file others can read, not even before the
/// old permissions are copied on. A temp for a fresh path gets what any
/// new file gets.
#[tokio::test]
async fn test_a_temp_standing_in_for_a_store_is_born_private() {
    use crate::io::write::create_temp;

    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain");
    std::fs::File::create(&plain).unwrap();
    let default = mode_of(&plain);
    if default == 0o600 {
        eprintln!("skipped: the umask already makes new files private");
        return;
    }
    let replacing = dir.path().join("replacing.tmp");
    let fresh = dir.path().join("fresh.tmp");

    create_temp(&replacing, true).unwrap();
    create_temp(&fresh, false).unwrap();

    assert_eq!(mode_of(&replacing), 0o600);
    assert_eq!(mode_of(&fresh), default);
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

/// The counter above counts: compacting a store file the process can
/// write gathers its live rows exactly once.
#[tokio::test]
async fn test_compaction_of_a_writable_store_gathers_once() {
    let (_dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let gathered = crate::store::test_hooks::gathers();

    let compacted = store.compact().await.unwrap();

    assert_eq!(crate::store::test_hooks::gathers(), gathered + 1);
    assert_eq!(compacted.size().await.unwrap(), 12);
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
/// fails and the file is left as it was — and it fails first, before the
/// live rows are gathered, sorted and built (the gather is counted).
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

    let tailed = store.add_quad(extra).await.unwrap();
    let gathered = crate::store::test_hooks::gathers();

    let error = tailed
        .compact()
        .await
        .err()
        .expect("a read-only store file must not be rewritten");

    assert_eq!(
        crate::store::test_hooks::gathers(),
        gathered,
        "the live rows were gathered although the store file cannot be rewritten"
    );

    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// Puts a directory's mode back when dropped, so the temp directory can
/// still be removed whatever a test did to it.
struct RestoreMode {
    path: PathBuf,
    mode: u32,
}

impl RestoreMode {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            mode: mode_of(path),
        }
    }
}

impl Drop for RestoreMode {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.mode));
    }
}

/// Make `path` a directory the process cannot create files in (`0o555`)
/// until the guard drops. `None` where the process creates files in such
/// a directory anyway (root): the refusal can only be observed where the
/// mode is honoured.
fn read_only_dir(path: &Path) -> Option<RestoreMode> {
    let restore = RestoreMode::new(path);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = path.join("probe");
    if std::fs::File::create(&probe).is_ok() {
        std::fs::remove_file(&probe).unwrap();
        eprintln!("skipped: this process can create files in a 0555 directory (root?)");
        return None;
    }
    Some(restore)
}

/// Compaction's spill runs go where its temp file goes — beside the file
/// the store replaces, links followed — not beside the link the store was
/// opened through. Through `current -> versions/v3.vortex` in a directory
/// that takes no new files, the compaction succeeds because `versions/`
/// does, and nothing is left in either directory. The run directory is
/// named after the file it sits beside, not after the link.
#[tokio::test]
async fn test_compaction_spills_beside_the_file_it_replaces_not_the_link() {
    use crate::store::builders::spill::spill_hooks;
    // `VORTEX_RDF_SPILL_DIR` outranks any placement by design: only assert
    // the default placement when it is absent.
    if std::env::var_os("VORTEX_RDF_SPILL_DIR").is_some() {
        eprintln!("skipped: VORTEX_RDF_SPILL_DIR is set");
        return;
    }
    let (dir, v3, current) = versioned_store().await;
    let store = VortexRdfStore::from_file(&current).await.unwrap();
    let extra = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let tailed = store.add_quad(extra).await.unwrap();
    let Some(_read_only) = read_only_dir(dir.path()) else {
        return;
    };
    // The setup's own build spilled beside `v3.vortex` as well.
    let setup_spill = spill_hooks::last_dir();

    let compacted = tailed
        .compact()
        .await
        .expect("the link's directory takes no spill runs, the target's directory does");

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
    let versions = dir.path().join("versions");
    let spilled = spill_hooks::last_dir().expect("the compaction made a run directory");
    assert_ne!(
        Some(&spilled),
        setup_spill.as_ref(),
        "the compaction made a run directory of its own"
    );
    assert_eq!(spilled.parent(), Some(versions.as_path()));
    let name = spilled.file_name().unwrap().to_string_lossy().into_owned();
    assert!(name.starts_with(".v3.vortex.spill-"), "{name}");
    assert_eq!(entries(dir.path()), vec![current.clone(), versions.clone()]);
    assert_eq!(entries(&versions), vec![v3]);
}

/// A spill run that cannot be written fails with an error that names the
/// directory it was to be written in and the `VORTEX_RDF_SPILL_DIR` way
/// out (a writable directory), and keeps the I/O error's kind: here, a
/// run file in a directory that takes no new files.
#[test]
fn test_spill_write_error_names_the_directory_and_the_hint() {
    use crate::store::builders::runs::RecordWriter;
    let dir = tempfile::tempdir().unwrap();
    let Some(_read_only) = read_only_dir(dir.path()) else {
        return;
    };

    let error = RecordWriter::create(&dir.path().join("quads_run_0.bin"))
        .err()
        .expect("a directory that takes no new files takes no spill run");

    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
        "{error}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&dir.path().display().to_string()),
        "{message}"
    );
    assert!(
        message.ends_with("; set VORTEX_RDF_SPILL_DIR to a writable directory"),
        "{message}"
    );
}

/// A run directory that cannot be created fails the same way, naming the
/// directory it was to be created in: `.` for the working directory, the
/// one a bare relative output path is in.
#[test]
fn test_spill_directory_error_names_the_directory_and_the_hint() {
    use crate::store::builders::spill::{TempRunsGuard, spill_io_error};
    // `VORTEX_RDF_SPILL_DIR` outranks the base by design: only assert
    // where it is absent.
    if std::env::var_os("VORTEX_RDF_SPILL_DIR").is_some() {
        eprintln!("skipped: VORTEX_RDF_SPILL_DIR is set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let Some(_read_only) = read_only_dir(dir.path()) else {
        return;
    };

    let error = TempRunsGuard::create(None, Some(dir.path()))
        .err()
        .expect("a directory that takes no new files takes no run directory");

    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
        "{error}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&dir.path().display().to_string()),
        "{message}"
    );
    assert!(
        message.ends_with("; set VORTEX_RDF_SPILL_DIR to a writable directory"),
        "{message}"
    );
    let in_cwd = spill_io_error(
        Path::new(""),
        "directory creation",
        std::io::ErrorKind::PermissionDenied.into(),
    );
    assert!(in_cwd.to_string().contains(" failed in .: "), "{in_cwd}");
}

/// A serialization to a file spills beside it, on the volume that takes
/// the store: the run directory is a hidden sibling named after the
/// output, and it is gone once the write is done.
#[tokio::test]
async fn test_serialize_spills_beside_the_output() {
    use crate::store::builders::spill::spill_hooks;
    // `VORTEX_RDF_SPILL_DIR` outranks any placement by design: only assert
    // the default placement when it is absent.
    if std::env::var_os("VORTEX_RDF_SPILL_DIR").is_some() {
        eprintln!("skipped: VORTEX_RDF_SPILL_DIR is set");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.vortex");

    rebuild(&path, modular_quads(12, 3, 4)).await.unwrap();

    let spilled = spill_hooks::last_dir().expect("the build made a run directory");
    assert_eq!(spilled.parent(), Some(dir.path()));
    let name = spilled.file_name().unwrap().to_string_lossy().into_owned();
    assert!(name.starts_with(".out.vortex.spill-"), "{name}");
    assert_eq!(entries(dir.path()), vec![path]);
}

/// 4,200 new quads: past the 4,096-row auto-compaction floor.
fn past_the_floor() -> Vec<Quad> {
    (100..4_300)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// What an append whose auto-compaction was refused leaves: `batch` is in
/// the tail on top of the twelve quads of the base, matches and counts
/// see it, nothing was gathered since `gathered` (the refusal came before
/// any work), an explicit `compact()` still reports `PermissionDenied`,
/// and a later append goes the same way and is kept too. Returns the
/// store after that later append.
async fn assert_kept_in_the_tail(
    appended: VortexRdfStore,
    batch: &[Quad],
    gathered: usize,
) -> VortexRdfStore {
    assert_eq!(appended.tail_len(), 4_200, "the batch is in the tail");
    assert_eq!(appended.size().await.unwrap(), 12 + 4_200);
    let appended_subject =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s00100").unwrap());
    assert_eq!(
        matched_size(&appended, Some(&appended_subject), None, None, None).await,
        1
    );
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let expected_p0 = modular_quads(12, 3, 4)
        .iter()
        .chain(batch.iter())
        .filter(|quad| quad.predicate == p0)
        .count();
    assert_eq!(
        matched_size(&appended, None, Some(&p0), None, None).await,
        expected_p0
    );
    assert_eq!(crate::store::test_hooks::gathers(), gathered);

    // Explicit compaction still says why it cannot happen.
    let error = appended
        .compact()
        .await
        .err()
        .expect("an explicit compaction of a store that cannot be rewritten must fail");
    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
        "{error}"
    );

    // A later append retries, is refused the same way, and is kept too.
    let extra = make_quad(
        "http://example.org/s99999",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let more = appended.add_quad(extra).await.unwrap();
    assert_eq!(more.tail_len(), 4_201);
    assert_eq!(more.size().await.unwrap(), 12 + 4_201);
    assert_eq!(crate::store::test_hooks::gathers(), gathered);
    more
}

/// An append whose auto-compaction is refused because the store file
/// cannot be written keeps its batch: the quads stay in the in-memory
/// tail, where matches and counts see them, and the store comes back.
/// Nothing is gathered or built to find that out, the file is untouched,
/// an explicit `compact()` still reports `PermissionDenied`, and later
/// appends go the same way.
#[tokio::test]
async fn test_appends_past_the_floor_stay_in_the_tail_when_the_file_cannot_be_rewritten() {
    let (dir, path) = write_store_file(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    if !read_only_is_honoured(&path) {
        return;
    }
    let before = std::fs::read(&path).unwrap();
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let batch = past_the_floor();
    let gathered = crate::store::test_hooks::gathers();

    let appended = store
        .add_quads(batch.clone())
        .await
        .expect("the batch must be kept in the tail, not lost to the refused compaction");
    assert_kept_in_the_tail(appended, &batch, gathered).await;

    // The file is as it was.
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(mode_of(&path), 0o444);
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// The rewrite also needs the directory (the temp file is created beside
/// the store, then renamed over it), so a writable store file in a
/// directory the process cannot write into is no more compactable than a
/// read-only file, and its appends are kept in the tail the same way.
#[tokio::test]
async fn test_appends_stay_in_the_tail_when_the_directory_cannot_be_written() {
    let (dir, path) = write_store_file(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let before = std::fs::read(&path).unwrap();
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let Some(_read_only) = read_only_dir(dir.path()) else {
        return;
    };
    let batch = past_the_floor();
    let gathered = crate::store::test_hooks::gathers();

    let appended = store
        .add_quads(batch.clone())
        .await
        .expect("the batch must be kept in the tail, not lost to the refused compaction");
    assert_kept_in_the_tail(appended, &batch, gathered).await;

    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// What compaction says when the file at the store's path is not the file
/// the store opened.
fn assert_replaced_since_open(error: &VortexRdfError, path: &Path) {
    assert!(
        matches!(
            error,
            VortexRdfError::InvalidOperation(message)
                if message.contains("was replaced since this store opened it")
                    && message.contains("reopen it")
                    && message.contains(&format!("{path:?}"))
        ),
        "{error}"
    );
    assert!(!error.is_unwritable(), "{error}");
}

/// A store opened through `current -> versions/v3.vortex` compacts the
/// file it opened. A link retargeted to another store since then is
/// refused, and neither store is touched.
#[tokio::test]
async fn test_compaction_refuses_a_link_retargeted_since_the_store_opened() {
    let (dir, v3, current) = versioned_store().await;
    let store = VortexRdfStore::from_file(&current).await.unwrap();
    let versions = dir.path().join("versions");
    let v4 = versions.join("v4.vortex");
    rebuild(&v4, modular_quads(5, 2, 2)).await.unwrap();
    std::fs::remove_file(&current).unwrap();
    symlink("versions/v4.vortex", &current).unwrap();
    let (v3_before, v4_before) = (std::fs::read(&v3).unwrap(), std::fs::read(&v4).unwrap());
    let extra = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let tailed = store.add_quad(extra).await.unwrap();

    let error = tailed
        .compact()
        .await
        .err()
        .expect("the link names another store");

    assert_replaced_since_open(&error, &current);
    assert_eq!(std::fs::read(&v4).unwrap(), v4_before, "v4 was overwritten");
    assert_eq!(std::fs::read(&v3).unwrap(), v3_before);
    assert_eq!(entries(&versions), vec![v3, v4]);
}

/// A file rebuilt by rename while the store has it open is another file
/// than the one the store opened: compaction refuses it and leaves the
/// rebuilt store as it is.
#[tokio::test]
async fn test_compaction_refuses_a_file_rebuilt_by_rename_since_the_store_opened() {
    let (dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    rebuild(&path, modular_quads(5, 2, 2)).await.unwrap();
    let rebuilt = std::fs::read(&path).unwrap();

    let error = store
        .compact()
        .await
        .err()
        .expect("the file is not the one the store opened");

    assert_replaced_since_open(&error, &path);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        rebuilt,
        "the rebuild was overwritten"
    );
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// A file removed since the store opened it is no other file: compaction
/// writes the store's rows to the path again, and leaves nothing else
/// beside them.
#[tokio::test]
async fn test_compacting_a_store_whose_file_was_removed_writes_it_again() {
    let quads = modular_quads(12, 3, 4);
    let (dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let extra = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    );
    let tailed = store.add_quad(extra.clone()).await.unwrap();
    std::fs::remove_file(&path).unwrap();

    let compacted = tailed.compact().await.unwrap();

    let mut want = quads;
    want.push(extra);
    assert_eq!(view_strings(&compacted).await, quad_strings(&want));
    let reopened = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(view_strings(&reopened).await, quad_strings(&want));
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// The append that crosses the auto-compaction threshold does not take
/// the refusal for the writer's "unwritable": it comes back as the error
/// it is, and the rebuilt file is untouched.
#[tokio::test]
async fn test_an_append_past_the_floor_does_not_absorb_a_replaced_file() {
    let (dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    rebuild(&path, modular_quads(5, 2, 2)).await.unwrap();
    let rebuilt = std::fs::read(&path).unwrap();

    let error = store
        .add_quads(past_the_floor())
        .await
        .err()
        .expect("the refusal is reported, not kept in the tail");

    assert_replaced_since_open(&error, &path);
    assert_eq!(std::fs::read(&path).unwrap(), rebuilt);
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// The file is checked again just before the rename: a store replaced
/// while the new one was being written is not overwritten, and the temp
/// file goes.
#[tokio::test]
async fn test_a_file_replaced_during_the_write_is_not_overwritten() {
    use crate::io::read::FileIdentity;
    use crate::io::write::PendingStore;
    use vortex_io::VortexWrite as _;

    let (dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
    let opened = FileIdentity::of(&std::fs::metadata(&path).unwrap());
    let pending = PendingStore::create_over(&path, opened).await.unwrap();
    let replacement = path.clone();

    let error = pending
        .write(|mut writer| async move {
            writer.write_all(b"the compacted rows".to_vec()).await?;
            rebuild(&replacement, modular_quads(5, 2, 2)).await
        })
        .await
        .expect_err("the file changed while the store was written");

    assert_replaced_since_open(&error, &path);
    let rebuilt = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(
        rebuilt.size().await.unwrap(),
        5,
        "the rebuild was overwritten"
    );
    assert_eq!(entries(dir.path()), vec![path.clone()]);
}

/// Only the writer's refusal before it builds anything — the store file
/// or its directory cannot be written — says "unwritable". A permission
/// error from later in the rewrite (here the rename, in a directory that
/// stopped taking files after the temp file was made) is a failure like
/// any other, and an append does not hide it.
#[tokio::test]
async fn test_only_the_refusal_before_the_build_counts_as_unwritable() {
    let (dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Default, vec![]).await;
    if !read_only_is_honoured(&path) {
        return;
    }
    let error = rebuild(&path, modular_quads(5, 2, 2))
        .await
        .expect_err("a read-only store must not be replaced");
    assert!(error.is_unwritable(), "a read-only file: {error}");

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    {
        let Some(_read_only) = read_only_dir(dir.path()) else {
            return;
        };
        let error = rebuild(&path, modular_quads(5, 2, 2))
            .await
            .expect_err("a store in a read-only directory must not be replaced");
        assert!(error.is_unwritable(), "a read-only directory: {error}");
    }

    let restore = RestoreMode::new(dir.path());
    let watched = dir.path().to_path_buf();
    let result = write_store_atomically(&path, |writer| async move {
        drop(writer);
        // The temp file exists; from here on the directory takes no rename.
        std::fs::set_permissions(&watched, std::fs::Permissions::from_mode(0o555)).unwrap();
        Ok(())
    })
    .await;
    drop(restore);
    let Err(error) = result else {
        eprintln!("skipped: the rename went through in a 0555 directory (root?)");
        return;
    };
    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
        "{error}"
    );
    assert!(
        !error.is_unwritable(),
        "a refused rename after the build is not the pre-flight refusal: {error}"
    );
}
