//! Opening store files: `from_file` maps the file and reads it in place,
//! `from_file_in_memory` loads the whole store, and both answer alike.

use super::*;

#[tokio::test]
async fn test_from_file_maps_the_file() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(mapped.debug_file_mapped(), Some(true));
    assert_eq!(view_strings(&mapped).await, quad_strings(&quads));
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let by_p = mapped
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(by_p.size().await.unwrap(), 4);
}

#[tokio::test]
async fn test_from_file_in_memory_loads_the_whole_store() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert_eq!(loaded.debug_file_mapped(), None, "the rows live in memory");
    assert!(
        loaded.code_read_snapshot().is_some(),
        "the dictionary is resident"
    );
    assert_eq!(loaded.indexes(), &[IndexType::SecondaryByCopy]);
    // The loaded store reads nothing from the file: its bytes are overwritten
    // in place, so a read of it (through a path, a descriptor or a mapping)
    // would see garbage.
    let length = std::fs::metadata(&path).unwrap().len() as usize;
    std::fs::write(&path, vec![0xA5u8; length]).unwrap();
    assert_eq!(view_strings(&loaded).await, quad_strings(&quads));
}

/// `/proc/self/maps` is the process's own record of its mappings: a store
/// opened with `from_file` lists its file there, a store loaded whole never
/// does, and the mapping is gone once the store is dropped.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_from_file_is_listed_in_the_process_maps_only_while_mapped() {
    let (_dir, path) = write_store_file(
        dictionary_test_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let real = std::fs::canonicalize(&path).unwrap();
    let listed = || {
        std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .any(|line| line.contains(real.to_str().unwrap()))
    };
    assert!(!listed(), "nothing has opened the file yet");

    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert!(!listed(), "a store loaded whole maps nothing");
    drop(loaded);

    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    assert!(listed(), "a mapped store lists its file");
    assert_eq!(mapped.debug_file_mapped(), Some(true));
    drop(mapped);
    assert!(!listed(), "the mapping goes with the store");
}

/// A store loaded whole never maps its file, whether the open succeeds or
/// fails: a failing open names a newer store root (or reports Vortex's error
/// for bytes that are no store) without a mapping, which a file being
/// rewritten in place could pull out from under the read. A mapped open maps
/// the file once.
#[tokio::test]
async fn test_from_file_in_memory_never_maps_the_file() {
    use crate::io::container;
    use crate::io::read::map_probe;

    let (_dir, good) = write_store_file(
        dictionary_test_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    let bytes = std::fs::read(&good).unwrap();
    let mut newer = bytes.clone();
    let current = container::STORE_LAYOUT_ID.as_bytes();
    let at = newer
        .windows(current.len())
        .position(|window| window == current)
        .expect("the footer names the root layout");
    newer[at..at + current.len()].copy_from_slice(b"vortex-rdf.store.v3");

    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, contents: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, contents).unwrap();
        path
    };
    let newer = write("newer.vortex", &newer);
    let garbage = write("garbage.vortex", &[0xA5; 4096]);
    let empty = write("empty.vortex", &[]);

    map_probe::take();
    VortexRdfStore::from_file_in_memory(&good).await.unwrap();
    assert_eq!(map_probe::take(), 0, "a store loaded whole maps nothing");

    for (what, path) in [("newer", &newer), ("garbage", &garbage), ("empty", &empty)] {
        let error = VortexRdfStore::from_file_in_memory(path)
            .await
            .err()
            .unwrap_or_else(|| panic!("{what}: must not open"));
        assert_eq!(map_probe::take(), 0, "{what}: a failed open maps nothing");
        assert_eq!(
            error.to_string().contains("written by a newer vortex-rdf"),
            what == "newer",
            "{what}: {error}"
        );
    }

    VortexRdfStore::from_file(&good).await.unwrap();
    assert_eq!(map_probe::take(), 1, "a mapped open maps the file once");
}

/// Compaction replaces a store file by renaming over it: the open, mapped
/// store keeps reading its own (old) file, a fresh open sees the new one.
#[cfg(unix)]
#[tokio::test]
async fn test_mapped_store_survives_rename_over_its_path() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    let (_other_dir, other) =
        write_store_file(modular_quads(5, 2, 2), LayoutStrategy::Dictionary, vec![]).await;
    std::fs::rename(&other, &path).unwrap();
    assert_eq!(view_strings(&mapped).await, quad_strings(&quads));
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

/// A path that cannot be opened is the same I/O error in both modes, naming
/// the path and keeping the kind: a missing file, and (off root) one the
/// process may not read.
#[tokio::test]
async fn test_open_errors_are_io_errors_naming_the_path_in_both_modes() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.vortex");
    #[allow(unused_mut)]
    let mut cases = vec![(missing, std::io::ErrorKind::NotFound)];
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let (_store_dir, store) =
            write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
        let sealed = dir.path().join("sealed.vortex");
        std::fs::copy(&store, &sealed).unwrap();
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::File::open(&sealed).is_ok() {
            eprintln!("skipped: this process can read a 0000 file (root?)");
        } else {
            cases.push((sealed, std::io::ErrorKind::PermissionDenied));
        }
    }

    for (path, kind) in cases {
        let mapped = VortexRdfStore::from_file(&path).await;
        let loaded = VortexRdfStore::from_file_in_memory(&path).await;
        for (how, result) in [("from_file", mapped), ("from_file_in_memory", loaded)] {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{how}: must not open"));
            assert!(
                matches!(&error, VortexRdfError::Io(e) if e.kind() == kind),
                "{how}: {error:?}"
            );
            assert!(
                error.to_string().contains(&format!("{path:?}")),
                "{how}: the error must name the path: {error}"
            );
        }
    }
}
