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
    // Nothing reads the file any more.
    std::fs::remove_file(&path).unwrap();
    assert_eq!(view_strings(&loaded).await, quad_strings(&quads));
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
