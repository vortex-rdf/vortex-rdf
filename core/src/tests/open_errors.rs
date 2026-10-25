//! Opening what this version cannot read faithfully is refused, on every
//! open path a test names, with an error that says why: term-code and row-id
//! columns narrower than `u64`, Dictionary rows without their dictionary, a
//! dictionary implementation this version does not know, dictionary and
//! index children and root layouts newer than it reads, a required component
//! it cannot interpret, an index child whose rows are not the quads', a
//! generic Vortex file and a `vortex-rdf.store.v1` root. An optional unknown
//! component is skipped.

use super::*;
use crate::io::container;

/// `bytes` opened each way `opens` lists, in that order: the byte opens over
/// the bytes, the file opens over a temp file holding them. The temp dir
/// comes back with the results, since a mapped store reads its file for as
/// long as it lives.
async fn open_each(
    bytes: &[u8],
    opens: &[Open],
) -> (
    tempfile::TempDir,
    Vec<(Open, crate::error::Result<VortexRdfStore>)>,
) {
    let (dir, path) = temp_store_file(bytes);
    let mut opened = Vec::new();
    for &open in opens {
        opened.push((open, open.open(&path).await));
    }
    (dir, opened)
}

/// The error each way `opens` lists refuses `bytes` with ([`open_each`]), in
/// order. Panics where an open succeeds.
async fn open_errors(bytes: &[u8], opens: &[Open]) -> Vec<(Open, VortexRdfError)> {
    let (_dir, opened) = open_each(bytes, opens).await;
    opened
        .into_iter()
        .map(|(open, result)| {
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{open}: open should fail"));
            (open, err)
        })
        .collect()
}

/// A child's version ceiling, over a store of `rows` beside the components
/// `writes` makes at a version, written without the sorted stamp: at each of
/// `opens` the store opens from its bytes and from a file, and at each of
/// `refused` every open path refuses it with a `Deserialization` error naming
/// each of `needles(version)`.
async fn assert_version_ceiling(
    rows: &vortex_array::ArrayRef,
    writes: impl Fn(u32) -> Vec<container::NativeComponentWrite>,
    opens: &[u32],
    refused: &[u32],
    needles: impl Fn(u32) -> Vec<String>,
) {
    for &version in opens {
        let bytes = unstamped_store_bytes(vec![rows.clone()], writes(version)).await;
        let (_dir, opened) = open_each(&bytes, &[Open::Bytes, Open::Mapped]).await;
        for (how, result) in opened {
            assert!(result.is_ok(), "{how}: version {version} must open");
        }
    }

    for &version in refused {
        let bytes = unstamped_store_bytes(vec![rows.clone()], writes(version)).await;
        let (_dir, opened) = open_each(&bytes, &[Open::Bytes, Open::Mapped, Open::Loaded]).await;
        for (how, result) in opened {
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{how}: version {version} must be refused"));
            let VortexRdfError::Deserialization(message) = &err else {
                panic!("{how}: expected a Deserialization error, got {err:?}");
            };
            assert!(
                needles(version)
                    .iter()
                    .all(|needle| message.contains(needle.as_str())),
                "{how}: unexpected error: {message}"
            );
        }
    }
}

/// Term-code columns narrower than [`TermCode`] (u32 codes) are refused by
/// every open path, with an error naming the table, the column and its width:
/// in the quad table, and in an index child beside u64 quad columns.
#[tokio::test]
async fn test_open_refuses_code_columns_narrower_than_u64() {
    use crate::store::layouts::dictionary::TermDictionary;
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;

    let dict = TermDictionary::from_sorted_column(VarBinViewArray::from_iter_str([
        "<http://example.org/a>",
    ]))
    .unwrap();
    fn narrow(names: &[&'static str]) -> vortex_array::ArrayRef {
        let columns: Vec<vortex_array::ArrayRef> = names
            .iter()
            .map(|_| Buffer::from_iter([0u32]).into_array())
            .collect();
        StructArray::try_new(names.into(), columns, 1, Validity::NonNullable)
            .unwrap()
            .into_array()
    }

    let quads = unstamped_store_bytes(
        vec![narrow(&["s", "p", "o", "g"])],
        vec![dict.to_write().unwrap()],
    )
    .await;
    let rows = narrow(&["val", "rid"]);
    let index = index_child_write(
        "index:ref-o",
        "secondary-by-reference/o",
        false,
        true,
        vec![rows],
    );
    let index_child = unstamped_store_bytes(
        vec![bare_code_quad_array(&[0])],
        vec![dict.to_write().unwrap(), index],
    )
    .await;

    for (bytes, want) in [
        (quads, "the quad table column s holds u32 term codes"),
        (
            index_child,
            "the index:ref-o column val holds u32 term codes",
        ),
    ] {
        for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped, Open::Loaded]).await {
            assert!(
                err.to_string().contains(want),
                "{open}: unexpected error: {err}"
            );
            assert!(err.to_string().contains("rebuild"), "{open}: {err}");
        }
    }
}

/// Row-id columns narrower than [`RowId`](crate::store::RowId) (u32 row ids)
/// are refused by every open path, with an error naming the child, the column
/// and its width: in a reference and a copy child beside u64 codes, and in a
/// reference child of the Default layout, whose values are strings.
#[tokio::test]
async fn test_open_refuses_row_ids_narrower_than_u64() {
    use crate::store::layouts::dictionary::TermDictionary;
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;

    let dict = TermDictionary::from_sorted_column(VarBinViewArray::from_iter_str([
        "<http://example.org/a>",
    ]))
    .unwrap();
    // One index child of `columns` (the last one a u32 `rid`) under `name`.
    let child = |name: &str, slug: &str, columns: Vec<(&'static str, vortex_array::ArrayRef)>| {
        let (names, arrays): (Vec<&str>, Vec<vortex_array::ArrayRef>) = columns.into_iter().unzip();
        let rows = StructArray::try_new(names.as_slice().into(), arrays, 1, Validity::NonNullable)
            .unwrap()
            .into_array();
        index_child_write(name, slug, false, true, vec![rows])
    };
    let code = || Buffer::from_iter([0u64]).into_array();
    let rid = || Buffer::from_iter([0u32]).into_array();

    let dictionary_ref = unstamped_store_bytes(
        vec![bare_code_quad_array(&[0])],
        vec![
            dict.to_write().unwrap(),
            child(
                "index:ref-o",
                "secondary-by-reference/o",
                vec![("val", code()), ("rid", rid())],
            ),
        ],
    )
    .await;
    let dictionary_copy = unstamped_store_bytes(
        vec![bare_code_quad_array(&[0])],
        vec![
            dict.to_write().unwrap(),
            child(
                "index:posg",
                "secondary-by-copy/posg",
                vec![
                    ("s", code()),
                    ("p", code()),
                    ("o", code()),
                    ("g", code()),
                    ("rid", rid()),
                ],
            ),
        ],
    )
    .await;
    let raw = crate::store::RawQuad::canonical(
        "<http://example.org/a>",
        "<http://example.org/a>",
        "<http://example.org/a>",
        "",
    )
    .unwrap();
    let default_ref = unstamped_store_bytes(
        vec![
            crate::store::builders::build_struct_array(&[raw], LayoutStrategy::Default, true)
                .unwrap(),
        ],
        vec![child(
            "index:ref-o",
            "secondary-by-reference/o",
            vec![
                (
                    "val",
                    VarBinViewArray::from_iter_str(["<http://example.org/a>"]).into_array(),
                ),
                ("rid", rid()),
            ],
        )],
    )
    .await;

    for (bytes, want) in [
        (
            dictionary_ref,
            "the index:ref-o column rid holds u32 row ids",
        ),
        (
            dictionary_copy,
            "the index:posg column rid holds u32 row ids",
        ),
        (default_ref, "the index:ref-o column rid holds u32 row ids"),
    ] {
        for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped, Open::Loaded]).await {
            assert!(
                err.to_string().contains(want),
                "{open}: unexpected error: {err}"
            );
            assert!(err.to_string().contains("rebuild"), "{open}: {err}");
        }
    }
}

/// Dictionary-dtype rows written without a dictionary component: bare codes
/// cannot self-describe, and both readers refuse to open them.
#[tokio::test]
async fn test_open_rejects_dictionary_rows_without_dictionary_component() {
    let bytes = unstamped_store_bytes(vec![bare_code_quad_array(&[0, 1, 2])], vec![]).await;
    for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped]).await {
        assert!(
            err.to_string().contains("no dictionary"),
            "{open}: unexpected error: {err}"
        );
    }
}

/// A dictionary component of an implementation this version does not know
/// is rejected at open, before any term is read.
#[tokio::test]
async fn test_open_rejects_unknown_dictionary_implementation() {
    let parts = built_store(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![])
        .await
        .to_serializable_parts()
        .await
        .unwrap();
    let mut dict = parts.dict.as_ref().unwrap().to_write().unwrap();
    dict.descriptor.implementation = "not-a-dictionary-v0".into();
    let bytes = unstamped_store_bytes(vec![parts.array.clone()], vec![dict]).await;

    for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped]).await {
        assert!(
            err.to_string()
                .contains("unsupported dictionary component implementation"),
            "{open}: unexpected error: {err}"
        );
    }
}

/// A dictionary component of a version newer than the one this version reads
/// is rejected at open, with an error that says so, whichever way the store
/// is opened: a newer writer may lay the child out in a way this reader would
/// silently misread. The current version, and every older one, still opens.
#[tokio::test]
async fn test_open_rejects_a_dictionary_version_newer_than_this_version_reads() {
    let parts = built_store(dictionary_test_quads(), LayoutStrategy::Dictionary, vec![])
        .await
        .to_serializable_parts()
        .await
        .unwrap();
    let with_version = |version: u32| {
        let mut dict = parts.dict.as_ref().unwrap().to_write().unwrap();
        dict.descriptor.version = version;
        vec![dict]
    };
    assert_version_ceiling(
        &parts.array,
        with_version,
        &[container::DICT_VERSION, container::DICT_VERSION - 1],
        &[container::DICT_VERSION + 1, 99, u32::MAX],
        |version| {
            vec![
                format!("dictionary component is version {version}"),
                format!("reads up to version {}", container::DICT_VERSION),
                "newer vortex-rdf".to_string(),
            ]
        },
    )
    .await;
}

/// An index child of a version newer than the one this version reads is
/// rejected at open like the dictionary child, whichever way the store is
/// opened. The current version still opens.
#[tokio::test]
async fn test_open_rejects_an_index_version_newer_than_this_version_reads() {
    let parts = built_store(
        dictionary_test_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .to_serializable_parts()
    .await
    .unwrap();
    let with_version = |version: u32| {
        let mut writes = vec![parts.dict.as_ref().unwrap().to_write().unwrap()];
        for component in &parts.components {
            let mut write = component.to_write().unwrap();
            write.descriptor.version = version;
            writes.push(write);
        }
        writes
    };

    assert_version_ceiling(
        &parts.array,
        with_version,
        &[container::INDEX_VERSION],
        &[container::INDEX_VERSION + 1, 99, u32::MAX],
        |version| {
            vec![
                format!("is version {version}"),
                format!("reads up to version {}", container::INDEX_VERSION),
                "written by a newer vortex-rdf".to_string(),
                "open it with a newer version".to_string(),
            ]
        },
    )
    .await;
}

/// A root layout from a `vortex-rdf.store.*` generation numbered above `v2`
/// was written by a newer vortex-rdf: every open path says so and names the
/// root layout.
#[tokio::test]
async fn test_open_rejects_a_root_layout_from_a_newer_vortex_rdf() {
    let current: &[u8] = container::STORE_LAYOUT_ID.as_bytes();
    let newer: &[u8] = b"vortex-rdf.store.v3";
    assert_eq!(current.len(), newer.len());

    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let mut bytes = built_store(
            modular_quads(12, 3, 4),
            layout,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .to_bytes()
        .await
        .unwrap();
        let at = bytes
            .windows(current.len())
            .position(|window| window == current)
            .expect("the footer names the root layout");
        bytes[at..at + newer.len()].copy_from_slice(newer);

        let (_dir, opened) = open_each(&bytes, &[Open::Bytes, Open::Mapped, Open::Loaded]).await;
        for (how, result) in opened {
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{layout:?}/{how}: a newer root must be refused"));
            let VortexRdfError::Deserialization(message) = &err else {
                panic!("{layout:?}/{how}: expected a Deserialization error, got {err:?}");
            };
            assert!(
                message.contains("written by a newer vortex-rdf")
                    && message.contains("vortex-rdf.store.v3")
                    && message.contains("open it with a newer version"),
                "{layout:?}/{how}: unexpected error: {message}"
            );
            assert!(
                !message.contains("Invalid encoding ID"),
                "{layout:?}/{how}: Vortex's own error leaked: {message}"
            );
        }
    }
}

/// A required component this version cannot interpret makes the store
/// unopenable — skipping it could silently change query results — while an
/// optional unknown component is skipped and the store answers as if it
/// were absent.
#[tokio::test]
async fn test_open_required_unknown_component_rejected_optional_skipped() {
    let quads = modular_quads(6, 3, 4);
    let raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    let rows = || {
        crate::store::builders::build_struct_array(&raws, LayoutStrategy::Default, false).unwrap()
    };

    let bytes = unstamped_store_bytes(
        vec![rows()],
        vec![unknown_component("index:future", "future-index-v1", true)],
    )
    .await;
    for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped]).await {
        assert!(
            err.to_string()
                .contains("required component this version cannot"),
            "{open}: unexpected error: {err}"
        );
    }

    let bytes = unstamped_store_bytes(
        vec![rows()],
        vec![unknown_component("index:future", "future-index-v1", false)],
    )
    .await;
    let (_dir, opened) = open_each(&bytes, &[Open::Bytes, Open::Mapped]).await;
    let p1 = iri("p1");
    for (open, result) in opened {
        let store = result.unwrap_or_else(|e| panic!("{open}: open should succeed: {e}"));
        assert_eq!(store.size().await.unwrap(), 6, "{open}");
        assert!(store.indexes().is_empty(), "{open}");
        assert_eq!(
            view_strings(&matched_view(&store, None, Some(&p1), None, None).await).await,
            expected_strings(&quads, |i| i % 3 == 1),
            "{open}"
        );
    }
}

/// An index child holding one row fewer than the quads is a corrupt or
/// foreign file; both readers reject it at open with the row counts — unless
/// the roster also carries a required component this version cannot
/// interpret, which both refuse first: every component is classified before
/// any row count is checked.
#[tokio::test]
async fn test_open_rejects_index_child_with_mismatched_rows() {
    let parts = built_store(
        modular_quads(6, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .to_serializable_parts()
    .await
    .unwrap();
    let short_children = || -> Vec<container::NativeComponentWrite> {
        parts
            .components
            .iter()
            .map(|component| {
                use vortex_array::IntoArray as _;
                let descriptor = component.to_write().unwrap().descriptor;
                let rows = component.rows().unwrap().clone().into_array();
                let short = rows.slice(0..rows.len() - 1).unwrap();
                component_write(descriptor, vec![short], container::default_child_strategy())
            })
            .collect()
    };
    let mut two_faults = short_children();
    two_faults.push(unknown_component("index:future", "future-index-v1", true));

    for (components, want) in [
        (short_children(), "holds 5 rows against 6 quad rows"),
        (two_faults, "required component this version cannot"),
    ] {
        let bytes = unstamped_store_bytes(vec![parts.array.clone()], components).await;
        for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped]).await {
            assert!(
                err.to_string().contains(want),
                "{open}: unexpected error: {err}"
            );
        }
    }
}

/// A generic Vortex writer's output — a valid Vortex file under a plain root
/// layout — is not a store: every open path says so with the generic error
/// naming the expected `v2` root layout, rather than a scan-time surprise.
#[tokio::test]
async fn test_open_rejects_foreign_vortex_bytes() {
    use vortex_file::WriteOptionsSessionExt as _;

    // A bare code schema, written by the raw Vortex writer under a plain
    // root layout.
    let array = bare_code_quad_array(&[1, 2, 3]);
    let dtype = array.dtype().clone();
    let stream = vortex_array::stream::ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(array) })),
    );
    let mut bytes: Vec<u8> = Vec::new();
    crate::session::VORTEX_SESSION
        .write_options()
        .write(&mut bytes, stream)
        .await
        .unwrap();

    for (open, err) in open_errors(&bytes, &[Open::Bytes, Open::Mapped, Open::Loaded]).await {
        let message = err.to_string();
        assert!(
            message.contains("not a vortex-rdf store file")
                && message.contains("expected the vortex-rdf.store.v2 root layout"),
            "{open}: unexpected error: {message}"
        );
    }
}

/// `bytes` with the store root layout renamed to `vortex-rdf.store.v1`. The
/// two ids are the same length, so no offset in the file moves, and a Vortex
/// file carries no checksum over its footer: a `v1` file is made here without
/// a binary fixture.
fn as_written_before_0_12(mut bytes: Vec<u8>) -> Vec<u8> {
    let current = container::STORE_LAYOUT_ID.as_bytes();
    let previous: &[u8] = b"vortex-rdf.store.v1";
    assert_eq!(current.len(), previous.len());
    let at: Vec<usize> = bytes
        .windows(current.len())
        .enumerate()
        .filter_map(|(at, window)| (window == current).then_some(at))
        .collect();
    assert_eq!(at.len(), 1, "the footer names the root layout once");
    bytes[at[0]..at[0] + previous.len()].copy_from_slice(previous);
    bytes
}

/// A store whose root layout is `vortex-rdf.store.v1` fails to open inside
/// Vortex, which has no layout by that id, on every open path: `from_bytes`,
/// `from_bytes_owned`, `from_file` and `from_file_in_memory`. The id is no
/// newer root, so the error does not say a newer vortex-rdf wrote the store,
/// and it names no vortex-rdf version.
#[tokio::test]
async fn a_v1_root_fails_to_open_inside_vortex() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let bytes = as_written_before_0_12(
            built_store(
                modular_quads(12, 3, 4),
                layout,
                vec![IndexType::SecondaryByReference],
            )
            .await
            .to_bytes()
            .await
            .unwrap(),
        );

        let opens = [Open::Bytes, Open::BytesOwned, Open::Mapped, Open::Loaded];
        let (_dir, opened) = open_each(&bytes, &opens).await;
        for (how, result) in opened {
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{layout:?}/{how}: a v1 root must not open"));
            let message = err.to_string();
            for needle in ["0.11", "newer"] {
                assert!(
                    !message.contains(needle),
                    "{layout:?}/{how}: the error mentions {needle:?}: {message}"
                );
            }
            assert!(
                matches!(err, VortexRdfError::Vortex(_)),
                "{layout:?}/{how}: expected Vortex's own error, got {err:?}"
            );
        }
    }
}
