//! Stores written by vortex-rdf 0.11 and earlier (root layout
//! `vortex-rdf.store.v1`) are refused, with an error that names the cause and
//! the way out.
//!
//! The `vortex-rdf.store.v2` readers rely on two guarantees that only a `v2`
//! writer gives: each quad is stored once, and a reference index's children
//! are in `(val, rid)` order (counting a located run from its width and
//! reading a window from its own rows both assume it, and nothing checks it at
//! open). A `v1` file can break either and still opens as a Vortex file, so a
//! `v1` root is refused rather than checked or repaired.
//!
//! The two ids are the same length, so renaming one into the other in a
//! written store moves no offset, and a Vortex file carries no checksum over
//! its footer: that is how these tests make a `v1` file without a binary
//! fixture.

use super::*;
use crate::store::RowId;
use crate::store::array::{StrColReader, field_as};
use crate::store::builders::sorted_stream;
use vortex_array::VortexSessionExecute as _;
use vortex_array::arrays::struct_::StructArrayExt as _;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

const CURRENT: &[u8] = b"vortex-rdf.store.v2";
const LEGACY: &[u8] = b"vortex-rdf.store.v1";

fn occurrences(bytes: &[u8], needle: &[u8]) -> usize {
    bytes
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

/// `bytes` as the 0.11 writer laid them out: the same container with the
/// previous root layout id.
fn as_written_before_0_12(mut bytes: Vec<u8>) -> Vec<u8> {
    assert_eq!(CURRENT.len(), LEGACY.len());
    assert_eq!(
        occurrences(&bytes, CURRENT),
        1,
        "the footer names the root layout once"
    );
    let at = bytes
        .windows(CURRENT.len())
        .position(|window| window == CURRENT)
        .unwrap();
    bytes[at..at + LEGACY.len()].copy_from_slice(LEGACY);
    bytes
}

/// The serialized bytes of a store built from [`modular_quads`].
async fn store_bytes(layout: LayoutStrategy, indexes: Indexes) -> Vec<u8> {
    let built =
        build_array::<SortedInMemoryBuilder>(quad_stream(modular_quads(12, 3, 4)), layout, indexes)
            .await
            .unwrap();
    VortexRdfStore::from_built(built)
        .unwrap()
        .to_bytes()
        .await
        .unwrap()
}

/// What a store written by 0.11 or earlier is told: the cause (the version
/// and the root layout), that it is refused, and the way out. Neither the
/// generic "not a vortex-rdf store file" message nor Vortex's own unknown
/// layout error is acceptable.
fn assert_refused_with_the_way_out(error: Option<VortexRdfError>, who: &str) {
    let error = error.unwrap_or_else(|| panic!("{who}: a pre-0.12 store must not open"));
    let VortexRdfError::Deserialization(message) = &error else {
        panic!("{who}: expected a Deserialization error, got {error:?}");
    };
    for needle in [
        "written by vortex-rdf 0.11 or earlier",
        "vortex-rdf.store.v1",
        "cannot read",
        "rebuild it from its RDF source with vortex-rdf 0.12 or later",
        "serialize_rdf",
    ] {
        assert!(
            message.contains(needle),
            "{who}: {message:?} lacks {needle:?}"
        );
    }
    for generic in ["not a vortex-rdf store file", "Invalid encoding ID"] {
        assert!(
            !message.contains(generic),
            "{who}: the generic error {generic:?} leaked: {message:?}"
        );
    }
}

/// A store written now carries the `v2` root, and only that: in the bytes
/// (once, in the footer's layout table) and as the opened file's root layout.
#[tokio::test]
async fn test_a_store_written_now_carries_the_v2_root_layout() {
    use vortex_file::OpenOptionsSessionExt as _;

    for layout in LAYOUTS {
        for indexes in index_sets() {
            let bytes = store_bytes(layout, indexes.clone()).await;
            let who = format!("{layout:?} / {indexes:?}");
            assert_eq!(occurrences(&bytes, CURRENT), 1, "{who}");
            assert_eq!(occurrences(&bytes, LEGACY), 0, "{who}");

            let file = crate::session::VORTEX_SESSION
                .open_options()
                .open_buffer(vortex_buffer::ByteBuffer::from(bytes))
                .unwrap();
            assert_eq!(
                file.footer().layout().encoding_id().as_ref(),
                "vortex-rdf.store.v2",
                "{who}"
            );
            assert!(crate::io::container::is_native_file(&file), "{who}");
        }
    }
}

/// A file written to disk carries it too.
#[tokio::test]
async fn test_a_store_file_written_now_carries_the_v2_root_layout() {
    let (_dir, path) =
        write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(occurrences(&bytes, CURRENT), 1);
    assert_eq!(occurrences(&bytes, LEGACY), 0);
}

/// Opening the bytes of a store with the `v1` root layout fails with the
/// actionable error, under every layout and with or without indexes.
#[tokio::test]
async fn test_a_pre_0_12_store_is_refused_from_bytes() {
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let who = format!("{layout:?} / {indexes:?}");
            let legacy = as_written_before_0_12(store_bytes(layout, indexes).await);
            assert_refused_with_the_way_out(
                VortexRdfStore::from_bytes(&legacy).await.err(),
                &format!("from_bytes {who}"),
            );
            assert_refused_with_the_way_out(
                VortexRdfStore::from_bytes_owned(legacy).await.err(),
                &format!("from_bytes_owned {who}"),
            );
        }
    }
}

/// So does opening the file, memory-mapped (`from_file`) or loaded whole.
#[tokio::test]
async fn test_a_pre_0_12_store_is_refused_from_a_file() {
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let who = format!("{layout:?} / {indexes:?}");
            let (dir, path) = write_store_file(modular_quads(12, 3, 4), layout, indexes).await;
            let legacy_path = dir.path().join("legacy.vortex");
            std::fs::write(
                &legacy_path,
                as_written_before_0_12(std::fs::read(&path).unwrap()),
            )
            .unwrap();

            assert_refused_with_the_way_out(
                VortexRdfStore::from_file(&legacy_path).await.err(),
                &format!("from_file {who}"),
            );
            assert_refused_with_the_way_out(
                VortexRdfStore::from_file_in_memory(&legacy_path)
                    .await
                    .err(),
                &format!("from_file_in_memory {who}"),
            );
            // The current file beside it still opens.
            assert_eq!(
                VortexRdfStore::from_file(&path)
                    .await
                    .unwrap()
                    .size()
                    .await
                    .unwrap(),
                12,
                "{who}"
            );
        }
    }
}

/// A file that is not a store at all keeps the generic error, now naming the
/// `v2` root: the legacy message is for the `v1` root only.
#[tokio::test]
async fn test_a_foreign_root_keeps_the_generic_error() {
    use vortex_file::WriteOptionsSessionExt as _;

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

    let message = VortexRdfStore::from_bytes(&bytes)
        .await
        .err()
        .expect("open should fail")
        .to_string();
    assert!(
        message.contains("not a vortex-rdf store file")
            && message.contains("expected the vortex-rdf.store.v2 root layout")
            && !message.contains("0.11"),
        "{message}"
    );
}

// ─── The guarantees the refusal protects ───────────────────────────────

/// One `val` of a reference index child: a term code under the Dictionary
/// layout, the term's N-Triples string under the others.
#[derive(Debug, PartialEq, PartialOrd)]
enum Val {
    Code(TermCode),
    Term(String),
}

/// A reference child's rows as `(val, rid)` pairs, in file order.
fn reference_child(component: &crate::store::indexes::IndexComponent) -> Vec<(Val, RowId)> {
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let rows = component.rows().unwrap();
    let rid: PrimitiveArray = field_as(rows, "rid", &mut ctx).unwrap();
    let val = rows.unmasked_field_by_name("val").unwrap();
    let vals: Vec<Val> = if matches!(val.dtype(), vortex_array::dtype::DType::Utf8(_)) {
        let col: VarBinViewArray = field_as(rows, "val", &mut ctx).unwrap();
        let reader = StrColReader::new(&col);
        (0..col.len())
            .map(|i| Val::Term(reader.str_at(i).unwrap().to_owned()))
            .collect()
    } else {
        let col: PrimitiveArray = field_as(rows, "val", &mut ctx).unwrap();
        col.as_slice::<TermCode>()
            .iter()
            .map(|&c| Val::Code(c))
            .collect()
    };
    vals.into_iter()
        .zip(rid.as_slice::<RowId>().iter().copied())
        .collect()
}

/// What a `v2` writer gives and a `v2` reader trusts: each quad once, and a
/// reference index's children in `(val, rid)` order, every row id once and
/// naming the deduplicated rows. Pinned on a build that deduplicates across
/// spilled runs — the case where dropping repeats could renumber row ids out
/// from under the children — under every layout.
#[tokio::test]
async fn test_a_0_12_build_gives_the_guarantees_the_refusal_protects() {
    // Every quad twice, a whole dataset apart, so the copies sit in different
    // runs; predicates and objects repeat across rows, so the row id is what
    // breaks ties between equal values.
    let mut quads = modular_quads(24, 3, 4);
    quads.extend(modular_quads(24, 3, 4));

    for layout in LAYOUTS {
        let built = sorted_stream::build_array(
            Box::new(quad_stream(quads.clone())),
            layout,
            vec![IndexType::SecondaryByReference],
            5,
        )
        .await
        .unwrap();
        assert_eq!(built.array.len(), 24, "{layout:?}: each quad once");

        let mut checked = 0;
        for component in built
            .components
            .iter()
            .filter(|c| c.name.starts_with("index:ref-"))
        {
            let pairs = reference_child(component);
            let who = format!("{layout:?} / {}", component.name);
            assert_eq!(pairs.len(), 24, "{who}: one record per quad");
            assert!(
                pairs.windows(2).all(|pair| pair[0] < pair[1]),
                "{who}: children are in (val, rid) order, each record once"
            );
            let mut rids: Vec<RowId> = pairs.iter().map(|(_, rid)| *rid).collect();
            rids.sort_unstable();
            assert_eq!(
                rids,
                (0..24).collect::<Vec<RowId>>(),
                "{who}: each row id once"
            );
            checked += 1;
        }
        assert_eq!(checked, 2, "{layout:?}: the object and predicate children");
    }
}
