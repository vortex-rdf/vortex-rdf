//! The native store container: the `vortex-rdf.store.v2` grammar, as a
//! custom Vortex layout root.
//!
//! A store file's root layout is `vortex-rdf.store.v2`: child 0 is the
//! *transparent* `quad-source` — the quad table itself, to which the root
//! delegates its dtype, row count, and scan — and every further child is an
//! *auxiliary* component (the term dictionary, the secondary indexes' own
//! sorted tables, and future additions such as change sets) with its own
//! rows and schema, written through the same segment sink and addressable by
//! name. A session that has this layout registered scans the file exactly
//! like a plain quad table; the components never appear in its columns.
//!
//! One concern per module: [`wire`] is the persisted metadata codec
//! (component descriptors and their JSON stamp), [`layout`] the root layout
//! vtable and its read-side inspection, [`sources`] the always-compiled
//! component producers builders construct on every target, and [`write`] the
//! write strategy, gated like `io::ser`. `ser` assembles a store's parts and drives
//! [`write::write_store`]; `read` reads the bytes back.

pub(crate) mod layout;
// The write strategy is the sole consumer of the sources' write hooks
// (`buffered_bytes`, the per-child strategy), so native no-file-io builds —
// which compile the sources but no serializer — see those as dead. One
// allowance here at the boundary, not per item.
#[cfg_attr(
    not(any(feature = "file-io", target_arch = "wasm32")),
    allow(dead_code)
)]
pub(crate) mod sources;
pub(crate) mod wire;
/// Write strategy; gated like `io::ser`.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod write;

/// Stable identity of the store root layout. Changing the container grammar
/// (`wire`, `layout`) means a new versioned id, not a silent
/// reinterpretation.
///
/// `v2` is the root of vortex-rdf 0.12. Its readers rely on two guarantees
/// that only a 0.12 writer gives — each quad is stored once, and a reference
/// index's children are in `(val, rid)` order — and a file of 0.11 or earlier
/// can break either, so the previous root ([`LEGACY_STORE_LAYOUT_ID`]) is
/// refused, not read.
pub(crate) const STORE_LAYOUT_ID: &str = "vortex-rdf.store.v2";
/// The store root layout of vortex-rdf 0.11 and earlier. Nothing writes it
/// and nothing reads it: it is registered only so that a file carrying it
/// opens far enough to be refused with an error that says what the file is
/// (see `LegacyStoreLayoutVTable`).
pub(crate) const LEGACY_STORE_LAYOUT_ID: &str = "vortex-rdf.store.v1";
/// The transparent quad table is always child 0.
const QUAD_SOURCE_CHILD: usize = 0;
const QUAD_SOURCE_NAME: &str = "quad-source";
/// Component name of the term dictionary child.
pub(crate) const DICT_COMPONENT_NAME: &str = "dictionary";
/// Implementation slug of the dictionary child: the lexicographically sorted
/// term column, FSST-compressed as held.
pub(crate) const DICT_IMPLEMENTATION: &str = "sorted-terms-fsst-v1";
/// Version of the dictionary child this crate writes: 2 adds exact per-window
/// `vortex.min()`/`vortex.max()` zone maps on the term column (vortex-rdf
/// 0.12); version 1 (vortex-rdf 0.11 and earlier) carries none. Readers take
/// both — without zone maps a window's bounds are read from its leaf.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) const DICT_VERSION: u32 = 2;

#[cfg(test)]
pub(crate) use layout::LegacyStoreLayoutVTable;
#[cfg(all(test, feature = "file-io"))]
pub(crate) use layout::store_metadata_of_bytes;
#[cfg(all(test, feature = "file-io"))]
pub(crate) use layout::subtree_bytes;
pub(crate) use layout::{
    RdfStoreLayoutVTable, is_legacy_file, is_native_file, legacy_store_message, quads_sorted,
    register, store_component, store_components,
};
pub(crate) use sources::{NativeComponentWrite, default_child_strategy};
// Consumed only by the write side (`ser` and `IndexComponent::to_write`),
// gated the same way.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) use sources::BufferedComponentSource;
pub(crate) use wire::{StoreComponentDescriptor, StoreComponentRole};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) use write::{dict_child_strategy, write_store};

#[cfg(test)]
mod tests {
    use super::wire::{decode_store_metadata, encode_store_metadata};
    use super::*;
    use crate::session::VORTEX_SESSION;
    use crate::store::layouts::dictionary::term_dict::COL_DICT_TERM;
    use vortex_array::IntoArray;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_array::dtype::{DType, Nullability};
    use vortex_buffer::Buffer;
    use vortex_layout::VTable;

    fn quad_chunk(base: u32, rows: u32) -> vortex_array::ArrayRef {
        let s: Buffer<u32> = (0..rows).map(|i| base + i).collect();
        let p: Buffer<u32> = (0..rows).map(|i| (base + i) % 3).collect();
        let o: Buffer<u32> = (0..rows).map(|i| (base + i) % 5).collect();
        let g: Buffer<u32> = (0..rows).map(|_| 0u32).collect();
        StructArray::from_fields(&[
            ("s", s.into_array()),
            ("p", p.into_array()),
            ("o", o.into_array()),
            ("g", g.into_array()),
        ])
        .unwrap()
        .into_array()
    }

    fn dict_chunk(terms: &[&str]) -> vortex_array::ArrayRef {
        StructArray::from_fields(&[(
            COL_DICT_TERM,
            VarBinViewArray::from_iter_str(terms.iter().copied()).into_array(),
        )])
        .unwrap()
        .into_array()
    }

    fn dict_descriptor(dtype: DType) -> StoreComponentDescriptor {
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: DICT_IMPLEMENTATION.into(),
            version: 1,
            required: true,
            sorted: true,
            dtype,
        }
    }

    #[test]
    fn registration_uses_stable_layout_id() {
        use vortex_layout::session::LayoutSessionExt;
        let id = <RdfStoreLayoutVTable as VTable>::id(&RdfStoreLayoutVTable);
        assert_eq!(id.as_ref(), STORE_LAYOUT_ID);
        // Pinned to the spelling, so the constant cannot move unnoticed: a
        // new id is a new grammar, and the one it replaces must be refused.
        assert_eq!(STORE_LAYOUT_ID, "vortex-rdf.store.v2");
        assert_eq!(LEGACY_STORE_LAYOUT_ID, "vortex-rdf.store.v1");
        assert!(VORTEX_SESSION.layouts().registry().get(&id).is_some());
    }

    /// The previous root is registered, so that its files open, and nothing
    /// else: it is no edition's member, so no writer can emit it.
    #[test]
    fn the_legacy_root_is_registered_for_reading_only() {
        use vortex_edition::{ComponentKind, EditionSessionExt as _};
        use vortex_layout::session::LayoutSessionExt;
        let id = <LegacyStoreLayoutVTable as VTable>::id(&LegacyStoreLayoutVTable);
        assert_eq!(id.as_ref(), LEGACY_STORE_LAYOUT_ID);
        assert!(VORTEX_SESSION.layouts().registry().get(&id).is_some());

        let writable: Vec<String> = VORTEX_SESSION
            .enabled_component_ids(ComponentKind::Layout)
            .iter()
            .map(|id| id.to_string())
            .collect();
        assert!(writable.contains(&STORE_LAYOUT_ID.to_string()));
        assert!(!writable.contains(&LEGACY_STORE_LAYOUT_ID.to_string()));
    }

    #[test]
    fn metadata_round_trips_and_rejects_duplicates() {
        let dict = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        let index = StoreComponentDescriptor {
            name: "index:posg".into(),
            role: StoreComponentRole::Index,
            implementation: "secondary-by-copy/posg".into(),
            version: 1,
            required: false,
            sorted: true,
            dtype: quad_chunk(0, 1).dtype().clone(),
        };
        let bytes = encode_store_metadata(true, &[dict.clone(), index.clone()]).unwrap();
        let decoded = decode_store_metadata(&bytes).unwrap();
        assert_eq!(decoded, (true, vec![dict.clone(), index]));

        let dup = encode_store_metadata(false, &[dict.clone(), dict]).unwrap();
        assert!(decode_store_metadata(&dup).is_err());
    }

    #[test]
    fn metadata_rejects_unknown_version() {
        let json = br#"{"version":999,"components":[]}"#;
        assert!(decode_store_metadata(json).is_err());
    }

    #[test]
    fn descriptor_rejects_reserved_name_and_foreign_dtypes() {
        let mut d = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        d.name = QUAD_SOURCE_NAME.into();
        assert!(d.validate().is_err());

        let mut d = dict_descriptor(DType::Utf8(Nullability::NonNullable));
        d.name = DICT_COMPONENT_NAME.into();
        assert!(d.validate().is_err(), "non-struct dtype must be rejected");
    }

    #[test]
    fn descriptor_rejects_empty_fields_and_zero_version() {
        let valid = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        assert!(valid.validate().is_ok());

        let mut d = valid.clone();
        d.name = String::new();
        assert!(d.validate().is_err(), "empty name must be rejected");

        let mut d = valid.clone();
        d.implementation = String::new();
        assert!(
            d.validate().is_err(),
            "empty implementation must be rejected"
        );

        let mut d = valid;
        d.version = 0;
        assert!(d.validate().is_err(), "version 0 must be rejected");
    }

    /// Absent root metadata decodes as an unsorted, component-less store.
    #[test]
    fn empty_metadata_decodes_to_no_components() {
        assert_eq!(decode_store_metadata(b"").unwrap(), (false, Vec::new()));
    }

    /// A component write pairs a descriptor with a source of the same dtype;
    /// a mismatch is rejected at construction.
    #[test]
    fn component_write_rejects_descriptor_source_dtype_mismatch() {
        use std::sync::Arc;
        let source =
            Arc::new(sources::BufferedComponentSource::try_new(vec![dict_chunk(&["a"])]).unwrap());
        let mismatched = dict_descriptor(quad_chunk(0, 1).dtype().clone());
        assert!(
            NativeComponentWrite::new(mismatched, source.clone(), default_child_strategy())
                .is_err()
        );
        let matching = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        assert!(NativeComponentWrite::new(matching, source, default_child_strategy()).is_ok());
    }

    #[test]
    fn buffered_source_rejects_empty_and_mixed_dtypes() {
        assert!(sources::BufferedComponentSource::try_new(Vec::new()).is_err());
        assert!(
            sources::BufferedComponentSource::try_new(vec![dict_chunk(&["a"]), quad_chunk(0, 1)])
                .is_err()
        );
        assert!(
            sources::BufferedComponentSource::try_new(vec![dict_chunk(&["a"]), dict_chunk(&["b"])])
                .is_ok()
        );
    }

    /// A pull-backed source hands out its stream once; a second open is an
    /// error.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    #[test]
    fn pull_source_replays_once() {
        use sources::{NativeComponentSource as _, PullComponentSource};
        let dtype = dict_chunk(&["a"]).dtype().clone();
        let source = PullComponentSource::new(dtype, 1, Box::new(|_| Ok(None)));
        assert!(source.open().is_ok());
        assert!(source.open().is_err());
    }

    /// The write-side round trips, compiled only where a store can be
    /// written (natively behind `file-io`, and on wasm).
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    mod write_tests {
        use std::sync::Arc;

        use super::super::layout::is_native_root;
        use super::*;
        use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt as _};
        use vortex_buffer::ByteBuffer;
        use vortex_file::OpenOptionsSessionExt as _;

        #[tokio::test]
        async fn quads_only_root_round_trips_scan() {
            let chunks = vec![quad_chunk(0, 4), quad_chunk(4, 3)];
            let dtype = chunks[0].dtype().clone();
            let stream = ArrayStreamAdapter::new(
                dtype.clone(),
                futures::stream::iter(chunks.into_iter().map(Ok)),
            );
            let mut bytes: Vec<u8> = Vec::new();
            let summary = write_store(
                &VORTEX_SESSION,
                &mut bytes,
                stream,
                default_child_strategy(),
                false,
                Vec::new(),
            )
            .await
            .unwrap();
            assert!(is_native_root(summary.footer().layout()));

            let file = VORTEX_SESSION
                .open_options()
                .open_buffer(ByteBuffer::from(bytes))
                .unwrap();
            assert!(is_native_file(&file));
            let root = file.footer().layout();
            assert_eq!(
                root.child_names().collect::<Vec<_>>(),
                vec![Arc::<str>::from(QUAD_SOURCE_NAME)]
            );
            assert_eq!(file.row_count(), 7);
            assert_eq!(file.dtype(), &dtype);

            let rows = file
                .scan()
                .unwrap()
                .into_array_stream()
                .unwrap()
                .read_all()
                .await
                .unwrap();
            assert_eq!(rows.len(), 7);
            assert_eq!(rows.dtype(), &dtype);
        }

        #[tokio::test]
        async fn dictionary_component_shares_file_and_scans_independently() {
            let quads = vec![quad_chunk(0, 5)];
            let dict_chunks = vec![dict_chunk(&["a", "b"]), dict_chunk(&["c"])];
            let dict_dtype = dict_chunks[0].dtype().clone();
            let dtype = quads[0].dtype().clone();

            let component = NativeComponentWrite::new(
                dict_descriptor(dict_dtype.clone()),
                Arc::new(BufferedComponentSource::try_new(dict_chunks).unwrap()),
                default_child_strategy(),
            )
            .unwrap();

            let stream = ArrayStreamAdapter::new(
                dtype.clone(),
                futures::stream::iter(quads.into_iter().map(Ok)),
            );
            let mut bytes: Vec<u8> = Vec::new();
            write_store(
                &VORTEX_SESSION,
                &mut bytes,
                stream,
                default_child_strategy(),
                false,
                vec![component],
            )
            .await
            .unwrap();

            let file = VORTEX_SESSION
                .open_options()
                .open_buffer(ByteBuffer::from(bytes))
                .unwrap();
            let root = file.footer().layout();
            assert_eq!(
                root.child_names().collect::<Vec<_>>(),
                vec![
                    Arc::<str>::from(QUAD_SOURCE_NAME),
                    Arc::<str>::from(DICT_COMPONENT_NAME)
                ]
            );
            // The root reads as the quad table…
            assert_eq!(file.row_count(), 5);
            assert_eq!(file.dtype(), &dtype);

            // …while the dictionary child scans independently.
            let typed = root.as_::<RdfStoreLayoutVTable>();
            let (descriptor, child) = store_component(typed, DICT_COMPONENT_NAME)
                .unwrap()
                .unwrap();
            assert_eq!(descriptor.role, StoreComponentRole::Dictionary);
            assert_eq!(child.row_count(), 3);
            assert_eq!(child.dtype(), &dict_dtype);
            assert!(
                subtree_bytes(&child, file.footer().segment_map()).unwrap() > 0,
                "dict child must own segment bytes"
            );

            let reader = child
                .new_reader(
                    DICT_COMPONENT_NAME.into(),
                    file.segment_source(),
                    file.session(),
                    &Default::default(),
                )
                .unwrap();
            let terms =
                vortex_layout::scan::scan_builder::ScanBuilder::new(file.session().clone(), reader)
                    .into_array_stream()
                    .unwrap()
                    .read_all()
                    .await
                    .unwrap();
            assert_eq!(terms.len(), 3);
            assert_eq!(terms.dtype(), &dict_dtype);
        }
    }
}
