//! The native store container: the `vortex-rdf.store.v1` grammar, as a
//! custom Vortex layout root.
//!
//! A store file's root layout is `vortex-rdf.store.v1`: child 0 is the
//! *transparent* `quad-source` — the quad table itself, to which the root
//! delegates its dtype, row count, and scan — and every further child is an
//! *auxiliary* component (the term dictionary, index copies, and future
//! additions such as change sets) with its own row space, written through
//! the same segment sink and addressable by name. A session that has this
//! layout registered scans the file exactly like a plain quad table; the
//! components never appear in the row space.
//!
//! One concern per module: [`wire`] is the persisted metadata codec
//! (component descriptors and their JSON stamp), [`layout`] the root layout
//! vtable and its read-side inspection, [`sources`] the always-compiled
//! component producers builders construct on every target, and [`write`] the
//! write strategy — gated as a whole, since only targets with a serializer
//! consume it. `ser` assembles a store's parts and drives
//! [`write::write_store`]; `native_file` reads the bytes back.

pub(crate) mod layout;
// The write strategy is the sole consumer of the sources' write hooks
// (`buffered_bytes`, `channel_backed`, the per-child strategy), so native
// no-file-io builds — which compile the sources but no serializer — see
// those as dead. One allowance here at the boundary, not per item.
#[cfg_attr(
    not(any(feature = "file-io", target_arch = "wasm32")),
    allow(dead_code)
)]
pub(crate) mod sources;
pub(crate) mod wire;
/// Every item in `write` is write-side container machinery, compiled only
/// where a store can be written: natively behind `file-io`, and on wasm
/// (whose bindings exchange file bytes).
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod write;

/// Stable identity of the store root layout. Changing the wire below means a
/// new versioned id, not a silent reinterpretation.
pub(crate) const STORE_LAYOUT_ID: &str = "vortex-rdf.store.v1";
/// The transparent quad table is always child 0.
pub(crate) const QUAD_SOURCE_CHILD: usize = 0;
pub(crate) const QUAD_SOURCE_NAME: &str = "quad-source";
/// Component name of the term dictionary child.
pub(crate) const DICT_COMPONENT_NAME: &str = "dictionary";
/// Implementation slug of the dictionary child: the lexicographically sorted
/// term column, FSST-compressed as held.
pub(crate) const DICT_IMPLEMENTATION: &str = "sorted-terms-fsst-v1";

#[cfg(all(test, feature = "file-io"))]
pub(crate) use layout::store_metadata_of_bytes;
#[cfg(feature = "file-io")]
pub(crate) use layout::subtree_bytes;
pub(crate) use layout::{
    RdfStoreLayoutVTable, is_native_file, quads_sorted, register, store_component, store_components,
};
pub(crate) use sources::{NativeComponentWrite, default_child_strategy};
// Consumed only by `ser`, which is gated the same way.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) use sources::ReplayableArraySource;
pub(crate) use wire::{StoreComponentDescriptor, StoreComponentRole};
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) use write::{dict_child_strategy, write_store};

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use std::sync::Arc;

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use super::layout::is_native_root;
    use super::wire::{decode_store_metadata, encode_store_metadata};
    use super::*;
    use crate::session::VORTEX_SESSION;
    use vortex_array::IntoArray;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_array::dtype::{DType, Nullability};
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt as _};
    use vortex_buffer::Buffer;
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use vortex_buffer::ByteBuffer;
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    use vortex_file::OpenOptionsSessionExt as _;
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
            "_dict_term",
            VarBinViewArray::from_iter_str(terms.iter().copied()).into_array(),
        )])
        .unwrap()
        .into_array()
    }

    fn dict_descriptor(dtype: DType) -> StoreComponentDescriptor {
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: "sorted-terms-fsst-v1".into(),
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
        assert!(VORTEX_SESSION.layouts().registry().find(&id).is_some());
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

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
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

    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    #[tokio::test]
    async fn dictionary_component_shares_file_and_scans_independently() {
        let quads = vec![quad_chunk(0, 5)];
        let dict_chunks = vec![dict_chunk(&["a", "b"]), dict_chunk(&["c"])];
        let dict_dtype = dict_chunks[0].dtype().clone();
        let dtype = quads[0].dtype().clone();

        let component = NativeComponentWrite::new(
            dict_descriptor(dict_dtype.clone()),
            Arc::new(ReplayableArraySource::try_new(dict_chunks).unwrap()),
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
