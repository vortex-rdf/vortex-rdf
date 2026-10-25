//! The native store container: the `vortex-rdf.store.v2` grammar, a custom
//! Vortex layout root. Child 0 is the *transparent* `quad-source`, the quad
//! table the root delegates its dtype, row count and scan to; every further
//! child is an *auxiliary* component (the term dictionary, the secondary
//! indexes' sorted tables) with its own rows and schema, written through the
//! same segment sink and addressed by name. A session with this layout
//! registered scans the file like a plain quad table; the components never
//! appear in its columns.
//!
//! [`wire`] is the persisted metadata codec (component descriptors and their
//! JSON stamp), [`layout`] the root layout vtable and its read-side
//! inspection, [`sources`] the component producers builders construct, and
//! [`write`](mod@write) the container's write strategy, which
//! `crate::io::write` drives.

pub(crate) mod layout;
pub(crate) mod sources;
pub(crate) mod wire;
pub(crate) mod write;

/// Stable identity of the store root layout. Changing the container grammar
/// (`wire`, `layout`) means a new versioned id, not a silent
/// reinterpretation.
///
/// Readers of `v2` rely on two guarantees its writers give and nothing checks
/// at open: each quad is stored once, and a reference index's children are in
/// `(val, rid)` order.
pub(crate) const STORE_LAYOUT_ID: &str = "vortex-rdf.store.v2";
/// What every store root layout id starts with, this version's and any other's.
pub(crate) const STORE_LAYOUT_FAMILY: &str = "vortex-rdf.store.";
/// The transparent quad table is always child 0.
const QUAD_SOURCE_CHILD: usize = 0;
const QUAD_SOURCE_NAME: &str = "quad-source";
/// Component name of the term dictionary child.
pub(crate) const DICT_COMPONENT_NAME: &str = "dictionary";
/// Implementation slug of the dictionary child: the lexicographically sorted
/// term column, FSST-compressed as held.
pub(crate) const DICT_IMPLEMENTATION: &str = "sorted-terms-fsst-v1";
/// Version of the dictionary child: the one this crate writes and the newest
/// it reads. Its term column carries exact per-window
/// `vortex.min()`/`vortex.max()` zone maps exactly when the windows are
/// uniform (chunks of uneven length, adopted from a foreign file, have no
/// uniform zone); without them, as in a version-1 child, readers read a
/// window's bounds from its leaf. A child of a newer version is refused at
/// open (`classify_component`): a newer writer may lay it out in a way this
/// reader would misread without noticing.
pub(crate) const DICT_VERSION: u32 = 2;
/// Version of an index child: the one this crate writes and the newest it
/// reads. A child of a newer version is refused at open
/// (`classify_component`), like a newer dictionary child.
pub(crate) const INDEX_VERSION: u32 = 1;

#[cfg(feature = "file-io")]
pub(crate) use layout::newer_root_id_at;
#[cfg(feature = "file-io")]
pub(crate) use layout::quad_source;
#[cfg(test)]
pub(crate) use layout::store_metadata_of_bytes;
#[cfg(test)]
pub(crate) use layout::subtree_bytes;
pub(crate) use layout::{
    RdfStoreLayoutVTable, component_child, component_reader, is_native_file, newer_root_id,
    register,
};
#[cfg(test)]
pub(crate) use sources::default_child_strategy;
pub(crate) use sources::{
    BufferedComponentSource, NativeComponentWrite, child_strategy, dict_child_strategy,
};
#[cfg(test)]
pub(crate) use sources::{ONE_MEG, child_strategy_with, id_fields};
pub(crate) use wire::{StoreComponentDescriptor, StoreComponentRole};
pub(crate) use write::write_store;

#[cfg(test)]
mod tests {
    use super::wire::{decode_store_metadata, encode_store_metadata};
    use super::*;
    use crate::session::VORTEX_SESSION;
    use crate::store::layouts::dictionary::storage::COL_DICT_TERM;
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
            version: DICT_VERSION,
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
        // new id is a new grammar.
        assert_eq!(STORE_LAYOUT_ID, "vortex-rdf.store.v2");
        assert!(VORTEX_SESSION.layouts().registry().get(&id).is_some());
    }

    #[test]
    fn metadata_round_trips_and_rejects_duplicates() {
        let dict = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        let index = StoreComponentDescriptor {
            name: "index:posg".into(),
            role: StoreComponentRole::Index,
            implementation: "secondary-by-copy/posg".into(),
            version: INDEX_VERSION,
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

    /// The exact root metadata bytes of a store with a dictionary and a copy
    /// index child, and of a bare store: every reader depends on them. A
    /// component without `sorted`, under a root without `quads_sorted`,
    /// decodes both as `false`.
    #[test]
    fn metadata_json_is_pinned() {
        use crate::store::indexes::COL_RID;
        use crate::store::schema::{CODE_PTYPE, PRIMARY_COLUMNS, ROW_ID_PTYPE};
        use vortex_array::dtype::StructFields;

        let dict = dict_descriptor(dict_chunk(&["a"]).dtype().clone());
        let code = DType::Primitive(CODE_PTYPE, Nullability::NonNullable);
        let row_id = DType::Primitive(ROW_ID_PTYPE, Nullability::NonNullable);
        let index = StoreComponentDescriptor {
            name: "index:posg".into(),
            role: StoreComponentRole::Index,
            implementation: "secondary-by-copy/posg".into(),
            version: INDEX_VERSION,
            required: false,
            sorted: true,
            dtype: DType::Struct(
                StructFields::new(
                    PRIMARY_COLUMNS
                        .iter()
                        .copied()
                        .chain([COL_RID])
                        .collect::<Vec<_>>()
                        .into(),
                    vec![code.clone(), code.clone(), code.clone(), code, row_id],
                ),
                Nullability::NonNullable,
            ),
        };
        let bytes = encode_store_metadata(true, &[dict.clone(), index]).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            concat!(
                r#"{"version":1,"quads_sorted":true,"components":["#,
                r#"{"name":"dictionary","role":"dictionary","implementation":"sorted-terms-fsst-v1","#,
                r#""version":2,"required":true,"sorted":true,"#,
                r#""fields":[{"name":"_dict_term","kind":"utf8"}]},"#,
                r#"{"name":"index:posg","role":"index","implementation":"secondary-by-copy/posg","#,
                r#""version":1,"required":false,"sorted":true,"#,
                r#""fields":[{"name":"s","kind":"u64"},{"name":"p","kind":"u64"},"#,
                r#"{"name":"o","kind":"u64"},{"name":"g","kind":"u64"},"#,
                r#"{"name":"rid","kind":"u64"}]}]}"#,
            )
        );
        assert_eq!(
            std::str::from_utf8(&encode_store_metadata(false, &[]).unwrap()).unwrap(),
            r#"{"version":1,"quads_sorted":false,"components":[]}"#
        );

        let unstamped = concat!(
            r#"{"version":1,"components":[{"name":"dictionary","role":"dictionary","#,
            r#""implementation":"sorted-terms-fsst-v1","version":2,"required":true,"#,
            r#""fields":[{"name":"_dict_term","kind":"utf8"}]}]}"#,
        );
        let unsorted_dict = StoreComponentDescriptor {
            sorted: false,
            ..dict
        };
        assert_eq!(
            decode_store_metadata(unstamped.as_bytes()).unwrap(),
            (false, vec![unsorted_dict])
        );
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

    /// The write-side round trips.
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
            let descriptor = &typed.data().components[0];
            let child = component_child(typed, 0).unwrap();
            assert_eq!(descriptor.name, DICT_COMPONENT_NAME);
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
