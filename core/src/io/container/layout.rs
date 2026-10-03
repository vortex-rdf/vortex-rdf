//! The store root layout vtable and its read-side inspection: recognizing a
//! native root, delegating its scan to the transparent quad-source child, and
//! addressing the auxiliary components by name.

use std::sync::Arc;

use vortex_array::RawMetadata;
use vortex_array::dtype::DType;
use vortex_error::{VortexResult, vortex_ensure_eq};
use vortex_layout::segments::SegmentSource;
use vortex_layout::{
    Layout, LayoutChildType, LayoutDeserializeArgs, LayoutEncoding, LayoutId, LayoutReaderContext,
    LayoutReaderRef, LayoutRef, VTable,
};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::wire::{StoreComponentDescriptor, decode_store_metadata, encode_store_metadata};
use super::{QUAD_SOURCE_CHILD, QUAD_SOURCE_NAME, STORE_LAYOUT_ID};

/// VTable of the native store root layout.
#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutVTable;

pub(super) type RdfStoreLayout = Layout<RdfStoreLayoutVTable>;

/// The root's persisted state: whether the quad rows are in global
/// `(s, p, o, g)` order, and the component inventory in child order.
#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutData {
    pub(crate) quads_sorted: bool,
    pub(crate) components: Arc<[StoreComponentDescriptor]>,
}

impl VTable for RdfStoreLayoutVTable {
    type LayoutData = RdfStoreLayoutData;
    type Metadata = RawMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new(STORE_LAYOUT_ID);
        *ID
    }

    fn metadata(layout: &Layout<Self>) -> Self::Metadata {
        RawMetadata(
            encode_store_metadata(layout.data().quads_sorted, &layout.data().components)
                .expect("validated store metadata must serialize"),
        )
    }

    fn deserialize(
        &self,
        args: &LayoutDeserializeArgs<'_>,
        metadata: &Vec<u8>,
    ) -> VortexResult<Self::LayoutData> {
        let (quads_sorted, components) = decode_store_metadata(metadata)?;
        vortex_ensure_eq!(
            args.children.nchildren(),
            1 + components.len(),
            "store root child count does not match its component inventory"
        );
        let quads = args.children.child(QUAD_SOURCE_CHILD, args.dtype)?;
        vortex_ensure_eq!(
            quads.row_count(),
            args.row_count,
            "quad-source row count must match the store root"
        );
        for (index, component) in components.iter().enumerate() {
            let child = args.children.child(index + 1, &component.dtype)?;
            vortex_ensure_eq!(
                child.dtype(),
                &component.dtype,
                "store component dtype does not match its descriptor"
            );
        }
        Ok(RdfStoreLayoutData {
            quads_sorted,
            components: components.into(),
        })
    }

    fn child_dtype(layout: &Layout<Self>, idx: usize) -> VortexResult<DType> {
        match idx {
            QUAD_SOURCE_CHILD => Ok(layout.dtype().clone()),
            _ => layout
                .data()
                .components
                .get(idx - 1)
                .map(|c| c.dtype.clone())
                .ok_or_else(|| vortex_error::vortex_err!("invalid store root child index: {idx}")),
        }
    }

    fn child_type(layout: &Layout<Self>, idx: usize) -> LayoutChildType {
        match idx {
            QUAD_SOURCE_CHILD => LayoutChildType::Transparent(QUAD_SOURCE_NAME.into()),
            _ => layout
                .data()
                .components
                .get(idx - 1)
                .map(|c| LayoutChildType::Auxiliary(c.name.as_str().into()))
                .unwrap_or_else(|| panic!("invalid store root child index: {idx}")),
        }
    }

    fn new_reader(
        layout: &Layout<Self>,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
        ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        // The root's scan is the quad-source child's.
        layout
            .slot(QUAD_SOURCE_CHILD)?
            .ok_or_else(|| {
                vortex_error::vortex_err!("store root is missing its quad-source child")
            })?
            .new_reader(name, segment_source, session, ctx)
    }
}

/// Register the store layout in `session`; reading a store file requires it.
pub(crate) fn register(session: &VortexSession) {
    use vortex_layout::session::LayoutSessionExt;
    static LAYOUT: RdfStoreLayoutVTable = RdfStoreLayoutVTable;
    session.layouts().register(&LAYOUT as &dyn LayoutEncoding);
}

pub(super) fn is_native_root(layout: &LayoutRef) -> bool {
    layout.encoding_id().as_ref() == STORE_LAYOUT_ID
}

pub(crate) fn is_native_file(file: &vortex_file::VortexFile) -> bool {
    is_native_root(file.footer().layout())
}

/// A named auxiliary child of a native root, with its descriptor.
pub(crate) fn store_component(
    layout: &RdfStoreLayout,
    name: &str,
) -> VortexResult<Option<(StoreComponentDescriptor, LayoutRef)>> {
    let Some(index) = layout.data().components.iter().position(|c| c.name == name) else {
        return Ok(None);
    };
    let descriptor = layout.data().components[index].clone();
    let child = layout.slot(index + 1)?.ok_or_else(|| {
        vortex_error::vortex_err!("store component {name} is missing its child layout")
    })?;
    Ok(Some((descriptor, child)))
}

/// The decoded root metadata of serialized store bytes: the `quads_sorted`
/// bit and the component inventory.
#[cfg(all(test, feature = "file-io"))]
pub(crate) fn store_metadata_of_bytes(bytes: &[u8]) -> (bool, Vec<StoreComponentDescriptor>) {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(vortex_buffer::ByteBuffer::from(bytes.to_vec()))
        .expect("valid Vortex bytes");
    assert!(is_native_file(&file), "not a native store file");
    let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
    (typed.data().quads_sorted, typed.data().components.to_vec())
}

/// On-disk byte size of a layout subtree: the sum of its segments' lengths
/// across all descendants, resolved through the footer's segment map.
#[cfg(feature = "file-io")]
pub(crate) fn subtree_bytes(
    layout: &LayoutRef,
    segment_map: &[vortex_file::SegmentSpec],
) -> VortexResult<u64> {
    let mut total: u64 = layout
        .segment_ids()
        .into_iter()
        .map(|id| {
            segment_map
                .get(*id as usize)
                .map(|spec| u64::from(spec.length))
                .unwrap_or(0)
        })
        .sum();
    for idx in 0..layout.nslots() {
        if let Some(child) = layout.slot(idx)? {
            total += subtree_bytes(&child, segment_map)?;
        }
    }
    Ok(total)
}
