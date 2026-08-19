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

pub(crate) type RdfStoreLayout = Layout<RdfStoreLayoutVTable>;

#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutData {
    pub(super) quads_sorted: bool,
    pub(super) components: Arc<[StoreComponentDescriptor]>,
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
        // The root's scan IS the quad-source scan; auxiliary components stay
        // independently addressable through `store_component`.
        layout
            .child(QUAD_SOURCE_CHILD)?
            .new_reader(name, segment_source, session, ctx)
    }
}

/// Whether the file's quad rows are recorded as globally `s`-sorted — the
/// writer's provenance for restoring the subject binary-search stamp on a
/// materialized read (see `WireMetadata::quads_sorted`).
pub(crate) fn quads_sorted(layout: &RdfStoreLayout) -> bool {
    layout.data().quads_sorted
}

/// Register the store layout in a session. Called once from the
/// `VORTEX_SESSION` initializer on every target — reading requires it.
pub(crate) fn register(session: &VortexSession) {
    use vortex_layout::session::LayoutSessionExt;
    static LAYOUT: RdfStoreLayoutVTable = RdfStoreLayoutVTable;
    session
        .layouts()
        .register((&LAYOUT as &dyn LayoutEncoding).into());
}

pub(crate) fn is_native_root(layout: &LayoutRef) -> bool {
    layout.encoding_id().as_ref() == STORE_LAYOUT_ID
}

pub(crate) fn is_native_file(file: &vortex_file::VortexFile) -> bool {
    is_native_root(file.footer().layout())
}

/// The persisted component inventory of a native root.
pub(crate) fn store_components(layout: &RdfStoreLayout) -> &[StoreComponentDescriptor] {
    &layout.data().components
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
    layout
        .child(index + 1)
        .map(|child| Some((descriptor, child)))
}

/// Test-only: the parsed root metadata of serialized store bytes — the
/// `quads_sorted` provenance bit and the component inventory. Wire-contract
/// tests assert on the decoded fields through this instead of
/// substring-searching the file for JSON fragments, which breaks on any
/// metadata re-serialization and can false-positive inside compressed data.
#[cfg(all(test, feature = "file-io"))]
pub(crate) fn store_metadata_of_bytes(bytes: &[u8]) -> (bool, Vec<StoreComponentDescriptor>) {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(vortex_buffer::ByteBuffer::from(bytes.to_vec()))
        .expect("valid Vortex bytes");
    assert!(is_native_file(&file), "not a native store file");
    let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
    (quads_sorted(typed), typed.data().components.to_vec())
}

/// On-disk byte size of a layout subtree: the sum of its segments' lengths
/// across all descendants, resolved through the footer's segment map. This
/// is the residency-threshold input for auxiliary components.
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
    for idx in 0..layout.nchildren() {
        total += subtree_bytes(&layout.child(idx)?, segment_map)?;
    }
    Ok(total)
}
