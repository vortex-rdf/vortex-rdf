//! The store root layout vtable and its read-side inspection: recognizing a
//! native root, delegating its scan to the transparent quad-source child, and
//! addressing the auxiliary components by their slot in the inventory.

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
use super::{QUAD_SOURCE_CHILD, QUAD_SOURCE_NAME, STORE_LAYOUT_FAMILY, STORE_LAYOUT_ID};

/// VTable of the native store root layout.
#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutVTable;

pub(super) type RdfStoreLayout = Layout<RdfStoreLayoutVTable>;

/// The root's persisted state: whether the quad rows are in global
/// `(s, p, o, g)` order (the meaning is defined on `WireMetadata::quads_sorted`;
/// a materialized read restores the subject sorted stamp from it), and the
/// component inventory in child order.
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
        // The root's scan IS the quad-source scan; auxiliary components stay
        // independently addressable through `component_child`.
        quad_source(layout)?.new_reader(name, segment_source, session, ctx)
    }
}

/// Register the store layout in a session. Called once from the
/// `VORTEX_SESSION` initializer on every target: reading requires it.
pub(crate) fn register(session: &VortexSession) {
    use vortex_layout::session::LayoutSessionExt;
    static LAYOUT: RdfStoreLayoutVTable = RdfStoreLayoutVTable;
    session.layouts().register(&LAYOUT as &dyn LayoutEncoding);
}

/// The id of the root layout of `bytes` when it is the root of a newer
/// vortex-rdf: a `vortex-rdf.store.` id whose version number is above the
/// current root's. The store session cannot open such a file (its registry has
/// no entry for the id), so the root is named through a session that reads an
/// unknown layout as a placeholder.
pub(crate) fn newer_root_id(bytes: &vortex_buffer::ByteBuffer) -> Option<String> {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::FOREIGN_LAYOUT_SESSION
        .open_options()
        .open_buffer(bytes.clone())
        .ok()?;
    newer_root_id_of(&file)
}

/// [`newer_root_id`] for the file at `path`, whose footer is read through
/// positioned reads: nothing is mapped.
#[cfg(feature = "file-io")]
pub(crate) async fn newer_root_id_at(path: &std::path::Path) -> Option<String> {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::FOREIGN_LAYOUT_SESSION
        .open_options()
        .open_path(path)
        .await
        .ok()?;
    newer_root_id_of(&file)
}

/// The root layout id of `file` when it is the root of a newer vortex-rdf
/// (see `is_newer_root_id`).
fn newer_root_id_of(file: &vortex_file::VortexFile) -> Option<String> {
    let id = file.footer().layout().encoding_id().to_string();
    is_newer_root_id(&id).then_some(id)
}

/// Whether `id` is the root layout id of a newer vortex-rdf: a
/// `vortex-rdf.store.v<N>` id whose `N`, plain decimal digits with no sign and
/// no leading zero, is above the current root's. A lower or an equal number, a
/// number spelled any other way, and every other id are not newer.
fn is_newer_root_id(id: &str) -> bool {
    let version = |id: &str| -> Option<u32> {
        let digits = id.strip_prefix(STORE_LAYOUT_FAMILY)?.strip_prefix('v')?;
        if digits.starts_with('0') || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    };
    matches!(
        (version(id), version(STORE_LAYOUT_ID)),
        (Some(found), Some(current)) if found > current
    )
}

pub(super) fn is_native_root(layout: &LayoutRef) -> bool {
    layout.encoding_id().as_ref() == STORE_LAYOUT_ID
}

pub(crate) fn is_native_file(file: &vortex_file::VortexFile) -> bool {
    is_native_root(file.footer().layout())
}

/// The quad table of a native root: its transparent quad-source child.
pub(crate) fn quad_source(root: &RdfStoreLayout) -> VortexResult<LayoutRef> {
    root.slot(QUAD_SOURCE_CHILD)?
        .ok_or_else(|| vortex_error::vortex_err!("store root is missing its quad-source child"))
}

/// The child layout of the component in `slot` of a native root's inventory,
/// whose descriptor is `components[slot]`: the root's child after the quad
/// source's and the `slot` components' before it.
pub(crate) fn component_child(root: &RdfStoreLayout, slot: usize) -> VortexResult<LayoutRef> {
    root.slot(QUAD_SOURCE_CHILD + 1 + slot)?
        .ok_or_else(|| vortex_error::vortex_err!("store root has no component child {slot}"))
}

/// Test-only: the decoded root metadata of serialized store bytes — the
/// `quads_sorted` bit and the component inventory.
#[cfg(test)]
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
/// across all descendants, resolved through the footer's segment map. What a
/// component occupies on disk (a test inspection).
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::is_newer_root_id;

    /// Only a `vortex-rdf.store.v<N>` id with `N` above the current root's is
    /// newer: a lower or an equal number, a number spelled with anything but
    /// plain decimal digits (a sign, a leading zero, trailing text), and every
    /// other id are not.
    #[test]
    fn only_a_higher_version_number_is_a_newer_root() {
        for (id, newer) in [
            ("vortex-rdf.store.v3", true),
            ("vortex-rdf.store.v9", true),
            ("vortex-rdf.store.v10", true),
            ("vortex-rdf.store.v100", true),
            ("vortex-rdf.store.v0", false),
            ("vortex-rdf.store.v1", false),
            ("vortex-rdf.store.v2", false),
            ("vortex-rdf.store.vx", false),
            ("vortex-rdf.store.v", false),
            ("vortex-rdf.store.v3x", false),
            ("vortex-rdf.store.v+3", false),
            ("vortex-rdf.store.v03", false),
            ("vortex-rdf.store.w3", false),
            ("vortex-rdf.storage.v3", false),
            ("vortex-rdf.stare.v3", false),
            ("other.family.v9", false),
            ("", false),
        ] {
            assert_eq!(is_newer_root_id(id), newer, "{id:?}");
        }
    }
}
