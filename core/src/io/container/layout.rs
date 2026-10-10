//! The store root layout vtable and its read-side inspection: recognizing a
//! native root, delegating its scan to the transparent quad-source child, and
//! addressing the auxiliary components by name.

use std::sync::Arc;

use vortex_array::RawMetadata;
use vortex_array::dtype::DType;
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};
use vortex_layout::segments::SegmentSource;
use vortex_layout::{
    Layout, LayoutChildType, LayoutDeserializeArgs, LayoutEncoding, LayoutId, LayoutReaderContext,
    LayoutReaderRef, LayoutRef, VTable,
};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::wire::{StoreComponentDescriptor, decode_store_metadata, encode_store_metadata};
use super::{
    LEGACY_STORE_LAYOUT_ID, QUAD_SOURCE_CHILD, QUAD_SOURCE_NAME, STORE_LAYOUT_FAMILY,
    STORE_LAYOUT_ID,
};

/// VTable of the native store root layout.
#[derive(Clone, Debug)]
pub(crate) struct RdfStoreLayoutVTable;

pub(super) type RdfStoreLayout = Layout<RdfStoreLayoutVTable>;

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
            .slot(QUAD_SOURCE_CHILD)?
            .ok_or_else(|| {
                vortex_error::vortex_err!("store root is missing its quad-source child")
            })?
            .new_reader(name, segment_source, session, ctx)
    }
}

/// What opening a store written by vortex-rdf 0.11 or earlier reports: the
/// cause (the version and the root layout), that it is refused, and the way
/// out. The one text every open path gives, in Rust, Python and JavaScript.
pub(crate) fn legacy_store_message() -> String {
    format!(
        "this store was written by vortex-rdf 0.11 or earlier (root layout \
         {LEGACY_STORE_LAYOUT_ID}), which this version cannot read; rebuild it \
         from its RDF source with vortex-rdf 0.12 or later (the CLI's \
         `serialize`, Python's `serialize_rdf` or JavaScript's `serializeRdf`)"
    )
}

/// VTable of the root layout vortex-rdf 0.11 and earlier wrote
/// ([`LEGACY_STORE_LAYOUT_ID`]).
///
/// A store of that vintage is refused, not read — but it has to *open* first,
/// because Vortex builds a footer's root layout from the session's registry
/// and an id nobody registered fails the open with "Invalid encoding ID: N",
/// which names neither the cause nor the way out. Registering the id lets the
/// open succeed, so [`is_native_file`] is false for it and the open path
/// reports [`legacy_store_message`]. Nothing is ever read through this layout:
/// it owns no data, its readers refuse, and no edition admits it for writing.
#[derive(Clone, Debug)]
pub(crate) struct LegacyStoreLayoutVTable;

impl VTable for LegacyStoreLayoutVTable {
    type LayoutData = ();
    type Metadata = RawMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new(LEGACY_STORE_LAYOUT_ID);
        *ID
    }

    fn metadata(_layout: &Layout<Self>) -> Self::Metadata {
        // Never serialized: no writer can emit this layout.
        RawMetadata(Vec::new())
    }

    fn deserialize(
        &self,
        _args: &LayoutDeserializeArgs<'_>,
        _metadata: &Vec<u8>,
    ) -> VortexResult<Self::LayoutData> {
        // The old grammar is not interpreted: the layout is recognized by its
        // id alone.
        Ok(())
    }

    fn child_dtype(_layout: &Layout<Self>, _slot: usize) -> VortexResult<DType> {
        vortex_bail!("{}", legacy_store_message())
    }

    fn child_type(_layout: &Layout<Self>, slot: usize) -> LayoutChildType {
        LayoutChildType::Auxiliary(format!("[{slot}]").into())
    }

    fn new_reader(
        _layout: &Layout<Self>,
        _name: Arc<str>,
        _segment_source: Arc<dyn SegmentSource>,
        _session: &VortexSession,
        _ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        vortex_bail!("{}", legacy_store_message())
    }
}

/// Whether the quad rows are recorded in global `(s, p, o, g)` order (the
/// meaning is defined on `WireMetadata::quads_sorted`); a materialized read
/// restores the subject sorted stamp from it.
pub(crate) fn quads_sorted(layout: &RdfStoreLayout) -> bool {
    layout.data().quads_sorted
}

/// Register the store layout — and the previous generation's, to refuse it —
/// in a session. Called once from the `VORTEX_SESSION` initializer on every
/// target — reading requires it.
pub(crate) fn register(session: &VortexSession) {
    use vortex_layout::session::LayoutSessionExt;
    static LAYOUT: RdfStoreLayoutVTable = RdfStoreLayoutVTable;
    static LEGACY_LAYOUT: LegacyStoreLayoutVTable = LegacyStoreLayoutVTable;
    session.layouts().register(&LAYOUT as &dyn LayoutEncoding);
    session
        .layouts()
        .register(&LEGACY_LAYOUT as &dyn LayoutEncoding);
}

/// The id of the root layout of `bytes` when it is a store root this version
/// does not know: a `vortex-rdf.store.` id other than the current and the
/// legacy one. The store session cannot open such a file (its registry has no
/// entry for the id), so the root is named through a session that reads an
/// unknown layout as a placeholder.
pub(crate) fn newer_root_id(bytes: &vortex_buffer::ByteBuffer) -> Option<String> {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::FOREIGN_LAYOUT_SESSION
        .open_options()
        .open_buffer(bytes.clone())
        .ok()?;
    let id = file.footer().layout().encoding_id().to_string();
    (id.starts_with(STORE_LAYOUT_FAMILY) && id != STORE_LAYOUT_ID && id != LEGACY_STORE_LAYOUT_ID)
        .then_some(id)
}

pub(super) fn is_native_root(layout: &LayoutRef) -> bool {
    layout.encoding_id().as_ref() == STORE_LAYOUT_ID
}

pub(crate) fn is_native_file(file: &vortex_file::VortexFile) -> bool {
    is_native_root(file.footer().layout())
}

/// Whether the file's root is the layout vortex-rdf 0.11 and earlier wrote.
pub(crate) fn is_legacy_file(file: &vortex_file::VortexFile) -> bool {
    file.footer().layout().encoding_id().as_ref() == LEGACY_STORE_LAYOUT_ID
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
    let child = layout.slot(index + 1)?.ok_or_else(|| {
        vortex_error::vortex_err!("store component {name} is missing its child layout")
    })?;
    Ok(Some((descriptor, child)))
}

/// Test-only: the decoded root metadata of serialized store bytes — the
/// `quads_sorted` bit and the component inventory.
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
/// across all descendants, resolved through the footer's segment map. What a
/// component occupies on disk (a test inspection).
#[cfg(all(test, feature = "file-io"))]
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
