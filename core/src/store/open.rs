//! Opening a serialized store: the file and bytes constructors, the
//! component-roster interpretation every open path shares, and the
//! dictionary-residency policy `from_file` applies.

use crate::error::{Result, VortexRdfError};
use crate::io::container;
use crate::io::native_file;
use crate::session::VORTEX_SESSION;
#[cfg(feature = "file-io")]
use crate::store::QuadsSource;
#[cfg(feature = "file-io")]
use crate::store::indexes::Indexes;
use crate::store::indexes::KnownComponent;
use crate::store::layouts::DictAccess;
use crate::store::layouts::dictionary::dict_from_reader;
#[cfg(feature = "file-io")]
use crate::store::layouts::dictionary::{FileBackedDict, TermChunks};
use crate::store::layouts::{LayoutStrategy, ResolvedLayout};
#[cfg(feature = "file-io")]
use crate::store::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use crate::store::selection::ViewSelection;

use vortex_file::OpenOptionsSessionExt as _;

use std::sync::Arc;

use vortex_array::arrays::StructArray;
use vortex_array::{IntoArray, VortexSessionExecute};

use super::VortexRdfStore;

/// What one entry of a store's component roster means to this version.
pub(super) enum ComponentKind {
    /// The required `dictionary` child (the Dictionary layout's terms).
    Dict,
    /// A known index child: the registry row carrying the identity an
    /// in-memory [`IndexComponent`](crate::store::indexes::IndexComponent)
    /// adopts and the index type it makes queryable.
    Index(KnownComponent),
    /// An optional component this version does not interpret — ignoring it
    /// cannot change query results.
    Skip,
}

/// Interpret one component descriptor for every open path (`from_file`,
/// `from_bytes`, `file_components`), owning the rejection of an
/// *uninterpretable required* component: skipping one — a future change set,
/// say — would silently change query results.
pub(super) fn classify_component(
    descriptor: &container::StoreComponentDescriptor,
) -> Result<ComponentKind> {
    if descriptor.name == container::DICT_COMPONENT_NAME {
        return Ok(ComponentKind::Dict);
    }
    if let Some(known) = crate::store::indexes::known_component(&descriptor.implementation) {
        return Ok(ComponentKind::Index(known));
    }
    if descriptor.required {
        return Err(VortexRdfError::Deserialization(format!(
            "this store carries a required component this version cannot \
             interpret: {} ({} v{})",
            descriptor.name, descriptor.implementation, descriptor.version
        )));
    }
    Ok(ComponentKind::Skip)
}

/// Default residency ceiling for a Dictionary-layout file's term dictionary:
/// up to this many bytes of dictionary child (its FSST-compressed size in the
/// file, known from the footer with no I/O) the dictionary is lifted resident
/// at open; above it the dictionary stays file-backed and every store keeps a
/// bounded footprint however large its term set. Byte-based rather than
/// term-based because bytes are what residency actually costs, and terms vary
/// wildly in size (IRIs vs large string literals).
#[cfg(feature = "file-io")]
const DICT_MAX_RESIDENT_BYTES_DEFAULT: u64 = 512 << 20;

/// The residency ceiling, with the `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`
/// environment override (a plain byte count; invalid values fall back to the
/// default).
#[cfg(feature = "file-io")]
fn dict_max_resident_bytes() -> u64 {
    std::env::var("VORTEX_RDF_DICT_MAX_RESIDENT_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DICT_MAX_RESIDENT_BYTES_DEFAULT)
}

impl VortexRdfStore {
    /// Open a Vortex file lazily; no data is read until queried — except for
    /// Dictionary-layout files, whose dictionary child is lifted resident
    /// when its size fits the residency threshold.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::from_file_with_dict_residency(path, dict_max_resident_bytes()).await
    }

    /// [`from_file`](Self::from_file) with an explicit residency threshold: a
    /// Dictionary-layout file whose dictionary child exceeds
    /// `max_resident_bytes` (its size in the file, FSST-compressed) keeps the
    /// dictionary file-backed — probed and decoded by scans through the
    /// bounded reader — instead of lifting it into memory.
    ///
    /// `from_file` uses the built-in default (overridable through the
    /// `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES` environment variable); this entry
    /// pins the choice per open — `0` forces file-backed, `u64::MAX` forces
    /// resident. On a file-backed store the synchronous dictionary surface
    /// ([`code_read_snapshot`](Self::code_read_snapshot)) answers `None`;
    /// queries and reconstruction work unchanged.
    #[cfg(feature = "file-io")]
    pub async fn from_file_with_dict_residency<P: AsRef<std::path::Path>>(
        path: P,
        max_resident_bytes: u64,
    ) -> Result<Self> {
        // Remember the source path before it is consumed below, so compaction
        // can later rewrite the compacted rows back over it.
        let source_path = path.as_ref().to_path_buf();
        // Opens the file footer only (schema + layout metadata); no row data
        // is read yet. The returned handle caches its layout reader tree so
        // later scans/prunes across this store (and stores derived from it)
        // share decoded zone-map stats instead of re-reading them each time.
        let raw = native_file::open_vortex_file(path).await?;
        if !container::is_native_file(&raw) {
            return Err(native_file::unsupported_file_error(&raw));
        }
        let file = Arc::new(NativeStoreFile::try_new(raw).map_err(VortexRdfError::Vortex)?);
        // Interpret the component roster: the dictionary child feeds the
        // layout below, index children map onto the index set, and unknown
        // components are skipped when optional, fatal when required (a
        // skipped required component — a future change set, say — would
        // silently change query results).
        let mut indexes: Indexes = Vec::new();
        let typed = file
            .footer()
            .layout()
            .as_::<container::RdfStoreLayoutVTable>();
        for descriptor in file.components() {
            if let ComponentKind::Index(known) = classify_component(descriptor)? {
                if let Some((_, child)) = container::store_component(typed, &descriptor.name)
                    .map_err(VortexRdfError::Vortex)?
                {
                    crate::store::indexes::components::check_component_rows(
                        &descriptor.name,
                        child.row_count(),
                        file.row_count(),
                    )?;
                }
                if !indexes.contains(&known.index) {
                    indexes.push(known.index);
                }
            }
        }
        let layout = match LayoutStrategy::from_dtype(file.dtype()) {
            LayoutStrategy::Default => ResolvedLayout::Default,
            LayoutStrategy::TypedObject => ResolvedLayout::TypedObject,
            LayoutStrategy::Dictionary => {
                let (descriptor, reader) = file
                    .component_reader(container::DICT_COMPONENT_NAME)
                    .map_err(VortexRdfError::Vortex)?
                    .ok_or_else(|| {
                        VortexRdfError::Deserialization(
                            "Dictionary-layout store file carries no dictionary component"
                                .to_string(),
                        )
                    })?;
                if descriptor.implementation != container::DICT_IMPLEMENTATION {
                    return Err(VortexRdfError::Deserialization(format!(
                        "unsupported dictionary component implementation: {} v{}",
                        descriptor.implementation, descriptor.version
                    )));
                }
                let dict_bytes = file
                    .component_bytes(container::DICT_COMPONENT_NAME)
                    .map_err(VortexRdfError::Vortex)?
                    .expect("the dictionary component resolved above");
                let dict_len = reader.row_count();
                // A dictionary that fits the residency budget is held whole.
                // A larger one stays in its child, read through the chunk
                // leaves a probe or decode touches — unless the child's
                // layout shape declines that handle, and holding it whole is
                // then the only way to read it at all.
                let chunks = if dict_bytes <= max_resident_bytes {
                    None
                } else {
                    container::store_component(typed, container::DICT_COMPONENT_NAME)
                        .map_err(VortexRdfError::Vortex)?
                        .and_then(|(_, child)| TermChunks::resolve(&child, file.segment_source()))
                };
                let access = match chunks {
                    Some(chunks) => {
                        DictAccess::FileBacked(FileBackedDict::new(reader, dict_len, chunks))
                    }
                    // One full scan of the dictionary child — chunks keep
                    // their FSST.
                    None => DictAccess::Resident(Arc::new(dict_from_reader(reader).await?)),
                };
                ResolvedLayout::Dictionary(access)
            }
        };
        // No filter and no selection yet: this view covers all quad rows.
        // A file-backed store holds no in-memory components (the `File`
        // variant has no place for them): resolution reaches the index
        // children through pushed-down scans.
        Ok(Self {
            layout,
            indexes,
            quads: QuadsSource::File {
                path: source_path,
                dict_max_resident_bytes: max_resident_bytes,
                file,
                filter: None,
                selection: ViewSelection::all(),
                deleted: None,
                serve: None,
            },
            tail: None,
        })
    }

    /// Load a store from Vortex file bytes ([`to_bytes`](Self::to_bytes)'s
    /// output, or a `.vortex` file read into memory): the quad child is read
    /// into an in-memory base, the dictionary child is lifted resident, and
    /// index children become in-memory components beside the base — adopted
    /// *un-executed* (buffer-backed) and canonicalized on their first genuine
    /// use, so a load pays nothing for an index it never queries.
    ///
    /// Sortedness is restored from the file's own provenance, never assumed:
    /// the subject binary-search stamp only when the root metadata records a
    /// sorted build, and each component's binary-searchability from its
    /// descriptor's `sorted` flag (children whose chunks carry only local
    /// sorts stay unsearchable — scanning them is correct, searching them
    /// would not be).
    ///
    /// Runs handle-free end to end (buffer-backed segment reads resolve
    /// synchronously).
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // The borrowed form's one copy: the caller lends a slice, and the
        // file machinery hands out refcounted slices of a buffer it must keep
        // alive. A caller that owns the bytes hands them to
        // `from_bytes_owned` and skips it.
        Self::from_bytes_owned(bytes.to_vec()).await
    }

    /// [`from_bytes`](Self::from_bytes) taking ownership of the buffer, so
    /// the file machinery slices it refcounted with no copy at all. This is
    /// the wasm bindings' load path: the JS→wasm marshalling already paid the
    /// one unavoidable copy crossing the boundary, and lending that buffer as
    /// a slice would double the load's transient high-water mark (wasm linear
    /// memory never shrinks).
    pub async fn from_bytes_owned(bytes: impl Into<vortex_buffer::ByteBuffer>) -> Result<Self> {
        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(bytes.into())
            .map_err(VortexRdfError::Vortex)?;
        if !container::is_native_file(&file) {
            return Err(native_file::unsupported_file_error(&file));
        }
        // The root scan is the transparent quad child.
        let quads = native_file::scan_all(&file).await?;
        let root = file.footer().layout();
        let typed = root.as_::<container::RdfStoreLayoutVTable>();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let quads = quads
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        // Restore the subject stamp from the file's recorded provenance,
        // through the same helper every materializing read path uses.
        let quads = Self::with_subject_stamp(quads, container::quads_sorted(typed))?;

        let mut components: Vec<crate::store::indexes::IndexComponent> = Vec::new();
        let mut dict = None;
        for descriptor in container::store_components(typed) {
            let Some((_, child)) = container::store_component(typed, &descriptor.name)
                .map_err(VortexRdfError::Vortex)?
            else {
                continue;
            };
            let kind = classify_component(descriptor)?;
            let reader = child
                .new_reader(
                    descriptor.name.as_str().into(),
                    file.segment_source(),
                    file.session(),
                    &Default::default(),
                )
                .map_err(VortexRdfError::Vortex)?;
            match kind {
                ComponentKind::Dict => {
                    if descriptor.implementation != container::DICT_IMPLEMENTATION {
                        return Err(VortexRdfError::Deserialization(format!(
                            "unsupported dictionary implementation: {} v{}",
                            descriptor.implementation, descriptor.version
                        )));
                    }
                    dict = Some(Arc::new(dict_from_reader(reader).await?));
                }
                ComponentKind::Index(known) => {
                    // Adopted by reader, nothing read: the roster row comes
                    // off the wire TOC alone, and the child's scan and
                    // canonicalization both defer to the component's first
                    // genuine use — an index probe, serialization — so a
                    // load pays nothing for index children it never touches.
                    // Sound here because this reader sits over the buffer
                    // the file was opened from (see `adopt_component_reader`).
                    components.push(crate::store::indexes::adopt_component_reader(
                        &known,
                        reader,
                        descriptor.sorted,
                        quads.len() as u64,
                    )?);
                }
                ComponentKind::Skip => {}
            }
        }
        let layout = match (dict, LayoutStrategy::from_dtype(quads.dtype())) {
            (Some(dict), _) => ResolvedLayout::Dictionary(DictAccess::Resident(dict)),
            // Bare code columns with no dictionary child cannot decode —
            // fail at open like `from_file`, not with a panic at first read.
            (None, LayoutStrategy::Dictionary) => {
                return Err(VortexRdfError::Deserialization(
                    "Dictionary-layout store has no dictionary component".to_string(),
                ));
            }
            (None, LayoutStrategy::TypedObject) => ResolvedLayout::TypedObject,
            (None, LayoutStrategy::Default) => ResolvedLayout::Default,
        };
        Self::from_parts_internal(quads, components, layout)
    }
}
