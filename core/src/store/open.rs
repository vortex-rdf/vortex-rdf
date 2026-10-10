//! Opening a serialized store: the file constructors (memory-mapped, or
//! loaded whole), the bytes constructors, and the component-roster
//! interpretation every open path shares.

use crate::error::{Result, VortexRdfError};
use crate::io::container;
use crate::io::read;
use crate::session::VORTEX_SESSION;
use crate::store::indexes::{IndexComponent, KnownComponent};
use crate::store::layouts::dictionary::TermDictionary;
#[cfg(feature = "file-io")]
use crate::store::{
    QuadsSource,
    indexes::Indexes,
    layouts::{DictAccess, LayoutStrategy, ResolvedLayout, dictionary::FileBackedDict},
    native_file::NativeStoreFile,
    selection::ViewSelection,
};

use vortex_file::OpenOptionsSessionExt as _;

use std::sync::Arc;

use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::{IntoArray, VortexSessionExecute};

use super::VortexRdfStore;
use super::indexes::COL_RID;
use super::schema::{CODE_PTYPE, ROW_ID_PTYPE, is_code_column_name};

/// Refuse a table whose id columns are integers of a width other than the
/// one this version reads: the readers take an integer `s`, `p`, `o`, `g`
/// (the quad table and the copy index's children) or `val` (the reference
/// index's) for a `u64` code column ([`TermCode`](super::TermCode)), and an
/// index child's `rid` for a `u64` row id ([`RowId`](super::RowId)), so a
/// file with narrower codes or row ids must not open. `table` names the
/// table in the error.
pub(super) fn check_id_columns(table: &str, dtype: &DType) -> Result<()> {
    let DType::Struct(fields, _) = dtype else {
        return Ok(());
    };
    for (name, field) in fields.names().iter().zip(fields.fields()) {
        let DType::Primitive(ptype, _) = field else {
            continue;
        };
        if is_code_column_name(name.as_ref()) && ptype != CODE_PTYPE {
            return Err(VortexRdfError::Deserialization(format!(
                "the {table} column {name} holds {ptype} term codes, but this version of \
                 vortex-rdf reads {CODE_PTYPE} codes only; rebuild the store from its RDF \
                 source"
            )));
        }
        if name.as_ref() == COL_RID && ptype != ROW_ID_PTYPE {
            return Err(VortexRdfError::Deserialization(format!(
                "the {table} column {name} holds {ptype} row ids, but this version of \
                 vortex-rdf reads {ROW_ID_PTYPE} row ids only; rebuild the store from its RDF \
                 source"
            )));
        }
    }
    Ok(())
}

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
/// `from_bytes`, `scanned_index_components`), owning the rejection of a
/// dictionary child of an unknown implementation or of a version newer than
/// [`DICT_VERSION`](container::DICT_VERSION), and of an *uninterpretable
/// required* component: skipping one — a future change set, say — would
/// silently change query results.
pub(super) fn classify_component(
    descriptor: &container::StoreComponentDescriptor,
) -> Result<ComponentKind> {
    if descriptor.name == container::DICT_COMPONENT_NAME {
        if descriptor.implementation != container::DICT_IMPLEMENTATION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported dictionary component implementation: {} v{}",
                descriptor.implementation, descriptor.version
            )));
        }
        // Fail closed on a layout this reader was never checked against.
        if descriptor.version > container::DICT_VERSION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported dictionary component version: this store's dictionary component \
                 is version {}, but this version of vortex-rdf reads up to version {}; \
                 open it with a newer vortex-rdf",
                descriptor.version,
                container::DICT_VERSION
            )));
        }
        return Ok(ComponentKind::Dict);
    }
    if let Some(known) = crate::store::indexes::known_component(&descriptor.implementation) {
        check_id_columns(&descriptor.name, &descriptor.dtype)?;
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

/// Lift an unrefined file view's index children into in-memory components:
/// each child is scanned whole and adopted as a deferred component
/// (canonicalized on its first genuine use, as `from_bytes` adopts its
/// children), with the descriptor's `sorted` provenance carried across.
#[cfg(feature = "file-io")]
pub(super) async fn scanned_index_components(
    file: &NativeStoreFile,
) -> Result<Vec<IndexComponent>> {
    let mut components = Vec::new();
    for descriptor in file.components() {
        let ComponentKind::Index(known) = classify_component(descriptor)? else {
            continue;
        };
        let Some((_, reader)) = file
            .component_reader(&descriptor.name)
            .map_err(VortexRdfError::Vortex)?
        else {
            continue;
        };
        let scanned = read::scan_all_reader(reader).await?;
        components.push(crate::store::indexes::adopt_scanned_component(
            &known,
            scanned,
            descriptor.sorted,
            file.row_count(),
        )?);
    }
    Ok(components)
}

impl VortexRdfStore {
    /// Open a store file memory-mapped (`memmap2`, through Vortex's
    /// `open_buffer`): a segment read is a slice of the map, so what stays
    /// in RAM is the kernel's page cache — file-backed, reclaimable RSS —
    /// never a copy on this process's heap. Only the footer is read up
    /// front; under the Dictionary layout the dictionary child's per-window
    /// term bounds are read too.
    ///
    /// The file is read in place for the store's lifetime: it must not be
    /// truncated or rewritten while open. Replacing it by a rename (as
    /// [`compact`](Self::compact) and the file writers do) is fine on Unix,
    /// where the mapping keeps the old file; Windows refuses to rename over a
    /// mapped file.
    /// Network filesystems are not supported for mapping.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::open_file(path, read::FileAccess::Mapped).await
    }

    /// Load a store file whole into memory — quad rows, dictionary and every
    /// index child — through Vortex's file reader rather than a mapping, so
    /// the loaded store owns its memory and never reads the file again: the
    /// explicit "load everything" opt-in (Python's `in_memory=True`). The
    /// file is opened, then its [`to_serializable_parts`](Self::to_serializable_parts)
    /// are adopted through [`from_parts`](Self::from_parts).
    #[cfg(feature = "file-io")]
    pub async fn from_file_in_memory<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let opened = Self::open_file(path, read::FileAccess::Read).await?;
        Self::from_parts(opened.to_serializable_parts().await?)
    }

    /// The open behind both: footer, component roster and resolved layout,
    /// the file reached through `access`. The dictionary stays in its child —
    /// unless the child's shape declines the file-backed handle, and holding
    /// it whole is then the only way to read it at all.
    #[cfg(feature = "file-io")]
    async fn open_file<P: AsRef<std::path::Path>>(
        path: P,
        access: read::FileAccess,
    ) -> Result<Self> {
        // Remember the source path before it is consumed below, so compaction
        // can later rewrite the compacted rows back over it.
        let source_path = path.as_ref().to_path_buf();
        // Opens the file footer only (schema + layout metadata); no row data
        // is read yet. The returned handle caches its layout reader tree so
        // later scans/prunes across this store (and stores derived from it)
        // share decoded zone-map stats instead of re-reading them each time.
        let file = Arc::new(NativeStoreFile::try_new(
            read::open_vortex_file(path, access).await?,
        )?);
        check_id_columns("quad table", file.dtype())?;
        log::debug!(
            "[open] {} {}",
            source_path.display(),
            if file.is_mapped() {
                "memory-mapped"
            } else {
                "through the file reader"
            }
        );
        // Interpret the component roster: the dictionary child feeds the
        // layout below, index children map onto the index set, and unknown
        // components are skipped when optional, fatal when required (a
        // skipped required component — a future change set, say — would
        // silently change query results).
        let mut indexes: Indexes = Vec::new();
        for descriptor in file.components() {
            if let ComponentKind::Index(known) = classify_component(descriptor)? {
                if let Some(child) = file
                    .component_layout(&descriptor.name)
                    .map_err(VortexRdfError::Vortex)?
                {
                    crate::store::indexes::check_component_rows(
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
                // The roster loop above already classified (and so
                // implementation-checked) the dictionary descriptor.
                let (_, reader) = file
                    .component_reader(container::DICT_COMPONENT_NAME)
                    .map_err(VortexRdfError::Vortex)?
                    .ok_or_else(|| {
                        VortexRdfError::Deserialization(
                            "Dictionary-layout store file carries no dictionary component"
                                .to_string(),
                        )
                    })?;
                // The dictionary stays in its child, read through the chunk
                // leaves a probe or decode touches — unless the child's
                // layout shape declines that handle, and holding it whole is
                // then the only way to read it at all.
                let dict_access = match FileBackedDict::open(&file).await? {
                    Some(dict) => DictAccess::FileBacked(dict),
                    // One full scan of the dictionary child — chunks keep
                    // their FSST.
                    None => DictAccess::Resident(Arc::new(
                        TermDictionary::from_child_reader(reader).await?,
                    )),
                };
                ResolvedLayout::Dictionary(dict_access)
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

    /// [`from_bytes`](Self::from_bytes) taking ownership of the buffer: the
    /// file machinery slices it refcounted, so no copy is made. Use it
    /// whenever the caller already owns the bytes; `from_bytes` copies a
    /// borrowed slice into one.
    pub async fn from_bytes_owned(bytes: impl Into<vortex_buffer::ByteBuffer>) -> Result<Self> {
        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(bytes.into())
            .map_err(VortexRdfError::Vortex)?;
        if !container::is_native_file(&file) {
            return Err(read::unsupported_file_error(&file));
        }
        check_id_columns("quad table", file.dtype())?;
        // The root scan is the transparent quad child.
        let quads = read::scan_all(&file).await?;
        let root = file.footer().layout();
        let typed = root.as_::<container::RdfStoreLayoutVTable>();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let quads = quads
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        // Restore the subject stamp from the file's recorded provenance,
        // through the same helper every materializing read path uses.
        let quads = crate::store::array::with_subject_stamp(quads, container::quads_sorted(typed))?;

        let mut components: Vec<IndexComponent> = Vec::new();
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
                    dict = Some(Arc::new(TermDictionary::from_child_reader(reader).await?));
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
        let layout = super::resolved_layout(dict, quads.dtype())?;
        Self::assemble_resident(quads, components, layout)
    }
}
