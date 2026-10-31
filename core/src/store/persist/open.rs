//! Opening a serialized store: the file constructors (memory-mapped, or
//! loaded whole), the bytes constructors, and the component-roster
//! interpretation every open path shares ([`ComponentRoster`]).

use crate::error::{Result, VortexRdfError};
use crate::io::container;
#[cfg(feature = "file-io")]
use crate::io::file::{FileAccess, open_vortex_file};
use crate::io::read;
use crate::session::VORTEX_SESSION;
use crate::store::indexes::{
    ComponentIdentity, IndexComponent, adopt_component, check_component_rows,
};
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{DictAccess, LayoutStrategy};
#[cfg(feature = "file-io")]
use crate::store::{
    Backend, FileBase, FileFilter, QuadsSource, array::into_struct_array, indexes::Indexes,
    layouts::dictionary::FileBackedDict, persist::native_file::NativeStoreFile,
};

use std::sync::Arc;

use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::{IntoArray, VortexSessionExecute};

use crate::store::VortexRdfStore;
use crate::store::construct::resolved_layout;
use crate::store::indexes::COL_RID;
use crate::store::schema::{CODE_PTYPE, ROW_ID_PTYPE, is_code_column_name};
use crate::store::zones::ZoneTable;

/// Refuse a table whose id columns are integers of another width than this
/// version reads: an integer `s`, `p`, `o`, `g` (the quad table and the copy
/// index's children) or `val` (the reference index's) is read as a `u64` code
/// ([`TermCode`](crate::store::TermCode)) and an index child's `rid` as a
/// `u64` row id ([`RowId`](crate::store::RowId)), so a file with narrower
/// codes or row ids must not open. `table` names the table in the error.
pub(in crate::store) fn check_id_columns(table: &str, dtype: &DType) -> Result<()> {
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
enum ComponentKind {
    /// The required `dictionary` child (the Dictionary layout's terms).
    Dict,
    /// A known index child: the registry row carrying the identity an
    /// in-memory [`IndexComponent`] adopts and the index type it makes
    /// queryable.
    Index(&'static ComponentIdentity),
    /// An optional component this version does not interpret: ignoring it
    /// cannot change query results.
    Skip,
}

/// Interpret one component descriptor for every open path (through
/// [`ComponentRoster::of`]). Refused: a dictionary child of an unknown
/// implementation or of a version newer than
/// [`DICT_VERSION`](container::DICT_VERSION), an index child newer than
/// [`INDEX_VERSION`](container::INDEX_VERSION) or with id columns of another
/// width, and an *uninterpretable required* component, since skipping one (a
/// future change set, say) would change query results. An index child is
/// classified by its implementation slug alone, never by its name.
fn classify_component(descriptor: &container::StoreComponentDescriptor) -> Result<ComponentKind> {
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
        // Fail closed on a layout this reader was never checked against.
        if descriptor.version > container::INDEX_VERSION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported index component version: this store's component {} is version {}, \
                 but this version of vortex-rdf reads up to version {}; it was written by a \
                 newer vortex-rdf, so open it with a newer version of vortex-rdf",
                descriptor.name,
                descriptor.version,
                container::INDEX_VERSION
            )));
        }
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

/// One known index child of a store's component roster: the registry row its
/// implementation slug names, its slot in the inventory (descriptor
/// `components[slot]`, which only the open paths read), and its descriptor's
/// `sorted` provenance.
pub(in crate::store) struct IndexChild {
    pub(in crate::store) identity: &'static ComponentIdentity,
    pub(super) slot: usize,
    pub(in crate::store) sorted: bool,
}

/// A store's component roster as this version reads it, classified once at
/// open: the slot of its dictionary child and its known index children, in
/// inventory order. Skipped components are left out, so whatever reaches a
/// child through the roster (the resolvers, the listing, the adoptions)
/// reaches only the children classified here.
pub(super) struct ComponentRoster {
    /// The slot of the `dictionary` child the quad table's codes read. Only
    /// the Dictionary layout has codes, and the quad table's dtype alone
    /// decides the layout, so a dictionary child beside string columns is
    /// left out like a skipped component.
    pub(super) dict: Option<usize>,
    /// That child's `bracketed_iris` provenance; `false` without one.
    pub(super) dict_bracketed_iris: bool,
    /// The known index children, in inventory order.
    pub(super) index_children: Vec<IndexChild>,
}

impl ComponentRoster {
    /// The roster of the native store file `file`, refused unless this
    /// version can read it, by the checks every open runs before it reads a
    /// row: the root must be the native store layout, the quad table's id
    /// columns this version's width ([`check_id_columns`]) and every
    /// component interpretable ([`classify_component`]); then each known
    /// index child must hold one row per quad ([`check_component_rows`]).
    /// The structural refusals come first: a roster at fault both ways is
    /// refused for its structure.
    pub(super) fn of(file: &vortex_file::VortexFile) -> Result<Self> {
        if !container::is_native_file(file) {
            return Err(read::unsupported_file_error(file));
        }
        check_id_columns("quad table", file.dtype())?;
        let root = file
            .footer()
            .layout()
            .as_::<container::RdfStoreLayoutVTable>();
        let descriptors = &root.data().components;
        let dictionary_layout =
            LayoutStrategy::from_dtype(file.dtype()) == LayoutStrategy::Dictionary;
        let mut roster = Self {
            dict: None,
            dict_bracketed_iris: false,
            index_children: Vec::new(),
        };
        for (slot, descriptor) in descriptors.iter().enumerate() {
            match classify_component(descriptor)? {
                ComponentKind::Dict if dictionary_layout => {
                    roster.dict = Some(slot);
                    roster.dict_bracketed_iris = descriptor.bracketed_iris;
                }
                ComponentKind::Index(identity) => roster.index_children.push(IndexChild {
                    identity,
                    slot,
                    sorted: descriptor.sorted,
                }),
                ComponentKind::Dict | ComponentKind::Skip => {}
            }
        }
        for child in &roster.index_children {
            check_component_rows(
                &descriptors[child.slot].name,
                container::component_child(root, child.slot)?.row_count(),
                file.row_count(),
            )?;
        }
        Ok(roster)
    }
}

/// Lift an unrefined file view's index children into in-memory components:
/// each known child of the file's roster is scanned whole and executed to its
/// rows (every consumer, a write or an in-memory open, reads them at once),
/// its descriptor's `sorted` provenance carried across.
#[cfg(feature = "file-io")]
pub(in crate::store) async fn scanned_index_components(
    file: &NativeStoreFile,
) -> Result<Vec<IndexComponent>> {
    let mut components = Vec::with_capacity(file.index_children().len());
    for child in file.index_children() {
        let scanned = read::scan_all_reader(file.index_child_reader(child)?).await?;
        components.push(IndexComponent::built(
            child.identity,
            into_struct_array(scanned)?,
            child.sorted,
        ));
    }
    Ok(components)
}

impl VortexRdfStore {
    /// Open a store file memory-mapped (`memmap2`, through Vortex's
    /// `open_buffer`): a segment read is a slice of the map, so what stays in
    /// RAM is the kernel's page cache (file-backed, reclaimable) and the
    /// dictionary's bounded window cache, never a copy of the file on this
    /// process's heap. Only the footer is read up front, plus, under the
    /// Dictionary layout, the dictionary child's per-window term bounds.
    ///
    /// The file is read in place for the store's lifetime: it must not be
    /// truncated or rewritten while open. Replacing it by a rename (as
    /// [`compact`](Self::compact) and the file writers do) is fine on Unix,
    /// where the mapping keeps the old file; Windows refuses to rename over a
    /// mapped file. Network filesystems are not supported for mapping.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::open_file(path, FileAccess::Mapped).await
    }

    /// Load a store file whole into memory (quad rows, dictionary and every
    /// index child) through Vortex's file reader rather than a mapping, so the
    /// loaded store owns its memory and never reads the file again (Python's
    /// `in_memory=True`). Each column keeps its compressed form wherever a
    /// match can bind it directly.
    #[cfg(feature = "file-io")]
    pub async fn from_file_in_memory<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let opened = Self::open_file(path, FileAccess::Read).await?;
        // The base's zone bounds come from the file's zone maps (its rows are
        // the file's quad rows, in file order), read beside the parts so the
        // two reads' waits overlap.
        let file = &opened
            .quads
            .file()
            .expect("a store opened from a file reads that file")
            .file;
        let (quads, source) = (file.quad_layout()?, file.segment_source());
        let (parts, zones) = futures::try_join!(
            opened.to_serializable_parts(),
            ZoneTable::read_from_file(&quads, &source)
        )?;
        Self::from_parts(parts, zones)
    }

    /// The open behind both: footer, component roster and resolved layout,
    /// the file reached through `access`. The dictionary stays in its child,
    /// read through the leaves a probe or decode touches, unless the child's
    /// shape declines that handle: then it is held whole, the only way left
    /// to read it.
    #[cfg(feature = "file-io")]
    async fn open_file<P: AsRef<std::path::Path>>(path: P, access: FileAccess) -> Result<Self> {
        // The footer only (schema and layout metadata); no row is read yet.
        // The handle owns the reader tree, so this store and the views
        // derived from it share decoded zone-map stats; it refuses a file this
        // version cannot read and classifies its roster once
        // (`ComponentRoster::of`).
        let file = Arc::new(NativeStoreFile::try_new(
            open_vortex_file(path.as_ref(), access).await?,
        )?);
        log::debug!(
            "[open] {} {}",
            path.as_ref().display(),
            if file.is_mapped() {
                "memory-mapped"
            } else {
                "through the file reader"
            }
        );
        // The roster's known index children map onto the index set, in
        // roster order.
        let mut indexes: Indexes = Vec::new();
        for child in file.index_children() {
            if !indexes.contains(&child.identity.index) {
                indexes.push(child.identity.index);
            }
        }
        // The roster holds a dictionary under the Dictionary layout only.
        let dict = match FileBackedDict::open(&file).await? {
            Some(dict) => Some(DictAccess::FileBacked(dict)),
            // One full scan of the dictionary child; the chunks keep their
            // FSST.
            None => match file.component_reader(container::DICT_COMPONENT_NAME)? {
                Some(reader) => Some(DictAccess::Resident(Arc::new(
                    TermDictionary::from_child_reader(reader, file.dict_bracketed_iris()).await?,
                ))),
                None => None,
            },
        };
        let layout = resolved_layout(dict, file.dtype())?;
        // No filter and no selection yet: this view covers all quad rows.
        // A file-backed store holds no in-memory components (a `FileBase`
        // has no place for them): resolution reaches the index children
        // through pushed-down scans.
        Ok(Self {
            layout,
            indexes,
            generation: crate::store::next_generation(),
            quads: QuadsSource::new(Backend::File(FileBase {
                file,
                filter: FileFilter::None,
                serve: None,
            })),
            tail: None,
        })
    }

    /// Load a store from Vortex file bytes ([`to_bytes`](Self::to_bytes)'s
    /// output, or a `.vortex` file read into memory): the quad child is read
    /// into an in-memory base, the dictionary child is lifted resident, and
    /// the index children become in-memory components adopted *un-executed*
    /// (buffer-backed) and canonicalized on their first use, so a load pays
    /// nothing for an index it never queries.
    ///
    /// Sortedness is restored from the file's own provenance, never assumed:
    /// the subject binary-search stamp only when the root metadata records a
    /// sorted build, and a component's binary search only when its descriptor
    /// is `sorted` (a child sorted only per chunk is scanned, never searched).
    /// Runs without a runtime handle: buffer-backed segment reads resolve
    /// synchronously.
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
        let file = read::open_store_buffer(bytes.into())?;
        // Refused before a row is read, for a fault of the roster too.
        let roster = ComponentRoster::of(&file)?;
        // The root scan is the transparent quad child.
        let quads = read::scan_all_reader(file.layout_reader()?).await?;
        let root = file.footer().layout();
        let typed = root.as_::<container::RdfStoreLayoutVTable>();
        // The base's zone bounds, from the quad child's zone maps.
        let zones =
            ZoneTable::read_from_file(&container::quad_source(typed)?, &file.segment_source())
                .await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let quads = quads.execute::<StructArray>(&mut ctx)?.into_array();
        // Restore the subject stamp from the file's recorded provenance,
        // through the same helper every materializing read path uses.
        let quads = crate::store::array::with_subject_stamp(quads, typed.data().quads_sorted)?;

        // A reader of the roster's component in `slot`, over the buffer.
        let reader = |slot: usize| container::component_reader(&file, slot);
        let dict = match roster.dict {
            Some(slot) => Some(DictAccess::Resident(Arc::new(
                TermDictionary::from_child_reader(reader(slot)?, roster.dict_bracketed_iris)
                    .await?,
            ))),
            None => None,
        };
        // Adopted by reader, nothing read: the child's scan and
        // canonicalization wait for the component's first use (an index
        // probe, serialization). Sound because each reader sits over the
        // buffer the file was opened from (see `adopt_component`).
        let components = roster
            .index_children
            .iter()
            .map(|child| -> Result<IndexComponent> {
                Ok(adopt_component(
                    child.identity,
                    reader(child.slot)?,
                    child.sorted,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let layout = resolved_layout(dict, quads.dtype())?;
        Self::assemble_resident(quads, components, layout, zones)
    }
}
