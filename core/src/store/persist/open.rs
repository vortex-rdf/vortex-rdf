//! Opening a serialized store: the file and bytes constructors, the
//! component-roster interpretation they share, and the dictionary residency
//! policy `from_file` applies.

use crate::error::{Result, VortexRdfError};
use crate::io::container;
use crate::io::read;
use crate::session::VORTEX_SESSION;
use crate::store::indexes::{DeferredSource, IndexComponent, KnownComponent, adopt_component};
use crate::store::layouts::dictionary::TermDictionary;
#[cfg(feature = "file-io")]
use crate::store::{
    QuadsSource,
    indexes::{Indexes, check_component_rows},
    layouts::{DictAccess, LayoutStrategy, ResolvedLayout, dictionary::FileBackedDict},
    persist::native_file::NativeStoreFile,
    view::selection::ViewSelection,
};

use vortex_file::OpenOptionsSessionExt as _;

use std::sync::Arc;

use vortex_array::arrays::StructArray;
use vortex_array::{IntoArray, VortexSessionExecute};

use crate::store::VortexRdfStore;

/// What one roster entry means to this version.
pub(in crate::store) enum ComponentKind {
    /// The `dictionary` child.
    Dict,
    /// A known index child, with its registry row.
    Index(KnownComponent),
    /// An optional component this version does not interpret.
    Skip,
}

/// Classify one roster entry: a dictionary of another implementation and a
/// required component this version cannot interpret are errors.
pub(in crate::store) fn classify_component(
    descriptor: &container::StoreComponentDescriptor,
) -> Result<ComponentKind> {
    if descriptor.name == container::DICT_COMPONENT_NAME {
        if descriptor.implementation != container::DICT_IMPLEMENTATION {
            return Err(VortexRdfError::Deserialization(format!(
                "unsupported dictionary component implementation: {} v{}",
                descriptor.implementation, descriptor.version
            )));
        }
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

/// The known index children of `file`'s roster with their descriptors; a
/// classification error fails the open.
#[cfg(feature = "file-io")]
fn index_children(
    file: &NativeStoreFile,
) -> Result<Vec<(KnownComponent, &container::StoreComponentDescriptor)>> {
    let mut children = Vec::new();
    for descriptor in file.components() {
        if let ComponentKind::Index(known) = classify_component(descriptor)? {
            children.push((known, descriptor));
        }
    }
    Ok(children)
}

/// The index set `file`'s roster carries, each index child's row count
/// checked against the root's.
#[cfg(feature = "file-io")]
fn index_types(file: &NativeStoreFile) -> Result<Indexes> {
    let mut indexes = Indexes::new();
    for (known, descriptor) in index_children(file)? {
        if let Some(child) = file.component_layout(&descriptor.name)? {
            check_component_rows(&descriptor.name, child.row_count(), file.row_count())?;
        }
        if !indexes.contains(&known.index) {
            indexes.push(known.index);
        }
    }
    Ok(indexes)
}

/// An unrefined file view's index children as in-memory components: each
/// child scanned whole and adopted deferred, with the descriptor's `sorted`
/// provenance.
#[cfg(feature = "file-io")]
pub(in crate::store) async fn scanned_index_components(
    file: &NativeStoreFile,
) -> Result<Vec<IndexComponent>> {
    let mut components = Vec::new();
    for (known, descriptor) in index_children(file)? {
        let Some(child) = file.child_reader(known.identity.name)? else {
            continue;
        };
        let scanned = read::scan_all_reader(child.reader).await?;
        components.push(adopt_component(
            &known,
            DeferredSource::Scanned(scanned),
            descriptor.sorted,
            file.row_count(),
        )?);
    }
    Ok(components)
}

/// The Dictionary layout's term access for `file`: the dictionary child
/// lifted resident when its FSST-compressed size in the file is at most
/// `max_resident_bytes`, else read on demand from the child; a child whose
/// layout shape declines the file-backed handle is lifted anyway.
#[cfg(feature = "file-io")]
async fn open_dictionary(
    file: &Arc<NativeStoreFile>,
    max_resident_bytes: u64,
) -> Result<DictAccess> {
    let child = file
        .child_reader(container::DICT_COMPONENT_NAME)?
        .ok_or_else(|| {
            VortexRdfError::Deserialization(
                "Dictionary-layout store file carries no dictionary component".to_string(),
            )
        })?;
    let dict_bytes = file
        .component_bytes(container::DICT_COMPONENT_NAME)?
        .expect("the dictionary component resolved above");
    if dict_bytes > max_resident_bytes
        && let Some(dict) = FileBackedDict::open(file)?
    {
        return Ok(DictAccess::FileBacked(dict));
    }
    Ok(DictAccess::Resident(Arc::new(
        TermDictionary::from_child_reader(child.reader).await?,
    )))
}

/// Default residency ceiling for a Dictionary-layout file's term dictionary:
/// a dictionary child of at most this many bytes (FSST-compressed, as in the
/// file) is lifted resident at open.
#[cfg(feature = "file-io")]
const DICT_MAX_RESIDENT_BYTES_DEFAULT: u64 = 512 << 20;

/// The residency ceiling, with the `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`
/// environment override.
#[cfg(feature = "file-io")]
fn dict_max_resident_bytes() -> u64 {
    dict_max_resident_bytes_from(std::env::var_os("VORTEX_RDF_DICT_MAX_RESIDENT_BYTES"))
}

/// The residency ceiling from the raw environment value, a plain byte
/// count; unset or unparseable falls back to
/// [`DICT_MAX_RESIDENT_BYTES_DEFAULT`].
#[cfg(feature = "file-io")]
fn dict_max_resident_bytes_from(raw: Option<std::ffi::OsString>) -> u64 {
    raw.and_then(|v| v.into_string().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(DICT_MAX_RESIDENT_BYTES_DEFAULT)
}

impl VortexRdfStore {
    /// Open a Vortex file lazily; no row data is read until queried. A
    /// Dictionary-layout file's dictionary child is lifted resident when its
    /// size fits the residency ceiling.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Self::from_file_with_dict_residency(path, dict_max_resident_bytes()).await
    }

    /// [`from_file`](Self::from_file) with an explicit residency ceiling: a
    /// dictionary child above `max_resident_bytes` (its FSST-compressed size
    /// in the file) stays file-backed and is read on demand; `0` forces
    /// file-backed, `u64::MAX` forces resident. On a file-backed store
    /// [`code_read_snapshot`](Self::code_read_snapshot) answers `None`;
    /// queries and reconstruction work unchanged. `from_file` uses the
    /// built-in default, overridable through `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`.
    #[cfg(feature = "file-io")]
    pub async fn from_file_with_dict_residency<P: AsRef<std::path::Path>>(
        path: P,
        max_resident_bytes: u64,
    ) -> Result<Self> {
        let source_path = path.as_ref().to_path_buf();
        let file = Arc::new(NativeStoreFile::try_new(
            read::open_vortex_file(path).await?,
        )?);
        let indexes = index_types(&file)?;
        let layout = match LayoutStrategy::from_dtype(file.dtype()) {
            LayoutStrategy::Default => ResolvedLayout::Default,
            LayoutStrategy::TypedObject => ResolvedLayout::TypedObject,
            LayoutStrategy::Dictionary => {
                ResolvedLayout::Dictionary(open_dictionary(&file, max_resident_bytes).await?)
            }
        };
        Ok(Self {
            layout,
            indexes,
            generation: crate::store::next_generation(),
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

    /// Load a store from native store bytes ([`to_bytes`](Self::to_bytes)'s
    /// output, or a `.vortex` file read into memory): the quad child becomes
    /// an in-memory base, the dictionary child is lifted resident, and the
    /// index children become in-memory components adopted un-executed and
    /// canonicalized on first use. Sortedness comes from the file's
    /// provenance alone: the subject stamp from the root's `quads_sorted`,
    /// each component's searchability from its descriptor's `sorted`. The
    /// slice is copied; [`from_bytes_owned`](Self::from_bytes_owned) is not.
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes_owned(bytes.to_vec()).await
    }

    /// [`from_bytes`](Self::from_bytes) over an owned buffer, which the file
    /// machinery slices without copying.
    pub async fn from_bytes_owned(bytes: impl Into<vortex_buffer::ByteBuffer>) -> Result<Self> {
        let file = VORTEX_SESSION.open_options().open_buffer(bytes.into())?;
        if !container::is_native_file(&file) {
            return Err(read::unsupported_file_error(&file));
        }
        // The root scan is the transparent quad child.
        let quads = read::scan_all(&file).await?;
        let root = file.footer().layout();
        let typed = root.as_::<container::RdfStoreLayoutVTable>();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let quads = quads.execute::<StructArray>(&mut ctx)?.into_array();
        let quads = crate::store::array::with_subject_stamp(quads, typed.data().quads_sorted)?;

        let mut components: Vec<IndexComponent> = Vec::new();
        let mut dict = None;
        for descriptor in typed.data().components.iter() {
            let Some((_, child)) = container::store_component(typed, &descriptor.name)? else {
                continue;
            };
            let kind = classify_component(descriptor)?;
            let reader = child.new_reader(
                descriptor.name.as_str().into(),
                file.segment_source(),
                file.session(),
                &Default::default(),
            )?;
            match kind {
                ComponentKind::Dict => {
                    dict = Some(Arc::new(TermDictionary::from_child_reader(reader).await?));
                }
                // The reader sits over the buffer the file was opened from,
                // so the deferred scan resolves synchronously.
                ComponentKind::Index(known) => components.push(adopt_component(
                    &known,
                    DeferredSource::Reader(reader),
                    descriptor.sorted,
                    quads.len() as u64,
                )?),
                ComponentKind::Skip => {}
            }
        }
        let layout = crate::store::construct::resolved_layout(dict, quads.dtype())?;
        Self::assemble_resident(quads, components, layout)
    }
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::{DICT_MAX_RESIDENT_BYTES_DEFAULT, dict_max_resident_bytes_from};
    use std::ffi::OsString;

    #[test]
    fn dict_max_resident_bytes_override() {
        assert_eq!(dict_max_resident_bytes_from(Some(OsString::from("0"))), 0);
        assert_eq!(dict_max_resident_bytes_from(Some(OsString::from("12"))), 12);
        assert_eq!(
            dict_max_resident_bytes_from(Some(OsString::from("nope"))),
            DICT_MAX_RESIDENT_BYTES_DEFAULT
        );
        assert_eq!(
            dict_max_resident_bytes_from(None),
            DICT_MAX_RESIDENT_BYTES_DEFAULT
        );
    }
}
