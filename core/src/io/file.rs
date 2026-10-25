//! Opening a native store file at a path, memory-mapped or read through
//! Vortex's file reader ([`open_vortex_file`]), and the identity of the file
//! a path named when it was opened ([`FileIdentity`]).

use crate::error::Result;
use crate::io::read::open_failure;

/// Which file a path named when it was opened: the device and inode on Unix.
/// A store keeps it to tell, before it rewrites its source file, whether the
/// path still names the file it opened. Off Unix there is none.
#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileIdentity {
    dev: u64,
    ino: u64,
}

impl FileIdentity {
    /// The identity of the file `meta` describes, where the platform has one.
    pub(crate) fn of(meta: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            Some(Self {
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = meta;
            None
        }
    }
}

/// How an opened store file's bytes are reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileAccess {
    /// Memory-mapped (`memmap2`) and opened over the mapping with
    /// `open_buffer`: a segment fetch is a slice of the map, and what stays
    /// in RAM is the kernel's page cache and the dictionary's bounded window
    /// cache, not a copy of the file on this process's heap.
    Mapped,
    /// Read through Vortex's file reader (`open_path`): every segment fetch
    /// is a positioned read into a buffer the caller then owns, so a
    /// whole-store load owns its memory.
    Read,
}

/// A store file opened by [`open_vortex_file`], with how its bytes are
/// reached and where from. `mapped` and `identity` describe what the open
/// did, not what was asked for.
pub(crate) struct OpenedFile {
    pub(crate) file: vortex_file::VortexFile,
    /// Whether the bytes are a memory mapping of the file.
    pub(crate) mapped: bool,
    /// The identity of the mapped file; none where the bytes are not a mapping.
    pub(crate) identity: Option<FileIdentity>,
    /// The absolute path the file was opened from, so that a later change of
    /// the working directory does not move it; none for a buffer.
    pub(crate) path: Option<std::path::PathBuf>,
}

/// A file opened over a buffer: no mapping, no identity, no path.
impl From<vortex_file::VortexFile> for OpenedFile {
    fn from(file: vortex_file::VortexFile) -> Self {
        Self {
            file,
            mapped: false,
            identity: None,
            path: None,
        }
    }
}

impl std::ops::Deref for OpenedFile {
    type Target = vortex_file::VortexFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

/// Map `file` read-only. The only place a store file is mapped.
fn map_file(file: &std::fs::File) -> std::io::Result<memmap2::Mmap> {
    #[cfg(test)]
    map_probe::note();
    // SAFETY: a store file is read-only while open; truncating or rewriting
    // it in place is unsupported (docs/file-format.md §8).
    unsafe { memmap2::Mmap::map(file) }
}

/// Open a Vortex file lazily: its footer is read, its rows when the returned
/// file is scanned. The handle caches no reader: the
/// [`NativeStoreFile`](crate::store::persist::native_file::NativeStoreFile)
/// around it owns the reader tree.
///
/// Both modes check the path, then reach the file through `File::open`, so a
/// path that cannot be opened is the same I/O error in either, with the path
/// in its message: a missing file is `NotFound`, a directory `IsADirectory`
/// (checked first: Unix opens one read-only and fails only at the map or the
/// read, Windows reports another kind).
///
/// A mapped file must not be truncated or rewritten in place while open
/// (its pages would fault); replacing it by rename is fine on Unix.
pub(crate) async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
    access: FileAccess,
) -> Result<OpenedFile> {
    use crate::error::path_error;
    use vortex_file::OpenOptionsSessionExt;

    let path = path.as_ref();
    if path.is_dir() {
        return Err(path_error(
            "open",
            path,
            std::io::ErrorKind::IsADirectory.into(),
        ));
    }
    let options = crate::session::VORTEX_SESSION.open_options();
    let file = std::fs::File::open(path).map_err(|e| path_error("open", path, e))?;
    let absolute = std::path::absolute(path).map_err(|e| path_error("resolve", path, e))?;
    match access {
        FileAccess::Mapped => {
            let identity = file.metadata().ok().as_ref().and_then(FileIdentity::of);
            let mmap = map_file(&file).map_err(|e| path_error("map", path, e))?;
            let bytes = vortex_buffer::ByteBuffer::from(mmap);
            let opened = options
                .open_buffer(bytes.clone())
                .map_err(|e| open_failure(e, super::container::newer_root_id(&bytes)))?;
            Ok(OpenedFile {
                file: opened,
                mapped: true,
                identity,
                path: Some(absolute),
            })
        }
        FileAccess::Read => {
            drop(file);
            match options.open_path(path).await {
                Ok(opened) => Ok(OpenedFile {
                    path: Some(absolute),
                    ..opened.into()
                }),
                Err(error) => Err(open_failure(
                    error,
                    super::container::newer_root_id_at(path).await,
                )),
            }
        }
    }
}

/// Test hook: the number of files [`map_file`] has mapped on this thread.
#[cfg(test)]
pub(crate) mod map_probe {
    use std::cell::Cell;

    thread_local! {
        static MAPPED: Cell<usize> = const { Cell::new(0) };
    }

    /// Count one mapped file.
    pub(super) fn note() {
        MAPPED.set(MAPPED.get() + 1);
    }

    /// The files mapped on this thread since the last call.
    pub(crate) fn take() -> usize {
        MAPPED.take()
    }
}
