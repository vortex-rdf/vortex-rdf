//! Temp-file spill machinery behind the out-of-core builder: rkyv-serialized
//! runs of quads and of the index families' `(value, row id)` entries, written
//! during the ingest and merge passes and read back during chunk emission, so
//! peak memory stays bounded by the chunk size.

use std::collections::BinaryHeap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use rkyv::api::high::{HighDeserializer, HighSerializer, to_bytes_in};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

/// Environment variable overriding where spill directories are created: a
/// volume with room for runs that exist because the data outgrew memory (the
/// OS temp dir is often a size-capped, RAM-backed tmpfs).
const SPILL_DIR_ENV: &str = "VORTEX_RDF_SPILL_DIR";

/// The rkyv bounds a record type needs to be spilled and read back.
pub(crate) trait Spillable:
    Sized
    + Archive<Archived: RkyvDeserialize<Self, HighDeserializer<RkyvError>>>
    + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>
{
}

impl<T> Spillable for T where
    T: Archive<Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>>
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>
{
}

/// The parent-directory precedence behind [`TempRunsGuard::create`]: the env
/// override (empty counts as unset), then `base`, then the OS temp dir.
fn resolve_spill_parent(env_override: Option<std::ffi::OsString>, base: Option<&Path>) -> PathBuf {
    env_override
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| base.map(Path::to_path_buf))
        .unwrap_or_else(std::env::temp_dir)
}

/// A spill run's I/O failure: `what` failed in `dir` (shown as `.`, the
/// working directory, when empty). The error keeps the I/O error's kind, and
/// its message names the directory and the [`SPILL_DIR_ENV`] way out: a
/// directory with more room when the volume or a quota is full, a writable
/// one otherwise.
pub(crate) fn spill_io_error(dir: &Path, what: &str, e: std::io::Error) -> VortexRdfError {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let dir = dir.display();
    let hint = match e.kind() {
        ErrorKind::StorageFull | ErrorKind::QuotaExceeded => "a directory with more room",
        _ => "a writable directory",
    };
    VortexRdfError::Io(std::io::Error::new(
        e.kind(),
        format!("spill run {what} failed in {dir}: {e}; set {SPILL_DIR_ENV} to {hint}"),
    ))
}

/// The longest file name, in bytes, the common filesystems take.
const MAX_NAME_BYTES: usize = 255;

/// A unique spill directory for one build's runs, deleted when dropped, so
/// the runs go even when the chunk stream is abandoned unfinished.
pub(crate) struct TempRunsGuard {
    dir: PathBuf,
}

impl TempRunsGuard {
    /// Create the run directory under the spill parent, named after the file
    /// the build writes: `.<output>.spill-<uuid>`, by default a hidden sibling
    /// of the output, or `.vortex-rdf.spill-<uuid>` when there is no output
    /// name (a build into memory or into a caller's writer) or it would make
    /// the directory's name longer than a file name may be.
    ///
    /// The parent is, in precedence order: the `VORTEX_RDF_SPILL_DIR`
    /// environment variable, the caller's `base` (a build that writes a file
    /// passes its temp file's directory, beside the file it writes or
    /// replaces, so spills share the output's volume), then
    /// [`std::env::temp_dir`]. Never the working directory: an embedding
    /// server or binding can run with any cwd, even a read-only one.
    pub(crate) fn create(output: Option<&OsStr>, base: Option<&Path>) -> Result<Self> {
        let parent = resolve_spill_parent(std::env::var_os(SPILL_DIR_ENV), base);
        let suffix = format!(".spill-{}", uuid::Uuid::new_v4());
        let output = output
            .filter(|output| 1 + output.len() + suffix.len() <= MAX_NAME_BYTES)
            .unwrap_or(OsStr::new("vortex-rdf"));
        let mut name = OsString::from(".");
        name.push(output);
        name.push(suffix);
        let dir = parent.join(name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| spill_io_error(&parent, "directory creation", e))?;
        #[cfg(test)]
        spill_hooks::note(&dir);
        Ok(Self { dir })
    }

    /// The directory spill files go in.
    pub(crate) fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for TempRunsGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Test hook: the run directory the last [`TempRunsGuard`] this thread
/// created, which the tests read to pin where a build spilled and what the
/// directory is called.
#[cfg(test)]
pub(crate) mod spill_hooks {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    thread_local! {
        static LAST_DIR: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// Record the run directory a guard created.
    pub(super) fn note(dir: &Path) {
        LAST_DIR.with(|last| *last.borrow_mut() = Some(dir.to_path_buf()));
    }

    /// The run directory the last guard created on this thread, if any.
    pub(crate) fn last_dir() -> Option<PathBuf> {
        LAST_DIR.with(|last| last.borrow().clone())
    }
}

/// Incremental writer of one spill run: per record, a `u32` little-endian
/// length and the rkyv bytes. Its I/O failures name the directory the run
/// file is in ([`spill_io_error`]).
pub(crate) struct RunWriter<T> {
    writer: BufWriter<File>,
    /// The directory the run file is in, for error messages.
    dir: PathBuf,
    /// Serialization buffer reused across pushes; `clear` keeps its capacity.
    buf: AlignedVec,
    _marker: PhantomData<T>,
}

impl<T: Spillable> RunWriter<T> {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let dir = path.parent().unwrap_or(path).to_path_buf();
        let file = File::create(path).map_err(|e| spill_io_error(&dir, "file creation", e))?;
        Ok(Self {
            writer: BufWriter::new(file),
            dir,
            buf: AlignedVec::new(),
            _marker: PhantomData,
        })
    }

    pub(crate) fn push(&mut self, item: &T) -> Result<()> {
        // rkyv takes and returns the buffer by value.
        self.buf.clear();
        let bytes = to_bytes_in::<_, RkyvError>(item, std::mem::take(&mut self.buf))
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        let len = u32::try_from(bytes.len()).map_err(|_| {
            VortexRdfError::Serialization(format!(
                "Spill record too large: {} bytes exceeds u32::MAX",
                bytes.len()
            ))
        })?;

        self.writer
            .write_all(&len.to_le_bytes())
            .and_then(|()| self.writer.write_all(bytes.as_ref()))
            .map_err(|e| spill_io_error(&self.dir, "write", e))?;
        self.buf = bytes;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| spill_io_error(&self.dir, "write", e))
    }
}

/// Write a whole buffer of items as one spill file.
fn write_run<T: Spillable>(path: &Path, items: &[T]) -> Result<()> {
    let mut writer = RunWriter::create(path)?;
    for item in items {
        writer.push(item)?;
    }
    writer.finish()
}

/// One sorted run of a merge, read sequentially: still in memory, or spilled
/// to a temp file. A dataset that fits in one run never spills: the sorted
/// ingest buffer is the run.
pub(crate) struct Run<T>(RunInner<T>);

enum RunInner<T> {
    Memory(std::vec::IntoIter<T>),
    File(RunReader<T>),
}

impl<T: Spillable> Run<T> {
    /// A sorted in-memory buffer, consumed in place.
    pub(crate) fn memory(items: Vec<T>) -> Self {
        Run(RunInner::Memory(items.into_iter()))
    }

    /// A run previously spilled to `path`.
    pub(crate) fn file(path: &Path) -> Result<Self> {
        RunReader::new(path).map(|r| Run(RunInner::File(r)))
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>> {
        match &mut self.0 {
            RunInner::Memory(items) => Ok(items.next()),
            RunInner::File(reader) => reader.next(),
        }
    }
}

/// Sequential rkyv reader over a spill file.
struct RunReader<T> {
    reader: BufReader<File>,
    /// Payload buffer reused across reads; `AlignedVec` gives rkyv's archived
    /// types their alignment.
    payload: AlignedVec,
    _marker: PhantomData<T>,
}

impl<T: Spillable> RunReader<T> {
    fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            payload: AlignedVec::new(),
            _marker: PhantomData,
        })
    }

    fn next(&mut self) -> Result<Option<T>> {
        let mut first_len_byte = [0u8; 1];
        let n = self.reader.read(&mut first_len_byte)?;
        if n == 0 {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes[0] = first_len_byte[0];
        read_exact_or_corrupt(&mut self.reader, &mut len_bytes[1..], "length")?;

        let len = u32::from_le_bytes(len_bytes) as usize;
        // Resize without clearing: `read_exact` overwrites all `len` bytes, so
        // the zero-fill only ever pays for the growth delta.
        self.payload.resize(len, 0);
        read_exact_or_corrupt(&mut self.reader, &mut self.payload, "payload")?;

        // SAFETY: spill files are written by this process with the matching
        // rkyv serializer and read back only by the build that wrote them; no
        // external data takes this path.
        let item = unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&self.payload) }
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        Ok(Some(item))
    }
}

/// `read_exact` of one spill record's `what` (its length or its payload)
/// into `buf`. A truncated record is a corrupt spill file, a
/// `Deserialization` failure; any other read error is filesystem I/O.
fn read_exact_or_corrupt(reader: &mut impl Read, buf: &mut [u8], what: &str) -> Result<()> {
    reader.read_exact(buf).map_err(|e| {
        if e.kind() == ErrorKind::UnexpectedEof {
            VortexRdfError::Deserialization(format!(
                "Unexpected EOF while reading spill record {what}"
            ))
        } else {
            VortexRdfError::Io(e)
        }
    })
}

/// External sort of `T`s: buffers items up to a capacity, spills each full
/// buffer as a sorted run, and hands back a [`RunMerger`] that streams the
/// items in global order.
///
/// A spiller keeps every item pushed, repeats included (the index
/// `(value, row id)` records all differ, as their row ids do), unless it was
/// made with [`distinct`](Self::distinct), as the quads' spiller is.
pub(crate) struct RunSpiller<T> {
    dir: PathBuf,
    name: &'static str,
    capacity: usize,
    buf: Vec<T>,
    run_paths: Vec<PathBuf>,
    /// Whether equal items collapse to one (see [`distinct`](Self::distinct)).
    distinct: bool,
}

impl<T: Ord + Spillable> RunSpiller<T> {
    /// Runs are written to `dir` as `{name}_run_{n}.bin`.
    pub(crate) fn new(dir: &Path, name: &'static str, capacity: usize) -> Self {
        Self {
            dir: dir.to_path_buf(),
            name,
            capacity,
            buf: Vec::with_capacity(capacity.min(4096)),
            run_paths: Vec::new(),
            distinct: false,
        }
    }

    /// A spiller whose merge yields each distinct item once: equal items
    /// collapse as a run is sorted, and again across runs as they merge, so
    /// a repeat is dropped wherever it falls. Equal items must be adjacent
    /// in `T`'s order, so its `Ord` has to agree with its `Eq` (all four
    /// fields of a quad take part in both).
    pub(crate) fn distinct(dir: &Path, name: &'static str, capacity: usize) -> Self {
        Self {
            distinct: true,
            ..Self::new(dir, name, capacity)
        }
    }

    /// Sort the buffer into a run, dropping its repeats when the spiller is
    /// [`distinct`](Self::distinct): equal items are adjacent once sorted.
    fn sort_buf(&mut self) {
        self.buf.sort_unstable();
        if self.distinct {
            self.buf.dedup();
        }
    }

    pub(crate) fn push(&mut self, item: T) -> Result<()> {
        // Spill only when the next item would not fit, so a dataset of exactly
        // `capacity` items stays a single in-memory run.
        if self.buf.len() == self.capacity {
            self.flush_run()?;
        }
        self.buf.push(item);
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        self.sort_buf();
        let path = self
            .dir
            .join(format!("{}_run_{}.bin", self.name, self.run_paths.len()));
        write_run(&path, &self.buf)?;
        log::debug!(
            "[RunSpiller] Wrote sorted run {} of {} ({} items)",
            self.run_paths.len(),
            self.name,
            self.buf.len()
        );
        self.run_paths.push(path);
        self.buf.clear();
        Ok(())
    }

    /// Flush the last run and set up the K-way merge over all runs; when
    /// nothing spilled, the sorted buffer is the single in-memory run.
    pub(crate) fn into_merger(mut self) -> Result<RunMerger<T>> {
        if self.run_paths.is_empty() {
            // The one run is deduplicated in place, so the merger reads it
            // straight through whether or not the spiller is distinct.
            self.sort_buf();
            log::debug!(
                "[RunSpiller] Kept the single sorted run of {} ({} items) in memory",
                self.name,
                self.buf.len()
            );
            return RunMerger::new(vec![Run::memory(self.buf)]);
        }
        if !self.buf.is_empty() {
            self.flush_run()?;
        }
        let runs = self
            .run_paths
            .iter()
            .map(|p| Run::file(p))
            .collect::<Result<_>>()?;
        RunMerger::merge(runs, self.distinct)
    }
}

/// Streams items in global sorted order: a K-way merge over sorted runs.
pub(crate) struct RunMerger<T> {
    runs: Vec<Run<T>>,
    /// Primed with each run's head; empty while a single run is read
    /// straight through.
    heap: BinaryHeap<MinHeapItem<T>>,
    /// Whether an item present more than once comes out once.
    distinct: bool,
}

impl<T: Ord + Spillable> RunMerger<T> {
    /// Merge `runs`, each already sorted, keeping every item.
    pub(crate) fn new(runs: Vec<Run<T>>) -> Result<Self> {
        Self::merge(runs, false)
    }

    /// Merge `runs`, each already sorted. With `distinct`, an item that
    /// appears more than once (in several runs, or repeated inside one) comes
    /// out once.
    pub(crate) fn merge(mut runs: Vec<Run<T>>, distinct: bool) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        // A lone run reads straight through, unless its repeats must be
        // dropped: that takes the heap, which sees the next item of a run
        // before anything is emitted.
        if runs.len() > 1 || distinct {
            for (run_idx, run) in runs.iter_mut().enumerate() {
                if let Some(item) = run.next()? {
                    heap.push(MinHeapItem { item, run_idx });
                }
            }
        }
        Ok(Self {
            runs,
            heap,
            distinct,
        })
    }

    /// How many runs feed the merge.
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// The next item in global order, or `None` once every run is drained.
    pub(crate) fn next(&mut self) -> Result<Option<T>> {
        if self.runs.len() == 1 && !self.distinct {
            return self.runs[0].next();
        }
        let Some(MinHeapItem { item, run_idx }) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(run_idx)?;
        if self.distinct {
            // Equal items are adjacent in the merged order, so every other
            // copy of `item` is the head of its run: at the top of the heap.
            // Popping them leaves the next distinct item on top.
            while self.heap.peek().is_some_and(|top| top.item == item) {
                if let Some(MinHeapItem { run_idx, .. }) = self.heap.pop() {
                    self.refill(run_idx)?;
                }
            }
        }
        Ok(Some(item))
    }

    /// Put run `run_idx`'s next item, if it has one, back on the heap.
    fn refill(&mut self, run_idx: usize) -> Result<()> {
        if let Some(next) = self.runs[run_idx].next()? {
            self.heap.push(MinHeapItem {
                item: next,
                run_idx,
            });
        }
        Ok(())
    }

    /// Pull up to `n` items off the merge (fewer at the end of the data).
    pub(crate) fn next_batch(&mut self, n: usize) -> Result<Vec<T>> {
        let mut batch = Vec::with_capacity(n.min(4096));
        while batch.len() < n {
            match self.next()? {
                Some(item) => batch.push(item),
                None => break,
            }
        }
        Ok(batch)
    }
}

/// Where [`merge_feeding_indexes`] puts the merged quads: straight into
/// memory when the merge had a single input run, otherwise into a spill file.
///
/// [`merge_feeding_indexes`]: super::stream_indexes::merge_feeding_indexes
pub(super) enum MergedSink {
    Memory(Vec<RawQuad>),
    File {
        writer: RunWriter<RawQuad>,
        path: PathBuf,
    },
}

impl MergedSink {
    /// An in-memory sink, or a writer to `merged.bin` under `dir`.
    pub(super) fn create(dir: &Path, in_memory: bool) -> Result<Self> {
        if in_memory {
            return Ok(MergedSink::Memory(Vec::new()));
        }
        let path = dir.join("merged.bin");
        Ok(MergedSink::File {
            writer: RunWriter::create(&path)?,
            path,
        })
    }

    pub(super) fn push(&mut self, quad: RawQuad) -> Result<()> {
        match self {
            MergedSink::Memory(quads) => {
                quads.push(quad);
                Ok(())
            }
            MergedSink::File { writer, .. } => writer.push(&quad),
        }
    }

    /// Close the sink and hand back the merged quads as a readable run.
    pub(super) fn finish(self) -> Result<Run<RawQuad>> {
        match self {
            MergedSink::Memory(quads) => Ok(Run::memory(quads)),
            MergedSink::File { writer, path } => {
                writer.finish()?;
                Run::file(&path)
            }
        }
    }
}

/// A run's head in the merge heap; ordered by item, then run index, both
/// reversed so `BinaryHeap` pops the minimum.
struct MinHeapItem<T> {
    item: T,
    run_idx: usize,
}

impl<T: Ord> Eq for MinHeapItem<T> {}
impl<T: Ord> PartialEq for MinHeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item == other.item && self.run_idx == other.run_idx
    }
}
impl<T: Ord> Ord for MinHeapItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .item
            .cmp(&self.item)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl<T: Ord> PartialOrd for MinHeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spill parent, by `(env override, base)`: the override wins over
    /// `base`, `base` over the OS temp dir, and an empty override counts as
    /// unset.
    #[test]
    fn spill_parent_resolves_env_then_base_then_os_temp() {
        for (env, base, parent) in [
            (
                Some("/custom/spill"),
                Some("/base"),
                PathBuf::from("/custom/spill"),
            ),
            (None, Some("/base"), PathBuf::from("/base")),
            (Some(""), None, std::env::temp_dir()),
        ] {
            assert_eq!(
                resolve_spill_parent(env.map(std::ffi::OsString::from), base.map(Path::new)),
                parent,
                "env {env:?}, base {base:?}"
            );
        }
    }

    #[test]
    fn temp_runs_guard_honors_base_and_cleans_up() {
        // The env override outranks `base` by design, so a preset override in
        // the test environment would (correctly) redirect this spill; only
        // assert placement when it is absent.
        if std::env::var_os(SPILL_DIR_ENV).is_some() {
            return;
        }
        let base = std::env::temp_dir().join(format!("vortex_spill_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        let guard = TempRunsGuard::create(None, Some(&base)).unwrap();
        let dir = guard.path().to_path_buf();
        assert!(dir.starts_with(&base));
        assert!(dir.is_dir());
        drop(guard);
        assert!(!dir.exists());
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn temp_runs_guard_names_the_directory_after_its_output() {
        // As above: a preset override would (correctly) take the directory
        // away from `base`, so only assert where it is absent.
        if std::env::var_os(SPILL_DIR_ENV).is_some() {
            return;
        }
        let base = std::env::temp_dir().join(format!("vortex_spill_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        for (output, prefix) in [
            (Some(OsStr::new("out.vortex")), ".out.vortex.spill-"),
            (None, ".vortex-rdf.spill-"),
        ] {
            let guard = TempRunsGuard::create(output, Some(&base)).unwrap();
            let dir = guard.path();
            assert_eq!(dir.parent(), Some(base.as_path()), "{output:?}");
            let name = dir.file_name().unwrap().to_str().unwrap();
            let id = name
                .strip_prefix(prefix)
                .unwrap_or_else(|| panic!("{output:?}: {name} does not start with {prefix}"));
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{output:?}: {name}");
        }
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn temp_runs_guard_falls_back_when_the_output_name_is_too_long() {
        // `.<output>.spill-<uuid>` is the output's length plus 44 bytes: a
        // 211-byte output makes a name of 255 bytes, as long as a file name
        // may be, and a longer one (212, and 250: 294 bytes) takes the name a
        // build without an output has.
        for (len, fits) in [(211, true), (212, false), (250, false)] {
            let output = "x".repeat(len);
            let guard = TempRunsGuard::create(Some(OsStr::new(&output)), None).unwrap();
            let dir = guard.path();
            assert!(dir.is_dir(), "{len}");
            let name = dir.file_name().unwrap().to_str().unwrap();
            let prefix = if fits {
                format!(".{output}.spill-")
            } else {
                ".vortex-rdf.spill-".to_owned()
            };
            let id = name
                .strip_prefix(prefix.as_str())
                .unwrap_or_else(|| panic!("{len}: {name} does not start with {prefix}"));
            assert!(uuid::Uuid::parse_str(id).is_ok(), "{len}: {name}");
            if fits {
                assert_eq!(name.len(), MAX_NAME_BYTES, "{len}");
            }
        }
    }

    #[test]
    fn spill_io_error_suggests_room_for_a_full_volume() {
        for kind in [ErrorKind::StorageFull, ErrorKind::QuotaExceeded] {
            let error = spill_io_error(Path::new("/spill/runs"), "write", kind.into());
            assert!(
                matches!(&error, VortexRdfError::Io(e) if e.kind() == kind),
                "{error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("spill run write failed in /spill/runs: "),
                "{message}"
            );
            assert!(
                message.ends_with("; set VORTEX_RDF_SPILL_DIR to a directory with more room"),
                "{message}"
            );
        }

        // A real full volume: a run smaller than the writer's buffer reaches
        // the device only at the final flush, which `finish` reports (the
        // buffered writer's drop would swallow its error).
        #[cfg(target_os = "linux")]
        {
            let error = write_run(Path::new("/dev/full"), &string_records()).unwrap_err();
            assert!(
                matches!(&error, VortexRdfError::Io(e) if e.kind() == ErrorKind::StorageFull),
                "{error}"
            );
            let message = error.to_string();
            assert!(
                message.contains("spill run write failed in /dev: "),
                "{message}"
            );
            assert!(
                message.ends_with("; set VORTEX_RDF_SPILL_DIR to a directory with more room"),
                "{message}"
            );
        }
    }

    fn string_records() -> Vec<(String, u32)> {
        // Variable-length records exercise the reused write/read buffers
        // growing and shrinking across pushes.
        (0..64u32)
            .map(|i| ("x".repeat((i as usize * 7) % 41), i))
            .collect()
    }

    #[test]
    fn run_roundtrip_through_reused_buffers() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let path = guard.path().join("run.bin");
        let records = string_records();
        write_run(&path, &records).unwrap();
        let mut run: Run<(String, u32)> = Run::file(&path).unwrap();
        for expected in &records {
            assert_eq!(run.next().unwrap().as_ref(), Some(expected));
        }
        assert!(run.next().unwrap().is_none());
    }

    #[test]
    fn truncated_run_is_a_deserialization_error() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let path = guard.path().join("run.bin");
        let records = string_records();
        write_run(&path, &records).unwrap();
        let full = std::fs::read(&path).unwrap();
        // Record 0 is an empty string; record 1's payload starts after its
        // 4-byte length prefix.
        let first_len = u32::from_le_bytes(full[..4].try_into().unwrap()) as usize;
        let second_start = 4 + first_len;
        let second_len =
            u32::from_le_bytes(full[second_start..second_start + 4].try_into().unwrap()) as usize;
        assert!(second_len > 1, "test needs a multi-byte payload");
        for (label, keep) in [
            ("mid-length-prefix", second_start + 2),
            ("mid-payload", second_start + 4 + second_len / 2),
        ] {
            std::fs::write(&path, &full[..keep]).unwrap();
            let mut merger: RunMerger<(String, u32)> =
                RunMerger::new(vec![Run::file(&path).unwrap()]).unwrap();
            assert_eq!(
                merger.next().unwrap().as_ref(),
                Some(&records[0]),
                "{label}"
            );
            assert!(
                matches!(merger.next(), Err(VortexRdfError::Deserialization(_))),
                "{label}: truncated record must not read short"
            );
        }
    }

    /// Items that repeat inside a run and across runs: twelve distinct pairs
    /// over a small domain, each showing up three or four times, in a
    /// scrambled order.
    fn repeated_items() -> Vec<(u32, u32)> {
        (0..40u32)
            .map(|i| {
                let x = (i * 7919) % 12;
                (x / 4, x % 4)
            })
            .collect()
    }

    #[test]
    fn distinct_spiller_collapses_repeats_inside_and_across_runs() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let items = repeated_items();
        let mut expected = items.clone();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(expected.len(), 12);
        assert!(expected.len() * 3 <= items.len(), "the test needs repeats");

        for capacity in [1, 3, 4, 7, 13, 40, 100] {
            let mut spiller: RunSpiller<(u32, u32)> =
                RunSpiller::distinct(guard.path(), "distinct", capacity);
            for item in &items {
                spiller.push(*item).unwrap();
            }
            let mut merger = spiller.into_merger().unwrap();
            // More than one run is the case that needs the heap; one run is
            // deduplicated in place.
            assert_eq!(merger.run_count() > 1, capacity < items.len(), "{capacity}");
            let mut merged = merger.next_batch(5).unwrap();
            merged.extend(merger.next_batch(usize::MAX).unwrap());
            assert_eq!(merged, expected, "run capacity {capacity}");
            assert!(merger.next().unwrap().is_none());
        }
    }

    /// The default spiller keeps every item pushed: the index records are
    /// distinct by their row ids, never by being deduplicated.
    #[test]
    fn plain_spiller_keeps_repeats() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let items = repeated_items();
        let mut expected = items.clone();
        expected.sort_unstable();

        for capacity in [3, 100] {
            let mut spiller: RunSpiller<(u32, u32)> =
                RunSpiller::new(guard.path(), "plain", capacity);
            for item in &items {
                spiller.push(*item).unwrap();
            }
            let mut merger = spiller.into_merger().unwrap();
            assert_eq!(
                merger.next_batch(usize::MAX).unwrap(),
                expected,
                "{capacity}"
            );
        }
    }

    /// A distinct merge over runs that were never deduplicated themselves
    /// (a lone file run with repeats, and several runs full of them) still
    /// yields each item once.
    #[test]
    fn distinct_merge_of_runs_with_repeats() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let write = |name: &str, items: &[(u32, u32)]| {
            let path = guard.path().join(name);
            write_run(&path, items).unwrap();
            Run::<(u32, u32)>::file(&path).unwrap()
        };
        let lone = write(
            "lone.bin",
            &[(0, 0), (0, 0), (1, 1), (1, 1), (1, 1), (2, 2)],
        );
        let mut merger = RunMerger::merge(vec![lone], true).unwrap();
        assert_eq!(
            merger.next_batch(usize::MAX).unwrap(),
            [(0, 0), (1, 1), (2, 2)]
        );

        let a = write("a.bin", &[(0, 0), (0, 0), (1, 1), (3, 3)]);
        let b = write("b.bin", &[(0, 0), (1, 1), (1, 1), (2, 2)]);
        let c = write("c.bin", &[(1, 1), (2, 2), (2, 2), (3, 3), (3, 3)]);
        let mut merger = RunMerger::merge(vec![a, b, c], true).unwrap();
        assert_eq!(
            merger.next_batch(usize::MAX).unwrap(),
            [(0, 0), (1, 1), (2, 2), (3, 3)]
        );
        assert!(merger.next().unwrap().is_none());
    }

    #[test]
    fn spiller_merges_runs_in_global_order() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let mut spiller: RunSpiller<(u32, u32)> = RunSpiller::new(guard.path(), "pairs", 4);
        let items: Vec<(u32, u32)> = (0..23u32).rev().map(|i| (i % 5, i)).collect();
        for item in &items {
            spiller.push(*item).unwrap();
        }
        let mut merger = spiller.into_merger().unwrap();
        assert_eq!(merger.run_count(), 6);
        let mut merged = merger.next_batch(7).unwrap();
        merged.extend(merger.next_batch(usize::MAX).unwrap());
        let mut expected = items.clone();
        expected.sort_unstable();
        assert_eq!(merged, expected);
        assert!(merger.next().unwrap().is_none());
    }
}
