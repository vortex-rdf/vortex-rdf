//! Temp-file spill machinery behind the out-of-core builder: quads (and, for
//! globally sorted secondary indexes, `(value, row ID)` pairs) are serialized
//! to disk with rkyv during ingestion/merge passes and read back during chunk
//! emission, so peak memory stays bounded by the chunk size.

use std::collections::BinaryHeap;
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

/// Environment variable overriding where spill directories are created — the
/// escape hatch for putting out-of-core runs on a specific volume. The OS
/// temp dir is commonly a size-capped, RAM-backed tmpfs, exactly the wrong
/// home for runs that exist because the data outgrew memory.
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

/// A unique temporary spill directory, deleted when dropped so spill files are
/// cleaned up even if the chunk stream is abandoned before being fully
/// consumed.
pub(crate) struct TempRunsGuard {
    dir: PathBuf,
}

impl TempRunsGuard {
    /// Create `tmp_vortex_{prefix}_{uuid}` under the spill parent.
    ///
    /// The parent is resolved in precedence order: the `VORTEX_RDF_SPILL_DIR`
    /// environment variable, then the caller-provided `base` (compaction
    /// passes the store file's own directory so spills share the output's
    /// volume), then [`std::env::temp_dir`]. The library never writes into
    /// the caller's working directory: an embedding server or binding can run
    /// with an arbitrary — even read-only — cwd.
    pub(crate) fn create(prefix: &str, base: Option<&Path>) -> Result<Self> {
        let parent = resolve_spill_parent(std::env::var_os(SPILL_DIR_ENV), base);
        let dir = parent.join(format!("tmp_vortex_{}_{}", prefix, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
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

/// Incremental rkyv writer for spilling items one at a time.
pub(crate) struct RunWriter<T> {
    writer: BufWriter<File>,
    /// Serialization buffer reused across pushes; `clear` keeps its capacity.
    buf: AlignedVec,
    _marker: PhantomData<T>,
}

impl<T: Spillable> RunWriter<T> {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            buf: AlignedVec::new(),
            _marker: PhantomData,
        })
    }

    pub(crate) fn push(&mut self, item: &T) -> Result<()> {
        // rkyv consumes and returns its writer by value, so the held buffer
        // is taken and put back around each serialization.
        self.buf.clear();
        let bytes = to_bytes_in::<_, RkyvError>(item, std::mem::take(&mut self.buf))
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        let len = u32::try_from(bytes.len()).map_err(|_| {
            VortexRdfError::Serialization(format!(
                "Spill record too large: {} bytes exceeds u32::MAX",
                bytes.len()
            ))
        })?;

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(bytes.as_ref())?;
        self.buf = bytes;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        Ok(self.writer.flush()?)
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

/// One sorted run of a merge, read sequentially — either still in memory or
/// spilled to a temp file.
///
/// A dataset that fits in a single run never round-trips through rkyv and
/// the filesystem: the ingest buffer *is* the run, already sorted and in
/// memory. Only once a second run exists does spilling buy anything, so the
/// builder spills lazily and keeps a lone run here.
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

/// Pull up to `n` items off `next` (fewer once it yields `None`).
fn pull_batch<T>(n: usize, mut next: impl FnMut() -> Result<Option<T>>) -> Result<Vec<T>> {
    let mut batch = Vec::with_capacity(n.min(4096));
    while batch.len() < n {
        match next()? {
            Some(item) => batch.push(item),
            None => break,
        }
    }
    Ok(batch)
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

        // A truncated record is a corrupt spill file — a format-level
        // `Deserialization` failure — while any other read error is plain
        // filesystem I/O.
        let mut len_bytes = [0u8; 4];
        len_bytes[0] = first_len_byte[0];
        self.reader.read_exact(&mut len_bytes[1..]).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                VortexRdfError::Deserialization(
                    "Unexpected EOF while reading spill record length".to_string(),
                )
            } else {
                VortexRdfError::Io(e)
            }
        })?;

        let len = u32::from_le_bytes(len_bytes) as usize;
        // Resize without clearing: `read_exact` overwrites all `len` bytes, so
        // the zero-fill only ever pays for the growth delta.
        self.payload.resize(len, 0);
        self.reader.read_exact(&mut self.payload).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                VortexRdfError::Deserialization(
                    "Unexpected EOF while reading spill record payload".to_string(),
                )
            } else {
                VortexRdfError::Io(e)
            }
        })?;

        // SAFETY: spill files are produced by this process using the matching
        // rkyv serializer and consumed immediately; we don't accept external
        // untrusted data on this path.
        let item = unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&self.payload) }
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        Ok(Some(item))
    }
}

/// External sort of `T`s: buffers items up to a capacity, spills each full
/// buffer as a sorted run, and hands back a [`RunMerger`] that streams the
/// items in global order.
pub(crate) struct RunSpiller<T> {
    dir: PathBuf,
    name: &'static str,
    capacity: usize,
    buf: Vec<T>,
    run_paths: Vec<PathBuf>,
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
        }
    }

    pub(crate) fn push(&mut self, item: T) -> Result<()> {
        // Spill only when the next item would not fit, so a dataset of exactly
        // `capacity` items stays a single in-memory run (see [`Run`]).
        if self.buf.len() == self.capacity {
            self.flush_run()?;
        }
        self.buf.push(item);
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        self.buf.sort_unstable();
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

    /// Flush the tail run and set up the K-way merge over all runs.
    ///
    /// Nothing spilled means everything still sits in `buf`: sorting it in
    /// place is the whole merge, so it becomes a single in-memory run.
    pub(crate) fn into_merger(mut self) -> Result<RunMerger<T>> {
        if self.run_paths.is_empty() {
            self.buf.sort_unstable();
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
        RunMerger::new(runs)
    }
}

/// Streams items in global sorted order: a K-way merge over sorted runs.
pub(crate) struct RunMerger<T> {
    runs: Vec<Run<T>>,
    /// Primed with each run's head; empty while a single run is read
    /// straight through.
    heap: BinaryHeap<MinHeapItem<T>>,
}

impl<T: Ord + Spillable> RunMerger<T> {
    /// Merge `runs`, each already sorted.
    pub(crate) fn new(mut runs: Vec<Run<T>>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        if runs.len() > 1 {
            for (run_idx, run) in runs.iter_mut().enumerate() {
                if let Some(item) = run.next()? {
                    heap.push(MinHeapItem { item, run_idx });
                }
            }
        }
        Ok(Self { runs, heap })
    }

    /// How many runs feed the merge.
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// The next item in global order, or `None` once every run is drained.
    pub(crate) fn next(&mut self) -> Result<Option<T>> {
        if self.runs.len() == 1 {
            return self.runs[0].next();
        }
        let Some(MinHeapItem { item, run_idx }) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(next) = self.runs[run_idx].next()? {
            self.heap.push(MinHeapItem {
                item: next,
                run_idx,
            });
        }
        Ok(Some(item))
    }

    /// Pull up to `n` items off the merge (fewer at the end of the data).
    pub(crate) fn next_batch(&mut self, n: usize) -> Result<Vec<T>> {
        pull_batch(n, || self.next())
    }
}

/// Where [`merge_quads_feeding_indexes`] puts the merged quads: straight into
/// memory when the merge had a single input run, otherwise into a spill file.
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

    #[test]
    fn spill_parent_env_override_wins() {
        let parent = resolve_spill_parent(Some("/custom/spill".into()), Some(Path::new("/base")));
        assert_eq!(parent, PathBuf::from("/custom/spill"));
    }

    #[test]
    fn spill_parent_prefers_base_over_os_temp() {
        let parent = resolve_spill_parent(None, Some(Path::new("/base")));
        assert_eq!(parent, PathBuf::from("/base"));
    }

    #[test]
    fn spill_parent_treats_empty_env_as_unset() {
        let parent = resolve_spill_parent(Some("".into()), None);
        assert_eq!(parent, std::env::temp_dir());
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
        let guard = TempRunsGuard::create("unit", Some(&base)).unwrap();
        let dir = guard.path().to_path_buf();
        assert!(dir.starts_with(&base));
        assert!(dir.is_dir());
        drop(guard);
        assert!(!dir.exists());
        std::fs::remove_dir_all(&base).unwrap();
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
        let guard = TempRunsGuard::create("unit_roundtrip", None).unwrap();
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
        let guard = TempRunsGuard::create("unit_truncated", None).unwrap();
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

    #[test]
    fn spiller_merges_runs_in_global_order() {
        let guard = TempRunsGuard::create("unit_merge", None).unwrap();
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
