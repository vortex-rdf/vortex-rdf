//! Temp-file spill machinery shared by the out-of-core builders: quads (and,
//! for globally sorted secondary indexes, `(value, row ID)` pairs) are
//! serialized to disk with rkyv during ingestion/merge passes and read back
//! during chunk emission, so peak memory stays bounded by the chunk size.

use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use web_time::{SystemTime, UNIX_EPOCH};

use rkyv::api::high::{HighDeserializer, HighSerializer, to_bytes_in};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::error::{Result, VortexRdfError};

/// Environment variable overriding where spill directories are created — the
/// escape hatch for putting out-of-core runs on a specific volume. The OS
/// temp dir is commonly a size-capped, RAM-backed tmpfs, exactly the wrong
/// home for runs that exist because the data outgrew memory.
pub(crate) const SPILL_DIR_ENV: &str = "VORTEX_RDF_SPILL_DIR";

/// Create a unique temp directory for spill files.
///
/// The parent directory is resolved in precedence order: the
/// [`SPILL_DIR_ENV`] (`VORTEX_RDF_SPILL_DIR`) environment variable, then the
/// caller-provided `base` (compaction passes the store file's own directory
/// so spills share the output's volume), then [`std::env::temp_dir`]. The
/// library must never write into the caller's working directory: a server or
/// binding embedding this crate can run with an arbitrary — even read-only —
/// cwd.
///
/// Spilling needs a real filesystem, which `wasm32-unknown-unknown` does not
/// have, so this whole module is compiled out there (see the module gate in
/// the `builders` hub); the wasm-reachable build paths never spill.
pub(crate) fn make_temp_dir(prefix: &str, base: Option<&Path>) -> Result<PathBuf> {
    let id = uuid::Uuid::new_v4();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let parent = resolve_spill_parent(std::env::var_os(SPILL_DIR_ENV), base);
    let dir = parent.join(format!("tmp_vortex_{}_{}_{}", prefix, now, id));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// The parent-directory precedence behind [`make_temp_dir`], split out so it
/// is testable without mutating the process environment (other tests spill
/// concurrently in this process and would race a real env override). An
/// empty override counts as unset.
fn resolve_spill_parent(env_override: Option<std::ffi::OsString>, base: Option<&Path>) -> PathBuf {
    env_override
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| base.map(Path::to_path_buf))
        .unwrap_or_else(std::env::temp_dir)
}

/// Deletes the temporary spill directory when dropped, so spill files are
/// cleaned up even if the chunk stream is abandoned before being fully consumed.
pub(crate) struct TempRunsGuard {
    pub(crate) dir: PathBuf,
}

impl Drop for TempRunsGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Incremental rkyv writer for spilling items one at a time.
pub(crate) struct RunWriter<T> {
    writer: BufWriter<File>,
    /// Reused per-record serialization buffer: `rkyv::to_bytes` would
    /// allocate a fresh `AlignedVec` per spilled record, the same allocator
    /// churn the reused `payload` buffer on [`RunReader`] was added to kill
    /// on the read side.
    buf: AlignedVec,
    _marker: PhantomData<T>,
}

impl<T> RunWriter<T> {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            buf: AlignedVec::new(),
            _marker: PhantomData,
        })
    }

    pub(crate) fn push(&mut self, item: &T) -> Result<()>
    where
        T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
        T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
        // Serialize into the held buffer (taken and put back because rkyv
        // consumes and returns its writer by value); `clear` keeps the
        // capacity, so steady-state pushes never touch the allocator.
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
pub(crate) fn write_run<T>(path: &Path, items: &[T]) -> Result<()>
where
    T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
{
    let mut writer = RunWriter::create(path)?;
    for item in items {
        writer.push(item)?;
    }
    writer.finish()
}

/// One sorted run of a merge, read sequentially — either still in memory or
/// spilled to a temp file.
///
/// A dataset that fits in a single run never has to round-trip through rkyv and
/// the filesystem at all: the ingest buffer *is* the run, already sorted and
/// already in memory. Only once a second run exists does spilling buy anything
/// (that is the point at which the data provably exceeds the memory budget), so
/// the builders spill lazily and keep a lone run here instead. This is the
/// common case for datasets up to the chunk size, where the old unconditional
/// spill paid a full serialize + write + read + deserialize of every quad.
pub(crate) enum Run<T> {
    Memory(std::vec::IntoIter<T>),
    File(RunReader<T>),
}

impl<T> Run<T> {
    /// A sorted in-memory buffer, consumed in place.
    pub(crate) fn memory(items: Vec<T>) -> Self {
        Run::Memory(items.into_iter())
    }

    /// A run previously spilled to `path`.
    pub(crate) fn file(path: &Path) -> Result<Self> {
        RunReader::new(path).map(Run::File)
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>>
    where
        T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
        T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
        match self {
            Run::Memory(items) => Ok(items.next()),
            Run::File(reader) => reader.next(),
        }
    }
}

/// Sequential rkyv reader over a spill file.
pub(crate) struct RunReader<T> {
    reader: BufReader<File>,
    /// Reused per-record payload buffer: a fresh `vec![0u8; len]` per record
    /// would malloc+zero on every read (profiling showed the allocator
    /// dominating spill read-back), and `AlignedVec` also guarantees the
    /// alignment rkyv's archived types require.
    payload: AlignedVec,
    _marker: PhantomData<T>,
}

impl<T> RunReader<T> {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            payload: AlignedVec::new(),
            _marker: PhantomData,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>>
    where
        T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
        T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
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
        // the zero-fill only ever pays for the growth delta, not every record.
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

/// External sort of `(value, row ID)` pairs: buffers pairs up to a capacity,
/// spills each full buffer as a sorted run, and hands back a [`PairMerger`]
/// that streams the pairs in global `(value, row ID)` order.
///
/// This is the machinery behind globally sorted secondary-index columns in
/// out-of-core builds: the row IDs are only known during the quad merge, so
/// the index order must be derived by a second sort after it.
pub(crate) struct PairRunSpiller<V> {
    dir: PathBuf,
    name: &'static str,
    capacity: usize,
    buf: Vec<PairRecord<V>>,
    run_paths: Vec<PathBuf>,
}

#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct PairRecord<V> {
    value: V,
    rid: u32,
}

impl<V> PairRunSpiller<V>
where
    V: Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
{
    pub(crate) fn new(dir: &Path, name: &'static str, capacity: usize) -> Self {
        Self {
            dir: dir.to_path_buf(),
            name,
            capacity,
            buf: Vec::with_capacity(capacity.min(4096)),
            run_paths: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, value: V, rid: u32) -> Result<()> {
        // Spill only to make room for a record that would not fit, never merely
        // on reaching capacity: a dataset of exactly `capacity` pairs then ends
        // as one in-memory run and skips the spill round-trip entirely (see
        // [`Run`]). Peak buffered records is unchanged.
        if self.buf.len() == self.capacity {
            self.flush_run()?;
        }
        self.buf.push(PairRecord { value, rid });
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        self.buf.sort_unstable();
        let path = self
            .dir
            .join(format!("{}_run_{}.bin", self.name, self.run_paths.len()));
        write_run(&path, &self.buf)?;
        self.run_paths.push(path);
        self.buf.clear();
        Ok(())
    }

    /// Flush the tail run and set up the K-way merge over all runs.
    ///
    /// Nothing spilled means everything still sits in `buf`: sorting it in place
    /// is the whole merge, so it becomes a single in-memory run rather than a
    /// file to write and immediately read back.
    pub(crate) fn into_merger(mut self) -> Result<PairMerger<V>> {
        if self.run_paths.is_empty() {
            self.buf.sort_unstable();
            return Ok(PairMerger::Memory(self.buf.into_iter()));
        }
        if !self.buf.is_empty() {
            self.flush_run()?;
        }
        let mut readers: Vec<RunReader<PairRecord<V>>> = self
            .run_paths
            .iter()
            .map(|p| RunReader::new(p))
            .collect::<Result<_>>()?;
        let mut heap = BinaryHeap::new();
        for (i, r) in readers.iter_mut().enumerate() {
            if let Some(pair) = r.next()? {
                heap.push(PairHeapItem {
                    pair,
                    reader_idx: i,
                });
            }
        }
        Ok(PairMerger::Spilled { readers, heap })
    }
}

/// Streams `(value, row ID)` pairs in global sorted order: a K-way merge of the
/// runs a [`PairRunSpiller`] spilled, or — when nothing had to spill — a walk
/// over the single sorted buffer it kept in memory.
pub(crate) enum PairMerger<V> {
    /// Everything fit in one run; the sorted buffer *is* the merged order.
    Memory(std::vec::IntoIter<PairRecord<V>>),
    Spilled {
        readers: Vec<RunReader<PairRecord<V>>>,
        heap: BinaryHeap<PairHeapItem<V>>,
    },
}

impl<V> PairMerger<V>
where
    V: Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
{
    /// Pull the next `n` pairs off the merge (fewer at the end of the data).
    pub(crate) fn next_batch(&mut self, n: usize) -> Result<Vec<(V, u32)>> {
        let mut batch = Vec::with_capacity(n.min(4096));
        match self {
            PairMerger::Memory(pairs) => {
                batch.extend(pairs.take(n).map(|pair| (pair.value, pair.rid)));
            }
            PairMerger::Spilled { readers, heap } => {
                while batch.len() < n {
                    let Some(item) = heap.pop() else { break };
                    let r_idx = item.reader_idx;
                    batch.push((item.pair.value, item.pair.rid));
                    if let Some(next_pair) = readers[r_idx].next()? {
                        heap.push(PairHeapItem {
                            pair: next_pair,
                            reader_idx: r_idx,
                        });
                    }
                }
            }
        }
        Ok(batch)
    }
}

pub(crate) struct PairHeapItem<V> {
    pair: PairRecord<V>,
    reader_idx: usize,
}

impl<V: Ord> Eq for PairHeapItem<V> {}
impl<V: Ord> PartialEq for PairHeapItem<V> {
    fn eq(&self, other: &Self) -> bool {
        self.pair == other.pair
    }
}
impl<V: Ord> Ord for PairHeapItem<V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.pair.cmp(&self.pair) // reversed for min-heap
    }
}
impl<V: Ord> PartialOrd for PairHeapItem<V> {
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
    fn make_temp_dir_honors_base_and_guard_cleans_up() {
        // The env override outranks `base` by design, so a preset override in
        // the test environment would (correctly) redirect this spill; only
        // assert placement when it is absent.
        if std::env::var_os(SPILL_DIR_ENV).is_some() {
            return;
        }
        let base = std::env::temp_dir().join(format!("vortex_spill_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        let dir = make_temp_dir("unit", Some(&base)).unwrap();
        assert!(dir.starts_with(&base));
        assert!(dir.is_dir());
        drop(TempRunsGuard { dir: dir.clone() });
        assert!(!dir.exists());
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn run_roundtrip_through_reused_buffers() {
        // Variable-length records exercise the reused write/read buffers
        // growing and shrinking across pushes.
        let dir = make_temp_dir("unit_roundtrip", None).unwrap();
        let guard = TempRunsGuard { dir: dir.clone() };
        let path = dir.join("run.bin");
        let records: Vec<PairRecord<String>> = (0..64u32)
            .map(|i| PairRecord {
                value: "x".repeat((i as usize * 7) % 41),
                rid: i,
            })
            .collect();
        write_run(&path, &records).unwrap();
        let mut reader: RunReader<PairRecord<String>> = RunReader::new(&path).unwrap();
        for expected in &records {
            assert_eq!(reader.next().unwrap().as_ref(), Some(expected));
        }
        assert!(reader.next().unwrap().is_none());
        drop(guard);
    }
}
