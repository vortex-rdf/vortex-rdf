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

use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::error::{Result, VortexRdfError};

/// Create a unique temp directory under `target/` for spill files.
pub(crate) fn make_temp_dir(prefix: &str) -> Result<PathBuf> {
    let id = uuid::Uuid::new_v4();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::current_dir()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
        .join("target")
        .join(format!("tmp_vortex_{}_{}_{}", prefix, now, id));
    std::fs::create_dir_all(&dir).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    Ok(dir)
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
    _marker: PhantomData<T>,
}

impl<T> RunWriter<T> {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let file =
            File::create(path).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        Ok(Self {
            writer: BufWriter::new(file),
            _marker: PhantomData,
        })
    }

    pub(crate) fn push(&mut self, item: &T) -> Result<()>
    where
        T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
        T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
        let bytes = rkyv::to_bytes::<RkyvError>(item)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        let len = u32::try_from(bytes.len()).map_err(|_| {
            VortexRdfError::Serialization(format!(
                "Spill record too large: {} bytes exceeds u32::MAX",
                bytes.len()
            ))
        })?;

        self.writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        self.writer
            .write_all(bytes.as_ref())
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))
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

/// Sequential rkyv reader over a spill file.
pub(crate) struct RunReader<T> {
    reader: BufReader<File>,
    _marker: PhantomData<T>,
}

impl<T> RunReader<T> {
    pub(crate) fn new(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        Ok(Self {
            reader: BufReader::new(file),
            _marker: PhantomData,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<T>>
    where
        T: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
        T::Archived: RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
        let mut first_len_byte = [0u8; 1];
        let n = self
            .reader
            .read(&mut first_len_byte)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
        if n == 0 {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes[0] = first_len_byte[0];
        self.reader.read_exact(&mut len_bytes[1..]).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                VortexRdfError::Deserialization(
                    "Unexpected EOF while reading spill record length".to_string(),
                )
            } else {
                VortexRdfError::Deserialization(e.to_string())
            }
        })?;

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                VortexRdfError::Deserialization(
                    "Unexpected EOF while reading spill record payload".to_string(),
                )
            } else {
                VortexRdfError::Deserialization(e.to_string())
            }
        })?;

        // SAFETY: spill files are produced by this process using the matching
        // rkyv serializer and consumed immediately; we don't accept external
        // untrusted data on this path.
        let item = unsafe { rkyv::from_bytes_unchecked::<T, RkyvError>(&payload) }
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
struct PairRecord<V> {
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
        self.buf.push(PairRecord { value, rid });
        if self.buf.len() >= self.capacity {
            self.flush_run()?;
        }
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
    pub(crate) fn into_merger(mut self) -> Result<PairMerger<V>> {
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
        Ok(PairMerger { readers, heap })
    }
}

/// Streams `(value, row ID)` pairs in global sorted order by K-way merging
/// the sorted runs produced by a [`PairRunSpiller`].
pub(crate) struct PairMerger<V> {
    readers: Vec<RunReader<PairRecord<V>>>,
    heap: BinaryHeap<PairHeapItem<V>>,
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
        while batch.len() < n {
            let Some(item) = self.heap.pop() else { break };
            let r_idx = item.reader_idx;
            batch.push((item.pair.value, item.pair.rid));
            if let Some(next_pair) = self.readers[r_idx].next()? {
                self.heap.push(PairHeapItem {
                    pair: next_pair,
                    reader_idx: r_idx,
                });
            }
        }
        Ok(batch)
    }
}

struct PairHeapItem<V> {
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
