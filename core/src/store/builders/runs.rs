//! Fixed-width spill runs of the out-of-core build — quads as four
//! provisional term ids, index-family entries as term ranks beside a row
//! offset — and the K-way merge that streams sorted runs back in global
//! order. Every record is little-endian, one width per kind of run, so a
//! run's size is its record count times that width (plus the header):
//!
//! | Run | Header | Record |
//! |---|---|---|
//! | `quads_run_N.bin` | none | `[u32; 4]` ids: 16 bytes |
//! | `idx_o_…`, `idx_p_…` | the run's first row id, `u64` | rank `u32`, row offset `u32`: 8 bytes |
//! | `idx_posg_…`, `idx_ospg_…` | the run's first row id, `u64` | four ranks in key order, row offset `u32`: 20 bytes |
//!
//! A quad run is sorted in its terms' order ([`TermInterner::cmp_quads`])
//! and read back through `rank_of`, each id mapped to its term's rank in
//! byte order. A rank keeps its term's order, so a run read back is sorted
//! by rank tuple, the order the merge compares. An index run is sorted by
//! key, then row id; its row ids are offsets from the run's first, which a
//! `u32` holds because a run holds at most `u32::MAX` entries numbered
//! consecutively (one per quad, in row order).

use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::interner::TermInterner;
use super::spill::spill_io_error;
use crate::error::{Result, VortexRdfError};
use crate::store::schema::RowId;

/// Bytes of a quad record: four `u32` term ids.
pub(crate) const QUAD_RECORD_BYTES: usize = 16;
/// Bytes of an index run's header: its first row id, a `u64`.
pub(crate) const RUN_HEADER_BYTES: usize = 8;
/// Bytes of a reference-index record: a rank and a row offset.
pub(crate) const REF_RECORD_BYTES: usize = 8;
/// Bytes of a copy-index record: four ranks and a row offset.
pub(crate) const COPY_RECORD_BYTES: usize = 20;

/// Bytes of a row offset inside an index record.
const ROW_OFFSET_BYTES: usize = 4;

/// The little-endian `u32` at the start of `bytes`.
fn le_u32(bytes: &[u8]) -> u32 {
    let mut word = [0u8; 4];
    word.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(word)
}

/// Four `u32`s, little-endian, into the first 16 bytes of `out`.
fn write_u32s(values: &[u32; 4], out: &mut [u8]) {
    let (slots, _) = out.as_chunks_mut::<4>();
    for (value, slot) in values.iter().zip(slots) {
        *slot = value.to_le_bytes();
    }
}

/// The four little-endian `u32`s at the start of `bytes`.
fn read_u32s(bytes: &[u8]) -> [u32; 4] {
    [
        le_u32(&bytes[0..]),
        le_u32(&bytes[4..]),
        le_u32(&bytes[8..]),
        le_u32(&bytes[12..]),
    ]
}

/// Sequential writer of one run file. Its failures, the final flush's
/// included, name the directory the file is in ([`spill_io_error`]).
pub(crate) struct RecordWriter {
    writer: BufWriter<File>,
    /// The directory the run file is in, for error messages.
    dir: PathBuf,
}

impl RecordWriter {
    pub(crate) fn create(path: &Path) -> Result<Self> {
        let dir = path.parent().unwrap_or(path).to_path_buf();
        let file = File::create(path).map_err(|e| spill_io_error(&dir, "file creation", e))?;
        Ok(Self {
            writer: BufWriter::new(file),
            dir,
        })
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| spill_io_error(&self.dir, "write", e))
    }

    /// Flush the run: a write the buffer still held fails here, never
    /// silently on drop.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| spill_io_error(&self.dir, "write", e))
    }
}

/// Sequential reader of one run file's fixed-width records.
pub(crate) struct RecordReader {
    reader: BufReader<File>,
}

impl RecordReader {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    /// Fill `buf` with the next `buf.len()` bytes: `false` when the run ends
    /// before the first of them. A run that ends partway through is a corrupt
    /// spill file, a `Deserialization` error naming `what` was being read;
    /// any other read failure is filesystem I/O.
    pub(crate) fn read(&mut self, buf: &mut [u8], what: &str) -> Result<bool> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.reader.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(VortexRdfError::Io(e)),
            }
        }
        match filled {
            0 => Ok(false),
            n if n == buf.len() => Ok(true),
            _ => Err(VortexRdfError::Deserialization(format!(
                "Unexpected EOF while reading spill record {what}"
            ))),
        }
    }
}

/// One sorted run of a merge, read in order.
pub(crate) trait RunRead {
    type Item: Ord;

    /// The run's next item, or `None` once it is drained.
    fn next(&mut self) -> Result<Option<Self::Item>>;
}

/// Streams items in global sorted order: a K-way merge over sorted runs.
pub(crate) struct RunMerger<R: RunRead> {
    runs: Vec<R>,
    /// Primed with each run's head; empty while a single run is read
    /// straight through.
    heap: BinaryHeap<Head<R::Item>>,
    /// Whether an item present more than once comes out once.
    distinct: bool,
}

impl<R: RunRead> RunMerger<R> {
    /// Merge `runs`, each already sorted. With `distinct`, an item that
    /// appears more than once (in several runs, or repeated inside one) comes
    /// out once.
    pub(crate) fn new(mut runs: Vec<R>, distinct: bool) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        // A lone run reads straight through, unless its repeats must be
        // dropped: that takes the heap, which sees the next item of a run
        // before anything is emitted.
        if runs.len() > 1 || distinct {
            for (run_idx, run) in runs.iter_mut().enumerate() {
                if let Some(item) = run.next()? {
                    heap.push(Head { item, run_idx });
                }
            }
        }
        Ok(Self {
            runs,
            heap,
            distinct,
        })
    }

    /// The next item in global order, or `None` once every run is drained.
    pub(crate) fn next(&mut self) -> Result<Option<R::Item>> {
        if self.runs.len() == 1 && !self.distinct {
            return self.runs[0].next();
        }
        let Some(Head { item, run_idx }) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(run_idx)?;
        if self.distinct {
            // Equal items are adjacent in the merged order, so every other
            // copy of `item` is the head of its run: at the top of the heap.
            while self.heap.peek().is_some_and(|top| top.item == item) {
                if let Some(Head { run_idx, .. }) = self.heap.pop() {
                    self.refill(run_idx)?;
                }
            }
        }
        Ok(Some(item))
    }

    /// Put run `run_idx`'s next item, if it has one, back on the heap.
    fn refill(&mut self, run_idx: usize) -> Result<()> {
        if let Some(item) = self.runs[run_idx].next()? {
            self.heap.push(Head { item, run_idx });
        }
        Ok(())
    }

    /// Pull up to `n` items off the merge (fewer at the end of the data).
    pub(crate) fn next_batch(&mut self, n: usize) -> Result<Vec<R::Item>> {
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

/// A run's head in the merge heap; ordered by item, then run index, both
/// reversed so `BinaryHeap` pops the minimum.
struct Head<T> {
    item: T,
    run_idx: usize,
}

impl<T: Ord> Eq for Head<T> {}
impl<T: Ord> PartialEq for Head<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item == other.item && self.run_idx == other.run_idx
    }
}
impl<T: Ord> Ord for Head<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .item
            .cmp(&self.item)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl<T: Ord> PartialOrd for Head<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// External sort of a build's quads as term ids: buffers up to `capacity`
/// quads, and spills each full buffer sorted in its terms' order, each quad
/// once, as `quads_run_N.bin`.
pub(crate) struct QuadSpiller {
    dir: PathBuf,
    capacity: usize,
    buf: Vec<[u32; 4]>,
    run_paths: Vec<PathBuf>,
}

impl QuadSpiller {
    /// Runs of up to `capacity` quads (at least one) under `dir`.
    pub(crate) fn new(dir: &Path, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            dir: dir.to_path_buf(),
            capacity,
            buf: Vec::with_capacity(capacity.min(4096)),
            run_paths: Vec::new(),
        }
    }

    /// Add one quad's ids, spilling the buffer first when it is full; `terms`
    /// is the interner the ids came from.
    pub(crate) fn push(&mut self, ids: [u32; 4], terms: &TermInterner) -> Result<()> {
        // Spill only when the next quad would not fit, so a dataset of exactly
        // `capacity` quads stays a single in-memory run.
        if self.buf.len() == self.capacity {
            self.flush_run(terms)?;
        }
        self.buf.push(ids);
        Ok(())
    }

    /// The buffer in its terms' order, each quad once: equal quads have equal
    /// ids, so they are adjacent once sorted.
    fn sort_buf(&mut self, terms: &TermInterner) {
        self.buf.sort_unstable_by(|a, b| terms.cmp_quads(a, b));
        self.buf.dedup();
    }

    fn flush_run(&mut self, terms: &TermInterner) -> Result<()> {
        self.sort_buf(terms);
        let path = self
            .dir
            .join(format!("quads_run_{}.bin", self.run_paths.len()));
        let mut writer = RecordWriter::create(&path)?;
        let mut record = [0u8; QUAD_RECORD_BYTES];
        for ids in &self.buf {
            write_u32s(ids, &mut record);
            writer.write(&record)?;
        }
        writer.finish()?;
        log::debug!(
            "[QuadSpiller] Wrote sorted run {} ({} quads)",
            self.run_paths.len(),
            self.buf.len()
        );
        self.run_paths.push(path);
        self.buf.clear();
        Ok(())
    }

    /// The runs, the last buffer spilled too; when nothing spilled, the
    /// sorted buffer is the single in-memory run.
    pub(crate) fn finish(mut self, terms: &TermInterner) -> Result<QuadRuns> {
        if self.run_paths.is_empty() {
            self.sort_buf(terms);
            return Ok(QuadRuns {
                memory: Some(Arc::from(std::mem::take(&mut self.buf))),
                paths: Vec::new(),
            });
        }
        if !self.buf.is_empty() {
            self.flush_run(terms)?;
        }
        Ok(QuadRuns {
            memory: None,
            paths: self.run_paths,
        })
    }
}

/// A build's quad runs: the files a [`QuadSpiller`] wrote, or its single
/// in-memory run. A merge over them can be opened any number of times.
pub(crate) struct QuadRuns {
    memory: Option<Arc<[[u32; 4]]>>,
    paths: Vec<PathBuf>,
}

impl QuadRuns {
    pub(crate) fn run_count(&self) -> usize {
        if self.memory.is_some() {
            1
        } else {
            self.paths.len()
        }
    }

    /// A merge of the runs yielding each distinct quad once, in global order,
    /// as the ranks `rank_of` maps its ids to.
    pub(crate) fn merger(&self, rank_of: &Arc<[u32]>) -> Result<RunMerger<QuadRunReader>> {
        let runs = match &self.memory {
            Some(ids) => vec![QuadRunReader {
                source: QuadSource::Memory {
                    ids: Arc::clone(ids),
                    at: 0,
                },
                rank_of: Arc::clone(rank_of),
            }],
            None => self
                .paths
                .iter()
                .map(|path| {
                    Ok(QuadRunReader {
                        source: QuadSource::File(RecordReader::open(path)?),
                        rank_of: Arc::clone(rank_of),
                    })
                })
                .collect::<Result<_>>()?,
        };
        RunMerger::new(runs, true)
    }
}

/// One quad run read back, each id mapped to its rank.
pub(crate) struct QuadRunReader {
    source: QuadSource,
    rank_of: Arc<[u32]>,
}

enum QuadSource {
    Memory { ids: Arc<[[u32; 4]]>, at: usize },
    File(RecordReader),
}

impl QuadRunReader {
    /// The rank of the term `id` names; an id past the table (a corrupt
    /// run) is a `Deserialization` error.
    fn rank(&self, id: u32) -> Result<u32> {
        self.rank_of.get(id as usize).copied().ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "spill record names term {id}, past the {} terms",
                self.rank_of.len()
            ))
        })
    }
}

impl RunRead for QuadRunReader {
    type Item = [u32; 4];

    fn next(&mut self) -> Result<Option<[u32; 4]>> {
        let ids = match &mut self.source {
            QuadSource::Memory { ids, at } => {
                let Some(&quad) = ids.get(*at) else {
                    return Ok(None);
                };
                *at += 1;
                quad
            }
            QuadSource::File(reader) => {
                let mut record = [0u8; QUAD_RECORD_BYTES];
                if !reader.read(&mut record, "quad")? {
                    return Ok(None);
                }
                read_u32s(&record)
            }
        };
        Ok(Some([
            self.rank(ids[0])?,
            self.rank(ids[1])?,
            self.rank(ids[2])?,
            self.rank(ids[3])?,
        ]))
    }
}

/// An index family's spilled entry: a key of term ranks, compared first,
/// and the primary row id.
pub(crate) trait IndexEntry: Copy + Ord {
    /// Bytes of the key in a record.
    const KEY_BYTES: usize;

    fn row_id(&self) -> RowId;

    /// The key, little-endian, into `out` (`KEY_BYTES` long).
    fn write_key(&self, out: &mut [u8]);

    /// The entry of `key` (`KEY_BYTES` long) at `row_id`.
    fn from_parts(key: &[u8], row_id: RowId) -> Self;
}

/// A reference-index entry: the value's rank and its row id.
pub(crate) type RefEntry = (u32, RowId);

/// A copy-index entry: the quad's four ranks in the family's key order and
/// its row id.
pub(crate) type CopyEntry = ([u32; 4], RowId);

impl IndexEntry for RefEntry {
    const KEY_BYTES: usize = REF_RECORD_BYTES - ROW_OFFSET_BYTES;

    fn row_id(&self) -> RowId {
        self.1
    }

    fn write_key(&self, out: &mut [u8]) {
        out[..4].copy_from_slice(&self.0.to_le_bytes());
    }

    fn from_parts(key: &[u8], row_id: RowId) -> Self {
        (le_u32(key), row_id)
    }
}

impl IndexEntry for CopyEntry {
    const KEY_BYTES: usize = COPY_RECORD_BYTES - ROW_OFFSET_BYTES;

    fn row_id(&self) -> RowId {
        self.1
    }

    fn write_key(&self, out: &mut [u8]) {
        write_u32s(&self.0, out);
    }

    fn from_parts(key: &[u8], row_id: RowId) -> Self {
        (read_u32s(key), row_id)
    }
}

/// External sort of one index family's entries: buffers up to `capacity`
/// entries and spills each full buffer sorted as `{name}_run_N.bin`, every
/// entry kept (their row ids differ).
pub(crate) struct EntrySpiller<E: IndexEntry> {
    dir: PathBuf,
    name: &'static str,
    capacity: usize,
    buf: Vec<E>,
    run_paths: Vec<PathBuf>,
}

impl<E: IndexEntry> EntrySpiller<E> {
    /// Runs of up to `capacity` entries (at least one) under `dir`. A
    /// capacity past `u32::MAX` is refused: a run's row offsets would not fit
    /// its records.
    pub(crate) fn new(dir: &Path, name: &'static str, capacity: usize) -> Result<Self> {
        if capacity > u32::MAX as usize {
            return Err(VortexRdfError::InvalidInput(format!(
                "an index spill run of {capacity} entries is past the {} a row offset reaches",
                u32::MAX
            )));
        }
        let capacity = capacity.max(1);
        Ok(Self {
            dir: dir.to_path_buf(),
            name,
            capacity,
            buf: Vec::with_capacity(capacity.min(4096)),
            run_paths: Vec::new(),
        })
    }

    pub(crate) fn push(&mut self, entry: E) -> Result<()> {
        if self.buf.len() == self.capacity {
            self.flush_run()?;
        }
        self.buf.push(entry);
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        self.buf.sort_unstable();
        let first = self.buf.iter().map(IndexEntry::row_id).min().unwrap_or(0);
        let path = self
            .dir
            .join(format!("{}_run_{}.bin", self.name, self.run_paths.len()));
        let mut writer = RecordWriter::create(&path)?;
        writer.write(&first.to_le_bytes())?;
        let mut record = vec![0u8; E::KEY_BYTES + ROW_OFFSET_BYTES];
        for entry in &self.buf {
            let offset = u32::try_from(entry.row_id() - first).map_err(|_| {
                VortexRdfError::Serialization(format!(
                    "index spill run {} of {} spans row ids from {first} to {}, past a u32 offset",
                    self.run_paths.len(),
                    self.name,
                    entry.row_id()
                ))
            })?;
            entry.write_key(&mut record[..E::KEY_BYTES]);
            record[E::KEY_BYTES..].copy_from_slice(&offset.to_le_bytes());
            writer.write(&record)?;
        }
        writer.finish()?;
        log::debug!(
            "[EntrySpiller] Wrote sorted run {} of {} ({} entries)",
            self.run_paths.len(),
            self.name,
            self.buf.len()
        );
        self.run_paths.push(path);
        self.buf.clear();
        Ok(())
    }

    /// Spill the last buffer and merge every run; when nothing spilled, the
    /// sorted buffer is the single in-memory run.
    pub(crate) fn into_merger(mut self) -> Result<RunMerger<EntryRunReader<E>>> {
        if self.run_paths.is_empty() {
            self.buf.sort_unstable();
            let run = EntryRunReader(EntrySource::Memory(
                std::mem::take(&mut self.buf).into_iter(),
            ));
            return RunMerger::new(vec![run], false);
        }
        if !self.buf.is_empty() {
            self.flush_run()?;
        }
        let runs = self
            .run_paths
            .iter()
            .map(|path| EntryRunReader::open(path))
            .collect::<Result<_>>()?;
        RunMerger::new(runs, false)
    }
}

/// One index run read back.
pub(crate) struct EntryRunReader<E>(EntrySource<E>);

enum EntrySource<E> {
    Memory(std::vec::IntoIter<E>),
    File {
        reader: RecordReader,
        /// The run's first row id, which its records' offsets count from.
        first: RowId,
        record: Vec<u8>,
    },
}

impl<E: IndexEntry> EntryRunReader<E> {
    /// The run at `path`, its header read.
    fn open(path: &Path) -> Result<Self> {
        let mut reader = RecordReader::open(path)?;
        let mut header = [0u8; RUN_HEADER_BYTES];
        if !reader.read(&mut header, "header")? {
            return Err(VortexRdfError::Deserialization(
                "Unexpected EOF while reading spill record header".to_owned(),
            ));
        }
        Ok(Self(EntrySource::File {
            reader,
            first: RowId::from_le_bytes(header),
            record: vec![0u8; E::KEY_BYTES + ROW_OFFSET_BYTES],
        }))
    }
}

impl<E: IndexEntry> RunRead for EntryRunReader<E> {
    type Item = E;

    fn next(&mut self) -> Result<Option<E>> {
        match &mut self.0 {
            EntrySource::Memory(entries) => Ok(entries.next()),
            EntrySource::File {
                reader,
                first,
                record,
            } => {
                if !reader.read(record, "entry")? {
                    return Ok(None);
                }
                let offset = le_u32(&record[E::KEY_BYTES..]);
                let row_id = first.checked_add(RowId::from(offset)).ok_or_else(|| {
                    VortexRdfError::Deserialization(format!(
                        "spill record's row offset {offset} past row id {first} overflows"
                    ))
                })?;
                Ok(Some(E::from_parts(&record[..E::KEY_BYTES], row_id)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::error::VortexRdfError;
    use crate::store::RawQuad;
    use crate::store::builders::interner::{FrozenTerms, TermInterner};
    use crate::store::builders::spill::TempRunsGuard;

    /// Twelve distinct quads: every subject differs, objects include a
    /// non-ASCII literal, graphs alternate with the default graph.
    fn distinct_quads() -> Vec<RawQuad> {
        (0..12u32)
            .map(|i| RawQuad {
                s: format!("<http://e.org/s{:02}>", (i * 7) % 12),
                p: format!("<http://e.org/p{}>", i % 3),
                o: if i % 4 == 0 {
                    "\"é\"".to_owned()
                } else {
                    format!("\"o{}\"", i % 4)
                },
                g: if i % 2 == 0 {
                    String::new()
                } else {
                    "<http://e.org/g>".to_owned()
                },
            })
            .collect()
    }

    /// The distinct quads in reverse, with eight repeats spread through them:
    /// 20 pushes, some repeats inside one run of 3 and some across runs.
    fn pushes() -> Vec<RawQuad> {
        let distinct = distinct_quads();
        let mut out: Vec<RawQuad> = distinct.iter().rev().cloned().collect();
        for i in [3, 0, 11, 5, 3, 7, 0, 9] {
            let at = (i * 5) % out.len();
            out.insert(at, distinct[i].clone());
        }
        out
    }

    /// `quads` interned and spilled in runs of `capacity` under `dir`.
    fn spill(dir: &Path, quads: &[RawQuad], capacity: usize) -> (QuadRuns, FrozenTerms) {
        let mut terms = TermInterner::new();
        let mut spiller = QuadSpiller::new(dir, capacity);
        for quad in quads {
            let ids = terms.intern_quad(quad.clone()).unwrap();
            spiller.push(ids, &terms).unwrap();
        }
        let runs = spiller.finish(&terms).unwrap();
        (runs, terms.finish())
    }

    /// A quad's four terms, comparable and printable (`RawQuad` is neither
    /// `Debug` nor meant to be).
    type Row = (String, String, String, String);

    /// Rank quads as the terms they name.
    fn decode(frozen: &FrozenTerms, ranks: &[[u32; 4]]) -> Vec<Row> {
        let term = |rank: u32| frozen.sorted[rank as usize].to_string();
        ranks
            .iter()
            .map(|&[s, p, o, g]| (term(s), term(p), term(o), term(g)))
            .collect()
    }

    /// `quads` sorted with each once, as rows: `RawQuad`'s order is its
    /// fields' tuple order.
    fn sorted_distinct(quads: Vec<RawQuad>) -> Vec<Row> {
        quads
            .into_iter()
            .map(|q| (q.s, q.p, q.o, q.g))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn file_len(dir: &Path, name: &str) -> u64 {
        std::fs::metadata(dir.join(name)).unwrap().len()
    }

    #[test]
    fn quad_runs_merge_in_rank_order_and_drop_repeats() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let (runs, frozen) = spill(guard.path(), &pushes(), 3);
        assert!(runs.run_count() > 1, "{} runs", runs.run_count());
        let rank_of: Arc<[u32]> = frozen.rank_of.clone().into();
        let merged = runs
            .merger(&rank_of)
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        assert_eq!(decode(&frozen, &merged), sorted_distinct(distinct_quads()));
    }

    #[test]
    fn quad_runs_open_twice_yield_the_same_sequence() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let (runs, frozen) = spill(guard.path(), &pushes(), 3);
        let rank_of: Arc<[u32]> = frozen.rank_of.into();
        let first = runs
            .merger(&rank_of)
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        let second = runs
            .merger(&rank_of)
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        assert_eq!(first.len(), 12);
        assert_eq!(first, second);
    }

    #[test]
    fn a_dataset_within_one_run_never_touches_the_disk() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let quads = &pushes()[..10];
        let (runs, frozen) = spill(guard.path(), quads, 100);
        assert_eq!(runs.run_count(), 1);
        assert_eq!(std::fs::read_dir(guard.path()).unwrap().count(), 0);
        let rank_of: Arc<[u32]> = frozen.rank_of.clone().into();
        let merged = runs
            .merger(&rank_of)
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        assert_eq!(decode(&frozen, &merged), sorted_distinct(quads.to_vec()));
    }

    #[test]
    fn quad_run_files_hold_sixteen_bytes_per_quad() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let (runs, _) = spill(guard.path(), &distinct_quads()[..10], 3);
        assert_eq!(runs.run_count(), 4);
        let lens: Vec<u64> = (0..4)
            .map(|n| file_len(guard.path(), &format!("quads_run_{n}.bin")))
            .collect();
        assert_eq!(lens, [48, 48, 48, 16]);
    }

    #[test]
    fn quad_run_id_past_the_table_is_a_deserialization_error() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let (runs, frozen) = spill(guard.path(), &distinct_quads(), 3);
        let short: Arc<[u32]> = frozen.rank_of[..2].to_vec().into();
        let read = runs
            .merger(&short)
            .and_then(|mut merger| merger.next_batch(usize::MAX));
        assert!(
            matches!(read, Err(VortexRdfError::Deserialization(_))),
            "{read:?}"
        );
    }

    #[test]
    fn entry_runs_round_trip_row_ids_past_u32() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let base = u64::from(u32::MAX) + 10;

        let refs: Vec<RefEntry> = (0..10u64)
            .map(|i| (((i * 7) % 10) as u32, base + i))
            .collect();
        let mut spiller = EntrySpiller::<RefEntry>::new(guard.path(), "idx_o", 3).unwrap();
        for entry in &refs {
            spiller.push(*entry).unwrap();
        }
        let merged = spiller
            .into_merger()
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        let mut want = refs.clone();
        want.sort_unstable();
        assert_eq!(merged, want);
        let lens: Vec<u64> = (0..4)
            .map(|n| file_len(guard.path(), &format!("idx_o_run_{n}.bin")))
            .collect();
        assert_eq!(lens, [32, 32, 32, 16]);

        let copies: Vec<CopyEntry> = (0..10u64)
            .map(|i| {
                let key = [
                    ((i * 3) % 10) as u32,
                    (i % 2) as u32,
                    ((i * 7) % 10) as u32,
                    0,
                ];
                (key, base + i)
            })
            .collect();
        let mut spiller = EntrySpiller::<CopyEntry>::new(guard.path(), "idx_posg", 3).unwrap();
        for entry in &copies {
            spiller.push(*entry).unwrap();
        }
        let merged = spiller
            .into_merger()
            .unwrap()
            .next_batch(usize::MAX)
            .unwrap();
        let mut want = copies.clone();
        want.sort_unstable();
        assert_eq!(merged, want);
        let lens: Vec<u64> = (0..4)
            .map(|n| file_len(guard.path(), &format!("idx_posg_run_{n}.bin")))
            .collect();
        assert_eq!(lens, [68, 68, 68, 28]);
    }

    #[test]
    fn a_cut_off_header_or_record_is_a_deserialization_error() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let entries: Vec<RefEntry> = (0..10u64).map(|i| ((i % 4) as u32, i)).collect();
        // Runs 0..2 are written by the pushes; the last at `into_merger`.
        for (name, keep) in [("idx_h", 4u64), ("idx_r", 8 + 3)] {
            let mut spiller = EntrySpiller::<RefEntry>::new(guard.path(), name, 3).unwrap();
            for entry in &entries {
                spiller.push(*entry).unwrap();
            }
            let run = std::fs::OpenOptions::new()
                .write(true)
                .open(guard.path().join(format!("{name}_run_1.bin")))
                .unwrap();
            run.set_len(keep).unwrap();
            let read = spiller
                .into_merger()
                .and_then(|mut merger| merger.next_batch(usize::MAX));
            assert!(
                matches!(read, Err(VortexRdfError::Deserialization(_))),
                "{name}: {read:?}"
            );
        }

        let mut terms = TermInterner::new();
        let mut spiller = QuadSpiller::new(guard.path(), 3);
        for quad in distinct_quads() {
            let ids = terms.intern_quad(quad).unwrap();
            spiller.push(ids, &terms).unwrap();
        }
        let run = std::fs::OpenOptions::new()
            .write(true)
            .open(guard.path().join("quads_run_0.bin"))
            .unwrap();
        run.set_len(10).unwrap();
        let runs = spiller.finish(&terms).unwrap();
        let rank_of: Arc<[u32]> = terms.finish().rank_of.into();
        let read = runs
            .merger(&rank_of)
            .and_then(|mut merger| merger.next_batch(usize::MAX));
        assert!(
            matches!(read, Err(VortexRdfError::Deserialization(_))),
            "quads: {read:?}"
        );
    }

    #[test]
    fn an_entry_spiller_refuses_a_capacity_past_u32() {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let made = EntrySpiller::<RefEntry>::new(guard.path(), "idx_o", u32::MAX as usize + 1);
        assert!(
            matches!(made, Err(VortexRdfError::InvalidInput(_))),
            "a run of more entries than a u32 offset reaches is refused"
        );
    }

    /// A run smaller than the writer's buffer reaches the device only at the
    /// final flush, which `finish` reports (the buffered writer's drop would
    /// swallow it).
    #[cfg(target_os = "linux")]
    #[test]
    fn record_writer_reports_a_full_volume_at_finish() {
        let mut writer = RecordWriter::create(Path::new("/dev/full")).unwrap();
        writer.write(&[0u8; QUAD_RECORD_BYTES]).unwrap();
        let error = writer.finish().unwrap_err();
        assert!(
            matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::StorageFull),
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
