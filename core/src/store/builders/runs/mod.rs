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
//! A build that spills nothing keeps its one run in memory, unsorted, and
//! sorts it once the build's terms are ranked, as rank tuples: integers
//! that order the quads as their terms do ([`SpilledQuads::ranked`]). A
//! spilled run is sorted in its terms' order before it is written (term by
//! term through [`TermInterner::cmp_terms`], or as ranks among its own
//! terms where they repeat) and read back through `rank_of`, each id mapped
//! to its term's rank in byte order. A rank keeps its term's order, so a
//! run read back is sorted by rank tuple, the order the merge compares. An
//! index run is sorted by key, then row id; its row ids are offsets from the
//! run's first, which a `u32` holds because a run holds at most `u32::MAX`
//! entries numbered consecutively (one per quad, in row order). The runs'
//! tests are in `tests.rs`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::interner::TermInterner;
use super::spill::{spill_io_error, spill_open_error};
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

/// A build's id → rank table ([`FrozenTerms::rank_of`]), shared by every
/// reader of its quad runs, the table moved in rather than copied.
///
/// [`FrozenTerms::rank_of`]: super::interner::FrozenTerms::rank_of
pub(crate) type RankTable = Arc<Vec<u32>>;

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
    /// The run at `path`; a run that cannot be opened names its directory
    /// ([`spill_open_error`]).
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|e| spill_open_error(path.parent().unwrap_or(path), e))?;
        Ok(Self {
            reader: BufReader::new(file),
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
    type Item: Ord + Copy;

    /// The run's next item, or `None` once it is drained.
    fn next(&mut self) -> Result<Option<Self::Item>>;
}

/// Streams items in global sorted order: a K-way merge over sorted runs, an
/// item that appears more than once (in several runs, or repeated inside
/// one) coming out once.
pub(crate) struct RunMerger<R: RunRead> {
    runs: Vec<R>,
    /// Primed with each run's head; empty while a single run is read
    /// straight through.
    heap: BinaryHeap<Head<R::Item>>,
    /// The item last emitted. Equal items are adjacent in the merged order,
    /// so a repeat is the item just emitted.
    last: Option<R::Item>,
}

impl<R: RunRead> RunMerger<R> {
    /// Merge `runs`, each already sorted. A lone run reads straight
    /// through; several fill the heap with their heads.
    pub(crate) fn new(mut runs: Vec<R>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        if runs.len() > 1 {
            for (run_idx, run) in runs.iter_mut().enumerate() {
                if let Some(item) = run.next()? {
                    heap.push(Head { item, run_idx });
                }
            }
        }
        Ok(Self {
            runs,
            heap,
            last: None,
        })
    }

    /// The next item in global order, a repeat of the last one skipped, or
    /// `None` once every run is drained.
    pub(crate) fn next(&mut self) -> Result<Option<R::Item>> {
        loop {
            let item = if self.runs.len() == 1 {
                match self.runs[0].next()? {
                    Some(item) => item,
                    None => return Ok(None),
                }
            } else {
                let Some(Head { item, run_idx }) = self.heap.pop() else {
                    return Ok(None);
                };
                self.refill(run_idx)?;
                item
            };
            if self.last != Some(item) {
                self.last = Some(item);
                return Ok(Some(item));
            }
        }
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
    /// Scratch for sorting a buffer, two `u32`s per interned term: the rank
    /// of each term the buffer holds among the buffer's terms, and the sort
    /// that last saw each term, so no slot is cleared between sorts.
    ranks: Vec<u32>,
    seen: Vec<u32>,
    sorts: u32,
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
            ranks: Vec::new(),
            seen: Vec::new(),
            sorts: 0,
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

    /// The buffer in its terms' order, each quad once, as a spilled run is
    /// written: before the build's terms are ranked. Where the buffer's terms
    /// repeat (at most one distinct term for two quads), they are compared
    /// once, to rank them among themselves, and the quads sort in place as
    /// tuples of those ranks, read back as ids; otherwise the quads are
    /// compared term by term, which reads a role's terms only where the
    /// roles before it tie, as a run of mostly distinct terms would have
    /// ranked them all. Equal quads (equal ids) end up adjacent.
    fn sort_buf(&mut self, terms: &TermInterner) {
        self.sorts = self.sorts.wrapping_add(1);
        if self.sorts == 0 {
            self.seen.fill(0);
            self.sorts = 1;
        }
        let sort = self.sorts;
        self.seen.resize(terms.len(), 0);
        self.ranks.resize(terms.len(), 0);
        let mut distinct: Vec<u32> = Vec::new();
        for &id in self.buf.iter().flatten() {
            let slot = &mut self.seen[id as usize];
            if *slot != sort {
                *slot = sort;
                distinct.push(id);
            }
        }
        if distinct.len().saturating_mul(2) > self.buf.len() {
            self.buf.sort_unstable_by(|a, b| {
                a.iter()
                    .zip(b)
                    .find(|(x, y)| x != y)
                    .map_or(Ordering::Equal, |(&x, &y)| terms.cmp_terms(x, y))
            });
            self.buf.dedup();
            return;
        }
        distinct.sort_unstable_by(|&a, &b| terms.cmp_terms(a, b));
        for (rank, &id) in distinct.iter().enumerate() {
            self.ranks[id as usize] = rank as u32;
        }
        for id in self.buf.iter_mut().flatten() {
            *id = self.ranks[*id as usize];
        }
        self.buf.sort_unstable();
        self.buf.dedup();
        for rank in self.buf.iter_mut().flatten() {
            *rank = distinct[*rank as usize];
        }
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
    /// buffer as it stands, the single in-memory run, sorted once the
    /// build's terms are ranked ([`SpilledQuads::ranked`]).
    pub(crate) fn finish(mut self, terms: &TermInterner) -> Result<SpilledQuads> {
        if self.run_paths.is_empty() {
            return Ok(SpilledQuads::Memory(std::mem::take(&mut self.buf)));
        }
        if !self.buf.is_empty() {
            self.flush_run(terms)?;
        }
        Ok(SpilledQuads::Spilled(self.run_paths))
    }
}

/// A build's quads once ingested, before its terms are ranked.
pub(crate) enum SpilledQuads {
    /// Nothing spilled: the spiller's buffer of ids, unsorted.
    Memory(Vec<[u32; 4]>),
    /// The sorted runs a [`QuadSpiller`] wrote.
    Spilled(Vec<PathBuf>),
}

impl SpilledQuads {
    /// How many runs: the files written, or the one buffer.
    pub(crate) fn run_count(&self) -> usize {
        match self {
            SpilledQuads::Memory(_) => 1,
            SpilledQuads::Spilled(paths) => paths.len(),
        }
    }

    /// The runs under the ranks `rank_of` gives the build's terms: an
    /// in-memory run's ids replaced by their ranks, then sorted and each quad
    /// kept once, integers alone compared, the terms' order being the ranks';
    /// spilled runs' ids mapped as they are read back.
    pub(crate) fn ranked(self, rank_of: RankTable) -> Result<QuadRuns> {
        Ok(match self {
            SpilledQuads::Memory(mut quads) => {
                for id in quads.iter_mut().flatten() {
                    *id = rank_in(&rank_of, *id)?;
                }
                quads.sort_unstable();
                quads.dedup();
                QuadRuns::Memory(Arc::from(quads))
            }
            SpilledQuads::Spilled(paths) => QuadRuns::Spilled { paths, rank_of },
        })
    }
}

/// A build's quad runs, read back as ranks. A merge over them can be opened
/// any number of times.
pub(crate) enum QuadRuns {
    /// A build that spilled nothing: its one run, held as ranks already.
    Memory(Arc<[[u32; 4]]>),
    /// The runs a [`QuadSpiller`] wrote, read back as the ranks `rank_of`
    /// maps their ids to.
    Spilled {
        paths: Vec<PathBuf>,
        rank_of: RankTable,
    },
}

impl QuadRuns {
    /// How many quads the in-memory run holds; `None` when the runs spilled.
    #[cfg(test)]
    pub(crate) fn memory_len(&self) -> Option<usize> {
        match self {
            QuadRuns::Memory(quads) => Some(quads.len()),
            QuadRuns::Spilled { .. } => None,
        }
    }

    /// How many runs: the files written, or the one in memory.
    #[cfg(test)]
    pub(crate) fn run_count(&self) -> usize {
        match self {
            QuadRuns::Memory(_) => 1,
            QuadRuns::Spilled { paths, .. } => paths.len(),
        }
    }

    /// A merge of the runs yielding each distinct quad once, in global order,
    /// as ranks.
    pub(crate) fn merger(&self) -> Result<RunMerger<QuadRunReader>> {
        let runs = match self {
            QuadRuns::Memory(ranks) => vec![QuadRunReader::Memory {
                ranks: Arc::clone(ranks),
                at: 0,
            }],
            QuadRuns::Spilled { paths, rank_of } => paths
                .iter()
                .map(|path| {
                    Ok(QuadRunReader::File {
                        reader: RecordReader::open(path)?,
                        rank_of: Arc::clone(rank_of),
                    })
                })
                .collect::<Result<_>>()?,
        };
        RunMerger::new(runs)
    }
}

/// One quad run read back as ranks.
pub(crate) enum QuadRunReader {
    /// The in-memory run, held as ranks already, read from `at` on.
    Memory { ranks: Arc<[[u32; 4]]>, at: usize },
    /// A spilled run, its ids mapped to their ranks as they are read.
    File {
        reader: RecordReader,
        rank_of: RankTable,
    },
}

/// The rank of the term `id` names; an id past the table (a corrupt run) is
/// a `Deserialization` error.
fn rank_in(rank_of: &[u32], id: u32) -> Result<u32> {
    rank_of.get(id as usize).copied().ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "spill record names term {id}, past the {} terms",
            rank_of.len()
        ))
    })
}

impl RunRead for QuadRunReader {
    type Item = [u32; 4];

    fn next(&mut self) -> Result<Option<[u32; 4]>> {
        match self {
            QuadRunReader::Memory { ranks, at } => {
                let Some(&quad) = ranks.get(*at) else {
                    return Ok(None);
                };
                *at += 1;
                Ok(Some(quad))
            }
            QuadRunReader::File { reader, rank_of } => {
                let mut record = [0u8; QUAD_RECORD_BYTES];
                if !reader.read(&mut record, "quad")? {
                    return Ok(None);
                }
                let ids = read_u32s(&record);
                Ok(Some([
                    rank_in(rank_of, ids[0])?,
                    rank_in(rank_of, ids[1])?,
                    rank_in(rank_of, ids[2])?,
                    rank_in(rank_of, ids[3])?,
                ]))
            }
        }
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
            return RunMerger::new(vec![run]);
        }
        if !self.buf.is_empty() {
            self.flush_run()?;
        }
        let runs = self
            .run_paths
            .iter()
            .map(|path| EntryRunReader::open(path))
            .collect::<Result<_>>()?;
        RunMerger::new(runs)
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
mod tests;
