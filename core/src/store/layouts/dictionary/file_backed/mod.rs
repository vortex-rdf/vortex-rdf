//! A term dictionary read in place from its file's `dictionary` child, the
//! file-backed arm of [`DictAccess`](super::handles::DictAccess). The handle
//! holds the child's flat leaves (one per FSST window) and each window's
//! first and last term, from the child's exact zone maps or else from the
//! leaves. A call reads only the windows its terms are in: over a mapped
//! file, a window is its segment sliced out of the map with the FSST array
//! rebuilt over it, and only the terms asked for are decompressed. Rebuilt
//! windows are kept in a byte-bounded, least-recently-used cache
//! ([`WINDOW_CACHE_BYTES`](cache::WINDOW_CACHE_BYTES) per open file). A
//! child no window search can address is lifted resident instead
//! ([`store::persist::open`](crate::store::persist::open)).
//!
//! [`open`] reads the child's shape and window bounds when the handle opens,
//! [`cache`] keeps the rebuilt windows, [`held`] holds the windows a chunk
//! decode reads in place, and this module holds the handle and its lookups.

mod cache;
mod held;
mod open;

#[cfg(test)]
pub(super) use self::cache::WINDOW_ENTRY_OVERHEAD;
pub(super) use self::held::HeldWindows;

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use futures::{StreamExt as _, TryStreamExt as _};
use vortex_array::VortexSessionExecute;
use vortex_buffer::Buffer;
use vortex_layout::layouts::flat::FlatLayout;
use vortex_layout::segments::SegmentSource;
use vortex_rdf_encoded_search::fetch_flat;

use crate::error::{Result, VortexRdfError};
use crate::io::read::available_parallelism;
use crate::session::VORTEX_SESSION;
use crate::store::schema::TermCode;

use self::cache::WindowCache;
use super::predicates::{KindRanges, TermPredicate};
use super::storage::{TermChunk, chunk_of};
use super::term_dict::{
    CodeSpace, TermDictionary, check_candidates, check_code, find_by, kind_verdicts,
    lower_bound_by, prefix_range_from, prefix_successor, split_verdicts, tolerant_fallback,
};

/// One flat leaf of the term column (one FSST window as written): the rank of
/// its first term, its row count (never 0), and its first and last terms.
struct Window {
    layout: FlatLayout,
    start: usize,
    rows: usize,
    first: Box<[u8]>,
    last: Box<[u8]>,
}

/// What a [`FileBackedDict`] holds: the child's leaves and window bounds, and
/// the cache of rebuilt windows.
struct Windows {
    /// The identity of this dictionary (see
    /// [`DictReader::dictionary_id`](crate::DictReader::dictionary_id)): one
    /// per open, shared by every clone of the handle.
    id: u64,
    /// The child's layout reader, for the transient whole-column lift.
    reader: vortex_layout::LayoutReaderRef,
    windows: Vec<Window>,
    /// `starts[i]` = `windows[i].start`, ascending (for [`chunk_of`]).
    starts: Vec<usize>,
    code_space: CodeSpace,
    /// Where the leaves' segments are read from: over a mapped file, the map.
    source: Arc<dyn SegmentSource>,
    /// The kind ranges, computed on first use.
    kinds: OnceLock<KindRanges>,
    /// The rebuilt windows. The lock is held for a lookup or an insert, never
    /// across an `.await`.
    cache: Mutex<WindowCache>,
    /// Whether a leaf's segment read has had to wait: never over a mapped
    /// file or a buffer, whose reads complete when first polled. Once set, a
    /// chunk decode no longer reads windows in place
    /// ([`FileBackedDict::hold_windows`]).
    reads_wait: AtomicBool,
    /// Test hook: windows rebuilt from their segments since open.
    #[cfg(test)]
    rebuilt: std::sync::atomic::AtomicUsize,
    /// Test hook: windows asked for since open, hits and misses alike.
    #[cfg(test)]
    requested: std::sync::atomic::AtomicUsize,
    /// Test hook: whole-dictionary lifts since open.
    #[cfg(test)]
    lifted: std::sync::atomic::AtomicUsize,
}

impl Windows {
    /// The window holding `needle`'s lower bound, with its index: the first
    /// whose last term is not below `needle`; `None` when every term is below
    /// it.
    fn locate(&self, needle: &[u8]) -> Option<(usize, &Window)> {
        let w = self
            .windows
            .partition_point(|window| &*window.last < needle);
        self.windows.get(w).map(|window| (w, window))
    }

    /// The cache's guard. Every entry is complete when it is inserted, so a
    /// lock poisoned by a panic elsewhere guards a consistent cache and is
    /// taken over.
    fn cache(&self) -> MutexGuard<'_, WindowCache> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Window `w`'s chunk if the cache holds it, the request counted.
    fn cached(&self, w: usize) -> Option<Arc<TermChunk>> {
        #[cfg(test)]
        self.requested
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.cache().get(w)
    }

    /// Window `w`'s chunk: the cached one, else its leaf rebuilt over its
    /// segment, adopted ([`TermChunk::from_wire`]) and cached. The rebuild
    /// runs with the lock released, so two tasks missing one window both
    /// rebuild it and the cache keeps the first insert.
    async fn window_chunk(&self, w: usize) -> Result<Arc<TermChunk>> {
        if let Some(chunk) = self.cached(w) {
            return Ok(chunk);
        }
        let leaf = fetch_flat(&self.windows[w].layout, &self.source, &VORTEX_SESSION).await?;
        #[cfg(test)]
        self.rebuilt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let chunk = TermChunk::from_wire(leaf, &mut VORTEX_SESSION.create_execution_ctx())?;
        Ok(self.cache().insert(w, Arc::new(chunk)))
    }

    /// [`window_chunk`](Self::window_chunk) when it needs no waiting: `None`
    /// when the leaf's segment read does not complete on its first poll,
    /// which sets [`reads_wait`](Self::reads_wait) and drops the read
    /// unfinished. Once reads wait, only a cached window is answered.
    fn window_chunk_now(&self, w: usize) -> Option<Result<Arc<TermChunk>>> {
        use futures::FutureExt as _;
        if self.reads_wait.load(std::sync::atomic::Ordering::Relaxed) {
            return self.cached(w).map(Ok);
        }
        let chunk = self.window_chunk(w).now_or_never();
        if chunk.is_none() {
            self.reads_wait
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        chunk
    }
}

/// A term dictionary left in its layout child: term → code probes and code →
/// term decodes read the windows they need from the (mapped) file, through a
/// bounded cache of rebuilt windows. Terms are read as UTF-8 unchecked, so
/// the child must be a Utf8 column of a file this crate wrote or the caller
/// trusts. Cloning is an `Arc` bump.
#[derive(Clone)]
pub(crate) struct FileBackedDict(Arc<Windows>);

impl FileBackedDict {
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }

    pub(crate) fn len(&self) -> usize {
        self.0.code_space.len()
    }

    /// The code of `term` exactly as spelled: the window whose bounds enclose
    /// it, then a binary search of that window's inside, read through the
    /// cache. A window's first or last term, and a term between two windows,
    /// read no window.
    pub(crate) async fn encode_exact(&self, term: &str) -> Result<Option<TermCode>> {
        let inner = &*self.0;
        let needle = term.as_bytes();
        let Some((w, window)) = inner.locate(needle) else {
            return Ok(None);
        };
        let code_at = |local: usize| inner.code_space.code_at(window.start + local);
        match needle.cmp(&*window.first) {
            Ordering::Less => return Ok(None),
            Ordering::Equal => return Ok(Some(code_at(0))),
            Ordering::Greater => {}
        }
        if needle == &*window.last {
            return Ok(Some(code_at(window.rows - 1)));
        }
        // first < needle < last: a hit lies strictly inside.
        let chunk = inner.window_chunk(w).await?;
        let mut cursor = chunk.cursor();
        Ok(find_by(1..window.rows - 1, |local| {
            cursor.bytes_at(local).cmp(needle)
        })
        .map(code_at))
    }

    /// The code of the first term not below `needle` in byte order; one past
    /// the last code when every term is below it. Reads at most the one
    /// window the bound is inside.
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<TermCode> {
        let inner = &*self.0;
        let Some((w, window)) = inner.locate(needle) else {
            return Ok(inner.code_space.code_end());
        };
        if needle <= &*window.first {
            return Ok(inner.code_space.code_at(window.start));
        }
        // first < needle <= last: the bound lies inside, past the first term.
        let chunk = inner.window_chunk(w).await?;
        let mut cursor = chunk.cursor();
        let local = lower_bound_by(1..window.rows - 1, |local| cursor.bytes_at(local) < needle);
        Ok(inner.code_space.code_at(window.start + local))
    }

    /// Visit the terms of `codes`, which must ascend strictly inside the
    /// dictionary ([`check_candidates`], checked before any window is read),
    /// in order: `visit` gets each code's position in `codes` and its term,
    /// read unchecked as UTF-8 (see [`FileBackedDict`]). Each window holding
    /// any of them is read once, through one cursor that decompresses only
    /// the asked rows.
    async fn visit_terms(
        &self,
        codes: &[TermCode],
        mut visit: impl FnMut(usize, &str),
    ) -> Result<()> {
        // Each window's run is a partition point, so a list out of order
        // would put a code before its window's start (wrapping
        // `rank - window.start`) or make a run of nothing (a loop that never
        // advances).
        let inner = &*self.0;
        check_candidates(codes, inner.code_space)?;
        // Every candidate is inside the dictionary (checked above), so its
        // offset from the first code is a rank below the term count: exact
        // as a `usize`.
        let base = inner.code_space.base();
        let rank = |code: TermCode| (code - base) as usize;
        let mut at = 0;
        while at < codes.len() {
            let (w, _) = chunk_of(&inner.starts, rank(codes[at]));
            let window = &inner.windows[w];
            let end = window.start + window.rows;
            let run = codes[at..].partition_point(|&code| rank(code) < end);
            if run == 0 {
                // Not reachable past the check above; were it, an error is
                // better than a loop that never advances.
                return Err(VortexRdfError::InvalidOperation(format!(
                    "code {} lies outside window {w}",
                    codes[at]
                )));
            }
            let chunk = inner.window_chunk(w).await?;
            let mut cursor = chunk.cursor();
            for (i, &code) in codes[at..at + run].iter().enumerate() {
                visit(at + i, cursor.str_at(rank(code) - window.start));
            }
            at += run;
        }
        Ok(())
    }

    /// The terms of `codes`, which must ascend strictly (else an
    /// `InvalidInput` error); a code past the dictionary is a
    /// `Deserialization` error. Reads only the windows that hold them.
    pub(crate) async fn decode_many(&self, codes: &[TermCode]) -> Result<Vec<Arc<str>>> {
        let Some(&max) = codes.last() else {
            return Ok(Vec::new());
        };
        check_code(max, self.0.code_space)?;
        let mut terms = Vec::with_capacity(codes.len());
        self.visit_terms(codes, |_, term| terms.push(Arc::from(term)))
            .await?;
        Ok(terms)
    }

    /// The terms of `codes`, in any order and with repeats, each handed to
    /// `visit(position, term)` once per position (`None` for an out-of-range
    /// code), borrowed: no string is built per term. Out-of-range positions
    /// are visited first, then the rest in code order, each distinct code
    /// read once ([`visit_terms`](Self::visit_terms)).
    pub(crate) async fn decode_many_with(
        &self,
        codes: &[TermCode],
        mut visit: impl FnMut(usize, Option<&str>),
    ) -> Result<()> {
        let mut sorted: Vec<(TermCode, usize)> = Vec::with_capacity(codes.len());
        for (i, &code) in codes.iter().enumerate() {
            if self.0.code_space.rank_of(code).is_some() {
                sorted.push((code, i));
            } else {
                visit(i, None);
            }
        }
        sorted.sort_unstable();
        let groups: Vec<&[(TermCode, usize)]> = sorted.chunk_by(|a, b| a.0 == b.0).collect();
        let distinct: Vec<TermCode> = groups.iter().map(|group| group[0].0).collect();
        self.visit_terms(&distinct, |j, term| {
            for &(_, i) in groups[j] {
                visit(i, Some(term));
            }
        })
        .await
    }

    /// The code of `term`, tolerant of spelling as `TermDictionary::encode`.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<TermCode>> {
        if let Some(code) = self.encode_exact(term).await? {
            return Ok(Some(code));
        }
        match tolerant_fallback(term)? {
            Some(canonical) => self.encode_exact(&canonical).await,
            None => Ok(None),
        }
    }

    /// [`encode`](Self::encode) over a batch, in order, the lookups overlapped.
    pub(crate) async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<TermCode>>> {
        futures::stream::iter(terms.iter().map(|term| self.encode(term)))
            .buffered(available_parallelism().max(4))
            .try_collect()
            .await
    }

    /// The code range of the terms spelled with `prefix`, as
    /// `TermDictionary::prefix_range`: two lower bounds.
    pub(crate) async fn prefix_range(&self, prefix: &str) -> Result<Range<TermCode>> {
        let lo = self.lower_bound(prefix.as_bytes()).await?;
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => Some(self.lower_bound(&successor).await?),
            None => None,
        };
        Ok(prefix_range_from(lo, hi, self.0.code_space.code_end()))
    }

    /// The kind ranges, computed once per handle (a few window searches;
    /// the default graph's `""` is window 0's first bound).
    pub(crate) async fn kind_ranges(&self) -> Result<KindRanges> {
        if let Some(kinds) = self.0.kinds.get() {
            return Ok(kinds.clone());
        }
        let code_space = self.0.code_space;
        let first_is_empty = self
            .0
            .windows
            .first()
            .is_some_and(|window| window.first.is_empty());
        let kinds = KindRanges::new(
            code_space.base(),
            first_is_empty,
            self.prefix_range("\"").await?,
            self.prefix_range("<").await?,
            self.prefix_range("_:").await?,
            code_space.code_end(),
        );
        Ok(self.0.kinds.get_or_init(|| kinds).clone())
    }

    /// `TermDictionary::filter_codes` over the file, under its precondition:
    /// the candidates their kind does not decide are read through
    /// [`visit_terms`](Self::visit_terms), so only the windows holding them
    /// are read.
    pub(crate) async fn filter_codes(
        &self,
        predicate: &TermPredicate,
        codes: &[TermCode],
    ) -> Result<(Buffer<TermCode>, Buffer<TermCode>)> {
        check_candidates(codes, self.0.code_space)?;
        let kinds = self.kind_ranges().await?;
        let (mut verdicts, read_at) = kind_verdicts(predicate, &kinds, codes);
        let read: Vec<TermCode> = read_at.iter().map(|&at| codes[at]).collect();
        self.visit_terms(&read, |j, spelling| {
            verdicts[read_at[j]] = predicate.eval(spelling);
        })
        .await?;
        Ok(split_verdicts(codes, &verdicts))
    }

    /// The whole dictionary lifted resident with one scan of the term
    /// column: the transient read behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::handles::DictAccess::ensure_resident
    pub(crate) async fn lift_resident(&self) -> Result<TermDictionary> {
        #[cfg(test)]
        self.0
            .lifted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        TermDictionary::from_child_reader(self.0.reader.clone()).await
    }

    /// Whether a window read has had to wait ([`Windows::reads_wait`]).
    #[cfg(test)]
    pub(crate) fn debug_reads_wait(&self) -> bool {
        self.0.reads_wait.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Windows rebuilt from their segments since the handle opened.
    #[cfg(test)]
    pub(crate) fn debug_windows_rebuilt(&self) -> usize {
        self.0.rebuilt.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The number of windows (leaves) of the term column.
    #[cfg(test)]
    pub(crate) fn debug_window_count(&self) -> usize {
        self.0.windows.len()
    }

    /// The rank of each window's first term, ascending.
    #[cfg(test)]
    pub(crate) fn debug_window_starts(&self) -> Vec<usize> {
        self.0.starts.clone()
    }

    /// Windows asked for since the handle opened, from the cache or not.
    #[cfg(test)]
    pub(crate) fn debug_windows_requested(&self) -> usize {
        self.0.requested.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whole-dictionary lifts ([`lift_resident`](Self::lift_resident)) since
    /// the handle opened.
    #[cfg(test)]
    pub(crate) fn debug_lifts(&self) -> usize {
        self.0.lifted.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// What the window cache's windows are charged.
    #[cfg(test)]
    pub(crate) fn debug_cached_bytes(&self) -> usize {
        self.0.cache().bytes
    }

    /// The indices of the cached windows, ascending.
    #[cfg(test)]
    pub(crate) fn debug_cached_windows(&self) -> Vec<usize> {
        let mut cached: Vec<usize> = self.0.cache().windows.keys().copied().collect();
        cached.sort_unstable();
        cached
    }

    /// Drop every cached window.
    #[cfg(test)]
    pub(crate) fn debug_clear_cache(&self) {
        let mut cache = self.0.cache();
        cache.windows.clear();
        cache.bytes = 0;
    }
}

/// Dictionary children written from known terms and opened, for the tests of
/// this module, [`cache`] and [`open`].
#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests;
