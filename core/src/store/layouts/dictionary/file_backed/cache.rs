//! The cache of the windows a [`FileBackedDict`](super::FileBackedDict)
//! rebuilds: one per open file, bounded by a byte budget
//! ([`WINDOW_CACHE_BYTES`]), the least recently used window evicted first.

use std::collections::HashMap;
use std::sync::Arc;

use super::super::storage::TermChunk;

/// The charge one open file's window cache may hold: its windows' bytes
/// ([`TermChunk::heap_bytes`]) plus [`WINDOW_ENTRY_OVERHEAD`] each, the least
/// recently used evicted first. A 65,536-term FSST window is charged about
/// 260 KiB (4 bytes of code offsets per term, plus the overhead), so about 15
/// fit. Over a mapped file (every `from_file` store) a window's code bytes are
/// slices of the map, so the charge bounds the heap the cache holds.
pub(super) const WINDOW_CACHE_BYTES: usize = 4 << 20;

/// What each cached window is charged beyond its chunk's bytes: its symbol
/// table, array, `Arc` and map entry. It also caps the windows a budget
/// admits when they are tiny, at `WINDOW_CACHE_BYTES / WINDOW_ENTRY_OVERHEAD`
/// (1,024).
pub(in crate::store::layouts::dictionary) const WINDOW_ENTRY_OVERHEAD: usize = 4 << 10;

/// The rebuilt windows of one file, by window index, bounded by their charge
/// (their chunks' bytes plus [`WINDOW_ENTRY_OVERHEAD`] each). A lookup
/// refreshes a window's recency; an insert evicts the least recently used
/// windows until the new one fits, and a window charged more than the whole
/// budget is not kept. An evicted window a read still holds lives until that
/// read ends.
pub(super) struct WindowCache {
    budget: usize,
    /// The cached windows' total charge.
    pub(super) bytes: usize,
    /// The last recency stamp handed out.
    clock: u64,
    pub(super) windows: HashMap<usize, CachedWindow>,
}

/// A cached window, its charge and the stamp of its last use.
pub(super) struct CachedWindow {
    chunk: Arc<TermChunk>,
    bytes: usize,
    last_use: u64,
}

impl WindowCache {
    /// An empty cache of `budget` bytes.
    pub(super) fn new(budget: usize) -> Self {
        Self {
            budget,
            bytes: 0,
            clock: 0,
            windows: HashMap::new(),
        }
    }

    /// The most the cached windows may be charged.
    pub(super) fn budget(&self) -> usize {
        self.budget
    }

    /// What a window holding `chunk` is charged: its bytes
    /// ([`TermChunk::heap_bytes`]) plus [`WINDOW_ENTRY_OVERHEAD`].
    pub(super) fn charge(chunk: &TermChunk) -> usize {
        chunk.heap_bytes().saturating_add(WINDOW_ENTRY_OVERHEAD)
    }

    /// Window `w`'s chunk, marked the most recently used.
    pub(super) fn get(&mut self, w: usize) -> Option<Arc<TermChunk>> {
        let cached = self.windows.get_mut(&w)?;
        self.clock += 1;
        cached.last_use = self.clock;
        Some(Arc::clone(&cached.chunk))
    }

    /// Keep `chunk` as window `w`'s entry and return the chunk to use: the
    /// entry already there when another task rebuilt the window first, else
    /// `chunk` itself (kept unless its charge exceeds the budget).
    pub(super) fn insert(&mut self, w: usize, chunk: Arc<TermChunk>) -> Arc<TermChunk> {
        if let Some(kept) = self.get(w) {
            return kept;
        }
        let bytes = Self::charge(&chunk);
        if bytes > self.budget {
            return chunk;
        }
        while self.bytes + bytes > self.budget {
            let Some(oldest) = self
                .windows
                .iter()
                .min_by_key(|(_, cached)| cached.last_use)
                .map(|(&w, _)| w)
            else {
                break;
            };
            if let Some(evicted) = self.windows.remove(&oldest) {
                self.bytes -= evicted.bytes;
            }
        }
        self.clock += 1;
        self.bytes += bytes;
        self.windows.insert(
            w,
            CachedWindow {
                chunk: Arc::clone(&chunk),
                bytes,
                last_use: self.clock,
            },
        );
        chunk
    }
}

#[cfg(test)]
mod tests {
    use super::super::FileBackedDict;
    use super::super::fixtures::{windowed_handle, windowed_native};
    use super::*;
    use crate::store::schema::TermCode;
    use vortex_array::arrays::VarBinViewArray;

    /// What one 100-term FSST window costs the cache: its code offsets, one
    /// `u32` per term plus one, and the per-entry overhead.
    const WINDOW_BYTES: usize = 101 * size_of::<u32>() + WINDOW_ENTRY_OVERHEAD;

    /// Probe an interior term of window `w` of a handle over 100-term
    /// windows.
    async fn touch(fbd: &FileBackedDict, terms: &[String], w: usize) {
        let i = w * 100 + 37;
        assert_eq!(
            fbd.encode_exact(&terms[i]).await.unwrap(),
            Some(i as TermCode)
        );
    }

    /// The cache holds at most its budget and evicts the least recently used
    /// window: with room for two windows, touching windows 0, 1 and 2 evicts
    /// 0, a hit on 1 refreshes it, and 0 is then a miss that evicts 2.
    #[tokio::test]
    async fn the_window_cache_evicts_the_least_recently_used_within_its_budget() {
        let (native, _, terms) = windowed_native(600, 100).await;
        let budget = 2 * WINDOW_BYTES;
        let fbd = FileBackedDict::open_with_budget(&native, budget)
            .await
            .unwrap()
            .unwrap();
        // (window touched, windows rebuilt by it, windows cached after it)
        let steps: [(usize, usize, &[usize]); 6] = [
            (0, 1, &[0]),
            (1, 1, &[0, 1]),
            (2, 1, &[1, 2]),
            (1, 0, &[1, 2]),
            (0, 1, &[0, 1]),
            (2, 1, &[0, 2]),
        ];
        for (w, rebuilds, cached) in steps {
            let before = fbd.debug_windows_rebuilt();
            touch(&fbd, &terms, w).await;
            assert_eq!(fbd.debug_windows_rebuilt() - before, rebuilds, "window {w}");
            assert_eq!(fbd.debug_cached_windows(), cached, "after window {w}");
            assert!(fbd.debug_cached_bytes() <= budget, "after window {w}");
        }
    }

    /// A window charged more than the whole budget — a budget one byte under
    /// a window's charge, or a budget of zero — is used but not kept: every
    /// read rebuilds it and answers correctly, and nothing is cached.
    #[tokio::test]
    async fn a_window_past_the_budget_is_used_but_not_kept() {
        let (native, _, terms) = windowed_native(600, 100).await;
        for budget in [WINDOW_BYTES - 1, 0] {
            let fbd = FileBackedDict::open_with_budget(&native, budget)
                .await
                .unwrap()
                .unwrap();
            for _ in 0..3 {
                touch(&fbd, &terms, 2).await;
            }
            assert_eq!(fbd.debug_windows_rebuilt(), 3, "budget {budget}");
            let got = fbd.decode_many(&[105, 250]).await.unwrap();
            assert_eq!(&*got[0], terms[105].as_str());
            assert_eq!(&*got[1], terms[250].as_str());
            assert_eq!(fbd.debug_windows_rebuilt(), 5, "budget {budget}");
            assert!(fbd.debug_cached_windows().is_empty(), "budget {budget}");
            assert_eq!(fbd.debug_cached_bytes(), 0, "budget {budget}");
        }
    }

    /// A window charged exactly the budget fits and is kept; the next one
    /// evicts it.
    #[tokio::test]
    async fn a_window_charged_exactly_the_budget_is_kept() {
        let (native, _, terms) = windowed_native(600, 100).await;
        let fbd = FileBackedDict::open_with_budget(&native, WINDOW_BYTES)
            .await
            .unwrap()
            .unwrap();
        touch(&fbd, &terms, 0).await;
        assert_eq!(fbd.debug_cached_windows(), [0]);
        assert_eq!(fbd.debug_cached_bytes(), WINDOW_BYTES);
        touch(&fbd, &terms, 0).await;
        assert_eq!(fbd.debug_windows_rebuilt(), 1);
        touch(&fbd, &terms, 1).await;
        assert_eq!(fbd.debug_cached_windows(), [1]);
        assert_eq!(fbd.debug_cached_bytes(), WINDOW_BYTES);
    }

    /// A panic while the cache's lock is held leaves the cache usable: its
    /// entries are whole, so the next lookup and insert take the poisoned lock
    /// over.
    #[tokio::test]
    async fn a_poisoned_cache_lock_is_recovered() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        touch(&fbd, &terms, 0).await;
        let poisoner = fbd.clone();
        let result = std::thread::spawn(move || {
            let _held = poisoner.0.cache.lock().unwrap();
            panic!("poison the cache");
        })
        .join();
        assert!(result.is_err());
        assert!(fbd.0.cache.is_poisoned());
        touch(&fbd, &terms, 0).await;
        touch(&fbd, &terms, 1).await;
        assert_eq!(fbd.debug_windows_rebuilt(), 2);
        assert_eq!(fbd.debug_cached_windows(), [0, 1]);
    }

    /// A window two tasks missed together is kept once, and both use the one
    /// kept chunk.
    #[test]
    fn a_window_missed_twice_together_is_kept_once() {
        let chunk = || {
            Arc::new(TermChunk::Canonical(VarBinViewArray::from_iter_str([
                "<a>", "<b>", "<c>",
            ])))
        };
        let mut cache = WindowCache::new(WINDOW_CACHE_BYTES);
        let first = cache.insert(3, chunk());
        let second = cache.insert(3, chunk());
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.windows.len(), 1);
        assert_eq!(cache.bytes, first.heap_bytes() + WINDOW_ENTRY_OVERHEAD);
    }

    /// A cached window costs its chunk's bytes (an FSST window its code
    /// offsets, a canonical chunk its array) and the per-entry overhead; the
    /// cache totals them.
    #[tokio::test]
    async fn cached_windows_are_charged_their_bytes_and_an_overhead() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        touch(&fbd, &terms, 0).await;
        assert_eq!(fbd.debug_cached_bytes(), WINDOW_BYTES);
        touch(&fbd, &terms, 4).await;
        assert_eq!(fbd.debug_cached_bytes(), 2 * WINDOW_BYTES);
        fbd.debug_clear_cache();
        assert_eq!(fbd.debug_cached_bytes(), 0);

        let plain = VarBinViewArray::from_iter_str(["<a>", "<b>", "<c>"]);
        let nbytes = usize::try_from(plain.nbytes()).unwrap();
        assert_eq!(TermChunk::Canonical(plain).heap_bytes(), nbytes);
    }

    /// Windows whose offsets cost far less than the per-entry overhead are
    /// bounded by the overhead: a budget admits `budget / overhead` of them,
    /// not one per few bytes.
    #[tokio::test]
    async fn tiny_windows_are_bounded_by_the_entry_overhead() {
        // 60 windows of four terms: 20 bytes of offsets each.
        let (native, _, terms) = windowed_native(240, 4).await;
        let budget = 3 * WINDOW_ENTRY_OVERHEAD;
        let fbd = FileBackedDict::open_with_budget(&native, budget)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fbd.debug_window_count(), 60);
        // The offsets of all sixty fit in the budget many times over, so
        // only the overhead can bound the cache.
        assert!(60 * 5 * size_of::<u32>() < budget / 2);
        for w in 0..60 {
            let i = w * 4 + 1;
            assert_eq!(
                fbd.encode_exact(&terms[i]).await.unwrap(),
                Some(i as TermCode)
            );
            assert!(fbd.debug_cached_bytes() <= budget, "after window {w}");
        }
        let cached = fbd.debug_cached_windows();
        assert!(!cached.is_empty() && cached.len() <= 3, "{cached:?}");
    }

    /// `encode_many` over terms that cycle through every window, under a
    /// two-window budget the windows would turn over one lookup at a time,
    /// returns the resident dictionary's codes, reading each window once.
    #[tokio::test]
    async fn encode_many_under_a_two_window_budget_matches_the_resident_dictionary() {
        let (native, d, terms) = windowed_native(600, 100).await;
        let budget = 2 * WINDOW_BYTES;
        let fbd = FileBackedDict::open_with_budget(&native, budget)
            .await
            .unwrap()
            .unwrap();
        // 101 is coprime to 600: a permutation whose neighbours sit in
        // different windows.
        let mut probes: Vec<String> = (0..600).map(|i| terms[i * 101 % 600].clone()).collect();
        probes.extend(["<http://zzz>", "<http://example.org/term/0250x>"].map(String::from));
        let refs: Vec<&str> = probes.iter().map(String::as_str).collect();

        assert_eq!(
            fbd.encode_many(&refs).await.unwrap(),
            d.encode_many(&refs).unwrap()
        );
        assert_eq!(fbd.debug_windows_rebuilt(), fbd.debug_window_count());
        assert!(fbd.debug_cached_bytes() <= budget);
    }
}
