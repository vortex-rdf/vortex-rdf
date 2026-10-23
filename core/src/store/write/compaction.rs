//! Compaction: folding the append tail into the base, reclaiming tombstoned
//! rows, and the auto-compaction policy that decides when `add_quads` does it.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::RawQuad;
use crate::store::builders::{DEFAULT_CHUNK_ROWS, build_parts_from_raws};
use crate::store::indexes::Indexes;

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    // ── compaction ───────────────────────────────────────────────────────────

    /// Compact the store keeping its current index set: fold the tail into
    /// the base, reclaim tombstoned rows, re-sort by (s, p, o, g) and rebuild
    /// the indexes ([`compact_with_indexes`] has the mechanics). `add_quads`
    /// calls it once the tail outgrows the auto-compaction thresholds; an
    /// owning file-backed store rewrites its source file atomically and stays
    /// file-backed either way. If that file cannot be written (a read-only
    /// store), `add_quads` keeps the batch in the tail instead, and only an
    /// explicit call reports the refusal.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn compact(&self) -> Result<Self> {
        self.compact_with_indexes(self.indexes.clone()).await
    }

    /// Gather this view's live rows into a standalone, owning store, re-sorted
    /// by (s, p, o, g) with each quad once, and `indexes` rebuilt over them:
    /// the store's compaction step, an O(n log n) rebuild that reclaims
    /// tombstoned rows, absorbs the tail and restores every sorted-order fast
    /// path. Gathering renumbers the rows `0..n`, so the old components'
    /// `rid`s cannot carry across: the rows are rebuilt in SPOG order (the
    /// tail re-encoded against a fresh dictionary under the Dictionary layout)
    /// and the requested indexes over the new order. Pass the current
    /// [`indexes`](Self::indexes) to keep them (or call [`compact`]), an empty
    /// set for a sort-only compaction, or another set to re-index.
    ///
    /// An owning file-backed store stays file-backed: the compacted rows are
    /// written over its own source file (a temp file, then an atomic rename)
    /// and the store is reopened from it; a derived file view rebuilds in
    /// memory, never over the shared file. A source file the process cannot
    /// write is never replaced: the compaction fails with the refusal
    /// (`PermissionDenied`, or a read-only filesystem's) before it has
    /// gathered or built anything. On Windows the rename is refused while the
    /// file is mapped, by this store included, so there a file-backed store
    /// cannot compact over its own path: the compaction fails with the I/O
    /// error and the file is left as it was. An in-memory store returns the
    /// in-memory rebuild.
    ///
    /// [`compact`]: Self::compact
    pub async fn compact_with_indexes(&self, indexes: Indexes) -> Result<Self> {
        // Only an owner rewrites its file, streaming the live rows through the
        // sorted builder over it (quads, index children and dictionary through
        // the writer's shared sink). A derived view's rows are a subset of the
        // shared file: renaming them over `path` would destroy the rest for
        // every other reader, and `owned()` promises an independent copy,
        // which the in-memory rebuild below gives.
        #[cfg(feature = "file-io")]
        if self.is_owner()
            && let Some(FileBase { file, .. }) = self.quads.file()
            && let Some(path) = file.path()
        {
            return self
                .stream_compacted_to_file(indexes, path, file.identity())
                .await;
        }
        let raws = self.sorted_live_raw_quads().await?;
        Self::from_built(build_parts_from_raws(
            &raws,
            self.layout.strategy(),
            &indexes,
        )?)
    }

    /// Every live quad this view covers in (s, p, o, g) order, each once: the
    /// rows every rebuild starts from (a compaction, a mutated store's
    /// `to_serializable_parts`). The base rows come first, in the base's order
    /// ([`live_raw_quads`](Self::live_raw_quads)); over a sorted base the tail
    /// sorted alone leaves two sorted runs, which the stable `slice::sort`
    /// (driftsort, which detects runs) merges in linear time. Only a base that
    /// never carried the sorted stamp pays a full sort, which heals it.
    pub(in crate::store) async fn sorted_live_raw_quads(&self) -> Result<Vec<RawQuad>> {
        let (mut raws, base_rows) = self.live_raw_quads().await?;
        if !self.quads.subject_sorted() {
            raws.sort_unstable();
        } else if base_rows < raws.len() {
            raws[base_rows..].sort_unstable();
            raws.sort();
        }
        // The rebuilt store holds each quad once, whatever the rows it was
        // gathered from carried: equal quads are adjacent in (s, p, o, g) order.
        raws.dedup();
        Ok(raws)
    }

    /// Write this view's compacted rows over `path` through the streaming
    /// builder and reopen the file, so a file-backed store stays file-backed.
    /// The rows go to a temporary sibling file renamed over `path`
    /// ([`PendingStore`], which writes every store file): never written in
    /// place, atomic against a process crash but not durable against power
    /// loss (nothing is `fsync`ed first), and `path` untouched on any failure
    /// before the rename.
    ///
    /// The writer is prepared *first*: a source file the process cannot
    /// write, a directory at the path, or a directory that takes no new file
    /// is refused (`PermissionDenied` and the like) before the rows are
    /// gathered, sorted or built, so a compaction that cannot happen costs a
    /// few stats and one failed open.
    ///
    /// [`PendingStore`]: crate::io::write::PendingStore
    #[cfg(feature = "file-io")]
    async fn stream_compacted_to_file(
        &self,
        indexes: Indexes,
        path: &std::path::Path,
        opened: Option<crate::io::read::FileIdentity>,
    ) -> Result<Self> {
        let pending = crate::io::write::PendingStore::create_over(path, opened).await?;
        let raws = self.sorted_live_raw_quads().await?;
        let stream = futures::stream::iter(raws.into_iter().map(Ok::<_, VortexRdfError>));
        // The merge runs can reach dataset size: they go in the directory of
        // the writer's temp file, beside the file being replaced with links
        // followed (not `path`'s directory, as a store opened through a link
        // may live elsewhere), the volume the finished store is renamed
        // within. `VORTEX_RDF_SPILL_DIR` still outranks that.
        let built = crate::store::builders::sorted_stream::build_chunk_stream(
            Box::new(stream),
            self.layout.strategy(),
            indexes,
            crate::store::builders::BuildSizes::DEFAULT,
            pending.spill_site(),
        )
        .await?;
        pending
            .write(|writer| crate::io::write::built_stream_to_vortex_writer(built, writer))
            .await?;
        // Reopen mapped. This store's old mapping keeps the replaced file's
        // pages until it drops; Windows refuses the rename above while that
        // mapping lives, and compaction then fails with the I/O error.
        Self::from_file(path).await
    }

    /// Whether `add_quads` folds the tail into the base now, in memory or on
    /// file: a file-backed store's append past the thresholds rewrites its
    /// source file ([`compact`](Self::compact)).
    pub(in crate::store) fn should_auto_compact(&self) -> bool {
        self.tail
            .as_ref()
            .is_some_and(|tail| tail_needs_compaction(self.quads.base_len(), tail.rows.len()))
    }
}

/// Auto-compaction floor: below this many tail rows, never compact, so a
/// small store does not rebuild every few appends.
const AUTO_COMPACT_TAIL_FLOOR: usize = 4_096;

/// Auto-compaction ratio: compact once the tail reaches base/10. A ratio keeps
/// the rebuild cost amortized constant per appended row.
const AUTO_COMPACT_BASE_RATIO: usize = 10;

/// Auto-compaction cap: compact once the tail could fill a builder chunk,
/// whatever the base size. The tail is the one unindexed, unsorted region,
/// mask-scanned by every query, so past this size it would dominate
/// index-routed lookups on a large base.
const AUTO_COMPACT_TAIL_CAP: usize = DEFAULT_CHUNK_ROWS;

/// The auto-compaction decision (see `VortexRdfStore::add_quads`): ratio with
/// a floor, or the absolute cap, whichever fires first.
fn tail_needs_compaction(base_rows: usize, tail_rows: usize) -> bool {
    tail_rows >= AUTO_COMPACT_TAIL_CAP
        || tail_rows >= AUTO_COMPACT_TAIL_FLOOR.max(base_rows / AUTO_COMPACT_BASE_RATIO)
}

#[cfg(test)]
mod tests {
    use super::tail_needs_compaction;

    #[test]
    fn auto_compaction_thresholds() {
        // Floor: however small the base, a tail below 4_096 rows never
        // triggers, so small stores don't thrash.
        assert!(!tail_needs_compaction(10, 4_095));
        assert!(tail_needs_compaction(10, 4_096));

        // Ratio: past the floor, a tenth of the base is the trigger.
        assert!(!tail_needs_compaction(100_000, 9_999));
        assert!(tail_needs_compaction(100_000, 10_000));
        assert!(!tail_needs_compaction(50_000, 4_999));
        assert!(tail_needs_compaction(50_000, 5_000));

        // Cap: on a large base the ratio would tolerate a huge tail, but one
        // builder chunk's worth compacts regardless.
        assert!(!tail_needs_compaction(100_000_000, 99_999));
        assert!(tail_needs_compaction(100_000_000, 100_000));
    }
}
