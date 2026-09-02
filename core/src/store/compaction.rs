//! Compaction: folding the append tail into the base, reclaiming tombstoned
//! rows, and the auto-compaction policy that decides when `add_quads` does it.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
use crate::store::QuadsSource;
use crate::store::RawQuad;
use crate::store::builders::{DEFAULT_CHUNK_ROWS, build_parts_from_raws};
use crate::store::indexes::{Indexes, unique_indexes};
use crate::store::layouts::LayoutStrategy;

use super::VortexRdfStore;

impl VortexRdfStore {
    // ── compaction ───────────────────────────────────────────────────────────

    /// Compact the store, keeping its current index set: fold the appended
    /// tail into the base, reclaim tombstoned rows, re-sort by (s, p, o, g),
    /// and rebuild the indexes the store already carries.
    ///
    /// See [`compact_with_indexes`] for the mechanics and for choosing a
    /// different index set. `add_quads` calls this automatically when the tail
    /// outgrows the auto-compaction thresholds; a file-backed store rewrites
    /// its source file (atomically) and stays file-backed, whether compacted
    /// automatically or explicitly.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn compact(&self) -> Result<Self> {
        self.compact_with_indexes(self.indexes.clone()).await
    }

    /// Gather this view's live rows into a standalone, owning store, re-sorted
    /// by (s, p, o, g), with the given secondary indexes rebuilt over them.
    ///
    /// Physically gathering the rows renumbers them to a fresh `0..n`, so the
    /// source components' `rid` columns — which addressed the old base — cannot
    /// carry across. This variant turns that into an opportunity: the rows are
    /// rebuilt in SPOG order (restoring the subject binary-search fast path that
    /// a narrowed view forfeits, and folding any appended tail back into the
    /// base — re-encoded against a fresh term dictionary under the Dictionary
    /// layout) and the requested indexes are rebuilt over the new order. Pass
    /// the store's current [`indexes`](Self::indexes) to preserve them (or use
    /// [`compact`]), an empty set for a sort-only compaction, or a different set
    /// to re-index.
    ///
    /// This is the store's compaction step: it reclaims tombstoned rows,
    /// absorbs the tail, and restores every sorted-order fast path, at the
    /// cost of an O(n log n) rebuild.
    ///
    /// A file-backed store stays file-backed: the compacted rows are written
    /// back over its own source file (via a temp file and an atomic rename) and
    /// the store is reopened from it. An in-memory store returns the in-memory
    /// rebuild directly.
    ///
    /// [`compact`]: Self::compact
    pub async fn compact_with_indexes(&self, indexes: Indexes) -> Result<Self> {
        let unique = unique_indexes(&indexes);
        let mut raws = self.live_raw_quads().await?;
        raws.sort_unstable();
        // An OWNING file-backed store stays file-backed: stream the live rows
        // through the sorted builder straight over their own source file (no
        // materialized rebuild — quads, index children, and the dictionary
        // all flow through the writer's shared sink) and reopen it. A derived
        // view must never take this arm: its rows are a subset of the shared
        // file, and renaming them over `path` would destroy the data outside
        // the view for every other reader — `owned()` promises an independent
        // copy, which the in-memory rebuild below provides.
        #[cfg(feature = "file-io")]
        if self.is_owner()
            && let QuadsSource::File {
                path,
                dict_max_resident_bytes,
                ..
            } = &self.quads
        {
            return Self::stream_compacted_to_file(
                raws,
                self.layout.strategy(),
                unique,
                path,
                *dict_max_resident_bytes,
            )
            .await;
        }
        Self::from_raw_quads(&raws, self.layout.strategy(), unique, true)
    }

    /// Persist freshly-compacted rows over `path` through the streaming
    /// builder and reopen the file, so a file-backed store stays file-backed
    /// after compaction.
    ///
    /// The rows are written to a temporary sibling file and then atomically
    /// renamed over `path`. Overwriting the file in place would be unsafe
    /// while a reader still maps the original, and a crash mid-write must
    /// never leave the only on-disk copy half-written; the rename makes the
    /// swap atomic and leaves `path` untouched on any earlier failure.
    #[cfg(feature = "file-io")]
    async fn stream_compacted_to_file(
        raws: Vec<RawQuad>,
        strategy: LayoutStrategy,
        indexes: Indexes,
        path: &std::path::Path,
        dict_max_resident_bytes: u64,
    ) -> Result<Self> {
        // A sibling temp file keeps the rename on one filesystem (so it is
        // atomic); the uuid suffix avoids colliding with a temp left behind by
        // an earlier interrupted compaction.
        let tmp = path.with_extension(format!("compact-{}.tmp", uuid::Uuid::new_v4()));
        let stream = futures::stream::iter(raws.into_iter().map(Ok::<_, VortexRdfError>));
        // The sorted builder spills merge runs to disk, and compaction rewrites
        // the whole store, so those runs can reach dataset size. Point them at
        // the store file's own directory — the one volume known to fit the
        // data, the same placement as the sibling temp file above. The
        // `VORTEX_RDF_SPILL_DIR` override still outranks this default.
        let write = async {
            let built = crate::store::builders::sorted_stream::build_chunk_stream(
                Box::new(stream),
                strategy,
                indexes,
                DEFAULT_CHUNK_ROWS,
                path.parent(),
            )
            .await?;
            let writer = crate::io::ser::create_store_file(&tmp).await?;
            crate::io::ser::built_stream_to_vortex_writer(built, writer).await
        };
        if let Err(e) = write.await {
            // Don't leave a partial temp file behind on a write failure.
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        tokio::fs::rename(&tmp, path).await.map_err(|e| {
            VortexRdfError::Io(std::io::Error::new(
                e.kind(),
                format!("replace {path:?}: {e}"),
            ))
        })?;
        // Reopen with the caller's pinned residency budget, not the default.
        Self::from_file_with_dict_residency(path, dict_max_resident_bytes).await
    }

    /// Build a fresh owning in-memory store from raw quads under `strategy` —
    /// the shared back half of compaction (see [`build_parts_from_raws`]).
    /// `sorted` must be `true` only when `raws` is SPOG-sorted.
    fn from_raw_quads(
        raws: &[RawQuad],
        strategy: LayoutStrategy,
        indexes: Indexes,
        sorted: bool,
    ) -> Result<Self> {
        let (base, components, dict) = build_parts_from_raws(raws, strategy, &indexes, sorted)?;
        let layout = super::resolved_layout(dict, base.dtype())?;
        // Compress like every other construction — a compacted store carries
        // the same resident form a freshly built one does.
        let (base, components) = super::resident_built_parts(base, components)?;
        Self::assemble_resident(base, components, layout)
    }

    /// Whether `add_quads` should fold the tail into the base now.
    ///
    /// Both in-memory and file-backed bases auto-compact once the tail crosses
    /// the compaction thresholds. For a file-backed store this rewrites its
    /// source file in place (see [`compact`](Self::compact)) and keeps it
    /// file-backed — an append past the threshold performs a disk write.
    pub(super) fn should_auto_compact(&self) -> bool {
        let (base_rows, tail) = match (&self.quads, &self.tail) {
            (QuadsSource::InMemory { base, .. }, Some(tail)) => (base.len(), tail),
            #[cfg(feature = "file-io")]
            (QuadsSource::File { file, .. }, Some(tail)) => (file.row_count() as usize, tail),
            _ => return false,
        };
        tail_needs_compaction(base_rows, tail.rows.len())
    }
}

/// Auto-compaction floor: below this many tail rows, never compact — a small
/// store would otherwise pay a rebuild every few appends.
const AUTO_COMPACT_TAIL_FLOOR: usize = 4_096;

/// Auto-compaction ratio: compact when the tail reaches this fraction of the
/// base (tail ≥ base/10). A ratio keeps the rebuild cost amortized-constant
/// per appended row (the dynamic-array growth argument); 10% trades roughly
/// seven whole-store rewrites per doubling for a tail that stays small
/// relative to the base.
const AUTO_COMPACT_BASE_RATIO: usize = 10;

/// Auto-compaction cap: compact once the tail could fill a builder chunk,
/// however large the base is. The tail is the one unindexed, unsorted region —
/// every query mask-scans it and every append rebuilds it — so past this size
/// it dominates index-routed lookups on a large base, where the 10% ratio
/// alone would let it grow a hundred times bigger.
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
