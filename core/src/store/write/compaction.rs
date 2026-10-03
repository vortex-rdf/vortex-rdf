//! Compaction: folding the append tail into the base, reclaiming tombstoned
//! rows, and the auto-compaction policy that decides when `add_quads` does it.

use crate::error::Result;
#[cfg(feature = "file-io")]
use crate::error::VortexRdfError;
use crate::store::QuadsSource;
#[cfg(feature = "file-io")]
use crate::store::RawQuad;
use crate::store::builders::DEFAULT_CHUNK_ROWS;
use crate::store::indexes::{Indexes, unique_indexes};
#[cfg(feature = "file-io")]
use crate::store::layouts::LayoutStrategy;

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    // ── compaction ───────────────────────────────────────────────────────────

    /// Compact the store keeping its current index set: fold the tail into
    /// the base, reclaim tombstoned rows, re-sort by (s, p, o, g) and rebuild
    /// the indexes. A file-backed store rewrites its source file and stays
    /// file-backed.
    pub async fn compact(&self) -> Result<Self> {
        self.compact_with_indexes(self.indexes.clone()).await
    }

    /// Gather this view's live rows into a standalone owning store, re-sorted
    /// by (s, p, o, g), with `indexes` rebuilt over them: the rows are
    /// renumbered, the tail folded in (re-encoded against a fresh dictionary
    /// under the Dictionary layout) and every sorted-order fast path
    /// restored. Pass the current [`indexes`](Self::indexes) to keep them, an
    /// empty set for a sort-only compaction, or another set to re-index. An
    /// owning file-backed store rewrites its own source file via a temp file
    /// and an atomic rename and is reopened with the same residency budget; a
    /// derived file view rebuilds in memory, never over the shared file.
    pub async fn compact_with_indexes(&self, indexes: Indexes) -> Result<Self> {
        let unique = unique_indexes(&indexes);
        let mut raws = self.live_raw_quads().await?;
        // Only an owner rewrites the file: a derived view's rows are a subset
        // of the shared file.
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
        raws.sort_unstable();
        Self::from_raw_quads(&raws, self.layout.strategy(), unique, true)
    }

    /// Write `raws` over `path` through the streaming builder (a sibling temp
    /// file, then an atomic rename) and reopen the file with
    /// `dict_max_resident_bytes`.
    #[cfg(feature = "file-io")]
    async fn stream_compacted_to_file(
        raws: Vec<RawQuad>,
        strategy: LayoutStrategy,
        indexes: Indexes,
        path: &std::path::Path,
        dict_max_resident_bytes: u64,
    ) -> Result<Self> {
        // A sibling keeps the rename on one filesystem; the uuid avoids an
        // earlier interrupted compaction's leftover.
        let tmp = path.with_extension(format!("compact-{}.tmp", uuid::Uuid::new_v4()));
        let stream = futures::stream::iter(raws.into_iter().map(Ok::<_, VortexRdfError>));
        // Spill runs land beside the store file; `VORTEX_RDF_SPILL_DIR` still
        // outranks that.
        let write = async {
            let built = crate::store::builders::sorted_stream::build_chunk_stream(
                Box::new(stream),
                strategy,
                indexes,
                DEFAULT_CHUNK_ROWS,
                path.parent(),
            )
            .await?;
            let writer = crate::io::write::create_store_file(&tmp).await?;
            crate::io::write::built_stream_to_vortex_writer(built, writer).await
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
        // Reopen with the caller's residency budget.
        Self::from_file_with_dict_residency(path, dict_max_resident_bytes).await
    }

    /// Whether `add_quads` folds the tail into the base now; a file-backed
    /// store's append past the threshold rewrites its file.
    pub(in crate::store) fn should_auto_compact(&self) -> bool {
        let (base_rows, tail) = match (&self.quads, &self.tail) {
            (QuadsSource::InMemory { base, .. }, Some(tail)) => (base.len(), tail),
            #[cfg(feature = "file-io")]
            (QuadsSource::File { file, .. }, Some(tail)) => (file.row_count() as usize, tail),
            _ => return false,
        };
        tail_needs_compaction(base_rows, tail.rows.len())
    }
}

/// Auto-compaction floor: below this many tail rows, never compact.
const AUTO_COMPACT_TAIL_FLOOR: usize = 4_096;

/// Auto-compaction ratio: compact once the tail reaches base/10.
const AUTO_COMPACT_BASE_RATIO: usize = 10;

/// Auto-compaction cap: compact once the tail reaches one builder chunk,
/// whatever the base size.
const AUTO_COMPACT_TAIL_CAP: usize = DEFAULT_CHUNK_ROWS;

/// The auto-compaction decision: ratio with a floor, or the absolute cap,
/// whichever fires first.
fn tail_needs_compaction(base_rows: usize, tail_rows: usize) -> bool {
    tail_rows >= AUTO_COMPACT_TAIL_CAP
        || tail_rows >= AUTO_COMPACT_TAIL_FLOOR.max(base_rows / AUTO_COMPACT_BASE_RATIO)
}

#[cfg(test)]
mod tests {
    use super::tail_needs_compaction;

    #[test]
    fn auto_compaction_thresholds() {
        // Floor: a tail below 4_096 rows never triggers.
        assert!(!tail_needs_compaction(10, 4_095));
        assert!(tail_needs_compaction(10, 4_096));

        // Ratio: past the floor, a tenth of the base is the trigger.
        assert!(!tail_needs_compaction(100_000, 9_999));
        assert!(tail_needs_compaction(100_000, 10_000));
        assert!(!tail_needs_compaction(50_000, 4_999));
        assert!(tail_needs_compaction(50_000, 5_000));

        // Cap: one builder chunk's worth compacts whatever the base size.
        assert!(!tail_needs_compaction(100_000_000, 99_999));
        assert!(tail_needs_compaction(100_000_000, 100_000));
    }
}
