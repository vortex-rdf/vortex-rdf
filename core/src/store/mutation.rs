//! Mutations: appends accrete in the tail and deletes tombstone, so the base
//! — its row ids, indexes, and file handle — is never rewritten in place.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::builders::build_struct_array;
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::selection::{RowSelection, gather_live, union_deleted};
use crate::store::{QuadsSource, Tail};

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::collections::HashSet;
use std::sync::Arc;

use vortex_array::arrays::ChunkedArray;
use vortex_array::{IntoArray, RecursiveCanonical, VortexSessionExecute};
#[cfg(feature = "file-io")]
use vortex_mask::Mask;

use super::VortexRdfStore;

impl VortexRdfStore {
    // ── mutations ─────────────────────────────────────────────────────────────

    /// Append a single quad — [`add_quads`] with a batch of one. Prefer the
    /// batch form when adding several: each call rebuilds the tail once.
    ///
    /// [`add_quads`]: Self::add_quads
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        self.add_quads([quad]).await
    }

    /// Append every quad not already present, per RDF/JS dataset (set)
    /// semantics: a quad equal to an existing one — or to an earlier quad in
    /// the same batch — is skipped.
    ///
    /// Appends land in the in-memory `Tail`, never the base, so the base —
    /// its row ids, secondary indexes, tombstones, or file handle — carries
    /// over untouched; queries run the base's fast paths plus a mask scan over
    /// the tail. This also makes Dictionary-layout appends possible: an
    /// appended term has no code in the sorted dictionary, so the tail stores
    /// terms as strings and patterns probe the base by code and the tail by
    /// string.
    ///
    /// Each append rebuilds the tail into one contiguous chunk (O(tail +
    /// batch) — hence batching), and each presence check is one fully-bound
    /// `match_pattern` — cheap where the store has a sorted subject column, an
    /// index, or file pruning; a scan per quad where it has none, in which
    /// case bulk-loading through the builders is the better tool.
    ///
    /// When the tail outgrows the auto-compaction thresholds — a tenth of the
    /// base (with a floor so small stores don't thrash) or a builder chunk's
    /// worth of rows, whichever comes first — the add that crossed the line
    /// finishes by folding the tail into the base ([`compact`]): occasional
    /// O(n log n) work, amortized constant per appended row. A file-backed
    /// store does this too, rewriting its source file in place and staying
    /// file-backed — so an append past the threshold performs a disk write
    /// (watch [`tail_len`](Self::tail_len)).
    ///
    /// [`compact`]: Self::compact
    pub async fn add_quads(&self, quads: impl IntoIterator<Item = Quad>) -> Result<Self> {
        self.ensure_owner("add_quads")?;

        let mut fresh: Vec<RawQuad> = Vec::new();
        let mut seen: HashSet<RawQuad> = HashSet::new();
        for quad in quads {
            let raw = RawQuad::from_quad(&quad);
            if seen.contains(&raw) || self.contains(&quad).await? {
                continue;
            }
            seen.insert(raw.clone());
            fresh.push(raw);
        }
        if fresh.is_empty() {
            return Ok(self.clone());
        }

        let fresh_rows =
            build_struct_array(&fresh, self.tail_layout().strategy(), &[], 0, false, false)?;
        let rows = match &self.tail {
            None => fresh_rows,
            // Append = accrete: the fresh rows join the tail as one more chunk
            // of a ChunkedArray accumulator, deferring the flatten. Flattening
            // on every add costs O(tail) per appended row (quadratic over an
            // addQuad loop); instead the accreted suffix is folded into the
            // flat prefix geometrically — when it rivals the prefix in rows —
            // plus a chunk-count bound so tail scans stay dense. Amortized
            // O(1) copies per appended row, the dynamic-array growth pattern.
            // Renumbering the old tail's ids on flatten is safe: views of the
            // pre-append store keep the old tail, and an owner's selections
            // are `All`.
            Some(tail) => {
                use vortex_array::arrays::Chunked;
                use vortex_array::arrays::chunked::ChunkedArrayExt;
                let old = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref(), None)?;
                let dtype = old.dtype().clone();
                let mut chunks = match old.clone().try_downcast::<Chunked>() {
                    Ok(ch) => ch.chunks(),
                    Err(_) => vec![old],
                };
                let flat_len = chunks[0].len();
                let accreted: usize =
                    chunks[1..].iter().map(|c| c.len()).sum::<usize>() + fresh_rows.len();
                chunks.push(fresh_rows);
                let n_chunks = chunks.len();
                let combined = ChunkedArray::try_new(chunks, dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array();
                if accreted >= flat_len.max(TAIL_FLATTEN_FLOOR) || n_chunks > TAIL_MAX_CHUNKS {
                    let mut ctx = VORTEX_SESSION.create_execution_ctx();
                    combined
                        .execute::<RecursiveCanonical>(&mut ctx)
                        .map_err(VortexRdfError::Vortex)?
                        .0
                        .into_array()
                } else {
                    combined
                }
            }
        };
        let appended = Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: self.quads.clone(),
            tail: Some(Tail {
                rows,
                selection: RowSelection::All,
                // Gathering above dropped any tombstoned tail rows already.
                deleted: None,
            }),
        };
        // Append-then-check: the append itself is policy-free, and the add
        // that pushes the tail over the thresholds pays for folding it into
        // the base — amortized-rare under the ratio trigger, exactly the
        // dynamic-array growth pattern.
        if appended.should_auto_compact() {
            return appended.compact().await;
        }
        Ok(appended)
    }

    /// Remove all quads matching the given quad exactly.
    pub async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        self.delete_matching(
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )
        .await
    }

    /// Remove every quad matching the pattern — the counterpart to
    /// [`match_pattern`], for when the rows a pattern selects should be dropped
    /// rather than read.
    ///
    /// The matched rows are tombstoned rather than rewritten away, so this
    /// costs a mask, not a copy of the surviving data, and the base's row ids —
    /// and with them any secondary index — stay valid across the delete.
    /// Tombstoned rows are only reclaimed by [`compact`], which is also how
    /// a store that has accumulated many deletes is compacted.
    ///
    /// Only a store that owns its rows can be mutated; call it on the store a
    /// view came from, or on `view.owned()`.
    ///
    /// [`match_pattern`]: Self::match_pattern
    /// [`compact`]: Self::compact
    pub async fn delete_matching(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        self.ensure_owner("delete_matching")?;

        // Reuse the matcher: which rows a pattern names is exactly the question
        // `match_pattern` answers, and the view it returns shares this store's
        // base (or file), so the doomed rows are already in base row ids.
        let doomed = self
            .match_pattern(subject, predicate, object, graph)
            .await?;

        // The tail tombstones exactly as the base does: the doomed view's
        // tail selection is already exact tail-local ids, so it folds into
        // the tail's own deleted mask the same way.
        let tail = match (&self.tail, &doomed.tail) {
            (Some(tail), Some(doomed_tail)) => Some(Tail {
                rows: tail.rows.clone(),
                selection: tail.selection.clone(),
                deleted: Some(union_deleted(
                    tail.deleted.as_ref(),
                    doomed_tail.selection.to_mask(tail.rows.len()),
                )),
            }),
            (tail, _) => tail.clone(),
        };

        // Fold the doomed rows into a base-wide tombstone mask. The matcher
        // doesn't consult the existing tombstones, so the doomed set may name
        // rows already deleted; the union absorbs that. Either way the base
        // (or file) and its secondary indexes are left untouched.
        //
        // The catch-all arm is only reachable with the file backend compiled
        // in; without it, the in-memory arm alone is exhaustive.
        #[cfg_attr(not(feature = "file-io"), allow(unreachable_patterns))]
        match (&self.quads, &doomed.quads) {
            (
                QuadsSource::InMemory {
                    base,
                    selection,
                    components,
                    deleted,
                    probes,
                    ..
                },
                QuadsSource::InMemory {
                    selection: doomed, ..
                },
            ) => {
                // In memory the matched view's selection maps straight to a
                // mask — materializing first if the match was served and its
                // exact ids are still pending (a delete is one of the
                // consumers that needs them).
                let doomed = doomed.materialized_sync()?.to_mask(base.len());
                Ok(Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    quads: QuadsSource::InMemory {
                        base: base.clone(),
                        selection: selection.clone(),
                        // Tombstoning never renumbers base rows, so the
                        // components' rid currency survives the delete.
                        components: Arc::clone(components),
                        deleted: Some(union_deleted(deleted.as_ref(), doomed)),
                        probes: Arc::clone(probes),
                        serve: None,
                    },
                    tail,
                })
            }
            #[cfg(feature = "file-io")]
            (
                QuadsSource::File {
                    path,
                    dict_max_resident_bytes,
                    file,
                    selection,
                    deleted,
                    ..
                },
                QuadsSource::File { .. },
            ) => {
                // A file view may still carry an unresolved filter, so the
                // doomed rows are evaluated to concrete file ids first (reading
                // only the filter columns, never the data ones).
                let doomed = doomed.matching_file_row_mask().await?;
                Ok(Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    quads: QuadsSource::File {
                        path: path.clone(),
                        dict_max_resident_bytes: *dict_max_resident_bytes,
                        file: file.clone(),
                        // An owner has no pending filter, and deleting doesn't
                        // introduce one — it only widens the tombstones.
                        filter: None,
                        selection: selection.clone(),
                        deleted: Some(union_deleted(deleted.as_ref(), doomed)),
                        serve: None,
                    },
                    tail,
                })
            }
            _ => unreachable!("a store only ever derives a view of its own backend"),
        }
    }

    /// Evaluate this file view's pending filter and selection to a base-wide
    /// mask of the file rows it matches (see
    /// [`file_scan::matching_file_rows`]).
    ///
    /// The in-memory delete path can read the doomed rows straight off the
    /// matched view's selection, but a file view may still carry an unresolved
    /// filter, so its matches have to be evaluated here (reading only the
    /// filter columns, never the data ones) before they can be tombstoned.
    #[cfg(feature = "file-io")]
    async fn matching_file_row_mask(&self) -> Result<Mask> {
        let QuadsSource::File {
            file,
            filter,
            selection,
            ..
        } = &self.quads
        else {
            unreachable!("matching_file_row_mask is only called on a file-backed view")
        };
        // A delete needs the doomed rows as concrete ids — a served match's
        // pending selection materializes here (its deferred index-child scan).
        let selection = selection.materialized().await?;
        file_scan::matching_file_rows(file, filter.as_ref(), &selection).await
    }
}

/// Tail-accumulator flatten policy (see `add_quads`): fold the accreted chunks
/// into the flat prefix once they rival it in rows — but not below this floor,
/// so small tails don't flatten on every add …
const TAIL_FLATTEN_FLOOR: usize = 1_024;
/// … and regardless of row counts once this many chunks accrete, so tail scans
/// (which visit every chunk) stay dense.
const TAIL_MAX_CHUNKS: usize = 64;
