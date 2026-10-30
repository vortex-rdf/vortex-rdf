//! Mutations: appends accrete in the tail and deletes tombstone, so the base
//! (its row ids, indexes and file handle) is never rewritten in place.

use crate::error::{Result, VortexRdfError};
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::RawQuad;
use crate::store::builders::build_struct_array;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::view::selection::RowSelection;
#[cfg(feature = "file-io")]
use crate::store::view::selection::ViewSelection;
use crate::store::{Backend, Tail, next_generation};

use hashbrown::HashTable;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use rustc_hash::FxBuildHasher;
use std::hash::BuildHasher;

#[cfg(feature = "file-io")]
use vortex_mask::Mask;

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    // ── mutations ─────────────────────────────────────────────────────────────

    /// This store as an owner that can be mutated: a cheap clone, tombstones
    /// and indexes kept, when it already owns its rows; otherwise (a view
    /// derived from `match_pattern`, which shares a base it does not own) an
    /// independent compacted copy with its indexes rebuilt
    /// ([`compact_with_indexes`]).
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn owned(&self) -> Result<Self> {
        if self.is_owner() {
            Ok(self.clone())
        } else {
            self.compact_with_indexes(self.indexes.clone()).await
        }
    }

    /// Whether this store owns its rows rather than being a window onto
    /// another's. Only an owner may be mutated: a narrowed view's rows are a
    /// subset of a base it shares, so mutating it would either drop the rows
    /// outside the view or write through to data it does not own. A view that
    /// selects everything (an unconstrained `match_pattern`) covers the same
    /// rows as the store it came from, so it counts as an owner.
    pub(in crate::store) fn is_owner(&self) -> bool {
        // A narrowed tail marks a view just as a narrowed base does: a match
        // may cover all base rows yet only some of the tail's.
        let tail_owned = self
            .tail
            .as_ref()
            .is_none_or(|tail| matches!(tail.selection, RowSelection::All));
        tail_owned && self.quads.is_unrefined()
    }

    /// Err unless this store owns its rows ([`is_owner`](Self::is_owner)): the
    /// gate every mutation takes.
    fn ensure_owner(&self, operation: &str) -> Result<()> {
        if self.is_owner() {
            return Ok(());
        }
        Err(VortexRdfError::InvalidOperation(format!(
            "{operation} is not supported on a store derived from match_pattern: its rows are a \
             view onto a larger base, so mutating it would either silently drop the rows outside \
             the view or write through to data it does not own. Call owned() for an \
             independent copy to mutate, or call the mutation on the store the view came from."
        )))
    }

    /// Append a single quad: [`add_quads`] with a batch of one, the form to
    /// prefer for several.
    ///
    /// [`add_quads`]: Self::add_quads
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        self.add_quads([quad]).await
    }

    /// Append every quad not already present (RDF/JS dataset semantics: a
    /// quad equal to an existing one, or to an earlier quad of the batch, is
    /// skipped; each presence check is one fully bound
    /// [`match_pattern`](Self::match_pattern)). Appends land in the `Tail`,
    /// never the base, as one more chunk, flattened at the tail's policy
    /// (`TAIL_FLATTEN_FLOOR`/`TAIL_MAX_CHUNKS`); under the Dictionary layout
    /// the tail holds terms as strings.
    ///
    /// The add that pushes the tail over the auto-compaction thresholds
    /// finishes with [`compact`], on a file-backed store a rewrite of its
    /// source file (watch [`tail_len`](Self::tail_len)). If the writer refuses
    /// that file as one this process cannot write (permission denied, a
    /// read-only filesystem, or a directory that takes no new file), it is
    /// never rewritten: the batch stays in the in-memory tail and the store is
    /// returned, while an explicit [`compact`] fails with the reason. Any
    /// other failure of the compaction is returned.
    ///
    /// [`compact`]: Self::compact
    pub async fn add_quads(&self, quads: impl IntoIterator<Item = Quad>) -> Result<Self> {
        self.ensure_owner("add_quads")?;

        let mut fresh: Vec<RawQuad> = Vec::new();
        // The quads kept so far, by their place in `fresh`: an earlier equal
        // quad of the batch is found there, with no copy of a kept quad.
        let mut kept: HashTable<usize> = HashTable::new();
        for quad in quads {
            let raw = RawQuad::from_quad(&quad);
            let hash = FxBuildHasher.hash_one(&raw);
            if kept.find(hash, |&i| fresh[i] == raw).is_some() || self.contains(&quad).await? {
                continue;
            }
            kept.insert_unique(hash, fresh.len(), |&i| FxBuildHasher.hash_one(&fresh[i]));
            fresh.push(raw);
        }
        if fresh.is_empty() {
            return Ok(self.clone());
        }

        let layout = Tail::layout_for(&self.layout);
        let fresh_rows = build_struct_array(&fresh, layout.strategy(), false)?;
        let tail = match &self.tail {
            None => Tail::new(fresh_rows, layout)?,
            // The fresh rows join the tail as one more chunk, flattened per
            // the tail's policy (see `Tail::append`).
            Some(tail) => tail.append(fresh_rows)?,
        };
        // An append changes the data: a new generation over the same base.
        let appended = Self {
            generation: next_generation(),
            ..self.derived(self.quads.clone(), Some(tail))
        };
        // The add that pushes the tail over the thresholds folds it into the base.
        if appended.should_auto_compact() {
            match appended.compact().await {
                Ok(compacted) => return Ok(compacted),
                // The writer refused the store file (or its directory) as one
                // this process cannot write, so there is nothing to fold the
                // tail into: the batch stays in the in-memory tail, where
                // queries see it, and an explicit `compact()` still reports
                // why. The refusal comes before anything is gathered or built,
                // so each later append retries for one failed open. A
                // permission error from later in the rewrite is not it, and is
                // returned.
                Err(error) if error.is_unwritable() => {
                    log::debug!("[add_quads] compaction refused, batch kept in the tail: {error}");
                }
                Err(error) => return Err(error),
            }
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

    /// Remove every quad matching the pattern: the counterpart of
    /// [`match_pattern`], dropping the rows it selects. The matched rows are
    /// tombstoned in place, so the base's row ids, and with them its secondary
    /// indexes, stay valid; only [`compact`] reclaims them. Only an owner can
    /// be mutated: call it on the store a view came from, or on
    /// `view.owned()`.
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

        // The matched view shares this store's base (or file), so the doomed
        // rows are already in base row ids.
        let doomed = self
            .match_pattern(subject, predicate, object, graph)
            .await?;

        // The tail tombstones exactly as the base does: the doomed view's
        // tail selection is already exact tail-local ids, so it folds into
        // the tail's own deleted mask the same way.
        let tail = match (&self.tail, &doomed.tail) {
            (Some(tail), Some(doomed_tail)) => Some(tail.tombstoned(&doomed_tail.selection)),
            (tail, _) => tail.clone(),
        };

        // Fold the doomed rows into a base-wide tombstone mask. The doomed set
        // may name rows already deleted (the matcher does not consult the
        // tombstones); the union absorbs them. The base (or file) and its
        // secondary indexes stay untouched, and an owner's filter stays
        // absent: deleting only widens the tombstones.
        let doomed = match &doomed.quads.backend {
            // In memory the matched view's selection maps straight to a mask,
            // materialized first if its exact ids are still pending.
            Backend::InMemory(_) => doomed
                .quads
                .selection
                .materialized()?
                .to_mask(self.quads.base_len()),
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                Self::matching_file_row_mask(base, &doomed.quads.selection).await?
            }
        };
        // A delete changes the data: a new generation over the same base.
        Ok(Self {
            generation: next_generation(),
            ..self.derived(self.quads.with_tombstones(doomed), tail)
        })
    }

    /// Evaluate a file view's pending filter (on its base `file`) and
    /// `selection`, materialized, to a base-wide mask of matching file rows,
    /// via [`file_filter::matching_file_rows`].
    #[cfg(feature = "file-io")]
    async fn matching_file_row_mask(base: &FileBase, selection: &ViewSelection) -> Result<Mask> {
        // A served match's pending selection materializes here.
        let selection = selection.materialized_async().await?;
        file_filter::matching_file_rows(&base.file, base.filter.as_ref(), &selection).await
    }
}
