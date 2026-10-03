//! Mutations: appends accrete in the tail and deletes tombstone, so the base
//! (its row ids, indexes and file handle) is never rewritten in place.

use crate::error::Result;
use crate::store::RawQuad;
use crate::store::builders::build_struct_array;
#[cfg(feature = "file-io")]
use crate::store::scan::file_filter;
use crate::store::{QuadsSource, Tail};

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::collections::HashSet;

use vortex_mask::Mask;

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    // ── mutations ─────────────────────────────────────────────────────────────

    /// Append a single quad: [`add_quads`](Self::add_quads) with a batch of
    /// one.
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        self.add_quads([quad]).await
    }

    /// Append every quad not already present (RDF/JS dataset semantics: a
    /// quad equal to an existing one, or to an earlier quad of the batch, is
    /// skipped). Appends land in the tail, never the base; under the
    /// Dictionary layout the tail holds terms as strings. Each presence check
    /// is one fully-bound [`match_pattern`](Self::match_pattern). The add
    /// that pushes the tail over the auto-compaction thresholds finishes with
    /// [`compact`](Self::compact), which rewrites a file-backed store's
    /// source file.
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

        let layout = Tail::layout_for(&self.layout);
        let fresh_rows = build_struct_array(&fresh, layout.strategy(), false)?;
        let tail = match &self.tail {
            None => Tail::new(fresh_rows, layout),
            Some(tail) => tail.append(fresh_rows)?,
        };
        let appended = Self {
            generation: crate::store::next_generation(),
            ..self.derived(self.quads.clone(), Some(tail))
        };
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

    /// Remove every quad matching the pattern: the matched rows are
    /// tombstoned in place, so the base's row ids and secondary indexes stay
    /// valid; [`compact`](Self::compact) reclaims them. Only an owner can be
    /// mutated (the store a view came from, or `view.owned()`).
    pub async fn delete_matching(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        self.ensure_owner("delete_matching")?;

        // The matched view shares this store's base, so its selection is
        // already in base row ids; it may name rows already deleted, which
        // the mask union absorbs.
        let doomed = self
            .match_pattern(subject, predicate, object, graph)
            .await?;
        let tail = match (&self.tail, &doomed.tail) {
            (Some(tail), Some(doomed_tail)) => Some(tail.tombstoned(&doomed_tail.selection)),
            (tail, _) => tail.clone(),
        };
        let doomed_rows = doomed.matched_base_rows().await?;
        Ok(Self {
            generation: crate::store::next_generation(),
            ..self.derived(self.quads.with_tombstones(doomed_rows), tail)
        })
    }

    /// This view's base rows as a base-wide mask; a pending selection
    /// materializes, a pending file filter is evaluated.
    async fn matched_base_rows(&self) -> Result<Mask> {
        match &self.quads {
            QuadsSource::InMemory {
                base, selection, ..
            } => Ok(selection.materialized()?.to_mask(base.len())),
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                file_filter::matching_file_rows(file, filter.as_ref(), &selection).await
            }
        }
    }
}
