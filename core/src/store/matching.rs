//! Pattern matching: composing a pattern's restrictions into derived views
//! over the base and the tail.

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{bool_array_to_mask, column_is_sorted, search_sorted_bounds};
#[cfg(feature = "file-io")]
use crate::store::indexes::resolve_indexes_file;
use crate::store::indexes::{
    InMemoryServePlan, IndexResolution, LazyRowIds, ResolvedRowIds, resolve_indexes_in_memory,
};
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, ResolvedLayout, TermRef};
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::scan::typed_eq::{typed_positions, typed_residual_ids};
use crate::store::schema;
use crate::store::selection::{RowSelection, ViewSelection};
use crate::store::{QuadsSource, Tail};

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
#[cfg(all(test, feature = "file-io"))]
use std::ops::Range;
use std::sync::Arc;
use web_time::Instant;

use vortex_array::arrays::StructArray;
use vortex_array::arrays::constant::ConstantArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_mask::Mask;

#[cfg(feature = "file-io")]
use vortex_array::expr::and;

use super::VortexRdfStore;

impl VortexRdfStore {
    // ── pattern matching ──────────────────────────────────────────────────────

    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let mut matched = self.match_base(subject, predicate, object, graph).await?;
        // The tail is matched independently of whatever the base concluded —
        // deliberately after any base short-circuit: a term the base's
        // dictionary has never seen proves nothing about rows appended since
        // (the tail stores strings precisely so such a term can still match).
        if let Some(tail) = &self.tail {
            matched.tail = Some(
                Self::match_tail(&self.tail_layout(), tail, subject, predicate, object, graph)
                    .await?,
            );
        }
        Ok(matched)
    }

    /// Narrow a tail to the rows matching the pattern — the tail counterpart
    /// of the base's mask-scan fallback. A tail is small, unsorted, and
    /// unindexed, so a scan over its (already few) selected rows is the right
    /// plan; the surviving positions refine the tail-local selection exactly
    /// as base matches refine the base's.
    async fn match_tail(
        layout: &ResolvedLayout,
        tail: &Tail,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Tail> {
        let carry = |selection: RowSelection| Tail {
            rows: tail.rows.clone(),
            selection,
            deleted: tail.deleted.clone(),
        };
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(carry(tail.selection.clone()));
        }
        // The tail's layouts store plain strings (see `tail_layout`), so this
        // prelude resolves nothing and never suspends — but the witness is
        // still minted by it, like every probe surface.
        let mut codes = layout
            .prepare_pattern(QuadPattern::new(subject, predicate, object, graph))
            .await?;
        let eqs = match codes.constraints(subject, predicate, object, graph)? {
            // A term the tail's layout can prove absent matches nothing.
            Constraints::AlwaysFalse => return Ok(carry(RowSelection::empty())),
            Constraints::Eq(eqs) => eqs,
        };
        if eqs.is_empty() {
            // Unconstrained pattern: every selected tail row matches.
            return Ok(carry(tail.selection.clone()));
        }
        let applied = tail.selection.apply(&tail.rows)?;
        // Typed fast path first: tail string columns compared as raw bytes,
        // no per-call compare/canonicalize pipeline — the path every
        // `contains` (and thus every `add_quad` presence check) rides.
        if let Some(positions) = typed_positions(&applied, &eqs) {
            let mask = Mask::from_indices(applied.len(), positions);
            return Ok(carry(tail.selection.clone().refine(&mask)));
        }
        match Self::mask_for(&applied, subject, predicate, object, graph, &mut codes)? {
            None => Ok(carry(tail.selection.clone())),
            Some(mask) => Ok(carry(
                tail.selection.clone().refine(&bool_array_to_mask(mask)?),
            )),
        }
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately).
    async fn match_base(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        // Started only when debug logging will read it: on wasm
        // `Instant::now()` is a JS-boundary `performance.now()` call sitting
        // on the hottest path in the crate.
        let t = log::log_enabled!(log::Level::Debug).then(Instant::now);

        // Resolve each bound term to its dictionary code at most once for this
        // whole match: the stages below each need the same mapping, and under
        // the Dictionary layout deriving it is a binary search per term.
        //
        // The prelude resolves them all up front and hands back the witness
        // every synchronous probe below runs on — the one point where a
        // dictionary may perform I/O (a file-backed one will scan here), so
        // every stage below reads `codes` synchronously.
        let mut codes = self
            .layout
            .prepare_pattern(QuadPattern::new(subject, predicate, object, graph))
            .await?;

        // A pattern the layout can prove unmatchable (e.g. a term absent from
        // the dictionary) needs no search — and no scan machinery — at all,
        // whichever backend holds the rows.
        if matches!(
            codes.constraints(subject, predicate, object, graph)?,
            Constraints::AlwaysFalse
        ) {
            return Ok(self.empty_view());
        }

        match &self.quads {
            QuadsSource::InMemory { .. } => {
                self.match_base_in_memory(subject, predicate, object, graph, &mut codes, t)
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => {
                self.match_base_file(subject, predicate, object, graph, &mut codes, t)
                    .await
            }
        }
    }

    /// The in-memory backend of [`match_base`](Self::match_base): every
    /// search below runs against the base array and answers in base row ids.
    /// Those ids are computed here, except for the one case that does not
    /// need them — a serving index's resolution that is the view's sole
    /// restriction leaves them pending (`LazyRowIds`), for the first consumer
    /// that reads through the selection rather than the plan.
    ///
    /// Tombstones are deliberately not consulted here: they are applied by
    /// every read path instead, so matching may name deleted rows without the
    /// result ever showing them. Keeping them out also keeps the mask scan's
    /// positions aligned with `selection.apply`, which is what `refine` maps
    /// back through.
    fn match_base_in_memory(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        codes: &mut PatternCodes,
        t: Option<Instant>,
    ) -> Result<Self> {
        // Without `file-io` the InMemory variant is the only one, making this
        // pattern irrefutable — which is fine, not a bug.
        #[allow(irrefutable_let_patterns)]
        let QuadsSource::InMemory {
            base,
            selection,
            components,
            deleted,
            probes,
            ..
        } = &self.quads
        else {
            unreachable!("match_base routes only InMemory sources here");
        };
        // Materialize the base struct so its columns can be inspected
        // (statistics, binary search). Every search below runs against
        // the base and yields base row ids, which are then intersected
        // into this view's selection — so a chained match narrows the
        // same coordinate space instead of rebasing onto a new array.
        // Bases adopted through the split are already canonical
        // structs, so the common case is a plain downcast — no
        // per-match canonicalization.
        let struct_arr = match base.clone().try_downcast::<vortex_array::arrays::Struct>() {
            Ok(s) => s,
            Err(other) => {
                let mut ctx = VORTEX_SESSION.create_execution_ctx();
                other
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?
            }
        };

        // A chained match folds this pattern's restrictions into the
        // previous ones, so a still-pending selection materializes here —
        // chaining is one of the consumers the deferral exists for.
        let mut selection = selection.materialized_sync()?;
        // A serving index's plan reads exactly a contiguous row run, so
        // it is only valid when that run *is* the whole result: the view
        // must start unrestricted and nothing but the serving index's
        // own resolution may narrow it (no subject range, no mask scan).
        let unrefined = matches!(selection, RowSelection::All);
        let mut narrowed_elsewhere = false;

        // The components still to be resolved. Each fast path below
        // clears the one it answers, since its ids already satisfy it.
        let mut pat = QuadPattern::new(subject, predicate, object, graph);
        let mut serve: Option<InMemoryServePlan> = None;
        // A serve-attached resolution whose exact ids stay deferred — set
        // only when that resolution is this view's sole restriction, and
        // becoming the view's `Pending` selection below.
        let mut pending: Option<LazyRowIds> = None;

        // ── Subject binary search on sorted s column ──────────────
        // If a subject is bound and the base's primary `s` column is
        // known to be sorted (stamped by sorted builders), binary
        // search finds the exact [lo, hi) row range for that subject in
        // O(log n) instead of scanning every row.
        if let Some(subj) = pat.subject
            && let Ok(s_col) = struct_arr.unmasked_field_by_name(schema::COL_S)
            && column_is_sorted(s_col)
            && let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subj))
            && let Ok(scalar) = probe.cast(s_col.dtype())
        {
            // Left/right binary search bounds the run of rows equal to
            // the probe value — through the store's cached probe when the
            // column resolves (skipping the per-call encoding-tree walk),
            // else the per-call search.
            let cached = probes.by_name(base, schema::COL_S);
            let (lo, hi) = match (cached, u64::try_from(&scalar)) {
                (Some(owned), Ok(needle)) => owned.bounds(needle),
                _ => search_sorted_bounds(s_col, &scalar)?,
            };
            selection = selection.intersect_range(lo as u64..hi as u64);
            pat.subject = None;
            narrowed_elsewhere = true;
            log::debug!("[match_pattern] s binary search {:?}", debug_elapsed(t));
        }

        // ── Secondary-index routing ───────────────────────────────
        // Ask the configured indexes to resolve the rest of the pattern
        // to exact base row ids — each index owns its own search over
        // its component's columns (e.g. a binary search of the sorted
        // `val`/`rid` pair for a bound object). The store just folds the
        // ids it hands back into the selection.
        // …but only while there is enough left to narrow. A fast path
        // that already cut the view to a handful of rows makes the
        // index a pessimization: resolving a component costs O(rows
        // matching *that component*) — a bound predicate is 1/32nd of
        // the store here — only to intersect it against a selection of
        // one. Below the threshold, filtering those few rows column-wise
        // is strictly cheaper. Nothing is lost by skipping: a view that
        // something else narrowed already discards any serving plan.
        let worth_indexing =
            !narrowed_elsewhere || selection.len(base.len()) >= INDEX_ROUTING_MIN_ROWS;
        if !selection.is_empty(base.len()) && worth_indexing {
            match resolve_indexes_in_memory(&self.indexes, components, &self.layout, pat, codes)? {
                // The probed term is absent from the data — nothing matches.
                IndexResolution::Empty => return Ok(self.empty_view()),
                // Fold the resolution into the selection, and hold onto any
                // serving plan the index handed back to decide below
                // whether it still describes exactly the result.
                IndexResolution::Resolved {
                    row_ids,
                    resolves,
                    serve: candidate,
                } => {
                    pat = resolves.clear(pat);
                    serve = candidate;
                    match row_ids {
                        ResolvedRowIds::Eager(ids) => selection = selection.intersect_ids(ids),
                        // A serve-attached resolution keeps its ids deferred
                        // when it is this view's sole restriction and nothing
                        // residual is left to check — reads go through the
                        // plan, so decoding and sorting the run's rids can
                        // wait for a consumer that needs the selection.
                        // Anything narrower needs them now.
                        ResolvedRowIds::Lazy(lazy) => {
                            if unrefined
                                && !narrowed_elsewhere
                                && !pat.any_bound()
                                && serve.is_some()
                            {
                                pending = Some(lazy);
                            } else {
                                selection = selection.intersect_ids(lazy.materialized_sync()?);
                            }
                        }
                    }
                    log::debug!(
                        "[match_pattern] index (sorted search) {:?}",
                        debug_elapsed(t)
                    );
                }
                // No index accelerates this pattern: whatever is still
                // bound falls to the mask scan below.
                IndexResolution::Declined => {}
            }
        }

        // ── Fallback: residual column filtering ───────────────────
        // Whatever no fast path answered is compared column-wise. Only
        // the rows this view already selects are compared, so a chained
        // match still pays for its own row count, not the base's.
        // Skipped entirely when the fast paths resolved every component:
        // `mask_for` would return `None` without reading a row, but its
        // arguments — `selection.apply` (a slice pushed through the
        // array optimizer) and a struct canonicalization — are exactly
        // the per-call cost this gate saves.
        if !selection.is_empty(base.len()) && pat.any_bound() {
            // Typed fast path: residual equalities over canonical u32
            // code columns (Dictionary layout) become direct slice
            // loops yielding exact base ids — no slice/compare/mask
            // pipeline. Rows outside the selection are never tested,
            // so the ids are already the intersection.
            let typed =
                match codes.constraints(pat.subject, pat.predicate, pat.object, pat.graph)? {
                    Constraints::AlwaysFalse => return Ok(self.empty_view()),
                    Constraints::Eq(eqs) => {
                        typed_residual_ids(&struct_arr, &selection, base.len(), &eqs)
                    }
                };
            match typed {
                Some(ids) => {
                    selection = RowSelection::Ids(ids);
                    narrowed_elsewhere = true;
                    log::debug!("[match_pattern] typed residual scan {:?}", debug_elapsed(t));
                }
                None => {
                    if let Some(mask) = Self::mask_for(
                        &selection.apply(base)?,
                        pat.subject,
                        pat.predicate,
                        pat.object,
                        pat.graph,
                        codes,
                    )? {
                        selection = selection.refine(&bool_array_to_mask(mask)?);
                        narrowed_elsewhere = true;
                        log::debug!("[match_pattern] mask scan {:?}", debug_elapsed(t));
                    }
                }
            }
        }

        // Keep the serving plan only if the serving index's resolution
        // is the sole thing that narrowed this view — otherwise its
        // contiguous run over-covers the actual selection.
        let serve = if unrefined && !narrowed_elsewhere {
            serve
        } else {
            None
        };
        // A deferred resolution *is* the whole selection (the deferral
        // condition made it the sole restriction, which also kept `serve`),
        // so it stays pending until a consumer needs the ids.
        let selection = match pending {
            Some(lazy) => ViewSelection::Pending(lazy),
            None => ViewSelection::Exact(selection),
        };

        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: QuadsSource::InMemory {
                base: base.clone(),
                selection,
                components: Arc::clone(components),
                deleted: deleted.clone(),
                probes: Arc::clone(probes),
                serve,
            },
            tail: self.tail.clone(),
        })
    }

    /// The file backend of [`match_base`](Self::match_base): restrictions
    /// compose into the derived view — index-resolved row ids, a pushed-down
    /// filter, a pruned row range — and no data is read until the next scan.
    #[cfg(feature = "file-io")]
    async fn match_base_file(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        codes: &mut PatternCodes,
        t: Option<Instant>,
    ) -> Result<Self> {
        let QuadsSource::File {
            path,
            dict_max_resident_bytes,
            file,
            filter: existing_filter,
            selection: existing_selection,
            deleted: existing_deleted,
            ..
        } = &self.quads
        else {
            unreachable!("match_base routes only File sources here");
        };
        // A bound subject on a sorted file resolves to its exact row range
        // first, by binary-searching the subject column's encoded chunks —
        // the file mirror of the in-memory subject fast path. Both index
        // resolvers decline bound-subject patterns, so this takes over an
        // uncontested route; the subject then leaves the pattern, and the
        // residual terms ride the narrowed range. Any decline (unsorted
        // file, string-subject layout, unknown term, unsupported chunk
        // encoding) leaves the pattern intact for today's scan path.
        let mut pat = QuadPattern::new(subject, predicate, object, graph);
        let mut subject_range: Option<std::ops::Range<u64>> = None;
        if let Some(subj) = pat.subject
            && file.quads_sorted()
            && let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subj))
            && let Ok(needle) = u64::try_from(&probe)
            && let Some(chunks) = file.sorted_subject_chunks()
            && let Some(range) = chunks
                .bounds(needle, &file.segment_source(), file.session())
                .await
                .map_err(VortexRdfError::Vortex)?
        {
            subject_range = Some(range);
            pat.subject = None;
        }
        // With the subject already resolved to a tiny range, an index
        // resolution for the residual terms is a pessimization (its pushed
        // scan costs O(rows matching that component)) — the same routing
        // rule the in-memory path applies.
        let worth_indexing = subject_range
            .as_ref()
            .is_none_or(|r| (r.end - r.start) as usize >= INDEX_ROUTING_MIN_ROWS);
        // Ask the configured indexes to resolve this pattern to exact
        // row ids — each index owns its own scan over its columns. A
        // resolved component is then left out of the pushed-down filter:
        // the row ids already are exactly its matches, so re-filtering
        // them would only re-read and re-compare that column.
        let resolution = if worth_indexing {
            resolve_indexes_file(&self.indexes, file, &self.layout, pat, codes).await?
        } else {
            IndexResolution::Declined
        };
        // If the index hands back a serving plan, it is kept only when this
        // match is the view's sole restriction: the plan's filter selects
        // exactly the matched rows over the index's own columns, which no
        // longer equals the selection once an earlier filter, narrowing, or
        // subject range also applies (see `FileServePlan`).
        let keep_serve =
            existing_filter.is_none() && existing_selection.is_all() && subject_range.is_none();
        // Fold the subject range into an exact selection, when present.
        let apply_subject = |selection: RowSelection| match &subject_range {
            Some(range) => selection.intersect_range(range.clone()),
            None => selection,
        };
        let (next_filter, resolved_selection, serve) = match resolution {
            // The probed term is absent — nothing can match. Short-
            // circuit to the empty view (an empty id set would just
            // intersect every other restriction down to nothing anyway).
            IndexResolution::Empty => return Ok(self.empty_view()),
            // An index resolved one component: push down a filter for
            // the rest of the pattern only, alongside its row ids.
            IndexResolution::Resolved {
                row_ids,
                resolves,
                serve,
            } => {
                let pat = resolves.clear(pat);
                let (s, p, o, g) = (pat.subject, pat.predicate, pat.object, pat.graph);
                let serve = keep_serve.then_some(serve).flatten();
                let selection = match row_ids {
                    // With the plan kept, the resolution is the view's sole
                    // id restriction, so its deferred ids *are* the
                    // selection — left pending until a consumer needs them
                    // (a count, a chained match, a delete, a base-order
                    // gather); reads stream through the plan without them.
                    ResolvedRowIds::Lazy(lazy) if serve.is_some() => ViewSelection::Pending(lazy),
                    // No plan kept (this view was already restricted): the
                    // ids are needed now — run the deferred scan and fold
                    // it exactly as an eager resolution would be.
                    ResolvedRowIds::Lazy(lazy) => ViewSelection::Exact(apply_subject(
                        existing_selection
                            .materialized()
                            .await?
                            .intersect_ids(lazy.materialized().await?),
                    )),
                    // An index answered with exact ids: fold them into the
                    // selection, which drops it to `Ids` — narrowing
                    // whatever a previous match had established (a range,
                    // or an earlier lookup's ids) without ever setting two
                    // restrictions at once.
                    ResolvedRowIds::Eager(ids) => ViewSelection::Exact(apply_subject(
                        existing_selection.materialized().await?.intersect_ids(ids),
                    )),
                };
                (
                    file_scan::build_file_filter(s, p, o, g, codes)?,
                    Some(selection),
                    serve,
                )
            }
            // No index applies: the residual pattern (the subject already
            // resolved to its range, when it was) becomes the pushed-down
            // filter.
            IndexResolution::Declined => (
                file_scan::build_file_filter(
                    pat.subject,
                    pat.predicate,
                    pat.object,
                    pat.graph,
                    codes,
                )?,
                None,
                None,
            ),
        };
        // Combine with whatever filter this view already carried
        // from earlier match_pattern calls (AND, since both must hold).
        let filter = match (existing_filter.clone(), next_filter) {
            (Some(lhs), Some(rhs)) => Some(and(lhs, rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, rhs) => rhs,
        };

        let selection = match resolved_selection {
            Some(selection) => selection,
            // No index involved: the subject's exact range narrows directly
            // (a zone envelope could only be wider), else narrow using
            // zone-map statistics. One full-range pruning evaluation on the
            // cached layout reader replaces any per-split probing. (A
            // chained match materializes a still-pending selection to fold
            // into — exactly one of the consumers the deferral is for.)
            None => {
                let existing = existing_selection.materialized().await?;
                ViewSelection::Exact(match (&subject_range, &filter) {
                    (Some(_), _) => apply_subject(existing),
                    (None, Some(f)) => match file_scan::row_range_from_pruning(file, f).await? {
                        Some(range) => existing.intersect_range(range),
                        None => existing,
                    },
                    (None, None) => existing,
                })
            }
        };

        // Nothing can match: normalize to the canonical empty view. A
        // pending selection's coverage is unknown by design — it may
        // materialize to empty later, which every consumer handles like any
        // other narrow selection.
        if let ViewSelection::Exact(exact) = &selection
            && exact.is_empty(file.row_count() as usize)
        {
            return Ok(self.empty_view());
        }

        log::debug!("[match_pattern] file filter built {:?}", debug_elapsed(t));
        // Build the new, more-restricted file view; no data has been
        // read yet — restrictions are only applied on the next scan.
        // The file (and thus its index columns) is shared, so the
        // indexes stay usable for further chained matches.
        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: QuadsSource::File {
                path: path.clone(),
                dict_max_resident_bytes: *dict_max_resident_bytes,
                file: file.clone(),
                filter,
                selection,
                // Tombstones are a property of the base file, not of the
                // pattern, so they carry across the match unchanged; the
                // read paths apply them (see `restrict_scan`).
                deleted: existing_deleted.clone(),
                serve,
            },
            tail: self.tail.clone(),
        })
    }

    // ── pattern matching helpers ─────────────────────────────────────────────

    /// Build an in-memory boolean mask (one bit per row of `array`, in its own
    /// order) marking which of its rows satisfy the given pattern. Returns
    /// `None` when the pattern is fully unconstrained (every row matches, so no
    /// mask is needed).
    ///
    /// The mask is positional, so it only means anything against the array it
    /// was computed over: callers holding a view must translate it back to base
    /// row ids via [`RowSelection::refine`].
    ///
    /// `codes` is the match's witness, prepared under the layout of `array`'s
    /// rows — the store's own for the base, [`Self::tail_layout`] for the
    /// tail (which stores strings under a Dictionary-encoded base).
    fn mask_for(
        array: &ArrayRef,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        codes: &mut PatternCodes,
    ) -> Result<Option<ArrayRef>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = array
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        // Translate the RDF pattern into column equality constraints (e.g.
        // "s == <iri>"). A pattern that can never match (like a term missing
        // from a dictionary) short-circuits to an all-false mask without
        // touching any column.
        let eqs = match codes.constraints(subject, predicate, object, graph)? {
            Constraints::AlwaysFalse => {
                let none = ConstantArray::new(Scalar::from(false), struct_arr.len()).into_array();
                return Ok(Some(none));
            }
            Constraints::Eq(eqs) => eqs,
        };

        // AND together one equality comparison per constrained column.
        let mut mask: Option<ArrayRef> = None;
        for (field, value) in eqs {
            let col = struct_arr
                .unmasked_field_by_name(field)
                .map_err(VortexRdfError::Vortex)?;
            // Cast the scalar to the column's dtype (so numeric columns like
            // `o_kind` compare against a scalar of matching type/nullability).
            let scalar = value.cast(col.dtype()).map_err(VortexRdfError::Vortex)?;
            // Broadcast the scalar to a constant column and compare element-wise.
            let rhs = ConstantArray::new(scalar, col.len()).into_array();
            let m = col
                .binary(rhs, Operator::Eq)
                .map_err(VortexRdfError::Vortex)?;
            // Fold this column's mask into the running AND of all constraints.
            mask = Some(match mask.take() {
                Some(prev) => prev
                    .binary(m, Operator::And)
                    .map_err(VortexRdfError::Vortex)?,
                None => m,
            });
        }
        Ok(mask)
    }

    /// Test-only hook: whether this view carries an index serving plan for
    /// `quads()`, so tests can assert the plan actually attaches (and drops)
    /// where intended instead of only observing results.
    #[cfg(test)]
    pub(crate) fn debug_has_serve_plan(&self) -> bool {
        match &self.quads {
            QuadsSource::InMemory { serve, .. } => serve.is_some(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { serve, .. } => serve.is_some(),
        }
    }

    /// Test-only hook: whether this view's base selection is still pending —
    /// a served match whose exact row ids no consumer has needed yet — so
    /// tests can assert the deferral engages (and materializes) exactly where
    /// intended instead of only observing results.
    #[cfg(test)]
    pub(crate) fn debug_selection_pending(&self) -> bool {
        match &self.quads {
            QuadsSource::InMemory { selection, .. } => {
                matches!(selection, ViewSelection::Pending(_))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { selection, .. } => matches!(selection, ViewSelection::Pending(_)),
        }
    }

    /// Test-only hook: whether a pending selection's row ids have actually
    /// been computed. `false` for a still-deferred resolution, so a test can
    /// pin that a read was answered off the serve plan alone. `None` when the
    /// selection is not pending at all.
    #[cfg(test)]
    pub(crate) fn debug_row_ids_materialized(&self) -> Option<bool> {
        let selection = match &self.quads {
            QuadsSource::InMemory { selection, .. } => selection,
            #[cfg(feature = "file-io")]
            QuadsSource::File { selection, .. } => selection,
        };
        match selection {
            ViewSelection::Exact(_) => None,
            ViewSelection::Pending(lazy) => Some(lazy.debug_materialized()),
        }
    }

    /// Test-only hook exposing the zone-map row-range envelope computed for a
    /// bound subject, so tests can assert on it directly instead of only on
    /// final match results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) async fn debug_subject_row_range(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        match &self.quads {
            QuadsSource::File { file, .. } => {
                // Mirror match_base's prelude so the probes below see every
                // bound role resolved (required for a file-backed dictionary).
                let mut codes = self
                    .layout
                    .prepare_pattern(QuadPattern::new(Some(subject), None, None, None))
                    .await?;
                // Term doesn't exist in the store — the envelope is empty
                // without needing to touch the file at all.
                if matches!(
                    codes.constraints(Some(subject), None, None, None)?,
                    Constraints::AlwaysFalse
                ) {
                    return Ok(Some(0..0));
                }
                // Otherwise compute the same envelope match_pattern would.
                match file_scan::build_file_filter(Some(subject), None, None, None, &mut codes)? {
                    Some(filter) => file_scan::row_range_from_pruning(file, &filter).await,
                    None => Ok(None),
                }
            }
            QuadsSource::InMemory { .. } => Ok(None),
        }
    }

    /// Test-only hook exposing the exact row range the encoded chunk-probe
    /// fast path computes for a bound subject — `None` when the fast path
    /// would not engage (unsorted file, unsupported layout, unknown term) —
    /// so tests can assert engagement instead of inferring it from results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) async fn debug_subject_bounds_range(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        let QuadsSource::File { file, .. } = &self.quads else {
            return Ok(None);
        };
        if !file.quads_sorted() {
            return Ok(None);
        }
        let mut codes = self
            .layout
            .prepare_pattern(QuadPattern::new(Some(subject), None, None, None))
            .await?;
        let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subject)) else {
            return Ok(None);
        };
        let Ok(needle) = u64::try_from(&probe) else {
            return Ok(None);
        };
        let Some(chunks) = file.sorted_subject_chunks() else {
            return Ok(None);
        };
        chunks
            .bounds(needle, &file.segment_source(), file.session())
            .await
            .map_err(VortexRdfError::Vortex)
    }

    /// Test-only hook exposing the index-child run the reference index's file
    /// resolution locates for a predicate/object pattern — `None` when the
    /// location declines and the resolution falls back to its pushed-down
    /// scan — so tests can assert engagement instead of inferring it from
    /// results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) async fn debug_reference_index_located_run(
        &self,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
    ) -> Result<Option<Range<u64>>> {
        let QuadsSource::File { file, .. } = &self.quads else {
            return Ok(None);
        };
        let pattern = QuadPattern::new(None, predicate, object, None);
        let mut codes = self.layout.prepare_pattern(pattern).await?;
        crate::store::indexes::secondary_by_reference::debug_located_run(file, pattern, &mut codes)
            .await
    }

    /// Whether the store holds a quad equal to `quad` (tombstoned rows count
    /// as absent). One fully-bound `match_pattern`, so it rides whatever fast
    /// path the store has — subject binary search, secondary indexes, or file
    /// pruning — and checks the tail too.
    pub async fn contains(&self, quad: &Quad) -> Result<bool> {
        let matched = self
            .match_pattern(
                Some(&quad.subject),
                Some(&quad.predicate),
                Some(&quad.object),
                Some(&quad.graph_name),
            )
            .await?;
        Ok(matched.size().await? > 0)
    }
}

/// The elapsed time on `match_base`'s debug-only timer — zero when the timer
/// was never started (debug logging off, in which case `log::debug!` discards
/// the value without formatting it anyway).
fn debug_elapsed(t: Option<Instant>) -> std::time::Duration {
    t.map_or(std::time::Duration::ZERO, |started| started.elapsed())
}

/// Selection size below which an *already narrowed* view should skip secondary
/// index routing and filter its remaining rows column-wise instead (see
/// `match_base`).
///
/// An index lookup's cost tracks how many rows match the component it resolves,
/// not how many the view still holds, so once a fast path has narrowed the view
/// far enough the lookup is pure overhead. An unrefined view never skips —
/// that is the case indexes exist for.
const INDEX_ROUTING_MIN_ROWS: usize = 4_096;
