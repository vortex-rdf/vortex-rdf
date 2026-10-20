//! Pattern matching: composing a pattern's restrictions into derived views
//! over the base and the tail.

use crate::debug;
use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{
    bool_array_to_mask, column_is_sorted, into_struct_array, search_sorted_bounds,
};
#[cfg(feature = "file-io")]
use crate::store::indexes::resolve_indexes_file;
use crate::store::indexes::{IndexResolution, ResolvedRowIds, resolve_indexes_in_memory};
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, TermRef};
use crate::store::probes::StructProbes;
use crate::store::scan::typed_eq::{typed_positions, typed_residual_ids};
#[cfg(feature = "file-io")]
use crate::store::scan::{file_filter, file_reads};
use crate::store::schema;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{Backend, MemoryBase, Tail};

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
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
use crate::store::FileBase;
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use std::ops::Range;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and};

use crate::store::VortexRdfStore;

/// What the caller of a match will do with the view's selection — whether a
/// located index run may leave its row ids unread without a serve plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdsNeed {
    /// The view may be read row by row: every resolution's ids are computed,
    /// or deferred behind a serve plan (the public `match_pattern`).
    Rows,
    /// The view is only counted (`size`, `size_capped`) or windowed: a run
    /// whose width is known keeps its ids deferred, so a count reads none
    /// and a window reads only the rows it takes.
    CountOrWindow,
}

impl VortexRdfStore {
    // ── pattern matching ──────────────────────────────────────────────────────

    /// Derive a view of this store narrowed to the quads matching the pattern
    /// (`None` = free). No quads are decoded: the base and the tail are
    /// matched independently and the resulting restrictions compose with
    /// this view's own — a prefix binary search over the sorted base, a
    /// secondary index's row ids (on a file store also a pushed-down filter
    /// or a pruned row range), then a column-wise scan of whatever remains.
    /// The derived view shares the base, so matches chain; tombstoned rows
    /// are excluded by every read of the result.
    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        self.match_pattern_for(subject, predicate, object, graph, IdsNeed::Rows)
            .await
    }

    /// [`match_pattern`](Self::match_pattern) for a caller whose use of the
    /// view `need` describes; with [`IdsNeed::CountOrWindow`] the view must
    /// only be counted, windowed or kept (each of which resolves it). A view
    /// built for its rows is never pending without a serve plan, which a
    /// debug assertion checks.
    pub(crate) async fn match_pattern_for(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
        need: IdsNeed,
    ) -> Result<Self> {
        let pattern = QuadPattern::new(subject, predicate, object, graph);
        let mut matched = self.match_base(pattern, need).await?;
        // The tail is matched independently of whatever the base concluded —
        // deliberately after any base short-circuit: a term the base's
        // dictionary has never seen proves nothing about rows appended since
        // (the tail stores strings precisely so such a term can still match).
        if let Some(tail) = &self.tail {
            matched.tail = Some(Self::match_tail(tail, pattern).await?);
        }
        debug_assert!(
            need == IdsNeed::CountOrWindow || !matched.quads.is_pending_without_plan(),
            "a view built for its rows (IdsNeed::Rows) is never pending without a serve plan"
        );
        Ok(matched)
    }

    /// Narrow a tail to the rows matching the pattern, under the tail's own
    /// layout — the tail counterpart of the base's mask-scan fallback. A tail
    /// is small, unsorted, and unindexed, so a scan over its (already few)
    /// selected rows is the right plan; the surviving positions refine the
    /// tail-local selection exactly as base matches refine the base's.
    async fn match_tail(tail: &Tail, pattern: QuadPattern<'_>) -> Result<Tail> {
        let t = debug::timer();
        let carry = |selection: RowSelection| tail.with_selection(selection);
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(carry(tail.selection.clone()));
        }
        // The tail's layouts store plain strings (see `Tail::layout_for`), so
        // this prelude resolves nothing and never suspends — but the witness
        // is still minted by it, like every probe surface.
        let mut codes = tail.layout.prepare_pattern(pattern).await?;
        let eqs = match codes.constraints(pattern)? {
            // The tail's string layouts never compile to AlwaysFalse (see
            // `Tail::layout_for`); the arm stays for totality.
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
            debug::step!(t, "[match_pattern] Tail matched by typed positions");
            return Ok(carry(tail.selection.clone().refine(&mask)));
        }
        let mask = Self::mask_for(&applied, &eqs)?;
        let matched = carry(tail.selection.clone().refine(&bool_array_to_mask(mask)?));
        debug::step!(t, "[match_pattern] Tail matched by mask scan");
        Ok(matched)
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately). `need` is the caller's use of the view, which
    /// decides whether a resolution's ids may stay deferred.
    async fn match_base(&self, pattern: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        let t = debug::timer();

        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            debug::step!(t, "[match_pattern] Layout proved the pattern unmatchable");
            return Ok(self.empty_view());
        };
        debug::step!(t, "[match_pattern] Prepared pattern codes");

        match &self.quads.backend {
            Backend::InMemory(memory) => {
                self.match_base_in_memory(memory, pattern, &mut codes, need, t)
            }
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                self.match_base_file(base, pattern, &mut codes, need, t)
                    .await
            }
        }
    }

    /// The match prelude: every bound term of `pattern` resolved to its code
    /// at most once, as the witness every synchronous probe of the match
    /// runs on — the one point where a dictionary may perform I/O (a
    /// file-backed one scans here). `None` when the layout proves the
    /// pattern unmatchable (e.g. a term absent from the dictionary), read off
    /// the prelude's role cache: such a pattern needs no search, and no scan
    /// machinery, whichever backend holds the rows.
    pub(in crate::store) async fn prepared_codes(
        &self,
        pattern: QuadPattern<'_>,
    ) -> Result<Option<PatternCodes<'_>>> {
        let codes = self.layout.prepare_pattern(pattern).await?;
        Ok((!codes.provably_empty(pattern)).then_some(codes))
    }

    /// The in-memory backend of [`match_base`](Self::match_base), `memory`
    /// the view's base: the prefix probe
    /// ([`prefix_probe`](Self::prefix_probe)), secondary-index routing and
    /// the residual column filter ([`residual_narrow`](Self::residual_narrow)),
    /// each narrowing the selection in base row ids. Those ids are computed
    /// here, except for the cases that do not need them — a lazy index
    /// resolution that is the view's sole restriction leaves them pending
    /// (`LazyRowIds`) when it serves the view's reads, or when the caller
    /// only counts or windows the view (`need`), for the first consumer that
    /// reads through the selection and not the plan.
    ///
    /// Tombstones are deliberately not consulted here: they are applied by
    /// every read path instead, so matching may name deleted rows without the
    /// result ever showing them. Keeping them out also keeps the mask scan's
    /// positions aligned with `selection.apply`, which is what `refine` maps
    /// back through.
    fn match_base_in_memory(
        &self,
        memory: &MemoryBase,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes<'_>,
        need: IdsNeed,
        t: Option<Instant>,
    ) -> Result<Self> {
        let MemoryBase {
            base,
            components,
            probes,
            ..
        } = memory;
        // Materialize the base struct so its columns can be inspected
        // (statistics, binary search). Every search below runs against
        // the base and yields base row ids, which are then intersected
        // into this view's selection — so a chained match narrows the
        // same coordinate space instead of rebasing onto a new array.
        // Every in-memory construction (`from_built`, `from_parts`) hands
        // the base over as a struct array, so the common case is a plain
        // downcast; the execute arm of `into_struct_array` is a fallback
        // for a non-struct base.
        let struct_arr = into_struct_array(base.clone())?;
        let base_len = base.len();

        // A chained match folds this pattern's restrictions into the
        // previous ones, so a still-pending selection materializes here —
        // chaining is one of the consumers the deferral exists for.
        let selection = self.quads.selection.materialized()?;
        // A serving index's plan reads exactly a contiguous row run, so
        // it is only valid when that run *is* the whole result: the view
        // must start unrestricted and nothing but the serving index's
        // own resolution may narrow it (no subject range, no mask scan).
        let unrefined = matches!(selection, RowSelection::All);

        // The components still to be resolved. Each fast path below
        // clears the one it answers, since its ids already satisfy it.
        let mut pat = pattern;
        let (selection, prefix_narrowed) =
            Self::prefix_probe(&struct_arr, base, probes, codes, &mut pat, selection, t)?;

        // ── Secondary-index routing ───────────────────────────────
        // Ask the configured indexes to resolve the rest of the pattern
        // to exact base row ids — each index picks its probe, which the
        // in-memory resolver runs over the component's columns (e.g. a
        // binary search of the sorted `val`/`rid` pair for a bound object).
        // The store just folds the ids it hands back into the selection.
        // …but only while there is enough left to narrow. Resolving a
        // component costs O(rows matching *that component*), regardless
        // of how few rows the view still holds, so once a fast path has
        // cut the view to a handful of rows, filtering those rows
        // column-wise is cheaper. Nothing is lost by skipping: a view
        // that something else narrowed already discards any serving plan.
        // A pattern the prefix probe answered in full leaves the indexes
        // nothing to resolve at all.
        let worth_indexing = !prefix_narrowed || selection.len(base_len) >= INDEX_ROUTING_MIN_ROWS;
        let (mut resolved, mut serve) = (None, None);
        if !selection.is_empty(base_len) && pat.any_bound() && worth_indexing {
            match resolve_indexes_in_memory(&self.indexes, components, &self.layout, pat, codes)? {
                // The probed term is absent from the data — nothing matches.
                IndexResolution::Empty => {
                    debug::step!(t, "[match_pattern] In-memory index proved empty");
                    return Ok(self.empty_view());
                }
                // Fold the resolution into the selection below, and hold onto
                // any serving plan the index handed back to decide whether it
                // still describes exactly the result.
                IndexResolution::Resolved {
                    row_ids,
                    resolves,
                    serve: plan,
                } => {
                    pat = resolves.clear(pat);
                    resolved = Some(row_ids);
                    serve = plan;
                }
                // No index accelerates this pattern: whatever is still
                // bound falls to the mask scan below.
                IndexResolution::Declined => {
                    debug::step!(t, "[match_pattern] In-memory index declined");
                }
            }
        }

        // An in-memory resolution is always lazy. A serve-attached one keeps
        // its ids deferred when it is this view's sole restriction (the view
        // started unrestricted, no prefix narrowed it and nothing residual is
        // left to check) — reads go through the plan, so decoding and sorting
        // the run's rids can wait for a consumer that needs the selection. So
        // does a sole run the caller only counts or windows: its width is the
        // count. Anything narrower, or read row by row without a plan, needs
        // them now; a deferred resolution *is* then the whole selection,
        // pending until a consumer needs the ids.
        let sole = unrefined && !prefix_narrowed && !pat.any_bound();
        let selection = match resolved {
            Some(row_ids) => {
                let defer = sole && (serve.is_some() || need == IdsNeed::CountOrWindow);
                let selection = fold_row_ids(selection, row_ids, defer)?;
                debug::step!(t, "[match_pattern] In-memory index resolved");
                selection
            }
            None => ViewSelection::Exact(selection),
        };

        // ── Fallback: residual column filtering ───────────────────
        // Whatever no fast path answered is compared column-wise. Only
        // the rows this view already selects are compared, so a chained
        // match still pays for its own row count, not the base's.
        // Skipped entirely when the fast paths resolved every component:
        // `mask_for` would return `None` without reading a row, but its
        // arguments — `selection.apply` (a slice pushed through the
        // array optimizer) and a struct canonicalization — are exactly
        // the per-call cost this gate saves. (A pending selection never
        // reaches it: the deferral left nothing bound.)
        let (selection, residual_narrowed) = match selection {
            ViewSelection::Exact(exact) if !exact.is_empty(base_len) && pat.any_bound() => {
                let Some(narrowed) =
                    Self::residual_narrow(&struct_arr, base, probes, exact, pat, codes, t)?
                else {
                    return Ok(self.empty_view());
                };
                (ViewSelection::Exact(narrowed), true)
            }
            selection => (selection, false),
        };

        // Keep the serving plan only if the serving index's resolution
        // is the sole thing that narrowed this view — otherwise its
        // contiguous run over-covers the actual selection.
        let serve = if unrefined && !prefix_narrowed && !residual_narrowed {
            serve
        } else {
            None
        };

        debug::step!(
            t,
            "[match_pattern] In-memory view built (serve: {}, pending ids: {})",
            serve.is_some(),
            matches!(selection, ViewSelection::Pending(_))
        );
        Ok(self.derived(
            self.quads.in_memory_with(memory, selection, serve),
            self.tail.clone(),
        ))
    }

    /// The prefix probe over the base's `(s, p, o, g)` order: `selection`
    /// narrowed to the one contiguous row run of the pattern's bound prefix
    /// of that order, by nested binary search instead of a scan, and whether
    /// a bound subject narrowed it.
    ///
    /// The base's rows are in that order whenever its `s` column carries the
    /// sorted stamp (see `stamp_is_sorted`). The subject searches the whole
    /// column, every further role only the run the previous one left; each
    /// answered role is cleared from `pat`, and the first the probes cannot
    /// answer ends the prefix, leaving whatever is still bound to the stages
    /// behind it.
    fn prefix_probe(
        struct_arr: &StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        codes: &mut PatternCodes<'_>,
        pat: &mut QuadPattern<'_>,
        mut selection: RowSelection,
        t: Option<Instant>,
    ) -> Result<(RowSelection, bool)> {
        if let Some(subj) = pat.subject
            && let Ok(s_col) = struct_arr.unmasked_field_by_name(schema::COL_S)
            && column_is_sorted(s_col)
            && let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subj))
            && let Ok(scalar) = probe.cast(s_col.dtype())
        {
            // Left/right binary search bounds the run of rows equal to
            // the probe value — through the store's cached probe when the
            // column resolves (skipping the per-call encoding-tree walk),
            // else the per-call search, which also serves the string
            // layouts' `VarBinView` subjects.
            let cached = probes.by_name(base, schema::COL_S).flatten();
            let (lo, hi) = match (cached, u64::try_from(&scalar)) {
                (Some(owned), Ok(needle)) => {
                    #[cfg(test)]
                    crate::store::scan::typed_eq::probe_hooks::note(true);
                    owned.probe().bounds(needle)
                }
                _ => {
                    #[cfg(test)]
                    crate::store::scan::typed_eq::probe_hooks::note(false);
                    search_sorted_bounds(s_col, &scalar)?
                }
            };
            selection = selection.intersect_range(lo as u64..hi as u64);
            pat.subject = None;
            debug::step!(
                t,
                "[match_pattern] In-memory subject bounded by binary search"
            );

            // The roles behind the subject, in sort order, while the
            // selection is still one run (a chained view's id list has
            // no window to search) and the role has a code and a cached
            // probe — the string layouts stop here, their columns
            // resolving none.
            if let RowSelection::Range(range) = &selection {
                let mut run = range.start as usize..range.end as usize;
                let roles = [
                    (pat.predicate.map(TermRef::Predicate), schema::COL_P),
                    (pat.object.map(TermRef::Object), schema::COL_O),
                    (pat.graph.map(TermRef::Graph), schema::COL_G),
                ];
                let mut answered = 0;
                for (term, column) in roles {
                    let Some(term) = term else { break };
                    let Some(owned) = probes.by_name(base, column).flatten() else {
                        break;
                    };
                    let Some(needle) = codes
                        .probe_scalar(term)?
                        .and_then(|scalar| u64::try_from(&scalar).ok())
                    else {
                        break;
                    };
                    // `bounds_in` consults only the window's order: a
                    // sub-run of the sorted base is sorted by its next key.
                    let (lo, hi) = owned.probe().bounds_in(run.clone(), needle);
                    run = lo..hi;
                    answered += 1;
                    if run.is_empty() {
                        break;
                    }
                }
                if answered > 0 {
                    selection = RowSelection::Range(run.start as u64..run.end as u64);
                    // Exactly the roles the run now satisfies, in order.
                    pat.predicate = None;
                    if answered > 1 {
                        pat.object = None;
                    }
                    if answered > 2 {
                        pat.graph = None;
                    }
                    debug::step!(
                        t,
                        "[match_pattern] In-memory prefix of {answered} more roles bounded by binary search"
                    );
                }
            }
            return Ok((selection, true));
        }
        Ok((selection, false))
    }

    /// The residual column filter: `selection` narrowed to its rows matching
    /// every role the fast paths left bound in `pat`, compared column-wise
    /// over those rows only. `None` when the residual compiles to
    /// `AlwaysFalse`.
    fn residual_narrow(
        struct_arr: &StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: RowSelection,
        pat: QuadPattern<'_>,
        codes: &mut PatternCodes<'_>,
        t: Option<Instant>,
    ) -> Result<Option<RowSelection>> {
        // The residual binds a subset of the roles the caller's gate
        // already resolved to codes, so the unmatchable arm cannot fire
        // here — it stays for totality.
        let eqs = match codes.constraints(pat)? {
            Constraints::AlwaysFalse => return Ok(None),
            Constraints::Eq(eqs) => eqs,
        };
        // Typed fast path: residual equalities over canonical code columns
        // (Dictionary layout) become direct slice loops yielding exact
        // base ids — no slice/compare/mask pipeline — and a probe-bound
        // column reads through the store's own probe. Rows outside the
        // selection are never tested, so the ids are already the
        // intersection.
        Ok(Some(
            match typed_residual_ids(
                struct_arr,
                &selection,
                base.len(),
                &eqs,
                Some((base, probes)),
            ) {
                Some(ids) => {
                    debug::step!(
                        t,
                        "[match_pattern] In-memory narrowed by typed residual scan"
                    );
                    RowSelection::Ids(ids)
                }
                None => {
                    let mask = Self::mask_for(&selection.apply(base)?, &eqs)?;
                    let narrowed = selection.refine(&bool_array_to_mask(mask)?);
                    debug::step!(t, "[match_pattern] In-memory narrowed by mask scan");
                    narrowed
                }
            },
        ))
    }

    /// The file backend of [`match_base`](Self::match_base), `base` the
    /// view's file: restrictions compose into the derived view — the
    /// subject's located run ([`locate_subject`](Self::locate_subject)),
    /// index-resolved row ids, a pushed-down filter, a pruned row range — and
    /// no data is read until the next scan, except a lazy resolution's ids
    /// when they cannot stay deferred and the point reads that check a
    /// pending filter at sparse ids.
    #[cfg(feature = "file-io")]
    async fn match_base_file(
        &self,
        base: &FileBase,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes<'_>,
        need: IdsNeed,
        t: Option<Instant>,
    ) -> Result<Self> {
        let FileBase {
            file,
            filter: existing_filter,
            ..
        } = base;
        let existing_selection = &self.quads.selection;
        // A bound subject on a sorted file resolves to its exact row range
        // first, by binary-searching the subject column's encoded chunks —
        // the file mirror of the in-memory subject fast path. Each index's
        // `choose` declines bound-subject patterns, so this takes over an
        // uncontested route; the subject then leaves the pattern, the roles
        // behind it narrow the run the same way (the in-memory prefix probe),
        // and whatever is still bound rides the narrowed range. Any decline
        // leaves the pattern intact for the scan path.
        let mut pat = pattern;
        let subject_range = Self::locate_subject(file, codes, &mut pat, t).await?;
        // With the subject already resolved to a tiny range, an index
        // resolution for the residual terms is a pessimization (its pushed
        // scan costs O(rows matching that component)) — the same routing
        // rule the in-memory path applies.
        let worth_indexing = subject_range
            .as_ref()
            .is_none_or(|r| (r.end - r.start) as usize >= INDEX_ROUTING_MIN_ROWS);
        // Ask the configured indexes to resolve this pattern to exact
        // row ids — each index picks its probe, the file resolver runs it.
        // A resolved component is then left out of the pushed-down filter:
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
            IndexResolution::Empty => {
                debug::step!(t, "[match_pattern] File index proved empty");
                return Ok(self.empty_view());
            }
            // An index resolved one component: push down a filter for
            // the rest of the pattern only, alongside its row ids.
            IndexResolution::Resolved {
                row_ids,
                resolves,
                serve,
            } => {
                let pat = resolves.clear(pat);
                // Built before the ids fold: a run held for a count needs
                // nothing residual left to filter.
                let residual = file_reads::build_file_filter(pat, codes)?;
                let serve = keep_serve.then_some(serve).flatten();
                // With the plan kept, the resolution is the view's sole id
                // restriction, so its deferred ids *are* the selection — left
                // pending until a consumer needs them (a count, a chained
                // match, a delete, a base-order gather); reads stream through
                // the plan without them. So are the ids of a located run the
                // caller only counts or windows: its width is the count, and
                // a window reads only its own rows — provided nothing else
                // restricts this view and nothing residual is left to filter.
                // Any other lazy resolution runs its deferred read now, and
                // folds exactly as an eager one's ids do.
                let defer = serve.is_some()
                    || (need == IdsNeed::CountOrWindow
                        && keep_serve
                        && residual.is_none()
                        && match &row_ids {
                            ResolvedRowIds::Lazy(lazy) => lazy.len_if_known().is_some(),
                            ResolvedRowIds::Eager(_) => false,
                        });
                let eager = matches!(row_ids, ResolvedRowIds::Eager(_));
                // A chained match materializes a still-pending selection to
                // fold into; a deferral only ever meets the unrefined one.
                let existing = existing_selection.materialized_async().await?;
                // A file run's ids take I/O: when they cannot stay deferred
                // they are read here, into the run's shared cell, where the
                // fold finds them.
                if !defer && let ResolvedRowIds::Lazy(lazy) = &row_ids {
                    lazy.materialized_async().await?;
                }
                let selection = match fold_row_ids(existing, row_ids, defer)? {
                    ViewSelection::Exact(exact) => ViewSelection::Exact(apply_subject(exact)),
                    pending => pending,
                };
                debug::step!(
                    t,
                    "[match_pattern] File index {}",
                    match (&selection, serve.is_some()) {
                        (ViewSelection::Pending(_), true) => "resolved (served, ids pending)",
                        (ViewSelection::Pending(_), false) => {
                            "located a run (ids pending for a count)"
                        }
                        (ViewSelection::Exact(_), _) if eager => "resolved (eager ids)",
                        (ViewSelection::Exact(_), _) => "resolved (ids materialized)",
                    }
                );
                (residual, Some(selection), serve)
            }
            // No index applies: the residual pattern (the subject already
            // resolved to its range, when it was) becomes the pushed-down
            // filter.
            IndexResolution::Declined => {
                debug::step!(t, "[match_pattern] File index declined");
                (file_reads::build_file_filter(pat, codes)?, None, None)
            }
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
            // zone-map statistics. (A chained match materializes a
            // still-pending selection to fold into — exactly one of the
            // consumers the deferral is for.)
            None => {
                let existing = existing_selection.materialized_async().await?;
                ViewSelection::Exact(match (&subject_range, &filter) {
                    (Some(_), _) => apply_subject(existing),
                    (None, Some(f)) => Self::prune_by_filter(file, existing, f, t).await?,
                    (None, None) => existing,
                })
            }
        };

        // Exact ids sparse in their splits check the filter at their own rows
        // through the chunk probes, as the in-memory path checks a residual at
        // the selected rows, and no filter is left to the scan: its
        // evaluation decodes every row of each split the ids touch.
        let (filter, selection) = match (filter, selection) {
            (Some(f), ViewSelection::Exact(RowSelection::Ids(ids))) => {
                match file_reads::point_filter_ids(file, &f, &ids).await? {
                    Some(kept) => {
                        debug::step!(t, "[match_pattern] File filter checked by point reads");
                        (None, ViewSelection::Exact(RowSelection::Ids(kept)))
                    }
                    None => (Some(f), ViewSelection::Exact(RowSelection::Ids(ids))),
                }
            }
            other => other,
        };

        // Nothing can match: normalize to the canonical empty view. A
        // pending selection's coverage is unknown by design — it may
        // materialize to empty later, which every consumer handles like any
        // other narrow selection.
        if let ViewSelection::Exact(exact) = &selection
            && exact.is_empty(file.row_count() as usize)
        {
            debug::step!(t, "[match_pattern] File selection proved empty");
            return Ok(self.empty_view());
        }

        debug::step!(
            t,
            "[match_pattern] File view built (filter: {}, serve: {}, pending ids: {})",
            filter.is_some(),
            serve.is_some(),
            matches!(selection, ViewSelection::Pending(_))
        );
        // Build the new, more-restricted file view; no data has been
        // read yet — restrictions are only applied on the next scan.
        // The file (and thus its index columns) is shared, so the
        // indexes stay usable for further chained matches. Tombstones are a
        // property of the base file, not of the pattern, so they carry
        // across the match unchanged; the read paths apply them (see
        // `restrict_scan`).
        Ok(self.derived(
            self.quads.file_with(base, filter, selection, serve),
            self.tail.clone(),
        ))
    }

    /// A bound subject's exact row run in a sorted file: the subject column's
    /// encoded chunks binary-searched through their cached chunk probes, then
    /// the run narrowed by the roles behind the subject in sort order through
    /// their columns' chunk probes — the file mirror of the in-memory prefix
    /// probe. The subject and every answered role leave `pat`. `None`, with
    /// `pat` intact, when no subject is bound or the location declines.
    #[cfg(feature = "file-io")]
    async fn locate_subject(
        file: &NativeStoreFile,
        codes: &mut PatternCodes<'_>,
        pat: &mut QuadPattern<'_>,
        t: Option<Instant>,
    ) -> Result<Option<Range<u64>>> {
        let Some(subject) = pat.subject else {
            return Ok(None);
        };
        let Some(mut range) = file_reads::locate_subject_run(file, codes, subject).await? else {
            return Ok(None);
        };
        pat.subject = None;
        debug::step!(t, "[match_pattern] File subject bounded by chunk probe");
        let answered = file_reads::narrow_subject_run(file, codes, pat, &mut range).await?;
        if answered > 0 {
            debug::step!(
                t,
                "[match_pattern] File prefix of {answered} more roles bounded by chunk probes"
            );
        }
        Ok(Some(range))
    }

    /// `existing` narrowed to the envelope zone-map pruning collapses
    /// `filter` to: one full-range pruning evaluation on the cached layout
    /// reader, statistics only, in place of any per-split probing. Left as it
    /// is when the statistics exclude nothing.
    #[cfg(feature = "file-io")]
    async fn prune_by_filter(
        file: &NativeStoreFile,
        existing: RowSelection,
        filter: &Expression,
        t: Option<Instant>,
    ) -> Result<RowSelection> {
        let pruned = file_filter::row_range_from_pruning(file, filter).await?;
        debug::step!(
            t,
            "[match_pattern] File narrowed by zone-map pruning (range: {})",
            pruned.is_some()
        );
        Ok(match pruned {
            Some(range) => existing.intersect_range(range),
            None => existing,
        })
    }

    // ── pattern matching helpers ─────────────────────────────────────────────

    /// Build an in-memory boolean mask (one bit per row of `array`, in its own
    /// order) marking which of its rows satisfy `eqs` — the per-column
    /// equalities a pattern compiled to under the layout of `array`'s rows
    /// (the store's own for the base, the tail's own for the tail, which
    /// stores strings under a Dictionary-encoded base: see
    /// [`Tail::layout_for`]).
    ///
    /// The mask is positional, so it only means anything against the array it
    /// was computed over: callers holding a view must translate it back to base
    /// row ids via [`RowSelection::refine`].
    ///
    /// Callers compile the constraints and pass them in, because each has
    /// already consulted them — to rule out an unmatchable pattern, and to try
    /// the typed comparison paths this is the fallback for.
    fn mask_for(array: &ArrayRef, eqs: &[(&'static str, Scalar)]) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = array.clone().execute::<StructArray>(&mut ctx)?;

        // AND together one equality comparison per constrained column.
        let mut mask: Option<ArrayRef> = None;
        for (field, value) in eqs {
            let col = struct_arr.unmasked_field_by_name(field)?;
            // Cast the scalar to the column's dtype (so numeric columns like
            // `o_kind` compare against a scalar of matching type/nullability).
            let scalar = value.cast(col.dtype())?;
            // Broadcast the scalar to a constant column and compare element-wise.
            let rhs = ConstantArray::new(scalar, col.len()).into_array();
            let m = col.binary(rhs, Operator::Eq)?;
            // Fold this column's mask into the running AND of all constraints.
            mask = Some(match mask.take() {
                Some(prev) => prev.binary(m, Operator::And)?,
                None => m,
            });
        }
        Ok(match mask {
            Some(mask) => mask,
            // An unconstrained pattern matches every row. Both callers gate on
            // a non-empty constraint set, so the fold always starts.
            None => ConstantArray::new(Scalar::from(true), struct_arr.len()).into_array(),
        })
    }

    /// Whether the store holds a quad equal to `quad` (tombstoned rows count
    /// as absent). One fully-bound `match_pattern`, so it rides whatever fast
    /// path the store has — subject binary search, secondary indexes, or file
    /// pruning — and checks the tail too; the match is read only as far as
    /// its first row ([`exists`](Self::exists)).
    pub async fn contains(&self, quad: &Quad) -> Result<bool> {
        let matched = self
            .match_pattern(
                Some(&quad.subject),
                Some(&quad.predicate),
                Some(&quad.object),
                Some(&quad.graph_name),
            )
            .await?;
        matched.exists().await
    }
}

/// `existing` narrowed by an index resolution's row ids: a lazy resolution
/// the caller may leave unread (`defer`) becomes the view's pending
/// selection, its ids computed by the first consumer that needs them; any
/// other lazy one's ids, like an eager resolution's, are intersected into
/// `existing`, which they can only narrow. Synchronous, as the in-memory
/// match is: an in-memory run's ids are computed here, while the file match
/// reads a file run's (they take I/O) before folding, so they are found
/// computed.
fn fold_row_ids(
    existing: RowSelection,
    row_ids: ResolvedRowIds,
    defer: bool,
) -> Result<ViewSelection> {
    Ok(match row_ids {
        ResolvedRowIds::Lazy(lazy) if defer => ViewSelection::Pending(lazy),
        ResolvedRowIds::Lazy(lazy) => {
            ViewSelection::Exact(existing.intersect_ids(lazy.materialized()?))
        }
        #[cfg(feature = "file-io")]
        ResolvedRowIds::Eager(ids) => ViewSelection::Exact(existing.intersect_ids(ids)),
    })
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
