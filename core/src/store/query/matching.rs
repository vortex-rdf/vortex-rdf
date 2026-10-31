//! Pattern matching: composing a pattern's restrictions into derived views
//! over the base and the tail.

use std::sync::Arc;

use crate::debug;
use crate::error::Result;
use crate::store::array::{
    bool_array_to_mask, column_is_sorted, into_struct_array, search_sorted_bounds, unsigned_value,
};
#[cfg(feature = "file-io")]
use crate::store::indexes::resolve_indexes_file;
use crate::store::indexes::{IndexResolution, ResolvedRowIds, resolve_indexes_in_memory};
use crate::store::layouts::{
    Constraints, OwnedQuadPattern, PatternCodes, Prepared, QuadPattern, ResolvedLayout, TermRef,
};
use crate::store::probes::StructProbes;
use crate::store::scan::pieces::FIRST_PIECE_ROWS;
use crate::store::scan::residual;
use crate::store::scan::typed_eq::typed_positions;
#[cfg(feature = "file-io")]
use crate::store::scan::{file_exprs, file_filter, file_reads, subject_runs};
use crate::store::schema;
use crate::store::view::pending::PendingResidual;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{Backend, MemoryBase, Tail};

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use web_time::Instant;

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::scalar::Scalar;
use vortex_mask::Mask;

#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use crate::store::scan::file_exprs::PushedFilter;
#[cfg(feature = "file-io")]
use crate::store::{FileBase, FileFilter};
#[cfg(feature = "file-io")]
use std::ops::Range;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and};

use crate::observe::observe;
use crate::store::VortexRdfStore;

/// What the caller of a match does with the view's selection: whether a
/// located index run may leave its row ids unread without a serve plan, and
/// an in-memory residual compare its rows uncompared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdsNeed {
    /// The view may be read row by row: every resolution's ids are computed,
    /// or deferred behind a serve plan (the public `match_pattern`).
    Rows,
    /// The view is only counted (`size`, `size_capped`), windowed or kept,
    /// and needs at most its first `reach` rows (`usize::MAX`: all of them):
    /// a run whose width is known keeps its ids deferred, so a count reads
    /// none and a window reads only the rows it takes, and an in-memory
    /// residual compare over more rows than `reach` is left pending, compared
    /// only as far as the count or the window reaches ([`PendingResidual`]).
    CountOrWindow { reach: usize },
}

impl IdsNeed {
    /// Whether the view is only counted, windowed or kept.
    pub(crate) fn counts_or_windows(self) -> bool {
        matches!(self, IdsNeed::CountOrWindow { .. })
    }

    /// Whether a residual compare over `rows` selected rows is left pending
    /// ([`PendingResidual`]): only a count or a window that needs fewer rows
    /// than the selection holds can stop early, and only over more rows than
    /// the first piece holds.
    fn defers_residual(self, rows: usize) -> bool {
        matches!(self, IdsNeed::CountOrWindow { reach } if reach < rows) && rows > FIRST_PIECE_ROWS
    }
}

impl VortexRdfStore {
    // ── pattern matching ──────────────────────────────────────────────────────

    /// A view of this store narrowed to the quads matching the pattern
    /// (`None` = free), with no quad decoded: the base and the tail are
    /// matched independently, and the restrictions compose with this view's
    /// own (a prefix binary search over the sorted base, an index's row ids,
    /// on file also a pushed-down filter or a pruned row range, then a
    /// column-wise scan of whatever remains). The view shares the base, so
    /// matches chain; every read of it excludes tombstoned rows.
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
        // The tail is matched whatever the base concluded: a term the base's
        // dictionary has never seen proves nothing about rows appended since
        // (the tail stores strings so such a term can still match).
        if let Some(tail) = &self.tail {
            matched.tail = Some(Self::match_tail(tail, pattern).await?);
        }
        debug_assert!(
            need.counts_or_windows() || !matched.quads.is_pending_without_plan(),
            "a view built for its rows (IdsNeed::Rows) is never pending without a serve plan"
        );
        Ok(matched)
    }

    /// A tail narrowed to the rows matching the pattern, under the tail's own
    /// layout: a compare over its selected rows (a tail is small, unsorted
    /// and unindexed), whose surviving positions refine the tail-local
    /// selection as base matches refine the base's. A fully bound pattern
    /// compares only the rows its key names.
    async fn match_tail(tail: &Tail, pattern: QuadPattern<'_>) -> Result<Tail> {
        let t = debug::timer();
        let carry = |selection: RowSelection| tail.with_selection(selection);
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(carry(tail.selection.clone()));
        }
        // The tail's layouts store plain strings (see `Tail::layout_for`), so
        // this prelude resolves nothing, never suspends and is never empty;
        // it still mints the witness, as for every probe surface.
        let layout = ResolvedLayout::from(tail.layout);
        let Prepared::Codes(mut codes) = layout.prepare_pattern(pattern).await? else {
            return Ok(carry(RowSelection::empty()));
        };
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
        // A fully bound pattern (every `contains`, so every presence check
        // `add_quads` makes) compares only the selected rows the tail's keys
        // name for it ([`TailKeys`](crate::store::view::tail_keys::TailKeys)),
        // instead of every selected row; a row that only shares the key is
        // dropped by the compare.
        let compared = match tail.keys.candidates(&eqs) {
            Some(candidates) => {
                observe!(TailMatch { keyed: true });
                let named: Vec<schema::RowId> = candidates
                    .into_iter()
                    .filter(|&row| tail.selection.contains(row))
                    .collect();
                RowSelection::from_ids(vortex_buffer::Buffer::from(named))
            }
            None => {
                observe!(TailMatch { keyed: false });
                tail.selection.clone()
            }
        };
        let matched = Self::tail_rows_holding(&tail.rows, compared, &eqs)?;
        debug::step!(t, "[match_pattern] Tail matched");
        Ok(carry(matched))
    }

    /// The rows of `selection` (tail-local ids into `rows`) that hold every
    /// equality of `eqs`: the tail's string columns compared as raw bytes
    /// where they can be ([`typed_positions`]), without the per-call
    /// compare/canonicalize pipeline, else by the mask scan.
    fn tail_rows_holding(
        rows: &ArrayRef,
        selection: RowSelection,
        eqs: &[(&'static str, Scalar)],
    ) -> Result<RowSelection> {
        if selection.is_empty(rows.len()) {
            return Ok(selection);
        }
        let applied = selection.apply(rows)?;
        if let Some(positions) = typed_positions(&applied, eqs) {
            let mask = Mask::from_indices(applied.len(), positions);
            return Ok(selection.refine(&mask));
        }
        let mask = residual::mask_for(&applied, eqs)?;
        Ok(selection.refine(&bool_array_to_mask(mask)?))
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately). `need` is the caller's use of the view, which
    /// decides whether a resolution's ids may stay deferred.
    async fn match_base(&self, pattern: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        // A view whose base still holds the pending matches of one pattern
        // over every row is matched as one match of both patterns, which
        // never computes those ids.
        if let ViewSelection::Pending {
            matches: Some(first),
            ..
        } = &self.quads.selection
            && let Some(both) = first.and(pattern)
        {
            return self.match_both(both, need).await;
        }
        let t = debug::timer();

        let Prepared::Codes(mut codes) = self.layout.prepare_pattern(pattern).await? else {
            debug::step!(t, "[match_pattern] Layout proved the pattern unmatchable");
            return Ok(self.empty_view());
        };
        debug::step!(t, "[match_pattern] Prepared pattern codes");

        let every_row = self.quads.is_unrefined();
        let mut matched = match &self.quads.backend {
            Backend::InMemory(memory) => {
                self.match_base_in_memory(memory, pattern, &mut codes, need, t)?
            }
            #[cfg(feature = "file-io")]
            Backend::File(base) => {
                self.match_base_file(base, pattern, &mut codes, need, t)
                    .await?
            }
        };
        // Matched over every base row, pending ids are (with the filter the
        // match left beside them) exactly the pattern's matches. A view built
        // for its rows, which a caller may chain on, names the pattern where
        // its run is wide enough that computing the ids costs more than
        // keeping the pattern.
        if every_row
            && need == IdsNeed::Rows
            && let ViewSelection::Pending { ids, matches } = &mut matched.quads.selection
            && ids.len_if_known().is_none_or(|rows| rows >= CHAIN_MIN_ROWS)
        {
            *matches = Some(Arc::new(OwnedQuadPattern::of(pattern)));
        }
        Ok(matched)
    }

    /// The base of a chained match whose view still holds the pending
    /// matches of a pattern over every base row, `both` binding that
    /// pattern's roles and the chained one's: one match of `both` over every
    /// base row, which the pending ids never enter. A view built for its rows
    /// gets the ids in base order with no serve plan, as a chained match
    /// always leaves them; one only counted or windowed keeps what the match
    /// left.
    async fn match_both(&self, both: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        observe!(ChainMerged);
        let t = debug::timer();
        let every_row = self.derived(self.quads.unrestricted(), self.tail.clone());
        let matched = Box::pin(every_row.match_base(both, need)).await?;
        if need.counts_or_windows() {
            debug::step!(
                t,
                "[match_pattern] Chained match answered as one match of both patterns"
            );
            return Ok(matched);
        }
        let ids = matched.quads.materialized_selection().await?;
        debug::step!(
            t,
            "[match_pattern] Chained match answered as one match of both patterns (ids in base order)"
        );
        Ok(matched.derived(
            matched.quads.with_selection(ViewSelection::Exact(ids)),
            matched.tail.clone(),
        ))
    }

    /// The in-memory backend of [`match_base`](Self::match_base), `memory`
    /// the view's base: the prefix probe
    /// ([`prefix_probe`](Self::prefix_probe)), secondary-index routing and
    /// the residual column filter ([`residual_narrow`](Self::residual_narrow)),
    /// each narrowing the selection in base row ids. The ids are computed
    /// here, except that an index resolution that is the view's sole
    /// restriction leaves them pending (`LazyRowIds`) when its plan serves
    /// the view's reads, or when the caller only counts or windows the view
    /// (`need`), and a residual compare a count or a window may stop short of
    /// is left pending too ([`PendingResidual`]).
    ///
    /// Tombstones are not consulted: every read applies them, so a match may
    /// name deleted rows the result never shows, and the mask scan's
    /// positions stay aligned with `selection.apply`, which `refine` maps
    /// back through.
    fn match_base_in_memory(
        &self,
        memory: &MemoryBase,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
        need: IdsNeed,
        t: Option<Instant>,
    ) -> Result<Self> {
        let MemoryBase {
            base,
            components,
            probes,
            ..
        } = memory;
        // The base as a struct, for its columns. Every search below yields
        // base row ids, intersected into this view's selection, so a chained
        // match narrows the same coordinate space. An in-memory construction
        // hands the base over as a struct array, so this is a downcast;
        // `into_struct_array` executes any other base.
        let struct_arr = into_struct_array(base.clone())?;
        let base_len = base.len();

        // A chained match folds into the previous restrictions, so a pending
        // selection materializes here.
        let selection = self.quads.selection.materialized()?;
        // A serve plan reads exactly a contiguous row run, so it is valid
        // only when that run is the whole result: the view starts
        // unrestricted and only the serving index's resolution narrows it
        // (no subject range, no mask scan).
        let unrefined = matches!(selection, RowSelection::All);

        // The components still to be resolved. Each fast path below
        // clears the one it answers, since its ids already satisfy it.
        let mut pat = pattern;
        let (selection, prefix_narrowed) =
            Self::prefix_probe(&struct_arr, base, probes, codes, &mut pat, selection, t)?;

        // ── Secondary-index routing ───────────────────────────────
        // The configured indexes resolve the rest of the pattern to exact
        // base row ids, each with its own probe over its component's columns
        // (a binary search of the sorted `val`/`rid` pair for a bound object,
        // say), and the ids fold into the selection. Only while enough is left
        // to narrow: a resolution costs the rows matching its component,
        // however few the view still holds, so once the prefix probe has cut
        // the view below `INDEX_ROUTING_MIN_ROWS` rows they are filtered
        // column-wise instead (the view has lost any serve plan already).
        let worth_indexing = !prefix_narrowed || selection.len(base_len) >= INDEX_ROUTING_MIN_ROWS;
        let (mut resolved, mut serve) = (None, None);
        if !selection.is_empty(base_len) && pat.any_bound() && worth_indexing {
            match resolve_indexes_in_memory(&self.indexes, components, &self.layout, pat, codes)? {
                // The probed term is absent from the data: nothing matches.
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

        // An in-memory resolution is always lazy. Its ids stay deferred when
        // it is the view's sole restriction (the view started unrestricted, no
        // prefix narrowed it, nothing residual is left) and either its plan
        // serves the reads or the caller only counts or windows the view (the
        // run's width is the count); the pending ids are then the whole
        // selection. Anything else needs them now.
        let sole = unrefined && !prefix_narrowed && !pat.any_bound();
        let selection = match resolved {
            Some(row_ids) => {
                let defer = sole && (serve.is_some() || need.counts_or_windows());
                let selection = fold_row_ids(selection, row_ids, defer)?;
                debug::step!(t, "[match_pattern] In-memory index resolved");
                selection
            }
            None => ViewSelection::Exact(selection),
        };

        // ── Fallback: residual column filtering ───────────────────
        // Whatever no fast path answered is compared column-wise over the
        // rows the view already selects, so a chained match pays for its own
        // rows, not the base's. Skipped when nothing is left bound, which
        // saves the per-call `selection.apply` and struct canonicalization; a
        // pending selection never gets here, as its deferral left nothing
        // bound.
        let (selection, residual_narrowed) = match selection {
            ViewSelection::Exact(exact) if !exact.is_empty(base_len) && pat.any_bound() => {
                let Some(narrowed) = Self::residual_narrow(memory, exact, pat, codes, need, t)?
                else {
                    return Ok(self.empty_view());
                };
                (narrowed, true)
            }
            selection => (selection, false),
        };

        // Keep the serving plan only if the serving index's resolution
        // is the sole thing that narrowed this view: otherwise its
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
            !matches!(selection, ViewSelection::Exact(_))
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
        codes: &mut PatternCodes,
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
            // Left and right binary searches bound the run equal to the
            // probe value: through the store's cached probe when the column
            // resolves one (no per-call encoding-tree walk), else the
            // per-call search, which also serves the string layouts'
            // `VarBinView` subjects.
            let cached = probes.by_name(base, schema::COL_S).flatten();
            let (lo, hi) = match (cached, unsigned_value(&scalar)) {
                (Some(owned), Some(needle)) => {
                    observe!(ProbeBound { cached: true });
                    owned.probe().bounds(needle)
                }
                _ => {
                    observe!(ProbeBound { cached: false });
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
            // selection is still one run (a chained view's id list has no
            // window to search) and the role has a code and a cached probe;
            // the string layouts stop here, their columns resolving none.
            if let RowSelection::Range(range) = &selection {
                let mut run = range.start as usize..range.end as usize;
                let mut answered = 0;
                for term in pat.prefix_behind_subject() {
                    let Some(owned) = probes.by_name(base, term.role().name()).flatten() else {
                        break;
                    };
                    let Some(needle) = codes.probe_code(term)? else {
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
                    pat.clear_prefix_behind_subject(answered);
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
    /// over those rows only ([`residual_rows`](residual::residual_rows)). A view
    /// only counted or windowed short of the selection's rows (`need`) leaves
    /// a compare over more rows than one piece holds pending instead
    /// ([`PendingResidual`]), compared only as far as the count or the window
    /// reaches. `None` when the residual compiles to `AlwaysFalse`.
    fn residual_narrow(
        memory: &MemoryBase,
        selection: RowSelection,
        pat: QuadPattern<'_>,
        codes: &mut PatternCodes,
        need: IdsNeed,
        t: Option<Instant>,
    ) -> Result<Option<ViewSelection>> {
        // The residual binds a subset of the roles the caller's gate
        // already resolved to codes, so the unmatchable arm cannot fire;
        // it stays for totality.
        let eqs = match codes.constraints(pat)? {
            Constraints::AlwaysFalse => return Ok(None),
            Constraints::Eq(eqs) => eqs,
        };
        if need.defers_residual(selection.len(memory.base.len())) {
            debug::step!(t, "[match_pattern] In-memory residual left pending");
            let pending = PendingResidual::new(memory.clone(), selection, eqs);
            return Ok(Some(ViewSelection::Residual(Arc::new(pending))));
        }
        let narrowed = residual::residual_rows(memory, selection, &eqs, t)?;
        Ok(Some(ViewSelection::Exact(narrowed)))
    }

    /// The file backend of [`match_base`](Self::match_base), `base` the
    /// view's file: the restrictions compose into the derived view (the
    /// subject's located run, [`locate_subject`](Self::locate_subject);
    /// index-resolved row ids; a pushed-down filter; a pruned row range), and
    /// the rows are read by the next scan. The match itself reads only the
    /// chunk probes that locate the subject's run, what an index resolution
    /// reads (its run's location, and its ids unless they stay deferred), a
    /// chained view's pending ids, the zone-map statistics that prune, and
    /// the point reads that check a pending filter at sparse ids.
    #[cfg(feature = "file-io")]
    async fn match_base_file(
        &self,
        base: &FileBase,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
        need: IdsNeed,
        t: Option<Instant>,
    ) -> Result<Self> {
        let file = &base.file;
        // The filter earlier matches left, which every selected row must
        // still pass (a window's kept masks are of the old selection).
        let existing_filter = base.filter.over_selection();
        let existing_selection = &self.quads.selection;
        // A bound subject on a sorted file resolves to its exact row range
        // first, by binary-searching the subject column's encoded chunks (the
        // file mirror of the in-memory prefix probe; every index's `choose`
        // declines a bound subject). The subject leaves the pattern, the roles
        // behind it narrow the run the same way, and whatever is still bound
        // rides the narrowed range; a decline leaves the pattern intact.
        let mut pat = pattern;
        let subject_range = Self::locate_subject(file, codes, &mut pat, t).await?;
        // With the subject already resolved to a tiny range, an index
        // resolution for the residual terms costs more than it saves (its
        // scan costs the rows matching its component), as in memory.
        let worth_indexing = subject_range
            .as_ref()
            .is_none_or(|r| (r.end - r.start) as usize >= INDEX_ROUTING_MIN_ROWS);
        // The configured indexes resolve the pattern to exact row ids, each
        // with its own probe. A resolved component stays out of the
        // pushed-down filter: its ids are exactly its matches.
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
            // The probed term is absent: nothing can match.
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
                let residual = file_exprs::build_file_filter(pat, codes)?;
                let serve = keep_serve.then_some(serve).flatten();
                // With the plan kept, the resolution is the view's sole id
                // restriction, so its deferred ids are the selection, pending
                // until a consumer needs them (a count, a chained match, a
                // delete, a base-order gather) while reads stream through the
                // plan. So are a located run's ids when the caller only
                // counts or windows the view, nothing else restricts it and
                // nothing residual is left: its width is the count, and a
                // window reads only its own rows. Any other lazy resolution
                // is read now and folds as an eager one's ids do.
                let defer = serve.is_some()
                    || (need.counts_or_windows()
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
                        (ViewSelection::Pending { .. }, true) => "resolved (served, ids pending)",
                        (ViewSelection::Pending { .. } | ViewSelection::Residual(_), _) => {
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
                (file_exprs::build_file_filter(pat, codes)?, None, None)
            }
        };
        // ANDed with the filter earlier matches left on this view, its code
        // equalities read out once for every read of the derived view.
        let filter = match (existing_filter, next_filter) {
            (Some(lhs), Some(rhs)) => Some(PushedFilter::new(and(lhs.expr().clone(), rhs))),
            (Some(lhs), None) => Some(lhs.clone()),
            (None, rhs) => rhs.map(PushedFilter::new),
        };

        let selection = match resolved_selection {
            Some(selection) => selection,
            // No index involved: the subject's exact range narrows directly
            // (a zone envelope could only be wider), else zone-map statistics
            // do. A chained match materializes a pending selection to fold
            // into.
            None => {
                let existing = existing_selection.materialized_async().await?;
                ViewSelection::Exact(match (&subject_range, &filter) {
                    (Some(_), _) => apply_subject(existing),
                    (None, Some(f)) => Self::prune_by_filter(file, existing, f.expr(), t).await?,
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

        // Nothing can match: the canonical empty view. A pending selection's
        // coverage is unknown until it materializes, possibly to empty, which
        // every consumer handles like any other narrow selection.
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
            !matches!(selection, ViewSelection::Exact(_))
        );
        // The more-restricted file view, whose rows the next scan reads. It
        // shares the file, and so the index children further chained matches
        // use; the tombstones carry over unchanged, and the reads apply them
        // (see `restrict_scan`).
        Ok(self.derived(
            self.quads
                .file_with(base, FileFilter::pending(filter), selection, serve),
            self.tail.clone(),
        ))
    }

    /// A bound subject's exact row run in a sorted file: the subject column's
    /// encoded chunks binary-searched through their cached chunk probes, then
    /// the run narrowed by the roles behind the subject in sort order through
    /// their columns' chunk probes, the file mirror of the in-memory prefix
    /// probe. The subject and every answered role leave `pat`. `None`, with
    /// `pat` intact, when no subject is bound or the location declines.
    #[cfg(feature = "file-io")]
    async fn locate_subject(
        file: &NativeStoreFile,
        codes: &mut PatternCodes,
        pat: &mut QuadPattern<'_>,
        t: Option<Instant>,
    ) -> Result<Option<Range<u64>>> {
        let Some(subject) = pat.subject else {
            return Ok(None);
        };
        let Some(mut range) = subject_runs::locate_subject_run(file, codes, subject).await? else {
            return Ok(None);
        };
        pat.subject = None;
        debug::step!(t, "[match_pattern] File subject bounded by chunk probe");
        let answered = subject_runs::narrow_subject_run(file, codes, pat, &mut range).await?;
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

    /// Whether the store holds a quad equal to `quad`, tombstoned rows counting
    /// as absent: one fully-bound `match_pattern` over the base and the tail,
    /// through whatever fast path applies, read only as far as its first row
    /// ([`exists`](Self::exists)).
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
        ResolvedRowIds::Lazy(lazy) if defer => ViewSelection::pending(lazy),
        ResolvedRowIds::Lazy(lazy) => {
            ViewSelection::Exact(existing.intersect_ids(lazy.materialized()?))
        }
        #[cfg(feature = "file-io")]
        ResolvedRowIds::Eager(ids) => ViewSelection::Exact(existing.intersect_ids(ids)),
    })
}

/// The narrowest pending run whose match, built for its rows, names its
/// pattern, so that a match chained on its view is one match of both
/// patterns ([`VortexRdfStore::match_both`]): copying the pattern's terms
/// costs about what computing a thousand of the run's ids does (a few
/// allocations, some 2,000 instructions), and a run of unknown width (a file
/// run the resolver did not locate) would cost a scan of the index child.
const CHAIN_MIN_ROWS: usize = 1_024;

/// Selection size below which a view the subject search already narrowed
/// skips index routing and filters its remaining rows column-wise (see
/// `match_base`): a lookup costs the rows matching the component it resolves,
/// not the rows the view still holds. An unrefined view never skips.
const INDEX_ROUTING_MIN_ROWS: usize = 4_096;
