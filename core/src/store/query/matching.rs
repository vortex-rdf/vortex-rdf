//! Pattern matching: composing a pattern's restrictions into derived views
//! over the base and the tail.

use crate::debug;
use crate::error::Result;
use crate::io::zone_map::ZONE_ROWS;
use crate::session::VORTEX_SESSION;
use crate::store::array::{
    bool_array_to_mask, column_is_sorted, into_struct_array, search_sorted_bounds, unsigned_value,
};
#[cfg(feature = "file-io")]
use crate::store::indexes::resolve_indexes_file;
use crate::store::indexes::{
    IndexResolution, LazyRowIds, ResolvedRowIds, resolve_indexes_in_memory,
};
use crate::store::layouts::{Constraints, OwnedQuadPattern, PatternCodes, QuadPattern, TermRef};
use crate::store::probes::StructProbes;
use crate::store::query::residual::PendingResidual;
use crate::store::scan::typed_eq::{typed_positions, typed_residual_ids};
#[cfg(feature = "file-io")]
use crate::store::scan::{file_exprs, file_filter, file_reads, subject_runs};
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
    /// layout: a scan over its selected rows (a tail is small, unsorted and
    /// unindexed), whose surviving positions refine the tail-local selection
    /// as base matches refine the base's.
    async fn match_tail(tail: &Tail, pattern: QuadPattern<'_>) -> Result<Tail> {
        let t = debug::timer();
        let carry = |selection: RowSelection| tail.with_selection(selection);
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(carry(tail.selection.clone()));
        }
        // The tail's layouts store plain strings (see `Tail::layout_for`), so
        // this prelude resolves nothing and never suspends; it still mints the
        // witness, as for every probe surface.
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
        // A fully bound pattern (every `contains`, so every presence check
        // `add_quads` makes) reads the rows its key names off the tail's
        // keys, instead of comparing every selected row.
        if let Some(rows) = Self::keyed_tail_rows(tail, &eqs)? {
            debug::step!(t, "[match_pattern] Tail matched by its row keys");
            return Ok(carry(rows));
        }
        #[cfg(test)]
        tail_hooks::note_scanned();
        let applied = tail.selection.apply(&tail.rows)?;
        // Typed fast path first: the tail's string columns compared as raw
        // bytes, without the per-call compare/canonicalize pipeline; every
        // pattern the keys cannot answer rides it.
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

    /// The selected tail rows that hold every equality of `eqs`, read off the
    /// tail's keys ([`TailKeys`](crate::store::view::tail_keys::TailKeys)):
    /// the rows the key of `eqs` names, each checked against every equality,
    /// so a row that only shares the key is not matched. `None` when the tail
    /// has no keys or `eqs` leave a key column free (a pattern not fully
    /// bound), which the scan answers.
    fn keyed_tail_rows(
        tail: &Tail,
        eqs: &[(&'static str, Scalar)],
    ) -> Result<Option<RowSelection>> {
        let Some(candidates) = tail.keys.as_ref().and_then(|keys| keys.candidates(eqs)) else {
            return Ok(None);
        };
        #[cfg(test)]
        tail_hooks::note_keyed();
        let mut rows = Vec::with_capacity(candidates.len());
        for row in candidates {
            if tail.selection.contains(row) && Self::tail_row_holds(&tail.rows, row, eqs)? {
                rows.push(row);
            }
        }
        Ok(Some(RowSelection::from_ids(vortex_buffer::Buffer::from(
            rows,
        ))))
    }

    /// Whether row `row` of the tail `rows` holds every equality of `eqs`,
    /// compared as the scan compares it.
    fn tail_row_holds(
        rows: &ArrayRef,
        row: schema::RowId,
        eqs: &[(&'static str, Scalar)],
    ) -> Result<bool> {
        let row = row as usize;
        let one = rows.slice(row..row + 1)?;
        Ok(bool_array_to_mask(Self::mask_for(&one, eqs)?)?.all_true())
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately). `need` is the caller's use of the view, which
    /// decides whether a resolution's ids may stay deferred.
    async fn match_base(&self, pattern: QuadPattern<'_>, need: IdsNeed) -> Result<Self> {
        // A view whose base still holds the pending matches of one pattern
        // over every row is matched as one match of both patterns, which
        // never computes those ids.
        if let ViewSelection::Pending(lazy) = &self.quads.selection
            && let Some(first) = lazy.matched_pattern()
            && let Some(both) = first.and(pattern)
        {
            return self.match_both(both, need).await;
        }
        let t = debug::timer();

        let Some(mut codes) = self.prepared_codes(pattern).await? else {
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
            && let ViewSelection::Pending(lazy) = &mut matched.quads.selection
            && lazy
                .len_if_known()
                .is_none_or(|rows| rows >= CHAIN_MIN_ROWS)
        {
            lazy.set_matched_pattern(OwnedQuadPattern::of(pattern));
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
        #[cfg(test)]
        chain_hooks::note_merged();
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

    /// The match prelude: every bound term of `pattern` resolved to its code
    /// at most once, the witness every synchronous probe of the match runs
    /// on, and the one point where a dictionary may do I/O (a file-backed one
    /// reads here). `None` when the layout proves the pattern unmatchable (a
    /// term absent from the dictionary, say), read off the prelude's role
    /// cache: such a pattern needs no search and no scan on either backend.
    pub(in crate::store) async fn prepared_codes(
        &self,
        pattern: QuadPattern<'_>,
    ) -> Result<Option<PatternCodes>> {
        let codes = self.layout.prepare_pattern(pattern).await?;
        Ok((!codes.provably_empty(pattern)).then_some(codes))
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
    /// over those rows only ([`residual_rows`](Self::residual_rows)). A view
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
        if PendingResidual::defers(need, selection.len(memory.base.len())) {
            debug::step!(t, "[match_pattern] In-memory residual left pending");
            let pending = PendingResidual::new(memory.clone(), selection, eqs);
            return Ok(Some(ViewSelection::Pending(LazyRowIds::from_residual(
                pending,
            ))));
        }
        let narrowed = Self::residual_rows(memory, selection, &eqs, t)?;
        Ok(Some(ViewSelection::Exact(narrowed)))
    }

    /// The rows of `selection` (of `memory`'s base) where every equality of
    /// `eqs` holds: the zones whose code bounds cannot hold a constrained
    /// code skipped, then the typed fast path or the mask scan over the rows
    /// left.
    pub(crate) fn residual_rows(
        memory: &MemoryBase,
        selection: RowSelection,
        eqs: &[(&'static str, Scalar)],
        t: Option<Instant>,
    ) -> Result<RowSelection> {
        let MemoryBase {
            base,
            probes,
            zones,
            ..
        } = memory;
        let struct_arr = into_struct_array(base.clone())?;
        // The zones whose code bounds cannot hold a constrained code are
        // skipped, as a file's zone maps skip them. Every row or a range has
        // the zone runs left inside it compared run by run, each a slice of
        // the base, with no id list of the rows left (those runs are many
        // where a code's zones scatter); an id list keeps only its ids in them.
        let mut selection = selection;
        if selection.len(base.len()) > ZONE_ROWS
            && let Some(runs) = zones.runs_holding(eqs, base.len())
        {
            #[cfg(test)]
            zone_hooks::note_narrowed();
            debug::step!(t, "[match_pattern] In-memory narrowed by zone bounds");
            match selection.clip_runs(&runs) {
                Some(runs) => {
                    let ids = Self::runs_matching(base, &runs, eqs)?;
                    return Ok(RowSelection::from_ids(vortex_buffer::Buffer::from(ids)));
                }
                None => selection = selection.intersect_runs(&runs),
            }
            if selection.is_empty(base.len()) {
                return Ok(selection);
            }
        }
        // Typed fast path: residual equalities over canonical code columns
        // (Dictionary layout) become slice loops yielding exact base ids,
        // with no slice/compare/mask pipeline, and a probe-bound column reads
        // through the store's own probe. Only the selection's rows are
        // tested, so the ids are already the intersection.
        Ok(
            match typed_residual_ids(
                &struct_arr,
                &selection,
                base.len(),
                eqs,
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
                    let mask = Self::mask_for(&selection.apply(base)?, eqs)?;
                    let narrowed = selection.refine(&bool_array_to_mask(mask)?);
                    debug::step!(t, "[match_pattern] In-memory narrowed by mask scan");
                    narrowed
                }
            },
        )
    }

    /// The base rows of `runs` that satisfy `eqs`, ascending: each run's
    /// slice of the base compared on the constrained columns alone
    /// ([`mask_for`](Self::mask_for)).
    fn runs_matching(
        base: &ArrayRef,
        runs: &[std::ops::Range<u64>],
        eqs: &[(&'static str, Scalar)],
    ) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        for run in runs {
            let rows = base.slice(run.start as usize..run.end as usize)?;
            match bool_array_to_mask(Self::mask_for(&rows, eqs)?)?.bit_buffer() {
                vortex_mask::AllOr::All => ids.extend(run.clone()),
                vortex_mask::AllOr::None => {}
                vortex_mask::AllOr::Some(bits) => {
                    bits.for_each_set_index(|i| ids.push(run.start + i as u64));
                }
            }
        }
        Ok(ids)
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
        let FileBase {
            file,
            filter: existing_filter,
            ..
        } = base;
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
                (file_exprs::build_file_filter(pat, codes)?, None, None)
            }
        };
        // ANDed with the filter earlier matches left on this view.
        let filter = match (existing_filter.clone(), next_filter) {
            (Some(lhs), Some(rhs)) => Some(and(lhs, rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, rhs) => rhs,
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
            matches!(selection, ViewSelection::Pending(_))
        );
        // The more-restricted file view, whose rows the next scan reads. It
        // shares the file, and so the index children further chained matches
        // use; the tombstones carry over unchanged, and the reads apply them
        // (see `restrict_scan`).
        Ok(self.derived(
            self.quads.file_with(base, filter, selection, serve),
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

    /// A boolean mask, one bit per row of `array` in its own order, of the
    /// rows that satisfy `eqs`: the per-column equalities a pattern compiled
    /// to under the layout of `array`'s rows (the store's for the base, the
    /// tail's for the tail; see [`Tail::layout_for`]). The mask is
    /// positional, so a caller holding a view maps it back to base row ids
    /// with [`RowSelection::refine`]. The fallback of the typed comparison
    /// paths: callers pass in the constraints they compiled to try those.
    fn mask_for(array: &ArrayRef, eqs: &[(&'static str, Scalar)]) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = array.clone().execute::<StructArray>(&mut ctx)?;

        // One equality comparison per constrained column, ANDed.
        let mut mask: Option<ArrayRef> = None;
        for (field, value) in eqs {
            let col = struct_arr.unmasked_field_by_name(field)?;
            // Cast to the column's dtype, so a numeric column such as
            // `o_kind` compares against a scalar of its type and nullability.
            let scalar = value.cast(col.dtype())?;
            let rhs = ConstantArray::new(scalar, col.len()).into_array();
            let m = col.binary(rhs, Operator::Eq)?;
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
        ResolvedRowIds::Lazy(lazy) if defer => ViewSelection::Pending(lazy),
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

/// Test hook: how many chained matches this thread answered as one match of
/// both patterns ([`VortexRdfStore::match_both`]).
#[cfg(test)]
pub(crate) mod chain_hooks {
    use std::cell::Cell;

    thread_local! {
        static MERGED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one chained match answered as one match of both patterns.
    pub(super) fn note_merged() {
        MERGED.set(MERGED.get() + 1);
    }

    /// The chained matches answered so on this thread so far.
    pub(crate) fn merged() -> usize {
        MERGED.get()
    }
}

/// Test hook: how many tail matches this thread answered off the tail's keys,
/// and how many by comparing the tail's selected rows.
#[cfg(test)]
pub(crate) mod tail_hooks {
    use std::cell::Cell;

    thread_local! {
        static KEYED: Cell<usize> = const { Cell::new(0) };
        static SCANNED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one tail match answered off the keys.
    pub(super) fn note_keyed() {
        KEYED.set(KEYED.get() + 1);
    }

    /// Record one tail match answered by comparing rows.
    pub(super) fn note_scanned() {
        SCANNED.set(SCANNED.get() + 1);
    }

    /// The tail matches answered off the keys on this thread so far.
    pub(crate) fn keyed() -> usize {
        KEYED.get()
    }

    /// The tail matches answered by comparing rows on this thread so far.
    pub(crate) fn scanned() -> usize {
        SCANNED.get()
    }
}

/// Test hook: how many in-memory residual filters this thread narrowed to
/// the zones their codes' bounds hold.
#[cfg(test)]
pub(crate) mod zone_hooks {
    use std::cell::Cell;

    thread_local! {
        static NARROWED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one residual filter narrowed by zone bounds.
    pub(super) fn note_narrowed() {
        NARROWED.set(NARROWED.get() + 1);
    }

    /// The residual filters narrowed by zone bounds on this thread so far.
    pub(crate) fn narrowed() -> usize {
        NARROWED.get()
    }
}
