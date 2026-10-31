//! The in-memory backend of a base match: the prefix probe over the
//! sorted base, secondary-index routing, and the residual column filter,
//! each narrowing the selection in base row ids.

use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use web_time::Instant;

use super::{INDEX_ROUTING_MIN_ROWS, IdsNeed, fold_row_ids};
use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::store::array::{
    column_is_sorted, into_struct_array, search_sorted_bounds, unsigned_value,
};
use crate::store::indexes::{IndexResolution, resolve_indexes_in_memory};
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, TermRef};
use crate::store::probes::StructProbes;
use crate::store::scan::residual;
use crate::store::schema;
use crate::store::view::pending::PendingResidual;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{MemoryBase, VortexRdfStore};

impl VortexRdfStore {
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
    pub(super) fn match_base_in_memory(
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
    pub(super) fn prefix_probe(
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
    pub(super) fn residual_narrow(
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
}
