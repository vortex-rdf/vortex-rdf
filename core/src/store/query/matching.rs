//! Pattern matching: a pattern's restrictions composed into a derived view
//! over the base and the tail.

use crate::debug;
use crate::error::{Result, VortexRdfError};
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
use crate::store::{QuadsSource, Tail};

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
use crate::store::persist::native_file::NativeStoreFile;
#[cfg(feature = "file-io")]
use std::ops::Range;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and};

use crate::store::VortexRdfStore;

impl VortexRdfStore {
    // ── pattern matching ──────────────────────────────────────────────────────

    /// Derive a view narrowed to the quads matching the pattern (`None` =
    /// free). No quads are decoded: a prefix binary search over the sorted
    /// base, a secondary index's row ids (on a file also a pushed-down filter
    /// or a pruned row range), then a column-wise scan of whatever remains;
    /// the tail is matched independently of the base. The derived view shares
    /// the base, so matches chain; every read excludes tombstoned rows.
    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let pattern = QuadPattern::new(subject, predicate, object, graph);
        let mut matched = self.match_base(pattern).await?;
        if let Some(tail) = &self.tail {
            matched.tail = Some(Self::match_tail(tail, pattern).await?);
        }
        Ok(matched)
    }

    /// `tail` narrowed to the rows matching `pattern`: a scan over its
    /// selected rows, whose surviving positions refine the tail-local
    /// selection.
    async fn match_tail(tail: &Tail, pattern: QuadPattern<'_>) -> Result<Tail> {
        let t = debug::timer();
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(tail.clone());
        }
        let mut codes = tail.layout.prepare_pattern(pattern).await?;
        let eqs = match codes.constraints(pattern)? {
            // The tail's string layouts never compile to AlwaysFalse.
            Constraints::AlwaysFalse => return Ok(tail.with_selection(RowSelection::empty())),
            Constraints::Eq(eqs) => eqs,
        };
        if eqs.is_empty() {
            return Ok(tail.clone());
        }
        let applied = tail.selection.apply(&tail.rows)?;
        // Typed path: string columns compared as raw bytes.
        if let Some(positions) = typed_positions(&applied, &eqs) {
            let mask = Mask::from_indices(applied.len(), positions);
            log::debug!(
                "[match_pattern] Tail matched by typed positions at {:?}",
                debug::elapsed(t)
            );
            return Ok(tail.with_selection(tail.selection.clone().refine(&mask)));
        }
        let mask = Self::mask_for(&applied, &eqs)?;
        let matched =
            tail.with_selection(tail.selection.clone().refine(&bool_array_to_mask(mask)?));
        log::debug!(
            "[match_pattern] Tail matched by mask scan at {:?}",
            debug::elapsed(t)
        );
        Ok(matched)
    }

    /// The pattern matched against the base alone; the tail carries over
    /// untouched.
    async fn match_base(&self, pattern: QuadPattern<'_>) -> Result<Self> {
        let t = debug::timer();
        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            log::debug!(
                "[match_pattern] Layout proved the pattern unmatchable at {:?}",
                debug::elapsed(t)
            );
            return Ok(self.empty_view());
        };
        log::debug!(
            "[match_pattern] Prepared pattern codes at {:?}",
            debug::elapsed(t)
        );
        match &self.quads {
            QuadsSource::InMemory { .. } => self.match_base_in_memory(pattern, &mut codes, t).await,
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => self.match_base_file(pattern, &mut codes, t).await,
        }
    }

    /// The pattern's bound terms resolved to codes once, for every probe of
    /// the match; `None` when a term is absent from the dictionary, which
    /// proves the pattern unmatchable.
    pub(in crate::store) async fn prepared_codes(
        &self,
        pattern: QuadPattern<'_>,
    ) -> Result<Option<PatternCodes>> {
        let codes = self.layout.prepare_pattern(pattern).await?;
        Ok((!codes.provably_empty(pattern)).then_some(codes))
    }

    /// The in-memory backend of [`match_base`](Self::match_base): the prefix
    /// probe, index routing and the residual column filter, each narrowing
    /// the selection in base row ids. Tombstones are not consulted; every
    /// read applies them, which keeps the mask scan's positions aligned with
    /// `selection.apply`.
    async fn match_base_in_memory(
        &self,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
        t: Option<Instant>,
    ) -> Result<Self> {
        #[cfg_attr(not(feature = "file-io"), allow(irrefutable_let_patterns))]
        let QuadsSource::InMemory {
            base,
            selection,
            components,
            probes,
            ..
        } = &self.quads
        else {
            unreachable!("match_base routes only InMemory sources here");
        };
        let struct_arr = into_struct_array(base.clone())?;
        let base_len = base.len();
        // A chained match materializes a pending selection.
        let selection = selection.materialized()?;
        let unrefined = matches!(selection, RowSelection::All);
        let mut pat = pattern;

        let (selection, prefix_hit) =
            Self::prefix_probe(&struct_arr, base, probes, codes, &mut pat, selection, t)?;

        // Index routing is skipped once the prefix probe cut the view below
        // INDEX_ROUTING_MIN_ROWS.
        let worth_indexing = !prefix_hit || selection.len(base_len) >= INDEX_ROUTING_MIN_ROWS;
        let mut resolved = None;
        if !selection.is_empty(base_len) && pat.any_bound() && worth_indexing {
            match resolve_indexes_in_memory(&self.indexes, components, &self.layout, pat, codes)? {
                IndexResolution::Empty => {
                    log::debug!(
                        "[match_pattern] In-memory index proved empty at {:?}",
                        debug::elapsed(t)
                    );
                    return Ok(self.empty_view());
                }
                IndexResolution::Resolved {
                    row_ids,
                    resolves,
                    serve,
                } => {
                    pat = resolves.clear(pat);
                    resolved = Some((row_ids, serve));
                    log::debug!(
                        "[match_pattern] In-memory index resolved at {:?}",
                        debug::elapsed(t)
                    );
                }
                IndexResolution::Declined => {
                    log::debug!(
                        "[match_pattern] In-memory index declined at {:?}",
                        debug::elapsed(t)
                    );
                }
            }
        }
        // The plan is kept only when the serving index's resolution is the
        // view's sole restriction; its ids then stay pending.
        let sole = unrefined && !prefix_hit && !pat.any_bound();
        let (selection, serve) = match resolved {
            Some((row_ids, candidate)) => {
                let serve = if sole { candidate } else { None };
                (
                    fold_row_ids(selection, row_ids, serve.is_some()).await?,
                    serve,
                )
            }
            None => (ViewSelection::Exact(selection), None),
        };
        let selection = match selection {
            ViewSelection::Exact(exact) if !exact.is_empty(base_len) && pat.any_bound() => {
                match Self::residual_narrow(&struct_arr, base, &exact, pat, codes, t)? {
                    Some(narrowed) => ViewSelection::Exact(narrowed),
                    None => return Ok(self.empty_view()),
                }
            }
            selection => selection,
        };

        log::debug!(
            "[match_pattern] In-memory view built (serve: {}, pending ids: {}) at {:?}",
            serve.is_some(),
            matches!(selection, ViewSelection::Pending(_)),
            debug::elapsed(t)
        );
        Ok(self.derived(
            self.quads.in_memory_with(selection, serve),
            self.tail.clone(),
        ))
    }

    /// The prefix probe over a sorted base: the subject's run by binary
    /// search of the `s` column, then each further role of the `(s, p, o, g)`
    /// order inside the run while a cached probe answers it. Answered roles
    /// leave `pat`. Returns the narrowed selection and whether it narrowed.
    fn prefix_probe<'a>(
        struct_arr: &StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        codes: &mut PatternCodes,
        pat: &mut QuadPattern<'a>,
        mut selection: RowSelection,
        t: Option<Instant>,
    ) -> Result<(RowSelection, bool)> {
        let Some(subject) = pat.subject else {
            return Ok((selection, false));
        };
        let Ok(s_col) = struct_arr.unmasked_field_by_name(schema::COL_S) else {
            return Ok((selection, false));
        };
        if !column_is_sorted(s_col) {
            return Ok((selection, false));
        }
        let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subject)) else {
            return Ok((selection, false));
        };
        let Ok(scalar) = probe.cast(s_col.dtype()) else {
            return Ok((selection, false));
        };
        // The cached probe when the column resolves one, else the per-call
        // search (also the string layouts' `VarBinView` subjects).
        let (lo, hi) = match (probes.by_name(base, schema::COL_S), u64::try_from(&scalar)) {
            (Some(owned), Ok(needle)) => owned.bounds(needle),
            _ => search_sorted_bounds(s_col, &scalar)?,
        };
        selection = selection.intersect_range(lo as u64..hi as u64);
        pat.subject = None;
        log::debug!(
            "[match_pattern] In-memory subject bounded by binary search at {:?}",
            debug::elapsed(t)
        );

        // The roles behind the subject, while the selection is one run and the
        // role has a code and a cached probe.
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
                let Some(owned) = probes.by_name(base, column) else {
                    break;
                };
                let Some(needle) = codes
                    .probe_scalar(term)?
                    .and_then(|scalar| u64::try_from(&scalar).ok())
                else {
                    break;
                };
                let (lo, hi) = owned.bounds_in(run.clone(), needle);
                run = lo..hi;
                answered += 1;
                if run.is_empty() {
                    break;
                }
            }
            if answered > 0 {
                selection = RowSelection::Range(run.start as u64..run.end as u64);
                pat.predicate = None;
                if answered > 1 {
                    pat.object = None;
                }
                if answered > 2 {
                    pat.graph = None;
                }
                log::debug!(
                    "[match_pattern] In-memory prefix of {answered} more roles bounded by binary search at {:?}",
                    debug::elapsed(t)
                );
            }
        }
        Ok((selection, true))
    }

    /// The residual column filter: the selected rows compared on every role
    /// still bound, by typed row loops over canonical code columns or a mask
    /// scan over the gathered rows. `None` when the constraints are
    /// unmatchable.
    fn residual_narrow(
        struct_arr: &StructArray,
        base: &ArrayRef,
        selection: &RowSelection,
        pat: QuadPattern<'_>,
        codes: &mut PatternCodes,
        t: Option<Instant>,
    ) -> Result<Option<RowSelection>> {
        let eqs = match codes.constraints(pat)? {
            Constraints::AlwaysFalse => return Ok(None),
            Constraints::Eq(eqs) => eqs,
        };
        Ok(Some(
            match typed_residual_ids(struct_arr, selection, base.len(), &eqs) {
                Some(ids) => {
                    log::debug!(
                        "[match_pattern] In-memory narrowed by typed residual scan at {:?}",
                        debug::elapsed(t)
                    );
                    RowSelection::Ids(ids)
                }
                None => {
                    let mask = Self::mask_for(&selection.apply(base)?, &eqs)?;
                    log::debug!(
                        "[match_pattern] In-memory narrowed by mask scan at {:?}",
                        debug::elapsed(t)
                    );
                    selection.clone().refine(&bool_array_to_mask(mask)?)
                }
            },
        ))
    }

    /// The file backend of [`match_base`](Self::match_base): the subject's
    /// located run, an index resolution's row ids and plan, and a pushed-down
    /// filter for the rest, composed into the derived view; no data is read
    /// until the next scan.
    #[cfg(feature = "file-io")]
    async fn match_base_file(
        &self,
        pattern: QuadPattern<'_>,
        codes: &mut PatternCodes,
        t: Option<Instant>,
    ) -> Result<Self> {
        let QuadsSource::File {
            file,
            filter: existing_filter,
            selection: existing_selection,
            ..
        } = &self.quads
        else {
            unreachable!("match_base routes only File sources here");
        };
        let mut pat = pattern;
        let subject_range = Self::locate_subject(file, codes, &mut pat, t).await?;
        // Index routing is skipped once the subject run is below
        // INDEX_ROUTING_MIN_ROWS.
        let worth_indexing = subject_range
            .as_ref()
            .is_none_or(|r| (r.end - r.start) as usize >= INDEX_ROUTING_MIN_ROWS);
        let resolution = if worth_indexing {
            resolve_indexes_file(&self.indexes, file, &self.layout, pat, codes).await?
        } else {
            IndexResolution::Declined
        };
        // The plan is kept only when this match is the view's sole
        // restriction.
        let keep_serve =
            existing_filter.is_none() && existing_selection.is_all() && subject_range.is_none();
        let apply_subject = |selection: RowSelection| match &subject_range {
            Some(range) => selection.intersect_range(range.clone()),
            None => selection,
        };
        let (next_filter, resolved, serve) = match resolution {
            IndexResolution::Empty => {
                log::debug!(
                    "[match_pattern] File index proved empty at {:?}",
                    debug::elapsed(t)
                );
                return Ok(self.empty_view());
            }
            // The resolved roles leave the pushed-down filter.
            IndexResolution::Resolved {
                row_ids,
                resolves,
                serve,
            } => {
                let pat = resolves.clear(pat);
                let serve = keep_serve.then_some(serve).flatten();
                let existing = existing_selection.materialized_async().await?;
                let selection = match fold_row_ids(existing, row_ids, serve.is_some()).await? {
                    ViewSelection::Exact(exact) => ViewSelection::Exact(apply_subject(exact)),
                    pending => pending,
                };
                log::debug!(
                    "[match_pattern] File index resolved (pending ids: {}) at {:?}",
                    matches!(selection, ViewSelection::Pending(_)),
                    debug::elapsed(t)
                );
                (
                    file_reads::build_file_filter(pat, codes)?,
                    Some(selection),
                    serve,
                )
            }
            IndexResolution::Declined => {
                log::debug!(
                    "[match_pattern] File index declined at {:?}",
                    debug::elapsed(t)
                );
                (file_reads::build_file_filter(pat, codes)?, None, None)
            }
        };
        let filter = match (existing_filter.clone(), next_filter) {
            (Some(lhs), Some(rhs)) => Some(and(lhs, rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, rhs) => rhs,
        };
        let selection = match resolved {
            Some(selection) => selection,
            // No index: the subject's exact range narrows directly, else the
            // combined filter's zone-map envelope.
            None => {
                let existing = existing_selection.materialized_async().await?;
                ViewSelection::Exact(match (&subject_range, &filter) {
                    (Some(_), _) => apply_subject(existing),
                    (None, Some(filter)) => {
                        Self::prune_by_filter(file, existing, filter, t).await?
                    }
                    (None, None) => existing,
                })
            }
        };
        if let ViewSelection::Exact(exact) = &selection
            && exact.is_empty(file.row_count() as usize)
        {
            log::debug!(
                "[match_pattern] File selection proved empty at {:?}",
                debug::elapsed(t)
            );
            return Ok(self.empty_view());
        }

        log::debug!(
            "[match_pattern] File view built (filter: {}, serve: {}, pending ids: {}) at {:?}",
            filter.is_some(),
            serve.is_some(),
            matches!(selection, ViewSelection::Pending(_)),
            debug::elapsed(t)
        );
        Ok(self.derived(
            self.quads.file_with(filter, selection, serve),
            self.tail.clone(),
        ))
    }

    /// The exact run of a bound subject in a sorted file, located through the
    /// `s` column's chunk probes; a located subject leaves `pat`. `None` when
    /// no subject is bound or the location declines.
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
        let range = file_reads::locate_subject_run(file, codes, subject).await?;
        if range.is_some() {
            pat.subject = None;
            log::debug!(
                "[match_pattern] File subject bounded by chunk probe at {:?}",
                debug::elapsed(t)
            );
        }
        Ok(range)
    }

    /// `existing` narrowed to the zone-map envelope of `filter`.
    #[cfg(feature = "file-io")]
    async fn prune_by_filter(
        file: &NativeStoreFile,
        existing: RowSelection,
        filter: &Expression,
        t: Option<Instant>,
    ) -> Result<RowSelection> {
        let pruned = file_filter::row_range_from_pruning(file, filter).await?;
        log::debug!(
            "[match_pattern] File narrowed by zone-map pruning (range: {}) at {:?}",
            pruned.is_some(),
            debug::elapsed(t)
        );
        Ok(match pruned {
            Some(range) => existing.intersect_range(range),
            None => existing,
        })
    }

    // ── pattern matching helpers ─────────────────────────────────────────────

    /// A boolean mask over `array`'s rows, in its own order, marking the rows
    /// satisfying `eqs`. Positional: a view translates it back to base row
    /// ids through [`RowSelection::refine`]. Callers compile and pre-check the
    /// constraints.
    fn mask_for(array: &ArrayRef, eqs: &[(&'static str, Scalar)]) -> Result<ArrayRef> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = array
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let mut mask: Option<ArrayRef> = None;
        for (field, value) in eqs {
            let col = struct_arr
                .unmasked_field_by_name(field)
                .map_err(VortexRdfError::Vortex)?;
            let scalar = value.cast(col.dtype()).map_err(VortexRdfError::Vortex)?;
            let rhs = ConstantArray::new(scalar, col.len()).into_array();
            let m = col
                .binary(rhs, Operator::Eq)
                .map_err(VortexRdfError::Vortex)?;
            mask = Some(match mask.take() {
                Some(prev) => prev
                    .binary(m, Operator::And)
                    .map_err(VortexRdfError::Vortex)?,
                None => m,
            });
        }
        Ok(match mask {
            Some(mask) => mask,
            // An unconstrained pattern matches every row.
            None => ConstantArray::new(Scalar::from(true), struct_arr.len()).into_array(),
        })
    }

    /// Whether the store holds a quad equal to `quad` (tombstoned rows count
    /// as absent): one fully-bound `match_pattern`, read as far as its first
    /// row.
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

/// `existing` narrowed by an index resolution's ids. A lazy resolution stays
/// pending while `served`: the plan reads without the ids.
async fn fold_row_ids(
    existing: RowSelection,
    row_ids: ResolvedRowIds,
    served: bool,
) -> Result<ViewSelection> {
    Ok(match row_ids {
        ResolvedRowIds::Lazy(lazy) if served => ViewSelection::Pending(lazy),
        ResolvedRowIds::Lazy(lazy) => {
            ViewSelection::Exact(existing.intersect_ids(lazy.materialized_async().await?))
        }
        ResolvedRowIds::Eager(ids) => ViewSelection::Exact(existing.intersect_ids(ids)),
    })
}

/// Selection size below which an already narrowed view skips secondary index
/// routing and filters its rows column-wise. An unrefined view never skips.
const INDEX_ROUTING_MIN_ROWS: usize = 4_096;
