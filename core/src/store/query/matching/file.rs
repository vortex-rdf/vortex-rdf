//! The file backend of a base match: the subject's located run, index
//! resolutions, a pushed-down filter and a pruned row range, composed into
//! the derived view whose next scan reads the rows.

use std::ops::Range;

use vortex_array::expr::{Expression, and};
use web_time::Instant;

use super::{INDEX_ROUTING_MIN_ROWS, IdsNeed, fold_row_ids};
use crate::debug;
use crate::error::Result;
use crate::store::indexes::{IndexResolution, ResolvedRowIds, resolve_indexes_file};
use crate::store::layouts::{PatternCodes, QuadPattern};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::PushedFilter;
use crate::store::scan::{file_exprs, file_filter, file_reads, subject_runs};
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{FileBase, FileFilter, VortexRdfStore};

impl VortexRdfStore {
    /// The file backend of [`match_base`](Self::match_base), `base` the
    /// view's file: the restrictions compose into the derived view (the
    /// subject's located run, [`locate_subject`](Self::locate_subject);
    /// index-resolved row ids; a pushed-down filter; a pruned row range), and
    /// the rows are read by the next scan. The match itself reads only the
    /// chunk probes that locate the subject's run, what an index resolution
    /// reads (its run's location, and its ids unless they stay deferred), a
    /// chained view's pending ids, the zone-map statistics that prune, and
    /// the point reads that check a pending filter at sparse ids.
    pub(super) async fn match_base_file(
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
    pub(super) async fn locate_subject(
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
    pub(super) async fn prune_by_filter(
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
}
