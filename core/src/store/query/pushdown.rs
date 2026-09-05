//! Narrowing a view beyond a pattern: a row window (`LIMIT`/`OFFSET`, an
//! `ASK` that stops at its first row) and a per-column term-code constraint
//! (`keep`: a code set or a code range). Both fold into the view's row
//! selection, so the rows they exclude are never gathered, decoded or handed
//! across a binding boundary.

use std::sync::Arc;

#[cfg(feature = "file-io")]
use vortex_array::VortexSessionExecute;
#[cfg(feature = "file-io")]
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
#[cfg(feature = "file-io")]
use crate::session::VORTEX_SESSION;
use crate::store::array::{canonical_u32, into_struct_array, subject_sorted};
use crate::store::arrow::QuadColumn;
use crate::store::indexes::Narrow;
#[cfg(feature = "file-io")]
use crate::store::scan::file_scan;
use crate::store::view::order::{SortOrder, keep_runs, sorted_within};
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{LayoutStrategy, QuadsSource, VortexRdfStore};

/// A term-code constraint on one quad column, for [`VortexRdfStore::keep`].
#[derive(Clone, Debug)]
pub enum Keep {
    /// The codes to keep, ascending and unique — what [`Keep::set`] builds
    /// from any codes, and what a dictionary predicate scan yields.
    Set(Buffer<u32>),
    /// The half-open code range `lo..hi` to keep — what a term prefix maps
    /// to through the dictionary's `prefix_range`.
    Range(u32, u32),
}

impl Keep {
    /// A set of any codes, sorted and deduplicated here.
    pub fn set(codes: impl IntoIterator<Item = u32>) -> Self {
        let mut codes: Vec<u32> = codes.into_iter().collect();
        codes.sort_unstable();
        codes.dedup();
        Keep::Set(Buffer::from_iter(codes))
    }

    /// The half-open code range `lo..hi`.
    pub fn range(lo: u32, hi: u32) -> Self {
        Keep::Range(lo, hi)
    }

    /// Whether `code` is kept.
    #[inline]
    pub(crate) fn contains(&self, code: u32) -> bool {
        match self {
            Keep::Set(codes) => codes.as_slice().binary_search(&code).is_ok(),
            Keep::Range(lo, hi) => (*lo..*hi).contains(&code),
        }
    }
}

impl VortexRdfStore {
    /// The view over `limit` of this view's rows after the first `offset`,
    /// in the order every read yields them — live base rows in base order,
    /// then the tail's — with tombstoned rows not counted. The window folds
    /// into an exact row selection, so nothing outside it is ever read; a
    /// file view carrying a pushed-down filter first resolves the filter to
    /// its row ids (one evaluation of the filter over the selection, no
    /// column projected).
    pub async fn window(&self, offset: usize, limit: usize) -> Result<Self> {
        let (quads, base_taken, base_live) = match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                components,
                deleted,
                probes,
                canonical,
                ..
            } => {
                let selection = selection.materialized()?;
                let live = selection.live_count(deleted.as_ref(), base.len());
                let (window, taken) = selection.window(deleted.as_ref(), base.len(), offset, limit);
                (
                    QuadsSource::InMemory {
                        base: base.clone(),
                        selection: ViewSelection::Exact(window),
                        components: Arc::clone(components),
                        deleted: deleted.clone(),
                        probes: Arc::clone(probes),
                        canonical: Arc::clone(canonical),
                        serve: None,
                    },
                    taken,
                    live,
                )
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                // The rows a pushed-down filter keeps are not known from the
                // selection alone; the window needs them as ids.
                let selection = match filter {
                    None => selection,
                    Some(filter) => {
                        use vortex_mask::AllOr;
                        let matched =
                            file_scan::matching_file_rows(file, Some(filter), &selection).await?;
                        match matched.indices() {
                            AllOr::All => RowSelection::All,
                            AllOr::None => RowSelection::empty(),
                            AllOr::Some(ids) => RowSelection::Ids(Buffer::from_iter(
                                ids.iter().map(|&id| id as u64),
                            )),
                        }
                    }
                };
                let row_count = file.row_count() as usize;
                let live = selection.live_count(deleted.as_ref(), row_count);
                let (window, taken) = selection.window(deleted.as_ref(), row_count, offset, limit);
                (
                    QuadsSource::File {
                        path: path.clone(),
                        dict_max_resident_bytes: *dict_max_resident_bytes,
                        file: Arc::clone(file),
                        filter: None,
                        selection: ViewSelection::Exact(window),
                        deleted: deleted.clone(),
                        serve: None,
                    },
                    taken,
                    live,
                )
            }
        };
        // The tail takes the window's remainder: what the base's live rows
        // left of the offset, and of the limit.
        let tail = self.tail.as_ref().map(|tail| {
            let (tail_offset, tail_limit) = if base_live >= offset {
                (0, limit - base_taken)
            } else {
                (offset - base_live, limit)
            };
            let (window, _) = tail.selection.window(
                tail.deleted.as_ref(),
                tail.rows.len(),
                tail_offset,
                tail_limit,
            );
            tail.with_selection(window)
        });
        Ok(Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads,
            tail,
        })
    }

    /// Number of quads in this view, counted no further than `limit` — an
    /// `ASK` is `size_capped(1)`, and reads nothing past its first row.
    pub async fn size_capped(&self, limit: usize) -> Result<usize> {
        self.window(0, limit).await?.size().await
    }

    /// The view narrowed to the rows whose term code in `column` `keep`
    /// admits. Codes are the Dictionary layout's currency, so the store must
    /// use it, and the view's rows must all be code-addressable: a non-empty
    /// append tail (whose terms have no code) is rejected — compact first.
    ///
    /// In memory the column's codes are tested directly over the selected
    /// rows. On a file a [`Keep::Range`] becomes a pushed-down filter (the
    /// scan prunes by it) and a [`Keep::Set`] is resolved to row ids by one
    /// projected scan of the column. Either way the result composes with
    /// every other restriction the view carries.
    pub async fn keep(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        if self.layout.strategy() != LayoutStrategy::Dictionary {
            return Err(VortexRdfError::InvalidOperation(format!(
                "keep constrains term codes; the {} layout has none",
                self.layout.strategy()
            )));
        }
        if self.tail_len() != 0 {
            return Err(VortexRdfError::InvalidOperation(
                "keep constrains term codes, and the append tail's terms have none; compact \
                 the store first"
                    .to_string(),
            ));
        }
        let quads = match &self.quads {
            QuadsSource::InMemory {
                base,
                selection,
                components,
                deleted,
                probes,
                canonical,
                serve,
            } => {
                // A served run narrows in place where the plan can say how:
                // the view keeps its plan and its deferred ids, over the
                // sub-run alone.
                if let Some(plan) = serve {
                    match plan.narrow(column, keep)? {
                        Narrow::Unchanged => return Ok(self.clone()),
                        Narrow::Empty => return Ok(self.empty_view()),
                        Narrow::Run(sub) => {
                            let (plan, ids) = plan.restricted(sub)?;
                            return Ok(self.with_quads(QuadsSource::InMemory {
                                base: base.clone(),
                                selection: ViewSelection::Pending(ids),
                                components: Arc::clone(components),
                                deleted: deleted.clone(),
                                probes: Arc::clone(probes),
                                canonical: Arc::clone(canonical),
                                serve: Some(plan),
                            }));
                        }
                        Narrow::Declined => {}
                    }
                }
                let selection = selection.materialized()?;
                // A contiguous selection of the sorted base, on a column that
                // is ascending over it — `s` always, a later column while
                // every column before it is constant over the range — narrows
                // by binary search, and a range stays a range.
                let contiguous = match &selection {
                    RowSelection::All => Some(0..base.len()),
                    RowSelection::Range(range) => Some(range.start as usize..range.end as usize),
                    RowSelection::Ids(_) => None,
                };
                if let Some(range) = contiguous
                    && subject_sorted(base)
                    && sorted_within(base, probes, SortOrder::SPOG, range.clone(), column)
                        == Some(true)
                    && let Some(probe) = probes.by_name(base, column.name())
                    && let Some(runs) = keep_runs(probe, range, keep)
                {
                    let selection = match runs.as_slice() {
                        [] => RowSelection::empty(),
                        [run] => RowSelection::Range(run.start as u64..run.end as u64),
                        runs => ids_selection(
                            runs.iter()
                                .flat_map(|run| run.start as u64..run.end as u64)
                                .collect(),
                        ),
                    };
                    return Ok(self.with_quads(QuadsSource::InMemory {
                        base: base.clone(),
                        selection: ViewSelection::Exact(selection),
                        components: Arc::clone(components),
                        deleted: deleted.clone(),
                        probes: Arc::clone(probes),
                        canonical: Arc::clone(canonical),
                        serve: None,
                    }));
                }
                let struct_arr = into_struct_array(base.clone())?;
                let col = struct_arr
                    .unmasked_field_by_name(column.name())
                    .map_err(VortexRdfError::Vortex)?;
                // A canonical column (a built base) binds directly; an
                // encoded one reads through the live canonical cache —
                // shared with any holder alive, and gone again with this
                // scan when there is none.
                let codes = match canonical_u32(col) {
                    Some(prim) => prim.into_buffer::<u32>(),
                    None => canonical.column(column.index(), col)?,
                };
                let codes = codes.as_slice();
                let admits = |id: &u64| keep.contains(codes[*id as usize]);
                let ids: Vec<u64> = match &selection {
                    RowSelection::All => (0..base.len() as u64).filter(admits).collect(),
                    RowSelection::Range(range) => range.clone().filter(admits).collect(),
                    RowSelection::Ids(ids) => ids.iter().copied().filter(admits).collect(),
                };
                QuadsSource::InMemory {
                    base: base.clone(),
                    selection: ViewSelection::Exact(ids_selection(ids)),
                    components: Arc::clone(components),
                    deleted: deleted.clone(),
                    probes: Arc::clone(probes),
                    canonical: Arc::clone(canonical),
                    serve: None,
                }
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                let selection = selection.materialized_async().await?;
                let (filter, selection) = match keep {
                    Keep::Range(lo, hi) => {
                        use vortex_array::expr::{and, get_item, gt_eq, lit, lt, root};
                        let bounds = and(
                            gt_eq(get_item(column.name(), root()), lit(*lo)),
                            lt(get_item(column.name(), root()), lit(*hi)),
                        );
                        let filter = match filter {
                            Some(existing) => and(existing.clone(), bounds),
                            None => bounds,
                        };
                        (Some(filter), selection)
                    }
                    Keep::Set(_) => {
                        let ids = self.file_column_ids(file, &selection, column, keep).await?;
                        (filter.clone(), ids_selection(ids))
                    }
                };
                QuadsSource::File {
                    path: path.clone(),
                    dict_max_resident_bytes: *dict_max_resident_bytes,
                    file: Arc::clone(file),
                    filter,
                    selection: ViewSelection::Exact(selection),
                    deleted: deleted.clone(),
                    serve: None,
                }
            }
        };
        Ok(self.with_quads(quads))
    }

    /// This view over `quads` — the same layout, indexes and tail.
    fn with_quads(&self, quads: QuadsSource) -> Self {
        Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads,
            tail: self.tail.clone(),
        }
    }

    /// The base row ids among `selection` whose code in `column` `keep`
    /// admits, by one ordered scan projecting that column alone over the
    /// selection — tombstones and the view's filter are left to the reads.
    #[cfg(feature = "file-io")]
    async fn file_column_ids(
        &self,
        file: &crate::store::persist::native_file::NativeStoreFile,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<Vec<u64>> {
        use futures::StreamExt;
        use vortex_array::arrays::struct_::StructArray;

        use crate::store::array::field_as;

        let scan = self
            .restricted_file_scan_projected(file, None, selection, None, &[column.name()])?
            .with_ordered(true);
        let mut chunks = scan.into_stream().map_err(VortexRdfError::Vortex)?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let mut position = 0u64;
        let mut ids = Vec::new();
        while let Some(chunk) = chunks.next().await {
            let rows = chunk
                .map_err(VortexRdfError::Vortex)?
                .execute::<StructArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;
            let codes = field_as::<PrimitiveArray>(&rows, column.name(), &mut ctx)?;
            for (i, &code) in codes.as_slice::<u32>().iter().enumerate() {
                if keep.contains(code) {
                    ids.push(base_id(selection, position + i as u64));
                }
            }
            position += codes.len() as u64;
        }
        Ok(ids)
    }
}

/// The base row id at `position` of `selection`'s rows, in selection order.
#[cfg(feature = "file-io")]
fn base_id(selection: &RowSelection, position: u64) -> u64 {
    match selection {
        RowSelection::All => position,
        RowSelection::Range(range) => range.start + position,
        RowSelection::Ids(ids) => ids.as_slice()[position as usize],
    }
}

/// `ids` as a selection, normalized to the canonical empty one.
fn ids_selection(ids: Vec<u64>) -> RowSelection {
    if ids.is_empty() {
        RowSelection::empty()
    } else {
        RowSelection::Ids(Buffer::from_iter(ids))
    }
}
