//! [`InMemoryServePlan`]: a resolved in-memory view's quads sliced out of
//! the answering index component's matched run, or read point by point
//! when the run is small.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use super::{LiveRows, ServeDecode, ServedRows, ServedSpan};
use crate::error::Result;
use crate::observe::observe;
use crate::store::SortOrder;
use crate::store::array::into_struct_array;
use crate::store::indexes::{COL_RID, ComponentRun};
use crate::store::layouts::ChunkDecode;
use crate::store::probes::StructProbes;
use crate::store::schema::{PRIMARY_COLUMNS, RowId, TermCode};
use crate::store::view::selection::point_sized;
use vortex_rdf_encoded_search::SortedProbe;

/// An index's serving plan for an in-memory view: the matched rows are the
/// contiguous run `range` of the index component's own array, bounded by a
/// binary search over its sorted lead column, so `quads()` slices them from
/// the component (an `Arc` bump) or point-reads a small run, instead of
/// gathering the primary columns at scattered row ids. Every read of the
/// view follows the component's order. `MemoryBase` carries only this type;
/// `FileServePlan` (in `file.rs`) is the file-backed half.
#[derive(Clone)]
pub(crate) struct InMemoryServePlan {
    decode: ServeDecode,
    /// The serving component's name (`index:posg`, `index:ospg`).
    component: &'static str,
    /// The index component's rows, in child schema.
    array: ArrayRef,
    /// The rows of the component the match resolved: the run.
    run: Range<u64>,
    /// The rows of the run the view covers: the run, or what a narrowing
    /// left of it.
    span: ServedSpan,
    /// Whether a narrowing (a window, keep, partition or chained match) cut
    /// the view's rows ([`is_narrowed`](Self::is_narrowed)).
    narrowed: bool,
    /// The run's row ids, shared with the view's lazy ids, so they decode
    /// once for the plan's reads and the ids alike.
    ids: Arc<ComponentRun>,
    /// The component's shared probe cache, so a small run reads
    /// point-by-point at its global positions instead of slicing (a slice's
    /// probe would be re-resolved per call).
    probes: Arc<StructProbes>,
}

impl InMemoryServePlan {
    /// A plan serving the contiguous `range` of the rows `array` of the
    /// in-memory index component named `component`, decoded through
    /// `decode`; `ids` is the run's row ids, which the view's lazy ids share.
    pub(in crate::store::indexes) fn new(
        decode: ServeDecode,
        component: &'static str,
        array: ArrayRef,
        range: Range<usize>,
        ids: Arc<ComponentRun>,
        probes: Arc<StructProbes>,
    ) -> Self {
        let run = range.start as u64..range.end as u64;
        Self {
            decode,
            component,
            array,
            span: ServedSpan::Range(run.clone()),
            run,
            narrowed: false,
            ids,
            probes,
        }
    }

    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The order the view's reads follow ([`ServeDecode::order`]).
    pub(crate) fn read_order(&self) -> SortOrder {
        self.decode.order
    }

    /// The decode the plan's reads take, tombstones included.
    pub(crate) fn decoder(&self) -> &ServeDecode {
        &self.decode
    }

    /// How many rows of the component the view covers: the run's, or what a
    /// narrowing left of it, tombstoned rows in a range included.
    pub(crate) fn span_len(&self) -> usize {
        self.span.len()
    }

    /// Whether a narrowing (a window, keep, partition or chained match) left
    /// the view only part of its match's run.
    pub(crate) fn is_narrowed(&self) -> bool {
        self.narrowed
    }

    /// This plan over `span`, what a narrowing left of the view's rows.
    #[inline]
    pub(crate) fn with_span(&self, span: ServedSpan) -> Self {
        Self {
            decode: self.decode.clone(),
            component: self.component,
            array: self.array.clone(),
            run: self.run.clone(),
            span,
            narrowed: true,
            ids: Arc::clone(&self.ids),
            probes: Arc::clone(&self.probes),
        }
    }

    /// The view's live rows `admit` keeps, in the component's order: each
    /// row's position in the component and base row id offered to `admit`,
    /// the `deleted` tombstones skipped. What a keep of the view keeps.
    pub(crate) fn kept_rows(
        &self,
        deleted: Option<&Mask>,
        mut admit: impl FnMut(u64, RowId) -> bool,
    ) -> Result<ServedRows> {
        Ok(match &self.span {
            // Kept rows are live: only `admit` tests them.
            ServedSpan::Kept(kept) => {
                ServedRows::collect(kept.positions_and_rids(), kept.len(), |(position, rid)| {
                    admit(position, rid).then_some((position, rid))
                })
            }
            ServedSpan::Range(range) => {
                let run = self.run_rids()?;
                let rows = range.clone().zip(run[self.slots(range)].iter().copied());
                let len = (range.end - range.start) as usize;
                match deleted {
                    None => ServedRows::collect(rows, len, |(position, rid)| {
                        admit(position, rid).then_some((position, rid))
                    }),
                    Some(deleted) => ServedRows::collect(rows, len, |(position, rid)| {
                        (!deleted.value(rid as usize) && admit(position, rid))
                            .then_some((position, rid))
                    }),
                }
            }
        })
    }

    /// The view's live rows where `holds` holds, in the component's order:
    /// what a pattern chained on the view keeps. `holds` tests the
    /// component's `columns` over every row the view covers (a slice of a
    /// range, its tombstoned rows among them, or one take at the kept rows'
    /// positions), the other columns never read; the rows are then kept in
    /// one pass, as [`kept_rows`](Self::kept_rows) keeps them.
    pub(crate) fn rows_where(
        &self,
        deleted: Option<&Mask>,
        columns: &[&'static str],
        holds: impl FnOnce(&ArrayRef) -> Result<Mask>,
    ) -> Result<ServedRows> {
        let rows = into_struct_array(self.array.clone())?;
        let kept = match &self.span {
            ServedSpan::Kept(kept) => Some(
                PrimitiveArray::new(kept.positions.clone(), Validity::NonNullable).into_array(),
            ),
            ServedSpan::Range(_) => None,
        };
        let fields = columns
            .iter()
            .map(|name| {
                let column = rows.unmasked_field_by_name(name)?;
                Ok(match (&self.span, &kept) {
                    (ServedSpan::Range(range), _) => {
                        column.slice(range.start as usize..range.end as usize)?
                    }
                    (_, Some(positions)) => column.take(positions.clone())?,
                    (ServedSpan::Kept(_), None) => unreachable!("kept rows take their positions"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let read = StructArray::try_new(
            FieldNames::from(columns),
            fields,
            self.span.len(),
            Validity::NonNullable,
        )?;
        let holding = holds(&read.into_array())?;
        Ok(match &self.span {
            ServedSpan::Kept(kept) => ServedRows::collect(
                kept.positions_and_rids().zip(0..),
                kept.len(),
                |(row, i)| holding.value(i).then_some(row),
            ),
            ServedSpan::Range(range) => {
                let start = range.start;
                self.kept_rows(deleted, |position, _| {
                    holding.value((position - start) as usize)
                })?
            }
        })
    }

    /// The serving component's `name` column through its cached probe, which
    /// reads it at the rows' positions; `None` when no probe resolves it.
    pub(crate) fn component_probe(&self, name: &str) -> Option<&SortedProbe<'_>> {
        Some(self.probes.by_name(&self.array, name).flatten()?.probe())
    }

    /// The run's base row ids in the component's order, decoded once for
    /// the plan and the view's lazy ids ([`ComponentRun::run_order`]).
    fn run_rids(&self) -> Result<Buffer<RowId>> {
        self.ids.run_order()
    }

    /// The base row ids of the component rows `range` (a part of the run), in
    /// its order: a slice of the run's ids, shared. What a window or
    /// partition of a range holds.
    #[inline]
    pub(crate) fn range_rids(&self, range: Range<u64>) -> Result<Buffer<RowId>> {
        Ok(self.run_rids()?.slice(self.slots(&range)))
    }

    /// The base row ids of the view's rows in the component's order, less
    /// the `deleted` tombstones: what a read gathers off the base in the
    /// index's order. A slice of the run's own ids, shared, when nothing is
    /// tombstoned.
    pub(crate) fn live_rids(&self, deleted: Option<&Mask>) -> Result<Buffer<RowId>> {
        let range = match &self.span {
            ServedSpan::Kept(kept) => return Ok(kept.rids.clone()),
            ServedSpan::Range(range) => range,
        };
        let rids = self.run_rids()?.slice(self.slots(range));
        Ok(match deleted {
            None => rids,
            Some(deleted) => rids
                .iter()
                .copied()
                .filter(|&rid| !deleted.value(rid as usize))
                .collect(),
        })
    }

    /// Where the component rows `range` sit in the run: the slots of the
    /// run's ids they cover.
    fn slots(&self, range: &Range<u64>) -> Range<usize> {
        (range.start - self.run.start) as usize..(range.end - self.run.start) as usize
    }

    /// The rows a point read of the view takes through the component's
    /// cached probes: a small range read whole when nothing is tombstoned,
    /// else the positions of its live rows (tombstones are defined over
    /// primary row ids, and the rid column says which primary row each
    /// served row mirrors; only a tombstoned view pays for the liveness
    /// pass), or the kept rows, which are live. `None` declines: more than
    /// [`POINT_GATHER_MAX_ROWS`] rows, a rid column no probe resolves, or a
    /// row id no `usize` holds.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    fn point_rows(&self, deleted: Option<&Mask>) -> Option<PointRows> {
        let range = match &self.span {
            ServedSpan::Kept(kept) => {
                return point_sized(kept.len() as u64).then(|| {
                    PointRows::At(
                        kept.positions
                            .iter()
                            .map(|&position| position as usize)
                            .collect(),
                    )
                });
            }
            ServedSpan::Range(range) => range.start as usize..range.end as usize,
        };
        if !point_sized(range.len() as u64) {
            return None;
        }
        let Some(deleted) = deleted else {
            return Some(PointRows::Range(range));
        };
        let rid = self.probes.by_name(&self.array, COL_RID).flatten()?.probe();
        let mut rids = vec![0; range.len()];
        rid.read_into(range.start, &mut rids);
        // A row id no `usize` holds declines to the slice path, whose
        // decode reports it.
        let mut live = Vec::with_capacity(range.len());
        for (pos, rid) in range.zip(rids) {
            let row =
                crate::store::indexes::row_index(crate::store::indexes::base_row(rid)).ok()?;
            if !deleted.value(row) {
                live.push(pos);
            }
        }
        Some(PointRows::At(live))
    }

    /// A small view's live rows read at their global positions through the
    /// component's cached probes, with no slice: the four `(s, p, o, g)`
    /// columns, each as the `u64`s its probe reads, which are the child's
    /// codes (a code column is `u64`, and a probe reads every width as one),
    /// each run of consecutive live positions in one pass. The point read
    /// behind both the decoded and the code-payload serve. `None` declines (a
    /// wide view, or a column no probe resolves, such as a string copy's);
    /// the caller keeps its slice or take path.
    fn live_point_columns(&self, deleted: Option<&Mask>) -> Option<[Buffer<TermCode>; 4]> {
        let rows = self.point_rows(deleted)?;
        let read = |name: &str| -> Option<Buffer<TermCode>> {
            let probe = self.probes.by_name(&self.array, name).flatten()?.probe();
            Some(match &rows {
                PointRows::Range(range) => {
                    let mut codes = vec![0; range.len()];
                    probe.read_into(range.start, &mut codes);
                    Buffer::from(codes)
                }
                PointRows::At(positions) => {
                    let mut codes = vec![0; positions.len()];
                    probe.read_at(positions, &mut codes);
                    Buffer::from(codes)
                }
            })
        };
        let [s, p, o, g] = PRIMARY_COLUMNS;
        Some([read(s)?, read(p)?, read(o)?, read(g)?])
    }

    /// A small run's live rows ([`live_point_columns`](Self::live_point_columns))
    /// as a primary-named `(s, p, o, g)` canonical struct. `Ok(None)`
    /// declines; the caller keeps the slice path.
    fn point_read_run_rows(&self, deleted: Option<&Mask>) -> Result<Option<ArrayRef>> {
        let Some(columns) = self.live_point_columns(deleted) else {
            return Ok(None);
        };
        observe!(ServePointRead);
        let len = columns[0].len();
        Ok(Some(
            StructArray::try_new(
                FieldNames::from(PRIMARY_COLUMNS),
                Vec::from(columns.map(IntoArray::into_array)),
                len,
                Validity::NonNullable,
            )?
            .into_array(),
        ))
    }

    /// The served rows' four [`TermCode`] columns, read off the index
    /// component's own columns: the code-payload counterpart of
    /// [`decode`](Self::decode), in the index's order as `decode` serves
    /// them. Under the Dictionary layout a copy holds the view's codes
    /// contiguously, so no row id is materialized and no primary column
    /// gathered: a run of at most [`POINT_GATHER_MAX_ROWS`] rows is read
    /// point by point through the component's cached probes, and any other
    /// is the component sliced to the run, each column decoded whole.
    ///
    /// Under a Dictionary decode layout only ([`ServeDecode::decodes_codes`]):
    /// the string layouts' copies hold terms, not codes.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn code_columns(&self, deleted: Option<&Mask>) -> Result<[Buffer<TermCode>; 4]> {
        if let Some(columns) = self.live_point_columns(deleted) {
            return Ok(columns);
        }
        let (rows, deleted) = self.covered_rows(deleted)?;
        self.decode.chunk_code_columns(&rows, deleted)
    }

    /// The matched rows decoded from the index component's own rows as the
    /// primary `(s, p, o, g)`: point reads at the run's global positions
    /// through the cached probes when the run is small, else the component
    /// sliced to the run.
    pub(crate) fn decode<T: ChunkDecode>(&self, deleted: Option<&Mask>) -> Vec<Result<T>> {
        match self.point_read_run_rows(deleted) {
            Ok(Some(rows)) => return T::decode(&self.decode.decode_layout, &rows),
            Ok(None) => {}
            Err(e) => return vec![Err(e)],
        }
        match self.covered_rows(deleted) {
            Ok((rows, deleted)) => self.decode.decode_columns(&rows, deleted),
            Err(e) => vec![Err(e)],
        }
    }

    /// The component's rows the view covers, in its order, beside the
    /// tombstones its decode still drops: a slice of the range, under
    /// `deleted`, or one take at the kept rows' positions, which are live.
    fn covered_rows<'a>(&self, deleted: Option<&'a Mask>) -> Result<(ArrayRef, Option<&'a Mask>)> {
        Ok(match &self.span {
            ServedSpan::Range(range) => (
                self.array.slice(range.start as usize..range.end as usize)?,
                deleted,
            ),
            ServedSpan::Kept(kept) => {
                let indices =
                    PrimitiveArray::new(kept.positions.clone(), Validity::NonNullable).into_array();
                (self.array.take(indices)?, None)
            }
        })
    }

    /// The view's live rows in the component's order ([`LiveRows`]): a range
    /// with nothing tombstoned as it is, else the live rows listed, each
    /// beside its base row id. What a narrowing of the view cuts, and a
    /// stream of it reads a window at a time
    /// ([`window_rows`](Self::window_rows)).
    #[inline]
    pub(crate) fn live(&self, deleted: Option<&Mask>) -> Result<LiveRows> {
        Ok(match (&self.span, deleted) {
            (ServedSpan::Kept(kept), _) => LiveRows::Kept((**kept).clone()),
            (ServedSpan::Range(range), None) => LiveRows::Range(range.clone()),
            (ServedSpan::Range(_), Some(_)) => {
                LiveRows::Spanned(self.kept_rows(deleted, |_, _| true)?)
            }
        })
    }

    /// The view's live rows `window` (positions in `rows`, as
    /// [`live`](Self::live) lists them) off
    /// the component's own columns, as a primary-named `(s, p, o, g)`
    /// struct: a slice of the run, or one take at their positions. What a
    /// served stream reads a chunk at a time when the base's columns are
    /// encoded, where a gather at the view's ids, in the index's order,
    /// would decode them out of order.
    pub(crate) fn window_rows(&self, rows: &LiveRows, window: Range<usize>) -> Result<ArrayRef> {
        self.decode
            .chunk_rows(&self.window_chunk(rows, window)?, None)
    }

    /// [`window_rows`](Self::window_rows) as the four code columns, under
    /// the Dictionary layout ([`ServeDecode::decodes_codes`]).
    pub(crate) fn window_code_columns(
        &self,
        rows: &LiveRows,
        window: Range<usize>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        self.decode
            .chunk_code_columns(&self.window_chunk(rows, window)?, None)
    }

    /// The component's rows `window` of `rows`, in child schema: a slice of
    /// the run, or one take at their positions.
    fn window_chunk(&self, rows: &LiveRows, window: Range<usize>) -> Result<ArrayRef> {
        observe!(ServedComponentChunk(window.len()));
        Ok(match rows {
            LiveRows::Range(range) => {
                let start = range.start as usize;
                self.array.slice(start + window.start..start + window.end)?
            }
            LiveRows::Spanned(rows) | LiveRows::Kept(rows) => self.array.take(
                PrimitiveArray::new(rows.positions.slice(window), Validity::NonNullable)
                    .into_array(),
            )?,
        })
    }
}

/// The rows a point read of a view takes ([`InMemoryServePlan::point_rows`]).
enum PointRows {
    /// A range, every row of it live.
    Range(Range<usize>),
    /// Live rows' positions.
    At(Vec<usize>),
}
