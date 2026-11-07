//! One chunked column's codes at positions, read through its leaves' probes
//! ([`ColumnChunks`]) wherever they answer: a range of positions
//! ([`read_codes`]), ascending positions ([`take_codes`]), the leaves those
//! span ([`leaves_holding`]), and which of a set of ids a sorted run of the
//! column lists ([`listed_in`]). They know nothing of patterns: a located
//! run reads its tables' columns through them.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::VortexSessionExecute as _;
use vortex_array::arrays::PrimitiveArray;
use vortex_buffer::{Buffer, BufferMut};
use vortex_layout::segments::SegmentSource;
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_session::VortexSession;

use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::schema::TermCode;

/// The segments a file's reads fetch.
type Source = Arc<dyn SegmentSource>;

/// Of `rows` (ascending, not empty), those that `rid`'s values at `run`
/// (ascending) list too: the run walked one leaf at a time, each leaf's
/// probe found once for all the rows that fall in it, and the leaves holding
/// no row skipped by a search for the next row's bound: the rows of a short
/// run sit far apart in a long one, and walking to them read every leaf
/// between. `None` when a leaf declines its probe.
pub(crate) async fn listed_in(
    rows: &[u64],
    rid: &ColumnChunks,
    run: Range<u64>,
    source: &Source,
    session: &VortexSession,
) -> Result<Option<Vec<u64>>> {
    let mut kept = Vec::with_capacity(rows.len());
    let mut pending = rows;
    let Some(mut from) = seek(rid, run.clone(), rows[0], source, session).await? else {
        return Ok(None);
    };
    while from < run.end && !pending.is_empty() {
        let to = rid.leaf_end(from).min(run.end);
        let Some(leaf) = rid.leaf_rows(from..to, source, session).await? else {
            return Ok(None);
        };
        while let Some((&row, rest)) = pending.split_first() {
            let hit = leaf.bounds(row);
            if hit.start == to {
                break;
            }
            if !hit.is_empty() {
                kept.push(row);
            }
            pending = rest;
        }
        let Some(&next) = pending.first() else {
            break;
        };
        let Some(at) = seek(rid, to..run.end, next, source, session).await? else {
            return Ok(None);
        };
        from = at;
    }
    Ok(Some(kept))
}

/// Rows up to which a column is read value by value through its leaves'
/// probes, each leaf found once for the rows inside it; more rows are taken
/// from their leaves at once, a take's fixed cost then being the smaller.
const POINT_CODES: usize = 1_024;

/// The codes of `column` at `rows`: a short run inside one leaf through that
/// leaf's probe, any other run through [`take_codes`].
pub(crate) async fn read_codes(
    column: &ColumnChunks,
    rows: Range<u64>,
    source: &Source,
    session: &VortexSession,
) -> Result<Buffer<TermCode>> {
    // A count's window, or a run past the end, reads nothing.
    if rows.is_empty() {
        return Ok(Buffer::empty());
    }
    // A run inside one leaf reads through that leaf's probe, found once.
    if rows.end - rows.start <= POINT_CODES as u64
        && let Some(leaf) = column.leaf_rows(rows.clone(), source, session).await?
    {
        let mut codes = vec![0; (rows.end - rows.start) as usize];
        leaf.read_into(rows.start, &mut codes);
        return Ok(Buffer::from(codes));
    }
    let ids: Vec<u64> = rows.collect();
    take_codes(column, &ids, source, session).await
}

/// The codes of `column` at `rows` (ascending): read leaf by leaf through
/// the leaves' probes for up to [`POINT_CODES`] rows, a leaf whose encoding
/// declines its probe taken instead, and one take of every leaf otherwise.
pub(crate) async fn take_codes(
    column: &ColumnChunks,
    rows: &[u64],
    source: &Source,
    session: &VortexSession,
) -> Result<Buffer<TermCode>> {
    if rows.len() > POINT_CODES {
        return taken_codes(column, rows, source, session).await;
    }
    let mut codes = BufferMut::with_capacity(rows.len());
    let mut rest = rows;
    while let Some(&first) = rest.first() {
        let inside = rest.partition_point(|&row| row < column.leaf_end(first));
        let leaf_rows = &rest[..inside];
        match column
            .leaf_rows(first..leaf_rows[inside - 1] + 1, source, session)
            .await?
        {
            Some(leaf) => {
                let mut values = vec![0; leaf_rows.len()];
                leaf.values_at(leaf_rows, &mut values);
                codes.extend_from_slice(&values);
            }
            None => {
                codes.extend_from_slice(&taken_codes(column, leaf_rows, source, session).await?)
            }
        }
        rest = &rest[inside..];
    }
    Ok(codes.freeze())
}

/// The codes of `column` at `rows` (ascending), by one take of the leaves
/// holding them, whatever their encoding.
async fn taken_codes(
    column: &ColumnChunks,
    rows: &[u64],
    source: &Source,
    session: &VortexSession,
) -> Result<Buffer<TermCode>> {
    let taken = column.take(rows, source, session).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let prim = taken.execute::<PrimitiveArray>(&mut ctx)?;
    Ok(prim.into_buffer::<TermCode>())
}

/// The first row of `run`, a window of `column`'s ascending values, whose
/// value is not below `value` (`run.end` when none is); `None` when a chunk
/// declines its probe.
async fn seek(
    column: &ColumnChunks,
    run: Range<u64>,
    value: u64,
    source: &Source,
    session: &VortexSession,
) -> Result<Option<u64>> {
    Ok(column
        .bounds_in(run, value, source, session)
        .await?
        .map(|hit| hit.start))
}

/// How many of `column`'s leaves hold `rows` (ascending), from the layout.
pub(crate) fn leaves_holding(column: &ColumnChunks, rows: &[u64]) -> usize {
    let mut leaves = 0;
    let mut rest = rows;
    while let Some(&first) = rest.first() {
        let end = column.leaf_end(first);
        rest = &rest[rest.partition_point(|&row| row < end)..];
        leaves += 1;
    }
    leaves
}
