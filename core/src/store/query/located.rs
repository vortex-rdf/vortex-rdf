//! Point lookups by term code from a located run: a pattern of codes whose
//! bound positions are a prefix of the quad table's `(s, p, o, g)` order, of
//! `index:posg`'s `(p, o, s, g)` or of `index:ospg`'s `(o, s, p, g)` is one
//! contiguous run of that table, found by binary search of its chunk probes
//! and read by the unbound columns only. A bound `p` or `o` is also a run of
//! a reference index, `index:ref-p` or `index:ref-o`, whose `rid`s are the
//! quad-table rows to read. A nested-loop join's per-binding probe is the
//! caller: it holds codes, and its result is a few rows.

use vortex_buffer::Buffer;

use crate::error::Result;
use crate::store::VortexRdfStore;
use crate::store::schema::TermCode;

/// A quad pattern of term codes, as
/// [`locate_codes`](VortexRdfStore::locate_codes) takes it; `None` is free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CodePattern {
    pub subject: Option<TermCode>,
    pub predicate: Option<TermCode>,
    pub object: Option<TermCode>,
    pub graph: Option<TermCode>,
}

impl CodePattern {
    /// The pattern with these slots bound.
    pub fn new(
        subject: Option<TermCode>,
        predicate: Option<TermCode>,
        object: Option<TermCode>,
        graph: Option<TermCode>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph,
        }
    }

    #[cfg(feature = "file-io")]
    fn slots(&self) -> [Option<TermCode>; 4] {
        [self.subject, self.predicate, self.object, self.graph]
    }
}

/// The answer of [`locate_codes`](VortexRdfStore::locate_codes): how many
/// quads match, and the window's rows as the `(s, p, o, g)` code columns, in
/// the order of the table that located them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocatedCodes {
    /// Quads matching the pattern, before the window.
    pub total: u64,
    /// The window's `(s, p, o, g)` codes, one buffer per column.
    pub columns: [Buffer<TermCode>; 4],
}

impl VortexRdfStore {
    /// The quads matching `pattern`, a run located by binary search, windowed
    /// to `limit` rows (`None`: every row) after `offset`; `Ok(None)` when no
    /// located run answers it, for the caller to [`match_pattern`] or
    /// [`run_probe`] instead.
    ///
    /// It answers on a whole, file-backed Dictionary-layout store (no tail,
    /// tombstones or narrowing) when the bound slots are a prefix of a sorted
    /// table's key order whose four code columns have chunk probes: the quad
    /// table's `(s, p, o, g)` (a `quads_sorted` file), and the
    /// secondary-by-copy children `index:posg`'s `(p, o, s, g)` and
    /// `index:ospg`'s `(o, s, p, g)`. Rows come in that table's order. Failing
    /// that, a bound `p` or `o` is located in the reference index
    /// (`index:ref-p`, `index:ref-o`) and its rows read from the quad table in
    /// its order, the other bound slots checked at them when its run is short
    /// enough; a bound subject's run of a `quads_sorted` file is the rows
    /// checked instead when no longer. A code past the dictionary, or one in
    /// a position no quad holds, matches nothing.
    ///
    /// [`match_pattern`]: VortexRdfStore::match_pattern
    /// [`run_probe`]: VortexRdfStore::run_probe
    pub async fn locate_codes(
        &self,
        pattern: CodePattern,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Option<LocatedCodes>> {
        #[cfg(feature = "file-io")]
        {
            self.locate_codes_in_file(pattern, offset, limit).await
        }
        #[cfg(not(feature = "file-io"))]
        {
            let _ = (pattern, offset, limit);
            Ok(None)
        }
    }

    #[cfg(feature = "file-io")]
    async fn locate_codes_in_file(
        &self,
        pattern: CodePattern,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Option<LocatedCodes>> {
        use crate::VortexRdfError;
        use crate::store::layouts::LayoutStrategy;
        use crate::store::persist::native_file::LocatedTables;
        use crate::store::schema::PRIMARY_COLUMNS;

        if self.layout.strategy() != LayoutStrategy::Dictionary
            || self.tail_len() != 0
            || self.quads.deleted.is_some()
            || !self.quads.selection.is_all()
        {
            return Ok(None);
        }
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        if base.filter.is_some() || base.serve.is_some() {
            return Ok(None);
        }
        let file = &base.file;
        let (source, session) = (file.segment_source(), file.session());
        let codes = pattern.slots();
        let bound = codes.iter().filter(|c| c.is_some()).count();
        if bound == 0 {
            return Ok(None);
        }
        // Each table's key order, as `(s, p, o, g)` positions.
        const TABLES: [(Option<&str>, [usize; 4]); 3] = [
            (None, [0, 1, 2, 3]),
            (Some("index:posg"), [1, 2, 0, 3]),
            (Some("index:ospg"), [2, 0, 1, 3]),
        ];
        let tables = file.located_tables(|| LocatedTables {
            sorted: TABLES.map(|(component, _)| {
                let handle = |column: &str| match component {
                    None => file.column_chunks(column),
                    Some(name) => file.component_column_chunks(name, column),
                };
                Some([
                    handle(PRIMARY_COLUMNS[0])?,
                    handle(PRIMARY_COLUMNS[1])?,
                    handle(PRIMARY_COLUMNS[2])?,
                    handle(PRIMARY_COLUMNS[3])?,
                ])
            }),
            references: REFERENCES.map(|(name, _)| {
                Some([
                    file.component_column_chunks(name, "val")?,
                    file.component_column_chunks(name, "rid")?,
                ])
            }),
        });
        for ((component, order), columns) in TABLES.iter().zip(&tables.sorted) {
            let prefix = order.iter().take_while(|&&c| codes[c].is_some()).count();
            if prefix != bound || (component.is_none() && !file.quads_sorted()) {
                continue;
            }
            let Some(columns) = columns else {
                continue;
            };
            let lead = order[0];
            let Some(mut run) = columns[lead]
                .bounds(codes[lead].expect("a bound lead key"), &source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            else {
                continue;
            };
            let mut declined = false;
            for &key in &order[1..prefix] {
                if run.is_empty() {
                    break;
                }
                match columns[key]
                    .bounds_in(
                        run.clone(),
                        codes[key].expect("a bound key"),
                        &source,
                        session,
                    )
                    .await
                    .map_err(VortexRdfError::Vortex)?
                {
                    Some(narrowed) => run = narrowed,
                    None => {
                        declined = true;
                        break;
                    }
                }
            }
            if declined {
                continue;
            }
            let total = run.end - run.start;
            let from = run.start.saturating_add(offset as u64).min(run.end);
            let to = match limit {
                Some(limit) => from.saturating_add(limit as u64).min(run.end),
                None => run.end,
            };
            let mut out: [Buffer<TermCode>; 4] = Default::default();
            for (column, slot) in out.iter_mut().enumerate() {
                *slot = match codes[column] {
                    Some(code) => {
                        vortex_buffer::BufferMut::full(code, (to - from) as usize).freeze()
                    }
                    None => match read_codes(&columns[column], from..to, &source, session).await? {
                        Some(codes) => codes,
                        None => return Ok(None),
                    },
                };
            }
            return Ok(Some(LocatedCodes {
                total,
                columns: out,
            }));
        }
        // Boxed: its state would otherwise enlarge every lookup's future,
        // the sorted tables' too.
        Box::pin(by_reference(
            codes,
            tables,
            file.quads_sorted(),
            offset,
            limit,
            &source,
            session,
        ))
        .await
    }
}

/// The reference indexes, each `(val, rid)` sorted by `val` then `rid`: a
/// run of one code's `val` lists, ascending, the quad-table rows holding it
/// at the `(s, p, o, g)` position given here.
#[cfg(feature = "file-io")]
const REFERENCES: [(&str, usize); 2] = [("index:ref-p", 1), ("index:ref-o", 2)];

/// Rows of a reference or subject run up to which the pattern's other bound
/// slots are checked at the run's quad-table rows; a longer run declines,
/// for the caller's generic path. Every window of such a pattern reads them
/// again, so this bounds what a page costs.
#[cfg(feature = "file-io")]
const CHECKED_ROWS: u64 = 16_384;

/// How many searches of a reference run's `rid`s cost about as much as
/// reaching one more quad-table leaf to read a slot there (about 250 ns
/// against 4 µs per leaf on WatDiv 100M's reference store, warm).
#[cfg(feature = "file-io")]
const SEARCHES_PER_LEAF: usize = 16;

/// [`locate_codes`](VortexRdfStore::locate_codes) through a reference index:
/// the shortest run among those whose slot is bound gives the quad-table rows
/// in order. With no other slot bound, its length is the total and only the
/// window's rows are read; otherwise all its rows are checked against the
/// other bound slots, within [`CHECKED_ROWS`]: against the other reference's
/// run for `p` or `o`, by reading the quad table for `s` or `g`. A bound
/// subject's run of a subject-sorted (`quads_sorted`) quad table is checked
/// instead when it is no longer than the shortest reference run.
#[cfg(feature = "file-io")]
async fn by_reference(
    codes: [Option<TermCode>; 4],
    tables: &crate::store::persist::native_file::LocatedTables,
    quads_sorted: bool,
    offset: usize,
    limit: Option<usize>,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Option<LocatedCodes>> {
    use crate::VortexRdfError;

    let Some(quads) = &tables.sorted[0] else {
        return Ok(None);
    };
    let mut runs: [Option<std::ops::Range<u64>>; 2] = [None, None];
    for (i, &(_, slot)) in REFERENCES.iter().enumerate() {
        let (Some(code), Some([val, _])) = (codes[slot], &tables.references[i]) else {
            continue;
        };
        runs[i] = val
            .bounds(code, source, session)
            .await
            .map_err(VortexRdfError::Vortex)?;
    }
    let Some(index) = (0..2)
        .filter(|&i| runs[i].is_some())
        .min_by_key(|&i| runs[i].as_ref().map(|run| run.end - run.start))
    else {
        return Ok(None);
    };
    let run = runs[index].clone().expect("a located run");
    let [_, rid] = tables.references[index]
        .as_ref()
        .expect("a reference that located a run");
    // A bound subject of a subject-sorted quad table is a run of it too,
    // found by one search and its rows contiguous. It is the rows checked
    // when no longer than the reference run: a subject's few quads, not the
    // many of a common object (`s?o`), as the reference index's own probe
    // leaves a bound subject to the subject column.
    let subject = match codes[0] {
        Some(code) if quads_sorted => quads[0]
            .bounds(code, source, session)
            .await
            .map_err(VortexRdfError::Vortex)?
            .filter(|subject| subject.end - subject.start <= run.end - run.start),
        _ => None,
    };
    let key = match subject {
        Some(_) => 0,
        None => REFERENCES[index].1,
    };
    let checked: Vec<usize> = (0..4)
        .filter(|&slot| slot != key && codes[slot].is_some())
        .collect();
    let window = |total: u64| {
        let from = (offset as u64).min(total);
        let to = limit.map_or(total, |limit| from.saturating_add(limit as u64).min(total));
        from..to
    };
    let (total, rows) = if checked.is_empty() {
        let total = run.end - run.start;
        let w = window(total);
        match read_codes(rid, run.start + w.start..run.start + w.end, source, session).await? {
            Some(rows) => (total, rows),
            None => return Ok(None),
        }
    } else {
        let by_subject = subject.is_some();
        let mut rows: Vec<u64> = match subject {
            Some(subject) if subject.end - subject.start <= CHECKED_ROWS => subject.collect(),
            None if run.end - run.start <= CHECKED_ROWS => {
                match read_codes(rid, run, source, session).await? {
                    Some(rows) => rows.to_vec(),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        for &slot in &checked {
            if rows.is_empty() {
                break;
            }
            // A slot with a located reference run keeps the rows that run
            // lists too: both lists ascend, so each row is searched in what
            // is left of the other's `rid`s, which sit together in one index.
            // Reading the quad table costs a leaf for each leaf the rows
            // reach, searching the other run a search per row: rows spread
            // one or two to a leaf (a large file) are searched, rows bunched
            // in few leaves read. A subject's rows are contiguous, so read.
            if !by_subject
                && let Some(other) = REFERENCES.iter().position(|&(_, s)| s == slot)
                && let (Some(other_run), Some([_, other_rid])) =
                    (&runs[other], &tables.references[other])
                && rows.len() < SEARCHES_PER_LEAF * leaves_holding(&quads[slot], &rows)
            {
                // The other run is walked one leaf at a time, each leaf's
                // probe found once for all the rows that fall in it.
                let mut kept = Vec::with_capacity(rows.len());
                let mut pending = rows.as_slice();
                let mut from = other_run.start;
                while from < other_run.end && !pending.is_empty() {
                    let to = other_rid.leaf_end(from).min(other_run.end);
                    let Some(leaf) = other_rid
                        .leaf_rows(from..to, source, session)
                        .await
                        .map_err(VortexRdfError::Vortex)?
                    else {
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
                    from = to;
                }
                rows = kept;
                continue;
            }
            let Some(values) = take_codes(&quads[slot], &rows, source, session).await? else {
                return Ok(None);
            };
            let code = codes[slot].expect("a checked slot is bound");
            rows = rows
                .iter()
                .zip(values.iter())
                .filter(|&(_, &value)| value == code)
                .map(|(&row, _)| row)
                .collect();
        }
        let total = rows.len() as u64;
        let w = window(total);
        (
            total,
            Buffer::copy_from(&rows[w.start as usize..w.end as usize]),
        )
    };
    let mut out: [Buffer<TermCode>; 4] = Default::default();
    for (slot, column) in out.iter_mut().enumerate() {
        *column = match codes[slot] {
            Some(code) => vortex_buffer::BufferMut::full(code, rows.len()).freeze(),
            None => match take_codes(&quads[slot], &rows, source, session).await? {
                Some(codes) => codes,
                None => return Ok(None),
            },
        };
    }
    Ok(Some(LocatedCodes {
        total,
        columns: out,
    }))
}

/// Rows up to which a column is read value by value through its leaves'
/// probes, each leaf found once for the rows inside it; more rows are taken
/// from their leaves at once, a take's fixed cost then being the smaller.
#[cfg(feature = "file-io")]
const POINT_CODES: usize = 1_024;

/// The codes of `column` at `rows`, through its chunk probes. `None` when a
/// chunk declines its probe.
#[cfg(feature = "file-io")]
async fn read_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: std::ops::Range<u64>,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Option<Buffer<TermCode>>> {
    use crate::VortexRdfError;

    // A count's window, or a run past the end, reads nothing.
    if rows.is_empty() {
        return Ok(Some(Buffer::empty()));
    }
    // A run inside one leaf reads through that leaf's probe, found once.
    if rows.end - rows.start <= POINT_CODES as u64
        && let Some(leaf) = column
            .leaf_rows(rows.clone(), source, session)
            .await
            .map_err(VortexRdfError::Vortex)?
    {
        let mut codes = vortex_buffer::BufferMut::with_capacity((rows.end - rows.start) as usize);
        codes.extend(rows.map(|row| leaf.value_at(row)));
        return Ok(Some(codes.freeze()));
    }
    let ids: Vec<u64> = rows.collect();
    take_codes(column, &ids, source, session).await
}

/// The codes of `column` at `rows` (ascending), through its chunk probes:
/// read leaf by leaf for up to [`POINT_CODES`] rows, a take otherwise.
/// `None` when a chunk declines its probe.
#[cfg(feature = "file-io")]
async fn take_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: &[u64],
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Option<Buffer<TermCode>>> {
    use crate::VortexRdfError;
    use crate::session::VORTEX_SESSION;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    if rows.len() <= POINT_CODES {
        let mut codes = vortex_buffer::BufferMut::with_capacity(rows.len());
        let mut rest = rows;
        while let Some(&first) = rest.first() {
            let inside = rest.partition_point(|&row| row < column.leaf_end(first));
            let Some(leaf) = column
                .leaf_rows(first..rest[inside - 1] + 1, source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            else {
                return Ok(None);
            };
            codes.extend(rest[..inside].iter().map(|&row| leaf.value_at(row)));
            rest = &rest[inside..];
        }
        return Ok(Some(codes.freeze()));
    }
    let Some(taken) = column
        .take(rows, source, session)
        .await
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(None);
    };
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let prim = taken
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(Some(prim.into_buffer::<TermCode>()))
}

/// How many of `column`'s leaves hold `rows` (ascending), from the layout.
#[cfg(feature = "file-io")]
fn leaves_holding(column: &vortex_rdf_encoded_search::ColumnChunks, rows: &[u64]) -> usize {
    let mut leaves = 0;
    let mut rest = rows;
    while let Some(&first) = rest.first() {
        let end = column.leaf_end(first);
        rest = &rest[rest.partition_point(|&row| row < end)..];
        leaves += 1;
    }
    leaves
}
