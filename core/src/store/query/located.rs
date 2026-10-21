//! Point lookups by term code from a located run: a pattern of codes whose
//! bound positions are a prefix of the quad table's `(s, p, o, g)` order, of
//! `index:posg`'s `(p, o, s, g)` or of `index:ospg`'s `(o, s, p, g)` is one
//! contiguous run of that table, found by binary search of its chunk probes
//! and read by the unbound columns only. A nested-loop join's per-binding
//! probe is the caller: it holds codes, and its result is a few rows.

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
    /// `index:ospg`'s `(o, s, p, g)`. Rows come in that table's order. A code
    /// past the dictionary, or one in a position no quad holds, matches
    /// nothing.
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
        let tables = file.located_tables(|| {
            TABLES.map(|(component, _)| {
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
            })
        });
        for ((component, order), columns) in TABLES.iter().zip(tables) {
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
        Ok(None)
    }
}

/// Rows up to which a run's column is read point by point through the chunk
/// probes; a longer one is taken from its leaves at once.
#[cfg(feature = "file-io")]
const POINT_CODES: u64 = 32;

/// The codes of `column` at `rows`, through its chunk probes: point reads
/// for a short run, a take otherwise. `None` when a chunk declines its probe.
#[cfg(feature = "file-io")]
async fn read_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: std::ops::Range<u64>,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Option<Buffer<TermCode>>> {
    use crate::VortexRdfError;
    use crate::session::VORTEX_SESSION;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    if rows.end - rows.start <= POINT_CODES {
        let mut codes = vortex_buffer::BufferMut::with_capacity((rows.end - rows.start) as usize);
        for row in rows {
            match column
                .value_at(row, source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            {
                Some(code) => codes.push(code),
                None => return Ok(None),
            }
        }
        return Ok(Some(codes.freeze()));
    }
    let ids: Vec<u64> = rows.collect();
    let Some(taken) = column
        .take(&ids, source, session)
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
