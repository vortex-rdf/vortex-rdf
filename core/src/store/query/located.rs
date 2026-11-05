//! Point lookups by term code from a located run: a pattern of codes whose
//! bound positions are a prefix of the quad table's `(s, p, o, g)` order, of
//! `index:posg`'s `(p, o, s, g)` or of `index:ospg`'s `(o, s, p, g)` is one
//! contiguous run of that table, found by binary search of its chunk probes
//! and read by the unbound columns only. A bound `p` or `o` is also a run of
//! a reference index, `index:ref-p` or `index:ref-o`, whose `rid`s are the
//! quad-table rows to read. A nested-loop join's per-binding probe is the
//! caller: it holds codes, and its result is a few rows.
//!
//! [`count_code_pattern`](VortexRdfStore::count_code_pattern) and
//! [`match_code_pattern`](VortexRdfStore::match_code_pattern) answer any
//! pattern of codes, and [`match_code_chunks`](VortexRdfStore::match_code_chunks)
//! streams it: from a located run when one answers it, through the general
//! match of the codes' terms otherwise. A located run's rows that the other
//! bound slots are checked at are checked as they are read, and nothing is
//! kept on the store between calls: a window checks its run again, where a
//! stream holds its position in the run and reads each row once.

use vortex_buffer::Buffer;

use crate::common::terms::{self, Trust};
use crate::error::{Result, VortexRdfError};

/// Whether [`VortexRdfStore::locate_codes`] answers at all, and
/// [`VortexRdfStore::match_code_chunks`] reads a located run: `VORTEX_RDF_LOCATE=0` (an ablation switch, read
/// once) makes them decline every pattern, so callers take the general match. Unset is on.
#[cfg(feature = "file-io")]
pub fn locate_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("VORTEX_RDF_LOCATE").as_deref() != Ok("0"))
}
use crate::store::VortexRdfStore;
use crate::store::schema::TermCode;
use crate::{DictReader, KindRanges, Probe};

/// A quad pattern of term codes, as
/// [`count_code_pattern`](VortexRdfStore::count_code_pattern),
/// [`match_code_pattern`](VortexRdfStore::match_code_pattern) and
/// [`locate_codes`](VortexRdfStore::locate_codes) take it; `None` is free.
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
    /// The number of quads matching `pattern`, a pattern of a Dictionary-layout
    /// store's term codes: a located run's length when one answers it
    /// ([`locate_codes`](Self::locate_codes)), the general match's count
    /// otherwise. A code past the dictionary, or a term in a position it cannot
    /// take, matches nothing. An
    /// [`InvalidOperation`](VortexRdfError::InvalidOperation) on a store
    /// without a term dictionary.
    pub async fn count_code_pattern(&self, pattern: CodePattern) -> Result<u64> {
        let dict = self.code_dictionary()?;
        if let Some(found) = self.locate_codes(pattern, 0, Some(0)).await? {
            return Ok(found.total);
        }
        match probe_of_codes(&dict, pattern).await? {
            Some(probe) => Ok(self.count_probe(&probe).await? as u64),
            None => Ok(0),
        }
    }

    /// The `(s, p, o, g)` code columns of the quads matching `pattern`,
    /// windowed to `limit` rows (`None`: every row) after `offset`: a located
    /// run's rows, in its table's order, when one answers it
    /// ([`locate_codes`](Self::locate_codes)), the general match's rows in its
    /// own order otherwise. Which of the two answers depends on the store and
    /// `pattern` alone, so every window of one pattern comes in the same order.
    /// Each window is found anew: a run whose rows are checked is checked
    /// from its start as far as the window's last row (no further, as no
    /// count is asked), so a caller reading a long match through
    /// [`match_code_chunks`](Self::match_code_chunks) reads it once.
    /// Matches nothing and fails as
    /// [`count_code_pattern`](Self::count_code_pattern) does.
    pub async fn match_code_pattern(
        &self,
        pattern: CodePattern,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        let dict = self.code_dictionary()?;
        #[cfg(feature = "file-io")]
        if let Some(run) = self.located_run(pattern).await? {
            return run.window_rows(offset, limit).await;
        }
        let Some(probe) = probe_of_codes(&dict, pattern).await? else {
            return Ok(Default::default());
        };
        self.run_probe(&probe.window(offset, limit))
            .await?
            .code_columns_gathered()
            .await?
            .ok_or_else(|| {
                VortexRdfError::InvalidOperation(
                    "the store's rows are not all in its term dictionary".into(),
                )
            })
    }

    /// [`match_code_pattern`](Self::match_code_pattern)'s rows, unwindowed
    /// and in its order, as a stream of chunks of at most `batch_rows` rows,
    /// each the four `(s, p, o, g)` code columns of its rows. A located run
    /// that answers the pattern ([`locate_codes`](Self::locate_codes)) is
    /// read `batch_rows` of its rows at a time, each block checked as it is
    /// read and yielding the rows it keeps (a block keeping none yields no
    /// chunk), so every row of the run is read once; any other pattern
    /// streams the general match's [`code_chunks`](Self::code_chunks). The
    /// run is located when the stream is made and its rows read as it is
    /// pulled; the stream holds its position in the run and handles to the
    /// file's columns, nothing on the store, so what it reads goes with it.
    /// A read error is a stream item. Empty for a pattern nothing can match.
    /// Errors: `batch_rows == 0` (invalid input, checked first), and as
    /// [`count_code_pattern`](Self::count_code_pattern).
    pub async fn match_code_chunks(
        &self,
        pattern: CodePattern,
        batch_rows: usize,
    ) -> Result<crate::CodeChunkStream> {
        use futures::StreamExt as _;

        crate::store::read::chunks::check_batch(batch_rows)?;
        let dict = self.code_dictionary()?;
        #[cfg(feature = "file-io")]
        if let Some(run) = self.located_run(pattern).await? {
            return Ok(run.chunks(batch_rows));
        }
        match probe_of_codes(&dict, pattern).await? {
            None => Ok(futures::stream::empty().boxed()),
            Some(probe) => self
                .run_probe(&probe)
                .await?
                .code_chunks(&crate::QuadColumn::ALL, batch_rows),
        }
    }

    /// The probe of `pattern`'s terms, spelled by the store's own dictionary: what
    /// [`match_code_pattern`](Self::match_code_pattern) runs when no located run answers it, for a caller that
    /// reads the match its own way ([`run_probe`](Self::run_probe), then
    /// [`code_chunks`](Self::code_chunks) or [`code_columns_gathered`](Self::code_columns_gathered)).
    /// `None` when nothing can match: a code past the dictionary, or one in a position its term's kind cannot
    /// take. Fails as [`count_code_pattern`](Self::count_code_pattern) does.
    pub async fn code_probe(&self, pattern: CodePattern) -> Result<Option<Probe>> {
        probe_of_codes(&self.code_dictionary()?, pattern).await
    }

    /// The term dictionary a code pattern's codes are drawn from.
    fn code_dictionary(&self) -> Result<DictReader> {
        self.dict_reader().ok_or_else(|| {
            VortexRdfError::InvalidOperation(format!(
                "a {} store has no term dictionary",
                self.layout()
            ))
        })
    }

    /// The quads matching `pattern`, a run located by binary search, windowed
    /// to `limit` rows (`None`: every row) after `offset`; `Ok(None)` when no
    /// located run answers it, for the caller to [`match_pattern`] or
    /// [`run_probe`] instead, as
    /// [`match_code_pattern`](Self::match_code_pattern) does.
    ///
    /// It answers on a whole, file-backed Dictionary-layout store (no tail,
    /// tombstones or narrowing) when the bound slots are a prefix of a sorted
    /// table's key order whose four code columns have chunk probes: the quad
    /// table's `(s, p, o, g)` (a `quads_sorted` file), and the
    /// secondary-by-copy children `index:posg`'s `(p, o, s, g)` and
    /// `index:ospg`'s `(o, s, p, g)`. Rows come in that table's order. Bound
    /// slots that are no such prefix (a bound graph after a free slot) are the
    /// run of the longest bound prefix, its rows checked against the others,
    /// when it is no longer than [`GAP_ROWS`]. Failing that, a bound `p` or
    /// `o` is located in the reference index (`index:ref-p`, `index:ref-o`)
    /// and its rows read from the quad table in its order, the other bound
    /// slots checked at them when its run is short enough; a bound subject's
    /// run of a `quads_sorted` file is the rows checked instead when no
    /// longer. A code past the dictionary, or one in a position no quad
    /// holds, matches nothing. Whether it answers depends on the store and
    /// `pattern` alone, never on the window, so every window of one pattern
    /// comes from the same table, in the same order. A run whose rows are
    /// checked is checked whole, a block at a time, to count them, and only
    /// the window's rows are kept; nothing is kept between calls. With the
    /// ablation switch `VORTEX_RDF_LOCATE=0` ([`locate_enabled`]) it declines
    /// every pattern.
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
            match self.located_run(pattern).await? {
                Some(run) => run.window(offset, limit).await.map(Some),
                None => Ok(None),
            }
        }
        #[cfg(not(feature = "file-io"))]
        {
            let _ = (pattern, offset, limit);
            Ok(None)
        }
    }

    /// The located run answering `pattern` ([`locate_codes`](Self::locate_codes)
    /// says when one does), found by binary search; `None` when none does.
    #[cfg(feature = "file-io")]
    async fn located_run(&self, pattern: CodePattern) -> Result<Option<LocatedRun>> {
        use crate::store::persist::native_file::LocatedTables;
        use crate::store::schema::PRIMARY_COLUMNS;

        if !locate_enabled() {
            return Ok(None);
        }
        if !self.is_code_view() || self.quads.deleted.is_some() || !self.quads.selection.is_all() {
            return Ok(None);
        }
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        if base.filter.over_selection().is_some() || base.serve.is_some() {
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
            LocatedTables::new(
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
                }),
                REFERENCES.map(|(name, _)| {
                    Some([
                        file.component_column_chunks(name, "val")?,
                        file.component_column_chunks(name, "rid")?,
                    ])
                }),
            )
        });
        let run = |columns: Columns, rows, checked| LocatedRun {
            codes,
            columns,
            rows,
            checked,
            source: source.clone(),
            session: session.clone(),
        };
        // The table whose key order starts with the most bound slots, when
        // none starts with all of them: a bound graph after a free slot.
        let mut gap: Option<(usize, usize)> = None;
        for (table, ((component, order), columns)) in TABLES.iter().zip(&tables.sorted).enumerate()
        {
            let prefix = order.iter().take_while(|&&c| codes[c].is_some()).count();
            if component.is_none() && !file.quads_sorted() {
                continue;
            }
            let Some(columns) = columns else {
                continue;
            };
            if prefix != bound {
                if prefix > 0 && gap.is_none_or(|(_, longest)| prefix > longest) {
                    gap = Some((table, prefix));
                }
                continue;
            }
            let Some(found) = locate_run(columns, order, prefix, &codes, &source, session).await?
            else {
                continue;
            };
            return Ok(Some(run(
                columns.clone(),
                RunRows::Table(found),
                Vec::new(),
            )));
        }
        // The run of the longest bound prefix, its rows checked against the
        // bound slots past it, in the table's order.
        if let Some((table, prefix)) = gap {
            let columns = tables.sorted[table].as_ref().expect("a table with columns");
            let order = &TABLES[table].1;
            if let Some(found) =
                locate_run(columns, order, prefix, &codes, &source, session).await?
                && found.end - found.start <= GAP_ROWS
            {
                let checked = order[prefix..]
                    .iter()
                    .copied()
                    .filter(|&slot| codes[slot].is_some())
                    .collect();
                return Ok(Some(run(columns.clone(), RunRows::Table(found), checked)));
            }
        }
        // Boxed: its state would otherwise enlarge every lookup's future,
        // the sorted tables' too.
        let found = Box::pin(by_reference(
            codes,
            tables,
            file.quads_sorted(),
            &source,
            session,
        ))
        .await?;
        Ok(found.map(|(columns, rows, checked)| run(columns, rows, checked)))
    }
}

/// The probe of `pattern`, its codes spelled out by `dict`, or `None` when it
/// can match nothing: a code past the dictionary, or a term in a position it
/// cannot take ([`holds`]).
async fn probe_of_codes(dict: &DictReader, pattern: CodePattern) -> Result<Option<Probe>> {
    let kinds = dict.kind_ranges().await?;
    let slots = pattern.slots();
    let mut spelled: [Option<String>; 4] = Default::default();
    for (slot, (spelling, code)) in spelled.iter_mut().zip(slots).enumerate() {
        let Some(code) = code else {
            continue;
        };
        if !holds(&kinds, slot, code) {
            return Ok(None);
        }
        *spelling = Some(dict.decode(code).await?.ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "code {code} of a term kind decodes to nothing"
            ))
        })?);
    }
    // The dictionary's own spellings, each in a position its kind can take.
    let [s, p, o, g] = spelled;
    Ok(Some(Probe::new(
        s.map(|s| terms::subject(&s, Trust::Stored)).transpose()?,
        p.map(|p| terms::named_node(&p, Trust::Stored))
            .transpose()?,
        o.map(|o| terms::term(&o, Trust::Stored)).transpose()?,
        g.map(|g| terms::graph_name(&g, Trust::Stored))
            .transpose()?,
    )))
}

/// Whether a quad can hold `code` at `slot` of `(s, p, o, g)`, by the term
/// kind `kinds` puts it in: a subject is an IRI or a blank node, a predicate
/// an IRI, an object an IRI, a blank node or a literal, and a graph an IRI, a
/// blank node or the default graph's name.
fn holds(kinds: &KindRanges, slot: usize, code: TermCode) -> bool {
    let node = kinds.iris.contains(&code) || kinds.blanks.contains(&code);
    match slot {
        0 => node,
        1 => kinds.iris.contains(&code),
        2 => node || kinds.literals.contains(&code),
        _ => node || kinds.default_graph == Some(code),
    }
}

/// A column's chunk-probe handle.
#[cfg(feature = "file-io")]
type Chunks = std::sync::Arc<vortex_rdf_encoded_search::ColumnChunks>;

/// A table's `(s, p, o, g)` chunk-probe handles.
#[cfg(feature = "file-io")]
type Columns = [Chunks; 4];

/// The segments a file's reads fetch.
#[cfg(feature = "file-io")]
type Source = std::sync::Arc<dyn vortex_layout::segments::SegmentSource>;

/// The run answering a pattern, as [`located_run`](VortexRdfStore::located_run)
/// finds it: the table its rows are read from, where they are, and the
/// bound slots each is checked against. It holds handles to the file's
/// columns and nothing on the store, so a window or a stream of it reads only
/// what it needs, and a stream of it outlives the store handle that found it.
#[cfg(feature = "file-io")]
struct LocatedRun {
    codes: [Option<TermCode>; 4],
    /// The `(s, p, o, g)` columns of the table the rows are read from: a
    /// sorted table's, or the quad table's for a reference run.
    columns: Columns,
    rows: RunRows,
    /// The bound slots the run does not pin, checked at each of its rows in
    /// this order.
    checked: Vec<usize>,
    source: Source,
    session: vortex_session::VortexSession,
}

/// Where a [`LocatedRun`]'s rows are.
#[cfg(feature = "file-io")]
enum RunRows {
    /// The table's rows `run`, contiguous.
    Table(std::ops::Range<u64>),
    /// The quad-table rows a reference run lists, ascending: `rid`'s values
    /// at `run`. `other` is the other reference's run, when it located the
    /// code of a checked slot.
    Reference {
        rid: Chunks,
        run: std::ops::Range<u64>,
        other: Option<OtherReference>,
    },
}

/// The other reference's run of a [`RunRows::Reference`], which lists the
/// rows holding the code of the checked slot `slot`, in ascending order.
#[cfg(feature = "file-io")]
struct OtherReference {
    slot: usize,
    rid: Chunks,
    run: std::ops::Range<u64>,
}

/// A block's rows of the table a [`LocatedRun`] reads: a contiguous run of
/// it, or the ascending rows a reference run and the checks kept.
#[cfg(feature = "file-io")]
enum Rows {
    Run(std::ops::Range<u64>),
    Ids(Vec<u64>),
}

#[cfg(feature = "file-io")]
impl Rows {
    fn is_empty(&self) -> bool {
        match self {
            Rows::Run(run) => run.is_empty(),
            Rows::Ids(ids) => ids.is_empty(),
        }
    }
}

#[cfg(feature = "file-io")]
impl LocatedRun {
    /// The run's rows, before their checks.
    fn len(&self) -> u64 {
        match &self.rows {
            RunRows::Table(run) | RunRows::Reference { run, .. } => run.end - run.start,
        }
    }

    /// The rows at positions `at` of the run (not empty, within its
    /// [`len`](Self::len)) that the checks keep, in the run's order: with no
    /// slot checked, a table run's own rows or the rows a reference run lists
    /// there.
    async fn block(&self, at: std::ops::Range<u64>) -> Result<Rows> {
        crate::observe::observe!(LocatedBlock(at.end - at.start));
        let (source, session) = (&self.source, &self.session);
        match &self.rows {
            RunRows::Table(run) => {
                let rows = run.start + at.start..run.start + at.end;
                let Some((&first, rest)) = self.checked.split_first() else {
                    return Ok(Rows::Run(rows));
                };
                // The first check reads the block's rows, still contiguous.
                let code = self.codes[first].expect("a checked slot is bound");
                let values =
                    read_codes(&self.columns[first], rows.clone(), source, session).await?;
                let mut kept: Vec<u64> = rows
                    .zip(values.iter())
                    .filter(|&(_, &value)| value == code)
                    .map(|(row, _)| row)
                    .collect();
                for &slot in rest {
                    if kept.is_empty() {
                        break;
                    }
                    kept = self.holding(slot, kept).await?;
                }
                Ok(Rows::Ids(kept))
            }
            RunRows::Reference { rid, run, other } => {
                let rows = run.start + at.start..run.start + at.end;
                let mut kept = read_codes(rid, rows, source, session).await?.to_vec();
                for &slot in &self.checked {
                    if kept.is_empty() {
                        break;
                    }
                    // A slot with a located reference run keeps the rows that
                    // run lists too: both lists ascend, so each row is
                    // searched in what is left of the other's `rid`s, which
                    // sit together in one index. Reading the quad table costs
                    // a leaf for each leaf the rows reach, searching the other
                    // run a search per row: rows spread one or two to a leaf
                    // (a large file) are searched, rows bunched in few leaves
                    // read, as are rows whose search a leaf declines.
                    if let Some(other) = other.as_ref().filter(|other| other.slot == slot)
                        && kept.len()
                            < SEARCHES_PER_LEAF * leaves_holding(&self.columns[slot], &kept)
                        && let Some(listed) =
                            listed_in(&kept, &other.rid, other.run.clone(), source, session).await?
                    {
                        kept = listed;
                        continue;
                    }
                    kept = self.holding(slot, kept).await?;
                }
                Ok(Rows::Ids(kept))
            }
        }
    }

    /// Of `rows` (ascending), those holding the checked `slot`'s code, read
    /// at them.
    async fn holding(&self, slot: usize, rows: Vec<u64>) -> Result<Vec<u64>> {
        let code = self.codes[slot].expect("a checked slot is bound");
        let values = take_codes(&self.columns[slot], &rows, &self.source, &self.session).await?;
        Ok(rows
            .into_iter()
            .zip(values.iter())
            .filter(|&(_, &value)| value == code)
            .map(|(row, _)| row)
            .collect())
    }

    /// The `(s, p, o, g)` code columns of a block's `rows`: a bound slot is
    /// its code repeated, a free one read from its chunk leaves
    /// ([`read_codes`], [`take_codes`]).
    async fn columns(&self, rows: &Rows) -> Result<[Buffer<TermCode>; 4]> {
        let len = match rows {
            Rows::Run(run) => (run.end - run.start) as usize,
            Rows::Ids(ids) => ids.len(),
        };
        let (source, session) = (&self.source, &self.session);
        let mut out: [Buffer<TermCode>; 4] = Default::default();
        for (slot, column) in out.iter_mut().enumerate() {
            *column = match (self.codes[slot], rows) {
                (Some(code), _) => vortex_buffer::BufferMut::full(code, len).freeze(),
                (None, Rows::Run(run)) => {
                    read_codes(&self.columns[slot], run.clone(), source, session).await?
                }
                (None, Rows::Ids(ids)) => {
                    take_codes(&self.columns[slot], ids, source, session).await?
                }
            };
        }
        Ok(out)
    }

    /// The window `offset..offset + limit` (`None`: to the last) of the
    /// run's matches, beside their number: with no slot checked the run's
    /// length, and only the window's rows read; otherwise every row of the
    /// run is checked, a block at a time, to count the rows kept, and only
    /// those in the window are held.
    async fn window(&self, offset: usize, limit: Option<usize>) -> Result<LocatedCodes> {
        let (total, rows) = self.window_of_matches(offset, limit, true).await?;
        let columns = self.columns(&rows).await?;
        Ok(LocatedCodes { total, columns })
    }

    /// The `(s, p, o, g)` code columns of [`window`](Self::window)'s rows,
    /// without its count: the run checked from its start only as far as the
    /// window's last row.
    async fn window_rows(
        &self,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        let (_, rows) = self.window_of_matches(offset, limit, false).await?;
        self.columns(&rows).await
    }

    /// The rows of the window `offset..offset + limit` of the run's matches,
    /// beside the number of matches the reads counted: all of them, with no
    /// slot checked or with `whole`; otherwise those up to the window's last
    /// row, where the checks stop. A checked run is read a block at a time,
    /// [`FIRST_GAP_BLOCK`] rows first and each next block twice as many, up
    /// to [`GAP_BLOCK`]: a window near the run's start reads little of it.
    async fn window_of_matches(
        &self,
        offset: usize,
        limit: Option<usize>,
        whole: bool,
    ) -> Result<(u64, Rows)> {
        let len = self.len();
        if self.checked.is_empty() {
            let w = window_of(len, offset, limit);
            let rows = match w.is_empty() {
                // A count's window, or one past the end, reads nothing.
                true => Rows::Ids(Vec::new()),
                false => self.block(w).await?,
            };
            return Ok((len, rows));
        }
        let from = offset as u64;
        let to = limit.map(|limit| from.saturating_add(limit as u64));
        let (mut total, mut window) = (0u64, Vec::new());
        let (mut at, mut block) = (0, FIRST_GAP_BLOCK);
        while at < len {
            if !whole && to.is_some_and(|to| total >= to) {
                break;
            }
            let next = at.saturating_add(block).min(len);
            let kept = match self.block(at..next).await? {
                Rows::Ids(kept) => kept,
                Rows::Run(rows) => rows.collect(),
            };
            // The kept rows' positions among the matches start at `total`.
            let n = kept.len() as u64;
            let lo = from.saturating_sub(total).min(n);
            let hi = to.map_or(n, |to| to.saturating_sub(total).min(n));
            if lo < hi {
                window.extend_from_slice(&kept[lo as usize..hi as usize]);
            }
            total += n;
            at = next;
            block = (block * 2).min(GAP_BLOCK);
        }
        Ok((total, Rows::Ids(window)))
    }

    /// The run's matches as chunks of their four code columns, a block of
    /// `batch_rows` rows of the run read per chunk (blocks keeping no row
    /// skipped), so a chunk holds at most `batch_rows` rows and every row of
    /// the run is read once; the stream holds the run and its position in it.
    fn chunks(self, batch_rows: usize) -> crate::CodeChunkStream {
        use futures::StreamExt as _;

        let batch = batch_rows as u64;
        futures::stream::try_unfold((self, 0u64), move |(run, mut at)| async move {
            let len = run.len();
            while at < len {
                let next = at.saturating_add(batch).min(len);
                let rows = run.block(at..next).await?;
                at = next;
                if rows.is_empty() {
                    continue;
                }
                let columns = run.columns(&rows).await?;
                return Ok(Some((Vec::from(columns), (run, at))));
            }
            Ok(None)
        })
        .boxed()
    }
}

/// The reference indexes, each `(val, rid)` sorted by `val` then `rid`: a
/// run of one code's `val` lists, ascending, the quad-table rows holding it
/// at the `(s, p, o, g)` position given here.
#[cfg(feature = "file-io")]
const REFERENCES: [(&str, usize); 2] = [("index:ref-p", 1), ("index:ref-o", 2)];

/// Rows of a bound subject's run of a subject-sorted quad table up to which
/// a lookup through a reference index checks the other bound slots at the
/// subject's rows, when they are no more than the reference run's; a longer
/// subject run declines, for the caller's generic path.
#[cfg(feature = "file-io")]
const CHECKED_ROWS: u64 = 16_384;

/// How many searches of a reference run's `rid`s cost about as much as
/// reaching one more quad-table leaf to read a slot there (about 250 ns
/// against 4 µs per leaf on WatDiv 100M's reference store, warm).
#[cfg(feature = "file-io")]
const SEARCHES_PER_LEAF: usize = 16;

/// [`located_run`](VortexRdfStore::located_run) through a reference index:
/// the shortest run among those whose slot is bound gives the quad-table rows
/// in order, all of them, or, with other slots bound, those that hold them,
/// checked within [`GAP_ROWS`]: against the other reference's run for `p` or
/// `o`, by reading the quad table for `s` or `g`. A bound subject's run of a
/// subject-sorted (`quads_sorted`) quad table is checked instead when it is
/// no longer than the shortest reference run, within [`CHECKED_ROWS`].
/// Answers the table the rows are read from (the quad table), where they
/// are, and the slots checked.
#[cfg(feature = "file-io")]
async fn by_reference(
    codes: [Option<TermCode>; 4],
    tables: &crate::store::persist::native_file::LocatedTables,
    quads_sorted: bool,
    source: &Source,
    session: &vortex_session::VortexSession,
) -> Result<Option<(Columns, RunRows, Vec<usize>)>> {
    let Some(quads) = &tables.sorted[0] else {
        return Ok(None);
    };
    let mut runs: [Option<std::ops::Range<u64>>; 2] = [None, None];
    for (i, &(_, slot)) in REFERENCES.iter().enumerate() {
        let (Some(code), Some([val, _])) = (codes[slot], &tables.references[i]) else {
            continue;
        };
        runs[i] = val.bounds(code, source, session).await?;
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
            .await?
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
    let rows = match subject {
        Some(subject) if subject.end - subject.start <= CHECKED_ROWS => RunRows::Table(subject),
        Some(_) => return Ok(None),
        None if checked.is_empty() || run.end - run.start <= GAP_ROWS => {
            let other = (0..2).filter(|&i| i != index).find_map(|i| {
                let slot = REFERENCES[i].1;
                let (Some(run), Some([_, rid])) = (&runs[i], &tables.references[i]) else {
                    return None;
                };
                checked.contains(&slot).then(|| OtherReference {
                    slot,
                    rid: rid.clone(),
                    run: run.clone(),
                })
            });
            RunRows::Reference {
                rid: rid.clone(),
                run,
                other,
            }
        }
        None => return Ok(None),
    };
    Ok(Some((quads.clone(), rows, checked)))
}

/// Of `rows` (ascending, not empty), those that `rid`'s values at `run`
/// (ascending) list too: the run walked one leaf at a time, each leaf's
/// probe found once for all the rows that fall in it, and the leaves holding
/// no row skipped by a search for the next row's bound: the rows of a short
/// run sit far apart in a long one, and walking to them read every leaf
/// between. `None` when a leaf declines its probe.
#[cfg(feature = "file-io")]
async fn listed_in(
    rows: &[u64],
    rid: &vortex_rdf_encoded_search::ColumnChunks,
    run: std::ops::Range<u64>,
    source: &Source,
    session: &vortex_session::VortexSession,
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

/// The run of a sorted table's rows holding `codes` in the first `prefix`
/// slots of its key `order` (`(s, p, o, g)` positions): the lead key's
/// bounds, narrowed key by key inside the run until it empties. `None` when a
/// chunk declines its probe.
#[cfg(feature = "file-io")]
async fn locate_run(
    columns: &Columns,
    order: &[usize; 4],
    prefix: usize,
    codes: &[Option<TermCode>; 4],
    source: &Source,
    session: &vortex_session::VortexSession,
) -> Result<Option<std::ops::Range<u64>>> {
    let lead = order[0];
    let Some(mut run) = columns[lead]
        .bounds(codes[lead].expect("a bound lead key"), source, session)
        .await?
    else {
        return Ok(None);
    };
    for &key in &order[1..prefix] {
        if run.is_empty() {
            break;
        }
        let code = codes[key].expect("a bound key");
        let Some(narrowed) = columns[key].bounds_in(run, code, source, session).await? else {
            return Ok(None);
        };
        run = narrowed;
    }
    Ok(Some(run))
}

/// Rows of a run whose rows are checked (a prefix run with bound slots past
/// it, or a reference run with other slots bound) up to which it answers; a
/// longer one declines, for the caller's other paths, as a window of it
/// checks every row to count them.
#[cfg(feature = "file-io")]
const GAP_ROWS: u64 = 1 << 23;

/// Rows of a checked run a window reads in its first block.
#[cfg(feature = "file-io")]
const FIRST_GAP_BLOCK: u64 = 1 << 12;

/// Rows of a checked run a window reads per block at most.
#[cfg(feature = "file-io")]
const GAP_BLOCK: u64 = 1 << 16;

/// The positions `offset..offset + limit` of `total` matches, clamped to
/// them (`None`: to the last).
#[cfg(feature = "file-io")]
fn window_of(total: u64, offset: usize, limit: Option<usize>) -> std::ops::Range<u64> {
    let from = (offset as u64).min(total);
    let to = limit.map_or(total, |limit| from.saturating_add(limit as u64).min(total));
    from..to
}

/// Rows up to which a column is read value by value through its leaves'
/// probes, each leaf found once for the rows inside it; more rows are taken
/// from their leaves at once, a take's fixed cost then being the smaller.
#[cfg(feature = "file-io")]
const POINT_CODES: usize = 1_024;

/// The codes of `column` at `rows`: a short run inside one leaf through that
/// leaf's probe, any other run through [`take_codes`].
#[cfg(feature = "file-io")]
async fn read_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: std::ops::Range<u64>,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
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
#[cfg(feature = "file-io")]
async fn take_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: &[u64],
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Buffer<TermCode>> {
    if rows.len() > POINT_CODES {
        return taken_codes(column, rows, source, session).await;
    }
    let mut codes = vortex_buffer::BufferMut::with_capacity(rows.len());
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
#[cfg(feature = "file-io")]
async fn taken_codes(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    rows: &[u64],
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Buffer<TermCode>> {
    use crate::session::VORTEX_SESSION;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    let taken = column.take(rows, source, session).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let prim = taken.execute::<PrimitiveArray>(&mut ctx)?;
    Ok(prim.into_buffer::<TermCode>())
}

/// The first row of `run`, a window of `column`'s ascending values, whose
/// value is not below `value` (`run.end` when none is); `None` when a chunk
/// declines its probe.
#[cfg(feature = "file-io")]
async fn seek(
    column: &vortex_rdf_encoded_search::ColumnChunks,
    run: std::ops::Range<u64>,
    value: u64,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    session: &vortex_session::VortexSession,
) -> Result<Option<u64>> {
    Ok(column
        .bounds_in(run, value, source, session)
        .await?
        .map(|hit| hit.start))
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
