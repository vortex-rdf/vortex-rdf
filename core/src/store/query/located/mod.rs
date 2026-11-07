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
//!
//! The run itself lives in three file-only modules: [`plan`] chooses the
//! run that answers a pattern, [`run`] reads it, and [`statistics`] asks
//! the file's footer and zone maps which of its rows its checks keep.

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
use crate::store::read::chunks::check_batch;
use crate::store::schema::TermCode;
use crate::{CodeChunkStream, DictReader, KindRanges, Probe, QuadColumn};

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
    /// Each window is found anew: a run whose rows are checked finds the
    /// window's first zone by its zones' counts where the statistics decide
    /// it, and is otherwise checked from its start as far as the window's
    /// last row (no further, as no count is asked), so a caller reading a
    /// long match through [`match_code_chunks`](Self::match_code_chunks)
    /// reads it once. A first window that the run's first 4,096 rows fill
    /// is read from them before any statistics are asked.
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
        if let Some(mut run) = self.located_run(pattern).await? {
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
    ) -> Result<CodeChunkStream> {
        use futures::StreamExt as _;

        check_batch(batch_rows)?;
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
                .code_chunks(&QuadColumn::ALL, batch_rows),
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
    /// slots that are no such prefix (a bound slot after a free one) are the
    /// run of the longest bound prefix, its rows checked against the others:
    /// first by the file's footer statistics, which settle a slot for every
    /// row; then zone by zone where the checked columns have zone maps; row
    /// by row otherwise, when the run is no longer than [`GAP_ROWS`](statistics::GAP_ROWS). That
    /// run gives way to a bound `p`'s or `o`'s run in its reference index
    /// (`index:ref-p`, `index:ref-o`) that is strictly shorter and answers,
    /// as a hub subject's quads do to its rare object's (`s?o`); when the
    /// run does not answer, the shortest such run is read. A reference run's rows are read from the
    /// quad table in its order, the other bound slots checked at them when
    /// it is no longer than [`GAP_ROWS`](statistics::GAP_ROWS); a bound graph is checked on the
    /// reference child's own `g` where it holds one, and when the graph is
    /// the run's only check the child's zone maps decide it, at any length.
    /// A code past the dictionary, or one in a position no quad holds,
    /// matches nothing. Whether it answers depends on the store and
    /// `pattern` alone, never on the window, so every window of one pattern
    /// comes from the same table, in the same order. A run whose rows are
    /// checked is counted whole: zone by zone where the statistics decide it
    /// (zones they settle by their size, a mixed zone lying whole within the
    /// run by the values its graph column recorded, the rest on the column's
    /// encoding or by their rows), a block at a time otherwise, and only the
    /// window's rows are kept; nothing is kept between calls. With the
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
                Some(mut run) => run.window(offset, limit).await.map(Some),
                None => Ok(None),
            }
        }
        #[cfg(not(feature = "file-io"))]
        {
            let _ = (pattern, offset, limit);
            Ok(None)
        }
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

#[cfg(feature = "file-io")]
mod plan;
#[cfg(feature = "file-io")]
mod run;
#[cfg(feature = "file-io")]
mod statistics;

#[cfg(feature = "file-io")]
pub(crate) use plan::LocatedTables;
