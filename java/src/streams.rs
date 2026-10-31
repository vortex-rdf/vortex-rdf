//! A match read a chunk at a time, for the Java iterators: core's chunk streams pulled one chunk a call and
//! sent as packed payloads ([`TermStream`]) or as rows of codes ([`CodeStream`]). A stream owns the view it
//! reads, so it lives on after the store handle that opened it is dropped.

use futures::StreamExt;
use vortex_buffer::Buffer;
use vortex_rdf_core::{
    CodeChunkStream, DictReader, QuadColumn, Result, RowChunkStream, SharedQuad, TermCode,
    VortexRdfError, VortexRdfStore,
};

use crate::engine::{ROW_BYTES, RUNTIME};
use crate::payload::{Limits, Pace, Payload, encode_codes, encode_shared};

/// `InvalidInput` for a chunk of no rows.
pub(crate) fn check_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 {
        return Err(VortexRdfError::InvalidInput(
            "batch_rows must be at least 1: the most rows a chunk holds".into(),
        ));
    }
    Ok(())
}

/// A match's terms in the match's order, a payload a call: a Dictionary store's code chunks encoded with
/// [`encode_codes`], any other store's row chunks decoded with `decode_row_chunk` and encoded with
/// [`encode_shared`]. A core chunk larger than one payload is sent over several calls.
pub struct TermStream {
    /// The chunks; `None` once the last was read.
    source: Option<Source>,
    /// The current chunk's rows not sent yet.
    rest: Rest,
    /// The rows the next payload offers, set by the first call's limits.
    pace: Option<Pace>,
}

enum Source {
    Codes {
        dict: DictReader,
        chunks: CodeChunkStream,
    },
    Rows {
        /// The view the chunks are read from, which decodes them.
        view: Box<VortexRdfStore>,
        chunks: RowChunkStream,
    },
}

enum Rest {
    Codes {
        columns: [Buffer<TermCode>; 4],
        at: usize,
    },
    Rows {
        rows: Vec<SharedQuad>,
        at: usize,
    },
    Empty,
}

impl Rest {
    fn left(&self) -> usize {
        match self {
            Rest::Codes { columns, at } => columns[0].len() - at,
            Rest::Rows { rows, at } => rows.len() - at,
            Rest::Empty => 0,
        }
    }

    fn advance(&mut self, rows: usize) {
        match self {
            Rest::Codes { at, .. } | Rest::Rows { at, .. } => *at += rows,
            Rest::Empty => {}
        }
    }
}

impl TermStream {
    /// A stream of `view`'s rows in chunks of at most `batch_rows`: its codes when `dict` is the store's
    /// dictionary, its terms otherwise.
    pub(crate) fn open(
        view: VortexRdfStore,
        dict: Option<DictReader>,
        batch_rows: usize,
    ) -> Result<Self> {
        check_batch_rows(batch_rows)?;
        let source = match dict {
            Some(dict) => Source::Codes {
                dict,
                chunks: view.code_chunks(&QuadColumn::ALL, batch_rows)?,
            },
            None => Source::Rows {
                chunks: view.row_chunks(batch_rows)?,
                view: Box::new(view),
            },
        };
        Ok(TermStream {
            source: Some(source),
            rest: Rest::Empty,
            pace: None,
        })
    }

    /// The next payload of at most `limits` (the first call's limits pace every later one); `None` once
    /// every row was sent.
    pub fn next(&mut self, limits: Limits) -> Result<Option<Payload>> {
        loop {
            let left = self.rest.left();
            if left > 0 {
                let pace = self.pace.get_or_insert_with(|| Pace::new(limits));
                let paced = pace.limits();
                let (payload, held) = match (&self.rest, &self.source) {
                    (Rest::Codes { columns, at }, Some(Source::Codes { dict, .. })) => {
                        let rest = std::array::from_fn(|c| &columns[c][*at..]);
                        RUNTIME.block_on(encode_codes(dict, rest, paced))?
                    }
                    (Rest::Rows { rows, at }, _) => encode_shared(&rows[*at..], paced),
                    _ => unreachable!("code rows come from a code source"),
                };
                pace.learn(left.min(paced.rows), held, payload.bytes());
                self.rest.advance(held);
                return Ok(Some(payload));
            }
            let pulled = match &mut self.source {
                None => None,
                Some(Source::Codes { chunks, .. }) => RUNTIME
                    .block_on(chunks.next())
                    .transpose()?
                    .map(|columns| Rest::Codes {
                        columns: four_columns(columns),
                        at: 0,
                    }),
                Some(Source::Rows { view, chunks }) => {
                    match RUNTIME.block_on(chunks.next()).transpose()? {
                        Some(chunk) => Some(Rest::Rows {
                            rows: view.decode_row_chunk(&chunk)?,
                            at: 0,
                        }),
                        None => None,
                    }
                }
            };
            match pulled {
                Some(rest) => self.rest = rest,
                None => {
                    self.source = None;
                    self.rest = Rest::Empty;
                    return Ok(None);
                }
            }
        }
    }
}

/// A match's term codes in the match's order, written into the caller's rows as `match_codes` writes them: the
/// current chunk's rest is kept for the next call, so the rows a call takes are independent of the chunks'.
pub struct CodeStream {
    /// The chunks; `None` once the last was read, or for a pattern no quad can match.
    chunks: Option<CodeChunkStream>,
    /// The current chunk and its first row not written yet.
    rest: Option<([Buffer<TermCode>; 4], usize)>,
}

impl CodeStream {
    /// A stream of `view`'s codes in chunks of at most `batch_rows`.
    pub(crate) fn open(view: &VortexRdfStore, batch_rows: usize) -> Result<Self> {
        check_batch_rows(batch_rows)?;
        Ok(CodeStream {
            chunks: Some(view.code_chunks(&QuadColumn::ALL, batch_rows)?),
            rest: None,
        })
    }

    /// The stream of a pattern no quad can match.
    pub(crate) fn empty() -> Self {
        CodeStream {
            chunks: None,
            rest: None,
        }
    }

    /// Writes up to `out.len()` rows, each its `(s, p, o, g)` codes, eight native-endian bytes apiece; returns
    /// the rows written, 0 once every row was (or when `out` holds no row).
    pub fn next(&mut self, out: &mut [[u8; ROW_BYTES]]) -> Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some((columns, at)) = &mut self.rest
                && *at < columns[0].len()
            {
                let rows = out.len().min(columns[0].len() - *at);
                for (row, bytes) in out[..rows].iter_mut().enumerate() {
                    for (code, column) in
                        bytes.as_chunks_mut::<8>().0.iter_mut().zip(columns.iter())
                    {
                        *code = column[*at + row].to_ne_bytes();
                    }
                }
                *at += rows;
                return Ok(rows);
            }
            let Some(chunks) = &mut self.chunks else {
                return Ok(0);
            };
            match RUNTIME.block_on(chunks.next()).transpose()? {
                Some(columns) => self.rest = Some((four_columns(columns), 0)),
                None => {
                    self.chunks = None;
                    self.rest = None;
                }
            }
        }
    }
}

/// A chunk of all four code columns, as `code_chunks(&QuadColumn::ALL, ..)` yields it.
fn four_columns(columns: Vec<Buffer<TermCode>>) -> [Buffer<TermCode>; 4] {
    columns
        .try_into()
        .unwrap_or_else(|columns: Vec<_>| panic!("{} code columns, not four", columns.len()))
}
