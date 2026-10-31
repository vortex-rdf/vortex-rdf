//! A match read a chunk at a time, for the Java side: sent as packed payloads ([`TermStream`]) or as rows of codes
//! ([`CodeStream`]). A stream owns what it reads (the view whose chunks it pulls, or a read already done), so it
//! lives on after the store handle that opened it is dropped.

use futures::StreamExt;
use vortex_buffer::Buffer;
use vortex_rdf_core::{
    CodeChunkStream, DictReader, QuadColumn, Result, RowChunkStream, SharedQuad, TermCode,
    VortexRdfStore,
};

use crate::engine::{ROW_BYTES, RUNTIME, write_rows};
use crate::payload::{Limits, Pace, Payload, encode_codes, encode_shared};

/// A match's terms in the match's order, a payload a call under the limits it was opened with: a Dictionary
/// store's codes encoded with [`encode_codes`], any other store's quads with [`encode_shared`]. The rows come
/// from core's chunk streams, pulled a chunk at a time ([`open`](Self::open)), or from a read already done, one
/// chunk ([`codes`](Self::codes), [`shared`](Self::shared)); a chunk larger than one payload is sent over
/// several calls.
pub struct TermStream {
    /// The chunks not pulled yet; `None` once the last was, or for a read already done.
    source: Option<Source>,
    /// The current chunk.
    chunk: Chunk,
    /// The current chunk's rows already sent.
    sent: usize,
    /// The rows the next payload offers.
    pace: Pace,
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

/// Rows to send.
enum Chunk {
    /// A Dictionary store's codes, a column each, and the dictionary that spells them.
    Codes {
        dict: DictReader,
        columns: [Buffer<TermCode>; 4],
    },
    /// Quads' terms.
    Shared(Vec<SharedQuad>),
}

impl Chunk {
    fn rows(&self) -> usize {
        match self {
            Chunk::Codes { columns, .. } => columns[0].len(),
            Chunk::Shared(rows) => rows.len(),
        }
    }

    /// The rows from `at` (some left) as one payload under `limits`, and the rows it holds.
    fn encode(&self, at: usize, limits: Limits) -> Result<(Payload, usize)> {
        match self {
            Chunk::Codes { dict, columns } => {
                let rest = std::array::from_fn(|c| &columns[c][at..]);
                RUNTIME.block_on(encode_codes(dict, rest, limits))
            }
            Chunk::Shared(rows) => Ok(encode_shared(&rows[at..], limits)),
        }
    }
}

impl TermStream {
    /// A stream of `view`'s rows, pulled in chunks of at most `batch_rows` (at least 1) and sent under
    /// `limits`: its codes when `dict` is the store's dictionary, its terms otherwise.
    pub(crate) fn open(
        view: VortexRdfStore,
        dict: Option<DictReader>,
        batch_rows: usize,
        limits: Limits,
    ) -> Result<Self> {
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
        Ok(Self::new(Some(source), Chunk::Shared(Vec::new()), limits))
    }

    /// A stream of rows read already: a Dictionary store's codes, a column each.
    pub(crate) fn codes(dict: DictReader, columns: [Buffer<TermCode>; 4], limits: Limits) -> Self {
        Self::new(None, Chunk::Codes { dict, columns }, limits)
    }

    /// A stream of rows read already: quads' terms.
    pub(crate) fn shared(rows: Vec<SharedQuad>, limits: Limits) -> Self {
        Self::new(None, Chunk::Shared(rows), limits)
    }

    fn new(source: Option<Source>, chunk: Chunk, limits: Limits) -> Self {
        TermStream {
            source,
            chunk,
            sent: 0,
            pace: Pace::new(limits),
        }
    }

    /// The next payload; `None` once every row was sent.
    pub fn next_payload(&mut self) -> Result<Option<Payload>> {
        loop {
            let left = self.chunk.rows() - self.sent;
            if left > 0 {
                let limits = self.pace.limits();
                let (payload, held) = self.chunk.encode(self.sent, limits)?;
                self.pace
                    .learn(left.min(limits.rows), held, payload.bytes());
                self.sent += held;
                return Ok(Some(payload));
            }
            match self.pull()? {
                Some(chunk) => (self.chunk, self.sent) = (chunk, 0),
                None => {
                    // The end: what the stream holds goes now, not when it is freed.
                    self.source = None;
                    (self.chunk, self.sent) = (Chunk::Shared(Vec::new()), 0);
                    return Ok(None);
                }
            }
        }
    }

    /// The source's next chunk; `None` once the last was pulled.
    fn pull(&mut self) -> Result<Option<Chunk>> {
        Ok(match &mut self.source {
            None => None,
            Some(Source::Codes { dict, chunks }) => {
                next_codes(chunks)?.map(|columns| Chunk::Codes {
                    dict: dict.clone(),
                    columns,
                })
            }
            Some(Source::Rows { view, chunks }) => {
                match RUNTIME.block_on(chunks.next()).transpose()? {
                    Some(chunk) => Some(Chunk::Shared(view.decode_row_chunk(&chunk)?)),
                    None => None,
                }
            }
        })
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
    /// A stream of `view`'s codes in chunks of at most `batch_rows` (at least 1).
    pub(crate) fn open(view: &VortexRdfStore, batch_rows: usize) -> Result<Self> {
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
                let rows = write_rows(columns, *at, out);
                *at += rows;
                return Ok(rows);
            }
            let Some(chunks) = &mut self.chunks else {
                return Ok(0);
            };
            match next_codes(chunks)? {
                Some(columns) => self.rest = Some((columns, 0)),
                None => {
                    self.chunks = None;
                    self.rest = None;
                }
            }
        }
    }
}

/// The next chunk of a stream of all four code columns, as `code_chunks(&QuadColumn::ALL, ..)` yields them;
/// `None` at its end.
fn next_codes(chunks: &mut CodeChunkStream) -> Result<Option<[Buffer<TermCode>; 4]>> {
    Ok(RUNTIME.block_on(chunks.next()).transpose()?.map(|columns| {
        columns
            .try_into()
            .unwrap_or_else(|columns: Vec<_>| panic!("{} code columns, not four", columns.len()))
    }))
}
