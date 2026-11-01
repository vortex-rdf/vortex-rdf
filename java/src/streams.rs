//! A match read a chunk at a time, for the Java side: sent as packed payloads ([`TermStream`]) or as rows of codes
//! ([`CodeStream`]), each taking its rows through a [`Cursor`]. A stream owns what it reads (the chunks of the view
//! it pulls, or a read already done), so it lives on after the store handle that opened it is dropped.

use futures::stream::{BoxStream, StreamExt};
use vortex_buffer::Buffer;
use vortex_rdf_core::{DictReader, QuadColumn, Result, SharedQuad, TermCode, VortexRdfStore};

use crate::RUNTIME;
use crate::payload::{Limits, Pace, Payload, ROW_BYTES, encode_codes, encode_shared, write_rows};

/// A Dictionary store's rows as code columns, `(s, p, o, g)`.
type Codes = [Buffer<TermCode>; 4];

/// Rows taken a chunk at a time: the chunk being taken and its first row not taken yet, beside the chunks not pulled
/// yet. Once every row was taken it holds nothing.
struct Cursor<C> {
    /// The chunks not pulled yet; `None` once the last was, or for a read already done.
    chunks: Option<BoxStream<'static, Result<C>>>,
    /// The chunk being taken.
    chunk: Option<C>,
    /// The chunk's first row not taken yet.
    at: usize,
}

/// How many rows a chunk holds.
trait Rows {
    fn rows(&self) -> usize;
}

impl Rows for Codes {
    fn rows(&self) -> usize {
        self[0].len()
    }
}

impl Rows for Vec<SharedQuad> {
    fn rows(&self) -> usize {
        self.len()
    }
}

impl<C: Rows> Cursor<C> {
    /// The rows of `chunks`, pulled as they are taken.
    fn pulling(chunks: BoxStream<'static, Result<C>>) -> Self {
        Cursor {
            chunks: Some(chunks),
            chunk: None,
            at: 0,
        }
    }

    /// The rows of one chunk read already.
    fn of(chunk: C) -> Self {
        Cursor {
            chunks: None,
            chunk: Some(chunk),
            at: 0,
        }
    }

    /// No rows.
    fn spent() -> Self {
        Cursor {
            chunks: None,
            chunk: None,
            at: 0,
        }
    }

    /// What `take` makes of the current chunk from its first row not taken (some are left), beside the rows it took
    /// (at least one); the next chunk is pulled once the current one is spent. `None` once every row was taken:
    /// what the cursor held goes then, not when it is dropped.
    fn take<T>(&mut self, take: impl FnOnce(&C, usize) -> Result<(T, usize)>) -> Result<Option<T>> {
        loop {
            if let Some(chunk) = &self.chunk
                && self.at < chunk.rows()
            {
                let (taken, rows) = take(chunk, self.at)?;
                self.at += rows;
                return Ok(Some(taken));
            }
            let pulled = match &mut self.chunks {
                Some(chunks) => RUNTIME.block_on(chunks.next()).transpose()?,
                None => None,
            };
            match pulled {
                Some(chunk) => (self.chunk, self.at) = (Some(chunk), 0),
                None => {
                    *self = Cursor::spent();
                    return Ok(None);
                }
            }
        }
    }
}

/// A match's terms in the match's order, a payload a call under the limits it was opened with: a Dictionary
/// store's codes encoded with [`encode_codes`], any other store's quads with [`encode_shared`]. The rows come
/// from core's chunk streams, pulled a chunk at a time ([`open`](Self::open)), or from a read already done, one
/// chunk ([`codes`](Self::codes), [`shared`](Self::shared)); a chunk larger than one payload is sent over
/// several calls.
pub struct TermStream {
    terms: Terms,
    /// The rows the next payload offers.
    pace: Pace,
}

/// A term stream's rows.
enum Terms {
    /// A Dictionary store's codes, and the dictionary that spells them.
    Codes {
        dict: DictReader,
        codes: Cursor<Codes>,
    },
    /// Quads' terms.
    Shared(Cursor<Vec<SharedQuad>>),
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
        let terms = match dict {
            Some(dict) => Terms::Codes {
                dict,
                codes: Cursor::pulling(code_chunks(&view, batch_rows)?),
            },
            None => {
                let chunks = view.row_chunks(batch_rows)?;
                // The view decodes its own chunks, so the stream holds it.
                let terms = chunks.map(move |chunk| view.decode_row_chunk(&chunk?));
                Terms::Shared(Cursor::pulling(terms.boxed()))
            }
        };
        Ok(Self::new(terms, limits))
    }

    /// A stream of rows read already: a Dictionary store's codes, a column each.
    pub(crate) fn codes(dict: DictReader, columns: Codes, limits: Limits) -> Self {
        let codes = Cursor::of(columns);
        Self::new(Terms::Codes { dict, codes }, limits)
    }

    /// A stream of rows read already: quads' terms.
    pub(crate) fn shared(rows: Vec<SharedQuad>, limits: Limits) -> Self {
        Self::new(Terms::Shared(Cursor::of(rows)), limits)
    }

    fn new(terms: Terms, limits: Limits) -> Self {
        TermStream {
            terms,
            pace: Pace::new(limits),
        }
    }

    /// The next payload; `None` once every row was sent.
    pub fn next_payload(&mut self) -> Result<Option<Payload>> {
        let pace = &mut self.pace;
        let limits = pace.limits();
        match &mut self.terms {
            Terms::Codes { dict, codes } => codes.take(|columns, at| {
                let rest = std::array::from_fn(|c| &columns[c][at..]);
                let encoded = RUNTIME.block_on(encode_codes(dict, rest, limits))?;
                Ok(paced(pace, limits, columns.rows() - at, encoded))
            }),
            Terms::Shared(quads) => quads.take(|rows, at| {
                let encoded = encode_shared(&rows[at..], limits);
                Ok(paced(pace, limits, rows.len() - at, encoded))
            }),
        }
    }
}

/// A payload and the rows it holds, of `left` rows offered under `limits`, after `pace` learns from it.
fn paced(
    pace: &mut Pace,
    limits: Limits,
    left: usize,
    (payload, held): (Payload, usize),
) -> (Payload, usize) {
    pace.learn(left.min(limits.rows), held, payload.bytes());
    (payload, held)
}

/// A match's term codes in the match's order, written into the caller's rows as `match_codes` writes them: the
/// current chunk's rest is kept for the next call, so the rows a call takes are independent of the chunks'.
pub struct CodeStream(Cursor<Codes>);

impl CodeStream {
    /// A stream of `view`'s codes in chunks of at most `batch_rows` (at least 1).
    pub(crate) fn open(view: &VortexRdfStore, batch_rows: usize) -> Result<Self> {
        Ok(CodeStream(Cursor::pulling(code_chunks(view, batch_rows)?)))
    }

    /// The stream of a pattern no quad can match.
    pub(crate) fn empty() -> Self {
        CodeStream(Cursor::spent())
    }

    /// Writes up to `out.len()` rows, each its `(s, p, o, g)` codes, eight native-endian bytes apiece; returns
    /// the rows written, 0 once every row was (or when `out` holds no row).
    pub fn next(&mut self, out: &mut [[u8; ROW_BYTES]]) -> Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let written = self.0.take(|columns, at| {
            let rows = write_rows(columns, at, out);
            Ok((rows, rows))
        })?;
        Ok(written.unwrap_or(0))
    }
}

/// `view`'s rows as chunks of all four code columns, at most `batch_rows` rows each, as
/// `code_chunks(&QuadColumn::ALL, ..)` yields them.
fn code_chunks(
    view: &VortexRdfStore,
    batch_rows: usize,
) -> Result<BoxStream<'static, Result<Codes>>> {
    let chunks = view.code_chunks(&QuadColumn::ALL, batch_rows)?;
    Ok(chunks
        .map(|columns| {
            columns.map(|columns| {
                columns.try_into().unwrap_or_else(|columns: Vec<_>| {
                    panic!("{} code columns, not four", columns.len())
                })
            })
        })
        .boxed())
}
