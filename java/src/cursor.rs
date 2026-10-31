//! A match read as packed chunks: the rows are found once, as the per-term read finds them, then encoded a
//! chunk at a time, so the Java side holds one chunk of terms beside its result rather than the whole read.

use vortex_buffer::Buffer;
use vortex_rdf_core::{DictReader, Result, SharedQuad, TermCode};

use crate::engine::RUNTIME;
use crate::payload::{Limits, Pace, Payload, encode_codes, encode_shared};

/// The rows of one read and how far it has been sent.
pub struct QuadCursor {
    rows: Rows,
    at: usize,
    pace: Pace,
}

enum Rows {
    /// A Dictionary store's matched codes, a column each.
    Codes {
        dict: DictReader,
        columns: [Buffer<TermCode>; 4],
    },
    /// The matched quads' terms.
    Shared(Vec<SharedQuad>),
}

impl QuadCursor {
    pub(crate) fn codes(dict: DictReader, columns: [Buffer<TermCode>; 4], limits: Limits) -> Self {
        Self::new(Rows::Codes { dict, columns }, limits)
    }

    pub(crate) fn shared(rows: Vec<SharedQuad>, limits: Limits) -> Self {
        Self::new(Rows::Shared(rows), limits)
    }

    fn new(rows: Rows, limits: Limits) -> Self {
        QuadCursor {
            rows,
            at: 0,
            pace: Pace::new(limits),
        }
    }

    /// The read's rows, sent or not.
    pub fn rows(&self) -> usize {
        match &self.rows {
            Rows::Codes { columns, .. } => columns[0].len(),
            Rows::Shared(rows) => rows.len(),
        }
    }
}

impl Iterator for QuadCursor {
    type Item = Result<Payload>;

    /// The next chunk of rows; `None` once every row was sent.
    fn next(&mut self) -> Option<Result<Payload>> {
        let left = self.rows() - self.at;
        if left == 0 {
            return None;
        }
        let limits = self.pace.limits();
        let chunk = match &self.rows {
            Rows::Codes { dict, columns } => {
                let rest = std::array::from_fn(|c| &columns[c][self.at..]);
                RUNTIME.block_on(encode_codes(dict, rest, limits))
            }
            Rows::Shared(rows) => Ok(encode_shared(&rows[self.at..], limits)),
        };
        Some(chunk.map(|(payload, held)| {
            self.pace
                .learn(left.min(limits.rows), held, payload.bytes());
            self.at += held;
            payload
        }))
    }
}
