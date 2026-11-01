//! The two forms a match's rows cross JNI in. Terms cross packed ([`Payload`]): the UTF-8 text of the chunk's
//! distinct terms, once each, and one small integer per position naming its term, so the Java side builds one
//! `String` per distinct term of a chunk rather than one per position, and the text crosses as one `byte[]`.
//! Codes cross as rows of [`ROW_BYTES`] bytes ([`write_rows`]).

use std::collections::HashMap;
use std::sync::Arc;

use vortex_buffer::Buffer;
use vortex_rdf_core::{DictReader, Result, SharedQuad, TermCode, VortexRdfError, distinct_codes};

/// Bytes per row [`Store::match_codes`](crate::engine::Store::match_codes) writes: four codes of eight bytes.
pub const ROW_BYTES: usize = 32;

/// Writes rows `from..` of `columns`, as many as `out` holds, into `out`: each its `(s, p, o, g)` codes, eight
/// native-endian bytes apiece. Returns the rows written.
pub(crate) fn write_rows(
    columns: &[Buffer<TermCode>; 4],
    from: usize,
    out: &mut [[u8; ROW_BYTES]],
) -> usize {
    let rows = out.len().min(columns[0].len() - from);
    for (row, bytes) in out[..rows].iter_mut().enumerate() {
        for (code, column) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(columns) {
            *code = column[from + row].to_ne_bytes();
        }
    }
    rows
}

/// How much one chunk may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Rows per chunk; codes, for a lookup.
    pub rows: usize,
    /// Bytes of the chunk's distinct terms, large ones included. A chunk of one row may pass it, by its large
    /// terms only.
    pub chunk_bytes: usize,
    /// Terms of more bytes than this travel as their own strings, never in the shared text.
    pub large_term: usize,
}

impl Limits {
    /// `InvalidInput` unless `rows >= 1`, `large_term >= 1` and `chunk_bytes >= 4 * large_term` (a row's
    /// shared text always fits), and unless the payload's `int`s address them: `rows <= i32::MAX / 4` and
    /// `chunk_bytes <= i32::MAX`.
    pub fn new(rows: usize, chunk_bytes: usize, large_term: usize) -> Result<Limits> {
        let valid = (1..=i32::MAX as usize / 4).contains(&rows)
            && large_term >= 1
            && chunk_bytes <= i32::MAX as usize
            && large_term
                .checked_mul(4)
                .is_some_and(|row| row <= chunk_bytes);
        if !valid {
            return Err(VortexRdfError::InvalidInput(format!(
                "chunk limits out of range: {rows} rows, {chunk_bytes} bytes, terms over {large_term} bytes \
                 on their own"
            )));
        }
        Ok(Limits {
            rows,
            chunk_bytes,
            large_term,
        })
    }
}

/// One chunk, as `Payload.java` reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Payload {
    /// The UTF-8 text of the chunk's distinct terms of at most `large_term` bytes, in any order.
    pub text: Vec<u8>,
    /// `bounds[2k]` and `bounds[2k + 1]`: where distinct term `k` starts and ends in `text`.
    pub bounds: Vec<i32>,
    /// One per position, four per row (row-major) or one per looked-up code: `k >= 0` is distinct term `k`,
    /// `-1` no term (a lookup's miss), `-2 - j` is `large[j]`.
    pub terms: Vec<i32>,
    /// The chunk's distinct terms of more than `large_term` bytes.
    pub large: Vec<String>,
}

/// The entry of `Payload::terms` for a position holding no term.
const NONE: i32 = -1;

impl Payload {
    /// The number of positions.
    pub fn positions(&self) -> usize {
        self.terms.len()
    }

    /// The bytes of the chunk's distinct terms, large ones included: what [`Limits::chunk_bytes`] budgets.
    pub fn bytes(&self) -> usize {
        self.text.len() + self.large.iter().map(String::len).sum::<usize>()
    }

    /// Adds a distinct term; returns its entry for `terms`.
    fn push(&mut self, term: &str, large_term: usize) -> i32 {
        if term.len() > large_term {
            self.large.push(term.to_owned());
            -1 - self.large.len() as i32
        } else {
            let start = self.text.len() as i32;
            self.text.extend_from_slice(term.as_bytes());
            self.bounds.extend([start, self.text.len() as i32]);
            (self.bounds.len() / 2 - 1) as i32
        }
    }
}

/// The rows of `columns` (`s`, `p`, `o`, `g`) from the first, as one chunk: at most `limits.rows`, and no
/// more than keep the chunk's distinct terms within `limits.chunk_bytes` (at least one row). Returns the
/// chunk and the rows it holds. A code outside the dictionary fails with the message of the per-term read.
pub async fn encode_codes(
    dict: &DictReader,
    columns: [&[TermCode]; 4],
    limits: Limits,
) -> Result<(Payload, usize)> {
    let rows = columns[0].len().min(limits.rows);
    let at = |position: usize| columns[position % 4][position / 4];
    let (payload, positions, missed) = encode_distinct::<4>(dict, 4 * rows, at, limits).await?;
    if missed {
        return Err(VortexRdfError::Deserialization(
            "a matched row holds a code outside the dictionary".into(),
        ));
    }
    Ok((payload, positions / 4))
}

/// The terms of `codes` from the first, as one chunk cut as [`encode_codes`] cuts rows, a code to a row.
/// Returns the chunk and the codes it holds; a code outside the dictionary is a position with no term.
pub async fn encode_lookup(
    dict: &DictReader,
    codes: &[TermCode],
    limits: Limits,
) -> Result<(Payload, usize)> {
    let count = codes.len().min(limits.rows);
    let (payload, held, _) =
        encode_distinct::<1>(dict, count, |position| codes[position], limits).await?;
    Ok((payload, held))
}

/// The first `positions` positions, `W` to a row, the code at position `p` being `code(p)`, as one chunk:
/// cut at a row boundary to keep the distinct terms within the budget, at least one row. Returns the chunk, the
/// positions it holds, and whether one of them holds a code outside the dictionary. The distinct codes are found as the bindings find them ([`distinct_codes`], a
/// cache of recent codes per column), and slots are made in position order, so a prefix of rows gets the same
/// slots however many follow it.
///
/// The distinct codes are decoded in one call, the common case. When their terms pass the budget, that call
/// copies only what fits and measures the rest, and a second call decodes the rows that fit.
async fn encode_distinct<const W: usize>(
    dict: &DictReader,
    positions: usize,
    code: impl Fn(usize) -> TermCode,
    limits: Limits,
) -> Result<(Payload, usize, bool)> {
    if positions == 0 {
        return Ok((Payload::default(), 0, false));
    }
    let (distinct, slots) = distinct_codes::<W>((0..positions).map(code));
    let first = decode(dict, &distinct, limits.large_term, limits.chunk_bytes).await?;
    if !first.over {
        let payload = first.payload.with_terms(&slots, &first.entries);
        return Ok((payload, positions, first.missed));
    }
    let kept = W * fitting_rows(&slots, W, &first.lengths, limits.chunk_bytes);
    // Slots are made in position order, so the slots of the kept positions are the first ones, each held by one.
    let used = slots[..kept].iter().max().map_or(0, |&slot| slot + 1);
    let second = decode(dict, &distinct[..used], limits.large_term, usize::MAX).await?;
    let payload = second.payload.with_terms(&slots[..kept], &second.entries);
    Ok((payload, kept, second.missed))
}

/// A chunk's distinct terms, decoded.
struct Decoded {
    /// The payload, `terms` aside; complete unless `over`.
    payload: Payload,
    /// Each distinct code's entry for `terms`.
    entries: Vec<i32>,
    /// Each distinct code's term length in bytes, 0 for a code outside the dictionary.
    lengths: Vec<usize>,
    /// Whether the terms passed the budget, which leaves the payload partial.
    over: bool,
    /// Whether a distinct code is outside the dictionary.
    missed: bool,
}

impl Payload {
    /// The payload with each position's entry, from its slot.
    fn with_terms(mut self, slots: &[usize], entries: &[i32]) -> Payload {
        self.terms = slots.iter().map(|&slot| entries[slot]).collect();
        self
    }
}

/// Decodes `distinct` in one call, copying terms while their total stays within `budget` bytes and only
/// measuring them past it, so a chunk that will be cut never holds more than its budget.
async fn decode(
    dict: &DictReader,
    distinct: &[TermCode],
    large_term: usize,
    budget: usize,
) -> Result<Decoded> {
    let mut payload = Payload::default();
    let mut entries = vec![NONE; distinct.len()];
    let mut lengths = vec![0; distinct.len()];
    let (mut bytes, mut missed) = (0usize, false);
    dict.decode_many_with(distinct, |i, term| match term {
        Some(term) => {
            lengths[i] = term.len();
            bytes += term.len();
            if bytes <= budget {
                entries[i] = payload.push(term, large_term);
            }
        }
        None => missed = true,
    })
    .await?;
    Ok(Decoded {
        payload,
        entries,
        lengths,
        over: bytes > budget,
        missed,
    })
}

/// How many rows from the first keep their distinct terms within `budget` bytes, at least one. Walked in
/// position order, a slot's term counts at the slot's first position, where it was made.
fn fitting_rows(slots: &[usize], width: usize, lengths: &[usize], budget: usize) -> usize {
    let (mut made, mut bytes) = (0, 0);
    for (row, positions) in slots.chunks(width).enumerate() {
        for &slot in positions {
            if slot == made {
                bytes += lengths[made];
                made += 1;
            }
        }
        if bytes > budget {
            return row.max(1);
        }
    }
    slots.len() / width
}

/// The rows from the first, as one chunk cut as [`encode_codes`] cuts it. A term whose `Arc` is shared
/// (strong count above one) is keyed by its pointer and sent once, as Python's `matched_columns` does; any
/// other is sent as it comes. Returns the chunk and the rows it holds.
pub fn encode_shared(rows: &[SharedQuad], limits: Limits) -> (Payload, usize) {
    let shared =
        |term: &Arc<str>| (Arc::strong_count(term) > 1).then(|| Arc::as_ptr(term).cast::<u8>());
    let mut payload = Payload::default();
    let mut sent: HashMap<*const u8, i32> = HashMap::new();
    let (mut bytes, mut held) = (0, 0);
    for quad in rows.iter().take(limits.rows) {
        let terms = [&quad.s, &quad.p, &quad.o, &quad.g];
        let keys = terms.map(shared);
        let sent_as = keys.map(|key| key.and_then(|key| sent.get(&key).copied()));
        // A shared term twice in the row counts twice: an estimate from above.
        let new: usize = terms
            .iter()
            .zip(&sent_as)
            .filter(|(_, entry)| entry.is_none())
            .map(|(term, _)| term.len())
            .sum();
        if held > 0 && bytes + new > limits.chunk_bytes {
            break;
        }
        for ((term, key), entry) in terms.into_iter().zip(keys).zip(sent_as) {
            let entry = match entry {
                Some(entry) => entry,
                None => {
                    let mut add = || {
                        bytes += term.len();
                        payload.push(term, limits.large_term)
                    };
                    // Keyed through its entry: a shared term twice in the row is sent once.
                    match key {
                        Some(key) => *sent.entry(key).or_insert_with(add),
                        None => add(),
                    }
                }
            };
            payload.terms.push(entry);
        }
        held += 1;
    }
    (payload, held)
}

/// The rows a read's next chunk offers its encoder: `limits.rows` at first; after a chunk the byte budget
/// cut, the rows that fit, so a read of long terms decodes past its budget about once rather than in every
/// chunk; doubled again, up to `limits.rows`, after a chunk that filled at most half its budget.
#[derive(Clone, Copy, Debug)]
pub struct Pace {
    limits: Limits,
    rows: usize,
}

impl Pace {
    pub fn new(limits: Limits) -> Pace {
        Pace {
            limits,
            rows: limits.rows,
        }
    }

    /// The limits the next chunk is encoded under.
    pub fn limits(&self) -> Limits {
        Limits {
            rows: self.rows,
            ..self.limits
        }
    }

    /// Learns from a chunk that held `held` rows in `bytes` bytes of distinct terms, the read having had `left`
    /// rows to send (the chunk was offered as many, up to the pace's rows).
    pub fn learn(&mut self, left: usize, held: usize, bytes: usize) {
        let offered = left.min(self.rows);
        self.rows =
            paced(self.rows, offered, held, bytes, self.limits.chunk_bytes).min(self.limits.rows);
    }

    /// The codes a lookup's next slice offers after one that held `held` of the `offered` codes in `bytes`
    /// bytes, under a budget of `chunk_bytes`: the same rule, for a lookup whose slices its Java caller cuts
    /// and caps.
    pub fn next_slice(offered: usize, held: usize, bytes: usize, chunk_bytes: usize) -> usize {
        paced(offered, offered, held, bytes, chunk_bytes)
    }
}

/// The rows after a chunk that held `held` of the `offered` rows in `bytes` bytes, the pace having been
/// `rows`: the rows that fit after a cut, twice as many after a chunk at most half full, else the same.
fn paced(rows: usize, offered: usize, held: usize, bytes: usize, chunk_bytes: usize) -> usize {
    if held < offered {
        held.max(1)
    } else if bytes <= chunk_bytes / 2 {
        rows.saturating_mul(2)
    } else {
        rows
    }
}
