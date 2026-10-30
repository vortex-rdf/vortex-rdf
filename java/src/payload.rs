//! The packed form a chunk of terms crosses JNI in: the UTF-8 text of the chunk's distinct terms, once each,
//! and one small integer per position naming its term. The Java side builds one `String` per distinct term
//! of a chunk rather than one per position, and the text crosses as one `byte[]`.

use std::collections::HashMap;
use std::sync::Arc;

use vortex_rdf_core::{DictReader, Result, SharedQuad, TermCode, VortexRdfError};

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
    /// 65,536 rows, 16 MiB, terms over 1 MiB on their own.
    pub const DEFAULT: Limits = Limits {
        rows: 65_536,
        chunk_bytes: 16 << 20,
        large_term: 1 << 20,
    };

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

    /// The term at `position`, read as `Payload.java` reads it; `None` for a lookup's miss.
    pub fn term(&self, position: usize) -> Option<&str> {
        match self.terms[position] {
            NONE => None,
            large if large < 0 => Some(&self.large[(-2 - large) as usize]),
            k => {
                let k = 2 * k as usize;
                let bytes = &self.text[self.bounds[k] as usize..self.bounds[k + 1] as usize];
                Some(std::str::from_utf8(bytes).expect("a term's bounds hold whole characters"))
            }
        }
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
    let (payload, positions) = encode_distinct(dict, 4 * rows, 4, at, limits).await?;
    if payload.terms.contains(&NONE) {
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
    encode_distinct(dict, count, 1, |position| codes[position], limits).await
}

/// Buckets of the per-column cache that finds a chunk's repeated codes.
const BUCKETS: usize = 256;

/// The first `positions` positions, `width` to a row, the code at position `p` being `code(p)`, as one
/// chunk: cut at a row boundary to keep the distinct terms within the budget, at least one row. Returns the
/// chunk and the positions it holds.
///
/// The distinct codes are decoded in one call, the common case. When their terms pass the budget, that call
/// copies only what fits and measures the rest, and a second call decodes the rows that fit.
async fn encode_distinct(
    dict: &DictReader,
    positions: usize,
    width: usize,
    code: impl Fn(usize) -> TermCode,
    limits: Limits,
) -> Result<(Payload, usize)> {
    if positions == 0 {
        return Ok((Payload::default(), 0));
    }
    let (distinct, slots) = distinct_codes(positions, width, code);
    let first = decode(dict, &distinct, limits.large_term, limits.chunk_bytes).await?;
    if !first.over {
        return Ok((first.payload.with_terms(&slots, &first.entries), positions));
    }
    let kept = width * fitting_rows(&slots, width, &first.lengths, limits.chunk_bytes);
    // Slots are made in position order, so the slots of the kept positions are the first ones.
    let used = slots[..kept]
        .iter()
        .max()
        .map_or(0, |&slot| slot as usize + 1);
    let second = decode(dict, &distinct[..used], limits.large_term, usize::MAX).await?;
    Ok((
        second.payload.with_terms(&slots[..kept], &second.entries),
        kept,
    ))
}

/// The distinct codes of the first `positions` positions, `width` to a row, and each position's slot among
/// them, found as Python's `decode_slice` finds them: a direct-mapped cache of recent codes per column; a
/// hit shares its slot, a miss (a new code, or one its bucket lost) makes a new one. Slots are made in
/// position order.
fn distinct_codes(
    positions: usize,
    width: usize,
    code: impl Fn(usize) -> TermCode,
) -> (Vec<TermCode>, Vec<u32>) {
    let mut recent = vec![[(0, u32::MAX); BUCKETS]; width];
    let mut distinct = Vec::new();
    let mut slots = Vec::with_capacity(positions);
    for position in 0..positions {
        let code = code(position);
        let entry = &mut recent[position % width][(code % BUCKETS as TermCode) as usize];
        if entry.1 == u32::MAX || entry.0 != code {
            *entry = (code, distinct.len() as u32);
            distinct.push(code);
        }
        slots.push(entry.1);
    }
    (distinct, slots)
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
}

impl Payload {
    /// The payload with each position's entry, from its slot.
    fn with_terms(mut self, slots: &[u32], entries: &[i32]) -> Payload {
        self.terms = slots.iter().map(|&slot| entries[slot as usize]).collect();
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
    let mut bytes = 0usize;
    dict.decode_many_with(distinct, |i, term| {
        if let Some(term) = term {
            lengths[i] = term.len();
            bytes += term.len();
            if bytes <= budget {
                entries[i] = payload.push(term, large_term);
            }
        }
    })
    .await?;
    Ok(Decoded {
        payload,
        entries,
        lengths,
        over: bytes > budget,
    })
}

/// How many rows from the first keep their distinct terms within `budget` bytes, at least one. Walked in
/// position order, a slot's term counts at the slot's first position, where it was made.
fn fitting_rows(slots: &[u32], width: usize, lengths: &[usize], budget: usize) -> usize {
    let (mut made, mut bytes) = (0, 0);
    for (row, positions) in slots.chunks(width).enumerate() {
        for &slot in positions {
            if slot as usize == made {
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
        // A shared term twice in the row counts twice: an estimate from above.
        let new: usize = terms
            .iter()
            .filter(|term| shared(term).is_none_or(|key| !sent.contains_key(&key)))
            .map(|term| term.len())
            .sum();
        if held > 0 && bytes + new > limits.chunk_bytes {
            break;
        }
        for term in terms {
            let key = shared(term);
            let entry = match key.and_then(|key| sent.get(&key)) {
                Some(&entry) => entry,
                None => {
                    bytes += term.len();
                    let entry = payload.push(term, limits.large_term);
                    if let Some(key) = key {
                        sent.insert(key, entry);
                    }
                    entry
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

    /// Learns from a chunk that held `held` of the `offered` rows, in `bytes` bytes of distinct terms.
    pub fn learn(&mut self, offered: usize, held: usize, bytes: usize) {
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
