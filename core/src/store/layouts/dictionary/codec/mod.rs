//! Dictionary-layout chunks: four [`TermCode`] (`u64`) columns encoded through
//! the store's term dictionary, and decoded back into quads. A chunk needs
//! the complete dictionary, so these builds bypass the generic
//! `build_struct_array` path; index children are built over the codes,
//! which sort as the terms do.

use std::ops::Range;
use std::sync::Arc;

use oxrdf::Quad;
#[cfg(feature = "file-io")]
use rustc_hash::FxHashMap;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::quad::NQuadLines;
use crate::common::terms::{Trust, graph_name, named_node, subject, term};
use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{field_as, named_struct, stamp_is_sorted};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS, TermCode};
use crate::store::{RawQuad, SharedQuad};

#[cfg(feature = "file-io")]
use super::file_backed::{FileBackedDict, HeldWindows};
use super::ingest::BorrowedTermCodeMap;
use super::storage::DictCursor;
use super::term_dict::{CodeSpace, TermDictionary, check_code};

/// Dictionary-encoded quad columns: every term replaced by its code. Code
/// order is the terms' byte order, so index children built over the codes
/// sort as they would over the terms. The default is the encoding of no
/// quads, whose chunk still carries the code dtypes.
#[derive(Default)]
pub(crate) struct QuadCodes {
    pub(crate) s: Vec<TermCode>,
    pub(crate) p: Vec<TermCode>,
    pub(crate) o: Vec<TermCode>,
    pub(crate) g: Vec<TermCode>,
}

impl QuadCodes {
    /// No codes yet, with room for `n` quads'.
    pub(crate) fn with_capacity(n: usize) -> Self {
        Self {
            s: Vec::with_capacity(n),
            p: Vec::with_capacity(n),
            o: Vec::with_capacity(n),
            g: Vec::with_capacity(n),
        }
    }

    /// Append one quad's `[s, p, o, g]` codes.
    #[inline]
    pub(crate) fn push(&mut self, [s, p, o, g]: [TermCode; 4]) {
        self.s.push(s);
        self.p.push(p);
        self.o.push(o);
        self.g.push(g);
    }
}

/// Every term of every quad as its code in `code_map`; a `Serialization`
/// error for a term the map lacks.
pub(crate) fn encode_quads(
    quads: &[RawQuad],
    code_map: &BorrowedTermCodeMap<'_>,
) -> Result<QuadCodes> {
    let start = debug::timer();
    let encode_column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<TermCode>> {
        let mut codes: Vec<TermCode> = Vec::with_capacity(quads.len());
        for q in quads {
            let term = term_of(q);
            let code = code_map.get(term).copied().ok_or_else(|| {
                VortexRdfError::Serialization(format!(
                    "Term missing from dictionary during encoding: {}",
                    term
                ))
            })?;
            codes.push(code);
        }
        Ok(codes)
    };
    let codes = QuadCodes {
        s: encode_column(|q| &q.s)?,
        p: encode_column(|q| &q.p)?,
        o: encode_column(|q| &q.o)?,
        g: encode_column(|q| &q.g)?,
    };
    log::debug!(
        "[Dictionary] Encoded {} quads ({} term lookups, {} dictionary terms) in {:?}",
        quads.len(),
        quads.len().saturating_mul(4),
        code_map.len(),
        debug::elapsed(start)
    );
    Ok(codes)
}

/// The chunk of rows `range` of `codes`: the four code columns and nothing
/// else (the dictionary travels beside the array). `s_sorted` stamps
/// `IsSorted` on the `s` column, which queries trust as global (s, p, o, g)
/// order: it must be `true` only for rows sorted so, and it holds over
/// codes because code order is term order.
pub(crate) fn build_code_chunk(
    codes: &QuadCodes,
    range: Range<usize>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let start = debug::timer();
    let n = range.len();
    let arrays: Vec<ArrayRef> = vec![
        PrimitiveArray::from_iter(codes.s[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.p[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.o[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.g[range].iter().copied()).into_array(),
    ];
    if s_sorted {
        stamp_is_sorted(&arrays[0]);
    }
    let chunk = named_struct(&PRIMARY_COLUMNS, arrays, n)?.into_array();
    log::debug!(
        "[Dictionary] Built encoded chunk of {} rows in {:?}",
        n,
        debug::elapsed(start)
    );
    Ok(chunk)
}

/// The four code columns of `chunk` in `[s, p, o, g]` order. The decoders'
/// [`TermCode`] slices ([`code_slices`]) borrow them, so they must outlive
/// the decode.
fn code_columns(chunk: &ArrayRef) -> Result<[PrimitiveArray; 4]> {
    #[cfg(test)]
    tests::CODE_COLUMN_READS.with(|reads| reads.set(reads.get() + 1));
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx)?;
    let mut col = |name: &str| field_as::<PrimitiveArray>(&struct_arr, name, &mut ctx);
    Ok([col(COL_S)?, col(COL_P)?, col(COL_O)?, col(COL_G)?])
}

/// The code slices of [`code_columns`]' arrays, in the same order.
fn code_slices(columns: &[PrimitiveArray; 4]) -> [&[TermCode]; 4] {
    columns
        .each_ref()
        .map(|column| column.as_slice::<TermCode>())
}

/// Where a decode reads a code's term. The four roles are asked separately,
/// so a dictionary-backed source can keep one cursor per role: the roles
/// occupy different regions of the sorted terms, and each keeps its own
/// chunk cursors warm.
trait TermSource {
    /// A term as [`decode_rows_shared`] holds it from row to row.
    type Shared: SharedTerm;

    /// The term with `code`, read for `role`.
    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str>;

    /// [`str_at`](Self::str_at) as a [`Shared`](Self::Shared) term.
    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Self::Shared>;

    /// Whether the source gave up on the chunk ([`FileTerms`]): the decode
    /// then ends at the row that failed, and its caller decodes the chunk
    /// another way. Asked only after a failed read.
    fn gave_up(&self) -> bool {
        false
    }
}

/// A term [`decode_rows_shared`] holds: a shared string, or a borrow of one.
trait SharedTerm: Clone + AsRef<str> {
    /// The term as a shared string: itself, or a reference-count bump of the
    /// borrowed one.
    fn into_shared(self) -> Arc<str>;
}

impl SharedTerm for Arc<str> {
    fn into_shared(self) -> Arc<str> {
        self
    }
}

#[cfg(feature = "file-io")]
impl SharedTerm for &Arc<str> {
    fn into_shared(self) -> Arc<str> {
        Arc::clone(self)
    }
}

/// A row of four term strings, built from the terms [`decode_rows_shared`]
/// holds, shared.
trait StringRow {
    fn from_terms<T: SharedTerm>(terms: [T; 4]) -> Self;
}

impl StringRow for SharedQuad {
    fn from_terms<T: SharedTerm>([s, p, o, g]: [T; 4]) -> Self {
        SharedQuad {
            s: s.into_shared(),
            p: p.into_shared(),
            o: o.into_shared(),
            g: g.into_shared(),
        }
    }
}

/// Term strings read from a resident dictionary, one cursor per role.
struct DictTerms<'a> {
    cursors: [DictCursor<'a>; 4],
    code_space: CodeSpace,
}

impl<'a> DictTerms<'a> {
    fn new(dict: &'a TermDictionary) -> Self {
        Self {
            cursors: [dict.cursor(), dict.cursor(), dict.cursor(), dict.cursor()],
            code_space: dict.code_space(),
        }
    }
}

impl TermSource for DictTerms<'_> {
    type Shared = Arc<str>;

    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
        let rank = check_code(code, self.code_space)?;
        Ok(self.cursors[role].str_at(rank))
    }

    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
        self.str_at(role, code).map(Arc::from)
    }
}

/// The terms of a chunk's distinct codes, keyed by code: the code → term
/// lookup of a chunk decoded against a file-backed dictionary. The hasher is
/// a fast one, since the keys are ranks the store itself produced.
#[cfg(feature = "file-io")]
type ChunkTerms = FxHashMap<TermCode, Arc<str>>;

/// Term strings read from a chunk's resolved [`ChunkTerms`] (the file-backed
/// path): a string decode holds borrows of the map's shared strings.
#[cfg(feature = "file-io")]
struct MappedTerms<'a>(&'a ChunkTerms);

#[cfg(feature = "file-io")]
impl<'a> MappedTerms<'a> {
    fn get(&self, code: TermCode) -> Result<&'a Arc<str>> {
        self.0.get(&code).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "Term code {} missing from the chunk's resolved term map",
                code
            ))
        })
    }
}

#[cfg(feature = "file-io")]
impl<'a> TermSource for MappedTerms<'a> {
    type Shared = &'a Arc<str>;

    fn str_at(&mut self, _role: usize, code: TermCode) -> Result<&str> {
        self.get(code).map(|term| &**term)
    }

    fn shared_at(&mut self, _role: usize, code: TermCode) -> Result<&'a Arc<str>> {
        self.get(code)
    }
}

/// Term strings read in place from a file-backed dictionary's windows
/// ([`HeldWindows`]), as [`DictTerms`] reads a resident dictionary's chunks:
/// a term is decompressed into its role's scratch. When the hold gives up
/// ([`TermSource::gave_up`]), the chunk decodes the resolved way
/// ([`resolve_chunk_terms`]).
#[cfg(feature = "file-io")]
struct FileTerms<'a> {
    held: HeldWindows<'a>,
    scratch: [Vec<u8>; 4],
}

#[cfg(feature = "file-io")]
impl TermSource for FileTerms<'_> {
    type Shared = Arc<str>;

    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
        self.held.str_at(code, &mut self.scratch[role])
    }

    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
        self.str_at(role, code).map(Arc::from)
    }

    fn gave_up(&self) -> bool {
        self.held.gave_up()
    }
}

/// Most slots of a role memo, so a memo never grows with the data.
const MEMO_MAX_SLOTS: usize = 1024;

/// Chunks below this many rows decode without a memo: the table's own
/// allocation would cost more than the few repeats it could catch.
const MEMO_MIN_ROWS: usize = 16;

/// Misses a memo's trial ([`TermMemo::new`]) counts before it decides
/// whether to keep admitting terms.
const MEMO_TRIAL_MISSES: u32 = 64;

/// Misses per hit past which a memo's trial ends its admissions: a role
/// whose codes do not come back within the memo's reach (an object column of
/// mostly distinct terms) would copy each term into a slot no row reads.
const MEMO_MISSES_PER_HIT: u32 = 8;

/// Whether a memo keeps the terms it decodes.
#[derive(Clone, Copy)]
enum Admission {
    /// Keeps each one, counting the memo's misses and hits up to the
    /// [`MEMO_TRIAL_MISSES`]th miss, which decides the rest: at least one
    /// hit per [`MEMO_MISSES_PER_HIT`] misses keeps admitting.
    Trial { misses: u32, hits: u32 },
    /// Keeps each one.
    Admit,
    /// Keeps none; the terms already kept still answer.
    Refuse,
}

/// A direct-mapped memo of one role's decoded terms, keyed by code: one slot
/// per masked code, overwritten on collision. Codes repeat down a column (a
/// predicate or graph on nearly every row), and each repeat would pay the
/// read (an FSST decompress) and the parse again; the fixed size keeps a
/// high-cardinality column from piling up entries it never reads again, and
/// the [`Admission`] trial keeps such a column from copying every term into
/// a slot.
struct TermMemo<T> {
    slots: Vec<Option<(TermCode, T)>>,
    mask: usize,
    admission: Admission,
}

impl<T: Clone> TermMemo<T> {
    /// Sized to `rows`, which bound the chunk's distinct codes: a power of
    /// two, at most [`MEMO_MAX_SLOTS`], and no table below [`MEMO_MIN_ROWS`].
    /// Its admissions go through a trial ([`Admission::Trial`]).
    fn new(rows: usize) -> Self {
        let slots = if rows < MEMO_MIN_ROWS {
            0
        } else {
            rows.next_power_of_two().clamp(1, MEMO_MAX_SLOTS)
        };
        Self {
            slots: vec![None; slots],
            mask: slots.saturating_sub(1),
            admission: Admission::Trial { misses: 0, hits: 0 },
        }
    }

    /// [`new`](Self::new), keeping every term it decodes: for shared terms,
    /// whose copy is a reference-count bump.
    fn admitting(rows: usize) -> Self {
        Self {
            admission: Admission::Admit,
            ..Self::new(rows)
        }
    }

    fn get_or_insert(&mut self, code: TermCode, decode: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.slots.is_empty() {
            return decode();
        }
        // The low bits pick the slot: `mask` is below the slot count, which
        // is a `usize`, so the masked code fits one on every target.
        let slot = &mut self.slots[(code & self.mask as TermCode) as usize];
        if let Some((cached, term)) = slot
            && *cached == code
        {
            if let Admission::Trial { hits, .. } = &mut self.admission {
                *hits = hits.saturating_add(1);
            }
            return Ok(term.clone());
        }
        let term = decode()?;
        if let Admission::Trial { misses, hits } = &mut self.admission {
            *misses += 1;
            if *misses == MEMO_TRIAL_MISSES {
                let keep = hits.saturating_mul(MEMO_MISSES_PER_HIT) >= *misses;
                self.admission = if keep {
                    Admission::Admit
                } else {
                    Admission::Refuse
                };
            }
        }
        if !matches!(self.admission, Admission::Refuse) {
            *slot = Some((code, term.clone()));
        }
        Ok(term)
    }
}

/// A chunk's `[s, p, o, g]` code columns decoded into quads through `src`,
/// a code repeated down a column read and parsed once while its role's memo
/// holds it ([`TermMemo`]). A row whose terms fail to read or parse is an `Err` at
/// that row's position; the other rows decode normally, unless `src` gave
/// up ([`TermSource::gave_up`]): the rows then end before that row.
fn decode_rows(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Quad>> {
    let [s_codes, p_codes, o_codes, g_codes] = cols;
    let n = s_codes.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
    );
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        let row = (|| {
            let subject = sm.get_or_insert(s_codes[i], || {
                subject(src.str_at(0, s_codes[i])?, Trust::Stored)
            })?;
            let predicate = pm.get_or_insert(p_codes[i], || {
                named_node(src.str_at(1, p_codes[i])?, Trust::Stored)
            })?;
            let object = om.get_or_insert(o_codes[i], || {
                term(src.str_at(2, o_codes[i])?, Trust::Stored)
            })?;
            let graph = gm.get_or_insert(g_codes[i], || {
                graph_name(src.str_at(3, g_codes[i])?, Trust::Stored)
            })?;
            Ok(Quad::new(subject, predicate, object, graph))
        })();
        if row.is_err() && src.gave_up() {
            break;
        }
        rows.push(row);
    }
    rows
}

/// The code columns decoded into rows of term strings ([`StringRow`])
/// through `src`: each distinct code's term is read at most once per role
/// and held as a [`Shared`](TermSource::Shared) term for every row repeating
/// it, and nothing is parsed into oxrdf terms. Any failure is the chunk's
/// one `Err`.
fn decode_rows_shared<S: TermSource, R: StringRow>(
    [s, p, o, g]: [&[TermCode]; 4],
    src: &mut S,
) -> Vec<Result<R>> {
    let n = s.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::admitting(n),
        TermMemo::admitting(n),
        TermMemo::admitting(n),
        TermMemo::admitting(n),
    );
    let mut row = |i: usize| -> Result<R> {
        Ok(R::from_terms([
            sm.get_or_insert(s[i], || src.shared_at(0, s[i]))?,
            pm.get_or_insert(p[i], || src.shared_at(1, p[i]))?,
            om.get_or_insert(o[i], || src.shared_at(2, o[i]))?,
            gm.get_or_insert(g[i], || src.shared_at(3, g[i]))?,
        ]))
    };
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        match row(i) {
            Ok(quad) => rows.push(Ok(quad)),
            Err(e) => return vec![Err(e)],
        }
    }
    rows
}

/// A row type a chunk's code columns decode into, through any
/// [`TermSource`]: quads through [`decode_rows`], rows of term strings
/// through [`decode_rows_shared`].
trait DecodeRows: Sized {
    fn decode(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Self>>;
}

impl DecodeRows for Quad {
    fn decode(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Self>> {
        decode_rows(cols, src)
    }
}

impl DecodeRows for SharedQuad {
    fn decode(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Self>> {
        decode_rows_shared(cols, src)
    }
}

impl DecodeRows for NQuadLines {
    /// The rows as one [`NQuadLines`], each term read through `src` unless
    /// the same role's term of a recent row with the same code is in the
    /// lines already ([`SpanMemo`]): then its bytes are copied from there.
    /// Any failure is the chunk's one `Err`.
    fn decode(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Self>> {
        let [s, p, o, g] = cols;
        let mut lines = NQuadLines::with_capacity(s.len());
        let mut spans: [SpanMemo; 4] = std::array::from_fn(|_| SpanMemo::new());
        for (((&s, &p), &o), &g) in s.iter().zip(p).zip(o).zip(g) {
            for (role, (memo, code)) in spans.iter_mut().zip([s, p, o, g]).enumerate() {
                let start = lines.text.len();
                match memo.get(code) {
                    Some(span) => lines.text.extend_from_within(span),
                    None => match src.str_at(role, code) {
                        Ok(term) => lines.text.push_str(term),
                        Err(e) => return vec![Err(e)],
                    },
                }
                memo.put(code, start..lines.text.len());
                // A graph is followed by its space only when there is one:
                // the default graph's `""` writes nothing.
                if role < 3 || lines.text.len() > start {
                    lines.named_graph |= role == 3;
                    lines.text.push(' ');
                }
            }
            lines.text.push_str(".\n");
        }
        vec![Ok(lines)]
    }
}

/// Slots of a [`SpanMemo`]: a power of two, so a code's slot is a mask.
const SPAN_SLOTS: usize = 256;

/// One role's recent terms in an [`NQuadLines`] being written: the span of
/// the lines each was last written to, in a direct-mapped table by code, so
/// a repeated code is copied from the lines rather than read again.
struct SpanMemo(Vec<(TermCode, std::ops::Range<usize>)>);

impl SpanMemo {
    fn new() -> Self {
        Self(vec![(TermCode::MAX, 0..0); SPAN_SLOTS])
    }

    /// The span `code` was last written to, while its slot still holds it.
    fn get(&self, code: TermCode) -> Option<std::ops::Range<usize>> {
        let (held, span) = &self.0[(code as usize) & (SPAN_SLOTS - 1)];
        (*held == code).then(|| span.clone())
    }

    fn put(&mut self, code: TermCode, span: std::ops::Range<usize>) {
        self.0[(code as usize) & (SPAN_SLOTS - 1)] = (code, span);
    }
}

/// `chunk`'s rows decoded against the resident `dict`; a failure to read the
/// code columns is the chunk's one `Err`.
fn decode_resident<T: DecodeRows>(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<T>> {
    let columns = match code_columns(chunk) {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };
    T::decode(code_slices(&columns), &mut DictTerms::new(dict))
}

/// `chunk`'s rows as quads, decoded against the resident `dict`
/// ([`decode_rows`]); a failure to read the code columns is the chunk's one
/// `Err`.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    decode_resident(chunk, dict)
}

/// `chunk`'s rows as N-Triples/N-Quads lines, decoded against the resident
/// `dict`.
pub(crate) fn decode_chunk_lines(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Vec<Result<NQuadLines>> {
    decode_resident(chunk, dict)
}

/// [`decode_chunk`] with shared-string terms (see [`decode_rows_shared`]).
pub(crate) fn decode_chunk_shared(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Vec<Result<SharedQuad>> {
    decode_resident(chunk, dict)
}

/// One role's code column as owned terms, a repeated code read once while
/// the memo holds it ([`TermMemo`]); a code outside the dictionary is an
/// error.
pub(in crate::store::layouts) fn decode_code_column<T: Clone + for<'a> From<&'a str>>(
    dict: &TermDictionary,
    codes: &[TermCode],
) -> Result<Vec<T>> {
    let mut cursor = dict.cursor();
    let mut memo: TermMemo<T> = TermMemo::new(codes.len());
    codes
        .iter()
        .map(|&code| {
            memo.get_or_insert(code, || {
                let rank = check_code(code, dict.code_space())?;
                Ok(T::from(cursor.str_at(rank)))
            })
        })
        .collect()
}

/// The distinct codes of a chunk's four code columns, ascending. A code
/// equal to the one before it in its column is skipped ahead of the sort:
/// `s` is sorted and `p` and `g` come in runs, so those columns add about one
/// code per run.
#[cfg(feature = "file-io")]
fn distinct_codes(columns: [&[TermCode]; 4]) -> Vec<TermCode> {
    let mut codes = Vec::with_capacity(columns[0].len());
    for column in columns {
        let mut previous = None;
        for &code in column {
            if previous != Some(code) {
                codes.push(code);
                previous = Some(code);
            }
        }
    }
    codes.sort_unstable();
    codes.dedup();
    codes
}

/// The terms of `columns`' distinct codes, read from the windows of `fb`
/// that hold them, each window once.
#[cfg(feature = "file-io")]
async fn resolve_chunk_terms(fb: &FileBackedDict, columns: [&[TermCode]; 4]) -> Result<ChunkTerms> {
    #[cfg(test)]
    tests::RESOLVED_CHUNKS.with(|resolved| resolved.set(resolved.get() + 1));
    let codes = distinct_codes(columns);
    let terms = fb.decode_many(&codes).await?;
    Ok(codes.into_iter().zip(terms).collect())
}

/// A chunk decoded against a file-backed dictionary, its code columns read
/// once. The rows decode in place first ([`FileTerms`]), as against a
/// resident dictionary; when that gives up, or once the handle's window
/// reads have had to wait, they decode against their distinct codes' terms
/// ([`resolve_chunk_terms`]). A failure to read the columns or to resolve
/// their codes is the chunk's one `Err`; in place, a code outside the
/// dictionary fails as it does against a resident dictionary.
#[cfg(feature = "file-io")]
async fn decode_file_backed<T: DecodeRows>(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<T>> {
    let columns = match code_columns(chunk) {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };
    let cols = code_slices(&columns);
    if let Some(held) = fb.hold_windows() {
        let mut terms = FileTerms {
            held,
            scratch: Default::default(),
        };
        let rows = T::decode(cols, &mut terms);
        if !terms.gave_up() {
            return rows;
        }
    }
    match resolve_chunk_terms(fb, cols).await {
        Ok(terms) => T::decode(cols, &mut MappedTerms(&terms)),
        Err(e) => vec![Err(e)],
    }
}

/// [`decode_chunk`] against a file-backed dictionary
/// ([`decode_file_backed`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<Quad>> {
    decode_file_backed(fb, chunk).await
}

/// [`decode_chunk_lines`] against a file-backed dictionary
/// ([`decode_file_backed`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_lines_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<NQuadLines>> {
    decode_file_backed(fb, chunk).await
}

/// [`decode_chunk_file_backed`] with shared-string terms (see
/// [`decode_rows_shared`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_shared_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<SharedQuad>> {
    decode_file_backed(fb, chunk).await
}

#[cfg(test)]
mod tests;
