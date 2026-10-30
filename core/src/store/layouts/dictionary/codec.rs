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
/// holds: a [`SharedQuad`] shares them, a [`RawQuad`] copies them out.
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

impl StringRow for RawQuad {
    fn from_terms<T: SharedTerm>(terms: [T; 4]) -> Self {
        let [s, p, o, g] = terms.each_ref().map(|term| String::from(term.as_ref()));
        RawQuad { s, p, o, g }
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

impl DecodeRows for RawQuad {
    fn decode(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Self>> {
        decode_rows_shared(cols, src)
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

/// [`decode_chunk_file_backed`] with shared-string terms (see
/// [`decode_rows_shared`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_shared_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<SharedQuad>> {
    decode_file_backed(fb, chunk).await
}

/// [`decode_chunk_file_backed`] into [`RawQuad`]s, every row's terms copied
/// out as owned N-Triples strings.
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_raw_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<RawQuad>> {
    decode_file_backed(fb, chunk).await
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use vortex_array::arrays::VarBinViewArray;

    use super::*;
    use crate::common::quad::{RawQuad, SharedQuad};
    use crate::store::layouts::ResolvedLayout;
    use crate::store::layouts::dictionary::handles::DictAccess;

    thread_local! {
        /// Executions of [`code_columns`] on this thread — what the tests
        /// read to pin that a chunk's columns are read once per decode.
        pub(super) static CODE_COLUMN_READS: Cell<usize> = const { Cell::new(0) };
        /// Chunks whose distinct codes were resolved
        /// ([`resolve_chunk_terms`]) on this thread: a file-backed decode
        /// that did not decode in place.
        pub(super) static RESOLVED_CHUNKS: Cell<usize> = const { Cell::new(0) };
    }

    /// 120 quads whose columns have the shapes a store's chunks have: `s`
    /// ascending in runs of ten, `p` in runs of four cycling over three
    /// predicates, `o` unsorted with repeats (blank nodes, IRIs and
    /// literals), `g` in three runs (default graph and two named graphs).
    fn skewed_quads() -> Vec<RawQuad> {
        (0..120usize)
            .map(|i| RawQuad {
                s: format!("<http://example.org/s{:02}>", i / 10),
                p: format!("<http://example.org/p{}>", (i / 4) % 3),
                o: match i % 5 {
                    0 => format!("_:b{}", i % 7),
                    1 | 3 => format!("\"lit {}\"", (i * 53) % 29),
                    _ => format!("<http://example.org/o{}>", (i * 37) % 23),
                },
                g: match i / 40 {
                    0 => String::new(),
                    1 => "<http://example.org/g1>".to_owned(),
                    _ => "<http://example.org/g2>".to_owned(),
                },
            })
            .collect()
    }

    /// The quads' dictionary (compressed in windows of eight terms, so a
    /// chunk's codes span many windows) and their code chunk.
    fn skewed_chunk(quads: &[RawQuad]) -> (TermDictionary, ArrayRef) {
        let mut terms: Vec<&str> = quads
            .iter()
            .flat_map(|q| [q.s.as_str(), q.p.as_str(), q.o.as_str(), q.g.as_str()])
            .collect();
        terms.sort_unstable();
        terms.dedup();
        let code_map: HashMap<&str, TermCode> = terms
            .iter()
            .enumerate()
            .map(|(rank, &term)| (term, rank as TermCode))
            .collect();
        let dict = TermDictionary::compress_windowed(
            VarBinViewArray::from_iter_str(terms.iter().copied()),
            8,
        )
        .unwrap();
        let codes = encode_quads(quads, &code_map).unwrap();
        let chunk = build_code_chunk(&codes, 0..quads.len(), false).unwrap();
        (dict, chunk)
    }

    /// `dict` left in a store file's dictionary child, opened file-backed.
    async fn file_backed(dict: &TermDictionary) -> FileBackedDict {
        let native =
            crate::tests::open_native_bytes(crate::tests::write_dict_only_store(dict).await);
        FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("the written dictionary child must be window-searchable")
    }

    /// Reads of the code columns, and chunks resolved, while `decode` runs.
    async fn decode_counts<T>(decode: impl std::future::Future<Output = T>) -> (T, usize, usize) {
        let (reads, resolved) = (
            CODE_COLUMN_READS.with(Cell::get),
            RESOLVED_CHUNKS.with(Cell::get),
        );
        let out = decode.await;
        (
            out,
            CODE_COLUMN_READS.with(Cell::get) - reads,
            RESOLVED_CHUNKS.with(Cell::get) - resolved,
        )
    }

    /// `chunk` decoded against `fb` into quads, shared quads and raw quads,
    /// each as the resident `dict` decodes it, each reading the code columns
    /// once and lifting no dictionary; `resolved` is whether each decode
    /// resolves the chunk's distinct codes rather than decoding in place.
    async fn assert_file_backed_decodes(
        fb: &FileBackedDict,
        dict: &Arc<TermDictionary>,
        chunk: &ArrayRef,
        resolved: bool,
        case: &str,
    ) {
        use crate::store::layouts::ChunkDecode;

        let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb.clone()));
        let counts = (1, usize::from(resolved));

        let (decoded, reads, resolves) = decode_counts(Quad::decode_async(&layout, chunk)).await;
        assert_eq!((reads, resolves), counts, "{case}: quads");
        let decoded: Vec<Quad> = decoded.into_iter().map(|row| row.unwrap()).collect();
        let want: Vec<Quad> = decode_chunk(chunk, dict)
            .into_iter()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(decoded.len(), chunk.len(), "{case}: quads");
        assert_eq!(decoded, want, "{case}: quads");

        let (shared, reads, resolves) =
            decode_counts(SharedQuad::decode_async(&layout, chunk)).await;
        assert_eq!((reads, resolves), counts, "{case}: shared quads");
        let shared: Vec<SharedQuad> = shared.into_iter().map(|row| row.unwrap()).collect();
        let want: Vec<SharedQuad> = decode_chunk_shared(chunk, dict)
            .into_iter()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(shared, want, "{case}: shared quads");

        let (raw, reads, resolves) = decode_counts(RawQuad::decode_async(&layout, chunk)).await;
        assert_eq!((reads, resolves), counts, "{case}: raw quads");
        let terms = |q: RawQuad| [q.s, q.p, q.o, q.g];
        let raw: Vec<[String; 4]> = raw.into_iter().map(|row| terms(row.unwrap())).collect();
        let want: Vec<[String; 4]> =
            ResolvedLayout::Dictionary(DictAccess::Resident(Arc::clone(dict)))
                .raw_quads(chunk)
                .unwrap()
                .into_iter()
                .map(terms)
                .collect();
        assert_eq!(raw, want, "{case}: raw quads");

        assert_eq!(fb.debug_lifts(), 0, "{case}");
    }

    /// A file-backed chunk decode over in-memory reads (a buffer here, as a
    /// mapped file) decodes in place, resolving no codes, and decodes the
    /// rows the resident dictionary decodes — for a chunk long enough to
    /// memoize its roles and for one too short to.
    #[tokio::test]
    async fn a_file_backed_chunk_decode_reads_its_windows_in_place() {
        let all = skewed_quads();
        for quads in [&all[..], &all[..7]] {
            let (dict, chunk) = skewed_chunk(quads);
            let dict = Arc::new(dict);
            let fb = file_backed(&dict).await;
            let case = format!("{} quads", quads.len());
            assert_file_backed_decodes(&fb, &dict, &chunk, false, &case).await;
            assert!(!fb.debug_reads_wait(), "{case}");
        }
    }

    /// Over reads that wait, the first decode gives up in place at its
    /// first window and resolves the chunk's codes, without reading its code
    /// columns again; the handle then knows its reads wait, and later decodes
    /// resolve without trying.
    #[tokio::test]
    async fn a_chunk_decode_over_reads_that_wait_resolves_its_codes() {
        let quads = skewed_quads();
        let (dict, chunk) = skewed_chunk(&quads);
        let dict = Arc::new(dict);
        let native =
            crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&dict).await);
        let fb = FileBackedDict::open_with_waiting_reads(&native)
            .await
            .unwrap()
            .expect("the written dictionary child must be window-searchable");
        assert!(!fb.debug_reads_wait());
        assert_file_backed_decodes(&fb, &dict, &chunk, true, "waiting reads").await;
        assert!(fb.debug_reads_wait());
        // Every window the chunk touches is cached now, yet a decode still
        // resolves: once reads wait, none is tried in place.
        assert!(!fb.debug_cached_windows().is_empty());
        assert_file_backed_decodes(&fb, &dict, &chunk, true, "cached windows").await;
    }

    /// An in-place decode holds its windows within the window cache's
    /// budget: a budget of exactly the charge of the windows the chunk
    /// touches decodes in place, one byte less gives up and resolves the
    /// chunk's codes, decoding the same rows.
    #[tokio::test]
    async fn an_in_place_decode_holds_its_windows_within_the_cache_budget() {
        let quads = skewed_quads();
        let (dict, chunk) = skewed_chunk(&quads);
        let dict = Arc::new(dict);
        let native =
            crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&dict).await);
        // What the touched windows are charged: what a roomy cache holds once
        // an in-place decode has taken each of them.
        let roomy = FileBackedDict::open(&native).await.unwrap().unwrap();
        assert_file_backed_decodes(&roomy, &dict, &chunk, false, "roomy").await;
        let touched = roomy.debug_cached_bytes();
        assert!(roomy.debug_cached_windows().len() > 2);
        for (budget, resolved) in [(touched, false), (touched - 1, true)] {
            let fb = FileBackedDict::open_with_budget(&native, budget)
                .await
                .unwrap()
                .unwrap();
            let case = format!("budget {budget} of {touched}");
            assert_file_backed_decodes(&fb, &dict, &chunk, resolved, &case).await;
            assert!(!fb.debug_reads_wait(), "{case}");
        }
    }

    /// `codes` looked up in `memo`, each decoding to itself: how many were
    /// decoded (missed).
    fn memo_decodes(
        memo: &mut TermMemo<TermCode>,
        codes: impl IntoIterator<Item = TermCode>,
    ) -> usize {
        let mut decoded = 0;
        for code in codes {
            let term = memo
                .get_or_insert(code, || {
                    decoded += 1;
                    Ok(code)
                })
                .unwrap();
            assert_eq!(term, code);
        }
        decoded
    }

    /// A memo's trial keeps it admitting terms while its hits keep up with
    /// its misses, at least one per [`MEMO_MISSES_PER_HIT`] at the
    /// [`MEMO_TRIAL_MISSES`]th miss; with fewer it admits no more, and a
    /// code met after that is decoded each time, while the terms it kept
    /// still answer. A memo for shared terms admits every term.
    #[test]
    fn a_memo_admits_terms_while_its_hits_keep_up() {
        let trial = u64::from(MEMO_TRIAL_MISSES);
        // The trial's misses with `hits` hits among them, spread out.
        let run = |memo: &mut TermMemo<TermCode>, hits: u64| {
            let codes = (0..trial).flat_map(|code| {
                let again = code < hits;
                std::iter::once(code).chain(again.then_some(code))
            });
            assert_eq!(memo_decodes(memo, codes), MEMO_TRIAL_MISSES as usize);
        };
        let enough = trial / u64::from(MEMO_MISSES_PER_HIT);
        for (hits, admits) in [(enough, true), (enough - 1, false), (0, false)] {
            let mut memo = TermMemo::new(4096);
            run(&mut memo, hits);
            // A code past the trial, met twice: kept, or decoded twice.
            let past = trial + 100;
            let decoded = memo_decodes(&mut memo, [past, past]);
            assert_eq!(decoded, if admits { 1 } else { 2 }, "{hits} hits");
            // A code the trial kept still answers (the miss that ends a
            // refusing trial is not kept).
            assert_eq!(memo_decodes(&mut memo, [trial / 2]), 0, "{hits} hits");
        }
        let mut shared = TermMemo::admitting(4096);
        run(&mut shared, 0);
        assert_eq!(memo_decodes(&mut shared, [trial + 100, trial + 100]), 1);
    }

    /// Term reads counted by code, over a resident dictionary.
    struct CountingTerms<'a> {
        inner: DictTerms<'a>,
        reads: HashMap<TermCode, usize>,
    }

    impl TermSource for CountingTerms<'_> {
        type Shared = Arc<str>;

        fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
            *self.reads.entry(code).or_default() += 1;
            self.inner.str_at(role, code)
        }

        fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
            self.str_at(role, code).map(Arc::from)
        }
    }

    /// Past a trial of distinct objects, a quad decode stops keeping the
    /// object column's terms, so a repeated object is read again; a shared
    /// decode keeps every term, so it is read once.
    #[test]
    fn a_shared_decode_keeps_every_term() {
        let n = MEMO_TRIAL_MISSES as usize + 8;
        // One subject, predicate and graph; distinct objects, then the last
        // one again.
        let quads: Vec<RawQuad> = (0..n)
            .map(|i| RawQuad {
                s: "<http://example.org/s>".to_owned(),
                p: "<http://example.org/p>".to_owned(),
                o: format!("\"o{:03}\"", i.min(n - 2)),
                g: String::new(),
            })
            .collect();
        let (dict, chunk) = skewed_chunk(&quads);
        let arrays = code_columns(&chunk).unwrap();
        let columns = code_slices(&arrays);
        let repeated = columns[2][n - 1];
        assert_eq!(columns[2][n - 2], repeated);
        let mut quads_src = CountingTerms {
            inner: DictTerms::new(&dict),
            reads: HashMap::new(),
        };
        let rows = decode_rows(columns, &mut quads_src);
        assert!(rows.iter().all(Result::is_ok));
        assert_eq!(
            quads_src.reads[&repeated], 2,
            "a quad decode reads it again"
        );
        let mut shared_src = CountingTerms {
            inner: DictTerms::new(&dict),
            reads: HashMap::new(),
        };
        let rows: Vec<Result<SharedQuad>> = decode_rows_shared(columns, &mut shared_src);
        assert!(rows.iter().all(Result::is_ok));
        assert_eq!(shared_src.reads[&repeated], 1, "a shared decode keeps it");
    }

    /// A source that fails on one code, giving up on the chunk or not.
    struct FailingTerms<'a> {
        inner: DictTerms<'a>,
        code: TermCode,
        gives_up: bool,
        gave_up: bool,
    }

    impl TermSource for FailingTerms<'_> {
        type Shared = Arc<str>;

        fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
            if code == self.code {
                self.gave_up = self.gives_up;
                return Err(VortexRdfError::InvalidOperation("unreadable".into()));
            }
            self.inner.str_at(role, code)
        }

        fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
            self.str_at(role, code).map(Arc::from)
        }

        fn gave_up(&self) -> bool {
            self.gave_up
        }
    }

    /// A quad decode whose source gives up ends at the row that failed, the
    /// rows before it decoded; a failed read of a source that has not given
    /// up is that row's `Err`, and the rows after it decode.
    #[test]
    fn a_quad_decode_ends_where_its_source_gives_up() {
        let quads = skewed_quads();
        let (dict, chunk) = skewed_chunk(&quads);
        let arrays = code_columns(&chunk).unwrap();
        let columns = code_slices(&arrays);
        // Rows 30..40 share their subject, which no row before them has.
        let code = columns[0][30];
        assert!(columns[0][..30].iter().all(|&c| c != code));
        let want = decode_chunk(&chunk, &dict);

        for gives_up in [true, false] {
            let mut src = FailingTerms {
                inner: DictTerms::new(&dict),
                code,
                gives_up,
                gave_up: false,
            };
            let rows = decode_rows(columns, &mut src);
            if gives_up {
                assert_eq!(rows.len(), 30);
            } else {
                assert_eq!(rows.len(), quads.len());
                assert!(rows[30..40].iter().all(Result::is_err));
                assert!(rows[40..].iter().all(Result::is_ok));
            }
            for (i, row) in rows
                .iter()
                .enumerate()
                .filter(|(i, _)| !(30..40).contains(i))
            {
                assert_eq!(row.as_ref().unwrap(), want[i].as_ref().unwrap(), "row {i}");
            }
        }
    }

    /// The distinct codes are the sorted set of all four columns, however
    /// the columns repeat: `s` ascending, `p` and `g` in runs (a code may
    /// return after another), `o` unsorted with repeats.
    #[test]
    fn distinct_codes_are_the_sorted_set_of_all_four_columns() {
        let sorted_set = |columns: &[&[TermCode]; 4]| -> Vec<TermCode> {
            let mut all: Vec<TermCode> = columns.iter().flat_map(|c| c.iter().copied()).collect();
            all.sort_unstable();
            all.dedup();
            all
        };
        let cases: [[&[TermCode]; 4]; 6] = [
            [
                &[1, 1, 2, 2, 2, 5, 5, 9],
                &[7, 7, 7, 3, 3, 7, 7, 7],
                &[40, 12, 40, 12, 33, 40, 5, 12],
                &[0, 0, 0, 0, 8, 8, 8, 8],
            ],
            [&[], &[], &[], &[]],
            [&[4], &[], &[4, 4], &[]],
            [
                &[0, 0],
                &[TermCode::MAX],
                &[TermCode::MAX, 0, TermCode::MAX],
                &[1 << 40, 1 << 40],
            ],
            [&[3, 3, 3], &[3, 3, 3], &[3, 3, 3], &[3, 3, 3]],
            [&[9, 8, 9, 8], &[1, 2, 1, 2], &[5, 5, 6, 6], &[0, 1, 0, 1]],
        ];
        for columns in cases {
            assert_eq!(distinct_codes(columns), sorted_set(&columns), "{columns:?}");
        }

        let (_, chunk) = skewed_chunk(&skewed_quads());
        let arrays = code_columns(&chunk).unwrap();
        let columns = code_slices(&arrays);
        let distinct = distinct_codes(columns);
        assert_eq!(distinct, sorted_set(&columns));
        // The skip rule leaves the sort far fewer than the 4n codes.
        assert!(distinct.len() < columns[0].len());
    }
}
