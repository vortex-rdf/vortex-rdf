//! Column-building and decoding logic for [`LayoutStrategy::Dictionary`]:
//! s/p/o/g stored as [`TermCode`]s (`u64`) into a global sorted term dictionary (see
//! [`term_dict`](super::term_dict)), which travels beside the array in memory
//! and reaches serialized files as the native container's `dictionary` child
//! (see `crate::io::container`).
//!
//! Unlike the other layouts, chunks are not built through the generic
//! `build_struct_array` path: encoding requires the global `TermDictionary`
//! (complete only after the whole dataset has been ingested), so the builders
//! run a dedicated two-pass pipeline that calls `build_chunk` directly.
//! Secondary indexes compose normally: like every layout's, their children
//! are built beside the chunks — over the encoded codes rather than the term
//! strings, which sort identically.
//!
//! [`LayoutStrategy::Dictionary`]: crate::store::layouts::LayoutStrategy::Dictionary

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use oxrdf::Quad;
#[cfg(feature = "file-io")]
use rustc_hash::FxHashMap;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::terms::{Trust, graph_name, named_node, subject, term};
use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{field_as, stamp_is_sorted};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS, TermCode};
use crate::store::{RawQuad, SharedQuad};

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::storage::DictCursor;
use super::term_dict::{CodeSpace, TermDictionary, check_code};

/// Dictionary-encoded quad columns: [`RawQuad`] terms replaced by their
/// codes in the global sorted term dictionary. Produced by the Dictionary
/// layout's encoding pass and consumed by index builders, which can work on
/// codes directly (sorted-dictionary codes preserve lexicographic order).
pub(crate) struct QuadCodes {
    pub(crate) s: Vec<TermCode>,
    pub(crate) p: Vec<TermCode>,
    pub(crate) o: Vec<TermCode>,
    pub(crate) g: Vec<TermCode>,
}

impl QuadCodes {
    /// The encoding of no quads — what an empty build's index children are
    /// built over, so they carry the code dtypes a non-empty build's would.
    pub(crate) fn empty() -> Self {
        Self {
            s: Vec::new(),
            p: Vec::new(),
            o: Vec::new(),
            g: Vec::new(),
        }
    }
}

/// The dictionary code of `term` in `code_map`, or the encoding error for a
/// term the dictionary does not hold.
///
/// Generic over the map's key so both the owned `TermCodeMap` (streaming
/// builders) and the borrowed [`BorrowedTermCodeMap`] (builders holding a
/// live quad slice) work without a second code path — `&str: Borrow<str>`
/// makes the `get(term)` lookup identical for either.
///
/// [`BorrowedTermCodeMap`]: super::ingest::BorrowedTermCodeMap
pub(crate) fn code_of<K>(code_map: &HashMap<K, TermCode>, term: &str) -> Result<TermCode>
where
    K: Borrow<str> + Eq + Hash,
{
    code_map.get(term).copied().ok_or_else(|| {
        VortexRdfError::Serialization(format!(
            "Term missing from dictionary during encoding: {}",
            term
        ))
    })
}

/// Encode every term of every quad to its dictionary code (see [`code_of`]).
pub(crate) fn encode_quads<K>(
    quads: &[RawQuad],
    code_map: &HashMap<K, TermCode>,
) -> Result<QuadCodes>
where
    K: Borrow<str> + Eq + Hash,
{
    let start = debug::timer();
    let encode_column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<TermCode>> {
        let mut codes: Vec<TermCode> = Vec::with_capacity(quads.len());
        for q in quads {
            codes.push(code_of(code_map, term_of(q))?);
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

/// Build a Dictionary-layout StructArray chunk from raw quads: four code
/// columns encoded against the global dictionary. Secondary indexes are built
/// separately as components (see
/// [`build_components_from_codes`](crate::store::builders::build_components_from_codes)),
/// so nothing else rides here.
pub(crate) fn build_chunk<K>(
    quads: &[RawQuad],
    code_map: &HashMap<K, TermCode>,
    s_sorted: bool,
) -> Result<ArrayRef>
where
    K: Borrow<str> + Eq + Hash,
{
    let codes = encode_quads(quads, code_map)?;
    build_code_chunk(&codes, 0..quads.len(), s_sorted)
}

/// Build the whole dataset as one contiguous Dictionary-layout chunk from its
/// codes — the in-memory builders' construction path, fed by the interning
/// ingest ([`InterningQuadBuilder`]) so no owned quad strings are involved.
///
/// The codes arrive in global (s, p, o, g) order, so the `s` column is
/// stamped sorted.
///
/// [`InterningQuadBuilder`]: super::ingest::InterningQuadBuilder
pub(crate) fn build_array(codes: &QuadCodes) -> Result<ArrayRef> {
    if codes.s.is_empty() {
        return empty_struct();
    }
    let n = codes.s.len();
    build_code_chunk(codes, 0..n, true)
}

/// Build a Dictionary-layout chunk for rows `range` of an already encoded
/// dataset: the four code columns, and nothing else. The term dictionary
/// is *not* a column of the chunk: in memory it lives in the layout
/// ([`DictAccess`]), and serialized files carry it as the native container's
/// `dictionary` child.
///
/// `s_sorted` stamps the `IsSorted` statistic on the `s` column; valid
/// because sorted-dictionary codes preserve lexicographic order.
///
/// [`DictAccess`]: super::handles::DictAccess
pub(crate) fn build_code_chunk(
    codes: &QuadCodes,
    range: Range<usize>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let start = debug::timer();
    let n = range.len();
    let names: Vec<Arc<str>> = PRIMARY_COLUMNS.iter().map(|&name| name.into()).collect();
    let arrays: Vec<ArrayRef> = vec![
        PrimitiveArray::from_iter(codes.s[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.p[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.o[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.g[range].iter().copied()).into_array(),
    ];
    if s_sorted {
        stamp_is_sorted(&arrays[0]);
    }
    let chunk = StructArray::try_new(names.into(), arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())?;
    log::debug!(
        "[Dictionary] Built encoded chunk of {} rows in {:?}",
        n,
        debug::elapsed(start)
    );
    Ok(chunk)
}

/// An empty StructArray with the Dictionary-layout code schema (an unstamped
/// `s` column: nothing to sort).
pub(crate) fn empty_struct() -> Result<ArrayRef> {
    build_code_chunk(&QuadCodes::empty(), 0..0, false)
}

/// The four primary code columns of a chunk in `[s, p, o, g]` order, as the
/// arrays whose [`TermCode`] slices the decoders read (see [`code_slices`]).
/// Returned by value: the slices borrow these arrays, so they must outlive
/// the decode.
fn code_columns(chunk: &ArrayRef) -> Result<[PrimitiveArray; 4]> {
    #[cfg(all(test, feature = "file-io"))]
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

/// Where a decode reads a code's term from: the four roles are asked
/// separately so a dictionary-backed source can keep one cursor (and, for a
/// chunked dictionary, one warm chunk cursor) per role — the roles occupy
/// different regions of the sorted term space.
trait TermSource {
    /// A term as [`decode_rows_shared`] holds it from row to row.
    type Shared: SharedTerm;

    /// The term with `code`, read for `role`.
    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str>;

    /// [`str_at`](Self::str_at) as a [`Shared`](Self::Shared) term.
    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Self::Shared>;
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

/// Upper bound on a role memo's slots: the memo is sized from the chunk's row
/// count and clamped here, so it never grows with the data.
const MEMO_MAX_SLOTS: usize = 1024;

/// Below this many rows a chunk decodes without a memo at all: the table's own
/// allocation costs more than the handful of repeats it could catch.
const MEMO_MIN_ROWS: usize = 16;

/// A direct-mapped memo of one role's decoded terms, keyed by term code.
///
/// Codes repeat heavily down a column — a predicate or graph name recurs on
/// nearly every row — and each repeat would otherwise pay the dictionary read
/// (an FSST decompress) *and* the term parse again. Direct-mapped: a miss
/// costs one compare and one overwrite, and the memory is fixed whatever the
/// column's cardinality, so a high-cardinality column like subjects cannot
/// accumulate entries it never reads again.
struct TermMemo<T> {
    slots: Vec<Option<(TermCode, T)>>,
    mask: usize,
}

impl<T: Clone> TermMemo<T> {
    /// Sized to the chunk (a power of two, capped): a short chunk cannot have
    /// more distinct codes than rows, so it should not clear a big table, and
    /// a tiny one gets no table at all (`vec![_; 0]` does not allocate).
    fn new(rows: usize) -> Self {
        let slots = if rows < MEMO_MIN_ROWS {
            0
        } else {
            rows.next_power_of_two().clamp(1, MEMO_MAX_SLOTS)
        };
        Self {
            slots: vec![None; slots],
            mask: slots.saturating_sub(1),
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
            return Ok(term.clone());
        }
        let term = decode()?;
        *slot = Some((code, term.clone()));
        Ok(term)
    }
}

/// `cols` — a chunk's `[s, p, o, g]` code columns — decoded into quads
/// through `src`, each distinct code's term read and parsed at most once per
/// role (see [`TermMemo`]). A row whose terms fail to read or parse is an
/// `Err` at that row's position, the other rows decoding normally.
fn decode_rows(cols: [&[TermCode]; 4], src: &mut impl TermSource) -> Vec<Result<Quad>> {
    let [s_codes, p_codes, o_codes, g_codes] = cols;
    let n = s_codes.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
    );

    (0..n)
        .map(|i| {
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
        })
        .collect()
}

/// `cols` decoded into rows of term strings through `src` — [`SharedQuad`]s
/// or [`RawQuad`]s (see [`StringRow`]). Each distinct code's term is read at
/// most once per role (see [`TermMemo`]) and held as a
/// [`Shared`](TermSource::Shared) term, so a code repeating down a column
/// reaches every row repeating it without another read and nothing is parsed
/// into oxrdf terms. Any failure is the chunk's one `Err`.
fn decode_rows_shared<S: TermSource, R: StringRow>(
    [s, p, o, g]: [&[TermCode]; 4],
    src: &mut S,
) -> Vec<Result<R>> {
    let n = s.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
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

/// Decode a Dictionary-layout StructArray chunk into Quads using the given
/// (store-cached) dictionary (see [`decode_rows`]).
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    let columns = match code_columns(chunk) {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };
    decode_rows(code_slices(&columns), &mut DictTerms::new(dict))
}

/// [`decode_chunk`] with shared-string terms (see [`decode_rows_shared`]).
pub(crate) fn decode_chunk_shared(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Vec<Result<SharedQuad>> {
    let columns = match code_columns(chunk) {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };
    decode_rows_shared(code_slices(&columns), &mut DictTerms::new(dict))
}

/// Decode one role's code column to owned term strings, reading each distinct
/// code's term at most once (see [`TermMemo`]) — the [`raw_quads`]
/// reconstruction path, where a predicate or graph column repeats a handful
/// of codes over every row and each repeat would otherwise pay the dictionary
/// read (an FSST decompress) again.
///
/// [`raw_quads`]: crate::store::layouts::ResolvedLayout::raw_quads
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

/// The distinct codes of a chunk's four code columns, ascending — what a
/// file-backed dictionary must resolve to decode the chunk. A code equal to
/// the one before it in its column is skipped ahead of the sort: `s` is
/// sorted and `p` and `g` come in runs, so those columns contribute about
/// one code per run.
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

/// The terms of `columns`' distinct codes, resolved with one read of the
/// windows of a file-backed dictionary that hold them.
#[cfg(feature = "file-io")]
async fn resolve_chunk_terms(fb: &FileBackedDict, columns: [&[TermCode]; 4]) -> Result<ChunkTerms> {
    let codes = distinct_codes(columns);
    let terms = fb.decode_many(&codes).await?;
    Ok(codes.into_iter().zip(terms).collect())
}

/// A chunk decoded against a file-backed dictionary: the chunk's code columns
/// are read once, their distinct codes are resolved to terms (see
/// [`resolve_chunk_terms`]), and `rows` decodes the same columns against the
/// resulting map. A failure to read the columns or to resolve their codes is
/// the chunk's one `Err`.
#[cfg(feature = "file-io")]
async fn decode_resolved<T>(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
    rows: impl FnOnce([&[TermCode]; 4], &mut MappedTerms<'_>) -> Vec<Result<T>>,
) -> Vec<Result<T>> {
    let columns = match code_columns(chunk) {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };
    let cols = code_slices(&columns);
    match resolve_chunk_terms(fb, cols).await {
        Ok(terms) => rows(cols, &mut MappedTerms(&terms)),
        Err(e) => vec![Err(e)],
    }
}

/// [`decode_chunk`] against a file-backed dictionary instead of a resident
/// one: the rows decode against the chunk's resolved terms (see
/// [`decode_resolved`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<Quad>> {
    decode_resolved(fb, chunk, |cols, terms| decode_rows(cols, terms)).await
}

/// [`decode_chunk_file_backed`] with shared-string terms: the resolved map
/// holds one `Arc<str>` per distinct code, so every row's term is a lookup
/// and a reference-count bump (see [`decode_rows_shared`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_shared_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<SharedQuad>> {
    decode_resolved(fb, chunk, |cols, terms| decode_rows_shared(cols, terms)).await
}

/// [`decode_chunk_file_backed`] into [`RawQuad`]s: the chunk reads only the
/// dictionary windows holding its distinct codes (see [`decode_resolved`]),
/// and every row's terms are copied out of the resolved map as owned
/// N-Triples strings (see [`decode_rows_shared`]).
#[cfg(feature = "file-io")]
pub(crate) async fn decode_chunk_raw_file_backed(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Vec<Result<RawQuad>> {
    decode_resolved(fb, chunk, |cols, terms| decode_rows_shared(cols, terms)).await
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use std::cell::Cell;

    use vortex_array::arrays::VarBinViewArray;

    use super::*;
    use crate::common::quad::{RawQuad, SharedQuad};
    use crate::store::layouts::ResolvedLayout;
    use crate::store::layouts::dictionary::handles::DictAccess;

    thread_local! {
        /// Executions of [`code_columns`] on this thread — what the tests
        /// read to pin that a chunk's columns are read once per decode.
        pub(super) static CODE_COLUMN_READS: Cell<usize> = const { Cell::new(0) };
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

    /// Reads of the code columns while `decode` runs.
    async fn code_column_reads<T>(decode: impl std::future::Future<Output = T>) -> (T, usize) {
        let before = CODE_COLUMN_READS.with(Cell::get);
        let out = decode.await;
        (out, CODE_COLUMN_READS.with(Cell::get) - before)
    }

    /// A file-backed chunk decode executes the chunk's code columns once,
    /// whichever row representation it produces, lifts no dictionary, and
    /// decodes the rows the resident dictionary decodes — for a chunk long
    /// enough to memoize its roles and for one too short to.
    #[tokio::test]
    async fn a_file_backed_chunk_decode_reads_the_code_columns_once() {
        use crate::store::layouts::ChunkDecode;

        let all = skewed_quads();
        for quads in [&all[..], &all[..7]] {
            let n = quads.len();
            let (dict, chunk) = skewed_chunk(quads);
            let fb = file_backed(&dict).await;
            let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb.clone()));

            let (decoded, reads) = code_column_reads(Quad::decode_async(&layout, &chunk)).await;
            assert_eq!(reads, 1, "{n} quads");
            assert_eq!(fb.debug_lifts(), 0, "{n} quads");
            let decoded: Vec<Quad> = decoded.into_iter().map(|row| row.unwrap()).collect();
            let want: Vec<Quad> = decode_chunk(&chunk, &dict)
                .into_iter()
                .map(|row| row.unwrap())
                .collect();
            assert_eq!(decoded.len(), n);
            assert_eq!(decoded, want, "{n} quads");

            let (shared, reads) =
                code_column_reads(SharedQuad::decode_async(&layout, &chunk)).await;
            assert_eq!(reads, 1, "{n} shared quads");
            assert_eq!(fb.debug_lifts(), 0, "{n} shared quads");
            let shared: Vec<SharedQuad> = shared.into_iter().map(|row| row.unwrap()).collect();
            let want: Vec<SharedQuad> = decode_chunk_shared(&chunk, &dict)
                .into_iter()
                .map(|row| row.unwrap())
                .collect();
            assert_eq!(shared, want, "{n} shared quads");

            let (raw, reads) = code_column_reads(RawQuad::decode_async(&layout, &chunk)).await;
            assert_eq!(reads, 1, "{n} raw quads");
            assert_eq!(fb.debug_lifts(), 0, "{n} raw quads");
            let terms = |q: RawQuad| [q.s, q.p, q.o, q.g];
            let raw: Vec<[String; 4]> = raw.into_iter().map(|row| terms(row.unwrap())).collect();
            let want: Vec<[String; 4]> =
                ResolvedLayout::Dictionary(DictAccess::Resident(Arc::new(dict)))
                    .raw_quads(&chunk)
                    .unwrap()
                    .into_iter()
                    .map(terms)
                    .collect();
            assert_eq!(raw, want, "{n} raw quads");
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
