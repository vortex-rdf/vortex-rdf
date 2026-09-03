//! Column-building and decoding logic for [`LayoutStrategy::Dictionary`]:
//! s/p/o/g stored as u32 codes into a global sorted term dictionary (see
//! [`term_dict`](self::term_dict)), which travels beside the array in memory
//! and reaches serialized files as the native container's `dictionary` child
//! (see `crate::io::container`).
//!
//! This folder is the whole dictionary subsystem: this file owns the chunk
//! encode/decode paths, [`ingest`] the build-side term collection and
//! interning, [`term_dict`] the frozen dictionary itself, [`file_backed`]
//! the on-demand file residency, and [`access`] the residency seam the
//! resolved layout speaks through.
//!
//! Unlike the other layouts, chunks are not built through the generic
//! `build_struct_array` path: encoding requires the global `TermDictionary`
//! (complete only after the whole dataset has been ingested), so the builders
//! run a dedicated two-pass pipeline that calls `build_chunk` directly.
//! Secondary indexes compose normally: like every layout's, their children
//! are built beside the chunks — over the encoded codes rather than the term
//! strings, which sort identically.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::terms::{parse_graph_name, parse_named_node, parse_object, parse_subject};
use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{field_as, stamp_is_sorted};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

pub(crate) mod access;
#[cfg(feature = "file-io")]
pub(crate) mod file_backed;
pub(crate) mod ingest;
pub(crate) mod predicates;
pub(crate) mod term_dict;

#[cfg(feature = "file-io")]
pub(crate) use self::file_backed::FileBackedDict;
pub use self::ingest::DictionaryQuadSink;
// Read only by the out-of-core builder, which is compiled out on
// wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::ingest::{TermCodeMap, TermDictionaryBuilder};
pub use self::predicates::{NumOp, TermPredicate, Verdict};
use self::term_dict::DictCursor;
pub(crate) use self::term_dict::TermDictionary;
pub use self::term_dict::{DictForm, DictSnapshot};

/// The primary columns: `s`, `p`, `o`, `g` (all u32 codes).
pub(crate) const COLUMNS: &[&str] = &PRIMARY_COLUMNS;

/// Dictionary-encoded quad columns: [`RawQuad`] terms replaced by their u32
/// codes in the global sorted term dictionary. Produced by the Dictionary
/// layout's encoding pass and consumed by index builders, which can work on
/// codes directly (sorted-dictionary codes preserve lexicographic order).
pub(crate) struct QuadCodes {
    pub(crate) s: Vec<u32>,
    pub(crate) p: Vec<u32>,
    pub(crate) o: Vec<u32>,
    pub(crate) g: Vec<u32>,
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
/// [`BorrowedTermCodeMap`]: self::ingest::BorrowedTermCodeMap
pub(crate) fn code_of<K>(code_map: &HashMap<K, u32>, term: &str) -> Result<u32>
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
pub(crate) fn encode_quads<K>(quads: &[RawQuad], code_map: &HashMap<K, u32>) -> Result<QuadCodes>
where
    K: Borrow<str> + Eq + Hash,
{
    let start = debug::timer();
    let encode_column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<u32>> {
        let mut codes: Vec<u32> = Vec::with_capacity(quads.len());
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

/// Build a Dictionary-layout StructArray chunk from raw quads: four u32 code
/// columns encoded against the global dictionary. Secondary indexes are built
/// separately as components (see
/// [`build_components_from_codes`](crate::store::builders::build_components_from_codes)),
/// so nothing else rides here.
pub(crate) fn build_chunk<K>(
    quads: &[RawQuad],
    code_map: &HashMap<K, u32>,
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
/// [`InterningQuadBuilder`]: self::ingest::InterningQuadBuilder
pub(crate) fn build_array(codes: &QuadCodes) -> Result<ArrayRef> {
    if codes.s.is_empty() {
        return empty_struct();
    }
    let n = codes.s.len();
    build_code_chunk(codes, 0..n, true)
}

/// Build a Dictionary-layout chunk for rows `range` of an already encoded
/// dataset: the four u32 code columns, and nothing else. The term dictionary
/// is *not* a column of the chunk: in memory it lives in the layout
/// ([`DictAccess`]), and serialized files carry it as the native container's
/// `dictionary` child.
///
/// `s_sorted` stamps the `IsSorted` statistic on the `s` column; valid
/// because sorted-dictionary codes preserve lexicographic order.
///
/// [`DictAccess`]: self::access::DictAccess
pub(crate) fn build_code_chunk(
    codes: &QuadCodes,
    range: Range<usize>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let start = debug::timer();
    let n = range.len();
    let names: Vec<Arc<str>> = COLUMNS.iter().map(|&name| name.into()).collect();
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

/// The four primary code columns of a chunk, as arrays whose `u32` slices the
/// decoders read. Returned by value: the slices borrow these arrays, so they
/// must outlive the decode.
fn code_columns(
    chunk: &ArrayRef,
) -> Result<(
    PrimitiveArray,
    PrimitiveArray,
    PrimitiveArray,
    PrimitiveArray,
)> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut col = |name: &str| field_as::<PrimitiveArray>(&struct_arr, name, &mut ctx);
    Ok((col(COL_S)?, col(COL_P)?, col(COL_O)?, col(COL_G)?))
}

/// Where a decode reads a code's term string from: the four roles are asked
/// separately so a dictionary-backed source can keep one cursor (and, for a
/// chunked dictionary, one warm chunk cursor) per role — the roles occupy
/// different regions of the sorted term space.
trait TermSource {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str>;
}

/// Reject a code outside a dictionary of `n_terms` terms.
fn check_code(code: u32, n_terms: usize) -> Result<()> {
    if code as usize >= n_terms {
        return Err(VortexRdfError::Deserialization(format!(
            "Term code {} out of dictionary bounds ({})",
            code, n_terms
        )));
    }
    Ok(())
}

/// Term strings read from a resident dictionary.
struct DictTerms<'a> {
    cursors: [DictCursor<'a>; 4],
    n_terms: usize,
}

impl TermSource for DictTerms<'_> {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str> {
        check_code(code, self.n_terms)?;
        self.cursors[role].str_at(code as usize)
    }
}

/// Term strings read from a pre-resolved map (the file-backed path).
#[cfg(feature = "file-io")]
struct MappedTerms<'a>(&'a HashMap<u32, Arc<str>>);

#[cfg(feature = "file-io")]
impl MappedTerms<'_> {
    fn get(&self, code: u32) -> Result<&Arc<str>> {
        self.0.get(&code).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "Term code {} missing from the chunk's resolved term map",
                code
            ))
        })
    }
}

#[cfg(feature = "file-io")]
impl TermSource for MappedTerms<'_> {
    fn str_at(&mut self, _role: usize, code: u32) -> Result<&str> {
        self.get(code).map(|term| &**term)
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
/// (a view lookup on a canonical dictionary, an FSST decompress on one
/// adopted as written) *and* the term parse again. Direct-mapped: a miss
/// costs one compare and one overwrite, and the memory is fixed whatever the
/// column's cardinality, so a high-cardinality column like subjects cannot
/// accumulate entries it never reads again.
struct TermMemo<T> {
    slots: Vec<Option<(u32, T)>>,
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

    fn get_or_insert(&mut self, code: u32, decode: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.slots.is_empty() {
            return decode();
        }
        let slot = &mut self.slots[code as usize & self.mask];
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

/// Decode a chunk's code columns into quads, reading each distinct code's
/// term at most once per role (see [`TermMemo`]).
fn decode_codes(
    s_codes: &[u32],
    p_codes: &[u32],
    o_codes: &[u32],
    g_codes: &[u32],
    src: &mut impl TermSource,
) -> Vec<Result<Quad>> {
    let n = s_codes.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
    );

    (0..n)
        .map(|i| {
            let subject =
                sm.get_or_insert(s_codes[i], || parse_subject(src.str_at(0, s_codes[i])?))?;
            let predicate =
                pm.get_or_insert(p_codes[i], || parse_named_node(src.str_at(1, p_codes[i])?))?;
            let object =
                om.get_or_insert(o_codes[i], || parse_object(src.str_at(2, o_codes[i])?))?;
            let graph =
                gm.get_or_insert(g_codes[i], || parse_graph_name(src.str_at(3, g_codes[i])?))?;
            Ok(Quad::new(subject, predicate, object, graph))
        })
        .collect()
}

/// Decode a Dictionary-layout StructArray chunk into Quads using the given
/// (store-cached) dictionary.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    let (s_col, p_col, o_col, g_col) = match code_columns(chunk) {
        Ok(cols) => cols,
        Err(e) => return vec![Err(e)],
    };
    let mut src = DictTerms {
        cursors: [dict.cursor(), dict.cursor(), dict.cursor(), dict.cursor()],
        n_terms: dict.len(),
    };
    decode_codes(
        s_col.as_slice::<u32>(),
        p_col.as_slice::<u32>(),
        o_col.as_slice::<u32>(),
        g_col.as_slice::<u32>(),
        &mut src,
    )
}

/// Decode one role's code column to owned term strings, reading each distinct
/// code's term at most once (see [`TermMemo`]) — the [`raw_quads`]
/// reconstruction path, where a predicate or graph column repeats a handful
/// of codes over every row and each repeat would otherwise pay the dictionary
/// read and the term parse again.
///
/// [`raw_quads`]: crate::store::layouts::ResolvedLayout::raw_quads
pub(super) fn decode_code_column<T: Clone + for<'a> From<&'a str>>(
    dict: &TermDictionary,
    codes: &[u32],
) -> Result<Vec<T>> {
    let mut cursor = dict.cursor();
    let mut memo: TermMemo<T> = TermMemo::new(codes.len());
    codes
        .iter()
        .map(|&code| {
            memo.get_or_insert(code, || {
                check_code(code, dict.len())?;
                cursor.str_at(code as usize).map(T::from)
            })
        })
        .collect()
}

/// The rows of a Dictionary-layout chunk as [`SharedQuad`]s: each role's
/// codes decoded through [`decode_code_column`] with `Arc<str>` terms, so a
/// code repeating down a column is decoded once and shared by reference
/// count — and nothing is parsed into oxrdf terms.
///
/// [`SharedQuad`]: crate::common::quad::SharedQuad
fn shared_rows(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Result<Vec<crate::common::quad::SharedQuad>> {
    let (s_col, p_col, o_col, g_col) = code_columns(chunk)?;
    let s = decode_code_column::<Arc<str>>(dict, s_col.as_slice::<u32>())?;
    let p = decode_code_column::<Arc<str>>(dict, p_col.as_slice::<u32>())?;
    let o = decode_code_column::<Arc<str>>(dict, o_col.as_slice::<u32>())?;
    let g = decode_code_column::<Arc<str>>(dict, g_col.as_slice::<u32>())?;
    Ok(s.into_iter()
        .zip(p)
        .zip(o)
        .zip(g)
        .map(|(((s, p), o), g)| crate::common::quad::SharedQuad { s, p, o, g })
        .collect())
}

/// [`decode_chunk`] with shared-string terms (see [`shared_rows`]).
pub(crate) fn decode_chunk_shared(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Vec<Result<crate::common::quad::SharedQuad>> {
    match shared_rows(chunk, dict) {
        Ok(rows) => rows.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    }
}

/// The distinct term codes a chunk's four code columns reference, ascending —
/// what a file-backed dictionary must resolve to decode the chunk.
#[cfg(feature = "file-io")]
pub(crate) fn unique_codes(chunk: &ArrayRef) -> Result<Vec<u32>> {
    let (s, p, o, g) = code_columns(chunk)?;
    let mut codes: Vec<u32> = Vec::with_capacity(s.len().saturating_mul(4));
    for col in [&s, &p, &o, &g] {
        codes.extend_from_slice(col.as_slice::<u32>());
    }
    codes.sort_unstable();
    codes.dedup();
    Ok(codes)
}

/// Resolve a chunk's distinct codes to terms with one scan of a file-backed
/// dictionary, keyed for [`decode_chunk_mapped`] and
/// [`decode_chunk_mapped_shared`].
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_chunk_terms(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Result<HashMap<u32, Arc<str>>> {
    let codes = unique_codes(chunk)?;
    let terms = fb.decode_many(&codes).await?;
    Ok(codes.into_iter().zip(terms).collect())
}

/// [`decode_chunk`] against a pre-resolved code→term map instead of a
/// resident dictionary — the file-backed reconstruction path: the caller
/// resolves the chunk's [`unique_codes`] with one scan and decodes with the
/// resulting map.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped(
    chunk: &ArrayRef,
    terms: &HashMap<u32, Arc<str>>,
) -> Vec<Result<Quad>> {
    let (s_col, p_col, o_col, g_col) = match code_columns(chunk) {
        Ok(cols) => cols,
        Err(e) => return vec![Err(e)],
    };
    decode_codes(
        s_col.as_slice::<u32>(),
        p_col.as_slice::<u32>(),
        o_col.as_slice::<u32>(),
        g_col.as_slice::<u32>(),
        &mut MappedTerms(terms),
    )
}

/// [`decode_chunk_mapped`] with shared-string terms: the pre-resolved map
/// already holds one `Arc<str>` per distinct code, so every row's term is a
/// lookup and a reference-count bump.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped_shared(
    chunk: &ArrayRef,
    terms: &HashMap<u32, Arc<str>>,
) -> Vec<Result<crate::common::quad::SharedQuad>> {
    let rows = || -> Result<Vec<crate::common::quad::SharedQuad>> {
        let (s_col, p_col, o_col, g_col) = code_columns(chunk)?;
        let terms = MappedTerms(terms);
        let (s, p, o, g) = (
            s_col.as_slice::<u32>(),
            p_col.as_slice::<u32>(),
            o_col.as_slice::<u32>(),
            g_col.as_slice::<u32>(),
        );
        (0..s.len())
            .map(|i| {
                Ok(crate::common::quad::SharedQuad {
                    s: terms.get(s[i])?.clone(),
                    p: terms.get(p[i])?.clone(),
                    o: terms.get(o[i])?.clone(),
                    g: terms.get(g[i])?.clone(),
                })
            })
            .collect()
    };
    match rows() {
        Ok(rows) => rows.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    }
}
