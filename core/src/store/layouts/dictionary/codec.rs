//! The Dictionary layout's chunk codec: quads to u32 code columns against
//! a term → code map, and code columns back to quads through a term source.

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

use crate::common::quad::SharedQuad;
use crate::common::terms::{parse_graph_name, parse_named_node, parse_object, parse_subject};
use crate::debug;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{field_as, stamp_is_sorted};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

#[cfg(feature = "file-io")]
use super::file_backed::FileBackedDict;
use super::storage::{DictCursor, check_code};
use super::term_dict::TermDictionary;

/// Dictionary-encoded quad columns: every term replaced by its u32 code.
/// Code order is term byte order, so index children built over the codes
/// sort as they would over the terms.
#[derive(Default)]
pub(crate) struct QuadCodes {
    pub(crate) s: Vec<u32>,
    pub(crate) p: Vec<u32>,
    pub(crate) o: Vec<u32>,
    pub(crate) g: Vec<u32>,
}

impl QuadCodes {
    /// The encoding of no quads, with the code dtypes of a non-empty one.
    pub(crate) fn empty() -> Self {
        Self {
            s: Vec::new(),
            p: Vec::new(),
            o: Vec::new(),
            g: Vec::new(),
        }
    }
}

/// The code of `term` in `code_map`; an error for a term the map lacks.
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

/// Every term of every quad as its code in `code_map`.
pub(crate) fn encode_quads<K>(quads: &[RawQuad], code_map: &HashMap<K, u32>) -> Result<QuadCodes>
where
    K: Borrow<str> + Eq + Hash,
{
    let start = debug::timer();
    let column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<u32>> {
        quads
            .iter()
            .map(|q| code_of(code_map, term_of(q)))
            .collect()
    };
    let codes = QuadCodes {
        s: column(|q| &q.s)?,
        p: column(|q| &q.p)?,
        o: column(|q| &q.o)?,
        g: column(|q| &q.g)?,
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

/// A Dictionary-layout chunk of `quads`: four u32 code columns encoded
/// through `code_map`; `s_sorted` stamps `IsSorted` on the `s` column.
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

/// The whole dataset as one chunk; `codes` arrive in global (s, p, o, g)
/// order, so the `s` column is stamped sorted.
pub(crate) fn build_array(codes: &QuadCodes) -> Result<ArrayRef> {
    if codes.s.is_empty() {
        return empty_struct();
    }
    let n = codes.s.len();
    build_code_chunk(codes, 0..n, true)
}

/// The chunk of rows `range` of `codes`: the four u32 code columns and
/// nothing else (the dictionary travels beside the array). `s_sorted` stamps
/// `IsSorted` on the `s` column, valid because code order is term order.
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

/// An empty chunk with the code schema, `s` unstamped.
pub(crate) fn empty_struct() -> Result<ArrayRef> {
    build_code_chunk(&QuadCodes::empty(), 0..0, false)
}

/// The four code columns of `chunk` in (s, p, o, g) order; the decoders'
/// `u32` slices borrow them.
fn code_columns(chunk: &ArrayRef) -> Result<[PrimitiveArray; 4]> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut col = |name: &str| field_as::<PrimitiveArray>(&struct_arr, name, &mut ctx);
    Ok([col(COL_S)?, col(COL_P)?, col(COL_O)?, col(COL_G)?])
}

/// Where a decode reads a code's term; the roles are asked separately so a
/// source can keep one cursor per role.
trait TermSource {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str>;

    /// [`str_at`](Self::str_at) as a shared string.
    fn shared_at(&mut self, role: usize, code: u32) -> Result<Arc<str>> {
        self.str_at(role, code).map(Arc::from)
    }
}

/// Terms read from a resident dictionary, one cursor per role.
struct DictTerms<'a> {
    cursors: [DictCursor<'a>; 4],
    n_terms: usize,
}

impl<'a> DictTerms<'a> {
    fn new(dict: &'a TermDictionary) -> Self {
        Self {
            cursors: std::array::from_fn(|_| dict.cursor()),
            n_terms: dict.len(),
        }
    }
}

impl TermSource for DictTerms<'_> {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str> {
        check_code(code, self.n_terms)?;
        self.cursors[role].str_at(code as usize)
    }
}

/// Terms read from a pre-resolved code → term map (the file-backed path).
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

    fn shared_at(&mut self, _role: usize, code: u32) -> Result<Arc<str>> {
        self.get(code).map(Arc::clone)
    }
}

/// Most slots of a role memo.
const MEMO_MAX_SLOTS: usize = 1024;

/// Chunks below this many rows decode without a memo.
const MEMO_MIN_ROWS: usize = 16;

/// A direct-mapped memo of one role's decoded terms, keyed by code: one slot
/// per masked code, overwritten on collision. Sized to the chunk's row count
/// (a power of two, capped at [`MEMO_MAX_SLOTS`]; none below
/// [`MEMO_MIN_ROWS`] rows).
struct TermMemo<T> {
    slots: Vec<Option<(u32, T)>>,
    mask: usize,
}

impl<T: Clone> TermMemo<T> {
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

/// `[s, p, o, g]` code columns as quads, each distinct code's term read and
/// parsed at most once per role.
fn decode_codes(cols: [&[u32]; 4], src: &mut impl TermSource) -> Vec<Result<Quad>> {
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

/// `chunk`'s rows as quads read through `src`: a chunk-level failure is a
/// single `Err` element; a row whose terms fail to parse is an `Err` at that
/// row's position.
fn decode_rows<S: TermSource>(chunk: &ArrayRef, src: &mut S) -> Vec<Result<Quad>> {
    let cols = match code_columns(chunk) {
        Ok(cols) => cols,
        Err(e) => return vec![Err(e)],
    };
    decode_codes(cols.each_ref().map(|col| col.as_slice::<u32>()), src)
}

/// `chunk`'s rows as [`SharedQuad`]s read through `src`, a code repeating
/// down a column decoded once per role; any failure is a single `Err`
/// element.
fn decode_rows_shared<S: TermSource>(chunk: &ArrayRef, src: &mut S) -> Vec<Result<SharedQuad>> {
    let mut rows = || -> Result<Vec<SharedQuad>> {
        let cols = code_columns(chunk)?;
        let [s, p, o, g] = cols.each_ref().map(|col| col.as_slice::<u32>());
        let mut memos: [TermMemo<Arc<str>>; 4] = std::array::from_fn(|_| TermMemo::new(s.len()));
        (0..s.len())
            .map(|i| {
                let [sm, pm, om, gm] = &mut memos;
                Ok(SharedQuad {
                    s: sm.get_or_insert(s[i], || src.shared_at(0, s[i]))?,
                    p: pm.get_or_insert(p[i], || src.shared_at(1, p[i]))?,
                    o: om.get_or_insert(o[i], || src.shared_at(2, o[i]))?,
                    g: gm.get_or_insert(g[i], || src.shared_at(3, g[i]))?,
                })
            })
            .collect()
    };
    match rows() {
        Ok(rows) => rows.into_iter().map(Ok).collect(),
        Err(e) => vec![Err(e)],
    }
}

/// `chunk` decoded against the resident `dict`.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    decode_rows(chunk, &mut DictTerms::new(dict))
}

/// [`decode_chunk`] with shared-string terms.
pub(crate) fn decode_chunk_shared(
    chunk: &ArrayRef,
    dict: &TermDictionary,
) -> Vec<Result<SharedQuad>> {
    decode_rows_shared(chunk, &mut DictTerms::new(dict))
}

/// One role's code column as owned terms, each distinct code read once.
pub(crate) fn decode_code_column<T: Clone + for<'a> From<&'a str>>(
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

/// The distinct codes of `chunk`'s four columns, ascending.
#[cfg(feature = "file-io")]
fn unique_codes(chunk: &ArrayRef) -> Result<Vec<u32>> {
    let cols = code_columns(chunk)?;
    let mut codes: Vec<u32> = Vec::with_capacity(cols[0].len().saturating_mul(4));
    for col in &cols {
        codes.extend_from_slice(col.as_slice::<u32>());
    }
    codes.sort_unstable();
    codes.dedup();
    Ok(codes)
}

/// `chunk`'s distinct codes resolved to terms with one read of `fb`, keyed
/// for [`decode_chunk_mapped`] and [`decode_chunk_mapped_shared`].
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_chunk_terms(
    fb: &FileBackedDict,
    chunk: &ArrayRef,
) -> Result<HashMap<u32, Arc<str>>> {
    let codes = unique_codes(chunk)?;
    let terms = fb.decode_many(&codes).await?;
    Ok(codes.into_iter().zip(terms).collect())
}

/// [`decode_chunk`] against a pre-resolved code → term map.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped(
    chunk: &ArrayRef,
    terms: &HashMap<u32, Arc<str>>,
) -> Vec<Result<Quad>> {
    decode_rows(chunk, &mut MappedTerms(terms))
}

/// [`decode_chunk_mapped`] with shared-string terms.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped_shared(
    chunk: &ArrayRef,
    terms: &HashMap<u32, Arc<str>>,
) -> Vec<Result<SharedQuad>> {
    decode_rows_shared(chunk, &mut MappedTerms(terms))
}
