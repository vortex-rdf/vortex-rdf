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
//! Secondary indexes compose normally: they are appended per chunk via
//! `IndexType::append_dictionary_columns`, working on the encoded codes.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use web_time::Instant;

use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::terms::{get_as_term, parse_graph_name, parse_named_node, parse_subject};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::builders::GlobalIndexes;
use crate::store::indexes::secondary_by_copy::CopyKey;
use crate::store::indexes::{IndexType, unique_indexes};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

pub(crate) mod access;
#[cfg(feature = "file-io")]
pub(crate) mod file_backed;
pub(crate) mod ingest;
pub(crate) mod term_dict;

#[cfg(feature = "file-io")]
pub(crate) use self::file_backed::{FileBackedDict, TermChunks};
pub use self::ingest::DictionaryQuadSink;
pub(crate) use self::ingest::{TermIdMap, ingest_interning};
// Only the (wasm-gated) external-sort builder collects terms ahead of the
// encoding pass.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::ingest::TermDictionaryBuilder;
use self::term_dict::DictReader;
pub use self::term_dict::DictSnapshot;
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) use self::term_dict::dict_child_chunks;
pub(crate) use self::term_dict::{TermDictionary, dict_from_reader};

/// Field names of the primary columns: `s`, `p`, `o`, `g` (all u32 codes).
pub(crate) fn field_names() -> Vec<Arc<str>> {
    PRIMARY_COLUMNS.iter().map(|&n| n.into()).collect()
}

/// Dictionary-encoded quad columns: [`RawQuad`] terms replaced by their u32
/// codes in the global sorted term dictionary. Produced by the Dictionary
/// layout's encoding pass and consumed by index builders, which can work on
/// codes directly (sorted-dictionary codes preserve lexicographic order).
pub(crate) struct QuadCodes {
    pub s: Vec<u32>,
    pub p: Vec<u32>,
    pub o: Vec<u32>,
    pub g: Vec<u32>,
}

/// Encode every term of every quad to its dictionary code.
///
/// Generic over the map's key so both the owned [`TermIdMap`] (streaming
/// builders) and the borrowed [`BorrowedTermIdMap`] (builders holding a live
/// quad slice) work without a second code path — `&str: Borrow<str>` makes the
/// `get(term)` lookup identical for either.
///
/// [`BorrowedTermIdMap`]: self::ingest::BorrowedTermIdMap
fn encode_quads<K>(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &HashMap<K, u32>,
) -> Result<QuadCodes>
where
    K: Borrow<str> + Eq + Hash,
{
    let start = Instant::now();
    let encode_column = |term_of: fn(&RawQuad) -> &str| -> Result<Vec<u32>> {
        let mut ids: Vec<u32> = Vec::with_capacity(quads.len());
        for q in quads {
            let term = term_of(q);
            ids.push(id_map.get(term).copied().ok_or_else(|| {
                VortexRdfError::Serialization(format!(
                    "Term missing from dictionary during encoding: {}",
                    term
                ))
            })?);
        }
        Ok(ids)
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
        dict.len(),
        start.elapsed()
    );
    Ok(codes)
}

/// Build the primary part of a Dictionary-layout chunk — the four u32 code
/// columns — returning the open field vectors so the caller can append index
/// columns before finalizing.
///
/// The term dictionary is *not* a column of the chunk: in memory it lives in
/// the layout ([`DictAccess`]), and serialized files carry it as the native
/// container's `dictionary` child.
/// `s_sorted` stamps the `IsSorted` statistic on the `s` column; valid
/// because sorted-dictionary codes preserve lexicographic order.
///
/// [`DictAccess`]: self::access::DictAccess
fn chunk_parts(
    codes: &QuadCodes,
    range: Range<usize>,
    s_sorted: bool,
) -> (Vec<Arc<str>>, Vec<ArrayRef>) {
    let names = field_names();
    let arrays: Vec<ArrayRef> = vec![
        PrimitiveArray::from_iter(codes.s[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.p[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.o[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.g[range].iter().copied()).into_array(),
    ];

    if s_sorted {
        stamp_is_sorted(&arrays[0]);
    }

    (names, arrays)
}

fn finish_chunk(names: Vec<Arc<str>>, arrays: Vec<ArrayRef>, n: usize) -> Result<ArrayRef> {
    StructArray::try_new(names.into(), arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Build a complete Dictionary-layout StructArray chunk: four u32 code columns
/// encoded against the global dictionary, plus the columns of every requested
/// secondary index (deduplicated, built over the same codes by sorting this
/// chunk's quads).
///
/// `start_row` has the same global-row-ID semantics as `build_struct_array`,
/// and `whole_dataset` the same index-stamping semantics: pass `true` only
/// when `quads` is the entire dataset, so the per-chunk index sort is the
/// global order.
pub(crate) fn build_chunk<K>(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &HashMap<K, u32>,
    indexes: &[IndexType],
    start_row: u32,
    s_sorted: bool,
    whole_dataset: bool,
) -> Result<ArrayRef>
where
    K: Borrow<str> + Eq + Hash,
{
    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let primary_start = Instant::now();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, s_sorted);
    let primary_elapsed = primary_start.elapsed();

    let indexes_start = Instant::now();
    for idx in unique_indexes(indexes) {
        idx.append_dictionary_columns(&mut names, &mut arrays, &codes, start_row, whole_dataset);
    }
    let indexes_elapsed = indexes_start.elapsed();

    let finish_start = Instant::now();
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built chunk of {} rows at row {}: encode {:?}, primary columns {:?}, indexes {:?}, struct {:?}, total {:?}",
        n,
        start_row,
        encode_elapsed,
        primary_elapsed,
        indexes_elapsed,
        finish_start.elapsed(),
        total_start.elapsed()
    );
    Ok(chunk)
}

/// Build the whole dataset as one contiguous Dictionary-layout chunk from its
/// codes — the in-memory builders' construction path, fed by the interning
/// ingest ([`InterningQuadBuilder`]) so no owned quad strings are involved.
///
/// The codes arrive in global (s, p, o, g) order, so the `s` column is
/// stamped sorted; index columns are globally sorted too
/// (`GlobalIndexes::from_codes` sorts pairs).
///
/// [`InterningQuadBuilder`]: self::ingest::InterningQuadBuilder
pub(crate) fn build_array(codes: &QuadCodes, indexes: &[IndexType]) -> Result<ArrayRef> {
    if codes.s.is_empty() {
        return empty_struct(indexes);
    }
    let n = codes.s.len();
    let global_idx = GlobalIndexes::from_codes(indexes, codes);
    build_chunk_global(codes, 0..n, &global_idx, true)
}

/// Build a Dictionary-layout chunk for rows `range` of a fully encoded
/// dataset, with index columns sliced from the precomputed global order —
/// the sorted in-memory builders' chunked emission path.
pub(crate) fn build_chunk_global(
    codes: &QuadCodes,
    range: Range<usize>,
    global_indexes: &GlobalIndexes,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let start = Instant::now();
    let n = range.len();
    let (mut names, mut arrays) = chunk_parts(codes, range.clone(), s_sorted);
    global_indexes.append_slice(&mut names, &mut arrays, range)?;
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built globally encoded chunk of {} rows in {:?}",
        n,
        start.elapsed()
    );
    Ok(chunk)
}

/// The two `SecondaryByReference` code columns of a presorted chunk, as
/// borrowed globally sorted (code, row ID) slices: (objects, predicates).
// This vocabulary serves only the external-sort builder, compiled out on
// wasm (see the module gate in `store::builders`).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
type RefPairSlices<'a> = (&'a [(u32, u32)], &'a [(u32, u32)]);
/// The two `SecondaryByCopy` code columns of a presorted chunk, as borrowed
/// globally sorted (sort key, row ID) slices: (POSG, OSPG).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
type CopyKeySlices<'a> = (&'a [(CopyKey<u32>, u32)], &'a [(CopyKey<u32>, u32)]);

/// Build a Dictionary-layout chunk with index columns taken from
/// already-globally-sorted (code, row ID) entries — the out-of-core sorted
/// builder's emission path, where the entries are merged from disk runs.
/// Each index family is appended only when its entries are supplied.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn build_chunk_presorted_indexes(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &TermIdMap,
    ref_pairs: Option<RefPairSlices<'_>>,
    copy_keys: Option<CopyKeySlices<'_>>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    use crate::store::indexes::secondary_by_copy::append_sorted_code_keys;
    use crate::store::indexes::secondary_by_reference::append_sorted_code_pairs;

    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, s_sorted);
    if let Some((posg, ospg)) = copy_keys {
        append_sorted_code_keys(&mut names, &mut arrays, posg, ospg, true);
    }
    if let Some((o_pairs, p_pairs)) = ref_pairs {
        append_sorted_code_pairs(&mut names, &mut arrays, o_pairs, p_pairs, true);
    }
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built presorted-index chunk of {} rows: encode {:?}, remaining build {:?}, total {:?}",
        n,
        encode_elapsed,
        total_start.elapsed().saturating_sub(encode_elapsed),
        total_start.elapsed()
    );
    Ok(chunk)
}

/// An empty StructArray with the Dictionary-layout schema (including the
/// columns of any requested secondary indexes).
pub(crate) fn empty_struct(indexes: &[IndexType]) -> Result<ArrayRef> {
    build_chunk(
        &[],
        &TermDictionary::empty(),
        &TermIdMap::new(),
        indexes,
        0,
        false,
        false,
    )
}

/// The four primary code columns of a chunk, as arrays whose `u32` slices the
/// decoders read. Returned rather than borrowed from a temporary: the slices
/// borrow these arrays, so they must outlive the decode.
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
    let mut col = |name: &str| -> Result<PrimitiveArray> {
        struct_arr
            .unmasked_field_by_name(name)
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)
    };
    Ok((col(COL_S)?, col(COL_P)?, col(COL_O)?, col(COL_G)?))
}

/// Where a decode reads a code's term string from: the four roles are asked
/// separately so a dictionary-backed source can keep one reader (and, for a
/// chunked dictionary, one warm chunk cursor) per role — the roles occupy
/// different regions of the sorted term space.
trait TermSource {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str>;
}

/// Term strings read from a resident dictionary.
struct DictTerms<'a> {
    readers: [DictReader<'a>; 4],
    n_terms: usize,
}

impl TermSource for DictTerms<'_> {
    fn str_at(&mut self, role: usize, code: u32) -> Result<&str> {
        if code as usize >= self.n_terms {
            return Err(VortexRdfError::Deserialization(format!(
                "Term code {} out of dictionary bounds ({})",
                code, self.n_terms
            )));
        }
        self.readers[role].str_at(code as usize)
    }
}

/// Term strings read from a pre-resolved map (the file-backed path).
#[cfg(feature = "file-io")]
struct MappedTerms<'a>(&'a HashMap<u32, String>);

#[cfg(feature = "file-io")]
impl TermSource for MappedTerms<'_> {
    fn str_at(&mut self, _role: usize, code: u32) -> Result<&str> {
        self.0.get(&code).map(String::as_str).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "Term code {} missing from the chunk's resolved term map",
                code
            ))
        })
    }
}

/// Upper bound on a role memo's slots. Sized to hold the distinct predicates
/// and graph names of realistic datasets outright, while keeping the memo an
/// L2-resident table rather than something that grows with the data.
const MEMO_MAX_SLOTS: usize = 1024;

/// Below this many rows a chunk decodes without a memo at all: the table's
/// own allocation would cost more than the handful of repeats it could catch,
/// and single-row decodes (an rdflib-style probe resolving one binding) are
/// hot enough to notice.
const MEMO_MIN_ROWS: usize = 16;

/// A direct-mapped memo of one role's decoded terms, keyed by term code.
///
/// Codes repeat heavily down a column — a predicate or graph name recurs on
/// nearly every row — and each repeat would otherwise pay the dictionary read
/// (an FSST decompress) *and* the term parse again. Direct mapping rather
/// than a hash map because the miss path must stay nearly free: a
/// high-cardinality column like subjects would fill any growing structure
/// with entries it never reads again, whereas here a miss costs one compare
/// and one overwrite, and memory is fixed regardless of the column.
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
    s_ids: &[u32],
    p_ids: &[u32],
    o_ids: &[u32],
    g_ids: &[u32],
    src: &mut impl TermSource,
) -> Vec<Result<Quad>> {
    let n = s_ids.len();
    let (mut sm, mut pm, mut om, mut gm) = (
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
        TermMemo::new(n),
    );

    (0..n)
        .map(|i| {
            let subject = sm.get_or_insert(s_ids[i], || parse_subject(src.str_at(0, s_ids[i])?))?;
            let predicate =
                pm.get_or_insert(p_ids[i], || parse_named_node(src.str_at(1, p_ids[i])?))?;
            let object = om.get_or_insert(o_ids[i], || {
                let term = src.str_at(2, o_ids[i])?;
                get_as_term(term).ok_or_else(|| {
                    VortexRdfError::Deserialization(format!("Invalid object: {term}"))
                })
            })?;
            let graph =
                gm.get_or_insert(g_ids[i], || parse_graph_name(src.str_at(3, g_ids[i])?))?;
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
        readers: [dict.reader(), dict.reader(), dict.reader(), dict.reader()],
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
/// read (an FSST decompress) again.
///
/// [`raw_quads`]: crate::store::layouts::ResolvedLayout::raw_quads
pub(super) fn decode_code_column(dict: &TermDictionary, codes: &[u32]) -> Result<Vec<String>> {
    let mut reader = dict.reader();
    let mut memo: TermMemo<String> = TermMemo::new(codes.len());
    codes
        .iter()
        .map(|&code| {
            memo.get_or_insert(code, || {
                if (code as usize) >= dict.len() {
                    return Err(VortexRdfError::Deserialization(format!(
                        "Term code {} out of dictionary bounds ({})",
                        code,
                        dict.len()
                    )));
                }
                reader.str_at(code as usize).map(str::to_string)
            })
        })
        .collect()
}

/// The distinct term codes a chunk's four code columns reference, ascending —
/// what a file-backed dictionary must resolve to decode the chunk.
#[cfg(feature = "file-io")]
pub(crate) fn unique_codes(chunk: &ArrayRef) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut codes: Vec<u32> = Vec::with_capacity(struct_arr.len().saturating_mul(4));
    for name in PRIMARY_COLUMNS {
        let col = struct_arr
            .unmasked_field_by_name(name)
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        codes.extend_from_slice(col.as_slice::<u32>());
    }
    codes.sort_unstable();
    codes.dedup();
    Ok(codes)
}

/// [`decode_chunk`] against a pre-resolved code→term map instead of a
/// resident dictionary — the file-backed reconstruction path: the caller
/// resolves the chunk's [`unique_codes`] with one scan and decodes with the
/// resulting map.
#[cfg(feature = "file-io")]
pub(crate) fn decode_chunk_mapped(
    chunk: &ArrayRef,
    terms: &HashMap<u32, String>,
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
