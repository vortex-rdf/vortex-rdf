//! Column-building and decoding logic for [`LayoutStrategy::Dictionary`]:
//! s/p/o/g stored as u32 codes into a global sorted term dictionary, which
//! itself lives in the layout's intrinsic `_dict_terms` column (see
//! [`super::term_dictionary`]).
//!
//! Unlike the other layouts, chunks are not built through the generic
//! `build_struct_array` path: encoding requires the global `TermDictionary`
//! (complete only after the whole dataset has been ingested), so the builders
//! run a dedicated two-pass pipeline that calls `build_chunk` directly.
//! Secondary indexes compose normally: they are appended per chunk via
//! `IndexType::append_dictionary_columns`, working on the encoded codes.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::ops::Range;
use std::sync::Arc;
use web_time::Instant;

use oxrdf::Quad;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use super::default::decode_spog;
use super::term_dictionary::{self, DICT_FIELD, TermDictionary, TermIdMap};
use crate::common::utils::{buf_as_str, stamp_is_sorted};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::indexes::secondary_by_copy::CopyKey;
use crate::store::indexes::{GlobalIndexes, IndexType, unique_indexes};
use crate::store::{QuadCodes, RawQuad};

/// Field names of the primary columns: `s`, `p`, `o`, `g` (all u32 codes).
pub(crate) fn field_names() -> Vec<Arc<str>> {
    vec!["s".into(), "p".into(), "o".into(), "g".into()]
}

/// Encode every term of every quad to its dictionary code.
pub(crate) fn encode_quads(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &TermIdMap,
) -> Result<QuadCodes> {
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
/// columns plus the `_dict_terms` column — returning the open field vectors
/// so the caller can append index columns before finalizing.
///
/// `carry_dict` must be set on exactly the first chunk of a build — it stores
/// the dictionary payload in row 0 of `_dict_terms`; later chunks carry empty
/// lists (same dtype). `s_sorted` stamps the `IsSorted` statistic on the `s`
/// column; valid because sorted-dictionary codes preserve lexicographic order.
fn chunk_parts(
    codes: &QuadCodes,
    range: Range<usize>,
    dict: &TermDictionary,
    s_sorted: bool,
    carry_dict: bool,
) -> Result<(Vec<Arc<str>>, Vec<ArrayRef>)> {
    let n = range.len();
    let mut names = field_names();
    let mut arrays: Vec<ArrayRef> = vec![
        PrimitiveArray::from_iter(codes.s[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.p[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.o[range.clone()].iter().copied()).into_array(),
        PrimitiveArray::from_iter(codes.g[range].iter().copied()).into_array(),
    ];

    if s_sorted {
        stamp_is_sorted(&arrays[0]);
    }

    names.push(DICT_FIELD.into());
    arrays.push(term_dictionary::dict_column(dict, n, carry_dict)?);

    Ok((names, arrays))
}

fn finish_chunk(names: Vec<Arc<str>>, arrays: Vec<ArrayRef>, n: usize) -> Result<ArrayRef> {
    StructArray::try_new(names.into(), arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Build a complete Dictionary-layout StructArray chunk: four u32 code columns
/// encoded against the global dictionary, the `_dict_terms` column, and the
/// columns of every requested secondary index (deduplicated, built over the
/// same codes by sorting this chunk's quads).
///
/// `start_row` has the same global-row-ID semantics as `build_struct_array`,
/// and `whole_dataset` the same index-stamping semantics: pass `true` only
/// when `quads` is the entire dataset, so the per-chunk index sort is the
/// global order.
#[allow(clippy::too_many_arguments)] // each input is distinct; bundling would only add ceremony
pub(crate) fn build_chunk(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &TermIdMap,
    indexes: &[IndexType],
    start_row: u32,
    s_sorted: bool,
    carry_dict: bool,
    whole_dataset: bool,
) -> Result<ArrayRef> {
    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let primary_start = Instant::now();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, dict, s_sorted, carry_dict)?;
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

/// Build a Dictionary-layout chunk for rows `range` of a fully encoded
/// dataset, with index columns sliced from the precomputed global order —
/// the sorted in-memory builders' chunked emission path.
pub(crate) fn build_chunk_global(
    codes: &QuadCodes,
    range: Range<usize>,
    dict: &TermDictionary,
    global_indexes: &GlobalIndexes,
    s_sorted: bool,
    carry_dict: bool,
) -> Result<ArrayRef> {
    let start = Instant::now();
    let n = range.len();
    let (mut names, mut arrays) = chunk_parts(codes, range.clone(), dict, s_sorted, carry_dict)?;
    global_indexes.append_slice(&mut names, &mut arrays, range)?;
    let chunk = finish_chunk(names, arrays, n)?;
    log::debug!(
        "[Dictionary] Built globally encoded chunk of {} rows (carry_payload={}) in {:?}",
        n,
        carry_dict,
        start.elapsed()
    );
    Ok(chunk)
}

/// The two `SecondaryByReference` code columns of a presorted chunk, as
/// borrowed globally sorted (code, row ID) slices: (objects, predicates).
type RefPairSlices<'a> = (&'a [(u32, u32)], &'a [(u32, u32)]);
/// The two `SecondaryByCopy` code columns of a presorted chunk, as borrowed
/// globally sorted (sort key, row ID) slices: (POSG, OSPG).
type CopyKeySlices<'a> = (&'a [(CopyKey<u32>, u32)], &'a [(CopyKey<u32>, u32)]);

/// Build a Dictionary-layout chunk with index columns taken from
/// already-globally-sorted (code, row ID) entries — the out-of-core sorted
/// builder's emission path, where the entries are merged from disk runs.
/// Each index family is appended only when its entries are supplied.
pub(crate) fn build_chunk_presorted_indexes(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &TermIdMap,
    ref_pairs: Option<RefPairSlices<'_>>,
    copy_keys: Option<CopyKeySlices<'_>>,
    s_sorted: bool,
    carry_dict: bool,
) -> Result<ArrayRef> {
    use crate::store::indexes::secondary_by_copy::append_sorted_code_keys;
    use crate::store::indexes::secondary_by_reference::append_sorted_code_pairs;

    let total_start = Instant::now();
    let n = quads.len();
    let encode_start = Instant::now();
    let codes = encode_quads(quads, dict, id_map)?;
    let encode_elapsed = encode_start.elapsed();
    let (mut names, mut arrays) = chunk_parts(&codes, 0..n, dict, s_sorted, carry_dict)?;
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
        false,
    )
}

/// Rewrite `array`'s `_dict_terms` column so that it carries `dict` as its
/// payload, leaving every other column untouched.
///
/// The payload lives in row 0 alone (see [`term_dictionary::dict_column`]), so
/// an array derived by slicing or filtering may have dropped it, and a
/// file-backed scan projects it away entirely. Such an array still decodes
/// through the store's cached dictionary, but on its own — once written out and
/// read back — its codes would resolve against an empty dictionary. Re-attaching
/// the payload makes the array self-describing again.
pub(crate) fn attach_payload(array: ArrayRef, dict: &TermDictionary) -> Result<ArrayRef> {
    // Read the field names before executing: `dtype()` is metadata, so this
    // avoids caring whether the array is still a lazy expression.
    let existing: Vec<Arc<str>> = match array.dtype() {
        DType::Struct(fields, _) => fields
            .names()
            .iter()
            .filter(|n| n.as_ref() != DICT_FIELD)
            .map(|n| n.inner().clone())
            .collect(),
        _ => {
            return Err(VortexRdfError::Serialization(
                "Dictionary-layout array is not a struct".to_string(),
            ));
        }
    };

    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = array
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let len = struct_arr.len();

    // Carry over every column except the payload, which is rebuilt from `dict`.
    let mut arrays: Vec<ArrayRef> = existing
        .iter()
        .map(|name| {
            struct_arr
                .unmasked_field_by_name(name.as_ref())
                .cloned()
                .map_err(VortexRdfError::Vortex)
        })
        .collect::<Result<_>>()?;

    let mut names = existing;
    names.push(DICT_FIELD.into());
    arrays.push(term_dictionary::dict_column(dict, len, true)?);

    finish_chunk(names, arrays, len)
}

/// Decode a Dictionary-layout StructArray chunk into Quads using the given
/// (store-cached) dictionary. The chunk's own `_dict_terms` column is ignored —
/// derived chunks (sliced/filtered/file-scanned) may have lost the payload row.
pub(crate) fn decode_chunk(chunk: &ArrayRef, dict: &TermDictionary) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();

    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    let n = struct_arr.len();

    macro_rules! get_u32_col {
        ($name:expr) => {
            match struct_arr
                .unmasked_field_by_name($name)
                .map_err(VortexRdfError::Vortex)
                .and_then(|c| {
                    c.clone()
                        .execute::<PrimitiveArray>(&mut ctx)
                        .map_err(VortexRdfError::Vortex)
                }) {
                Ok(arr) => arr,
                Err(e) => return vec![Err(e)],
            }
        };
    }

    let s_col = get_u32_col!("s");
    let p_col = get_u32_col!("p");
    let o_col = get_u32_col!("o");
    let g_col = get_u32_col!("g");

    let s_ids = s_col.as_slice::<u32>();
    let p_ids = p_col.as_slice::<u32>();
    let o_ids = o_col.as_slice::<u32>();
    let g_ids = g_col.as_slice::<u32>();

    let term_at = |id: u32| -> Result<_> {
        if (id as usize) < dict.len() {
            Ok(dict.view().bytes_at(id as usize))
        } else {
            Err(VortexRdfError::Deserialization(format!(
                "Term code {} out of dictionary bounds ({})",
                id,
                dict.len()
            )))
        }
    };

    (0..n)
        .map(|i| {
            // Zero-copy views over the dictionary's term bytes; the oxrdf
            // constructors make the single owned copy.
            let s_buf = term_at(s_ids[i])?;
            let p_buf = term_at(p_ids[i])?;
            let o_buf = term_at(o_ids[i])?;
            let g_buf = term_at(g_ids[i])?;
            decode_spog(
                buf_as_str(s_buf.as_ref())?,
                buf_as_str(p_buf.as_ref())?,
                buf_as_str(o_buf.as_ref())?,
                buf_as_str(g_buf.as_ref())?,
            )
        })
        .collect()
}
