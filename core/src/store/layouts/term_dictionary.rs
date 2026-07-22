//! The global term dictionary backing [`LayoutStrategy::Dictionary`]:
//! the lexicographically sorted set of unique RDF term strings, where a term's
//! ID is its sorted position. The s/p/o/g columns store these IDs as u32 codes.
//!
//! Because IDs are sorted ranks, ID comparisons are order-isomorphic to string
//! comparisons and term→ID lookup is a binary search — no HashMap is needed on
//! the query side, and the dictionary is held in its compact columnar form
//! (`VarBinViewArray`) rather than as owned `String`s.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

use std::collections::{HashMap, HashSet};
use web_time::Instant;

use vortex_array::arrays::listview::ListViewArrayExt as _;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{ListArray, ListViewArray, PrimitiveArray, VarBinViewArray};
#[cfg(feature = "file-io")]
use vortex_array::expr::{root, select};
use vortex_array::scalar::Scalar;
use vortex_array::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt as _;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

/// Name of the dictionary column: a `list<utf8>` root column where row 0 holds
/// the entire sorted dictionary as one list and every other row is an empty list.
pub(crate) const DICT_FIELD: &str = "_dict_terms";

/// Build-only term-to-ID lookup table. It is deliberately kept separate from
/// [`TermDictionary`] so stores retain only the compact columnar dictionary;
/// builders drop this map as soon as all quad terms have been encoded.
pub(crate) type TermIdMap = HashMap<String, u32>;

/// Incrementally collects the unique term strings of a dataset during the
/// ingestion pass of a build. Owned strings exist only for the build's lifetime.
pub(crate) struct TermDictionaryBuilder {
    set: HashSet<String>,
}

impl TermDictionaryBuilder {
    pub(crate) fn new() -> Self {
        Self {
            set: HashSet::new(),
        }
    }

    pub(crate) fn insert_quad(&mut self, q: &RawQuad) {
        for term in [&q.s, &q.p, &q.o, &q.g] {
            if !self.set.contains(term.as_str()) {
                self.set.insert(term.clone());
            }
        }
    }

    /// Sort the unique terms and freeze them into the columnar dictionary.
    pub(crate) fn finish(self) -> Result<TermDictionary> {
        let total_start = Instant::now();
        let collect_start = Instant::now();
        let mut terms: Vec<String> = self.set.into_iter().collect();
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        terms.sort_unstable();
        let sort_elapsed = sort_start.elapsed();
        let freeze_start = Instant::now();
        let dict = TermDictionary::from_sorted(terms.iter().map(String::as_str))?;
        log::debug!(
            "[Dictionary] Finished incremental dictionary ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, total {:?}",
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok(dict)
    }
}

/// The frozen, sorted term dictionary in columnar form.
///
/// term→ID is a host-side binary search over zero-copy `bytes_at` views;
/// ID→term is a zero-copy `bytes_at` read.
#[derive(Clone)]
pub(crate) struct TermDictionary {
    terms: VarBinViewArray,
}

impl TermDictionary {
    pub(crate) fn empty() -> Self {
        Self {
            terms: VarBinViewArray::from_iter_str(std::iter::empty::<&str>()),
        }
    }

    /// Build from already-sorted unique term strings.
    fn from_sorted<'a>(terms: impl Iterator<Item = &'a str> + Clone) -> Result<Self> {
        let dict = Self {
            terms: VarBinViewArray::from_iter_str(terms),
        };
        // List offsets are i32, so the term count must fit in one.
        if dict.len() > i32::MAX as usize {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {} unique terms exceeds the supported maximum ({})",
                dict.len(),
                i32::MAX
            )));
        }
        Ok(dict)
    }

    /// Build from a complete in-memory quad slice (single-pass builders).
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Result<Self> {
        let total_start = Instant::now();
        let collect_start = Instant::now();
        let mut set: HashSet<&str> = HashSet::new();
        for q in quads {
            set.insert(&q.s);
            set.insert(&q.p);
            set.insert(&q.o);
            set.insert(&q.g);
        }
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        let mut terms: Vec<&str> = set.into_iter().collect();
        terms.sort_unstable();
        let sort_elapsed = sort_start.elapsed();
        let freeze_start = Instant::now();
        let dict = Self::from_sorted(terms.into_iter())?;
        log::debug!(
            "[Dictionary] Built dictionary from {} quads ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, total {:?}",
            quads.len(),
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_start.elapsed(),
            total_start.elapsed()
        );
        Ok(dict)
    }

    pub(crate) fn len(&self) -> usize {
        self.terms.len()
    }

    /// The sorted term column itself (utf8, non-nullable).
    pub(crate) fn view(&self) -> &VarBinViewArray {
        &self.terms
    }

    /// Decode a code back to its term string (canonical N-Triples form), or
    /// `None` if the code is out of the dictionary's range. Zero-copy read of
    /// the term bytes; the returned `String` is the single owned copy.
    pub(crate) fn term_at(&self, code: u32) -> Option<String> {
        let i = code as usize;
        if i < self.len() {
            let buf = self.terms.bytes_at(i);
            std::str::from_utf8(buf.as_ref()).ok().map(str::to_owned)
        } else {
            None
        }
    }

    /// Materialize a temporary O(1) lookup table for bulk encoding.
    ///
    /// Query-side lookups remain binary searches over the compact dictionary,
    /// but a build performs four lookups per quad and benefits substantially
    /// from paying this allocation once per build.
    pub(crate) fn build_id_map(&self) -> TermIdMap {
        let start = Instant::now();
        let map = (0..self.len())
            .map(|id| {
                let term = self.terms.bytes_at(id);
                let term = std::str::from_utf8(term.as_ref())
                    .expect("term dictionary contains only valid UTF-8")
                    .to_owned();
                (term, id as u32)
            })
            .collect();
        log::debug!(
            "[Dictionary] Built temporary term-ID map for {} terms in {:?}",
            self.len(),
            start.elapsed()
        );
        map
    }

    /// Look up a term's ID: its position in the sorted dictionary.
    /// Uses Vortex's SearchSorted compute kernel via ArrayRef for optimized sorted search.
    /// Falls back to manual binary search only if the kernel fails.
    pub(crate) fn get_id(&self, term: &str) -> Option<u32> {
        let probe = Scalar::from(term);

        // Try the SearchSorted kernel first (optimized path for sorted columns)
        let arr_ref = self.terms.clone().into_array();
        if let Ok(result) = arr_ref.search_sorted(&probe, SearchSortedSide::Left) {
            // SearchSorted returns the position where term should be inserted
            // For an exact match, the value at that position must equal the probe
            let idx = match result {
                SearchResult::Found(i) | SearchResult::NotFound(i) => i,
            };
            if idx < self.len() && self.terms.bytes_at(idx).as_ref() == term.as_bytes() {
                return Some(idx as u32);
            }
            return None;
        }

        // Fallback: manual binary search (should not happen in normal operation)
        let needle = term.as_bytes();
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let buf = self.terms.bytes_at(mid);
            match buf.as_ref().cmp(needle) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Equal => return Some(mid as u32),
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

/// Build the `_dict_terms` column for a chunk of `n_rows` quads.
///
/// When `carry_payload` is set (the first chunk of a build), row 0 holds the
/// entire dictionary as one list; otherwise every row is an empty list. Either
/// way the column dtype is identical across chunks.
pub(crate) fn dict_column(
    dict: &TermDictionary,
    n_rows: usize,
    carry_payload: bool,
) -> Result<ArrayRef> {
    let start = Instant::now();
    let m = dict.len() as i32;
    let (elements, offsets): (ArrayRef, Vec<i32>) = if carry_payload && n_rows > 0 {
        (
            dict.view().clone().into_array(),
            std::iter::once(0)
                .chain(std::iter::repeat_n(m, n_rows))
                .collect(),
        )
    } else {
        (
            VarBinViewArray::from_iter_str(std::iter::empty::<&str>()).into_array(),
            vec![0; n_rows + 1],
        )
    };

    let column = ListArray::try_new(
        elements,
        PrimitiveArray::from_iter(offsets).into_array(),
        Validity::NonNullable,
    )
    .map(|a| a.into_array())
    .map_err(VortexRdfError::Vortex)?;
    log::debug!(
        "[Dictionary] Built dictionary payload column for {} rows ({} terms, carry_payload={}) in {:?}",
        n_rows,
        dict.len(),
        carry_payload,
        start.elapsed()
    );
    Ok(column)
}

/// Recover the dictionary from a complete `_dict_terms` column: by
/// construction only row 0 can be non-empty, so its list is the dictionary.
///
/// Must be called on the full column as built/written — derived (sliced or
/// filtered) arrays may have lost row 0; stores cache the dictionary at
/// construction instead of re-reading it from derived data.
pub(crate) fn dict_from_list_column(col: &ArrayRef) -> Result<TermDictionary> {
    if col.is_empty() {
        return Ok(TermDictionary::empty());
    }
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let list = col
        .clone()
        .execute::<ListViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let elements = list.list_elements_at(0).map_err(VortexRdfError::Vortex)?;
    let terms = elements
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(TermDictionary { terms })
}

/// Extract the term dictionary from an in-memory Dictionary-layout array.
///
/// Only row 0 carries the payload, so slice to a single row first — this keeps
/// the extraction cheap (no canonicalization of the full, possibly chunked,
/// array) and zero-copy into the existing buffers.
pub(crate) fn dict_from_array(array: &ArrayRef) -> Result<TermDictionary> {
    let head = if array.is_empty() {
        array.clone()
    } else {
        array.slice(0..1).map_err(VortexRdfError::Vortex)?
    };
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = head
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let col = struct_arr
        .unmasked_field_by_name(DICT_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    dict_from_list_column(&col)
}

/// Read the term dictionary from a Dictionary-layout file: a single-column
/// projection scan of `_dict_terms` (the quad columns are never touched).
#[cfg(feature = "file-io")]
pub(crate) async fn dict_from_file(file: &VortexFile) -> Result<TermDictionary> {
    let arr = file
        .scan()
        .map_err(VortexRdfError::Vortex)?
        .with_projection(select([DICT_FIELD], root()))
        .into_array_stream()
        .map_err(VortexRdfError::Vortex)?
        .read_all()
        .await
        .map_err(VortexRdfError::Vortex)?;
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let col = struct_arr
        .unmasked_field_by_name(DICT_FIELD)
        .map_err(VortexRdfError::Vortex)?
        .clone();
    dict_from_list_column(&col)
}
