//! The file-backed arm of the Dictionary layout's residency axis: a term
//! dictionary read in place from its serialized child. A file store is
//! memory-mapped, so reading a window is slicing its segment out of the map
//! and rebuilding the FSST array over those bytes; only the terms a call
//! asks for are decompressed, and nothing read is kept once the call
//! returns. The handle holds the child's layout (one flat leaf per FSST
//! window) and each window's first and last term — read at open from the
//! child's exact zone maps, or from the leaves of a child written without
//! them (vortex-rdf 0.11 and earlier). A child whose shape a window search
//! cannot address is not file-backed at all: [`store::open`](crate::store::open)
//! lifts it resident instead. The policy enum choosing between this and the
//! resident form is [`DictAccess`](super::access::DictAccess); the module
//! only compiles with `file-io`, since without a file there is nothing to
//! leave the terms in.

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use futures::{StreamExt as _, TryStreamExt as _};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::serde::SerializedArray;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};

use crate::common::terms::canonical_spelling;
use crate::error::{Result, VortexRdfError};
use crate::io::container::DICT_COMPONENT_NAME;
use crate::io::read::available_parallelism;
use crate::session::VORTEX_SESSION;
use crate::store::array::StrColReader;
use crate::store::native_file::NativeStoreFile;

use super::check_code;
use super::predicates::{KindRanges, Scanned, TermPredicate};
use super::term_dict::{
    COL_DICT_TERM, TermDictionary, VerdictSets, chunk_of, prefix_successor, window_bound_aggregates,
};

/// A window's first and last term.
type Bounds = (Box<[u8]>, Box<[u8]>);

/// The term column's flat leaves, each with its row count.
type Leaves = Vec<(LayoutRef, usize)>;

/// One flat leaf of the term column — one FSST window as written — with the
/// code of its first term, its row count, and its first and last terms.
struct Window {
    layout: LayoutRef,
    start: usize,
    rows: usize,
    first: Box<[u8]>,
    last: Box<[u8]>,
}

/// What a [`FileBackedDict`] holds: the child's layout metadata and the
/// per-window bounds — never a term read for a call.
struct Windows {
    /// The dictionary child's layout reader, for the transient whole-column
    /// lift (serialization, compaction).
    reader: vortex_layout::LayoutReaderRef,
    windows: Vec<Window>,
    /// `starts[i]` = `windows[i].start`, ascending (for [`chunk_of`]).
    starts: Vec<usize>,
    /// Number of terms.
    len: usize,
    /// Where the leaves' segments are read from — over a mapped file, the map.
    source: Arc<dyn SegmentSource>,
    /// The kind ranges (three code ranges and the default graph's code),
    /// computed on first use.
    kinds: OnceLock<KindRanges>,
    /// Windows rebuilt since open — what the tests read to pin that a call
    /// touches only the windows holding its codes.
    #[cfg(test)]
    rebuilt: std::sync::atomic::AtomicUsize,
}

impl Windows {
    /// Window `w`'s leaf, rebuilt over its segment.
    async fn window_array(&self, w: usize) -> Result<ArrayRef> {
        #[cfg(test)]
        self.rebuilt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let window = &self.windows[w];
        rebuild_leaf(&window.layout, window.rows, &self.source).await
    }
}

/// A term dictionary left in its layout child, with no term held: term →
/// code probes and code → term decodes read, per call, the windows they
/// need from the (mapped) file. Cloning is an `Arc` bump.
#[derive(Clone)]
pub(crate) struct FileBackedDict(Arc<Windows>);

impl FileBackedDict {
    /// The file-backed form of `native`'s dictionary child: its reader, its
    /// leaves, and each window's bounds (from the exact zone maps, else from
    /// the leaves). `None` when the file has no dictionary component, or the
    /// child's shape cannot be searched by window (an empty child, a single
    /// flat struct leaf, unsorted bounds): the caller lifts it resident.
    pub(crate) async fn open(native: &NativeStoreFile) -> Result<Option<Self>> {
        let Some((_, reader)) = native
            .component_reader(DICT_COMPONENT_NAME)
            .map_err(VortexRdfError::Vortex)?
        else {
            return Ok(None);
        };
        let Some(child) = native
            .component_layout(DICT_COMPONENT_NAME)
            .map_err(VortexRdfError::Vortex)?
        else {
            return Ok(None);
        };
        let Some((leaves, zones)) = term_column(&child) else {
            return Ok(None);
        };
        let source = native.segment_source();
        let zoned = match &zones {
            Some(zones) => zone_bounds(zones, leaves.len(), &source).await?,
            None => None,
        };
        let bounds = match zoned {
            Some(bounds) => bounds,
            None => leaf_bounds(&leaves, &source).await?,
        };
        if !ordered(&bounds) {
            return Ok(None);
        }
        let mut windows = Vec::with_capacity(leaves.len());
        let mut starts = Vec::with_capacity(leaves.len());
        let mut start = 0usize;
        for ((layout, rows), (first, last)) in leaves.into_iter().zip(bounds) {
            starts.push(start);
            windows.push(Window {
                layout,
                start,
                rows,
                first,
                last,
            });
            start += rows;
        }
        Ok(Some(Self(Arc::new(Windows {
            reader,
            windows,
            starts,
            len: start,
            source,
            kinds: OnceLock::new(),
            #[cfg(test)]
            rebuilt: std::sync::atomic::AtomicUsize::new(0),
        }))))
    }

    /// Number of terms.
    pub(crate) fn len(&self) -> usize {
        self.0.len
    }

    /// Term → code: the window whose bounds enclose `term`, then a binary
    /// search of that one window — none at all for a window's first or last
    /// term, or a term falling between two windows.
    pub(crate) async fn encode(&self, term: &str) -> Result<Option<u32>> {
        let inner = &*self.0;
        let needle = term.as_bytes();
        let w = inner
            .windows
            .partition_point(|window| &*window.last < needle);
        let Some(window) = inner.windows.get(w) else {
            return Ok(None);
        };
        match needle.cmp(&*window.first) {
            Ordering::Less => return Ok(None),
            Ordering::Equal => return Ok(Some(window.start as u32)),
            Ordering::Greater => {}
        }
        if needle == &*window.last {
            return Ok(Some((window.start + window.rows - 1) as u32));
        }
        let array = inner.window_array(w).await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let (mut lo, mut hi) = (1, window.rows - 1);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match term_cmp(&array, mid, needle, &mut ctx)? {
                Ordering::Less => lo = mid + 1,
                Ordering::Equal => return Ok(Some((window.start + mid) as u32)),
                Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// The code of the first term not below `needle` in byte order (the
    /// term count when every term is below it).
    pub(crate) async fn lower_bound(&self, needle: &[u8]) -> Result<u32> {
        let inner = &*self.0;
        let w = inner
            .windows
            .partition_point(|window| &*window.last < needle);
        let Some(window) = inner.windows.get(w) else {
            return Ok(inner.len as u32);
        };
        if needle <= &*window.first {
            return Ok(window.start as u32);
        }
        // first < needle <= last: the bound lies inside, past the first term.
        let array = inner.window_array(w).await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let (mut lo, mut hi) = (1, window.rows - 1);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if term_cmp(&array, mid, needle, &mut ctx)? == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok((window.start + lo) as u32)
    }

    /// Visit the terms of `codes` (ascending and in range — the caller's
    /// contract) in order: per window holding any of them, the leaf is
    /// rebuilt over its segment and only the asked rows are taken out of the
    /// FSST array and decompressed. `visit` gets each code's position in
    /// `codes` and its term (an error for a term that is not UTF-8).
    async fn visit_terms(
        &self,
        codes: &[u32],
        mut visit: impl FnMut(usize, Result<&str>),
    ) -> Result<()> {
        let inner = &*self.0;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let mut at = 0;
        while at < codes.len() {
            let (w, _) = chunk_of(&inner.starts, codes[at] as usize);
            let window = &inner.windows[w];
            let end = window.start + window.rows;
            let run = codes[at..].partition_point(|&code| (code as usize) < end);
            let locals = PrimitiveArray::from_iter(
                codes[at..at + run]
                    .iter()
                    .map(|&code| code - window.start as u32),
            )
            .into_array();
            let taken = inner
                .window_array(w)
                .await?
                .take(locals)
                .map_err(VortexRdfError::Vortex)?
                .execute::<VarBinViewArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;
            let reader = StrColReader::new(&taken);
            for i in 0..run {
                visit(at + i, reader.str_at(i));
            }
            at += run;
        }
        Ok(())
    }

    /// Code → term for `codes` (ascending, unique; out-of-range codes are an
    /// error), reading only the windows that hold them.
    pub(crate) async fn decode_many(&self, codes: &[u32]) -> Result<Vec<Arc<str>>> {
        let Some(&max) = codes.last() else {
            return Ok(Vec::new());
        };
        check_code(max, self.len())?;
        let mut terms = Vec::with_capacity(codes.len());
        let mut failure = None;
        self.visit_terms(codes, |_, term| match term {
            Ok(term) => terms.push(Arc::from(term)),
            Err(e) => {
                failure.get_or_insert(e);
            }
        })
        .await?;
        match failure {
            Some(e) => Err(e),
            None => Ok(terms),
        }
    }

    /// Code → term for `codes` in any order, repeats allowed, out-of-range
    /// codes decoding to `None`: the distinct in-range codes are read once.
    pub(crate) async fn decode_many_any(&self, codes: &[u32]) -> Result<Vec<Option<String>>> {
        let len = self.len();
        let mut distinct: Vec<u32> = codes
            .iter()
            .copied()
            .filter(|&code| (code as usize) < len)
            .collect();
        distinct.sort_unstable();
        distinct.dedup();
        let terms = self.decode_many(&distinct).await?;
        Ok(codes
            .iter()
            .map(|code| {
                distinct
                    .binary_search(code)
                    .ok()
                    .map(|i| terms[i].to_string())
            })
            .collect())
    }

    /// The async twin of `TermDictionary::encode_tolerant`.
    pub(crate) async fn encode_tolerant(&self, term: &str) -> Result<Option<u32>> {
        if let Some(code) = self.encode(term).await? {
            return Ok(Some(code));
        }
        let canonical = canonical_spelling(term)?;
        if canonical == term {
            return Ok(None);
        }
        self.encode(&canonical).await
    }

    /// [`encode_tolerant`](Self::encode_tolerant) over a batch, in order.
    pub(crate) async fn encode_many(&self, terms: &[&str]) -> Result<Vec<Option<u32>>> {
        futures::stream::iter(terms.iter().map(|term| self.encode_tolerant(term)))
            .buffered(available_parallelism().max(4))
            .try_collect()
            .await
    }

    /// The async twin of `TermDictionary::prefix_range`.
    pub(crate) async fn prefix_range(&self, prefix: &str) -> Result<Range<u32>> {
        let lo = self.lower_bound(prefix.as_bytes()).await?;
        let hi = match prefix_successor(prefix.as_bytes()) {
            Some(successor) => self.lower_bound(&successor).await?,
            None => self.len() as u32,
        };
        Ok(lo..hi.max(lo))
    }

    /// The kind ranges, computed once per handle (a few window searches;
    /// the default graph's `""` is window 0's first bound).
    pub(crate) async fn kind_ranges(&self) -> Result<KindRanges> {
        if let Some(kinds) = self.0.kinds.get() {
            return Ok(kinds.clone());
        }
        let default_graph = self
            .0
            .windows
            .first()
            .filter(|window| window.first.is_empty())
            .map(|_| 0);
        let kinds = KindRanges {
            default_graph,
            literals: self.prefix_range("\"").await?,
            iris: self.prefix_range("<").await?,
            blanks: self.prefix_range("_:").await?,
            len: self.len() as u32,
        };
        Ok(self.0.kinds.get_or_init(|| kinds).clone())
    }

    /// The dictionary-wide partition — `TermDictionary::filter_codes`'s
    /// file-backed twin — read window by window, nothing memoized.
    pub(crate) async fn filter_codes(&self, predicate: &TermPredicate) -> Result<VerdictSets> {
        let kinds = self.kind_ranges().await?;
        let plan = predicate.scan_plan(&kinds);
        let mut scanned = Scanned::default();
        if let Some(range) = plan.scan.filter(|range| !range.is_empty()) {
            let codes: Vec<u32> = range.collect();
            self.visit_terms(&codes, |i, term| match term {
                Ok(spelling) => scanned.visit(predicate, codes[i], spelling),
                Err(_) => scanned.unknown.push(codes[i]),
            })
            .await?;
        }
        let mut true_ranges = Vec::with_capacity(plan.true_prefixes.len());
        for prefix in &plan.true_prefixes {
            true_ranges.push(self.prefix_range(prefix).await?);
        }
        Ok(Arc::new(predicate.assemble(&kinds, scanned, &true_ranges)))
    }

    /// Lift the whole dictionary resident — the transient full-column read
    /// behind [`DictAccess::ensure_resident`].
    ///
    /// [`DictAccess::ensure_resident`]: super::access::DictAccess::ensure_resident
    pub(crate) async fn lift_resident(&self) -> Result<TermDictionary> {
        TermDictionary::from_child_reader(self.0.reader.clone()).await
    }

    /// Windows rebuilt from their segments since the handle opened.
    #[cfg(test)]
    pub(crate) fn debug_windows_rebuilt(&self) -> usize {
        self.0.rebuilt.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The number of windows (leaves) of the term column.
    #[cfg(test)]
    pub(crate) fn debug_window_count(&self) -> usize {
        self.0.windows.len()
    }
}

/// The term column's flat leaves — one per FSST window, in row order, with
/// their row counts — and, when the column is zone-mapped with exact window
/// bounds, its zones child. `None` for any shape a window search cannot
/// address.
fn term_column(dict: &LayoutRef) -> Option<(Leaves, Option<LayoutRef>)> {
    dict.as_opt::<StructLayout>()?;
    let column = (0..dict.nslots()).find_map(|i| {
        matches!(dict.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
            .then(|| dict.slot(i).ok().flatten())
            .flatten()
    })?;
    let zones = match column.as_opt::<Zoned>() {
        Some(zoned) if exact_bounds(&zoned.present_aggregates()) => column.slot(1).ok().flatten(),
        _ => None,
    };
    let data = unwrap_zoned(column)?;
    let rows = data.row_count();
    // Codes are u32; an empty child has nothing to search.
    if rows == 0 || rows > u64::from(u32::MAX) {
        return None;
    }
    let mut leaves = Vec::new();
    if data.is::<Flat>() {
        leaves.push((data, rows as usize));
    } else if data.is::<ChunkedLayout>() {
        for i in 0..data.nslots() {
            let Some(LayoutChildType::Chunk(_)) = data.slot_type(i) else {
                return None;
            };
            let leaf = unwrap_zoned(data.slot(i).ok().flatten()?)?;
            let rows = leaf.row_count();
            if rows == 0 {
                continue;
            }
            if !leaf.is::<Flat>() {
                return None;
            }
            leaves.push((leaf, rows as usize));
        }
    } else {
        return None;
    }
    Some((leaves, zones))
}

/// Whether a zone map records this crate's exact window bounds.
fn exact_bounds(present: &[String]) -> bool {
    window_bound_aggregates()
        .iter()
        .all(|aggregate| present.contains(&aggregate.to_string()))
}

/// Descend through zoned wrappers to their data child (child 0).
fn unwrap_zoned(mut node: LayoutRef) -> Option<LayoutRef> {
    while node.is::<Zoned>() {
        node = node.slot(0).ok().flatten()?;
    }
    Some(node)
}

/// Each window's bounds from the term column's zone table — one zone per
/// window — or `None` when the table does not line up with the leaves (a
/// zone count other than the window count, or a null bound).
async fn zone_bounds(
    zones: &LayoutRef,
    windows: usize,
    source: &Arc<dyn SegmentSource>,
) -> Result<Option<Vec<Bounds>>> {
    if zones.row_count() != windows as u64 {
        return Ok(None);
    }
    let reader = zones
        .new_reader(
            "dictionary-zones".into(),
            Arc::clone(source),
            &VORTEX_SESSION,
            &Default::default(),
        )
        .map_err(VortexRdfError::Vortex)?;
    let table = crate::io::read::scan_all_reader(reader).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let table = table
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let aggregates = window_bound_aggregates();
    let Some(maxes) = bound_column(&table, &aggregates[0].to_string(), &mut ctx)? else {
        return Ok(None);
    };
    let Some(mins) = bound_column(&table, &aggregates[1].to_string(), &mut ctx)? else {
        return Ok(None);
    };
    let (maxes, mins) = (StrColReader::new(&maxes), StrColReader::new(&mins));
    Ok(Some(
        (0..windows)
            .map(|w| (Box::from(mins.bytes_at(w)), Box::from(maxes.bytes_at(w))))
            .collect(),
    ))
}

/// One bound column of the zone table, `None` when absent or holding a null.
fn bound_column(
    table: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Result<Option<VarBinViewArray>> {
    let Ok(column) = table.unmasked_field_by_name(name) else {
        return Ok(None);
    };
    if !column.all_valid(ctx).map_err(VortexRdfError::Vortex)? {
        return Ok(None);
    }
    Ok(Some(
        column
            .clone()
            .execute::<VarBinViewArray>(ctx)
            .map_err(VortexRdfError::Vortex)?,
    ))
}

/// Each window's bounds read from its own leaf — the fallback for a term
/// column without exact zone maps (vortex-rdf 0.11 and earlier, or a foreign
/// writer): two single-term reads per window, at open.
async fn leaf_bounds(
    leaves: &[(LayoutRef, usize)],
    source: &Arc<dyn SegmentSource>,
) -> Result<Vec<Bounds>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let mut bounds = Vec::with_capacity(leaves.len());
    for (layout, rows) in leaves {
        let array = rebuild_leaf(layout, *rows, source).await?;
        bounds.push((
            term_bytes(&array, 0, &mut ctx)?,
            term_bytes(&array, rows - 1, &mut ctx)?,
        ));
    }
    Ok(bounds)
}

/// Whether window bounds are sorted and disjoint — `first <= last` within a
/// window, `last < next first` across — what the window search relies on.
fn ordered(bounds: &[Bounds]) -> bool {
    bounds.iter().all(|(first, last)| first <= last)
        && bounds.windows(2).all(|pair| pair[0].1 < pair[1].0)
}

/// A flat leaf rebuilt over its segment in the encoding it was written in —
/// over a mapped file, metadata over a slice of the map; nothing is
/// decompressed.
async fn rebuild_leaf(
    layout: &LayoutRef,
    rows: usize,
    source: &Arc<dyn SegmentSource>,
) -> Result<ArrayRef> {
    let flat = layout
        .as_opt::<Flat>()
        .expect("term leaves are validated flat at open");
    let segment = source
        .request(flat.segment_id())
        .await
        .map_err(VortexRdfError::Vortex)?;
    let parts = match flat.array_tree().cloned() {
        Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree, segment),
        None => SerializedArray::try_from(segment),
    }
    .map_err(VortexRdfError::Vortex)?;
    parts
        .decode(flat.dtype(), rows, flat.array_ctx(), &VORTEX_SESSION)
        .map_err(VortexRdfError::Vortex)
}

/// Term `local` of a rebuilt leaf compared with `needle`, decompressing that
/// one term.
fn term_cmp(
    array: &ArrayRef,
    local: usize,
    needle: &[u8],
    ctx: &mut ExecutionCtx,
) -> Result<Ordering> {
    let scalar = array
        .execute_scalar(local, ctx)
        .map_err(VortexRdfError::Vortex)?;
    let utf8 = scalar.as_utf8();
    let term = utf8
        .value()
        .map_or(&b""[..], |term| term.as_str().as_bytes());
    Ok(term.cmp(needle))
}

/// Term `local` of a rebuilt leaf, owned.
fn term_bytes(array: &ArrayRef, local: usize, ctx: &mut ExecutionCtx) -> Result<Box<[u8]>> {
    let scalar = array
        .execute_scalar(local, ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(scalar
        .as_utf8()
        .value()
        .map(|term| Box::from(term.as_str().as_bytes()))
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_buffer::ByteBuffer;
    use vortex_file::OpenOptionsSessionExt as _;

    /// `n` IRIs compressed in windows of `window` terms, written as a store's
    /// dictionary child and opened file-backed over the written bytes.
    async fn windowed_handle(n: usize, window: usize) -> (FileBackedDict, Vec<String>) {
        let terms: Vec<String> = (0..n)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, window).unwrap();
        let bytes = crate::tests::write_dict_only_store(&d).await;
        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = NativeStoreFile::try_new(file).unwrap();
        let fbd = FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("the written dictionary child must be window-searchable");
        (fbd, terms)
    }

    /// The compression windows survive as the child's leaves — one per window,
    /// none merged or re-cut — and every term resolves both ways.
    #[tokio::test]
    async fn windowed_dict_child_chunk_leaves() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        assert_eq!(fbd.debug_window_count(), 6);
        assert_eq!(fbd.len(), 600);
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(fbd.encode(term).await.unwrap(), Some(i as u32), "{term}");
        }
        let codes: Vec<u32> = (0..600).collect();
        let decoded = fbd.decode_many(&codes).await.unwrap();
        assert!(
            decoded
                .iter()
                .zip(&terms)
                .all(|(got, want)| &**got == want.as_str())
        );
        assert_eq!(fbd.encode("<http://zzz>").await.unwrap(), None);
    }

    /// A window's first and last term are answered from the bounds alone, an
    /// interior term rebuilds exactly one window, and a term between two
    /// windows, below the first or past the last reads no leaf at all.
    #[tokio::test]
    async fn window_search_reads_one_window_at_most() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        for w in 0..6 {
            let before = fbd.debug_windows_rebuilt();
            assert_eq!(
                fbd.encode(&terms[w * 100]).await.unwrap(),
                Some((w * 100) as u32)
            );
            assert_eq!(
                fbd.encode(&terms[w * 100 + 99]).await.unwrap(),
                Some((w * 100 + 99) as u32)
            );
            assert_eq!(fbd.debug_windows_rebuilt(), before, "window {w}'s edges");
            assert_eq!(
                fbd.encode(&terms[w * 100 + 37]).await.unwrap(),
                Some((w * 100 + 37) as u32)
            );
            assert_eq!(
                fbd.debug_windows_rebuilt(),
                before + 1,
                "window {w}'s interior"
            );
        }
        let before = fbd.debug_windows_rebuilt();
        let between = format!("{}~", terms[99]);
        assert_eq!(fbd.encode(&between).await.unwrap(), None);
        assert_eq!(fbd.lower_bound(between.as_bytes()).await.unwrap(), 100);
        assert_eq!(fbd.encode("!").await.unwrap(), None);
        assert_eq!(fbd.lower_bound(b"!").await.unwrap(), 0);
        assert_eq!(fbd.encode("~").await.unwrap(), None);
        assert_eq!(fbd.lower_bound(b"~").await.unwrap(), 600);
        assert_eq!(fbd.debug_windows_rebuilt(), before);
        let inside = format!("{}~", terms[150]);
        assert_eq!(fbd.lower_bound(inside.as_bytes()).await.unwrap(), 151);
    }

    /// A decode rebuilds only the windows holding its codes.
    #[tokio::test]
    async fn decode_rebuilds_only_the_windows_holding_codes() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        let codes = [105u32, 150, 199, 401, 450];
        let before = fbd.debug_windows_rebuilt();
        let got = fbd.decode_many(&codes).await.unwrap();
        assert_eq!(fbd.debug_windows_rebuilt(), before + 2);
        for (code, term) in codes.iter().zip(&got) {
            assert_eq!(&**term, terms[*code as usize].as_str());
        }
        assert!(matches!(
            fbd.decode_many(&[600]).await,
            Err(VortexRdfError::Deserialization(_))
        ));
    }

    /// The dictionary child's term column carries exact window bounds: one
    /// zone per FSST window whose `vortex.min()`/`vortex.max()` are the
    /// window's first and last terms; the component is stamped version 2.
    /// The terms share a 100-byte prefix, so bounds cut anywhere inside it
    /// could not tell the windows apart.
    #[tokio::test]
    async fn dict_child_zone_maps_hold_window_bounds() {
        let prefix = format!("<http://example.org/{}", "p".repeat(80));
        assert_eq!(prefix.len(), 100);
        let terms: Vec<String> = (0..600).map(|i| format!("{prefix}/term/{i:04}>")).collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
        let descriptor = native
            .components()
            .iter()
            .find(|c| c.name == DICT_COMPONENT_NAME)
            .unwrap();
        assert_eq!(descriptor.version, 2);

        let column = crate::tests::dict_term_column(&native);
        let zoned = column
            .as_opt::<Zoned>()
            .expect("the term column is zone-mapped");
        assert_eq!(zoned.nzones(), 6);
        assert_eq!(zoned.zone_len(), 100);
        // The zone table holds the maximum, then the minimum.
        let names: Vec<String> = window_bound_aggregates()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(names, ["vortex.max()", "vortex.min()"]);
        for name in ["vortex.max()", "vortex.min()"] {
            assert!(
                zoned.present_aggregates().contains(&name.to_string()),
                "{name}"
            );
        }
        let zones = column.slot(1).unwrap().unwrap();
        let reader = zones
            .new_reader(
                "zones".into(),
                native.segment_source(),
                &VORTEX_SESSION,
                &Default::default(),
            )
            .unwrap();
        let table = crate::io::read::scan_all_reader(reader).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let table = table.execute::<StructArray>(&mut ctx).unwrap();
        let maxes = table
            .unmasked_field_by_name("vortex.max()")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        let mins = table
            .unmasked_field_by_name("vortex.min()")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        for w in 0..6 {
            assert_eq!(StrColReader::new(&mins).str_at(w).unwrap(), terms[w * 100]);
            assert_eq!(
                StrColReader::new(&maxes).str_at(w).unwrap(),
                terms[w * 100 + 99]
            );
        }
    }

    /// Terms far past any string-bound truncation length still resolve through
    /// the zone maps: the table holds each window's first and last term
    /// whole, so every term — a window's first and last included — finds its
    /// own code, and those edge terms are answered from the bounds alone.
    #[tokio::test]
    async fn zone_bounds_of_long_terms_are_exact() {
        // 326-byte terms sharing a 321-byte prefix: they differ only in the
        // last five bytes.
        let terms: Vec<String> = (0..400)
            .map(|i| format!("<http://example.org/{}/{i:04}>", "x".repeat(300)))
            .collect();
        assert!(terms.iter().all(|term| term.len() > 256));
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
        assert!(crate::tests::dict_term_column(&native).is::<Zoned>());

        // The zone table itself — not the leaves — holds the whole terms.
        let child = native
            .component_layout(DICT_COMPONENT_NAME)
            .unwrap()
            .unwrap();
        let (leaves, zones) = term_column(&child).expect("a zoned, chunked term column");
        let zones = zones.expect("the zone maps record exact window bounds");
        let bounds = zone_bounds(&zones, leaves.len(), &native.segment_source())
            .await
            .unwrap()
            .expect("one zone per window");
        assert_eq!(bounds.len(), 4);
        for (w, (first, last)) in bounds.iter().enumerate() {
            let (first, last) = (std::str::from_utf8(first), std::str::from_utf8(last));
            assert_eq!(first.unwrap(), terms[w * 100], "window {w}'s first");
            assert_eq!(last.unwrap(), terms[w * 100 + 99], "window {w}'s last");
        }

        let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
        assert_eq!(fbd.debug_window_count(), 4);
        let before = fbd.debug_windows_rebuilt();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(fbd.encode(term).await.unwrap(), Some(i as u32), "{i}");
        }
        // Eight edge terms (a window's first and last) read no leaf; each of
        // the others rebuilt exactly one window.
        assert_eq!(fbd.debug_windows_rebuilt() - before, 400 - 8);
        // A near miss — the last byte changed — is absent, not a neighbour.
        let near = format!("{}?", &terms[150][..terms[150].len() - 1]);
        assert_eq!(fbd.encode(&near).await.unwrap(), None);
        assert_eq!(fbd.lower_bound(near.as_bytes()).await.unwrap(), 151);
    }

    /// A version-1 child (what vortex-rdf 0.11 wrote: no zone maps) with many
    /// windows — the real shape of an old file — opens mapped with every
    /// window's bounds read from its own leaf, and answers like the plain
    /// column in both directions.
    #[tokio::test]
    async fn v1_child_with_many_windows_reads_every_bound_from_leaves() {
        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain.clone(), 100).unwrap();
        let bytes = crate::tests::write_v1_dict_only_store(&d).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1-windows.vortex");
        std::fs::write(&path, &bytes).unwrap();
        let native = NativeStoreFile::try_new(
            crate::io::read::open_vortex_file(&path, crate::io::read::FileAccess::Mapped)
                .await
                .unwrap(),
        )
        .unwrap();
        let descriptor = native
            .components()
            .iter()
            .find(|c| c.name == DICT_COMPONENT_NAME)
            .unwrap();
        assert_eq!(descriptor.version, 1);
        assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());

        let fbd = FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("a chunked child is window-searchable");
        assert_eq!(fbd.debug_window_count(), 6);
        assert_eq!(fbd.len(), 600);
        // Every window's bounds are its first and last term, read from the leaf.
        for (w, window) in fbd.0.windows.iter().enumerate() {
            assert_eq!((window.start, window.rows), (w * 100, 100), "window {w}");
            let (first, last) = (
                std::str::from_utf8(&window.first),
                std::str::from_utf8(&window.last),
            );
            assert_eq!(first.unwrap(), terms[w * 100], "window {w}'s first");
            assert_eq!(last.unwrap(), terms[w * 100 + 99], "window {w}'s last");
        }

        // Encode and decode parity with the plain column, term by term.
        let column = StrColReader::new(&plain);
        let before = fbd.debug_windows_rebuilt();
        for code in 0..600 {
            let term = column.str_at(code).unwrap();
            assert_eq!(fbd.encode(term).await.unwrap(), Some(code as u32), "{term}");
        }
        // The edge terms were answered from the bounds read at open.
        assert_eq!(fbd.debug_windows_rebuilt() - before, 600 - 12);
        let codes: Vec<u32> = (0..600).collect();
        let decoded = fbd.decode_many(&codes).await.unwrap();
        for (code, term) in decoded.iter().enumerate() {
            assert_eq!(&**term, column.str_at(code).unwrap(), "code {code}");
        }
        assert_eq!(fbd.encode("<http://zzz>").await.unwrap(), None);
        assert_eq!(
            fbd.encode("<http://example.org/term/0100>~").await.unwrap(),
            None
        );
    }

    /// Searches that cross windows — lower bounds, prefix ranges, the kind
    /// ranges and dictionary-wide partitions — answer exactly like the
    /// resident dictionary they were written from, over terms of every kind:
    /// window boundaries fall inside the literal and IRI runs, and each change
    /// of kind falls inside a window.
    #[tokio::test]
    async fn window_searches_match_the_resident_dictionary() {
        let mut terms: Vec<String> = vec![String::new()];
        terms.extend((0..150).map(|i| format!("\"lit {i:03}\"")));
        terms.extend((0..30).map(|i| format!("\"lit {i:03}\"@en")));
        terms.extend(
            (0..30).map(|i| format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>", i * 10)),
        );
        terms.extend((0..300).map(|i| format!("<http://example.org/iri/{i:04}>")));
        terms.extend((0..80).map(|i| format!("_:b{i:03}")));
        terms.sort();
        terms.dedup();
        assert_eq!(terms.len(), 591);
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
        let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
        assert_eq!(fbd.debug_window_count(), 6);

        assert_eq!(fbd.kind_ranges().await.unwrap(), *d.kind_ranges());
        let mut probes: Vec<String> = [
            "",
            "\"",
            "\"lit 1",
            "<",
            "<http://example.org/iri/00",
            "_:",
            "_:b0",
            "~",
        ]
        .map(String::from)
        .into();
        for term in &terms {
            probes.push(term.clone());
            probes.push(format!("{term} "));
            probes.push(term.strip_suffix('>').unwrap_or(term).to_owned());
        }
        for probe in &probes {
            assert_eq!(
                fbd.lower_bound(probe.as_bytes()).await.unwrap(),
                d.lower_bound(probe.as_bytes()),
                "lower_bound {probe:?}"
            );
            assert_eq!(
                fbd.prefix_range(probe).await.unwrap(),
                d.prefix_range(probe),
                "prefix_range {probe:?}"
            );
        }
        for (kind, arg) in [
            ("is_literal", ""),
            ("is_iri", ""),
            ("is_blank", ""),
            ("str_prefix", "lit 1"),
            ("lang", "en"),
            ("datatype", "http://www.w3.org/2001/XMLSchema#integer"),
            ("num_lt", "95"),
        ] {
            let predicate = TermPredicate::parse(kind, arg).unwrap();
            assert_eq!(
                *fbd.filter_codes(&predicate).await.unwrap(),
                *d.filter_codes(&predicate),
                "filter_codes {kind} {arg:?}"
            );
        }
    }
}
