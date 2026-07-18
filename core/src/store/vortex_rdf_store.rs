use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::common::utils::{
    bool_array_to_mask,
    column_is_sorted,
    search_sorted_bounds
};
#[cfg(feature = "file-io")]
use crate::common::utils::graph_name_str;
#[cfg(feature = "file-io")]
use vortex_array::arrays::PrimitiveArray;
use crate::store::builders::{
    VortexArrayBuilder,
    UnsortedStreamBuilder,
    build_struct_array,
    DEFAULT_CHUNK_SIZE
};
use crate::store::RawQuad;
use crate::store::layouts::{dictionary, Constraints, LayoutStrategy, ResolvedLayout};
use crate::store::layouts::term_dictionary::{self, TermDictionary};
use crate::store::indexes::{
    Indexes,
    IndexType,
    detect_indexes,
    resolve_indexes_in_memory,
    strip_index_columns,
    unique_indexes,
    IndexResolution
};
#[cfg(feature = "file-io")]
use crate::store::indexes::resolve_indexes_file;
#[cfg(feature = "file-io")]
use crate::store::indexes::secondary_by_copy;
use crate::store::selection::RowSelection;
use crate::store::{QuadsSource, Tail};
#[cfg(feature = "file-io")]
use crate::store::CopyScan;

#[cfg(feature = "file-io")]
use crate::io::de;
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

use web_time::Instant;
use std::collections::HashSet;
use std::io::Cursor;
#[cfg(feature = "file-io")]
use std::ops::Range;
use std::sync::Arc;
use futures::{Stream, stream};
use oxrdf::{GraphName, NamedNode, Quad, NamedOrBlankNode, Term};

use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::arrays::{ChunkedArray, StructArray};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::constant::ConstantArray;
use vortex_array::{ArrayRef, IntoArray, RecursiveCanonical, VortexSessionExecute};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_mask::Mask;

#[cfg(feature = "file-io")]
use vortex_array::expr::{
    Expression,
    root,
    select,
    and,
    eq,
    get_item,
    lit
};
#[cfg(feature = "file-io")]
use vortex_array::expr::forms::conjuncts;
#[cfg(feature = "file-io")]
use vortex_array::MaskFuture;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_scan::selection::Selection;
#[cfg(feature = "file-io")]
use vortex_layout::LayoutReader;
#[cfg(feature = "file-io")]
use std::ops::BitAnd;
#[cfg(feature = "file-io")]
use futures::StreamExt;

/// Columnar RDF quad storage backed by Vortex.
///
/// Stores quad terms according to the chosen layout strategy.
/// And applies indexes as chosen at build time.
#[derive(Clone)]
pub struct VortexRdfStore {
    /// The store's backing quad data, either an in-memory array or a lazily
    /// scanned Vortex file, together with the row selection, filters, and
    /// tombstones that define the rows visible through this store or view.
    quads: QuadsSource,
    /// The layout resolved against the backing array, carrying any state
    /// intrinsic to it (the Dictionary layout's term dictionary is loaded
    /// once at construction and propagated to derived stores, which may have
    /// lost the payload row through slicing/filtering).
    layout: ResolvedLayout,
    /// The secondary indexes whose columns this store's base schema carries,
    /// detected at construction (`detect_indexes`). Pattern matching plans
    /// index lookups against this set.
    ///
    /// Views derived through `match_pattern` keep their indexes: a view narrows
    /// a [`RowSelection`] over the base rather than rewriting rows, so the
    /// `_idx_*_rid` columns still address the base the ids were built against.
    /// Only [`materialize`] — which physically gathers rows and so destroys row
    /// identity — drops them.
    ///
    /// [`materialize`]: Self::materialize
    indexes: Indexes,
    /// Rows appended since construction ([`add_quads`]), kept outside the base
    /// so appending never rewrites it — which is what lets the base's indexes
    /// and tombstones survive an append. `None` until something is appended.
    /// Queries run the base's fast paths plus a mask scan over the tail and
    /// union the two; [`compact_with_indexes`] folds the tail back in.
    ///
    /// [`add_quads`]: Self::add_quads
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    tail: Option<Tail>,
}

impl VortexRdfStore {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Build from an existing Vortex StructArray; auto-detects layout from field names.
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        // Inspect the struct's field names (no data materialization needed)
        // to figure out which of the three column layouts this array uses.
        let layout = match LayoutStrategy::from_dtype(vortex_array.dtype()) {
            LayoutStrategy::Default => ResolvedLayout::Default,
            LayoutStrategy::TypedObject => ResolvedLayout::TypedObject,
            LayoutStrategy::Dictionary => {
                // Dictionary layout stores terms as codes; eagerly pull the
                // term dictionary out of row 0 so later queries can translate
                // RDF terms to codes before comparing.
                ResolvedLayout::Dictionary(
                    Arc::new(term_dictionary::dict_from_array(&vortex_array)?)
                )
            }
        };
        // Discover which secondary indexes the schema carries, so pattern
        // matching knows what lookups it can plan.
        let indexes = detect_indexes(vortex_array.dtype());
        // An unrefined view over the whole array.
        Ok(Self {
            layout,
            indexes,
            quads: QuadsSource::InMemory {
                base: vortex_array,
                selection: RowSelection::All,
                deleted: None,
            },
            tail: None,
        })
    }

    /// Create an empty in-memory store with Default layout.
    pub fn empty() -> Self {
        use vortex_array::arrays::VarBinViewArray;
        // Build one empty string column and reuse it for all four fields —
        // they're all zero-length anyway, so there's nothing to distinguish.
        let e = VarBinViewArray::from_iter_str(std::iter::empty::<&str>()).into_array();

        let quads = StructArray::try_new(
            FieldNames::from(["s", "p", "o", "g"]),
            vec![e.clone(), e.clone(), e.clone(), e],
            0,
            Validity::NonNullable,
        )
        .expect("empty StructArray")
        .into_array();

        Self {
            layout: ResolvedLayout::Default,
            indexes: vec![],
            quads: QuadsSource::InMemory {
                base: quads,
                selection: RowSelection::All,
                deleted: None,
            },
            tail: None,
        }
    }

    /// Open a Vortex file lazily; no data is read until queried — except for
    /// Dictionary-layout files, whose term dictionary is read once (a
    /// single-column projection) so queries can translate terms to codes.
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        // Opens the file footer only (schema + layout metadata); no row data
        // is read yet. The returned handle caches its layout reader tree so
        // later scans/prunes across this store (and stores derived from it)
        // share decoded zone-map stats instead of re-reading them each time.
        let file = Arc::new(de::open_vortex_file(path).await?);
        let layout = match LayoutStrategy::from_dtype(file.dtype()) {
            LayoutStrategy::Default => ResolvedLayout::Default,
            LayoutStrategy::TypedObject => ResolvedLayout::TypedObject,
            LayoutStrategy::Dictionary => {
                // Dictionary-layout files need their dictionary up front too;
                // this is a single-column projection scan, not a full read.
                ResolvedLayout::Dictionary(Arc::new(term_dictionary::dict_from_file(&file).await?))
            }
        };
        // Discover which secondary indexes the file's schema carries.
        let indexes = detect_indexes(file.dtype());
        // No filter and no selection yet: this view covers the whole file.
        Ok(Self {
            layout,
            indexes,
            quads: QuadsSource::File {
                file,
                filter: None,
                selection: RowSelection::All,
                deleted: None,
                copy_scan: None,
            },
            tail: None,
        })
    }

    /// Load from IPC bytes.
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // Wrap the byte slice so it can be read like a file, decode the
        // single IPC array message, then reuse `new` to detect the layout.
        let cursor = Cursor::new(bytes);
        let arr = crate::io::de::array_from_ipc_reader(cursor)?;
        Self::new(arr)
    }

    /// Derive a view over this store's base, narrowed to `selection`.
    ///
    /// The base and the index set carry over untouched: `selection` names base
    /// row ids, so nothing the indexes know has been invalidated, and the rows
    /// outside the selection remain reachable. The tail carries over as-is
    /// (its own selection is tail-local and untouched by a base narrowing);
    /// `match_pattern` narrows it separately.
    fn with_selection(&self, selection: RowSelection) -> Self {
        let quads = match &self.quads {
            QuadsSource::InMemory { base, deleted, .. } => QuadsSource::InMemory {
                base: base.clone(),
                selection,
                deleted: deleted.clone(),
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter, deleted, .. } => QuadsSource::File {
                file: file.clone(),
                filter: filter.clone(),
                selection,
                deleted: deleted.clone(),
                // A different selection breaks the copy plan's "filter selects
                // exactly the selection's rows" invariant, so it never carries
                // across a re-selection.
                copy_scan: None,
            },
        };
        Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads,
            tail: self.tail.clone(),
        }
    }

    /// An empty view of this store: same base (and tail), selecting no row of
    /// either.
    ///
    /// Scans over it plan no work and `size()` answers 0 without touching the
    /// data. Indexes are dropped: chained matches on an empty view would
    /// otherwise run pointless lookups just to intersect with nothing.
    fn empty_view(&self) -> Self {
        let mut view = self.with_selection(RowSelection::empty());
        view.indexes = vec![];
        if let Some(tail) = &mut view.tail {
            tail.selection = RowSelection::empty();
        }
        view
    }

    /// Collapse this view into a standalone store holding only the rows it
    /// selects, physically gathered.
    ///
    /// This is the one operation that breaks row identity — the gathered rows
    /// are renumbered from zero — so secondary-index columns are projected away
    /// and the index set emptied: their row ids address the base, and searching
    /// them here would silently misresolve.
    ///
    /// Views are the cheaper default (matching composes selections instead of
    /// copying rows, and keeps the indexes). Reach for this when a result must
    /// stand on its own: to hand it across a boundary, or to stop a small match
    /// result from pinning a large base alive.
    pub async fn materialize(&self) -> Result<Self> {
        // An unrefined in-memory view with nothing tombstoned or appended
        // already is exactly its own rows — and its indexes are still valid,
        // so don't gather (and strip) needlessly.
        if self.tail.is_none()
            && let QuadsSource::InMemory {
                selection: RowSelection::All,
                deleted: None,
                ..
            } = &self.quads
        {
            return Ok(self.clone());
        }
        // Without a tail the selected rows can be gathered as columns, with no
        // decode round-trip.
        if self.tail.is_none() {
            return Ok(Self {
                layout: self.layout.clone(),
                indexes: vec![],
                quads: QuadsSource::InMemory {
                    base: self.selected_rows().await?,
                    selection: RowSelection::All,
                    // Gathering leaves only live rows behind, reclaiming
                    // whatever the tombstones were holding.
                    deleted: None,
                },
                tail: None,
            });
        }
        // A tail can't in general be concatenated onto the gathered base (its
        // schema differs under the Dictionary layout, whose codes an appended
        // term isn't in), so rebuild from the quads themselves, in view order:
        // base rows first, then tail rows.
        let raws = self.live_raw_quads().await?;
        Self::from_raw_quads(&raws, self.layout.strategy(), vec![], false)
    }

    /// Compact the store, keeping its current index set: fold the appended
    /// tail into the base, reclaim tombstoned rows, re-sort by (s, p, o, g),
    /// and rebuild the indexes the store already carries.
    ///
    /// See [`compact_with_indexes`] for the mechanics and for choosing a
    /// different index set. `add_quads` calls this automatically when the tail
    /// outgrows the auto-compaction thresholds (in-memory bases only); calling
    /// it explicitly is how a file-backed store's tail is folded — note that
    /// this converts the store into an in-memory one.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn compact(&self) -> Result<Self> {
        self.compact_with_indexes(self.indexes.clone()).await
    }

    /// Like [`materialize`], but compacts fully: the gathered quads are
    /// re-sorted by (s, p, o, g) and the given secondary indexes are rebuilt
    /// over them, instead of every index being dropped.
    ///
    /// Materializing renumbers rows to a fresh `0..n`, which is exactly why
    /// plain [`materialize`] has to drop indexes — their `_idx_*_rid` columns
    /// addressed the old base. This variant turns that into an opportunity:
    /// the rows are rebuilt in SPOG order (restoring the subject binary-search
    /// fast path that gathering forfeits, and folding any appended tail back
    /// into the base — re-encoded against a fresh term dictionary under the
    /// Dictionary layout) and the requested indexes are rebuilt over the new
    /// order. Pass the store's current [`indexes`](Self::indexes) to preserve
    /// them (or use [`compact`]), an empty set for a sort-only compaction, or
    /// a different set to re-index.
    ///
    /// This is the store's compaction step: it reclaims tombstoned rows,
    /// absorbs the tail, and restores every sorted-order fast path, at the
    /// cost of an O(n log n) rebuild.
    ///
    /// [`materialize`]: Self::materialize
    /// [`compact`]: Self::compact
    pub async fn compact_with_indexes(&self, indexes: Indexes) -> Result<Self> {
        let unique = unique_indexes(&indexes);
        let mut raws = self.live_raw_quads().await?;
        raws.sort_unstable();
        Self::from_raw_quads(&raws, self.layout.strategy(), unique, true)
    }

    /// Build a fresh owning in-memory store from raw quads under `strategy` —
    /// the shared back half of materializing.
    ///
    /// The Dictionary layout re-derives its term dictionary from the quads
    /// (they may hold appended terms the old dictionary has no code for); the
    /// other layouts rebuild their columns directly. `sorted` must be `true`
    /// only when `raws` is SPOG-sorted: it stamps the `s` column (and, with
    /// indexes, is what makes the single-chunk index emission globally
    /// binary-searchable).
    fn from_raw_quads(
        raws: &[RawQuad],
        strategy: LayoutStrategy,
        indexes: Indexes,
        sorted: bool,
    ) -> Result<Self> {
        let (layout, base) = match strategy {
            LayoutStrategy::Dictionary if raws.is_empty() => (
                ResolvedLayout::Dictionary(Arc::new(TermDictionary::empty())),
                dictionary::empty_struct(&indexes)?,
            ),
            LayoutStrategy::Dictionary => {
                let dict = TermDictionary::from_quads(raws)?;
                let id_map = dict.build_id_map();
                let base =
                    dictionary::build_chunk(raws, &dict, &id_map, &indexes, 0, sorted, true, true)?;
                (ResolvedLayout::Dictionary(Arc::new(dict)), base)
            }
            strategy => {
                let base =
                    build_struct_array(raws, strategy, &indexes, raws.len(), 0, sorted, true)?;
                let layout = match strategy {
                    LayoutStrategy::TypedObject => ResolvedLayout::TypedObject,
                    _ => ResolvedLayout::Default,
                };
                (layout, base)
            }
        };
        Ok(Self {
            layout,
            indexes,
            quads: QuadsSource::InMemory {
                base,
                selection: RowSelection::All,
                deleted: None,
            },
            tail: None,
        })
    }

    /// Every live quad this view covers, decoded to raw N-Triples term strings
    /// — base rows first (in view order), then tail rows.
    async fn live_raw_quads(&self) -> Result<Vec<RawQuad>> {
        let mut raws = self.layout.raw_quads(&self.base_selected_rows().await?)?;
        if let Some(tail) = &self.tail {
            let rows = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref())?;
            raws.extend(self.tail_layout().raw_quads(&rows)?);
        }
        Ok(raws)
    }

    /// The layout the tail's rows are stored in: the store's own, except under
    /// the Dictionary layout, where an appended term has no code in the sorted
    /// dictionary, so the tail holds Default-layout strings instead — patterns
    /// probe the base by code and the tail by string.
    fn tail_layout(&self) -> ResolvedLayout {
        match &self.layout {
            ResolvedLayout::Dictionary(_) => ResolvedLayout::Default,
            other => other.clone(),
        }
    }

    /// The secondary indexes this store's schema carries.
    pub fn indexes(&self) -> &[IndexType] {
        &self.indexes
    }

    /// Number of physical rows in the append tail (including any tombstoned
    /// since they were appended); `0` when nothing has been appended or the
    /// tail has been compacted away.
    ///
    /// The tail is the store's only unindexed, unsorted region, so this is the
    /// number to watch when tuning compaction: `add_quads` folds it back into
    /// the base automatically for in-memory bases, but a file-backed store
    /// keeps growing its tail until [`compact`](Self::compact) is called
    /// explicitly.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
    }

    /// Whether `add_quads` should fold the tail into the base now.
    ///
    /// Only in-memory bases auto-compact: compacting a file-backed store pulls
    /// the whole file into memory — a change of backend and memory footprint
    /// too consequential to trigger implicitly from an append. File-backed
    /// stores compact only through an explicit [`compact`](Self::compact).
    fn should_auto_compact(&self) -> bool {
        match (&self.quads, &self.tail) {
            (QuadsSource::InMemory { base, .. }, Some(tail)) => {
                tail_needs_compaction(base.len(), tail.rows.len())
            }
            _ => false,
        }
    }

    /// This store as one that owns its rows and can be mutated — cheaply when it
    /// already is an owner, otherwise an independent materialized copy.
    ///
    /// A view derived from `match_pattern` shares a base it does not own, so it
    /// cannot be mutated in place. This turns such a view into an owner by
    /// materializing, rebuilding its declared indexes
    /// ([`compact_with_indexes`]) so mutating a match result yields an
    /// independent store that is still indexed rather than one degraded to full
    /// scans. An owner is returned as a cheap clone, preserving its tombstones
    /// and indexes, so repeated in-place deletes stay cheap and keep their
    /// indexes.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn owned(&self) -> Result<Self> {
        if self.is_owner() {
            Ok(self.clone())
        } else {
            self.compact_with_indexes(self.indexes.clone()).await
        }
    }

    /// Whether this store owns its rows, rather than being a window onto
    /// someone else's.
    ///
    /// Only an owner may be mutated: a narrowed view's rows are a subset of a
    /// base it shares, so mutating it would either silently discard the rows
    /// outside the view or write through to data it doesn't own. A view that
    /// happens to select everything (an unconstrained `match_pattern`) covers
    /// exactly the same rows as the store it came from, so it counts as an
    /// owner — mutating it is indistinguishable from mutating that store.
    fn is_owner(&self) -> bool {
        // A narrowed tail marks a view just as a narrowed base does — a match
        // may cover all base rows yet only some of the tail's.
        let tail_owned = self
            .tail
            .as_ref()
            .is_none_or(|tail| matches!(tail.selection, RowSelection::All));
        tail_owned
            && match &self.quads {
                QuadsSource::InMemory { selection, .. } => matches!(selection, RowSelection::All),
                #[cfg(feature = "file-io")]
                QuadsSource::File { filter, selection, .. } => {
                    filter.is_none() && matches!(selection, RowSelection::All)
                }
            }
    }

    fn ensure_owner(&self, operation: &str) -> Result<()> {
        if self.is_owner() {
            return Ok(());
        }
        Err(VortexRdfError::Serialization(format!(
            "{operation} is not supported on a store derived from match_pattern: its rows are a \
             view onto a larger base, so mutating it would either silently drop the rows outside \
             the view or write through to data it does not own. Call materialize() for an \
             independent copy to mutate, or call the mutation on the store the view came from."
        )))
    }

    // ── build ─────────────────────────────────────────────────────────────────

    /// Build using the default builder (UnsortedStream, Default layout, no indexes).
    pub async fn build_vortex_array(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        // Convenience wrapper around the generic builder entrypoint below,
        // pinned to the streaming/unsorted/no-index defaults.
        Self::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
            quad_stream,
            LayoutStrategy::Default,
            vec![],
        )
        .await
    }

    /// Build using a specified builder, layout, and secondary indexes.
    pub async fn build_vortex_array_with_builder<B: VortexArrayBuilder>(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<ArrayRef> {
        // Delegate entirely to the builder type `B`: it consumes the quad
        // stream and produces the final columnar array according to `layout`
        // and `indexes` (the builder strategies live under `store::builders`).
        B::build_vortex_array(Box::new(quad_stream), layout, indexes).await
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    /// Number of quads in the store.
    ///
    /// For a file-backed store with a pending `match_pattern` filter, this
    /// counts matching rows from the filter masks alone — only the columns the
    /// filter references are read, and no rows are projected or decoded.
    /// `file.row_count()` alone would report the unfiltered total.
    pub async fn size(&self) -> Result<usize> {
        let base = match &self.quads {
            // In-memory patterns resolve to exact row ids at match time, so the
            // selection alone knows the answer — no rows are touched. Deletions
            // are only counted out, never gathered.
            QuadsSource::InMemory { base, selection, deleted: None } => {
                selection.len(base.len())
            }
            QuadsSource::InMemory { base, selection, deleted: Some(deleted) } => {
                selection.live_mask(deleted, base.len()).true_count()
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter, selection, deleted, .. } => match filter {
                // No filter pending: the selection is exact, minus whatever the
                // tombstones have removed from it.
                None => match deleted {
                    None => selection.len(file.row_count() as usize),
                    Some(d) => selection.live_mask(d, file.row_count() as usize).true_count(),
                },
                // A filter is pending: its selectivity is unknown ahead of
                // time, so the rows actually have to be evaluated (with the
                // tombstoned rows excluded before counting).
                Some(f) => {
                    self.count_matching_rows(file, f, selection, deleted.as_ref())
                        .await?
                }
            },
        };
        // The tail's contribution: its selection is always exact (tail matches
        // are resolved eagerly), minus its own tombstones.
        let tail = self.tail.as_ref().map_or(0, |tail| match &tail.deleted {
            None => tail.selection.len(tail.rows.len()),
            Some(deleted) => tail.selection.live_mask(deleted, tail.rows.len()).true_count(),
        });
        Ok(base + tail)
    }

    /// Count rows matching `filter` by driving the layout reader's pruning and
    /// filter evaluations directly and summing mask true-counts, mirroring the
    /// filter phase of vortex's own `split_exec` (per-conjunct pruning first,
    /// then per-conjunct filter evaluation threading the mask). A projection
    /// scan would additionally decode a data column for every matching row
    /// just to measure its length.
    #[cfg(feature = "file-io")]
    async fn count_matching_rows(
        &self,
        file: &VortexFile,
        filter: &Expression,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<usize> {
        // The cached layout reader tree — reused across every split task
        // below, so zone-map stats are looked up once, not once per split.
        let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
        // Split the filter into its top-level AND-ed conditions: the struct
        // layout can only prune a single-field expression at a time.
        let filter_conjuncts = conjuncts(filter);
        // Translate this view's selection into the two knobs the split loop
        // below understands: the bounds it iterates and the per-split starting
        // mask (see `split_start_mask`).
        let (row_selection, bounds) = split_bounds(selection, file.row_count());

        // Build one counting task per natural file split (zone), clamped to
        // `bounds` and dropping splits that fall entirely outside it.
        let tasks = file
            .splits()
            .map_err(VortexRdfError::Vortex)?
            .into_iter()
            .filter_map(|split| {
                let start = split.start.max(bounds.start);
                let end = split.end.min(bounds.end);
                (start < end).then_some(start..end)
            })
            .map(|range| {
                let reader = Arc::clone(&reader);
                let filter_conjuncts = filter_conjuncts.clone();
                // The starting mask for this split: the selected rows within
                // `range`, minus any this view has tombstoned.
                let start_mask = split_start_mask(&row_selection, deleted, &range);
                async move {
                    // The final mask's true-count is this split's contribution
                    // — no column is ever projected or decoded to get it.
                    let mask =
                        evaluate_filter_split(reader, &filter_conjuncts, &range, start_mask).await?;
                    Ok::<usize, VortexRdfError>(mask.true_count())
                }
            });

        // Run split tasks concurrently (bounded by available parallelism) and
        // sum their counts as they complete.
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * 4;
        let mut counts = stream::iter(tasks).buffer_unordered(concurrency);
        let mut count = 0usize;
        while let Some(n) = counts.next().await {
            count += n?;
        }
        Ok(count)
    }

    /// Evaluate this file view's pending filter and selection to a base-wide
    /// mask of the file rows it matches — the concrete row ids a deferred
    /// `match_pattern` on a file resolves to.
    ///
    /// The in-memory delete path can read the doomed rows straight off the
    /// matched view's selection, but a file view may still carry an unresolved
    /// filter, so its matches have to be evaluated here (reading only the filter
    /// columns, never the data ones) before they can be tombstoned. Tombstones
    /// are ignored: this answers "which rows does the pattern name", and the
    /// caller unions the result into the existing tombstones.
    #[cfg(feature = "file-io")]
    async fn matching_file_row_mask(&self) -> Result<Mask> {
        let QuadsSource::File { file, filter, selection, .. } = &self.quads else {
            unreachable!("matching_file_row_mask is only called on a file-backed view")
        };
        let row_count = file.row_count();
        // No pending filter: the selection alone is exact, so its rows are the
        // matches — no scan needed.
        let Some(filter) = filter else {
            return Ok(selection.to_mask(row_count as usize));
        };

        let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
        let filter_conjuncts = conjuncts(filter);
        let (row_selection, bounds) = split_bounds(selection, row_count);

        // Same per-split evaluation as the counting path, but collecting the
        // surviving rows' absolute ids rather than only their number.
        let tasks = file
            .splits()
            .map_err(VortexRdfError::Vortex)?
            .into_iter()
            .filter_map(|split| {
                let start = split.start.max(bounds.start);
                let end = split.end.min(bounds.end);
                (start < end).then_some(start..end)
            })
            .map(|range| {
                let reader = Arc::clone(&reader);
                let filter_conjuncts = filter_conjuncts.clone();
                // Tombstones are deliberately not applied to the start mask
                // here (see the doc comment); the pattern's own matches are
                // what this computes.
                let start_mask = split_start_mask(&row_selection, None, &range);
                async move {
                    let mask =
                        evaluate_filter_split(reader, &filter_conjuncts, &range, start_mask).await?;
                    // Lift the split-relative survivors back to absolute file
                    // row ids.
                    let ids: Vec<usize> = match mask.indices() {
                        vortex_mask::AllOr::All => {
                            (range.start as usize..range.end as usize).collect()
                        }
                        vortex_mask::AllOr::None => Vec::new(),
                        vortex_mask::AllOr::Some(indices) => {
                            indices.iter().map(|&i| range.start as usize + i).collect()
                        }
                    };
                    Ok::<Vec<usize>, VortexRdfError>(ids)
                }
            });

        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * 4;
        let mut results = stream::iter(tasks).buffer_unordered(concurrency);
        let mut matched: Vec<usize> = Vec::new();
        while let Some(ids) = results.next().await {
            matched.extend(ids?);
        }
        matched.sort_unstable();
        Ok(Mask::from_indices(row_count as usize, matched))
    }


    pub fn layout(&self) -> LayoutStrategy {
        // Report the build-time strategy tag regardless of whether extra
        // state (like the Dictionary layout's term dictionary) is attached.
        self.layout.strategy()
    }

    /// Gather the rows this view selects into a single in-memory StructArray.
    ///
    /// Index columns survive only when the view still *is* its base (an
    /// unrefined in-memory store), where the `_idx_*_rid` ids address exactly
    /// the rows returned. Any narrowed view gathers and renumbers rows, so its
    /// index columns are stripped rather than handed out stale; a file-backed
    /// view never projects them in the first place.
    pub async fn get_quads_array(&self) -> Result<ArrayRef> {
        self.selected_rows().await
    }

    /// The rows this view covers, base and tail combined, as one array of
    /// primary columns.
    ///
    /// For most layouts the tail (which shares the base's primary schema) is
    /// appended as a second chunk. Under the Dictionary layout the tail holds
    /// strings the base's codes can't express, so the combined rows are
    /// re-encoded against a fresh term dictionary, and the result carries its
    /// own `_dict_terms` payload (it is self-describing, no longer decoding
    /// through this store's cached dictionary).
    async fn selected_rows(&self) -> Result<ArrayRef> {
        let base = self.base_selected_rows().await?;
        let Some(tail) = &self.tail else {
            return Ok(base);
        };
        let tail_rows = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref())?;
        match &self.layout {
            ResolvedLayout::Dictionary(_) => {
                let mut raws = self.layout.raw_quads(&base)?;
                raws.extend(ResolvedLayout::Default.raw_quads(&tail_rows)?);
                if raws.is_empty() {
                    return dictionary::empty_struct(&[]);
                }
                let dict = TermDictionary::from_quads(&raws)?;
                let id_map = dict.build_id_map();
                dictionary::build_chunk(&raws, &dict, &id_map, &[], 0, false, true, false)
            }
            _ => {
                // The base part may carry index columns the tail lacks;
                // project them away so the chunk dtypes agree.
                let base = self.layout.project_primary(&base)?;
                let dtype = base.dtype().clone();
                ChunkedArray::try_new(vec![base, tail_rows], dtype)
                    .map_err(VortexRdfError::Vortex)
                    .map(|a| a.into_array())
            }
        }
    }

    /// The base rows this view covers (gathered in memory, or scanned from the
    /// file with the pending filter and selection applied) — without the tail.
    async fn base_selected_rows(&self) -> Result<ArrayRef> {
        match &self.quads {
            // The whole base, nothing deleted: hand back the array as it
            // stands, indexes and all.
            QuadsSource::InMemory {
                base,
                selection: RowSelection::All,
                deleted: None,
            } => Ok(base.clone()),
            // Anything narrower: gather the live selected rows, dropping index
            // columns whose ids no longer address them.
            QuadsSource::InMemory { base, selection, deleted } => {
                strip_index_columns(gather_live(base, selection, deleted.as_ref())?)
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter, selection, deleted, .. } => {
                // Project only the layout's primary columns (index columns
                // are internal and never surfaced to callers of this method).
                let proj = self.layout.primary_column_names();
                let mut scan = file.scan()
                    .map_err(VortexRdfError::Vortex)?
                    .with_projection(select(proj, root()));
                // Apply the restrictions this view accumulated via
                // match_pattern: a pushed-down filter for the components no
                // index resolved, and the row selection (with tombstoned rows
                // excluded) for those it did.
                if let Some(f) = filter {
                    scan = scan.with_filter(f.clone());
                }
                scan = selection.restrict_scan(scan, deleted.as_ref());
                // Execute the scan and materialize every matching row into a
                // single in-memory array.
                let arr = scan
                    .into_array_stream()
                    .map_err(VortexRdfError::Vortex)?
                    .read_all()
                    .await
                    .map_err(VortexRdfError::Vortex)?;
                Ok(arr)
            }
        }
    }

    /// Return this store's array in a form that can be serialized and read back
    /// standalone.
    ///
    /// A store resolves its layout once and caches any state that layout holds
    /// intrinsically, so a store derived through `match_pattern` keeps decoding
    /// correctly even when slicing or filtering has dropped that state from the
    /// array itself. A serialized copy has no such cache, so the state is
    /// written back into the array here.
    pub async fn to_serializable_array(&self) -> Result<ArrayRef> {
        let array = self.get_quads_array().await?;
        // A tailed Dictionary read already re-encoded everything against a
        // fresh dictionary and embedded it (see `selected_rows`); re-attaching
        // this store's cached, pre-append dictionary would mismatch the new
        // codes. The other layouts hold no intrinsic state, so skipping is
        // equally correct for them.
        if self.tail.is_some() {
            return Ok(array);
        }
        self.layout.attach_intrinsic_state(array)
    }

    // ── quads streaming ───────────────────────────────────────────────────────

    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        let layout = self.layout.clone();
        // Tail rows are in memory and few: decode them eagerly, to be appended
        // after whatever the base yields.
        let tail_quads: Vec<Result<Quad>> = match &self.tail {
            None => vec![],
            Some(tail) => self.tail_layout().decode_chunk(&gather_live(
                &tail.rows,
                &tail.selection,
                tail.deleted.as_ref(),
            )?),
        };
        match &self.quads {
            QuadsSource::InMemory { base, selection, deleted } => {
                // The data is already in memory: gather the live selected rows
                // and decode them up front, then hand back a simple iterator
                // wrapped as a stream.
                let mut quads =
                    layout.decode_chunk(&gather_live(base, selection, deleted.as_ref())?);
                quads.extend(tail_quads);
                Ok(Box::new(stream::iter(quads)))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, filter, selection, deleted, copy_scan } => {
                // A copy-served view streams from the matched family's own
                // columns instead: the family's sort order clusters the
                // matching rows into a contiguous run of zones, so the scan
                // prunes to those rather than scattering row-id reads across
                // the whole file. The plan's filter selects exactly the rows
                // this view's selection names (see `CopyScan`); tombstones are
                // applied through the family's rid column.
                if let Some(copy_scan) = copy_scan {
                    let family = copy_scan.family;
                    let copy_layout = self.copy_layout();
                    let deleted = deleted.clone();
                    let scan = file
                        .scan()
                        .map_err(VortexRdfError::Vortex)?
                        .with_projection(select(family.column_names(), root()))
                        .with_filter(copy_scan.filter.clone());
                    let stream = scan
                        .map(move |chunk| {
                            Ok(decode_copy_chunk(&chunk, family, &copy_layout, deleted.as_ref()))
                        })
                        .into_stream()
                        .map_err(VortexRdfError::Vortex)?;
                    let quad_stream = stream
                        .flat_map(|chunk_res| {
                            let quads = match chunk_res {
                                Err(e) => vec![Err(VortexRdfError::Vortex(e))],
                                Ok(quads) => quads,
                            };
                            stream::iter(quads)
                        })
                        .chain(stream::iter(tail_quads));
                    return Ok(Box::new(quad_stream));
                }
                // Same restriction setup as `get_quads_array`: project only
                // the primary columns and apply any pending filter/selection
                // (with tombstoned rows excluded).
                let proj = layout.primary_column_names();
                let mut scan = file.scan().map_err(VortexRdfError::Vortex)?
                    .with_projection(select(proj, root()));
                if let Some(f) = filter {
                    scan = scan.with_filter(f.clone());
                }
                scan = selection.restrict_scan(scan, deleted.as_ref());
                // Decode chunks inside the scan's spawned split tasks (via the
                // scan's map function) so decoding runs concurrently across the
                // runtime's workers instead of serially at the stream consumer.
                let stream = scan
                    .map(move |chunk| Ok(layout.decode_chunk(&chunk)))
                    .into_stream()
                    .map_err(VortexRdfError::Vortex)?;
                // Each polled item is now a `Vec<Result<Quad>>` (one decoded
                // chunk); flatten it back into a stream of individual quads,
                // propagating any per-chunk scan error as a single quad error.
                let quad_stream = stream
                    .flat_map(|chunk_res| {
                        let quads = match chunk_res {
                            Err(e) => vec![Err(VortexRdfError::Vortex(e))],
                            Ok(quads) => quads,
                        };
                        stream::iter(quads)
                    })
                    .chain(stream::iter(tail_quads));
                Ok(Box::new(quad_stream))
            }
        }
    }

    // ── pattern matching ──────────────────────────────────────────────────────

    pub async fn match_pattern(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let mut matched = self.match_base(subject, predicate, object, graph).await?;
        // The tail is matched independently of whatever the base concluded —
        // deliberately after any base short-circuit: a term the base's
        // dictionary has never seen proves nothing about rows appended since
        // (the tail stores strings precisely so such a term can still match).
        if let Some(tail) = &self.tail {
            matched.tail = Some(Self::match_tail(
                &self.tail_layout(),
                tail,
                subject,
                predicate,
                object,
                graph,
            )?);
        }
        Ok(matched)
    }

    /// Narrow a tail to the rows matching the pattern — the tail counterpart
    /// of the base's mask-scan fallback. A tail is small, unsorted, and
    /// unindexed, so a scan over its (already few) selected rows is the right
    /// plan; the surviving positions refine the tail-local selection exactly
    /// as base matches refine the base's.
    fn match_tail(
        layout: &ResolvedLayout,
        tail: &Tail,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Tail> {
        let selection = if tail.selection.is_empty(tail.rows.len()) {
            tail.selection.clone()
        } else {
            match Self::mask_for(
                layout,
                &tail.selection.apply(&tail.rows)?,
                subject,
                predicate,
                object,
                graph,
            )? {
                // Unconstrained pattern: every selected tail row matches.
                None => tail.selection.clone(),
                Some(mask) => tail.selection.clone().refine(&bool_array_to_mask(mask)?),
            }
        };
        Ok(Tail {
            rows: tail.rows.clone(),
            selection,
            deleted: tail.deleted.clone(),
        })
    }

    /// Match the pattern against the base alone, composing its restrictions
    /// into a derived view (the tail carries over untouched; `match_pattern`
    /// narrows it separately).
    async fn match_base(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let t = Instant::now();

        match &self.quads {
            // Tombstones are deliberately not consulted here: they are applied
            // by every read path instead, so matching may name deleted rows
            // without the result ever showing them. Keeping them out also keeps
            // the mask scan's positions aligned with `selection.apply`, which is
            // what `refine` maps back through.
            QuadsSource::InMemory { base, selection, deleted: _ } => {
                // A pattern the layout can prove unmatchable (e.g. a term
                // absent from the dictionary) needs no search at all.
                if matches!(
                    self.layout.constraints(subject, predicate, object, graph),
                    Constraints::AlwaysFalse
                ) {
                    return Ok(self.empty_view());
                }

                // Materialize the base struct so its columns can be inspected
                // (statistics, binary search). Every search below runs against
                // the base and yields base row ids, which are then intersected
                // into this view's selection — so a chained match narrows the
                // same coordinate space instead of rebasing onto a new array.
                let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
                let struct_arr = base
                    .clone()
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let mut selection = selection.clone();
                // The components still to be resolved. Each fast path below
                // clears the one it answers, since its ids already satisfy it.
                let (mut s, mut p, mut o, mut g) = (subject, predicate, object, graph);

                // ── Subject binary search on sorted s column ──────────────
                // If a subject is bound and the base's primary `s` column is
                // known to be sorted (stamped by sorted builders), binary
                // search finds the exact [lo, hi) row range for that subject in
                // O(log n) instead of scanning every row.
                if let Some(subj) = s
                    && let Ok(s_col) = struct_arr.unmasked_field_by_name("s")
                    && column_is_sorted(s_col)
                    && let Some(probe) = self.layout.probe_scalar(&subj.to_string())
                    && let Ok(scalar) = probe.cast(s_col.dtype())
                {
                    // Left/right binary search bounds the run of rows equal to
                    // the probe value.
                    let (lo, hi) = search_sorted_bounds(s_col, &scalar)?;
                    selection = selection.intersect_range(lo as u64..hi as u64);
                    s = None;
                    log::debug!("[match_pattern] s binary search {:?}", t.elapsed());
                }

                // ── Secondary-index routing ───────────────────────────────
                // Ask the configured indexes to resolve the rest of the pattern
                // to exact base row ids — each index owns its own search over
                // its columns (e.g. a binary search of the sorted `_idx_o_val` /
                // `_idx_o_rid` pair for a bound object). The store just folds the
                // ids it hands back into the selection.
                if !selection.is_empty(base.len()) {
                    match resolve_indexes_in_memory(
                        &self.indexes,
                        &struct_arr,
                        &self.layout,
                        s,
                        p,
                        o,
                        g,
                    )? {
                        // The probed term is absent from the data — nothing matches.
                        IndexResolution::Empty => return Ok(self.empty_view()),
                        IndexResolution::Resolved { row_ids, resolves } => {
                            selection = selection.intersect_ids(row_ids);
                            (s, p, o, g) = resolves.clear(s, p, o, g);
                            log::debug!("[match_pattern] index (sorted search) {:?}", t.elapsed());
                        }
                        // No index accelerates this pattern: whatever is still
                        // bound falls to the mask scan below.
                        IndexResolution::Declined => {}
                    }
                }

                // ── Fallback: boolean mask scan ───────────────────────────
                // Whatever no fast path answered is compared column-wise. Only
                // the rows this view already selects are gathered and compared,
                // so a chained match still pays for its own row count, not the
                // base's; the surviving positions map back to base row ids.
                if !selection.is_empty(base.len())
                    && let Some(mask) =
                        Self::mask_for(&self.layout, &selection.apply(base)?, s, p, o, g)?
                {
                    selection = selection.refine(&bool_array_to_mask(mask)?);
                    log::debug!("[match_pattern] mask scan {:?}", t.elapsed());
                }

                Ok(self.with_selection(selection))
            }

            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter: existing_filter,
                selection: existing_selection,
                deleted: existing_deleted,
                ..
            } => {
                // A pattern the layout can prove unmatchable (e.g. a term
                // absent from the dictionary) needs no scan machinery at all.
                if matches!(
                    self.layout.constraints(subject, predicate, object, graph),
                    Constraints::AlwaysFalse
                ) {
                    return Ok(self.empty_view());
                }

                // Ask the configured indexes to resolve this pattern to exact
                // row ids — each index owns its own scan over its columns. A
                // resolved component is then left out of the pushed-down filter:
                // the row ids already are exactly its matches, so re-filtering
                // them would only re-read and re-compare that column.
                let (resolution, resolved_by) = resolve_indexes_file(
                    &self.indexes,
                    file,
                    &self.layout,
                    subject,
                    predicate,
                    object,
                    graph,
                )
                .await?;
                let (next_filter, index_ids, copy_scan) = match resolution {
                    // The probed term is absent — nothing can match. Short-
                    // circuit to the empty view (an empty id set would just
                    // intersect every other restriction down to nothing anyway).
                    IndexResolution::Empty => return Ok(self.empty_view()),
                    // An index resolved one component: push down a filter for
                    // the rest of the pattern only, alongside its exact row ids.
                    IndexResolution::Resolved { row_ids, resolves } => {
                        let (s, p, o, g) = resolves.clear(subject, predicate, object, graph);
                        // When the copy index answered and this match is the
                        // view's only restriction, the matched rows can also be
                        // *served* from the copy family — its sort order holds
                        // them contiguously — instead of being scattered reads
                        // over the primary columns (see `CopyScan`).
                        let copy_scan = (resolved_by == Some(IndexType::SecondaryByCopy)
                            && existing_filter.is_none()
                            && matches!(existing_selection, RowSelection::All))
                        .then(|| self.build_copy_scan(subject, predicate, object, graph))
                        .flatten();
                        (self.build_file_filter(s, p, o, g), Some(row_ids), copy_scan)
                    }
                    // No index applies: the whole pattern becomes the pushed-down filter.
                    IndexResolution::Declined => {
                        (self.build_file_filter(subject, predicate, object, graph), None, None)
                    }
                };
                // Combine with whatever filter this view already carried
                // from earlier match_pattern calls (AND, since both must hold).
                let filter = match (existing_filter.clone(), next_filter) {
                    (Some(lhs), Some(rhs)) => Some(and(lhs, rhs)),
                    (Some(lhs), None) => Some(lhs),
                    (None, rhs) => rhs,
                };

                let selection = match index_ids {
                    // An index answered with exact ids: fold them into the
                    // selection, which drops it to `Ids` — narrowing whatever a
                    // previous match had established (a range, or an earlier
                    // lookup's ids) without ever setting two restrictions at once.
                    Some(ids) => existing_selection.clone().intersect_ids(ids),
                    // No index involved: narrow using zone-map statistics
                    // instead. One full-range pruning evaluation on the cached
                    // layout reader replaces any per-split probing.
                    None => match &filter {
                        Some(f) => match self.row_range_from_pruning(file, f).await? {
                            Some(range) => existing_selection.clone().intersect_range(range),
                            None => existing_selection.clone(),
                        },
                        None => existing_selection.clone(),
                    },
                };

                // Nothing can match: normalize to the canonical empty view.
                if selection.is_empty(file.row_count() as usize) {
                    return Ok(self.empty_view());
                }

                log::debug!("[match_pattern] file filter built {:?}", t.elapsed());
                // Build the new, more-restricted file view; no data has been
                // read yet — restrictions are only applied on the next scan.
                // The file (and thus its index columns) is shared, so the
                // indexes stay usable for further chained matches.
                Ok(Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    quads: QuadsSource::File {
                        file: file.clone(),
                        filter,
                        selection,
                        // Tombstones are a property of the base file, not of the
                        // pattern, so they carry across the match unchanged; the
                        // read paths apply them (see `restrict_scan`).
                        deleted: existing_deleted.clone(),
                        copy_scan,
                    },
                    tail: self.tail.clone(),
                })
            }
        }
    }

    // ── pattern matching helpers ─────────────────────────────────────────────

    /// Build an in-memory boolean mask (one bit per row of `array`, in its own
    /// order) marking which of its rows satisfy the given pattern. Returns
    /// `None` when the pattern is fully unconstrained (every row matches, so no
    /// mask is needed).
    ///
    /// The mask is positional, so it only means anything against the array it
    /// was computed over: callers holding a view must translate it back to base
    /// row ids via [`RowSelection::refine`].
    ///
    /// `layout` is the layout of `array`'s rows — the store's own for the
    /// base, [`Self::tail_layout`] for the tail (which stores strings under a
    /// Dictionary-encoded base).
    fn mask_for(
        layout: &ResolvedLayout,
        array: &ArrayRef,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = array
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        // Ask the layout to translate the RDF pattern into column equality
        // constraints (e.g. "s == <iri>"). A pattern that can never match
        // (like a term missing from a dictionary) short-circuits to an
        // all-false mask without touching any column.
        let eqs = match layout.constraints(subject, predicate, object, graph) {
            Constraints::AlwaysFalse => {
                let none = ConstantArray::new(Scalar::from(false), struct_arr.len()).into_array();
                return Ok(Some(none));
            }
            Constraints::Eq(eqs) => eqs,
        };

        // AND together one equality comparison per constrained column.
        let mut mask: Option<ArrayRef> = None;
        for (field, value) in eqs {
            let col = struct_arr
                .unmasked_field_by_name(field)
                .map_err(VortexRdfError::Vortex)?;
            // Cast the scalar to the column's dtype (so numeric columns like
            // `o_kind` compare against a scalar of matching type/nullability).
            let scalar = value
                .cast(col.dtype())
                .map_err(VortexRdfError::Vortex)?;
            // Broadcast the scalar to a constant column and compare element-wise.
            let rhs = ConstantArray::new(scalar, col.len()).into_array();
            let m = col.binary(rhs, Operator::Eq).map_err(VortexRdfError::Vortex)?;
            // Fold this column's mask into the running AND of all constraints.
            mask = Some(match mask.take() {
                Some(prev) => prev.binary(m, Operator::And).map_err(VortexRdfError::Vortex)?,
                None => m,
            });
        }
        Ok(mask)
    }

    /// The copy-family plan serving exactly the rows of the given pattern, or
    /// `None` when the pattern isn't copy-servable (no family covers its
    /// shape, or a bound residual term has no dictionary code — a case the
    /// caller has already short-circuited to an empty view).
    ///
    /// The copy columns hold each component as one full term (a string, or a
    /// dictionary code), so every bound component — including a TypedObject
    /// store's object — probes as a single term equality on the family's
    /// column. The subject never appears here: the copy index declines
    /// subject-bound patterns, so a resolved pattern has no bound subject.
    #[cfg(feature = "file-io")]
    fn build_copy_scan(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Option<CopyScan> {
        let family = secondary_by_copy::family_for(subject, predicate, object)?;
        let bound = [
            (family.p_col(), predicate.map(|p| p.to_string())),
            (family.o_col(), object.map(|o| o.to_string())),
            (family.g_col(), graph.map(graph_name_str)),
        ];
        let mut filter: Option<Expression> = None;
        for (column, term) in bound {
            let Some(term) = term else { continue };
            let scalar = self.layout.probe_scalar(&term)?;
            let expr = eq(get_item(column, root()), lit(scalar));
            filter = Some(match filter.take() {
                Some(f) => and(f, expr),
                None => expr,
            });
        }
        Some(CopyScan {
            family,
            filter: filter?,
        })
    }

    /// The layout a copy family's rows decode through: the copies always store
    /// each component as one term — u32 codes under the Dictionary layout,
    /// N-Triples strings otherwise (a TypedObject store's copies hold the
    /// object as its full term string, so they decode as Default).
    #[cfg(feature = "file-io")]
    fn copy_layout(&self) -> ResolvedLayout {
        match &self.layout {
            ResolvedLayout::Dictionary(dict) => ResolvedLayout::Dictionary(dict.clone()),
            _ => ResolvedLayout::Default,
        }
    }

    /// Convert an RDF pattern (subject, predicate, object, graph) into a Vortex filter expression
    /// that can be applied to a file-backed array during scanning.
    /// This allows the file reader to push filters down and avoid reading unnecessary data.
    #[cfg(feature = "file-io")]
    fn build_file_filter(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Option<Expression> {
        match self.layout.constraints(subject, predicate, object, graph) {
            // If the layout determines that no rows can possibly match (e.g., asking for a
            // term that doesn't exist in a dictionary layout), return a filter that matches
            // nothing (always evaluates to false).
            Constraints::AlwaysFalse => Some(lit(false)),
            
            // If the layout provides equality constraints (field_name, value pairs), build a
            // filter expression by combining them with AND operations. Each constraint requires
            // a specific column to equal a specific value.
            Constraints::Eq(eqs) => {
                let mut filter: Option<Expression> = None;
                for (field, value) in eqs {
                    // Create an equality expression: get the column field from root, and check
                    // if it equals the given value.
                    let expr = eq(get_item(field, root()), lit(value));
                    filter = Some(match filter.take() {
                        // If we already have a filter, combine it with the new expression using AND.
                        // `filter.take()` consumes the Option, leaving None in its place.
                        Some(f) => and(f, expr),
                        // If this is the first constraint, use it as the filter.
                        None => expr,
                    });
                }
                filter
            }
        }
    }

    /// Zone-map envelope of `filter`: the contiguous row range outside of
    /// which the file's statistics prove no row can match.
    ///
    /// One `pruning_evaluation` per filter conjunct over the full file — the
    /// zoned layout evaluates its cached zone map vectorized, chunks are
    /// evaluated concurrently, and file-level footer stats short-circuit the
    /// whole thing (the reader is wrapped in `FileStatsLayoutReader`). The
    /// conjuncts are evaluated separately because the struct layout only
    /// prunes single-field expressions.
    ///
    /// The envelope is order-agnostic (no sortedness assumption) and keeps
    /// interior non-matching stretches — the scan's own per-split pruning
    /// skips those from the same cached zone masks.
    ///
    /// Returns `Some(0..0)` when nothing can match and `None` when the stats
    /// exclude nothing (leaving the range unset).
    #[cfg(feature = "file-io")]
    async fn row_range_from_pruning(
        &self,
        file: &VortexFile,
        filter: &Expression,
    ) -> Result<Option<Range<u64>>> {
        let row_count = file.row_count();
        // A row count that doesn't fit in usize can't back a Mask; bail out
        // to "no envelope known" rather than fail the whole match.
        let Ok(len) = usize::try_from(row_count) else {
            return Ok(None);
        };
        if len == 0 {
            return Ok(Some(0..0));
        }

        // Start from "everything might match" and narrow it down using only
        // statistics (zone maps / footer stats) — no row data is read here.
        let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
        let mut mask = Mask::new_true(len);
        for conjunct in conjuncts(filter) {
            // Once nothing can match, further conjuncts can't un-prune rows.
            if mask.all_false() {
                break;
            }
            // Evaluate this conjunct's prunability over the *entire* file in
            // one call: the zoned reader vectorizes this over all its zones
            // and the file-stats wrapper checks footer-level bounds first.
            let pruned = reader
                .pruning_evaluation(&(0..row_count), &conjunct, mask.clone())
                .map_err(VortexRdfError::Vortex)?
                .await
                .map_err(VortexRdfError::Vortex)?;
            mask = mask.bitand(&pruned);
        }

        // Collapse the surviving mask to its enclosing contiguous range: the
        // first and last set bit. Interior gaps of non-matching rows are kept
        // (the scan's own per-split pruning will skip those later using the
        // same cached zone masks) — only the outer dead space is trimmed.
        Ok(match (mask.first(), mask.last()) {
            (Some(first), Some(last)) => {
                let range = first as u64..last as u64 + 1;
                // No trimming actually happened — leave the range unset
                // rather than recording a no-op range.
                if range == (0..row_count) { None } else { Some(range) }
            }
            // No bit survived: the filter provably matches nothing in this file.
            _ => Some(0..0),
        })
    }

    /// Test-only hook: whether this view plans to serve `quads()` from a
    /// `SecondaryByCopy` family, so tests can assert the plan actually
    /// attaches (and drops) where intended instead of only observing results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn debug_has_copy_scan(&self) -> bool {
        matches!(&self.quads, QuadsSource::File { copy_scan: Some(_), .. })
    }

    /// Test-only hook exposing the zone-map row-range envelope computed for a
    /// bound subject, so tests can assert on it directly instead of only on
    /// final match results.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) async fn debug_subject_row_range(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        match &self.quads {
            QuadsSource::File { file, .. } => {
                // Term doesn't exist in the store — the envelope is empty
                // without needing to touch the file at all.
                if matches!(
                    self.layout.constraints(Some(subject), None, None, None),
                    Constraints::AlwaysFalse
                ) {
                    return Ok(Some(0..0));
                }
                // Otherwise compute the same envelope match_pattern would.
                match self.build_file_filter(Some(subject), None, None, None) {
                    Some(filter) => self.row_range_from_pruning(file, &filter).await,
                    None => Ok(None),
                }
            }
            QuadsSource::InMemory { .. } => Ok(None),
        }
    }
    
    /// Whether the store holds a quad equal to `quad` (tombstoned rows count
    /// as absent). One fully-bound `match_pattern`, so it rides whatever fast
    /// path the store has — subject binary search, secondary indexes, or file
    /// pruning — and checks the tail too.
    pub async fn contains(&self, quad: &Quad) -> Result<bool> {
        let matched = self
            .match_pattern(
                Some(&quad.subject),
                Some(&quad.predicate),
                Some(&quad.object),
                Some(&quad.graph_name),
            )
            .await?;
        Ok(matched.size().await? > 0)
    }

    // ── mutations ─────────────────────────────────────────────────────────────

    /// Append a single quad — [`add_quads`] with a batch of one. Prefer the
    /// batch form when adding several: each call rebuilds the tail once.
    ///
    /// [`add_quads`]: Self::add_quads
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        self.add_quads([quad]).await
    }

    /// Append every quad not already present, per RDF/JS dataset (set)
    /// semantics: a quad equal to an existing one — or to an earlier quad in
    /// the same batch — is skipped.
    ///
    /// Appends land in the in-memory [`Tail`], never the base, so the base —
    /// its row ids, secondary indexes, tombstones, or file handle — carries
    /// over untouched; queries run the base's fast paths plus a mask scan over
    /// the tail. This also makes Dictionary-layout appends possible: an
    /// appended term has no code in the sorted dictionary, so the tail stores
    /// terms as strings and patterns probe the base by code and the tail by
    /// string.
    ///
    /// Each append rebuilds the tail into one contiguous chunk (O(tail +
    /// batch) — hence batching), and each presence check is one fully-bound
    /// `match_pattern` — cheap where the store has a sorted subject column, an
    /// index, or file pruning; a scan per quad where it has none, in which
    /// case bulk-loading through the builders is the better tool.
    ///
    /// When the tail outgrows the auto-compaction thresholds — a tenth of the
    /// base (with a floor so small stores don't thrash) or a builder chunk's
    /// worth of rows, whichever comes first — the add that crossed the line
    /// finishes by folding the tail into the base ([`compact`]): occasional
    /// O(n log n) work, amortized constant per appended row. Only in-memory
    /// bases do this; a file-backed store never implicitly converts itself
    /// into an in-memory one, so its tail grows until [`compact`] is called
    /// explicitly (watch [`tail_len`](Self::tail_len)).
    ///
    /// [`compact`]: Self::compact
    pub async fn add_quads(&self, quads: impl IntoIterator<Item = Quad>) -> Result<Self> {
        self.ensure_owner("add_quads")?;

        let mut fresh: Vec<RawQuad> = Vec::new();
        let mut seen: HashSet<RawQuad> = HashSet::new();
        for quad in quads {
            let raw = RawQuad::from_quad(&quad);
            if seen.contains(&raw) || self.contains(&quad).await? {
                continue;
            }
            seen.insert(raw.clone());
            fresh.push(raw);
        }
        if fresh.is_empty() {
            return Ok(self.clone());
        }

        let fresh_rows =
            build_struct_array(
                &fresh, 
                self.tail_layout().strategy(), 
                &[], 
                fresh.len(), 
                0, 
                false, 
                false
            )?;
        let rows = match &self.tail {
            None => fresh_rows,
            // Append = rebuild: the old live tail rows plus the fresh ones,
            // flattened back into one contiguous chunk (an accretion of
            // per-add chunks would degrade every tail scan). Renumbering the
            // old tail's ids is safe: views of the pre-append store keep the
            // old tail, and an owner's selections are `All`.
            Some(tail) => {
                let old = gather_live(&tail.rows, &tail.selection, tail.deleted.as_ref())?;
                let dtype = old.dtype().clone();
                let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
                ChunkedArray::try_new(vec![old, fresh_rows], dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array()
                    .execute::<RecursiveCanonical>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?
                    .0
                    .into_array()
            }
        };
        let appended = Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            quads: self.quads.clone(),
            tail: Some(Tail {
                rows,
                selection: RowSelection::All,
                // Gathering above dropped any tombstoned tail rows already.
                deleted: None,
            }),
        };
        // Append-then-check: the append itself is policy-free, and the add
        // that pushes the tail over the thresholds pays for folding it into
        // the base — amortized-rare under the ratio trigger, exactly the
        // dynamic-array growth pattern.
        if appended.should_auto_compact() {
            return appended.compact().await;
        }
        Ok(appended)
    }

    /// Remove all quads matching the given quad exactly.
    pub async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        self.delete_matching(
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )
        .await
    }

    /// Remove every quad matching the pattern — the counterpart to
    /// [`match_pattern`], for when the rows a pattern selects should be dropped
    /// rather than read.
    ///
    /// The matched rows are tombstoned rather than rewritten away, so this
    /// costs a mask, not a copy of the surviving data, and the base's row ids —
    /// and with them any secondary index — stay valid across the delete.
    /// Tombstoned rows are only reclaimed by [`materialize`], which is also how
    /// a store that has accumulated many deletes is compacted.
    ///
    /// Only a store that owns its rows can be mutated; call it on the store a
    /// view came from, or on `view.materialize()`.
    ///
    /// [`match_pattern`]: Self::match_pattern
    /// [`materialize`]: Self::materialize
    pub async fn delete_matching(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        self.ensure_owner("delete_matching")?;

        // Reuse the matcher: which rows a pattern names is exactly the question
        // `match_pattern` answers, and the view it returns shares this store's
        // base (or file), so the doomed rows are already in base row ids.
        let doomed = self.match_pattern(subject, predicate, object, graph).await?;

        // The tail tombstones exactly as the base does: the doomed view's
        // tail selection is already exact tail-local ids, so it folds into
        // the tail's own deleted mask the same way.
        let tail = match (&self.tail, &doomed.tail) {
            (Some(tail), Some(doomed_tail)) => Some(Tail {
                rows: tail.rows.clone(),
                selection: tail.selection.clone(),
                deleted: Some(union_deleted(
                    tail.deleted.as_ref(),
                    doomed_tail.selection.to_mask(tail.rows.len()),
                )),
            }),
            (tail, _) => tail.clone(),
        };

        // Fold the doomed rows into a base-wide tombstone mask. The matcher
        // doesn't consult the existing tombstones, so the doomed set may name
        // rows already deleted; the union absorbs that. Either way the base
        // (or file) and its secondary indexes are left untouched.
        //
        // The catch-all arm is only reachable with the file backend compiled
        // in; without it, the in-memory arm alone is exhaustive.
        #[cfg_attr(not(feature = "file-io"), allow(unreachable_patterns))]
        match (&self.quads, &doomed.quads) {
            (
                QuadsSource::InMemory { base, selection, deleted },
                QuadsSource::InMemory { selection: doomed, .. },
            ) => {
                // In memory the matched view's selection is already exact row
                // ids, so it maps straight to a mask.
                let doomed = doomed.to_mask(base.len());
                Ok(Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    quads: QuadsSource::InMemory {
                        base: base.clone(),
                        selection: selection.clone(),
                        deleted: Some(union_deleted(deleted.as_ref(), doomed)),
                    },
                    tail,
                })
            }
            #[cfg(feature = "file-io")]
            (
                QuadsSource::File { file, selection, deleted, .. },
                QuadsSource::File { .. },
            ) => {
                // A file view may still carry an unresolved filter, so the
                // doomed rows are evaluated to concrete file ids first (reading
                // only the filter columns, never the data ones).
                let doomed = doomed.matching_file_row_mask().await?;
                Ok(Self {
                    layout: self.layout.clone(),
                    indexes: self.indexes.clone(),
                    quads: QuadsSource::File {
                        file: file.clone(),
                        // An owner has no pending filter, and deleting doesn't
                        // introduce one — it only widens the tombstones.
                        filter: None,
                        selection: selection.clone(),
                        deleted: Some(union_deleted(deleted.as_ref(), doomed)),
                        copy_scan: None,
                    },
                    tail,
                })
            }
            _ => unreachable!("a store only ever derives a view of its own backend"),
        }
    }
}

/// Gather the rows of `base` that `selection` covers and `deleted` has not
/// tombstoned.
///
/// The single place the in-memory read paths turn a view into rows, so that
/// applying the tombstones cannot be forgotten by one of them: deletions are
/// deliberately kept out of the selection (see [`RowSelection::live_mask`]), so
/// a selection alone always over-reports.
fn gather_live(
    base: &ArrayRef,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<ArrayRef> {
    let rows = selection.apply(base)?;
    let Some(deleted) = deleted else {
        return Ok(rows);
    };
    let live = selection.live_mask(deleted, base.len());
    if live.all_true() {
        return Ok(rows);
    }
    rows.filter(live).map_err(VortexRdfError::Vortex)
}

/// Fold a freshly-doomed set of base rows into a store's existing tombstones,
/// shared by both backends' delete paths.
fn union_deleted(existing: Option<&Mask>, doomed: Mask) -> Mask {
    match existing {
        Some(existing) => existing | &doomed,
        None => doomed,
    }
}

/// Decode one copy-served scan chunk into quads: the family's columns are
/// re-labeled as primary (s, p, o, g) columns, tombstoned rows are dropped via
/// the family's rid column, and the result decodes through `layout` (the
/// store's copy encoding — Default strings, or Dictionary codes).
#[cfg(feature = "file-io")]
fn decode_copy_chunk(
    chunk: &ArrayRef,
    family: secondary_by_copy::Family,
    layout: &crate::store::layouts::ResolvedLayout,
    deleted: Option<&Mask>,
) -> Vec<Result<Quad>> {
    match copy_chunk_rows(chunk, family, deleted) {
        Ok(rows) => layout.decode_chunk(&rows),
        Err(e) => vec![Err(e)],
    }
}

/// A copy-served chunk's live rows as a primary-named (s, p, o, g) struct.
#[cfg(feature = "file-io")]
fn copy_chunk_rows(
    chunk: &ArrayRef,
    family: secondary_by_copy::Family,
    deleted: Option<&Mask>,
) -> Result<ArrayRef> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let col = |name: &'static str| {
        struct_arr
            .unmasked_field_by_name(name)
            .cloned()
            .map_err(VortexRdfError::Vortex)
    };
    let len = struct_arr.len();
    let rows = StructArray::try_new(
        FieldNames::from(["s", "p", "o", "g"]),
        vec![
            col(family.s_col())?,
            col(family.p_col())?,
            col(family.o_col())?,
            col(family.g_col())?,
        ],
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)?
    .into_array();

    let Some(deleted) = deleted else {
        return Ok(rows);
    };
    // Tombstones are defined over primary row ids; the family's rid column
    // says which primary row each copy row mirrors.
    let rid_col = col(family.rid_col())?
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let live = Mask::from_indices(
        len,
        rid_col
            .as_slice::<u32>()
            .iter()
            .enumerate()
            .filter(|&(_, &rid)| !deleted.value(rid as usize))
            .map(|(position, _)| position),
    );
    if live.all_true() {
        return Ok(rows);
    }
    rows.filter(live).map_err(VortexRdfError::Vortex)
}

/// Auto-compaction floor: below this many tail rows, never compact — a small
/// store would otherwise pay a rebuild every few appends.
const AUTO_COMPACT_TAIL_FLOOR: usize = 4_096;

/// Auto-compaction ratio: compact when the tail reaches this fraction of the
/// base (tail ≥ base/10). A ratio — rather than a fixed size — is what keeps
/// the rebuild cost amortized-constant per appended row, the dynamic-array
/// growth argument; 10% trades roughly seven whole-store rewrites per doubling
/// for a tail that stays small relative to the base.
const AUTO_COMPACT_BASE_RATIO: usize = 10;

/// Auto-compaction cap: compact once the tail could fill a builder chunk,
/// however large the base is. The tail is the one unindexed, unsorted region —
/// every query mask-scans it and every append rebuilds it — so past this size
/// it dominates index-routed lookups on a large base, where the 10% ratio
/// alone would let it grow a hundred times bigger.
const AUTO_COMPACT_TAIL_CAP: usize = DEFAULT_CHUNK_SIZE;

/// The auto-compaction decision (see `VortexRdfStore::add_quads`): ratio with
/// a floor, or the absolute cap, whichever fires first.
fn tail_needs_compaction(base_rows: usize, tail_rows: usize) -> bool {
    tail_rows >= AUTO_COMPACT_TAIL_CAP
        || tail_rows >= AUTO_COMPACT_TAIL_FLOOR.max(base_rows / AUTO_COMPACT_BASE_RATIO)
}

/// Split a file view's [`RowSelection`] into the two knobs the per-split filter
/// loop understands: a [`Selection`] narrowing the mask (an id list, e.g. from a
/// secondary index) and the row-id `bounds` it iterates. A `Range` narrows the
/// bounds; an `Ids` list narrows the mask; `All` narrows neither.
#[cfg(feature = "file-io")]
fn split_bounds(selection: &RowSelection, row_count: u64) -> (Selection, Range<u64>) {
    match selection {
        RowSelection::All => (Selection::All, 0..row_count),
        RowSelection::Range(range) => (Selection::All, range.clone()),
        RowSelection::Ids(ids) => (Selection::IncludeByIndex(ids.clone()), 0..row_count),
    }
}

/// The starting mask for one file split: the rows `selection` covers within
/// `range`, minus any that `deleted` has tombstoned. Returned split-relative
/// (one bit per row of `range`), ready for [`evaluate_filter_split`].
#[cfg(feature = "file-io")]
fn split_start_mask(selection: &Selection, deleted: Option<&Mask>, range: &Range<u64>) -> Mask {
    let mask = selection.row_mask(range).mask().clone();
    match deleted {
        None => mask,
        Some(deleted) => {
            let live = !&deleted.slice(range.start as usize..range.end as usize);
            mask.bitand(&live)
        }
    }
}

/// Evaluate a filter over one file split, threading a narrowing mask through the
/// two phases the layout reader exposes — cheap zone-map/stats pruning first,
/// then real per-conjunct filter evaluation for whatever survives. Returns the
/// split-relative surviving mask; callers either count its set bits or lift them
/// to absolute row ids. Mirrors the filter phase of vortex's own `split_exec`.
#[cfg(feature = "file-io")]
async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    filter_conjuncts: &[Expression],
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    let mut mask = start_mask;
    // Phase 1: prune using zone-map/footer stats only — no I/O beyond the
    // cached stats tables. Each conjunct narrows the mask; stop once nothing
    // survives.
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        let pruned = reader
            .pruning_evaluation(range, conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }
    // Phase 2: for whatever the stats couldn't rule out, read and evaluate each
    // conjunct for real, threading the narrowing mask so later conjuncts see
    // fewer rows.
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        mask = reader
            .filter_evaluation(range, conjunct, MaskFuture::ready(mask))
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
    }
    Ok(mask)
}

#[cfg(test)]
mod tests {
    use super::tail_needs_compaction;

    #[test]
    fn auto_compaction_thresholds() {
        // Floor: however small the base, a tail below 4_096 rows never
        // triggers, so small stores don't thrash.
        assert!(!tail_needs_compaction(10, 4_095));
        assert!(tail_needs_compaction(10, 4_096));

        // Ratio: past the floor, a tenth of the base is the trigger.
        assert!(!tail_needs_compaction(100_000, 9_999));
        assert!(tail_needs_compaction(100_000, 10_000));
        assert!(!tail_needs_compaction(50_000, 4_999));
        assert!(tail_needs_compaction(50_000, 5_000));

        // Cap: on a large base the ratio would tolerate a huge tail, but one
        // builder chunk's worth compacts regardless.
        assert!(!tail_needs_compaction(100_000_000, 99_999));
        assert!(tail_needs_compaction(100_000_000, 100_000));
    }
}
