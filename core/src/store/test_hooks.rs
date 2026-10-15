//! Test hooks on [`VortexRdfStore`]: read-only accessors over internal state
//! no public API reports, so tests assert which mechanism answered a query
//! (a located run, a deferred selection's row ids, a prefix-probe range, a
//! retained wire encoding) rather than only the result. What a planner can
//! observe — the serving component, the selection kind, the sort order, a
//! pending filter, the dictionary's residency — is asserted through
//! [`view_statistics`](VortexRdfStore::view_statistics) and
//! [`dict_reader`](VortexRdfStore::dict_reader).

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::{Struct, StructArray};

#[cfg(feature = "file-io")]
use crate::error::Result;
use crate::store::array;
use crate::store::indexes::IndexComponent;
#[cfg(feature = "file-io")]
use crate::store::layouts::QuadPattern;
#[cfg(feature = "file-io")]
use crate::store::layouts::{DictAccess, ResolvedLayout};
#[cfg(feature = "file-io")]
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{QuadsSource, RowId, TermCode, VortexRdfStore};

pub(crate) use crate::store::write::mutation::{TAIL_FLATTEN_FLOOR, TAIL_MAX_CHUNKS};

thread_local! {
    /// How many times this thread gathered a store's live rows into raw quads
    /// (`live_raw_quads`, compaction's first step). Thread-local because the
    /// tests share a process and run on the current-thread runtime, so a test
    /// reads only its own gathers.
    static GATHERS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };

    /// How many row ids this thread's in-memory resolutions decoded from an
    /// index component's `rid` column (`sorted_row_ids`). Thread-local like
    /// `GATHERS`.
    static DECODED_RIDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };

    /// The code the dictionaries built or opened on this thread give their
    /// first term (see [`CodeBase`]); 0 outside a guard.
    static CODE_BASE: std::cell::Cell<TermCode> = const { std::cell::Cell::new(0) };

    /// The row id the indexed builds on this thread give their first row
    /// (see [`RowIdBase`]); 0 outside a guard.
    static ROW_ID_BASE: std::cell::Cell<RowId> = const { std::cell::Cell::new(0) };
}

/// The row id the indexed builds on this thread give their first row, and
/// that the readers on this thread take off every row id they read: 0,
/// unless a [`RowIdBase`] guard is live.
pub(crate) fn row_id_base() -> RowId {
    ROW_ID_BASE.with(std::cell::Cell::get)
}

/// While alive, every indexed build on this thread numbers its rows from
/// `base` instead of 0 — row id = `base` + row — and every reader on this
/// thread maps a row id back to its row by taking `base` off again. A
/// handful of quads then carry row ids a 32-bit width cannot hold, through
/// every path that stores or reads one: the in-memory index builds, the
/// out-of-core merge's spilled records, the written `rid` columns, located
/// runs, served reads under tombstones and the rebuilds. A base close to
/// `RowId::MAX` instead makes the refusal of an id past the last one
/// reachable with a handful of quads. The base is never written: a store
/// built under a guard must be read under the same one, as a store of 2^32
/// more rows would be. Dropping the guard restores the base it replaced.
/// Thread-local like `GATHERS`, for the same reason.
pub(crate) struct RowIdBase(RowId);

impl RowIdBase {
    /// Number this thread's indexed builds from `base` until the guard
    /// drops.
    pub(crate) fn set(base: RowId) -> Self {
        RowIdBase(ROW_ID_BASE.with(|cell| cell.replace(base)))
    }
}

impl Drop for RowIdBase {
    fn drop(&mut self) {
        ROW_ID_BASE.with(|cell| cell.set(self.0));
    }
}

/// The code a dictionary built or opened on this thread gives its first
/// term: 0, unless a [`CodeBase`] guard is live. Read once by every
/// dictionary constructor (`TermDictionary::new`, `FileBackedDict::open`).
pub(crate) fn code_base() -> TermCode {
    CODE_BASE.with(std::cell::Cell::get)
}

/// While alive, every dictionary built or opened on this thread numbers its
/// terms from `base` instead of 0: code = `base` + rank. A dictionary of a
/// handful of terms then hands out codes past `u32::MAX`, so a test drives
/// codes a 32-bit width cannot hold through every path — the build's code
/// map, the written code columns, matches, keeps, index children, the
/// dictionary handles and the column kernels — without four billion terms.
/// The base is never written: a file built under a guard must be reopened
/// under the same one, as a file of 2^32 more terms would be. Dropping the
/// guard restores the base it replaced. Thread-local like `GATHERS`, for
/// the same reason.
pub(crate) struct CodeBase(TermCode);

impl CodeBase {
    /// Number this thread's dictionaries from `base` until the guard drops.
    pub(crate) fn set(base: TermCode) -> Self {
        CodeBase(CODE_BASE.with(|cell| cell.replace(base)))
    }
}

impl Drop for CodeBase {
    fn drop(&mut self) {
        CODE_BASE.with(|cell| cell.set(self.0));
    }
}

/// Record `rows` row ids decoded from a component's `rid` column (called by
/// `sorted_row_ids`).
pub(crate) fn note_decoded_rids(rows: usize) {
    DECODED_RIDS.with(|decoded| decoded.set(decoded.get() + rows));
}

/// The row ids this thread has decoded from component `rid` columns so far.
pub(crate) fn decoded_rids() -> usize {
    DECODED_RIDS.with(std::cell::Cell::get)
}

/// Record one gather of live rows (called by `live_raw_quads`).
pub(crate) fn note_gather() {
    GATHERS.with(|gathers| gathers.set(gathers.get() + 1));
}

/// The gathers of live rows this thread has made so far: a refusal that comes
/// before any work leaves it unchanged. (Read by the file-backed compaction
/// tests.)
#[cfg(feature = "file-io")]
pub(crate) fn gathers() -> usize {
    GATHERS.with(std::cell::Cell::get)
}

#[cfg(feature = "file-io")]
use oxrdf::NamedOrBlankNode;

impl VortexRdfStore {
    /// The in-memory base; `None` for a file-backed store.
    fn in_memory_base(&self) -> Option<&ArrayRef> {
        match &self.quads {
            QuadsSource::InMemory { base, .. } => Some(base),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => None,
        }
    }

    /// The in-memory base as a struct; `None` for a file-backed store or a
    /// base that is not a canonical struct.
    fn in_memory_struct(&self) -> Option<StructArray> {
        self.in_memory_base()
            .and_then(|base| base.clone().try_downcast::<Struct>().ok())
    }

    /// The in-memory index components; `None` for a file-backed store.
    fn in_memory_components(&self) -> Option<&[IndexComponent]> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => Some(components),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => None,
        }
    }

    /// Whether a pending selection's row ids have been computed: `false` for
    /// a still-deferred resolution, `None` when the selection is not pending
    /// at all.
    pub(crate) fn debug_row_ids_materialized(&self) -> Option<bool> {
        match self.quads.view_selection() {
            ViewSelection::Exact(_) => None,
            ViewSelection::Pending(lazy) => Some(lazy.debug_materialized()),
        }
    }

    /// The exact row range this view's base selection is, when it is one
    /// (what the prefix probe leaves behind); `None` for every other shape.
    pub(crate) fn debug_selection_range(&self) -> Option<Range<u64>> {
        match self.quads.view_selection() {
            ViewSelection::Exact(RowSelection::Range(range)) => Some(range.clone()),
            _ => None,
        }
    }

    /// The append tail's physical rows, `None` when nothing has been
    /// appended.
    pub(crate) fn debug_tail_rows(&self) -> Option<&ArrayRef> {
        self.tail.as_ref().map(|tail| &tail.rows)
    }

    /// Whether every non-nullable integer child of an in-memory base is a
    /// canonical primitive (adoption decoded everything, or the children
    /// were built canonical). Vacuously true for a file-backed store, whose
    /// base is not held in memory.
    pub(crate) fn debug_base_int_children_canonical(&self) -> bool {
        self.in_memory_base().is_none_or(|_| {
            self.in_memory_struct()
                .is_some_and(|base| debug_int_children_canonical(&base))
        })
    }

    /// Whether the named in-memory component's rows hold canonical integer
    /// children. `None` when this store holds no such in-memory component.
    pub(crate) fn debug_index_component_int_children_canonical(&self, name: &str) -> Option<bool> {
        let component = self
            .in_memory_components()?
            .iter()
            .find(|c| c.identity.name == name)?;
        Some(debug_int_children_canonical(component.rows().ok()?))
    }

    /// Whether every sorted-stamped child of an in-memory base resolves an
    /// encoded search probe (see [`debug_sorted_children_probe_resolvable`]):
    /// false only when a bounds search on some child would fall through to
    /// the generic kernel. Vacuously true for a file-backed store.
    pub(crate) fn debug_base_probe_resolvable(&self) -> bool {
        self.in_memory_base().is_none_or(|_| {
            self.in_memory_struct()
                .is_some_and(|base| debug_sorted_children_probe_resolvable(&base))
        })
    }
}

#[cfg(feature = "file-io")]
impl VortexRdfStore {
    /// The opened file a file-backed store reads; `None` for an in-memory
    /// store.
    fn backing_file(&self) -> Option<&NativeStoreFile> {
        match &self.quads {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { file, .. } => Some(file),
        }
    }

    /// How many times the file-backed dictionary was lifted whole into memory
    /// since the file opened (shared by every view of the file); `None` for a
    /// resident dictionary or another layout.
    pub(crate) fn debug_dict_lifts(&self) -> Option<usize> {
        match &self.layout {
            ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) => Some(fb.debug_lifts()),
            _ => None,
        }
    }

    /// Whether this store reads a memory-mapped file: `Some(true)` for a
    /// mapped open, `Some(false)` for a file read through Vortex's reader,
    /// `None` for an in-memory store.
    pub(crate) fn debug_file_mapped(&self) -> Option<bool> {
        self.backing_file().map(NativeStoreFile::is_mapped)
    }

    /// How many (scope, filter shape) trees the file handle has bound;
    /// `None` off-file.
    pub(crate) fn debug_bound_exprs(&self) -> Option<usize> {
        self.backing_file().map(NativeStoreFile::debug_bound_exprs)
    }

    /// The file's current quad-table root reader; `None` off-file. A test
    /// holds it weakly to see whether a reader tree outlives its retirement.
    pub(crate) fn debug_root_reader(&self) -> Option<vortex_layout::LayoutReaderRef> {
        self.backing_file()?.layout_reader().ok()
    }

    /// How many times a keep has streamed a quad column through a scan on
    /// this file handle (shared by every view of the file); `None` off-file.
    pub(crate) fn debug_column_streams(&self) -> Option<usize> {
        self.backing_file()
            .map(NativeStoreFile::debug_column_streams)
    }

    /// Whether this view's selection is pending with no serve plan to read
    /// through: the state only a view built to be counted or windowed may be
    /// in, and none a caller gets back is.
    pub(crate) fn debug_pending_without_plan(&self) -> bool {
        self.quads.is_pending_without_plan()
    }

    /// An index component column's chunk-probe handle as `(row count, flat
    /// leaves)` — `None` off-file or when the column's shape declines the
    /// handle.
    pub(crate) fn debug_component_column_chunks(
        &self,
        component: &str,
        column: &str,
    ) -> Option<(u64, usize)> {
        self.backing_file()?
            .component_column_chunks(component, column)
            .map(|chunks| (chunks.row_count(), chunks.chunk_count()))
    }

    /// How many row ids reads of located index-child runs have asked for on
    /// this file handle (shared by every view of the file); `None` off-file.
    pub(crate) fn debug_located_rid_reads(&self) -> Option<usize> {
        self.backing_file()
            .map(NativeStoreFile::debug_located_rid_reads)
    }

    /// The exact row range the located-run kernel computes for the subject
    /// codes `range` (`lo <= s < hi`); `None` when it declines — off-file, a
    /// file not sorted by subject, a column without a probeable chunk.
    pub(crate) async fn debug_subject_code_range(
        &self,
        range: Range<TermCode>,
    ) -> Result<Option<Range<u64>>> {
        use crate::store::scan::file_reads;
        let Some(file) = self.backing_file() else {
            return Ok(None);
        };
        file_reads::locate_subject_code_range(file, range).await
    }

    /// Whether one named integer child of an in-memory base is a canonical
    /// primitive. `None` when the base has no such child or is not in memory.
    pub(crate) fn debug_base_child_int_canonical(&self, name: &str) -> Option<bool> {
        use vortex_array::arrays::Primitive;
        use vortex_array::arrays::struct_::StructArrayExt;
        let base = self.in_memory_struct()?;
        let child = base.unmasked_field_by_name(name).ok()?;
        child.dtype().is_int().then(|| child.is::<Primitive>())
    }

    /// Whether the named in-memory index component has canonicalized its
    /// rows yet; `None` when this store holds no such component.
    pub(crate) fn debug_index_component_materialized(&self, name: &str) -> Option<bool> {
        self.in_memory_components()?
            .iter()
            .find(|c| c.identity.name == name)
            .map(|c| c.is_materialized())
    }

    /// The index-child row range a file view's serve plan located for its
    /// run. `None` without a plan, or when the plan's run is unlocated.
    pub(crate) fn debug_serve_row_range(&self) -> Option<Range<u64>> {
        match &self.quads {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { serve, .. } => serve.as_ref().and_then(|plan| plan.row_range()),
        }
    }

    /// The zone-map row range a bound subject prunes to on a file view:
    /// `Some(0..0)` when the layout or the statistics prove it absent, `None`
    /// off-file or when the statistics exclude nothing.
    pub(crate) async fn debug_subject_pruning_envelope(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        use crate::store::scan::{file_filter, file_reads};
        let Some(file) = self.backing_file() else {
            return Ok(None);
        };
        let pattern = QuadPattern::new(Some(subject), None, None, None);
        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            return Ok(Some(0..0));
        };
        match file_reads::build_file_filter(pattern, &mut codes)? {
            Some(filter) => file_filter::row_range_from_pruning(file, &filter).await,
            None => Ok(None),
        }
    }

    /// The exact row range the encoded chunk-probe fast path computes for a
    /// bound subject; `None` when the fast path would not engage (off-file,
    /// unsorted file, unsupported layout, unknown term).
    pub(crate) async fn debug_subject_chunk_probe_range(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        use crate::store::scan::file_reads;
        let Some(file) = self.backing_file() else {
            return Ok(None);
        };
        let Some(mut codes) = self
            .prepared_codes(QuadPattern::new(Some(subject), None, None, None))
            .await?
        else {
            return Ok(None);
        };
        file_reads::locate_subject_run(file, &mut codes, subject).await
    }

    /// The index-child run the reference index's file resolution locates for
    /// a predicate/object pattern; `None` when the location declines and the
    /// resolution falls back to its pushed-down scan.
    pub(crate) async fn debug_reference_index_located_run(
        &self,
        predicate: Option<&oxrdf::NamedNode>,
        object: Option<&oxrdf::Term>,
    ) -> Result<Option<Range<u64>>> {
        let Some(file) = self.backing_file() else {
            return Ok(None);
        };
        let pattern = QuadPattern::new(None, predicate, object, None);
        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            return Ok(None);
        };
        crate::store::indexes::reference::debug_located_run(file, pattern, &mut codes).await
    }
}

/// Whether every non-nullable integer child of `struct_arr` is a canonical
/// primitive — the shared predicate behind the resident-adoption hooks.
fn debug_int_children_canonical(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::Primitive;
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        let int = child.dtype().is_int() && !child.dtype().is_nullable();
        !int || child.clone().try_downcast::<Primitive>().is_ok()
    })
}

/// Whether every sorted-stamped child of `struct_arr` binds an encoded
/// search probe — the property that keeps every bounds search off the
/// generic per-scalar kernel, whether the child is canonical or
/// wire-encoded. Unsorted children never take bounds searches, so they are
/// not constrained.
fn debug_sorted_children_probe_resolvable(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        !array::column_is_sorted(child)
            || vortex_rdf_encoded_search::SortedProbe::resolve(child).is_some()
    })
}
