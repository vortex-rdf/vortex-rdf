//! The opened native store file: the runtime handle the store's file-backed
//! query paths drive. It holds what is fixed per file (the component roster
//! classified at open, the quad table's split ranges, one chunk-probe handle
//! per column), the reader tree every scan and probe reads through, and two
//! bounded memos: pruning envelopes per filter shape, and bound filter trees.
//! Vortex keys its reader caches by the identity of the bound trees, so the
//! reader tree is retired whenever the bound-tree memo clears.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;
use vortex_array::expr::{BoundExpression, Expression, root, select};
use vortex_error::VortexResult;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::{Layout, LayoutReaderRef, LayoutRef};

use crate::io::container::{self, RdfStoreLayoutVTable};
use crate::io::file::{FileIdentity, OpenedFile};
use crate::store::persist::open::{ComponentRoster, IndexChild};
use crate::store::schema;

/// An opened native store file: the [`vortex_file::VortexFile`] plus its
/// classified component roster and reader tree.
///
/// Derefs to the inner file for its footer, dtype, row count, segment source
/// and session. The inner file keeps no reader of its own: scans and probes
/// take theirs from the reader tree ([`layout_reader`](Self::layout_reader),
/// [`scan`](Self::scan), [`component_reader`](Self::component_reader)). The
/// root reader delegates to the transparent quad-source child, so scans,
/// splits, row counts and pruning all speak quad coordinates, as over a plain
/// quad table. Readers are built once per tree and cached, so their zone-map
/// stats decode once per tree.
pub(crate) struct NativeStoreFile {
    file: vortex_file::VortexFile,
    /// The component roster, classified once at open: the dictionary child's
    /// slot and the known index children, the only children the handle
    /// reaches.
    roster: ComponentRoster,
    /// The root metadata's `quads_sorted` provenance (see `WireMetadata`),
    /// captured at open so read paths can restore the subject stamp on
    /// materialized rows without re-walking the layout.
    quads_sorted: bool,
    /// The reader tree scans and probes read through; replaced when the bound
    /// tree memo clears (see [`ReaderTree`]).
    readers: Mutex<Arc<ReaderTree>>,
    /// The quad table's natural split ranges, computed once: every count and
    /// match iterates them, and deriving them walks the layout tree.
    splits: OnceLock<Arc<[Range<u64>]>>,
    /// Statistics-only pruning envelopes keyed by filter shape: the
    /// expression itself, whose `Eq`/`Hash` are structural (fn id and options
    /// per node). An envelope costs a pruning evaluation over every zone, and
    /// the bindings' repeated-pattern workloads (e.g. rdflib joins) re-ask the
    /// same few filters; keyed this way a hit costs a tree walk but no
    /// allocation. Bounded by [`PRUNING_MEMO_MAX`].
    pruning_envelopes: BoundedMemo<Expression, Option<Range<u64>>>,
    /// The quad table's chunk-probe handles, by column (see [`ChunkMemo`]),
    /// but for the code columns', which
    /// [`code_column_chunks`](Self::code_column_chunks) holds. Each handle
    /// keeps the probes of the leaves it fetched, so a repeated subject or
    /// index-run location skips the layout walk and the leaf rebuilds.
    /// Bounded by the file's layout: one handle per column, and per fetched
    /// leaf a probe over mapped bytes.
    quad_chunks: ChunkMemo<()>,
    /// The `s`, `p`, `o` and `g` columns' handles, one slot each, held as
    /// [`quad_chunks`](Self::quad_chunks) holds the others': the lookups
    /// every read by id makes take no lock. Racing first lookups of a slot
    /// wait for its one build (`OnceLock`), where the memo builds outside its
    /// lock and keeps the first insert.
    code_column_chunks: [OnceLock<ColumnHandle>; 4],
    /// The index components' chunk-probe handles, by component name and
    /// column, held and bounded as the quad table's are.
    component_chunks: ChunkMemo<String>,
    /// The located-run tables' four code-column handles, resolved once (see
    /// `located_tables`).
    located: OnceLock<LocatedTables>,
    /// One bound tree per (scope, filter shape): see [`BoundExprMemo`].
    bound_exprs: Arc<BoundExprMemo>,
    /// Whether the file is read through a memory mapping, as the open found
    /// it.
    mapped: bool,
    /// The identity of the file this handle maps, where the platform has one.
    identity: Option<FileIdentity>,
    /// The absolute path the file was opened from, which an owner's
    /// compaction rewrites its rows over; none for a file opened over a
    /// buffer.
    path: Option<PathBuf>,
    /// How many times a quad column has been streamed through a scan for a
    /// keep (test hook: pins which keeps were served without reading a column).
    #[cfg(test)]
    column_streams: std::sync::atomic::AtomicUsize,
    /// How many row ids reads of located index-child runs have asked for
    /// (test hook: pins that a count reads none and a window only its own
    /// rows).
    #[cfg(test)]
    located_rid_reads: std::sync::atomic::AtomicUsize,
    /// How many of those reads were rid-only scans rather than point reads
    /// (test hook: pins that a narrow run is point-read wherever it sits).
    #[cfg(test)]
    located_rid_scans: std::sync::atomic::AtomicUsize,
}

/// The readers of one opened file: the quad table's root reader and one
/// reader per component slot, each built on first use.
///
/// Vortex keeps caches inside a reader (zone-map pruning results,
/// partitioned expressions, dictionary evaluations) keyed by the identity of
/// the bound expressions it was asked about, and never evicts them. Every
/// bound expression comes from the file's [`BoundExprMemo`], which clears
/// wholesale at its cap, so over an endless run of new constants a tree's
/// caches would grow with each one. A tree is therefore retired when the memo
/// clears: the next reader taken comes from a fresh tree over the same file,
/// and a view still holding readers of the old tree keeps those readers, and
/// their caches, alive until it drops.
struct ReaderTree {
    /// How many times the bound tree memo had cleared when this tree was
    /// made.
    generation: u64,
    /// The file with a reader cache of its own: it builds and holds the root
    /// reader.
    root: vortex_file::VortexFile,
    components: Vec<OnceLock<LayoutReaderRef>>,
}

impl ReaderTree {
    fn new(file: &vortex_file::VortexFile, components: usize, generation: u64) -> Self {
        Self {
            generation,
            root: file.clone().with_caching(),
            components: (0..components).map(|_| OnceLock::new()).collect(),
        }
    }
}

type ColumnHandle = Option<Arc<vortex_rdf_encoded_search::ColumnChunks>>;

/// The chunk-probe handles of the tables a located run is found in, `None`
/// where one declines or the file has no such table.
pub(crate) struct LocatedTables {
    /// Per sorted table (the quad table, `index:posg`, `index:ospg`), its
    /// `(s, p, o, g)` columns.
    pub(crate) sorted: [Option<[Arc<vortex_rdf_encoded_search::ColumnChunks>; 4]>; 3],
    /// Per reference index (`index:ref-p`, `index:ref-o`), its `(val, rid)`.
    pub(crate) references: [Option<[Arc<vortex_rdf_encoded_search::ColumnChunks>; 2]>; 2],
}

/// Chunk-probe handles by table and column, each built on first use; `None`
/// memoizes a decline. `T` names the table: `()` for the quad table, the
/// component name for an index child.
///
/// A lookup borrows its key, hashed with `FxHash` (the keys are the file's
/// own table and column names, and the quad table's `()` hashes to nothing);
/// a miss builds with the lock released, so two racing misses may both build
/// and the first insert wins. Every entry is complete when it is inserted, so
/// a lock poisoned by a panic elsewhere guards consistent entries and is taken
/// over.
#[derive(Default)]
struct ChunkMemo<T>(Mutex<FxHashMap<T, FxHashMap<String, ColumnHandle>>>);

impl<T: Hash + Eq> ChunkMemo<T> {
    /// `table`'s `column` handle, from `build` on a miss.
    fn get_or_build<Q>(
        &self,
        table: &Q,
        column: &str,
        build: impl FnOnce() -> ColumnHandle,
    ) -> ColumnHandle
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        if let Some(found) = self
            .handles()
            .get(table)
            .and_then(|columns| columns.get(column))
        {
            return found.clone();
        }
        let built = build();
        self.handles()
            .entry(table.to_owned())
            .or_default()
            .entry(column.to_owned())
            .or_insert(built)
            .clone()
    }

    fn handles(&self) -> MutexGuard<'_, FxHashMap<T, FxHashMap<String, ColumnHandle>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Structural (scope, expression) → the one [`BoundExpression`] this file
/// hands out for that shape.
///
/// Vortex keys its reader-side pruning and evaluation caches by bound-tree
/// *identity* (`ExactBoundExpr` compares the children `Arc` pointer), not
/// structure, and those caches live as long as their reader: a fresh `bind`
/// per call would miss them and grow them on every call. This memo pins one
/// identity per shape, so repeats, across splits and across calls, land on
/// the entries the first use created; a clone of a memoized tree shares its
/// `Arc`s and so its identity. The scope tag separates trees bound against
/// different schemas (the quad root vs. an index child). Bounded by
/// [`BIND_MEMO_MAX`]; each clear retires the file's reader tree
/// ([`ReaderTree`]).
pub(crate) struct BoundExprMemo(BoundedMemo<(&'static str, Expression), BoundExpression>);

/// Entry cap on [`BoundExprMemo`]: sized for a query workload's distinct
/// filter shapes, not for arbitrary term churn.
pub(crate) const BIND_MEMO_MAX: usize = 4096;

impl BoundExprMemo {
    fn new() -> Self {
        Self(BoundedMemo::new(BIND_MEMO_MAX))
    }

    /// The memoized bound form of `expr` against `dtype`, binding on first
    /// use. `scope` names the schema the dtype belongs to; the same shape
    /// bound against two scopes is two entries.
    pub(crate) fn bind(
        &self,
        scope: &'static str,
        expr: &Expression,
        dtype: &vortex_array::dtype::DType,
    ) -> VortexResult<BoundExpression> {
        self.0
            .get_or_try_insert_with((scope, expr.clone()), || expr.bind(dtype))
    }

    /// How many times the memo has cleared: the generation of the reader tree
    /// its identities belong to.
    fn clears(&self) -> u64 {
        self.0.clears.load(Ordering::Acquire)
    }

    /// Entries held: what the tests read to pin which shapes get bound.
    #[cfg(test)]
    pub(crate) fn debug_len(&self) -> usize {
        self.0.map.lock().expect("memo lock").len()
    }
}

/// A lock-guarded memo with an entry cap: once `cap` entries are held, the
/// next insert clears the map wholesale before adding its entry.
struct BoundedMemo<K, V> {
    map: Mutex<HashMap<K, V>>,
    cap: usize,
    /// How many times the map has been cleared at its cap.
    clears: AtomicU64,
}

impl<K: std::hash::Hash + Eq, V: Clone> BoundedMemo<K, V> {
    fn new(cap: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            cap,
            clears: AtomicU64::new(0),
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.map.lock().expect("memo lock").get(key).cloned()
    }

    fn insert(&self, key: K, value: V) {
        let mut map = self.map.lock().expect("memo lock");
        self.insert_capped(&mut map, key, value);
    }

    /// The value under `key`, computed by `build` and inserted on a miss;
    /// the lock is held across `build`, so a shape is built once even under
    /// concurrent misses.
    fn get_or_try_insert_with(
        &self,
        key: K,
        build: impl FnOnce() -> VortexResult<V>,
    ) -> VortexResult<V> {
        let mut map = self.map.lock().expect("memo lock");
        if let Some(value) = map.get(&key) {
            return Ok(value.clone());
        }
        let value = build()?;
        self.insert_capped(&mut map, key, value.clone());
        Ok(value)
    }

    fn insert_capped(&self, map: &mut HashMap<K, V>, key: K, value: V) {
        if map.len() >= self.cap {
            map.clear();
            self.clears.fetch_add(1, Ordering::Release);
        }
        map.insert(key, value);
    }
}

/// Entry cap on [`NativeStoreFile::pruning_envelopes`]: sized for a query
/// workload's distinct filter shapes, not for arbitrary term churn.
const PRUNING_MEMO_MAX: usize = 512;

/// One index child of the file, as the index readers use it: the child's
/// reader, its `sorted` provenance, and the file's bind memo, through which
/// every expression over the child binds under the child's name, so the eager
/// scans, the deferred row-id sources and the serve plans of one child all
/// hand its reader the same bound-tree identities (see [`BoundExprMemo`]).
/// Taken by name through [`NativeStoreFile::child_reader`].
#[derive(Clone)]
pub(crate) struct ChildReader {
    /// The component name: the child's bind scope, and the key of its
    /// columns' chunk-probe handles on the file.
    pub(crate) name: &'static str,
    /// Whether the child's rows are globally sorted by its sort keys (the
    /// descriptor's provenance), so a run of them can be located by binary
    /// search.
    pub(crate) sorted: bool,
    /// The child's reader, from the reader tree current when it was taken.
    pub(crate) reader: LayoutReaderRef,
    memo: Arc<BoundExprMemo>,
}

impl ChildReader {
    /// `expr` bound against the child's dtype through the file's memo.
    pub(crate) fn bind(&self, expr: &Expression) -> crate::error::Result<BoundExpression> {
        Ok(self.memo.bind(self.name, expr, self.reader.dtype())?)
    }

    /// A scan of the child projecting just `columns`. Restrictions (a filter,
    /// a row range) are the caller's to add.
    pub(crate) fn scan(
        &self,
        columns: &[&'static str],
    ) -> crate::error::Result<ScanBuilder<ArrayRef>> {
        let projection = self.bind(&select(columns, root()))?;
        Ok(
            ScanBuilder::new(crate::session::VORTEX_SESSION.clone(), self.reader.clone())
                .with_projection(projection),
        )
    }
}

impl std::ops::Deref for NativeStoreFile {
    type Target = vortex_file::VortexFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl NativeStoreFile {
    /// Wrap an opened file, refusing one this version cannot read and
    /// classifying its component roster once ([`ComponentRoster::of`]): the
    /// one place a file's root layout is checked on the open path. A
    /// structural refusal comes before a row-count refusal.
    pub(crate) fn try_new(opened: impl Into<OpenedFile>) -> crate::error::Result<Self> {
        let OpenedFile {
            file,
            mapped,
            identity,
            path,
        } = opened.into();
        let roster = ComponentRoster::of(&file)?;
        let data = file.footer().layout().as_::<RdfStoreLayoutVTable>().data();
        let quads_sorted = data.quads_sorted;
        let readers = Mutex::new(Arc::new(ReaderTree::new(&file, data.components.len(), 0)));
        Ok(Self {
            file,
            roster,
            quads_sorted,
            readers,
            splits: OnceLock::new(),
            pruning_envelopes: BoundedMemo::new(PRUNING_MEMO_MAX),
            quad_chunks: ChunkMemo::default(),
            code_column_chunks: Default::default(),
            component_chunks: ChunkMemo::default(),
            located: OnceLock::new(),
            bound_exprs: Arc::new(BoundExprMemo::new()),
            mapped,
            identity,
            path,
            #[cfg(test)]
            column_streams: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            located_rid_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            located_rid_scans: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The handle's bound-expression memo, shared (`Arc`) so serve plans and
    /// deferred row-id sources that outlive a borrow keep binding through it.
    pub(crate) fn bound_exprs(&self) -> &Arc<BoundExprMemo> {
        &self.bound_exprs
    }

    /// The bound-expression memo's entry count (test hook).
    #[cfg(test)]
    pub(crate) fn debug_bound_exprs(&self) -> usize {
        self.bound_exprs.debug_len()
    }

    /// Record one column stream for a keep (test hook).
    #[cfg(test)]
    pub(crate) fn note_column_stream(&self) {
        self.column_streams
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many column streams keeps have run on this handle (test hook).
    #[cfg(test)]
    pub(crate) fn debug_column_streams(&self) -> usize {
        self.column_streams
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a read of `rows` row ids from a located index-child run (test
    /// hook).
    #[cfg(test)]
    pub(crate) fn note_located_rid_reads(&self, rows: u64) {
        self.located_rid_reads
            .fetch_add(rows as usize, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many row ids reads of located runs have asked for on this handle
    /// (test hook).
    #[cfg(test)]
    pub(crate) fn debug_located_rid_reads(&self) -> usize {
        self.located_rid_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record one read of a located run's row ids by a rid-only scan (test
    /// hook).
    #[cfg(test)]
    pub(crate) fn note_located_rid_scan(&self) {
        self.located_rid_scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many reads of located runs' row ids were rid-only scans on this
    /// handle (test hook).
    #[cfg(test)]
    pub(crate) fn debug_located_rid_scans(&self) -> usize {
        self.located_rid_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether the file is read through a memory mapping.
    pub(crate) fn is_mapped(&self) -> bool {
        self.mapped
    }

    /// The identity of the mapped file, to tell whether its path still names it.
    pub(crate) fn identity(&self) -> Option<FileIdentity> {
        self.identity
    }

    /// The absolute path the file was opened from; `None` for a file opened
    /// over a buffer.
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// A quad column's chunk-probe handle by name, for point reads and exact
    /// bound-term row ranges through the wire-encoded chunks: built from the
    /// layout on first use and memoized for the handle's lifetime, so the
    /// leaves the handle fetched stay probed between calls. `None` when the
    /// quad child's layout shape or that column's dtype declines (memoized
    /// too); callers keep the scan.
    pub(crate) fn column_chunks(&self, column: &str) -> ColumnHandle {
        let build = || {
            let quads = container::quad_source(self.root()).ok()?;
            vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&quads, column)
                .map(Arc::new)
        };
        let slot = match column {
            schema::COL_S => 0,
            schema::COL_P => 1,
            schema::COL_O => 2,
            schema::COL_G => 3,
            _ => return self.quad_chunks.get_or_build(&(), column, build),
        };
        self.code_column_chunks[slot].get_or_init(build).clone()
    }

    /// An index component column's chunk-probe handle, the auxiliary-child
    /// counterpart of [`column_chunks`](Self::column_chunks), memoized alike
    /// per (component, column): the component by the name the roster
    /// classified it under (see [`component_layout`](Self::component_layout)).
    /// `None` on any decline.
    pub(crate) fn component_column_chunks(&self, component: &str, column: &str) -> ColumnHandle {
        self.component_chunks.get_or_build(component, column, || {
            let child = self.slot_layout(self.slot_of(component)?).ok()?;
            vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&child, column)
                .map(Arc::new)
        })
    }

    /// The located-run tables' handles, built by `build` on first use.
    pub(crate) fn located_tables(&self, build: impl FnOnce() -> LocatedTables) -> &LocatedTables {
        self.located.get_or_init(build)
    }

    /// The quad table's natural splits, memoized. Shadows the inner file's
    /// `splits()` (which recomputes from the layout tree per call).
    pub(crate) fn splits(&self) -> VortexResult<Arc<[Range<u64>]>> {
        if let Some(splits) = self.splits.get() {
            return Ok(Arc::clone(splits));
        }
        let computed: Arc<[Range<u64>]> = self.readers().root.splits()?.into();
        let _ = self.splits.set(Arc::clone(&computed));
        Ok(computed)
    }

    /// A memoized statistics-only pruning envelope for `filter`. The outer
    /// `Option` is a memo miss; the inner is the envelope itself, whose
    /// `None` means "nothing prunable".
    #[allow(clippy::option_option)]
    pub(crate) fn pruning_envelope(&self, filter: &Expression) -> Option<Option<Range<u64>>> {
        self.pruning_envelopes.get(filter)
    }

    /// Memoize a pruning envelope; at [`PRUNING_MEMO_MAX`] entries the memo
    /// is cleared wholesale before the insert.
    pub(crate) fn memoize_pruning_envelope(
        &self,
        filter: Expression,
        envelope: Option<Range<u64>>,
    ) {
        self.pruning_envelopes.insert(filter, envelope);
    }

    /// Whether the file records its quad rows in global `(s, p, o, g)` order.
    pub(crate) fn quads_sorted(&self) -> bool {
        self.quads_sorted
    }

    /// The known index children of the file's roster, in inventory order.
    pub(in crate::store) fn index_children(&self) -> &[IndexChild] {
        &self.roster.index_children
    }

    /// A classified component's child layout, by the name the roster knows
    /// it under: the `dictionary`, or a known index child's registry name.
    /// `None` for any other name, a component the roster skipped included.
    pub(crate) fn component_layout(&self, name: &str) -> VortexResult<Option<LayoutRef>> {
        self.slot_of(name)
            .map(|slot| self.slot_layout(slot))
            .transpose()
    }

    /// A classified component's reader from the current tree, by the name
    /// the roster knows it under (see
    /// [`component_layout`](Self::component_layout)).
    pub(crate) fn component_reader(&self, name: &str) -> VortexResult<Option<LayoutReaderRef>> {
        self.slot_of(name)
            .map(|slot| self.slot_reader(slot))
            .transpose()
    }

    /// A known index child's reader from the current tree.
    pub(super) fn index_child_reader(&self, child: &IndexChild) -> VortexResult<LayoutReaderRef> {
        self.slot_reader(child.slot)
    }

    /// The known index child under the registry name `name` as a
    /// [`ChildReader`]: its reader from the current tree, its descriptor's
    /// `sorted` provenance, and the handle's bind memo. `None` when the
    /// roster holds no such child: the file has none, or only one under an
    /// implementation slug this version does not know.
    pub(crate) fn child_reader(&self, name: &str) -> VortexResult<Option<ChildReader>> {
        let Some(child) = self.index_child(name) else {
            return Ok(None);
        };
        Ok(Some(ChildReader {
            name: child.identity.name,
            sorted: child.sorted,
            reader: self.slot_reader(child.slot)?,
            memo: Arc::clone(&self.bound_exprs),
        }))
    }

    /// The roster's known index child under the registry name `name`.
    fn index_child(&self, name: &str) -> Option<&IndexChild> {
        self.roster
            .index_children
            .iter()
            .find(|child| child.identity.name == name)
    }

    /// The slot of the classified component `name` names: the dictionary, or
    /// a known index child under its registry name.
    fn slot_of(&self, name: &str) -> Option<usize> {
        if name == container::DICT_COMPONENT_NAME {
            return self.roster.dict;
        }
        self.index_child(name).map(|child| child.slot)
    }

    /// The root layout: the native store root [`try_new`](Self::try_new)
    /// required.
    fn root(&self) -> &Layout<RdfStoreLayoutVTable> {
        self.file.footer().layout().as_::<RdfStoreLayoutVTable>()
    }

    /// The child layout of the component in `slot` of the inventory.
    fn slot_layout(&self, slot: usize) -> VortexResult<LayoutRef> {
        container::component_child(self.root(), slot)
    }

    /// The reader of the component in `slot` of the inventory, from the
    /// current tree: built on its first use there.
    fn slot_reader(&self, slot: usize) -> VortexResult<LayoutReaderRef> {
        let tree = self.readers();
        let cell = &tree.components[slot];
        if let Some(reader) = cell.get() {
            return Ok(reader.clone());
        }
        let reader = self.slot_layout(slot)?.new_reader(
            self.root().data().components[slot].name.as_str().into(),
            self.file.segment_source(),
            self.file.session(),
            &Default::default(),
        )?;
        Ok(cell.get_or_init(|| reader).clone())
    }

    /// The current reader tree: the one made for the bound tree memo's
    /// present generation, replacing a tree the memo has cleared since.
    fn readers(&self) -> Arc<ReaderTree> {
        let mut current = self
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Read under the lock: a caller that read the count before another
        // took the lock must not put an older tree back.
        let generation = self.bound_exprs.clears();
        if current.generation != generation {
            *current = Arc::new(ReaderTree::new(
                &self.file,
                self.root().data().components.len(),
                generation,
            ));
        }
        Arc::clone(&current)
    }

    /// The quad table's root reader from the current tree.
    pub(crate) fn layout_reader(&self) -> VortexResult<LayoutReaderRef> {
        self.readers().root.layout_reader()
    }

    /// A scan of the quad table over the current tree's root reader.
    pub(crate) fn scan(&self) -> VortexResult<ScanBuilder<ArrayRef>> {
        Ok(ScanBuilder::new(
            self.file.session().clone(),
            self.layout_reader()?,
        ))
    }

    /// The generation of the current reader tree (test hook).
    #[cfg(test)]
    pub(crate) fn debug_reader_generation(&self) -> u64 {
        self.readers().generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::dtype::{DType, Nullability, PType, StructFields};
    use vortex_array::expr::{ExactBoundExpr, eq, get_item, lit, root};

    /// Reaching the cap clears the memo before the next insert, so it never
    /// holds more than `cap` entries.
    #[test]
    fn bounded_memo_clears_at_cap() {
        let memo: BoundedMemo<u32, u32> = BoundedMemo::new(2);
        memo.insert(1, 10);
        memo.insert(2, 20);
        assert_eq!(memo.map.lock().unwrap().len(), 2);
        memo.insert(3, 30);
        assert_eq!(memo.map.lock().unwrap().len(), 1);
        assert_eq!(memo.get(&3), Some(30));
        assert_eq!(memo.get(&1), None);

        let built = memo.get_or_try_insert_with(4, || Ok(40)).unwrap();
        assert_eq!(built, 40);
        let hit = memo
            .get_or_try_insert_with(4, || panic!("a hit does not rebuild"))
            .unwrap();
        assert_eq!(hit, 40);
    }

    /// Two binds of one shape hand out the same tree identity, which a fresh
    /// bind of the same expression does not share.
    #[test]
    fn bound_expr_memo_pins_one_identity_per_shape() {
        let dtype = DType::Struct(
            StructFields::new(
                vec![Arc::<str>::from("s")].into(),
                vec![DType::Primitive(PType::U32, Nullability::NonNullable)],
            ),
            Nullability::NonNullable,
        );
        let expr = eq(get_item("s", root()), lit(1u32));
        let memo = BoundExprMemo::new();
        let first = memo.bind("quads", &expr, &dtype).unwrap();
        let second = memo.bind("quads", &expr, &dtype).unwrap();
        assert_eq!(ExactBoundExpr(first.clone()), ExactBoundExpr(second));

        let fresh = expr.bind(&dtype).unwrap();
        assert_eq!(fresh, first, "structurally the same tree");
        assert_ne!(ExactBoundExpr(fresh), ExactBoundExpr(first.clone()));

        let other_scope = memo.bind("index", &expr, &dtype).unwrap();
        assert_ne!(ExactBoundExpr(other_scope), ExactBoundExpr(first));
        assert_eq!(memo.0.map.lock().unwrap().len(), 2);
    }

    /// Chunk-probe handles are memoized, one per quad column and one per
    /// (index component, column): every call hands out the same handle, so
    /// the leaves it fetched stay probed between calls, and a column that
    /// declines stays declined. Bounded by the file's layout (one handle per
    /// column, and per fetched leaf a probe over mapped bytes), not by the
    /// workload.
    #[tokio::test]
    async fn column_chunks_are_memoized_per_column() {
        use crate::IndexType;

        let (_dir, native) =
            crate::tests::mapped_file(50, vec![IndexType::SecondaryByReference]).await;

        // One handle per quad column and per (component, column).
        let a = native
            .column_chunks("s")
            .expect("the subject column resolves");
        let b = native
            .column_chunks("s")
            .expect("the subject column resolves");
        let c = native
            .component_column_chunks("index:ref-p", "val")
            .expect("the reference index's value column resolves");
        let d = native
            .component_column_chunks("index:ref-p", "val")
            .expect("the reference index's value column resolves");
        let (quad_shared, component_shared) = (Arc::ptr_eq(&a, &b), Arc::ptr_eq(&c, &d));
        assert!(
            quad_shared && component_shared,
            "one handle per call site: quad column shared {quad_shared}, \
             component column shared {component_shared}"
        );

        // Distinct keys get distinct handles: another quad column, another
        // column of the component, another component's same-named column.
        let p = native
            .column_chunks("p")
            .expect("the predicate column resolves");
        let rid = native
            .component_column_chunks("index:ref-p", "rid")
            .expect("the reference index's row-id column resolves");
        let other = native
            .component_column_chunks("index:ref-o", "val")
            .expect("the other reference child's value column resolves");
        for (x, y) in [(&a, &p), (&c, &rid), (&c, &other), (&a, &c)] {
            assert!(!Arc::ptr_eq(x, y));
        }

        // The leaf a handle fetched stays probed with it: the next call
        // hands out the handle that already holds it.
        let source = native.segment_source();
        assert!(
            a.value_at(0, &source, native.session())
                .await
                .unwrap()
                .is_some()
        );
        let again = native
            .column_chunks("s")
            .expect("the subject column resolves");
        assert_eq!(again.fetched_chunks(), 1);

        // A decline is memoized as well, for both kinds of key.
        assert!(native.column_chunks("no-such-column").is_none());
        assert!(
            native
                .component_column_chunks("no-such-component", "val")
                .is_none()
        );
        assert!(matches!(
            native
                .quad_chunks
                .handles()
                .get(&())
                .and_then(|columns| columns.get("no-such-column")),
            Some(None)
        ));
        assert!(matches!(
            native
                .component_chunks
                .handles()
                .get("no-such-component")
                .and_then(|columns| columns.get("val")),
            Some(None)
        ));
    }

    /// A handle is built with the memo's lock released, and a hit builds
    /// nothing.
    #[tokio::test]
    async fn chunk_handles_are_built_outside_the_lock() {
        let (_dir, native) = crate::tests::mapped_file(50, vec![]).await;

        let built = native.quad_chunks.get_or_build(&(), "probe", || {
            assert!(
                native.quad_chunks.0.try_lock().is_ok(),
                "the handle was built under the memo's lock"
            );
            None
        });
        assert!(built.is_none());
        let hit = native
            .quad_chunks
            .get_or_build(&(), "probe", || panic!("a hit does not rebuild"));
        assert!(hit.is_none());

        native.component_chunks.get_or_build("index:x", "val", || {
            assert!(
                native.component_chunks.0.try_lock().is_ok(),
                "the component handle was built under the memo's lock"
            );
            None
        });
        native
            .component_chunks
            .get_or_build("index:x", "val", || panic!("a hit does not rebuild"));
    }

    /// A panic while a memo's lock is held leaves the memo usable: its
    /// entries are whole, so the next lookup takes the poisoned lock over,
    /// and a column it remembers is not built again. The code columns'
    /// handles, in their own slots, take no lock at all.
    #[tokio::test]
    async fn a_poisoned_chunk_handle_lock_is_recovered() {
        let (_dir, native) = crate::tests::mapped_file(50, vec![]).await;
        let native = Arc::new(native);
        let before = native
            .column_chunks("s")
            .expect("the subject column resolves");
        // A column outside the code columns' slots: the memo remembers its
        // decline.
        assert!(native.column_chunks("no-such-column").is_none());

        let poisoner = Arc::clone(&native);
        let result = std::thread::spawn(move || {
            let _quads = poisoner.quad_chunks.0.lock().unwrap();
            let _components = poisoner.component_chunks.0.lock().unwrap();
            panic!("poison the memos");
        })
        .join();
        assert!(result.is_err());
        assert!(native.quad_chunks.0.is_poisoned() && native.component_chunks.0.is_poisoned());

        assert!(
            native
                .quad_chunks
                .get_or_build(&(), "no-such-column", || panic!("a hit does not rebuild"))
                .is_none()
        );
        assert!(native.column_chunks("no-such-column").is_none());
        let after = native
            .column_chunks("s")
            .expect("the subject column still resolves");
        assert!(Arc::ptr_eq(&before, &after));
        assert!(native.column_chunks("p").is_some());
        assert!(native.component_column_chunks("nothing", "val").is_none());
    }

    /// The bound tree memo clearing retires the reader tree: up to the cap the
    /// same readers come back; the bind that clears the memo makes the next
    /// root and component readers new ones. A reader a view still holds keeps
    /// working, and keeps its caches alive until it is dropped.
    #[tokio::test]
    async fn the_reader_tree_is_retired_when_the_bind_memo_clears() {
        use crate::IndexType;

        let (_dir, native) =
            crate::tests::mapped_file(50, vec![IndexType::SecondaryByReference]).await;
        let dtype = native.dtype().clone();
        let bind = |i: u64| {
            native
                .bound_exprs()
                .bind("quads", &eq(get_item("o", root()), lit(i)), &dtype)
                .unwrap()
        };
        let component = || native.component_reader("index:ref-p").unwrap().unwrap();

        let root_reader = native.layout_reader().unwrap();
        let root_weak = Arc::downgrade(&root_reader);
        let index_reader = component();
        let index_weak = Arc::downgrade(&index_reader);

        for i in 0..BIND_MEMO_MAX as u64 {
            bind(i);
        }
        assert_eq!(native.debug_bound_exprs(), BIND_MEMO_MAX);
        assert!(Arc::ptr_eq(&root_reader, &native.layout_reader().unwrap()));
        assert!(Arc::ptr_eq(&index_reader, &component()));
        assert_eq!(native.debug_reader_generation(), 0);

        bind(BIND_MEMO_MAX as u64);
        assert_eq!(native.debug_bound_exprs(), 1);
        let next_root = native.layout_reader().unwrap();
        assert!(!Arc::ptr_eq(&root_reader, &next_root));
        assert!(!Arc::ptr_eq(&index_reader, &component()));
        assert!(Arc::ptr_eq(&next_root, &native.layout_reader().unwrap()));
        assert_eq!(native.debug_reader_generation(), 1);

        assert!(root_weak.upgrade().is_some() && index_weak.upgrade().is_some());
        assert_eq!(root_reader.row_count(), next_root.row_count());
        drop((root_reader, index_reader));
        assert!(
            root_weak.upgrade().is_none(),
            "the old root reader lives on"
        );
        assert!(
            index_weak.upgrade().is_none(),
            "the old component reader lives on"
        );
    }
}
