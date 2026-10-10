//! The opened native store file: the runtime handle the store's file-backed
//! query paths drive. It holds what is fixed per file — the component
//! inventory, one reader per component, the quad table's split ranges, one
//! chunk-probe handle per column — and two bounded memos that let Vortex's
//! identity-keyed reader caches hit: pruning envelopes per filter shape, and
//! bound filter trees. The pure open/materialize primitives are in
//! [`io::read`](crate::io::read).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use vortex_array::expr::{BoundExpression, Expression};
use vortex_error::VortexResult;
use vortex_layout::{LayoutReaderRef, LayoutRef};

use crate::io::container::{
    RdfStoreLayoutVTable, StoreComponentDescriptor, is_native_file, quads_sorted, store_component,
    store_components,
};
use crate::io::read::{FileIdentity, OpenedFile, unsupported_file_error};

/// An opened native store file: the [`vortex_file::VortexFile`] plus its
/// component inventory and per-component reader cache.
///
/// Derefs to the inner file, whose root reader delegates to the transparent
/// quad-source child — so scans, splits, row counts, and pruning all speak
/// quad coordinates, exactly like a plain quad table. Component readers are
/// built once and cached, so their zone-map stats decode once per store.
pub(crate) struct NativeStoreFile {
    file: vortex_file::VortexFile,
    components: Vec<StoreComponentDescriptor>,
    /// The root metadata's `quads_sorted` provenance (see `WireMetadata`),
    /// captured at open so read paths can restore the subject stamp on
    /// materialized rows without re-walking the layout.
    quads_sorted: bool,
    child_readers: Vec<OnceLock<LayoutReaderRef>>,
    /// The quad table's natural split ranges, computed once — every
    /// counting/matching call iterates them, and deriving them walks the
    /// layout tree.
    splits: OnceLock<Arc<[Range<u64>]>>,
    /// Statistics-only pruning envelopes keyed by filter shape — the
    /// expression itself, whose `Eq`/`Hash` are structural (fn id + options
    /// per node), so a hit costs a tree walk but no allocation: the
    /// repeated-pattern workloads the bindings serve (e.g. rdflib joins)
    /// re-ask the same handful of filters, and each envelope costs a pruning
    /// evaluation over every zone. Bounded by [`PRUNING_MEMO_MAX`].
    pruning_envelopes: BoundedMemo<Expression, Option<Range<u64>>>,
    /// Per-column chunk-probe handles, keyed by quad column (an index
    /// component's column as `component/column`); `None` memoizes a decline.
    /// Each handle keeps the probes of the leaves it fetched, so a repeated
    /// subject or index-run location skips the layout walk and the leaf
    /// rebuilds. Bounded by the file's layout: one handle per column, and per
    /// fetched leaf a probe over mapped bytes.
    column_chunks: Mutex<ChunkHandles>,
    /// One bound tree per (scope, filter shape), held for the handle's
    /// lifetime — see [`BoundExprMemo`].
    bound_exprs: Arc<BoundExprMemo>,
    /// Whether the file is read through a memory mapping, as the open found
    /// it.
    mapped: bool,
    /// The identity of the file this handle maps, where the platform has one.
    identity: Option<FileIdentity>,
    /// How many times a quad column has been streamed through a scan for a
    /// keep (test hook: pins which keeps were served without reading a column).
    #[cfg(test)]
    column_streams: std::sync::atomic::AtomicUsize,
    /// How many row ids reads of located index-child runs have asked for
    /// (test hook: pins that a count reads none and a window only its own
    /// rows).
    #[cfg(test)]
    located_rid_reads: std::sync::atomic::AtomicUsize,
}

/// The chunk-probe handles of a file, by quad column and by (component,
/// column); `None` memoizes a decline.
#[derive(Default)]
struct ChunkHandles {
    quads: HashMap<String, ColumnHandle>,
    components: HashMap<String, HashMap<String, ColumnHandle>>,
}

type ColumnHandle = Option<Arc<vortex_rdf_encoded_search::ColumnChunks>>;

/// Structural (scope, expression) → the one [`BoundExpression`] this file
/// hands out for that shape.
///
/// Vortex keys its reader-side pruning and evaluation caches by bound-tree
/// *identity* (`ExactBoundExpr` compares the children `Arc` pointer), not
/// structure, and those caches live as long as the cached reader tree — the
/// handle's lifetime. A fresh `bind` per call would never hit them and grow
/// them per call; this memo pins one identity per shape so repeats, across
/// splits and across calls, land on the entries the first use created. A
/// clone of a memoized tree shares its `Arc`s and therefore its identity.
/// The scope tag separates trees bound against different schemas (the quad
/// root vs. an index child). Bounded by [`BIND_MEMO_MAX`].
pub(crate) struct BoundExprMemo(BoundedMemo<(&'static str, Expression), BoundExpression>);

/// Entry cap on [`BoundExprMemo`] — sized for a query workload's distinct
/// filter shapes, not for arbitrary term churn.
const BIND_MEMO_MAX: usize = 4096;

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

    /// Entries held — what the tests read to pin which shapes get bound.
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
}

impl<K: std::hash::Hash + Eq, V: Clone> BoundedMemo<K, V> {
    fn new(cap: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            cap,
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.map.lock().expect("memo lock").get(key).cloned()
    }

    fn insert(&self, key: K, value: V) {
        let mut map = self.map.lock().expect("memo lock");
        Self::insert_capped(&mut map, self.cap, key, value);
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
        Self::insert_capped(&mut map, self.cap, key, value.clone());
        Ok(value)
    }

    fn insert_capped(map: &mut HashMap<K, V>, cap: usize, key: K, value: V) {
        if map.len() >= cap {
            map.clear();
        }
        map.insert(key, value);
    }
}

/// Entry cap on [`NativeStoreFile::pruning_envelopes`] — sized for a query
/// workload's distinct filter shapes, not for arbitrary term churn.
const PRUNING_MEMO_MAX: usize = 512;

impl std::ops::Deref for NativeStoreFile {
    type Target = vortex_file::VortexFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl NativeStoreFile {
    /// Wrap an opened file, requiring the native store root — the one place
    /// a file's root layout is checked on the open path.
    pub(crate) fn try_new(opened: impl Into<OpenedFile>) -> crate::error::Result<Self> {
        let OpenedFile {
            file,
            mapped,
            identity,
        } = opened.into();
        if !is_native_file(&file) {
            return Err(unsupported_file_error(&file));
        }
        let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let components = store_components(typed).to_vec();
        let quads_sorted = quads_sorted(typed);
        let child_readers = components.iter().map(|_| OnceLock::new()).collect();
        Ok(Self {
            file,
            components,
            quads_sorted,
            child_readers,
            splits: OnceLock::new(),
            pruning_envelopes: BoundedMemo::new(PRUNING_MEMO_MAX),
            column_chunks: Mutex::new(ChunkHandles::default()),
            bound_exprs: Arc::new(BoundExprMemo::new()),
            mapped,
            identity,
            #[cfg(test)]
            column_streams: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            located_rid_reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The handle's bound-expression memo — shared (`Arc`) so serve plans
    /// and deferred row-id sources outliving a borrow can keep binding
    /// through it.
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

    /// Whether the file is read through a memory mapping.
    pub(crate) fn is_mapped(&self) -> bool {
        self.mapped
    }

    /// The identity of the mapped file, to tell whether its path still names it.
    pub(crate) fn identity(&self) -> Option<FileIdentity> {
        self.identity
    }

    /// A quad column's chunk-probe handle by name, for point reads and exact
    /// bound-term row ranges through the wire-encoded chunks — built from the
    /// layout on first use and memoized for the handle's lifetime, so the
    /// leaves the handle fetched stay probed between calls. `None` when the
    /// quad child's layout shape or that column's dtype declines (memoized
    /// too); callers keep the scan.
    pub(crate) fn column_chunks(&self, column: &str) -> ColumnHandle {
        self.memoized_quad_chunks(column, || {
            let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
            let quads = typed.slot(0).ok().flatten()?;
            vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&quads, column)
                .map(Arc::new)
        })
    }

    /// An index component column's chunk-probe handle, the auxiliary-child
    /// counterpart of [`column_chunks`](Self::column_chunks), memoized alike
    /// per (component, column). `None` on any decline.
    pub(crate) fn component_column_chunks(&self, component: &str, column: &str) -> ColumnHandle {
        self.memoized_component_chunks(component, column, || {
            let (_, child) = self.component_child(component).ok().flatten()?;
            vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&child, column)
                .map(Arc::new)
        })
    }

    /// The memo's guard. Every entry is complete when it is inserted, so a
    /// lock poisoned by a panic elsewhere guards consistent entries and is
    /// taken over.
    fn chunk_handles(&self) -> std::sync::MutexGuard<'_, ChunkHandles> {
        self.column_chunks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `column`'s memoized handle, from `build` on a miss. The lookup borrows
    /// the key; `build` runs with the lock released, so two racing misses may
    /// both build and the first insert wins.
    fn memoized_quad_chunks(
        &self,
        column: &str,
        build: impl FnOnce() -> ColumnHandle,
    ) -> ColumnHandle {
        if let Some(found) = self.chunk_handles().quads.get(column) {
            return found.clone();
        }
        let built = build();
        self.chunk_handles()
            .quads
            .entry(column.to_owned())
            .or_insert(built)
            .clone()
    }

    /// [`memoized_quad_chunks`](Self::memoized_quad_chunks) for a component's
    /// column.
    fn memoized_component_chunks(
        &self,
        component: &str,
        column: &str,
        build: impl FnOnce() -> ColumnHandle,
    ) -> ColumnHandle {
        if let Some(found) = self
            .chunk_handles()
            .components
            .get(component)
            .and_then(|columns| columns.get(column))
        {
            return found.clone();
        }
        let built = build();
        self.chunk_handles()
            .components
            .entry(component.to_owned())
            .or_default()
            .entry(column.to_owned())
            .or_insert(built)
            .clone()
    }

    /// The quad table's natural splits, memoized. Shadows the inner file's
    /// `splits()` (which recomputes from the layout tree per call).
    pub(crate) fn splits(&self) -> VortexResult<Arc<[Range<u64>]>> {
        if let Some(splits) = self.splits.get() {
            return Ok(Arc::clone(splits));
        }
        let computed: Arc<[Range<u64>]> = self.file.splits()?.into();
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

    /// Whether the file records its quad rows as globally `s`-sorted.
    pub(crate) fn quads_sorted(&self) -> bool {
        self.quads_sorted
    }

    /// The persisted component inventory (auxiliary children only).
    pub(crate) fn components(&self) -> &[StoreComponentDescriptor] {
        &self.components
    }

    /// A component's slot in the inventory and its child layout, by name.
    fn component_child(&self, name: &str) -> VortexResult<Option<(usize, LayoutRef)>> {
        let Some(index) = self.components.iter().position(|c| c.name == name) else {
            return Ok(None);
        };
        let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let (_, child) = store_component(typed, name)?
            .ok_or_else(|| vortex_error::vortex_err!("store component {name} has no child"))?;
        Ok(Some((index, child)))
    }

    /// A component's child layout, by name.
    pub(crate) fn component_layout(&self, name: &str) -> VortexResult<Option<LayoutRef>> {
        Ok(self.component_child(name)?.map(|(_, child)| child))
    }

    /// A component's descriptor and cached reader, by name.
    pub(crate) fn component_reader(
        &self,
        name: &str,
    ) -> VortexResult<Option<(&StoreComponentDescriptor, LayoutReaderRef)>> {
        let Some((index, child)) = self.component_child(name)? else {
            return Ok(None);
        };
        if self.child_readers[index].get().is_none() {
            let reader = child.new_reader(
                self.components[index].name.as_str().into(),
                self.file.segment_source(),
                self.file.session(),
                &Default::default(),
            )?;
            let _ = self.child_readers[index].set(reader);
        }
        Ok(Some((
            &self.components[index],
            self.child_readers[index]
                .get()
                .expect("the reader was just initialized above")
                .clone(),
        )))
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
        use crate::io::read::{FileAccess, open_vortex_file};

        let quads = crate::tests::modular_quads_for_tests(50);
        let (_dir, path) =
            crate::tests::write_store_file_for_tests(quads, vec![IndexType::SecondaryByReference])
                .await;
        let native =
            NativeStoreFile::try_new(open_vortex_file(&path, FileAccess::Mapped).await.unwrap())
                .unwrap();

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
        assert!(format!("{again:?}").contains("fetched: 1"), "{again:?}");

        // A decline is memoized as well, for both kinds of key.
        assert!(native.column_chunks("no-such-column").is_none());
        assert!(
            native
                .component_column_chunks("no-such-component", "val")
                .is_none()
        );
        let memo = native.column_chunks.lock().unwrap();
        assert!(matches!(memo.quads.get("no-such-column"), Some(None)));
        assert!(matches!(
            memo.components
                .get("no-such-component")
                .and_then(|columns| columns.get("val")),
            Some(None)
        ));
    }

    async fn mapped_native() -> (tempfile::TempDir, NativeStoreFile) {
        use crate::io::read::{FileAccess, open_vortex_file};

        let quads = crate::tests::modular_quads_for_tests(50);
        let (dir, path) = crate::tests::write_store_file_for_tests(quads, vec![]).await;
        let native =
            NativeStoreFile::try_new(open_vortex_file(&path, FileAccess::Mapped).await.unwrap())
                .unwrap();
        (dir, native)
    }

    /// A handle is built with the memo's lock released, and a hit builds
    /// nothing.
    #[tokio::test]
    async fn chunk_handles_are_built_outside_the_lock() {
        let (_dir, native) = mapped_native().await;

        let built = native.memoized_quad_chunks("probe", || {
            assert!(
                native.column_chunks.try_lock().is_ok(),
                "the handle was built under the memo's lock"
            );
            None
        });
        assert!(built.is_none());
        let hit = native.memoized_quad_chunks("probe", || panic!("a hit does not rebuild"));
        assert!(hit.is_none());

        native.memoized_component_chunks("index:x", "val", || {
            assert!(
                native.column_chunks.try_lock().is_ok(),
                "the component handle was built under the memo's lock"
            );
            None
        });
        native.memoized_component_chunks("index:x", "val", || panic!("a hit does not rebuild"));
    }

    /// A panic while the memo's lock is held leaves the memo usable: its
    /// entries are whole, so the next lookup takes the poisoned lock over.
    #[tokio::test]
    async fn a_poisoned_chunk_handle_lock_is_recovered() {
        let (_dir, native) = mapped_native().await;
        let native = Arc::new(native);
        let before = native
            .column_chunks("s")
            .expect("the subject column resolves");

        let poisoner = Arc::clone(&native);
        let result = std::thread::spawn(move || {
            let _held = poisoner.column_chunks.lock().unwrap();
            panic!("poison the memo");
        })
        .join();
        assert!(result.is_err());
        assert!(native.column_chunks.is_poisoned());

        let after = native
            .column_chunks("s")
            .expect("the subject column still resolves");
        assert!(Arc::ptr_eq(&before, &after));
        assert!(native.column_chunks("p").is_some());
        assert!(native.component_column_chunks("nothing", "val").is_none());
    }
}
