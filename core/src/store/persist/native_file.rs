//! The opened native store file: the runtime handle the store's file-backed
//! query paths drive. Everything it holds is query-execution state:
//! memoized splits, per-filter pruning envelopes (whose envelope semantics
//! [`file_scan`](crate::store::scan::file_scan) defines and consumes), and cached
//! component readers. The pure open/materialize primitives are in
//! [`io::read`](crate::io::read).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use vortex_array::expr::{BoundExpression, Expression};
use vortex_error::VortexResult;
use vortex_layout::{LayoutReaderRef, LayoutRef};

use crate::io::container::{
    RdfStoreLayoutVTable, StoreComponentDescriptor, is_native_file, quads_sorted, store_component,
    store_components, subtree_bytes,
};
use crate::io::read::unsupported_file_error;

/// An opened native store file: the [`vortex_file::VortexFile`] plus its
/// component inventory and per-component reader cache.
///
/// Derefs to the inner file, whose root reader delegates to the transparent
/// quad-source child — so scans, splits, row counts, and pruning all speak
/// quad coordinates, exactly like a plain quad table. Component readers are
/// built once and cached, so their zone-map stats decode once per store, not
/// per query (the auxiliary analogue of `with_layout_reader_cache`).
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
    /// Per-column chunk-probe handles keyed by column name (`None` memoizes
    /// a column whose layout shape or dtype declines), resolved once per open
    /// file. Each handle caches fetched chunk probes internally, so repeated
    /// bound-subject queries and point reads touch segments once.
    column_chunks: Mutex<ColumnChunksMemo>,
    /// One bound tree per (scope, filter shape), held for the handle's
    /// lifetime — see [`BoundExprMemo`].
    bound_exprs: Arc<BoundExprMemo>,
}

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

/// Memoized per-column chunk-probe handles; see
/// [`NativeStoreFile::column_chunks`].
type ColumnChunksMemo = HashMap<String, Option<Arc<vortex_rdf_encoded_search::ColumnChunks>>>;

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
    pub(crate) fn try_new(file: vortex_file::VortexFile) -> crate::error::Result<Self> {
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
            column_chunks: Mutex::new(HashMap::new()),
            bound_exprs: Arc::new(BoundExprMemo::new()),
        })
    }

    /// The handle's bound-expression memo — shared (`Arc`) so serve plans
    /// and deferred row-id sources outliving a borrow can keep binding
    /// through it.
    pub(crate) fn bound_exprs(&self) -> &Arc<BoundExprMemo> {
        &self.bound_exprs
    }

    /// A quad column's chunk-probe handle by name, for point reads and exact
    /// bound-term row ranges through the wire-encoded chunks. `None`
    /// (memoized) when the quad child's layout shape or that column's dtype
    /// declines — callers then keep the scan path.
    pub(crate) fn column_chunks(
        &self,
        column: &str,
    ) -> Option<Arc<vortex_rdf_encoded_search::ColumnChunks>> {
        let mut memo = self.column_chunks.lock().expect("column chunks lock");
        memo.entry(column.to_owned())
            .or_insert_with(|| {
                let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
                let quads = typed.slot(0).ok().flatten()?;
                vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&quads, column)
                    .map(Arc::new)
            })
            .clone()
    }

    /// An index component column's chunk-probe handle, the auxiliary-child
    /// counterpart of [`column_chunks`](Self::column_chunks) — for locating
    /// matched runs and point-reading served rows through the component's
    /// wire-encoded chunks. `None` (memoized) on any decline.
    pub(crate) fn component_column_chunks(
        &self,
        component: &str,
        column: &str,
    ) -> Option<Arc<vortex_rdf_encoded_search::ColumnChunks>> {
        // The `/` separator cannot appear in a bare quad column name, so the
        // composite keys share the quad columns' memo without collisions.
        let key = format!("{component}/{column}");
        let mut memo = self.column_chunks.lock().expect("column chunks lock");
        memo.entry(key)
            .or_insert_with(|| {
                let (_, child) = self.component_child(component).ok().flatten()?;
                vortex_rdf_encoded_search::ColumnChunks::from_struct_layout(&child, column)
                    .map(Arc::new)
            })
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

    /// A component's on-disk byte size, by name — the residency-threshold
    /// input.
    pub(crate) fn component_bytes(&self, name: &str) -> VortexResult<Option<u64>> {
        let Some((_, child)) = self.component_child(name)? else {
            return Ok(None);
        };
        subtree_bytes(&child, self.file.footer().segment_map()).map(Some)
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
}
