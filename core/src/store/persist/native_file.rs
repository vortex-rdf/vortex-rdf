//! The opened native store file: the runtime handle the file-backed query
//! paths drive, with its memoized splits, pruning envelopes, column chunk
//! handles, bound expressions and component readers.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::expr::{BoundExpression, Expression, root, select};
use vortex_error::VortexResult;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::{LayoutReaderRef, LayoutRef};
use vortex_rdf_encoded_search::ColumnChunks;

use crate::error::{Result, VortexRdfError};
use crate::io::container::{
    RdfStoreLayoutVTable, StoreComponentDescriptor, is_native_file, quads_sorted, store_component,
    store_components, subtree_bytes,
};
use crate::io::read::unsupported_file_error;
use crate::session::VORTEX_SESSION;

/// An opened native store file: the [`vortex_file::VortexFile`] plus its
/// component inventory and per-component reader cache. Derefs to the inner
/// file, whose root reader delegates to the transparent quad child, so scans,
/// splits, row counts and pruning speak quad coordinates.
pub(crate) struct NativeStoreFile {
    file: vortex_file::VortexFile,
    components: Vec<StoreComponentDescriptor>,
    /// The root metadata's `quads_sorted` provenance.
    quads_sorted: bool,
    child_readers: Vec<OnceLock<LayoutReaderRef>>,
    /// The quad table's natural split ranges.
    splits: OnceLock<Arc<[Range<u64>]>>,
    /// Statistics-only pruning envelopes keyed by filter shape, cleared at
    /// [`PRUNING_MEMO_MAX`] entries.
    pruning_envelopes: BoundedMemo<Expression, Option<Range<u64>>>,
    /// Chunk-probe handles keyed by column name (`None` memoizes a decline);
    /// each handle caches its fetched chunks.
    column_chunks: Mutex<ColumnChunksMemo>,
    bound_exprs: Arc<BoundExprMemo>,
}

/// One [`BoundExpression`] per (scope, expression shape).
///
/// Vortex keys its reader-side caches by bound-tree identity, so every bind
/// of one shape against one schema must hand out the same tree; `scope`
/// separates schemas (the quad root, each index child). Cleared at
/// [`BIND_MEMO_MAX`] entries.
pub(crate) struct BoundExprMemo(BoundedMemo<(&'static str, Expression), BoundExpression>);

/// Entry cap on [`BoundExprMemo`].
const BIND_MEMO_MAX: usize = 4096;

impl BoundExprMemo {
    fn new() -> Self {
        Self(BoundedMemo::new(BIND_MEMO_MAX))
    }

    /// The memoized bound form of `expr` against `dtype`, under `scope`.
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

/// A lock-guarded memo with an entry cap: at `cap` entries the next insert
/// clears the map first.
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

    /// The value under `key`, built and inserted on a miss; the lock is held
    /// across `build`.
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

/// Memoized chunk-probe handles by column key; see
/// [`NativeStoreFile::column_chunks`].
type ColumnChunksMemo = HashMap<String, Option<Arc<ColumnChunks>>>;

/// Entry cap on [`NativeStoreFile::pruning_envelopes`].
const PRUNING_MEMO_MAX: usize = 512;

/// A cached reader over one of the file's components, binding expressions
/// through the handle's memo under the component's own scope.
#[derive(Clone)]
pub(crate) struct ChildReader {
    /// The component name, also the bind-memo scope.
    pub(crate) name: &'static str,
    /// The descriptor's `sorted` provenance.
    pub(crate) sorted: bool,
    pub(crate) reader: LayoutReaderRef,
    memo: Arc<BoundExprMemo>,
}

impl ChildReader {
    /// `expr` bound against the child's dtype.
    pub(crate) fn bind(&self, expr: &Expression) -> Result<BoundExpression> {
        self.memo
            .bind(self.name, expr, self.reader.dtype())
            .map_err(VortexRdfError::Vortex)
    }

    /// A scan of the child projecting `columns`.
    pub(crate) fn scan(&self, columns: &[&'static str]) -> Result<ScanBuilder<ArrayRef>> {
        let projection = self.bind(&select(columns, root()))?;
        Ok(
            ScanBuilder::new(VORTEX_SESSION.clone(), self.reader.clone())
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
    /// Wrap an opened file, requiring the native store root.
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

    /// The handle's bound-expression memo.
    pub(crate) fn bound_exprs(&self) -> &Arc<BoundExprMemo> {
        &self.bound_exprs
    }

    /// The memoized chunk handle under `key` for `column` of the struct layout
    /// `layout` yields; `None` (memoized) when the layout shape or the
    /// column's dtype declines.
    fn chunks_memo(
        &self,
        key: String,
        column: &str,
        layout: impl FnOnce() -> Option<LayoutRef>,
    ) -> Option<Arc<ColumnChunks>> {
        let mut memo = self.column_chunks.lock().expect("column chunks lock");
        memo.entry(key)
            .or_insert_with(|| ColumnChunks::from_struct_layout(&layout()?, column).map(Arc::new))
            .clone()
    }

    /// A quad column's chunk-probe handle, for point reads and exact
    /// bound-term row ranges; `None` (memoized) on a decline.
    pub(crate) fn column_chunks(&self, column: &str) -> Option<Arc<ColumnChunks>> {
        self.chunks_memo(column.to_owned(), column, || {
            let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
            typed.slot(0).ok().flatten()
        })
    }

    /// An index component column's chunk-probe handle; `None` (memoized) on a
    /// decline. The `component/column` key relies on `/` never appearing in a
    /// quad column name.
    pub(crate) fn component_column_chunks(
        &self,
        component: &str,
        column: &str,
    ) -> Option<Arc<ColumnChunks>> {
        self.chunks_memo(format!("{component}/{column}"), column, || {
            self.component_child(component)
                .ok()
                .flatten()
                .map(|(_, child)| child)
        })
    }

    /// The quad table's natural splits, memoized; shadows the inner file's
    /// `splits()`.
    pub(crate) fn splits(&self) -> VortexResult<Arc<[Range<u64>]>> {
        if let Some(splits) = self.splits.get() {
            return Ok(Arc::clone(splits));
        }
        let computed: Arc<[Range<u64>]> = self.file.splits()?.into();
        let _ = self.splits.set(Arc::clone(&computed));
        Ok(computed)
    }

    /// The memoized pruning envelope for `filter`: the outer `Option` is the
    /// memo miss, the inner `None` means nothing prunable.
    #[allow(clippy::option_option)]
    pub(crate) fn pruning_envelope(&self, filter: &Expression) -> Option<Option<Range<u64>>> {
        self.pruning_envelopes.get(filter)
    }

    /// Memoize a pruning envelope.
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

    /// A component's [`ChildReader`], by name.
    pub(crate) fn child_reader(&self, name: &'static str) -> VortexResult<Option<ChildReader>> {
        Ok(self
            .component_reader(name)?
            .map(|(descriptor, reader)| ChildReader {
                name,
                sorted: descriptor.sorted,
                reader,
                memo: Arc::clone(&self.bound_exprs),
            }))
    }

    /// A component's on-disk byte size, by name.
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

    /// Reaching the cap clears the memo before the next insert.
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

    /// Two binds of one shape hand out one tree identity; a fresh bind and
    /// another scope do not share it.
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
