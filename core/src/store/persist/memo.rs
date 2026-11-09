//! The bounded and identity memos the native file handle keeps: chunk-probe
//! handles by table and column ([`ChunkMemo`]), one bound tree per filter
//! shape ([`BoundExprMemo`]), and the capped map both of the handle's
//! bounded memos are ([`BoundedMemo`]). Generic over what they memoize; the
//! handle owns when each is consulted.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rustc_hash::FxHashMap;
use vortex_array::expr::{BoundExpression, Expression};
use vortex_error::VortexResult;

/// A column's chunk-probe handle, `None` memoizing a decline.
pub(super) type ColumnHandle = Option<Arc<vortex_rdf_encoded_search::ColumnChunks>>;

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
pub(super) struct ChunkMemo<T>(pub(super) Mutex<FxHashMap<T, FxHashMap<String, ColumnHandle>>>);

impl<T: Hash + Eq> ChunkMemo<T> {
    /// `table`'s `column` handle, from `build` on a miss.
    pub(super) fn get_or_build<Q>(
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

    pub(super) fn handles(&self) -> MutexGuard<'_, FxHashMap<T, FxHashMap<String, ColumnHandle>>> {
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
    pub(super) fn new() -> Self {
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
    pub(super) fn clears(&self) -> u64 {
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
pub(super) struct BoundedMemo<K, V> {
    map: Mutex<HashMap<K, V>>,
    cap: usize,
    /// How many times the map has been cleared at its cap.
    clears: AtomicU64,
}

impl<K: std::hash::Hash + Eq, V: Clone> BoundedMemo<K, V> {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            cap,
            clears: AtomicU64::new(0),
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.map.lock().expect("memo lock").get(key).cloned()
    }

    pub(super) fn insert(&self, key: K, value: V) {
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
