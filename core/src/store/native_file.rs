//! The opened native store file: the runtime handle the store's file-backed
//! query paths drive. Everything it holds is query-execution state:
//! memoized splits, per-filter pruning envelopes (whose envelope semantics
//! [`file_scan`](super::scan::file_scan) defines and consumes), and cached
//! component readers. The pure open/materialize primitives are in
//! [`io::native_file`](crate::io::native_file).

use vortex_array::expr::Expression;
use vortex_error::{VortexResult, vortex_bail};
use vortex_layout::LayoutReaderRef;

use crate::io::container::{
    RdfStoreLayoutVTable, STORE_LAYOUT_ID, StoreComponentDescriptor, is_native_file, quads_sorted,
    store_components, subtree_bytes,
};

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
    child_readers: Vec<std::sync::OnceLock<LayoutReaderRef>>,
    /// The quad table's natural split ranges, computed once — every
    /// counting/matching call iterates them, and deriving them walks the
    /// layout tree.
    splits: std::sync::OnceLock<std::sync::Arc<[std::ops::Range<u64>]>>,
    /// Statistics-only pruning envelopes keyed by filter shape — the
    /// expression itself, whose `Eq`/`Hash` are structural (fn id + options
    /// per node), so a hit costs a tree walk but no allocation: the
    /// repeated-pattern workloads the bindings serve (e.g. rdflib joins)
    /// re-ask the same handful of filters, and each envelope costs a pruning
    /// evaluation over every zone. Bounded: cleared wholesale past
    /// [`PRUNING_MEMO_MAX`].
    pruning_envelopes:
        std::sync::Mutex<std::collections::HashMap<Expression, Option<std::ops::Range<u64>>>>,
    /// Per-column chunk-probe handles keyed by column name (`None` memoizes
    /// a column whose layout shape or dtype declines), resolved once per open
    /// file. Each handle caches fetched chunk probes internally, so repeated
    /// bound-subject queries and point reads touch segments once.
    column_chunks: std::sync::Mutex<ColumnChunksMemo>,
}

/// Memoized per-column chunk-probe handles; see
/// [`NativeStoreFile::column_chunks`].
type ColumnChunksMemo =
    std::collections::HashMap<String, Option<std::sync::Arc<vortex_encoded_search::ColumnChunks>>>;

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
    /// Wrap an opened file, requiring the native store root.
    pub(crate) fn try_new(file: vortex_file::VortexFile) -> VortexResult<Self> {
        if !is_native_file(&file) {
            vortex_bail!(
                "expected the {STORE_LAYOUT_ID} root layout, found {}",
                file.footer().layout().encoding_id()
            );
        }
        let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let components = store_components(typed).to_vec();
        let quads_sorted = quads_sorted(typed);
        let child_readers = components
            .iter()
            .map(|_| std::sync::OnceLock::new())
            .collect();
        Ok(Self {
            file,
            components,
            quads_sorted,
            child_readers,
            splits: std::sync::OnceLock::new(),
            pruning_envelopes: std::sync::Mutex::new(std::collections::HashMap::new()),
            column_chunks: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// The subject column's chunk-probe handle, for exact bound-subject row
    /// ranges via encoded chunk probes. `None` (memoized) when the quad
    /// child's layout shape declines — callers then keep the scan path.
    pub(crate) fn sorted_subject_chunks(
        &self,
    ) -> Option<std::sync::Arc<vortex_encoded_search::ColumnChunks>> {
        self.column_chunks(crate::store::schema::COL_S)
    }

    /// A quad column's chunk-probe handle by name, for point reads through
    /// the wire-encoded chunks. `None` (memoized) when the quad child's
    /// layout shape or that column's dtype declines.
    pub(crate) fn column_chunks(
        &self,
        column: &str,
    ) -> Option<std::sync::Arc<vortex_encoded_search::ColumnChunks>> {
        let mut memo = self.column_chunks.lock().expect("column chunks lock");
        memo.entry(column.to_owned())
            .or_insert_with(|| {
                let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
                let quads = typed.child(0).ok()?;
                vortex_encoded_search::ColumnChunks::from_struct_layout(&quads, column)
                    .map(std::sync::Arc::new)
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
    ) -> Option<std::sync::Arc<vortex_encoded_search::ColumnChunks>> {
        // The `/` separator cannot appear in a bare quad column name, so the
        // composite keys share the quad columns' memo without collisions.
        let key = format!("{component}/{column}");
        let mut memo = self.column_chunks.lock().expect("column chunks lock");
        memo.entry(key)
            .or_insert_with(|| {
                let index = self.components.iter().position(|c| c.name == component)?;
                let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
                let child = typed.child(index + 1).ok()?;
                vortex_encoded_search::ColumnChunks::from_struct_layout(&child, column)
                    .map(std::sync::Arc::new)
            })
            .clone()
    }

    /// The quad table's natural splits, memoized. Shadows the inner file's
    /// `splits()` (which recomputes from the layout tree per call).
    pub(crate) fn splits(&self) -> VortexResult<std::sync::Arc<[std::ops::Range<u64>]>> {
        if let Some(splits) = self.splits.get() {
            return Ok(std::sync::Arc::clone(splits));
        }
        let computed: std::sync::Arc<[std::ops::Range<u64>]> = self.file.splits()?.into();
        let _ = self.splits.set(std::sync::Arc::clone(&computed));
        Ok(computed)
    }

    /// A memoized statistics-only pruning envelope for `filter`. The outer
    /// `Option` is a memo miss; the inner is the envelope itself, whose
    /// `None` means "nothing prunable".
    #[allow(clippy::option_option)]
    pub(crate) fn pruning_envelope(
        &self,
        filter: &Expression,
    ) -> Option<Option<std::ops::Range<u64>>> {
        self.pruning_envelopes
            .lock()
            .expect("pruning memo lock")
            .get(filter)
            .cloned()
    }

    /// Memoize a pruning envelope, clearing the memo wholesale at the cap
    /// (crude, but a workload with more distinct filter shapes than the cap
    /// was not going to hit anyway).
    pub(crate) fn memoize_pruning_envelope(
        &self,
        filter: Expression,
        envelope: Option<std::ops::Range<u64>>,
    ) {
        let mut memo = self.pruning_envelopes.lock().expect("pruning memo lock");
        if memo.len() >= PRUNING_MEMO_MAX {
            memo.clear();
        }
        memo.insert(filter, envelope);
    }

    /// Whether the file records its quad rows as globally `s`-sorted.
    pub(crate) fn quads_sorted(&self) -> bool {
        self.quads_sorted
    }

    /// The persisted component inventory (auxiliary children only).
    pub(crate) fn components(&self) -> &[StoreComponentDescriptor] {
        &self.components
    }

    /// A component's descriptor, child layout, and cached reader, by name.
    pub(crate) fn component_reader(
        &self,
        name: &str,
    ) -> VortexResult<Option<(&StoreComponentDescriptor, LayoutReaderRef)>> {
        let Some(index) = self.components.iter().position(|c| c.name == name) else {
            return Ok(None);
        };
        if self.child_readers[index].get().is_none() {
            let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
            let child = typed.child(index + 1)?;
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
        let Some(index) = self.components.iter().position(|c| c.name == name) else {
            return Ok(None);
        };
        let typed = self.file.footer().layout().as_::<RdfStoreLayoutVTable>();
        let child = typed.child(index + 1)?;
        subtree_bytes(&child, self.file.footer().segment_map()).map(Some)
    }
}
