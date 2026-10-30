//! Serialization: a store's parts (rows, components and dictionary), which
//! `to_bytes` writes and the in-memory open (`from_file_in_memory`) adopts.

use crate::error::Result;
use crate::store::Backend;
#[cfg(feature = "file-io")]
use crate::store::FileBase;
use crate::store::builders::BuiltArray;
use crate::store::layouts::ResolvedLayout;

use crate::store::VortexRdfStore;
#[cfg(feature = "file-io")]
use crate::store::persist::open::scanned_index_components;

impl BuiltArray {
    /// These parts as a single-chunk [`BuiltStream`] for the writer: the rows
    /// as the one chunk, each component as a write, the dictionary beside
    /// them, and the rows' subject stamp as the root's `quads_sorted`. A
    /// Dictionary-layout primary comes with its dictionary
    /// (`to_serializable_parts` always pairs them). Refuses while the
    /// experimental patched-array switch is on
    /// ([`refuse_experimental_patches`]) before it packs any component, so a
    /// write that cannot happen materializes no deferred index child.
    ///
    /// [`BuiltStream`]: crate::store::builders::BuiltStream
    /// [`refuse_experimental_patches`]: crate::io::write::refuse_experimental_patches
    pub(crate) fn into_stream(self) -> Result<crate::store::builders::BuiltStream> {
        use crate::store::array::subject_sorted;
        use crate::store::indexes::IndexComponent;
        use crate::store::layouts::LayoutStrategy;
        use futures::StreamExt as _;

        crate::io::write::refuse_experimental_patches()?;
        let BuiltArray {
            array,
            components,
            dict,
        } = self;
        debug_assert!(
            !matches!(
                LayoutStrategy::from_dtype(array.dtype()),
                LayoutStrategy::Dictionary
            ) || dict.is_some(),
            "to_serializable_parts always pairs a Dictionary primary with its dictionary"
        );
        let components = components
            .iter()
            .map(IndexComponent::to_stream)
            .collect::<Result<Vec<_>>>()?;
        let quads_sorted = subject_sorted(&array);
        Ok(crate::store::builders::BuiltStream {
            dtype: array.dtype().clone(),
            chunks: futures::stream::once(async move { Ok(array) }).boxed(),
            components,
            quads_sorted,
            dict,
        })
    }
}

impl VortexRdfStore {
    /// This store's serializable parts, as a [`BuiltArray`]: the rows this
    /// view covers, base and tail combined, as one array of primary columns,
    /// their sortedness traveling as their `s` column's stamp, the index
    /// components describing them and, under the Dictionary layout, the term
    /// dictionary their codes address. `to_bytes` writes them
    /// ([`BuiltArray::into_stream`]) and the in-memory open
    /// (`from_file_in_memory`) adopts them back
    /// ([`from_parts`](Self::from_parts)).
    ///
    /// Components come only when their `rid`s address the returned rows: an
    /// unrefined, untombstoned view without a tail passes its components
    /// through (in memory) or lifts its index children (file); a view with a
    /// tail, or an unrefined tombstoned one with indexes, rebuilds them over
    /// the surviving rows (the held components' rids predate the mutation);
    /// any other narrowed view returns none, its gathered rows being
    /// renumbered (re-indexing a view is compaction's job).
    ///
    /// A rebuild re-emits the surviving rows in `(s, p, o, g)` order, each
    /// once (`sorted_live_raw_quads`, as compaction does), so the output
    /// carries the subject stamp: serialization preserves the store's quads,
    /// not their row numbering. Under the Dictionary layout it re-encodes them
    /// against a fresh dictionary, which the parts carry (the cached one
    /// predates the tail). A view that does not rebuild carries the store's
    /// own dictionary, a file-backed one lifted resident for the write, and
    /// its rows are read alone (`base_read`), which never materializes
    /// components.
    pub(crate) async fn to_serializable_parts(&self) -> Result<BuiltArray> {
        let owner_shaped = self.quads.is_unrefined();
        let tombstoned = self.quads.deleted.is_some();
        let rebuild =
            self.tail.is_some() || (owner_shaped && tombstoned && !self.indexes.is_empty());
        if rebuild {
            // A rebuild re-emits every surviving row in (s, p, o, g) order,
            // each once, as every builder's output is, so the written
            // artifact carries the subject stamp and readers keep the
            // subject binary search and, on a file, the subject chunk probe.
            return self.rebuilt_parts(&self.indexes).await;
        }
        let base = self.base_selected_rows().await?;
        let components = if owner_shaped && !tombstoned {
            match &self.quads.backend {
                Backend::InMemory(memory) => memory.components.to_vec(),
                // An unrefined file view reads its index children
                // wholesale, so a file-backed store's serialization (and
                // its in-memory open) keeps its indexes.
                #[cfg(feature = "file-io")]
                Backend::File(FileBase { file, .. }) => scanned_index_components(file).await?,
            }
        } else {
            Vec::new()
        };
        let dict = match &self.layout {
            ResolvedLayout::Dictionary(access) => Some(access.ensure_resident().await?),
            _ => None,
        };
        Ok(BuiltArray {
            array: base,
            components,
            dict,
        })
    }

    /// This store as native-container bytes, the bindings' exchange format:
    /// the quad table as the transparent root child, the dictionary and index
    /// children as auxiliary children. Read back with
    /// [`from_bytes`](Self::from_bytes), or written as a `.vortex` file.
    ///
    /// A store opened from a file it has not changed since (no tail, no
    /// tombstone, no narrowing) is that file: its mapped bytes are copied as
    /// they are, with no row read or written.
    pub async fn to_bytes(&self) -> Result<Vec<u8>> {
        #[cfg(feature = "file-io")]
        if let Some(bytes) = self.untouched_file_bytes() {
            #[cfg(test)]
            serialize_hooks::note_copied();
            return Ok(bytes.as_slice().to_vec());
        }
        let parts = self.to_serializable_parts().await?;
        let start = crate::debug::timer();
        let mut bytes = Vec::new();
        crate::io::write::built_stream_to_vortex_writer(parts.into_stream()?, &mut bytes).await?;
        log::debug!(
            "[persist::serialize::to_bytes] Vortex writing took {:?}",
            crate::debug::elapsed(start)
        );
        Ok(bytes)
    }

    /// The bytes of the file this store was opened from, while the store is
    /// still exactly that file's: a mapped file's base with no pushed-down
    /// filter, an all-rows selection, no tombstone and no tail. `None`
    /// otherwise, and for a file not mapped.
    #[cfg(feature = "file-io")]
    fn untouched_file_bytes(&self) -> Option<&vortex_buffer::ByteBuffer> {
        let Backend::File(FileBase { file, .. }) = &self.quads.backend else {
            return None;
        };
        let untouched =
            self.quads.is_unrefined() && self.quads.deleted.is_none() && self.tail.is_none();
        if untouched { file.bytes() } else { None }
    }
}

/// Test hook: how many `to_bytes` calls this thread answered by copying the
/// store's file.
#[cfg(test)]
pub(crate) mod serialize_hooks {
    use std::cell::Cell;

    thread_local! {
        static COPIED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one store serialized as a copy of its file.
    pub(super) fn note_copied() {
        COPIED.set(COPIED.get() + 1);
    }

    /// The stores serialized as copies of their files on this thread so far.
    pub(crate) fn copied() -> usize {
        COPIED.get()
    }
}
