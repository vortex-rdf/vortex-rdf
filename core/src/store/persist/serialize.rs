//! Serialization: the rows-plus-components-plus-dictionary parts every write
//! path produces (`to_serializable_parts`, `to_bytes`, the bindings'
//! in-memory round-trips).

use crate::error::Result;
use crate::store::QuadsSource;
use crate::store::array::subject_sorted;
use crate::store::builders::{BuiltArray, build_parts_from_raws};
use crate::store::layouts::ResolvedLayout;

use crate::store::RawQuad;

#[cfg(feature = "file-io")]
use crate::store::persist::open::scanned_index_components;
use crate::store::{StoreParts, VortexRdfStore};

/// Put a rebuild's rows into the (s, p, o, g) order every builder emits, so
/// the array they build carries the subject sorted stamp.
///
/// The first `base_rows` entries came from the base and `base_sorted` reports
/// whether they already hold that order; the rest is the appended tail. When
/// the base was sorted, sorting the tail alone leaves two concatenated sorted
/// runs — the case `slice::sort` is documented to merge in a linear pass —
/// and the tail is small, being capped by auto-compaction. Only a base that
/// never carried the stamp pays a full sort, which is also what heals it.
fn order_for_rebuild(raws: &mut [RawQuad], base_rows: usize, base_sorted: bool) {
    if !base_sorted {
        raws.sort_unstable();
    } else if base_rows < raws.len() {
        raws[base_rows..].sort_unstable();
        raws.sort();
    }
}

impl StoreParts {
    /// These parts as a single-chunk [`BuiltStream`] for the writer: the
    /// rows as the one chunk, the components as writes, the dictionary
    /// beside them. A Dictionary-layout primary comes with its dictionary
    /// (`to_serializable_parts` always pairs them).
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn into_stream(self) -> Result<crate::store::builders::BuiltStream> {
        use crate::store::indexes::IndexComponent;
        use crate::store::layouts::LayoutStrategy;
        use futures::StreamExt as _;

        let BuiltArray {
            array,
            components,
            dict,
        } = self.built;
        debug_assert!(
            !matches!(
                LayoutStrategy::from_dtype(array.dtype()),
                LayoutStrategy::Dictionary
            ) || dict.is_some(),
            "to_serializable_parts always pairs a Dictionary primary with its dictionary"
        );
        let components = components
            .iter()
            .map(IndexComponent::to_write)
            .collect::<Result<Vec<_>>>()?;
        Ok(crate::store::builders::BuiltStream {
            dtype: array.dtype().clone(),
            chunks: futures::stream::once(async move { Ok(array) }).boxed(),
            components,
            quads_sorted: self.quads_sorted,
            dict,
        })
    }
}

impl VortexRdfStore {
    /// The rows this view covers, base and tail combined, as one array of
    /// primary columns — plus the index components describing them and, when
    /// a tailed Dictionary view re-encoded them, the *fresh* term dictionary
    /// those codes address.
    ///
    /// Components are included only when their `rid`s actually address the
    /// returned rows: an unrefined, untombstoned owner passes its components
    /// through (in memory) or lifts its index children (file); an owner that
    /// is tailed, or tombstoned and indexed, REBUILDS them over the surviving
    /// rows (the held components' rids predate the mutation); a narrowed
    /// view returns none — its gathered rows are renumbered, and rebuilding
    /// indexes for an arbitrary view is compaction's job, not
    /// serialization's.
    ///
    /// A rebuild also **reorders** the rows it re-emits (see
    /// [`order_for_rebuild`]), so the merged output is `(s, p, o, g)`-sorted
    /// and carries the subject stamp — serialization preserves the store's
    /// quads, not their row numbering.
    ///
    /// A tailed Dictionary view re-encodes its rows against a fresh
    /// dictionary, which is preferred here over the store's cached one (the
    /// cache predates the tail and would mismatch the new codes); otherwise a
    /// file-backed dictionary is lifted resident transiently for the write.
    pub async fn to_serializable_parts(&self) -> Result<StoreParts> {
        let base = self.base_selected_rows().await?;
        let owner_shaped = self.quads.is_unrefined();
        let tombstoned = self.quads.deleted().is_some();
        let rebuild =
            self.tail.is_some() || (owner_shaped && tombstoned && !self.indexes.is_empty());
        if rebuild {
            let base_sorted = subject_sorted(&base);
            let (mut raws, base_rows) = self.merged_raw_quads(&base).await?;
            order_for_rebuild(&mut raws, base_rows, base_sorted);
            let built = build_parts_from_raws(&raws, self.layout.strategy(), &self.indexes, true)?;
            return Ok(StoreParts {
                built,
                quads_sorted: true,
            });
        }
        let components = if owner_shaped && !tombstoned {
            match &self.quads {
                QuadsSource::InMemory { components, .. } => components.to_vec(),
                // An unrefined file view reads its index children
                // wholesale, so a file-backed store's serialization (and
                // the bindings' in-memory round-trip) keeps its indexes.
                #[cfg(feature = "file-io")]
                QuadsSource::File { file, .. } => scanned_index_components(file).await?,
            }
        } else {
            Vec::new()
        };
        let dict = match &self.layout {
            ResolvedLayout::Dictionary(access) => Some(access.ensure_resident().await?),
            _ => None,
        };
        Ok(StoreParts {
            quads_sorted: subject_sorted(&base),
            built: BuiltArray {
                array: base,
                components,
                dict,
            },
        })
    }

    /// Serialize this store to native-container bytes: the quad table as the
    /// transparent root child, the dictionary and index copies as auxiliary
    /// children. The exchange format of the bindings — read back with
    /// [`from_bytes`](Self::from_bytes) or written to disk as a `.vortex`
    /// file.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub async fn to_bytes(&self) -> Result<Vec<u8>> {
        let stream = self.to_serializable_parts().await?.into_stream()?;
        let mut bytes = Vec::new();
        crate::io::write::built_stream_to_vortex_writer(stream, &mut bytes).await?;
        Ok(bytes)
    }
}
