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

/// Put a rebuild's rows into (s, p, o, g) order. The first `base_rows`
/// entries are the base, already in order when `base_sorted`; the rest is
/// the tail. A sorted base gets its tail sorted and the two runs merged, an
/// unsorted base a full sort.
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
    /// primary columns with the index components describing them and, under
    /// the Dictionary layout, the term dictionary the codes address. An
    /// unrefined, untombstoned owner passes its components through (in
    /// memory) or lifts its index children (file); an owner that is tailed, or
    /// tombstoned and indexed, rebuilds them over the surviving rows and
    /// re-emits the rows in `(s, p, o, g)` order (a tailed Dictionary view
    /// against a fresh dictionary); a narrowed view returns no components. A
    /// file-backed dictionary is lifted resident for the write.
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
                // An unrefined file view's serialization keeps its index
                // children.
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

    /// This store as native-container bytes: the quad table as the transparent
    /// root child, the dictionary and index children as auxiliary children;
    /// read back with [`from_bytes`](Self::from_bytes) or written as a
    /// `.vortex` file.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub async fn to_bytes(&self) -> Result<Vec<u8>> {
        let stream = self.to_serializable_parts().await?.into_stream()?;
        let mut bytes = Vec::new();
        crate::io::write::built_stream_to_vortex_writer(stream, &mut bytes).await?;
        Ok(bytes)
    }
}
