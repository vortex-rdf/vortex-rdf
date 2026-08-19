//! Serialization: the rows-plus-components-plus-dictionary parts every write
//! path produces (`to_serializable_parts`, `to_bytes`, the bindings'
//! in-memory round-trips).

use crate::error::{Result, VortexRdfError};
#[cfg(feature = "file-io")]
use crate::io::native_file;
use crate::store::QuadsSource;
use crate::store::builders::build_struct_array;
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{ResolvedLayout, dictionary};
use crate::store::selection::gather_live;

use std::sync::Arc;

use vortex_array::arrays::ChunkedArray;
use vortex_array::{ArrayRef, IntoArray};

#[cfg(feature = "file-io")]
use super::open::{ComponentKind, classify_component};
use super::{StoreParts, VortexRdfStore};

impl VortexRdfStore {
    /// The rows this view covers, base and tail combined, as one array of
    /// primary columns — plus the index components describing them and, when
    /// a tailed Dictionary view re-encoded them, the *fresh* term dictionary
    /// those codes address.
    ///
    /// Components are included only when their `rid`s actually address the
    /// returned rows: an unrefined owner passes its components through
    /// untouched; a **tailed** owner REBUILDS them over the merged rows (the
    /// old components predate the tail); a narrowed view returns none — its
    /// gathered rows are renumbered, and rebuilding indexes for an arbitrary
    /// view is compaction's job, not serialization's.
    ///
    /// Serialization policy only: [`to_serializable_parts`] is the sole
    /// caller. The row read paths go through their own rows-only method
    /// (`selected_rows`), which never materializes components.
    ///
    /// [`to_serializable_parts`]: Self::to_serializable_parts
    async fn selected_parts(
        &self,
    ) -> Result<(
        ArrayRef,
        Vec<crate::store::indexes::IndexComponent>,
        Option<Arc<TermDictionary>>,
    )> {
        let base = self.base_selected_rows().await?;
        // Which serialization shape this view gets:
        // - a pristine owner passes its components through (in memory) or
        //   lifts its index children (file);
        // - a mutated owner (tombstones and/or a tail) REBUILDS them — the
        //   held components' rids predate the mutation;
        // - a narrowed view serializes primary rows only (rebuilding indexes
        //   for an arbitrary view is compaction's job).
        let (owner_shaped, tombstoned) = match &self.quads {
            QuadsSource::InMemory {
                selection, deleted, ..
            } => (selection.is_all(), deleted.is_some()),
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                filter,
                selection,
                deleted,
                ..
            } => (filter.is_none() && selection.is_all(), deleted.is_some()),
        };
        let rebuild =
            self.tail.is_some() || (owner_shaped && tombstoned && !self.indexes.is_empty());
        if !rebuild {
            let components = if owner_shaped && !tombstoned {
                match &self.quads {
                    QuadsSource::InMemory { components, .. } => components.to_vec(),
                    // An unrefined file view reads its index children
                    // wholesale, so a file-backed store's serialization (and
                    // the bindings' in-memory round-trip) keeps its indexes.
                    #[cfg(feature = "file-io")]
                    QuadsSource::File { file, .. } => Self::file_components(file).await?,
                }
            } else {
                Vec::new()
            };
            return Ok((base, components, None));
        }
        let tail_rows = match &self.tail {
            Some(tail) => Some(gather_live(
                &tail.rows,
                &tail.selection,
                tail.deleted.as_ref(),
                None,
            )?),
            None => None,
        };
        // Without a tail the surviving rows keep the base's order, so its
        // subject sortedness carries to the rebuild; appended tail rows
        // break it.
        let sorted = tail_rows.is_none() && Self::base_subject_sorted(&base);
        match &self.layout {
            ResolvedLayout::Dictionary(_) => {
                let mut raws = self.base_raw_quads(&base).await?;
                if let Some(tail_rows) = &tail_rows {
                    raws.extend(ResolvedLayout::Default.raw_quads(tail_rows)?);
                }
                if raws.is_empty() {
                    let empty = dictionary::empty_struct(&[])?;
                    return Ok((empty, Vec::new(), Some(Arc::new(TermDictionary::empty()))));
                }
                let (dict, id_map) = TermDictionary::from_quads_with_map(&raws)?;
                // Rebuild the index components over the surviving rows
                // (welded then split — the same adoption path as a fresh
                // build), so a mutated store's serialization keeps its
                // indexes instead of silently dropping them.
                let chunk =
                    dictionary::build_chunk(&raws, &dict, &id_map, &self.indexes, 0, sorted, true)?;
                let (primary, components) = crate::store::indexes::split_built_row_space(chunk)?;
                Ok((primary, components, Some(Arc::new(dict))))
            }
            _ if !self.indexes.is_empty() => {
                // Rebuild the components over the surviving rows — decoding
                // to raws is what gives the index sorts something to permute.
                let mut raws = self.layout.raw_quads(&base)?;
                if let Some(tail_rows) = &tail_rows {
                    raws.extend(self.tail_layout().raw_quads(tail_rows)?);
                }
                let welded = build_struct_array(
                    &raws,
                    self.layout.strategy(),
                    &self.indexes,
                    0,
                    sorted,
                    true,
                )?;
                let (primary, components) = crate::store::indexes::split_built_row_space(welded)?;
                Ok((primary, components, None))
            }
            _ => {
                // A tail with no indexes: append it as a second chunk (this
                // arm is only reachable with a tail — a tombstone-only store
                // without indexes never rebuilds).
                let tail_rows = tail_rows.expect("rebuild without indexes implies a tail");
                let dtype = base.dtype().clone();
                let rows = ChunkedArray::try_new(vec![base, tail_rows], dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array();
                Ok((rows, Vec::new(), None))
            }
        }
    }

    /// Lift an unrefined file view's index children into in-memory
    /// components: the same rows under the same child schema, with each
    /// descriptor's `sorted` provenance carried across. Adoption shares
    /// `from_bytes`' deferral — the sole caller serializes the components
    /// immediately, so they canonicalize at the write either way.
    #[cfg(feature = "file-io")]
    async fn file_components(
        file: &crate::store::native_file::NativeStoreFile,
    ) -> Result<Vec<crate::store::indexes::IndexComponent>> {
        let mut components = Vec::new();
        for descriptor in file.components() {
            let ComponentKind::Index(known) = classify_component(descriptor)? else {
                continue;
            };
            let Some((_, reader)) = file
                .component_reader(&descriptor.name)
                .map_err(VortexRdfError::Vortex)?
            else {
                continue;
            };
            let scanned = native_file::scan_all_reader(reader).await?;
            components.push(crate::store::indexes::adopt_scanned_component(
                &known,
                scanned,
                descriptor.sorted,
                file.row_count(),
            )?);
        }
        Ok(components)
    }

    /// This store's rows and, under the Dictionary layout, the term
    /// dictionary those rows' codes address — the pair every serialization
    /// path writes (`to_bytes`, compaction's file rewrite, the bindings'
    /// in-memory round-trips).
    ///
    /// A tailed Dictionary view re-encodes its rows against a fresh
    /// dictionary, which is preferred here over the store's cached one (the
    /// cache predates the tail and would mismatch the new codes); otherwise a
    /// file-backed dictionary is lifted resident transiently for the write.
    pub async fn to_serializable_parts(&self) -> Result<StoreParts> {
        let (array, components, fresh) = self.selected_parts().await?;
        let dict = match (&self.layout, fresh) {
            (ResolvedLayout::Dictionary(_), Some(fresh)) => Some(fresh),
            (ResolvedLayout::Dictionary(access), None) => Some(access.ensure_resident().await?),
            _ => None,
        };
        Ok(StoreParts {
            array,
            components,
            dict,
        })
    }

    /// Serialize this store to native-container bytes: the quad table as the
    /// transparent root child, the dictionary and index copies as auxiliary
    /// children. The exchange format of the bindings — read back with
    /// [`from_bytes`](Self::from_bytes) or written to disk as a `.vortex`
    /// file.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub async fn to_bytes(&self) -> Result<Vec<u8>> {
        let parts = self.to_serializable_parts().await?;
        let mut bytes = Vec::new();
        crate::io::ser::serialize_parts(
            parts.array,
            &parts.components,
            parts.dict.as_deref(),
            &mut bytes,
        )
        .await?;
        Ok(bytes)
    }
}
