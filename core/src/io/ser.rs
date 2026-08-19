//! The write side of the native store container.
//!
//! Assembles a store's parts — the primary quad table, the in-memory index
//! components, and the term dictionary — into
//! [`NativeComponentWrite`](crate::io::container::NativeComponentWrite)s
//! and drives [`write_store`](crate::io::container::write_store) over
//! them, carrying each part's sortedness provenance onto the descriptors a
//! reader will trust. Also owns the `quads_stream_to_*` entry points, which
//! run a builder's chunk stream straight into that writer.
//!
//! Reading these bytes back is [`native_file`](crate::io::native_file)'s job,
//! and the container's own on-disk grammar is
//! [`container`](crate::io::container)'s.

use crate::error::{Result, VortexRdfError};
use vortex_array::ArrayRef;

use crate::io::container::{
    self, DICT_COMPONENT_NAME, NativeComponentWrite, ReplayableArraySource,
    StoreComponentDescriptor, StoreComponentRole, default_child_strategy,
};
use crate::store::LayoutStrategy;
use crate::store::layouts::dictionary::{TermDictionary, dict_child_chunks};
use std::sync::Arc;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_io::VortexWrite;
use web_time::Instant;

#[cfg(feature = "file-io")]
use crate::error;
#[cfg(feature = "file-io")]
use crate::store::builders::{BuiltStream, SortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, RawQuad};
#[cfg(feature = "file-io")]
use futures::Stream;

/// The term dictionary as a native store component: the sorted term column,
/// one chunk per held FSST window, written verbatim through the pass-through
/// strategy as the root's required `dictionary` child (see
/// [`container::dict_child_strategy`]).
pub(crate) fn dict_component(dict: &TermDictionary) -> Result<NativeComponentWrite> {
    let chunks = dict_child_chunks(dict)?;
    let dtype = chunks[0].dtype().clone();
    NativeComponentWrite::new(
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: container::DICT_IMPLEMENTATION.into(),
            version: 1,
            required: true,
            sorted: true,
            dtype,
        },
        Arc::new(ReplayableArraySource::try_new(chunks)?),
        container::dict_child_strategy(),
    )
    .map_err(VortexRdfError::Vortex)
}

/// Serialize a store's split parts — the primary quad array, its in-memory
/// index components, and (for the Dictionary layout) the term dictionary —
/// as a native store file. Sortedness provenance is carried faithfully: the
/// root records whether `s` is globally sorted (off the primary's own
/// stamp), and each index child records its component's `sorted` flag.
pub(crate) async fn serialize_parts<W: VortexWrite + Unpin + Send>(
    primary: ArrayRef,
    components: &[crate::store::indexes::IndexComponent],
    dict: Option<&TermDictionary>,
    mut writer: W,
) -> Result<()> {
    let start = Instant::now();

    let layout = LayoutStrategy::from_dtype(primary.dtype());
    if matches!(layout, LayoutStrategy::Dictionary) && dict.is_none() {
        return Err(VortexRdfError::Serialization(
            "a bare Dictionary-layout array cannot be serialized without its term \
             dictionary"
                .to_string(),
        ));
    }

    let mut writes = Vec::with_capacity(components.len() + 1);
    for component in components {
        writes.push(component_write(component)?);
    }
    if let Some(dict) = dict {
        writes.push(dict_component(dict)?);
    }

    let (quads_sorted, _) = crate::store::builders::row_space_sortedness(&primary);
    let dtype = primary.dtype().clone();
    let stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(primary) })),
    );
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut writer,
        stream,
        default_child_strategy(),
        quads_sorted,
        writes,
    )
    .await
    .map_err(VortexRdfError::Vortex)?;
    // A shutdown failure is writer I/O, not an encoding problem — surface it
    // through the `Io` variant.
    writer.shutdown().await.map_err(VortexRdfError::Io)?;

    log::debug!(
        "[ser::serialize_parts] Vortex writing took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// An in-memory [`IndexComponent`] as a replayable native child write, its
/// descriptor carrying the component's own sortedness provenance.
///
/// [`IndexComponent`]: crate::store::indexes::IndexComponent
pub(crate) fn component_write(
    component: &crate::store::indexes::IndexComponent,
) -> Result<NativeComponentWrite> {
    // Serialization is a genuine use: a deferred adoption materializes here,
    // so the written child is byte-identical to an eagerly-adopted one's.
    let array = component.as_array()?;
    NativeComponentWrite::new(
        StoreComponentDescriptor {
            name: component.name.into(),
            role: StoreComponentRole::Index,
            implementation: component.implementation.into(),
            version: 1,
            required: false,
            sorted: component.sorted,
            dtype: array.dtype().clone(),
        },
        Arc::new(ReplayableArraySource::try_new(vec![array]).map_err(VortexRdfError::Vortex)?),
        default_child_strategy(),
    )
    .map_err(VortexRdfError::Vortex)
}

/// Stream quads directly into a native store file as compressed chunks.
///
/// The build pipeline is the target's, not the caller's: writing a file means
/// a filesystem exists, so the rows go through the out-of-core global sort
/// ([`SortedStreamBuilder`]) — the one pipeline whose peak memory does not
/// scale with the dataset. (The in-memory sort is what targets without a
/// filesystem use; see [`SortedInMemoryBuilder`].)
///
/// [`SortedInMemoryBuilder`]: crate::SortedInMemoryBuilder
///
/// [`VortexArrayBuilder::build_vortex_stream`] produces row-space chunks
/// lazily; the splitter tees each one into the quad child's
/// stream and the index children's channels, and the layout writer
/// compresses all children concurrently through one segment sink. For
/// a build WITHOUT index children, peak memory is bounded
/// by the chunk size instead of the dataset size. With index children the
/// bound is looser: the sequenced segment sink assigns the quad subtree's
/// segment ids ahead of every auxiliary child's, so a component's compressed
/// segments accumulate in the sink until the quad table finishes writing —
/// peak memory then includes the in-flight components' compressed size (far
/// below the raw dataset, but not O(chunk)). The dictionary is complete
/// before any chunk flows, and becomes the required `dictionary` child.
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer<S, W>(
    quads: S,
    writer: W,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    let start = Instant::now();

    let built = SortedStreamBuilder::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    built_stream_to_vortex_writer(built, writer).await?;

    log::debug!(
        "[ser::quads_stream_to_vortex_writer] Streaming write took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// Drive an already-built chunk stream into `writer` — the back half of
/// [`quads_stream_to_vortex_writer`] (whose docs describe the
/// whole pipeline and its memory bounds), split out so compaction can hand in
/// a stream it built with its own spill-directory placement.
#[cfg(feature = "file-io")]
pub(crate) async fn built_stream_to_vortex_writer<W>(
    built: BuiltStream,
    mut writer: W,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    // Builders that stream components natively hand them over here; row-space
    // builders leave the split to the tee below (a no-op on primary dtypes).
    let split = crate::store::indexes::tee::split_row_space(
        built.dtype,
        built.chunks,
        built.components_sorted,
    )?;
    let mut components = split.components;
    components.extend(built.components);
    if let Some(dict) = &built.dict {
        components.push(dict_component(dict)?);
    }

    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut writer,
        ArrayStreamAdapter::new(split.quad_dtype, split.quad_chunks),
        default_child_strategy(),
        built.quads_sorted,
        components,
    )
    .await
    .map_err(VortexRdfError::Vortex)?;

    // A shutdown failure is writer I/O, not an encoding problem — surface it
    // through the `Io` variant.
    writer.shutdown().await.map_err(VortexRdfError::Io)
}

/// Serialize a quad stream to a native store file at `path` — the path-based
/// convenience over [`quads_stream_to_vortex_writer`].
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_file<S>(
    quads: S,
    path: &std::path::Path,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = error::Result<RawQuad>> + Unpin + Send + 'static,
{
    let writer = tokio::fs::File::create(path)
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("create {:?}: {}", path, e)))?;
    quads_stream_to_vortex_writer(quads, writer, layout, indexes).await
}
