//! The write side of the native store container: the driver that writes a
//! [`BuiltStream`] (a builder's, or a store's parts) through
//! [`write_store`](crate::io::container::write_store), carrying each part's
//! sortedness provenance onto the descriptors a reader trusts; the
//! `quads_stream_to_*` entry points; and [`write_sorted_store`], the sorted
//! build into a file that the file writer and a compaction share, through
//! [`PendingStore`], the one way a store reaches a path on disk.

use crate::error::{Result, VortexRdfError};

use crate::io::container::{self, child_strategy};
use crate::store::builders::BuiltStream;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_io::VortexWrite;

#[cfg(feature = "file-io")]
use crate::debug;
#[cfg(feature = "file-io")]
use crate::io::pending::PendingStore;
#[cfg(feature = "file-io")]
use crate::store::builders::{BuildSizes, sorted_stream::build_chunk_stream};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, LayoutStrategy, RawQuad};
#[cfg(feature = "file-io")]
use futures::Stream;

/// Stream quads into `writer` as a native store file, through the out-of-core
/// global sort ([`SortedStreamBuilder`]), whose peak memory does not scale with
/// the dataset: it is bounded by the chunk size, plus the in-flight components'
/// compressed size when index children are written
/// (`RdfStoreWriteStrategy::write_stream`). The dictionary is complete before
/// any chunk flows and becomes the required `dictionary` child.
///
/// A writer has no directory to spill beside: the sort's runs go under
/// `VORTEX_RDF_SPILL_DIR`, else the OS temp dir, in a hidden
/// `.vortex-rdf.spill-<uuid>` directory removed when the write ends.
///
/// The quads must be canonical: build them with [`RawQuad::from_quad`], the
/// parser ([`parse_quads_from_reader`]) or [`RawQuad::canonical`]. The
/// builder interns and compares the spelling it is given, so a hand-built
/// `RawQuad` in another spelling of the same terms is a different quad.
///
/// [`SortedStreamBuilder`]: crate::SortedStreamBuilder
/// [`parse_quads_from_reader`]: crate::common::terms::parse_quads_from_reader
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer<S, W>(
    quads: S,
    writer: W,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    refuse_experimental_patches()?;
    let start = debug::timer();

    let built =
        build_chunk_stream(Box::new(quads), layout, indexes, BuildSizes::DEFAULT, None).await?;
    built_stream_to_vortex_writer(built, writer).await?;

    log::debug!(
        "[write::quads_stream_to_vortex_writer] Streaming write took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Drive a built chunk stream into `writer`: the primary chunks as the
/// transparent root child, each component and the dictionary as auxiliary
/// children. Every store write ends here (`to_bytes` with a store's parts as
/// one chunk, the two file writers and compaction with a builder's stream), so
/// every write refuses here while the experimental patched-array switch is on
/// ([`refuse_experimental_patches`]). The memory bound is
/// `RdfStoreWriteStrategy::write_stream`'s.
pub(crate) async fn built_stream_to_vortex_writer<W>(
    built: BuiltStream,
    mut writer: W,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    refuse_experimental_patches()?;
    let mut components = built.components;
    if let Some(dict) = &built.dict {
        components.push(dict.to_write()?);
    }

    let quad_strategy = child_strategy(&built.dtype);
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut writer,
        ArrayStreamAdapter::new(built.dtype, built.chunks),
        quad_strategy,
        built.quads_sorted,
        components,
    )
    .await?;

    // A shutdown failure is writer I/O, not an encoding problem.
    writer.shutdown().await.map_err(VortexRdfError::Io)
}

/// Serialize a quad stream to a native store file at `path`:
/// [`quads_stream_to_vortex_writer`] into a file written beside `path` and
/// renamed into place.
///
/// The file is all-or-nothing. An input that fails partway (a parse error
/// halfway through the stream) leaves no file at a fresh `path` and the
/// previous store untouched at an existing one, and a store that has `path`
/// memory-mapped keeps reading the file it mapped (the old file is replaced,
/// never rewritten in place; on Windows the rename is refused while a store
/// has the file mapped, so the write fails until that store is closed).
///
/// A path that cannot take a store (a missing directory, no permission to
/// write there, a directory at `path`) is reported before any input is read.
/// A character device or a pipe at `path` (`/dev/null`, `/dev/stdout` with a
/// pipe behind it) takes the store in place, with no temp file and so no
/// all-or-nothing guarantee.
///
/// The sort spills its runs beside the file it writes (where the links at
/// `path` end), on the volume that takes the store, in a hidden directory
/// named after that file (`.out.vortex.spill-<uuid>` for `out.vortex`, or
/// `.vortex-rdf.spill-<uuid>` when that name would be too long for a file
/// name) and removed when the write ends. `VORTEX_RDF_SPILL_DIR` overrides
/// that place; a character device or a pipe has no directory to spill beside,
/// and spills where [`quads_stream_to_vortex_writer`] does.
///
/// The quads must be canonical, as for [`quads_stream_to_vortex_writer`]:
/// from [`RawQuad::from_quad`], the parser ([`parse_quads_from_reader`]) or
/// [`RawQuad::canonical`].
///
/// [`parse_quads_from_reader`]: crate::common::terms::parse_quads_from_reader
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_file<S>(
    quads: S,
    path: &std::path::Path,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
{
    let start = debug::timer();

    // The path is checked and the temp file created before any input is read.
    // The build spills beside the temp file, and an input that fails leaves
    // nothing behind: the run directory and the empty temp file go too.
    let pending = PendingStore::create(path).await?;
    write_sorted_store(pending, Box::new(quads), layout, indexes).await?;

    log::debug!(
        "[write::quads_stream_to_vortex_file] Streaming write took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Build `quads` through the out-of-core global sort and write the store
/// into `pending`, renamed into place once complete: the one path of a sorted
/// build into a file, which [`quads_stream_to_vortex_file`] and a file-backed
/// store's compaction share. The sort spills its runs at the pending store's
/// [`spill_site`](PendingStore::spill_site), beside the file it replaces
/// (`VORTEX_RDF_SPILL_DIR` outranks that). The caller creates `pending` before
/// it reads any input, so a path that cannot take a store is refused first.
#[cfg(feature = "file-io")]
pub(crate) async fn write_sorted_store(
    pending: PendingStore,
    quads: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()> {
    let built = build_chunk_stream(
        quads,
        layout,
        indexes,
        BuildSizes::DEFAULT,
        pending.spill_site(),
    )
    .await?;
    pending
        .write(|writer| built_stream_to_vortex_writer(built, writer))
        .await
}

/// Refuse a store write while Vortex's experimental patched-array switch is
/// on: its compressor then emits an array no store edition admits, and the
/// write would fail at the first chunk that holds one.
pub(crate) fn refuse_experimental_patches() -> Result<()> {
    if vortex_array::arrays::patched::use_experimental_patches() {
        return Err(VortexRdfError::Serialization(
            "VORTEX_EXPERIMENTAL_PATCHED_ARRAY=1 makes Vortex's compressor emit the \
             experimental patched array, which no store file may hold; unset it to write a store"
                .to_string(),
        ));
    }
    Ok(())
}
