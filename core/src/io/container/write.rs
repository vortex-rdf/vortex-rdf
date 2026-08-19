//! The write side of the container grammar: assembling a native root layout
//! from its written children, and the write strategy that turns a quad
//! stream plus component sources into a store file. Gated as a whole at the
//! module declaration (see `container`'s docs); the always-compiled
//! component data types live in [`sources`](super::sources).

use std::sync::Arc;

use vortex_error::{VortexResult, vortex_ensure_eq};
use vortex_layout::{LayoutParts, LayoutRef, layout_children};
use vortex_session::VortexSession;

use super::layout::{RdfStoreLayout, RdfStoreLayoutData, RdfStoreLayoutVTable};
use super::sources::NativeComponentWrite;
use super::wire::{StoreComponentDescriptor, validate_components};

/// One descriptor paired with its written child layout.
#[derive(Clone)]
pub(crate) struct StoreComponent {
    pub(crate) descriptor: StoreComponentDescriptor,
    pub(crate) layout: LayoutRef,
}

impl StoreComponent {
    /// Pair a descriptor with the layout its write produced, checking only
    /// that the written child matches the descriptor's dtype — descriptor
    /// validity was established when the inventory entered the write path
    /// ([`validate_components`]).
    pub(crate) fn new(
        descriptor: StoreComponentDescriptor,
        layout: LayoutRef,
    ) -> VortexResult<Self> {
        vortex_ensure_eq!(
            &descriptor.dtype,
            layout.dtype(),
            "store component descriptor dtype does not match its child layout"
        );
        Ok(Self { descriptor, layout })
    }
}

/// Assemble a native root from the written quad-source child and its
/// components. The component inventory must already be validated — the write
/// strategy's [`with_components`](RdfStoreWriteStrategy::with_components) is
/// the sole entry feeding this.
pub(crate) fn new_store_layout_with_components(
    quad_source: LayoutRef,
    quads_sorted: bool,
    components: Vec<StoreComponent>,
) -> VortexResult<RdfStoreLayout> {
    let dtype = quad_source.dtype().clone();
    let row_count = quad_source.row_count();
    let mut descriptors = Vec::with_capacity(components.len());
    let mut children = Vec::with_capacity(1 + components.len());
    children.push(quad_source);
    for component in components {
        descriptors.push(component.descriptor);
        children.push(component.layout);
    }
    Ok(LayoutParts::new(
        RdfStoreLayoutVTable,
        dtype,
        row_count,
        Vec::new(),
        layout_children(children),
        RdfStoreLayoutData {
            quads_sorted,
            components: descriptors.into(),
        },
    )
    .into_typed())
}

/// The store's write strategy: the input stream becomes the transparent
/// quad-source child, each component becomes an auxiliary child, and all of
/// them share the file's segment sink.
#[derive(Clone)]
pub(crate) struct RdfStoreWriteStrategy {
    quad_source: Arc<dyn vortex_layout::LayoutStrategy>,
    /// Provenance recorded in the root metadata: whether the quad stream's
    /// `s` column is globally sorted (see `WireMetadata::quads_sorted`).
    quads_sorted: bool,
    components: Arc<[NativeComponentWrite]>,
}

impl RdfStoreWriteStrategy {
    pub(crate) fn new(
        quad_source: Arc<dyn vortex_layout::LayoutStrategy>,
        quads_sorted: bool,
    ) -> Self {
        Self {
            quad_source,
            quads_sorted,
            components: Arc::from([]),
        }
    }

    /// Adopt the component inventory, validating it once here — the write
    /// path's entry point for validation (each write's descriptor was
    /// dtype-checked against its source at construction).
    pub(crate) fn with_components(
        mut self,
        components: Vec<NativeComponentWrite>,
    ) -> VortexResult<Self> {
        validate_components(components.iter().map(|c| &c.descriptor))?;
        self.components = components.into();
        Ok(self)
    }
}

#[async_trait::async_trait]
impl vortex_layout::LayoutStrategy for RdfStoreWriteStrategy {
    async fn write_stream(
        &self,
        ctx: vortex_array::ArrayContext,
        segment_sink: vortex_layout::segments::SegmentSinkRef,
        stream: vortex_layout::sequence::SendableSequentialStream,
        mut eof: vortex_layout::sequence::SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        use futures::{StreamExt as _, TryStreamExt as _};
        use vortex_layout::sequence::SequentialArrayStreamExt as _;

        // The input stream already occupies the first sequence subtree;
        // reserve its boundary, then ordered sibling subtrees per component.
        let quad_eof = eof.split_off();
        let mut jobs = Vec::with_capacity(self.components.len());
        for component in self.components.iter().cloned() {
            let stream_pointer = eof.split_off();
            let component_eof = eof.split_off();
            let child_ctx = ctx.clone();
            let child_sink = Arc::clone(&segment_sink);
            let child_session = session.clone();
            jobs.push(async move {
                let child_stream = component.source.open()?;
                vortex_ensure_eq!(child_stream.dtype(), &component.descriptor.dtype);
                let layout = component
                    .strategy
                    .write_stream(
                        child_ctx,
                        child_sink,
                        child_stream.sequenced(stream_pointer),
                        component_eof,
                        &child_session,
                    )
                    .await?;
                StoreComponent::new(component.descriptor, layout)
            });
        }

        let quad_future = self.quad_source.write_stream(
            ctx,
            Arc::clone(&segment_sink),
            stream,
            quad_eof,
            session,
        );
        // Channel-backed components are fed by the quad stream's poll loop:
        // every job must then be in flight or the tee blocks the quad write.
        // Lazy (replayable / merger-backed) sources cost nothing until
        // polled, so a small window only bounds concurrent compression.
        let concurrency = if self.components.iter().any(|c| c.source.channel_backed()) {
            jobs.len().max(1)
        } else {
            jobs.len().clamp(1, 2)
        };
        let components_future = futures::stream::iter(jobs)
            .buffered(concurrency)
            .try_collect::<Vec<_>>();
        let (quad_source, components) =
            futures::future::try_join(quad_future, components_future).await?;
        Ok(
            new_store_layout_with_components(quad_source, self.quads_sorted, components)?
                .into_layout(),
        )
    }

    fn buffered_bytes(&self) -> u64 {
        self.quad_source.buffered_bytes()
            + self
                .components
                .iter()
                .map(|c| c.source.buffered_bytes() + c.strategy.buffered_bytes())
                .sum::<u64>()
    }
}

/// Write a native store file: the quad stream as the transparent root child,
/// plus one auxiliary child per component.
pub(crate) async fn write_store<W, S>(
    session: &VortexSession,
    writer: W,
    stream: S,
    quad_source_strategy: Arc<dyn vortex_layout::LayoutStrategy>,
    quads_sorted: bool,
    components: Vec<NativeComponentWrite>,
) -> VortexResult<vortex_file::WriteSummary>
where
    W: vortex_io::VortexWrite + Unpin,
    S: vortex_array::stream::ArrayStream + Send + 'static,
{
    use vortex_file::WriteOptionsSessionExt as _;
    let strategy = RdfStoreWriteStrategy::new(quad_source_strategy, quads_sorted)
        .with_components(components)?;
    session
        .write_options()
        .with_strategy(Arc::new(strategy))
        .write(writer, stream)
        .await
}

/// The dictionary child's pass-through strategy: every chunk the source
/// emits is written verbatim as one flat leaf under a chunked node — no
/// sampling, no re-encoding. The dictionary is FSST-compressed at the source
/// in self-contained windows (`TermDictionary::compress`), so the default
/// strategy's compressor would only re-do work it cannot improve on, and the
/// window boundaries become the child's chunk leaves — the granularity at
/// which `FileBackedDict` point-reads and lifts it.
pub(crate) fn dict_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;
    Arc::new(StructStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
    ))
}
