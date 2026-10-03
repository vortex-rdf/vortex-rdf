//! The write side of the container grammar: assembling a native root from
//! its written children, and the write strategy that turns a quad stream
//! plus component sources into a store file.

use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_layout::segments::SegmentSinkRef;
use vortex_layout::sequence::{SendableSequentialStream, SequencePointer};
use vortex_layout::{LayoutParts, LayoutRef, LayoutStrategy, LayoutWriterContext, layout_children};
use vortex_session::VortexSession;

use super::layout::{RdfStoreLayout, RdfStoreLayoutData, RdfStoreLayoutVTable};
use super::sources::NativeComponentWrite;
use super::wire::{StoreComponentDescriptor, validate_components};

/// How many component children compress concurrently beside the quad
/// source; bounds writer-buffered memory.
const COMPONENT_WRITE_CONCURRENCY: usize = 2;

/// One descriptor paired with its written child layout.
#[derive(Clone)]
struct StoreComponent {
    descriptor: StoreComponentDescriptor,
    layout: LayoutRef,
}

/// Assemble a native root from the written quad-source child and its
/// components.
fn new_store_layout_with_components(
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
struct RdfStoreWriteStrategy {
    quad_source: Arc<dyn LayoutStrategy>,
    /// Recorded in the root metadata as `quads_sorted`.
    quads_sorted: bool,
    components: Arc<[NativeComponentWrite]>,
}

impl RdfStoreWriteStrategy {
    fn new(quad_source: Arc<dyn LayoutStrategy>, quads_sorted: bool) -> Self {
        Self {
            quad_source,
            quads_sorted,
            components: Arc::from([]),
        }
    }

    /// Adopt the component inventory, validated once.
    fn with_components(mut self, components: Vec<NativeComponentWrite>) -> VortexResult<Self> {
        validate_components(components.iter().map(|c| &c.descriptor))?;
        self.components = components.into();
        Ok(self)
    }
}

#[async_trait::async_trait]
impl LayoutStrategy for RdfStoreWriteStrategy {
    /// All children compress concurrently through one segment sink; the quad
    /// subtree's segment ids come first, so a component's compressed segments
    /// wait in the sink until the quad table finishes.
    async fn write_stream(
        &self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        mut eof: SequencePointer,
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
                // The source's retained chunks count as writer-buffered memory
                // until the job completes.
                let _reserved = child_ctx.reserve_buffered_bytes(component.source.buffered_bytes());
                let child_stream = component.source.open()?;
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
                Ok(StoreComponent {
                    descriptor: component.descriptor,
                    layout,
                })
            });
        }

        let quad_future = self.quad_source.write_stream(
            ctx,
            Arc::clone(&segment_sink),
            stream,
            quad_eof,
            session,
        );
        let concurrency = jobs.len().clamp(1, COMPONENT_WRITE_CONCURRENCY);
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
}

/// Write a native store file: the quad stream as the transparent root child,
/// plus one auxiliary child per component.
pub(crate) async fn write_store<W, S>(
    session: &VortexSession,
    writer: W,
    stream: S,
    quad_source_strategy: Arc<dyn LayoutStrategy>,
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
