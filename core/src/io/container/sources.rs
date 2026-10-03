//! Component sources: replayable producers of one component's chunks, the
//! descriptor-source-strategy triple a write consumes, and the per-child
//! write strategies.

use std::sync::Arc;

use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};

use super::wire::StoreComponentDescriptor;

/// A replayable producer of one component's chunks, lazy until the write
/// strategy polls it.
pub(crate) trait NativeComponentSource: Send + Sync + 'static {
    fn dtype(&self) -> &DType;
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream>;
    fn buffered_bytes(&self) -> u64 {
        0
    }
}

/// A component source over already-materialized chunks; replay is
/// `Arc`-cheap.
#[derive(Clone)]
pub(crate) struct BufferedComponentSource {
    dtype: DType,
    chunks: Arc<[vortex_array::ArrayRef]>,
    retained_bytes: u64,
}

impl BufferedComponentSource {
    pub(crate) fn try_new(chunks: Vec<vortex_array::ArrayRef>) -> VortexResult<Self> {
        let Some(first) = chunks.first() else {
            vortex_bail!("a buffered component source requires at least one chunk");
        };
        let dtype = first.dtype().clone();
        for chunk in &chunks {
            vortex_ensure_eq!(
                chunk.dtype(),
                &dtype,
                "component chunks must share one dtype"
            );
        }
        let retained_bytes = chunks.iter().map(|c| c.nbytes()).sum();
        Ok(Self {
            dtype,
            chunks: chunks.into(),
            retained_bytes,
        })
    }
}

impl NativeComponentSource for BufferedComponentSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let chunks = Arc::clone(&self.chunks);
        let stream = futures::stream::unfold((chunks, 0usize), |(chunks, index)| async move {
            let chunk = chunks.get(index)?.clone();
            Some((Ok(chunk), (chunks, index + 1)))
        });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }

    fn buffered_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

/// A pull closure yielding one component chunk of at most the given rows
/// per call, `Ok(None)` once exhausted; a zero-row pull yields the empty
/// chunk of the component's dtype.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type PullFn =
    Box<dyn FnMut(usize) -> VortexResult<Option<vortex_array::ArrayRef>> + Send>;

/// A single-shot component source over a pull closure; each call reads the
/// next window off a spill-run merger.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) struct PullComponentSource {
    dtype: DType,
    batch_rows: usize,
    pull: std::sync::Mutex<Option<PullFn>>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl PullComponentSource {
    pub(crate) fn new(dtype: DType, batch_rows: usize, pull: PullFn) -> Self {
        Self {
            dtype,
            batch_rows,
            pull: std::sync::Mutex::new(Some(pull)),
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl NativeComponentSource for PullComponentSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream> {
        let pull = self
            .pull
            .lock()
            .expect("pull source lock")
            .take()
            .ok_or_else(|| {
                vortex_error::vortex_err!("a pull-backed component source replays only once")
            })?;
        let batch_rows = self.batch_rows;
        // At least one chunk is emitted: an empty component contributes its
        // schema-carrying empty chunk.
        let stream =
            futures::stream::unfold((Some(pull), false), move |(state, emitted)| async move {
                let mut pull = state?;
                match pull(batch_rows) {
                    Ok(Some(chunk)) => Some((Ok(chunk), (Some(pull), true))),
                    Ok(None) if !emitted => {
                        let empty = pull(0).and_then(|chunk| {
                            chunk.ok_or_else(|| {
                                vortex_error::vortex_err!(
                                    "a zero-row pull must yield an empty chunk"
                                )
                            })
                        });
                        Some((empty, (None, true)))
                    }
                    Ok(None) => None,
                    Err(e) => Some((Err(e), (None, true))),
                }
            });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}

/// A component to write: its descriptor, chunk source and per-child write
/// strategy.
#[derive(Clone)]
pub(crate) struct NativeComponentWrite {
    pub(crate) descriptor: StoreComponentDescriptor,
    pub(crate) source: Arc<dyn NativeComponentSource>,
    pub(crate) strategy: Arc<dyn vortex_layout::LayoutStrategy>,
}

impl NativeComponentWrite {
    /// Pair a descriptor with its source and per-child write strategy; the
    /// source's dtype must match the descriptor's.
    pub(crate) fn new(
        descriptor: StoreComponentDescriptor,
        source: Arc<dyn NativeComponentSource>,
        strategy: Arc<dyn vortex_layout::LayoutStrategy>,
    ) -> VortexResult<Self> {
        vortex_ensure_eq!(
            &descriptor.dtype,
            source.dtype(),
            "component source dtype mismatch"
        );
        Ok(Self {
            descriptor,
            source,
            strategy,
        })
    }
}

/// The write strategy of the quad child and the index components: the
/// BtrBlocks cascade minus FastLanes delta (a point read must not decode a
/// 1,024-value block).
pub(crate) fn default_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_btrblocks::schemes::integer::DeltaScheme;
    use vortex_btrblocks::{BtrBlocksCompressorBuilder, SchemeExt as _};

    let compressor =
        BtrBlocksCompressorBuilder::default().exclude_schemes([DeltaScheme::new(1.25).id()]);
    Arc::new(
        vortex_file::WriteStrategyBuilder::default()
            .with_btrblocks_builder(compressor)
            .build(),
    )
}

/// The dictionary child's pass-through strategy: every source chunk written
/// verbatim as one flat leaf under a chunked node, so the leaves are the
/// FSST windows `FileBackedDict` point-reads.
pub(crate) fn dict_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;
    Arc::new(StructStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
    ))
}
