//! Component sources: replayable producers of one independently typed
//! component's chunks, plus the descriptor-source-strategy triple a write
//! consumes. Always compiled — builders construct [`NativeComponentWrite`]s
//! on every target, even when no serializer is compiled in; only the write
//! strategy that consumes them (`write`) is gated.

use std::sync::Arc;

use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};

use super::wire::StoreComponentDescriptor;

/// Replayable producer for one independently typed component. Sources may be
/// buffered arrays ([`ReplayableArraySource`]), spill-run mergers, or
/// channels tee-fed by the quad stream — see [`NativeComponentSource::
/// channel_backed`] for the concurrency contract of the last kind.
pub(crate) trait NativeComponentSource: Send + Sync + 'static {
    fn dtype(&self) -> &DType;
    fn open(&self) -> VortexResult<vortex_array::stream::SendableArrayStream>;
    fn buffered_bytes(&self) -> u64 {
        0
    }
    /// True when this source is fed by the quad stream's own poll loop (a
    /// tee channel): the write strategy must then poll every component job
    /// concurrently, or a full channel blocks the quad stream and the write
    /// deadlocks.
    fn channel_backed(&self) -> bool {
        false
    }
}

/// A component source over already-materialized chunks. Replay is Arc-cheap;
/// large components should prefer run-backed sources.
#[derive(Clone)]
pub(crate) struct ReplayableArraySource {
    dtype: DType,
    chunks: Arc<[vortex_array::ArrayRef]>,
    retained_bytes: u64,
}

impl ReplayableArraySource {
    pub(crate) fn try_new(chunks: Vec<vortex_array::ArrayRef>) -> VortexResult<Self> {
        let Some(first) = chunks.first() else {
            vortex_bail!("a replayable component source requires at least one chunk");
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

impl NativeComponentSource for ReplayableArraySource {
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

/// A pull closure yielding one component chunk per call (`Ok(None)` = end).
// Constructed only by the external-sort builder's mergers, which are compiled
// out on wasm (see the module gate in `store::builders`).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) type PullFn =
    Box<dyn FnMut(usize) -> VortexResult<Option<vortex_array::ArrayRef>> + Send>;

/// A single-shot component source over a pull closure — how spill-run mergers
/// stream a component's chunks without materializing them (each call reads
/// the next window off the merger's run files).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) struct PullComponentSource {
    dtype: DType,
    batch_rows: usize,
    pull: std::sync::Mutex<Option<PullFn>>,
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
impl PullComponentSource {
    pub(crate) fn new(dtype: DType, batch_rows: usize, pull: PullFn) -> Self {
        Self {
            dtype,
            batch_rows,
            pull: std::sync::Mutex::new(Some(pull)),
        }
    }
}

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
        let stream = futures::stream::unfold(Some(pull), move |state| async move {
            let mut pull = state?;
            match pull(batch_rows) {
                Ok(Some(chunk)) => Some((Ok(chunk), Some(pull))),
                Ok(None) => None,
                Err(e) => Some((Err(e), None)),
            }
        });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.dtype.clone(),
            stream,
        )))
    }
}

#[derive(Clone)]
pub(crate) struct NativeComponentWrite {
    pub(crate) descriptor: StoreComponentDescriptor,
    pub(crate) source: Arc<dyn NativeComponentSource>,
    pub(crate) strategy: Arc<dyn vortex_layout::LayoutStrategy>,
}

impl NativeComponentWrite {
    /// Pair a descriptor with its source and per-child write strategy. Only
    /// the source's dtype is checked here (a construction invariant);
    /// inventory-wide validation — descriptor well-formedness and name
    /// uniqueness — runs once at the container's write entry path, see
    /// [`validate_components`](super::wire::validate_components).
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

/// The stock write strategy `write_options()` installs — used for the quad
/// child and the index components so their encoding pipeline is exactly what
/// a plain table write produces (the dictionary child instead passes its
/// pre-compressed chunks through `write::dict_child_strategy`). Ungated like
/// the component data types: builders construct [`NativeComponentWrite`]s on
/// every target, even when no serializer is compiled in.
pub(crate) fn default_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    Arc::new(vortex_file::WriteStrategyBuilder::default().build())
}
