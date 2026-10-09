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

/// Replayable producer for one independently typed component. Sources are
/// buffered arrays ([`BufferedComponentSource`]) or spill-run mergers pulled
/// chunk by chunk ([`PullComponentSource`]); both are lazy — nothing is
/// produced until the write strategy polls, which is what lets it bound how
/// many components compress concurrently.
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

/// A pull closure yielding one component chunk per call (`Ok(None)` = end).
// Constructed only by the out-of-core builder, which is compiled out on
// wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type PullFn =
    Box<dyn FnMut(usize) -> VortexResult<Option<vortex_array::ArrayRef>> + Send>;

/// A single-shot component source over a pull closure — how spill-run mergers
/// stream a component's chunks without materializing them (each call reads
/// the next window off the merger's run files).
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

/// Vortex's default uncompressed block target, the coalescing size of every
/// column the store writes except its term-code columns.
pub(crate) const ONE_MEG: u64 = 1 << 20;

/// The uncompressed block target of a term-code column: twice Vortex's
/// default, so a `u64` code column coalesces to the 262,144 rows per leaf a
/// `u32` one held at 1 MiB (see [`child_strategy`]).
pub(crate) const CODE_BLOCK_TARGET: u64 = 2 * ONE_MEG;

/// The compressor every store child is written with: the session's schemes,
/// limited to the wire forms its write editions allow, minus FastLanes delta.
///
/// Delta wins on size for exactly the columns the store binary-searches in
/// place — the sorted subject column and the index children's lead columns
/// — but a delta value is a running sum, so a point read decodes its whole
/// 1,024-value block; every cold probe and point read through the chunk
/// leaves would pay that where a bit-packed or frame-of-reference column
/// answers from one word. The store's own files stay on the encodings its
/// probes read in place (the reader still probes delta, for files written
/// elsewhere). The pinned core edition keeps frame-of-reference on a single
/// reference and, today, leaves delta out as well; the exclusion keeps delta
/// out should a later core edition admit it.
fn store_compressor() -> vortex_btrblocks::BtrBlocksCompressorBuilder {
    use vortex_btrblocks::schemes::integer::DeltaScheme;
    use vortex_btrblocks::{BtrBlocksCompressorBuilder, SchemeExt as _};

    BtrBlocksCompressorBuilder::from_session(&crate::session::VORTEX_SESSION)
        .exclude_schemes([DeltaScheme::new(1.25).id()])
}

/// Vortex's stock write strategy builder over the [`store_compressor`] — the
/// pipeline `write_options()` installs — with every other option at its
/// default.
fn stock_builder() -> vortex_file::WriteStrategyBuilder {
    vortex_file::WriteStrategyBuilder::from_session(&crate::session::VORTEX_SESSION)
        .with_btrblocks_builder(store_compressor())
}

/// The stock write strategy (see [`stock_builder`]): what every store
/// child's columns are written with except its term-code columns (see
/// [`child_strategy`]); the dictionary child instead passes its
/// pre-compressed chunks through `write::dict_child_strategy`.
pub(crate) fn default_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    stock_builder().build()
}

/// The fields of `dtype` that are term-code columns
/// ([`is_code_field`](crate::store::schema::is_code_field): a non-nullable
/// `u64` `s`, `p`, `o`, `g` or `val`), in field order.
pub(crate) fn code_fields(dtype: &DType) -> Vec<vortex_array::dtype::FieldName> {
    let DType::Struct(fields, _) = dtype else {
        return Vec::new();
    };
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .filter(|(name, field)| crate::store::schema::is_code_field(name.as_ref(), field))
        .map(|(name, _)| name.clone())
        .collect()
}

/// The write strategy for a store child whose columns are `dtype`'s fields:
/// [`default_child_strategy`], with each of its [`code_fields`] written by
/// the same stock pipeline coalescing to [`CODE_BLOCK_TARGET`] instead,
/// through the writer's per-field override
/// (`WriteStrategyBuilder::with_field_writer`). Every other column — the
/// `u32` row ids, the Default and TypedObject layouts' strings — keeps the
/// stock 1 MiB; a child without code columns gets the stock strategy
/// unchanged.
///
/// Vortex coalesces a column to 1 MiB uncompressed per leaf, which is
/// 262,144 rows of a `u32` but 131,072 of a `u64`, and a column read in
/// twice as many leaves costs every probe that walks or rebuilds them; at
/// 2 MiB a plain `u64` code column keeps the rows per leaf a `u32` one had.
/// The target applies to whatever the pipeline coalesces, so the codes of a
/// column that dictionary-encodes — as narrow as its cardinality needs,
/// whatever the width of its values — coalesce to 2 MiB too: 1,048,576 rows
/// of `u16` codes.
pub(crate) fn child_strategy(dtype: &DType) -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_array::dtype::FieldPath;

    let code_fields = code_fields(dtype);
    if code_fields.is_empty() {
        return default_child_strategy();
    }
    // A nested stock strategy: handed a single column, it runs Vortex's own
    // per-column pipeline at the wider target.
    let code = stock_builder()
        .with_data_block_target_bytes(Some(CODE_BLOCK_TARGET))
        .build();
    let mut builder = stock_builder();
    for name in code_fields {
        builder = builder.with_field_writer(FieldPath::from_name(name), Arc::clone(&code));
    }
    builder.build()
}
