//! Component sources: producers of one component's chunks, the
//! descriptor-source-strategy triple ([`NativeComponentWrite`]) a write
//! consumes, and the per-child write strategies.

use std::sync::Arc;

use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure_eq};

use super::wire::StoreComponentDescriptor;

/// A producer of one component's chunks: buffered arrays
/// ([`BufferedComponentSource`], replayable) or a spill-run merger pulled
/// chunk by chunk ([`PullComponentSource`], single-shot). Nothing is produced
/// until the write strategy polls, so it bounds how many components compress
/// at once.
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

/// A pull closure yielding one component chunk of at most the given rows per
/// call, `Ok(None)` once exhausted; a zero-row pull yields the empty chunk of
/// the component's dtype.
// Made only by the out-of-core builder, absent on wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type PullFn =
    Box<dyn FnMut(usize) -> VortexResult<Option<vortex_array::ArrayRef>> + Send>;

/// A single-shot component source over a pull closure: each call reads the
/// next window off a spill-run merger's run files, so the component is never
/// materialized.
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
        // At least one chunk is emitted: the child strategy needs one,
        // possibly empty, to write a schema-complete component, so an empty
        // component contributes its zero-row pull.
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

/// Vortex's default uncompressed block target, the coalescing size of every
/// column the store writes outside [`id_column_strategy`].
pub(crate) const ONE_MEG: u64 = 1 << 20;

/// Rows per repartitioned block and per zone-map zone: the
/// `WriteStrategyBuilder` default the store keeps.
const ROW_BLOCK: usize = 8192;

/// The compressor every store child is written with: the session's schemes,
/// limited to the wire forms its write editions allow, minus FastLanes delta.
/// Delta suits exactly the columns the store binary-searches in place (the
/// sorted subject column, the index children's lead columns), but a delta
/// value is a running sum, so a point read decodes its whole 1,024-value block
/// where a bit-packed or frame-of-reference column answers from one word. The
/// reader still probes delta, for files written elsewhere. The pinned core
/// edition leaves delta out too; the exclusion holds under one that admits it.
fn store_compressor() -> vortex_btrblocks::BtrBlocksCompressorBuilder {
    use vortex_btrblocks::schemes::integer::DeltaScheme;
    use vortex_btrblocks::{BtrBlocksCompressorBuilder, SchemeExt as _};

    BtrBlocksCompressorBuilder::from_session(&crate::session::VORTEX_SESSION)
        .exclude_schemes([DeltaScheme::new(1.25).id()])
}

/// The stock write strategy (`write_options()`'s) with the
/// [`store_compressor`]: what a store child's columns are written with, except
/// its id columns ([`child_strategy`]) and the dictionary child's
/// pre-compressed chunks (`dict_child_strategy`).
pub(crate) fn default_child_strategy() -> Arc<dyn vortex_layout::LayoutStrategy> {
    vortex_file::WriteStrategyBuilder::from_session(&crate::session::VORTEX_SESSION)
        .with_btrblocks_builder(store_compressor())
        .build()
}

/// The write strategy for a store child whose columns are `dtype`'s fields:
/// [`default_child_strategy`], with each id column
/// ([`is_id_field`](crate::store::schema::is_id_field): a `u64` term-code
/// `s`, `p`, `o`, `g` or `val`, or a `u64` row-id `rid`) written by
/// [`id_column_strategy`] instead, through the writer's per-field override
/// (`WriteStrategyBuilder::with_field_writer`). Every other column (the
/// Default and TypedObject layouts' strings) keeps the stock strategy.
///
/// The override cuts a plain id column into leaves of 262,144 rows (2 MiB
/// uncompressed, where the stock 1 MiB holds 131,072 `u64` values) and a
/// dictionary-encoded column's codes into leaves of 1 MiB: fewer leaves for
/// every probe that walks or rebuilds them.
pub(crate) fn child_strategy(dtype: &DType) -> Arc<dyn vortex_layout::LayoutStrategy> {
    child_strategy_with(dtype, ONE_MEG, 2 * ONE_MEG)
}

/// The fields of `dtype` that are id columns
/// ([`is_id_field`](crate::store::schema::is_id_field): a non-nullable `u64`
/// `s`, `p`, `o`, `g`, `val` or `rid`), in field order: the fields
/// [`child_strategy`] overrides.
pub(crate) fn id_fields(dtype: &DType) -> Vec<vortex_array::dtype::FieldName> {
    let DType::Struct(fields, _) = dtype else {
        return Vec::new();
    };
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .filter(|(name, field)| crate::store::schema::is_id_field(name.as_ref(), field))
        .map(|(name, _)| name.clone())
        .collect()
}

/// [`child_strategy`] with explicit [`id_column_strategy`] targets: the
/// tests' handle on the override, which at 1 MiB and 1 MiB must write exactly
/// what [`default_child_strategy`] writes.
pub(crate) fn child_strategy_with(
    dtype: &DType,
    codes_target: u64,
    fallback_target: u64,
) -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_array::dtype::FieldPath;

    let id_fields = id_fields(dtype);
    if id_fields.is_empty() {
        return default_child_strategy();
    }
    let id = id_column_strategy(codes_target, fallback_target);
    let mut builder =
        vortex_file::WriteStrategyBuilder::from_session(&crate::session::VORTEX_SESSION)
            .with_btrblocks_builder(store_compressor());
    for name in id_fields {
        builder = builder.with_field_writer(FieldPath::from_name(name), Arc::clone(&id));
    }
    builder.build()
}

/// An id column's leaf pipeline: the per-column pipeline
/// `WriteStrategyBuilder::build` assembles (vortex-file 0.88, `strategy.rs`)
/// step for step (repartition into 8 Ki-row blocks, zone statistics per block,
/// dictionary encoding or its fallback, coalescing, compression, buffering,
/// one flat leaf per chunk), with the coalescing target split in two:
/// `codes_target` for the codes of a column that dictionary-encodes,
/// `fallback_target` for a column that does not.
///
/// [`child_strategy`] passes 1 MiB and 2 MiB. A dictionary's codes are as
/// wide as its cardinality needs, so they keep the stock 1 MiB; a plain `u64`
/// column (a term-code column that does not dictionary-encode, or a row-id
/// column, whose ids are unique) takes 2 MiB, 262,144 rows per leaf. 1 MiB
/// twice gives the stock pipeline exactly, which a test pins by writing
/// columns both ways, so a Vortex upgrade that changes the stock pipeline
/// fails it.
fn id_column_strategy(
    codes_target: u64,
    fallback_target: u64,
) -> Arc<dyn vortex_layout::LayoutStrategy> {
    use std::num::NonZeroUsize;

    use vortex_btrblocks::SchemeExt as _;
    use vortex_btrblocks::schemes::integer::IntDictScheme;
    use vortex_layout::layouts::buffered::BufferedStrategy;
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::compressed::{CompressingStrategy, CompressorPlugin};
    use vortex_layout::layouts::dict::writer::DictStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::repartition::{RepartitionStrategy, RepartitionWriterOptions};
    use vortex_layout::layouts::zoned::writer::{ZonedLayoutOptions, ZonedStrategy};

    let compressor = store_compressor();
    let flat = FlatLayoutStrategy::default();
    // The data compressor leaves integer dictionaries to the dictionary
    // step, as the stock builder's does.
    let data_compressor: Arc<dyn CompressorPlugin> = Arc::new(
        compressor
            .clone()
            .exclude_schemes([IntDictScheme.id()])
            .build(),
    );
    // Coalesce to `target` bytes in whole blocks, then compress, buffer and
    // write one flat leaf per chunk.
    let coalescing = |target: u64| {
        let chunked = ChunkedLayoutStrategy::new(flat.clone());
        let buffered = BufferedStrategy::new(chunked, 2 * ONE_MEG);
        RepartitionStrategy::new(
            CompressingStrategy::new(buffered, Arc::clone(&data_compressor)),
            RepartitionWriterOptions {
                block_size_minimum: target,
                block_len_multiple: ROW_BLOCK,
                block_size_target: Some(target),
                canonicalize: true,
            },
        )
    };
    // Zone tables and dictionary values, compressed whole.
    let stats_compressor: Arc<dyn CompressorPlugin> = Arc::new(compressor.build());
    let compress_then_flat = CompressingStrategy::new(flat.clone(), Arc::clone(&stats_compressor));
    let dict = DictStrategy::new(
        coalescing(codes_target),
        compress_then_flat.clone(),
        coalescing(fallback_target),
        Default::default(),
        stats_compressor,
    );
    let zoned = ZonedStrategy::new(
        dict,
        compress_then_flat,
        ZonedLayoutOptions {
            block_size: NonZeroUsize::new(ROW_BLOCK).expect("the row block is not empty"),
            ..Default::default()
        },
    );
    Arc::new(RepartitionStrategy::new(
        zoned,
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: ROW_BLOCK,
            block_size_target: None,
            canonicalize: false,
        },
    ))
}

/// The dictionary child's strategy: every source chunk written verbatim as
/// one flat leaf under a chunked node (no sampling, no re-encoding), so the
/// leaves are the self-contained FSST windows (`TermDictionary::compress`)
/// the file-backed dictionary reads. With `zone_rows` (the window length) the
/// term column gets one zone per window, recording the window's exact first
/// and last term (`vortex.min()`/`vortex.max()`); `None`, for chunks of uneven
/// length, writes no zone map.
pub(crate) fn dict_child_strategy(
    zone_rows: Option<std::num::NonZeroUsize>,
) -> Arc<dyn vortex_layout::LayoutStrategy> {
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;
    use vortex_layout::layouts::zoned::writer::{ZonedLayoutOptions, ZonedStrategy};

    let leaves = ChunkedLayoutStrategy::new(FlatLayoutStrategy::default());
    let terms: Arc<dyn vortex_layout::LayoutStrategy> = match zone_rows {
        Some(block_size) => Arc::new(ZonedStrategy::new(
            leaves,
            FlatLayoutStrategy::default(),
            ZonedLayoutOptions {
                block_size,
                aggregate_fns: Some(
                    crate::store::layouts::dictionary::storage::window_bound_aggregates(),
                ),
                concurrency: std::num::NonZeroUsize::MIN,
            },
        )),
        None => Arc::new(leaves),
    };
    Arc::new(StructStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        terms,
    ))
}
