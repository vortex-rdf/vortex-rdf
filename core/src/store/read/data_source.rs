//! A vortex [`DataSource`] over any store view — the interface a query
//! engine built on vortex (its DataFusion integration first of all) reads
//! this crate through without learning its view model.
//!
//! [`data_source`](VortexRdfStore::data_source) wraps a view — matched,
//! narrowed, windowed or whole — as a source whose rows are the view's rows
//! in base row order and whose dtype is the layout's primary struct (`u32`
//! codes under the Dictionary layout, strings otherwise). A scan request's
//! projection and filter are bound once against that dtype and applied to
//! every chunk the view streams ([`row_chunks`](VortexRdfStore::row_chunks)),
//! filter before projection; its `row_range` and `selection` address the
//! view's output rows; its `limit` is enforced exactly, after filtering. The
//! source answers one partition, so a consumer that fans out over partitions
//! sees the whole view in one ordered stream.
//!
//! [`component_data_source`](VortexRdfStore::component_data_source) exposes a
//! store's persisted children — the index copies (`index:posg`, `index:ospg`,
//! `index:ref-p`, `index:ref-o`) and the `dictionary` — as plain tables, so
//! an engine can read a sort order or the term column directly.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::dtype::{DType, FieldPath, PType};
use vortex_array::expr::BoundExpression;
use vortex_array::expr::stats::{Precision, Stat};
use vortex_array::scalar::ScalarValue;
use vortex_array::stats::StatsSet;
use vortex_array::stream::{ArrayStreamAdapter, SendableArrayStream};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute as _};
use vortex_error::{VortexError, VortexResult, vortex_err};
use vortex_mask::Mask;
use vortex_scan::selection::Selection;
use vortex_scan::{
    DataSource, DataSourceRef, DataSourceScan, DataSourceScanRef, Partition, PartitionRef,
    PartitionStream, ScanRequest,
};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::QuadsSource;
use crate::store::VortexRdfStore;
use crate::store::layouts::LayoutStrategy;
use crate::store::read::chunks::rechunk;

/// Rows per chunk a partition emits from an in-memory view — DataFusion's
/// default batch size; a file-backed view's chunks follow the file's splits
/// cut to this cap.
pub const DATA_SOURCE_BATCH_ROWS: usize = 8_192;

/// Where a [`VortexRdfDataSource`] reads its rows.
#[derive(Clone)]
enum Rows {
    /// A store view, streamed in base row order on first poll.
    View(Arc<VortexRdfStore>),
    /// Resident chunks in their final dtype (an index component's rows, the
    /// dictionary's term column).
    Chunks(Arc<[ArrayRef]>),
}

/// A [`DataSource`] over a store view or one of a store's persisted children;
/// see the [module docs](self).
pub struct VortexRdfDataSource {
    rows: Rows,
    dtype: DType,
    row_count: Precision<u64>,
    byte_size: Precision<u64>,
}

impl VortexRdfStore {
    /// This view as a vortex [`DataSource`]: its rows in base row order, under
    /// the layout's primary struct dtype, every narrowing applied. The row
    /// count is exact unless a pushed-down file filter is still pending
    /// (then an upper bound, the rows the filter has yet to test).
    ///
    /// Errors for a Dictionary-layout view with a non-empty append tail: its
    /// tail holds strings whose terms have no code, so the view has no single
    /// dtype — `compact` the store first.
    pub async fn data_source(&self) -> Result<DataSourceRef> {
        if self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() != 0 {
            self.ensure_code_view("data_source")?;
        }
        let dtype = self.primary_dtype()?;
        let (row_count, byte_size) = self.row_count_hint();
        Ok(Arc::new(VortexRdfDataSource {
            rows: Rows::View(Arc::new(self.clone())),
            dtype,
            row_count,
            byte_size,
        }))
    }

    /// One of this store's persisted children as a plain table: an index
    /// component by name (`index:posg`, `index:ospg`, `index:ref-p`,
    /// `index:ref-o`) — its rows sorted as the component's sort order says,
    /// with the primary row id in `rid` — or the term `dictionary`
    /// (`{_dict_term}`, row i = the term with code i). `None` for a name
    /// this store has no child of; the dictionary is reachable when it is
    /// resident or the store is file-backed.
    pub fn component_data_source(&self, name: &str) -> Result<Option<DataSourceRef>> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => {
                if let Some(component) = components.iter().find(|c| c.name == name) {
                    let rows = component.rows()?.clone().into_array();
                    return Ok(Some(chunks_source(vec![rows])));
                }
                #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
                if name == crate::io::container::DICT_COMPONENT_NAME
                    && let crate::store::layouts::ResolvedLayout::Dictionary(access) = &self.layout
                    && let Some(dict) = access.resident()
                {
                    return Ok(Some(chunks_source(dict.child_chunks()?)));
                }
                Ok(None)
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => {
                use vortex_layout::scan::layout::LayoutReaderDataSource;
                match file
                    .component_reader(name)
                    .map_err(VortexRdfError::Vortex)?
                {
                    Some((_, reader)) => Ok(Some(Arc::new(LayoutReaderDataSource::new(
                        reader,
                        VORTEX_SESSION.clone(),
                    )))),
                    None => Ok(None),
                }
            }
        }
    }

    /// The struct dtype of this view's rows: the base's, which a string
    /// layout's tail shares.
    fn primary_dtype(&self) -> Result<DType> {
        let dtype = match &self.quads {
            QuadsSource::InMemory { base, .. } => base.dtype().clone(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.dtype().clone(),
        };
        if !matches!(dtype, DType::Struct(..)) {
            return Err(VortexRdfError::InvalidOperation(format!(
                "a store's rows are a struct, found {dtype}"
            )));
        }
        Ok(dtype)
    }

    /// The row count and byte size a source advertises — the view's
    /// statistics as vortex precisions: exact whenever
    /// [`view_statistics`](Self::view_statistics) knows the count, else
    /// its upper bound.
    fn row_count_hint(&self) -> (Precision<u64>, Precision<u64>) {
        let hint = self.view_statistics().rows;
        let rows = match hint.exact {
            Some(n) => Precision::exact(n as u64),
            None => Precision::inexact(hint.upper_bound as u64),
        };
        let bytes = match self.layout.strategy() {
            // Four `u32` columns: the uncompressed footprint of the rows.
            LayoutStrategy::Dictionary => rows.map(|n| n * 16),
            _ => Precision::Absent,
        };
        (rows, bytes)
    }
}

/// A source over resident chunks already in their final dtype.
fn chunks_source(chunks: Vec<ArrayRef>) -> DataSourceRef {
    let dtype = chunks
        .first()
        .map(|c| c.dtype().clone())
        .unwrap_or_else(|| DType::Struct(Default::default(), Default::default()));
    let rows: u64 = chunks.iter().map(|c| c.len() as u64).sum();
    Arc::new(VortexRdfDataSource {
        rows: Rows::Chunks(chunks.into()),
        dtype,
        row_count: Precision::exact(rows),
        byte_size: Precision::Absent,
    })
}

#[async_trait]
impl DataSource for VortexRdfDataSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn row_count(&self) -> Precision<u64> {
        self.row_count
    }

    fn byte_size(&self) -> Precision<u64> {
        self.byte_size
    }

    async fn scan(&self, request: ScanRequest) -> VortexResult<DataSourceScanRef> {
        let projection = request
            .projection
            .optimize_recursive(&self.dtype)?
            .bind(&self.dtype)?;
        let filter = request
            .filter
            .as_ref()
            .map(|f| f.optimize_recursive(&self.dtype)?.bind(&self.dtype))
            .transpose()?;
        let dtype = projection.dtype().clone();
        // One partition, index 0 — unless the request's partition window
        // leaves it out.
        let wanted = request
            .partition_range
            .as_ref()
            .is_none_or(|r| r.contains(&0))
            && request
                .partition_selection
                .row_mask(&(0..1))
                .mask()
                .value(0);
        let partition = wanted.then(|| ViewPartition {
            rows: self.rows.clone(),
            dtype: dtype.clone(),
            plan: Arc::new(RequestPlan {
                projection,
                filter,
                row_range: request.row_range,
                selection: request.selection,
                limit: request.limit,
            }),
            row_count: self.row_count,
            byte_size: self.byte_size,
        });
        Ok(Box::new(ViewScan { dtype, partition }))
    }

    async fn field_statistics(&self, field_path: &FieldPath) -> VortexResult<StatsSet> {
        let mut stats = StatsSet::default();
        let Some(name) = field_path.parts().first().and_then(|f| f.as_name()) else {
            return Ok(stats);
        };
        let DType::Struct(fields, _) = &self.dtype else {
            return Ok(stats);
        };
        let Some(field) = fields.field(name) else {
            return Ok(stats);
        };
        // The store writes no nulls into a non-nullable column, and a `u32`
        // code column's uncompressed footprint follows from the row count.
        if !field.is_nullable() {
            stats.set(Stat::NullCount, Precision::exact(ScalarValue::from(0u64)));
        }
        if let (DType::Primitive(PType::U32, _), Precision::Exact(rows)) = (&field, self.row_count)
        {
            stats.set(
                Stat::UncompressedSizeInBytes,
                Precision::exact(ScalarValue::from(rows * 4)),
            );
        }
        Ok(stats)
    }
}

/// A scan of a [`VortexRdfDataSource`]: its one partition, or none.
struct ViewScan {
    dtype: DType,
    partition: Option<ViewPartition>,
}

impl DataSourceScan for ViewScan {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn partition_count(&self) -> Precision<usize> {
        Precision::exact(usize::from(self.partition.is_some()))
    }

    fn partitions(self: Box<Self>) -> PartitionStream {
        stream::iter(
            self.partition
                .into_iter()
                .map(|p| Ok(Box::new(p) as PartitionRef)),
        )
        .boxed()
    }
}

/// The request a partition applies to every chunk it reads.
struct RequestPlan {
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    row_range: Option<Range<u64>>,
    selection: Selection,
    limit: Option<u64>,
}

/// The partition of a [`ViewScan`]: the whole view, read on first poll.
struct ViewPartition {
    rows: Rows,
    dtype: DType,
    plan: Arc<RequestPlan>,
    row_count: Precision<u64>,
    byte_size: Precision<u64>,
}

impl Partition for ViewPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn index(&self) -> usize {
        0
    }

    fn row_count(&self) -> Precision<u64> {
        let plan = &self.plan;
        if plan.filter.is_some() || !matches!(plan.selection, Selection::All) {
            // Rows still to test: the count is a bound at best.
            return match self.row_count {
                Precision::Exact(n) | Precision::Inexact(n) => Precision::inexact(n),
                Precision::Absent => Precision::Absent,
            };
        }
        let mut rows = self.row_count;
        if let Some(range) = &plan.row_range {
            rows = rows.map(|n| n.min(range.end).saturating_sub(range.start));
        }
        if let Some(limit) = plan.limit {
            rows = rows.map(|n| n.min(limit));
        }
        rows
    }

    fn byte_size(&self) -> Precision<u64> {
        self.byte_size
    }

    fn execute(self: Box<Self>) -> VortexResult<SendableArrayStream> {
        let chunks: BoxStream<'static, Result<ArrayRef>> = match self.rows {
            Rows::View(store) => store
                .row_chunks(DATA_SOURCE_BATCH_ROWS)
                .map_err(to_vortex)?,
            Rows::Chunks(chunks) => {
                let pieces: Vec<ArrayRef> = chunks
                    .iter()
                    .map(|c| rechunk(c.clone(), DATA_SOURCE_BATCH_ROWS))
                    .collect::<Result<Vec<_>>>()
                    .map_err(to_vortex)?
                    .into_iter()
                    .flatten()
                    .collect();
                stream::iter(pieces.into_iter().map(Ok)).boxed()
            }
        };
        let state = RequestState {
            plan: self.plan,
            offset: 0,
            remaining: None,
            done: false,
        };
        let stream = stream::unfold((chunks, state), |(mut chunks, mut state)| async move {
            loop {
                if state.done {
                    return None;
                }
                let chunk = match chunks.next().await? {
                    Ok(chunk) => chunk,
                    Err(e) => return Some((Err(to_vortex(e)), (chunks, state))),
                };
                match state.apply(chunk) {
                    Ok(Some(out)) => return Some((Ok(out), (chunks, state))),
                    Ok(None) => continue,
                    Err(e) => return Some((Err(e), (chunks, state))),
                }
            }
        });
        Ok(vortex_array::stream::ArrayStreamExt::boxed(
            ArrayStreamAdapter::new(self.dtype, stream),
        ))
    }
}

/// The per-chunk application of a [`RequestPlan`]: output-row bookkeeping
/// for `row_range`/`selection`, the filter, the limit, then the projection.
struct RequestState {
    plan: Arc<RequestPlan>,
    /// Output rows (before any filtering) the chunks so far covered.
    offset: u64,
    /// Rows still to emit under the limit, once initialized.
    remaining: Option<u64>,
    done: bool,
}

impl RequestState {
    fn apply(&mut self, chunk: ArrayRef) -> VortexResult<Option<ArrayRef>> {
        let len = chunk.len() as u64;
        let start = self.offset;
        self.offset += len;
        let (mut lo, mut hi) = (0u64, len);
        if let Some(range) = &self.plan.row_range {
            if start >= range.end {
                self.done = true;
                return Ok(None);
            }
            lo = range.start.saturating_sub(start).min(len);
            hi = range.end.saturating_sub(start).min(len);
        }
        if lo >= hi {
            return Ok(None);
        }
        let mut chunk = if lo > 0 || hi < len {
            chunk.slice(lo as usize..hi as usize)?
        } else {
            chunk
        };
        if !matches!(self.plan.selection, Selection::All) {
            let mask: Mask = self
                .plan
                .selection
                .row_mask(&(start + lo..start + hi))
                .mask()
                .clone();
            if mask.all_false() {
                return Ok(None);
            }
            if !mask.all_true() {
                chunk = chunk.filter(mask)?;
            }
        }
        if let Some(filter) = &self.plan.filter {
            let mut ctx = VORTEX_SESSION.create_execution_ctx();
            let mask = chunk
                .clone()
                .apply_bound(filter)?
                .null_as_false()
                .execute(&mut ctx)?;
            if mask.all_false() {
                return Ok(None);
            }
            if !mask.all_true() {
                chunk = chunk.filter(mask)?;
            }
        }
        if let Some(limit) = self.plan.limit {
            let remaining = self.remaining.get_or_insert(limit);
            if *remaining == 0 {
                self.done = true;
                return Ok(None);
            }
            let rows = chunk.len() as u64;
            if rows >= *remaining {
                chunk = chunk.slice(0..*remaining as usize)?;
                *remaining = 0;
                self.done = true;
            } else {
                *remaining -= rows;
            }
        }
        if chunk.is_empty() {
            return Ok(None);
        }
        Ok(Some(chunk.apply_bound(&self.plan.projection)?))
    }
}

/// This crate's error as the vortex error a [`DataSource`] reports.
fn to_vortex(e: VortexRdfError) -> VortexError {
    match e {
        VortexRdfError::Vortex(v) => v,
        other => vortex_err!("vortex-rdf: {other}"),
    }
}
