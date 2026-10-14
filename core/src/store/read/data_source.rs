//! A vortex [`DataSource`] over any store view, and over a store's persisted
//! children: the interface a query engine built on vortex (its DataFusion
//! integration first of all) reads this crate through.
//!
//! [`data_source`](VortexRdfStore::data_source) wraps a view (matched,
//! narrowed, windowed or whole) as a source whose rows are the view's rows in
//! base row order and whose dtype is the layout's primary struct
//! ([`TermCode`](crate::TermCode) columns under the Dictionary layout,
//! strings otherwise). A scan request's projection and filter are bound once
//! against that dtype and applied to every chunk the view streams
//! ([`row_chunks`](VortexRdfStore::row_chunks)), filter before projection;
//! its `row_range` and `selection` address the view's output rows; its
//! `limit` is enforced exactly, after filtering. The source answers one
//! partition, so a consumer that fans out over partitions sees the whole view
//! in one ordered stream.
//!
//! [`component_data_source`](VortexRdfStore::component_data_source) exposes a
//! store's persisted children (the copy children `index:posg` and
//! `index:ospg`, the reference children `index:ref-p` and `index:ref-o`, and
//! the `dictionary`) as plain tables, so an engine can read a sort order or
//! the term column directly.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::builtins::ArrayBuiltins as _;
use vortex_array::dtype::{DType, FieldPath};
use vortex_array::expr::BoundExpression;
use vortex_array::expr::stats::{Precision, Stat};
use vortex_array::scalar::ScalarValue;
use vortex_array::stats::StatsSet;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt, SendableArrayStream};
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
use crate::store::array::rechunk;
use crate::store::{LayoutStrategy, QuadsSource, VortexRdfStore};

/// The most rows a chunk of a partition's stream holds, DataFusion's default
/// batch size: gathered rows, rows read by id and an in-memory child's rows
/// are cut into chunks of this many, and a file scan's splits are cut to it.
pub(crate) const DATA_SOURCE_BATCH_ROWS: usize = 8_192;

/// Where a [`VortexRdfDataSource`] reads its rows.
#[derive(Clone)]
enum Rows {
    /// A store view, streamed in base row order from the first poll on.
    View(Arc<VortexRdfStore>),
    /// Resident chunks in their final dtype: an in-memory index child's rows,
    /// or the dictionary's term column.
    Chunks(Arc<[ArrayRef]>),
}

/// A [`DataSource`] over a store view or one of a store's in-memory
/// children; see the [module docs](self).
pub(crate) struct VortexRdfDataSource {
    rows: Rows,
    dtype: DType,
    row_count: Precision<u64>,
    byte_size: Precision<u64>,
}

impl VortexRdfStore {
    /// This view as a vortex [`DataSource`]: its rows in base row order,
    /// under the layout's primary struct dtype, every narrowing applied. Its
    /// row count is the [`view_statistics`](Self::view_statistics) count:
    /// exact when that is known without reading, otherwise its upper bound.
    /// Under the Dictionary layout its byte size is the rows' uncompressed
    /// width, four `u64` codes a row; a string layout's source has none.
    ///
    /// A scan reads through [`row_chunks`](Self::row_chunks), so nothing is
    /// read until a partition's stream is first polled, and an in-memory
    /// view's scan needs no runtime. A file-backed view's scan chooses at that
    /// first poll how it reads: its rows by id, or the file's splits. A read
    /// by id, a scan of at most 42 splits, and a scan first polled where no
    /// tokio runtime is current run inline on the polling task and need no
    /// runtime; any other scan spawns its splits onto the tokio runtime it is
    /// polled in, so every later poll of that stream must run inside a tokio
    /// runtime too.
    ///
    /// Errors for a Dictionary-layout view with a non-empty append tail: its
    /// tail holds strings whose terms have no code, so the view has no single
    /// dtype (`compact` the store first).
    pub fn data_source(&self) -> Result<DataSourceRef> {
        if self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() != 0 {
            self.ensure_code_view("data_source")?;
        }
        let dtype = self.primary_dtype();
        let (row_count, byte_size) = self.row_count_hint(&dtype)?;
        Ok(Arc::new(VortexRdfDataSource {
            rows: Rows::View(Arc::new(self.clone())),
            dtype,
            row_count,
            byte_size,
        }))
    }

    /// One of this store's persisted children as a plain table: an index
    /// child by the name [`index_components`](Self::index_components) lists
    /// it under (`index:posg`, `index:ospg`, `index:ref-p`, `index:ref-o`),
    /// its rows in the child's own order with the primary row id in `rid`; or
    /// the term `dictionary` (`{_dict_term}`, row i the term with code i),
    /// which needs a `file-io` or a wasm build. `None` for a name this store
    /// has no child of.
    ///
    /// An in-memory store's tables read its resident children and need no
    /// runtime. A file-backed store's table is vortex's own
    /// `LayoutReaderDataSource` over the child, whose scan spawns its work
    /// onto the tokio runtime it is polled in: read it inside a tokio
    /// runtime.
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
                let Some((_, reader)) = file
                    .component_reader(name)
                    .map_err(VortexRdfError::Vortex)?
                else {
                    return Ok(None);
                };
                Ok(Some(Arc::new(LayoutReaderDataSource::new(
                    reader,
                    VORTEX_SESSION.clone(),
                ))))
            }
        }
    }

    /// The struct dtype of this view's rows: the base's, which a string
    /// layout's tail shares.
    fn primary_dtype(&self) -> DType {
        match &self.quads {
            QuadsSource::InMemory { base, .. } => base.dtype().clone(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { file, .. } => file.dtype().clone(),
        }
    }

    /// The row count and byte size this view's source advertises: the
    /// [`view_statistics`](Self::view_statistics) count as a vortex precision
    /// (exact when known, otherwise the upper bound), and that count times
    /// the width of a `dtype` row when every field is a primitive.
    fn row_count_hint(&self, dtype: &DType) -> Result<(Precision<u64>, Precision<u64>)> {
        let hint = self.view_statistics()?.rows;
        let rows: Precision<u64> = match hint.exact {
            Some(n) => Precision::exact(n as u64),
            None => Precision::inexact(hint.upper_bound as u64),
        };
        let bytes = match row_width(dtype) {
            Some(width) => rows.map(|n| n.saturating_mul(width)),
            None => Precision::Absent,
        };
        Ok((rows, bytes))
    }
}

/// The uncompressed width of one row of `dtype`, in bytes: the sum of its
/// fields' primitive widths, or `None` when a field is not a primitive.
fn row_width(dtype: &DType) -> Option<u64> {
    let DType::Struct(fields, _) = dtype else {
        return None;
    };
    fields
        .fields()
        .map(|field| match field {
            DType::Primitive(ptype, _) => Some(ptype.byte_width() as u64),
            _ => None,
        })
        .sum()
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
        let projection = request.projection.bind(&self.dtype)?.optimize_recursive()?;
        let filter = request
            .filter
            .as_ref()
            .map(|f| f.bind(&self.dtype)?.optimize_recursive())
            .transpose()?;
        let dtype = projection.dtype().clone();
        // One partition, index 0, unless the request's partition window or
        // partition selection leaves it out.
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
        let ([part], DType::Struct(fields, _)) = (field_path.parts(), &self.dtype) else {
            return Ok(stats);
        };
        let Some(field) = part.as_name().and_then(|name| fields.field(name)) else {
            return Ok(stats);
        };
        // The store writes no nulls into a non-nullable column, and a
        // primitive column's uncompressed size follows from the row count.
        if !field.is_nullable() {
            stats.set(Stat::NullCount, Precision::exact(ScalarValue::from(0u64)));
        }
        if let (DType::Primitive(ptype, _), Precision::Exact(rows)) = (&field, self.row_count) {
            let bytes = rows.saturating_mul(ptype.byte_width() as u64);
            stats.set(
                Stat::UncompressedSizeInBytes,
                Precision::exact(ScalarValue::from(bytes)),
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

/// The partition of a [`ViewScan`]: the whole source, read from the first
/// poll on.
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
            return self.row_count.into_inexact();
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
        let ViewPartition {
            rows, dtype, plan, ..
        } = *self;
        let chunks: BoxStream<'static, Result<ArrayRef>> = match rows {
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
            plan,
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
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            dtype, stream,
        )))
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
            // A row the filter evaluates to null is dropped.
            let mask = chunk
                .clone()
                .apply_bound(filter)?
                .fill_null(false)?
                .execute::<Mask>(&mut ctx)?;
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
