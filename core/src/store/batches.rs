//! Arrow record-batch export: the chunk pipeline behind
//! [`VortexRdfStore::to_record_batches`].
//!
//! Two pipelines, chosen by [`TermEncoding`]. Code-typed exports (`codes`,
//! `terms`) read the primary columns as the Dictionary layout stores them
//! — `u32` code chunks off the in-memory base or straight from the file
//! scan, projected columns only — and convert each column buffer-sharing.
//! The string export rides the shared-term decode stream
//! ([`shared_quad_chunks`](VortexRdfStore::shared_quad_chunks)), which
//! already resolves codes, applies serve plans and appends the tail, and
//! builds a `StringViewArray` per column from it.

use std::sync::Arc;

use arrow_array::builder::StringViewBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{ArrayRef as ArrowArrayRef, DictionaryArray, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, future, stream};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_arrow::primitive::canonical_primitive_to_arrow;
use vortex_buffer::Buffer;

use crate::store::arrow::{
    QuadBatches, QuadColumn, TermEncoding, arrow_err, projected_schema, quad_schema,
};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::field_as;
use crate::store::layouts::ResolvedLayout;
use crate::store::{QuadsSource, SharedQuad, VortexRdfStore};

impl VortexRdfStore {
    /// The rows this view covers as Arrow record batches — one per decode
    /// chunk (the in-memory base, or each scan split of a file) — over the
    /// schema [`quad_schema`] gives `encoding` on this store's layout,
    /// restricted to `projection` (all four columns when `None`; a file scan
    /// reads only the projected columns).
    ///
    /// - [`TermEncoding::Codes`]: the Dictionary layout's `u32` code columns,
    ///   buffer-sharing with the store wherever the rows are already
    ///   canonical in memory.
    /// - [`TermEncoding::Terms`]: the same codes as Arrow dictionary keys over
    ///   the whole term dictionary
    ///   ([`DictSnapshot::to_arrow`](crate::store::DictSnapshot::to_arrow)),
    ///   one values array shared by every batch of the stream.
    /// - [`TermEncoding::Strings`]: N-Triples strings, decoded chunk by chunk.
    ///
    /// Codes and terms need every row to be code-addressable, so a view with
    /// a non-empty append tail (whose terms have no code in the dictionary)
    /// is rejected: export strings, or compact first. Tombstoned rows are
    /// never exported; empty chunks are skipped. The stream owns what it
    /// reads from and outlives this handle.
    pub async fn to_record_batches(
        &self,
        encoding: TermEncoding,
        projection: Option<&[QuadColumn]>,
    ) -> Result<QuadBatches> {
        let full = quad_schema(self.layout.strategy(), encoding)?;
        let columns: Vec<QuadColumn> =
            projection.map_or_else(|| QuadColumn::ALL.to_vec(), <[QuadColumn]>::to_vec);
        let schema = projected_schema(&full, &columns)?;
        match encoding {
            TermEncoding::Strings => self.string_batches(schema, columns),
            TermEncoding::Codes => self.code_batches(schema, columns, None).await,
            TermEncoding::Terms => {
                let values = self.dictionary_values().await?;
                self.code_batches(schema, columns, Some(values)).await
            }
        }
    }

    /// The whole term dictionary as the Arrow values array `terms` batches
    /// share, lifting a file-backed dictionary into memory for it.
    async fn dictionary_values(&self) -> Result<ArrowArrayRef> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) => access.ensure_resident().await?.arrow_values(),
            _ => Err(VortexRdfError::InvalidOperation(
                "term encoding \"terms\" needs the dictionary layout".to_string(),
            )),
        }
    }

    /// Code-typed batches: each primary-column chunk's `u32` columns as Arrow
    /// `UInt32` arrays, wrapped as dictionary keys over `values` when given.
    async fn code_batches(
        &self,
        schema: SchemaRef,
        columns: Vec<QuadColumn>,
        values: Option<ArrowArrayRef>,
    ) -> Result<QuadBatches> {
        if self.tail_len() != 0 {
            return Err(VortexRdfError::InvalidOperation(
                "the append tail's terms have no codes in this store's dictionary; export \
                 \"strings\", or compact the store first"
                    .to_string(),
            ));
        }
        // An in-memory view whose codes `code_columns_shared` serves hands
        // out those very buffers — a built base's canonical columns, or an
        // adopted base's live canonical form, shared with every holder alive;
        // anything else reads the primary chunks.
        let batches: BoxStream<'static, Result<RecordBatch>> = match self.code_columns_shared()? {
            Some(buffers) => {
                let batch = code_buffers_to_batch(&buffers, &schema, &columns, values.as_ref())?;
                stream::once(future::ready(Ok(batch))).boxed()
            }
            None => {
                let names: Vec<&'static str> = columns.iter().map(|c| c.name()).collect();
                let chunks = self.primary_chunks(&names).await?;
                let batch_schema = schema.clone();
                chunks
                    .map(move |chunk| {
                        code_chunk_to_batch(&chunk?, &batch_schema, &columns, values.as_ref())
                    })
                    .boxed()
            }
        };
        Ok(QuadBatches::new(schema, non_empty(batches)))
    }

    /// String batches over the shared-term decode stream.
    fn string_batches(&self, schema: SchemaRef, columns: Vec<QuadColumn>) -> Result<QuadBatches> {
        let chunks = self.shared_quad_chunks()?;
        let batch_schema = schema.clone();
        let batches =
            chunks.map(move |chunk| shared_chunk_to_batch(chunk, &batch_schema, &columns));
        Ok(QuadBatches::new(schema, non_empty(batches)))
    }

    /// The base's primary columns as encoded chunks, in base row order, with
    /// the view's selection applied and tombstones excluded: one chunk for an
    /// in-memory base (the array itself when the view covers all of it), one
    /// per scan split for a file, projected to `columns` there.
    // Without `file-io` there is no scan to project, and the in-memory
    // conversion picks its columns by name.
    #[cfg_attr(not(feature = "file-io"), allow(unused_variables))]
    async fn primary_chunks(
        &self,
        columns: &[&str],
    ) -> Result<BoxStream<'static, Result<ArrayRef>>> {
        match &self.quads {
            QuadsSource::InMemory { .. } => {
                let rows = self.base_selected_rows().await?;
                Ok(stream::once(future::ready(Ok(rows))).boxed())
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                file,
                filter,
                selection,
                deleted,
                ..
            } => {
                // Base row order needs the exact ids: a served match's
                // pending selection materializes here.
                let selection = selection.materialized_async().await?;
                let scan = self.restricted_file_scan_projected(
                    file,
                    filter.as_ref(),
                    &selection,
                    deleted.as_ref(),
                    columns,
                )?;
                let chunks = scan.into_stream().map_err(VortexRdfError::Vortex)?;
                Ok(chunks.map(|chunk| chunk.map_err(VortexRdfError::Vortex)).boxed())
            }
        }
    }
}

/// The four served code buffers as a record batch over `schema`'s columns,
/// each an Arrow `UInt32` array sharing the buffer, or those keys over
/// `values` as a dictionary array.
fn code_buffers_to_batch(
    buffers: &[Buffer<u32>; 4],
    schema: &SchemaRef,
    columns: &[QuadColumn],
    values: Option<&ArrowArrayRef>,
) -> Result<RecordBatch> {
    let arrays = columns
        .iter()
        .map(|column| {
            let buffer = buffers[column.index()].clone();
            let keys: ArrowArrayRef = Arc::new(UInt32Array::new(buffer.into_arrow_scalar_buffer(), None));
            keyed(keys, values)
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), arrays).map_err(arrow_err)
}

/// One `u32` primary-column chunk as a record batch over `schema`'s columns,
/// each column an Arrow `UInt32` array sharing the chunk's buffer, or those
/// keys over `values` as a dictionary array.
fn code_chunk_to_batch(
    chunk: &ArrayRef,
    schema: &SchemaRef,
    columns: &[QuadColumn],
    values: Option<&ArrowArrayRef>,
) -> Result<RecordBatch> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let rows = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let mut arrays: Vec<ArrowArrayRef> = Vec::with_capacity(columns.len());
    for column in columns {
        let codes = field_as::<PrimitiveArray>(&rows, column.name(), &mut ctx)?;
        let keys = canonical_primitive_to_arrow::<UInt32Type>(codes, &mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        arrays.push(keyed(keys, values)?);
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(arrow_err)
}

/// `keys` as they are, or as dictionary keys over `values`.
fn keyed(keys: ArrowArrayRef, values: Option<&ArrowArrayRef>) -> Result<ArrowArrayRef> {
    match values {
        None => Ok(keys),
        Some(values) => Ok(Arc::new(
            DictionaryArray::<UInt32Type>::try_new(
                keys.as_primitive::<UInt32Type>().clone(),
                values.clone(),
            )
            .map_err(arrow_err)?,
        )),
    }
}

/// One decoded chunk as a record batch of `Utf8View` columns.
fn shared_chunk_to_batch(
    chunk: Vec<Result<SharedQuad>>,
    schema: &SchemaRef,
    columns: &[QuadColumn],
) -> Result<RecordBatch> {
    let quads = chunk.into_iter().collect::<Result<Vec<_>>>()?;
    let arrays: Vec<ArrowArrayRef> = columns
        .iter()
        .map(|&column| {
            let mut builder = StringViewBuilder::with_capacity(quads.len());
            for quad in &quads {
                builder.append_value(term_of(column, quad));
            }
            Arc::new(builder.finish()) as ArrowArrayRef
        })
        .collect();
    RecordBatch::try_new(schema.clone(), arrays).map_err(arrow_err)
}

/// The term `column` holds in `quad`.
fn term_of(column: QuadColumn, quad: &SharedQuad) -> &str {
    match column {
        QuadColumn::S => &quad.s,
        QuadColumn::P => &quad.p,
        QuadColumn::O => &quad.o,
        QuadColumn::G => &quad.g,
    }
}

/// `batches` without its empty ones (a scan split or the tail may hold no
/// live row); errors pass through.
fn non_empty(
    batches: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
) -> BoxStream<'static, Result<RecordBatch>> {
    batches
        .filter(|batch| future::ready(!matches!(batch, Ok(batch) if batch.num_rows() == 0)))
        .boxed()
}
