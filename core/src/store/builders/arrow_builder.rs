//! Apache Arrow interop: turning Arrow record batches with `s`, `p`, `o`,
//! `g` string columns into the [`RawQuad`] stream every store constructor
//! and builder consumes (see
//! [`VortexRdfStore::from_quads`](crate::VortexRdfStore::from_quads)).
//!
//! Column values are read as already-serialized N-Triples term strings — the
//! same contract [`parse_quads_from_reader`](crate::common::terms::parse_quads_from_reader)
//! produces — but, unlike that trusted decode path, an Arrow source did not
//! come out of the store's own columns, so every term is re-validated
//! through [`raw_quad_checked`] the way a user-typed pattern is.
//!
//! Compiled in only under the `arrow` feature, off by default so bindings
//! that never touch Arrow (wasm, the CLI) don't link `arrow-array`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::DataType;

use crate::error::{Result, VortexRdfError};
use crate::store::layouts::dictionary::ingest::{InterningQuadBuilder, finish_interned};
use crate::store::schema::PRIMARY_COLUMNS;
use crate::{BuiltArray, BuiltStream, Indexes, LayoutStrategy, debug};

/// The four quad-term columns an Arrow source's schema must carry.
const QUAD_COLUMNS: [&str; 4] = PRIMARY_COLUMNS;

/// A source of Arrow-shaped quad data, convertible into the [`RawQuad`]
/// stream every builder consumes.
///
/// Implemented for a single [`RecordBatch`] and for a boxed
/// [`RecordBatchReader`] — the latter covers a multi-batch reader (a
/// pyarrow `Table`'s batches bridged through the Arrow C Stream interface,
/// or a chunked read off disk) without buffering the whole table into one
/// batch first. Both are concrete types rather than one blanket impl over
/// `RecordBatchReader`, since `RecordBatch` and that trait are both foreign
/// (defined in `arrow-array`) and Rust's coherence check cannot rule out a
/// future upstream `impl RecordBatchReader for RecordBatch`.
pub trait ArrowBuilder {
    fn into_vortex_array(
        self,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;
}

impl ArrowBuilder for Box<dyn RecordBatchReader + Send> {
    async fn into_vortex_array(
        mut self,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();
        let (n, build_start, built);
        //TODO: Implement building with other layout strategies!
        if layout == LayoutStrategy::Dictionary {
            build_start = debug::timer();
            let mut interner = InterningQuadBuilder::new();
            while let Some(batch) = self.next().transpose()? {
                intern_batch(&batch, &mut interner)?;
            }
            built = finish_interned(interner, &indexes)?;
            n = built.array.len();
            log::debug!(
                "[ArrowBuilder] for [RecordBatchReader] Constructed StructArray in {:?}",
                debug::elapsed(build_start)
            );
            log::debug!(
                "[ArrowBuilder] for [RecordBatchReader] Completed serialization of {} quads in {:?}",
                n,
                debug::elapsed(start)
            );
            Ok(built)
        } else {
            Err(VortexRdfError::Deserialization(format!(
                "arrow deserializing: only suppots dictionary layout for now. Current layout: {layout:?}"
            )))
        }
    }
}

impl ArrowBuilder for RecordBatch {
    async fn into_vortex_array(
        self,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();
        let (n, build_start, built);
        //TODO: Implement building with other layout strategies!
        if layout == LayoutStrategy::Dictionary {
            build_start = debug::timer();
            let mut interner = InterningQuadBuilder::new();
            intern_batch(&self, &mut interner)?;
            built = finish_interned(interner, &indexes)?;
            n = built.array.len();
            log::debug!(
                "[ArrowBuilder] for [RecordBatch] Constructed StructArray in {:?}",
                debug::elapsed(build_start)
            );
            log::debug!(
                "[ArrowBuilder] for [RecordBatch] Completed serialization of {} quads in {:?}",
                n,
                debug::elapsed(start)
            );
            Ok(built)
        } else {
            Err(VortexRdfError::Deserialization(format!(
                "arrow deserializing: only suppots dictionary layout for now. Current layout: {layout:?}"
            )))
        }
    }
}

fn intern_batch(batch: &RecordBatch, interner: &mut InterningQuadBuilder) -> Result<()> {
    let columns: Vec<_> = QUAD_COLUMNS
        .iter()
        .map(|name| string_column(&batch, name))
        .collect::<Result<_>>()?;
    let (s, p, o, g) = (&columns[0], &columns[1], &columns[2], &columns[3]);
    interner.push_s_col(s);
    interner.push_p_col(p);
    interner.push_o_col(o);
    interner.push_g_col(g);
    Ok(())
}

/// `batch`'s column named `name`, cast to `Utf8` and read as owned strings;
/// a null in the column is refused, since a quad term is never absent.
fn string_column(batch: &RecordBatch, name: &str) -> Result<Vec<Box<str>>> {
    let idx = batch.schema_ref().index_of(name).map_err(|_| {
        VortexRdfError::Deserialization(format!(
            "arrow quad source: missing required column {name:?}"
        ))
    })?;

    let casted_column = arrow_cast::cast(batch.column(idx), &arrow_schema::DataType::Utf8)
        .map_err(|_| {
            VortexRdfError::Deserialization(format!(
                "arrow quad source: cannot cast column {name:?} to utf8"
            ))
        })?;

    let strings = casted_column.as_string_opt::<i32>().ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "arrow quad source: column {name:?} did not cast to a utf8 string array"
        ))
    })?;

    if strings.null_count() > 0 {
        return Err(VortexRdfError::Deserialization(format!(
            "arrow quad source: column {name:?} contains a null term"
        )));
    }

    Ok(strings
        .iter()
        .map(|s| Box::from(s.expect("null checked above")))
        .collect())
}
