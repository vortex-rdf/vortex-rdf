use crate::error::{Result, VortexRdfError};
use crate::store::layout::cottas::TripleOrdering;

use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfSerializer};

use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Instant;

use vortex::expr::{Expression, and, col, eq, lit};
use vortex_array::arrays::{StructArray, VarBinViewArray};
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_array::{ArrayRef, IntoArray};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use vortex::dtype::{DType, Nullability};

use vortex_btrblocks::BtrBlocksCompressorBuilder;

static NATIVE_STRING_FILE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    use vortex_array::scalar_fn::session::ScalarFnSession;
    use vortex_array::session::ArraySession;
    use vortex_io::session::RuntimeSession;
    use vortex_layout::session::LayoutSession;

    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();

    vortex_file::register_default_encodings(&session);
    session
});

use crate::common::utils::{parse_graph_name, parse_named_node, parse_subject, parse_term};
use vortex_array::arrays::struct_::StructArrayExt;

#[derive(Clone, Copy, Debug)]
pub enum CottasVortexCompressionProfile {
    Balanced,
    Compact,
}

#[derive(Clone, Debug)]
pub struct CottasNativeStringConfig {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
    pub compression_profile: CottasVortexCompressionProfile,
}

impl Default for CottasNativeStringConfig {
    fn default() -> Self {
        Self {
            ordering: TripleOrdering::SPO,
            // COTTAS / DuckDB baseline from the paper.
            row_group_size: 122_880,
            compression_profile: CottasVortexCompressionProfile::Balanced,
        }
    }
}

#[derive(Clone, Debug)]
struct NativeStringQuad {
    s: String,
    p: String,
    o: String,
    g: String,
}

impl NativeStringQuad {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> std::cmp::Ordering {
        match ordering {
            TripleOrdering::SPO => self
                .s
                .cmp(&other.s)
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::PSO => self
                .p
                .cmp(&other.p)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.o.cmp(&other.o))
                .then_with(|| self.g.cmp(&other.g)),
            TripleOrdering::OSP => self
                .o
                .cmp(&other.o)
                .then_with(|| self.s.cmp(&other.s))
                .then_with(|| self.p.cmp(&other.p))
                .then_with(|| self.g.cmp(&other.g)),
        }
    }
}

fn quad_to_native_string_quad(quad: &Quad) -> NativeStringQuad {
    NativeStringQuad {
        s: quad.subject.to_string(),
        p: quad.predicate.to_string(),
        o: quad.object.to_string(),
        g: quad.graph_name.to_string(),
    }
}

fn build_string_spog_array(group: &[NativeStringQuad]) -> Result<ArrayRef> {
    let s = VarBinViewArray::from_iter(
        group.iter().map(|q| Some(q.s.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    let p = VarBinViewArray::from_iter(
        group.iter().map(|q| Some(q.p.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    let o = VarBinViewArray::from_iter(
        group.iter().map(|q| Some(q.o.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    let g = VarBinViewArray::from_iter(
        group.iter().map(|q| Some(q.g.as_str())),
        DType::Utf8(Nullability::NonNullable),
    );

    let arr = StructArray::from_fields(&[
        ("s", s.into_array()),
        ("p", p.into_array()),
        ("o", o.into_array()),
        ("g", g.into_array()),
    ])
    .map_err(VortexRdfError::Vortex)?
    .into_array();

    Ok(arr)
}

fn empty_string_spog_array() -> Result<ArrayRef> {
    build_string_spog_array(&[])
}

async fn collect_globally_sorted_string_row_groups<S>(
    mut quad_stream: S,
    ordering: TripleOrdering,
    row_group_size: usize,
) -> Result<Vec<Vec<NativeStringQuad>>>
where
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let row_group_size = row_group_size.max(1);

    let mut quads = Vec::new();

    while let Some(item) = quad_stream.next().await {
        let quad = item?;
        quads.push(quad_to_native_string_quad(&quad));
    }

    quads.sort_by(|a, b| a.cmp_by_order(b, ordering));

    Ok(quads
        .chunks(row_group_size)
        .map(|chunk| chunk.to_vec())
        .collect())
}

async fn write_string_array_stream_to_vortex_file<W>(
    writer: &mut W,
    arrays: Vec<ArrayRef>,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = arrays
        .first()
        .map(|a| a.dtype().clone())
        .unwrap_or_else(|| {
            empty_string_spog_array()
                .expect("empty string SPOG array must be constructible")
                .dtype()
                .clone()
        });

    let stream = ArrayStreamAdapter::new(dtype, Box::pin(stream::iter(arrays.into_iter().map(Ok))));

    let strategy_builder =
        WriteStrategyBuilder::default().with_row_block_size(row_group_size.max(1));

    let strategy_builder = match compression_profile {
        CottasVortexCompressionProfile::Balanced => strategy_builder,

        CottasVortexCompressionProfile::Compact => strategy_builder
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact()),
    };

    let strategy = strategy_builder.build();

    let write_opts = NATIVE_STRING_FILE_SESSION
        .write_options()
        .with_strategy(strategy);

    let start = Instant::now();

    write_opts
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native_strings::write_string_array_stream_to_vortex_file] wrote native string COTTAS Vortex file in {:?}",
        start.elapsed()
    );

    Ok(())
}

pub async fn serialize_cottas_native_string_file<S>(
    quad_stream: S,
    output_path: &Path,
    config: CottasNativeStringConfig,
) -> Result<()>
where
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let groups = collect_globally_sorted_string_row_groups(
        quad_stream,
        config.ordering,
        config.row_group_size,
    )
    .await?;

    let mut row_group_arrays = Vec::with_capacity(groups.len());

    for group in &groups {
        row_group_arrays.push(build_string_spog_array(group)?);
    }

    if row_group_arrays.is_empty() {
        row_group_arrays.push(empty_string_spog_array()?);
    }

    let mut data_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_string_array_stream_to_vortex_file(
        &mut data_file,
        row_group_arrays,
        config.row_group_size,
        config.compression_profile,
    )
    .await?;

    log::info!(
        "[cottas_native_strings] wrote native string COTTAS Vortex file {:?} with profile {:?}",
        output_path,
        config.compression_profile
    );

    Ok(())
}

enum NativeStringPatternFilter {
    All,
    Expr(Expression),
}

fn build_native_string_pattern_filter(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> NativeStringPatternFilter {
    let mut filters: Vec<Expression> = Vec::new();

    if let Some(subject) = subject {
        filters.push(eq(col("s"), lit(subject.to_string())));
    }

    if let Some(predicate) = predicate {
        filters.push(eq(col("p"), lit(predicate.to_string())));
    }

    if let Some(object) = object {
        filters.push(eq(col("o"), lit(object.to_string())));
    }

    if let Some(graph) = graph {
        filters.push(eq(col("g"), lit(graph.to_string())));
    }

    match filters.into_iter().reduce(and) {
        Some(expr) => NativeStringPatternFilter::Expr(expr),
        None => NativeStringPatternFilter::All,
    }
}

async fn write_string_quads_array_as_rdf<W>(
    quads: ArrayRef,
    writer: W,
    format: RdfFormat,
) -> Result<()>
where
    W: Write,
{
    use vortex::VortexSessionDefault;
    use vortex_array::VortexSessionExecute;

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let struct_array = quads
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let s_arr = struct_array
        .unmasked_field_by_name("s")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let p_arr = struct_array
        .unmasked_field_by_name("p")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let o_arr = struct_array
        .unmasked_field_by_name("o")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let g_arr = struct_array
        .unmasked_field_by_name("g")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    for i in 0..struct_array.len() {
        let s = String::from_utf8_lossy(&s_arr.bytes_at(i)).into_owned();
        let p = String::from_utf8_lossy(&p_arr.bytes_at(i)).into_owned();
        let o = String::from_utf8_lossy(&o_arr.bytes_at(i)).into_owned();
        let g = String::from_utf8_lossy(&g_arr.bytes_at(i)).into_owned();
        let quad = Quad {
            subject: parse_subject(&s)?,
            predicate: parse_named_node(&p)?,
            object: parse_term(&o)?,
            graph_name: parse_graph_name(&g)?,
        };

        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    Ok(())
}

pub async fn match_cottas_native_string_file<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    writer: W,
    format: RdfFormat,
) -> Result<()>
where
    W: Write,
{
    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);

    let open_start = Instant::now();

    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native_strings::match] opened native string COTTAS file in {:?}",
        open_start.elapsed()
    );

    //if let NativeStringPatternFilter::Expr(expr) = &filter {
    //    match file.can_prune(expr) {
    //        Ok(can_prune) => {
    //            log::debug!(
    //                "[cottas_native_strings::match] file.can_prune(filter) = {}",
    //                can_prune
    //            );
    //        }
    //        Err(e) => {
    //            log::debug!(
    //                "[cottas_native_strings::match] file.can_prune(filter) failed: {}",
    //                e
    //            );
    //        }
    //    }
    //}

    //match file.splits() {
    //    Ok(splits) => {
    //        log::debug!(
    //            "[cottas_native_strings::match] native string file has {} scan splits: {:?}",
    //            splits.len(),
    //            splits
    //        );
    //    }
    //    Err(e) => {
    //        log::debug!(
    //            "[cottas_native_strings::match] failed to inspect native string file splits: {}",
    //            e
    //        );
    //    }
    //}

    let scan_start = Instant::now();

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativeStringPatternFilter::All => scan,
        NativeStringPatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native_strings::match] scan builder setup took {:?}",
        scan_start.elapsed()
    );

    let read_start = Instant::now();

    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native_strings::match] filtered scan materialized {} rows in {:?}",
        matched_quads.len(),
        read_start.elapsed()
    );

    write_string_quads_array_as_rdf(matched_quads, writer, format).await
}
