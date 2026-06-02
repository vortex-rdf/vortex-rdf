use crate::common::indexes::wrap_array_in_list;
use crate::error::{Result, VortexRdfError};
use crate::index::{RdfDictionary, SimpleDictionaryView};
use crate::store::layout::cottas::TripleOrdering;

use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use vortex_array::arrays::{ConstantArray, PrimitiveArray, StructArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use crate::store::layout::RdfQuadLayout;
use crate::store::layout::flat::FlatLayout;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;

use vortex::expr::{Expression, and, col, eq, lit};
use vortex_array::stream::ArrayStreamExt;

static NATIVE_FILE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
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

#[derive(Clone, Debug)]
pub struct CottasNativeConfig {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
}

impl Default for CottasNativeConfig {
    fn default() -> Self {
        Self {
            ordering: TripleOrdering::SPO,
            row_group_size: 1024,
        }
    }
}

#[derive(Clone, Debug)]
struct NativeTriple {
    s: String,
    p: String,
    o: String,
    g: String,
}

impl NativeTriple {
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

fn quad_to_native_triple(quad: &Quad) -> NativeTriple {
    NativeTriple {
        s: quad.subject.to_string(),
        p: quad.predicate.to_string(),
        o: quad.object.to_string(),
        g: quad.graph_name.to_string(),
    }
}

fn native_dict_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");

    data_path.with_file_name(format!("{file_name}.dict.vortex"))
}

fn seed_dictionary_from_groups<Dict>(dictionary: &mut Dict, groups: &[Vec<NativeTriple>])
where
    Dict: RdfDictionary,
{
    let mut terms: Vec<&str> = groups
        .iter()
        .flat_map(|group| {
            group.iter().flat_map(|triple| {
                [
                    triple.s.as_str(),
                    triple.p.as_str(),
                    triple.o.as_str(),
                    triple.g.as_str(),
                ]
            })
        })
        .collect();

    terms.sort_unstable();
    terms.dedup();

    let _ = dictionary.get_or_insert_bulk(&terms);
}

fn encode_group_to_array<Dict>(dictionary: &Dict, group: &[NativeTriple]) -> Result<ArrayRef>
where
    Dict: RdfDictionary,
{
    let mut s_ids = Vec::with_capacity(group.len());
    let mut p_ids = Vec::with_capacity(group.len());
    let mut o_ids = Vec::with_capacity(group.len());
    let mut g_ids = Vec::with_capacity(group.len());

    for triple in group {
        s_ids.push(dictionary.get_id(&triple.s).ok_or_else(|| {
            VortexRdfError::Serialization(format!("Missing subject in dictionary: {}", triple.s))
        })?);

        p_ids.push(dictionary.get_id(&triple.p).ok_or_else(|| {
            VortexRdfError::Serialization(format!("Missing predicate in dictionary: {}", triple.p))
        })?);

        o_ids.push(dictionary.get_id(&triple.o).ok_or_else(|| {
            VortexRdfError::Serialization(format!("Missing object in dictionary: {}", triple.o))
        })?);

        g_ids.push(dictionary.get_id(&triple.g).ok_or_else(|| {
            VortexRdfError::Serialization(format!("Missing graph in dictionary: {}", triple.g))
        })?);
    }

    build_spog_array(s_ids, p_ids, o_ids, g_ids)
}

fn build_spog_array(
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    let arr = StructArray::from_fields(&[
        ("s", PrimitiveArray::from_iter(s_ids).into_array()),
        ("p", PrimitiveArray::from_iter(p_ids).into_array()),
        ("o", PrimitiveArray::from_iter(o_ids).into_array()),
        ("g", PrimitiveArray::from_iter(g_ids).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)?
    .into_array();

    Ok(arr)
}

fn empty_spog_array() -> Result<ArrayRef> {
    build_spog_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())
}

fn build_dictionary_root<Dict>(dictionary: &Dict) -> Result<ArrayRef>
where
    Dict: RdfDictionary,
{
    let mut field_names: Vec<Arc<str>> = Vec::new();
    let mut field_arrays: Vec<ArrayRef> = Vec::new();

    field_names.push(Arc::<str>::from("store_type"));
    field_arrays.push(ConstantArray::new(Dict::store_type(), 1).into_array());

    for (name, arr) in dictionary.to_vortex_array()? {
        field_names.push(Arc::<str>::from(name));
        field_arrays.push(wrap_array_in_list(arr)?);
    }

    StructArray::try_new(field_names.into(), field_arrays, 1, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|arr| arr.into_array())
}

async fn write_array_stream_to_vortex_file<W>(
    writer: &mut W,
    arrays: Vec<ArrayRef>,
    row_group_size: usize,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = arrays
        .first()
        .map(|a| a.dtype().clone())
        .unwrap_or_else(|| {
            empty_spog_array()
                .expect("empty SPOG array must be constructible")
                .dtype()
                .clone()
        });

    let stream = ArrayStreamAdapter::new(dtype, Box::pin(stream::iter(arrays.into_iter().map(Ok))));

    let write_opts = NATIVE_FILE_SESSION.write_options().with_strategy(
        WriteStrategyBuilder::default()
            .with_row_block_size(row_group_size.max(1))
            .build(),
    );

    let start = Instant::now();

    write_opts
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::write_array_stream_to_vortex_file] wrote native Vortex file in {:?}",
        start.elapsed()
    );

    Ok(())
}

async fn write_single_array_to_vortex_file<W>(writer: &mut W, array: ArrayRef) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = array.dtype().clone();

    let stream = ArrayStreamAdapter::new(dtype, Box::pin(stream::once(async move { Ok(array) })));

    let write_opts = NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(WriteStrategyBuilder::default().build());

    write_opts
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;

    Ok(())
}

pub async fn serialize_cottas_native_file<Dict, S>(
    quad_stream: S,
    output_path: &Path,
    config: CottasNativeConfig,
) -> Result<()>
where
    Dict: RdfDictionary,
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let groups =
        collect_globally_sorted_row_groups(quad_stream, config.ordering, config.row_group_size)
            .await?;

    let mut dictionary = Dict::new();
    seed_dictionary_from_groups(&mut dictionary, &groups);

    let mut row_group_arrays = Vec::with_capacity(groups.len());
    for group in &groups {
        row_group_arrays.push(encode_group_to_array(&dictionary, group)?);
    }

    if row_group_arrays.is_empty() {
        row_group_arrays.push(empty_spog_array()?);
    }

    let mut data_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_array_stream_to_vortex_file(&mut data_file, row_group_arrays, config.row_group_size)
        .await?;

    let dict_root = build_dictionary_root(&dictionary)?;
    let dict_path = native_dict_path(output_path);

    let mut dict_file = tokio::fs::File::create(&dict_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_single_array_to_vortex_file(&mut dict_file, dict_root).await?;

    log::info!(
        "[cottas_native] wrote native COTTAS data file {:?} and dictionary sidecar {:?}",
        output_path,
        dict_path
    );

    Ok(())
}

pub async fn load_cottas_native_dictionary<Dict>(data_path: &Path) -> Result<Dict>
where
    Dict: RdfDictionary,
{
    let dict_path = native_dict_path(data_path);

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&dict_path)
        .await
        .map_err(VortexRdfError::from)?;

    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let dict_root = vortex_array::stream::ArrayStreamExt::read_all(stream)
        .await
        .map_err(VortexRdfError::from)?;

    Dict::from_vortex_array(&dict_root)
}

enum NativePatternFilter {
    /// No bound RDF terms, so scan all rows.
    All,

    /// At least one bound RDF term was not present in the dictionary.
    /// Therefore the result is definitely empty.
    Empty,

    /// A concrete Vortex filter expression over top-level s/p/o/g columns.
    Expr(Expression),
}

fn build_native_pattern_filter<Dict>(
    dictionary: &Dict,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativePatternFilter>
where
    Dict: RdfDictionary,
{
    let build_native_pattern_start = Instant::now();
    let mut filters: Vec<Expression> = Vec::new();

    if let Some(subject) = subject {
        let term = subject.to_string();

        let Some(id) = dictionary.get_id(&term) else {
            log::debug!(
                "[cottas_native::filter] subject term absent from dictionary: {}",
                term
            );
            return Ok(NativePatternFilter::Empty);
        };

        log::debug!(
            "[cottas_native::filter] subject term {} resolved to id {}",
            term,
            id
        );

        filters.push(eq(col("s"), lit(id)));
    }

    if let Some(predicate) = predicate {
        let term = predicate.to_string();

        let Some(id) = dictionary.get_id(&term) else {
            log::debug!(
                "[cottas_native::filter] predicate term absent from dictionary: {}",
                term
            );
            return Ok(NativePatternFilter::Empty);
        };
        log::debug!(
            "[cottas_native::filter] predicate term {} resolved to id {}",
            term,
            id
        );
        filters.push(eq(col("p"), lit(id)));
    }

    if let Some(object) = object {
        let term = object.to_string();
        let Some(id) = dictionary.get_id(&term) else {
            log::debug!(
                "[cottas_native::filter] object term absent from dictionary: {}",
                term
            );
            return Ok(NativePatternFilter::Empty);
        };
        log::debug!(
            "[cottas_native::filter] object term {} resolved to id {}",
            term,
            id
        );

        filters.push(eq(col("o"), lit(id)));
    }

    if let Some(graph) = graph {
        let term = graph.to_string();

        let Some(id) = dictionary.get_id(&term) else {
            log::debug!(
                "[cottas_native::filter] graph term absent from dictionary: {}",
                term
            );
            return Ok(NativePatternFilter::Empty);
        };
        log::debug!(
            "[cottas_native::filter] graph term {} resolved to id {}",
            term,
            id
        );

        filters.push(eq(col("g"), lit(id)));
    }

    let Some(first) = filters.into_iter().reduce(and) else {
        return Ok(NativePatternFilter::All);
    };
        log::debug!(
        "[cottas_native::build_native_pattern_filter] Built filters in {:?}",
        build_native_pattern_start.elapsed()
    );

    Ok(NativePatternFilter::Expr(first))
}

async fn write_quads_array_as_rdf<Dict, W>(
    dictionary: &Dict,
    quads: ArrayRef,
    writer: W,
    format: RdfFormat,
) -> Result<()>
where
    Dict: RdfDictionary,
    W: Write,
{
    let write_start = Instant::now();
    let mut quads_stream = <FlatLayout as RdfQuadLayout<Dict>>::quads(dictionary, &quads)?;

    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;

        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    log::debug!(
        "[cottas_native::write_quads_array_as_rdf] Write completed in {:?}",
        write_start.elapsed()
    );

    Ok(())
}

async fn write_empty_rdf<W>(writer: W, format: RdfFormat) -> Result<()>
where
    W: Write,
{
    let rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    Ok(())
}

pub async fn match_cottas_native_file<W>(
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
    let dictionary = load_cottas_native_simple_dictionary_view(input_path).await?;

    let filter = build_native_pattern_filter(&dictionary, subject, predicate, object, graph)?;

    if matches!(filter, NativePatternFilter::Empty) {
        log::debug!(
            "[cottas_native::match] at least one bound term is absent from dictionary; returning empty result"
        );

        return write_empty_rdf(writer, format).await;
    }

    let open_start = Instant::now();

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::match] opened native COTTAS file in {:?}",
        open_start.elapsed()
    );

    if let NativePatternFilter::Expr(expr) = &filter {
        match file.can_prune(expr) {
            Ok(can_prune) => {
                log::debug!(
                    "[cottas_native::match] file.can_prune(filter) = {}",
                    can_prune
                );
            }
            Err(e) => {
                log::debug!(
                    "[cottas_native::match] file.can_prune(filter) failed: {}",
                    e
                );
            }
        }
    }
    match file.splits() {
        Ok(splits) => {
            log::debug!(
                "[cottas_native::match] native file has {} scan splits: {:?}",
                splits.len(),
                splits
            );
        }
        Err(e) => {
            log::debug!(
                "[cottas_native::match] failed to inspect native file splits: {}",
                e
            );
        }
    }

    let scan_start = Instant::now();

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativePatternFilter::All => scan,
        NativePatternFilter::Empty => unreachable!("handled above"),
        NativePatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::match] scan builder setup took {:?}",
        scan_start.elapsed()
    );

    let read_start: Instant = Instant::now();

    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::match] filtered scan materialized {} rows in {:?}",
        matched_quads.len(),
        read_start.elapsed()
    );

    write_quads_array_as_rdf(&dictionary, matched_quads, writer, format).await
}

pub async fn load_cottas_native_simple_dictionary_view(
    data_path: &Path,
) -> Result<SimpleDictionaryView> {
    let read_dict_start: Instant = Instant::now();

    let dict_path = native_dict_path(data_path);

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&dict_path)
        .await
        .map_err(VortexRdfError::from)?;

    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let dict_root = stream.read_all().await.map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native::load_cottas_native_simple_dictionary_view] loaded dictionary root array with {} rows in {:?}",
        dict_root.len(),
        read_dict_start.elapsed()
    );

    SimpleDictionaryView::from_dictionary_sidecar_root(&dict_root)
}

async fn collect_globally_sorted_row_groups<S>(
    mut quad_stream: S,
    ordering: TripleOrdering,
    row_group_size: usize,
) -> Result<Vec<Vec<NativeTriple>>>
where
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let row_group_size = row_group_size.max(1);

    let mut triples = Vec::new();

    while let Some(item) = quad_stream.next().await {
        let quad = item?;
        triples.push(quad_to_native_triple(&quad));
    }

    triples.sort_by(|a, b| a.cmp_by_order(b, ordering));

    let groups = triples
        .chunks(row_group_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    Ok(groups)
}
