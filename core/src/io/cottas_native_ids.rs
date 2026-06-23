use crate::common::indexes::wrap_array_in_list;
use crate::error::{Result, VortexRdfError};
use crate::index::{RdfDictionary, SimpleDictionaryView};
use crate::io::utils::CottasVortexCompressionProfile;
use crate::store::layout::cottas::TripleOrdering;

use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use vortex::VortexSessionDefault;

use std::collections::{HashMap, HashSet};
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{ConstantArray, PrimitiveArray, StructArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;

use vortex::expr::{Expression, and, col, eq, lit, or};
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
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct CottasNativeConfig {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
    pub dict_row_group_size: usize,
    pub compression_profile: CottasVortexCompressionProfile,
}

impl Default for CottasNativeConfig {
    fn default() -> Self {
        Self {
            ordering: TripleOrdering::SPO,
            row_group_size: 122_880,
            dict_row_group_size: 16_384,
            compression_profile: CottasVortexCompressionProfile::Balanced,
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
            TripleOrdering::None => {
                unreachable!("cmp_by_order should not be called when ordering is None")
            }
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

async fn write_lookup_array_to_vortex_file<W>(
    writer: &mut W,
    array: ArrayRef,
    row_group_size: usize,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = array.dtype().clone();

    let stream = ArrayStreamAdapter::new(dtype, Box::pin(stream::iter(std::iter::once(Ok(array)))));

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
        "[cottas_native_ids::write_lookup_array_to_vortex_file] wrote lookup sidecar in {:?}",
        start.elapsed()
    );

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
    write_dictionary_lookup_sidecars(&dictionary, output_path, config.dict_row_group_size).await?;

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

#[derive(Clone, Debug, Default, Serialize)]
pub struct CottasNativeIdsDiagnostics {
    pub term_lookup_ms: f64,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub id_extract_ms: f64,
    pub id_to_term_lookup_ms: f64,
    pub serialize_ms: f64,
    pub total_ms: f64,
    pub rows_out: usize,
    pub unique_ids_requested: usize,
    pub unique_ids_loaded: usize,
    pub vortex_can_prune: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct LazyRdfWriteStats {
    id_extract_ms: f64,
    id_to_term_lookup_ms: f64,
    serialize_ms: f64,
    rows_out: usize,
    unique_ids_requested: usize,
    unique_ids_loaded: usize,
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

async fn write_quads_array_as_rdf_lazy<W>(
    data_path: &Path,
    quads: ArrayRef,
    writer: W,
    format: RdfFormat,
) -> Result<LazyRdfWriteStats>
where
    W: Write,
{
    let write_start = Instant::now();

    let id_extract_start = Instant::now();
    let (s_ids, p_ids, o_ids, g_ids) = extract_spog_id_columns(&quads)?;
    let id_extract_ms = elapsed_ms(id_extract_start);

    let unique_ids = collect_unique_ids(&s_ids, &p_ids, &o_ids, &g_ids);

    let id_lookup_start = Instant::now();
    let id_to_term = lookup_terms_by_ids_from_sidecar(data_path, &unique_ids).await?;
    let id_to_term_lookup_ms = elapsed_ms(id_lookup_start);

    let serialize_start = Instant::now();
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    for i in 0..s_ids.len() {
        let s_raw = id_to_term.get(&s_ids[i]).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "S ID {} missing from id_to_term sidecar",
                s_ids[i]
            ))
        })?;
        let p_raw = id_to_term.get(&p_ids[i]).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "P ID {} missing from id_to_term sidecar",
                p_ids[i]
            ))
        })?;
        let o_raw = id_to_term.get(&o_ids[i]).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "O ID {} missing from id_to_term sidecar",
                o_ids[i]
            ))
        })?;
        let g_raw = id_to_term.get(&g_ids[i]).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "G ID {} missing from id_to_term sidecar",
                g_ids[i]
            ))
        })?;

        let subject = crate::common::utils::parse_subject(s_raw)?;
        let predicate = crate::common::utils::parse_named_node(p_raw)?;
        let object = crate::common::utils::parse_term(o_raw)?;
        let graph_name = crate::common::utils::parse_graph_name(g_raw)?;

        let quad = Quad::new(subject, predicate, object, graph_name);
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    let serialize_ms = elapsed_ms(serialize_start);

    log::debug!(
        "[cottas_native_ids::write_quads_array_as_rdf_lazy] wrote {} rows using {} unique dictionary ids in {:?}",
        s_ids.len(),
        unique_ids.len(),
        write_start.elapsed()
    );

    Ok(LazyRdfWriteStats {
        id_extract_ms,
        id_to_term_lookup_ms,
        serialize_ms,
        rows_out: s_ids.len(),
        unique_ids_requested: unique_ids.len(),
        unique_ids_loaded: id_to_term.len(),
    })
}

fn extract_spog_id_columns(quads: &ArrayRef) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>)> {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let quads_struct = quads
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let fields = quads_struct.unmasked_fields();

    let s_ids = fields
        .get(0)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let p_ids = fields
        .get(1)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let o_ids = fields
        .get(2)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    let g_ids = fields
        .get(3)
        .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .as_slice::<u32>()
        .to_vec();

    Ok((s_ids, p_ids, o_ids, g_ids))
}

fn collect_unique_ids(s_ids: &[u32], p_ids: &[u32], o_ids: &[u32], g_ids: &[u32]) -> Vec<u32> {
    let mut set = HashSet::new();

    for id in s_ids {
        set.insert(*id);
    }
    for id in p_ids {
        set.insert(*id);
    }
    for id in o_ids {
        set.insert(*id);
    }
    for id in g_ids {
        set.insert(*id);
    }

    let mut ids: Vec<u32> = set.into_iter().collect();
    ids.sort_unstable();
    ids
}

fn build_id_lookup_filter(ids: &[u32]) -> Option<Expression> {
    ids.iter()
        .copied()
        .map(|id| eq(col("id"), lit(id)))
        .reduce(or)
}

async fn lookup_terms_by_ids_from_sidecar(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    const MAX_OR_FILTER_IDS: usize = 1024;

    let lookup_start = Instant::now();
    let path = native_dict_id_to_term_path(data_path);

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;

    let requested: HashSet<u32> = ids.iter().copied().collect();

    let scan =
        file.scan()
            .map_err(VortexRdfError::from)?
            .with_projection(vortex_array::expr::select(
                ["id", "term"],
                vortex_array::expr::root(),
            ));

    let scan = if ids.len() <= MAX_OR_FILTER_IDS {
        let Some(expr) = build_id_lookup_filter(ids) else {
            return Ok(HashMap::new());
        };

        if let Ok(can_prune) = file.can_prune(&expr) {
            log::debug!(
                "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] can_prune(ids={}) = {}",
                ids.len(),
                can_prune
            );
        }

        scan.with_filter(expr)
    } else {
        log::debug!(
            "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] {} ids requested; scanning id_to_term sidecar without OR filter",
            ids.len()
        );

        scan
    };

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    let rows = stream.read_all().await.map_err(VortexRdfError::from)?;
    let mut map = extract_id_term_map(&rows)?;

    if ids.len() > MAX_OR_FILTER_IDS {
        map.retain(|id, _| requested.contains(id));
    }

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] resolved {} / {} ids in {:?}",
        map.len(),
        ids.len(),
        lookup_start.elapsed()
    );

    return Ok(map);
}

fn scalar_string_clean(value: String) -> String {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(&value)
        .to_string()
}

fn extract_id_term_map(array: &ArrayRef) -> Result<HashMap<u32, String>> {
    let mut out = HashMap::new();

    if array.len() == 0 {
        return Ok(out);
    }

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let id_col = struct_array
        .unmasked_field_by_name("id")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let term_col = struct_array
        .unmasked_field_by_name("term")
        .map_err(VortexRdfError::Vortex)?;

    let ids = id_col.as_slice::<u32>();

    for row in 0..array.len() {
        let scalar = term_col
            .execute_scalar(row, &mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let term = scalar_string_clean(format!("{}", scalar));
        out.insert(ids[row], term);
    }

    Ok(out)
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
    let _diagnostics = match_cottas_native_file_with_diagnostics(
        input_path, subject, predicate, object, graph, writer, format,
    )
    .await?;

    Ok(())
}

pub async fn match_cottas_native_file_with_diagnostics<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    writer: W,
    format: RdfFormat,
) -> Result<CottasNativeIdsDiagnostics>
where
    W: Write,
{
    let total_start = Instant::now();
    let mut diagnostics = CottasNativeIdsDiagnostics::default();

    let (filter, term_lookup_ms) =
        build_native_pattern_filter_lazy_with_stats(input_path, subject, predicate, object, graph)
            .await?;
    diagnostics.term_lookup_ms = term_lookup_ms;

    if matches!(filter, NativePatternFilter::Empty) {
        log::debug!(
            "[cottas_native::match] at least one bound term is absent from dictionary; returning empty result"
        );

        let serialize_start = Instant::now();
        write_empty_rdf(writer, format).await?;
        diagnostics.serialize_ms = elapsed_ms(serialize_start);
        diagnostics.total_ms = elapsed_ms(total_start);

        return Ok(diagnostics);
    }

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    diagnostics.open_ms = elapsed_ms(open_start);

    log::debug!(
        "[cottas_native::match] opened native COTTAS file in {:.3}ms",
        diagnostics.open_ms
    );

    if let NativePatternFilter::Expr(expr) = &filter {
        match file.can_prune(expr) {
            Ok(can_prune) => {
                diagnostics.vortex_can_prune = Some(can_prune);
                log::debug!(
                    "[cottas_native::match] file.can_prune(filter) = {}",
                    can_prune
                );
            }
            Err(e) => {
                diagnostics.vortex_can_prune = None;
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
    diagnostics.scan_build_ms = elapsed_ms(scan_start);

    log::debug!(
        "[cottas_native::match] scan builder setup took {:.3}ms",
        diagnostics.scan_build_ms
    );

    let read_start = Instant::now();
    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;
    diagnostics.read_all_ms = elapsed_ms(read_start);

    log::debug!(
        "[cottas_native::match] filtered scan materialized {} rows in {:.3}ms",
        matched_quads.len(),
        diagnostics.read_all_ms
    );

    if matched_quads.len() == 0 {
        log::debug!(
            "[cottas_native_ids::match] scan produced 0 rows; skipping dictionary decoding"
        );

        let serialize_start = Instant::now();
        write_empty_rdf(writer, format).await?;
        diagnostics.serialize_ms = elapsed_ms(serialize_start);
        diagnostics.total_ms = elapsed_ms(total_start);

        return Ok(diagnostics);
    }

    let write_stats =
        write_quads_array_as_rdf_lazy(input_path, matched_quads, writer, format).await?;

    diagnostics.id_extract_ms = write_stats.id_extract_ms;
    diagnostics.id_to_term_lookup_ms = write_stats.id_to_term_lookup_ms;
    diagnostics.serialize_ms = write_stats.serialize_ms;
    diagnostics.rows_out = write_stats.rows_out;
    diagnostics.unique_ids_requested = write_stats.unique_ids_requested;
    diagnostics.unique_ids_loaded = write_stats.unique_ids_loaded;
    diagnostics.total_ms = elapsed_ms(total_start);

    log::debug!("[cottas_native_ids::diagnostics] {:?}", diagnostics);

    Ok(diagnostics)
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

    if ordering != TripleOrdering::None {
        triples.sort_by(|a, b| a.cmp_by_order(b, ordering));
    }

    let groups = triples
        .chunks(row_group_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    Ok(groups)
}
fn native_dict_term_to_id_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");

    data_path.with_file_name(format!("{file_name}.dict.term_to_id.vortex"))
}

fn native_dict_id_to_term_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");

    data_path.with_file_name(format!("{file_name}.dict.id_to_term.vortex"))
}
fn build_term_to_id_lookup_array(pairs: &[(u32, String)]) -> Result<ArrayRef> {
    let term_array = vortex_array::arrays::VarBinViewArray::from_iter(
        pairs.iter().map(|(_, term)| Some(term.as_str())),
        vortex_array::dtype::DType::Utf8(vortex_array::dtype::Nullability::NonNullable),
    )
    .into_array();

    let id_array = PrimitiveArray::from_iter(pairs.iter().map(|(id, _)| *id)).into_array();

    StructArray::from_fields(&[("term", term_array), ("id", id_array)])
        .map_err(VortexRdfError::Vortex)
        .map(|arr| arr.into_array())
}

fn extract_first_u32_from_single_column_array(
    array: &ArrayRef,
    column_name: &str,
) -> Result<Option<u32>> {
    if array.len() == 0 {
        return Ok(None);
    }

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::from)?;

    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::from)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::from)?;

    let values = column.as_slice::<u32>();

    values.first().copied().map(Some).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "projected column {:?} has no u32 values despite parent array len {}",
            column_name,
            array.len()
        ))
    })
}

fn build_id_to_term_lookup_array(pairs: &[(u32, String)]) -> Result<ArrayRef> {
    let id_array = PrimitiveArray::from_iter(pairs.iter().map(|(id, _)| *id)).into_array();

    let term_array = vortex_array::arrays::VarBinViewArray::from_iter(
        pairs.iter().map(|(_, term)| Some(term.as_str())),
        vortex_array::dtype::DType::Utf8(vortex_array::dtype::Nullability::NonNullable),
    )
    .into_array();

    StructArray::from_fields(&[("id", id_array), ("term", term_array)])
        .map_err(VortexRdfError::Vortex)
        .map(|arr| arr.into_array())
}
async fn write_dictionary_lookup_sidecars<Dict>(
    dictionary: &Dict,
    data_path: &Path,
    row_group_size: usize,
) -> Result<()>
where
    Dict: RdfDictionary,
{
    let term_id_pairs = dictionary.term_id_pairs();

    let mut term_to_id_rows = term_id_pairs.clone();
    term_to_id_rows.sort_by(|(_, a), (_, b)| a.cmp(b));

    let mut id_to_term_rows = term_id_pairs;
    id_to_term_rows.sort_by_key(|(id, _)| *id);

    let term_to_id_array = build_term_to_id_lookup_array(&term_to_id_rows)?;
    let id_to_term_array = build_id_to_term_lookup_array(&id_to_term_rows)?;

    let term_to_id_path = native_dict_term_to_id_path(data_path);
    let id_to_term_path = native_dict_id_to_term_path(data_path);

    let mut term_to_id_file = tokio::fs::File::create(&term_to_id_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_lookup_array_to_vortex_file(&mut term_to_id_file, term_to_id_array, row_group_size)
        .await?;

    let mut id_to_term_file = tokio::fs::File::create(&id_to_term_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_lookup_array_to_vortex_file(&mut id_to_term_file, id_to_term_array, row_group_size)
        .await?;

    log::info!(
        "[cottas_native_ids] wrote dictionary lookup sidecars {:?} and {:?}",
        term_to_id_path,
        id_to_term_path
    );

    Ok(())
}

async fn build_native_pattern_filter_lazy_with_stats(
    data_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(NativePatternFilter, f64)> {
    let start = Instant::now();
    let mut term_lookup_ms = 0.0;
    let mut filters: Vec<Expression> = Vec::new();

    if let Some(subject) = subject {
        let term = subject.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("s"), lit(id)));
    }

    if let Some(predicate) = predicate {
        let term = predicate.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("p"), lit(id)));
    }

    if let Some(object) = object {
        let term = object.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("o"), lit(id)));
    }

    if let Some(graph) = graph {
        let term = graph.to_string();

        let lookup_start = Instant::now();
        let id = lookup_term_id_from_sidecar(data_path, &term).await?;
        term_lookup_ms += elapsed_ms(lookup_start);

        let Some(id) = id else {
            return Ok((NativePatternFilter::Empty, term_lookup_ms));
        };

        filters.push(eq(col("g"), lit(id)));
    }

    let Some(expr) = filters.into_iter().reduce(and) else {
        return Ok((NativePatternFilter::All, term_lookup_ms));
    };

    log::debug!(
        "[cottas_native_ids::build_native_pattern_filter_lazy] built filter in {:?}",
        start.elapsed()
    );

    Ok((NativePatternFilter::Expr(expr), term_lookup_ms))
}

async fn lookup_term_id_from_sidecar(data_path: &Path, term: &str) -> Result<Option<u32>> {
    let lookup_start = Instant::now();
    let path = native_dict_term_to_id_path(data_path);

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;

    let expr = eq(col("term"), lit(term));

    if let Ok(can_prune) = file.can_prune(&expr) {
        log::debug!(
            "[cottas_native_ids::lookup_term_id_from_sidecar] can_prune(term={}) = {}",
            term,
            can_prune
        );
    }

    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(expr)
        .with_projection(vortex_array::expr::select(
            ["id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let ids = stream.read_all().await.map_err(VortexRdfError::from)?;

    let id = extract_first_u32_from_single_column_array(&ids, "id")?;

    log::debug!(
        "[cottas_native_ids::lookup_term_id_from_sidecar] resolved term {:?} to {:?} in {:?}",
        term,
        id,
        lookup_start.elapsed()
    );

    Ok(id)
}
