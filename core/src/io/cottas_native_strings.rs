use crate::error::{Result, VortexRdfError};
use crate::io::utils::CottasVortexCompressionProfile;
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

use serde::{Deserialize, Serialize};
use std::fs;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeStringRowGroupStats {
    row_group_idx: usize,
    rows: usize,

    s_min: String,
    s_max: String,

    p_min: String,
    p_max: String,

    o_min: String,
    o_max: String,

    g_min: String,
    g_max: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeStringFileStats {
    ordering: String,
    row_group_size: usize,
    row_groups: Vec<NativeStringRowGroupStats>,
}

#[derive(Debug, Clone)]
struct NativeStringPatternProbe {
    s: Option<String>,
    p: Option<String>,
    o: Option<String>,
    g: Option<String>,
}

fn make_pattern_probe(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> NativeStringPatternProbe {
    NativeStringPatternProbe {
        s: subject.map(|x| x.to_string()),
        p: predicate.map(|x| x.to_string()),
        o: object.map(|x| x.to_string()),
        g: graph.map(|x| x.to_string()),
    }
}

fn compute_row_group_stats(
    row_group_idx: usize,
    group: &[NativeStringQuad],
) -> Option<NativeStringRowGroupStats> {
    if group.is_empty() {
        return None;
    }

    let mut s_min = group[0].s.clone();
    let mut s_max = group[0].s.clone();

    let mut p_min = group[0].p.clone();
    let mut p_max = group[0].p.clone();

    let mut o_min = group[0].o.clone();
    let mut o_max = group[0].o.clone();

    let mut g_min = group[0].g.clone();
    let mut g_max = group[0].g.clone();

    for q in &group[1..] {
        if q.s < s_min {
            s_min = q.s.clone();
        }
        if q.s > s_max {
            s_max = q.s.clone();
        }

        if q.p < p_min {
            p_min = q.p.clone();
        }
        if q.p > p_max {
            p_max = q.p.clone();
        }

        if q.o < o_min {
            o_min = q.o.clone();
        }
        if q.o > o_max {
            o_max = q.o.clone();
        }

        if q.g < g_min {
            g_min = q.g.clone();
        }
        if q.g > g_max {
            g_max = q.g.clone();
        }
    }

    Some(NativeStringRowGroupStats {
        row_group_idx,
        rows: group.len(),
        s_min,
        s_max,
        p_min,
        p_max,
        o_min,
        o_max,
        g_min,
        g_max,
    })
}

fn stats_sidecar_path(output_path: &Path) -> std::path::PathBuf {
    let file_name = output_path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("data.vortex");

    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{file_name}.rgstats.json"))
}

async fn write_row_group_stats_sidecar(
    output_path: &Path,
    ordering: TripleOrdering,
    row_group_size: usize,
    groups: &[Vec<NativeStringQuad>],
) -> Result<()> {
    let row_groups: Vec<NativeStringRowGroupStats> = groups
        .iter()
        .enumerate()
        .filter_map(|(idx, g)| compute_row_group_stats(idx, g))
        .collect();

    let stats = NativeStringFileStats {
        ordering: format!("{ordering:?}"),
        row_group_size,
        row_groups,
    };

    let serialized = serde_json::to_vec_pretty(&stats)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    tokio::fs::write(stats_sidecar_path(output_path), serialized)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    Ok(())
}

fn row_group_may_match(pattern: &NativeStringPatternProbe, rg: &NativeStringRowGroupStats) -> bool {
    if let Some(s) = &pattern.s {
        if s < &rg.s_min || s > &rg.s_max {
            return false;
        }
    }

    if let Some(p) = &pattern.p {
        if p < &rg.p_min || p > &rg.p_max {
            return false;
        }
    }

    if let Some(o) = &pattern.o {
        if o < &rg.o_min || o > &rg.o_max {
            return false;
        }
    }

    if let Some(g) = &pattern.g {
        if g < &rg.g_min || g > &rg.g_max {
            return false;
        }
    }

    true
}

fn load_row_group_stats_sidecar(input_path: &Path) -> Result<Option<NativeStringFileStats>> {
    let sidecar = stats_sidecar_path(input_path);

    if !sidecar.exists() {
        return Ok(None);
    }

    let raw = fs::read(sidecar).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let parsed: NativeStringFileStats =
        serde_json::from_slice(&raw).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    Ok(Some(parsed))
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeStringPruningReport {
    pub filter_is_all: bool,
    pub vortex_can_prune: Option<bool>,

    pub total_row_groups: Option<usize>,
    pub candidate_row_groups: Option<usize>,
    pub candidate_rows_upper_bound: Option<usize>,

    pub ordering: Option<String>,
    pub row_group_size: Option<usize>,
}

pub async fn inspect_cottas_native_string_pruning(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeStringPruningReport> {
    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);
    let pattern = make_pattern_probe(subject, predicate, object, graph);

    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    let vortex_can_prune = match &filter {
        NativeStringPatternFilter::All => None,
        NativeStringPatternFilter::Expr(expr) => file.can_prune(expr).ok(),
    };

    let sidecar = load_row_group_stats_sidecar(input_path)?;

    let (
        total_row_groups,
        candidate_row_groups,
        candidate_rows_upper_bound,
        ordering,
        row_group_size,
    ) = if let Some(stats) = sidecar {
        let candidates: Vec<&NativeStringRowGroupStats> = stats
            .row_groups
            .iter()
            .filter(|rg| row_group_may_match(&pattern, rg))
            .collect();

        (
            Some(stats.row_groups.len()),
            Some(candidates.len()),
            Some(candidates.iter().map(|rg| rg.rows).sum()),
            Some(stats.ordering),
            Some(stats.row_group_size),
        )
    } else {
        (None, None, None, None, None)
    };

    Ok(NativeStringPruningReport {
        filter_is_all: matches!(filter, NativeStringPatternFilter::All),
        vortex_can_prune,
        total_row_groups,
        candidate_row_groups,
        candidate_rows_upper_bound,
        ordering,
        row_group_size,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeStringMatchTimings {
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub stream_init_ms: f64,
    pub read_all_ms: f64,
    pub serialize_ms: f64,
    pub total_ms: f64,

    pub rows_out: usize,

    pub vortex_can_prune: Option<bool>,
    pub total_row_groups: Option<usize>,
    pub candidate_row_groups: Option<usize>,
    pub candidate_rows_upper_bound: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ProcIoSnapshot {
    pub rchar: u64,
    pub wchar: u64,
    pub syscr: u64,
    pub syscw: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

fn read_proc_io_snapshot() -> Option<ProcIoSnapshot> {
    let raw = std::fs::read_to_string("/proc/self/io").ok()?;

    let mut rchar = None;
    let mut wchar = None;
    let mut syscr = None;
    let mut syscw = None;
    let mut read_bytes = None;
    let mut write_bytes = None;

    for line in raw.lines() {
        let mut parts = line.splitn(2, ':');
        let key = match parts.next() {
            Some(k) => k.trim(),
            None => continue,
        };
        let val_str = match parts.next() {
            Some(v) => v.trim(),
            None => continue,
        };
        let val = match val_str.parse::<u64>() {
            Ok(v) => v,
            Err(_) => continue,
        };

        match key {
            "rchar" => rchar = Some(val),
            "wchar" => wchar = Some(val),
            "syscr" => syscr = Some(val),
            "syscw" => syscw = Some(val),
            "read_bytes" => read_bytes = Some(val),
            "write_bytes" => write_bytes = Some(val),
            _ => {}
        }
    }

    Some(ProcIoSnapshot {
        rchar: rchar?,
        wchar: wchar?,
        syscr: syscr?,
        syscw: syscw?,
        read_bytes: read_bytes?,
        write_bytes: write_bytes?,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcIoDelta {
    pub rchar_delta: u64,
    pub wchar_delta: u64,
    pub syscr_delta: u64,
    pub syscw_delta: u64,
    pub read_bytes_delta: u64,
    pub write_bytes_delta: u64,
}

fn diff_proc_io(before: ProcIoSnapshot, after: ProcIoSnapshot) -> ProcIoDelta {
    ProcIoDelta {
        rchar_delta: after.rchar.saturating_sub(before.rchar),
        wchar_delta: after.wchar.saturating_sub(before.wchar),
        syscr_delta: after.syscr.saturating_sub(before.syscr),
        syscw_delta: after.syscw.saturating_sub(before.syscw),
        read_bytes_delta: after.read_bytes.saturating_sub(before.read_bytes),
        write_bytes_delta: after.write_bytes.saturating_sub(before.write_bytes),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeStringScanDiagnostics {
    pub timings: NativeStringMatchTimings,
    pub proc_io: Option<ProcIoDelta>,
}

pub async fn scan_cottas_native_string_file_with_diagnostics(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(ArrayRef, NativeStringScanDiagnostics)> {
    let total_start = Instant::now();

    let pruning_report =
        inspect_cottas_native_string_pruning(input_path, subject, predicate, object, graph).await?;

    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);

    let open_start = Instant::now();
    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = open_start.elapsed().as_secs_f64() * 1000.0;

    let scan_build_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let scan = match filter {
        NativeStringPatternFilter::All => scan,
        NativeStringPatternFilter::Expr(expr) => scan.with_filter(expr),
    };
    let scan_build_ms = scan_build_start.elapsed().as_secs_f64() * 1000.0;

    let stream_init_start = Instant::now();
    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    let stream_init_ms = stream_init_start.elapsed().as_secs_f64() * 1000.0;

    let proc_before = read_proc_io_snapshot();

    let read_start = Instant::now();
    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;
    let read_all_ms = read_start.elapsed().as_secs_f64() * 1000.0;

    let proc_after = read_proc_io_snapshot();
    let proc_io = match (proc_before, proc_after) {
        (Some(before), Some(after)) => Some(diff_proc_io(before, after)),
        _ => None,
    };

    let rows_out = matched_quads.len();

    let timings = NativeStringMatchTimings {
        open_ms,
        scan_build_ms,
        stream_init_ms,
        read_all_ms,
        serialize_ms: 0.0,
        total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
        rows_out,
        vortex_can_prune: pruning_report.vortex_can_prune,
        total_row_groups: pruning_report.total_row_groups,
        candidate_row_groups: pruning_report.candidate_row_groups,
        candidate_rows_upper_bound: pruning_report.candidate_rows_upper_bound,
    };

    Ok((
        matched_quads,
        NativeStringScanDiagnostics { timings, proc_io },
    ))
}

pub async fn match_cottas_native_string_file_with_diagnostics<W>(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    writer: W,
    format: RdfFormat,
) -> Result<NativeStringScanDiagnostics>
where
    W: Write,
{
    let (matched_quads, mut diag) = scan_cottas_native_string_file_with_diagnostics(
        input_path, subject, predicate, object, graph,
    )
    .await?;

    let serialize_start = Instant::now();
    write_string_quads_array_as_rdf(matched_quads, writer, format).await?;
    diag.timings.serialize_ms = serialize_start.elapsed().as_secs_f64() * 1000.0;

    diag.timings.total_ms += diag.timings.serialize_ms;

    Ok(diag)
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
            TripleOrdering::None => {
                unreachable!("cmp_by_order should not be called when ordering is None")
            }
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

    if ordering != TripleOrdering::None {
        quads.sort_by(|a, b| a.cmp_by_order(b, ordering));
    }
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

    write_row_group_stats_sidecar(output_path, config.ordering, config.row_group_size, &groups)
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

pub async fn match_cottas_native_string_file_as_triples(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<Vec<(String, String, String)>> {
    use vortex::VortexSessionDefault;
    use vortex_array::VortexSessionExecute;

    let (matched_quads, _diag) = scan_cottas_native_string_file_with_diagnostics(
        input_path, subject, predicate, object, graph,
    )
    .await?;

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let struct_array = matched_quads
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

    let mut out = Vec::with_capacity(struct_array.len());

    for i in 0..struct_array.len() {
        let s = String::from_utf8_lossy(&s_arr.bytes_at(i)).into_owned();
        let p = String::from_utf8_lossy(&p_arr.bytes_at(i)).into_owned();
        let o = String::from_utf8_lossy(&o_arr.bytes_at(i)).into_owned();

        out.push((s, p, o));
    }

    Ok(out)
}
//pub async fn match_cottas_native_string_file<W>(
//    input_path: &Path,
//    subject: Option<&NamedOrBlankNode>,
//    predicate: Option<&NamedNode>,
//    object: Option<&Term>,
//    graph: Option<&GraphName>,
//    writer: W,
//    format: RdfFormat,
//) -> Result<()>
//where
//    W: Write,
//{
//    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);
//
//    let open_start = Instant::now();
//
//    let file = NATIVE_STRING_FILE_SESSION
//        .open_options()
//        .open_path(input_path)
//        .await
//        .map_err(VortexRdfError::from)?;
//
//    log::debug!(
//        "[cottas_native_strings::match] opened native string COTTAS file in {:?}",
//        open_start.elapsed()
//    );
//
//    //if let NativeStringPatternFilter::Expr(expr) = &filter {
//    //    match file.can_prune(expr) {
//    //        Ok(can_prune) => {
//    //            log::debug!(
//    //                "[cottas_native_strings::match] file.can_prune(filter) = {}",
//    //                can_prune
//    //            );
//    //        }
//    //        Err(e) => {
//    //            log::debug!(
//    //                "[cottas_native_strings::match] file.can_prune(filter) failed: {}",
//    //                e
//    //            );
//    //        }
//    //    }
//    //}
//
//    //match file.splits() {
//    //    Ok(splits) => {
//    //        log::debug!(
//    //            "[cottas_native_strings::match] native string file has {} scan splits: {:?}",
//    //            splits.len(),
//    //            splits
//    //        );
//    //    }
//    //    Err(e) => {
//    //        log::debug!(
//    //            "[cottas_native_strings::match] failed to inspect native string file splits: {}",
//    //            e
//    //        );
//    //    }
//    //}
//
//    let scan_start = Instant::now();
//
//    let scan = file.scan().map_err(VortexRdfError::from)?;
//
//    let scan = match filter {
//        NativeStringPatternFilter::All => scan,
//        NativeStringPatternFilter::Expr(expr) => scan.with_filter(expr),
//    };
//
//    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
//
//    log::debug!(
//        "[cottas_native_strings::match] scan builder setup took {:?}",
//        scan_start.elapsed()
//    );
//
//    let read_start = Instant::now();
//
//    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;
//
//    log::debug!(
//        "[cottas_native_strings::match] filtered scan materialized {} rows in {:?}",
//        matched_quads.len(),
//        read_start.elapsed()
//    );
//
//    write_string_quads_array_as_rdf(matched_quads, writer, format).await
//}
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
    let _diag = match_cottas_native_string_file_with_diagnostics(
        input_path, subject, predicate, object, graph, writer, format,
    )
    .await?;

    Ok(())
}

pub async fn count_cottas_native_string_file(input_path: &Path) -> Result<usize> {
    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    Ok(file.row_count() as usize)
}
