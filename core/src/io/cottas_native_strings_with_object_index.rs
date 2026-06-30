use crate::error::{Result, VortexRdfError};
use crate::io::utils::CottasVortexCompressionProfile;
use crate::store::layout::cottas::TripleOrdering;
use futures::{stream, Stream, StreamExt};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfSerializer};

use std::collections::{BinaryHeap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Instant;

use vortex::expr::{Expression, and, col, eq, lit};
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt};
use vortex_array::{ArrayRef, IntoArray};
use vortex_error::{VortexError, VortexResult};
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use vortex::dtype::{DType, Nullability};

use vortex_btrblocks::BtrBlocksCompressorBuilder;

use serde::{Deserialize, Serialize};
use std::fs;

use std::io::{BufRead, BufReader, BufWriter};
use std::pin::Pin;

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

fn object_term_to_id_sidecar_path(output_path: &Path) -> PathBuf {
    let file_name = output_path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("data.vortex");
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{file_name}.object_term_to_id.vortex"))
}

fn object_idx_o_val_rid_sidecar_path(output_path: &Path) -> PathBuf {
    let file_name = output_path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("data.vortex");
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{file_name}.object_idx_o_val_rid.vortex"))
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
    pub row_count_ms: f64,
    pub scan_build_ms: f64,
    pub stream_init_ms: f64,
    pub read_all_ms: f64,
    pub serialize_ms: f64,
    pub total_ms: f64,

    pub file_rows: Option<usize>,
    pub rows_out: usize,
    pub selectivity: Option<f64>,

    pub stream_batches: usize,
    pub empty_stream_batches: usize,
    pub max_stream_batch_rows: usize,

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

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ProcMemSnapshot {
    pub current_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcMemTimeline {
    pub before_open: Option<ProcMemSnapshot>,
    pub after_open: Option<ProcMemSnapshot>,
    pub after_scan_build: Option<ProcMemSnapshot>,
    pub after_stream_init: Option<ProcMemSnapshot>,
    pub before_read_all: Option<ProcMemSnapshot>,
    pub after_read_all: Option<ProcMemSnapshot>,
}

fn read_proc_mem_snapshot() -> Option<ProcMemSnapshot> {
    Some(ProcMemSnapshot {
        current_rss_bytes: current_rss_bytes(),
        peak_rss_bytes: peak_rss_bytes(),
    })
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/self/status").ok()?;

    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;

            return Some(kb * 1024);
        }
    }

    None
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn peak_rss_bytes() -> Option<u64> {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();

        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return None;
        }

        let raw = usage.ru_maxrss as u64;

        #[cfg(target_os = "linux")]
        {
            // Linux reports ru_maxrss in KiB.
            Some(raw * 1024)
        }

        #[cfg(target_os = "macos")]
        {
            // macOS reports ru_maxrss in bytes.
            Some(raw)
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeStringFilterDebug {
    pub bound_s: Option<String>,
    pub bound_p: Option<String>,
    pub bound_o: Option<String>,
    pub bound_g: Option<String>,

    pub combined_can_prune: Option<bool>,
    pub s_can_prune: Option<bool>,
    pub p_can_prune: Option<bool>,
    pub o_can_prune: Option<bool>,
    pub g_can_prune: Option<bool>,

    pub split_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NativeStringScanDiagnostics {
    pub timings: NativeStringMatchTimings,
    pub proc_io: Option<ProcIoDelta>,
    pub memory: Option<ProcMemTimeline>,
    pub filter_debug: Option<NativeStringFilterDebug>,
}

pub async fn scan_cottas_native_string_file_with_diagnostics(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(ArrayRef, NativeStringScanDiagnostics)> {
    let total_start = Instant::now();

    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);
    let filter_is_all = matches!(&filter, NativeStringPatternFilter::All);

    // Keep deep pruning diagnostics out of the normal hot path.
    // Enable only when explicitly requested:
    //   VORTEX_RDF_NATIVE_STRING_DEEP_DIAGNOSTICS=1 ...
    let deep_diagnostics = std::env::var_os("VORTEX_RDF_NATIVE_STRING_DEEP_DIAGNOSTICS").is_some();

    let pruning_report = if deep_diagnostics {
        inspect_cottas_native_string_pruning(input_path, subject, predicate, object, graph).await?
    } else {
        NativeStringPruningReport {
            filter_is_all,
            vortex_can_prune: None,
            total_row_groups: None,
            candidate_row_groups: None,
            candidate_rows_upper_bound: None,
            ordering: None,
            row_group_size: None,
        }
    };

    let mem_before_open = read_proc_mem_snapshot();

    let open_start = Instant::now();

    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    let bound_s = subject.map(|x| x.to_string());
    let bound_p = predicate.map(|x| x.to_string());
    let bound_o = object.map(|x| x.to_string());
    let bound_g = graph.map(|x| x.to_string());

    let combined_can_prune = match &filter {
        NativeStringPatternFilter::All => None,
        NativeStringPatternFilter::Expr(expr) => file.can_prune(expr).ok(),
    };

    let s_expr = bound_s.as_ref().map(|v| eq(col("s"), lit(v.clone())));
    let p_expr = bound_p.as_ref().map(|v| eq(col("p"), lit(v.clone())));
    let o_expr = bound_o.as_ref().map(|v| eq(col("o"), lit(v.clone())));
    let g_expr = bound_g.as_ref().map(|v| eq(col("g"), lit(v.clone())));

    let s_can_prune = s_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let p_can_prune = p_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let o_can_prune = o_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let g_can_prune = g_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());

    let split_count = file.splits().ok().map(|splits| splits.len());

    let filter_debug = Some(NativeStringFilterDebug {
        bound_s,
        bound_p,
        bound_o,
        bound_g,

        combined_can_prune,
        s_can_prune,
        p_can_prune,
        o_can_prune,
        g_can_prune,

        split_count,
    });

    let vortex_can_prune = match &filter {
        NativeStringPatternFilter::All => None,
        NativeStringPatternFilter::Expr(expr) => file.can_prune(expr).ok(),
    };

    let open_ms = open_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_open = read_proc_mem_snapshot();

    let row_count_start = Instant::now();
    let file_rows = Some(file.row_count() as usize);
    let row_count_ms = row_count_start.elapsed().as_secs_f64() * 1000.0;

    let scan_build_start = Instant::now();

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativeStringPatternFilter::All => scan,
        NativeStringPatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let scan_build_ms = scan_build_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_scan_build = read_proc_mem_snapshot();

    let stream_init_start = Instant::now();

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    let stream_init_ms = stream_init_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_stream_init = read_proc_mem_snapshot();

    let proc_before = read_proc_io_snapshot();
    let mem_before_read_all = read_proc_mem_snapshot();

    let read_start = Instant::now();

    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;

    let read_all_ms = read_start.elapsed().as_secs_f64() * 1000.0;

    let mem_after_read_all = read_proc_mem_snapshot();
    let proc_after = read_proc_io_snapshot();

    let proc_io = match (proc_before, proc_after) {
        (Some(before), Some(after)) => Some(diff_proc_io(before, after)),
        _ => None,
    };

    let rows_out = matched_quads.len();

    let selectivity = file_rows.and_then(|rows| {
        if rows == 0 {
            None
        } else {
            Some(rows_out as f64 / rows as f64)
        }
    });

    let timings = NativeStringMatchTimings {
        open_ms,
        row_count_ms,
        scan_build_ms,
        stream_init_ms,
        read_all_ms,
        serialize_ms: 0.0,
        total_ms: total_start.elapsed().as_secs_f64() * 1000.0,

        file_rows,
        rows_out,
        selectivity,

        stream_batches: 0,
        empty_stream_batches: 0,
        max_stream_batch_rows: matched_quads.len(),

        vortex_can_prune: combined_can_prune,
        total_row_groups: pruning_report.total_row_groups,
        candidate_row_groups: pruning_report.candidate_row_groups,
        candidate_rows_upper_bound: pruning_report.candidate_rows_upper_bound,
    };

    let memory = Some(ProcMemTimeline {
        before_open: mem_before_open,
        after_open: mem_after_open,
        after_scan_build: mem_after_scan_build,
        after_stream_init: mem_after_stream_init,
        before_read_all: mem_before_read_all,
        after_read_all: mem_after_read_all,
    });

    Ok((
        matched_quads,
        NativeStringScanDiagnostics {
            timings,
            proc_io,
            memory,
            filter_debug,
        },
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
    use vortex::VortexSessionDefault;
    use vortex_array::VortexSessionExecute;

    let total_start = Instant::now();

    let filter = build_native_string_pattern_filter(subject, predicate, object, graph);
    let filter_is_all = matches!(&filter, NativeStringPatternFilter::All);

    let deep_diagnostics = std::env::var_os("VORTEX_RDF_NATIVE_STRING_DEEP_DIAGNOSTICS").is_some();

    let pruning_report = if deep_diagnostics {
        inspect_cottas_native_string_pruning(input_path, subject, predicate, object, graph).await?
    } else {
        NativeStringPruningReport {
            filter_is_all,
            vortex_can_prune: None,
            total_row_groups: None,
            candidate_row_groups: None,
            candidate_rows_upper_bound: None,
            ordering: None,
            row_group_size: None,
        }
    };

    let mem_before_open = read_proc_mem_snapshot();

    let open_start = Instant::now();

    let file = NATIVE_STRING_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    let bound_s = subject.map(|x| x.to_string());
    let bound_p = predicate.map(|x| x.to_string());
    let bound_o = object.map(|x| x.to_string());
    let bound_g = graph.map(|x| x.to_string());

    let combined_can_prune = match &filter {
        NativeStringPatternFilter::All => None,
        NativeStringPatternFilter::Expr(expr) => file.can_prune(expr).ok(),
    };

    let s_expr = bound_s.as_ref().map(|v| eq(col("s"), lit(v.clone())));
    let p_expr = bound_p.as_ref().map(|v| eq(col("p"), lit(v.clone())));
    let o_expr = bound_o.as_ref().map(|v| eq(col("o"), lit(v.clone())));
    let g_expr = bound_g.as_ref().map(|v| eq(col("g"), lit(v.clone())));

    let s_can_prune = s_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let p_can_prune = p_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let o_can_prune = o_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());
    let g_can_prune = g_expr.as_ref().and_then(|expr| file.can_prune(expr).ok());

    let split_count = file.splits().ok().map(|splits| splits.len());

    let filter_debug = Some(NativeStringFilterDebug {
        bound_s,
        bound_p,
        bound_o,
        bound_g,

        combined_can_prune,
        s_can_prune,
        p_can_prune,
        o_can_prune,
        g_can_prune,

        split_count,
    });

    let vortex_can_prune = match &filter {
        NativeStringPatternFilter::All => None,
        NativeStringPatternFilter::Expr(expr) => file.can_prune(expr).ok(),
    };

    let open_ms = open_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_open = read_proc_mem_snapshot();

    let row_count_start = Instant::now();
    let file_rows = Some(file.row_count() as usize);
    let row_count_ms = row_count_start.elapsed().as_secs_f64() * 1000.0;

    let scan_build_start = Instant::now();

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativeStringPatternFilter::All => scan,
        NativeStringPatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let scan_build_ms = scan_build_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_scan_build = read_proc_mem_snapshot();

    let stream_init_start = Instant::now();

    let mut stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    let stream_init_ms = stream_init_start.elapsed().as_secs_f64() * 1000.0;
    let mem_after_stream_init = read_proc_mem_snapshot();

    let proc_before = read_proc_io_snapshot();
    let mem_before_read_all = read_proc_mem_snapshot();

    let stream_read_start = Instant::now();

    let mut serialize_ms = 0.0;
    let mut rows_out = 0usize;

    let mut stream_batches = 0usize;
    let mut empty_stream_batches = 0usize;
    let mut max_stream_batch_rows = 0usize;

    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    while let Some(batch) = stream.next().await {
        let quads = batch.map_err(VortexRdfError::from)?;

        stream_batches += 1;

        let batch_rows = quads.len();

        if batch_rows == 0 {
            empty_stream_batches += 1;
        }

        max_stream_batch_rows = max_stream_batch_rows.max(batch_rows);
        rows_out += batch_rows;

        let serialize_start = Instant::now();

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

        serialize_ms += serialize_start.elapsed().as_secs_f64() * 1000.0;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;

    let read_all_ms = stream_read_start.elapsed().as_secs_f64() * 1000.0;

    let mem_after_read_all = read_proc_mem_snapshot();
    let proc_after = read_proc_io_snapshot();

    let proc_io = match (proc_before, proc_after) {
        (Some(before), Some(after)) => Some(diff_proc_io(before, after)),
        _ => None,
    };

    let selectivity = file_rows.and_then(|rows| {
        if rows == 0 {
            None
        } else {
            Some(rows_out as f64 / rows as f64)
        }
    });

    let timings = NativeStringMatchTimings {
        open_ms,
        row_count_ms,
        scan_build_ms,
        stream_init_ms,
        read_all_ms,
        serialize_ms,
        total_ms: total_start.elapsed().as_secs_f64() * 1000.0,

        file_rows,
        rows_out,
        selectivity,

        stream_batches,
        empty_stream_batches,
        max_stream_batch_rows,

        vortex_can_prune: combined_can_prune,
        total_row_groups: pruning_report.total_row_groups,
        candidate_row_groups: pruning_report.candidate_row_groups,
        candidate_rows_upper_bound: pruning_report.candidate_rows_upper_bound,
    };

    let memory = Some(ProcMemTimeline {
        before_open: mem_before_open,
        after_open: mem_after_open,
        after_scan_build: mem_after_scan_build,
        after_stream_init: mem_after_stream_init,
        before_read_all: mem_before_read_all,
        after_read_all: mem_after_read_all,
    });

    Ok(NativeStringScanDiagnostics {
        timings,
        proc_io,
        memory,
        filter_debug,
    })
}

#[derive(Clone, Debug)]
pub struct CottasNativeStringConfig {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
    pub compression_profile: CottasVortexCompressionProfile,
    /// If enabled, write CNS object secondary-index sidecars:
    ///   *.object_term_to_id.vortex
    ///   *.object_idx_o_val_rid.vortex
    ///
    /// This does not change the primary string layout.
    pub enable_object_index: bool,
}

impl Default for CottasNativeStringConfig {
    fn default() -> Self {
        Self {
            ordering: TripleOrdering::SPO,
            // COTTAS / DuckDB baseline from the paper.
            row_group_size: 122_880,
            compression_profile: CottasVortexCompressionProfile::Balanced,
            // Disabled by default so previous CNS benchmarks remain comparable.
            enable_object_index: false,
        }
    }
}

fn cns_object_index_enabled(config_value: bool) -> bool {
    match std::env::var("VORTEX_RDF_CNS_OBJECT_INDEX") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => config_value,
    }
}

#[derive(Clone, Debug)]
struct NativeStringQuad {
    s: String,
    p: String,
    o: String,
    g: String,
}


#[derive(Default)]
struct NativeStringObjectIndexBuilder {
    object_to_id: HashMap<String, u32>,
    term_id_rows: Vec<(String, u32)>,
    val_rid_rows: Vec<(u32, u32)>,
}

impl NativeStringObjectIndexBuilder {
    fn observe_object(&mut self, row_id: u32, object: &str) -> Result<()> {
        let object_id = if let Some(id) = self.object_to_id.get(object) {
            *id
        } else {
            let next_id = u32::try_from(self.object_to_id.len()).map_err(|_| {
                VortexRdfError::Serialization(
                    "Too many distinct CNS object terms for u32 object index".into(),
                )
            })?;

            self.object_to_id.insert(object.to_owned(), next_id);
            self.term_id_rows.push((object.to_owned(), next_id));
            next_id
        };

        self.val_rid_rows.push((object_id, row_id));
        Ok(())
    }

    fn finish(mut self) -> (Vec<(String, u32)>, Vec<(u32, u32)>) {
        self.term_id_rows
            .sort_by(|(a_term, _), (b_term, _)| a_term.cmp(b_term));
        self.val_rid_rows
            .sort_by_key(|(object_id, row_id)| (*object_id, *row_id));
        (self.term_id_rows, self.val_rid_rows)
    }
}

fn build_object_term_to_id_array(rows: &[(String, u32)]) -> Result<ArrayRef> {
    let term_array = VarBinViewArray::from_iter(
        rows.iter().map(|(term, _)| Some(term.as_str())),
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();

    let id_array = PrimitiveArray::from_iter(rows.iter().map(|(_, id)| *id)).into_array();

    StructArray::from_fields(&[("term", term_array), ("id", id_array)])
        .map_err(VortexRdfError::Vortex)
        .map(|arr| arr.into_array())
}

fn build_object_idx_o_val_rid_array(rows: &[(u32, u32)]) -> Result<ArrayRef> {
    let val_array = PrimitiveArray::from_iter(rows.iter().map(|(object_id, _)| *object_id))
        .into_array();
    let rid_array = PrimitiveArray::from_iter(rows.iter().map(|(_, row_id)| *row_id)).into_array();

    StructArray::from_fields(&[("_idx_o_val", val_array), ("_idx_o_rid", rid_array)])
        .map_err(VortexRdfError::Vortex)
        .map(|arr| arr.into_array())
}

async fn write_single_array_to_vortex_file<W>(
    writer: &mut W,
    array: ArrayRef,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = array.dtype().clone();
    let arrays = stream::iter(vec![Ok(array)]);
    let array_stream = ArrayStreamAdapter::new(dtype, Box::pin(arrays));

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

    write_opts
        .write(writer, array_stream)
        .await
        .map_err(VortexRdfError::from)?;

    Ok(())
}

async fn write_object_index_sidecars(
    output_path: &Path,
    builder: NativeStringObjectIndexBuilder,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()> {
    let (term_id_rows, val_rid_rows) = builder.finish();

    let term_to_id_array = build_object_term_to_id_array(&term_id_rows)?;
    let val_rid_array = build_object_idx_o_val_rid_array(&val_rid_rows)?;

    let term_to_id_path = object_term_to_id_sidecar_path(output_path);
    let val_rid_path = object_idx_o_val_rid_sidecar_path(output_path);

    let mut term_to_id_file = tokio::fs::File::create(&term_to_id_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    write_single_array_to_vortex_file(
        &mut term_to_id_file,
        term_to_id_array,
        row_group_size,
        compression_profile,
    )
    .await?;

    let mut val_rid_file = tokio::fs::File::create(&val_rid_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    write_single_array_to_vortex_file(
        &mut val_rid_file,
        val_rid_array,
        row_group_size,
        compression_profile,
    )
    .await?;

    log::info!(
        "[cottas_native_strings::object_index] wrote object sidecars {:?} and {:?}",
        term_to_id_path,
        val_rid_path
    );

    Ok(())
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
    arrays: Pin<Box<dyn Stream<Item = VortexResult<ArrayRef>> + Send>>,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = empty_string_spog_array()?.dtype().clone();

    let stream = ArrayStreamAdapter::new(dtype, arrays);

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
    let row_group_size = config.row_group_size.max(1);

    let sort_batch_size = std::env::var("VORTEX_RDF_NATIVE_STRING_SORT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(row_group_size.saturating_mul(8).max(1_000_000));

    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let run_paths = spill_sorted_native_string_runs(
        quad_stream,
        config.ordering,
        sort_batch_size,
        temp_dir.path(),
    )
    .await?;

    let enable_object_index = cns_object_index_enabled(config.enable_object_index);
    let array_stream = merge_sorted_native_string_runs_to_array_stream(
        run_paths,
        output_path.to_path_buf(),
        config.ordering,
        row_group_size,
        enable_object_index,
        config.compression_profile,
    )?;

    let mut data_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_string_array_stream_to_vortex_file(
        &mut data_file,
        Box::pin(array_stream),
        row_group_size,
        config.compression_profile,
    )
    .await?;

    log::info!(
        "[cottas_native_strings] wrote globally sorted native string COTTAS Vortex file {:?} with profile {:?}",
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

async fn spill_sorted_native_string_runs<S>(
    mut quad_stream: S,
    ordering: TripleOrdering,
    sort_batch_size: usize,
    temp_dir: &Path,
) -> Result<Vec<PathBuf>>
where
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let sort_batch_size = sort_batch_size.max(1);
    let mut runs = Vec::new();
    let mut batch = Vec::with_capacity(sort_batch_size);
    let mut run_idx = 0usize;

    while let Some(item) = quad_stream.next().await {
        let quad = item?;
        batch.push(quad_to_native_string_quad(&quad));

        if batch.len() >= sort_batch_size {
            if ordering != TripleOrdering::None {
                batch.sort_by(|a, b| a.cmp_by_order(b, ordering));
            }

            let path = temp_dir.join(format!("native_string_run_{run_idx:06}.tsv"));
            write_native_string_run(&path, &batch)?;
            runs.push(path);

            batch.clear();
            run_idx += 1;
        }
    }

    if !batch.is_empty() {
        if ordering != TripleOrdering::None {
            batch.sort_by(|a, b| a.cmp_by_order(b, ordering));
        }

        let path = temp_dir.join(format!("native_string_run_{run_idx:06}.tsv"));
        write_native_string_run(&path, &batch)?;
        runs.push(path);
    }

    Ok(runs)
}

fn write_native_string_run(path: &Path, quads: &[NativeStringQuad]) -> Result<()> {
    let file =
        std::fs::File::create(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);

    for q in quads {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            escape_run_field(&q.s),
            escape_run_field(&q.p),
            escape_run_field(&q.o),
            escape_run_field(&q.g),
        )
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    Ok(())
}

fn escape_run_field(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn unescape_run_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }

    out
}

struct NativeStringRunReader {
    reader: BufReader<std::fs::File>,
}

impl NativeStringRunReader {
    fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativeStringQuad>> {
        let mut line = String::new();

        let n = self
            .reader
            .read_line(&mut line)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        if n == 0 {
            return Ok(None);
        }

        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }

        let mut parts = line.splitn(4, '\t');

        let subject = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed native string run".into()))?;
        let predicate = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed native string run".into()))?;
        let object = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed native string run".into()))?;
        let graph = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed native string run".into()))?;

        Ok(Some(NativeStringQuad {
            s: unescape_run_field(subject),
            p: unescape_run_field(predicate),
            o: unescape_run_field(object),
            g: unescape_run_field(graph),
        }))
    }
}

struct RunHeapItem {
    quad: NativeStringQuad,
    run_idx: usize,
    ordering: TripleOrdering,
}

impl PartialEq for RunHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.quad.cmp_by_order(&other.quad, self.ordering) == std::cmp::Ordering::Equal
    }
}

impl Eq for RunHeapItem {}

impl PartialOrd for RunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RunHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse because BinaryHeap is max-heap; we need min-heap.
        other
            .quad
            .cmp_by_order(&self.quad, self.ordering)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}

fn merge_sorted_native_string_runs_to_array_stream(
    run_paths: Vec<PathBuf>,
    output_path: PathBuf,
    ordering: TripleOrdering,
    row_group_size: usize,
    enable_object_index: bool,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);

    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());

        for path in &run_paths {
            readers.push(
                NativeStringRunReader::new(path)
                    .map_err(rdf_err_to_vortex_err)?
            );
        }

        let mut heap = BinaryHeap::new();

        for run_idx in 0..readers.len() {
            if let Some(quad) = readers[run_idx]
                .read_one()
                .map_err(rdf_err_to_vortex_err)?
            {
                heap.push(RunHeapItem {
                    quad,
                    run_idx,
                    ordering,
                });
            }
        }

        let mut row_group = Vec::with_capacity(row_group_size);
        let mut row_group_idx = 0usize;
        let mut row_group_stats = Vec::new();
        let mut global_row_id: u32 = 0;
        let mut object_index_builder = if enable_object_index {
            Some(NativeStringObjectIndexBuilder::default())
        } else {
            None
        };

        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let quad = item.quad;
            if let Some(builder) = object_index_builder.as_mut() {
                builder
                    .observe_object(global_row_id, &quad.o)
                    .map_err(rdf_err_to_vortex_err)?;
            }
            global_row_id = global_row_id.checked_add(1).ok_or_else(|| {
                rdf_err_to_vortex_err(VortexRdfError::Serialization(
                    "Too many CNS primary rows for u32 row-id object index".into(),
                ))
            })?;
            row_group.push(quad);

            if let Some(next_quad) = readers[run_idx]
                .read_one()
                .map_err(rdf_err_to_vortex_err)?
            {
                heap.push(RunHeapItem {
                    quad: next_quad,
                    run_idx,
                    ordering,
                });
            }

            if row_group.len() >= row_group_size {
                if let Some(stats) = compute_row_group_stats(row_group_idx, &row_group) {
                    row_group_stats.push(stats);
                }

                let array = build_string_spog_array(&row_group)
                    .map_err(rdf_err_to_vortex_err)?;

                row_group.clear();
                row_group_idx += 1;

                yield array;
            }
        }

        if !row_group.is_empty() {
            if let Some(stats) = compute_row_group_stats(row_group_idx, &row_group) {
                row_group_stats.push(stats);
            }

            let array = build_string_spog_array(&row_group)
                .map_err(rdf_err_to_vortex_err)?;

            yield array;
        }

        if row_group_idx == 0 && row_group_stats.is_empty() {
            yield empty_string_spog_array()
                .map_err(rdf_err_to_vortex_err)?;
        }

        write_row_group_stats_sidecar_from_stats(
            &output_path,
            ordering,
            row_group_size,
            row_group_stats,
        )
        .await
        .map_err(rdf_err_to_vortex_err)?;

        if let Some(builder) = object_index_builder {
            write_object_index_sidecars(
                &output_path,
                builder,
                row_group_size,
                compression_profile,
            )
            .await
            .map_err(rdf_err_to_vortex_err)?;
        }
    })
}

async fn write_row_group_stats_sidecar_from_stats(
    output_path: &Path,
    ordering: TripleOrdering,
    row_group_size: usize,
    row_groups: Vec<NativeStringRowGroupStats>,
) -> Result<()> {
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

fn rdf_err_to_vortex_err(e: VortexRdfError) -> VortexError {
    vortex_error::vortex_err!(
        "vortex-rdf error while streaming native string row group: {}",
        e
    )
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
