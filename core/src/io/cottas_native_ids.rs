use crate::error::{Result, VortexRdfError};
use crate::index::{RdfDictionary, SimpleDictionaryView};
use crate::io::utils::CottasVortexCompressionProfile;
use crate::store::layout::cottas::TripleOrdering;
use async_trait::async_trait;

use futures::{Stream, StreamExt};
use oxrdf::Quad;
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;
use vortex::VortexSessionDefault;
use vortex_error::{VortexError, VortexResult};

use std::collections::{BinaryHeap, HashMap, HashSet};
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef, IntoArray};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::Buffer;
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfSerializer};

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
use serde::{Deserialize, Serialize};

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
struct NativeDictPair {
    id: u32,
    term: String,
}

#[derive(Clone, Copy, Debug)]
enum PairRunOrder {
    Term,
    Id,
}

/// Sort/spill representation. This is deliberately string-based only for the
/// external-sort phase, where the strings live on disk, not in memory.
#[derive(Clone, Debug)]
struct NativeTriple {
    s: String,
    p: String,
    o: String,
    g: String,
}

impl NativeTriple {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> Ordering {
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
            TripleOrdering::None => Ordering::Equal,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct NativeIdTriple {
    s: u32,
    p: u32,
    o: u32,
    g: u32,
}

impl NativeIdTriple {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> Ordering {
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
            TripleOrdering::None => Ordering::Equal,
        }
    }
}

fn native_dict_term_to_id_entries_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.dict.term_to_id.entries.bin"))
}

fn native_dict_term_to_id_blob_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.dict.term_to_id.blob"))
}

fn native_term_to_id_binary_sidecar_exists(data_path: &Path) -> bool {
    native_dict_term_to_id_entries_path(data_path).is_file()
        && native_dict_term_to_id_blob_path(data_path).is_file()
}

fn native_dict_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.dict.vortex"))
}

fn native_dict_term_to_id_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.dict.term_to_id.vortex"))
}

fn quad_to_native_triple(quad: &Quad) -> NativeTriple {
    NativeTriple {
        s: quad.subject.to_string(),
        p: quad.predicate.to_string(),
        o: quad.object.to_string(),
        g: quad.graph_name.to_string(),
    }
}

pub async fn match_cottas_native_file_as_triples(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<Vec<(String, String, String)>> {
    let (filter, _term_lookup_ms) =
        build_native_pattern_filter_lazy_with_stats(input_path, subject, predicate, object, graph)
            .await?;

    if matches!(filter, NativePatternFilter::Empty) {
        return Ok(Vec::new());
    }

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;

    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match filter {
        NativePatternFilter::All => scan,
        NativePatternFilter::Empty => unreachable!("handled above"),
        NativePatternFilter::Expr(expr) => scan.with_filter(expr),
    };

    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;

    let matched_quads = stream.read_all().await.map_err(VortexRdfError::from)?;

    if matched_quads.len() == 0 {
        return Ok(Vec::new());
    }

    let (s_ids, p_ids, o_ids, g_ids) = extract_spog_id_columns(&matched_quads)?;
    let unique_ids = collect_unique_ids(&s_ids, &p_ids, &o_ids, &g_ids);
    let id_to_term = lookup_terms_by_ids_from_sidecar(input_path, &unique_ids).await?;

    let mut out = Vec::with_capacity(s_ids.len());

    for i in 0..s_ids.len() {
        let s = id_to_term
            .get(&s_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "S ID {} missing from id_to_term sidecar",
                    s_ids[i]
                ))
            })?
            .clone();

        let p = id_to_term
            .get(&p_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "P ID {} missing from id_to_term sidecar",
                    p_ids[i]
                ))
            })?
            .clone();

        let o = id_to_term
            .get(&o_ids[i])
            .ok_or_else(|| {
                VortexRdfError::Deserialization(format!(
                    "O ID {} missing from id_to_term sidecar",
                    o_ids[i]
                ))
            })?
            .clone();

        out.push((s, p, o));
    }

    Ok(out)
}

/// Executes the same optimized access planner used by
/// `match_cottas_native_file_with_diagnostics`, but returns triples for the
/// Python/RDFLib binding.
///
/// The indexed planner returns projected native-ID rows directly; only IDs from
/// unbound output columns are decoded before constructing returned triples.
pub async fn match_cottas_native_file_as_triples_optimized(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<Vec<(String, String, String)>> {
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;

    projected_native_id_rows_as_triples(input_path, &planned.rows, &planned.bound_terms).await
}

pub async fn serialize_cottas_native_file<Dict, S>(
    quad_stream: S,
    output_path: &Path,
    config: CottasNativeConfig,
) -> Result<()>
where
    Dict: RdfDictionary + Send + Sync + 'static,
    S: Stream<Item = Result<Quad>> + Unpin + Send + 'static,
{
    let row_group_size = config.row_group_size.max(1);

    let sort_batch_size = std::env::var("VORTEX_RDF_NATIVE_ID_SORT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(row_group_size.saturating_mul(8).max(1_000_000));

    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let string_run_paths = spill_sorted_native_id_string_runs(
        quad_stream,
        config.ordering,
        sort_batch_size,
        temp_dir.path(),
    )
    .await?;

    let mut dictionary = Dict::new();

    let pair_run_paths = build_dictionary_and_pair_runs::<Dict>(
        &mut dictionary,
        &string_run_paths,
        temp_dir.path(),
    )?;

    let id_run_paths = encode_string_runs_to_id_runs::<Dict>(
        &dictionary,
        &string_run_paths,
        config.ordering,
        temp_dir.path(),
    )?;

    drop(string_run_paths);

    let array_stream =
        merge_sorted_id_runs_to_array_stream(id_run_paths, config.ordering, row_group_size)?;

    let mut data_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_array_stream_to_vortex_file_streaming(
        &mut data_file,
        Box::pin(array_stream),
        row_group_size,
        config.compression_profile,
    )
    .await?;

    write_dictionary_lookup_sidecars_from_pair_runs(
        &pair_run_paths,
        output_path,
        config.dict_row_group_size,
    )
    .await?;
    if config.ordering == TripleOrdering::SPO {
        let subject_index_stats = build_cottas_native_subject_range_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built SPO subject range index during serialization: ranges={}, rows={}, total_ms={:.3}",
            subject_index_stats.ranges_written,
            subject_index_stats.rows_scanned,
            subject_index_stats.total_ms
        );
        let po_exact_stats = build_cottas_native_po_exact_ranges_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built PO exact-ranges index during serialization: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            po_exact_stats.row_groups,
            po_exact_stats.rows_scanned,
            po_exact_stats.unique_po_hashes_written,
            po_exact_stats.total_ms
        );
        let po_v2_stats = build_cottas_native_po_exact_ranges_v2_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built typed PO v2 directory/payload: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            po_v2_stats.row_groups,
            po_v2_stats.rows_scanned,
            po_v2_stats.unique_po_hashes_written,
            po_v2_stats.total_ms
        );
        let p_exact_stats = build_cottas_native_p_exact_ranges_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built predicate exact-ranges index during serialization: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            p_exact_stats.row_groups,
            p_exact_stats.rows_scanned,
            p_exact_stats.unique_po_hashes_written,
            p_exact_stats.total_ms
        );
        let o_exact_stats = build_cottas_native_o_exact_ranges_index(output_path).await?;
        log::info!(
            "[cottas_native_ids] built object exact-ranges index during serialization: row_groups={}, rows={}, exact_ranges={}, total_ms={:.3}",
            o_exact_stats.row_groups,
            o_exact_stats.rows_scanned,
            o_exact_stats.unique_po_hashes_written,
            o_exact_stats.total_ms
        );
    } else {
        log::info!(
            "[cottas_native_ids] skipping subject range index for ordering {:?}; subject ranges require SPO ordering",
            config.ordering
        );
    }

    Ok(())
}

async fn spill_sorted_native_id_string_runs<S>(
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
        batch.push(quad_to_native_triple(&quad));

        if batch.len() >= sort_batch_size {
            flush_string_run(&mut batch, ordering, temp_dir, run_idx, &mut runs)?;
            run_idx += 1;
        }
    }

    if !batch.is_empty() {
        flush_string_run(&mut batch, ordering, temp_dir, run_idx, &mut runs)?;
    }

    Ok(runs)
}

fn flush_string_run(
    batch: &mut Vec<NativeTriple>,
    ordering: TripleOrdering,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    if ordering != TripleOrdering::None {
        batch.sort_by(|a, b| a.cmp_by_order(b, ordering));
    }
    let path = temp_dir.join(format!("native_id_string_run_{run_idx:06}.tsv"));
    write_native_string_run(&path, batch)?;
    runs.push(path);
    batch.clear();
    Ok(())
}

fn build_dictionary_and_pair_runs<Dict>(
    dictionary: &mut Dict,
    string_run_paths: &[PathBuf],
    temp_dir: &Path,
) -> Result<PairRunPaths>
where
    Dict: RdfDictionary,
{
    let pair_batch_size = std::env::var("VORTEX_RDF_NATIVE_ID_PAIR_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let mut batch: Vec<NativeDictPair> = Vec::with_capacity(pair_batch_size);

    let mut term_run_paths = Vec::new();
    let mut id_run_paths = Vec::new();
    let mut run_idx = 0usize;

    for path in string_run_paths {
        let mut reader = NativeStringRunReader::new(path)?;

        while let Some(triple) = reader.read_one()? {
            insert_term_and_record_pair(dictionary, &triple.s, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.p, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.o, &mut batch)?;
            insert_term_and_record_pair(dictionary, &triple.g, &mut batch)?;

            if batch.len() >= pair_batch_size {
                flush_pair_runs(
                    &mut batch,
                    temp_dir,
                    run_idx,
                    &mut term_run_paths,
                    &mut id_run_paths,
                )?;
                run_idx += 1;
            }
        }
    }

    if !batch.is_empty() {
        flush_pair_runs(
            &mut batch,
            temp_dir,
            run_idx,
            &mut term_run_paths,
            &mut id_run_paths,
        )?;
    }

    Ok(PairRunPaths {
        term_run_paths,
        id_run_paths,
    })
}
#[derive(Clone, Debug)]
struct PairRunPaths {
    term_run_paths: Vec<PathBuf>,
    id_run_paths: Vec<PathBuf>,
}
fn insert_term_and_record_pair<Dict>(
    dictionary: &mut Dict,
    term: &str,
    batch: &mut Vec<NativeDictPair>,
) -> Result<()>
where
    Dict: RdfDictionary,
{
    if dictionary.get_id(term).is_none() {
        dictionary.get_or_insert(term);

        let id = dictionary.get_id(term).ok_or_else(|| {
            VortexRdfError::Serialization(format!(
                "Dictionary inserted term but get_id failed afterward: {}",
                term
            ))
        })?;

        batch.push(NativeDictPair {
            id,
            term: term.to_string(),
        });
    }

    Ok(())
}
fn flush_pair_runs(
    batch: &mut Vec<NativeDictPair>,
    temp_dir: &Path,
    run_idx: usize,
    term_run_paths: &mut Vec<PathBuf>,
    id_run_paths: &mut Vec<PathBuf>,
) -> Result<()> {
    let term_path = temp_dir.join(format!("native_id_pair_term_run_{run_idx:06}.tsv"));
    let id_path = temp_dir.join(format!("native_id_pair_id_run_{run_idx:06}.tsv"));

    batch.sort_by(|a, b| a.term.cmp(&b.term).then_with(|| a.id.cmp(&b.id)));
    write_pair_run(&term_path, batch)?;
    term_run_paths.push(term_path);

    batch.sort_by_key(|p| p.id);
    write_pair_run(&id_path, batch)?;
    id_run_paths.push(id_path);

    batch.clear();

    Ok(())
}

fn write_pair_run(path: &Path, pairs: &[NativeDictPair]) -> Result<()> {
    let file =
        std::fs::File::create(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);

    for pair in pairs {
        writeln!(writer, "{}\t{}", pair.id, escape_run_field(&pair.term))
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    Ok(())
}
struct PairRunReader {
    reader: BufReader<std::fs::File>,
}

impl PairRunReader {
    fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativeDictPair>> {
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

        let mut parts = line.splitn(2, '\t');

        let id_raw = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed dictionary pair run".into()))?;

        let term_raw = parts
            .next()
            .ok_or_else(|| VortexRdfError::Serialization("Malformed dictionary pair run".into()))?;

        let id = id_raw
            .parse::<u32>()
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Some(NativeDictPair {
            id,
            term: unescape_run_field(term_raw),
        }))
    }
}
struct PairHeapItem {
    pair: NativeDictPair,
    run_idx: usize,
    order: PairRunOrder,
}

impl PartialEq for PairHeapItem {
    fn eq(&self, other: &Self) -> bool {
        match self.order {
            PairRunOrder::Term => {
                self.pair.term == other.pair.term && self.pair.id == other.pair.id
            }
            PairRunOrder::Id => self.pair.id == other.pair.id,
        }
    }
}

impl Eq for PairHeapItem {}

impl PartialOrd for PairHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PairHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.order {
            PairRunOrder::Term => other
                .pair
                .term
                .cmp(&self.pair.term)
                .then_with(|| other.pair.id.cmp(&self.pair.id))
                .then_with(|| other.run_idx.cmp(&self.run_idx)),

            PairRunOrder::Id => other
                .pair
                .id
                .cmp(&self.pair.id)
                .then_with(|| other.run_idx.cmp(&self.run_idx)),
        }
    }
}

fn encode_string_runs_to_id_runs<Dict>(
    dictionary: &Dict,
    string_run_paths: &[PathBuf],
    ordering: TripleOrdering,
    temp_dir: &Path,
) -> Result<Vec<PathBuf>>
where
    Dict: RdfDictionary,
{
    let mut id_run_paths = Vec::with_capacity(string_run_paths.len());
    for (run_idx, string_path) in string_run_paths.iter().enumerate() {
        let id_path = temp_dir.join(format!("native_id_encoded_run_{run_idx:06}.bin"));
        let mut reader = NativeStringRunReader::new(string_path)?;
        let mut encoded_batch: Vec<NativeIdTriple> = Vec::new();

        while let Some(triple) = reader.read_one()? {
            encoded_batch.push(NativeIdTriple {
                s: dictionary.get_id(&triple.s).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing subject in dictionary: {}",
                        triple.s
                    ))
                })?,
                p: dictionary.get_id(&triple.p).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing predicate in dictionary: {}",
                        triple.p
                    ))
                })?,
                o: dictionary.get_id(&triple.o).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing object in dictionary: {}",
                        triple.o
                    ))
                })?,
                g: dictionary.get_id(&triple.g).ok_or_else(|| {
                    VortexRdfError::Serialization(format!(
                        "Missing graph in dictionary: {}",
                        triple.g
                    ))
                })?,
            });
        }

        // Critical v4 invariant fix: string runs were sorted by RDF lexical term order,
        // but dictionary IDs are assigned independently. After encoding strings -> u32 IDs,
        // each run must be re-sorted by native-ID order before the final k-way merge.
        if ordering != TripleOrdering::None {
            encoded_batch.sort_by(|a, b| a.cmp_by_order(b, ordering));

            for pair in encoded_batch.windows(2) {
                if pair[0].cmp_by_order(&pair[1], ordering) == Ordering::Greater {
                    return Err(VortexRdfError::Serialization(format!(
                        "Encoded native ID run {} is not sorted by {:?}",
                        run_idx, ordering
                    )));
                }
            }
        }

        let file = std::fs::File::create(&id_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        for encoded in encoded_batch {
            write_id_triple(&mut writer, encoded)?;
        }
        writer
            .flush()
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        id_run_paths.push(id_path);
    }
    Ok(id_run_paths)
}

fn write_native_string_run(path: &Path, triples: &[NativeTriple]) -> Result<()> {
    let file =
        std::fs::File::create(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut writer = BufWriter::new(file);
    for q in triples {
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

    fn read_one(&mut self) -> Result<Option<NativeTriple>> {
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
        let s = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let p = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let o = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;
        let g = parts.next().ok_or_else(|| {
            VortexRdfError::Serialization("Malformed native ID string run".into())
        })?;

        Ok(Some(NativeTriple {
            s: unescape_run_field(s),
            p: unescape_run_field(p),
            o: unescape_run_field(o),
            g: unescape_run_field(g),
        }))
    }
}

fn write_id_triple<W: Write>(writer: &mut W, triple: NativeIdTriple) -> Result<()> {
    writer
        .write_all(&triple.s.to_le_bytes())
        .and_then(|_| writer.write_all(&triple.p.to_le_bytes()))
        .and_then(|_| writer.write_all(&triple.o.to_le_bytes()))
        .and_then(|_| writer.write_all(&triple.g.to_le_bytes()))
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}

struct NativeIdRunReader {
    reader: BufReader<std::fs::File>,
}

impl NativeIdRunReader {
    fn new(path: &Path) -> Result<Self> {
        let file =
            std::fs::File::open(path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativeIdTriple>> {
        let mut buf = [0u8; 16];

        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(NativeIdTriple {
                s: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
                p: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
                o: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
                g: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            })),

            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),

            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}

struct IdRunHeapItem {
    triple: NativeIdTriple,
    run_idx: usize,
    ordering: TripleOrdering,
}

impl PartialEq for IdRunHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.triple.cmp_by_order(&other.triple, self.ordering) == Ordering::Equal
    }
}
impl Eq for IdRunHeapItem {}
impl PartialOrd for IdRunHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IdRunHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: BinaryHeap is max-heap; we need min-heap.
        other
            .triple
            .cmp_by_order(&self.triple, self.ordering)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}

fn merge_sorted_id_runs_to_array_stream(
    run_paths: Vec<PathBuf>,
    ordering: TripleOrdering,
    row_group_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());
        for path in &run_paths {
            readers.push(NativeIdRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }

        let mut heap = BinaryHeap::new();
        for run_idx in 0..readers.len() {
            if let Some(triple) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple, run_idx, ordering });
            }
        }

        let mut s_ids = Vec::with_capacity(row_group_size);
        let mut p_ids = Vec::with_capacity(row_group_size);
        let mut o_ids = Vec::with_capacity(row_group_size);
        let mut g_ids = Vec::with_capacity(row_group_size);

        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            s_ids.push(item.triple.s);
            p_ids.push(item.triple.p);
            o_ids.push(item.triple.o);
            g_ids.push(item.triple.g);

            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(IdRunHeapItem { triple: next, run_idx, ordering });
            }

            if s_ids.len() >= row_group_size {
                let array = build_spog_array(
                    std::mem::take(&mut s_ids),
                    std::mem::take(&mut p_ids),
                    std::mem::take(&mut o_ids),
                    std::mem::take(&mut g_ids),
                ).map_err(rdf_err_to_vortex_err)?;
                s_ids = Vec::with_capacity(row_group_size);
                p_ids = Vec::with_capacity(row_group_size);
                o_ids = Vec::with_capacity(row_group_size);
                g_ids = Vec::with_capacity(row_group_size);
                yield array;
            }
        }

        if !s_ids.is_empty() {
            yield build_spog_array(s_ids, p_ids, o_ids, g_ids).map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_spog_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}

fn build_spog_array(
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("s", PrimitiveArray::from_iter(s_ids).into_array()),
        ("p", PrimitiveArray::from_iter(p_ids).into_array()),
        ("o", PrimitiveArray::from_iter(o_ids).into_array()),
        ("g", PrimitiveArray::from_iter(g_ids).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|arr| arr.into_array())
}

fn empty_spog_array() -> Result<ArrayRef> {
    build_spog_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())
}

async fn write_array_stream_to_vortex_file_streaming<W>(
    writer: &mut W,
    arrays: Pin<Box<dyn Stream<Item = VortexResult<ArrayRef>> + Send>>,
    row_group_size: usize,
    compression_profile: CottasVortexCompressionProfile,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let dtype = empty_spog_array()?.dtype().clone();
    let stream = ArrayStreamAdapter::new(dtype, arrays);
    let strategy_builder =
        WriteStrategyBuilder::default().with_row_block_size(row_group_size.max(1));
    let strategy_builder = match compression_profile {
        CottasVortexCompressionProfile::Balanced => strategy_builder,
        CottasVortexCompressionProfile::Compact => strategy_builder
            .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact()),
    };

    let start = Instant::now();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy_builder.build())
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!(
        "[cottas_native_ids] streamed ID-only Vortex data file in {:?}",
        start.elapsed()
    );
    Ok(())
}

fn rdf_err_to_vortex_err(e: VortexRdfError) -> VortexError {
    vortex_error::vortex_err!(
        "vortex-rdf error while streaming native string row group: {}",
        e
    )
}

/// Format-independent dictionary access for native-ID planning and decoding.
/// The current implementation delegates to binary sidecars; a Vortex-native
/// dictionary can implement this contract without changing the planner.
#[async_trait]
pub trait NativeDictionaryProvider: Send + Sync {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)>;

    async fn lookup_terms_by_ids(
        &self,
        ids: &[u32],
    ) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)>;
}

/// Format-independent exact row-range access used by the native-ID planner.
#[async_trait]
pub trait NativeIndexProvider: Send + Sync {
    async fn subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>>;
    async fn po_access(&self, predicate_id: u32, object_id: u32) -> Result<Option<NativePoAccess>>;
    async fn predicate_access(&self, predicate_id: u32) -> Result<Option<NativePredicateAccess>>;
    async fn object_access(&self, object_id: u32) -> Result<Option<NativeObjectAccess>>;
    fn subject_strategy(&self) -> &'static str;
}

#[derive(Clone, Debug)]
pub struct NativePoAccess {
    ranges: Option<Vec<Range<u64>>>,
    candidate_ranges: usize,
    candidate_rows: u64,
    strategy: &'static str,
}

#[derive(Clone, Debug)]
pub struct NativePredicateAccess {
    ranges: Option<Vec<Range<u64>>>,
    candidate_ranges: usize,
    candidate_rows: u64,
    strategy: &'static str,
}
pub type NativeObjectAccess = NativePredicateAccess;

#[derive(Clone, Debug)]
pub struct BinaryNativeProviders {
    data_path: PathBuf,
}

impl BinaryNativeProviders {
    pub fn new(data_path: &Path) -> Self {
        Self {
            data_path: data_path.to_path_buf(),
        }
    }
}

#[async_trait]
impl NativeDictionaryProvider for BinaryNativeProviders {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
        lookup_term_id_from_sidecar_with_stats(&self.data_path, term, column).await
    }

    async fn lookup_terms_by_ids(
        &self,
        ids: &[u32],
    ) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
        lookup_terms_by_ids_from_sidecar_with_stats(&self.data_path, ids).await
    }
}

#[async_trait]
impl NativeIndexProvider for BinaryNativeProviders {
    async fn subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>> {
        match native_subject_index_backend(&self.data_path)? {
            NativeSubjectIndexBackend::Binary => {
                if !native_subject_range_index_exists(&self.data_path) {
                    return Ok(None);
                }
                Ok(
                    lookup_subject_range_from_sidecar(&self.data_path, subject_id)?
                        .map(|range| range.start..range.end),
                )
            }
            NativeSubjectIndexBackend::Vortex => {
                lookup_subject_range_from_vortex(&self.data_path, subject_id).await
            }
        }
    }

    async fn po_access(&self, predicate_id: u32, object_id: u32) -> Result<Option<NativePoAccess>> {
        match native_po_index_backend(&self.data_path)? {
            NativePoIndexBackend::Binary => {
                let ranges = lookup_exact_row_ranges(
                    &native_po_exact_ranges_path(&self.data_path),
                    NATIVE_PO_EXACT_RANGES_MAGIC,
                    native_po_hash(predicate_id, object_id),
                    "PO exact range",
                )?;
                let candidate_ranges = ranges.len();
                let candidate_rows = range_rows(&ranges);
                let accepted = po_exact_access_accepted(candidate_ranges, candidate_rows);
                Ok(Some(NativePoAccess {
                    ranges: accepted.then_some(ranges),
                    candidate_ranges,
                    candidate_rows,
                    strategy: "po-exact-ranges-binary",
                }))
            }
            NativePoIndexBackend::VortexV2 => {
                lookup_po_access_from_vortex_v2(&self.data_path, predicate_id, object_id).await
            }
        }
    }

    async fn predicate_access(&self, predicate_id: u32) -> Result<Option<NativePredicateAccess>> {
        match native_predicate_index_backend(&self.data_path)? {
            NativePredicateIndexBackend::Binary => {
                let ranges = lookup_exact_row_ranges(
                    &native_p_exact_ranges_path(&self.data_path),
                    NATIVE_P_EXACT_RANGES_MAGIC,
                    u64::from(predicate_id),
                    "predicate exact range",
                )?;
                let candidate_rows = range_rows(&ranges);
                let candidate_ranges = ranges.len();
                let use_ranges = candidate_ranges <= predicate_exact_max_ranges()
                    && candidate_rows <= predicate_exact_max_rows();
                Ok(Some(NativePredicateAccess {
                    ranges: use_ranges.then_some(ranges),
                    candidate_ranges,
                    candidate_rows,
                    strategy: "p-exact-ranges-binary",
                }))
            }
            NativePredicateIndexBackend::VortexV1 => {
                let ranges =
                    lookup_predicate_ranges_from_vortex(&self.data_path, predicate_id).await?;
                let candidate_rows = range_rows(&ranges);
                let candidate_ranges = ranges.len();
                let use_ranges = candidate_ranges <= predicate_exact_max_ranges()
                    && candidate_rows <= predicate_exact_max_rows();
                Ok(Some(NativePredicateAccess {
                    ranges: use_ranges.then_some(ranges),
                    candidate_ranges,
                    candidate_rows,
                    strategy: "p-exact-ranges-vortex-v1",
                }))
            }
            NativePredicateIndexBackend::VortexV2 => {
                lookup_predicate_access_from_vortex_v2(&self.data_path, predicate_id).await
            }
        }
    }
    async fn object_access(&self, object_id: u32) -> Result<Option<NativeObjectAccess>> {
        let directory = native_o_exact_directory_v2_path(&self.data_path);
        let payload = native_o_exact_ranges_v2_path(&self.data_path);
        let configured = std::env::var("VORTEX_RDF_NATIVE_OBJECT_INDEX_BACKEND")
            .unwrap_or_else(|_| "auto".to_string());
        match configured.as_str() {
            "auto" if directory.is_file() && payload.is_file() => {
                lookup_object_access_from_vortex_v2(&self.data_path, object_id).await
            }
            "auto" | "none" => Ok(None),
            "vortex-v2" if directory.is_file() && payload.is_file() => {
                lookup_object_access_from_vortex_v2(&self.data_path, object_id).await
            }
            "vortex-v2" => Err(VortexRdfError::InvalidOperation(format!(
                "Vortex object v2 directory {:?} or payload {:?} is missing",
                directory, payload
            ))),
            other => Err(VortexRdfError::InvalidOperation(format!(
                "Unsupported VORTEX_RDF_NATIVE_OBJECT_INDEX_BACKEND={other:?}; expected auto, none, or vortex-v2"
            ))),
        }
    }

    fn subject_strategy(&self) -> &'static str {
        match native_subject_index_backend(&self.data_path) {
            Ok(NativeSubjectIndexBackend::Vortex) => "subject-ranges-vortex-v1",
            _ => "subject-ranges-binary",
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
struct ResolvedNativePattern {
    s: Option<u32>,
    p: Option<u32>,
    o: Option<u32>,
    g: Option<u32>,
}

impl ResolvedNativePattern {
    fn filter(self) -> NativePatternFilter {
        let mut filters = Vec::new();
        if let Some(id) = self.s {
            filters.push(eq(col("s"), lit(id)));
        }
        if let Some(id) = self.p {
            filters.push(eq(col("p"), lit(id)));
        }
        if let Some(id) = self.o {
            filters.push(eq(col("o"), lit(id)));
        }
        if let Some(id) = self.g {
            filters.push(eq(col("g"), lit(id)));
        }
        match filters.into_iter().reduce(and) {
            Some(expr) => NativePatternFilter::Expr(expr),
            None => NativePatternFilter::All,
        }
    }
}

#[derive(Debug, Default)]
struct NativeAccessPlan {
    ranges: Option<Vec<Range<u64>>>,
    strategy: String,
    lookup_ms: f64,
    candidate_ranges: usize,
    candidate_rows: u64,
    subject_range: Option<Range<u64>>,
    po_index_used: bool,
}

async fn resolve_native_pattern<D: NativeDictionaryProvider + ?Sized>(
    dictionary: &D,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(
    Option<ResolvedNativePattern>,
    f64,
    Vec<NativeTermToIdLookupStats>,
)> {
    let mut resolved = ResolvedNativePattern::default();
    let mut total_lookup_ms = 0.0;
    let mut stats_out = Vec::new();

    macro_rules! resolve_bound {
        ($value:expr, $field:ident, $column:literal) => {
            if let Some(value) = $value {
                let (id, stats) = dictionary
                    .lookup_term_id(&value.to_string(), Some($column))
                    .await?;
                total_lookup_ms += stats.total_ms;
                stats_out.push(stats);
                let Some(id) = id else {
                    return Ok((None, total_lookup_ms, stats_out));
                };
                resolved.$field = Some(id);
            }
        };
    }

    resolve_bound!(subject, s, "s");
    resolve_bound!(predicate, p, "p");
    resolve_bound!(object, o, "o");
    resolve_bound!(graph, g, "g");
    Ok((Some(resolved), total_lookup_ms, stats_out))
}

async fn plan_native_access<I: NativeIndexProvider + ?Sized>(
    indexes: &I,
    resolved: ResolvedNativePattern,
    subject_bound: bool,
    predicate_bound: bool,
    object_bound: bool,
) -> Result<NativeAccessPlan> {
    let start = Instant::now();

    if subject_bound {
        if let Some(subject_id) = resolved.s {
            if let Some(range) = indexes.subject_range(subject_id).await? {
                return Ok(NativeAccessPlan {
                    ranges: Some(vec![range.clone()]),
                    strategy: indexes.subject_strategy().to_string(),
                    lookup_ms: elapsed_ms(start),
                    candidate_ranges: 1,
                    candidate_rows: range.end.saturating_sub(range.start),
                    subject_range: Some(range),
                    po_index_used: false,
                });
            }
        }
    }

    if !subject_bound && predicate_bound && object_bound {
        if let (Some(predicate_id), Some(object_id)) = (resolved.p, resolved.o) {
            if let Some(access) = indexes.po_access(predicate_id, object_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-po".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: use_ranges,
                });
            }
        }
    }

    if !subject_bound && predicate_bound && !object_bound {
        if let Some(predicate_id) = resolved.p {
            if let Some(access) = indexes.predicate_access(predicate_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-predicate".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: false,
                });
            }
        }
    }

    if !subject_bound && !predicate_bound && object_bound {
        if let Some(object_id) = resolved.o {
            if let Some(access) = indexes.object_access(object_id).await? {
                let use_ranges = access.ranges.is_some();
                return Ok(NativeAccessPlan {
                    candidate_ranges: access.candidate_ranges,
                    candidate_rows: access.candidate_rows,
                    ranges: access.ranges,
                    strategy: if use_ranges {
                        access.strategy.to_string()
                    } else {
                        "none-high-cardinality-object".to_string()
                    },
                    lookup_ms: elapsed_ms(start),
                    subject_range: None,
                    po_index_used: false,
                });
            }
        }
    }
    Ok(NativeAccessPlan {
        strategy: "none".to_string(),
        lookup_ms: elapsed_ms(start),
        ..NativeAccessPlan::default()
    })
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
pub struct NativeTermToIdLookupStats {
    pub column: Option<String>,
    pub term_len: usize,
    pub term_preview: String,
    pub found_id: Option<u32>,
    pub total_ms: f64,
    pub open_ms: f64,
    pub can_prune_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub extract_ms: f64,
    pub can_prune: Option<bool>,
    pub strategy: String,
    pub binary_probe_count: usize,
    pub binary_entry_read_ms: f64,
    pub binary_blob_read_ms: f64,
    pub binary_metadata_ms: f64,
    pub binary_entry_bytes_read: usize,
    pub binary_blob_bytes_read: usize,
    pub binary_entries_file_bytes: u64,
    pub binary_blob_file_bytes: u64,
    pub result_array_len: usize,
}

fn native_term_preview(term: &str) -> String {
    const MAX_CHARS: usize = 160;
    let mut out = String::new();
    for (idx, ch) in term.chars().enumerate() {
        if idx >= MAX_CHARS {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
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
    pub total_splits: Option<usize>,
    pub scan_batches: usize,
    pub max_scan_batch_rows: usize,
    pub scan_rows_materialized: usize,
    pub subject_range_index_used: bool,
    pub subject_range_lookup_ms: f64,
    pub subject_range_start: Option<u64>,
    pub subject_range_end: Option<u64>,
    pub subject_range_rows: Option<u64>,
    pub po_rowgroup_index_used: bool,
    pub po_rowgroup_lookup_ms: f64,
    pub po_candidate_ranges: usize,
    pub po_candidate_rows: u64,
    pub access_index_strategy: String,
    pub access_index_lookup_ms: f64,
    pub access_candidate_ranges: usize,
    pub access_candidate_rows: u64,
    pub access_execution_strategy: String,
    pub access_original_range_count: usize,
    pub access_executed_scan_count: usize,
    pub access_selected_rows: u64,
    pub id_to_term_stats: NativeIdToTermLookupStats,
    pub term_to_id_stats: Vec<NativeTermToIdLookupStats>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeIdsCountMode {
    NativeFilter,
    ManualEq,
    ExecuteOnly,
    RowsOnly,
}

#[derive(Clone, Debug, Serialize)]
pub struct CottasNativeIdsCountTimings {
    pub term_lookup_ms: f64,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub stream_init_ms: f64,
    pub consume_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct CottasNativeIdsCountDiagnostics {
    pub mode: NativeIdsCountMode,
    pub count: usize,
    pub timings: CottasNativeIdsCountTimings,

    pub filter_empty: bool,
    pub projected_columns: Vec<String>,
    pub bound_column: Option<String>,
    pub bound_id: Option<u32>,

    pub batches: usize,
    pub max_batch_rows: usize,
    pub decoded_values: usize,
}

#[derive(Clone, Debug, Default)]
struct LazyRdfWriteStats {
    id_extract_ms: f64,
    id_to_term_lookup_ms: f64,
    serialize_ms: f64,
    rows_out: usize,
    unique_ids_requested: usize,
    unique_ids_loaded: usize,
    id_to_term_stats: NativeIdToTermLookupStats,
}

#[derive(Clone, Debug, Default)]
struct BoundNativeRdfTerms {
    s: Option<String>,
    p: Option<String>,
    o: Option<String>,
    g: Option<String>,
}

impl BoundNativeRdfTerms {
    fn from_pattern(
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Self {
        Self {
            s: subject.map(|v| v.to_string()),
            p: predicate.map(|v| v.to_string()),
            o: object.map(|v| v.to_string()),
            g: graph.map(|v| v.to_string()),
        }
    }
}

fn collect_unique_ids_for_unbound_native_columns(
    s_ids: &[u32],
    p_ids: &[u32],
    o_ids: &[u32],
    g_ids: &[u32],
    bound_terms: &BoundNativeRdfTerms,
) -> Vec<u32> {
    let mut set = HashSet::new();

    if bound_terms.s.is_none() {
        for id in s_ids {
            set.insert(*id);
        }
    }

    if bound_terms.p.is_none() {
        for id in p_ids {
            set.insert(*id);
        }
    }

    if bound_terms.o.is_none() {
        for id in o_ids {
            set.insert(*id);
        }
    }

    if bound_terms.g.is_none() {
        for id in g_ids {
            set.insert(*id);
        }
    }

    let mut ids: Vec<u32> = set.into_iter().collect();
    ids.sort_unstable();
    ids
}

#[derive(Debug)]
struct NativeMatchPlanResult {
    rows: NativeProjectedIdRows,
    bound_terms: BoundNativeRdfTerms,
    diagnostics: CottasNativeIdsDiagnostics,
}

#[derive(Clone, Debug, Default)]
struct NativeProjectedIdRows {
    s: Option<Vec<u32>>,
    p: Option<Vec<u32>>,
    o: Option<Vec<u32>>,
    g: Option<Vec<u32>>,
    rows: usize,
}

fn native_projection_columns_for_bound_terms(
    bound_terms: &BoundNativeRdfTerms,
) -> Vec<&'static str> {
    let mut columns = Vec::new();

    if bound_terms.s.is_none() {
        columns.push("s");
    }
    if bound_terms.p.is_none() {
        columns.push("p");
    }
    if bound_terms.o.is_none() {
        columns.push("o");
    }
    if bound_terms.g.is_none() {
        columns.push("g");
    }

    // Vortex projection cannot be empty in this path because we still need row counts
    // from the filtered stream for fully-bound quad patterns.
    if columns.is_empty() {
        columns.push("s");
    }

    columns
}

fn append_optional_projected_u32_column(
    batch: &ArrayRef,
    column_name: &str,
    target: &mut Option<Vec<u32>>,
) -> Result<()> {
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let struct_array = batch
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let field = match struct_array.unmasked_field_by_name(column_name) {
        Ok(field) => field.clone(),
        Err(_) => return Ok(()),
    };

    let col = field
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let values = col.as_slice::<u32>();

    let out = target.get_or_insert_with(Vec::new);
    out.extend_from_slice(values);

    Ok(())
}

fn exact_ranges_to_row_indices(ranges: &[Range<u64>], expected_rows: u64) -> Result<Buffer<u64>> {
    let capacity = usize::try_from(expected_rows).map_err(|_| {
        VortexRdfError::InvalidOperation(format!(
            "selected row count {expected_rows} does not fit in usize"
        ))
    })?;
    let mut indices = Vec::with_capacity(capacity);
    let mut previous_end = None;
    let mut actual_rows = 0u64;

    for range in ranges {
        if range.start >= range.end {
            return Err(VortexRdfError::Deserialization(format!(
                "selected row range is empty or reversed: {}..{}",
                range.start, range.end
            )));
        }
        if let Some(end) = previous_end {
            if range.start < end {
                return Err(VortexRdfError::Deserialization(format!(
                    "selected row ranges overlap or are not sorted: previous_end={}, next={}..{}",
                    end, range.start, range.end
                )));
            }
        }
        actual_rows = actual_rows
            .checked_add(range.end - range.start)
            .ok_or_else(|| {
                VortexRdfError::Deserialization(
                    "selected row count overflow while expanding ranges".into(),
                )
            })?;
        indices.extend(range.clone());
        previous_end = Some(range.end);
    }

    if actual_rows != expected_rows || indices.len() != capacity {
        return Err(VortexRdfError::Deserialization(format!(
            "selected row count mismatch: expected={}, ranges={}, expanded={}",
            expected_rows,
            actual_rows,
            indices.len()
        )));
    }
    Ok(Buffer::from(indices))
}

async fn read_native_projected_stream_all_with_scan_stats<S>(
    stream: S,
) -> Result<(NativeProjectedIdRows, usize, usize)>
where
    S: Stream<Item = VortexResult<ArrayRef>>,
{
    let mut stream = Box::pin(stream);

    let mut rows = NativeProjectedIdRows::default();
    let mut batches = 0usize;
    let mut max_batch_rows = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let batch_rows = batch.len();

        batches += 1;
        max_batch_rows = max_batch_rows.max(batch_rows);
        rows.rows += batch_rows;

        if batch_rows == 0 {
            continue;
        }

        append_optional_projected_u32_column(&batch, "s", &mut rows.s)?;
        append_optional_projected_u32_column(&batch, "p", &mut rows.p)?;
        append_optional_projected_u32_column(&batch, "o", &mut rows.o)?;
        append_optional_projected_u32_column(&batch, "g", &mut rows.g)?;
    }

    Ok((rows, batches, max_batch_rows))
}

fn collect_unique_ids_from_projected_unbound_rows(
    rows: &NativeProjectedIdRows,
    bound_terms: &BoundNativeRdfTerms,
) -> Vec<u32> {
    let empty: &[u32] = &[];

    collect_unique_ids_for_unbound_native_columns(
        rows.s.as_deref().unwrap_or(empty),
        rows.p.as_deref().unwrap_or(empty),
        rows.o.as_deref().unwrap_or(empty),
        rows.g.as_deref().unwrap_or(empty),
        bound_terms,
    )
}

fn projected_id_at<'a>(
    values: &'a Option<Vec<u32>>,
    bound: &Option<String>,
    row_idx: usize,
    column_label: &str,
) -> Result<Option<u32>> {
    if bound.is_some() {
        return Ok(None);
    }

    let values = values.as_ref().ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} column was required for unbound output but was not projected",
            column_label
        ))
    })?;

    values.get(row_idx).copied().map(Some).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} projected column has no value at row {}",
            column_label, row_idx
        ))
    })
}

fn lookup_projected_or_use_bound<'a>(
    id_to_term: &'a HashMap<u32, String>,
    bound: &'a Option<String>,
    projected_id: Option<u32>,
    column_label: &str,
) -> Result<&'a str> {
    if let Some(value) = bound {
        return Ok(value.as_str());
    }

    let id = projected_id.ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} projected ID missing for unbound column",
            column_label
        ))
    })?;

    id_to_term.get(&id).map(|s| s.as_str()).ok_or_else(|| {
        VortexRdfError::Deserialization(format!(
            "{} ID {} missing from id_to_term sidecar",
            column_label, id
        ))
    })
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeIdToTermLookupStats {
    pub strategy: String,

    pub total_ms: f64,
    pub open_files_ms: f64,
    pub metadata_ms: f64,
    pub sort_dedup_ms: f64,
    pub offset_read_ms: f64,
    pub blob_read_ms: f64,
    pub utf8_decode_ms: f64,
    pub hashmap_insert_ms: f64,

    pub requested_ids_in: usize,
    pub requested_ids_unique: usize,
    pub ids_loaded: usize,

    pub offset_reads: usize,
    pub offset_bytes_read: usize,
    pub blob_reads: usize,
    pub blob_bytes_read: usize,

    pub offsets_file_bytes: u64,
    pub blob_file_bytes: u64,
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

async fn projected_native_id_rows_as_triples(
    data_path: &Path,
    rows: &NativeProjectedIdRows,
    bound_terms: &BoundNativeRdfTerms,
) -> Result<Vec<(String, String, String)>> {
    if rows.rows == 0 {
        return Ok(Vec::new());
    }

    let unique_ids = collect_unique_ids_from_projected_unbound_rows(rows, bound_terms);
    let id_to_term = lookup_terms_by_ids_from_sidecar(data_path, &unique_ids).await?;
    let mut triples = Vec::with_capacity(rows.rows);

    for row_idx in 0..rows.rows {
        let s_id = projected_id_at(&rows.s, &bound_terms.s, row_idx, "S")?;
        let p_id = projected_id_at(&rows.p, &bound_terms.p, row_idx, "P")?;
        let o_id = projected_id_at(&rows.o, &bound_terms.o, row_idx, "O")?;

        let subject = lookup_projected_or_use_bound(&id_to_term, &bound_terms.s, s_id, "S")?;
        let predicate = lookup_projected_or_use_bound(&id_to_term, &bound_terms.p, p_id, "P")?;
        let object = lookup_projected_or_use_bound(&id_to_term, &bound_terms.o, o_id, "O")?;

        triples.push((subject.to_owned(), predicate.to_owned(), object.to_owned()));
    }

    Ok(triples)
}

async fn write_projected_native_id_rows_as_rdf_lazy<W>(
    data_path: &Path,
    rows: NativeProjectedIdRows,
    bound_terms: &BoundNativeRdfTerms,
    writer: W,
    format: RdfFormat,
) -> Result<LazyRdfWriteStats>
where
    W: Write,
{
    let write_start = Instant::now();

    let id_extract_start = Instant::now();
    let unique_ids = collect_unique_ids_from_projected_unbound_rows(&rows, bound_terms);
    let id_extract_ms = elapsed_ms(id_extract_start);

    let id_lookup_start = Instant::now();
    let (id_to_term, id_to_term_stats) =
        lookup_terms_by_ids_from_sidecar_with_stats(data_path, &unique_ids).await?;
    let id_to_term_lookup_ms = elapsed_ms(id_lookup_start);

    let serialize_start = Instant::now();
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    for i in 0..rows.rows {
        let s_id = projected_id_at(&rows.s, &bound_terms.s, i, "S")?;
        let p_id = projected_id_at(&rows.p, &bound_terms.p, i, "P")?;
        let o_id = projected_id_at(&rows.o, &bound_terms.o, i, "O")?;
        let g_id = projected_id_at(&rows.g, &bound_terms.g, i, "G")?;

        let s_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.s, s_id, "S")?;
        let p_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.p, p_id, "P")?;
        let o_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.o, o_id, "O")?;
        let g_raw = lookup_projected_or_use_bound(&id_to_term, &bound_terms.g, g_id, "G")?;

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
        "[cottas_native_ids::write_projected_native_id_rows_as_rdf_lazy] wrote {} rows using {} unique dictionary ids in {:?}",
        rows.rows,
        unique_ids.len(),
        write_start.elapsed()
    );

    Ok(LazyRdfWriteStats {
        id_extract_ms,
        id_to_term_lookup_ms,
        serialize_ms,
        rows_out: rows.rows,
        unique_ids_requested: unique_ids.len(),
        unique_ids_loaded: id_to_term.len(),
        id_to_term_stats,
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

async fn lookup_terms_by_ids_from_sidecar(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    let provider = BinaryNativeProviders::new(data_path);
    let (terms, _stats) = provider.lookup_terms_by_ids(ids).await?;
    Ok(terms)
}

async fn lookup_terms_by_ids_from_sidecar_with_stats(
    data_path: &Path,
    ids: &[u32],
) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
    let total_start = Instant::now();
    if ids.is_empty() {
        let mut stats = NativeIdToTermLookupStats::default();
        stats.strategy = "empty".to_string();
        stats.total_ms = elapsed_ms(total_start);
        return Ok((HashMap::new(), stats));
    }
    if !native_id_to_term_binary_sidecar_exists(data_path) {
        return Err(VortexRdfError::Deserialization(format!(
            "Missing required binary id_to_term sidecars for {:?}",
            data_path
        )));
    }
    lookup_terms_by_ids_from_binary_index_random_read_with_stats(data_path, ids)
}

fn read_exact_at_native_sidecar(
    file: &std::fs::File,
    buf: &mut [u8],
    mut offset: u64,
    label: &str,
) -> Result<()> {
    let mut read_total = 0usize;
    while read_total < buf.len() {
        let n = file.read_at(&mut buf[read_total..], offset).map_err(|e| {
            VortexRdfError::Deserialization(format!(
                "Failed to read {} at offset {} after {} bytes: {}",
                label, offset, read_total, e
            ))
        })?;
        if n == 0 {
            return Err(VortexRdfError::Deserialization(format!(
                "Unexpected EOF while reading {} at offset {} after {} / {} bytes",
                label,
                offset,
                read_total,
                buf.len()
            )));
        }
        read_total += n;
        offset += n as u64;
    }
    Ok(())
}

fn lookup_terms_by_ids_from_binary_index_random_read_with_stats(
    data_path: &Path,
    ids: &[u32],
) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
    let lookup_start = Instant::now();

    let mut stats = NativeIdToTermLookupStats::default();
    stats.strategy = "binary-random-read".to_string();
    stats.requested_ids_in = ids.len();

    if ids.is_empty() {
        stats.total_ms = elapsed_ms(lookup_start);
        return Ok((HashMap::new(), stats));
    }

    let offsets_path = native_dict_id_to_term_offsets_path(data_path);
    let blob_path = native_dict_id_to_term_blob_path(data_path);

    let open_start = Instant::now();

    let offsets_file = std::fs::File::open(&offsets_path).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Failed to open native id_to_term offsets sidecar {:?}: {}",
            offsets_path, e
        ))
    })?;

    let blob_file = std::fs::File::open(&blob_path).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Failed to open native id_to_term blob sidecar {:?}: {}",
            blob_path, e
        ))
    })?;

    stats.open_files_ms = elapsed_ms(open_start);

    let metadata_start = Instant::now();

    let offsets_len = offsets_file
        .metadata()
        .map_err(|e| {
            VortexRdfError::Deserialization(format!(
                "Failed to stat native id_to_term offsets sidecar {:?}: {}",
                offsets_path, e
            ))
        })?
        .len();

    let blob_len = blob_file
        .metadata()
        .map_err(|e| {
            VortexRdfError::Deserialization(format!(
                "Failed to stat native id_to_term blob sidecar {:?}: {}",
                blob_path, e
            ))
        })?
        .len();

    stats.metadata_ms = elapsed_ms(metadata_start);
    stats.offsets_file_bytes = offsets_len;
    stats.blob_file_bytes = blob_len;

    if offsets_len < 16 || offsets_len % 8 != 0 {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed native id_to_term offsets sidecar {:?}: byte length {} must be >= 16 and divisible by 8",
            offsets_path, offsets_len
        )));
    }

    let sort_start = Instant::now();

    let mut requested: Vec<u32> = ids.to_vec();
    requested.sort_unstable();
    requested.dedup();

    stats.sort_dedup_ms = elapsed_ms(sort_start);
    stats.requested_ids_unique = requested.len();

    let mut out = HashMap::with_capacity(requested.len());

    for id in requested.iter().copied() {
        let offset_pos = (id as u64).checked_mul(8).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "Offset position overflow for dictionary ID {}",
                id
            ))
        })?;

        if offset_pos + 16 > offsets_len {
            return Err(VortexRdfError::Deserialization(format!(
                "ID {} is outside native id_to_term offsets range: need bytes {}..{}, offsets_len={}",
                id,
                offset_pos,
                offset_pos + 16,
                offsets_len
            )));
        }

        let mut offset_buf = [0u8; 16];

        let offset_read_start = Instant::now();

        read_exact_at_native_sidecar(
            &offsets_file,
            &mut offset_buf,
            offset_pos,
            "id_to_term offsets",
        )?;

        stats.offset_read_ms += elapsed_ms(offset_read_start);
        stats.offset_reads += 1;
        stats.offset_bytes_read += offset_buf.len();

        let start = u64::from_le_bytes(offset_buf[0..8].try_into().unwrap());
        let end = u64::from_le_bytes(offset_buf[8..16].try_into().unwrap());

        if start > end || end > blob_len {
            return Err(VortexRdfError::Deserialization(format!(
                "ID {} has invalid native id_to_term blob byte range {}..{} for blob_len={}",
                id, start, end, blob_len
            )));
        }

        let term_len = (end - start) as usize;
        let mut term_buf = vec![0u8; term_len];

        if term_len > 0 {
            let blob_read_start = Instant::now();

            read_exact_at_native_sidecar(&blob_file, &mut term_buf, start, "id_to_term blob")?;

            stats.blob_read_ms += elapsed_ms(blob_read_start);
            stats.blob_reads += 1;
            stats.blob_bytes_read += term_len;
        }

        let utf8_start = Instant::now();

        let term = String::from_utf8(term_buf).map_err(|e| {
            VortexRdfError::Deserialization(format!(
                "Dictionary term for ID {} is not valid UTF-8 in binary sidecar byte range {}..{}: {}",
                id, start, end, e
            ))
        })?;

        stats.utf8_decode_ms += elapsed_ms(utf8_start);

        let insert_start = Instant::now();
        out.insert(id, term);
        stats.hashmap_insert_ms += elapsed_ms(insert_start);
    }

    stats.ids_loaded = out.len();
    stats.total_ms = elapsed_ms(lookup_start);

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] binary random-read resolved {} / {} ids in {:.3}ms; open_files_ms={:.3}, metadata_ms={:.3}, sort_dedup_ms={:.3}, offset_read_ms={:.3}, blob_read_ms={:.3}, utf8_decode_ms={:.3}, hashmap_insert_ms={:.3}, offset_reads={}, offset_bytes={}, blob_reads={}, blob_bytes={}, offsets_file_bytes={}, blob_file_bytes={}",
        out.len(),
        requested.len(),
        stats.total_ms,
        stats.open_files_ms,
        stats.metadata_ms,
        stats.sort_dedup_ms,
        stats.offset_read_ms,
        stats.blob_read_ms,
        stats.utf8_decode_ms,
        stats.hashmap_insert_ms,
        stats.offset_reads,
        stats.offset_bytes_read,
        stats.blob_reads,
        stats.blob_bytes_read,
        stats.offsets_file_bytes,
        stats.blob_file_bytes,
    );

    Ok((out, stats))
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

async fn execute_cottas_native_match(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeMatchPlanResult> {
    let total_start = Instant::now();
    let mut diagnostics = CottasNativeIdsDiagnostics::default();
    let bound_terms = BoundNativeRdfTerms::from_pattern(subject, predicate, object, graph);
    let providers = BinaryNativeProviders::new(input_path);
    let (resolved, term_lookup_ms, term_to_id_stats) =
        resolve_native_pattern(&providers, subject, predicate, object, graph).await?;
    diagnostics.term_lookup_ms = term_lookup_ms;
    diagnostics.term_to_id_stats = term_to_id_stats;
    let Some(resolved) = resolved else {
        diagnostics.total_ms = elapsed_ms(total_start);
        return Ok(NativeMatchPlanResult {
            rows: NativeProjectedIdRows::default(),
            bound_terms,
            diagnostics,
        });
    };
    let filter = resolved.filter();

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    diagnostics.open_ms = elapsed_ms(open_start);

    if let NativePatternFilter::Expr(expr) = &filter {
        diagnostics.vortex_can_prune = file.can_prune(expr).ok();
    }
    diagnostics.total_splits = file.splits().ok().map(|splits| splits.len());

    let projected_columns = native_projection_columns_for_bound_terms(&bound_terms);
    let access_plan = plan_native_access(
        &providers,
        resolved,
        subject.is_some(),
        predicate.is_some(),
        object.is_some(),
    )
    .await?;
    diagnostics.access_index_strategy = access_plan.strategy.clone();
    diagnostics.access_index_lookup_ms = access_plan.lookup_ms;
    diagnostics.access_candidate_ranges = access_plan.candidate_ranges;
    diagnostics.access_candidate_rows = access_plan.candidate_rows;
    diagnostics.po_rowgroup_index_used = access_plan.po_index_used;
    diagnostics.po_rowgroup_lookup_ms = if access_plan.po_index_used {
        access_plan.lookup_ms
    } else {
        0.0
    };
    diagnostics.po_candidate_ranges = if access_plan.po_index_used {
        access_plan.candidate_ranges
    } else {
        0
    };
    diagnostics.po_candidate_rows = if access_plan.po_index_used {
        access_plan.candidate_rows
    } else {
        0
    };
    if let Some(range) = &access_plan.subject_range {
        diagnostics.subject_range_index_used = true;
        diagnostics.subject_range_lookup_ms = access_plan.lookup_ms;
        diagnostics.subject_range_start = Some(range.start);
        diagnostics.subject_range_end = Some(range.end);
        diagnostics.subject_range_rows = Some(range.end.saturating_sub(range.start));
    }
    let selected_ranges = access_plan.ranges;
    let mut scan_build_ms = 0.0;
    let mut read_all_ms = 0.0;
    let mut matched_rows = NativeProjectedIdRows::default();
    let mut scan_batches = 0usize;
    let mut max_scan_batch_rows = 0usize;
    let mut execution_strategy = "full-scan";
    let mut original_range_count = 0usize;
    let mut executed_scan_count = 0usize;
    let mut selected_rows = 0u64;

    if let Some(ranges) = selected_ranges {
        original_range_count = ranges.len();
        selected_rows = range_rows(&ranges);
        if selected_rows != access_plan.candidate_rows {
            return Err(VortexRdfError::Deserialization(format!(
                "access-plan row mismatch: metadata={}, ranges={}",
                access_plan.candidate_rows, selected_rows
            )));
        }
        if ranges.is_empty() {
            execution_strategy = "empty-index-result";
        } else {
            let scan_build_start = Instant::now();
            let scan = file.scan().map_err(VortexRdfError::from)?;
            let scan = if ranges.len() == 1 {
                execution_strategy = "single-row-range";
                scan.with_row_range(ranges[0].clone())
            } else {
                execution_strategy = "include-by-index";
                scan.with_row_indices(exact_ranges_to_row_indices(
                    &ranges,
                    access_plan.candidate_rows,
                )?)
            };
            let scan = match &filter {
                NativePatternFilter::All => scan,
                NativePatternFilter::Empty => unreachable!("handled above"),
                NativePatternFilter::Expr(expr) => scan.with_filter(expr.clone()),
            };
            let stream = scan
                .with_projection(vortex_array::expr::select(
                    projected_columns.as_slice(),
                    vortex_array::expr::root(),
                ))
                .into_array_stream()
                .map_err(VortexRdfError::from)?;
            scan_build_ms += elapsed_ms(scan_build_start);
            executed_scan_count = 1;
            let read_start = Instant::now();
            let (rows, batches, max_rows) =
                read_native_projected_stream_all_with_scan_stats(stream).await?;
            read_all_ms += elapsed_ms(read_start);
            matched_rows = rows;
            scan_batches = batches;
            max_scan_batch_rows = max_rows;
        }
    } else {
        let scan_build_start = Instant::now();
        let scan = file.scan().map_err(VortexRdfError::from)?;
        let scan = match &filter {
            NativePatternFilter::All => scan,
            NativePatternFilter::Empty => unreachable!("handled above"),
            NativePatternFilter::Expr(expr) => scan.with_filter(expr.clone()),
        };
        let stream = scan
            .with_projection(vortex_array::expr::select(
                projected_columns.as_slice(),
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        scan_build_ms += elapsed_ms(scan_build_start);
        executed_scan_count = 1;
        let read_start = Instant::now();
        let (rows, batches, max_rows) =
            read_native_projected_stream_all_with_scan_stats(stream).await?;
        read_all_ms += elapsed_ms(read_start);
        matched_rows = rows;
        scan_batches = batches;
        max_scan_batch_rows = max_rows;
    }
    diagnostics.scan_build_ms = scan_build_ms;
    diagnostics.read_all_ms = read_all_ms;
    diagnostics.access_execution_strategy = execution_strategy.to_string();
    diagnostics.access_original_range_count = original_range_count;
    diagnostics.access_executed_scan_count = executed_scan_count;
    diagnostics.access_selected_rows = selected_rows;
    diagnostics.scan_batches = scan_batches;
    diagnostics.max_scan_batch_rows = max_scan_batch_rows;
    diagnostics.scan_rows_materialized = matched_rows.rows;
    diagnostics.rows_out = matched_rows.rows;
    diagnostics.total_ms = elapsed_ms(total_start);

    Ok(NativeMatchPlanResult {
        rows: matched_rows,
        bound_terms,
        diagnostics,
    })
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
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    let mut diagnostics = planned.diagnostics;

    if planned.rows.rows == 0 {
        let serialize_start = Instant::now();
        write_empty_rdf(writer, format).await?;
        diagnostics.serialize_ms = elapsed_ms(serialize_start);
        diagnostics.total_ms = elapsed_ms(total_start);
        return Ok(diagnostics);
    }

    let write_stats = write_projected_native_id_rows_as_rdf_lazy(
        input_path,
        planned.rows,
        &planned.bound_terms,
        writer,
        format,
    )
    .await?;

    diagnostics.id_extract_ms = write_stats.id_extract_ms;
    diagnostics.id_to_term_lookup_ms = write_stats.id_to_term_lookup_ms;
    diagnostics.serialize_ms = write_stats.serialize_ms;
    diagnostics.rows_out = write_stats.rows_out;
    diagnostics.unique_ids_requested = write_stats.unique_ids_requested;
    diagnostics.unique_ids_loaded = write_stats.unique_ids_loaded;
    diagnostics.id_to_term_stats = write_stats.id_to_term_stats;
    diagnostics.total_ms = elapsed_ms(total_start);
    Ok(diagnostics)
}

pub async fn count_cottas_native_ids_file_with_diagnostics_mode(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    mode: NativeIdsCountMode,
) -> Result<CottasNativeIdsCountDiagnostics> {
    let total_start = Instant::now();
    let term_lookup_ms: f64;
    let filter_empty: bool;
    let bound_column: Option<String>;
    let mut bound_id: Option<u32> = None;

    let projection_col: &'static str;
    let native_filter: NativePatternFilter;

    match mode {
        NativeIdsCountMode::NativeFilter => {
            let (filter, lookup_ms) = build_native_pattern_filter_lazy_with_stats(
                input_path, subject, predicate, object, graph,
            )
            .await?;
            term_lookup_ms = lookup_ms;
            filter_empty = matches!(filter, NativePatternFilter::Empty);
            projection_col = first_bound_native_id_column(subject, predicate, object, graph);
            bound_column = Some(projection_col.to_string());
            native_filter = filter;
        }

        NativeIdsCountMode::ManualEq
        | NativeIdsCountMode::ExecuteOnly
        | NativeIdsCountMode::RowsOnly => {
            let (col, id, lookup_ms, empty) =
                resolve_single_bound_id_for_count(input_path, subject, predicate, object, graph)
                    .await?;

            term_lookup_ms = lookup_ms;
            filter_empty = empty;
            projection_col = col.unwrap_or("s");
            bound_column = Some(projection_col.to_string());
            bound_id = id;
            native_filter = NativePatternFilter::All;
        }
    }

    let projected_columns = vec![projection_col.to_string()];

    if filter_empty {
        let timings = CottasNativeIdsCountTimings {
            term_lookup_ms,
            open_ms: 0.0,
            scan_build_ms: 0.0,
            stream_init_ms: 0.0,
            consume_ms: 0.0,
            total_ms: elapsed_ms(total_start),
        };

        return Ok(CottasNativeIdsCountDiagnostics {
            mode,
            count: 0,
            timings,
            filter_empty: true,
            projected_columns,
            bound_column,
            bound_id,
            batches: 0,
            max_batch_rows: 0,
            decoded_values: 0,
        });
    }

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);

    let scan_build_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;

    let scan = match mode {
        NativeIdsCountMode::NativeFilter => match native_filter {
            NativePatternFilter::All => scan,
            NativePatternFilter::Empty => unreachable!("handled above"),
            NativePatternFilter::Expr(expr) => scan.with_filter(expr),
        },
        NativeIdsCountMode::ManualEq
        | NativeIdsCountMode::ExecuteOnly
        | NativeIdsCountMode::RowsOnly => scan,
    };

    let scan = scan.with_projection(vortex_array::expr::select(
        [projection_col].as_slice(),
        vortex_array::expr::root(),
    ));

    let scan_build_ms = elapsed_ms(scan_build_start);

    let stream_init_start = Instant::now();
    let mut stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    let stream_init_ms = elapsed_ms(stream_init_start);

    let consume_start = Instant::now();

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();

    let mut count = 0usize;
    let mut batches = 0usize;
    let mut max_batch_rows = 0usize;
    let mut decoded_values = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let rows = batch.len();

        batches += 1;
        max_batch_rows = max_batch_rows.max(rows);

        match mode {
            NativeIdsCountMode::NativeFilter | NativeIdsCountMode::RowsOnly => {
                count += rows;
            }

            NativeIdsCountMode::ExecuteOnly => {
                let struct_array = batch
                    .clone()
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let _ids = struct_array
                    .unmasked_field_by_name(projection_col)
                    .map_err(VortexRdfError::Vortex)?
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                count += rows;
            }

            NativeIdsCountMode::ManualEq => {
                let Some(expected_id) = bound_id else {
                    count += rows;
                    continue;
                };

                let struct_array = batch
                    .clone()
                    .execute::<StructArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let id_array = struct_array
                    .unmasked_field_by_name(projection_col)
                    .map_err(VortexRdfError::Vortex)?
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)
                    .map_err(VortexRdfError::Vortex)?;

                let ids = id_array.as_slice::<u32>();
                decoded_values += ids.len();

                for id in ids {
                    if *id == expected_id {
                        count += 1;
                    }
                }
            }
        }
    }

    let consume_ms = elapsed_ms(consume_start);

    let timings = CottasNativeIdsCountTimings {
        term_lookup_ms,
        open_ms,
        scan_build_ms,
        stream_init_ms,
        consume_ms,
        total_ms: elapsed_ms(total_start),
    };

    Ok(CottasNativeIdsCountDiagnostics {
        mode,
        count,
        timings,
        filter_empty,
        projected_columns,
        bound_column,
        bound_id,
        batches,
        max_batch_rows,
        decoded_values,
    })
}

pub async fn count_cottas_native_ids_file_with_diagnostics(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<CottasNativeIdsCountDiagnostics> {
    count_cottas_native_ids_file_with_diagnostics_mode(
        input_path,
        subject,
        predicate,
        object,
        graph,
        NativeIdsCountMode::NativeFilter,
    )
    .await
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

fn native_dict_id_to_term_offsets_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");

    data_path.with_file_name(format!("{file_name}.dict.id_to_term.offsets.bin"))
}

fn native_dict_id_to_term_blob_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");

    data_path.with_file_name(format!("{file_name}.dict.id_to_term.blob"))
}

fn native_id_to_term_binary_sidecar_exists(data_path: &Path) -> bool {
    native_dict_id_to_term_offsets_path(data_path).is_file()
        && native_dict_id_to_term_blob_path(data_path).is_file()
}

fn native_subject_range_index_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.subject_ranges.bin"))
}

fn native_subject_range_index_exists(data_path: &Path) -> bool {
    native_subject_range_index_path(data_path).is_file()
}

fn native_subject_range_vortex_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.subject_ranges.v1.vortex"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeSubjectIndexBackend {
    Binary,
    Vortex,
}

fn native_subject_index_backend(data_path: &Path) -> Result<NativeSubjectIndexBackend> {
    let configured = std::env::var("VORTEX_RDF_NATIVE_SUBJECT_INDEX_BACKEND")
        .unwrap_or_else(|_| "auto".to_string());
    match configured.as_str() {
        "auto" => {
            if native_subject_range_vortex_path(data_path).is_file() {
                Ok(NativeSubjectIndexBackend::Vortex)
            } else {
                Ok(NativeSubjectIndexBackend::Binary)
            }
        }
        "binary" => Ok(NativeSubjectIndexBackend::Binary),
        "vortex" => {
            let path = native_subject_range_vortex_path(data_path);
            if !path.is_file() {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "Vortex subject index backend requested but {:?} does not exist",
                    path
                )));
            }
            Ok(NativeSubjectIndexBackend::Vortex)
        }
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_NATIVE_SUBJECT_INDEX_BACKEND={other:?}; expected auto, binary, or vortex"
        ))),
    }
}

async fn lookup_subject_range_from_vortex(
    data_path: &Path,
    subject_id: u32,
) -> Result<Option<Range<u64>>> {
    let path = native_subject_range_vortex_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(eq(col("subject_id"), lit(subject_id)))
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    if result.len() == 0 {
        return Ok(None);
    }
    if result.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex subject index {:?} returned {} rows for subject ID {}; expected at most one",
            path,
            result.len(),
            subject_id
        )));
    }
    let starts = extract_projected_u64_column(&result, "row_start")?;
    let ends = extract_projected_u64_column(&result, "row_end")?;
    let start = starts[0];
    let end = ends[0];
    if start > end {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex subject index {:?} contains invalid range {}..{} for subject ID {}",
            path, start, end, subject_id
        )));
    }
    Ok(Some(start..end))
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativePoRowGroupIndexBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub row_groups: usize,
    pub rows_scanned: u64,
    pub unique_po_hashes_written: u64,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}

fn native_po_hash(p: u32, o: u32) -> u64 {
    let mut x = ((p as u64) << 32) | (o as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn native_po_exact_ranges_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.po_exact_ranges.bin"))
}

fn native_po_exact_ranges_exists(data_path: &Path) -> bool {
    native_po_exact_ranges_path(data_path).is_file()
}

const NATIVE_PO_EXACT_RANGES_MAGIC: &[u8; 8] = b"VRDFPX1\0";
const NATIVE_PO_EXACT_RANGE_ENTRY_BYTES: u64 = 24;

fn write_exact_range_entry<W: Write>(
    writer: &mut W,
    hash: u64,
    start: u64,
    end: u64,
) -> Result<()> {
    writer
        .write_all(&hash.to_le_bytes())
        .and_then(|_| writer.write_all(&start.to_le_bytes()))
        .and_then(|_| writer.write_all(&end.to_le_bytes()))
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}

fn read_exact_range_entry_at(file: &std::fs::File, entry_idx: u64) -> Result<(u64, u64, u64)> {
    let mut buf = [0u8; 24];
    let offset = 16u64
        .checked_add(
            entry_idx
                .checked_mul(NATIVE_PO_EXACT_RANGE_ENTRY_BYTES)
                .ok_or_else(|| {
                    VortexRdfError::Deserialization(format!(
                        "PO exact range index offset overflow for entry {}",
                        entry_idx
                    ))
                })?,
        )
        .ok_or_else(|| {
            VortexRdfError::Deserialization("PO exact range index offset overflow".to_string())
        })?;
    read_exact_at_native_sidecar(file, &mut buf, offset, "PO exact range entry")?;
    let hash = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let start = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let end = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((hash, start, end))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativePoIndexBackend {
    Binary,
    VortexV2,
}

fn native_po_v2_exists(data_path: &Path) -> bool {
    native_po_exact_directory_v2_path(data_path).is_file()
        && native_po_exact_ranges_v2_path(data_path).is_file()
}

fn native_po_index_backend(data_path: &Path) -> Result<NativePoIndexBackend> {
    let configured =
        std::env::var("VORTEX_RDF_NATIVE_PO_INDEX_BACKEND").unwrap_or_else(|_| "auto".to_string());
    match configured.as_str() {
        "auto" if native_po_v2_exists(data_path) => Ok(NativePoIndexBackend::VortexV2),
        "auto" if native_po_exact_ranges_exists(data_path) => Ok(NativePoIndexBackend::Binary),
        "auto" => Err(VortexRdfError::InvalidOperation(format!(
            "No production PO index exists for {:?}",
            data_path
        ))),
        "binary" if native_po_exact_ranges_exists(data_path) => Ok(NativePoIndexBackend::Binary),
        "binary" => Err(VortexRdfError::InvalidOperation(format!(
            "Binary PO index {:?} does not exist",
            native_po_exact_ranges_path(data_path)
        ))),
        "vortex-v2" if native_po_v2_exists(data_path) => Ok(NativePoIndexBackend::VortexV2),
        "vortex-v2" => Err(VortexRdfError::InvalidOperation(format!(
            "Vortex PO v2 directory {:?} or payload {:?} is missing",
            native_po_exact_directory_v2_path(data_path),
            native_po_exact_ranges_v2_path(data_path)
        ))),
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_NATIVE_PO_INDEX_BACKEND={other:?}; expected auto, binary, or vortex-v2"
        ))),
    }
}

#[derive(Clone, Copy, Debug)]
struct NativePoDirectoryEntry {
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}

#[derive(Clone, Copy, Debug)]
struct NativePoPredicatePartition {
    predicate_id: u32,
    directory_start: u64,
    directory_end: u64,
}

static NATIVE_PO_PREDICATE_PARTITION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, Arc<[NativePoPredicatePartition]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn po_partition_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, Arc<[NativePoPredicatePartition]>>>> {
    NATIVE_PO_PREDICATE_PARTITION_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("PO predicate partition cache mutex was poisoned".into())
    })
}

async fn po_predicate_partitions(
    data_path: &Path,
) -> Result<Option<Arc<[NativePoPredicatePartition]>>> {
    let path = native_po_predicate_partitions_v2_path(data_path);
    if !path.is_file() {
        return Ok(None);
    }
    if let Some(cached) = po_partition_cache_lock()?.get(&path).cloned() {
        return Ok(Some(cached));
    }
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["predicate_id", "directory_start", "directory_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let ids = extract_projected_u32_column(&array, "predicate_id")?;
    let starts = extract_projected_u64_column(&array, "directory_start")?;
    let ends = extract_projected_u64_column(&array, "directory_end")?;
    if ids.len() != array.len() || starts.len() != array.len() || ends.len() != array.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} has inconsistent column lengths",
            path
        )));
    }
    let mut entries = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        if starts[index] >= ends[index] {
            return Err(VortexRdfError::Deserialization(format!(
                "PO predicate partition {:?} has invalid range {}..{} for predicate {}",
                path, starts[index], ends[index], ids[index]
            )));
        }
        entries.push(NativePoPredicatePartition {
            predicate_id: ids[index],
            directory_start: starts[index],
            directory_end: ends[index],
        });
    }
    if entries.windows(2).any(|pair| {
        pair[0].predicate_id >= pair[1].predicate_id
            || pair[0].directory_end != pair[1].directory_start
    }) {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} is not strictly sorted and contiguous",
            path
        )));
    }
    if entries.first().map(|entry| entry.directory_start) != Some(0) {
        return Err(VortexRdfError::Deserialization(format!(
            "PO predicate partition {:?} does not start at directory row zero",
            path
        )));
    }
    let entries: Arc<[NativePoPredicatePartition]> = entries.into();
    let mut cache = po_partition_cache_lock()?;
    Ok(Some(
        cache
            .entry(path)
            .or_insert_with(|| Arc::clone(&entries))
            .clone(),
    ))
}

async fn po_predicate_partition(
    data_path: &Path,
    predicate_id: u32,
) -> Result<Option<NativePoPredicatePartition>> {
    let Some(partitions) = po_predicate_partitions(data_path).await? else {
        return Ok(None);
    };
    Ok(partitions
        .binary_search_by_key(&predicate_id, |entry| entry.predicate_id)
        .ok()
        .map(|index| partitions[index]))
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativePoPredicatePartitionBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub directory_rows: u64,
    pub predicates: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}

fn build_po_partition_array(ids: Vec<u32>, starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("predicate_id", PrimitiveArray::from_iter(ids).into_array()),
        (
            "directory_start",
            PrimitiveArray::from_iter(starts).into_array(),
        ),
        (
            "directory_end",
            PrimitiveArray::from_iter(ends).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

pub async fn build_cottas_native_po_predicate_partitions_v2(
    data_path: &Path,
) -> Result<NativePoPredicatePartitionBuildStats> {
    let total_start = Instant::now();
    let directory_path = native_po_exact_directory_v2_path(data_path);
    if !directory_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Cannot build PO predicate partitions: directory {:?} does not exist",
            directory_path
        )));
    }
    let output_path = native_po_predicate_partitions_v2_path(data_path);
    let temporary_path = output_path.with_extension("vortex.tmp");
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&directory_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["predicate_id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let mut ids = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut current = None;
    let mut current_start = 0u64;
    let mut directory_rows = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "predicate_id")?;
        if values.len() != batch.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "PO directory predicate projection returned {} values for {} rows",
                values.len(),
                batch.len()
            )));
        }
        for predicate_id in values {
            match current {
                None => {
                    current = Some(predicate_id);
                    current_start = directory_rows;
                }
                Some(previous) if previous != predicate_id => {
                    if predicate_id <= previous {
                        return Err(VortexRdfError::Deserialization(format!(
                            "PO directory is not sorted by predicate_id: {} followed by {}",
                            previous, predicate_id
                        )));
                    }
                    ids.push(previous);
                    starts.push(current_start);
                    ends.push(directory_rows);
                    current = Some(predicate_id);
                    current_start = directory_rows;
                }
                Some(_) => {}
            }
            directory_rows += 1;
        }
    }
    if let Some(predicate_id) = current {
        ids.push(predicate_id);
        starts.push(current_start);
        ends.push(directory_rows);
    }
    let scan_ms = elapsed_ms(scan_start);
    let predicates = ids.len();
    let array = build_po_partition_array(ids, starts, ends)?;
    let dtype = build_po_partition_array(Vec::new(), Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let output_stream = ArrayStreamAdapter::new(dtype, futures::stream::iter(vec![Ok(array)]));
    let write_start = Instant::now();
    let mut output_file = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(65_536)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output_file, output_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output_file);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    po_partition_cache_lock()?.remove(&output_path);
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote PO predicate partitions {:?}: directory_rows={}, predicates={}, total_ms={:.3}",
        output_path,
        directory_rows,
        predicates,
        total_ms
    );
    Ok(NativePoPredicatePartitionBuildStats {
        input_path: directory_path.display().to_string(),
        output_path: output_path.display().to_string(),
        directory_rows,
        predicates,
        open_ms,
        scan_ms,
        write_ms,
        total_ms,
    })
}

async fn lookup_po_directory_entry_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
    object_id: u32,
) -> Result<Option<NativePoDirectoryEntry>> {
    let path = native_po_exact_directory_v2_path(data_path);
    let partition = po_predicate_partition(data_path, predicate_id).await?;
    if native_po_predicate_partitions_v2_path(data_path).is_file() && partition.is_none() {
        return Ok(None);
    }
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let scan = match partition {
        Some(entry) => scan.with_row_range(entry.directory_start..entry.directory_end),
        None => scan,
    };
    let result = scan
        .with_filter(and(
            eq(col("predicate_id"), lit(predicate_id)),
            eq(col("object_id"), lit(object_id)),
        ))
        .with_projection(vortex_array::expr::select(
            ["range_offset", "range_count", "candidate_rows"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if result.len() == 0 {
        return Ok(None);
    }
    if result.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 directory {:?} returned {} rows for ({}, {}); expected one",
            path,
            result.len(),
            predicate_id,
            object_id
        )));
    }
    let offsets = extract_projected_u64_column(&result, "range_offset")?;
    let counts = extract_projected_u32_column(&result, "range_count")?;
    let rows = extract_projected_u64_column(&result, "candidate_rows")?;
    if offsets.len() != 1 || counts.len() != 1 || rows.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 directory {:?} returned inconsistent metadata columns",
            path
        )));
    }
    Ok(Some(NativePoDirectoryEntry {
        range_offset: offsets[0],
        range_count: counts[0],
        candidate_rows: rows[0],
    }))
}

async fn read_po_v2_payload(
    data_path: &Path,
    entry: NativePoDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| VortexRdfError::Deserialization("PO v2 payload slice overflow".into()))?;
    let path = native_po_exact_ranges_v2_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if payload.len() != entry.range_count as usize {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 payload {:?} returned {} rows for expected slice {}..{}",
            path,
            payload.len(),
            entry.range_offset,
            range_end
        )));
    }
    let starts = extract_projected_u64_column(&payload, "row_start")?;
    let ends = extract_projected_u64_column(&payload, "row_end")?;
    if starts.len() != payload.len() || ends.len() != payload.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 payload {:?} returned inconsistent range columns",
            path
        )));
    }
    let mut ranges = Vec::with_capacity(payload.len());
    for (start, end) in starts.into_iter().zip(ends) {
        if start >= end {
            return Err(VortexRdfError::Deserialization(format!(
                "PO v2 payload {:?} contains invalid range {}..{}",
                path, start, end
            )));
        }
        ranges.push(start..end);
    }
    let payload_rows = range_rows(&ranges);
    if payload_rows != entry.candidate_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "PO v2 candidate row mismatch: directory={}, payload={}",
            entry.candidate_rows, payload_rows
        )));
    }
    Ok(ranges)
}

async fn lookup_po_access_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
    object_id: u32,
) -> Result<Option<NativePoAccess>> {
    let Some(entry) =
        lookup_po_directory_entry_from_vortex_v2(data_path, predicate_id, object_id).await?
    else {
        return Ok(Some(NativePoAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "po-exact-ranges-vortex-v2",
        }));
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = po_exact_access_accepted(candidate_ranges, entry.candidate_rows);
    let ranges = if accepted {
        Some(read_po_v2_payload(data_path, entry).await?)
    } else {
        None
    };
    Ok(Some(NativePoAccess {
        ranges,
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "po-exact-ranges-vortex-v2",
    }))
}

fn po_exact_access_accepted(candidate_ranges: usize, candidate_rows: u64) -> bool {
    candidate_ranges <= po_exact_max_ranges() && candidate_rows <= po_exact_max_rows()
}

fn po_exact_max_ranges() -> usize {
    std::env::var("VORTEX_RDF_PO_EXACT_MAX_RANGES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
}

fn po_exact_max_rows() -> u64 {
    std::env::var("VORTEX_RDF_PO_EXACT_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000)
}

const NATIVE_P_EXACT_RANGES_MAGIC: &[u8; 8] = b"VRDFPR1\0";

fn native_p_exact_ranges_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.p_exact_ranges.bin"))
}

fn native_p_exact_ranges_exists(data_path: &Path) -> bool {
    native_p_exact_ranges_path(data_path).is_file()
}

fn native_p_exact_ranges_vortex_path(data_path: &Path) -> PathBuf {
    let file_name = data_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{file_name}.p_exact_ranges.v1.vortex"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativePredicateIndexBackend {
    Binary,
    VortexV1,
    VortexV2,
}

fn native_predicate_index_backend(data_path: &Path) -> Result<NativePredicateIndexBackend> {
    let configured = std::env::var("VORTEX_RDF_NATIVE_PREDICATE_INDEX_BACKEND")
        .unwrap_or_else(|_| "auto".to_string());
    let v2_exists = native_p_exact_directory_v2_path(data_path).is_file()
        && native_p_exact_ranges_v2_path(data_path).is_file();
    match configured.as_str() {
        "auto" => {
            if v2_exists {
                Ok(NativePredicateIndexBackend::VortexV2)
            } else if native_p_exact_ranges_exists(data_path) {
                Ok(NativePredicateIndexBackend::Binary)
            } else {
                Err(VortexRdfError::InvalidOperation(format!(
                    "No production predicate index exists for {:?}",
                    data_path
                )))
            }
        }
        "binary" => {
            let path = native_p_exact_ranges_path(data_path);
            if !path.is_file() {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "Binary predicate index backend requested but {:?} does not exist",
                    path
                )));
            }
            Ok(NativePredicateIndexBackend::Binary)
        }
        "vortex" | "vortex-v1" => {
            let path = native_p_exact_ranges_vortex_path(data_path);
            if !path.is_file() {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "Vortex predicate v1 backend requested but {:?} does not exist",
                    path
                )));
            }
            Ok(NativePredicateIndexBackend::VortexV1)
        }
        "vortex-v2" => {
            if !v2_exists {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "Vortex predicate v2 backend requested but directory {:?} or payload {:?} is missing",
                    native_p_exact_directory_v2_path(data_path),
                    native_p_exact_ranges_v2_path(data_path)
                )));
            }
            Ok(NativePredicateIndexBackend::VortexV2)
        }
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_NATIVE_PREDICATE_INDEX_BACKEND={other:?}; expected auto, binary, vortex-v1, or vortex-v2"
        ))),
    }
}

async fn lookup_predicate_ranges_from_vortex(
    data_path: &Path,
    predicate_id: u32,
) -> Result<Vec<Range<u64>>> {
    let path = native_p_exact_ranges_vortex_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(eq(col("predicate_id"), lit(predicate_id)))
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    if result.len() == 0 {
        return Ok(Vec::new());
    }
    let starts = extract_projected_u64_column(&result, "row_start")?;
    let ends = extract_projected_u64_column(&result, "row_end")?;
    if starts.len() != ends.len() || starts.len() != result.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "Vortex predicate index {:?} returned inconsistent columns: rows={}, starts={}, ends={}",
            path,
            result.len(),
            starts.len(),
            ends.len()
        )));
    }
    let mut ranges = Vec::with_capacity(starts.len());
    for (start, end) in starts.into_iter().zip(ends) {
        if start > end {
            return Err(VortexRdfError::Deserialization(format!(
                "Vortex predicate index {:?} contains invalid range {}..{} for predicate ID {}",
                path, start, end, predicate_id
            )));
        }
        ranges.push(start..end);
    }
    ranges.sort_unstable_by_key(|r| (r.start, r.end));
    Ok(ranges)
}

#[derive(Clone, Copy, Debug)]
struct NativePredicateDirectoryEntry {
    predicate_id: u32,
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}

static NATIVE_PREDICATE_V2_DIRECTORY_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, Arc<[NativePredicateDirectoryEntry]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn predicate_v2_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, Arc<[NativePredicateDirectoryEntry]>>>> {
    NATIVE_PREDICATE_V2_DIRECTORY_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("predicate v2 directory cache mutex was poisoned".into())
    })
}

async fn predicate_v2_directory(data_path: &Path) -> Result<Arc<[NativePredicateDirectoryEntry]>> {
    let path = native_p_exact_directory_v2_path(data_path);
    if let Some(cached) = predicate_v2_cache_lock()?.get(&path).cloned() {
        return Ok(cached);
    }

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            [
                "predicate_id",
                "range_offset",
                "range_count",
                "candidate_rows",
            ],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;

    let ids = extract_projected_u32_column(&array, "predicate_id")?;
    let offsets = extract_projected_u64_column(&array, "range_offset")?;
    let counts = extract_projected_u32_column(&array, "range_count")?;
    let rows = extract_projected_u64_column(&array, "candidate_rows")?;
    let len = array.len();
    if [ids.len(), offsets.len(), counts.len(), rows.len()]
        .into_iter()
        .any(|column_len| column_len != len)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 directory {:?} has inconsistent column lengths",
            path
        )));
    }

    let mut entries = Vec::with_capacity(len);
    for index in 0..len {
        entries.push(NativePredicateDirectoryEntry {
            predicate_id: ids[index],
            range_offset: offsets[index],
            range_count: counts[index],
            candidate_rows: rows[index],
        });
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].predicate_id >= pair[1].predicate_id)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 directory {:?} is not strictly sorted by predicate_id",
            path
        )));
    }

    let entries: Arc<[NativePredicateDirectoryEntry]> = entries.into();
    let mut cache = predicate_v2_cache_lock()?;
    Ok(cache
        .entry(path)
        .or_insert_with(|| Arc::clone(&entries))
        .clone())
}

fn predicate_access_from_directory_entry(
    entry: Option<NativePredicateDirectoryEntry>,
) -> NativePredicateAccess {
    let Some(entry) = entry else {
        return NativePredicateAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "p-exact-ranges-vortex-v2-cached",
        };
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = candidate_ranges <= predicate_exact_max_ranges()
        && entry.candidate_rows <= predicate_exact_max_rows();
    NativePredicateAccess {
        ranges: accepted.then(Vec::new),
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "p-exact-ranges-vortex-v2-cached",
    }
}

async fn read_predicate_v2_payload(
    data_path: &Path,
    entry: NativePredicateDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| {
            VortexRdfError::Deserialization("predicate v2 payload row range overflow".into())
        })?;
    let path = native_p_exact_ranges_v2_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if payload.len() != entry.range_count as usize {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 payload {:?} returned {} rows for expected slice {}..{}",
            path,
            payload.len(),
            entry.range_offset,
            range_end
        )));
    }

    let starts = extract_projected_u64_column(&payload, "row_start")?;
    let ends = extract_projected_u64_column(&payload, "row_end")?;
    let mut ranges = Vec::with_capacity(payload.len());
    for (start, end) in starts.into_iter().zip(ends) {
        if start > end {
            return Err(VortexRdfError::Deserialization(format!(
                "predicate v2 contains invalid row range {start}..{end}"
            )));
        }
        ranges.push(start..end);
    }
    if range_rows(&ranges) != entry.candidate_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "predicate v2 candidate row mismatch: directory={}, payload={}",
            entry.candidate_rows,
            range_rows(&ranges)
        )));
    }
    Ok(ranges)
}

async fn lookup_predicate_access_from_vortex_v2(
    data_path: &Path,
    predicate_id: u32,
) -> Result<Option<NativePredicateAccess>> {
    let directory = predicate_v2_directory(data_path).await?;
    let entry = directory
        .binary_search_by_key(&predicate_id, |entry| entry.predicate_id)
        .ok()
        .map(|index| directory[index]);
    let mut access = predicate_access_from_directory_entry(entry);
    if access.ranges.is_some() {
        if let Some(entry) = entry {
            access.ranges = Some(read_predicate_v2_payload(data_path, entry).await?);
        }
    }
    Ok(Some(access))
}

fn predicate_exact_max_ranges() -> usize {
    std::env::var("VORTEX_RDF_P_EXACT_MAX_RANGES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
}

fn predicate_exact_max_rows() -> u64 {
    std::env::var("VORTEX_RDF_P_EXACT_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000)
}

fn range_rows(ranges: &[std::ops::Range<u64>]) -> u64 {
    ranges
        .iter()
        .map(|range| range.end.saturating_sub(range.start))
        .sum()
}

fn lookup_exact_row_ranges(
    path: &Path,
    expected_magic: &[u8; 8],
    needle: u64,
    label: &str,
) -> Result<Vec<std::ops::Range<u64>>> {
    let file = std::fs::File::open(path).map_err(|error| {
        VortexRdfError::Deserialization(
            format!("Failed to open {label} index {:?}: {error}", path,),
        )
    })?;
    let len = file
        .metadata()
        .map_err(|error| VortexRdfError::Deserialization(error.to_string()))?
        .len();
    if len < 16 || (len - 16) % NATIVE_PO_EXACT_RANGE_ENTRY_BYTES != 0 {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed {label} index {:?}: len={len}",
            path,
        )));
    }

    let mut magic = [0u8; 8];
    read_exact_at_native_sidecar(&file, &mut magic, 0, label)?;
    if &magic != expected_magic {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed {label} index {:?}: bad magic",
            path,
        )));
    }
    let mut count_buffer = [0u8; 8];
    read_exact_at_native_sidecar(&file, &mut count_buffer, 8, label)?;
    let count = u64::from_le_bytes(count_buffer);
    if 16 + count.saturating_mul(NATIVE_PO_EXACT_RANGE_ENTRY_BYTES) != len {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed {label} index {:?}: count/len mismatch",
            path,
        )));
    }

    let mut low = 0u64;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let (key, _, _) = read_exact_range_entry_at(&file, middle)?;
        if key < needle {
            low = middle + 1;
        } else {
            high = middle;
        }
    }

    let mut ranges = Vec::new();
    let mut index = low;
    while index < count {
        let (key, start, end) = read_exact_range_entry_at(&file, index)?;
        if key != needle {
            break;
        }
        ranges.push(start..end);
        index += 1;
    }
    Ok(ranges)
}

fn write_exact_range_index(
    output_path: &Path,
    magic: &[u8; 8],
    entries: &mut Vec<(u64, u64, u64)>,
) -> Result<()> {
    entries.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let temporary_path = output_path.with_extension("exact_ranges.bin.tmp");
    let temporary_file = std::fs::File::create(&temporary_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let mut writer = BufWriter::new(temporary_file);
    writer
        .write_all(magic)
        .and_then(|_| writer.write_all(&(entries.len() as u64).to_le_bytes()))
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    for (key, start, end) in entries.iter().copied() {
        write_exact_range_entry(&mut writer, key, start, end)?;
    }
    writer
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    drop(writer);
    std::fs::rename(&temporary_path, output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    Ok(())
}

pub async fn build_cottas_native_po_exact_ranges_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    let total_start = Instant::now();
    let output_path = native_po_exact_ranges_path(input_path);
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["p", "o"].as_slice(),
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    // Prototype exact access path: hash(p,o) -> exact consecutive row ranges.
    // This is intentionally collision-safe at query time because we still apply the exact p/o Vortex filter.
    let mut ranges_by_hash: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    let mut row_groups = 0usize;
    let mut rows_scanned = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let batch_rows = batch.len();
        if batch_rows == 0 {
            continue;
        }
        let p_ids = extract_projected_u32_column(&batch, "p")?;
        let o_ids = extract_projected_u32_column(&batch, "o")?;
        if p_ids.len() != batch_rows || o_ids.len() != batch_rows {
            return Err(VortexRdfError::Serialization(format!(
                "PO exact range index saw p/o length mismatch: p={}, o={}, rows={}",
                p_ids.len(),
                o_ids.len(),
                batch_rows
            )));
        }
        for row in 0..batch_rows {
            let row_id = rows_scanned + row as u64;
            let hash = native_po_hash(p_ids[row], o_ids[row]);
            let ranges = ranges_by_hash.entry(hash).or_default();
            if let Some(last) = ranges.last_mut() {
                if last.1 == row_id {
                    last.1 = row_id + 1;
                    continue;
                }
            }
            ranges.push((row_id, row_id + 1));
        }
        rows_scanned += batch_rows as u64;
        row_groups += 1;
    }
    let scan_ms = elapsed_ms(scan_start);
    let write_start = Instant::now();

    let mut entries: Vec<(u64, u64, u64)> = Vec::new();
    for (hash, ranges) in ranges_by_hash {
        entries.reserve(ranges.len());
        for (start, end) in ranges {
            entries.push((hash, start, end));
        }
    }
    write_exact_range_index(&output_path, NATIVE_PO_EXACT_RANGES_MAGIC, &mut entries)?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: output_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: entries.len() as u64,
        open_ms,
        scan_ms,
        write_ms,
        total_ms,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativePoRangeRecord {
    predicate_id: u32,
    object_id: u32,
    row_start: u64,
    row_end: u64,
}

impl Ord for NativePoRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.predicate_id,
            self.object_id,
            self.row_start,
            self.row_end,
        )
            .cmp(&(
                other.predicate_id,
                other.object_id,
                other.row_start,
                other.row_end,
            ))
    }
}
impl PartialOrd for NativePoRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn native_po_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    native_sidecar_path(data_path, "po_exact_directory.v2.vortex")
}

fn native_po_predicate_partitions_v2_path(data_path: &Path) -> PathBuf {
    native_sidecar_path(data_path, "po_predicate_partitions.v2.vortex")
}

fn native_po_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_sidecar_path(data_path, "po_exact_ranges.v2.vortex")
}

fn native_sidecar_path(data_path: &Path, suffix: &str) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{name}.{suffix}"))
}

fn write_po_range_record<W: Write>(writer: &mut W, value: NativePoRangeRecord) -> Result<()> {
    writer
        .write_all(&value.predicate_id.to_le_bytes())
        .and_then(|_| writer.write_all(&value.object_id.to_le_bytes()))
        .and_then(|_| writer.write_all(&value.row_start.to_le_bytes()))
        .and_then(|_| writer.write_all(&value.row_end.to_le_bytes()))
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))
}

struct NativePoRangeRunReader {
    reader: BufReader<std::fs::File>,
}

impl NativePoRangeRunReader {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
            ),
        })
    }

    fn read_one(&mut self) -> Result<Option<NativePoRangeRecord>> {
        let mut buf = [0u8; 24];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(NativePoRangeRecord {
                predicate_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
                object_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
                row_start: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
                row_end: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(VortexRdfError::Serialization(error.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativePoRangeHeapItem {
    value: NativePoRangeRecord,
    run_idx: usize,
}

impl Ord for NativePoRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for NativePoRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn flush_po_range_run(
    records: &mut Vec<NativePoRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    records.sort_unstable();
    let path = temp_dir.join(format!("po_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
    );
    for value in records.iter().copied() {
        write_po_range_record(&mut writer, value)?;
    }
    writer
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    records.clear();
    runs.push(path);
    Ok(())
}

fn build_po_directory_array(
    predicate_ids: Vec<u32>,
    object_ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        (
            "predicate_id",
            PrimitiveArray::from_iter(predicate_ids).into_array(),
        ),
        (
            "object_id",
            PrimitiveArray::from_iter(object_ids).into_array(),
        ),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

pub async fn build_cottas_native_po_exact_ranges_v2_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_po_exact_directory_v2_path(input_path);
    let payload_path = native_po_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir =
        tempfile::tempdir().map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_PO_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["p", "o"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut active_key: Option<(u32, u32)> = None;
    let mut active_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let predicates = extract_projected_u32_column(&batch, "p")?;
        let objects = extract_projected_u32_column(&batch, "o")?;
        if predicates.len() != objects.len() || predicates.len() != batch.len() {
            return Err(VortexRdfError::Serialization(format!(
                "PO v2 scan column mismatch: rows={}, predicates={}, objects={}",
                batch.len(),
                predicates.len(),
                objects.len()
            )));
        }
        row_groups += 1;
        for key in predicates.into_iter().zip(objects) {
            match active_key {
                None => {
                    active_key = Some(key);
                    active_start = rows_scanned;
                }
                Some(previous) if previous != key => {
                    records.push(NativePoRangeRecord {
                        predicate_id: previous.0,
                        object_id: previous.1,
                        row_start: active_start,
                        row_end: rows_scanned,
                    });
                    active_key = Some(key);
                    active_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let run_idx = runs.len();
                        flush_po_range_run(&mut records, temp_dir.path(), run_idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some((predicate_id, object_id)) = active_key {
        records.push(NativePoRangeRecord {
            predicate_id,
            object_id,
            row_start: active_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let run_idx = runs.len();
        flush_po_range_run(&mut records, temp_dir.path(), run_idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("po_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|error| VortexRdfError::Serialization(error.to_string()))?,
    );
    let mut readers = runs
        .iter()
        .map(|path| NativePoRangeRunReader::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.read_one()? {
            heap.push(NativePoRangeHeapItem { value, run_idx });
        }
    }

    let mut dir_predicates = Vec::new();
    let mut dir_objects = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_key: Option<(u32, u32)> = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    let finish_entry = |key: (u32, u32),
                        offset: u64,
                        count: u32,
                        rows: u64,
                        predicates: &mut Vec<u32>,
                        objects: &mut Vec<u32>,
                        offsets: &mut Vec<u64>,
                        counts: &mut Vec<u32>,
                        row_counts: &mut Vec<u64>| {
        predicates.push(key.0);
        objects.push(key.1);
        offsets.push(offset);
        counts.push(count);
        row_counts.push(rows);
    };

    while let Some(item) = heap.pop() {
        let value = item.value;
        let key = (value.predicate_id, value.object_id);
        if active_key != Some(key) {
            if let Some(previous) = active_key {
                finish_entry(
                    previous,
                    active_offset,
                    active_count,
                    active_rows,
                    &mut dir_predicates,
                    &mut dir_objects,
                    &mut dir_offsets,
                    &mut dir_counts,
                    &mut dir_rows,
                );
            }
            active_key = Some(key);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start >= value.row_end {
            return Err(VortexRdfError::Serialization(format!(
                "PO v2 contains invalid range {}..{} for ({}, {})",
                value.row_start, value.row_end, value.predicate_id, value.object_id
            )));
        }
        write_po_range_record(&mut merged_writer, value)?;
        active_count = active_count
            .checked_add(1)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 range count overflow".into()))?;
        active_rows = active_rows
            .checked_add(value.row_end - value.row_start)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 row count overflow".into()))?;
        payload_offset = payload_offset
            .checked_add(1)
            .ok_or_else(|| VortexRdfError::Serialization("PO v2 payload offset overflow".into()))?;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(NativePoRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(key) = active_key {
        finish_entry(
            key,
            active_offset,
            active_count,
            active_rows,
            &mut dir_predicates,
            &mut dir_objects,
            &mut dir_offsets,
            &mut dir_counts,
            &mut dir_rows,
        );
    }
    merged_writer
        .flush()
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = NativePoRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_predicate_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_predicate_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let write_start = Instant::now();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(
            WriteStrategyBuilder::default()
                .with_row_block_size(OUTPUT_BATCH_ROWS)
                .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
                .build(),
        )
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_entries = dir_predicates.len();
    let directory_array = build_po_directory_array(
        dir_predicates,
        dir_objects,
        dir_offsets,
        dir_counts,
        dir_rows,
    )?;
    let directory_dtype =
        build_po_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_stream = ArrayStreamAdapter::new(
        directory_dtype,
        futures::stream::iter(vec![Ok(directory_array)]),
    );
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    build_cottas_native_po_predicate_partitions_v2(input_path).await?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] PO v2 scan_ms={scan_ms:.3}, merge_ms={merge_ms:.3}, directory_entries={directory_entries}, payload_ranges={payload_offset}"
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms,
        total_ms,
    })
}

// VVO_TYPED_OBJECT_INDEX_V1
#[derive(Clone, Copy, Debug)]
struct NativeObjectDirectoryEntry {
    object_id: u32,
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}

static NATIVE_OBJECT_V2_DIRECTORY_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, Arc<[NativeObjectDirectoryEntry]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn object_v2_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, Arc<[NativeObjectDirectoryEntry]>>>> {
    NATIVE_OBJECT_V2_DIRECTORY_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("object v2 directory cache mutex was poisoned".into())
    })
}

async fn object_v2_directory(data_path: &Path) -> Result<Arc<[NativeObjectDirectoryEntry]>> {
    let path = native_o_exact_directory_v2_path(data_path);
    if let Some(cached) = object_v2_cache_lock()?.get(&path).cloned() {
        return Ok(cached);
    }

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["object_id", "range_offset", "range_count", "candidate_rows"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;

    let ids = extract_projected_u32_column(&array, "object_id")?;
    let offsets = extract_projected_u64_column(&array, "range_offset")?;
    let counts = extract_projected_u32_column(&array, "range_count")?;
    let rows = extract_projected_u64_column(&array, "candidate_rows")?;
    let len = array.len();
    if [ids.len(), offsets.len(), counts.len(), rows.len()]
        .into_iter()
        .any(|column_len| column_len != len)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 directory {:?} has inconsistent column lengths",
            path
        )));
    }

    let mut entries = Vec::with_capacity(len);
    for index in 0..len {
        entries.push(NativeObjectDirectoryEntry {
            object_id: ids[index],
            range_offset: offsets[index],
            range_count: counts[index],
            candidate_rows: rows[index],
        });
    }
    if entries
        .windows(2)
        .any(|pair| pair[0].object_id >= pair[1].object_id)
    {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 directory {:?} is not strictly sorted by object_id",
            path
        )));
    }

    let entries: Arc<[NativeObjectDirectoryEntry]> = entries.into();
    let mut cache = object_v2_cache_lock()?;
    Ok(cache
        .entry(path)
        .or_insert_with(|| Arc::clone(&entries))
        .clone())
}

fn object_access_from_directory_entry(
    entry: Option<NativeObjectDirectoryEntry>,
) -> NativeObjectAccess {
    let Some(entry) = entry else {
        return NativeObjectAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "o-exact-ranges-vortex-v2-cached",
        };
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = candidate_ranges <= object_exact_max_ranges()
        && entry.candidate_rows <= object_exact_max_rows();
    NativeObjectAccess {
        ranges: accepted.then(Vec::new),
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "o-exact-ranges-vortex-v2-cached",
    }
}

async fn read_object_v2_payload(
    data_path: &Path,
    entry: NativeObjectDirectoryEntry,
) -> Result<Vec<Range<u64>>> {
    let range_end = entry
        .range_offset
        .checked_add(u64::from(entry.range_count))
        .ok_or_else(|| {
            VortexRdfError::Deserialization("object v2 payload row range overflow".into())
        })?;
    let path = native_o_exact_ranges_v2_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let payload = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_range(entry.range_offset..range_end)
        .with_projection(vortex_array::expr::select(
            ["row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if payload.len() != entry.range_count as usize {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 payload {:?} returned {} rows for expected slice {}..{}",
            path,
            payload.len(),
            entry.range_offset,
            range_end
        )));
    }

    let starts = extract_projected_u64_column(&payload, "row_start")?;
    let ends = extract_projected_u64_column(&payload, "row_end")?;
    let mut ranges = Vec::with_capacity(payload.len());
    for (start, end) in starts.into_iter().zip(ends) {
        if start > end {
            return Err(VortexRdfError::Deserialization(format!(
                "object v2 contains invalid row range {start}..{end}"
            )));
        }
        ranges.push(start..end);
    }
    if range_rows(&ranges) != entry.candidate_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 candidate row mismatch: directory={}, payload={}",
            entry.candidate_rows,
            range_rows(&ranges)
        )));
    }
    Ok(ranges)
}

async fn lookup_object_access_from_vortex_v2(
    data_path: &Path,
    object_id: u32,
) -> Result<Option<NativeObjectAccess>> {
    let directory = object_v2_directory(data_path).await?;
    let entry = directory
        .binary_search_by_key(&object_id, |entry| entry.object_id)
        .ok()
        .map(|index| directory[index]);
    let mut access = object_access_from_directory_entry(entry);
    if access.ranges.is_some() {
        if let Some(entry) = entry {
            access.ranges = Some(read_object_v2_payload(data_path, entry).await?);
        }
    }
    Ok(Some(access))
}

fn object_exact_max_ranges() -> usize {
    std::env::var("VORTEX_RDF_O_EXACT_MAX_RANGES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
}

fn object_exact_max_rows() -> u64 {
    std::env::var("VORTEX_RDF_O_EXACT_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativePredicateRangeRecord {
    predicate_id: u32,
    row_start: u64,
    row_end: u64,
}

impl Ord for NativePredicateRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.predicate_id
            .cmp(&other.predicate_id)
            .then_with(|| self.row_start.cmp(&other.row_start))
            .then_with(|| self.row_end.cmp(&other.row_end))
    }
}
impl PartialOrd for NativePredicateRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn native_p_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{name}.p_exact_directory.v2.vortex"))
}

fn native_p_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{name}.p_exact_ranges.v2.vortex"))
}

fn write_predicate_range_record<W: Write>(
    writer: &mut W,
    value: NativePredicateRangeRecord,
) -> Result<()> {
    writer
        .write_all(&value.predicate_id.to_le_bytes())
        .and_then(|_| writer.write_all(&value.row_start.to_le_bytes()))
        .and_then(|_| writer.write_all(&value.row_end.to_le_bytes()))
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}

struct NativePredicateRangeRunReader {
    reader: BufReader<std::fs::File>,
}
impl NativePredicateRangeRunReader {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
            ),
        })
    }
    fn read_one(&mut self) -> Result<Option<NativePredicateRangeRecord>> {
        let mut buf = [0u8; 20];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(NativePredicateRangeRecord {
                predicate_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
                row_start: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
                row_end: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativePredicateRangeHeapItem {
    value: NativePredicateRangeRecord,
    run_idx: usize,
}
impl Ord for NativePredicateRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for NativePredicateRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn flush_predicate_range_run(
    records: &mut Vec<NativePredicateRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    records.sort_unstable();
    let path = temp_dir.join(format!("predicate_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    for value in records.iter().copied() {
        write_predicate_range_record(&mut writer, value)?;
    }
    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    records.clear();
    runs.push(path);
    Ok(())
}

fn build_predicate_payload_array(starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("row_start", PrimitiveArray::from_iter(starts).into_array()),
        ("row_end", PrimitiveArray::from_iter(ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

fn build_predicate_directory_array(
    ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("predicate_id", PrimitiveArray::from_iter(ids).into_array()),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

pub async fn build_cottas_native_p_exact_ranges_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_p_exact_directory_v2_path(input_path);
    let payload_path = native_p_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_P_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["p"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut current_predicate = None;
    let mut current_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "p")?;
        row_groups += 1;
        for predicate_id in values {
            match current_predicate {
                None => {
                    current_predicate = Some(predicate_id);
                    current_start = rows_scanned;
                }
                Some(previous) if previous != predicate_id => {
                    records.push(NativePredicateRangeRecord {
                        predicate_id: previous,
                        row_start: current_start,
                        row_end: rows_scanned,
                    });
                    current_predicate = Some(predicate_id);
                    current_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let idx = runs.len();
                        flush_predicate_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some(predicate_id) = current_predicate {
        records.push(NativePredicateRangeRecord {
            predicate_id,
            row_start: current_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let idx = runs.len();
        flush_predicate_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("predicate_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    for path in &runs {
        readers.push(NativePredicateRangeRunReader::new(path)?);
    }
    for run_idx in 0..readers.len() {
        if let Some(value) = readers[run_idx].read_one()? {
            heap.push(NativePredicateRangeHeapItem { value, run_idx });
        }
    }
    let mut dir_ids = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_predicate = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    while let Some(item) = heap.pop() {
        let value = item.value;
        if active_predicate != Some(value.predicate_id) {
            if let Some(predicate_id) = active_predicate {
                dir_ids.push(predicate_id);
                dir_offsets.push(active_offset);
                dir_counts.push(active_count);
                dir_rows.push(active_rows);
            }
            active_predicate = Some(value.predicate_id);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start > value.row_end {
            return Err(VortexRdfError::Serialization(
                "predicate v2 range start exceeds end".into(),
            ));
        }
        write_predicate_range_record(&mut merged_writer, value)?;
        active_count = active_count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("predicate v2 range count overflow".into())
        })?;
        active_rows = active_rows.saturating_add(value.row_end - value.row_start);
        payload_offset += 1;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(NativePredicateRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(predicate_id) = active_predicate {
        dir_ids.push(predicate_id);
        dir_offsets.push(active_offset);
        dir_counts.push(active_count);
        dir_rows.push(active_rows);
    }
    merged_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = NativePredicateRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_predicate_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_predicate_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let payload_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(payload_strategy)
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_rows = dir_ids.len();
    let directory_array =
        build_predicate_directory_array(dir_ids, dir_offsets, dir_counts, dir_rows)?;
    let directory_dtype =
        build_predicate_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_arrays = futures::stream::iter(vec![Ok(directory_array)]);
    let directory_stream = ArrayStreamAdapter::new(directory_dtype, directory_arrays);
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let directory_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(directory_strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    // A rebuild in the same process must not retain old directory metadata.
    predicate_v2_cache_lock()?.remove(&directory_path);

    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote predicate v2 directory {:?} predicates={} and payload {:?} ranges={} rows={} runs={} scan_ms={:.3} merge_ms={:.3} total_ms={:.3}",
        directory_path,
        directory_rows,
        payload_path,
        payload_offset,
        rows_scanned,
        runs.len(),
        scan_ms,
        merge_ms,
        total_ms
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms: merge_ms,
        total_ms,
    })
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativeObjectRangeRecord {
    object_id: u32,
    row_start: u64,
    row_end: u64,
}

impl Ord for NativeObjectRangeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.object_id
            .cmp(&other.object_id)
            .then_with(|| self.row_start.cmp(&other.row_start))
            .then_with(|| self.row_end.cmp(&other.row_end))
    }
}
impl PartialOrd for NativeObjectRangeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn native_o_exact_directory_v2_path(data_path: &Path) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{name}.o_exact_directory.v2.vortex"))
}

fn native_o_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    let name = data_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("data.vortex");
    data_path.with_file_name(format!("{name}.o_exact_ranges.v2.vortex"))
}

fn write_object_range_record<W: Write>(
    writer: &mut W,
    value: NativeObjectRangeRecord,
) -> Result<()> {
    writer
        .write_all(&value.object_id.to_le_bytes())
        .and_then(|_| writer.write_all(&value.row_start.to_le_bytes()))
        .and_then(|_| writer.write_all(&value.row_end.to_le_bytes()))
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))
}

struct NativeObjectRangeRunReader {
    reader: BufReader<std::fs::File>,
}
impl NativeObjectRangeRunReader {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(
                std::fs::File::open(path)
                    .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
            ),
        })
    }
    fn read_one(&mut self) -> Result<Option<NativeObjectRangeRecord>> {
        let mut buf = [0u8; 20];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(NativeObjectRangeRecord {
                object_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
                row_start: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
                row_end: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(VortexRdfError::Serialization(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativeObjectRangeHeapItem {
    value: NativeObjectRangeRecord,
    run_idx: usize,
}
impl Ord for NativeObjectRangeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.run_idx.cmp(&self.run_idx))
    }
}
impl PartialOrd for NativeObjectRangeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn flush_object_range_run(
    records: &mut Vec<NativeObjectRangeRecord>,
    temp_dir: &Path,
    run_idx: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    records.sort_unstable();
    let path = temp_dir.join(format!("object_range_run_{run_idx:06}.bin"));
    let mut writer = BufWriter::new(
        std::fs::File::create(&path).map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    for value in records.iter().copied() {
        write_object_range_record(&mut writer, value)?;
    }
    writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    records.clear();
    runs.push(path);
    Ok(())
}

fn build_object_payload_array(starts: Vec<u64>, ends: Vec<u64>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("row_start", PrimitiveArray::from_iter(starts).into_array()),
        ("row_end", PrimitiveArray::from_iter(ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

fn build_object_directory_array(
    ids: Vec<u32>,
    offsets: Vec<u64>,
    counts: Vec<u32>,
    rows: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("object_id", PrimitiveArray::from_iter(ids).into_array()),
        (
            "range_offset",
            PrimitiveArray::from_iter(offsets).into_array(),
        ),
        (
            "range_count",
            PrimitiveArray::from_iter(counts).into_array(),
        ),
        (
            "candidate_rows",
            PrimitiveArray::from_iter(rows).into_array(),
        ),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

pub async fn build_cottas_native_o_exact_ranges_index(
    input_path: &Path,
) -> Result<NativePoRowGroupIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;
    let total_start = Instant::now();
    let directory_path = native_o_exact_directory_v2_path(input_path);
    let payload_path = native_o_exact_ranges_v2_path(input_path);
    let directory_tmp = directory_path.with_extension("vortex.tmp");
    let payload_tmp = payload_path.with_extension("vortex.tmp");
    let temp_dir = tempfile::tempdir().map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let sort_batch = std::env::var("VORTEX_RDF_O_V2_SORT_BATCH_RANGES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(1);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["o"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut records = Vec::with_capacity(sort_batch);
    let mut runs = Vec::new();
    let mut rows_scanned = 0u64;
    let mut row_groups = 0usize;
    let mut current_object = None;
    let mut current_start = 0u64;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        let values = extract_projected_u32_column(&batch, "o")?;
        row_groups += 1;
        for object_id in values {
            match current_object {
                None => {
                    current_object = Some(object_id);
                    current_start = rows_scanned;
                }
                Some(previous) if previous != object_id => {
                    records.push(NativeObjectRangeRecord {
                        object_id: previous,
                        row_start: current_start,
                        row_end: rows_scanned,
                    });
                    current_object = Some(object_id);
                    current_start = rows_scanned;
                    if records.len() >= sort_batch {
                        let idx = runs.len();
                        flush_object_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
                    }
                }
                Some(_) => {}
            }
            rows_scanned += 1;
        }
    }
    if let Some(object_id) = current_object {
        records.push(NativeObjectRangeRecord {
            object_id,
            row_start: current_start,
            row_end: rows_scanned,
        });
    }
    if !records.is_empty() {
        let idx = runs.len();
        flush_object_range_run(&mut records, temp_dir.path(), idx, &mut runs)?;
    }
    let scan_ms = elapsed_ms(scan_start);

    let merge_start = Instant::now();
    let merged_path = temp_dir.path().join("object_ranges_merged.bin");
    let mut merged_writer = BufWriter::new(
        std::fs::File::create(&merged_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?,
    );
    let mut readers = Vec::with_capacity(runs.len());
    let mut heap = BinaryHeap::new();
    for path in &runs {
        readers.push(NativeObjectRangeRunReader::new(path)?);
    }
    for run_idx in 0..readers.len() {
        if let Some(value) = readers[run_idx].read_one()? {
            heap.push(NativeObjectRangeHeapItem { value, run_idx });
        }
    }
    let mut dir_ids = Vec::new();
    let mut dir_offsets = Vec::new();
    let mut dir_counts = Vec::new();
    let mut dir_rows = Vec::new();
    let mut payload_offset = 0u64;
    let mut active_object = None;
    let mut active_offset = 0u64;
    let mut active_count = 0u32;
    let mut active_rows = 0u64;
    while let Some(item) = heap.pop() {
        let value = item.value;
        if active_object != Some(value.object_id) {
            if let Some(object_id) = active_object {
                dir_ids.push(object_id);
                dir_offsets.push(active_offset);
                dir_counts.push(active_count);
                dir_rows.push(active_rows);
            }
            active_object = Some(value.object_id);
            active_offset = payload_offset;
            active_count = 0;
            active_rows = 0;
        }
        if value.row_start > value.row_end {
            return Err(VortexRdfError::Serialization(
                "object v2 range start exceeds end".into(),
            ));
        }
        write_object_range_record(&mut merged_writer, value)?;
        active_count = active_count.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization("object v2 range count overflow".into())
        })?;
        active_rows = active_rows.saturating_add(value.row_end - value.row_start);
        payload_offset += 1;
        if let Some(next) = readers[item.run_idx].read_one()? {
            heap.push(NativeObjectRangeHeapItem {
                value: next,
                run_idx: item.run_idx,
            });
        }
    }
    if let Some(object_id) = active_object {
        dir_ids.push(object_id);
        dir_offsets.push(active_offset);
        dir_counts.push(active_count);
        dir_rows.push(active_rows);
    }
    merged_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(merged_writer);
    let merge_ms = elapsed_ms(merge_start);

    let payload_reader = NativeObjectRangeRunReader::new(&merged_path)?;
    let payload_arrays = async_stream::try_stream! {
        let mut reader = payload_reader;
        loop {
            let mut starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            let mut ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
            while starts.len() < OUTPUT_BATCH_ROWS {
                let Some(value) = reader.read_one().map_err(rdf_err_to_vortex_err)? else { break; };
                starts.push(value.row_start);
                ends.push(value.row_end);
            }
            if starts.is_empty() { break; }
            yield build_object_payload_array(starts, ends).map_err(rdf_err_to_vortex_err)?;
        }
    };
    let payload_dtype = build_object_payload_array(Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let payload_stream = ArrayStreamAdapter::new(payload_dtype, payload_arrays);
    let payload_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    let mut payload_file = tokio::fs::File::create(&payload_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(payload_strategy)
        .write(&mut payload_file, payload_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(payload_file);

    let directory_rows = dir_ids.len();
    let directory_array = build_object_directory_array(dir_ids, dir_offsets, dir_counts, dir_rows)?;
    let directory_dtype =
        build_object_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
            .dtype()
            .clone();
    let directory_arrays = futures::stream::iter(vec![Ok(directory_array)]);
    let directory_stream = ArrayStreamAdapter::new(directory_dtype, directory_arrays);
    let mut directory_file = tokio::fs::File::create(&directory_tmp)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let directory_strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(directory_strategy)
        .write(&mut directory_file, directory_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(directory_file);
    std::fs::rename(&payload_tmp, &payload_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    std::fs::rename(&directory_tmp, &directory_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    // A rebuild in the same process must not retain old directory metadata.
    object_v2_cache_lock()?.remove(&directory_path);

    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] wrote object v2 directory {:?} objects={} and payload {:?} ranges={} rows={} runs={} scan_ms={:.3} merge_ms={:.3} total_ms={:.3}",
        directory_path,
        directory_rows,
        payload_path,
        payload_offset,
        rows_scanned,
        runs.len(),
        scan_ms,
        merge_ms,
        total_ms
    );
    Ok(NativePoRowGroupIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: directory_path.display().to_string(),
        row_groups,
        rows_scanned,
        unique_po_hashes_written: payload_offset,
        open_ms,
        scan_ms,
        write_ms: merge_ms,
        total_ms,
    })
}

const NATIVE_SUBJECT_RANGE_ENTRY_BYTES: u64 = 20;

#[derive(Clone, Copy, Debug)]
struct NativeSubjectRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeSubjectRangeIndexBuildStats {
    pub input_path: String,
    pub output_path: String,
    pub rows_scanned: u64,
    pub ranges_written: u64,
    pub batches: usize,
    pub max_batch_rows: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}

fn extract_projected_u32_column(array: &ArrayRef, column_name: &str) -> Result<Vec<u32>> {
    if array.len() == 0 {
        return Ok(Vec::new());
    }
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(column.as_slice::<u32>().to_vec())
}

fn extract_projected_u64_column(array: &ArrayRef, column_name: &str) -> Result<Vec<u64>> {
    if array.len() == 0 {
        return Ok(Vec::new());
    }
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let struct_array = array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let column = struct_array
        .unmasked_field_by_name(column_name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(column.as_slice::<u64>().to_vec())
}

fn read_subject_range_entry_at(file: &std::fs::File, entry_idx: u64) -> Result<(u32, u64, u64)> {
    let mut buf = [0u8; 20];
    let offset = entry_idx
        .checked_mul(NATIVE_SUBJECT_RANGE_ENTRY_BYTES)
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "subject range index offset overflow for entry {}",
                entry_idx
            ))
        })?;
    read_exact_at_native_sidecar(file, &mut buf, offset, "subject range index")?;
    let subject_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let start = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let end = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    Ok((subject_id, start, end))
}

fn lookup_subject_range_from_sidecar(
    data_path: &Path,
    subject_id: u32,
) -> Result<Option<NativeSubjectRange>> {
    let path = native_subject_range_index_path(data_path);
    let file = std::fs::File::open(&path).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Failed to open native subject range index {:?}: {}",
            path, e
        ))
    })?;
    let len = file
        .metadata()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
        .len();
    if len % NATIVE_SUBJECT_RANGE_ENTRY_BYTES != 0 {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed subject range index {:?}: byte length {} is not divisible by {}",
            path, len, NATIVE_SUBJECT_RANGE_ENTRY_BYTES
        )));
    }
    let mut lo = 0u64;
    let mut hi = len / NATIVE_SUBJECT_RANGE_ENTRY_BYTES;
    while lo < hi {
        let mid = lo + ((hi - lo) / 2);
        let (candidate, start, end) = read_subject_range_entry_at(&file, mid)?;
        match candidate.cmp(&subject_id) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Ok(Some(NativeSubjectRange { start, end })),
        }
    }
    Ok(None)
}

#[derive(Clone, Debug, Default)]
struct NativeSubjectRangeBuildState {
    rows_scanned: u64,
    ranges_written: u64,
    batches: usize,
    max_batch_rows: usize,
}

fn store_subject_range_build_state(
    shared_state: &Mutex<NativeSubjectRangeBuildState>,
    state: NativeSubjectRangeBuildState,
) -> VortexResult<()> {
    let mut guard = shared_state
        .lock()
        .map_err(|_| vortex_error::vortex_err!("subject range build-state mutex was poisoned"))?;
    *guard = state;
    Ok(())
}

fn build_subject_range_array(
    subject_ids: Vec<u32>,
    row_starts: Vec<u64>,
    row_ends: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        (
            "subject_id",
            PrimitiveArray::from_iter(subject_ids).into_array(),
        ),
        (
            "row_start",
            PrimitiveArray::from_iter(row_starts).into_array(),
        ),
        ("row_end", PrimitiveArray::from_iter(row_ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

fn empty_subject_range_array() -> Result<ArrayRef> {
    build_subject_range_array(Vec::new(), Vec::new(), Vec::new())
}

/// Writes the production subject index directly to Vortex. The legacy binary
/// reader is retained for old datasets, but new serializations do not produce
/// `subject_ranges.bin`.
pub async fn build_cottas_native_subject_range_index(
    input_path: &Path,
) -> Result<NativeSubjectRangeIndexBuildStats> {
    const OUTPUT_BATCH_ROWS: usize = 65_536;

    let total_start = Instant::now();
    let output_path = native_subject_range_vortex_path(input_path);
    let temporary_path = output_path.with_extension("vortex.tmp");

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(input_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);

    let scan_start = Instant::now();
    let mut input_stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["s"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let shared_state = Arc::new(Mutex::new(NativeSubjectRangeBuildState::default()));
    let stream_state = Arc::clone(&shared_state);
    let output_arrays = async_stream::try_stream! {
        let mut subject_ids = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut row_starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut row_ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
        let mut rows_scanned = 0u64;
        let mut ranges_written = 0u64;
        let mut batches = 0usize;
        let mut max_batch_rows = 0usize;
        let mut current_subject: Option<u32> = None;
        let mut current_start = 0u64;
        let mut last_completed_subject: Option<u32> = None;

        while let Some(batch_result) = input_stream.next().await {
            let batch = batch_result?;
            let batch_rows = batch.len();
            batches += 1;
            max_batch_rows = max_batch_rows.max(batch_rows);
            if batch_rows == 0 {
                continue;
            }

            let values = extract_projected_u32_column(&batch, "s")
                .map_err(rdf_err_to_vortex_err)?;
            if values.len() != batch_rows {
                Err(vortex_error::vortex_err!(
                    "subject range build saw {} subject IDs for {} rows",
                    values.len(),
                    batch_rows
                ))?;
            }

            for subject_id in values {
                match current_subject {
                    None => {
                        current_subject = Some(subject_id);
                        current_start = rows_scanned;
                    }
                    Some(previous) if previous != subject_id => {
                        if let Some(completed) = last_completed_subject {
                            if previous <= completed {
                                Err(vortex_error::vortex_err!(
                                    "subject IDs are not strictly grouped/increasing: completed={}, next={}; SPO ordering is required",
                                    completed,
                                    previous
                                ))?;
                            }
                        }

                        subject_ids.push(previous);
                        row_starts.push(current_start);
                        row_ends.push(rows_scanned);
                        ranges_written += 1;
                        last_completed_subject = Some(previous);
                        current_subject = Some(subject_id);
                        current_start = rows_scanned;

                        if subject_ids.len() >= OUTPUT_BATCH_ROWS {
                            yield build_subject_range_array(
                                std::mem::take(&mut subject_ids),
                                std::mem::take(&mut row_starts),
                                std::mem::take(&mut row_ends),
                            ).map_err(rdf_err_to_vortex_err)?;
                            subject_ids = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                            row_starts = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                            row_ends = Vec::with_capacity(OUTPUT_BATCH_ROWS);
                        }
                    }
                    Some(_) => {}
                }
                rows_scanned += 1;
            }
        }

        if let Some(subject_id) = current_subject {
            if let Some(completed) = last_completed_subject {
                if subject_id <= completed {
                    Err(vortex_error::vortex_err!(
                        "final subject ID {} is not greater than completed ID {}; SPO ordering is required",
                        subject_id,
                        completed
                    ))?;
                }
            }
            subject_ids.push(subject_id);
            row_starts.push(current_start);
            row_ends.push(rows_scanned);
            ranges_written += 1;
        }

        if !subject_ids.is_empty() {
            yield build_subject_range_array(subject_ids, row_starts, row_ends)
                .map_err(rdf_err_to_vortex_err)?;
        } else if rows_scanned == 0 {
            yield empty_subject_range_array().map_err(rdf_err_to_vortex_err)?;
        }

        store_subject_range_build_state(
            stream_state.as_ref(),
            NativeSubjectRangeBuildState {
                rows_scanned,
                ranges_written,
                batches,
                max_batch_rows,
            },
        )?;
    };

    let dtype = empty_subject_range_array()?.dtype().clone();
    let output_stream = ArrayStreamAdapter::new(dtype, output_arrays);
    let mut output_file = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(OUTPUT_BATCH_ROWS)
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();

    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output_file, output_stream)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output_file);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;

    let scan_ms = elapsed_ms(scan_start);
    let state = shared_state
        .lock()
        .map_err(|_| {
            VortexRdfError::Serialization(
                "subject range build-state mutex was poisoned".to_string(),
            )
        })?
        .clone();
    let total_ms = elapsed_ms(total_start);

    log::info!(
        "[cottas_native_ids] wrote direct Vortex subject index {:?}: rows={}, ranges={}, batches={}, max_batch_rows={}, total_ms={:.3}",
        output_path,
        state.rows_scanned,
        state.ranges_written,
        state.batches,
        state.max_batch_rows,
        total_ms
    );

    Ok(NativeSubjectRangeIndexBuildStats {
        input_path: input_path.display().to_string(),
        output_path: output_path.display().to_string(),
        rows_scanned: state.rows_scanned,
        ranges_written: state.ranges_written,
        batches: state.batches,
        max_batch_rows: state.max_batch_rows,
        open_ms,
        scan_ms,
        write_ms: scan_ms,
        total_ms,
    })
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

async fn write_dictionary_lookup_sidecars_from_pair_runs(
    pair_run_paths: &PairRunPaths,
    data_path: &Path,
    row_group_size: usize,
) -> Result<()> {
    let _ = row_group_size; // retained in the signature until config cleanup is propagated
    write_id_to_term_binary_sidecar_from_id_runs(&pair_run_paths.id_run_paths, data_path)?;
    write_term_to_id_binary_sidecar_from_term_runs(&pair_run_paths.term_run_paths, data_path)?;
    log::info!(
        "[cottas_native_ids] wrote production dictionary sidecars {:?}, {:?}, {:?}, {:?}",
        native_dict_id_to_term_offsets_path(data_path),
        native_dict_id_to_term_blob_path(data_path),
        native_dict_term_to_id_entries_path(data_path),
        native_dict_term_to_id_blob_path(data_path),
    );
    Ok(())
}

fn write_id_to_term_binary_sidecar_from_id_runs(
    id_run_paths: &[PathBuf],
    data_path: &Path,
) -> Result<()> {
    let write_start = Instant::now();
    let offsets_path = native_dict_id_to_term_offsets_path(data_path);
    let blob_path = native_dict_id_to_term_blob_path(data_path);

    let mut readers = Vec::with_capacity(id_run_paths.len());
    for path in id_run_paths {
        readers.push(PairRunReader::new(path)?);
    }

    let mut heap = BinaryHeap::new();
    for run_idx in 0..readers.len() {
        if let Some(pair) = readers[run_idx].read_one()? {
            heap.push(PairHeapItem {
                pair,
                run_idx,
                order: PairRunOrder::Id,
            });
        }
    }

    let blob_file = std::fs::File::create(&blob_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut blob_writer = BufWriter::new(blob_file);

    let mut offsets: Vec<u64> = Vec::new();
    offsets.push(0);
    let mut current_offset = 0u64;
    let mut next_expected_id = 0u32;
    let mut pairs_written = 0usize;

    while let Some(item) = heap.pop() {
        let run_idx = item.run_idx;
        let pair = item.pair;

        if pair.id < next_expected_id {
            return Err(VortexRdfError::Serialization(format!(
                "id_to_term binary sidecar saw non-monotonic or duplicate ID {}; next_expected_id={}",
                pair.id, next_expected_id
            )));
        }

        while next_expected_id < pair.id {
            offsets.push(current_offset);
            next_expected_id += 1;
        }

        blob_writer
            .write_all(pair.term.as_bytes())
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        current_offset = current_offset
            .checked_add(pair.term.as_bytes().len() as u64)
            .ok_or_else(|| {
                VortexRdfError::Serialization(
                    "id_to_term binary blob offset overflowed u64".to_string(),
                )
            })?;
        offsets.push(current_offset);
        next_expected_id = pair.id.checked_add(1).ok_or_else(|| {
            VortexRdfError::Serialization(
                "u32 dictionary ID overflow while writing binary id_to_term sidecar".to_string(),
            )
        })?;
        pairs_written += 1;

        if let Some(next_pair) = readers[run_idx].read_one()? {
            heap.push(PairHeapItem {
                pair: next_pair,
                run_idx,
                order: PairRunOrder::Id,
            });
        }
    }

    blob_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    let offsets_file = std::fs::File::create(&offsets_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut offsets_writer = BufWriter::new(offsets_file);
    for offset in &offsets {
        offsets_writer
            .write_all(&offset.to_le_bytes())
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }
    offsets_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    log::info!(
        "[cottas_native_ids] wrote binary id_to_term sidecars pairs={}, offsets={}, blob_bytes={} to {:?} and {:?} in {:?}",
        pairs_written,
        offsets.len(),
        current_offset,
        offsets_path,
        blob_path,
        write_start.elapsed()
    );

    Ok(())
}

fn first_bound_native_id_column(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> &'static str {
    if object.is_some() {
        "o"
    } else if subject.is_some() {
        "s"
    } else if predicate.is_some() {
        "p"
    } else if graph.is_some() {
        "g"
    } else {
        "s"
    }
}

async fn resolve_single_bound_id_for_count(
    data_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<(Option<&'static str>, Option<u32>, f64, bool)> {
    let mut bound: Vec<(&'static str, String)> = Vec::new();

    if let Some(s) = subject {
        bound.push(("s", s.to_string()));
    }
    if let Some(p) = predicate {
        bound.push(("p", p.to_string()));
    }
    if let Some(o) = object {
        bound.push(("o", o.to_string()));
    }
    if let Some(g) = graph {
        bound.push(("g", g.to_string()));
    }

    if bound.is_empty() {
        return Ok((Some("s"), None, 0.0, false));
    }

    if bound.len() != 1 {
        return Err(VortexRdfError::Serialization(
            "native-id manual/execute/rows count diagnostics currently expect zero or one bound term"
                .to_string(),
        ));
    }

    let (col_name, term) = &bound[0];
    let lookup_start = Instant::now();
    let id = lookup_term_id_from_sidecar(data_path, term).await?;
    let term_lookup_ms = elapsed_ms(lookup_start);

    match id {
        Some(id) => Ok((Some(*col_name), Some(id), term_lookup_ms, false)),
        None => Ok((Some(*col_name), None, term_lookup_ms, true)),
    }
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

const NATIVE_TERM_TO_ID_ENTRY_BYTES: u64 = 24;

fn read_term_to_id_entry_at(
    entries_file: &std::fs::File,
    entry_idx: u64,
) -> Result<(u64, u64, u32)> {
    let mut buf = [0u8; 24];
    let offset = entry_idx
        .checked_mul(NATIVE_TERM_TO_ID_ENTRY_BYTES)
        .ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "term_to_id entry offset overflow for entry {}",
                entry_idx
            ))
        })?;
    read_exact_at_native_sidecar(entries_file, &mut buf, offset, "term_to_id entry")?;
    let start = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let end = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let id = u32::from_le_bytes(buf[16..20].try_into().unwrap());
    Ok((start, end, id))
}

fn lookup_term_id_from_binary_index_with_stats(
    data_path: &Path,
    term: &str,
    column: Option<&'static str>,
) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
    let lookup_start = Instant::now();
    let mut stats = NativeTermToIdLookupStats {
        column: column.map(|c| c.to_string()),
        term_len: term.len(),
        term_preview: native_term_preview(term),
        strategy: "binary-lexicographic-random-read".to_string(),
        ..NativeTermToIdLookupStats::default()
    };

    let entries_path = native_dict_term_to_id_entries_path(data_path);
    let blob_path = native_dict_term_to_id_blob_path(data_path);
    let open_start = Instant::now();
    let entries_file = std::fs::File::open(&entries_path).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Failed to open native term_to_id entries sidecar {:?}: {}",
            entries_path, e
        ))
    })?;
    let blob_file = std::fs::File::open(&blob_path).map_err(|e| {
        VortexRdfError::Deserialization(format!(
            "Failed to open native term_to_id blob sidecar {:?}: {}",
            blob_path, e
        ))
    })?;
    stats.open_ms = elapsed_ms(open_start);

    let metadata_start = Instant::now();
    let entries_len = entries_file
        .metadata()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
        .len();
    let blob_len = blob_file
        .metadata()
        .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?
        .len();
    stats.binary_metadata_ms = elapsed_ms(metadata_start);
    stats.binary_entries_file_bytes = entries_len;
    stats.binary_blob_file_bytes = blob_len;
    if entries_len % NATIVE_TERM_TO_ID_ENTRY_BYTES != 0 {
        return Err(VortexRdfError::Deserialization(format!(
            "Malformed term_to_id entries size {}",
            entries_len
        )));
    }

    let key = term.as_bytes();
    let mut lo: u64 = 0;
    let mut hi: u64 = entries_len / NATIVE_TERM_TO_ID_ENTRY_BYTES;
    let mut found = None;
    let search_start = Instant::now();
    while lo < hi {
        let mid = lo + ((hi - lo) / 2);
        let entry_read_start = Instant::now();
        let (start, end, id) = read_term_to_id_entry_at(&entries_file, mid)?;
        stats.binary_entry_read_ms += elapsed_ms(entry_read_start);
        stats.binary_probe_count += 1;
        stats.binary_entry_bytes_read += NATIVE_TERM_TO_ID_ENTRY_BYTES as usize;
        if start > end || end > blob_len {
            return Err(VortexRdfError::Deserialization(format!(
                "Invalid term_to_id blob range {}..{}",
                start, end
            )));
        }
        let len = (end - start) as usize;
        let mut candidate = vec![0u8; len];
        if len > 0 {
            let blob_read_start = Instant::now();
            read_exact_at_native_sidecar(&blob_file, &mut candidate, start, "term_to_id blob")?;
            stats.binary_blob_read_ms += elapsed_ms(blob_read_start);
            stats.binary_blob_bytes_read += len;
        }
        match candidate.as_slice().cmp(key) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => {
                found = Some(id);
                break;
            }
        }
    }
    stats.read_all_ms = elapsed_ms(search_start);
    stats.result_array_len = usize::from(found.is_some());
    stats.found_id = found;
    stats.total_ms = elapsed_ms(lookup_start);
    log::debug!(
        "[cottas_native_ids::lookup_term_id_from_sidecar] binary term_to_id resolved column={:?}, term {:?} to {:?} in {:.3}ms; probes={}, entry_bytes={}, blob_bytes={}, open_ms={:.3}, metadata_ms={:.3}, search_ms={:.3}, entry_read_ms={:.3}, blob_read_ms={:.3}",
        column,
        term,
        found,
        stats.total_ms,
        stats.binary_probe_count,
        stats.binary_entry_bytes_read,
        stats.binary_blob_bytes_read,
        stats.open_ms,
        stats.binary_metadata_ms,
        stats.read_all_ms,
        stats.binary_entry_read_ms,
        stats.binary_blob_read_ms
    );
    Ok((found, stats))
}

fn write_term_to_id_binary_sidecar_from_term_runs(
    term_run_paths: &[PathBuf],
    data_path: &Path,
) -> Result<()> {
    let write_start = Instant::now();
    let entries_path = native_dict_term_to_id_entries_path(data_path);
    let blob_path = native_dict_term_to_id_blob_path(data_path);
    let mut readers = Vec::with_capacity(term_run_paths.len());
    for path in term_run_paths {
        readers.push(PairRunReader::new(path)?);
    }
    let mut heap = BinaryHeap::new();
    for run_idx in 0..readers.len() {
        if let Some(pair) = readers[run_idx].read_one()? {
            heap.push(PairHeapItem {
                pair,
                run_idx,
                order: PairRunOrder::Term,
            });
        }
    }
    let blob_file = std::fs::File::create(&blob_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let entries_file = std::fs::File::create(&entries_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let mut blob_writer = BufWriter::new(blob_file);
    let mut entries_writer = BufWriter::new(entries_file);
    let mut current_offset = 0u64;
    let mut pairs_written = 0usize;
    let mut last_term: Option<String> = None;
    while let Some(item) = heap.pop() {
        let run_idx = item.run_idx;
        let pair = item.pair;
        if let Some(prev) = &last_term {
            if pair.term <= *prev {
                return Err(VortexRdfError::Serialization(format!(
                    "Non-increasing term_to_id order: {:?} then {:?}",
                    prev, pair.term
                )));
            }
        }
        let start = current_offset;
        blob_writer
            .write_all(pair.term.as_bytes())
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        current_offset = current_offset
            .checked_add(pair.term.as_bytes().len() as u64)
            .ok_or_else(|| {
                VortexRdfError::Serialization(
                    "term_to_id binary blob offset overflowed u64".to_string(),
                )
            })?;
        let end = current_offset;
        entries_writer
            .write_all(&start.to_le_bytes())
            .and_then(|_| entries_writer.write_all(&end.to_le_bytes()))
            .and_then(|_| entries_writer.write_all(&pair.id.to_le_bytes()))
            .and_then(|_| entries_writer.write_all(&0u32.to_le_bytes()))
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        last_term = Some(pair.term);
        pairs_written += 1;
        if let Some(next_pair) = readers[run_idx].read_one()? {
            heap.push(PairHeapItem {
                pair: next_pair,
                run_idx,
                order: PairRunOrder::Term,
            });
        }
    }
    blob_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    entries_writer
        .flush()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    log::info!(
        "[cottas_native_ids] wrote binary term_to_id sidecars pairs={}, entry_bytes={}, blob_bytes={} to {:?} and {:?} in {:?}",
        pairs_written,
        pairs_written as u64 * NATIVE_TERM_TO_ID_ENTRY_BYTES,
        current_offset,
        entries_path,
        blob_path,
        write_start.elapsed()
    );
    Ok(())
}
async fn lookup_term_id_from_sidecar(data_path: &Path, term: &str) -> Result<Option<u32>> {
    let (id, _stats) = lookup_term_id_from_sidecar_with_stats(data_path, term, None).await?;
    Ok(id)
}

async fn lookup_term_id_from_sidecar_with_stats(
    data_path: &Path,
    term: &str,
    column: Option<&'static str>,
) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
    if native_term_to_id_binary_sidecar_exists(data_path) {
        return lookup_term_id_from_binary_index_with_stats(data_path, term, column);
    }

    let lookup_start = Instant::now();
    let mut stats = NativeTermToIdLookupStats {
        column: column.map(|c| c.to_string()),
        term_len: term.len(),
        term_preview: native_term_preview(term),
        strategy: "vortex-sidecar-scan".to_string(),
        ..NativeTermToIdLookupStats::default()
    };
    let path = native_dict_term_to_id_path(data_path);
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    stats.open_ms = elapsed_ms(open_start);

    let expr = eq(col("term"), lit(term));
    let can_prune_start = Instant::now();
    match file.can_prune(&expr) {
        Ok(can_prune) => stats.can_prune = Some(can_prune),
        Err(_) => stats.can_prune = None,
    }
    stats.can_prune_ms = elapsed_ms(can_prune_start);

    let scan_build_start = Instant::now();
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
    stats.scan_build_ms = elapsed_ms(scan_build_start);

    let read_all_start = Instant::now();
    let ids = stream.read_all().await.map_err(VortexRdfError::from)?;
    stats.read_all_ms = elapsed_ms(read_all_start);
    stats.result_array_len = ids.len();

    let extract_start = Instant::now();
    let id = extract_first_u32_from_single_column_array(&ids, "id")?;
    stats.extract_ms = elapsed_ms(extract_start);
    stats.found_id = id;
    stats.total_ms = elapsed_ms(lookup_start);

    log::debug!(
        "[cottas_native_ids::lookup_term_id_from_sidecar] resolved column={:?}, term {:?} to {:?} in {:.3}ms; strategy={}, open_ms={:.3}, can_prune_ms={:.3}, scan_build_ms={:.3}, read_all_ms={:.3}, extract_ms={:.3}, result_array_len={}, can_prune={:?}",
        column,
        term,
        id,
        stats.total_ms,
        stats.strategy,
        stats.open_ms,
        stats.can_prune_ms,
        stats.scan_build_ms,
        stats.read_all_ms,
        stats.extract_ms,
        stats.result_array_len,
        stats.can_prune
    );
    Ok((id, stats))
}
