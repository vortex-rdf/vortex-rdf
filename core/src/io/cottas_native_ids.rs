use crate::error::{Result, VortexRdfError};
use crate::index::{RdfDictionary, SimpleDictionaryView};
use crate::io::utils::CottasVortexCompressionProfile;
use crate::store::layout::cottas::TripleOrdering;

use futures::{Stream, StreamExt};
use oxrdf::Quad;
use std::cmp::Ordering;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Instant;
use vortex::VortexSessionDefault;
use vortex_error::{VortexError, VortexResult};

use std::collections::{BinaryHeap, HashMap, HashSet};
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef, IntoArray};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use oxrdfio::{RdfFormat, RdfSerializer};

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

    let id_run_paths =
        encode_string_runs_to_id_runs::<Dict>(&dictionary, &string_run_paths, temp_dir.path())?;

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
    temp_dir: &Path,
) -> Result<Vec<PathBuf>>
where
    Dict: RdfDictionary,
{
    let mut id_run_paths = Vec::with_capacity(string_run_paths.len());

    for (run_idx, string_path) in string_run_paths.iter().enumerate() {
        let id_path = temp_dir.join(format!("native_id_encoded_run_{run_idx:06}.bin"));
        let mut reader = NativeStringRunReader::new(string_path)?;
        let file = std::fs::File::create(&id_path)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
        let mut writer = BufWriter::new(file);

        while let Some(triple) = reader.read_one()? {
            let encoded = NativeIdTriple {
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
            };
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

// -----------------------------------------------------------------------------
// Expected existing functions from your current cottas_native_ids.rs
// -----------------------------------------------------------------------------
// Keep these implementations from the current file unchanged and paste this
// serializer block above them / use it to replace only the old serializer helpers:
//
//   fn build_dictionary_root<Dict: RdfDictionary>(&Dict) -> Result<ArrayRef>
//   async fn write_single_array_to_vortex_file<W>(...) -> Result<()>
//   async fn write_dictionary_lookup_sidecars<Dict: RdfDictionary>(...) -> Result<()>
//
// Also keep the existing match/count/query-time functions unchanged:
//
//   load_cottas_native_dictionary
//   load_cottas_native_simple_dictionary_view
//   match_cottas_native_file(_with_diagnostics)
//   count_cottas_native_ids_file_with_diagnostics(_mode)
//   lookup_term_id_from_sidecar
//   lookup_terms_by_ids_from_sidecar
//
// The important long-term change is that the final data merge is ID-only:
//
//   string runs -> dictionary -> encoded ID runs -> merge u32 tuples -> Vortex
//
// There is no Vec<NativeTriple> and no String allocation in the final merge.
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

fn rdf_err_to_vortex_err(e: VortexRdfError) -> VortexError {
    vortex_error::vortex_err!(
        "vortex-rdf error while streaming native string row group: {}",
        e
    )
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

fn native_id_lookup_small_or_threshold() -> usize {
    std::env::var("VORTEX_RDF_NATIVE_ID_LOOKUP_SMALL_OR_IDS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(32)
}

async fn lookup_terms_by_ids_from_sidecar(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let small_or_threshold = native_id_lookup_small_or_threshold();

    if ids.len() <= small_or_threshold {
        lookup_terms_by_ids_from_sidecar_small_or(data_path, ids).await
    } else {
        lookup_terms_by_ids_from_sidecar_streaming(data_path, ids).await
    }
}

async fn lookup_terms_by_ids_from_sidecar_small_or(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    let lookup_start = Instant::now();
    let path = native_dict_id_to_term_path(data_path);

    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;

    let Some(expr) = build_id_lookup_filter(ids) else {
        return Ok(HashMap::new());
    };

    if let Ok(can_prune) = file.can_prune(&expr) {
        log::debug!(
            "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] small OR can_prune(ids={}) = {}",
            ids.len(),
            can_prune
        );
    }

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] small OR lookup for {} ids",
        ids.len()
    );

    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(expr)
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let rows = stream.read_all().await.map_err(VortexRdfError::from)?;
    let map = extract_id_term_map(&rows)?;

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] small OR resolved {} / {} ids in {:?}",
        map.len(),
        ids.len(),
        lookup_start.elapsed()
    );

    Ok(map)
}

async fn lookup_terms_by_ids_from_sidecar_streaming(
    data_path: &Path,
    ids: &[u32],
) -> Result<HashMap<u32, String>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let lookup_start = Instant::now();
    let requested: HashSet<u32> = ids.iter().copied().collect();
    let mut out: HashMap<u32, String> = HashMap::with_capacity(requested.len());

    let path = native_dict_id_to_term_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] streaming sidecar scan for {} requested ids",
        requested.len()
    );

    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;

    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let mut batches = 0usize;
    let mut decoded_rows = 0usize;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(VortexRdfError::from)?;
        batches += 1;
        decoded_rows += batch.len();

        if batch.len() == 0 {
            continue;
        }

        let struct_array = batch
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
            .map_err(VortexRdfError::Vortex)?
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let batch_ids = id_col.as_slice::<u32>();

        for row in 0..batch.len() {
            let id = batch_ids[row];

            if !requested.contains(&id) || out.contains_key(&id) {
                continue;
            }

            let raw = term_col.bytes_at(row);
            let term = String::from_utf8(raw.as_ref().to_vec()).map_err(|e| {
                VortexRdfError::Deserialization(format!(
                    "Dictionary term is not valid UTF-8 at sidecar row {}: {}",
                    row, e
                ))
            })?;

            out.insert(id, term);

            if out.len() >= requested.len() {
                log::debug!(
                    "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] resolved all {} ids after {} batches / {} decoded rows in {:?}",
                    out.len(),
                    batches,
                    decoded_rows,
                    lookup_start.elapsed()
                );
                return Ok(out);
            }
        }
    }

    log::debug!(
        "[cottas_native_ids::lookup_terms_by_ids_from_sidecar] resolved {} / {} ids after full streaming scan, {} batches / {} decoded rows in {:?}",
        out.len(),
        requested.len(),
        batches,
        decoded_rows,
        lookup_start.elapsed()
    );

    Ok(out)
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
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let ids = id_col.as_slice::<u32>();

    for row in 0..array.len() {
        let raw = term_col.bytes_at(row);
        let term = String::from_utf8(raw.as_ref().to_vec()).map_err(|e| {
            VortexRdfError::Deserialization(format!(
                "Dictionary term is not valid UTF-8 at row {}: {}",
                row, e
            ))
        })?;

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
                "[cottas_native::match] native file has {} scan splits.",
                splits.len(),
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

    log::debug!(
        "[cottas_native_ids::match] entering lazy RDF materialization for {} rows",
        matched_quads.len()
    );

    let materialize_start = Instant::now();
    let write_stats =
        write_quads_array_as_rdf_lazy(input_path, matched_quads, writer, format).await?;

    log::debug!(
        "[cottas_native_ids::match] lazy RDF materialization finished in {:?}: rows_out={}, unique_ids_requested={}, unique_ids_loaded={}, id_extract_ms={:.3}, id_lookup_ms={:.3}, serialize_ms={:.3}",
        materialize_start.elapsed(),
        write_stats.rows_out,
        write_stats.unique_ids_requested,
        write_stats.unique_ids_loaded,
        write_stats.id_extract_ms,
        write_stats.id_to_term_lookup_ms,
        write_stats.serialize_ms
    );

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

async fn write_dictionary_lookup_sidecars_from_pair_runs(
    pair_run_paths: &PairRunPaths,
    data_path: &Path,
    row_group_size: usize,
) -> Result<()> {
    let term_to_id_path = native_dict_term_to_id_path(data_path);
    let id_to_term_path = native_dict_id_to_term_path(data_path);

    let mut term_to_id_file = tokio::fs::File::create(&term_to_id_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_pair_runs_as_lookup_sidecar(
        &mut term_to_id_file,
        pair_run_paths.term_run_paths.clone(),
        PairRunOrder::Term,
        LookupSidecarKind::TermToId,
        row_group_size,
    )
    .await?;

    let mut id_to_term_file = tokio::fs::File::create(&id_to_term_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    write_pair_runs_as_lookup_sidecar(
        &mut id_to_term_file,
        pair_run_paths.id_run_paths.clone(),
        PairRunOrder::Id,
        LookupSidecarKind::IdToTerm,
        row_group_size,
    )
    .await?;

    log::info!(
        "[cottas_native_ids] wrote streaming dictionary sidecars {:?} and {:?}",
        term_to_id_path,
        id_to_term_path
    );

    Ok(())
}
#[derive(Clone, Copy, Debug)]
enum LookupSidecarKind {
    TermToId,
    IdToTerm,
}
async fn write_pair_runs_as_lookup_sidecar<W>(
    writer: &mut W,
    run_paths: Vec<PathBuf>,
    order: PairRunOrder,
    kind: LookupSidecarKind,
    row_group_size: usize,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let row_group_size = row_group_size.max(1);

    let dtype = match kind {
        LookupSidecarKind::TermToId => build_term_to_id_lookup_array(&[])?.dtype().clone(),
        LookupSidecarKind::IdToTerm => build_id_to_term_lookup_array(&[])?.dtype().clone(),
    };

    let array_stream =
        merge_pair_runs_to_lookup_array_stream(run_paths, order, kind, row_group_size)?;

    let stream = ArrayStreamAdapter::new(dtype, Box::pin(array_stream));

    let write_opts = NATIVE_FILE_SESSION.write_options().with_strategy(
        WriteStrategyBuilder::default()
            .with_row_block_size(row_group_size)
            .build(),
    );

    write_opts
        .write(writer, stream)
        .await
        .map_err(VortexRdfError::from)?;

    Ok(())
}
fn merge_pair_runs_to_lookup_array_stream(
    run_paths: Vec<PathBuf>,
    order: PairRunOrder,
    kind: LookupSidecarKind,
    row_group_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);

    Ok(async_stream::try_stream! {
        let mut readers = Vec::with_capacity(run_paths.len());

        for path in &run_paths {
            readers.push(PairRunReader::new(path).map_err(rdf_err_to_vortex_err)?);
        }

        let mut heap = BinaryHeap::new();

        for run_idx in 0..readers.len() {
            if let Some(pair) = readers[run_idx]
                .read_one()
                .map_err(rdf_err_to_vortex_err)?
            {
                heap.push(PairHeapItem {
                    pair,
                    run_idx,
                    order,
                });
            }
        }

        let mut chunk: Vec<NativeDictPair> = Vec::with_capacity(row_group_size);

        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            chunk.push(item.pair);

            if let Some(next_pair) = readers[run_idx]
                .read_one()
                .map_err(rdf_err_to_vortex_err)?
            {
                heap.push(PairHeapItem {
                    pair: next_pair,
                    run_idx,
                    order,
                });
            }

            if chunk.len() >= row_group_size {
                let array = build_lookup_array_from_pairs(&chunk, kind)
                    .map_err(rdf_err_to_vortex_err)?;
                chunk.clear();
                yield array;
            }
        }

        if !chunk.is_empty() {
            yield build_lookup_array_from_pairs(&chunk, kind)
                .map_err(rdf_err_to_vortex_err)?;
        }
    })
}
fn build_lookup_array_from_pairs(
    pairs: &[NativeDictPair],
    kind: LookupSidecarKind,
) -> Result<ArrayRef> {
    match kind {
        LookupSidecarKind::TermToId => {
            let temp: Vec<(u32, String)> = pairs.iter().map(|p| (p.id, p.term.clone())).collect();

            build_term_to_id_lookup_array(&temp)
        }

        LookupSidecarKind::IdToTerm => {
            let temp: Vec<(u32, String)> = pairs.iter().map(|p| (p.id, p.term.clone())).collect();

            build_id_to_term_lookup_array(&temp)
        }
    }
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
