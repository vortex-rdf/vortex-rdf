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
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;
use vortex::VortexSessionDefault;
use vortex_error::{VortexError, VortexResult};

use std::collections::{BinaryHeap, HashMap, HashSet};
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray, VarBinArray, VarBinViewArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef, IntoArray};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::Buffer;
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
            dict_row_group_size: 1_024,
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum NativeComponent {
    DictionaryVortex,
    DictionaryTermToIdVortex,
    DictionaryTermDirectoryVortex,
    SubjectRangesVortex,
    PredicateDirectoryVortexV2,
    PredicateRangesVortexV2,
    PredicateObjectPartitionsVortexV2,
    PredicateObjectDirectoryVortexV2,
    PredicateObjectRangesVortexV2,
    ObjectDirectoryVortexV2,
    ObjectRangesVortexV2,
}

impl NativeComponent {
    fn logical_name(self) -> &'static str {
        match self {
            Self::DictionaryVortex => "rdf.dictionary.id-to-term.vortex",
            Self::DictionaryTermToIdVortex => "rdf.dictionary.term-to-id.vortex",
            Self::DictionaryTermDirectoryVortex => "rdf.dictionary.term-directory.vortex-v1",
            Self::SubjectRangesVortex => "rdf.index.subject.ranges.vortex-v1",
            Self::PredicateDirectoryVortexV2 => "rdf.index.p.directory.vortex-v2",
            Self::PredicateRangesVortexV2 => "rdf.index.p.ranges.vortex-v2",
            Self::PredicateObjectPartitionsVortexV2 => "rdf.index.po.partitions.vortex-v2",
            Self::PredicateObjectDirectoryVortexV2 => "rdf.index.po.directory.vortex-v2",
            Self::PredicateObjectRangesVortexV2 => "rdf.index.po.ranges.vortex-v2",
            Self::ObjectDirectoryVortexV2 => "rdf.index.o.directory.vortex-v2",
            Self::ObjectRangesVortexV2 => "rdf.index.o.ranges.vortex-v2",
        }
    }

    fn external_suffix(self) -> &'static str {
        match self {
            Self::DictionaryVortex => "dict.vortex",
            Self::DictionaryTermToIdVortex => "dict.term_to_id.vortex",
            Self::DictionaryTermDirectoryVortex => "dict.term_directory.v1.vortex",
            Self::SubjectRangesVortex => "subject_ranges.v1.vortex",
            Self::PredicateDirectoryVortexV2 => "p_exact_directory.v2.vortex",
            Self::PredicateRangesVortexV2 => "p_exact_ranges.v2.vortex",
            Self::PredicateObjectPartitionsVortexV2 => "po_predicate_partitions.v2.vortex",
            Self::PredicateObjectDirectoryVortexV2 => "po_exact_directory.v2.vortex",
            Self::PredicateObjectRangesVortexV2 => "po_exact_ranges.v2.vortex",
            Self::ObjectDirectoryVortexV2 => "o_exact_directory.v2.vortex",
            Self::ObjectRangesVortexV2 => "o_exact_ranges.v2.vortex",
        }
    }

    fn external_path(self, data_path: &Path) -> PathBuf {
        let name = data_path
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("data.vortex");
        data_path.with_file_name(format!("{name}.{}", self.external_suffix()))
    }
}

#[derive(Clone, Debug)]
enum ComponentLocation {
    External(PathBuf),
    Embedded {
        artifact_path: PathBuf,
        component: NativeComponent,
    },
}

#[derive(Clone, Debug)]
struct NativeComponentResolver {
    artifact_path: PathBuf,
}

impl NativeComponentResolver {
    fn new(artifact_path: &Path) -> Self {
        Self {
            artifact_path: artifact_path.to_path_buf(),
        }
    }

    fn location(&self, component: NativeComponent) -> ComponentLocation {
        // Phase B keeps the proven external Vortex components. Phase C changes
        // this single decision to Embedded using the artifact manifest.
        ComponentLocation::External(component.external_path(&self.artifact_path))
    }

    fn external_path(&self, component: NativeComponent) -> Result<PathBuf> {
        match self.location(component) {
            ComponentLocation::External(path) => Ok(path),
            ComponentLocation::Embedded {
                artifact_path,
                component,
            } => Err(VortexRdfError::InvalidOperation(format!(
                "embedded component {} in {:?} requires the Phase-C component reader",
                component.logical_name(),
                artifact_path
            ))),
        }
    }
}

fn native_component_path(data_path: &Path, component: NativeComponent) -> PathBuf {
    NativeComponentResolver::new(data_path)
        .external_path(component)
        .expect("external component resolution is infallible before Phase C")
}

fn require_vortex_component(
    data_path: &Path,
    component: NativeComponent,
    label: &str,
) -> Result<PathBuf> {
    let path = native_component_path(data_path, component);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "required {label} component {} is missing at {:?}",
            component.logical_name(),
            path
        )));
    }
    Ok(path)
}

fn native_dict_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::DictionaryVortex)
}

fn native_dict_term_to_id_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::DictionaryTermToIdVortex)
}

fn native_dict_term_directory_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::DictionaryTermDirectoryVortex)
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

// VORTEX_RDF_COMPACT_PYTHON_RESULT_V1
#[derive(Clone, Debug, Default)]
pub struct NativeCompactTripleBatch {
    pub terms: Vec<String>,
    pub rows: Vec<(u32, u32, u32)>,
}

fn intern_compact_term(
    value: &str,
    terms: &mut Vec<String>,
    indexes: &mut HashMap<String, u32>,
) -> Result<u32> {
    if let Some(index) = indexes.get(value) {
        return Ok(*index);
    }
    let index = u32::try_from(terms.len()).map_err(|_| {
        VortexRdfError::InvalidOperation("compact query term table exceeds u32".into())
    })?;
    let owned = value.to_owned();
    terms.push(owned.clone());
    indexes.insert(owned, index);
    Ok(index)
}

// VORTEX_RDF_DIRECT_COMPACT_DECODER_V1
async fn projected_native_id_rows_as_compact_triples_legacy_hashmaps(
    data_path: &Path,
    rows: &NativeProjectedIdRows,
    bound: &BoundNativeRdfTerms,
) -> Result<NativeCompactTripleBatch> {
    if rows.rows == 0 {
        return Ok(NativeCompactTripleBatch::default());
    }
    let unique_ids = collect_unique_ids_from_projected_unbound_rows(rows, bound);
    let id_to_term = lookup_terms_by_ids_from_sidecar(data_path, &unique_ids).await?;
    let mut terms = Vec::with_capacity(id_to_term.len().saturating_add(3));
    let mut lexical_indexes = HashMap::with_capacity(terms.capacity());
    let mut id_indexes = HashMap::with_capacity(id_to_term.len());
    for id in unique_ids {
        let value = id_to_term.get(&id).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "ID {id} missing during compact reconstruction"
            ))
        })?;
        id_indexes.insert(
            id,
            intern_compact_term(value, &mut terms, &mut lexical_indexes)?,
        );
    }
    let bound_s = bound
        .s
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    let bound_p = bound
        .p
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    let bound_o = bound
        .o
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    compact_rows_from_id_indexes(rows, bound, terms, id_indexes, bound_s, bound_p, bound_o)
}

fn append_bound_compact_term(
    value: Option<&str>,
    terms: &mut Vec<String>,
    bound_indexes: &mut HashMap<String, u32>,
) -> Result<Option<u32>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(index) = bound_indexes.get(value) {
        return Ok(Some(*index));
    }
    let index = u32::try_from(terms.len()).map_err(|_| {
        VortexRdfError::InvalidOperation("compact query term table exceeds u32".into())
    })?;
    let owned = value.to_owned();
    terms.push(owned.clone());
    bound_indexes.insert(owned, index);
    Ok(Some(index))
}

fn compact_rows_from_id_indexes(
    rows: &NativeProjectedIdRows,
    bound: &BoundNativeRdfTerms,
    terms: Vec<String>,
    id_indexes: HashMap<u32, u32>,
    bound_s: Option<u32>,
    bound_p: Option<u32>,
    bound_o: Option<u32>,
) -> Result<NativeCompactTripleBatch> {
    let resolve = |fixed: Option<u32>, id: Option<u32>, label: &str| -> Result<u32> {
        if let Some(index) = fixed {
            return Ok(index);
        }
        let id = id.ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} projected ID missing for compact output"
            ))
        })?;
        id_indexes.get(&id).copied().ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} ID {id} missing from compact query dictionary"
            ))
        })
    };
    let mut compact = Vec::with_capacity(rows.rows);
    for i in 0..rows.rows {
        compact.push((
            resolve(bound_s, projected_id_at(&rows.s, &bound.s, i, "S")?, "S")?,
            resolve(bound_p, projected_id_at(&rows.p, &bound.p, i, "P")?, "P")?,
            resolve(bound_o, projected_id_at(&rows.o, &bound.o, i, "O")?, "O")?,
        ));
    }
    Ok(NativeCompactTripleBatch {
        terms,
        rows: compact,
    })
}

async fn projected_native_id_rows_as_compact_triples_direct_v1(
    data_path: &Path,
    rows: &NativeProjectedIdRows,
    bound: &BoundNativeRdfTerms,
) -> Result<NativeCompactTripleBatch> {
    if rows.rows == 0 {
        return Ok(NativeCompactTripleBatch::default());
    }

    let mut requested = collect_unique_ids_from_projected_unbound_rows(rows, bound);
    requested.sort_unstable();
    requested.dedup();

    let path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryVortex,
        "ID-to-term dictionary",
    )?;
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let indices = Buffer::from(
        requested
            .iter()
            .map(|id| u64::from(*id))
            .collect::<Vec<_>>(),
    );
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_indices(indices)
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    if array.len() != requested.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "direct compact dictionary selection returned {} rows for {} requested IDs",
            array.len(),
            requested.len()
        )));
    }

    // Execute both projected columns once. Each lexical value is allocated
    // directly in the final compact term table; no HashMap<u32, String> and no
    // second lexical HashMap<String, u32> are constructed.
    let mut ctx = NATIVE_FILE_SESSION.create_execution_ctx();
    let struct_array = array
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let id_column = struct_array
        .unmasked_field_by_name("id")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let term_column = struct_array
        .unmasked_field_by_name("term")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let loaded_ids = id_column.as_slice::<u32>();
    if loaded_ids.len() != requested.len() || term_column.len() != requested.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "direct compact dictionary column mismatch: ids={}, terms={}, requested={}",
            loaded_ids.len(),
            term_column.len(),
            requested.len()
        )));
    }

    let mut terms = Vec::with_capacity(requested.len().saturating_add(3));
    let mut id_indexes = HashMap::with_capacity(requested.len());
    for (index, (&expected, &actual)) in requested.iter().zip(loaded_ids).enumerate() {
        if expected != actual {
            return Err(VortexRdfError::Deserialization(format!(
                "direct compact ID invariant failed at position {index}: requested {expected}, loaded {actual}"
            )));
        }
        let term_bytes = term_column.bytes_at(index);
        let lexical = std::str::from_utf8(&term_bytes).map_err(|error| {
            VortexRdfError::Deserialization(format!(
                "direct compact dictionary term for ID {actual} is invalid UTF-8: {error}"
            ))
        })?;
        let compact_index = u32::try_from(terms.len()).map_err(|_| {
            VortexRdfError::InvalidOperation("compact query term table exceeds u32".into())
        })?;
        terms.push(lexical.to_owned());
        if id_indexes.insert(actual, compact_index).is_some() {
            return Err(VortexRdfError::Deserialization(format!(
                "direct compact dictionary returned duplicate ID {actual}"
            )));
        }
    }

    // Bound terms are at most three output values. Deduplicate only among those
    // values, avoiding a lexical hash table over the entire decoded dictionary.
    // A bound lexical value may duplicate a decoded unbound value; retaining that
    // extra entry is semantically harmless and bounds the duplicate count to three.
    let mut bound_indexes = HashMap::with_capacity(3);
    let bound_s = append_bound_compact_term(bound.s.as_deref(), &mut terms, &mut bound_indexes)?;
    let bound_p = append_bound_compact_term(bound.p.as_deref(), &mut terms, &mut bound_indexes)?;
    let bound_o = append_bound_compact_term(bound.o.as_deref(), &mut terms, &mut bound_indexes)?;
    compact_rows_from_id_indexes(rows, bound, terms, id_indexes, bound_s, bound_p, bound_o)
}

async fn projected_native_id_rows_as_compact_triples(
    data_path: &Path,
    rows: &NativeProjectedIdRows,
    bound: &BoundNativeRdfTerms,
) -> Result<NativeCompactTripleBatch> {
    let strategy = std::env::var("VORTEX_RDF_COMPACT_RECONSTRUCTION_STRATEGY")
        .unwrap_or_else(|_| "legacy-hashmaps".to_string());
    match strategy.as_str() {
        "legacy-hashmaps" => {
            projected_native_id_rows_as_compact_triples_legacy_hashmaps(data_path, rows, bound)
                .await
        }
        "direct-v1" => {
            projected_native_id_rows_as_compact_triples_direct_v1(data_path, rows, bound).await
        }
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_COMPACT_RECONSTRUCTION_STRATEGY={other:?}; expected legacy-hashmaps or direct-v1"
        ))),
    }
}

/// Transfers each lexical term once and rows as indexes into that query-local table.
pub async fn match_cottas_native_file_as_compact_triples(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeCompactTripleBatch> {
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    projected_native_id_rows_as_compact_triples(input_path, &planned.rows, &planned.bound_terms)
        .await
}

// VORTEX_RDF_NATIVE_RESULT_PIPELINE_DIAGNOSTICS_V1
/// Diagnostic timings for the Rust-native result path. `ids_only_ms` ends at
/// NativeProjectedIdRows and is the closest current proxy for a DataFusion handoff.
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeResultPipelineDiagnostics {
    pub rows_out: usize,
    pub projected_columns: usize,
    pub projected_values: usize,
    pub unique_ids: usize,
    pub terms_loaded: usize,
    pub lexical_bytes: usize,
    pub compact_rows: usize,
    pub term_lookup_ms: f64,
    pub access_index_lookup_ms: f64,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub scan_read_and_id_extract_ms: f64,
    pub ids_only_ms: f64,
    pub unique_id_collect_ms: f64,
    pub id_to_term_ms: f64,
    pub compact_intern_and_row_build_ms: f64,
    pub compact_native_ms: f64,
    pub total_rust_ms: f64,
    pub id_to_term_stats: NativeIdToTermLookupStats,
    pub access_index_strategy: String,
    pub access_execution_strategy: String,
}

pub async fn diagnose_cottas_native_result_pipeline(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<NativeResultPipelineDiagnostics> {
    let total_start = Instant::now();
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    let ids_only_ms = elapsed_ms(total_start);
    let rows = &planned.rows;
    let projected_columns = [&rows.s, &rows.p, &rows.o, &rows.g]
        .into_iter()
        .filter(|column| column.is_some())
        .count();
    let projected_values = [&rows.s, &rows.p, &rows.o, &rows.g]
        .into_iter()
        .filter_map(|column| column.as_ref())
        .map(Vec::len)
        .sum();

    let unique_start = Instant::now();
    let unique_ids = collect_unique_ids_from_projected_unbound_rows(rows, &planned.bound_terms);
    let unique_id_collect_ms = elapsed_ms(unique_start);

    let lookup_start = Instant::now();
    let (id_to_term, id_to_term_stats) =
        lookup_terms_by_ids_from_sidecar_with_stats(input_path, &unique_ids).await?;
    let id_to_term_ms = elapsed_ms(lookup_start);

    let compact_start = Instant::now();
    let mut terms = Vec::with_capacity(id_to_term.len().saturating_add(3));
    let mut lexical_indexes = HashMap::with_capacity(terms.capacity());
    let mut id_indexes = HashMap::with_capacity(id_to_term.len());
    for id in &unique_ids {
        let value = id_to_term.get(id).ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "ID {id} missing during diagnostic compact reconstruction"
            ))
        })?;
        id_indexes.insert(
            *id,
            intern_compact_term(value, &mut terms, &mut lexical_indexes)?,
        );
    }
    let bound_s = planned
        .bound_terms
        .s
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    let bound_p = planned
        .bound_terms
        .p
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    let bound_o = planned
        .bound_terms
        .o
        .as_deref()
        .map(|v| intern_compact_term(v, &mut terms, &mut lexical_indexes))
        .transpose()?;
    let resolve = |fixed: Option<u32>, id: Option<u32>, label: &str| -> Result<u32> {
        if let Some(index) = fixed {
            return Ok(index);
        }
        let id = id.ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} projected ID missing in pipeline diagnostic"
            ))
        })?;
        id_indexes.get(&id).copied().ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "{label} ID {id} missing from diagnostic compact dictionary"
            ))
        })
    };
    let mut compact_rows = Vec::with_capacity(rows.rows);
    for i in 0..rows.rows {
        compact_rows.push((
            resolve(
                bound_s,
                projected_id_at(&rows.s, &planned.bound_terms.s, i, "S")?,
                "S",
            )?,
            resolve(
                bound_p,
                projected_id_at(&rows.p, &planned.bound_terms.p, i, "P")?,
                "P",
            )?,
            resolve(
                bound_o,
                projected_id_at(&rows.o, &planned.bound_terms.o, i, "O")?,
                "O",
            )?,
        ));
    }
    let compact_intern_and_row_build_ms = elapsed_ms(compact_start);
    let lexical_bytes = terms.iter().map(String::len).sum();
    let diagnostics = &planned.diagnostics;
    Ok(NativeResultPipelineDiagnostics {
        rows_out: rows.rows,
        projected_columns,
        projected_values,
        unique_ids: unique_ids.len(),
        terms_loaded: terms.len(),
        lexical_bytes,
        compact_rows: compact_rows.len(),
        term_lookup_ms: diagnostics.term_lookup_ms,
        access_index_lookup_ms: diagnostics.access_index_lookup_ms,
        open_ms: diagnostics.open_ms,
        scan_build_ms: diagnostics.scan_build_ms,
        scan_read_and_id_extract_ms: diagnostics.read_all_ms,
        ids_only_ms,
        unique_id_collect_ms,
        id_to_term_ms,
        compact_intern_and_row_build_ms,
        compact_native_ms: unique_id_collect_ms + id_to_term_ms + compact_intern_and_row_build_ms,
        total_rust_ms: elapsed_ms(total_start),
        id_to_term_stats,
        access_index_strategy: diagnostics.access_index_strategy.clone(),
        access_execution_strategy: diagnostics.access_execution_strategy.clone(),
    })
}

// VORTEX_RDF_ADAPTIVE_ID_TO_TERM_BENCHMARK_V1
#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeIdDecodeStrategyTrial {
    pub strategy: String,
    pub status: String,
    pub max_gap: Option<u32>,
    pub scan_count: usize,
    pub rows_read: u64,
    pub terms_loaded: usize,
    pub open_ms: f64,
    pub read_ms: f64,
    pub extract_filter_ms: f64,
    pub total_ms: f64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeIdDecodeStrategyDiagnostics {
    pub rows_out: usize,
    pub ids_only_ms: f64,
    pub requested_ids: usize,
    pub min_id: Option<u32>,
    pub max_id: Option<u32>,
    pub id_span: u64,
    pub id_density: f64,
    pub exact_run_count: usize,
    pub max_range_scans: usize,
    pub trials: Vec<NativeIdDecodeStrategyTrial>,
}

fn merge_requested_id_ranges(ids: &[u32], max_gap: u32) -> Vec<Range<u64>> {
    let Some(&first) = ids.first() else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut start = first;
    let mut last = first;
    for &id in &ids[1..] {
        if id <= last.saturating_add(max_gap).saturating_add(1) {
            last = id;
        } else {
            ranges.push(u64::from(start)..u64::from(last) + 1);
            start = id;
            last = id;
        }
    }
    ranges.push(u64::from(start)..u64::from(last) + 1);
    ranges
}

async fn decode_ids_by_ranges_for_trial(
    data_path: &Path,
    requested: &HashSet<u32>,
    ranges: &[Range<u64>],
    strategy: &str,
    max_gap: Option<u32>,
) -> Result<(HashMap<u32, String>, NativeIdDecodeStrategyTrial)> {
    let total_start = Instant::now();
    let path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryVortex,
        "ID-to-term dictionary",
    )?;
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let mut out = HashMap::with_capacity(requested.len());
    let mut read_ms = 0.0;
    let mut extract_filter_ms = 0.0;
    let mut rows_read = 0u64;
    for range in ranges {
        rows_read = rows_read
            .checked_add(range.end - range.start)
            .ok_or_else(|| {
                VortexRdfError::InvalidOperation("diagnostic rows_read overflow".into())
            })?;
        let read_start = Instant::now();
        let array = file
            .scan()
            .map_err(VortexRdfError::from)?
            .with_row_range(range.clone())
            .with_projection(vortex_array::expr::select(
                ["id", "term"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?
            .read_all()
            .await
            .map_err(VortexRdfError::from)?;
        read_ms += elapsed_ms(read_start);
        let extract_start = Instant::now();
        let ids = extract_projected_u32_column(&array, "id")?;
        let terms = extract_projected_utf8_column(&array, "term")?;
        if ids.len() != terms.len() || ids.len() != array.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "{strategy} trial returned inconsistent dictionary columns"
            )));
        }
        for (id, term) in ids.into_iter().zip(terms) {
            if requested.contains(&id) && out.insert(id, term).is_some() {
                return Err(VortexRdfError::Deserialization(format!(
                    "{strategy} trial loaded duplicate ID {id}"
                )));
            }
        }
        extract_filter_ms += elapsed_ms(extract_start);
    }
    let trial = NativeIdDecodeStrategyTrial {
        strategy: strategy.to_string(),
        status: "ok".to_string(),
        max_gap,
        scan_count: ranges.len(),
        rows_read,
        terms_loaded: out.len(),
        open_ms,
        read_ms,
        extract_filter_ms,
        total_ms: elapsed_ms(total_start),
        error: None,
    };
    Ok((out, trial))
}

fn skipped_id_decode_trial(
    strategy: &str,
    max_gap: Option<u32>,
    scan_count: usize,
    rows_read: u64,
    reason: String,
) -> NativeIdDecodeStrategyTrial {
    NativeIdDecodeStrategyTrial {
        strategy: strategy.to_string(),
        status: "skipped".to_string(),
        max_gap,
        scan_count,
        rows_read,
        error: Some(reason),
        ..Default::default()
    }
}

/// Diagnostic only. It reuses the exact query-scoped ID set and validates every
/// completed alternative against the current row-index decoder before reporting it.
pub async fn diagnose_cottas_native_id_decode_strategies(
    input_path: &Path,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    max_range_scans: usize,
) -> Result<NativeIdDecodeStrategyDiagnostics> {
    let ids_start = Instant::now();
    let planned =
        execute_cottas_native_match(input_path, subject, predicate, object, graph).await?;
    let ids_only_ms = elapsed_ms(ids_start);
    let mut ids =
        collect_unique_ids_from_projected_unbound_rows(&planned.rows, &planned.bound_terms);
    ids.sort_unstable();
    ids.dedup();
    let min_id = ids.first().copied();
    let max_id = ids.last().copied();
    let id_span = match (min_id, max_id) {
        (Some(min), Some(max)) => u64::from(max) - u64::from(min) + 1,
        _ => 0,
    };
    let id_density = if id_span == 0 {
        0.0
    } else {
        ids.len() as f64 / id_span as f64
    };
    let exact_ranges = merge_requested_id_ranges(&ids, 0);
    let exact_run_count = exact_ranges.len();
    if ids.is_empty() {
        return Ok(NativeIdDecodeStrategyDiagnostics {
            rows_out: planned.rows.rows,
            ids_only_ms,
            requested_ids: 0,
            min_id,
            max_id,
            id_span,
            id_density,
            exact_run_count,
            max_range_scans,
            trials: Vec::new(),
        });
    }
    let requested: HashSet<u32> = ids.iter().copied().collect();
    let baseline_start = Instant::now();
    let (baseline, baseline_stats) =
        lookup_terms_by_ids_from_sidecar_with_stats(input_path, &ids).await?;
    let mut trials = vec![NativeIdDecodeStrategyTrial {
        strategy: "row-indices-current".to_string(),
        status: "ok".to_string(),
        max_gap: None,
        scan_count: 1,
        rows_read: ids.len() as u64,
        terms_loaded: baseline.len(),
        open_ms: baseline_stats.open_files_ms,
        read_ms: baseline_stats.blob_read_ms,
        extract_filter_ms: (baseline_stats.total_ms
            - baseline_stats.open_files_ms
            - baseline_stats.blob_read_ms)
            .max(0.0),
        total_ms: elapsed_ms(baseline_start),
        error: None,
    }];
    if baseline.len() != ids.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "baseline ID decoder loaded {} of {} requested IDs",
            baseline.len(),
            ids.len()
        )));
    }
    for max_gap in [0u32, 8, 64, 512] {
        let ranges = merge_requested_id_ranges(&ids, max_gap);
        let rows_read = range_rows(&ranges);
        let strategy = format!("merged-ranges-gap-{max_gap}");
        if ranges.len() > max_range_scans {
            trials.push(skipped_id_decode_trial(
                &strategy,
                Some(max_gap),
                ranges.len(),
                rows_read,
                format!("range count exceeds max_range_scans={max_range_scans}"),
            ));
            continue;
        }
        let (candidate, trial) = decode_ids_by_ranges_for_trial(
            input_path,
            &requested,
            &ranges,
            &strategy,
            Some(max_gap),
        )
        .await?;
        if candidate != baseline {
            return Err(VortexRdfError::Deserialization(format!(
                "{strategy} result differs from row-index baseline"
            )));
        }
        trials.push(trial);
    }
    let bounding = u64::from(min_id.unwrap())..u64::from(max_id.unwrap()) + 1;
    let (candidate, trial) = decode_ids_by_ranges_for_trial(
        input_path,
        &requested,
        std::slice::from_ref(&bounding),
        "bounding-range",
        None,
    )
    .await?;
    if candidate != baseline {
        return Err(VortexRdfError::Deserialization(
            "bounding-range result differs from row-index baseline".into(),
        ));
    }
    trials.push(trial);
    let dictionary_path = native_dict_path(input_path);
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&dictionary_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let read_start = Instant::now();
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let read_ms = elapsed_ms(read_start);
    let extract_start = Instant::now();
    let loaded_ids = extract_projected_u32_column(&array, "id")?;
    let terms = extract_projected_utf8_column(&array, "term")?;
    let mut full = HashMap::with_capacity(ids.len());
    for (id, term) in loaded_ids.into_iter().zip(terms) {
        if requested.contains(&id) {
            full.insert(id, term);
        }
    }
    let extract_filter_ms = elapsed_ms(extract_start);
    if full != baseline {
        return Err(VortexRdfError::Deserialization(
            "full-scan result differs from row-index baseline".into(),
        ));
    }
    trials.push(NativeIdDecodeStrategyTrial {
        strategy: "full-sequential-scan".to_string(),
        status: "ok".to_string(),
        max_gap: None,
        scan_count: 1,
        rows_read: array.len() as u64,
        terms_loaded: full.len(),
        open_ms,
        read_ms,
        extract_filter_ms,
        total_ms: open_ms + read_ms + extract_filter_ms,
        error: None,
    });
    Ok(NativeIdDecodeStrategyDiagnostics {
        rows_out: planned.rows.rows,
        ids_only_ms,
        requested_ids: ids.len(),
        min_id,
        max_id,
        id_span,
        id_density,
        exact_run_count,
        max_range_scans,
        trials,
    })
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
/// Dictionary access remains behind this contract until Phase C embeds it.
#[async_trait]
pub trait NativeDictionaryProvider: Send + Sync {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)>;

    async fn lookup_bound_term_ids(
        &self,
        terms: &[(String, &'static str)],
    ) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)>;
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
pub struct NativeRdfProviders {
    data_path: PathBuf,
}

impl NativeRdfProviders {
    pub fn new(data_path: &Path) -> Self {
        Self {
            data_path: data_path.to_path_buf(),
        }
    }
}

#[async_trait]
impl NativeDictionaryProvider for NativeRdfProviders {
    async fn lookup_term_id(
        &self,
        term: &str,
        column: Option<&'static str>,
    ) -> Result<(Option<u32>, NativeTermToIdLookupStats)> {
        lookup_term_id_from_sidecar_with_stats(&self.data_path, term, column).await
    }

    async fn lookup_bound_term_ids(
        &self,
        terms: &[(String, &'static str)],
    ) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
        lookup_bound_term_ids_from_sidecar_with_stats(&self.data_path, terms).await
    }

    async fn lookup_terms_by_ids(
        &self,
        ids: &[u32],
    ) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
        lookup_terms_by_ids_from_sidecar_with_stats(&self.data_path, ids).await
    }
}

#[async_trait]
impl NativeIndexProvider for NativeRdfProviders {
    async fn subject_range(&self, subject_id: u32) -> Result<Option<Range<u64>>> {
        require_vortex_component(
            &self.data_path,
            NativeComponent::SubjectRangesVortex,
            "subject index",
        )?;
        lookup_subject_range_from_vortex(&self.data_path, subject_id).await
    }

    async fn po_access(&self, predicate_id: u32, object_id: u32) -> Result<Option<NativePoAccess>> {
        require_vortex_component(
            &self.data_path,
            NativeComponent::PredicateObjectDirectoryVortexV2,
            "PO v2 directory",
        )?;
        require_vortex_component(
            &self.data_path,
            NativeComponent::PredicateObjectRangesVortexV2,
            "PO v2 payload",
        )?;
        lookup_po_access_from_vortex_v2(&self.data_path, predicate_id, object_id).await
    }

    async fn predicate_access(&self, predicate_id: u32) -> Result<Option<NativePredicateAccess>> {
        require_vortex_component(
            &self.data_path,
            NativeComponent::PredicateDirectoryVortexV2,
            "predicate v2 directory",
        )?;
        require_vortex_component(
            &self.data_path,
            NativeComponent::PredicateRangesVortexV2,
            "predicate v2 payload",
        )?;
        lookup_predicate_access_from_vortex_v2(&self.data_path, predicate_id).await
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
        "subject-ranges-vortex-v1"
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
    let mut requested = Vec::with_capacity(4);
    if let Some(value) = subject {
        requested.push((value.to_string(), "s"));
    }
    if let Some(value) = predicate {
        requested.push((value.to_string(), "p"));
    }
    if let Some(value) = object {
        requested.push((value.to_string(), "o"));
    }
    if let Some(value) = graph {
        requested.push((value.to_string(), "g"));
    }

    if requested.len() == 1 {
        let (term, column) = &requested[0];
        let (id, stats) = dictionary.lookup_term_id(term, Some(*column)).await?;
        let total_lookup_ms = stats.total_ms;
        let Some(id) = id else {
            return Ok((None, total_lookup_ms, vec![stats]));
        };
        let mut resolved = ResolvedNativePattern::default();
        match *column {
            "s" => resolved.s = Some(id),
            "p" => resolved.p = Some(id),
            "o" => resolved.o = Some(id),
            "g" => resolved.g = Some(id),
            _ => unreachable!("only native SPOG columns are requested"),
        }
        return Ok((Some(resolved), total_lookup_ms, vec![stats]));
    }

    let (ids, stats, total_lookup_ms) = dictionary.lookup_bound_term_ids(&requested).await?;
    if requested.iter().any(|(term, _)| !ids.contains_key(term)) {
        return Ok((None, total_lookup_ms, stats));
    }

    let mut resolved = ResolvedNativePattern::default();
    for (term, column) in requested {
        let id = ids[&term];
        match column {
            "s" => resolved.s = Some(id),
            "p" => resolved.p = Some(id),
            "o" => resolved.o = Some(id),
            "g" => resolved.g = Some(id),
            _ => unreachable!("only native SPOG columns are requested"),
        }
    }
    Ok((Some(resolved), total_lookup_ms, stats))
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
    let provider = NativeRdfProviders::new(data_path);
    let (terms, _stats) = provider.lookup_terms_by_ids(ids).await?;
    Ok(terms)
}

async fn lookup_terms_by_ids_from_sidecar_with_stats(
    data_path: &Path,
    ids: &[u32],
) -> Result<(HashMap<u32, String>, NativeIdToTermLookupStats)> {
    let total_start = Instant::now();
    let mut stats = NativeIdToTermLookupStats::default();
    stats.strategy = "vortex-id-row-selection".to_string();
    stats.requested_ids_in = ids.len();
    if ids.is_empty() {
        stats.total_ms = elapsed_ms(total_start);
        return Ok((HashMap::new(), stats));
    }
    let sort_start = Instant::now();
    let mut requested = ids.to_vec();
    requested.sort_unstable();
    requested.dedup();
    stats.sort_dedup_ms = elapsed_ms(sort_start);
    stats.requested_ids_unique = requested.len();
    let path = native_dict_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex id_to_term dictionary component is missing at {:?}",
            path
        )));
    }
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    stats.open_files_ms = elapsed_ms(open_start);
    let indices = Buffer::from(
        requested
            .iter()
            .map(|id| u64::from(*id))
            .collect::<Vec<_>>(),
    );
    let read_start = Instant::now();
    let array = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_row_indices(indices)
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    stats.blob_read_ms = elapsed_ms(read_start);
    let loaded_ids = extract_projected_u32_column(&array, "id")?;
    let terms = extract_projected_utf8_column(&array, "term")?;
    if loaded_ids.len() != requested.len() || terms.len() != requested.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "id_to_term selection returned ids={}, terms={}, requested={}",
            loaded_ids.len(),
            terms.len(),
            requested.len()
        )));
    }
    let mut out = HashMap::with_capacity(requested.len());
    for ((expected, actual), term) in requested.iter().zip(loaded_ids).zip(terms) {
        if *expected != actual {
            return Err(VortexRdfError::Deserialization(format!(
                "id_to_term row invariant failed: requested ID {}, row contained ID {}",
                expected, actual
            )));
        }
        out.insert(actual, term);
    }
    stats.ids_loaded = out.len();
    stats.total_ms = elapsed_ms(total_start);
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
    let providers = NativeRdfProviders::new(input_path);
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

fn native_subject_range_vortex_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::SubjectRangesVortex)
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

fn decode_exact_range_payload(
    payload: &ArrayRef,
    expected_range_count: usize,
    expected_candidate_rows: u64,
    context: &str,
) -> Result<Vec<Range<u64>>> {
    if payload.len() != expected_range_count {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} returned {} rows; expected {expected_range_count}",
            payload.len()
        )));
    }
    let starts = extract_projected_u64_column(payload, "row_start")?;
    let ends = extract_projected_u64_column(payload, "row_end")?;
    if starts.len() != payload.len() || ends.len() != payload.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} returned inconsistent range columns"
        )));
    }
    let mut ranges = Vec::with_capacity(payload.len());
    let mut previous_end = None;
    for (start, end) in starts.into_iter().zip(ends) {
        if start >= end {
            return Err(VortexRdfError::Deserialization(format!(
                "{context} contains invalid range {start}..{end}"
            )));
        }
        if previous_end.is_some_and(|value| start < value) {
            return Err(VortexRdfError::Deserialization(format!(
                "{context} contains overlapping or unsorted ranges"
            )));
        }
        previous_end = Some(end);
        ranges.push(start..end);
    }
    let actual_rows = range_rows(&ranges);
    if actual_rows != expected_candidate_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "{context} candidate-row mismatch: expected={expected_candidate_rows}, actual={actual_rows}"
        )));
    }
    Ok(ranges)
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
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "PO v2 payload {:?} slice {}..{}",
            path, entry.range_offset, range_end
        ),
    )
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

#[derive(Clone, Copy, Debug)]
struct ExactAccessLimits {
    max_ranges: usize,
    max_rows: u64,
}

impl ExactAccessLimits {
    fn from_env(
        ranges_var: &str,
        rows_var: &str,
        default_max_ranges: usize,
        default_max_rows: u64,
    ) -> Self {
        Self {
            max_ranges: env_value_or(ranges_var, default_max_ranges),
            max_rows: env_value_or(rows_var, default_max_rows),
        }
    }

    fn accepts(self, candidate_ranges: usize, candidate_rows: u64) -> bool {
        candidate_ranges <= self.max_ranges && candidate_rows <= self.max_rows
    }
}

fn env_value_or<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn predicate_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_P_EXACT_MAX_RANGES",
        "VORTEX_RDF_P_EXACT_MAX_ROWS",
        256,
        100_000,
    )
}

fn po_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_PO_EXACT_MAX_RANGES",
        "VORTEX_RDF_PO_EXACT_MAX_ROWS",
        64,
        100_000,
    )
}

fn object_exact_limits() -> ExactAccessLimits {
    ExactAccessLimits::from_env(
        "VORTEX_RDF_O_EXACT_MAX_RANGES",
        "VORTEX_RDF_O_EXACT_MAX_ROWS",
        512,
        100_000,
    )
}

fn po_exact_access_accepted(candidate_ranges: usize, candidate_rows: u64) -> bool {
    po_exact_limits().accepts(candidate_ranges, candidate_rows)
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
    let accepted = predicate_exact_limits().accepts(candidate_ranges, entry.candidate_rows);
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
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "predicate v2 payload {:?} slice {}..{}",
            path, entry.range_offset, range_end
        ),
    )
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

fn range_rows(ranges: &[std::ops::Range<u64>]) -> u64 {
    ranges
        .iter()
        .map(|range| range.end.saturating_sub(range.start))
        .sum()
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
    native_component_path(data_path, NativeComponent::PredicateObjectDirectoryVortexV2)
}

fn native_po_predicate_partitions_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(
        data_path,
        NativeComponent::PredicateObjectPartitionsVortexV2,
    )
}

fn native_po_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateObjectRangesVortexV2)
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
    range_offset: u64,
    range_count: u32,
    candidate_rows: u64,
}

async fn lookup_object_v2_directory_entry(
    data_path: &Path,
    object_id: u32,
) -> Result<Option<NativeObjectDirectoryEntry>> {
    let path = native_o_exact_directory_v2_path(data_path);
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let result = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(eq(col("object_id"), lit(object_id)))
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
            "object v2 directory {:?} returned {} rows for object ID {}; expected at most one",
            path,
            result.len(),
            object_id
        )));
    }

    let offsets = extract_projected_u64_column(&result, "range_offset")?;
    let counts = extract_projected_u32_column(&result, "range_count")?;
    let rows = extract_projected_u64_column(&result, "candidate_rows")?;
    if offsets.len() != 1 || counts.len() != 1 || rows.len() != 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "object v2 directory {:?} returned inconsistent metadata columns for object ID {}",
            path, object_id
        )));
    }

    Ok(Some(NativeObjectDirectoryEntry {
        range_offset: offsets[0],
        range_count: counts[0],
        candidate_rows: rows[0],
    }))
}

fn object_access_from_directory_entry(
    entry: Option<NativeObjectDirectoryEntry>,
) -> NativeObjectAccess {
    let Some(entry) = entry else {
        return NativeObjectAccess {
            ranges: Some(Vec::new()),
            candidate_ranges: 0,
            candidate_rows: 0,
            strategy: "o-exact-ranges-vortex-v2-point",
        };
    };
    let candidate_ranges = entry.range_count as usize;
    let accepted = object_exact_limits().accepts(candidate_ranges, entry.candidate_rows);
    NativeObjectAccess {
        ranges: accepted.then(Vec::new),
        candidate_ranges,
        candidate_rows: entry.candidate_rows,
        strategy: "o-exact-ranges-vortex-v2-point",
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
    decode_exact_range_payload(
        &payload,
        entry.range_count as usize,
        entry.candidate_rows,
        &format!(
            "object v2 payload {:?} slice {}..{}",
            path, entry.range_offset, range_end
        ),
    )
}

async fn lookup_object_access_from_vortex_v2(
    data_path: &Path,
    object_id: u32,
) -> Result<Option<NativeObjectAccess>> {
    let entry = lookup_object_v2_directory_entry(data_path, object_id).await?;
    let mut access = object_access_from_directory_entry(entry);
    if access.ranges.is_some() {
        if let Some(entry) = entry {
            access.ranges = Some(read_object_v2_payload(data_path, entry).await?);
        }
    }
    Ok(Some(access))
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
    native_component_path(data_path, NativeComponent::PredicateDirectoryVortexV2)
}

fn native_p_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::PredicateRangesVortexV2)
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
    native_component_path(data_path, NativeComponent::ObjectDirectoryVortexV2)
}

fn native_o_exact_ranges_v2_path(data_path: &Path) -> PathBuf {
    native_component_path(data_path, NativeComponent::ObjectRangesVortexV2)
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

/// Writes the production subject index directly to Vortex.
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

fn build_native_dictionary_array(ids: Vec<u32>, terms: Vec<String>) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("id", PrimitiveArray::from_iter(ids).into_array()),
        ("term", VarBinArray::from(terms).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|array| array.into_array())
}

fn empty_native_dictionary_array() -> Result<ArrayRef> {
    build_native_dictionary_array(Vec::new(), Vec::new())
}

fn extract_projected_utf8_column(array: &ArrayRef, column_name: &str) -> Result<Vec<String>> {
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
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    (0..column.len())
        .map(|index| {
            String::from_utf8(column.bytes_at(index).to_vec()).map_err(|error| {
                VortexRdfError::Deserialization(format!(
                    "dictionary column {column_name:?} contains \
                         invalid UTF-8 at row {index}: {error}"
                ))
            })
        })
        .collect()
}

fn dictionary_pair_stream(
    run_paths: Vec<PathBuf>,
    order: PairRunOrder,
    row_group_size: usize,
) -> Result<impl Stream<Item = VortexResult<ArrayRef>> + Send> {
    let row_group_size = row_group_size.max(1);
    Ok(async_stream::try_stream! {
        let mut readers = run_paths
            .iter()
            .map(|path| PairRunReader::new(path))
            .collect::<Result<Vec<_>>>()
            .map_err(rdf_err_to_vortex_err)?;
        let mut heap = BinaryHeap::new();
        for (run_idx, reader) in readers.iter_mut().enumerate() {
            if let Some(pair) = reader.read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair, run_idx, order });
            }
        }
        let mut ids = Vec::with_capacity(row_group_size);
        let mut terms = Vec::with_capacity(row_group_size);
        let mut previous_id = None;
        let mut previous_term: Option<String> = None;
        while let Some(item) = heap.pop() {
            let run_idx = item.run_idx;
            let pair = item.pair;
            match order {
                PairRunOrder::Id => {
                    if previous_id.is_some_and(|previous| pair.id <= previous) {
                        Err(vortex_error::vortex_err!(
                            "id_to_term dictionary is not strictly ordered: previous={previous_id:?}, next={}",
                            pair.id
                        ))?;
                    }
                    previous_id = Some(pair.id);
                }
                PairRunOrder::Term => {
                    if previous_term.as_ref().is_some_and(|previous| pair.term <= *previous) {
                        Err(vortex_error::vortex_err!(
                            "term_to_id dictionary is not strictly ordered"
                        ))?;
                    }
                    previous_term = Some(pair.term.clone());
                }
            }
            ids.push(pair.id);
            terms.push(pair.term);
            if let Some(next) = readers[run_idx].read_one().map_err(rdf_err_to_vortex_err)? {
                heap.push(PairHeapItem { pair: next, run_idx, order });
            }
            if ids.len() >= row_group_size {
                yield build_native_dictionary_array(
                    std::mem::take(&mut ids),
                    std::mem::take(&mut terms),
                ).map_err(rdf_err_to_vortex_err)?;
                ids = Vec::with_capacity(row_group_size);
                terms = Vec::with_capacity(row_group_size);
            }
        }
        if !ids.is_empty() {
            yield build_native_dictionary_array(ids, terms).map_err(rdf_err_to_vortex_err)?;
        } else if readers.is_empty() {
            yield empty_native_dictionary_array().map_err(rdf_err_to_vortex_err)?;
        }
    })
}

async fn write_native_dictionary_component(
    output_path: &Path,
    run_paths: &[PathBuf],
    order: PairRunOrder,
    row_group_size: usize,
) -> Result<()> {
    let temporary_path = output_path.with_extension("vortex.tmp");
    let stream = dictionary_pair_stream(run_paths.to_vec(), order, row_group_size)?;
    let dtype = empty_native_dictionary_array()?.dtype().clone();
    let arrays = ArrayStreamAdapter::new(dtype, stream);
    let mut output = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(row_group_size.max(1))
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output, arrays)
        .await
        .map_err(VortexRdfError::from)?;
    drop(output);
    std::fs::rename(&temporary_path, output_path)
        .map_err(|error| VortexRdfError::Serialization(error.to_string()))?;
    Ok(())
}

async fn write_dictionary_lookup_sidecars_from_pair_runs(
    pair_run_paths: &PairRunPaths,
    data_path: &Path,
    row_group_size: usize,
) -> Result<()> {
    let id_to_term_path = native_dict_path(data_path);
    let term_to_id_path = native_dict_term_to_id_path(data_path);
    write_native_dictionary_component(
        &id_to_term_path,
        &pair_run_paths.id_run_paths,
        PairRunOrder::Id,
        row_group_size,
    )
    .await?;
    write_native_dictionary_component(
        &term_to_id_path,
        &pair_run_paths.term_run_paths,
        PairRunOrder::Term,
        row_group_size,
    )
    .await?;
    log::info!(
        "[cottas_native_ids] wrote Vortex dictionary components {:?} and {:?}",
        id_to_term_path,
        term_to_id_path
    );
    Ok(())
}

// VORTEX_RDF_SPARSE_TERM_DIRECTORY_V1
#[derive(Clone, Debug)]
struct NativeTermDirectoryEntry {
    first_term: String,
    last_term: String,
    row_start: u64,
    row_end: u64,
}

static NATIVE_TERM_DIRECTORY_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, Arc<[NativeTermDirectoryEntry]>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn term_directory_cache_lock()
-> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, Arc<[NativeTermDirectoryEntry]>>>> {
    NATIVE_TERM_DIRECTORY_CACHE.lock().map_err(|_| {
        VortexRdfError::Deserialization("term directory cache mutex was poisoned".into())
    })
}

fn build_native_term_directory_array(
    first: Vec<String>,
    last: Vec<String>,
    starts: Vec<u64>,
    ends: Vec<u64>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("first_term", VarBinArray::from(first).into_array()),
        ("last_term", VarBinArray::from(last).into_array()),
        ("row_start", PrimitiveArray::from_iter(starts).into_array()),
        ("row_end", PrimitiveArray::from_iter(ends).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeTermDirectoryBuildStats {
    pub data_path: String,
    pub source_path: String,
    pub output_path: String,
    pub fence_rows: usize,
    pub dictionary_rows: u64,
    pub directory_entries: usize,
    pub open_ms: f64,
    pub scan_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}

/// Builds only the sparse lexical directory from the existing sorted
/// term-to-ID Vortex component. Triples and unrelated components are untouched.
pub async fn build_cottas_native_term_directory(
    data_path: &Path,
    fence_rows: usize,
) -> Result<NativeTermDirectoryBuildStats> {
    let total_start = Instant::now();
    if fence_rows == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "fence_rows must be positive".into(),
        ));
    }
    let source_path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryTermToIdVortex,
        "term-to-ID dictionary",
    )?;
    let output_path = native_dict_term_directory_path(data_path);
    let temporary_path = output_path.with_extension("vortex.tmp");
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let scan_start = Instant::now();
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let (mut first, mut last, mut starts, mut ends) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut rows = 0u64;
    let mut fence_first: Option<String> = None;
    let mut fence_last: Option<String> = None;
    let mut fence_start = 0u64;
    let mut fence_len = 0usize;
    let mut previous: Option<String> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(VortexRdfError::from)?;
        let terms = extract_projected_utf8_column(&batch, "term")?;
        if terms.len() != batch.len() {
            return Err(VortexRdfError::Deserialization(
                "term directory source length mismatch".into(),
            ));
        }
        for term in terms {
            if previous.as_ref().is_some_and(|p| p >= &term) {
                return Err(VortexRdfError::Deserialization(format!(
                    "term-to-ID dictionary is not strictly sorted near row {rows}"
                )));
            }
            if fence_first.is_none() {
                fence_start = rows;
                fence_first = Some(term.clone());
            }
            fence_last = Some(term.clone());
            previous = Some(term);
            fence_len += 1;
            rows = rows
                .checked_add(1)
                .ok_or_else(|| VortexRdfError::Serialization("row overflow".into()))?;
            if fence_len == fence_rows {
                first.push(fence_first.take().unwrap());
                last.push(fence_last.take().unwrap());
                starts.push(fence_start);
                ends.push(rows);
                fence_len = 0;
            }
        }
    }
    if fence_len != 0 {
        first.push(fence_first.take().unwrap());
        last.push(fence_last.take().unwrap());
        starts.push(fence_start);
        ends.push(rows);
    }
    let scan_ms = elapsed_ms(scan_start);
    let directory_entries = first.len();
    let array = build_native_term_directory_array(first, last, starts, ends)?;
    let dtype = build_native_term_directory_array(Vec::new(), Vec::new(), Vec::new(), Vec::new())?
        .dtype()
        .clone();
    let arrays = ArrayStreamAdapter::new(dtype, futures::stream::iter(vec![Ok(array)]));
    let write_start = Instant::now();
    let mut output = tokio::fs::File::create(&temporary_path)
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    let strategy = WriteStrategyBuilder::default()
        .with_row_block_size(directory_entries.max(1).min(65_536))
        .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
        .build();
    if let Err(error) = NATIVE_FILE_SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut output, arrays)
        .await
    {
        drop(output);
        let _ = std::fs::remove_file(&temporary_path);
        return Err(VortexRdfError::from(error));
    }
    output
        .sync_all()
        .await
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    drop(output);
    std::fs::rename(&temporary_path, &output_path)
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    term_directory_cache_lock()?.remove(&output_path);
    let write_ms = elapsed_ms(write_start);
    Ok(NativeTermDirectoryBuildStats {
        data_path: data_path.display().to_string(),
        source_path: source_path.display().to_string(),
        output_path: output_path.display().to_string(),
        fence_rows,
        dictionary_rows: rows,
        directory_entries,
        open_ms,
        scan_ms,
        write_ms,
        total_ms: elapsed_ms(total_start),
    })
}

async fn native_term_directory(data_path: &Path) -> Result<Arc<[NativeTermDirectoryEntry]>> {
    let path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryTermDirectoryVortex,
        "sparse term directory",
    )?;
    if let Some(v) = term_directory_cache_lock()?.get(&path).cloned() {
        return Ok(v);
    }
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let a = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["first_term", "last_term", "row_start", "row_end"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let first = extract_projected_utf8_column(&a, "first_term")?;
    let last = extract_projected_utf8_column(&a, "last_term")?;
    let starts = extract_projected_u64_column(&a, "row_start")?;
    let ends = extract_projected_u64_column(&a, "row_end")?;
    if [first.len(), last.len(), starts.len(), ends.len()]
        .into_iter()
        .any(|n| n != a.len())
    {
        return Err(VortexRdfError::Deserialization(
            "term directory column length mismatch".into(),
        ));
    }
    let mut entries = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        if first[i] > last[i]
            || starts[i] >= ends[i]
            || (i > 0 && (last[i - 1] >= first[i] || ends[i - 1] != starts[i]))
        {
            return Err(VortexRdfError::Deserialization(format!(
                "invalid term directory entry {i}"
            )));
        }
        entries.push(NativeTermDirectoryEntry {
            first_term: first[i].clone(),
            last_term: last[i].clone(),
            row_start: starts[i],
            row_end: ends[i],
        });
    }
    if entries.first().is_some_and(|e| e.row_start != 0) {
        return Err(VortexRdfError::Deserialization(
            "term directory does not start at row zero".into(),
        ));
    }
    let entries: Arc<[NativeTermDirectoryEntry]> = entries.into();
    let mut cache = term_directory_cache_lock()?;
    Ok(cache
        .entry(path)
        .or_insert_with(|| Arc::clone(&entries))
        .clone())
}

fn term_directory_range(entries: &[NativeTermDirectoryEntry], term: &str) -> Option<Range<u64>> {
    let i = entries.partition_point(|e| e.last_term.as_str() < term);
    let e = entries.get(i)?;
    (e.first_term.as_str() <= term).then(|| e.row_start..e.row_end)
}

#[derive(Debug)]
struct NativeTermLookupWindow {
    range: Range<u64>,
    terms: Vec<String>,
}

fn merge_native_term_lookup_windows(
    mut input: Vec<NativeTermLookupWindow>,
) -> Vec<NativeTermLookupWindow> {
    input.sort_by_key(|w| w.range.start);
    let mut out: Vec<NativeTermLookupWindow> = Vec::with_capacity(input.len());
    for mut w in input {
        if let Some(p) = out.last_mut() {
            if w.range.start <= p.range.end {
                p.range.end = p.range.end.max(w.range.end);
                p.terms.append(&mut w.terms);
                continue;
            }
        }
        out.push(w);
    }
    for w in &mut out {
        w.terms.sort();
        w.terms.dedup();
    }
    out
}

async fn lookup_bound_term_ids_sparse_directory(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }
    let directory = native_term_directory(data_path).await?;
    let mut stats: Vec<_> = terms
        .iter()
        .map(|(term, column)| NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-sparse-directory-v1".to_string(),
            ..Default::default()
        })
        .collect();
    let windows = merge_native_term_lookup_windows(
        terms
            .iter()
            .filter_map(|(term, _)| {
                term_directory_range(&directory, term).map(|range| NativeTermLookupWindow {
                    range,
                    terms: vec![term.clone()],
                })
            })
            .collect(),
    );
    if windows.is_empty() {
        let ms = elapsed_ms(total_start);
        stats[0].total_ms = ms;
        return Ok((HashMap::new(), stats, ms));
    }
    let path = require_vortex_component(
        data_path,
        NativeComponent::DictionaryTermToIdVortex,
        "term-to-ID dictionary",
    )?;
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let (mut scan_ms, mut read_ms, mut extract_ms) = (0.0, 0.0, 0.0);
    let mut found = HashMap::with_capacity(terms.len());
    for window in windows {
        let expr = window
            .terms
            .iter()
            .map(|t| eq(col("term"), lit(t.as_str())))
            .reduce(or)
            .unwrap();
        let t = Instant::now();
        let stream = file
            .scan()
            .map_err(VortexRdfError::from)?
            .with_row_range(window.range)
            .with_filter(expr)
            .with_projection(vortex_array::expr::select(
                ["term", "id"],
                vortex_array::expr::root(),
            ))
            .into_array_stream()
            .map_err(VortexRdfError::from)?;
        scan_ms += elapsed_ms(t);
        let t = Instant::now();
        let result = stream.read_all().await.map_err(VortexRdfError::from)?;
        read_ms += elapsed_ms(t);
        let t = Instant::now();
        let loaded_terms = extract_projected_utf8_column(&result, "term")?;
        let ids = extract_projected_u32_column(&result, "id")?;
        if loaded_terms.len() != ids.len() {
            return Err(VortexRdfError::Deserialization(
                "sparse lookup length mismatch".into(),
            ));
        }
        for (term, id) in loaded_terms.into_iter().zip(ids) {
            if found.insert(term.clone(), id).is_some() {
                return Err(VortexRdfError::Deserialization(format!(
                    "duplicate sparse lookup result {term:?}"
                )));
            }
        }
        extract_ms += elapsed_ms(t);
    }
    let total_ms = elapsed_ms(total_start);
    for (i, (term, _)) in terms.iter().enumerate() {
        stats[i].found_id = found.get(term).copied();
        stats[i].result_array_len = usize::from(stats[i].found_id.is_some());
    }
    stats[0].open_ms = open_ms;
    stats[0].scan_build_ms = scan_ms;
    stats[0].read_all_ms = read_ms;
    stats[0].extract_ms = extract_ms;
    stats[0].total_ms = total_ms;
    Ok((found, stats, total_ms))
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct NativeDictionaryRebuildStats {
    pub data_path: String,
    pub source_path: String,
    pub output_path: String,
    pub terms_read: u64,
    pub temporary_runs: usize,
    pub row_group_size: usize,
    pub scan_ms: f64,
    pub sort_spill_ms: f64,
    pub write_ms: f64,
    pub total_ms: f64,
}

/// Rebuilds only the lexicographically ordered Vortex term-to-ID dictionary.
///
/// The triple artifact, ID-to-term dictionary, and all native indexes remain
/// untouched. Temporary files are private external-sort runs and are deleted
/// when this function returns; they are not runtime components.
pub async fn rebuild_cottas_native_term_dictionary(
    data_path: &Path,
    row_group_size: usize,
) -> Result<NativeDictionaryRebuildStats> {
    let total_start = Instant::now();
    let row_group_size = row_group_size.max(1);
    let source_path = native_dict_path(data_path);
    let output_path = native_dict_term_to_id_path(data_path);
    if !source_path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "cannot rebuild term dictionary: ID-to-term Vortex component is missing at {:?}",
            source_path
        )));
    }

    let sort_batch_size = std::env::var("VORTEX_RDF_TERM_DICT_REBUILD_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(500_000)
        .max(1);
    let temp_dir =
        tempfile::tempdir().map_err(|error| VortexRdfError::Serialization(error.to_string()))?;

    let scan_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&source_path)
        .await
        .map_err(VortexRdfError::from)?;
    let mut stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["id", "term"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let open_and_scan_build_ms = elapsed_ms(scan_start);

    let spill_start = Instant::now();
    let mut batch = Vec::with_capacity(sort_batch_size);
    let mut term_run_paths = Vec::new();
    let mut terms_read = 0u64;
    while let Some(batch_result) = stream.next().await {
        let array = batch_result.map_err(VortexRdfError::from)?;
        let ids = extract_projected_u32_column(&array, "id")?;
        let terms = extract_projected_utf8_column(&array, "term")?;
        if ids.len() != array.len() || terms.len() != array.len() {
            return Err(VortexRdfError::Deserialization(format!(
                "ID-to-term dictionary projection mismatch: rows={}, ids={}, terms={}",
                array.len(),
                ids.len(),
                terms.len()
            )));
        }
        for (id, term) in ids.into_iter().zip(terms) {
            batch.push(NativeDictPair { id, term });
            terms_read += 1;
            if batch.len() >= sort_batch_size {
                batch.sort_by(|left, right| {
                    left.term
                        .cmp(&right.term)
                        .then_with(|| left.id.cmp(&right.id))
                });
                let path = temp_dir.path().join(format!(
                    "term_dictionary_rebuild_{:06}.tsv",
                    term_run_paths.len()
                ));
                write_pair_run(&path, &batch)?;
                term_run_paths.push(path);
                batch.clear();
            }
        }
    }
    let scan_ms = open_and_scan_build_ms + elapsed_ms(spill_start);
    if !batch.is_empty() {
        batch.sort_by(|left, right| {
            left.term
                .cmp(&right.term)
                .then_with(|| left.id.cmp(&right.id))
        });
        let path = temp_dir.path().join(format!(
            "term_dictionary_rebuild_{:06}.tsv",
            term_run_paths.len()
        ));
        write_pair_run(&path, &batch)?;
        term_run_paths.push(path);
    }
    let sort_spill_ms = elapsed_ms(spill_start);

    let write_start = Instant::now();
    write_native_dictionary_component(
        &output_path,
        &term_run_paths,
        PairRunOrder::Term,
        row_group_size,
    )
    .await?;
    let write_ms = elapsed_ms(write_start);
    let total_ms = elapsed_ms(total_start);
    log::info!(
        "[cottas_native_ids] rebuilt only {:?}: terms={}, runs={}, row_group_size={}, total_ms={:.3}",
        output_path,
        terms_read,
        term_run_paths.len(),
        row_group_size,
        total_ms
    );
    Ok(NativeDictionaryRebuildStats {
        data_path: data_path.display().to_string(),
        source_path: source_path.display().to_string(),
        output_path: output_path.display().to_string(),
        terms_read,
        temporary_runs: term_run_paths.len(),
        row_group_size,
        scan_ms,
        sort_spill_ms,
        write_ms,
        total_ms,
    })
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

async fn lookup_bound_term_ids_from_sidecar_with_stats(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let strategy = std::env::var("VORTEX_RDF_TERM_LOOKUP_STRATEGY")
        .unwrap_or_else(|_| "batched-or".to_string());
    match strategy.as_str() {
        "batched-or" => lookup_bound_term_ids_batched_or(data_path, terms).await,
        "shared-open-equalities" => {
            lookup_bound_term_ids_shared_open_equalities(data_path, terms).await
        }
        "sparse-directory-v1" => lookup_bound_term_ids_sparse_directory(data_path, terms).await,
        other => Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_TERM_LOOKUP_STRATEGY={other:?}; expected batched-or, shared-open-equalities, or sparse-directory-v1"
        ))),
    }
}

async fn lookup_bound_term_ids_batched_or(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }

    let path = native_dict_term_to_id_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex term_to_id dictionary component is missing at {:?}",
            path
        )));
    }

    let mut stats: Vec<NativeTermToIdLookupStats> = terms
        .iter()
        .map(|(term, column)| NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-batched-term-filter".to_string(),
            ..NativeTermToIdLookupStats::default()
        })
        .collect();

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);
    let expr = terms
        .iter()
        .map(|(term, _)| eq(col("term"), lit(term.as_str())))
        .reduce(or)
        .expect("non-empty bound-term batch");
    let can_prune_start = Instant::now();
    let can_prune = file.can_prune(&expr).ok();
    let can_prune_ms = elapsed_ms(can_prune_start);
    let scan_start = Instant::now();
    let stream = file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_filter(expr)
        .with_projection(vortex_array::expr::select(
            ["term", "id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?;
    let scan_build_ms = elapsed_ms(scan_start);
    let read_start = Instant::now();
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    let read_all_ms = elapsed_ms(read_start);
    let extract_start = Instant::now();
    let loaded_terms = extract_projected_utf8_column(&result, "term")?;
    let loaded_ids = extract_projected_u32_column(&result, "id")?;
    if loaded_terms.len() != loaded_ids.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "batched term_to_id lookup returned terms={} and ids={}",
            loaded_terms.len(),
            loaded_ids.len()
        )));
    }
    let mut out = HashMap::with_capacity(loaded_terms.len());
    for (term, id) in loaded_terms.into_iter().zip(loaded_ids) {
        if out.insert(term.clone(), id).is_some() {
            return Err(VortexRdfError::Deserialization(format!(
                "batched term_to_id lookup returned duplicate term {term:?}"
            )));
        }
    }
    let extract_ms = elapsed_ms(extract_start);
    let total_ms = elapsed_ms(total_start);
    for (index, (term, _)) in terms.iter().enumerate() {
        stats[index].found_id = out.get(term).copied();
        stats[index].result_array_len = usize::from(stats[index].found_id.is_some());
    }
    // Shared costs are stored once so aggregate diagnostics do not multiply them.
    stats[0].open_ms = open_ms;
    stats[0].can_prune_ms = can_prune_ms;
    stats[0].scan_build_ms = scan_build_ms;
    stats[0].read_all_ms = read_all_ms;
    stats[0].extract_ms = extract_ms;
    stats[0].can_prune = can_prune;
    stats[0].total_ms = total_ms;
    Ok((out, stats, total_ms))
}

async fn lookup_bound_term_ids_shared_open_equalities(
    data_path: &Path,
    terms: &[(String, &'static str)],
) -> Result<(HashMap<String, u32>, Vec<NativeTermToIdLookupStats>, f64)> {
    let total_start = Instant::now();
    if terms.is_empty() {
        return Ok((HashMap::new(), Vec::new(), elapsed_ms(total_start)));
    }

    let path = native_dict_term_to_id_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex term_to_id dictionary component is missing at {:?}",
            path
        )));
    }

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let open_ms = elapsed_ms(open_start);

    let mut out = HashMap::with_capacity(terms.len());
    let mut stats = Vec::with_capacity(terms.len());
    for (index, (term, column)) in terms.iter().enumerate() {
        let lookup_start = Instant::now();
        let mut item = NativeTermToIdLookupStats {
            column: Some((*column).to_string()),
            term_len: term.len(),
            term_preview: native_term_preview(term),
            strategy: "vortex-shared-open-equality".to_string(),
            ..NativeTermToIdLookupStats::default()
        };
        if index == 0 {
            item.open_ms = open_ms;
        }

        let expr = eq(col("term"), lit(term.as_str()));
        let can_prune_start = Instant::now();
        item.can_prune = file.can_prune(&expr).ok();
        item.can_prune_ms = elapsed_ms(can_prune_start);

        let scan_start = Instant::now();
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
        item.scan_build_ms = elapsed_ms(scan_start);

        let read_start = Instant::now();
        let result = stream.read_all().await.map_err(VortexRdfError::from)?;
        item.read_all_ms = elapsed_ms(read_start);
        item.result_array_len = result.len();
        if result.len() > 1 {
            return Err(VortexRdfError::Deserialization(format!(
                "shared-open term_to_id equality returned {} IDs for term {:?}",
                result.len(),
                term
            )));
        }

        let extract_start = Instant::now();
        let id = extract_first_u32_from_single_column_array(&result, "id")?;
        item.extract_ms = elapsed_ms(extract_start);
        item.found_id = id;
        if let Some(id) = id {
            if let Some(previous) = out.insert(term.clone(), id) {
                if previous != id {
                    return Err(VortexRdfError::Deserialization(format!(
                        "shared-open term_to_id lookup returned conflicting IDs for duplicate term {term:?}"
                    )));
                }
            }
        }
        item.total_ms = elapsed_ms(lookup_start) + if index == 0 { open_ms } else { 0.0 };
        stats.push(item);
    }

    let total_ms = elapsed_ms(total_start);
    Ok((out, stats, total_ms))
}

#[derive(Clone, Debug, Serialize)]
pub struct NativeTermWindowTrial {
    pub strategy: String,
    pub window_rows: usize,
    pub row_start: u64,
    pub row_end: u64,
    pub run: usize,
    pub open_ms: f64,
    pub scan_build_ms: f64,
    pub read_all_ms: f64,
    pub extract_ms: f64,
    pub total_ms: f64,
    pub result_rows: usize,
    pub found_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NativeTermWindowDiagnostics {
    pub term: String,
    pub term_preview: String,
    pub dictionary_rows: usize,
    pub discovered_row: u64,
    pub expected_id: u32,
    pub discovery_open_ms: f64,
    pub discovery_read_ms: f64,
    pub discovery_extract_ms: f64,
    pub trials: Vec<NativeTermWindowTrial>,
}

/// Diagnostic-only feasibility test for a sparse lexical term directory.
///
/// Discovery intentionally scans the sorted term_to_id dictionary once and is
/// reported separately. Timed trials then compare the current full-layout
/// equality scan with exact row windows around the discovered lexical row.
/// This function does not alter production lookup routing or persist metadata.
pub async fn diagnose_cottas_native_term_windows(
    data_path: &Path,
    term: &str,
    window_sizes: &[usize],
    runs: usize,
) -> Result<NativeTermWindowDiagnostics> {
    if runs == 0 {
        return Err(VortexRdfError::InvalidOperation(
            "term-window diagnostics require at least one run".into(),
        ));
    }
    if window_sizes.is_empty() || window_sizes.iter().any(|size| *size == 0) {
        return Err(VortexRdfError::InvalidOperation(
            "term-window diagnostics require non-zero window sizes".into(),
        ));
    }

    let path = native_dict_term_to_id_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex term_to_id dictionary component is missing at {:?}",
            path
        )));
    }

    let discovery_open_start = Instant::now();
    let discovery_file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let discovery_open_ms = elapsed_ms(discovery_open_start);
    let discovery_read_start = Instant::now();
    let dictionary = discovery_file
        .scan()
        .map_err(VortexRdfError::from)?
        .with_projection(vortex_array::expr::select(
            ["term", "id"],
            vortex_array::expr::root(),
        ))
        .into_array_stream()
        .map_err(VortexRdfError::from)?
        .read_all()
        .await
        .map_err(VortexRdfError::from)?;
    let discovery_read_ms = elapsed_ms(discovery_read_start);
    let discovery_extract_start = Instant::now();
    let terms = extract_projected_utf8_column(&dictionary, "term")?;
    let ids = extract_projected_u32_column(&dictionary, "id")?;
    if terms.len() != ids.len() || terms.len() != dictionary.len() {
        return Err(VortexRdfError::Deserialization(format!(
            "term-window discovery column mismatch: rows={}, terms={}, ids={}",
            dictionary.len(),
            terms.len(),
            ids.len()
        )));
    }
    if terms.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(VortexRdfError::Deserialization(
            "term_to_id dictionary is not strictly lexically sorted".into(),
        ));
    }
    let row = terms
        .binary_search_by(|candidate| candidate.as_str().cmp(term))
        .map_err(|_| {
            VortexRdfError::InvalidOperation(format!(
                "diagnostic term {:?} does not exist in the term_to_id dictionary",
                term
            ))
        })?;
    let expected_id = ids[row];
    let dictionary_rows = terms.len();
    let discovery_extract_ms = elapsed_ms(discovery_extract_start);
    drop(dictionary);
    drop(discovery_file);

    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    let shared_open_ms = elapsed_ms(open_start);
    let mut trials = Vec::with_capacity(runs.saturating_mul(window_sizes.len() + 1));
    macro_rules! run_trial {
        ($range:expr, $window_rows:expr, $run:expr, $open_ms:expr, $strategy:expr) => {{
            let total_start = Instant::now();
            let scan_start = Instant::now();
            let scan = file.scan().map_err(VortexRdfError::from)?;
            let scan = match $range.clone() {
                Some(range) => scan.with_row_range(range),
                None => scan,
            };
            let stream = scan
                .with_filter(eq(col("term"), lit(term)))
                .with_projection(vortex_array::expr::select(
                    ["id"],
                    vortex_array::expr::root(),
                ))
                .into_array_stream()
                .map_err(VortexRdfError::from)?;
            let scan_build_ms = elapsed_ms(scan_start);
            let read_start = Instant::now();
            let result = stream.read_all().await.map_err(VortexRdfError::from)?;
            let read_all_ms = elapsed_ms(read_start);
            if result.len() > 1 {
                return Err(VortexRdfError::Deserialization(format!(
                    "term-window diagnostic returned {} IDs for exact term {:?}",
                    result.len(),
                    term
                )));
            }
            let extract_start = Instant::now();
            let found_id = extract_first_u32_from_single_column_array(&result, "id")?;
            let extract_ms = elapsed_ms(extract_start);
            let (row_start, row_end) = $range
                .as_ref()
                .map(|range: &Range<u64>| (range.start, range.end))
                .unwrap_or((0, 0));
            NativeTermWindowTrial {
                strategy: $strategy.to_string(),
                window_rows: $window_rows,
                row_start,
                row_end,
                run: $run,
                open_ms: $open_ms,
                scan_build_ms,
                read_all_ms,
                extract_ms,
                total_ms: elapsed_ms(total_start) + $open_ms,
                result_rows: result.len(),
                found_id,
            }
        }};
    }
    for run in 0..runs {
        let baseline_range: Option<Range<u64>> = None;
        let baseline = run_trial!(
            baseline_range,
            dictionary_rows,
            run,
            if run == 0 { shared_open_ms } else { 0.0 },
            "full-layout-equality"
        );
        if baseline.found_id != Some(expected_id) {
            return Err(VortexRdfError::Deserialization(format!(
                "full-layout diagnostic returned {:?}; expected ID {}",
                baseline.found_id, expected_id
            )));
        }
        trials.push(baseline);

        for &window_rows in window_sizes {
            let half = window_rows / 2;
            let mut start = row.saturating_sub(half);
            let end = start.saturating_add(window_rows).min(dictionary_rows);
            start = end.saturating_sub(window_rows).min(start);
            if !(start <= row && row < end) {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "computed diagnostic window {}..{} does not contain row {}",
                    start, end, row
                )));
            }
            let window_range = Some(start as u64..end as u64);
            let window = run_trial!(
                window_range,
                end - start,
                run,
                0.0,
                "known-window-row-range"
            );
            if window.found_id != Some(expected_id) {
                return Err(VortexRdfError::Deserialization(format!(
                    "window {}..{} returned {:?}; expected ID {}",
                    start, end, window.found_id, expected_id
                )));
            }
            trials.push(window);
        }
    }

    Ok(NativeTermWindowDiagnostics {
        term: term.to_string(),
        term_preview: native_term_preview(term),
        dictionary_rows,
        discovered_row: row as u64,
        expected_id,
        discovery_open_ms,
        discovery_read_ms,
        discovery_extract_ms,
        trials,
    })
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
    let strategy = std::env::var("VORTEX_RDF_TERM_LOOKUP_STRATEGY")
        .unwrap_or_else(|_| "batched-or".to_string());
    if strategy == "sparse-directory-v1" {
        let requested = vec![(term.to_string(), column.unwrap_or("term"))];
        let (ids, mut stats, _) =
            lookup_bound_term_ids_sparse_directory(data_path, &requested).await?;
        return Ok((ids.get(term).copied(), stats.pop().unwrap()));
    }
    if strategy != "batched-or" && strategy != "shared-open-equalities" {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Unsupported VORTEX_RDF_TERM_LOOKUP_STRATEGY={strategy:?}; expected batched-or, shared-open-equalities, or sparse-directory-v1"
        )));
    }
    let lookup_start = Instant::now();
    let mut stats = NativeTermToIdLookupStats {
        column: column.map(|value| value.to_string()),
        term_len: term.len(),
        term_preview: native_term_preview(term),
        strategy: "vortex-term-filter".to_string(),
        ..NativeTermToIdLookupStats::default()
    };
    let path = native_dict_term_to_id_path(data_path);
    if !path.is_file() {
        return Err(VortexRdfError::InvalidOperation(format!(
            "Vortex term_to_id dictionary component is missing at {:?}",
            path
        )));
    }
    let open_start = Instant::now();
    let file = NATIVE_FILE_SESSION
        .open_options()
        .open_path(&path)
        .await
        .map_err(VortexRdfError::from)?;
    stats.open_ms = elapsed_ms(open_start);
    let expr = eq(col("term"), lit(term));
    let can_prune_start = Instant::now();
    stats.can_prune = file.can_prune(&expr).ok();
    stats.can_prune_ms = elapsed_ms(can_prune_start);
    let scan_start = Instant::now();
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
    stats.scan_build_ms = elapsed_ms(scan_start);
    let read_start = Instant::now();
    let result = stream.read_all().await.map_err(VortexRdfError::from)?;
    stats.read_all_ms = elapsed_ms(read_start);
    stats.result_array_len = result.len();
    if result.len() > 1 {
        return Err(VortexRdfError::Deserialization(format!(
            "term_to_id dictionary returned {} IDs for one exact term",
            result.len()
        )));
    }
    let extract_start = Instant::now();
    let id = extract_first_u32_from_single_column_array(&result, "id")?;
    stats.extract_ms = elapsed_ms(extract_start);
    stats.found_id = id;
    stats.total_ms = elapsed_ms(lookup_start);
    Ok((id, stats))
}
