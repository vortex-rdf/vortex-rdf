use crate::common::{indexes, utils};
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use crate::store::VortexRdfStoreLike;
use crate::store::vortex_rdf_store::{IndexBuilder, LayoutStrategy, VortexRdfStore};
use futures::Stream;
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use vortex::compute::{Operator, and, compare, filter};
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::{BoolArray, ConstantArray, PrimitiveArray, StructArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};
use vortex_mask::Mask;
use vortex_scalar::Scalar;

impl<Dict: RdfDictionary> VortexRdfStoreLike<Dict> for CottasVortexStore<Dict> {
    fn dictionary(&self) -> &Dict {
        &self.base.dictionary
    }

    fn quads_array(&self) -> &ArrayRef {
        &self.base.quads
    }

    fn quads_stream(
        &self,
    ) -> Result<Box<dyn futures::Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.base.quads()
    }
}

pub struct CottasLayout {
    ordering: TripleOrdering,
    row_group_size: usize,

    buffer: Vec<Triple>,

    /// Raw sorted row groups.
    ///
    /// We keep raw strings until finalize so that we can seed the dictionary
    /// in lexical order before assigning IDs.
    raw_row_groups: Vec<Vec<Triple>>,

    /// Encoded row groups after dictionary assignment.
    encoded_row_groups: Vec<EncodedRowGroup>,
}

impl CottasLayout {
    pub fn new(ordering: TripleOrdering, row_group_size: usize) -> Self {
        Self {
            ordering,
            row_group_size,
            buffer: Vec::with_capacity(row_group_size),
            raw_row_groups: Vec::new(),
            encoded_row_groups: Vec::new(),
        }
    }

    fn flush_raw_row_group(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        self.buffer.sort_by(|a, b| a.cmp_by_order(b, self.ordering));

        let mut group = Vec::new();
        std::mem::swap(&mut group, &mut self.buffer);

        self.raw_row_groups.push(group);
    }

    fn seed_dictionary_in_lexical_order<Dict: RdfDictionary>(&self, dictionary: &mut Dict) {
        let mut terms = BTreeSet::new();

        for group in &self.raw_row_groups {
            for row in group {
                terms.insert(row.s.clone());
                terms.insert(row.p.clone());
                terms.insert(row.o.clone());
                terms.insert(row.g.clone());
            }
        }

        for term in terms {
            dictionary.get_or_insert(&term);
        }
    }

    fn encode_row_groups<Dict: RdfDictionary>(&mut self, dictionary: &mut Dict) -> Result<()> {
        self.encoded_row_groups.clear();

        let mut global_offset: u32 = 0;

        for (row_group_id, group) in self.raw_row_groups.iter().enumerate() {
            let mut s_ids = Vec::with_capacity(group.len());
            let mut p_ids = Vec::with_capacity(group.len());
            let mut o_ids = Vec::with_capacity(group.len());
            let mut g_ids = Vec::with_capacity(group.len());

            for row in group {
                let s_id = dictionary
                    .get_id(&row.s)
                    .unwrap_or_else(|| dictionary.get_or_insert(&row.s));

                let p_id = dictionary
                    .get_id(&row.p)
                    .unwrap_or_else(|| dictionary.get_or_insert(&row.p));

                let o_id = dictionary
                    .get_id(&row.o)
                    .unwrap_or_else(|| dictionary.get_or_insert(&row.o));

                let g_id = dictionary
                    .get_id(&row.g)
                    .unwrap_or_else(|| dictionary.get_or_insert(&row.g));

                s_ids.push(s_id);
                p_ids.push(p_id);
                o_ids.push(o_id);
                g_ids.push(g_id);
            }

            let len = group.len() as u32;

            self.encoded_row_groups.push(EncodedRowGroup {
                row_group_id: row_group_id as u32,
                global_start: global_offset,
                global_end: global_offset + len,

                s_ids,
                p_ids,
                o_ids,
                g_ids,
            });

            global_offset += len;
        }

        Ok(())
    }

    fn build_row_group_stats_array(&self) -> Result<ArrayRef> {
        let stats: Vec<RowGroupStats> = self
            .encoded_row_groups
            .iter()
            .map(|group| group.stats())
            .collect::<Result<Vec<_>>>()?;

        let row_group_ids: Vec<u32> = stats.iter().map(|s| s.row_group_id).collect();
        let block_starts: Vec<u32> = stats.iter().map(|s| s.block_start).collect();
        let block_ends: Vec<u32> = stats.iter().map(|s| s.block_end).collect();

        let min_s: Vec<u32> = stats.iter().map(|s| s.min_s).collect();
        let max_s: Vec<u32> = stats.iter().map(|s| s.max_s).collect();

        let min_p: Vec<u32> = stats.iter().map(|s| s.min_p).collect();
        let max_p: Vec<u32> = stats.iter().map(|s| s.max_p).collect();

        let min_o: Vec<u32> = stats.iter().map(|s| s.min_o).collect();
        let max_o: Vec<u32> = stats.iter().map(|s| s.max_o).collect();

        let min_g: Vec<u32> = stats.iter().map(|s| s.min_g).collect();
        let max_g: Vec<u32> = stats.iter().map(|s| s.max_g).collect();

        let arr = StructArray::from_fields(&[
            (
                "row_group_id",
                PrimitiveArray::from_iter(row_group_ids).into_array(),
            ),
            (
                "block_start",
                PrimitiveArray::from_iter(block_starts).into_array(),
            ),
            (
                "block_end",
                PrimitiveArray::from_iter(block_ends).into_array(),
            ),
            ("min_s", PrimitiveArray::from_iter(min_s).into_array()),
            ("max_s", PrimitiveArray::from_iter(max_s).into_array()),
            ("min_p", PrimitiveArray::from_iter(min_p).into_array()),
            ("max_p", PrimitiveArray::from_iter(max_p).into_array()),
            ("min_o", PrimitiveArray::from_iter(min_o).into_array()),
            ("max_o", PrimitiveArray::from_iter(max_o).into_array()),
            ("min_g", PrimitiveArray::from_iter(min_g).into_array()),
            ("max_g", PrimitiveArray::from_iter(max_g).into_array()),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(arr)
    }

    fn num_triples(&self) -> usize {
        self.encoded_row_groups.iter().map(|g| g.len()).sum()
    }

    fn empty_quads_array() -> Result<ArrayRef> {
        let arr = StructArray::from_fields(&[
            (
                "s",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "p",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "o",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "g",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(arr)
    }
}

impl<Dict: RdfDictionary> LayoutStrategy<Dict> for CottasLayout {
    fn ingest(&mut self, quad: &Quad, _dictionary: &mut Dict) -> Result<()> {
        self.buffer.push(Triple {
            s: quad.subject.to_string(),
            p: quad.predicate.to_string(),
            o: quad.object.to_string(),
            g: quad.graph_name.to_string(),
        });

        if self.buffer.len() >= self.row_group_size {
            self.flush_raw_row_group();
        }

        Ok(())
    }

    fn finalize(&mut self, dictionary: &mut Dict) -> Result<()> {
        self.flush_raw_row_group();

        self.seed_dictionary_in_lexical_order(dictionary);
        self.encode_row_groups(dictionary)?;

        Ok(())
    }

    fn build_quads(&self) -> Result<ArrayRef> {
        if self.encoded_row_groups.is_empty() {
            return Self::empty_quads_array();
        }

        let chunks: Vec<ArrayRef> = self
            .encoded_row_groups
            .iter()
            .map(|group| group.to_struct_array())
            .collect::<Result<Vec<_>>>()?;

        if chunks.len() == 1 {
            return Ok(chunks[0].clone());
        }

        let dtype = chunks[0].dtype().clone();

        let chunked = ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array();

        Ok(chunked)
    }

    fn build_extra_root_fields(&self) -> Result<Vec<(Arc<str>, ArrayRef)>> {
        let storage_layout = ConstantArray::new("cottas-vortex-row-groups", 1).into_array();

        let row_group_stats = self.build_row_group_stats_array()?;
        let row_group_stats_list = indexes::wrap_array_in_list(row_group_stats)?;

        let file_metadata = CottasVortexStore::<Dict>::build_file_metadata(&FileMetadata {
            ordering: self.ordering,
            row_group_size: self.row_group_size,
            num_triples: self.num_triples(),
        })?;

        /*
         * Future Bloom-filter slot.
         *
         * Do not implement actual filters now. We only reserve the schema idea.
         * Later this can become a StructArray with one Bloom filter blob per
         * row group and per column.
         */

        Ok(vec![
            ("storage_layout".into(), storage_layout),
            ("row_group_stats".into(), row_group_stats_list),
            ("file_metadata".into(), file_metadata),
        ])
    }
}

/// Logical row representation for a COTTAS-style store.
#[derive(Clone, Debug)]
pub struct Triple {
    pub s: String,
    pub p: String,
    pub o: String,
    pub g: String,
}

impl Triple {
    fn cmp_by_order(&self, other: &Self, ordering: TripleOrdering) -> std::cmp::Ordering {
        let cmp = match ordering {
            TripleOrdering::SPO => (
                self.s.cmp(&other.s),
                self.p.cmp(&other.p),
                self.o.cmp(&other.o),
            ),
            TripleOrdering::PSO => (
                self.p.cmp(&other.p),
                self.s.cmp(&other.s),
                self.o.cmp(&other.o),
            ),
            TripleOrdering::OSP => (
                self.o.cmp(&other.o),
                self.s.cmp(&other.s),
                self.p.cmp(&other.p),
            ),
        };

        if cmp.0 != std::cmp::Ordering::Equal {
            cmp.0
        } else if cmp.1 != std::cmp::Ordering::Equal {
            cmp.1
        } else if cmp.2 != std::cmp::Ordering::Equal {
            cmp.2
        } else {
            self.g.cmp(&other.g)
        }
    }
}

pub mod triple_ordering {
    use super::*;

    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    pub enum TripleOrdering {
        SPO,
        PSO,
        OSP,
    }

    impl TripleOrdering {
        pub fn as_str(&self) -> &'static str {
            match self {
                TripleOrdering::SPO => "SPO",
                TripleOrdering::PSO => "PSO",
                TripleOrdering::OSP => "OSP",
            }
        }
    }

    impl fmt::Display for TripleOrdering {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.as_str())
        }
    }

    impl FromStr for TripleOrdering {
        type Err = VortexRdfError;

        fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
            match value {
                "SPO" => Ok(TripleOrdering::SPO),
                "PSO" => Ok(TripleOrdering::PSO),
                "OSP" => Ok(TripleOrdering::OSP),
                other => Err(VortexRdfError::Deserialization(format!(
                    "Unsupported ordering '{}'",
                    other
                ))),
            }
        }
    }
}

use triple_ordering::TripleOrdering;

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub ordering: TripleOrdering,
    pub row_group_size: usize,
    pub num_triples: usize,
}

#[derive(Clone, Debug)]
pub struct EncodedRowGroup {
    pub row_group_id: u32,
    pub global_start: u32,
    pub global_end: u32,

    pub s_ids: Vec<u32>,
    pub p_ids: Vec<u32>,
    pub o_ids: Vec<u32>,
    pub g_ids: Vec<u32>,
}

impl EncodedRowGroup {
    pub fn len(&self) -> usize {
        self.s_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.s_ids.is_empty()
    }

    pub fn to_struct_array(&self) -> Result<ArrayRef> {
        let arr = StructArray::from_fields(&[
            (
                "s",
                PrimitiveArray::from_iter(self.s_ids.clone()).into_array(),
            ),
            (
                "p",
                PrimitiveArray::from_iter(self.p_ids.clone()).into_array(),
            ),
            (
                "o",
                PrimitiveArray::from_iter(self.o_ids.clone()).into_array(),
            ),
            (
                "g",
                PrimitiveArray::from_iter(self.g_ids.clone()).into_array(),
            ),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(arr)
    }

    pub fn stats(&self) -> Result<RowGroupStats> {
        if self.is_empty() {
            return Err(VortexRdfError::Deserialization(
                "Cannot build stats for empty row group".to_string(),
            ));
        }

        Ok(RowGroupStats {
            row_group_id: self.row_group_id,
            block_start: self.global_start,
            block_end: self.global_end,

            min_s: *self.s_ids.iter().min().unwrap(),
            max_s: *self.s_ids.iter().max().unwrap(),

            min_p: *self.p_ids.iter().min().unwrap(),
            max_p: *self.p_ids.iter().max().unwrap(),

            min_o: *self.o_ids.iter().min().unwrap(),
            max_o: *self.o_ids.iter().max().unwrap(),

            min_g: *self.g_ids.iter().min().unwrap(),
            max_g: *self.g_ids.iter().max().unwrap(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct RowGroupStats {
    pub row_group_id: u32,
    pub block_start: u32,
    pub block_end: u32,

    pub min_s: u32,
    pub max_s: u32,

    pub min_p: u32,
    pub max_p: u32,

    pub min_o: u32,
    pub max_o: u32,

    pub min_g: u32,
    pub max_g: u32,
}

/// A Vortex store that preserves an SPOG ordering and emits zone map metadata.
/// This is a COTTAS-inspired layout built on top of Vortex arrays.
pub struct CottasVortexStore<Dict: RdfDictionary> {
    pub base: VortexRdfStore<Dict>,
    /// Row-group-level min/max statistics.
    pub row_group_stats: Option<ArrayRef>,
    /// Reserved for future Bloom filter metadata.
    pub bloom_filters: Option<ArrayRef>,
    pub metadata: FileMetadata,
}

impl<Dict: RdfDictionary> CottasVortexStore<Dict> {
    const DEFAULT_ROW_GROUP_SIZE: usize = 1024;

    /// Create a new store from a Vortex array.
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        let vortex_struct = vortex_array.to_struct();
        let base = VortexRdfStore::<Dict>::new(vortex_array.clone())?;
        let row_group_stats =
            utils::extract_vortex_struct_field_optional(&vortex_struct, "row_group_stats").or_else(
                || utils::extract_vortex_struct_field_optional(&vortex_struct, "zone_maps"),
            );
        let bloom_filters =
            utils::extract_vortex_struct_field_optional(&vortex_struct, "bloom_filters");
        let metadata = Self::extract_file_metadata_optional(&vortex_struct)?;
        Ok(Self {
            base,
            row_group_stats,
            bloom_filters,
            metadata,
        })
    }

    pub async fn build_spog_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        Self::build_ordered_vortex_index(
            quad_stream,
            TripleOrdering::SPO,
            Self::DEFAULT_ROW_GROUP_SIZE,
        )
        .await
    }

    pub async fn build_ordered_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
        ordering: TripleOrdering,
        row_group_size: usize,
    ) -> Result<ArrayRef> {
        IndexBuilder::build::<Dict, _>(quad_stream, CottasLayout::new(ordering, row_group_size))
            .await
    }

    fn build_file_metadata(metadata: &FileMetadata) -> Result<ArrayRef> {
        let metadata_struct = StructArray::from_fields(&[
            (
                "ordering",
                ConstantArray::new(metadata.ordering.as_str(), 1).into_array(),
            ),
            (
                "row_group_size",
                PrimitiveArray::from_iter(vec![metadata.row_group_size as u64]).into_array(),
            ),
            (
                "num_triples",
                PrimitiveArray::from_iter(vec![metadata.num_triples as u64]).into_array(),
            ),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        indexes::wrap_array_in_list(metadata_struct)
    }

    fn extract_file_metadata(vortex_struct: &StructArray) -> Result<FileMetadata> {
        let metadata_array = utils::extract_vortex_struct_field(vortex_struct, "file_metadata")?;
        let metadata_struct = metadata_array.to_struct();
        let fields = metadata_struct.fields();

        let ordering_str = fields
            .get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing ordering".to_string()))?
            .scalar_at(0)
            .to_string()
            .trim_matches('"')
            .to_string();
        let ordering = ordering_str.parse()?;

        let row_group_size = fields
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing row_group_size".to_string()))?
            .clone()
            .to_primitive()
            .as_slice::<u64>()[0] as usize;

        let num_triples = fields
            .get(2)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing num_triples".to_string()))?
            .clone()
            .to_primitive()
            .as_slice::<u64>()[0] as usize;

        Ok(FileMetadata {
            ordering,
            row_group_size,
            num_triples,
        })
    }

    fn extract_file_metadata_optional(vortex_struct: &StructArray) -> Result<FileMetadata> {
        if vortex_struct
            .names()
            .iter()
            .any(|n| n.as_ref() == "file_metadata")
        {
            Self::extract_file_metadata(vortex_struct)
        } else {
            let quads = utils::extract_vortex_struct_field(vortex_struct, "quads")?;
            let len = quads.len();
            Ok(FileMetadata {
                ordering: TripleOrdering::SPO,
                row_group_size: 0,
                num_triples: len,
            })
        }
    }

    fn build_row_group_pruning_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let stats_array = match &self.row_group_stats {
            Some(stats) => stats,
            None => return Ok(None),
        };

        let stats_struct = stats_array.to_struct();
        let fields = stats_struct.fields();

        let block_starts = fields
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing block_start".to_string()))?
            .clone()
            .to_primitive();

        let block_ends = fields
            .get(2)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing block_end".to_string()))?
            .clone()
            .to_primitive();

        let min_s = fields
            .get(3)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing min_s".to_string()))?
            .clone()
            .to_primitive();

        let max_s = fields
            .get(4)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing max_s".to_string()))?
            .clone()
            .to_primitive();

        let min_p = fields
            .get(5)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing min_p".to_string()))?
            .clone()
            .to_primitive();

        let max_p = fields
            .get(6)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing max_p".to_string()))?
            .clone()
            .to_primitive();

        let min_o = fields
            .get(7)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing min_o".to_string()))?
            .clone()
            .to_primitive();

        let max_o = fields
            .get(8)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing max_o".to_string()))?
            .clone()
            .to_primitive();

        let min_g = fields
            .get(9)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing min_g".to_string()))?
            .clone()
            .to_primitive();

        let max_g = fields
            .get(10)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing max_g".to_string()))?
            .clone()
            .to_primitive();

        let block_starts = block_starts.as_slice::<u32>();
        let block_ends = block_ends.as_slice::<u32>();

        let min_s = min_s.as_slice::<u32>();
        let max_s = max_s.as_slice::<u32>();

        let min_p = min_p.as_slice::<u32>();
        let max_p = max_p.as_slice::<u32>();

        let min_o = min_o.as_slice::<u32>();
        let max_o = max_o.as_slice::<u32>();

        let min_g = min_g.as_slice::<u32>();
        let max_g = max_g.as_slice::<u32>();

        let subject_id = match subject {
            Some(s) => match self.base.dictionary.get_id(&s.to_string()) {
                Some(id) => Some(id),
                None => {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            },
            None => None,
        };

        let predicate_id = match predicate {
            Some(p) => match self.base.dictionary.get_id(&p.to_string()) {
                Some(id) => Some(id),
                None => {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            },
            None => None,
        };

        let object_id = match object {
            Some(o) => match self.base.dictionary.get_id(&o.to_string()) {
                Some(id) => Some(id),
                None => {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            },
            None => None,
        };

        let graph_id = match graph {
            Some(g) => match self.base.dictionary.get_id(&g.to_string()) {
                Some(id) => Some(id),
                None => {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            },
            None => None,
        };

        let mut selected_indices: Vec<usize> = Vec::new();
        let mut selected_row_groups = 0usize;

        for row_group_index in 0..block_starts.len() {
            let mut candidate = true;

            if let Some(id) = subject_id {
                candidate &= id >= min_s[row_group_index] && id <= max_s[row_group_index];
            }

            if let Some(id) = predicate_id {
                candidate &= id >= min_p[row_group_index] && id <= max_p[row_group_index];
            }

            if let Some(id) = object_id {
                candidate &= id >= min_o[row_group_index] && id <= max_o[row_group_index];
            }

            if let Some(id) = graph_id {
                candidate &= id >= min_g[row_group_index] && id <= max_g[row_group_index];
            }

            if candidate {
                selected_row_groups += 1;

                let start = block_starts[row_group_index] as usize;
                let end = block_ends[row_group_index] as usize;

                for row in start..end {
                    selected_indices.push(row);
                }
            }
        }

        if selected_indices.is_empty() {
            return Ok(Some(
                ConstantArray::new(false, self.base.quads.len()).into_array(),
            ));
        }

        if selected_indices.len() == self.base.quads.len() {
            return Ok(None);
        }

        log::debug!(
            "[CottasVortexStore::row_group_pruning] selected row groups: {}/{}",
            selected_row_groups,
            block_starts.len()
        );

        let mask = BoolArray::from_indices(
            self.base.quads.len(),
            selected_indices,
            Validity::NonNullable,
        )
        .into_array();

        Ok(Some(mask))
    }

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        self.base.get_quads_array()
    }

    pub fn size(&self) -> usize {
        self.base.size()
    }

    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let quads_struct = self.base.quads.to_struct();
        let fields = quads_struct.fields();

        log::debug!(
            "Finding mask for pattern: s={:?}, p={:?}, o={:?}, g={:?}",
            subject,
            predicate,
            object,
            graph
        );

        let mut mask: Option<ArrayRef> =
            self.build_row_group_pruning_mask(subject, predicate, object, graph)?;

        if let Some(ref existing_mask) = mask {
            let canonical: Canonical = (&**existing_mask).to_canonical();

            if let Canonical::Bool(b) = canonical {
                if b.bit_buffer().true_count() == 0 {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            }
        }

        let mut combine_mask = |new_mask: ArrayRef| -> Result<()> {
            if let Some(m) = mask.take() {
                mask = Some(and(&m, &new_mask).map_err(VortexRdfError::Vortex)?);
            } else {
                mask = Some(new_mask);
            }

            Ok(())
        };

        let patterns = [
            (subject.map(|s| s.to_string()), 0, "Subject"),
            (predicate.map(|p| p.to_string()), 1, "Predicate"),
            (object.map(|o| o.to_string()), 2, "Object"),
            (graph.map(|g| g.to_string()), 3, "Graph"),
        ];

        for (term_opt, col_idx, label) in patterns {
            if let Some(term_str) = term_opt {
                let start = Instant::now();

                if let Some(id) = self.base.dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();

                    let scalar = Scalar::from(id)
                        .cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;

                    let column_mask = self.compare_with_pruning(col, &scalar)?;

                    log::debug!(
                        "[CottasVortexStore::find_mask] {} comparison took {:?}",
                        label,
                        start.elapsed()
                    );

                    combine_mask(column_mask)?;
                } else {
                    return Ok(Some(
                        ConstantArray::new(false, self.base.quads.len()).into_array(),
                    ));
                }
            }
        }

        Ok(mask)
    }

    fn compare_with_pruning(&self, col: &ArrayRef, scalar: &Scalar) -> Result<ArrayRef> {
        compare(
            col,
            &ConstantArray::new(scalar.clone(), col.len()).into_array(),
            Operator::Eq,
        )
        .map_err(VortexRdfError::Vortex)
    }

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        let num_triples = quads.len();

        Ok(Self {
            base: VortexRdfStore {
                dictionary: self.base.dictionary.clone(),
                quads,
            },
            row_group_stats: None,
            bloom_filters: None,
            metadata: FileMetadata {
                ordering: self.metadata.ordering,
                row_group_size: self.metadata.row_group_size,
                num_triples,
            },
        })
    }

    pub fn quads(
        &self,
    ) -> Result<Box<dyn futures::Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.base.quads()
    }

    pub async fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let start = Instant::now();

        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
            let canonical: Canonical = (&*m).to_canonical();

            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Mask must be boolean".to_string(),
                    ));
                }
            };

            let quads_array_ref = self.get_quads_array()?;

            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;

            let filtered_store = self.with_quads(filtered_quads)?;

            log::debug!(
                "[CottasVortexStore::match_pattern] Pattern matching took overall {:?}",
                start.elapsed()
            );

            Ok(filtered_store)
        } else {
            Ok(Self {
                base: VortexRdfStore {
                    dictionary: self.base.dictionary.clone(),
                    quads: self.base.quads.clone(),
                },
                row_group_stats: self.row_group_stats.clone(),
                bloom_filters: self.bloom_filters.clone(),
                metadata: self.metadata.clone(),
            })
        }
    }
}

impl<D: RdfDictionary> crate::store::QuadStore for CottasVortexStore<D> {
    fn quads(&self) -> Result<Box<dyn futures::Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.quads()
    }
}
