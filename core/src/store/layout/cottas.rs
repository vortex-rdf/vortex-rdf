use crate::common::indexes;
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use crate::store::layout::flat::FlatLayout;
use crate::store::layout::{RdfQuadLayout, RdfQuadLayoutBuilder};
use futures::{Stream, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::str::FromStr;
use std::sync::Arc;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{BoolArray, ChunkedArray, ConstantArray, PrimitiveArray, StructArray};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray};
use vortex_array::{LEGACY_SESSION, VortexSessionExecute};

#[derive(Clone, Debug)]
pub struct CottasLayout;

#[derive(Clone, Debug)]
pub struct Triple {
    pub s: String,
    pub p: String,
    pub o: String,
    pub g: String,
}

impl Triple {
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

impl FromStr for TripleOrdering {
    type Err = VortexRdfError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "SPO" | "spo" => Ok(TripleOrdering::SPO),
            "PSO" | "pso" => Ok(TripleOrdering::PSO),
            "OSP" | "osp" => Ok(TripleOrdering::OSP),
            _ => Err(VortexRdfError::Deserialization(format!(
                "Unknown TripleOrdering: {}",
                value,
            ))),
        }
    }
}

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

pub struct CottasLayoutBuilder {
    ordering: TripleOrdering,
    row_group_size: usize,
    buffer: Vec<Triple>,
    raw_row_groups: Vec<Vec<Triple>>,
    encoded_row_groups: Vec<EncodedRowGroup>,
}

impl CottasLayoutBuilder {
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
        let mut terms: Vec<&str> = self
            .raw_row_groups
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
        let _ = dictionary.get_or_insert_bulk(&terms.iter().copied().collect::<Vec<_>>());
    }

    fn encode_row_groups<Dict: RdfDictionary>(&mut self, dictionary: &Dict) -> Result<()> {
        let mut global_start = 0u32;

        for (group_id, group) in self.raw_row_groups.iter().enumerate() {
            let s_ids = group
                .iter()
                .map(|quad| {
                    dictionary
                        .get_id(&quad.s)
                        .expect("dictionary seeded before encoding")
                })
                .collect::<Vec<_>>();
            let p_ids = group
                .iter()
                .map(|quad| {
                    dictionary
                        .get_id(&quad.p)
                        .expect("dictionary seeded before encoding")
                })
                .collect::<Vec<_>>();
            let o_ids = group
                .iter()
                .map(|quad| {
                    dictionary
                        .get_id(&quad.o)
                        .expect("dictionary seeded before encoding")
                })
                .collect::<Vec<_>>();
            let g_ids = group
                .iter()
                .map(|quad| {
                    dictionary
                        .get_id(&quad.g)
                        .expect("dictionary seeded before encoding")
                })
                .collect::<Vec<_>>();

            let group_size = group.len() as u32;
            let encoded = EncodedRowGroup {
                row_group_id: group_id as u32,
                global_start,
                global_end: global_start + group_size,
                s_ids,
                p_ids,
                o_ids,
                g_ids,
            };

            self.encoded_row_groups.push(encoded);
            global_start += group_size;
        }

        Ok(())
    }

    fn num_triples(&self) -> usize {
        self.raw_row_groups.iter().map(|group| group.len()).sum()
    }

    fn build_row_group_stats_array(&self) -> Result<ArrayRef> {
        let mut row_group_id = Vec::new();
        let mut block_start = Vec::new();
        let mut block_end = Vec::new();
        let mut min_s = Vec::new();
        let mut max_s = Vec::new();
        let mut min_p = Vec::new();
        let mut max_p = Vec::new();
        let mut min_o = Vec::new();
        let mut max_o = Vec::new();
        let mut min_g = Vec::new();
        let mut max_g = Vec::new();

        for group in &self.encoded_row_groups {
            row_group_id.push(group.row_group_id);
            block_start.push(group.global_start);
            block_end.push(group.global_end);
            min_s.push(*group.s_ids.iter().min().unwrap_or(&0));
            max_s.push(*group.s_ids.iter().max().unwrap_or(&0));
            min_p.push(*group.p_ids.iter().min().unwrap_or(&0));
            max_p.push(*group.p_ids.iter().max().unwrap_or(&0));
            min_o.push(*group.o_ids.iter().min().unwrap_or(&0));
            max_o.push(*group.o_ids.iter().max().unwrap_or(&0));
            min_g.push(*group.g_ids.iter().min().unwrap_or(&0));
            max_g.push(*group.g_ids.iter().max().unwrap_or(&0));
        }

        let stats = StructArray::from_fields(&[
            (
                "row_group_id",
                PrimitiveArray::from_iter(row_group_id).into_array(),
            ),
            (
                "block_start",
                PrimitiveArray::from_iter(block_start).into_array(),
            ),
            (
                "block_end",
                PrimitiveArray::from_iter(block_end).into_array(),
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

        Ok(stats)
    }

    fn empty_quads_array() -> Result<ArrayRef> {
        let quads = StructArray::from_fields(&[
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

        Ok(quads)
    }
}

impl EncodedRowGroup {
    fn to_struct_array(&self) -> Result<ArrayRef> {
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
}

impl<Dict> RdfQuadLayoutBuilder<Dict> for CottasLayoutBuilder
where
    Dict: RdfDictionary,
{
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
        let file_metadata = build_file_metadata(&FileMetadata {
            ordering: self.ordering,
            row_group_size: self.row_group_size,
            num_triples: self.num_triples(),
        })?;

        Ok(vec![
            ("storage_layout".into(), storage_layout),
            ("row_group_stats".into(), row_group_stats_list),
            ("file_metadata".into(), file_metadata),
        ])
    }
}

impl<Dict> RdfQuadLayout<Dict> for CottasLayout
where
    Dict: RdfDictionary,
{
    const STORAGE_LAYOUT: &'static str = "cottas-vortex-row-groups";

    fn empty_quads() -> Result<ArrayRef> {
        CottasLayoutBuilder::empty_quads_array()
    }

    fn extract_quads(root: &ArrayRef) -> Result<ArrayRef> {
        <FlatLayout as RdfQuadLayout<Dict>>::extract_quads(root)
    }

    async fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        super::IndexBuilder::build::<Dict, _>(
            quad_stream,
            CottasLayoutBuilder::new(TripleOrdering::SPO, 1024),
        )
        .await
    }

    fn quads<'a>(
        dictionary: &'a Dict,
        quads: &'a ArrayRef,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'a>> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();

        let quads_struct = quads
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let fields = quads_struct.unmasked_fields();

        let s_ids = fields
            .get(3)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing COTTAS S IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let p_ids = fields
            .get(4)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing COTTAS P IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let o_ids = fields
            .get(5)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing COTTAS O IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let g_ids = fields
            .get(6)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing COTTAS G IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let len = s_ids.len();

        let iter = (0..len).map(move |i| {
            let s_id = s_ids.as_slice::<u32>()[i];
            let p_id = p_ids.as_slice::<u32>()[i];
            let o_id = o_ids.as_slice::<u32>()[i];
            let g_id = g_ids.as_slice::<u32>()[i];

            let s_term = dictionary.get_term(s_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("S ID {} not in dictionary", s_id))
            })?;

            let p_term = dictionary.get_term(p_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("P ID {} not in dictionary", p_id))
            })?;

            let o_term = dictionary.get_term(o_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("O ID {} not in dictionary", o_id))
            })?;

            let g_name = dictionary.get_graph_name(g_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("G ID {} not in dictionary", g_id))
            })?;

            let subject = match s_term {
                Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
                Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Invalid subject type".to_string(),
                    ));
                }
            };

            let predicate = match p_term {
                Term::NamedNode(n) => n,
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Invalid predicate type".to_string(),
                    ));
                }
            };

            Ok(Quad::new(subject, predicate, o_term, g_name))
        });

        Ok(Box::new(stream::iter(iter)))
    }

    fn find_mask(
        dictionary: &Dict,
        quads: &ArrayRef,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let mut ctx = LEGACY_SESSION.create_execution_ctx();

        let quads_struct = quads
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let fields = quads_struct.unmasked_fields();

        let mut mask: Option<ArrayRef> = None;

        let mut combine_mask = |new_mask: ArrayRef| -> Result<()> {
            if let Some(existing) = mask.take() {
                mask = Some(
                    existing
                        .binary(new_mask, Operator::And)
                        .map_err(VortexRdfError::Vortex)?,
                );
            } else {
                mask = Some(new_mask);
            }

            Ok(())
        };

        let patterns = [
            (subject.map(|s| s.to_string()), 3usize, "Subject"),
            (predicate.map(|p| p.to_string()), 4usize, "Predicate"),
            (object.map(|o| o.to_string()), 5usize, "Object"),
            (graph.map(|g| g.to_string()), 6usize, "Graph"),
        ];

        for (term_opt, col_idx, _label) in patterns {
            if let Some(term_str) = term_opt {
                if let Some(id) = dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).ok_or_else(|| {
                        VortexRdfError::Deserialization(format!(
                            "Missing COTTAS column at index {}",
                            col_idx
                        ))
                    })?;

                    let scalar = Scalar::from(id)
                        .cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;

                    let column_mask = col
                        .binary(
                            ConstantArray::new(scalar, col.len()).into_array(),
                            Operator::Eq,
                        )
                        .map_err(VortexRdfError::Vortex)?;

                    combine_mask(column_mask)?;
                } else {
                    return Ok(Some(ConstantArray::new(false, quads.len()).into_array()));
                }
            }
        }

        Ok(mask)
    }

    fn add_quad(dictionary: &mut Dict, quads: &ArrayRef, quad: Quad) -> Result<ArrayRef> {
        FlatLayout::add_quad(dictionary, quads, quad)
    }

    fn delete_quad(dictionary: &Dict, quads: &ArrayRef, quad: &Quad) -> Result<ArrayRef> {
        let mask = Self::find_mask(
            dictionary,
            quads,
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )?;

        if let Some(m) = mask {
            let inverse_mask = m.not().map_err(VortexRdfError::Vortex)?;

            let mut ctx = LEGACY_SESSION.create_execution_ctx();

            let bool_arr = inverse_mask
                .execute::<BoolArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;

            let canonical_mask = bool_arr.to_mask_fill_null_false(&mut ctx);

            let filtered = quads
                .filter(canonical_mask)
                .map_err(VortexRdfError::Vortex)?;

            Ok(filtered)
        } else {
            Ok(quads.clone())
        }
    }

    fn append_quads_chunked(
        dictionary: &mut Dict,
        quads: &ArrayRef,
        new_quads: Vec<Quad>,
        chunk_size: usize,
    ) -> Result<ArrayRef> {
        <FlatLayout as RdfQuadLayout<Dict>>::append_quads_chunked(
            dictionary, quads, new_quads, chunk_size,
        )
    }

    fn compact_quads(quads: &ArrayRef) -> Result<ArrayRef> {
        <FlatLayout as RdfQuadLayout<Dict>>::compact_quads(quads)
    }
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

    Ok(metadata_struct)
}
