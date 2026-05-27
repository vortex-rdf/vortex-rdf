use std::sync::Arc;
use futures::Stream;
use vortex_array::{ArrayRef, IntoArray};
use vortex_array::arrays::{PrimitiveArray, StructArray, ChunkedArray};
use vortex_array::validity::Validity;
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use oxrdf::Quad;

use clap::ValueEnum;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum BuilderStrategy {
    /// Natural insertion order, no sorting, index-free for maximum ingestion speed.
    UnsortedInMemory,
    /// Global sort of quads by Subject -> Predicate -> Object -> Graph in memory.
    SortedInMemory,
    /// Local sorting of quads inside individual chunks.
    ChunkSort,
    /// External-memory out-of-core global sort using merge runs.
    GlobalSort,
}

impl BuilderStrategy {
    pub const fn as_str(&self) -> &'static str {
        match self {
            BuilderStrategy::UnsortedInMemory => "unsorted-in-memory",
            BuilderStrategy::SortedInMemory => "sorted-in-memory",
            BuilderStrategy::ChunkSort => "chunk-sort",
            BuilderStrategy::GlobalSort => "global-sort",
        }
    }
}

pub mod unsorted_in_memory;
pub mod sorted_in_memory;
pub mod chunk_sort;
pub mod global_sort;

pub use unsorted_in_memory::UnsortedInMemoryBuilder;
pub use sorted_in_memory::SortedInMemoryBuilder;
pub use chunk_sort::ChunkSortBuilder;
pub use global_sort::GlobalSortBuilder;

pub trait VortexArrayBuilder<Dict: RdfDictionary> {
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>
    ) -> impl std::future::Future<Output = Result<ArrayRef>> + Send;
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct EncodedQuad {
    pub s: u32,
    pub p: u32,
    pub o: u32,
    pub g: u32,
}

impl Ord for EncodedQuad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.s.cmp(&other.s)
            .then_with(|| self.p.cmp(&other.p))
            .then_with(|| self.o.cmp(&other.o))
            .then_with(|| self.g.cmp(&other.g))
    }
}

impl PartialOrd for EncodedQuad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn assemble_chunks(mut chunks: Vec<ArrayRef>) -> Result<ArrayRef> {
    if chunks.is_empty() {
        let field_names: Vec<Arc<str>> = vec![
            "s".into(), "p".into(), "o".into(), "g".into(),
            "_idx_o_val".into(), "_idx_o_rid".into(),
            "_idx_p_val".into(), "_idx_p_rid".into(),
        ];
        let empty_struct = StructArray::try_new(
            field_names.into(),
            vec![
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ],
            0,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        Ok(empty_struct)
    } else if chunks.len() == 1 {
        Ok(chunks.remove(0))
    } else {
        let dtype = chunks[0].dtype().clone();
        let chunked_arr = ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        Ok(chunked_arr)
    }
}
