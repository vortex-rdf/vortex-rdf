pub mod chained_hash_store;
pub mod dictionary_store;

use crate::error::Result;
use futures::Stream;
use std::future::Future;
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use vortex_array::ArrayRef;

pub trait VortexRdfStore {
    fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static
    ) -> impl Future<Output = Result<ArrayRef>>;
    
    fn size(&self) -> usize;

    fn add_quad(
        &self,
        quad: Quad,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    fn delete_quad(
        &self,
        quad: &Quad,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>>;
}