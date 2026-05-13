pub mod simple_dictionary;
pub mod chained_hash;

// Re-export dictionary implementations
pub use simple_dictionary::SimpleDictionary;
pub use chained_hash::ChainedHash;

use crate::error::Result;
use vortex_array::ArrayRef;
use oxrdf::{GraphName, Term};

/// Trait for RDF dictionary implementations that map between terms and IDs
pub trait RdfDictionary: Clone + Send + Sync {
    /// Create a new empty dictionary
    fn new() -> Self;
    
    /// Build a dictionary from a Vortex array representation
    fn from_vortex_array(vortex_array: &ArrayRef) -> Result<Self>;
    
    /// Get or insert a term, returning its ID
    fn get_or_insert(&mut self, term_str: &str) -> u32;

    /// Get or insert multiple terms, returning their IDs
    fn get_or_insert_bulk(&mut self, terms: &[&str]) -> Vec<u32>;
    
    /// Get the ID for a term, if it exists
    fn get_id(&self, term_str: &str) -> Option<u32>;
    
    /// Get a term by its ID
    fn get_term(&self, id: u32) -> Option<Term>;
    
    /// Get a graph name by its ID
    fn get_graph_name(&self, id: u32) -> Option<GraphName>;
    
    /// Encode the dictionary into a list of named Vortex arrays
    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>>;
    
    /// Get the store type identifier for this dictionary implementation
    fn store_type() -> &'static str;
}
