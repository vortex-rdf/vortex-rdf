pub mod chained_hash;
pub mod simple_dictionary;
pub mod simple_dictionary_view;

// Re-export dictionary implementations
pub use chained_hash::ChainedHash;
pub use simple_dictionary::SimpleDictionary;
pub use simple_dictionary_view::SimpleDictionaryView;

use crate::error::Result;
use vortex_array::ArrayRef;
use vortex_array::arrays::VarBinViewArray;
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

    /// Get a term by its ID.
    ///
    /// Prefer [`values_view`] for bulk access (e.g. full deserialization).
    /// This method is best suited for sparse access such as decoding a small
    /// number of matching rows after a `match_pattern` filter.
    fn get_term(&self, id: u32) -> Option<Term>;

    /// Get a graph name by its ID.
    ///
    /// Same guidance as [`get_term`]: use [`values_view`] for bulk loops.
    fn get_graph_name(&self, id: u32) -> Option<GraphName>;

    /// Decode the underlying values array **once** and return it for bulk per-row
    /// lookups during a full scan.
    ///
    /// Call this a single time at the start of a `quads()` loop and then use
    /// `VarBinViewArray::bytes_at(id)` for each row.  This avoids the per-call
    /// `execute()` + context-creation overhead that `get_term` / `get_graph_name`
    /// would otherwise incur for every single row.
    ///
    /// The returned array is a decoded in-memory view.
    fn values_view(&self) -> Result<VarBinViewArray>;

    /// Encode the dictionary into a list of named Vortex arrays
    fn to_vortex_array(&self) -> Result<Vec<(String, ArrayRef)>>;

    /// Get the store type identifier for this dictionary implementation
    fn store_type() -> &'static str;

    /// Get the list of column suffix names used by this dictionary in serialized representation
    fn vortex_field_names() -> &'static [&'static str];

    fn term_id_pairs(&self) -> Vec<(u32, String)>;
}
