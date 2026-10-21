//! Textual RDF, shared by the store's decode paths and every binding: term
//! parsing and reconstruction ([`terms`]), the N-Triples-form quad (`quad`),
//! the RDF/XSD datatype IRIs (`vocab`) and format-name resolution
//! ([`formats`]).

pub mod formats;
pub(crate) mod quad;
pub mod terms;
pub(crate) mod vocab;
