//! Textual RDF vocabulary, shared by the store's decode paths and every
//! binding: term parsing/reconstruction ([`terms`]), the N-Triples-form quad
//! (`quad`), the RDF/XSD datatype IRIs read off spellings (`vocab`), and
//! format-name resolution ([`formats`]).
//!
//! Items here are about RDF *text*: how terms and quads are spelled, parsed,
//! named and written. Export consumes a store but produces text only;
//! anything that touches arrays, layouts, or the container format belongs in
//! [`store`](crate::store) or [`io`](crate::io).

pub mod formats;
pub(crate) mod quad;
pub mod terms;
pub(crate) mod vocab;
