//! Textual RDF vocabulary, shared by the store's decode paths and every
//! binding: term parsing/reconstruction ([`terms`]), the N-Triples-form quad
//! ([`quad`]), textual export ([`export`]), and format-name resolution
//! ([`formats`]).
//!
//! The boundary is the reason this module can be named `common` without
//! becoming a grab bag: an item belongs here only when it is about RDF *text*
//! and nothing about Vortex. Anything that touches arrays, layouts, or the
//! container format belongs in [`store`](crate::store) or [`io`](crate::io).

pub mod export;
pub mod formats;
pub(crate) mod quad;
pub mod terms;
