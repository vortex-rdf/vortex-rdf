//! Resolving a pattern into a view (`matching`) and narrowing a view beyond
//! its pattern ([`pushdown`]: keeps and windows).
mod matching;
pub(crate) mod pushdown;
