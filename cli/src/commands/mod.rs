//! The three subcommand bodies, one file per [`crate::Action`] arm; `main.rs`
//! keeps only the clap definitions and the dispatch. The `match` arm lives in
//! [`matching`] because `match` is a keyword.

pub mod deserialize;
pub mod matching;
pub mod serialize;
