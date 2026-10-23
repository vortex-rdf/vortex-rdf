//! The term dictionary of [`LayoutStrategy::Dictionary`]: s/p/o/g stored as
//! codes into one sorted dictionary of N-Triples spellings, a term's code
//! being its rank, so code order is byte order. [`codec`] encodes and
//! decodes chunks, [`ingest`] collects and interns terms at build time,
//! [`storage`] holds a resident dictionary's terms and reads and writes the
//! `dictionary` child, [`term_dict`] is the frozen dictionary,
//! [`file_backed`] reads the terms from the file on demand, [`handles`] is
//! the residency seam and the public handles, and [`predicates`] evaluates
//! term predicates on spellings.
//!
//! [`LayoutStrategy::Dictionary`]: super::LayoutStrategy::Dictionary

mod codec;
#[cfg(feature = "file-io")]
pub(crate) mod file_backed;
mod handles;
pub(crate) mod ingest;
pub mod predicates;
pub(crate) mod storage;
pub(crate) mod term_dict;

pub(super) use self::codec::decode_code_column;
pub(crate) use self::codec::{
    QuadCodes, build_code_chunk, decode_chunk, decode_chunk_shared, empty_struct, encode_quads,
};
#[cfg(feature = "file-io")]
pub(crate) use self::codec::{
    decode_chunk_file_backed, decode_chunk_raw_file_backed, decode_chunk_shared_file_backed,
};
#[cfg(feature = "file-io")]
pub(crate) use self::file_backed::FileBackedDict;
pub(crate) use self::handles::DictAccess;
pub use self::handles::{DictReader, DictSnapshot};
pub use self::ingest::DictionaryQuadSink;
pub use self::predicates::{
    CaseMap, KindRanges, NumOp, Number, TermPredicate, TextOptions, TextPredicate, Verdict,
};
pub(crate) use self::term_dict::TermDictionary;
