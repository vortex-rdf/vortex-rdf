//! The Dictionary layout's term dictionary: s/p/o/g stored as u32 codes
//! into one sorted term dictionary that travels beside the array in memory
//! and as the native container's `dictionary` child in a file.
//!
//! [`codec`] encodes and decodes chunks, [`ingest`] collects and interns
//! terms at build time, [`storage`] holds a resident dictionary's terms,
//! [`term_dict`] is its lookup API, [`file_backed`] reads terms from the
//! file child on demand, [`handles`] are the access seam and the public
//! handles, and [`predicates`] evaluates term predicates on spellings.

pub(crate) mod codec;
#[cfg(feature = "file-io")]
pub(crate) mod file_backed;
pub(crate) mod handles;
pub(crate) mod ingest;
pub mod predicates;
pub(crate) mod storage;
pub(crate) mod term_dict;

pub(crate) use self::codec::{
    COLUMNS, QuadCodes, build_chunk, build_code_chunk, code_of, decode_chunk, decode_chunk_shared,
    decode_code_column, empty_struct, encode_quads,
};
#[cfg(feature = "file-io")]
pub(crate) use self::codec::{
    decode_chunk_mapped, decode_chunk_mapped_shared, resolve_chunk_terms,
};
#[cfg(feature = "file-io")]
pub(crate) use self::file_backed::FileBackedDict;
pub(crate) use self::handles::DictAccess;
pub use self::handles::{DictReader, DictSnapshot};
pub use self::ingest::DictionaryQuadSink;
// Read only by the out-of-core builder, which is compiled out on
// wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::ingest::{TermCodeMap, TermDictionaryBuilder};
pub use self::predicates::{Domain, KindRanges, NumOp, TermPredicate, Verdict};
pub(crate) use self::term_dict::TermDictionary;
