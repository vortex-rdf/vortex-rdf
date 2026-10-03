//! The Dictionary layout's term dictionary: s/p/o/g stored as u32 codes
//! into one sorted term dictionary, held beside the array in memory and
//! written as the native container's `dictionary` child. Codes are the
//! sorted ranks of the N-Triples spellings, so code order is byte order.

pub(crate) mod codec;
#[cfg(feature = "file-io")]
pub(crate) mod file_backed;
pub(crate) mod handles;
pub(crate) mod ingest;
pub mod predicates;
pub(crate) mod storage;
pub(crate) mod term_dict;

pub(crate) use self::codec::{
    QuadCodes, build_chunk, build_code_chunk, decode_chunk, decode_chunk_shared,
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
pub(crate) use self::ingest::{code_map, sorted_unique_terms};
// Compiled out with the out-of-core builder on wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::codec::code_of;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::ingest::{TermCodeMap, TermDictionaryBuilder};
pub use self::predicates::{Domain, KindRanges, NumOp, TermPredicate, Verdict};
pub(crate) use self::term_dict::TermDictionary;
