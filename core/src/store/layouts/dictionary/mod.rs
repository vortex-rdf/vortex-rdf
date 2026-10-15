//! The term-dictionary subsystem of [`LayoutStrategy::Dictionary`]. This
//! folder is the whole of it: [`codec`] owns the chunk encode/decode paths,
//! [`ingest`] the build-side term collection and interning, [`storage`] how
//! a resident dictionary holds its terms and the dictionary child's read
//! and write sides, [`term_dict`] the frozen dictionary itself and its
//! lookups, [`file_backed`] the on-demand file residency, [`handles`] the
//! residency seam the resolved layout speaks through and the public
//! handles, and [`predicates`] the term predicates evaluated on spellings.
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
    QuadCodes, build_chunk, build_code_chunk, decode_chunk, decode_chunk_shared, empty_struct,
    encode_quads,
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
pub use self::predicates::{CaseMap, KindRanges, NumOp, TermPredicate, TextOptions, Verdict};
pub(crate) use self::term_dict::TermDictionary;

// Read only by the out-of-core builder, which is compiled out on
// wasm32-unknown-unknown.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::codec::code_of;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::ingest::{TermCodeMap, TermDictionaryBuilder};
