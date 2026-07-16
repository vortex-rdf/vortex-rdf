pub mod cottas_native_ids;
pub mod cottas_native_strings;
pub mod de;
pub mod ser;
pub mod utils;

pub use de::{array_from_ipc_reader, deserialize};

#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, open_vortex_file};

pub use ser::{quads_stream_to_vortex, quads_stream_to_vortex_writer, serialize};

pub use cottas_native_ids::{
    CottasNativeConfig, CottasNativeIdsDiagnostics, NativeIdsCountMode,
    build_cottas_native_o_exact_ranges_index, build_cottas_native_po_predicate_partitions_v2,
    build_cottas_native_subject_range_index, count_cottas_native_ids_file_with_diagnostics,
    count_cottas_native_ids_file_with_diagnostics_mode, match_cottas_native_file,
    match_cottas_native_file_as_triples, match_cottas_native_file_as_triples_optimized,
    match_cottas_native_file_with_diagnostics, serialize_cottas_native_file,
};

pub use cottas_native_strings::{
    CottasNativeStringConfig, NativeStringCountMode, count_cottas_native_string_file,
    count_cottas_native_string_file_with_diagnostics,
    count_cottas_native_string_file_with_diagnostics_mode, match_cottas_native_string_file,
    match_cottas_native_string_file_as_triples, match_cottas_native_string_file_with_diagnostics,
    serialize_cottas_native_string_file,
};

pub use utils::CottasVortexCompressionProfile;
