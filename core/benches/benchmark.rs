use divan::Bencher;
use tokio::runtime::Runtime;
use oxrdf::{NamedOrBlankNode, NamedNode};
use std::sync::{OnceLock, Mutex};
use std::collections::HashMap;
use std::hint::black_box;
use futures::TryStreamExt;
use vortex_array::ArrayRef;

use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::{
    io::quads_stream_to_vortex_writer_with_builder,
    IndexType,
    VortexRdfStore,
    VortexArrayBuilder,
    SortedInMemoryBuilder,
    SortedStreamBuilder,
    UnsortedStreamBuilder,
    LayoutStrategy,
};

fn main() {
    divan::main();
}

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

#[derive(Copy, Clone, Debug)]
enum IndexConfig {
    None,
    SecondaryByReference,
    SecondaryByCopy,
}

impl IndexConfig {
    fn name(self) -> &'static str {
        match self {
            Self::None => "no-index",
            Self::SecondaryByReference => "secondary-by-reference",
            Self::SecondaryByCopy => "secondary-by-copy",
        }
    }

    fn indexes(self) -> Vec<IndexType> {
        match self {
            Self::None => vec![],
            Self::SecondaryByReference => vec![IndexType::SecondaryByReference],
            Self::SecondaryByCopy => vec![IndexType::SecondaryByCopy],
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum SourceConfig {
    File,
    InMemory,
}

impl SourceConfig {
}

static ARRAY_CACHE: OnceLock<Mutex<HashMap<
    (&'static str, &'static str, &'static str, usize), 
    ArrayRef
>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<
    (&'static str, &'static str, &'static str, usize), 
    std::path::PathBuf
>>> = OnceLock::new();

fn get_cached_array<Builder: VortexArrayBuilder>(
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_config: IndexConfig,
    size: usize,
) -> ArrayRef {
    let cache = ARRAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = cache.lock().unwrap();
    let index_name = index_config.name();
    let key = (builder_name, layout_name, index_name, size);
    if let Some(arr) = lock.get(&key) {
        return arr.clone();
    }

    let rt = get_runtime();
    let quad_stream = generate_rdf_data_stream(size);
    let arr = rt.block_on(async {
        VortexRdfStore::build_vortex_array_with_builder::<Builder>(
            quad_stream, layout, index_config.indexes(),
        )
        .await
        .expect("Failed to build vortex array")
    });

    lock.insert(key, arr.clone());
    arr
}

fn get_cached_file_path<Builder: VortexArrayBuilder>(
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_config: IndexConfig,
    size: usize,
) -> std::path::PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = cache.lock().unwrap();
    let index_name = index_config.name();
    let key = (builder_name, layout_name, index_name, size);
    if let Some(path) = lock.get(&key) {
        return path.clone();
    }

    let rt = get_runtime();
    let quad_stream = generate_rdf_data_stream(size);
    let arr = rt.block_on(async {
        VortexRdfStore::build_vortex_array_with_builder::<Builder>(
            quad_stream, layout, index_config.indexes(),
        )
        .await
        .expect("Failed to build vortex array")
    });

    let filename = format!(
        "{}_{}_{}_{}.vortex",
        builder_name,
        layout_name,
        index_name,
        size
    );
    std::fs::create_dir_all("target/bench_vortex_files").unwrap();
    let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);

    rt.block_on(async {
        let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
        vortex_rdf_core::io::serialize(arr, writer).await.expect("Failed to serialize");
    });

    lock.insert(key, filepath.clone());
    filepath
}

// ==================== SERIALIZATION MATRIX ====================

macro_rules! define_serialize_bench {
    ($name:ident, $builder:ty, $layout:expr) => {
        #[divan::bench(
            consts = [10_000, 100_000, 1_000_000],
            args = [
                IndexConfig::None,
                IndexConfig::SecondaryByReference,
                IndexConfig::SecondaryByCopy,
            ],
            sample_count = 10
        )]
        fn $name<const SIZE: usize>(bencher: Bencher, index_config: IndexConfig) {
            let rt = get_runtime();
            bencher
                .with_inputs(|| generate_rdf_data_stream(SIZE))
                .bench_values(|quad_stream| {
                    rt.block_on(async {
                        let mut buffer = Vec::new();
                        quads_stream_to_vortex_writer_with_builder::<$builder, _, _>(
                            quad_stream,
                            &mut buffer,
                            $layout,
                            index_config.indexes(),
                        )
                        .await
                        .expect("Failed to serialize with builder/layout");
                        black_box(buffer.len())
                    })
                });
        }
    };
}

define_serialize_bench!(serialize_sorted_in_memory_default, SortedInMemoryBuilder, LayoutStrategy::Default);
define_serialize_bench!(serialize_sorted_in_memory_typed_object, SortedInMemoryBuilder, LayoutStrategy::TypedObject);
define_serialize_bench!(serialize_sorted_in_memory_dictionary, SortedInMemoryBuilder, LayoutStrategy::Dictionary);

define_serialize_bench!(serialize_sorted_stream_default, SortedStreamBuilder, LayoutStrategy::Default);
define_serialize_bench!(serialize_sorted_stream_typed_object, SortedStreamBuilder, LayoutStrategy::TypedObject);
define_serialize_bench!(serialize_sorted_stream_dictionary, SortedStreamBuilder, LayoutStrategy::Dictionary);

define_serialize_bench!(serialize_unsorted_stream_default, UnsortedStreamBuilder, LayoutStrategy::Default);
define_serialize_bench!(serialize_unsorted_stream_typed_object, UnsortedStreamBuilder, LayoutStrategy::TypedObject);
define_serialize_bench!(serialize_unsorted_stream_dictionary, UnsortedStreamBuilder, LayoutStrategy::Dictionary);

// ==================== MATCH QUERY MATRIX ====================

#[derive(Copy, Clone, Debug)]
enum QueryPattern {
    S,
    P,
    O,
    G,
    SP,
    SO,
    SG,
    PO,
    PG,
    OG,
    SPO,
    SPG,
    SOG,
    POG,
    SPOG,
}

fn terms_for_pattern(
    pattern: QueryPattern,
) -> (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<oxrdf::Term>,
    Option<oxrdf::GraphName>,
) {
    let s = Some(NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0")));
    let p = Some(NamedNode::new_unchecked("http://example.org/predicate/0"));
    let o = Some(oxrdf::Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0")));
    let g = Some(oxrdf::GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph")));

    match pattern {
        QueryPattern::S => (s, None, None, None),
        QueryPattern::P => (None, p, None, None),
        QueryPattern::O => (None, None, o, None),
        QueryPattern::G => (None, None, None, g),
        QueryPattern::SP => (s, p, None, None),
        QueryPattern::SO => (s, None, o, None),
        QueryPattern::SG => (s, None, None, g),
        QueryPattern::PO => (None, p, o, None),
        QueryPattern::PG => (None, p, None, g),
        QueryPattern::OG => (None, None, o, g),
        QueryPattern::SPO => (s, p, o, None),
        QueryPattern::SPG => (s, p, None, g),
        QueryPattern::SOG => (s, None, o, g),
        QueryPattern::POG => (None, p, o, g),
        QueryPattern::SPOG => (s, p, o, g),
    }
}

fn run_match_bench_for_combo<Builder: VortexArrayBuilder, const SIZE: usize>(
    bencher: Bencher,
    builder_name: &'static str,
    layout_name: &'static str,
    layout: LayoutStrategy,
    index_config: IndexConfig,
    source: SourceConfig,
    pattern: QueryPattern,
) {
    bencher
        .with_inputs(|| {
            let rt = get_runtime();
            match source {
                SourceConfig::File => {
                    let path = get_cached_file_path::<Builder>(
                        builder_name,
                        layout_name,
                        layout,
                        index_config,
                        SIZE,
                    );
                    rt.block_on(async {
                        VortexRdfStore::from_file(path)
                            .await
                            .expect("Failed to create file-backed store")
                    })
                }
                SourceConfig::InMemory => {
                    let arr = get_cached_array::<Builder>(
                        builder_name,
                        layout_name,
                        layout,
                        index_config,
                        SIZE,
                    );
                    VortexRdfStore::new(arr).expect("Failed to create in-memory store")
                }
            }
        })
        .bench_values(|store| {
            let (s_term, p_term, o_term, g_term) = terms_for_pattern(pattern);

            let rt = get_runtime();
            rt.block_on(async {
                let filtered = store
                    .match_pattern(
                        s_term.as_ref(),
                        p_term.as_ref(),
                        o_term.as_ref(),
                        g_term.as_ref(),
                    )
                    .await
                    .expect("Failed to match pattern");
                // `match_pattern` returns a lazy derived store. Force execution
                // by materializing the matched quads in this benchmark.
                let matched: Vec<_> = filtered
                    .quads()
                    .expect("Failed to create quad stream")
                    .try_collect()
                    .await
                    .expect("Failed to execute filtered query");
                black_box(matched)
            })
        });
}

macro_rules! define_match_matrix_bench {
    ($name:ident, $builder:ty, $builder_name:expr, $layout:expr, $layout_name:expr, $index_config:expr, $source:expr) => {
        #[divan::bench(
            consts = [10_000, 100_000, 1_000_000],
            args = [
                QueryPattern::S,
                QueryPattern::P,
                QueryPattern::O,
                QueryPattern::G,
                QueryPattern::SP,
                QueryPattern::SO,
                QueryPattern::SG,
                QueryPattern::PO,
                QueryPattern::PG,
                QueryPattern::OG,
                QueryPattern::SPO,
                QueryPattern::SPG,
                QueryPattern::SOG,
                QueryPattern::POG,
                QueryPattern::SPOG,
            ],
            sample_count = 10
        )]
        fn $name<const SIZE: usize>(bencher: Bencher, pattern: QueryPattern) {
            run_match_bench_for_combo::<$builder, SIZE>(
                bencher,
                $builder_name,
                $layout_name,
                $layout,
                $index_config,
                $source,
                pattern,
            );
        }
    };
}

/// All combinations with an unsorted quad array
define_match_matrix_bench!(match_pattern_unsorted_stream_default_no_index_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_default_no_index_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_default_secondary_by_reference_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_default_secondary_by_reference_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_default_secondary_by_copy_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_default_secondary_by_copy_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_no_index_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_no_index_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_secondary_by_reference_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_secondary_by_reference_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_secondary_by_copy_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_typed_object_secondary_by_copy_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_no_index_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_no_index_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_secondary_by_reference_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_secondary_by_reference_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_secondary_by_copy_file, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_unsorted_stream_dictionary_secondary_by_copy_in_memory, UnsortedStreamBuilder, "UnsortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);

/// All combinations with a sorted in memory quad array
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_no_index_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_no_index_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_secondary_by_reference_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_secondary_by_reference_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_secondary_by_copy_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_default_secondary_by_copy_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_no_index_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_no_index_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_secondary_by_reference_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_secondary_by_reference_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_secondary_by_copy_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_typed_object_secondary_by_copy_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_no_index_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_no_index_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_secondary_by_reference_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_secondary_by_reference_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_secondary_by_copy_file, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_in_memory_dictionary_secondary_by_copy_in_memory, SortedInMemoryBuilder, "SortedInMemoryBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);

/// All combinations with a sorted stream quad array
define_match_matrix_bench!(match_pattern_sorted_stream_default_no_index_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_default_no_index_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_default_secondary_by_reference_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_default_secondary_by_reference_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_default_secondary_by_copy_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_default_secondary_by_copy_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Default, "default", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_no_index_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_no_index_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_secondary_by_reference_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_secondary_by_reference_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_secondary_by_copy_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_typed_object_secondary_by_copy_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::TypedObject, "typed-object", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_no_index_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_no_index_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::None, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_secondary_by_reference_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_secondary_by_reference_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByReference, SourceConfig::InMemory);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_secondary_by_copy_file, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::File);
define_match_matrix_bench!(match_pattern_sorted_stream_dictionary_secondary_by_copy_in_memory, SortedStreamBuilder, "SortedStreamBuilder", LayoutStrategy::Dictionary, "dictionary", IndexConfig::SecondaryByCopy, SourceConfig::InMemory);

