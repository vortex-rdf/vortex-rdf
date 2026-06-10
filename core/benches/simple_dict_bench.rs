use divan::Bencher;
use tokio::runtime::Runtime;
use oxrdf::{NamedOrBlankNode, NamedNode};
use std::sync::{OnceLock, Mutex};
use std::collections::HashMap;

use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::store::{
    VortexRdfStore,
    UnsortedInMemoryBuilder,
    SortedInMemoryBuilder,
    SortedStreamBuilder,
    UnsortedStreamBuilder,
};
use vortex_rdf_core::index::SimpleDictionary;

fn main() {
    divan::main();
}

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// Global cache for built vortex file paths to reuse across benchmarks
static FILE_CACHE: OnceLock<Mutex<HashMap<(&'static str, &'static str, usize), std::path::PathBuf>>> = OnceLock::new();

fn get_cached_file_path<Dict: vortex_rdf_core::index::RdfDictionary + 'static, Builder: vortex_rdf_core::store::VortexArrayBuilder<Dict>>(
    builder_name: &'static str,
    dict_name: &'static str,
    size: usize,
) -> std::path::PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut lock = cache.lock().unwrap();
    let key = (builder_name, dict_name, size);
    if let Some(path) = lock.get(&key) {
        return path.clone();
    }

    // Build the array and store it as a file on disk
    let rt = get_runtime();
    let quad_stream = generate_rdf_data_stream(size);
    let arr = rt.block_on(async {
        VortexRdfStore::<Dict>::build_vortex_array_with_builder::<Builder>(quad_stream)
            .await
            .expect("Failed to build vortex array")
    });

    let filename = format!("{}_{}_{}.vortex", builder_name, dict_name, size);
    std::fs::create_dir_all("target/bench_vortex_files").unwrap();
    let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);

    rt.block_on(async {
        let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
        vortex_rdf_core::io::serialize(arr, writer).await.expect("Failed to serialize");
    });

    lock.insert(key, filepath.clone());
    filepath
}

// ==================== BUILD VORTEX INDEX ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_unsorted<const SIZE: usize>(bencher: Bencher) {
    let rt = get_runtime();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            let arr = rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("UnsortedInMemoryBuilder_SimpleDictionary_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("UnsortedInMemoryBuilder", "SimpleDictionary", SIZE), filepath);
            arr
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_sorted<const SIZE: usize>(bencher: Bencher) {
    let rt = get_runtime();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            let arr = rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("SortedInMemoryBuilder_SimpleDictionary_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("SortedInMemoryBuilder", "SimpleDictionary", SIZE), filepath);
            arr
        });
}



#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_sorted_stream<const SIZE: usize>(bencher: Bencher) {
    let rt = get_runtime();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            let arr = rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::build_vortex_array_with_builder::<SortedStreamBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("SortedStreamBuilder_SimpleDictionary_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("SortedStreamBuilder", "SimpleDictionary", SIZE), filepath);
            arr
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_unsorted_stream<const SIZE: usize>(bencher: Bencher) {
    let rt = get_runtime();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            let arr = rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::build_vortex_array_with_builder::<UnsortedStreamBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("UnsortedStreamBuilder_SimpleDictionary_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("UnsortedStreamBuilder", "SimpleDictionary", SIZE), filepath);
            arr
        });
}

// ==================== INSTANTIATE STORE ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_unsorted<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<SimpleDictionary, UnsortedInMemoryBuilder>("UnsortedInMemoryBuilder", "SimpleDictionary", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_sorted<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<SimpleDictionary, SortedInMemoryBuilder>("SortedInMemoryBuilder", "SimpleDictionary", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}



#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_sorted_stream<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<SimpleDictionary, SortedStreamBuilder>("SortedStreamBuilder", "SimpleDictionary", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_unsorted_stream<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<SimpleDictionary, UnsortedStreamBuilder>("UnsortedStreamBuilder", "SimpleDictionary", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}

// ==================== MATCH PATTERN ====================

macro_rules! define_match_bench {
    ($name:ident, $dict:ty, $builder:ty, $sub:expr, $pred:expr, $obj:expr, $graph:expr) => {
        #[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
        fn $name<const SIZE: usize>(bencher: Bencher) {
            bencher
                .with_inputs(|| {
                    let path = get_cached_file_path::<$dict, $builder>(stringify!($builder), stringify!($dict), SIZE);
                    let rt = get_runtime();
                    rt.block_on(async {
                        VortexRdfStore::<$dict>::from_file(path).await.expect("Failed to create store")
                    })
                })
                .bench_values(|store| {
                    let s_val: Option<&str> = $sub;
                    let p_val: Option<&str> = $pred;
                    let o_val: Option<&str> = $obj;
                    let g_val: Option<&str> = $graph;

                    let s_term = s_val.map(|s| NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(s)));
                    let p_term = p_val.map(|p| NamedNode::new_unchecked(p));
                    let o_term = o_val.map(|o| oxrdf::Term::NamedNode(NamedNode::new_unchecked(o)));
                    let g_term = g_val.map(|g| oxrdf::GraphName::NamedNode(NamedNode::new_unchecked(g)));

                    let rt = get_runtime();
                    rt.block_on(async {
                        store.match_pattern(s_term.as_ref(), p_term.as_ref(), o_term.as_ref(), g_term.as_ref())
                            .await
                            .expect("Failed to match pattern")
                    })
                });
        }
    };
}

define_match_bench!(match_pattern_unsorted_s, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_unsorted_p, SimpleDictionary, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_o, SimpleDictionary, UnsortedInMemoryBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_g, SimpleDictionary, UnsortedInMemoryBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_sp, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_so, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_sg, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_po, SimpleDictionary, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_pg, SimpleDictionary, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_spo, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_pog, SimpleDictionary, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_sog, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_spg, SimpleDictionary, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_sorted_s, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_sorted_p, SimpleDictionary, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_o, SimpleDictionary, SortedInMemoryBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_g, SimpleDictionary, SortedInMemoryBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_sp, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_so, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_sg, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_po, SimpleDictionary, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_pg, SimpleDictionary, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_spo, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_pog, SimpleDictionary, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_sog, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_spg, SimpleDictionary, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_sorted_stream_s, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_sorted_stream_p, SimpleDictionary, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_stream_o, SimpleDictionary, SortedStreamBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_g, SimpleDictionary, SortedStreamBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_sp, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_stream_so, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_sg, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_po, SimpleDictionary, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_pg, SimpleDictionary, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_spo, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_pog, SimpleDictionary, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_sog, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_spg, SimpleDictionary, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_unsorted_stream_s, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_unsorted_stream_p, SimpleDictionary, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_stream_o, SimpleDictionary, UnsortedStreamBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_g, SimpleDictionary, UnsortedStreamBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_sp, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_stream_so, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_sg, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_po, SimpleDictionary, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_pg, SimpleDictionary, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_spo, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_pog, SimpleDictionary, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_sog, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_spg, SimpleDictionary, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
