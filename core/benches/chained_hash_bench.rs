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
use vortex_rdf_core::index::ChainedHash;

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
                VortexRdfStore::<ChainedHash>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("UnsortedInMemoryBuilder_ChainedHash_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("UnsortedInMemoryBuilder", "ChainedHash", SIZE), filepath);
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
                VortexRdfStore::<ChainedHash>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("SortedInMemoryBuilder_ChainedHash_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("SortedInMemoryBuilder", "ChainedHash", SIZE), filepath);
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
                VortexRdfStore::<ChainedHash>::build_vortex_array_with_builder::<SortedStreamBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("SortedStreamBuilder_ChainedHash_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("SortedStreamBuilder", "ChainedHash", SIZE), filepath);
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
                VortexRdfStore::<ChainedHash>::build_vortex_array_with_builder::<UnsortedStreamBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            });
            // Seed the file cache
            let filename = format!("UnsortedStreamBuilder_ChainedHash_{}.vortex", SIZE);
            std::fs::create_dir_all("target/bench_vortex_files").unwrap();
            let filepath = std::path::PathBuf::from("target/bench_vortex_files").join(filename);
            rt.block_on(async {
                let writer = tokio::fs::File::create(&filepath).await.expect("Failed to create file");
                vortex_rdf_core::io::serialize(arr.clone(), writer).await.expect("Failed to serialize");
            });
            let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            cache.lock().unwrap().insert(("UnsortedStreamBuilder", "ChainedHash", SIZE), filepath);
            arr
        });
}

// ==================== INSTANTIATE STORE ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_unsorted<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<ChainedHash, UnsortedInMemoryBuilder>("UnsortedInMemoryBuilder", "ChainedHash", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<ChainedHash>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_sorted<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<ChainedHash, SortedInMemoryBuilder>("SortedInMemoryBuilder", "ChainedHash", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<ChainedHash>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}



#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_sorted_stream<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<ChainedHash, SortedStreamBuilder>("SortedStreamBuilder", "ChainedHash", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<ChainedHash>::from_file(filepath)
                    .await
                    .expect("Failed to create store")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_unsorted_stream<const SIZE: usize>(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            get_cached_file_path::<ChainedHash, UnsortedStreamBuilder>("UnsortedStreamBuilder", "ChainedHash", SIZE)
        })
        .bench_values(|filepath| {
            let rt = get_runtime();
            rt.block_on(async {
                VortexRdfStore::<ChainedHash>::from_file(filepath)
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

define_match_bench!(match_pattern_unsorted_s, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_unsorted_p, ChainedHash, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_o, ChainedHash, UnsortedInMemoryBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_g, ChainedHash, UnsortedInMemoryBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_sp, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_so, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_sg, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_po, ChainedHash, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_pg, ChainedHash, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_spo, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_pog, ChainedHash, UnsortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_sog, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_spg, ChainedHash, UnsortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_sorted_s, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_sorted_p, ChainedHash, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_o, ChainedHash, SortedInMemoryBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_g, ChainedHash, SortedInMemoryBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_sp, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_so, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_sg, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_po, ChainedHash, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_pg, ChainedHash, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_spo, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_pog, ChainedHash, SortedInMemoryBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_sog, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_spg, ChainedHash, SortedInMemoryBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_sorted_stream_s, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_sorted_stream_p, ChainedHash, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_stream_o, ChainedHash, SortedStreamBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_g, ChainedHash, SortedStreamBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_sp, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_sorted_stream_so, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_sg, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_po, ChainedHash, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_pg, ChainedHash, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_spo, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_sorted_stream_pog, ChainedHash, SortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_sog, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_sorted_stream_spg, ChainedHash, SortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));

define_match_bench!(match_pattern_unsorted_stream_s, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, None, None);
define_match_bench!(match_pattern_unsorted_stream_p, ChainedHash, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_stream_o, ChainedHash, UnsortedStreamBuilder, None, None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_g, ChainedHash, UnsortedStreamBuilder, None, None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_sp, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, None);
define_match_bench!(match_pattern_unsorted_stream_so, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_sg, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_po, ChainedHash, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_pg, ChainedHash, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_spo, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), None);
define_match_bench!(match_pattern_unsorted_stream_pog, ChainedHash, UnsortedStreamBuilder, None, Some("http://example.org/predicate/0"), Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_sog, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), None, Some("http://example.org/object/0"), Some("http://example.org/graph"));
define_match_bench!(match_pattern_unsorted_stream_spg, ChainedHash, UnsortedStreamBuilder, Some("http://example.org/subject/0"), Some("http://example.org/predicate/0"), None, Some("http://example.org/graph"));