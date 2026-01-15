
use divan::Bencher;
use tokio::runtime::Runtime;
use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::store::{
    simple_dictionary_store::SimpleDictionaryStore, 
    VortexRdfStore
};


fn main() {
    // Run registered benchmarks.
    divan::main();
}

/// Benchmark SimpleDictionaryStore::build_vortex_index with different dataset sizes
#[divan::bench(
    consts = [10_000, 100_000, 1_000_000],
    sample_count = 10
)]
fn build_vortex_index<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    
    bencher
        .with_inputs(|| {
            // Generate the quad stream - this time is NOT counted in the benchmark
            generate_rdf_data_stream(SIZE)
        })
        .bench_values(|quad_stream| {
            // Only this block is timed
            rt.block_on(async {
                SimpleDictionaryStore::build_vortex_index(quad_stream)
                    .await
                    .expect("Failed to build vortex index")
            })
        });
}

/// Benchmark SimpleDictionaryStore::new() with pre-built vortex arrays
#[divan::bench(
    consts = [10_000, 100_000, 1_000_000],
    sample_count = 10
)]
fn instantiate_store<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    
    bencher
        .with_inputs(|| {
            // Pre-generate the ArrayRef - this time is NOT counted in the benchmark
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                SimpleDictionaryStore::build_vortex_index(quad_stream)
                    .await
                    .expect("Failed to build vortex index")
            })
        })
        .bench_values(|vortex_array| {
            // Only this block is timed - measuring SimpleDictionaryStore::new()
            SimpleDictionaryStore::new(vortex_array)
                .expect("Failed to create SimpleDictionaryStore")
        });
}
