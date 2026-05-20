
use divan::Bencher;
use tokio::runtime::Runtime;
use oxrdf::{NamedOrBlankNode, NamedNode};

use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::store::VortexRdfStore;
use vortex_rdf_core::index::ChainedHash;


fn main() {
    // Run registered benchmarks.
    divan::main();
}

/// Benchmark VortexRdfStore::<ChainedHash>::build_vortex_index with different dataset sizes
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
                VortexRdfStore::<ChainedHash>::build_vortex_index(quad_stream)
                    .await
                    .expect("Failed to build vortex index")
            })
        });
}

/// Benchmark VortexRdfStore::<ChainedHash>::new() with pre-built vortex arrays
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
                VortexRdfStore::<ChainedHash>::build_vortex_index(quad_stream)
                    .await
                    .expect("Failed to build vortex index")
            })
        })
        .bench_values(|vortex_array| {
            // Only this block is timed - measuring VortexRdfStore::<ChainedHash>::new()
            VortexRdfStore::<ChainedHash>::new(vortex_array)
                .expect("Failed to create VortexRdfStore::<ChainedHash>")
        });
}

/// Benchmark VortexRdfStore::<ChainedHash>::match()
#[divan::bench(
    consts = [10_000, 100_000, 1_000_000],
    sample_count = 10
)]
fn match_pattern<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    
    bencher
        .with_inputs(|| {
            // Pre-generate the ArrayRef - this time is NOT counted in the benchmark
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                let varray = VortexRdfStore::<ChainedHash>::build_vortex_index(quad_stream)
                    .await
                    .expect("Failed to build vortex index");
                VortexRdfStore::<ChainedHash>::new(varray)
                    .expect("Failed to create VortexRdfStore::<ChainedHash>")
            })
        })
        .bench_values(|store| {
            // Only this block is timed - measuring VortexRdfStore::<ChainedHash>::match()
            let subject = Some(&NamedOrBlankNode::NamedNode(
                NamedNode::new_unchecked("http://example.org/subject/0")
            ));
            let predicate = Some(&NamedNode::new_unchecked("http://example.org/predicate/0"));
            
            rt.block_on(async {
                store.match_pattern(
                    subject,
                    predicate,
                    None,
                    None
                )
                .await
                .expect("Failed to match pattern")
            })
    });
}