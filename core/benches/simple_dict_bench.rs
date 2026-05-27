use divan::Bencher;
use oxrdf::NamedNode;
use oxrdf::NamedOrBlankNode;
use tokio::runtime::Runtime;

use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::store::{
    VortexRdfStore,
    UnsortedInMemoryBuilder,
    SortedInMemoryBuilder,
    ChunkSortBuilder,
    GlobalSortBuilder,
};
use vortex_rdf_core::index::SimpleDictionary;
use vortex_rdf_core::store::layout::flat::FlatLayout;

fn main() {
    divan::main();
}

// ==================== BUILD VORTEX INDEX ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_unsorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_sorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_chunk_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<ChunkSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn build_vortex_index_global_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| generate_rdf_data_stream(SIZE))
        .bench_values(|quad_stream| {
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<GlobalSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        });
}

// ==================== INSTANTIATE STORE ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_unsorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        })
        .bench_values(|vortex_array| {
            VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                .expect("Failed to create store")
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_sorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        })
        .bench_values(|vortex_array| {
            VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                .expect("Failed to create store")
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_chunk_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<ChunkSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        })
        .bench_values(|vortex_array| {
            VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                .expect("Failed to create store")
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn instantiate_store_global_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<GlobalSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array")
            })
        })
        .bench_values(|vortex_array| {
            VortexRdfStore::<SimpleDictionary, FlatLayout>::new(vortex_array)
                .expect("Failed to create store")
        });
}

// ==================== MATCH PATTERN ====================

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn match_pattern_unsorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                let varray = VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<UnsortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array");
                VortexRdfStore::<SimpleDictionary, FlatLayout>::new(varray).expect("Failed to create store")
            })
        })
        .bench_values(|store| {
            let subject = Some(&NamedOrBlankNode::NamedNode(
                NamedNode::new_unchecked("http://example.org/subject/0")
            ));
            let predicate = Some(&NamedNode::new_unchecked("http://example.org/predicate/0"));
            rt.block_on(async {
                store.match_pattern(subject, predicate, None, None)
                    .await
                    .expect("Failed to match pattern")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn match_pattern_sorted<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                let varray = VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<SortedInMemoryBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array");
                VortexRdfStore::<SimpleDictionary, FlatLayout>::new(varray).expect("Failed to create store")
            })
        })
        .bench_values(|store| {
            let subject = Some(&NamedOrBlankNode::NamedNode(
                NamedNode::new_unchecked("http://example.org/subject/0")
            ));
            let predicate = Some(&NamedNode::new_unchecked("http://example.org/predicate/0"));
            rt.block_on(async {
                store.match_pattern(subject, predicate, None, None)
                    .await
                    .expect("Failed to match pattern")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn match_pattern_chunk_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                let varray = VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<ChunkSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array");
                VortexRdfStore::<SimpleDictionary, FlatLayout>::new(varray).expect("Failed to create store")
            })
        })
        .bench_values(|store| {
            let subject = Some(&NamedOrBlankNode::NamedNode(
                NamedNode::new_unchecked("http://example.org/subject/0")
            ));
            let predicate = Some(&NamedNode::new_unchecked("http://example.org/predicate/0"));
            rt.block_on(async {
                store.match_pattern(subject, predicate, None, None)
                    .await
                    .expect("Failed to match pattern")
            })
        });
}

#[divan::bench(consts = [10_000, 100_000, 1_000_000], sample_count = 10)]
fn match_pattern_global_sort<const SIZE: usize>(bencher: Bencher) {
    let rt = Runtime::new().unwrap();
    bencher
        .with_inputs(|| {
            let quad_stream = generate_rdf_data_stream(SIZE);
            rt.block_on(async {
                let varray = VortexRdfStore::<SimpleDictionary, FlatLayout>::build_vortex_array_with_builder::<GlobalSortBuilder>(quad_stream)
                    .await
                    .expect("Failed to build vortex array");
                VortexRdfStore::<SimpleDictionary, FlatLayout>::new(varray).expect("Failed to create store")
            })
        })
        .bench_values(|store| {
            let subject = Some(&NamedOrBlankNode::NamedNode(
                NamedNode::new_unchecked("http://example.org/subject/0")
            ));
            let predicate = Some(&NamedNode::new_unchecked("http://example.org/predicate/0"));
            rt.block_on(async {
                store.match_pattern(subject, predicate, None, None)
                    .await
                    .expect("Failed to match pattern")
            })
        });
}
