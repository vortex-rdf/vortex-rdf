//! Wasm build of the hdt crate for the JS comparative benchmark: hdt's own
//! `Hdt` class (`hdt::wasm`, compiled for `wasm32`). Read-only — HDT
//! construction uses OS threads and rayon, which trap on
//! `wasm32-unknown-unknown`, so the artifact is built natively and only opened
//! here. Linking the crate is what puts its `#[wasm_bindgen]` exports into
//! this cdylib.

extern crate hdt;

/// Counts live heap bytes (see `js/bench/memory-stats`), so the dashboard can
/// read this build's heap through the `memoryStats` export. Counting stays off
/// until `setMemoryCounting(true)`. Referencing the crate here also links its
/// wasm-bindgen exports into this cdylib.
#[global_allocator]
static ALLOCATOR: bench_memory_stats::CountingAlloc<std::alloc::System> =
    bench_memory_stats::CountingAlloc(std::alloc::System);
