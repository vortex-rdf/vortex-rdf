//! Resident memory of a Dictionary-layout store per form, from a counting
//! allocator: built (a canonical dictionary), adopted from its own bytes as
//! written (the file's FSST chunks), and adopted plaintext — each with the
//! dictionary's Arrow values held and then dropped, and with the time each
//! construction took. `BENCH_SIZE` rows of the comparative dataset, default
//! 262,144.
//!
//! ```text
//! BENCH_SIZE=1048576 cargo run --release --example dict_memory
//! ```

// The crate's `mimalloc` feature installs its own global allocator, which
// this one would collide with; the workspace's CLI enables it, so a
// workspace-wide build compiles the example without the measurement.
#[cfg(feature = "mimalloc")]
fn main() {
    eprintln!("dict_memory counts through the system allocator: build without --features mimalloc");
    std::process::exit(2);
}

#[cfg(not(feature = "mimalloc"))]
fn main() {
    counting::main();
}

#[cfg(not(feature = "mimalloc"))]
#[allow(dead_code)]
#[path = "../benches/support/dataset.rs"]
mod dataset;

#[cfg(not(feature = "mimalloc"))]
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use futures::stream;
    use vortex_rdf_core::{
        CodeForm, DictForm, LayoutStrategy, RawQuad, ResidentForm, VortexRdfStore,
    };

    use super::dataset;

    /// Live heap bytes, kept by every allocation and release.
    static LIVE: AtomicUsize = AtomicUsize::new(0);

    struct Counting;

    // SAFETY: every call forwards to `System` unchanged; only the counter is
    // added on top.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(layout) };
            if !p.is_null() {
                LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            }
            p
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let p = unsafe { System.realloc(ptr, layout, new_size) };
            if !p.is_null() {
                LIVE.fetch_add(new_size, Ordering::Relaxed);
                LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            }
            p
        }
    }

    #[global_allocator]
    static ALLOC: Counting = Counting;

    fn live() -> usize {
        LIVE.load(Ordering::Relaxed)
    }

    fn mib(bytes: usize) -> String {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }

    fn delta(from: usize) -> String {
        let now = live();
        if now >= from {
            format!("+{}", mib(now - from))
        } else {
            format!("-{}", mib(from - now))
        }
    }

    fn secs(d: Duration) -> String {
        if d.as_secs_f64() >= 1.0 {
            format!("{:.2} s", d.as_secs_f64())
        } else {
            format!("{:.1} ms", d.as_secs_f64() * 1e3)
        }
    }

    /// The dictionary's Arrow values held, then dropped: what each costs on top
    /// of the store.
    fn arrow_values_cost(store: &VortexRdfStore) -> (String, String) {
        let dict = store
            .code_read_snapshot()
            .expect("a resident dictionary store is code-readable");
        let before = live();
        let values = dict.to_arrow().expect("to_arrow");
        let held = delta(before);
        drop(values);
        (held, delta(before))
    }

    /// What a whole-store code read costs while its four columns are held,
    /// and what stays after they are dropped.
    async fn code_read_cost(store: &VortexRdfStore) -> (String, String) {
        let before = live();
        let columns = store
            .code_columns_gathered()
            .await
            .expect("code read")
            .expect("a resident dictionary store serves codes");
        let held = delta(before);
        drop(columns);
        (held, delta(before))
    }

    pub(super) fn main() {
        let n: usize = std::env::var("BENCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(262_144);
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async move {
        let m = dataset::moduli(n, dataset::WANT_GRAPHS);
        // Built: the quads are consumed by the build and freed with it, so
        // what is left above the baseline taken before them is the store.
        let baseline = live();
        let quads: Vec<RawQuad> = (0..n)
            .map(|i| RawQuad::from_quad(&dataset::dataset_quad(i, m)))
            .collect();
        println!("rows {n}, terms {}", m.terms());
        let start = Instant::now();
        let built = VortexRdfStore::from_quads(
            stream::iter(quads.into_iter().map(Ok)),
            LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .expect("build");
        let build_time = start.elapsed();
        let built_retained = delta(baseline);
        let terms = built
            .code_read_snapshot()
            .expect("built dictionary")
            .len();
        let (held, after) = arrow_values_cost(&built);
        let (codes_held, codes_after) = code_read_cost(&built).await;
        println!(
            "| built (canonical dictionary, {terms} terms) | {} | {built_retained} | {held} | {after} | {codes_held} | {codes_after} |",
            secs(build_time)
        );

        let bytes = built.to_bytes().await.expect("to_bytes");
        println!("| file bytes | | {} | | |", mib(bytes.len()));

        for (dict, codes) in [
            (DictForm::AsWritten, CodeForm::AsWritten),
            (DictForm::Plaintext, CodeForm::AsWritten),
            (DictForm::Plaintext, CodeForm::Canonical),
        ] {
            let before = live();
            let start = Instant::now();
            let adopted =
                VortexRdfStore::from_bytes_owned_as(bytes.clone(), ResidentForm { dict, codes })
                    .await
                    .expect("adopt");
            let adopt_time = start.elapsed();
            let retained = delta(before);
            let (held, after) = arrow_values_cost(&adopted);
            let (codes_held, codes_after) = code_read_cost(&adopted).await;
            println!(
                "| adopted {dict} dictionary, {codes} codes | {} | {retained} (incl. the bytes) | {held} | {after} | {codes_held} | {codes_after} |",
                secs(adopt_time)
            );
            drop(adopted);
        }
        println!(
            "columns: form | construction | retained | to_arrow held | after drop | code read held | after drop"
        );
        drop(built);
    });
    }
}
