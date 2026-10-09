//! `VORTEX_EXPERIMENTAL_PATCHED_ARRAY=1` registers Vortex's experimental
//! `vortex.patched` array and has its bit-packing scheme emit it. No core
//! edition includes it, so no store file carries one: the compressor drops
//! the bit-packing scheme that declares it, and an array that still comes out
//! patched (frame-of-reference bit-packs its offsets itself) fails the write
//! with an edition error instead of reaching the file. Vortex reads the
//! switch once per process, so this binary holds a single test that sets it
//! before anything touches Vortex.

use futures::stream;
use vortex_rdf_core::{IndexType, LayoutStrategy, RawQuad, VortexRdfStore};

const ROWS: usize = 50_000;

fn quads() -> Vec<RawQuad> {
    (0..ROWS)
        .map(|i| RawQuad {
            s: format!("<http://example.org/subject/{:06}>", i / 3),
            p: format!("<http://example.org/predicate/{}>", i % 7),
            o: format!("\"value {:06}\"", (i * 7919) % 20_000),
            g: String::new(),
        })
        .collect()
}

fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}

#[tokio::test]
async fn experimental_patched_arrays_stay_out_of_store_files() {
    // SAFETY: the binary's only test sets the variable on its own thread
    // before any other thread exists to read the environment.
    unsafe { std::env::set_var("VORTEX_EXPERIMENTAL_PATCHED_ARRAY", "1") };
    assert!(
        vortex_array::arrays::patched::use_experimental_patches(),
        "the switch must be on before Vortex first reads it"
    );

    let store = VortexRdfStore::from_quads(
        stream::iter(quads().into_iter().map(Ok)),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    match store.to_bytes().await {
        Ok(bytes) => {
            // The footer names every array encoding the file may use; the
            // patched array is not among them, so no array in it is one.
            assert!(contains(&bytes, b"fastlanes.bitpacked"));
            assert!(!contains(&bytes, b"vortex.patched"));
            let back = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            assert_eq!(back.size().await.unwrap(), ROWS);
        }
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("vortex.patched") && message.contains("not permitted"),
                "{message}"
            );
        }
    }
}
