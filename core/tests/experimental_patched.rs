//! `VORTEX_EXPERIMENTAL_PATCHED_ARRAY=1` registers Vortex's experimental
//! `vortex.patched` array and has its compressor emit it. No core edition
//! includes it, so no store file may carry one: a store write refuses to start
//! while the switch is on, rather than failing at the first chunk that holds
//! such an array, and reads nothing from its input first. Vortex reads the
//! switch once per process, so this binary holds a single test that sets it
//! before anything touches Vortex.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream;
use vortex_rdf_core::{
    IndexType, LayoutStrategy, RawQuad, VortexRdfError, VortexRdfStore,
    io::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer},
};

const ROWS: usize = 2_000;

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

/// An input that records whether anyone read it.
fn recording_stream(
    polled: Arc<AtomicBool>,
) -> impl futures::Stream<Item = Result<RawQuad, VortexRdfError>> + Unpin + Send + 'static {
    stream::poll_fn(move |_| {
        polled.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(None)
    })
}

fn assert_refused(error: VortexRdfError, what: &str) {
    assert!(
        matches!(&error, VortexRdfError::Serialization(message)
            if message.contains("VORTEX_EXPERIMENTAL_PATCHED_ARRAY")),
        "{what}: {error}"
    );
}

#[tokio::test]
async fn a_store_write_refuses_to_start_while_the_patched_array_switch_is_on() {
    // SAFETY: the binary's only test sets the variable on its own thread
    // before any other thread exists to read the environment.
    unsafe { std::env::set_var("VORTEX_EXPERIMENTAL_PATCHED_ARRAY", "1") };
    assert!(
        vortex_array::arrays::patched::use_experimental_patches(),
        "the switch must be on before Vortex first reads it"
    );

    // Building a store in memory is not a write.
    let store = VortexRdfStore::from_quads(
        stream::iter(quads().into_iter().map(Ok)),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    // Not `expect_err`: a store that serialized would print its bytes.
    let Err(error) = store.to_bytes().await else {
        panic!("serializing must refuse");
    };
    assert_refused(error, "to_bytes");

    let polled = Arc::new(AtomicBool::new(false));
    let mut sink: Vec<u8> = Vec::new();
    let error = quads_stream_to_vortex_writer(
        recording_stream(polled.clone()),
        &mut sink,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .expect_err("streaming into a writer must refuse");
    assert_refused(error, "quads_stream_to_vortex_writer");
    assert!(sink.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    let error = quads_stream_to_vortex_file(
        recording_stream(polled.clone()),
        &path,
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .expect_err("writing a file must refuse");
    assert_refused(error, "quads_stream_to_vortex_file");
    assert!(
        std::fs::read_dir(dir.path()).unwrap().next().is_none(),
        "the refusal left a file behind"
    );
    assert!(
        !polled.load(Ordering::SeqCst),
        "the input was read before the refusal"
    );
}
