//! Shared fixtures for the integration tests and the local bench.
#![allow(dead_code)]

use std::ops::Range;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

/// A session with every default encoding registered and, as vortex-rdf's
/// store session does, the frozen Vortex core edition 2026.08.3 enabled for
/// writing, plus a test edition adding FastLanes delta: the one wire form
/// outside core the probe reads, which the forced delta fixtures compress
/// to. The file writer refuses components outside an enabled edition, and a
/// compressor built `from_session` only produces the wire forms they allow,
/// so frame-of-reference keeps the single reference the probe reads in place
/// (per-chunk references, `fastlanes.for.v2`, are in no core edition).
pub fn session() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    enable_test_editions(&session);
    session
}

/// [`session`] with a tokio runtime, for the file writer.
pub fn writer_session() -> VortexSession {
    use vortex_io::session::RuntimeSessionExt as _;

    session().with_tokio()
}

fn enable_test_editions(session: &VortexSession) {
    use vortex_edition::declarations::core::CORE_2026_08_3;
    use vortex_edition::{
        EDITION_DECLARATIONS, EDITION_FAMILIES, Edition, EditionDeclaration, EditionFamily,
        EditionId, EditionMember, EditionSessionExt as _,
    };

    static TEST_FAMILY: EditionFamily = EditionFamily {
        name: "test",
        origin: "vortex-rdf-encoded-search",
        doc: "Wire forms the probe reads beyond Vortex's core edition.",
    };
    const TEST_EDITION: EditionId = EditionId::new("test", 2026, 7, 0);
    static TEST_DECLARATION: EditionDeclaration = EditionDeclaration {
        edition: Edition {
            id: TEST_EDITION,
            min_library_version: None,
        },
        added: &[EditionMember::array(&"fastlanes.delta")],
    };

    // The edition the store writes with (vortex-rdf-core's pinned core
    // edition): the probes are tested against the wire forms it admits.
    assert_eq!(CORE_2026_08_3.to_string(), "core2026.08.3");
    for family in EDITION_FAMILIES.iter().copied().chain([&TEST_FAMILY]) {
        session.editions().declare_family(family).unwrap();
    }
    for declaration in EDITION_DECLARATIONS.iter().copied() {
        session.register_edition(declaration).unwrap();
    }
    session.register_edition(&TEST_DECLARATION).unwrap();
    session.enable_edition(CORE_2026_08_3).unwrap();
    session.enable_edition(TEST_EDITION).unwrap();
}

/// The `partition_point` floor: the half-open run of `needle` in sorted
/// `data`.
pub fn canonical_bounds(data: &[u32], needle: u64) -> Range<u64> {
    let lo = data.partition_point(|&v| u64::from(v) < needle) as u64;
    let hi = data.partition_point(|&v| u64::from(v) <= needle) as u64;
    lo..hi.max(lo)
}
