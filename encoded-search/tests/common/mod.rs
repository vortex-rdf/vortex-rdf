//! Shared fixtures for the integration tests and the local bench.
#![allow(dead_code)]

use std::ops::Range;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

/// A session with every default encoding registered and an edition enabled
/// that mirrors the store's: every registered array, layout and dtype plus
/// the default zone-map aggregates, less the per-chunk frame-of-reference
/// wire form (`fastlanes.for.v2`). The file writer refuses components
/// outside an enabled edition, and a compressor built `from_session` only
/// produces the wire forms the edition allows, so frame-of-reference keeps
/// the single reference the probe reads in place.
pub fn session() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    enable_test_edition(&session);
    session
}

/// [`session`] with a tokio runtime, for the file writer.
pub fn writer_session() -> VortexSession {
    use vortex_io::session::RuntimeSessionExt as _;

    session().with_tokio()
}

fn enable_test_edition(session: &VortexSession) {
    use vortex_array::dtype::session::DTypeSessionExt as _;
    use vortex_array::session::ArraySessionExt as _;
    use vortex_edition::{
        ComponentKind, Edition, EditionId, EditionInclusion, EditionSessionExt as _,
    };
    use vortex_fastlanes::for_v2_id;
    use vortex_layout::session::LayoutSessionExt as _;

    const TEST_EDITION: EditionId = EditionId::new("test", 2026, 7, 0);
    let editions = session.editions();
    editions
        .declare_edition(Edition {
            id: TEST_EDITION,
            min_library_version: None,
        })
        .unwrap();
    let registered = [
        (
            ComponentKind::Array,
            session.arrays().registry().read(|map| {
                map.keys()
                    .copied()
                    .filter(|id| *id != for_v2_id())
                    .collect::<Vec<_>>()
            }),
        ),
        (
            ComponentKind::Layout,
            session
                .layouts()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
        ),
        (
            ComponentKind::DType,
            session
                .dtypes()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
        ),
    ];
    for (kind, ids) in &registered {
        for id in ids {
            editions
                .declare_inclusion(EditionInclusion::new(*kind, id, TEST_EDITION))
                .unwrap();
        }
    }
    for id in [
        "vortex.bounded_max",
        "vortex.bounded_min",
        "vortex.max",
        "vortex.min",
        "vortex.nan_count",
        "vortex.null_count",
    ] {
        editions
            .declare_inclusion(EditionInclusion::new(
                ComponentKind::Aggregate,
                id,
                TEST_EDITION,
            ))
            .unwrap();
    }
    session.enable_edition(TEST_EDITION).unwrap();
}

/// The `partition_point` floor: the half-open run of `needle` in sorted
/// `data`.
pub fn canonical_bounds(data: &[u32], needle: u64) -> Range<u64> {
    let lo = data.partition_point(|&v| u64::from(v) < needle) as u64;
    let hi = data.partition_point(|&v| u64::from(v) <= needle) as u64;
    lo..hi.max(lo)
}
