//! Shared fixtures for the integration tests and the local bench.
#![allow(dead_code)]

use std::ops::Range;
use std::sync::Arc;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::{ArrayRef, IntoArray as _};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_layout::{LayoutRef, LayoutStrategy};
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

    // Must equal `CORE_EDITION` in vortex-rdf-core's `session.rs`, the edition
    // the store writes with: the probes are tested against the wire forms it
    // admits. Moving that pin means moving this one by hand.
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

/// A struct chunk over the given arrays, all of one length.
pub fn struct_of(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    use vortex_array::arrays::StructArray;
    use vortex_array::validity::Validity;

    let len = columns[0].1.len();
    let (names, arrays): (Vec<&str>, Vec<ArrayRef>) = columns.into_iter().unzip();
    StructArray::try_new(names.into(), arrays, len, Validity::NonNullable)
        .unwrap()
        .into_array()
}

/// A struct chunk over non-nullable `u32` columns.
pub fn struct_chunk(columns: &[(&str, &[u32])]) -> ArrayRef {
    use vortex_array::arrays::PrimitiveArray;

    struct_of(
        columns
            .iter()
            .map(|(name, data)| {
                (
                    *name,
                    PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                )
            })
            .collect(),
    )
}

/// Writes `chunks` as one file (through `strategy` when given) and returns
/// the opened file.
pub async fn write_file(
    session: &VortexSession,
    chunks: Vec<ArrayRef>,
    strategy: Option<Arc<dyn LayoutStrategy>>,
) -> vortex_file::VortexFile {
    use vortex_array::stream::ArrayStreamAdapter;
    use vortex_file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};

    let dtype = chunks[0].dtype().clone();
    let mut options = session.write_options();
    if let Some(strategy) = strategy {
        options = options.with_strategy(strategy);
    }
    let mut bytes = Vec::new();
    options
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks.into_iter().map(Ok))),
        )
        .await
        .unwrap();
    session.open_options().open_buffer(bytes).unwrap()
}

/// The `u32` values of a fetched or taken column array.
pub fn taken_u32(array: ArrayRef, session: &VortexSession) -> Vec<u32> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    array
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap()
        .as_slice::<u32>()
        .to_vec()
}

/// A field's data node beneath its zoned wrappers, for shape assertions.
pub fn data_node(root: &LayoutRef, field: &str) -> LayoutRef {
    use vortex_layout::LayoutChildType;
    use vortex_layout::layouts::zoned::Zoned;

    let mut node = (0..root.nslots())
        .find_map(|i| {
            matches!(root.slot_type(i), Some(LayoutChildType::Field(ref name)) if name.as_ref() == field)
                .then(|| root.slot(i).ok().flatten())
                .flatten()
        })
        .expect("field present");
    while node.is::<Zoned>() {
        node = node.slot(0).unwrap().unwrap();
    }
    node
}
