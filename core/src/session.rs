//! Crate-wide Vortex session infrastructure. Lives at the crate root because
//! executing *any* Vortex kernel — an in-memory decode as much as a file scan
//! — needs the session's registries.

use std::sync::LazyLock;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

#[cfg(any(
    all(feature = "file-io", not(target_arch = "wasm32")),
    all(target_arch = "wasm32", target_os = "unknown")
))]
use vortex_io::session::RuntimeSessionExt;

/// The one Vortex session: array, layout, scalar-fn and runtime registries,
/// with the store's container layout registered and the store edition
/// enabled. The runtime handle is the only per-target piece: tokio on native
/// file-io builds; the microtask-queue `WasmRuntime` on
/// wasm32-unknown-unknown, where the file writer spawns tasks; none on native
/// no-file-io builds, whose code paths are all handle-free.
pub(crate) static VORTEX_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    #[cfg(all(feature = "file-io", not(target_arch = "wasm32")))]
    let session = session.with_tokio();
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let session = session.with_handle(vortex_io::runtime::wasm::WasmRuntime::handle());
    vortex_file::register_default_encodings(&session);
    // Arrow conversion registry (`session.arrow()`), used by the batch export
    // in `crate::arrow`; registering eagerly keeps session setup in one place.
    vortex_arrow::initialize(&session);
    crate::io::container::register(&session);
    enable_store_edition(&session);
    session
});

/// The edition every registered component and [`ZONE_AGGREGATES`] entry is
/// declared in and that the session enables for writing.
const STORE_EDITION: vortex_edition::EditionId =
    vortex_edition::EditionId::new("vortexrdf", 2026, 8, 0);

/// The zone-map aggregate ids the vortex file writer emits by default
/// (bounded min/max for string columns, min/max otherwise, nan and null
/// counts). The aggregate registry cannot be enumerated, so this list is
/// mirrored by hand: an id the writer emits that is missing here makes every
/// file write fail with an edition error. The inline test below checks it
/// against the writer's per-dtype defaults for the schema's column types.
const ZONE_AGGREGATES: [&str; 6] = [
    "vortex.bounded_max",
    "vortex.bounded_min",
    "vortex.max",
    "vortex.min",
    "vortex.nan_count",
    "vortex.null_count",
];

/// Declare and enable one edition ([`STORE_EDITION`]) containing every
/// registered array encoding, layout and extension dtype plus
/// [`ZONE_AGGREGATES`]. The file writer only emits components from an enabled
/// edition (reading needs registration alone); the edition is a writer
/// allow-list, so it spans the full registries.
fn enable_store_edition(session: &VortexSession) {
    use vortex_array::dtype::session::DTypeSessionExt as _;
    use vortex_array::session::ArraySessionExt as _;
    use vortex_edition::{ComponentKind, Edition, EditionInclusion, EditionSessionExt as _};
    use vortex_error::{VortexExpect as _, vortex_err};
    use vortex_layout::session::LayoutSessionExt as _;

    let editions = session.editions();
    editions
        .declare_edition(Edition {
            id: STORE_EDITION,
            min_vortex_version: None,
        })
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store edition is valid");
    let registered = [
        (
            ComponentKind::Array,
            session
                .arrays()
                .registry()
                .read(|map| map.keys().copied().collect::<Vec<_>>()),
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
    let inclusions = registered
        .iter()
        .flat_map(|(kind, ids)| {
            ids.iter()
                .map(move |id| EditionInclusion::new(*kind, id, STORE_EDITION))
        })
        .chain(
            ZONE_AGGREGATES
                .into_iter()
                .map(|id| EditionInclusion::new(ComponentKind::Aggregate, id, STORE_EDITION)),
        );
    for inclusion in inclusions {
        editions
            .declare_inclusion(inclusion)
            .map_err(|error| vortex_err!("{error}"))
            .vortex_expect("every registered component joins the store edition once");
    }
    session
        .enable_edition(STORE_EDITION)
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store edition was just declared");
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::aggregate_fn::session::AggregateFnSessionExt as _;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_edition::{ComponentKind, EditionSessionExt as _};

    /// Every zone-map aggregate the file writer emits for the schema's column
    /// dtypes (utf8 strings and unsigned integer codes) is in the enabled
    /// edition, so no column type the store writes can fail the writer's
    /// edition check.
    #[test]
    fn store_edition_covers_writer_zone_aggregates() {
        let enabled: Vec<String> = VORTEX_SESSION
            .enabled_component_ids(ComponentKind::Aggregate)
            .iter()
            .map(|id| id.to_string())
            .collect();
        let dtypes = [
            DType::Utf8(Nullability::NonNullable),
            DType::Primitive(PType::U8, Nullability::NonNullable),
            DType::Primitive(PType::U16, Nullability::NonNullable),
            DType::Primitive(PType::U32, Nullability::NonNullable),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ];
        let mut wanted: Vec<String> = ZONE_AGGREGATES.iter().map(|id| id.to_string()).collect();
        for dtype in &dtypes {
            wanted.extend(
                VORTEX_SESSION
                    .aggregate_fns()
                    .zone_stat_defaults(dtype)
                    .iter()
                    .map(|f| f.id().to_string()),
            );
        }
        for id in wanted {
            assert!(
                enabled.contains(&id),
                "zone aggregate {id} is not in the store edition"
            );
        }
    }
}
