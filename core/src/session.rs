//! Crate-wide Vortex session infrastructure. Lives at the crate root because
//! executing *any* Vortex kernel — an in-memory decode as much as a file scan
//! — needs the session's registries.

use std::sync::LazyLock;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_edition::{Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

#[cfg(any(
    all(feature = "file-io", not(target_arch = "wasm32")),
    all(target_arch = "wasm32", target_os = "unknown")
))]
use vortex_io::session::RuntimeSessionExt;

/// The one Vortex session: array, layout, scalar-fn and runtime registries,
/// with the store's container layout registered and the write editions
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
    crate::io::container::register(&session);
    enable_write_editions(&session);
    session
});

/// The frozen Vortex `core` edition the store writes with: every array
/// encoding, layout, extension dtype and zone-map aggregate a store file may
/// hold, apart from the store's own root layout. Pinned, not Vortex's moving
/// default (`vortex::editions::DEFAULT_CORE_EDITION`): a frozen edition never
/// changes, so a Vortex upgrade cannot widen what the store writes. A wire
/// form outside it, such as per-chunk frame-of-reference (`fastlanes.for.v2`)
/// or the experimental patched array (`vortex.patched`), stays out of store
/// files until this pin moves, whatever the session registers.
const CORE_EDITION: EditionId = vortex_edition::declarations::core::CORE_2026_08_3;

/// The family of the components vortex-rdf adds to a Vortex file.
static STORE_FAMILY: EditionFamily = EditionFamily {
    name: "vortexrdf",
    origin: "vortex-rdf",
    doc: "The components vortex-rdf adds to a Vortex file: the store's root layout.",
};

/// The store edition: the root layout and nothing else.
const STORE_EDITION: EditionId = EditionId::new("vortexrdf", 2026, 8, 0);

static STORE_DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: STORE_EDITION,
        min_library_version: None,
    },
    added: &[EditionMember::layout(
        &crate::io::container::STORE_LAYOUT_ID,
    )],
};

/// Register Vortex's edition declarations and the store's, then enable
/// [`CORE_EDITION`] and the store edition for writing. The file writer emits
/// only components of an enabled edition (reading needs registration
/// alone), and a compressor built from the session produces only the wire
/// forms they allow, so the two editions are the allow-list of everything a
/// store file can contain.
fn enable_write_editions(session: &VortexSession) {
    use vortex_edition::EditionSessionExt as _;
    use vortex_error::{VortexExpect as _, vortex_err};

    vortex::editions::register_default_editions(session);
    session
        .editions()
        .declare_family(&STORE_FAMILY)
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store family is valid");
    session
        .register_edition(&STORE_DECLARATION)
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the store edition is valid");
    for edition in [CORE_EDITION, STORE_EDITION] {
        session
            .enable_edition(edition)
            .map_err(|error| vortex_err!("{error}"))
            .vortex_expect("the edition was just registered");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::aggregate_fn::session::AggregateFnSessionExt as _;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_edition::{ComponentKind, EditionSessionExt as _};

    use crate::io::container::STORE_LAYOUT_ID;

    fn ids(kind: ComponentKind) -> Vec<String> {
        VORTEX_SESSION
            .enabled_component_ids(kind)
            .iter()
            .map(|id| id.to_string())
            .collect()
    }

    /// Every component the session admits for writing is a member of the
    /// pinned core edition or the store's root layout. A Vortex upgrade that
    /// enables more (a plugin enabling its own edition as it initializes, a
    /// default edition switched on) fails here before a store file carries a
    /// wire form the readers were never checked against.
    #[test]
    fn session_admits_only_the_pinned_editions() {
        let editions = VORTEX_SESSION.editions();
        for kind in [
            ComponentKind::Array,
            ComponentKind::Layout,
            ComponentKind::DType,
            ComponentKind::Aggregate,
        ] {
            let pinned: Vec<String> = editions
                .components_in(&CORE_EDITION, kind)
                .iter()
                .map(|inclusion| inclusion.component_id.to_string())
                .collect();
            for id in ids(kind) {
                let own = kind == ComponentKind::Layout && id == STORE_LAYOUT_ID;
                assert!(
                    own || pinned.contains(&id),
                    "{kind} {id} is admitted outside {CORE_EDITION}"
                );
            }
        }
        assert!(ids(ComponentKind::Layout).contains(&STORE_LAYOUT_ID.to_string()));
        let arrays = ids(ComponentKind::Array);
        for id in ["fastlanes.for.v2", "vortex.patched"] {
            assert!(!arrays.contains(&id.to_string()), "{id} is admitted");
        }
        editions.validate().unwrap();
    }

    /// Every zone-map aggregate the file writer emits for the schema's column
    /// dtypes (utf8 strings and unsigned integer codes) is admitted, so no
    /// column type the store writes can fail the writer's edition check.
    #[test]
    fn write_editions_cover_writer_zone_aggregates() {
        let enabled = ids(ComponentKind::Aggregate);
        let dtypes = [
            DType::Utf8(Nullability::NonNullable),
            DType::Primitive(PType::U8, Nullability::NonNullable),
            DType::Primitive(PType::U16, Nullability::NonNullable),
            DType::Primitive(PType::U32, Nullability::NonNullable),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ];
        for dtype in &dtypes {
            for aggregate in VORTEX_SESSION
                .aggregate_fns()
                .zone_stat_defaults(dtype)
                .iter()
            {
                let id = aggregate.id().to_string();
                assert!(
                    enabled.contains(&id),
                    "zone aggregate {id} ({dtype}) is not admitted"
                );
            }
        }
    }
}
