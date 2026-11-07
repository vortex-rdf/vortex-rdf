//! The crate's Vortex sessions: the store session, whose registries every
//! kernel needs (an in-memory decode as much as a file scan), and the
//! placeholder session that names a refused file's root layout.

use std::sync::LazyLock;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_edition::{Edition, EditionDeclaration, EditionFamily, EditionId, EditionMember};
use vortex_io::session::{RuntimeSession, RuntimeSessionExt};
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

/// The store session: the array, layout, scalar-fn and runtime registries,
/// the store's own kernels ([`kernels`](crate::kernels)), the default
/// encodings, the store's container layout, and the write editions
/// enabled. The runtime is the one per-target piece: tokio natively,
/// the microtask-queue `WasmRuntime` on wasm32-unknown-unknown, where the file
/// writer spawns tasks.
pub(crate) static VORTEX_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = registries();
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let session = session.with_handle(vortex_io::runtime::wasm::WasmRuntime::handle());
    // Ahead of the default encodings' kernels, so that the store's are tried
    // first.
    crate::kernels::register(&session);
    crate::zone_stats::rules::register(&session);
    vortex_file::register_default_encodings(&session);
    crate::io::container::register(&session);
    {
        use vortex_array::aggregate_fn::session::AggregateFnSessionExt as _;
        session
            .aggregate_fns()
            .register(crate::zone_stats::value_counts::ValueCounts);
    }
    enable_write_editions(&session);
    session
});

/// A session that reads an unregistered layout as a placeholder. Only footers
/// are read through it: it names the root layout of a file the store session
/// refused ([`newer_root_id`](crate::io::container::newer_root_id)), from a
/// buffer or by positioned reads of a path.
pub(crate) static FOREIGN_LAYOUT_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = registries();
    session.allow_unknown();
    session
});

/// The registries both sessions start from: arrays, layouts, scalar
/// functions and the runtime, which natively runs on tokio. The store
/// session adds its encodings, its layout and its write editions (and its
/// runtime handle on wasm32-unknown-unknown), the placeholder session its
/// reading of unknown layouts.
fn registries() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    #[cfg(all(feature = "file-io", not(target_arch = "wasm32")))]
    let session = session.with_tokio();
    session
}

/// The frozen Vortex `core` edition the store writes with: every array
/// encoding, layout, extension dtype and zone-map aggregate a store file may
/// hold, apart from vortex-rdf's own: the store's root layout and the graph
/// columns' `vortexrdf.value_counts` aggregate. A frozen edition never changes,
/// so a Vortex upgrade cannot widen what the store writes: a wire form outside
/// it, such as per-chunk frame-of-reference (`fastlanes.for.v2`), bit-packing
/// with a width per block (`fastlanes.bitpacked.v2`) or the experimental
/// patched array (`vortex.patched`), stays out of store files until this pin
/// moves, whatever the session registers.
const CORE_EDITION: EditionId = vortex_edition::declarations::core::CORE_2026_08_3;

/// The family of the components vortex-rdf adds to a Vortex file.
static STORE_FAMILY: EditionFamily = EditionFamily {
    name: "vortexrdf",
    origin: "vortex-rdf",
    doc: "The components vortex-rdf adds to a Vortex file: the store's root layout and the graph columns' value-counts zone aggregate.",
};

/// The store edition: the `v2` root layout and nothing else. A released
/// edition is frozen: a new member gets a new edition, not a changed one.
const STORE_EDITION: EditionId = EditionId::new("vortexrdf", 2026, 10, 0);

/// The edition adding the graph columns' `vortexrdf.value_counts` zone
/// aggregate: each zone's graphs with their rows, for Vortex's pruning of a
/// bound graph and the store's counts. A file of this edition needs a reader
/// that registers the aggregate.
const VALUE_COUNTS_EDITION: EditionId = EditionId::new("vortexrdf", 2026, 11, 0);

static VALUE_COUNTS_DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: VALUE_COUNTS_EDITION,
        immutable: true,
        min_library_version: None,
    },
    added: &[EditionMember::aggregate(
        &crate::zone_stats::value_counts::VALUE_COUNTS_ID,
    )],
};

static STORE_DECLARATION: EditionDeclaration = EditionDeclaration {
    edition: Edition {
        id: STORE_EDITION,
        immutable: true,
        min_library_version: None,
    },
    added: &[EditionMember::layout(
        &crate::io::container::STORE_LAYOUT_ID,
    )],
};

/// Register Vortex's edition declarations and the store's, then enable
/// [`CORE_EDITION`], [`STORE_EDITION`] and [`VALUE_COUNTS_EDITION`] for
/// writing. The file writer emits
/// only components of an enabled edition (reading needs registration alone)
/// and a compressor built from the session only the wire forms they allow, so
/// the three editions are the allow-list of what a store file can contain.
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
    session
        .register_edition(&VALUE_COUNTS_DECLARATION)
        .map_err(|error| vortex_err!("{error}"))
        .vortex_expect("the value-counts edition is valid");
    for edition in [CORE_EDITION, STORE_EDITION, VALUE_COUNTS_EDITION] {
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
    /// pinned core edition or one of vortex-rdf's own: the store's root
    /// layout and the value-counts aggregate. A Vortex upgrade that
    /// enables more (a plugin enabling its own edition as it initializes, a
    /// default edition switched on) fails here before a store file carries a
    /// wire form the readers were never checked against.
    #[test]
    fn session_admits_only_the_pinned_editions() {
        // The pin is a literal here: pointing `CORE_EDITION` at Vortex's
        // moving default would pass the subset check below by construction.
        assert_eq!(CORE_EDITION.to_string(), "core2026.08.3");
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
                let own = (kind == ComponentKind::Layout && id == STORE_LAYOUT_ID)
                    || (kind == ComponentKind::Aggregate
                        && id == crate::zone_stats::value_counts::VALUE_COUNTS_ID);
                assert!(
                    own || pinned.contains(&id),
                    "{kind} {id} is admitted outside {CORE_EDITION}"
                );
            }
        }
        assert!(ids(ComponentKind::Layout).contains(&STORE_LAYOUT_ID.to_string()));
        let arrays = ids(ComponentKind::Array);
        for id in [
            "fastlanes.for.v2",
            "fastlanes.bitpacked.v2",
            "vortex.patched",
        ] {
            assert!(!arrays.contains(&id.to_string()), "{id} is admitted");
        }
        editions.validate().unwrap();
    }

    /// The store edition is a new one, dated after `vortexrdf2026.08.0` (which
    /// the session does not declare), and its only member is the `v2` root
    /// layout.
    #[test]
    fn the_store_edition_is_new_for_the_v2_root() {
        let editions = VORTEX_SESSION.editions();
        let earlier_edition = EditionId::new("vortexrdf", 2026, 8, 0);
        assert!(editions.find(&earlier_edition).is_none());
        assert!(editions.find(&STORE_EDITION).is_some());
        assert!(STORE_EDITION.is_at_or_before(&STORE_EDITION));
        assert!(
            earlier_edition.is_at_or_before(&STORE_EDITION) && earlier_edition != STORE_EDITION,
            "{STORE_EDITION} must come after {earlier_edition}"
        );
        let members: Vec<String> = editions
            .components_in(&STORE_EDITION, ComponentKind::Layout)
            .iter()
            .map(|inclusion| inclusion.component_id.to_string())
            .collect();
        assert_eq!(members, [STORE_LAYOUT_ID.to_string()]);
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

    /// The `vortexrdf` edition after the store edition adds one member, the
    /// graph columns' `vortexrdf.value_counts` zone aggregate, and is enabled
    /// for writing.
    #[test]
    fn the_value_counts_edition_adds_the_statistic() {
        let edition = EditionId::new("vortexrdf", 2026, 11, 0);
        let members: Vec<String> = VORTEX_SESSION
            .editions()
            .components_in(&edition, ComponentKind::Aggregate)
            .iter()
            .map(|inclusion| inclusion.component_id.to_string())
            .collect();
        assert_eq!(members, ["vortexrdf.value_counts"]);
        assert!(ids(ComponentKind::Aggregate).contains(&"vortexrdf.value_counts".to_string()));
    }

    /// 100 quads over `graphs` graphs (`g{i % graphs}`), written to a store
    /// file and read whole in a session that doesn't register the aggregate
    /// (vortex-rdf 0.14's): the rows it read, or the error opening or reading
    /// the file.
    async fn read_without_value_counts(graphs: usize) -> std::result::Result<usize, String> {
        use oxrdf::{GraphName, NamedNode, Quad};
        use vortex_file::OpenOptionsSessionExt as _;

        let iri = |name: &str| NamedNode::new_unchecked(format!("http://example.org/{name}"));
        let quads: Vec<crate::Result<crate::RawQuad>> = (0..100)
            .map(|i| {
                let quad = Quad::new(
                    iri(&format!("s{i}")),
                    iri("p"),
                    iri("o"),
                    GraphName::NamedNode(iri(&format!("g{}", i % graphs))),
                );
                Ok(crate::RawQuad::from_quad(&quad))
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.vortex");
        crate::io::quads_stream_to_vortex_file(
            futures::stream::iter(quads),
            &path,
            crate::LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .unwrap();
        let session = registries();
        vortex_file::register_default_encodings(&session);
        crate::io::container::register(&session);
        let error = |error: &dyn std::fmt::Display| error.to_string();
        let file = session
            .open_options()
            .open_path(&path)
            .await
            .map_err(|e| error(&e))?;
        let reader = file.layout_reader().map_err(|e| error(&e))?;
        let rows = crate::io::read::scan_all_reader(reader)
            .await
            .map_err(|e| error(&e))?;
        Ok(rows.len())
    }

    /// A session that doesn't register the aggregate (vortex-rdf 0.14's)
    /// can't read a store file carrying it, and says which aggregate.
    #[tokio::test]
    async fn a_session_without_value_counts_refuses_a_new_file() {
        let error = read_without_value_counts(3)
            .await
            .expect_err("a session without the aggregate reads the file");
        assert!(error.contains("vortexrdf.value_counts"), "{error}");
    }

    /// A store whose every graph zone holds more than 16 graphs records no
    /// value counts: each zone's state is null, the zone maps carry no
    /// `vortexrdf.value_counts` column, and a session that doesn't register
    /// the aggregate (vortex-rdf 0.14's) reads the file.
    #[tokio::test]
    async fn a_store_of_wide_graph_zones_reads_without_value_counts() {
        assert_eq!(read_without_value_counts(40).await, Ok(100));
    }
}
