//! Row ids past `u32::MAX`. Every store here is built and read under the
//! [`RowIdBase`] test hook, which numbers an indexed build's rows from a base
//! above `u32::MAX` — every reader takes the base off again — so a handful of
//! quads carry row ids a 32-bit width cannot hold, through every path that
//! stores or reads one: the in-memory index builds, the out-of-core merge's
//! spilled `(key, rid)` runs, the written `rid` columns, located-run counts
//! and windows, the copy index's served reads under tombstones, chained
//! matches, compaction, `to_bytes` and `from_parts`.
//!
//! An id narrowed to 32 bits anywhere on the way lands below the base, where
//! the readers' subtraction fails loudly, instead of naming another row.

use std::collections::BTreeSet;

use super::*;
use crate::store::array::{StrColReader, field_as};
use crate::store::builders::sorted_stream;
use crate::store::test_hooks::RowIdBase;
use crate::store::{Probe, RowId, StoreParts};
use vortex_array::VortexSessionExecute as _;
use vortex_array::arrays::struct_::StructArrayExt as _;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

/// The first row id: past `u32::MAX`, and congruent to 5 modulo 2^32, so an
/// id narrowed to 32 bits becomes a small number — a row of an unhooked store
/// of this size, but below every id here.
const BASE: RowId = (1 << 32) + 5;

/// A base whose ids straddle `u32::MAX`: half the dataset's rows are numbered
/// below it and half above, so every value's run of a reference child crosses
/// it, and a `(val, rid)` order kept in 32 bits would break inside the runs.
const STRADDLE: RowId = u32::MAX as RowId - 449;

/// Rows of the dataset.
const N: usize = 900;

/// Rows per spilled run in the out-of-core builds: 900 quads make 15 runs.
const RUN_ROWS: usize = 64;

const LAYOUTS: [LayoutStrategy; 3] = [
    LayoutStrategy::Default,
    LayoutStrategy::TypedObject,
    LayoutStrategy::Dictionary,
];

/// Each index alone: the copy index would answer every probe of a store
/// holding both. (The spilled and compacted stores carry both.)
fn index_sets() -> [Indexes; 2] {
    [
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ]
}

fn iri(s: &str) -> NamedNode {
    NamedNode::new(format!("http://example.org/{s}")).unwrap()
}

fn g1() -> GraphName {
    GraphName::NamedNode(iri("g1"))
}

/// 900 quads `s{i:04} p{i % 3} "o{i % 7}"`, alternating between the default
/// graph and `g1`: quad `i` is row `i` (subjects are unique and sort as `i`),
/// `p1` covers 300 rows (past the point-read cap), `"o2"` 129 (inside it),
/// and both together 43.
fn dataset() -> Vec<Quad> {
    graph_modular_quads(N, 4, 3, 7, &[GraphName::DefaultGraph, g1()])
}

/// The rows of `quads` at the indexes `keep` accepts, in row order.
fn rows_where(quads: &[Quad], keep: impl Fn(usize) -> bool) -> Vec<String> {
    expected_strings(quads, keep)
}

/// Every row-id-reading surface of `store`, which holds [`dataset`] and was
/// built or opened under the guard: index matches (served from a copy, or
/// gathered by a reference's ids), a graph scan, counts and windows of an
/// index run, a match chained onto an index match, and tombstones over index
/// matches.
async fn check_store(store: &VortexRdfStore, quads: &[Quad], label: &str) {
    let p1 = iri("p1");
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    assert_eq!(store.size().await.unwrap(), N, "{label}");

    // ── matches ──────────────────────────────────────────────────────────
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let p_rows = rows_where(quads, |i| i % 3 == 1);
    assert_eq!(view_strings(&by_p).await, p_rows, "{label}: by predicate");
    let by_o = store
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_o).await,
        rows_where(quads, |i| i % 7 == 2),
        "{label}: by object"
    );
    let both = store
        .match_pattern(None, Some(&p1), Some(&o2), None)
        .await
        .unwrap();
    let both_rows = rows_where(quads, |i| i % 21 == 16);
    assert_eq!(view_strings(&both).await, both_rows, "{label}: by both");
    let g = g1();
    let by_g = store
        .match_pattern(None, None, None, Some(&g))
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_g).await,
        rows_where(quads, |i| i % 2 == 1),
        "{label}: by graph"
    );
    // An index match's rows, narrowed by a second index match: the first
    // match's ids — deferred behind a serve plan, or a located run — are
    // read to intersect them.
    let chained = by_p
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(view_strings(&chained).await, both_rows, "{label}: chained");

    // ── counts and windows of the index runs ─────────────────────────────
    let probe_p = Probe::new(None, Some(p1.clone()), None, None);
    let probe_o = Probe::new(None, None, Some(o2.clone()), None);
    assert_eq!(
        store
            .count_many(&[
                probe_p.clone(),
                probe_o.clone(),
                probe_p.clone().window(5, Some(10)),
                probe_o.clone().window(120, Some(50)),
            ])
            .await
            .unwrap(),
        vec![300, 129, 10, 9],
        "{label}: counts"
    );
    for (offset, limit) in [(0, 5), (7, 20), (100, 180), (250, 10), (295, 10), (0, 300)] {
        let window = store
            .run_probe(&probe_p.clone().window(offset, Some(limit)))
            .await
            .unwrap();
        let end = (offset + limit).min(p_rows.len());
        assert_eq!(
            view_strings(&window).await,
            p_rows[offset..end].to_vec(),
            "{label}: window ({offset}, {limit})"
        );
    }

    // ── tombstones over index matches ────────────────────────────────────
    // Quad 1 carries `p1`, quad 2 `"o2"`: each index match drops one row,
    // by the row id its child records.
    let deleted = store
        .delete_quad(&quads[1])
        .await
        .unwrap()
        .delete_quad(&quads[2])
        .await
        .unwrap();
    let live = |i: usize| i != 1 && i != 2;
    let by_p = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_p).await,
        rows_where(quads, |i| i % 3 == 1 && live(i)),
        "{label}: by predicate after deletes"
    );
    let by_o = deleted
        .match_pattern(None, None, Some(&o2), None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&by_o).await,
        rows_where(quads, |i| i % 7 == 2 && live(i)),
        "{label}: by object after deletes"
    );
    assert_eq!(
        deleted
            .count_many(&[probe_p.clone(), probe_o.clone()])
            .await
            .unwrap(),
        vec![299, 128],
        "{label}: counts after deletes"
    );
    let window = deleted
        .run_probe(&probe_p.clone().window(0, Some(3)))
        .await
        .unwrap();
    assert_eq!(
        view_strings(&window).await,
        rows_where(quads, |i| i % 3 == 1 && live(i))[..3].to_vec(),
        "{label}: window after deletes"
    );
}

/// One `val` of a reference child: a term code under the Dictionary layout,
/// the term's N-Triples string under the others.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Val {
    Code(TermCode),
    Term(String),
}

/// What the index children of `parts` record, checked against a store of
/// [`N`] rows numbered from `base`: each child's row ids are `base..base + N`,
/// each once, and a reference child's rows are in `(val, rid)` order. Returns
/// how many reference children had a value whose run crosses `u32::MAX`.
fn check_children(parts: &StoreParts, base: RowId, label: &str) -> usize {
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let mut straddling = 0;
    assert!(!parts.components.is_empty(), "{label}: index children");
    for component in &parts.components {
        let who = format!("{label} / {}", component.name);
        let rows = component.rows().unwrap();
        let rid: PrimitiveArray = field_as(rows, "rid", &mut ctx).unwrap();
        let rids: &[RowId] = rid.as_slice::<RowId>();
        let ids: BTreeSet<RowId> = rids.iter().copied().collect();
        assert_eq!(rids.len(), N, "{who}: one row per quad");
        assert_eq!(
            ids,
            (base..base + N as RowId).collect::<BTreeSet<_>>(),
            "{who}: every row id once, numbered from the base"
        );
        if !component.name.starts_with("index:ref-") {
            continue;
        }
        let val = rows.unmasked_field_by_name("val").unwrap();
        let vals: Vec<Val> = if matches!(val.dtype(), vortex_array::dtype::DType::Utf8(_)) {
            let col: VarBinViewArray = field_as(rows, "val", &mut ctx).unwrap();
            let reader = StrColReader::new(&col);
            (0..col.len())
                .map(|i| Val::Term(reader.str_at(i).unwrap().to_owned()))
                .collect()
        } else {
            let col: PrimitiveArray = field_as(rows, "val", &mut ctx).unwrap();
            col.as_slice::<TermCode>()
                .iter()
                .map(|&c| Val::Code(c))
                .collect()
        };
        let pairs: Vec<(Val, RowId)> = vals.into_iter().zip(rids.iter().copied()).collect();
        assert!(
            pairs.windows(2).all(|pair| pair[0] < pair[1]),
            "{who}: rows are in (val, rid) order"
        );
        let crosses = pairs.windows(2).any(|pair| {
            pair[0].0 == pair[1].0
                && pair[0].1 <= RowId::from(u32::MAX)
                && pair[1].1 > RowId::from(u32::MAX)
        });
        straddling += usize::from(crosses);
    }
    straddling
}

/// The bytes of a store file the out-of-core builder writes from `quads`,
/// its merge spilling runs of [`RUN_ROWS`] quads.
#[cfg(feature = "file-io")]
async fn spilled_store_bytes(
    quads: Vec<Quad>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Vec<u8> {
    let built = sorted_stream::build_chunk_stream(
        Box::new(quad_stream(quads)),
        layout,
        indexes,
        RUN_ROWS,
        None,
    )
    .await
    .unwrap();
    let mut bytes = Vec::new();
    crate::io::ser::built_stream_to_vortex_writer(built, &mut bytes)
        .await
        .unwrap();
    bytes
}

/// `store`, which holds [`dataset`], adopted as parts and as serialized
/// bytes: every surface reads row ids past `u32::MAX`, and the children hold
/// them.
async fn check_adoptions(store: &VortexRdfStore, quads: &[Quad], label: &str) {
    check_store(store, quads, &format!("{label}: built")).await;
    let parts = store.to_serializable_parts().await.unwrap();
    check_children(&parts, BASE, label);
    let adopted = VortexRdfStore::from_parts(parts).unwrap();
    check_store(&adopted, quads, &format!("{label}: from_parts")).await;
    #[cfg(feature = "file-io")]
    {
        let bytes = store.to_bytes().await.unwrap();
        let reopened = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        check_store(&reopened, quads, &format!("{label}: from_bytes")).await;
    }
}

/// The in-memory index builds — the sorted in-memory builder under every
/// layout, which is what `from_quads` takes on wasm, and the interning sink
/// behind the JS binding's ingest — number rows past `u32::MAX`, and the
/// built stores, their parts and their bytes read them.
#[tokio::test]
async fn row_ids_past_u32_round_trip_in_memory() {
    let _base = RowIdBase::set(BASE);
    let quads = dataset();
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let label = format!("{layout:?} {indexes:?}");
            let built =
                build_array::<SortedInMemoryBuilder>(quad_stream(quads.clone()), layout, indexes)
                    .await
                    .unwrap();
            let store = VortexRdfStore::from_built(built).unwrap();
            check_adoptions(&store, &quads, &label).await;
        }
    }
    for indexes in index_sets() {
        let label = format!("interning sink {indexes:?}");
        let mut sink = DictionaryQuadSink::new(indexes);
        for quad in &quads {
            sink.push(crate::store::RawQuad::from_quad(quad));
        }
        let store = VortexRdfStore::from_built(sink.finish().unwrap()).unwrap();
        check_adoptions(&store, &quads, &label).await;
    }
}

/// The out-of-core builder — what `from_quads` takes wherever a filesystem
/// exists — numbers rows as its merge emits them and spills each family's
/// `(key, rid)` records in runs: the merged children hold every id past
/// `u32::MAX`, in `(val, rid)` order, and the built store, its parts and its
/// bytes read them.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn row_ids_past_u32_round_trip_through_spilled_runs() {
    let _base = RowIdBase::set(BASE);
    let quads = dataset();
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let label = format!("spilled {layout:?} {indexes:?}");
            let built = sorted_stream::build_array(
                Box::new(quad_stream(quads.clone())),
                layout,
                indexes,
                RUN_ROWS,
            )
            .await
            .unwrap();
            let store = VortexRdfStore::from_built(built).unwrap();
            check_adoptions(&store, &quads, &label).await;
        }
    }
}

/// A file the out-of-core builder writes from spilled runs: every `rid`
/// column is a u64 on the wire holding the ids past `u32::MAX`, and the
/// mapped open, the loaded open and `from_bytes` read them — under the
/// Dictionary layout through located runs, whose windows read exactly their
/// own rows by point reads inside the cap and by a range scan beyond it.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn row_ids_past_u32_round_trip_through_a_file() {
    use vortex_array::dtype::{DType, Nullability, PType};

    let _base = RowIdBase::set(BASE);
    let quads = dataset();
    let p1 = iri("p1");
    let o2 = Term::Literal(Literal::new_simple_literal("o2"));
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let label = format!("{layout:?} {indexes:?}");
            let by_reference = indexes == [IndexType::SecondaryByReference];
            let bytes = spilled_store_bytes(quads.clone(), layout, indexes).await;

            // The wire: every index child's `rid` is a non-nullable u64.
            let (_, components) = crate::io::container::store_metadata_of_bytes(&bytes);
            let u64_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
            let mut rid_columns = 0;
            for component in components.iter().filter(|c| c.name.starts_with("index:")) {
                let DType::Struct(fields, _) = &component.dtype else {
                    panic!("{label}: {} is a struct", component.name);
                };
                assert_eq!(
                    fields.field("rid").as_ref(),
                    Some(&u64_dtype),
                    "{label}: {}.rid",
                    component.name
                );
                rid_columns += 1;
            }
            assert!(rid_columns >= 2, "{label}: index children");

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.vortex");
            std::fs::write(&path, &bytes).unwrap();
            let mapped = VortexRdfStore::from_file(&path).await.unwrap();
            check_children(&mapped.to_serializable_parts().await.unwrap(), BASE, &label);
            check_store(&mapped, &quads, &format!("{label}: mapped")).await;
            if by_reference && layout == LayoutStrategy::Dictionary {
                // The runs are located, and their windows read only their
                // own row ids — past the point-read cap (the 300-row
                // predicate run) and inside it (the 129-row object run).
                let run = mapped
                    .debug_reference_index_located_run(Some(&p1), None)
                    .await
                    .unwrap()
                    .expect("the predicate run is located");
                assert_eq!(run.end - run.start, 300, "{label}");
                super::indexes_file::assert_windows_read_only_their_rows(
                    &mapped,
                    Some(&p1),
                    None,
                    300,
                    &[(0, 5), (7, 20), (100, 180), (0, 299), (298, 1), (295, 10)],
                )
                .await;
                super::indexes_file::assert_windows_read_only_their_rows(
                    &mapped,
                    None,
                    Some(&o2),
                    129,
                    &[(0, 5), (10, 50), (127, 1), (120, 20), (0, 129)],
                )
                .await;
            }
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            check_store(&loaded, &quads, &format!("{label}: from_file_in_memory")).await;
            let reopened = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            check_store(&reopened, &quads, &format!("{label}: from_bytes")).await;
        }
    }
}

/// `(val, rid)` order holds when a value's run crosses `u32::MAX` — the
/// order a located window reads its rows by — in the children the in-memory
/// builds and the spilled merge write, and through a file's located
/// windows.
#[tokio::test]
async fn reference_children_keep_val_rid_order_across_u32_max() {
    let _base = RowIdBase::set(STRADDLE);
    let quads = dataset();
    for layout in LAYOUTS {
        let label = format!("{layout:?}");
        let built = build_array::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            layout,
            vec![IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(built).unwrap();
        let parts = store.to_serializable_parts().await.unwrap();
        assert_eq!(
            check_children(&parts, STRADDLE, &format!("{label}: in memory")),
            2,
            "{label}: both children have a run crossing u32::MAX"
        );
        check_store(&store, &quads, &format!("{label}: in memory")).await;

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let built = sorted_stream::build_array(
                Box::new(quad_stream(quads.clone())),
                layout,
                vec![IndexType::SecondaryByReference],
                RUN_ROWS,
            )
            .await
            .unwrap();
            let store = VortexRdfStore::from_built(built).unwrap();
            let parts = store.to_serializable_parts().await.unwrap();
            assert_eq!(
                check_children(&parts, STRADDLE, &format!("{label}: spilled")),
                2,
                "{label}: both children have a run crossing u32::MAX"
            );
        }
    }

    #[cfg(feature = "file-io")]
    {
        let p1 = iri("p1");
        let bytes = spilled_store_bytes(
            quads.clone(),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference],
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.vortex");
        std::fs::write(&path, &bytes).unwrap();
        let mapped = VortexRdfStore::from_file(&path).await.unwrap();
        check_store(&mapped, &quads, "straddling file").await;
        super::indexes_file::assert_windows_read_only_their_rows(
            &mapped,
            Some(&p1),
            None,
            300,
            &[(140, 20), (100, 180), (0, 300), (149, 2)],
        )
        .await;
    }
}

/// The rebuilds — an append compacted in memory and in a file, a deletion
/// compacted, the serialization of a store with a tail — number the rebuilt
/// rows from the base again, and the rebuilt stores read them.
#[tokio::test]
async fn row_ids_past_u32_survive_compaction() {
    let _base = RowIdBase::set(BASE);
    let mut quads = dataset();
    let extra = quads.pop().unwrap();
    for layout in LAYOUTS {
        let label = format!("{layout:?}");
        let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
        let store = VortexRdfStore::from_quads(quad_stream(quads.clone()), layout, indexes.clone())
            .await
            .unwrap();
        let grown = store.add_quad(extra.clone()).await.unwrap();
        let mut all = quads.clone();
        all.push(extra.clone());
        let compacted = grown.compact().await.unwrap();
        check_children(
            &compacted.to_serializable_parts().await.unwrap(),
            BASE,
            &label,
        );
        check_store(&compacted, &all, &format!("{label}: compacted")).await;
        #[cfg(feature = "file-io")]
        {
            let bytes = grown.to_bytes().await.unwrap();
            let reopened = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            check_store(&reopened, &all, &format!("{label}: tail serialized")).await;
        }
        let shrunk = compacted
            .delete_quad(&extra)
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();
        assert_eq!(view_strings(&shrunk).await, quad_strings(&quads), "{label}");

        #[cfg(feature = "file-io")]
        {
            let (_dir, path) = write_store_file(quads.clone(), layout, indexes).await;
            let file = VortexRdfStore::from_file(&path).await.unwrap();
            let compacted = file
                .add_quad(extra.clone())
                .await
                .unwrap()
                .compact()
                .await
                .unwrap();
            assert_eq!(compacted.debug_file_mapped(), Some(true), "{label}");
            check_store(&compacted, &all, &format!("{label}: file compacted")).await;
            let reopened = VortexRdfStore::from_file(&path).await.unwrap();
            check_children(
                &reopened.to_serializable_parts().await.unwrap(),
                BASE,
                &format!("{label}: file compacted"),
            );
        }
    }
}
