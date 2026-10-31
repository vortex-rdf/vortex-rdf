//! Filtered file views whose reads take their pending filter by its masks
//! first: the filter evaluated split by split into masks, and the rows they
//! keep read as the masks choose — point by point, by one take, or each split
//! projected at its mask. The fixtures are a file of sixteen splits whose
//! patterns match at three densities, with a tail to append and deletes that
//! keep each pattern's read, and a file of more splits than a scan keeps in
//! flight. [`reads`] holds the one-shot reads and the drivers under them,
//! [`windows`] the windows that keep what the filter found and the streams
//! over them.

use super::*;

/// The rows of one leaf, and so of one split, of [`masks_file`].
const MASKS_SPLIT_ROWS: usize = 1_024;
/// The splits of [`masks_file`].
const MASKS_SPLITS: usize = 16;
/// The split of [`masks_quads`] that holds none of its patterns' matches.
const MASKS_EMPTY_SPLIT: usize = 3;

/// [`MASKS_SPLITS`] × [`MASKS_SPLIT_ROWS`] quads with subjects `s{i:05}`, so in
/// row order, whose patterns match at three densities in every split but
/// [`MASKS_EMPTY_SPLIT`]: the object `"sparse"` on every 512th row, the
/// predicate `pm` on every 32nd and the graph `gd` on every other one. Any
/// other row has the predicate `p{i % 3}`, the object `"o{i % 7}"` and the
/// default graph.
fn masks_quads() -> Vec<Quad> {
    let gd = iri_graph("gd");
    (0..MASKS_SPLITS * MASKS_SPLIT_ROWS)
        .map(|i| {
            let matched = i / MASKS_SPLIT_ROWS != MASKS_EMPTY_SPLIT;
            let p = if matched && i % 32 == 0 {
                "http://example.org/pm".to_string()
            } else {
                format!("http://example.org/p{}", i % 3)
            };
            let o = if matched && i % 512 == 7 {
                "sparse".to_string()
            } else {
                format!("o{}", i % 7)
            };
            let g = if matched && i % 2 == 1 {
                gd.clone()
            } else {
                GraphName::DefaultGraph
            };
            make_quad(&format!("http://example.org/s{i:05}"), &p, &o, g)
        })
        .collect()
}

/// A probe of [`masks_quads`], beside what the file makes of it.
struct MasksProbe {
    /// Its name in failure messages.
    shape: &'static str,
    probe: Probe,
    /// The read its masks choose.
    read: crate::store::scan::read_plan::MaskedRead,
    /// Its masks are smaller than its ids (a bit a row of each split a mask
    /// keeps some rows of, against 64 a match), so a window that cuts none of
    /// its rows keeps them.
    dense: bool,
    /// How many of [`masks_tail`]'s twelve rows it matches.
    tail_rows: usize,
}

/// The probes of [`masks_quads`], each beside the read its masks choose: the
/// object (two rows a split) and the predicate with `"o0"` (about five a
/// split) point by point, the predicate (32 a split) by a take, and the graph
/// (half the rows) by projecting each split at its mask. The predicate and
/// the graph are the dense ones.
fn masks_probes() -> Vec<MasksProbe> {
    use crate::store::scan::read_plan::MaskedRead;
    let pm = iri("pm");
    let gd = iri_graph("gd");
    vec![
        MasksProbe {
            shape: "O",
            probe: Probe::new(None, None, Some(plain_literal("sparse")), None),
            read: MaskedRead::Points,
            dense: false,
            tail_rows: 6,
        },
        MasksProbe {
            shape: "PO",
            probe: Probe::new(None, Some(pm.clone()), Some(plain_literal("o0")), None),
            read: MaskedRead::Points,
            dense: false,
            tail_rows: 6,
        },
        MasksProbe {
            shape: "P",
            probe: Probe::new(None, Some(pm), None, None),
            read: MaskedRead::Take,
            dense: true,
            tail_rows: 12,
        },
        MasksProbe {
            shape: "G",
            probe: Probe::new(None, None, None, Some(gd)),
            read: MaskedRead::Project,
            dense: true,
            tail_rows: 12,
        },
    ]
}

/// [`masks_quads`] as a Dictionary-layout file of [`MASKS_SPLIT_ROWS`]-row
/// leaves ([`write_chunked_store_file`]), so of [`MASKS_SPLITS`] splits, with
/// no index, in a fresh temp dir. Serialized once per process (see
/// [`cached_store_bytes`]). Keep the `TempDir` alive for the file's lifetime.
async fn masks_file() -> (tempfile::TempDir, std::path::PathBuf) {
    masks_file_with(false).await
}

/// [`masks_file`] with its dictionary child written as one flat struct leaf,
/// which a mapped open holds resident (see [`write_chunked_store`]).
async fn masks_file_resident_dictionary() -> (tempfile::TempDir, std::path::PathBuf) {
    masks_file_with(true).await
}

async fn masks_file_with(flat_dictionary: bool) -> (tempfile::TempDir, std::path::PathBuf) {
    static BYTES: [OnceLock<Vec<u8>>; 2] = [const { OnceLock::new() }; 2];
    let bytes = cached_store_bytes(&BYTES[usize::from(flat_dictionary)], || async move {
        let (_dir, path) = write_chunked_store(
            &masks_quads(),
            MASKS_SPLIT_ROWS,
            None,
            flat_dictionary,
            None,
        )
        .await;
        std::fs::read(&path).unwrap()
    })
    .await;
    temp_store_file(bytes)
}

/// Twelve quads to append into a tail, each with the predicate `pm` and the
/// graph `gd`, and the object `"sparse"` and `"o0"` by turns: of the
/// [`masks_probes`] patterns, `P` and `G` match all twelve, `O` and `PO` half
/// (each probe's `tail_rows`).
fn masks_tail() -> Vec<Quad> {
    let gd = iri_graph("gd");
    (0..12)
        .map(|k| {
            let o = if k % 2 == 0 { "sparse" } else { "o0" };
            make_quad(
                &format!("http://example.org/tail{k:02}"),
                "http://example.org/pm",
                o,
                gd.clone(),
            )
        })
        .collect()
}

/// `store` with some of every [`masks_probes`] pattern's rows deleted, each
/// pattern keeping its read: the predicate's rows whose object is `"o3"`, the
/// graph's whose object is `"o5"`, the object's whose predicate is `p1`, and
/// three of the rows of `pm` with `"o0"`.
async fn masks_deleted(store: &VortexRdfStore) -> VortexRdfStore {
    let gd = iri_graph("gd");
    let store = store
        .delete_matching(None, Some(&iri("pm")), Some(&plain_literal("o3")), None)
        .await
        .unwrap()
        .delete_matching(None, None, Some(&plain_literal("o5")), Some(&gd))
        .await
        .unwrap()
        .delete_matching(None, Some(&iri("p1")), Some(&plain_literal("sparse")), None)
        .await
        .unwrap();
    let po: Vec<Quad> = masks_quads()
        .into_iter()
        .filter(|q| q.predicate == iri("pm") && q.object == plain_literal("o0"))
        .take(3)
        .collect();
    delete_all(&store, &po).await
}

/// The rows of one leaf, and so of one split, of [`many_splits_file`].
const MANY_SPLIT_ROWS: usize = 32;
/// The splits of [`many_splits_file`]: more than a spawned scan keeps in
/// flight, four per unit of available parallelism, on most hosts.
pub(super) const MANY_SPLITS: usize = 256;

/// [`MANY_SPLITS`] × [`MANY_SPLIT_ROWS`] quads `s{i:05} p{i % 3} "o{i % 4}"` in
/// the default graph as a Dictionary-layout file of [`MANY_SPLIT_ROWS`]-row
/// leaves with no index, in a fresh temp dir: `p0`'s matches spread over every
/// split, ten or eleven a split. Serialized once per process (see
/// [`cached_store_bytes`]). Keep the `TempDir` alive for the file's lifetime.
pub(super) async fn many_splits_file() -> (tempfile::TempDir, std::path::PathBuf) {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        let quads = graph_modular_quads(
            MANY_SPLITS * MANY_SPLIT_ROWS,
            5,
            3,
            4,
            &[GraphName::DefaultGraph],
        );
        let (_dir, path) = write_chunked_store_file(&quads, MANY_SPLIT_ROWS).await;
        std::fs::read(&path).unwrap()
    })
    .await;
    temp_store_file(bytes)
}

/// A view's rows in read order, as the bindings' one-shot reads return them:
/// its code columns, or the quads of a view with a tail, whose rows have no
/// codes.
#[derive(Debug, PartialEq)]
enum ReadRows {
    Codes(Vec<[TermCode; 4]>),
    Quads(Vec<Quad>),
}

async fn read_rows(view: &VortexRdfStore) -> ReadRows {
    match view.code_columns_gathered().await.unwrap() {
        Some(c) => ReadRows::Codes(code_rows(&c)),
        None => ReadRows::Quads(view.quads_vec().await.unwrap()),
    }
}

mod pieces;
mod reads;
mod windows;
