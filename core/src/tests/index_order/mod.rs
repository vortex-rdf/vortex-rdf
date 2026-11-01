//! Views a by-copy index serves read straight off the index, in its order,
//! and the views cut from them stay served. [`ids`] holds the lazy id caches
//! a served view carries, which count and narrow a run without sorting its
//! ids into base order; [`reads`] a served view's streams, one-shot reads and
//! export; [`windows`] its windows and partitions; [`keeps`] its keeps; and
//! [`chained`] a pattern matched on a narrowed one. Most cases run over the
//! same store in memory and on file ([`served_stores`]).

use super::*;

mod chained;
mod ids;
mod keeps;
mod reads;
mod windows;

/// How many times `rec` saw a pending selection's ids sorted into base order.
fn ids_sorted(rec: &Recorder) -> usize {
    rec.count_where(|e| matches!(e, Event::IdsSorted(_)))
}

/// The by-copy [`stock_file`] under `layout`, opened in memory.
async fn memory_store(layout: LayoutStrategy) -> (tempfile::TempDir, VortexRdfStore) {
    let (dir, path) = stock_file(layout, IndexConfig::Copy).await;
    (
        dir,
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    )
}

/// The by-copy Dictionary [`stock_file`], opened from its file.
async fn file_store() -> (tempfile::TempDir, VortexRdfStore) {
    let (dir, path) = stock_file(LayoutStrategy::Dictionary, IndexConfig::Copy).await;
    (dir, VortexRdfStore::from_file(&path).await.unwrap())
}

/// The by-copy Dictionary [`stock_file`] three ways, each beside its name, for
/// a case that runs over all of them: built in memory (its code columns
/// resident, so a served read gathers the base at the view's ids), read into
/// memory (its code columns encoded, so a served read takes the index's own
/// columns) and read off its file. Keep each `TempDir` alive while its store
/// is used.
async fn served_stores() -> Vec<(&'static str, Option<tempfile::TempDir>, VortexRdfStore)> {
    let built = built_store(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let (read_dir, read) = memory_store(LayoutStrategy::Dictionary).await;
    let (file_dir, file) = file_store().await;
    vec![
        ("built in memory", None, built),
        ("read into memory", Some(read_dir), read),
        ("on file", Some(file_dir), file),
    ]
}

/// A quad's terms as the store spells them, `(s, p, o, g)`: their N-Triples
/// forms, the default graph as the empty string.
fn spellings(quad: &Quad) -> [String; 4] {
    let crate::store::RawQuad { s, p, o, g } = crate::store::RawQuad::from_quad(quad);
    [s, p, o, g]
}

/// `modular_quads(rows, 7, 11)` with every third quad in the named graph
/// `g1`: the default graph's rows of a predicate are scattered over its run
/// in (p, o, s, g) order, so a bound default graph is a residual term.
fn graphed_quads(rows: usize) -> Vec<Quad> {
    modular_quads(rows, 7, 11)
        .into_iter()
        .enumerate()
        .map(|(i, mut quad)| {
            if i % 3 == 0 {
                quad.graph_name = iri_graph("g1");
            }
            quad
        })
        .collect()
}

/// Three quads of `p2` whose object is `"tail"`: added to a store, its tail.
fn tail_quads() -> Vec<Quad> {
    (0..3)
        .map(|k| {
            make_quad(
                &format!("http://example.org/t{k}"),
                "http://example.org/p2",
                "tail",
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// `view` cut into `n` partitions, each served as the view is, gathered in
/// turn; beside how many partitions there are.
async fn partition_rows(view: &VortexRdfStore, n: usize) -> (usize, Vec<[TermCode; 4]>) {
    let parts = view
        .partitions(std::num::NonZeroUsize::new(n).unwrap())
        .await
        .unwrap();
    let mut rows = Vec::new();
    for part in &parts {
        assert_eq!(
            served_component(part),
            served_component(view),
            "a partition is served as its view is"
        );
        rows.extend(row_list(part).await);
    }
    (parts.len(), rows)
}

/// The `?p2??` view of `store` and its rows in the index's order.
async fn p2_view(store: &VortexRdfStore) -> (VortexRdfStore, Vec<[TermCode; 4]>) {
    let p = matched_view(store, None, Some(&iri("p2")), None, None).await;
    let want = posg(base_ordered_codes(&p).await);
    (p, want)
}

/// The rows of a window, none when it is empty.
async fn window_rows(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    if view.size().await.unwrap() == 0 {
        return vec![];
    }
    row_list(view).await
}

/// `rows[offset..offset + limit]`, clamped to `rows`.
fn slice_of<T: Clone>(rows: &[T], offset: usize, limit: usize) -> Vec<T> {
    let n = rows.len();
    rows[offset.min(n)..offset.saturating_add(limit).min(n)].to_vec()
}

/// The objects of `rows`, each once, ascending.
fn objects_of(rows: &[[TermCode; 4]]) -> Vec<TermCode> {
    let objects: std::collections::BTreeSet<TermCode> = rows.iter().map(|r| r[2]).collect();
    objects.into_iter().collect()
}

/// How many served streams `rec` saw read off their plans.
fn served_streams(rec: &Recorder) -> usize {
    rec.count(&Event::ServedRunChunks)
}

/// The quads of `quads` in `index:posg`'s order over their N-Triples
/// spellings, (p, o, s, g): a served `?p??` view's order on a string layout.
fn posg_quads(quads: &[Quad]) -> Vec<[String; 4]> {
    let mut rows: Vec<[String; 4]> = quads.iter().map(spellings).collect();
    rows.sort_by(|a, b| (&a[1], &a[2], &a[0], &a[3]).cmp(&(&b[1], &b[2], &b[0], &b[3])));
    rows
}
