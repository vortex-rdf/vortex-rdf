//! `locate_codes` against the term path: a located run over a file store
//! answers every shape of the store's own codes as the term path does, by
//! subject, by object and through the reference indexes, in pages and
//! streams, under tombstones and narrowed views (which it declines); and the
//! planner's choices between its runs: the shorter of a subject's and an
//! object's, the reference runs a gap run searches, and the gap run a capped
//! reference run leaves to answer. With `VORTEX_RDF_LOCATE=0` no located run
//! reads a block and `locate_codes` declines every pattern, so each test
//! returns at once.

use super::zones::{code_of, streamed, term_rows};
use super::*;
use crate::store::query::located::locate_enabled;

/// Quads whose sort-key prefixes repeat at every depth: 40 subjects, each
/// with three predicates, each predicate with two or three objects, each
/// triple in one to three graphs. So a bound `(s, p)` has several objects, a
/// `(p, o)` several subjects and a `(p, o, s)` several graphs: every key of a
/// located run's prefix narrows it.
fn multiplicity_quads() -> Vec<Quad> {
    let mut quads = Vec::new();
    for s in 0..40 {
        for p in 0..3 {
            for o in 0..(2 + (s + p) % 2) {
                for g in 0..=((s + o) % 3) {
                    quads.push(make_quad(
                        &format!("http://example.org/s{s:02}"),
                        &format!("http://example.org/p{}", (s + p) % 5),
                        &format!("o{}", (s * 3 + o) % 7),
                        numbered_graph(g),
                    ));
                }
            }
        }
    }
    quads
}

/// [`multiplicity_quads`] and a hub subject with 60 quads, each object held
/// by no other quad: under the reference index the hub's `s?o` checks the
/// object's one-row reference run, every other subject's its own run, shorter
/// than any object's.
fn hub_quads() -> Vec<Quad> {
    let mut quads = multiplicity_quads();
    for k in 0..60 {
        quads.push(make_quad(
            "http://example.org/hub",
            &format!("http://example.org/p{}", k % 5),
            &format!("rare{k}"),
            GraphName::DefaultGraph,
        ));
    }
    quads
}

/// Under the reference index, a bound subject and object read the shorter
/// of the subject's run and the object's reference run: the hub's 60 rows
/// give way to its object's one-row reference run, and a subject's few rows
/// win over its object's long run.
#[tokio::test]
async fn subject_and_object_read_the_shorter_run() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }
    let (_dir, path) = write_store_file(
        hub_quads(),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let all = row_list(&store).await;
    let rows_where =
        |slot: usize, code: TermCode| all.iter().filter(|row| row[slot] == code).count();
    let hub = code_of(&store, "<http://example.org/hub>").await;
    let rare = code_of(&store, "\"rare7\"").await;
    let s05 = code_of(&store, "<http://example.org/s05>").await;
    let first = all.iter().find(|row| row[0] == s05).expect("s05's quads")[2];
    assert_eq!((rows_where(0, hub), rows_where(2, rare)), (60, 1));
    assert!(
        rows_where(0, s05) < rows_where(2, first),
        "s05's run is the shorter"
    );
    // Each pattern beside the rows its run reads: the object's one row for
    // the hub, the subject's own for s05.
    for (s, o, read) in [(hub, rare, 1), (s05, first, rows_where(0, s05))] {
        let pattern = CodePattern::new(Some(s), None, Some(o), None);
        let want: Vec<[TermCode; 4]> = all
            .iter()
            .filter(|row| row[0] == s && row[2] == o)
            .copied()
            .collect();
        let rec = Recorder::start();
        let count = store.count_code_pattern(pattern).await.unwrap();
        let blocks = located_blocks(&rec);
        drop(rec);
        assert_eq!(count, want.len() as u64, "{pattern:?}: the count");
        assert_eq!(
            blocks.iter().sum::<u64>(),
            read as u64,
            "{pattern:?}: rows read"
        );
        let got = store.match_code_pattern(pattern, 0, None).await.unwrap();
        assert_eq!(code_rows::<4>(&got), want, "{pattern:?}: the rows");
    }
}

/// A gap run is weighed only against the reference runs that could be
/// shorter than it, those of bound slots past its prefix: a slot in the
/// prefix holds every row of the gap run. So on a by-reference file `(s p ?
/// g)` searches no reference index and `(s ? o ?)` and `(s ? o g)` search
/// `o`'s alone, and on a file with both index families no gap shape searches
/// any, the copy tables' prefixes covering its `p` and `o`. Each answers
/// the term path's rows.
#[tokio::test]
async fn gap_runs_search_only_the_reference_runs_that_could_be_shorter() {
    use crate::CodePattern;
    use crate::store::indexes::ALL_INDEX_TYPES;

    if !locate_enabled() {
        return;
    }
    // Each index set beside the shapes checked on it (bit `i` binds slot
    // `i` of `(s, p, o, g)`) and the reference indexes each searches.
    let cases = [
        (
            IndexConfig::Reference.types(),
            vec![(0b1011, 0), (0b0101, 1), (0b1101, 1)],
        ),
        (
            ALL_INDEX_TYPES.to_vec(),
            vec![
                (0b1010, 0),
                (0b1100, 0),
                (0b1101, 0),
                (0b1011, 0),
                (0b1110, 0),
            ],
        ),
    ];
    for (indexes, shapes) in cases {
        let (_dir, path) =
            write_store_file(multiplicity_quads(), LayoutStrategy::Dictionary, indexes).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let all = row_list(&store).await;
        let quad = all[all.len() / 3];
        for (mask, searched) in shapes {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            let pattern = CodePattern::new(slot(0), slot(1), slot(2), slot(3));
            let rec = Recorder::start();
            let located = store.locate_codes(pattern, 0, None).await.unwrap();
            let searches = rec.picked(|event| match event {
                Event::ReferenceSearched { slot } => Some(*slot),
                _ => None,
            });
            drop(rec);
            let located = located.unwrap_or_else(|| panic!("{pattern:?}: located"));
            assert_eq!(
                searches.len(),
                searched,
                "{pattern:?}: searched {searches:?}"
            );
            let mut got = code_rows::<4>(&located.columns);
            let mut want: Vec<[TermCode; 4]> = all
                .iter()
                .filter(|row| (0..4).all(|i| slot(i).is_none_or(|code| row[i] == code)))
                .copied()
                .collect();
            got.sort_unstable();
            want.sort_unstable();
            assert_eq!(got, want, "{pattern:?}: the rows");
        }
    }
}

/// A gap run gives way only to a shorter reference run that answers: with
/// the row-by-row cap forced below both, `(big ? common)`'s subject run,
/// which its zones decide, answers, where the object's shorter reference
/// run, its subjects checked row by row, would be capped and decline.
#[tokio::test]
async fn a_capped_reference_run_leaves_the_gap_run_to_answer() {
    use crate::CodePattern;
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    if !locate_enabled() {
        return;
    }
    let _zones = ForcedZones::set(true);
    let quads: Vec<Quad> = (0..10_000)
        .map(|k| {
            let object = match k % 2 {
                0 => "common".to_string(),
                _ => format!("o{k}"),
            };
            make_quad(
                "http://example.org/big",
                &format!("http://example.org/p{k:05}"),
                &object,
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let big = code_of(&store, "<http://example.org/big>").await;
    let common = code_of(&store, "\"common\"").await;
    let pattern = CodePattern::new(Some(big), None, Some(common), None);
    let _cap = ForcedGapRows::set(1_000);
    assert!(
        store
            .locate_codes(pattern, 0, Some(0))
            .await
            .unwrap()
            .is_some(),
        "the subject's run, decided by its zones"
    );
    assert_eq!(store.count_code_pattern(pattern).await.unwrap(), 5_000);
}

/// `locate_codes` answers exactly what the term path answers: for every bound
/// subset of a store's own quads' codes, on every file open and index set
/// (the suite's fixture, and [`multiplicity_quads`] and [`hub_quads`]
/// mapped), the total is `size()` and the rows are the match's, in pages that
/// cover them once. It declines (`None`) rather than answer a tombstoned or
/// narrowed view; a code no quad holds matches nothing.
#[tokio::test]
async fn test_locate_codes_matches_the_term_path() {
    if !locate_enabled() {
        return;
    }
    /// The store file at `path`, written under `index`, opened mapped, its
    /// dir guard kept beside it.
    async fn mapped(
        index: IndexConfig,
        (dir, path): (tempfile::TempDir, std::path::PathBuf),
    ) -> FixtureStore {
        FixtureStore {
            index,
            open: Some(Open::Mapped),
            store: VortexRdfStore::from_file(&path).await.unwrap(),
            _dir: Some(std::sync::Arc::new(dir)),
        }
    }

    // Each store beside its tag, the quads it holds and the stride of the
    // rows checked.
    let mut stores: Vec<(String, FixtureStore, Vec<Quad>, usize)> = Vec::new();
    for fixture in file_stores().await {
        stores.push((fixture.tag(), fixture, fixture_quads(), 97));
    }
    for index in IndexConfig::ALL {
        let file = write_store_file(
            multiplicity_quads(),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        stores.push((
            format!("multiplicity/{}/mapped", index.name()),
            mapped(index, file).await,
            multiplicity_quads(),
            3,
        ));
    }
    // Seven-row flat leaves: most subject runs cross a leaf boundary, so the
    // narrowing and the reads go across leaves as well as inside one.
    let file = write_chunked_store_file(&multiplicity_quads(), 7).await;
    stores.push((
        "multiplicity/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::NoIndex, file).await,
        multiplicity_quads(),
        1,
    ));
    // The reference index in seven-row leaves: a run of `index:ref-p` or
    // `index:ref-o` spans leaves, so the other run is walked leaf by leaf.
    let file = write_chunked_reference_store_file(&multiplicity_quads(), 7).await;
    stores.push((
        "multiplicity/reference/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        multiplicity_quads(),
        3,
    ));
    // A bound subject's rows are checked when its run is the shorter one, a
    // reference run's otherwise: the hub holds both, in one leaf and in
    // seven-row leaves, where a subject's run crosses them.
    let file = write_store_file(
        hub_quads(),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    stores.push((
        "hub/reference/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        hub_quads(),
        3,
    ));
    let file = write_chunked_reference_store_file(&hub_quads(), 7).await;
    stores.push((
        "hub/reference/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        hub_quads(),
        3,
    ));
    for (tag, fixture, quads, step) in stores {
        check_locate_codes(&tag, &fixture, &quads, step).await;
    }
    for fixture in file_stores().await {
        check_code_patterns(&fixture).await;
    }
    // A store without a term dictionary has no code patterns, streamed or
    // not; a stream whose chunks would hold no row (`batch_rows` 0) is
    // refused, whatever its pattern.
    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let error = store
        .count_code_pattern(crate::CodePattern::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no term dictionary"), "{error}");
    let error = store
        .match_code_chunks(crate::CodePattern::default(), 7)
        .await
        .err()
        .expect("no code stream without a dictionary");
    assert!(error.to_string().contains("no term dictionary"), "{error}");
    let stores = file_stores().await;
    let store = &stores[0].store;
    let past = store.dict_reader().unwrap().len() as TermCode + 7;
    let nothing = crate::CodePattern::new(Some(past), None, None, None);
    for pattern in [crate::CodePattern::default(), nothing] {
        let error = store.match_code_chunks(pattern, 0).await.err();
        assert!(error.is_some(), "{pattern:?}: batch_rows 0");
    }
}

/// The rows of each block of a located run that `rec` saw read, before the
/// block's checks.
fn located_blocks(rec: &Recorder) -> Vec<u64> {
    rec.picked(|event| match event {
        Event::LocatedBlock(rows) => Some(*rows),
        _ => None,
    })
}

/// `count_code_pattern` and `match_code_pattern` answer every shape of a
/// store's own codes as the term path does, whether a located run answers it
/// or the general match: the count is the match's size, pages cover its rows
/// once, and `match_code_chunks` streams the unwindowed match's rows in its
/// order. A code in a position its term kind cannot take, or past the
/// dictionary, matches nothing.
async fn check_code_patterns(fixture: &FixtureStore) {
    use crate::CodePattern;
    let tag = fixture.tag();
    let store = &fixture.store;
    let dict = store.dict_reader().unwrap();
    let all = row_list(store).await;
    let mut patterns = vec![CodePattern::default()];
    // Every bound subset of four quads spread over the store.
    for quad in all.iter().step_by(all.len() / 4) {
        for mask in 1..16u8 {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            patterns.push(CodePattern::new(slot(0), slot(1), slot(2), slot(3)));
        }
    }
    for pattern in patterns {
        let want = term_rows(store, pattern).await;
        let tag = format!("{tag} {pattern:?}");
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            want.len() as u64,
            "{tag}: count"
        );
        // About five pages, the last one short.
        let page = want.len() / 4 + 1;
        let mut seen = Vec::new();
        let mut offset = 0;
        loop {
            let got = store
                .match_code_pattern(pattern, offset, Some(page))
                .await
                .unwrap();
            let n = got[0].len();
            seen.extend(code_rows::<4>(&got));
            if n < page {
                break;
            }
            offset += page;
        }
        seen.sort_unstable();
        assert_eq!(seen, want, "{tag}: pages of {page}");
        // Streamed: the unwindowed match's rows, in its order.
        let whole = store.match_code_pattern(pattern, 0, None).await.unwrap();
        assert_eq!(
            streamed(store, pattern, page).await,
            code_rows::<4>(&whole),
            "{tag}: chunks of {page}"
        );
    }
    let kinds = dict.kind_ranges().await.unwrap();
    let mut nothing = vec![CodePattern::new(
        Some(dict.len() as TermCode + 7),
        None,
        None,
        None,
    )];
    if !kinds.literals.is_empty() {
        let literal = Some(kinds.literals.start);
        nothing.push(CodePattern::new(literal, None, None, None));
        nothing.push(CodePattern::new(None, literal, None, None));
        nothing.push(CodePattern::new(None, None, None, literal));
    }
    if !kinds.blanks.is_empty() {
        nothing.push(CodePattern::new(None, Some(kinds.blanks.start), None, None));
    }
    if let Some(default_graph) = kinds.default_graph {
        nothing.push(CodePattern::new(Some(default_graph), None, None, None));
    }
    for pattern in nothing {
        let tag = format!("{tag} {pattern:?}");
        assert_eq!(store.count_code_pattern(pattern).await.unwrap(), 0, "{tag}");
        let got = store.match_code_pattern(pattern, 0, None).await.unwrap();
        assert!(got.iter().all(|column| column.is_empty()), "{tag}");
        assert!(
            streamed(store, pattern, 7).await.is_empty(),
            "{tag}: streamed"
        );
    }
}

/// A bound graph after a free slot over a long run:
/// `(? p o g)`, one `(p, o)` run of 40,000 quads alternating between two
/// graphs, on the copy index (its `posg` run, checked) and the reference
/// index (a long reference run, checked), read in pages and counted, more
/// than once and interleaved with another such pattern, as the term path
/// answers it; and streamed, each row of the run read once, in the pages'
/// order.
#[tokio::test]
async fn test_locate_codes_long_graph_bound_run() {
    if !locate_enabled() {
        return;
    }
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        check_long_graph_bound_run(index).await;
    }
}

async fn check_long_graph_bound_run(index: IndexConfig) {
    use crate::CodePattern;
    let quads: Vec<Quad> = (0..40_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                "http://example.org/type",
                "Taxon",
                numbered_graph(i % 2),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, index.types()).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let row = row_list(&store).await[0];
    let other = row_list(&store)
        .await
        .into_iter()
        .find(|r| r[3] != row[3])
        .unwrap();
    for _ in 0..2 {
        for graph in [row[3], other[3]] {
            let pattern = CodePattern::new(None, Some(row[1]), Some(row[2]), Some(graph));
            let want = term_rows(&store, pattern).await;
            assert_eq!(want.len(), 20_000);
            assert_eq!(store.count_code_pattern(pattern).await.unwrap(), 20_000);
            let mut seen = Vec::new();
            let mut offset = 0;
            while offset < want.len() {
                let got = store
                    .locate_codes(pattern, offset, Some(4096))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(got.total, 20_000);
                seen.extend(code_rows::<4>(&got.columns));
                offset += 4096;
            }
            let paged = seen.clone();
            seen.sort_unstable();
            assert_eq!(seen, want);
            // A window without its count checks the run only as far as its
            // own rows; a count checks the whole run.
            let rec = Recorder::start();
            let first = store
                .match_code_pattern(pattern, 0, Some(64))
                .await
                .unwrap();
            let read: u64 = located_blocks(&rec).iter().sum();
            assert!(
                read < 40_000,
                "a first page of 64 read {read} rows of the run"
            );
            assert_eq!(code_rows::<4>(&first), paged[..64], "the first page");
            drop(rec);
            // A count of the copy index's run decides its zones, every one
            // mixed here and, lying whole within the run, counted from the
            // graphs it records; a reference run's rows are read and checked.
            let rec = Recorder::start();
            assert_eq!(store.count_code_pattern(pattern).await.unwrap(), 20_000);
            if matches!(index, IndexConfig::Copy) && crate::zone_stats::rules::zones_enabled() {
                let rows = |counted_from_records: bool| -> u64 {
                    rec.rows(|e| match (e, counted_from_records) {
                        (Event::ZoneRecorded(rows), true) | (Event::ZoneCounted(rows), false) => {
                            Some(*rows)
                        }
                        _ => None,
                    })
                };
                assert_eq!(rows(true), 40_000, "a count, zone by zone, from records");
                assert_eq!(rows(false), 0, "nothing counted on the encoding");
                assert_eq!(located_blocks(&rec).iter().sum::<u64>(), 0, "a count");
            } else {
                assert_eq!(located_blocks(&rec).iter().sum::<u64>(), 40_000, "a count");
            }
            drop(rec);
            // Streamed in chunks that do not divide the run, which is read
            // once, its rows in the pages' order.
            for batch in [4096, 999] {
                let rec = Recorder::start();
                let stream = streamed(&store, pattern, batch).await;
                let blocks = located_blocks(&rec);
                assert_eq!(
                    blocks.iter().sum::<u64>(),
                    40_000,
                    "batch {batch}: the run, once"
                );
                assert!(
                    blocks.iter().all(|&rows| rows <= batch as u64),
                    "batch {batch}"
                );
                assert_eq!(stream, paged, "batch {batch}");
            }
        }
    }
}

async fn check_locate_codes(tag: &str, fixture: &FixtureStore, quads: &[Quad], step: usize) {
    use crate::{CodePattern, Probe};
    let store = &fixture.store;
    let dict = store.dict_reader().unwrap();
    let all = row_list(store).await;
    let mut answered = std::collections::BTreeSet::new();
    for (k, quad) in all.iter().step_by(step).enumerate() {
        for mask in 1..16u8 {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            let pattern = CodePattern::new(slot(0), slot(1), slot(2), slot(3));
            let located = store.locate_codes(pattern, 0, None).await.unwrap();
            // A stream reads from the located run exactly when a window does:
            // checked for a declined pattern where a run can answer, a mapped
            // file, on its first eight quads, as the general match's stream
            // costs a scan.
            if located.is_none() && (!fixture.mapped() || k >= 8) {
                continue;
            }
            let rec = Recorder::start();
            let stream = streamed(store, pattern, 64).await;
            let blocks = located_blocks(&rec);
            drop(rec);
            let Some(full) = located else {
                assert!(
                    blocks.is_empty(),
                    "{tag} {pattern:?}: declined, yet streamed from a located run"
                );
                continue;
            };
            answered.insert(mask);
            let mut terms = Vec::new();
            for i in 0..4 {
                terms.push(match slot(i) {
                    Some(code) => Some(dict.decode(code).await.unwrap().unwrap()),
                    None => None,
                });
            }
            let probe = Probe::parse(
                terms[0].as_deref(),
                terms[1].as_deref(),
                terms[2].as_deref(),
                terms[3].as_deref(),
            )
            .unwrap();
            let view = store.run_probe(&probe).await.unwrap();
            let want = sorted_rows(&view).await;
            let tag = format!("{tag} {pattern:?}");
            assert_eq!(
                full.total,
                view.size().await.unwrap() as u64,
                "{tag}: total"
            );
            assert_eq!(
                full.total,
                full.columns[0].len() as u64,
                "{tag}: unwindowed rows"
            );
            let mut located: Vec<[TermCode; 4]> = code_rows(&full.columns);
            located.sort_unstable();
            assert_eq!(located, want, "{tag}: rows");
            // Streamed from the run: the lookup's rows in its order, in
            // chunks of 64, and of one and of three on a small match (a block
            // a row is a read a row).
            assert!(
                !blocks.is_empty(),
                "{tag}: not streamed from the located run"
            );
            assert_eq!(stream, code_rows::<4>(&full.columns), "{tag}: chunks of 64");
            if full.total <= 64 {
                for batch in [1usize, 3] {
                    assert_eq!(
                        streamed(store, pattern, batch).await,
                        code_rows::<4>(&full.columns),
                        "{tag}: chunks of {batch}"
                    );
                }
            }
            for page in [1usize, 3, 64] {
                let mut seen = Vec::new();
                let mut offset = 0;
                while offset < full.total as usize {
                    let got = store
                        .locate_codes(pattern, offset, Some(page))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(got.total, full.total, "{tag}: page total");
                    let n = got.columns[0].len();
                    assert_eq!(
                        n,
                        page.min(full.total as usize - offset),
                        "{tag}: page {page} at {offset}"
                    );
                    seen.extend(code_rows::<4>(&got.columns));
                    offset += page;
                }
                seen.sort_unstable();
                assert_eq!(seen, want, "{tag}: pages of {page}");
            }
        }
    }
    // Masks over (s, p, o, g) = bits 0..4. A mapped sorted file answers the
    // subject prefixes; the copy index also the posg and ospg ones (`s?o`
    // among them); the reference index every shape with `p` or `o` bound, the
    // other bound slots checked at its rows; the no-index file none of the
    // index-only shapes, and nothing answers `s??g`.
    if fixture.mapped() {
        for mask in [0b0001, 0b0011, 0b0111, 0b1111] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::Copy && fixture.mapped() {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::Reference && fixture.mapped() {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101, 0b1010, 0b1100, 0b1110] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::NoIndex {
        for mask in [0b0010, 0b0110, 0b0100] {
            assert!(
                !answered.contains(&mask),
                "{tag}: mask {mask:04b} has no index to answer it"
            );
        }
    }
    // A bound graph after a free slot is the longest bound prefix's run,
    // checked: `s??g` and `sp?g` on a mapped sorted file, and the shapes
    // of the copy index's orders too.
    if fixture.mapped() {
        for mask in [0b1001, 0b1011] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::Copy && fixture.mapped() {
        for mask in [0b1010, 0b1100, 0b1101, 0b1110] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    // A code no quad holds, past the dictionary, as a subject.
    let absent = CodePattern::new(Some(dict.len() as TermCode + 7), None, None, None);
    if let Some(got) = store.locate_codes(absent, 0, None).await.unwrap() {
        assert_eq!(got.total, 0, "{tag}: absent code");
    }
    // Narrowed and tombstoned views decline.
    let s = CodePattern::new(Some(all[0][0]), None, None, None);
    let window = store.window(1, 10).await.unwrap();
    assert!(
        window.locate_codes(s, 0, None).await.unwrap().is_none(),
        "{tag}: window"
    );
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert!(
        deleted.locate_codes(s, 0, None).await.unwrap().is_none(),
        "{tag}: tombstones"
    );
}
