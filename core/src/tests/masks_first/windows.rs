//! A window over a filtered file view that cuts no row evaluates the filter
//! and keeps what it found: the masks, where they are smaller than the
//! matches' ids, or the ids. Its rows and counts are exact, every view
//! derived from it starts without the masks, under tombstones it counts no
//! live rows, and the streams over it read the rows it kept by their ids,
//! with no filter.

use super::*;

/// A window over a filtered file view that finds fewer matches than its limit
/// cuts no base row: it reads the plain rows and knows its size and
/// statistics exactly, no filter left pending, without evaluating a split
/// again. Under tombstones its counts are the live rows'; under a tail longer
/// than the rest of the limit it reads the base's rows, then the tail's first
/// rows up to the limit. Under a string layout, whose filter compares strings
/// that no read by id checks, the window keeps the matches' ids, not masks.
#[tokio::test]
async fn test_window_that_finds_it_cuts_nothing_keeps_the_view() {
    use crate::store::scan::file_filter::split_hooks;

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let tombstoned = masks_deleted(&store).await;
    for (variant, store) in [("as written", &store), ("tombstoned", &tombstoned)] {
        for MasksProbe { shape, probe, .. } in masks_probes() {
            let label = format!("{variant} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let rows = row_list(&plain).await;
            let n = rows.len();
            let bound = plain.view_statistics().unwrap().rows.upper_bound;
            assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(n + 1)))
                .await
                .unwrap();
            let evaluated = split_hooks::evaluated();
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: no filter pending");
            assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
            assert_eq!(windowed.size().await.unwrap(), n, "{label}: size");
            assert_eq!(
                windowed.size_capped(n + 1).await.unwrap(),
                n,
                "{label}: capped"
            );
            assert_eq!(row_list(&windowed).await, rows, "{label}: the plain rows");
            assert_eq!(
                split_hooks::evaluated(),
                evaluated,
                "{label}: no split evaluated again"
            );
        }
    }
    let tailed = store.add_quads(masks_tail()).await.unwrap();
    for MasksProbe { shape, probe, .. } in masks_probes() {
        let label = format!("tailed {shape}");
        let quads = tailed
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        let base = quads
            .iter()
            .take_while(|q| {
                !q.subject
                    .to_string()
                    .starts_with("<http://example.org/tail")
            })
            .count();
        assert!(quads.len() - base > 2, "{label}: a tail past the limit");
        let limit = base + 2;
        let windowed = tailed
            .run_probe(&probe.clone().window(0, Some(limit)))
            .await
            .unwrap();
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{label}: no filter pending");
        assert_eq!(
            stats.rows,
            RowCountHint::exact(limit),
            "{label}: exact rows"
        );
        assert_eq!(windowed.size().await.unwrap(), limit, "{label}: size");
        assert_eq!(
            windowed.quads_vec().await.unwrap(),
            quads[..limit],
            "{label}: the base's rows, then the tail's first"
        );
    }
    // The fixture's graph `g1`, on every third row: dense enough for masks.
    let g1 = iri_graph("g1");
    let probe = Probe::new(None, None, None, Some(g1));
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let label = format!("{layout:?} G");
        let (_dir, path) = write_store_file(fixture_quads(), layout, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let plain = store.run_probe(&probe).await.unwrap();
        let quads = plain.quads_vec().await.unwrap();
        let n = quads.len();
        let stats = plain.view_statistics().unwrap();
        assert!(stats.pending_filter, "{label}: a pending filter");
        let bound = stats.rows.upper_bound;
        assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
        let windowed = store
            .run_probe(&probe.clone().window(0, Some(n + 1)))
            .await
            .unwrap();
        assert_eq!(windowed.debug_evaluated_len(), None, "{label}: no masks");
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{label}: the filter evaluated");
        assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
        assert_eq!(
            windowed.quads_vec().await.unwrap(),
            quads,
            "{label}: the plain rows"
        );
    }
}

/// On a Dictionary file one of whose primary columns has no chunk probes (the
/// object column written dictionary-encoded with its values under a chunked
/// node, a shape the probes decline), the reads by id cannot check a filter
/// of code equalities, so a filtered view's reads scan, and a window that
/// finds fewer matches than its limit keeps the matches' ids, not the
/// filter's masks: the rows an in-memory open reads, known exactly, no filter
/// pending.
#[tokio::test]
async fn test_window_over_a_column_without_chunk_probes_keeps_ids() {
    let (_dir, path) =
        write_chunked_store(&masks_quads(), MASKS_SPLIT_ROWS, false, false, Some("o")).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let twin = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert_eq!(store.debug_quad_column_chunks("o"), None, "o: no probes");
    assert!(store.debug_quad_column_chunks("p").is_some(), "p: probes");
    let dense = masks_probes().into_iter().filter(|p| p.dense);
    for MasksProbe { shape, probe, .. } in dense {
        let plain = store.run_probe(&probe).await.unwrap();
        assert!(
            plain.view_statistics().unwrap().pending_filter,
            "{shape}: a pending filter"
        );
        assert!(!plain.debug_reads_masks_first(), "{shape}: no masks first");
        let rows = row_list(&twin.run_probe(&probe).await.unwrap()).await;
        assert_eq!(row_list(&plain).await, rows, "{shape}: the twin's rows");
        let n = rows.len();
        let windowed = store
            .run_probe(&probe.clone().window(0, Some(n + 1)))
            .await
            .unwrap();
        assert_eq!(windowed.debug_evaluated_len(), None, "{shape}: no masks");
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{shape}: the filter evaluated");
        assert_eq!(stats.selection, SelectionKind::Ids, "{shape}: the ids");
        assert_eq!(stats.rows, RowCountHint::exact(n), "{shape}: exact rows");
        assert_eq!(row_list(&windowed).await, rows, "{shape}: the plain rows");
    }
}

/// A window that cuts no row of a filtered file view keeps the filter's masks
/// for its reads where they are smaller than the matches' ids (a bit a row of
/// each split a mask keeps some rows of, against 64 a match), and the ids
/// otherwise: the dense predicate and graph keep their masks, while the sparse
/// object and predicate-object pair, spread over the splits, keep their ids
/// and drop the filter. Either way the view knows its rows exactly and reads
/// the plain read's rows in base order, with and without a tail.
#[tokio::test]
async fn test_window_keeps_masks_only_when_smaller_than_ids() {
    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let tailed = store.add_quads(masks_tail()).await.unwrap();
    for (variant, store) in [("plain", &store), ("tailed", &tailed)] {
        for MasksProbe {
            shape,
            probe,
            dense,
            ..
        } in masks_probes()
        {
            let label = format!("{variant} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let want = read_rows(&plain).await;
            let n = plain.size().await.unwrap();
            let tail = plain.view_statistics().unwrap().tail_rows;
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(n + 1)))
                .await
                .unwrap();
            assert_eq!(
                windowed.debug_evaluated_len(),
                dense.then_some(n - tail),
                "{label}: masks kept only where smaller than the ids"
            );
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: no filter pending");
            assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
            assert_eq!(read_rows(&windowed).await, want, "{label}: the plain rows");
        }
    }
}

/// The masks a window keeps belong to its view alone: every view derived from
/// it (a located subject keep, a streamed one, an object keep, a chained
/// match, windows that cut or skip, pieces, an owned copy) starts without
/// them and answers what the same derivation of an in-memory twin answers.
/// An owner's later delete leaves the view's own rows as they were, and the
/// probe run again sees it, as the twin's owner does.
#[tokio::test]
async fn test_kept_masks_never_outlive_a_derivation() {
    use crate::store::Keep;
    use std::num::NonZeroUsize;

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let dict = store.dict_reader().unwrap();
    let o0 = dict.encode("\"o0\"").await.unwrap().unwrap();
    let o5 = dict.encode("\"o5\"").await.unwrap().unwrap();
    let (s_lo, s_hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
    let o0_term = plain_literal("o0");
    let dense = masks_probes().into_iter().filter(|p| p.dense);
    for MasksProbe { shape, probe, .. } in dense {
        let n = store.run_probe(&probe).await.unwrap().size().await.unwrap();
        let windowed = probe.clone().window(0, Some(n + 1));
        let view = store.run_probe(&windowed).await.unwrap();
        let twin = memory.run_probe(&windowed).await.unwrap();
        assert_eq!(view.debug_evaluated_len(), Some(n), "{shape}: kept masks");
        let rows = row_list(&twin).await;
        assert_eq!(row_list(&view).await, rows, "{shape}: the twin's rows");
        // A subject set of one code per sixteen of the view's rows is
        // located: the rule counts a set's codes against the selection's
        // rows, here every row of the file. One with the codes of two in
        // every three of the store's subjects, odd and even rows' alike, more
        // than one per sixteen of those rows, is streamed.
        let located = Keep::set(rows.iter().step_by(16).map(|row| row[0]));
        let streamed = Keep::set((s_lo..s_hi).filter(|code| (code - s_lo) % 3 != 0));
        let objects = Keep::set([o0, o5]);
        let streams = || view.debug_column_streams().unwrap();
        let before = streams();
        let located_view = view.keep(QuadColumn::S, &located).await.unwrap();
        assert_eq!(streams(), before, "{shape}: the sparse subject set located");
        let streamed_view = view.keep(QuadColumn::S, &streamed).await.unwrap();
        assert!(
            streams() > before,
            "{shape}: the dense subject set streamed"
        );
        let derived = [
            (
                "located subject keep",
                located_view,
                twin.keep(QuadColumn::S, &located).await.unwrap(),
            ),
            (
                "streamed subject keep",
                streamed_view,
                twin.keep(QuadColumn::S, &streamed).await.unwrap(),
            ),
            (
                "object keep",
                view.keep(QuadColumn::O, &objects).await.unwrap(),
                twin.keep(QuadColumn::O, &objects).await.unwrap(),
            ),
            (
                "chained match",
                matched_view(&view, None, None, Some(&o0_term), None).await,
                matched_view(&twin, None, None, Some(&o0_term), None).await,
            ),
            (
                "cutting window",
                view.window(0, n / 2).await.unwrap(),
                twin.window(0, n / 2).await.unwrap(),
            ),
            (
                "skipping window",
                view.window(1, usize::MAX).await.unwrap(),
                twin.window(1, usize::MAX).await.unwrap(),
            ),
        ];
        for (tag, derived, twin_derived) in &derived {
            let label = format!("{shape} {tag}");
            let want = row_list(twin_derived).await;
            assert!(!want.is_empty(), "{label}: rows to check");
            assert_eq!(derived.debug_evaluated_len(), None, "{label}: no masks");
            assert_eq!(row_list(derived).await, want, "{label}: the twin's rows");
            assert_eq!(derived.size().await.unwrap(), want.len(), "{label}: size");
        }
        let pieces = view
            .partitions(NonZeroUsize::new(3).unwrap())
            .await
            .unwrap();
        assert!(pieces.len() > 1, "{shape}: pieces");
        let mut pieced = Vec::new();
        for piece in &pieces {
            assert_eq!(piece.debug_evaluated_len(), None, "{shape}: a piece");
            pieced.extend(row_list(piece).await);
        }
        assert_eq!(pieced, rows, "{shape}: the pieces' rows");
        let quads = twin.quads_vec().await.unwrap();
        let owned = view.owned().await.unwrap();
        assert_eq!(owned.debug_evaluated_len(), None, "{shape}: the owned copy");
        assert_eq!(
            owned.quads_vec().await.unwrap(),
            quads,
            "{shape}: owned quads"
        );
        // The store, the view's owner, deletes three of the view's rows, and
        // so does the twin's.
        let owner = delete_all(&store, &quads[..3]).await;
        let twin_owner = delete_all(&memory, &quads[..3]).await;
        assert_eq!(row_list(&view).await, rows, "{shape}: the view's own rows");
        let again = owner.run_probe(&windowed).await.unwrap();
        let twin_again = twin_owner.run_probe(&windowed).await.unwrap();
        let want = twin_again.quads_vec().await.unwrap();
        assert_eq!(want.len(), quads.len() - 3, "{shape}: three rows deleted");
        assert_eq!(
            again.quads_vec().await.unwrap(),
            want,
            "{shape}: the probe after the delete"
        );
    }
}

/// Under tombstones, a window over a file view whose reads take its pending
/// filter by the masks first counts none of the view's live rows: its window
/// evaluates the filter whatever the bound, so a count for the bound would
/// go unused. The window still reads the plain rows, known exactly.
#[tokio::test]
async fn test_masks_first_window_under_tombstones_counts_no_live_rows() {
    use crate::store::view::selection::live_count_hooks;

    let quads = fixture_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = delete_all(
        &VortexRdfStore::from_file(&path).await.unwrap(),
        quads.iter().step_by(11).take(40),
    )
    .await;
    let p1 = iri("p1");
    let probe = Probe::new(None, Some(p1), None, None);
    let plain = store.run_probe(&probe).await.unwrap();
    assert!(
        plain.debug_reads_masks_first(),
        "read by the filter's masks first"
    );
    let rows = row_list(&plain).await;
    let n = rows.len();
    // A limit short of the selected rows: the bound would count the live
    // rows.
    assert!(
        n + 1 < quads.len(),
        "{n} rows under a base of {}",
        quads.len()
    );
    let counts = live_count_hooks::counts();
    let windowed = store
        .run_probe(&probe.window(0, Some(n + 1)))
        .await
        .unwrap();
    assert_eq!(live_count_hooks::counts(), counts, "no live rows counted");
    let stats = windowed.view_statistics().unwrap();
    assert!(!stats.pending_filter, "the filter evaluated");
    assert_eq!(stats.rows, RowCountHint::exact(n), "exact rows");
    assert_eq!(row_list(&windowed).await, rows, "the plain rows");
}

// ─── The streams over a window that kept what it found ──────────────────

/// The read hooks' counters on this thread: split masks evaluated, masked
/// reads by kind, restricted scans built with a filter, row scans, and takes
/// through the chunk probes.
#[derive(Clone, Copy)]
struct ReadCounters {
    evaluated: usize,
    masked: [usize; 3],
    filtered_scans: usize,
    scans: usize,
    takes: usize,
}

impl ReadCounters {
    fn now() -> Self {
        use crate::store::read::rows::restricted_scan_hooks;
        use crate::store::scan::file_filter::split_hooks;
        use crate::store::scan::{
            file_reads::take_hooks, file_scans::scan_hooks, read_plan::masked_read_hooks,
        };
        Self {
            evaluated: split_hooks::evaluated(),
            masked: masked_read_hooks::reads(),
            filtered_scans: restricted_scan_hooks::filtered(),
            scans: scan_hooks::scans(),
            takes: take_hooks::takes().0,
        }
    }
}

/// Each of `view`'s streams drained in turn: `code_chunks` and `row_chunks`
/// at 4,096 rows a chunk answer `codes`, and `quads()` answers `quads`. After each, `check` is handed the stream's name
/// and the read counters from just before it.
async fn assert_streams_answer(
    view: &VortexRdfStore,
    codes: &[[TermCode; 4]],
    quads: &[Quad],
    label: &str,
    check: impl Fn(&str, ReadCounters),
) {
    let before = ReadCounters::now();
    let (streamed, _) = collect_code_chunks(view, &QuadColumn::ALL, 4_096).await;
    assert_eq!(streamed, codes, "{label}: code_chunks");
    check("code_chunks", before);
    let before = ReadCounters::now();
    assert_eq!(
        struct_chunk_codes(&collect_row_chunks(view, 4_096).await),
        codes,
        "{label}: row_chunks"
    );
    check("row_chunks", before);
    let before = ReadCounters::now();
    let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
    assert_eq!(streamed, quads, "{label}: quads()");
    check("quads()", before);
}

/// The streams over a view whose window kept its filter's masks (`code_chunks`,
/// `row_chunks` and `quads()`) read the masks' ids as
/// exact ids with no filter: the plain view's rows, each reader evaluating no
/// split of the filter, making no masked read and building no scan with the
/// filter, for a take's density (the ids taken, no row scan) and a
/// projection's.
#[tokio::test]
async fn test_stream_over_a_kept_masks_view_reads_its_ids() {
    use crate::store::scan::read_plan::MaskedRead;

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let dense = masks_probes().into_iter().filter(|p| p.dense);
    for MasksProbe {
        shape, probe, read, ..
    } in dense
    {
        let plain = store.run_probe(&probe).await.unwrap();
        let (codes, _) = collect_code_chunks(&plain, &QuadColumn::ALL, 4_096).await;
        let quads: Vec<Quad> = plain.quads().try_collect().await.unwrap();
        let n = codes.len();
        let kept = store
            .run_probe(&probe.clone().window(0, Some(n + 1)))
            .await
            .unwrap();
        assert_eq!(kept.debug_evaluated_len(), Some(n), "{shape}: kept masks");
        // What a reader did since `before`.
        let check = |reader: &str, before: ReadCounters| {
            let label = format!("{shape} {reader}");
            let after = ReadCounters::now();
            assert_eq!(
                after.evaluated, before.evaluated,
                "{label}: no split evaluated"
            );
            assert_eq!(after.masked, before.masked, "{label}: no masked read");
            assert_eq!(
                after.filtered_scans, before.filtered_scans,
                "{label}: no scan with the filter"
            );
            if read == MaskedRead::Take {
                assert_eq!(after.scans, before.scans, "{label}: no row scan");
                assert!(after.takes > before.takes, "{label}: the ids taken");
            }
        };
        assert_streams_answer(&kept, &codes, &quads, shape, check).await;
    }
}

/// A window whose limit fits the bound of a filtered no-index file view
/// whose reads take the filter by its masks first (no offset, a limit of the
/// bound or more) evaluates the filter: for `O`, `PO` and `P`, its one-shot
/// reads return the plain view's rows, evaluating no split again and
/// building no scan with the filter, its statistics are exact with no filter
/// pending, and each stream (`code_chunks`, `row_chunks`, `quads()` and the
/// data source's scan) reads the matches' ids, where the plain view's streams
/// scan with the filter.
#[tokio::test]
async fn test_fitting_window_over_a_masks_first_view_reads_ids() {
    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let shapes = masks_probes()
        .into_iter()
        .filter(|p| matches!(p.shape, "O" | "PO" | "P"));
    for MasksProbe { shape, probe, .. } in shapes {
        let plain = store.run_probe(&probe).await.unwrap();
        assert!(plain.debug_reads_masks_first(), "{shape}: masks first");
        let stats = plain.view_statistics().unwrap();
        assert!(stats.pending_filter, "{shape}: the plain view's filter");
        let codes = row_list(&plain).await;
        let quads = plain.quads_vec().await.unwrap();
        let n = codes.len();
        let bound = stats.rows.upper_bound;
        assert!(n < bound, "{shape}: {n} rows under a bound of {bound}");
        // Each view beside its label and whether its streams scan with the
        // filter.
        let mut views = vec![(format!("{shape} plain"), plain, true)];
        for limit in [bound, usize::MAX] {
            let label = format!("{shape} limit {limit}");
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(limit)))
                .await
                .unwrap();
            let got = windowed.view_statistics().unwrap();
            assert!(!got.pending_filter, "{label}: evaluated");
            assert_eq!(got.rows, RowCountHint::exact(n), "{label}: exact rows");
            let before = ReadCounters::now();
            assert_eq!(row_list(&windowed).await, codes, "{label}: the plain rows");
            assert_eq!(
                windowed.quads_vec().await.unwrap(),
                quads,
                "{label}: the plain quads"
            );
            let after = ReadCounters::now();
            assert_eq!(
                after.evaluated, before.evaluated,
                "{label}: one-shot reads evaluate no split"
            );
            assert_eq!(
                after.filtered_scans, before.filtered_scans,
                "{label}: one-shot reads build no scan with the filter"
            );
            views.push((label, windowed, false));
        }
        for (label, view, scans_filter) in &views {
            // What a reader did since `before`.
            let check = |reader: &str, before: ReadCounters| {
                let label = format!("{label} {reader}");
                let after = ReadCounters::now();
                if *scans_filter {
                    assert!(
                        after.filtered_scans > before.filtered_scans,
                        "{label}: a scan with the filter"
                    );
                    return;
                }
                assert_eq!(
                    after.filtered_scans, before.filtered_scans,
                    "{label}: no scan with the filter"
                );
                assert_eq!(
                    after.evaluated, before.evaluated,
                    "{label}: no split evaluated"
                );
                assert_eq!(after.masked, before.masked, "{label}: no masked read");
            };
            assert_streams_answer(view, &codes, &quads, label, check).await;
        }
    }
}
