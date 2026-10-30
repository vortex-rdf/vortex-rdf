//! The one-shot reads of a filtered no-index file view — its code columns,
//! `quads_vec` and `shared_quads_vec` — evaluate the filter's masks first and
//! read the rows they keep as `MaskedRead` chooses: over selections that
//! start and end inside a split, under tombstones and a tail, with the masks
//! evaluated inline or spawned, and outside a tokio runtime. Windows that cut
//! the filtered rows stop at the split that fills them, and the masks of a
//! read or a capped count follow the row-scan driver's rule.

use super::*;

/// The masked reads chosen on this thread since `before`, by kind.
fn masked_reads_since(before: [usize; 3]) -> [usize; 3] {
    let now = crate::store::scan::read_plan::masked_read_hooks::reads();
    std::array::from_fn(|kind| now[kind] - before[kind])
}

/// One masked read of kind `read`, as [`masked_reads_since`] counts them.
fn one_masked_read(read: crate::store::scan::read_plan::MaskedRead) -> [usize; 3] {
    let mut reads = [0; 3];
    reads[read as usize] = 1;
    reads
}

/// Each of [`masks_probes`] on `file` against `memory`: a view with a pending
/// filter whose one-shot code read chooses the probe's masked read once and
/// answers the rows, in order, of the view's scan (`row_chunks`) and of
/// `memory`'s view.
async fn assert_masks_first_reads(file: &VortexRdfStore, memory: &VortexRdfStore, tag: &str) {
    use crate::store::scan::read_plan::masked_read_hooks;
    for MasksProbe {
        shape, probe, read, ..
    } in masks_probes()
    {
        let label = format!("{tag} {shape}");
        let view = file.run_probe(&probe).await.unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: a pending filter"
        );
        let before = masked_read_hooks::reads();
        let rows = row_list(&view).await;
        assert_eq!(
            masked_reads_since(before),
            one_masked_read(read),
            "{label}: read as {read:?}"
        );
        assert!(!rows.is_empty(), "{label}: the pattern matches");
        assert_eq!(
            struct_chunk_codes(&collect_row_chunks(&view, 4_096).await),
            rows,
            "{label}: the scan's rows, in order"
        );
        assert_eq!(
            row_list(&memory.run_probe(&probe).await.unwrap()).await,
            rows,
            "{label}: the in-memory store's rows"
        );
    }
}

/// A filtered file view's one-shot reads against its scan and `memory`, an
/// in-memory view of the same rows: the code read chooses `read` once and
/// answers the scan's rows, in order, and `memory`'s; `quads_vec` answers the
/// stream's quads.
async fn assert_masked_reads_match(
    view: &VortexRdfStore,
    memory: &VortexRdfStore,
    read: crate::store::scan::read_plan::MaskedRead,
    label: &str,
) {
    let before = crate::store::scan::read_plan::masked_read_hooks::reads();
    let rows = row_list(view).await;
    assert_eq!(
        masked_reads_since(before),
        one_masked_read(read),
        "{label}: read as {read:?}"
    );
    assert!(!rows.is_empty(), "{label}: rows to read");
    assert_eq!(
        struct_chunk_codes(&collect_row_chunks(view, 4_096).await),
        rows,
        "{label}: the scan's rows, in order"
    );
    assert_eq!(
        row_list(memory).await,
        rows,
        "{label}: the in-memory store's rows"
    );
    let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
    assert_eq!(
        view.quads_vec().await.unwrap(),
        streamed,
        "{label}: quads_vec"
    );
}

/// 40,000 quads `s{i:05} p{i % 3}` in the default graph whose objects `"cp"`,
/// `"ct"` and `"cd"` lie in rows 10,000 to 30,000 alone: on every 1,000th row
/// there, every other 20th and every other even one (point reads, a take, a
/// projection). Every other row's object is `"o{i % 7}"`.
fn clustered_quads() -> Vec<Quad> {
    (0..40_000usize)
        .map(|i| {
            let o = match i {
                i if !(10_000..30_000).contains(&i) => format!("o{}", i % 7),
                i if i % 1_000 == 0 => "cp".to_string(),
                i if i % 20 == 0 => "ct".to_string(),
                i if i % 2 == 0 => "cd".to_string(),
                i => format!("o{}", i % 7),
            };
            make_quad(
                &format!("http://example.org/s{i:05}"),
                &format!("http://example.org/p{}", i % 3),
                &o,
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// The masks first over a selection that starts and ends inside a split: a
/// zone-map envelope inside a written file's splits (the clustered objects of
/// [`clustered_quads`]), and a located subject range across [`masks_file`]'s
/// sixteen splits. Every read kind answers the scan's rows and the in-memory
/// store's.
#[tokio::test]
async fn test_masks_first_reads_a_selection_clamped_inside_splits() {
    use crate::store::Keep;
    use crate::store::scan::read_plan::MaskedRead;

    let (_envelope_dir, path) =
        write_store_file(clustered_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let splits = open_mapped(&path).await.splits().unwrap();
    let inside = |row: u64| {
        splits
            .iter()
            .any(|split| split.start < row && row < split.end)
    };
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    for (object, read) in [
        ("cp", MaskedRead::Points),
        ("ct", MaskedRead::Take),
        ("cd", MaskedRead::Project),
    ] {
        let label = format!("envelope {object}");
        let probe = Probe::new(None, None, Some(plain_literal(object)), None);
        let view = file.run_probe(&probe).await.unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: a pending filter"
        );
        let range = view.debug_selection_range().expect("a zone-map envelope");
        assert!(
            inside(range.start) && inside(range.end),
            "{label}: {range:?} inside the splits {splits:?}"
        );
        let twin = memory.run_probe(&probe).await.unwrap();
        assert_masked_reads_match(&view, &twin, read, &label).await;
    }
    let (_masks_dir, path) = masks_file().await;
    let splits = open_mapped(&path).await.splits().unwrap();
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let dict = file.dict_reader().unwrap();
    let lo = dict
        .encode("<http://example.org/s01500>")
        .await
        .unwrap()
        .unwrap();
    let hi = dict
        .encode("<http://example.org/s13500>")
        .await
        .unwrap()
        .unwrap();
    let subjects = Keep::range(lo..hi);
    let inside = |row: u64| {
        splits
            .iter()
            .any(|split| split.start < row && row < split.end)
    };
    assert!(inside(1_500) && inside(13_500), "{splits:?}");
    for MasksProbe {
        shape, probe, read, ..
    } in masks_probes()
    {
        let label = format!("subject range {shape}");
        let view = file
            .run_probe(&probe)
            .await
            .unwrap()
            .keep(QuadColumn::S, &subjects)
            .await
            .unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: the filter still pending"
        );
        assert_eq!(
            view.debug_selection_range(),
            Some(1_500..13_500),
            "{label}: the located range"
        );
        let twin = memory
            .run_probe(&probe)
            .await
            .unwrap()
            .keep(QuadColumn::S, &subjects)
            .await
            .unwrap();
        assert_masked_reads_match(&view, &twin, read, &label).await;
    }
}

/// A filtered no-index file view's one-shot code read evaluates the filter's
/// masks first and reads the rows they keep as `MaskedRead` chooses: point by
/// point, by one take, or each split projected at its mask. Over sixteen
/// splits, one of them empty among the matched, the rows and their order are
/// the scan's and the in-memory store's.
#[tokio::test]
async fn test_masks_first_reads_each_kind() {
    let (_dir, path) = masks_file().await;
    assert_eq!(
        open_mapped(&path).await.splits().unwrap().len(),
        MASKS_SPLITS
    );
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert_masks_first_reads(&file, &memory, "file").await;
}

/// The masks first skip tombstoned rows: with some of every pattern's rows
/// deleted from the file store and the in-memory store alike, each pattern
/// still reads as its kind and answers the in-memory store's live rows.
#[tokio::test]
async fn test_masks_first_skips_tombstones() {
    let (_dir, path) = masks_file().await;
    let file = masks_deleted(&VortexRdfStore::from_file(&path).await.unwrap()).await;
    let memory = masks_deleted(&VortexRdfStore::from_file_in_memory(&path).await.unwrap()).await;
    assert!(file.view_statistics().unwrap().tombstones > 0);
    assert_masks_first_reads(&file, &memory, "tombstoned").await;
}

/// A filtered no-index file view's `quads_vec` and `shared_quads_vec` read
/// the filter's masks first: the quads, in order, are the `quads()` stream's,
/// the tail's after the base's, for each read kind, under a file-backed and
/// a resident dictionary, with the masks evaluated inline and, the split
/// limit forced to zero, spawned. Rows read point by point are read inline;
/// a take or a projection reads each split with a match in a task of its
/// own, spawned since there is more than one, and polled inline, one after
/// another, where no tokio runtime is current.
#[tokio::test]
async fn test_quads_vec_over_a_filtered_file_matches_quads() {
    use crate::store::scan::{
        file_scans::driver_hooks,
        read_plan::{MaskedRead, masked_read_hooks},
    };

    let (_file_backed_dir, file_backed) = masks_file().await;
    let (_resident_dir, resident) = masks_file_resident_dictionary().await;
    for (dictionary, path, file_backed) in [
        ("file-backed", &file_backed, true),
        ("resident", &resident, false),
    ] {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        assert_eq!(
            store.dict_reader().unwrap().is_file_backed(),
            file_backed,
            "{dictionary}"
        );
        let tailed = store.add_quads(masks_tail()).await.unwrap();
        for forced in [None, Some(0)] {
            let _limit = forced.map(driver_hooks::ForcedLimit::set);
            for MasksProbe {
                shape,
                probe,
                read,
                tail_rows,
                ..
            } in masks_probes()
            {
                let label = format!("{dictionary} {shape} limit {forced:?}");
                let view = tailed.run_probe(&probe).await.unwrap();
                let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
                assert!(
                    streamed[streamed.len() - tail_rows..].iter().all(|q| q
                        .subject
                        .to_string()
                        .starts_with("<http://example.org/tail")),
                    "{label}: the tail's rows last"
                );
                let before = masked_read_hooks::reads();
                driver_hooks::take_spawned();
                let quads = view.quads_vec().await.unwrap();
                let spawned = driver_hooks::take_spawned();
                assert_eq!(
                    masked_reads_since(before),
                    one_masked_read(read),
                    "{label}: read as {read:?}"
                );
                assert_eq!(quads, streamed, "{label}: the stream's quads, in order");
                // The masks spawn every split under the forced limit; a take
                // or a projection spawns each split with a match.
                let masks = if forced.is_some() { MASKS_SPLITS } else { 0 };
                let reads = match read {
                    MaskedRead::Points => 0,
                    MaskedRead::Take | MaskedRead::Project => MASKS_SPLITS - 1,
                };
                assert_eq!(spawned, masks + reads, "{label}: spawned splits");
                let shared = view.shared_quads_vec().await.unwrap();
                assert_eq!(
                    shared_tuple_rows(&shared),
                    tuple_rows(&streamed),
                    "{label}: shared_quads_vec"
                );
            }
        }
        // A bare executor on a plain thread: no runtime to spawn onto.
        for MasksProbe {
            shape, probe, read, ..
        } in masks_probes()
        {
            if read == MaskedRead::Points {
                continue;
            }
            let label = format!("{dictionary} {shape} outside a runtime");
            let view = tailed.run_probe(&probe).await.unwrap();
            let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
            let (quads, spawned) = std::thread::spawn(move || {
                futures::executor::block_on(async {
                    driver_hooks::take_spawned();
                    let quads = view.quads_vec().await.unwrap();
                    (quads, driver_hooks::take_spawned())
                })
            })
            .join()
            .expect("quads_vec must not panic without a tokio runtime");
            assert_eq!(spawned, 0, "{label}: nothing spawned");
            assert_eq!(quads, streamed, "{label}: the stream's quads, in order");
        }
    }
}

/// On no-index file stores, opened mapped and loaded whole, as written, with
/// tombstones and with a tail, each of `O`, `PO`, `P` and `G` reads the same
/// rows in base order plain and windowed to a limit that cannot bind: limits
/// between the match's rows and their bound (the window route) and at or
/// past the bound (the plain view loaded; mapped, whose reads take these
/// filters by their masks first, the window route too). They are the rows
/// the loaded open, an in-memory store, reads plain.
#[tokio::test]
async fn test_plain_and_unbound_limit_reads_return_the_same_rows() {
    let (_dir, path) = masks_file().await;
    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let variants = [
        ("as written", mapped.clone(), loaded.clone()),
        (
            "tombstoned",
            masks_deleted(&mapped).await,
            masks_deleted(&loaded).await,
        ),
        (
            "tailed",
            mapped.add_quads(masks_tail()).await.unwrap(),
            loaded.add_quads(masks_tail()).await.unwrap(),
        ),
    ];
    for (variant, mapped, loaded) in &variants {
        for MasksProbe { shape, probe, .. } in masks_probes() {
            let want = read_rows(&loaded.run_probe(&probe).await.unwrap()).await;
            for (open, store) in [("mapped", mapped), ("loaded", loaded)] {
                let label = format!("{variant} {open} {shape}");
                let plain = store.run_probe(&probe).await.unwrap();
                let n = plain.size().await.unwrap();
                let bound = plain.view_statistics().unwrap().rows.upper_bound;
                assert_eq!(read_rows(&plain).await, want, "{label}: plain");
                let mut limits = vec![bound, bound + 1, usize::MAX];
                if open == "mapped" {
                    assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
                    limits.extend([n + 1, n + (bound - n) / 2, bound - 1]);
                }
                for limit in limits {
                    let windowed = store
                        .run_probe(&probe.clone().window(0, Some(limit)))
                        .await
                        .unwrap();
                    assert_eq!(read_rows(&windowed).await, want, "{label} limit {limit}");
                }
            }
        }
    }
}

/// A window that cuts a filtered file view, a limit below its rows, is the
/// first `limit` matches in base order, read as exact ids with the filter
/// evaluated.
#[tokio::test]
async fn test_window_that_cuts_a_filter_keeps_base_order() {
    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for MasksProbe { shape, probe, .. } in masks_probes() {
        let rows = row_list(&store.run_probe(&probe).await.unwrap()).await;
        let n = rows.len();
        for limit in [1, n / 2, n - 1] {
            let label = format!("{shape}: limit {limit} of {n} rows");
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(limit)))
                .await
                .unwrap();
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: the filter evaluated");
            assert_eq!(stats.selection, SelectionKind::Ids, "{label}: exact ids");
            assert_eq!(stats.rows, RowCountHint::exact(limit), "{label}");
            assert_eq!(
                row_list(&windowed).await,
                rows[..limit],
                "{label}: the first matches"
            );
        }
    }
}

/// A window that cuts a filtered file view evaluates its splits in file order
/// and stops at the first that fills it. On a file of more splits than the
/// fold's lookahead it consumes exactly the splits up to that one, and starts
/// at most a lookahead's worth of splits past them: inline (the split limit
/// forced to the file's splits), evaluated as the fold polls them; spawned
/// (the limit forced to zero), counted as they are spawned, since a spawned
/// split may or may not run before its abort. The window's rows are the
/// first matches.
#[tokio::test]
async fn test_cutting_window_over_a_filter_stops_early() {
    use crate::io::read::available_parallelism;
    use crate::store::scan::file_filter::split_hooks;
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = many_splits_file().await;
    assert_eq!(
        open_mapped(&path).await.splits().unwrap().len(),
        MANY_SPLITS
    );
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = iri("p0");
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    let rows = row_list(&view).await;
    let limit = 64;
    // `p0` is on every third row: the splits whose matches reach the limit.
    let filled = (1..=MANY_SPLITS)
        .find(|&splits| (0..splits * MANY_SPLIT_ROWS).filter(|i| i % 3 == 0).count() >= limit)
        .unwrap();
    let reach = (filled + available_parallelism().max(2) - 1).min(MANY_SPLITS);
    for (forced, inline) in [(MANY_SPLITS, true), (0, false)] {
        let _limit = driver_hooks::ForcedLimit::set(forced);
        let (evaluated, consumed) = (split_hooks::evaluated(), split_hooks::consumed());
        driver_hooks::take_spawned();
        let windowed = view.window(0, limit).await.unwrap();
        let spawned = driver_hooks::take_spawned();
        let evaluated = split_hooks::evaluated() - evaluated;
        assert_eq!(
            split_hooks::consumed() - consumed,
            filled,
            "limit {forced}: the splits up to the one that fills the window"
        );
        if inline {
            assert_eq!(spawned, 0, "inline: nothing spawned");
            assert!(
                (filled..=reach).contains(&evaluated),
                "inline: {evaluated} splits evaluated, {filled} to {reach} expected"
            );
        } else {
            assert_eq!(spawned, reach, "spawned: a lookahead past the consumed");
        }
        assert!(filled < MANY_SPLITS, "the window stops early");
        assert_eq!(
            row_list(&windowed).await,
            rows[..limit],
            "limit {forced}: the first matches"
        );
    }
}

/// The masks of a one-shot read, and a capped count, evaluate their splits
/// under the row-scan driver: with the split limit forced below the file's
/// sixteen splits every split is spawned, at sixteen none. A projection's own
/// split reads follow the same rule over the fifteen splits with a match:
/// inline at fifteen, every one spawned at zero. The rows and counts agree
/// under every limit.
#[tokio::test]
async fn test_masks_driver_follows_the_row_scan_rule() {
    use crate::store::scan::{file_scans::driver_hooks, read_plan::MaskedRead};

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for MasksProbe {
        shape, probe, read, ..
    } in masks_probes()
    {
        let view = store.run_probe(&probe).await.unwrap();
        let mut answers = Vec::new();
        for limit in [MASKS_SPLITS - 1, MASKS_SPLITS, 0] {
            let label = format!("{shape} limit {limit}");
            let masks = if limit < MASKS_SPLITS {
                MASKS_SPLITS
            } else {
                0
            };
            let projection = if read == MaskedRead::Project && limit < MASKS_SPLITS - 1 {
                MASKS_SPLITS - 1
            } else {
                0
            };
            let _limit = driver_hooks::ForcedLimit::set(limit);
            driver_hooks::take_spawned();
            let rows = row_list(&view).await;
            assert_eq!(
                driver_hooks::take_spawned(),
                masks + projection,
                "{label}: the read's masks and split reads"
            );
            let count = view.size_capped(usize::MAX).await.unwrap();
            assert_eq!(
                driver_hooks::take_spawned(),
                masks,
                "{label}: the capped count's masks"
            );
            answers.push((rows, count));
        }
        for answer in &answers[1..] {
            assert_eq!(*answer, answers[0], "{shape}: every driver");
        }
        assert_eq!(answers[0].0.len(), answers[0].1, "{shape}: the count");
    }
}

/// Dropping a capped count whose masks are spawned (the split limit forced to
/// zero) aborts the split evaluations in flight: its first poll spawns a
/// lookahead of splits, which on this single-threaded runtime have not run
/// yet, and once the count is dropped they never run.
#[tokio::test]
async fn test_dropping_a_spawned_capped_count_aborts_its_splits() {
    use crate::io::read::available_parallelism;
    use crate::store::scan::file_filter::split_hooks;
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = many_splits_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = iri("p0");
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    let _limit = driver_hooks::ForcedLimit::set(0);
    driver_hooks::take_spawned();
    let evaluated = split_hooks::evaluated();
    {
        let mut count = std::pin::pin!(view.size_capped(usize::MAX));
        assert!(
            futures::poll!(count.as_mut()).is_pending(),
            "the spawned splits have not run"
        );
        assert_eq!(
            driver_hooks::take_spawned(),
            available_parallelism().clamp(2, MANY_SPLITS),
            "a lookahead of splits spawned"
        );
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        split_hooks::evaluated(),
        evaluated,
        "the dropped count's splits never ran"
    );
}

/// A read of the rows a filter's masks keep reads only the columns the
/// filter leaves free and fills the ones it pins with their codes, where the
/// read pins: every take and projection, and a point read of at least
/// [`PIN_POINT_MIN_ROWS`] rows. The one-shot reads (the code read and
/// `quads_vec`) of `O` (30 rows, point by point) read every column, of `PO`
/// (about 75, point by point) `s` and `g`, of `P` (taken) `s`, `o` and `g`,
/// of `G` (projected) `s`, `p` and `o`, of `pm` in the default graph (taken)
/// `s` and `o`, and of a window that keeps `P`'s masks as `P`'s. The streams
/// read a split at a time, so the 32 rows a split of `P` and of `pm` in the
/// default graph keep are read point by point, every column, as are `O`'s
/// and `PO`'s few; `G`'s 512 a split are projected, its pinned graph left
/// out. Every answer is the in-memory store's.
///
/// [`PIN_POINT_MIN_ROWS`]: crate::store::scan::file_reads::PIN_POINT_MIN_ROWS
#[tokio::test]
async fn test_masked_reads_skip_the_columns_the_filter_pins() {
    use crate::store::scan::file_reads::masked_column_hooks;

    /// Asserts `reads` holds at least one read and only reads of `want`.
    fn assert_reads(reads: Vec<Vec<String>>, want: &[&str], label: &str) {
        assert!(!reads.is_empty(), "{label}: reads");
        for columns in &reads {
            let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
            assert_eq!(columns, want, "{label}: the columns read");
        }
    }

    let (_dir, path) = masks_file().await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let pm = iri("pm");
    let p = Probe::new(None, Some(pm.clone()), None, None);
    let every: &[&str] = &["s", "p", "o", "g"];
    let all_rows = MASKS_SPLITS * MASKS_SPLIT_ROWS;
    for (shape, probe, one_shot, streamed) in [
        (
            "O",
            Probe::new(None, None, Some(plain_literal("sparse")), None),
            every,
            Some(every),
        ),
        (
            "PO",
            Probe::new(None, Some(pm.clone()), Some(plain_literal("o0")), None),
            ["s", "g"].as_slice(),
            Some(every),
        ),
        ("P", p.clone(), ["s", "o", "g"].as_slice(), Some(every)),
        (
            "G",
            Probe::new(None, None, None, Some(iri_graph("gd"))),
            ["s", "p", "o"].as_slice(),
            Some(["s", "p", "o"].as_slice()),
        ),
        (
            "PG",
            Probe::new(None, Some(pm), None, Some(GraphName::DefaultGraph)),
            ["s", "o"].as_slice(),
            Some(every),
        ),
        // The window's streams read its kept ids, with no filter to pin.
        (
            "P windowed",
            p.window(0, Some(all_rows)),
            ["s", "o", "g"].as_slice(),
            None,
        ),
    ] {
        let view = file.run_probe(&probe).await.unwrap();
        let in_memory = memory.run_probe(&probe).await.unwrap();
        let want = row_list(&in_memory).await;
        assert!(!want.is_empty(), "{shape}: the pattern matches");
        masked_column_hooks::take();
        assert_eq!(row_list(&view).await, want, "{shape}: the code read");
        assert_eq!(
            view.quads_vec().await.unwrap(),
            in_memory.quads_vec().await.unwrap(),
            "{shape}: quads_vec"
        );
        assert_reads(
            masked_column_hooks::take(),
            one_shot,
            &format!("{shape} one-shot"),
        );
        let (codes, _) = collect_code_chunks(&view, &QuadColumn::ALL, 4_096).await;
        assert_eq!(codes, want, "{shape}: the code chunks");
        assert_eq!(
            struct_chunk_codes(&collect_row_chunks(&view, 4_096).await),
            want,
            "{shape}: the row chunks"
        );
        let reads = masked_column_hooks::take();
        if let Some(streamed) = streamed {
            assert_reads(reads, streamed, &format!("{shape} streamed"));
        }
    }
}

/// A count, a delete and a `keep` on the object column evaluate their
/// pattern's filter over every split of the file, the splits spawned onto the
/// runtime's workers rather than all evaluated on the calling thread, and
/// answer as the in-memory store over the same quads does.
#[tokio::test]
async fn test_whole_file_filter_evaluations_spawn_their_splits() {
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = masks_file().await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(masks_quads(), LayoutStrategy::Dictionary, vec![]).await;
    for MasksProbe { shape, probe, .. } in masks_probes() {
        let (s, p, o, g) = (
            probe.subject.as_ref(),
            probe.predicate.as_ref(),
            probe.object.as_ref(),
            probe.graph.as_ref(),
        );
        driver_hooks::take_spawned();
        let count = file.count_probe(&probe).await.unwrap();
        assert!(driver_hooks::take_spawned() > 1, "{shape}: count spawns");
        assert_eq!(count, memory.count_probe(&probe).await.unwrap(), "{shape}");

        let view = matched_view(&file, s, p, o, g).await;
        let objects = object_keep(&file).await;
        driver_hooks::take_spawned();
        let kept = view.keep(QuadColumn::O, &objects).await.unwrap();
        assert!(driver_hooks::take_spawned() > 1, "{shape}: keep spawns");
        let memory_kept = matched_view(&memory, s, p, o, g)
            .await
            .keep(QuadColumn::O, &object_keep(&memory).await)
            .await
            .unwrap();
        let kept_rows = sorted_rows(&kept).await;
        assert!(!kept_rows.is_empty(), "{shape}: keep");
        assert_eq!(kept_rows, sorted_rows(&memory_kept).await, "{shape}: keep");

        let deleted = file.delete_matching(s, p, o, g).await.unwrap();
        assert!(driver_hooks::take_spawned() > 1, "{shape}: delete spawns");
        let kept = memory.delete_matching(s, p, o, g).await.unwrap();
        assert_eq!(
            deleted.size().await.unwrap(),
            kept.size().await.unwrap(),
            "{shape}"
        );
    }
}

/// A keep of the objects `"o0"`, `"o1"` and `"sparse"`, in `store`'s codes.
async fn object_keep(store: &VortexRdfStore) -> Keep {
    let dict = store.dict_reader().unwrap();
    let mut codes = Vec::new();
    for object in ["o0", "o1", "sparse"] {
        let term = plain_literal(object).to_string();
        codes.extend(dict.encode(&term).await.unwrap());
    }
    assert_eq!(codes.len(), 3, "every object has a code");
    Keep::set(codes)
}
