//! A served view's streams, one-shot reads and export read its rows off the
//! index, in the index's order: on every layout, in memory and on file, under
//! tombstones, over an encoded base, for a TypedObject view and for runs small
//! enough to read point by point. What does not depend on order reads as
//! before, and `sort_order` reports the order the reads follow.

use super::*;

/// Every stream of a served view reads its rows off the index, in its
/// order, once off the plan, and the one-shot read agrees: `?p2??` over the
/// run the index locates, in `index:posg`'s (o, s) order; `??o?` and `?po?`,
/// whose index order is base order, in base order. In memory and on file;
/// on file also `?p2?g` (the default graph, every row's) over the rows the
/// plan's filter keeps, in `index:posg`'s order.
#[tokio::test]
async fn streams_follow_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let (p2, o3) = (iri("p2"), plain_literal("object 3"));
        let mut views = vec![
            (
                "?p2??",
                matched_view(&store, None, Some(&p2), None, None).await,
                true,
            ),
            (
                "??o?",
                matched_view(&store, None, None, Some(&o3), None).await,
                false,
            ),
            (
                "?po?",
                matched_view(&store, None, Some(&p2), Some(&o3), None).await,
                false,
            ),
        ];
        // Only a file plan serves a run under a residual filter: an in-memory
        // one serves a view whose match is its sole restriction.
        if residency == "on file" {
            let default = GraphName::DefaultGraph;
            let view = matched_view(&store, None, Some(&p2), None, Some(&default)).await;
            views.push(("?p2?g", view, true));
        }
        for (name, view, object_free) in views {
            let tag = format!("{residency} {name}");
            assert!(served_component(&view).is_some(), "{tag}: served");
            let base = base_ordered_codes(&view).await;
            let want = if object_free {
                posg(base.clone())
            } else {
                base.clone()
            };
            if object_free {
                assert_eq!(served_component(&view), Some("index:posg"), "{tag}");
                assert_ne!(want, base, "{tag}: the orders differ");
            }
            for batch in [64, 65_536] {
                let rec = Recorder::start();
                assert_eq!(
                    collect_code_chunks(&view, &QuadColumn::ALL, batch).await.0,
                    want,
                    "{tag} at {batch}"
                );
                assert_eq!(served_streams(&rec), 1, "{tag} at {batch}: off the plan");
            }
            let objects: Vec<[TermCode; 1]> = want.iter().map(|r| [r[2]]).collect();
            assert_eq!(
                collect_code_chunks(&view, &[QuadColumn::O], 64).await.0,
                objects,
                "{tag}: o"
            );
            let rec = Recorder::start();
            assert_eq!(
                struct_chunk_codes(&collect_row_chunks(&view, 256).await),
                want,
                "{tag}: rows"
            );
            assert_eq!(served_streams(&rec), 1, "{tag}: rows off the plan");
            assert_eq!(row_list(&view).await, want, "{tag}: one-shot");
        }
    }
}

/// The same on a string layout: a served `?p??` view's row chunks and its
/// `quads_vec` follow (p, o, s, g) over the terms' N-Triples spellings.
#[tokio::test]
async fn memory_string_streams_follow_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Default).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let unserved = p.debug_unserved().await.unwrap().quads_vec().await.unwrap();
    let mut want: Vec<[String; 4]> = unserved.iter().map(spellings).collect();
    want.sort_by(|a, b| (&a[1], &a[2], &a[0], &a[3]).cmp(&(&b[1], &b[2], &b[0], &b[3])));
    let chunks = collect_row_chunks(&p, 256).await;
    let rows: Vec<(String, String)> = chunk_subjects(&chunks)
        .into_iter()
        .zip(chunk_strings(&chunks, "o"))
        .collect();
    let want_rows: Vec<(String, String)> =
        want.iter().map(|q| (q[0].clone(), q[2].clone())).collect();
    assert_eq!(rows, want_rows);
    let quads: Vec<[String; 4]> = p.quads_vec().await.unwrap().iter().map(spellings).collect();
    assert_eq!(quads, want);
}

/// The N-Quads export of a served view writes its rows in the order
/// `quads_vec` reads them, the index's, not base order. In memory and on
/// file.
#[tokio::test]
async fn export_follows_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
        let mut out = Vec::new();
        crate::export_rdf(p.clone(), &mut out, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        let exported: Vec<Quad> = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NQuads)
            .for_reader(out.as_slice())
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        let quads = p.quads_vec().await.unwrap();
        assert_eq!(exported, quads, "{residency}");
        let unserved = p.debug_unserved().await.unwrap().quads_vec().await.unwrap();
        assert_ne!(
            quads, unserved,
            "{residency}: the index's order is not base order"
        );
    }
}

/// On file too, a served view's streams drop its tombstoned rows and keep
/// the index's order, and a string layout's row chunks decode, in order, to
/// the quads `quads_vec` reads.
#[tokio::test]
async fn file_streams_under_tombstones_and_strings() {
    let (_dir, store) = file_store().await;
    let store = store
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    let (p, want) = p2_view(&store).await;
    assert!(want.len() < 20_000 / 7, "tombstones fall inside the run");
    assert_eq!(collect_code_chunks(&p, &QuadColumn::ALL, 100).await.0, want);
    assert_eq!(struct_chunk_codes(&collect_row_chunks(&p, 100).await), want);

    let (_dir, path) = write_store_file(
        modular_quads(2_000, 7, 11),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let mut decoded = Vec::new();
    let mut chunks = p.row_chunks(50).unwrap();
    while let Some(chunk) = chunks.next().await {
        decoded.extend(p.decode_row_chunk(&chunk.unwrap()).unwrap());
    }
    assert_eq!(decoded, p.shared_quads_vec().await.unwrap());
}

/// TypedObject's row chunks, in both residencies, read a served view in the
/// index's order: decoded, they are `quads_vec`'s rows in its order.
#[tokio::test]
async fn typed_object_row_chunks_follow_index_order() {
    let (_dir, path) = write_store_file(
        modular_quads(5_000, 7, 11),
        LayoutStrategy::TypedObject,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    for in_memory in [false, true] {
        let store = if in_memory {
            VortexRdfStore::from_file_in_memory(&path).await
        } else {
            VortexRdfStore::from_file(&path).await
        }
        .unwrap();
        let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
        assert_eq!(served_component(&p), Some("index:posg"));
        let mut decoded = Vec::new();
        let mut chunks = p.row_chunks(256).unwrap();
        while let Some(chunk) = chunks.next().await {
            decoded.extend(p.decode_row_chunk(&chunk.unwrap()).unwrap());
        }
        assert_eq!(
            decoded,
            p.shared_quads_vec().await.unwrap(),
            "in_memory={in_memory}"
        );
    }
}

/// An in-memory store read off a file keeps its columns encoded, so its
/// base cannot hand out code primitives, and a gather at a served run's ids,
/// in the index's order, would decode them out of order. Its served code and
/// row streams read each chunk off the index component instead (a slice of
/// the run, or a take at its live rows), chunks exactly `batch_rows` long but
/// the last, in the index's order, for a plain view, a window, a keep and
/// under tombstones. A built store, whose code columns its shared cache hands
/// out, keeps gathering the base.
#[tokio::test]
async fn encoded_memory_streams_read_off_the_component() {
    let component_rows = |rec: &Recorder| {
        rec.picked(|e| match e {
            Event::ServedComponentChunk(rows) => Some(*rows),
            _ => None,
        })
    };
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let p2 = iri("p2");
    let p = matched_view(&store, None, Some(&p2), None, None).await;
    let want = posg(base_ordered_codes(&p).await);
    let doomed: Vec<Quad> = p
        .quads_vec()
        .await
        .unwrap()
        .into_iter()
        .step_by(7)
        .collect();
    let tombstoned = delete_all(&store, &doomed).await;
    let t = matched_view(&tombstoned, None, Some(&p2), None, None).await;
    let t_want = posg(base_ordered_codes(&t).await);
    let window = p.window(100, 1_000).await.unwrap();
    let kept = p
        .keep(
            QuadColumn::O,
            &Keep::set(objects_of(&want).into_iter().step_by(2)),
        )
        .await
        .unwrap();
    let mut views = vec![
        ("plain", p.clone(), want.clone()),
        ("tombstoned", t, t_want),
        ("window", window, slice_of(&want, 100, 1_000)),
    ];
    let kept_want = row_list(&kept).await;
    views.push(("keep", kept, kept_want));
    for (tag, view, want) in views {
        assert!(served_component(&view).is_some(), "{tag}: served");
        let rec = Recorder::start();
        let (rows, sizes) = collect_code_chunks(&view, &QuadColumn::ALL, 64).await;
        assert_eq!(rows, want, "{tag}: code chunks in the index's order");
        assert!(
            sizes[..sizes.len() - 1].iter().all(|&n| n == 64),
            "{tag}: code chunks of exactly 64 rows but the last: {sizes:?}"
        );
        assert_eq!(
            component_rows(&rec),
            sizes,
            "{tag}: each code chunk off the component"
        );
        let rec = Recorder::start();
        let chunks = collect_row_chunks(&view, 256).await;
        assert_eq!(struct_chunk_codes(&chunks), want, "{tag}: row chunks");
        let lens: Vec<usize> = chunks.iter().map(|c| c.len()).collect();
        assert_eq!(
            component_rows(&rec),
            lens,
            "{tag}: each row chunk off the component"
        );
    }
    // A built store's code columns come from its shared cache: no chunk off
    // the component.
    let built = built_store(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let p = matched_view(&built, None, Some(&p2), None, None).await;
    let rec = Recorder::start();
    assert_eq!(
        collect_code_chunks(&p, &QuadColumn::ALL, 64).await.0,
        posg(base_ordered_codes(&p).await)
    );
    assert!(
        component_rows(&rec).is_empty(),
        "a built store gathers its base"
    );
}

/// Over a base whose code columns the shared cache cannot hand out (as a
/// chunked or wire-encoded one), a served view's code chunks read its rows
/// and its keeps test the serving component's column at the rows'
/// positions: both still in the index's order.
#[tokio::test]
async fn served_reads_without_the_code_cache() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let sparse = Keep::set(objects_of(&want).into_iter().step_by(3));
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    let _uncached = crate::store::test_hooks::NoCodeCache::set();
    assert_eq!(collect_code_chunks(&p, &QuadColumn::ALL, 64).await.0, want);
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    assert_eq!(served_component(&kept), Some("index:posg"));
    assert_eq!(
        collect_code_chunks(&kept, &QuadColumn::ALL, 64).await.0,
        filtered
    );
    assert_eq!(row_list(&kept).await, filtered);
}

/// What does not depend on order reads a narrowed served view as before:
/// its bytes, reopened, hold its rows.
#[tokio::test]
async fn order_free_consumers_unchanged() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, _) = p2_view(&store).await;
    let w = p.window(10, 100).await.unwrap();
    let reopened = VortexRdfStore::from_bytes(&w.to_bytes().await.unwrap())
        .await
        .unwrap();
    let mut want = quad_strings(&w.quads_vec().await.unwrap());
    want.sort();
    let mut got = quad_strings(&reopened.quads_vec().await.unwrap());
    got.sort();
    assert_eq!(got, want);
}

/// `view_statistics().sort_order` reports the order a served view's reads
/// follow: `Posg` for `?p2??`, whose object is unbound, and for its window,
/// keep, partitions and a pattern chained on its window; `Spog` where the
/// index's order is base order (`?p2 "object 3"`, `??"object 3"`); `None`
/// once tail rows follow the base's, in both residencies.
#[tokio::test]
async fn sort_order_reports_the_reads_order() {
    let (_memory_dir, memory) = memory_store(LayoutStrategy::Dictionary).await;
    let (_file_dir, file) = file_store().await;
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let order = |view: &VortexRdfStore| view.view_statistics().unwrap().sort_order;
    for (tag, store) in [("memory", memory), ("file", file)] {
        let p = matched_view(&store, None, Some(&p2), None, None).await;
        assert_eq!(served_component(&p), Some("index:posg"), "{tag}");
        assert_eq!(order(&p), Some(SortOrder::Posg), "{tag} ?p2??");
        let window = p.window(10, 100).await.unwrap();
        assert_eq!(order(&window), Some(SortOrder::Posg), "{tag}: its window");
        let objects = objects_of(&row_list(&p).await);
        let kept = p
            .keep(QuadColumn::O, &Keep::set(objects.into_iter().step_by(2)))
            .await
            .unwrap();
        assert_eq!(order(&kept), Some(SortOrder::Posg), "{tag}: its keep");
        let parts = p
            .partitions(std::num::NonZeroUsize::new(3).unwrap())
            .await
            .unwrap();
        for part in &parts {
            assert_eq!(order(part), Some(SortOrder::Posg), "{tag}: a partition");
        }
        let chained = window
            .match_pattern(None, None, None, Some(&GraphName::DefaultGraph))
            .await
            .unwrap();
        assert_eq!(
            order(&chained),
            Some(SortOrder::Posg),
            "{tag}: a chain on its window"
        );
        let po = matched_view(&store, None, Some(&p2), Some(&o3), None).await;
        assert_eq!(served_component(&po), Some("index:posg"), "{tag}");
        assert_eq!(order(&po), Some(SortOrder::Spog), "{tag} ?p2 o3");
        let o = matched_view(&store, None, None, Some(&o3), None).await;
        assert_eq!(served_component(&o), Some("index:ospg"), "{tag}");
        assert_eq!(order(&o), Some(SortOrder::Spog), "{tag} ??o3");
        let tailed = store
            .add_quads(vec![make_quad(
                "http://example.org/t0",
                "http://example.org/p2",
                "tail",
                GraphName::DefaultGraph,
            )])
            .await
            .unwrap();
        let p = matched_view(&tailed, None, Some(&p2), None, None).await;
        assert_eq!(served_component(&p), Some("index:posg"), "{tag}");
        assert_eq!(order(&p), None, "{tag}: tail rows follow");
    }
}

/// A point-sized served file run (`??o?`, `?po?`) reads its codes point by
/// point straight into code buffers, for the one-shot gather and the chunk
/// stream alike: the columns its terms pin are filled with their codes, not
/// read, and the row id is read only under tombstones. The codes are the
/// view's rows in its order (base order, for these shapes).
#[tokio::test]
async fn point_sized_served_runs_read_only_their_unpinned_codes() {
    let quads = modular_quads(2_000, 7, 11);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    // Every fourth quad of the object's.
    let doomed: Vec<Quad> = quads
        .iter()
        .filter(|quad| quad.object == o3)
        .step_by(4)
        .cloned()
        .collect();
    let tombstoned = delete_all(&store, &doomed).await;
    let codes_read = |rec: &Recorder| {
        rec.picked(|e| match e {
            Event::ServedCodesRead(columns) => Some(columns.clone()),
            _ => None,
        })
    };
    for (variant, store) in [("as written", store), ("tombstoned", tombstoned)] {
        for (shape, view, mut read) in [
            (
                "??o?",
                matched_view(&store, None, None, Some(&o3), None).await,
                vec!["s", "p", "g"],
            ),
            (
                "?po?",
                matched_view(&store, None, Some(&p2), Some(&o3), None).await,
                vec!["s", "g"],
            ),
        ] {
            let tag = format!("{variant} {shape}");
            if variant == "tombstoned" {
                read.push("rid");
            }
            assert!(served_component(&view).is_some(), "{tag}: served");
            let want = base_ordered_codes(&view).await;
            assert!(
                !want.is_empty() && want.len() <= 256,
                "{tag}: a point-sized run"
            );
            let rec = Recorder::start();
            assert_eq!(row_list(&view).await, want, "{tag}: gathered");
            assert_eq!(
                codes_read(&rec),
                vec![read.clone()],
                "{tag}: gathered reads"
            );
            // One read a batch of the run's rows, tombstoned ones included.
            let run = view.debug_serve_row_range().expect("a located run");
            let batches = ((run.end - run.start) as usize).div_ceil(64);
            let rec = Recorder::start();
            let (rows, _) = collect_code_chunks(&view, &QuadColumn::ALL, 64).await;
            assert_eq!(rows, want, "{tag}: chunks");
            assert_eq!(
                codes_read(&rec),
                vec![read.clone(); batches],
                "{tag}: chunk reads, one a batch"
            );
        }
    }
}
