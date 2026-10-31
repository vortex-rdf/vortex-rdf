use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::error::VortexRdfError;
use crate::store::RawQuad;
use crate::store::builders::interner::{FrozenTerms, TermInterner};
use crate::store::builders::spill::TempRunsGuard;

/// Twelve distinct quads: every subject differs, objects include a
/// non-ASCII literal, graphs alternate with the default graph.
fn distinct_quads() -> Vec<RawQuad> {
    (0..12u32)
        .map(|i| RawQuad {
            s: format!("<http://e.org/s{:02}>", (i * 7) % 12),
            p: format!("<http://e.org/p{}>", i % 3),
            o: if i % 4 == 0 {
                "\"é\"".to_owned()
            } else {
                format!("\"o{}\"", i % 4)
            },
            g: if i % 2 == 0 {
                String::new()
            } else {
                "<http://e.org/g>".to_owned()
            },
        })
        .collect()
}

/// The distinct quads in reverse, with eight repeats spread through them:
/// 20 pushes, some repeats inside one run of 3 and some across runs.
fn pushes() -> Vec<RawQuad> {
    let distinct = distinct_quads();
    let mut out: Vec<RawQuad> = distinct.iter().rev().cloned().collect();
    for i in [3, 0, 11, 5, 3, 7, 0, 9] {
        let at = (i * 5) % out.len();
        out.insert(at, distinct[i].clone());
    }
    out
}

/// `quads` interned and spilled in runs of `capacity` under `dir`, not
/// yet ranked, beside the build's frozen terms.
fn spilled(dir: &Path, quads: &[RawQuad], capacity: usize) -> (SpilledQuads, FrozenTerms) {
    let mut terms = TermInterner::new();
    let mut spiller = QuadSpiller::new(dir, capacity);
    for quad in quads {
        let ids = terms.intern_quad(quad.clone()).unwrap();
        spiller.push(ids, &terms).unwrap();
    }
    let spilled = spiller.finish(&terms).unwrap();
    (spilled, terms.finish())
}

/// [`spilled`], ranked by the build's own table.
fn spill(dir: &Path, quads: &[RawQuad], capacity: usize) -> (QuadRuns, FrozenTerms) {
    let (spilled, frozen) = spilled(dir, quads, capacity);
    let runs = spilled.ranked(Arc::new(frozen.rank_of.clone())).unwrap();
    (runs, frozen)
}

/// A quad's four terms, comparable and printable (`RawQuad` is neither
/// `Debug` nor meant to be).
type Row = (String, String, String, String);

/// Rank quads as the terms they name.
fn decode(frozen: &FrozenTerms, ranks: &[[u32; 4]]) -> Vec<Row> {
    let term = |rank: u32| frozen.sorted[rank as usize].to_string();
    ranks
        .iter()
        .map(|&[s, p, o, g]| (term(s), term(p), term(o), term(g)))
        .collect()
}

/// `quads` sorted with each once, as rows: `RawQuad`'s order is its
/// fields' tuple order.
fn sorted_distinct(quads: Vec<RawQuad>) -> Vec<Row> {
    quads
        .into_iter()
        .map(|q| (q.s, q.p, q.o, q.g))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn file_len(dir: &Path, name: &str) -> u64 {
    std::fs::metadata(dir.join(name)).unwrap().len()
}

#[test]
fn quad_runs_merge_in_rank_order_and_drop_repeats() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let (runs, frozen) = spill(guard.path(), &pushes(), 3);
    assert!(runs.run_count() > 1, "{} runs", runs.run_count());
    let merged = runs.merger().unwrap().next_batch(usize::MAX).unwrap();
    assert_eq!(decode(&frozen, &merged), sorted_distinct(distinct_quads()));
}

/// Quads over terms whose byte order a sloppy comparison gets wrong (one
/// a prefix of another, the empty default graph, a non-ASCII byte),
/// pushed in an order of their own with repeats, sort and dedup as
/// `RawQuad`s do, whether they fill one run (sorted by the build's
/// ranks) or spill runs of 100 (their terms ranked among themselves, the
/// second run's lowest term ranked third in the first), 13 or one
/// (compared term by term).
#[test]
fn quad_runs_sort_as_their_terms_whatever_the_runs() {
    const POOL: [&str; 7] = ["", "<a>", "<ab>", "<b>", "_:b0", "\"z\"", "\"é\""];
    let quads: Vec<RawQuad> = (0..200usize)
        .map(|i| {
            let pool = if i < 100 { &POOL[..] } else { &POOL[3..] };
            let term = |k: usize| pool[k % pool.len()].to_owned();
            RawQuad {
                s: term(i * 7 + 3),
                p: term(i * 5 + 1),
                o: term(i * 3),
                g: term(i),
            }
        })
        .collect();
    for capacity in [usize::MAX, 100, 13, 1] {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let (runs, frozen) = spill(guard.path(), &quads, capacity);
        let merged = runs.merger().unwrap().next_batch(usize::MAX).unwrap();
        assert_eq!(
            decode(&frozen, &merged),
            sorted_distinct(quads.clone()),
            "runs of {capacity}"
        );
    }
}

#[test]
fn quad_runs_open_twice_yield_the_same_sequence() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let (runs, _) = spill(guard.path(), &pushes(), 3);
    let first = runs.merger().unwrap().next_batch(usize::MAX).unwrap();
    let second = runs.merger().unwrap().next_batch(usize::MAX).unwrap();
    assert_eq!(first.len(), 12);
    assert_eq!(first, second);
}

#[test]
fn a_dataset_within_one_run_never_touches_the_disk() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let quads = &pushes()[..10];
    let (runs, frozen) = spill(guard.path(), quads, 100);
    assert_eq!(runs.run_count(), 1);
    assert_eq!(std::fs::read_dir(guard.path()).unwrap().count(), 0);
    let merged = runs.merger().unwrap().next_batch(usize::MAX).unwrap();
    assert_eq!(decode(&frozen, &merged), sorted_distinct(quads.to_vec()));
}

/// A run holds each of its quads once, however it was sorted: two
/// quads pushed twenty times spill a run of two (their eight terms
/// ranked among themselves), pushed four times too (compared term by
/// term), and the in-memory run of a build that spills nothing holds
/// them once as well.
#[test]
fn a_run_holds_each_of_its_quads_once() {
    let two = &distinct_quads()[..2];
    for pushes in [20, 4] {
        let guard = TempRunsGuard::create(None, None).unwrap();
        let quads: Vec<RawQuad> = two.iter().cycle().take(pushes + 1).cloned().collect();
        let (runs, _) = spill(guard.path(), &quads, pushes);
        assert_eq!(runs.run_count(), 2, "{pushes} pushes");
        assert_eq!(
            file_len(guard.path(), "quads_run_0.bin"),
            32,
            "{pushes} pushes"
        );
    }
    let guard = TempRunsGuard::create(None, None).unwrap();
    let quads: Vec<RawQuad> = two.iter().cycle().take(20).cloned().collect();
    let (runs, _) = spill(guard.path(), &quads, 100);
    assert_eq!(runs.memory_len(), Some(2));
}

#[test]
fn quad_run_files_hold_sixteen_bytes_per_quad() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let (runs, _) = spill(guard.path(), &distinct_quads()[..10], 3);
    assert_eq!(runs.run_count(), 4);
    let lens: Vec<u64> = (0..4)
        .map(|n| file_len(guard.path(), &format!("quads_run_{n}.bin")))
        .collect();
    assert_eq!(lens, [48, 48, 48, 16]);
}

#[test]
fn quad_run_id_past_the_table_is_a_deserialization_error() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let (spilled, frozen) = spilled(guard.path(), &distinct_quads(), 3);
    let short = Arc::new(frozen.rank_of[..2].to_vec());
    let read = spilled
        .ranked(short)
        .and_then(|runs| runs.merger())
        .and_then(|mut merger| merger.next_batch(usize::MAX));
    assert!(
        matches!(read, Err(VortexRdfError::Deserialization(_))),
        "{read:?}"
    );
}

#[test]
fn entry_runs_round_trip_row_ids_past_u32() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let base = u64::from(u32::MAX) + 10;

    let refs: Vec<RefEntry> = (0..10u64)
        .map(|i| (((i * 7) % 10) as u32, base + i))
        .collect();
    let mut spiller = EntrySpiller::<RefEntry>::new(guard.path(), "idx_o", 3).unwrap();
    for entry in &refs {
        spiller.push(*entry).unwrap();
    }
    let merged = spiller
        .into_merger()
        .unwrap()
        .next_batch(usize::MAX)
        .unwrap();
    let mut want = refs.clone();
    want.sort_unstable();
    assert_eq!(merged, want);
    let lens: Vec<u64> = (0..4)
        .map(|n| file_len(guard.path(), &format!("idx_o_run_{n}.bin")))
        .collect();
    assert_eq!(lens, [32, 32, 32, 16]);

    let copies: Vec<CopyEntry> = (0..10u64)
        .map(|i| {
            let key = [
                ((i * 3) % 10) as u32,
                (i % 2) as u32,
                ((i * 7) % 10) as u32,
                0,
            ];
            (key, base + i)
        })
        .collect();
    let mut spiller = EntrySpiller::<CopyEntry>::new(guard.path(), "idx_posg", 3).unwrap();
    for entry in &copies {
        spiller.push(*entry).unwrap();
    }
    let merged = spiller
        .into_merger()
        .unwrap()
        .next_batch(usize::MAX)
        .unwrap();
    let mut want = copies.clone();
    want.sort_unstable();
    assert_eq!(merged, want);
    let lens: Vec<u64> = (0..4)
        .map(|n| file_len(guard.path(), &format!("idx_posg_run_{n}.bin")))
        .collect();
    assert_eq!(lens, [68, 68, 68, 28]);
}

#[test]
fn a_cut_off_header_or_record_is_a_deserialization_error() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let entries: Vec<RefEntry> = (0..10u64).map(|i| ((i % 4) as u32, i)).collect();
    // Runs 0..2 are written by the pushes; the last at `into_merger`.
    for (name, keep) in [("idx_h", 4u64), ("idx_r", 8 + 3)] {
        let mut spiller = EntrySpiller::<RefEntry>::new(guard.path(), name, 3).unwrap();
        for entry in &entries {
            spiller.push(*entry).unwrap();
        }
        let run = std::fs::OpenOptions::new()
            .write(true)
            .open(guard.path().join(format!("{name}_run_1.bin")))
            .unwrap();
        run.set_len(keep).unwrap();
        let read = spiller
            .into_merger()
            .and_then(|mut merger| merger.next_batch(usize::MAX));
        assert!(
            matches!(read, Err(VortexRdfError::Deserialization(_))),
            "{name}: {read:?}"
        );
    }

    let mut terms = TermInterner::new();
    let mut spiller = QuadSpiller::new(guard.path(), 3);
    for quad in distinct_quads() {
        let ids = terms.intern_quad(quad).unwrap();
        spiller.push(ids, &terms).unwrap();
    }
    let run = std::fs::OpenOptions::new()
        .write(true)
        .open(guard.path().join("quads_run_0.bin"))
        .unwrap();
    run.set_len(10).unwrap();
    let spilled = spiller.finish(&terms).unwrap();
    let rank_of = Arc::new(terms.finish().rank_of);
    let read = spilled
        .ranked(rank_of)
        .and_then(|runs| runs.merger())
        .and_then(|mut merger| merger.next_batch(usize::MAX));
    assert!(
        matches!(read, Err(VortexRdfError::Deserialization(_))),
        "quads: {read:?}"
    );
}

#[test]
fn an_entry_spiller_refuses_a_capacity_past_u32() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let made = EntrySpiller::<RefEntry>::new(guard.path(), "idx_o", u32::MAX as usize + 1);
    assert!(
        matches!(made, Err(VortexRdfError::InvalidInput(_))),
        "a run of more entries than a u32 offset reaches is refused"
    );
}

/// A run the merge cannot open fails with an error that names the
/// directory the run was to be read from and keeps the I/O error's kind.
#[test]
fn a_run_that_cannot_be_opened_names_its_directory() {
    let guard = TempRunsGuard::create(None, None).unwrap();
    let dir = guard.path().join("gone");
    let error = RecordReader::open(&dir.join("quads_run_0.bin"))
        .err()
        .expect("a run in a missing directory does not open");
    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
        "{error}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&format!("spill run open failed in {}: ", dir.display())),
        "{message}"
    );
}

/// A run smaller than the writer's buffer reaches the device only at the
/// final flush, which `finish` reports (the buffered writer's drop would
/// swallow it).
#[cfg(target_os = "linux")]
#[test]
fn record_writer_reports_a_full_volume_at_finish() {
    let mut writer = RecordWriter::create(Path::new("/dev/full")).unwrap();
    writer.write(&[0u8; QUAD_RECORD_BYTES]).unwrap();
    let error = writer.finish().unwrap_err();
    assert!(
        matches!(&error, VortexRdfError::Io(e) if e.kind() == std::io::ErrorKind::StorageFull),
        "{error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("spill run write failed in /dev: "),
        "{message}"
    );
    assert!(
        message.ends_with("; set VORTEX_RDF_SPILL_DIR to a directory with more room"),
        "{message}"
    );
}
