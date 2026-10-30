use std::cell::Cell;
use std::collections::HashMap;

use vortex_array::arrays::VarBinViewArray;

use super::*;
use crate::common::quad::{RawQuad, SharedQuad};
use crate::store::layouts::ResolvedLayout;
use crate::store::layouts::dictionary::handles::DictAccess;

thread_local! {
    /// Executions of [`code_columns`] on this thread — what the tests
    /// read to pin that a chunk's columns are read once per decode.
    pub(super) static CODE_COLUMN_READS: Cell<usize> = const { Cell::new(0) };
    /// Chunks whose distinct codes were resolved
    /// ([`resolve_chunk_terms`]) on this thread: a file-backed decode
    /// that did not decode in place.
    pub(super) static RESOLVED_CHUNKS: Cell<usize> = const { Cell::new(0) };
}

/// 120 quads whose columns have the shapes a store's chunks have: `s`
/// ascending in runs of ten, `p` in runs of four cycling over three
/// predicates, `o` unsorted with repeats (blank nodes, IRIs and
/// literals), `g` in three runs (default graph and two named graphs).
fn skewed_quads() -> Vec<RawQuad> {
    (0..120usize)
        .map(|i| RawQuad {
            s: format!("<http://example.org/s{:02}>", i / 10),
            p: format!("<http://example.org/p{}>", (i / 4) % 3),
            o: match i % 5 {
                0 => format!("_:b{}", i % 7),
                1 | 3 => format!("\"lit {}\"", (i * 53) % 29),
                _ => format!("<http://example.org/o{}>", (i * 37) % 23),
            },
            g: match i / 40 {
                0 => String::new(),
                1 => "<http://example.org/g1>".to_owned(),
                _ => "<http://example.org/g2>".to_owned(),
            },
        })
        .collect()
}

/// The quads' dictionary (compressed in windows of eight terms, so a
/// chunk's codes span many windows) and their code chunk.
fn skewed_chunk(quads: &[RawQuad]) -> (TermDictionary, ArrayRef) {
    let mut terms: Vec<&str> = quads
        .iter()
        .flat_map(|q| [q.s.as_str(), q.p.as_str(), q.o.as_str(), q.g.as_str()])
        .collect();
    terms.sort_unstable();
    terms.dedup();
    let code_map: BorrowedTermCodeMap<'_> = terms
        .iter()
        .enumerate()
        .map(|(rank, &term)| (term, rank as TermCode))
        .collect();
    let dict =
        TermDictionary::compress_windowed(VarBinViewArray::from_iter_str(terms.iter().copied()), 8)
            .unwrap();
    let codes = encode_quads(quads, &code_map).unwrap();
    let chunk = build_code_chunk(&codes, 0..quads.len(), false).unwrap();
    (dict, chunk)
}

/// `dict` left in a store file's dictionary child, opened file-backed.
async fn file_backed(dict: &TermDictionary) -> FileBackedDict {
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(dict).await);
    FileBackedDict::open(&native)
        .await
        .unwrap()
        .expect("the written dictionary child must be window-searchable")
}

/// Reads of the code columns, and chunks resolved, while `decode` runs.
async fn decode_counts<T>(decode: impl std::future::Future<Output = T>) -> (T, usize, usize) {
    let (reads, resolved) = (
        CODE_COLUMN_READS.with(Cell::get),
        RESOLVED_CHUNKS.with(Cell::get),
    );
    let out = decode.await;
    (
        out,
        CODE_COLUMN_READS.with(Cell::get) - reads,
        RESOLVED_CHUNKS.with(Cell::get) - resolved,
    )
}

/// `chunk` decoded against `fb` into quads, shared quads and raw quads,
/// each as the resident `dict` decodes it, each reading the code columns
/// once and lifting no dictionary; `resolved` is whether each decode
/// resolves the chunk's distinct codes rather than decoding in place.
async fn assert_file_backed_decodes(
    fb: &FileBackedDict,
    dict: &Arc<TermDictionary>,
    chunk: &ArrayRef,
    resolved: bool,
    case: &str,
) {
    use crate::store::layouts::ChunkDecode;

    let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb.clone()));
    let counts = (1, usize::from(resolved));

    let (decoded, reads, resolves) = decode_counts(Quad::decode_async(&layout, chunk)).await;
    assert_eq!((reads, resolves), counts, "{case}: quads");
    let decoded: Vec<Quad> = decoded.into_iter().map(|row| row.unwrap()).collect();
    let want: Vec<Quad> = decode_chunk(chunk, dict)
        .into_iter()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(decoded.len(), chunk.len(), "{case}: quads");
    assert_eq!(decoded, want, "{case}: quads");

    let (shared, reads, resolves) = decode_counts(SharedQuad::decode_async(&layout, chunk)).await;
    assert_eq!((reads, resolves), counts, "{case}: shared quads");
    let shared: Vec<SharedQuad> = shared.into_iter().map(|row| row.unwrap()).collect();
    let want: Vec<SharedQuad> = decode_chunk_shared(chunk, dict)
        .into_iter()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(shared, want, "{case}: shared quads");

    assert_eq!(fb.debug_lifts(), 0, "{case}");
}

/// A file-backed chunk decode over in-memory reads (a buffer here, as a
/// mapped file) decodes in place, resolving no codes, and decodes the
/// rows the resident dictionary decodes — for a chunk long enough to
/// memoize its roles and for one too short to.
#[tokio::test]
async fn a_file_backed_chunk_decode_reads_its_windows_in_place() {
    let all = skewed_quads();
    for quads in [&all[..], &all[..7]] {
        let (dict, chunk) = skewed_chunk(quads);
        let dict = Arc::new(dict);
        let fb = file_backed(&dict).await;
        let case = format!("{} quads", quads.len());
        assert_file_backed_decodes(&fb, &dict, &chunk, false, &case).await;
        assert!(!fb.reads_wait(), "{case}");
    }
}

/// Over reads that wait, the first decode gives up in place at its
/// first window and resolves the chunk's codes, without reading its code
/// columns again; the handle then knows its reads wait, and later decodes
/// resolve without trying.
#[tokio::test]
async fn a_chunk_decode_over_reads_that_wait_resolves_its_codes() {
    let quads = skewed_quads();
    let (dict, chunk) = skewed_chunk(&quads);
    let dict = Arc::new(dict);
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&dict).await);
    let fb = FileBackedDict::open_with_waiting_reads(&native)
        .await
        .unwrap()
        .expect("the written dictionary child must be window-searchable");
    assert!(!fb.reads_wait());
    assert_file_backed_decodes(&fb, &dict, &chunk, true, "waiting reads").await;
    assert!(fb.reads_wait());
    // Every window the chunk touches is cached now, yet a decode still
    // resolves: once reads wait, none is tried in place.
    assert!(!fb.debug_cached_windows().is_empty());
    assert_file_backed_decodes(&fb, &dict, &chunk, true, "cached windows").await;
}

/// A decode that cannot await (a scan's split task) decodes a chunk whole
/// over reads that complete when first polled, in place or, past the
/// window budget, from its resolved codes, as `decode_async` does. Over
/// reads that wait it leaves the chunk, and once the handle knows its
/// reads wait, it leaves every chunk without reading its code columns.
#[tokio::test]
async fn a_decode_that_cannot_await_takes_only_the_chunks_it_can() {
    use crate::store::layouts::ChunkDecode;

    let quads = skewed_quads();
    let (dict, chunk) = skewed_chunk(&quads);
    let dict = Arc::new(dict);
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&dict).await);
    let want: Vec<Quad> = decode_chunk(&chunk, &dict)
        .into_iter()
        .map(|row| row.unwrap())
        .collect();
    let roomy = FileBackedDict::open(&native).await.unwrap().unwrap();
    let tight = FileBackedDict::open_with_budget(&native, 1)
        .await
        .unwrap()
        .unwrap();
    for (fb, resolved, case) in [(roomy, 0, "in place"), (tight, 1, "resolved")] {
        let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb.clone()));
        let (decoded, reads, resolves) =
            decode_counts(async { Quad::decode_now(&layout, &chunk) }).await;
        let decoded: Vec<Quad> = decoded
            .expect(case)
            .into_iter()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(decoded, want, "{case}");
        assert_eq!((reads, resolves), (1, resolved), "{case}");
        assert!(!fb.reads_wait(), "{case}");
    }

    let fb = FileBackedDict::open_with_waiting_reads(&native)
        .await
        .unwrap()
        .unwrap();
    let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb.clone()));
    let (decoded, reads, _) = decode_counts(async { Quad::decode_now(&layout, &chunk) }).await;
    assert!(decoded.is_none());
    assert_eq!(reads, 1);
    assert!(fb.reads_wait());
    let (decoded, reads, resolves) =
        decode_counts(async { Quad::decode_now(&layout, &chunk) }).await;
    assert!(decoded.is_none());
    assert_eq!((reads, resolves), (0, 0));
}

/// An in-place decode holds its windows within the window cache's
/// budget: a budget of exactly the charge of the windows the chunk
/// touches decodes in place, one byte less gives up and resolves the
/// chunk's codes, decoding the same rows.
#[tokio::test]
async fn an_in_place_decode_holds_its_windows_within_the_cache_budget() {
    let quads = skewed_quads();
    let (dict, chunk) = skewed_chunk(&quads);
    let dict = Arc::new(dict);
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&dict).await);
    // What the touched windows are charged: what a roomy cache holds once
    // an in-place decode has taken each of them.
    let roomy = FileBackedDict::open(&native).await.unwrap().unwrap();
    assert_file_backed_decodes(&roomy, &dict, &chunk, false, "roomy").await;
    let touched = roomy.debug_cached_bytes();
    assert!(roomy.debug_cached_windows().len() > 2);
    for (budget, resolved) in [(touched, false), (touched - 1, true)] {
        let fb = FileBackedDict::open_with_budget(&native, budget)
            .await
            .unwrap()
            .unwrap();
        let case = format!("budget {budget} of {touched}");
        assert_file_backed_decodes(&fb, &dict, &chunk, resolved, &case).await;
        assert!(!fb.reads_wait(), "{case}");
    }
}

/// `codes` looked up in `memo`, each decoding to itself: how many were
/// decoded (missed).
fn memo_decodes(memo: &mut TermMemo<TermCode>, codes: impl IntoIterator<Item = TermCode>) -> usize {
    let mut decoded = 0;
    for code in codes {
        let term = memo
            .get_or_insert(code, || {
                decoded += 1;
                Ok(code)
            })
            .unwrap();
        assert_eq!(term, code);
    }
    decoded
}

/// A memo's trial keeps it admitting terms while its hits keep up with
/// its misses, at least one per [`MEMO_MISSES_PER_HIT`] at the
/// [`MEMO_TRIAL_MISSES`]th miss; with fewer it admits no more, and a
/// code met after that is decoded each time, while the terms it kept
/// still answer. A memo for shared terms admits every term.
#[test]
fn a_memo_admits_terms_while_its_hits_keep_up() {
    let trial = u64::from(MEMO_TRIAL_MISSES);
    // The trial's misses with `hits` hits among them, spread out.
    let run = |memo: &mut TermMemo<TermCode>, hits: u64| {
        let codes = (0..trial).flat_map(|code| {
            let again = code < hits;
            std::iter::once(code).chain(again.then_some(code))
        });
        assert_eq!(memo_decodes(memo, codes), MEMO_TRIAL_MISSES as usize);
    };
    let enough = trial / u64::from(MEMO_MISSES_PER_HIT);
    for (hits, admits) in [(enough, true), (enough - 1, false), (0, false)] {
        let mut memo = TermMemo::new(4096);
        run(&mut memo, hits);
        // A code past the trial, met twice: kept, or decoded twice.
        let past = trial + 100;
        let decoded = memo_decodes(&mut memo, [past, past]);
        assert_eq!(decoded, if admits { 1 } else { 2 }, "{hits} hits");
        // A code the trial kept still answers (the miss that ends a
        // refusing trial is not kept).
        assert_eq!(memo_decodes(&mut memo, [trial / 2]), 0, "{hits} hits");
    }
    let mut shared = TermMemo::admitting(4096);
    run(&mut shared, 0);
    assert_eq!(memo_decodes(&mut shared, [trial + 100, trial + 100]), 1);
}

/// Term reads counted by code, over a resident dictionary.
struct CountingTerms<'a> {
    inner: DictTerms<'a>,
    reads: HashMap<TermCode, usize>,
}

impl TermSource for CountingTerms<'_> {
    type Shared = Arc<str>;

    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
        *self.reads.entry(code).or_default() += 1;
        self.inner.str_at(role, code)
    }

    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
        self.str_at(role, code).map(Arc::from)
    }
}

/// Past a trial of distinct objects, a quad decode stops keeping the
/// object column's terms, so a repeated object is read again; a shared
/// decode keeps every term, so it is read once.
#[test]
fn a_shared_decode_keeps_every_term() {
    let n = MEMO_TRIAL_MISSES as usize + 8;
    // One subject, predicate and graph; distinct objects, then the last
    // one again.
    let quads: Vec<RawQuad> = (0..n)
        .map(|i| RawQuad {
            s: "<http://example.org/s>".to_owned(),
            p: "<http://example.org/p>".to_owned(),
            o: format!("\"o{:03}\"", i.min(n - 2)),
            g: String::new(),
        })
        .collect();
    let (dict, chunk) = skewed_chunk(&quads);
    let arrays = code_columns(&chunk).unwrap();
    let columns = code_slices(&arrays);
    let repeated = columns[2][n - 1];
    assert_eq!(columns[2][n - 2], repeated);
    let mut quads_src = CountingTerms {
        inner: DictTerms::new(&dict),
        reads: HashMap::new(),
    };
    let rows = decode_rows(columns, &mut quads_src);
    assert!(rows.iter().all(Result::is_ok));
    assert_eq!(
        quads_src.reads[&repeated], 2,
        "a quad decode reads it again"
    );
    let mut shared_src = CountingTerms {
        inner: DictTerms::new(&dict),
        reads: HashMap::new(),
    };
    let rows: Vec<Result<SharedQuad>> = decode_rows_shared(columns, &mut shared_src);
    assert!(rows.iter().all(Result::is_ok));
    assert_eq!(shared_src.reads[&repeated], 1, "a shared decode keeps it");
}

/// A source that fails on one code, giving up on the chunk or not.
struct FailingTerms<'a> {
    inner: DictTerms<'a>,
    code: TermCode,
    gives_up: bool,
    gave_up: bool,
}

impl TermSource for FailingTerms<'_> {
    type Shared = Arc<str>;

    fn str_at(&mut self, role: usize, code: TermCode) -> Result<&str> {
        if code == self.code {
            self.gave_up = self.gives_up;
            return Err(VortexRdfError::InvalidOperation("unreadable".into()));
        }
        self.inner.str_at(role, code)
    }

    fn shared_at(&mut self, role: usize, code: TermCode) -> Result<Arc<str>> {
        self.str_at(role, code).map(Arc::from)
    }

    fn gave_up(&self) -> bool {
        self.gave_up
    }
}

/// A quad decode whose source gives up ends at the row that failed, the
/// rows before it decoded; a failed read of a source that has not given
/// up is that row's `Err`, and the rows after it decode.
#[test]
fn a_quad_decode_ends_where_its_source_gives_up() {
    let quads = skewed_quads();
    let (dict, chunk) = skewed_chunk(&quads);
    let arrays = code_columns(&chunk).unwrap();
    let columns = code_slices(&arrays);
    // Rows 30..40 share their subject, which no row before them has.
    let code = columns[0][30];
    assert!(columns[0][..30].iter().all(|&c| c != code));
    let want = decode_chunk(&chunk, &dict);

    for gives_up in [true, false] {
        let mut src = FailingTerms {
            inner: DictTerms::new(&dict),
            code,
            gives_up,
            gave_up: false,
        };
        let rows = decode_rows(columns, &mut src);
        if gives_up {
            assert_eq!(rows.len(), 30);
        } else {
            assert_eq!(rows.len(), quads.len());
            assert!(rows[30..40].iter().all(Result::is_err));
            assert!(rows[40..].iter().all(Result::is_ok));
        }
        for (i, row) in rows
            .iter()
            .enumerate()
            .filter(|(i, _)| !(30..40).contains(i))
        {
            assert_eq!(row.as_ref().unwrap(), want[i].as_ref().unwrap(), "row {i}");
        }
    }
}

/// The distinct codes are the sorted set of all four columns, however
/// the columns repeat: `s` ascending, `p` and `g` in runs (a code may
/// return after another), `o` unsorted with repeats.
#[test]
fn distinct_codes_are_the_sorted_set_of_all_four_columns() {
    let sorted_set = |columns: &[&[TermCode]; 4]| -> Vec<TermCode> {
        let mut all: Vec<TermCode> = columns.iter().flat_map(|c| c.iter().copied()).collect();
        all.sort_unstable();
        all.dedup();
        all
    };
    let cases: [[&[TermCode]; 4]; 6] = [
        [
            &[1, 1, 2, 2, 2, 5, 5, 9],
            &[7, 7, 7, 3, 3, 7, 7, 7],
            &[40, 12, 40, 12, 33, 40, 5, 12],
            &[0, 0, 0, 0, 8, 8, 8, 8],
        ],
        [&[], &[], &[], &[]],
        [&[4], &[], &[4, 4], &[]],
        [
            &[0, 0],
            &[TermCode::MAX],
            &[TermCode::MAX, 0, TermCode::MAX],
            &[1 << 40, 1 << 40],
        ],
        [&[3, 3, 3], &[3, 3, 3], &[3, 3, 3], &[3, 3, 3]],
        [&[9, 8, 9, 8], &[1, 2, 1, 2], &[5, 5, 6, 6], &[0, 1, 0, 1]],
    ];
    for columns in cases {
        assert_eq!(distinct_codes(columns), sorted_set(&columns), "{columns:?}");
    }

    let (_, chunk) = skewed_chunk(&skewed_quads());
    let arrays = code_columns(&chunk).unwrap();
    let columns = code_slices(&arrays);
    let distinct = distinct_codes(columns);
    assert_eq!(distinct, sorted_set(&columns));
    // The skip rule leaves the sort far fewer than the 4n codes.
    assert!(distinct.len() < columns[0].len());
}
