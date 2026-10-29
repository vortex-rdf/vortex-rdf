//! The in-memory backend of [`keep`](VortexRdfStore::keep): a binary search
//! when the selected rows are a run of the sorted base that the column
//! orders, else one pass over the column's selected rows, read in place from
//! a canonical column, else a piece of a chunk at a time, decoded and
//! compared where the selected rows are dense and read row by row through the
//! column's probe where they are sparse or point-sized.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_buffer::Buffer;
use vortex_rdf_encoded_search::SortedProbe;

use super::{Keep, KeepTest};
use crate::debug;
use crate::error::Result;
use crate::store::array::{column_is_sorted, into_struct_array};
use crate::store::probes::StructProbes;
use crate::store::resident::cached_code_primitive;
use crate::store::schema::{self, QuadColumn, RowId, TermCode};
use crate::store::view::selection::{RowSelection, SelectedRows, ViewSelection, point_sized};
use crate::store::{MemoryBase, VortexRdfStore};

impl VortexRdfStore {
    /// The in-memory backend of [`keep`](Self::keep), `memory` the view's
    /// base.
    pub(super) fn keep_in_memory(
        &self,
        memory: &MemoryBase,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<Self> {
        let t = debug::timer();
        let MemoryBase { base, probes, .. } = memory;
        // A pending view's deferred ids are needed now: the keep narrows exact
        // row ids, and a serve plan's run no longer describes its result.
        let selection = self.quads.selection.materialized()?;
        let base_len = base.len();
        if selection.is_empty(base_len) {
            return Ok(self.empty_view());
        }
        let struct_arr = into_struct_array(base.clone())?;
        let narrowed =
            match Self::keep_sorted_run(&struct_arr, base, probes, &selection, column, keep) {
                Some(narrowed) => {
                    #[cfg(test)]
                    keep_hooks::note_sorted_run_keep();
                    debug::step!(
                        t,
                        "[keep] {:?} narrowed by binary search inside a sorted run",
                        column
                    );
                    narrowed
                }
                None => {
                    let narrowed =
                        Self::keep_by_pass(&struct_arr, base, probes, &selection, column, keep)?;
                    debug::step!(
                        t,
                        "[keep] {:?} narrowed by a pass over {} selected rows",
                        column,
                        selection.len(base_len)
                    );
                    narrowed
                }
            };
        if narrowed.is_empty(base_len) {
            return Ok(self.empty_view());
        }
        Ok(self.derived(
            self.quads.with_selection(ViewSelection::Exact(narrowed)),
            self.tail.clone(),
        ))
    }

    /// The keep by binary search, when the selection is a run of the sorted
    /// base that `column` orders (every earlier column of the `(s, p, o, g)`
    /// order is constant over the run), every row of the base counting as
    /// one run. A range keep is two lower bounds; a small set is one bounded
    /// sub-run per code. A keep on `s` over an id list searches the whole
    /// base instead ([`keep_located_subject`](Self::keep_located_subject)).
    /// `None` declines to the row pass.
    fn keep_sorted_run(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Option<RowSelection> {
        let s_col = struct_arr.unmasked_field_by_name(schema::COL_S).ok()?;
        if !column_is_sorted(s_col) {
            return None;
        }
        let run = match selection.as_rows(base.len()) {
            SelectedRows::Run(run) => run,
            SelectedRows::Ids(ids) if column == QuadColumn::S => {
                return Self::keep_located_subject(base, probes, selection, ids.len(), keep);
            }
            SelectedRows::Ids(_) => return None,
        };
        if run.is_empty() {
            return None;
        }
        for earlier in &QuadColumn::ALL[..column.index()] {
            let probe = probes.by_name(base, earlier.name()).flatten()?.probe();
            if probe.value_at(run.start) != probe.value_at(run.end - 1) {
                return None;
            }
        }
        let probe = probes.by_name(base, column.name()).flatten()?.probe();
        let width = run.len();
        match keep {
            Keep::Range(lo, hi) => {
                let (start, _) = probe.bounds_in(run.clone(), *lo);
                let (end, _) = probe.bounds_in(start..run.end, *hi);
                Some(RowSelection::Range(start as u64..end as u64))
            }
            Keep::Set(codes) => {
                // One bounded search per code while that beats a pass, in
                // probe reads: a point-sized run's pass reads each of its
                // rows through the probe, a wider run's decodes them, a
                // probe read costing `KEEP_DECODE_ROWS_PER_ID` decoded rows.
                let searches = codes.len().saturating_mul(width.ilog2() as usize + 1);
                let pass = if point_sized(width as u64) {
                    width
                } else {
                    width / KEEP_DECODE_ROWS_PER_ID
                };
                if searches >= pass {
                    return None;
                }
                let mut ids: Vec<u64> = Vec::new();
                let mut from = run.start;
                for &code in codes.as_slice() {
                    let (lo, hi) = probe.bounds_in(from..run.end, code);
                    ids.extend(lo as u64..hi as u64);
                    from = hi;
                    if from >= run.end {
                        break;
                    }
                }
                Some(RowSelection::Ids(Buffer::from(ids)))
            }
        }
    }

    /// A keep on the sorted `s` column over an id list of `selected` rows. The
    /// rows it admits are runs of the whole base whatever the selection, so
    /// they are located there by binary search through the column's probe,
    /// as the file backend locates them in a file sorted by `s`, and
    /// intersected with the ids: a range is two lower bounds, a set one
    /// bounded run per code while those searches beat a pass over the
    /// selected rows. `None` declines to the pass.
    fn keep_located_subject(
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        selected: usize,
        keep: &Keep,
    ) -> Option<RowSelection> {
        let probe = probes.by_name(base, schema::COL_S).flatten()?.probe();
        let len = probe.len();
        match keep {
            Keep::Range(lo, hi) => {
                let (start, _) = probe.bounds_in(0..len, *lo);
                let (end, _) = probe.bounds_in(start..len, *hi);
                Some(selection.clone().intersect_range(start as u64..end as u64))
            }
            Keep::Set(codes) => {
                // In probe reads, as `keep_sorted_run` weighs a set.
                let searches = codes.len().saturating_mul(len.max(2).ilog2() as usize + 1);
                let pass = if point_sized(selected as u64) {
                    selected
                } else {
                    selected / KEEP_DECODE_ROWS_PER_ID
                };
                if searches >= pass {
                    return None;
                }
                let mut runs: Vec<Range<u64>> = Vec::new();
                let mut from = 0;
                for &code in codes.as_slice() {
                    let (lo, hi) = probe.bounds_in(from..len, code);
                    if lo < hi {
                        runs.push(lo as u64..hi as u64);
                    }
                    from = hi;
                    if from >= len {
                        break;
                    }
                }
                Some(selection.clone().intersect_runs(&runs))
            }
        }
    }

    /// The keep by one pass over the selected rows of `column`: read in place
    /// from its canonical primitive when one is materialized, else through
    /// its cached probe a piece at a time ([`keep_by_pieces`]); a column
    /// neither serves is decoded for the selected rows only.
    fn keep_by_pass(
        struct_arr: &vortex_array::arrays::StructArray,
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<RowSelection> {
        use vortex_array::VortexSessionExecute as _;
        use vortex_array::arrays::PrimitiveArray;

        let col = struct_arr.unmasked_field_by_name(column.name())?;
        let base_len = base.len();
        let test = keep.test(selection.len(base_len));
        if let Some(prim) = cached_code_primitive(col) {
            let codes = prim.as_slice::<TermCode>();
            let kept = selection.filter_rows(base_len, |row| test.admits(codes[row]));
            return Ok(survivors(selection, base_len, kept));
        }
        if let Some(probe) = probes.by_name(base, column.name()).flatten() {
            return keep_by_pieces(
                col,
                probe.probe(),
                selection,
                &test,
                base_len,
                KEEP_PIECE_ROWS,
            );
        }
        // The selected rows only, decoded once.
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let prim = selection.apply(col)?.execute::<PrimitiveArray>(&mut ctx)?;
        Ok(selection
            .clone()
            .refine(&keep_mask(prim.as_slice::<TermCode>(), &test)))
    }
}

/// The positions of `codes` the keep admits, as a mask over them.
fn keep_mask(codes: &[TermCode], test: &KeepTest) -> vortex_mask::Mask {
    vortex_mask::Mask::from_indices(
        codes.len(),
        (0..codes.len()).filter(|&i| test.admits(codes[i])),
    )
}

/// The selection a pass leaves when `kept`, ascending, are the rows of
/// `selection` (out of `base_len`) the keep admits: `selection` itself when
/// it admits every one, else the kept ids (the canonical empty selection for
/// none).
fn survivors(selection: &RowSelection, base_len: usize, kept: Vec<RowId>) -> RowSelection {
    if !kept.is_empty() && kept.len() == selection.len(base_len) {
        return selection.clone();
    }
    RowSelection::from_ids(Buffer::from(kept))
}

/// Rows of one chunk of a column the in-memory pass decodes at once
/// ([`keep_by_pieces`]): a longer chunk is decoded a piece at a time, so the
/// codes decoded at once stay bounded whatever the chunk's length.
const KEEP_PIECE_ROWS: usize = 1 << 16;

/// Selected rows a piece holds at most for the pass to read them one by one
/// through the column's probe whatever their density: so few probe reads
/// cost less than a decode's fixed part (a slice, an execution, a buffer).
const KEEP_POINT_IDS_PER_PIECE: usize = 16;

/// Rows a decode reads per selected row, at most, for a piece to be decoded
/// and compared rather than read row by row through the column's probe: one
/// probe read costs as much as decoding many rows, so ids sparser than this
/// are cheaper read in place.
const KEEP_DECODE_ROWS_PER_ID: usize = 16;

/// Whether a piece with `selected` selected rows among the `span` rows from
/// its first selected row to its last is decoded and compared rather than
/// read row by row through the probe: more than
/// [`KEEP_POINT_IDS_PER_PIECE`] selected rows, at least one per
/// [`KEEP_DECODE_ROWS_PER_ID`] rows of the span.
fn decodes_piece(selected: usize, span: usize) -> bool {
    selected > KEEP_POINT_IDS_PER_PIECE && selected.saturating_mul(KEEP_DECODE_ROWS_PER_ID) >= span
}

/// The rows of `selection` (out of `base_len`) whose code in `column` the
/// keep's `test` admits, as the selection they leave ([`survivors`]): the
/// pass over a column with a probe and no canonical primitive. A point-sized
/// selection is read row by row through `probe`. Any other is taken a piece
/// at a time, a piece being at most `piece_rows` rows of one of the column's
/// chunks ([`code_chunks`]): a piece whose selected rows are dense
/// ([`decodes_piece`]) has the rows from its first selected row to its last
/// decoded at once and compared, any other reads its selected rows one by
/// one through `probe`. A piece's decoded codes are dropped before the next
/// piece is read.
fn keep_by_pieces(
    column: &ArrayRef,
    probe: &SortedProbe<'_>,
    selection: &RowSelection,
    test: &KeepTest,
    base_len: usize,
    piece_rows: usize,
) -> Result<RowSelection> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    if selection.is_point_sized() {
        #[cfg(test)]
        keep_hooks::note_probed_rows(selection.len(base_len));
        let kept = selection.filter_rows(base_len, |row| test.admits(probe.value_at(row)));
        return Ok(survivors(selection, base_len, kept));
    }
    // The selected rows: every row of one stretch, or an ascending id list.
    let (stretch, listed) = match selection.as_rows(base_len) {
        SelectedRows::Run(run) => (run, None),
        SelectedRows::Ids(ids) => (0..base_len, Some(ids.as_slice())),
    };
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let mut kept: Vec<RowId> = Vec::new();
    // The first id of `listed` no piece has taken yet.
    let mut next = 0;
    'chunks: for (chunk_start, chunk) in code_chunks(column) {
        let chunk_end = chunk_start + chunk.len();
        for start in (chunk_start..chunk_end).step_by(piece_rows) {
            if start >= stretch.end || listed.is_some_and(|ids| next == ids.len()) {
                break 'chunks;
            }
            let end = chunk_end.min(start.saturating_add(piece_rows));
            let rows = match listed {
                None => PieceRows::Stretch(stretch.start.max(start)..stretch.end.min(end)),
                Some(ids) => {
                    let taken = ids[next..].partition_point(|&id| id < end as RowId);
                    next += taken;
                    PieceRows::Ids(&ids[next - taken..next])
                }
            };
            let span = rows.span();
            if span.is_empty() {
                continue;
            }
            if decodes_piece(rows.selected(), span.len()) {
                #[cfg(test)]
                keep_hooks::note_decoded_piece();
                let codes = chunk
                    .slice(span.start - chunk_start..span.end - chunk_start)?
                    .execute::<PrimitiveArray>(&mut ctx)?;
                rows.admit_decoded(&mut kept, test, codes.as_slice::<TermCode>(), span.start);
            } else {
                #[cfg(test)]
                keep_hooks::note_probed_rows(rows.selected());
                rows.admit_probed(&mut kept, test, probe);
            }
        }
    }
    Ok(survivors(selection, base_len, kept))
}

/// One piece's selected rows: every row of a stretch, or a run of the
/// selection's ascending id list.
enum PieceRows<'a> {
    Stretch(Range<usize>),
    Ids(&'a [RowId]),
}

impl PieceRows<'_> {
    /// How many rows are selected.
    fn selected(&self) -> usize {
        match self {
            PieceRows::Stretch(rows) => rows.len(),
            PieceRows::Ids(ids) => ids.len(),
        }
    }

    /// The rows from the first selected to the last, which a decode reads;
    /// empty when none is selected.
    fn span(&self) -> Range<usize> {
        match self {
            PieceRows::Stretch(rows) => rows.clone(),
            PieceRows::Ids(ids) => match (ids.first(), ids.last()) {
                (Some(&first), Some(&last)) => first as usize..last as usize + 1,
                _ => 0..0,
            },
        }
    }

    /// Push onto `kept` the selected rows whose code the keep's `test`
    /// admits, reading it from `codes`, the decoded codes of the rows from
    /// `first` on.
    fn admit_decoded(
        &self,
        kept: &mut Vec<RowId>,
        test: &KeepTest,
        codes: &[TermCode],
        first: usize,
    ) {
        match self {
            PieceRows::Stretch(_) => kept.extend(
                codes
                    .iter()
                    .enumerate()
                    .filter(|&(_, &code)| test.admits(code))
                    .map(|(i, _)| (first + i) as RowId),
            ),
            PieceRows::Ids(ids) => kept.extend(
                ids.iter()
                    .copied()
                    .filter(|&id| test.admits(codes[id as usize - first])),
            ),
        }
    }

    /// Push onto `kept` the selected rows whose code the keep's `test`
    /// admits, reading it row by row through `probe`.
    fn admit_probed(&self, kept: &mut Vec<RowId>, test: &KeepTest, probe: &SortedProbe<'_>) {
        match self {
            PieceRows::Stretch(rows) => kept.extend(
                rows.clone()
                    .filter(|&row| test.admits(probe.value_at(row)))
                    .map(|row| row as RowId),
            ),
            PieceRows::Ids(ids) => kept.extend(
                ids.iter()
                    .copied()
                    .filter(|&id| test.admits(probe.value_at(id as usize))),
            ),
        }
    }
}

/// A code column's chunks, each beside the base row it starts at: a chunked
/// array's own chunks, any other array as one chunk. A `vortex.shared`
/// wrapper is looked through to the array it holds, so a piece decoded from
/// it never fills the wrapper's cache, which would keep the whole column
/// decoded for the base's lifetime.
fn code_chunks(column: &ArrayRef) -> Vec<(usize, ArrayRef)> {
    use vortex_array::arrays::Chunked;
    use vortex_array::arrays::chunked::ChunkedArrayExt as _;
    use vortex_layout::arrays::shared::{Shared, SharedArrayExt as _};

    let source = match column.as_opt::<Shared>() {
        Some(shared) => shared.current_array_ref().clone(),
        None => column.clone(),
    };
    if !source.is::<Chunked>() {
        return vec![(0, source)];
    }
    let chunked = source.as_::<Chunked>();
    let offsets = chunked.chunk_offset_values();
    (0..chunked.nchunks())
        .map(|i| (offsets[i], chunked.chunk(i).clone()))
        .collect()
}

/// Test hook: how many of this thread's in-memory keeps were answered by
/// binary search inside a sorted run (`keep_sorted_run`), not by a pass
/// over the selected rows; and how many pieces of a column the passes
/// decoded, and how many selected rows they read through a probe instead
/// (`keep_by_pieces`).
#[cfg(test)]
pub(crate) mod keep_hooks {
    use std::cell::Cell;

    thread_local! {
        static SORTED_RUN_KEEPS: Cell<usize> = const { Cell::new(0) };
        static DECODED_PIECES: Cell<usize> = const { Cell::new(0) };
        static PROBED_ROWS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one keep answered inside a sorted run.
    pub(super) fn note_sorted_run_keep() {
        SORTED_RUN_KEEPS.with(|c| c.set(c.get() + 1));
    }

    /// The keeps answered inside a sorted run on this thread so far.
    pub(crate) fn sorted_run_keeps() -> usize {
        SORTED_RUN_KEEPS.with(Cell::get)
    }

    /// Record one piece of a column a keep's pass decoded.
    pub(super) fn note_decoded_piece() {
        DECODED_PIECES.set(DECODED_PIECES.get() + 1);
    }

    /// The pieces this thread's keeps decoded so far.
    pub(crate) fn decoded_pieces() -> usize {
        DECODED_PIECES.get()
    }

    /// Record `rows` selected rows a keep's pass read one by one through a
    /// probe.
    pub(super) fn note_probed_rows(rows: usize) {
        PROBED_ROWS.set(PROBED_ROWS.get() + rows);
    }

    /// The selected rows this thread's keeps read through a probe so far.
    pub(crate) fn probed_rows() -> usize {
        PROBED_ROWS.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use vortex::encodings::runend::RunEnd;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::validity::Validity;
    use vortex_array::{IntoArray, VortexSessionExecute as _};

    use crate::store::array::{chunked_or_single, stamp_is_sorted};
    use crate::store::resident::with_compressed_int_children;

    /// A piece is decoded past sixteen selected rows, at one or more per
    /// sixteen rows of its span.
    #[test]
    fn dense_pieces_are_decoded() {
        assert!(!decodes_piece(16, 16));
        assert!(decodes_piece(17, 17));
        assert!(decodes_piece(17, 272));
        assert!(!decodes_piece(17, 273));
        assert!(decodes_piece(4_096, KEEP_PIECE_ROWS));
        assert!(!decodes_piece(4_095, KEEP_PIECE_ROWS));
    }

    /// The code columns of a `rows`-row base: `s` ascending in runs of four,
    /// `p` cycling through thirteen codes, `o` through a thousand and `g`
    /// through eight in ten-row blocks.
    fn base_codes(rows: u64) -> [Vec<TermCode>; 4] {
        [
            (0..rows).map(|i| i / 4).collect(),
            (0..rows).map(|i| (i * 7) % 13).collect(),
            (0..rows).map(|i| (i * 31 + i / 3) % 1_000).collect(),
            (0..rows).map(|i| (i / 10) % 8).collect(),
        ]
    }

    /// `codes` as an in-memory base of `chunk_rows`-row chunks (one array
    /// when that is every row) in the encodings a build and an adoption
    /// leave: `s`, `p` and `o` compressed by the build's pass (`s`, stamped
    /// sorted, run-end; `p` and `o` bit-packed) behind its shared wrapper,
    /// and `g` run-end encoded chunk by chunk and left bare, as an adopted
    /// file's column is; its probes warmed.
    fn encoded_base(
        codes: &[Vec<TermCode>; 4],
        chunk_rows: usize,
    ) -> (StructArray, ArrayRef, Arc<StructProbes>) {
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let mut children = Vec::with_capacity(codes.len());
        for (index, values) in codes.iter().enumerate() {
            let mut chunks = Vec::new();
            for part in values.chunks(chunk_rows) {
                let chunk = PrimitiveArray::from_iter(part.iter().copied()).into_array();
                chunks.push(if index == QuadColumn::G.index() {
                    RunEnd::encode(chunk, &mut ctx).unwrap().into_array()
                } else {
                    chunk
                });
            }
            let child = chunked_or_single(chunks, dtype.clone()).unwrap();
            if index == QuadColumn::S.index() {
                stamp_is_sorted(&child);
            }
            children.push(child);
        }
        let rows = StructArray::try_new(
            ["s", "p", "o", "g"].into(),
            children,
            codes[0].len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let base = with_compressed_int_children(rows, true).unwrap();
        let probes = StructProbes::new();
        probes.warm(&base);
        (into_struct_array(base.clone()).unwrap(), base, probes)
    }

    /// The base rows `selection` covers out of `base_len`, ascending.
    fn rows_of(selection: &RowSelection, base_len: usize) -> Vec<u64> {
        selection.filter_rows(base_len, |_| true)
    }

    /// The per-row pass's answer: the selected rows whose code in `column`,
    /// read through the column's probe, `keep` admits, ascending.
    fn per_row_pass(
        base: &ArrayRef,
        probes: &StructProbes,
        selection: &RowSelection,
        column: QuadColumn,
        keep: &Keep,
    ) -> Vec<u64> {
        let probe = probes
            .by_name(base, column.name())
            .flatten()
            .expect("the column has a probe")
            .probe();
        let test = keep.test(selection.len(base.len()));
        selection.filter_rows(base.len(), |row| test.admits(probe.value_at(row)))
    }

    /// The selections the pass is checked over, each beside its name, out of
    /// 6,000 rows in 2,000-row chunks: every row, a range across two chunk
    /// boundaries, a point-sized range, and id lists dense (seven rows in
    /// eight), sparse (one in 23), clustered (each chunk's first 300 rows
    /// amid one in 29) and point-sized (one in 40).
    fn pass_selections() -> Vec<(&'static str, RowSelection)> {
        let ids = |admit: &dyn Fn(u64) -> bool| {
            RowSelection::Ids(Buffer::from_iter((0..6_000u64).filter(|&i| admit(i))))
        };
        vec![
            ("every row", RowSelection::All),
            ("a range", RowSelection::Range(1_500..4_700)),
            ("a point-sized range", RowSelection::Range(1_990..2_100)),
            ("dense ids", ids(&|i: u64| !i.is_multiple_of(8))),
            ("sparse ids", ids(&|i: u64| i.is_multiple_of(23))),
            (
                "clustered ids",
                ids(&|i: u64| i % 2_000 < 300 || i.is_multiple_of(29)),
            ),
            ("point-sized ids", ids(&|i: u64| i.is_multiple_of(40))),
        ]
    }

    /// Over every row of a base sorted by subject (an `All` selection), a
    /// keep on `s` is a binary search, a range as one run of the column and a
    /// small set as a run per code, each equal to the per-row pass; a set
    /// with more searches than the base has rows, and a keep on `p`, which
    /// the base's subjects do not hold constant, decline to the pass.
    #[test]
    fn subject_keep_over_every_row_is_a_search() {
        let codes = base_codes(6_000);
        let (struct_arr, base, probes) = encoded_base(&codes, 2_000);
        let all = RowSelection::All;
        let search = |column: QuadColumn, keep: &Keep| {
            VortexRdfStore::keep_sorted_run(&struct_arr, &base, &probes, &all, column, keep)
        };
        let last = codes[0][5_999];
        for keep in [
            Keep::range(0..last + 1),
            Keep::range(10..900),
            Keep::range(700..701),
            Keep::range(last + 1..last + 9),
            Keep::set([0, 3, 499, 500, 1_001, last]),
        ] {
            let narrowed =
                search(QuadColumn::S, &keep).unwrap_or_else(|| panic!("{keep:?} is searched"));
            assert_eq!(
                rows_of(&narrowed, 6_000),
                per_row_pass(&base, &probes, &all, QuadColumn::S, &keep),
                "{keep:?}"
            );
        }
        assert!(matches!(
            search(QuadColumn::S, &Keep::range(10..900)),
            Some(RowSelection::Range(_))
        ));
        // A thousand codes take thirteen steps each, past what a pass costs.
        let wide = Keep::set((0..1_000).map(|i| i * 3 / 2));
        assert!(search(QuadColumn::S, &wide).is_none());
        assert!(search(QuadColumn::P, &Keep::range(0..5)).is_none());
    }

    /// The pass over columns read through their probes (bit-packed and
    /// run-end chunks behind the build's shared wrapper, bare run-end
    /// chunks, and each as one unchunked array) equals the per-row pass for
    /// every keep over every selection of [`pass_selections`], with pieces
    /// shorter than the chunks too, so a chunk is decoded in several; and no
    /// decode fills a shared wrapper's cache.
    #[test]
    fn piecewise_pass_equals_the_per_row_pass() {
        const ROWS: usize = 6_000;
        let codes = base_codes(ROWS as u64);
        let selections = pass_selections();
        for chunk_rows in [2_000, ROWS] {
            let (struct_arr, base, probes) = encoded_base(&codes, chunk_rows);
            for column in QuadColumn::ALL {
                let values = &codes[column.index()];
                let col = struct_arr.unmasked_field_by_name(column.name()).unwrap();
                let probe = probes
                    .by_name(&base, column.name())
                    .flatten()
                    .unwrap()
                    .probe();
                let top = values.iter().max().unwrap() + 1;
                for keep in [
                    Keep::range(0..top),
                    Keep::range(0..top / 2),
                    Keep::range(top / 3..top / 3 + 1),
                    Keep::set((0..top).step_by(2)),
                    Keep::set([1, top / 2, top - 1]),
                    Keep::range(top..top + 5),
                ] {
                    let test = keep.test(ROWS);
                    let brute = |selection: &RowSelection| -> Vec<u64> {
                        rows_of(selection, ROWS)
                            .into_iter()
                            .filter(|&row| keep.admits(values[row as usize]))
                            .collect()
                    };
                    assert_eq!(
                        per_row_pass(&base, &probes, &RowSelection::All, column, &keep),
                        brute(&RowSelection::All),
                        "{chunk_rows}-row chunks, {column:?} {keep:?}: the per-row pass"
                    );
                    for (name, selection) in &selections {
                        let tag = format!("{chunk_rows}-row chunks, {column:?} {keep:?}, {name}");
                        let want = brute(selection);
                        let narrowed = VortexRdfStore::keep_by_pass(
                            &struct_arr,
                            &base,
                            &probes,
                            selection,
                            column,
                            &keep,
                        )
                        .unwrap();
                        assert_eq!(rows_of(&narrowed, ROWS), want, "{tag}");
                        for piece_rows in [500, 96] {
                            let narrowed =
                                keep_by_pieces(col, probe, selection, &test, ROWS, piece_rows)
                                    .unwrap();
                            assert_eq!(
                                rows_of(&narrowed, ROWS),
                                want,
                                "{tag}, {piece_rows}-row pieces"
                            );
                        }
                    }
                }
            }
            // VORTEX_RDF_ENCODED_SEARCH=0 decodes searched columns into the cache by design.
            for column in ["s", "p", "o"] {
                let col = struct_arr.unmasked_field_by_name(column).unwrap();
                assert!(
                    !vortex_rdf_encoded_search::encoded_search_enabled()
                        || cached_code_primitive(col).is_none(),
                    "{chunk_rows}-row chunks, {column}: the shared cache stays empty"
                );
            }
        }
    }

    /// The gate: every row and a range are decoded a piece at a time, one
    /// piece a chunk or several, with no row read through the probe; a sparse
    /// id list (one row in 23), a point-sized range, dense as it is, and a
    /// point-sized id set read every selected row through the probe and
    /// decode nothing; a list dense in each chunk's first 300 rows and sparse
    /// elsewhere decodes the first piece of each chunk and probes the rest.
    /// The column's shared cache stays empty.
    #[test]
    fn the_pass_decodes_dense_pieces_and_probes_sparse_ones() {
        if !vortex_rdf_encoded_search::encoded_search_enabled() {
            return; // VORTEX_RDF_ENCODED_SEARCH=0 decodes every leaf whole: no pieces to count
        }
        use super::keep_hooks::{decoded_pieces, probed_rows};
        const ROWS: usize = 6_000;
        let codes = base_codes(ROWS as u64);
        let (struct_arr, base, probes) = encoded_base(&codes, 2_000);
        let col = struct_arr.unmasked_field_by_name("p").unwrap();
        let probe = probes.by_name(&base, "p").flatten().unwrap().probe();
        let keep = Keep::range(0..7);
        let test = keep.test(ROWS);
        let run = |selection: &RowSelection, piece_rows: usize| {
            let (pieces, probed) = (decoded_pieces(), probed_rows());
            keep_by_pieces(col, probe, selection, &test, ROWS, piece_rows).unwrap();
            (decoded_pieces() - pieces, probed_rows() - probed)
        };
        let ids = |admit: &dyn Fn(u64) -> bool| -> Vec<u64> {
            (0..ROWS as u64).filter(|&i| admit(i)).collect()
        };
        assert_eq!(
            run(&RowSelection::All, KEEP_PIECE_ROWS),
            (3, 0),
            "a chunk a piece"
        );
        assert_eq!(run(&RowSelection::All, 500), (12, 0), "four pieces a chunk");
        assert_eq!(
            run(&RowSelection::Range(1_500..4_700), KEEP_PIECE_ROWS),
            (3, 0),
            "the chunks a range touches"
        );
        let sparse = ids(&|i: u64| i.is_multiple_of(23));
        assert_eq!(
            run(
                &RowSelection::Ids(Buffer::from(sparse.clone())),
                KEEP_PIECE_ROWS
            ),
            (0, sparse.len()),
            "sparse ids"
        );
        assert_eq!(
            run(&RowSelection::Range(2_000..2_200), KEEP_PIECE_ROWS),
            (0, 200),
            "a point-sized range"
        );
        let point = ids(&|i: u64| i.is_multiple_of(40));
        assert_eq!(
            run(
                &RowSelection::Ids(Buffer::from(point.clone())),
                KEEP_PIECE_ROWS
            ),
            (0, point.len()),
            "a point-sized id set"
        );
        let clustered = ids(&|i: u64| i % 2_000 < 300 || i.is_multiple_of(29));
        let in_first_pieces = clustered.iter().filter(|&&i| i % 2_000 < 500).count();
        assert_eq!(
            run(&RowSelection::Ids(Buffer::from(clustered.clone())), 500),
            (3, clustered.len() - in_first_pieces),
            "clustered ids"
        );
        assert!(
            cached_code_primitive(col).is_none(),
            "the shared cache stays empty"
        );
    }
}
