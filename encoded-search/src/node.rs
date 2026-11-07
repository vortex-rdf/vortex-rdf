//! The resolved probe tree. A [`Node`] mirrors one encoded array node,
//! borrowing its buffers; every operation is a slice read, integer
//! arithmetic or a bit-packed extraction (of one word, or of a 1024-value
//! chunk onto the stack), with no `ExecutionCtx`, no decode and no
//! allocation. The exception is [`DeltaNode`], whose values
//! are running sums: it decodes a 1024-value block the first time one of its
//! values is read, and keeps it. The leaves' word buffers are in
//! [`crate::words`], bit-packed nodes and their unpacking in
//! [`crate::bitpacked`], and the searches the nodes run in [`crate::search`].

use crate::bitpacked::PackedNode;
use crate::delta::DeltaNode;
use crate::search::{EqSink, partition, search_ascending, search_ascending_capped};
use crate::words::Words;
use std::ops::Range;

/// Values per FastLanes chunk: the stride of a chunked FoR's references and
/// of a run-length array's value offsets.
pub(crate) const FL_CHUNK: usize = vortex_fastlanes::FL_CHUNK_SIZE;

/// Values a dictionary may hold for [`Node::eq_into`] to scan them for a
/// needle's codes; a larger one is counted by reading.
const DICT_SCAN: usize = 4_096;

/// One resolved chunk of a [`Node::Chunked`] tree, never empty, with its
/// logical start row. Its last value is read when a bounds search first
/// probes the chunk, then kept, so resolution stays downcast-only and a
/// long-lived owned probe pays the read once.
pub(crate) struct Chunk<'a> {
    pub(crate) start: usize,
    pub(crate) node: Node<'a>,
    last: std::sync::OnceLock<u64>,
}

impl<'a> Chunk<'a> {
    pub(crate) fn new(start: usize, node: Node<'a>) -> Self {
        Self {
            start,
            node,
            last: std::sync::OnceLock::new(),
        }
    }

    /// The chunk's last value, read once.
    fn last(&self) -> u64 {
        *self
            .last
            .get_or_init(|| self.node.value_at(self.node.len() - 1))
    }
}

/// Exception values of a bit-packed array, resolved as probe nodes: `indices`
/// holds strictly increasing patched positions (searchable), `values` the
/// replacement values (point access only, so its sort order never matters).
pub(crate) struct PatchProbe<'a> {
    pub(crate) indices: Box<Node<'a>>,
    pub(crate) values: Box<Node<'a>>,
    pub(crate) offset: usize,
}

impl PatchProbe<'_> {
    /// Replacement value for logical index `i`, if that position is patched.
    pub(crate) fn lookup(&self, i: usize) -> Option<u64> {
        let key = (i + self.offset) as u64;
        let at = self.indices.lower_bound(key);
        (at < self.indices.len() && self.indices.value_at(at) == key)
            .then(|| self.values.value_at(at))
    }

    /// Write the replacement value of every patched position among the
    /// logical indices `start..start + out.len()` into its place in `out`:
    /// the first patch in range found by one search, the rest walked.
    pub(crate) fn overwrite(&self, start: usize, out: &mut [u64]) {
        let first = (start + self.offset) as u64;
        let end = first + out.len() as u64;
        let mut at = self.indices.lower_bound(first);
        while at < self.indices.len() {
            let key = self.indices.value_at(at);
            if key >= end {
                break;
            }
            out[(key - first) as usize] = self.values.value_at(at);
            at += 1;
        }
    }
}

/// One node of the resolved probe tree, a variant per supported encoding.
/// Children are resolved with their parent
/// ([`crate::SortedProbe::resolve`]), so every operation below is
/// infallible.
pub(crate) enum Node<'a> {
    Primitive(Words<'a>),
    Constant {
        value: u64,
        len: usize,
    },
    /// `value_at(i) = base + i * multiplier`, `multiplier > 0` (a zero
    /// multiplier resolves to `Constant`).
    Sequence {
        base: u64,
        multiplier: u64,
        len: usize,
    },
    RunEnd {
        ends: Box<Node<'a>>,
        values: Box<Node<'a>>,
        offset: usize,
        len: usize,
    },
    FoR {
        reference: u64,
        child: Box<Node<'a>>,
    },
    /// Frame of reference with one reference per 1024-value FastLanes chunk
    /// (`fastlanes.for.v2`): `value_at(i) = child.value_at(i) +
    /// references.value_at((offset + i) / 1024)`, wrapping. A needle maps to
    /// a different encoded value in every chunk, so searches probe the
    /// composed values.
    ChunkedFoR {
        references: Box<Node<'a>>,
        offset: usize,
        child: Box<Node<'a>>,
    },
    BitPacked(PackedNode<'a>),
    Slice {
        child: Box<Node<'a>>,
        start: usize,
        len: usize,
    },
    Chunked {
        chunks: Vec<Chunk<'a>>,
        len: usize,
    },
    /// `value_at(i) = values.value_at(codes.value_at(i))`. Dictionary values
    /// are not order-preserving, so bounds searches probe the composed
    /// logical values rather than the values child.
    Dict {
        codes: Box<Node<'a>>,
        values: Box<Node<'a>>,
    },
    /// FastLanes delta encoding, decoded one 1024-value block at a time on
    /// first touch (see [`DeltaNode`]).
    Delta(DeltaNode<'a>),
    /// `vortex.sparse`: `fill` everywhere except the patched positions.
    Sparse {
        fill: u64,
        patches: PatchProbe<'a>,
        len: usize,
    },
    /// FastLanes run-length (`fastlanes.rle`): each 1024-value chunk keeps
    /// its run values in `values`, from `value_offsets.value_at(chunk) -
    /// base`, and one index per position into them, so `value_at(i) =
    /// values.value_at(value_offsets.value_at(c) - base +
    /// indices.value_at(offset + i))` with `c = (offset + i) / 1024`.
    Rle {
        values: Box<Node<'a>>,
        indices: Box<Node<'a>>,
        value_offsets: Box<Node<'a>>,
        base: u64,
        offset: usize,
        len: usize,
    },
}

impl Node<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Node::Primitive(w) => w.len(),
            Node::Constant { len, .. }
            | Node::Sequence { len, .. }
            | Node::RunEnd { len, .. }
            | Node::Slice { len, .. }
            | Node::Chunked { len, .. }
            | Node::Sparse { len, .. }
            | Node::Rle { len, .. } => *len,
            Node::FoR { child, .. } | Node::ChunkedFoR { child, .. } => child.len(),
            Node::BitPacked(p) => p.len(),
            Node::Dict { codes, .. } => codes.len(),
            Node::Delta(d) => d.len(),
        }
    }

    /// Exact logical value at `i`; requires `i < self.len()` but no sortedness.
    pub(crate) fn value_at(&self, i: usize) -> u64 {
        match self {
            Node::Primitive(w) => w.get(i),
            Node::Constant { value, .. } => *value,
            Node::Sequence {
                base, multiplier, ..
            } => (u128::from(*base) + i as u128 * u128::from(*multiplier)) as u64,
            Node::RunEnd {
                ends,
                values,
                offset,
                ..
            } => {
                let pos = (i + offset) as u64;
                let run = partition(ends.len(), |r| ends.value_at(r) <= pos);
                values.value_at(run)
            }
            Node::FoR { reference, child } => reference.saturating_add(child.value_at(i)),
            Node::ChunkedFoR {
                references,
                offset,
                child,
            } => chunked_for_value_at(references, *offset, child, i),
            Node::BitPacked(p) => p.value_at(i),
            Node::Slice { child, start, .. } => child.value_at(start + i),
            Node::Chunked { chunks, .. } => {
                let c = partition(chunks.len(), |c| chunks[c].start <= i) - 1;
                chunks[c].node.value_at(i - chunks[c].start)
            }
            // A code is below the values' length (a construction invariant),
            // so it fits a `usize`.
            Node::Dict { codes, values } => values.value_at(codes.value_at(i) as usize),
            Node::Delta(d) => d.value_at(i),
            Node::Sparse { fill, patches, .. } => patches.lookup(i).unwrap_or(*fill),
            // Offsets and indices point inside `values` (a construction
            // invariant, as for a dictionary's codes).
            Node::Rle {
                values,
                indices,
                value_offsets,
                base,
                offset,
                ..
            } => {
                let pos = offset + i;
                let first = value_offsets.value_at(pos / FL_CHUNK) - base;
                values.value_at((first + indices.value_at(pos)) as usize)
            }
        }
    }

    /// The exact values at `start..start + out.len()`, into `out`: what
    /// [`Self::value_at`] reads at each, with the work the indices share done
    /// once. A run-end node finds the first run once and walks the runs, a
    /// bit-packed node searches its patches once, a delta node copies from
    /// its decoded blocks, a chunked node finds each chunk once, and the
    /// nodes over a child (frame of reference, slice, dictionary codes) read
    /// the child's range whole. Requires `start + out.len() <= self.len()`
    /// but no sortedness.
    pub(crate) fn read_into(&self, start: usize, out: &mut [u64]) {
        if out.is_empty() {
            return;
        }
        match self {
            Node::Primitive(w) => w.read_into(start, out),
            Node::Constant { value, .. } => out.fill(*value),
            Node::RunEnd {
                ends,
                values,
                offset,
                ..
            } => {
                for (run, value) in runs_in(ends, values, *offset, start, out.len()) {
                    out[run].fill(value);
                }
            }
            Node::FoR { reference, child } => {
                child.read_into(start, out);
                for o in out.iter_mut() {
                    *o = reference.saturating_add(*o);
                }
            }
            Node::ChunkedFoR {
                references,
                offset,
                child,
            } => {
                child.read_into(start, out);
                for (k, o) in out.iter_mut().enumerate() {
                    *o = o.wrapping_add(references.value_at((offset + start + k) / FL_CHUNK));
                }
            }
            Node::BitPacked(p) => p.read_into(start, out),
            Node::Slice {
                child, start: at, ..
            } => child.read_into(at + start, out),
            Node::Chunked { chunks, .. } => {
                for (node, from, take, done) in chunk_windows(chunks, start, out.len()) {
                    node.read_into(from, &mut out[done..done + take]);
                }
            }
            // A code is below the values' length (a construction invariant),
            // so it fits a `usize`.
            Node::Dict { codes, values } => {
                codes.read_into(start, out);
                for o in out.iter_mut() {
                    *o = values.value_at(*o as usize);
                }
            }
            Node::Delta(d) => d.read_into(start, out),
            Node::Sparse { fill, patches, .. } => {
                out.fill(*fill);
                patches.overwrite(start, out);
            }
            Node::Sequence { .. } | Node::Rle { .. } => {
                for (k, o) in out.iter_mut().enumerate() {
                    *o = self.value_at(start + k);
                }
            }
        }
    }

    /// The values at `start..start + len` equal to `needle`, into `sink` as
    /// the offsets `base + k` of the values at `start + k`, ascending: found
    /// on the encoding where it allows (a constant at once, a sequence by
    /// arithmetic, run ends by their runs, a frame of reference against the
    /// shifted needle, a small dictionary's codes against the needle's code,
    /// unpatched bit-packed values a 1024-value chunk at a time, words as
    /// they are), and read otherwise ([`Self::eq_into_read`]). Requires
    /// `start + len <= self.len()` but no sortedness.
    pub(crate) fn eq_into<S: EqSink>(
        &self,
        start: usize,
        len: usize,
        needle: u64,
        base: usize,
        sink: &mut S,
    ) {
        if len == 0 {
            return;
        }
        match self {
            Node::Primitive(w) => w.eq_into(start, len, needle, base, sink),
            Node::Constant { value, .. } => {
                if *value == needle {
                    sink.run(base..base + len);
                }
            }
            Node::Sequence {
                base: first,
                multiplier,
                ..
            } => {
                if let Some(step) = needle.checked_sub(*first)
                    && step % multiplier == 0
                    && let Ok(at) = usize::try_from(step / multiplier)
                    && (start..start + len).contains(&at)
                {
                    let at = base + at - start;
                    sink.run(at..at + 1);
                }
            }
            Node::RunEnd {
                ends,
                values,
                offset,
                ..
            } => {
                for (run, value) in runs_in(ends, values, *offset, start, len) {
                    if value == needle {
                        sink.run(base + run.start..base + run.end);
                    }
                }
            }
            Node::FoR { reference, child } => {
                if let Some(shifted) = needle.checked_sub(*reference) {
                    child.eq_into(start, len, shifted, base, sink);
                }
            }
            Node::Slice {
                child, start: at, ..
            } => child.eq_into(at + start, len, needle, base, sink),
            Node::Chunked { chunks, .. } => {
                for (node, from, take, done) in chunk_windows(chunks, start, len) {
                    node.eq_into(from, take, needle, base + done, sink);
                }
            }
            // A dictionary small enough to scan maps the needle to its code
            // once, then searches its codes for that code; a needle under
            // several codes reads the window, which keeps the offsets
            // ascending.
            Node::Dict { codes, values } if values.len() <= DICT_SCAN => {
                let mut matching =
                    (0..values.len()).filter(|&code| values.value_at(code) == needle);
                match (matching.next(), matching.next()) {
                    (None, _) => {}
                    (Some(code), None) => codes.eq_into(start, len, code as u64, base, sink),
                    (Some(_), Some(_)) => self.eq_into_read(start, len, needle, base, sink),
                }
            }
            Node::BitPacked(p) if p.patches().is_none() => {
                p.eq_into(start, len, needle, base, sink)
            }
            _ => self.eq_into_read(start, len, needle, base, sink),
        }
    }

    /// [`Self::eq_into`] by reading the window a 1024-value block at a time.
    /// Out of line, so the block is reserved only by the searches that read.
    #[inline(never)]
    fn eq_into_read<S: EqSink>(
        &self,
        start: usize,
        len: usize,
        needle: u64,
        base: usize,
        sink: &mut S,
    ) {
        let mut block = [0u64; FL_CHUNK];
        let (mut at, end) = (start, start + len);
        while at < end {
            let n = (end - at).min(FL_CHUNK);
            self.read_into(at, &mut block[..n]);
            sink.matching(base + at - start, &block[..n], needle);
            at += n;
        }
    }

    /// First index whose value is `>= needle`, the length when every value is
    /// below it; requires ascending order.
    pub(crate) fn lower_bound(&self, needle: u64) -> usize {
        match self {
            Node::Primitive(w) => w.lower_bound(needle),
            Node::Constant { value, len } => {
                if *value < needle {
                    *len
                } else {
                    0
                }
            }
            Node::Sequence {
                base,
                multiplier,
                len,
            } => {
                if needle <= *base {
                    0
                } else {
                    clamp_to_len((needle - *base).div_ceil(*multiplier), *len)
                }
            }
            Node::RunEnd {
                ends,
                values,
                offset,
                len,
            } => {
                let (first, span) = window_runs(ends, *offset, *len);
                let k = search_ascending(span, |k| values.value_at(first + k), needle);
                run_start(ends, first + k, *offset, *len)
            }
            Node::FoR { reference, child } => {
                // Every stored value is `reference + encoded >= reference`.
                if needle <= *reference {
                    0
                } else {
                    child.lower_bound(needle - *reference)
                }
            }
            Node::BitPacked(p) => search_ascending(p.len(), |i| p.value_at(i), needle),
            // Sortedness is asserted for the window only, so the search reads
            // through the window, never the child's own order.
            Node::Slice { child, start, len } => {
                partition(*len, |i| child.value_at(*start + i) < needle)
            }
            Node::Chunked { chunks, len } => {
                // First chunk whose last value reaches the needle holds the
                // boundary.
                let c = partition(chunks.len(), |c| chunks[c].last() < needle);
                if c == chunks.len() {
                    *len
                } else {
                    chunks[c].start + chunks[c].node.lower_bound(needle)
                }
            }
            Node::Dict { codes, values } => partition(codes.len(), |i| {
                values.value_at(codes.value_at(i) as usize) < needle
            }),
            // A bisection touches O(log blocks) blocks, each decoded once.
            Node::Delta(d) => partition(d.len(), |i| d.value_at(i) < needle),
            Node::ChunkedFoR { .. } | Node::Sparse { .. } | Node::Rle { .. } => {
                partition(self.len(), |i| self.value_at(i) < needle)
            }
        }
    }

    /// First index whose value is `> needle`: values are integers, so it is
    /// the lower bound of `needle + 1`, and the length when `needle` is
    /// `u64::MAX`. Requires ascending order.
    pub(crate) fn upper_bound(&self, needle: u64) -> usize {
        needle
            .checked_add(1)
            .map_or(self.len(), |next| self.lower_bound(next))
    }

    /// `(lower_bound, upper_bound)` of `needle`; requires ascending order. A
    /// RunEnd node finds the needle's run once and reads both ends of it from
    /// `ends`, where the two bounds would each search the runs again. A
    /// frame of reference takes its child's bounds of the needle less the
    /// reference, and a bit-packed node searches once: the lower bound's
    /// probes cap the needle's rows at the first value they read above it,
    /// and the upper bound is searched below that cap only.
    #[inline]
    pub(crate) fn bounds(&self, needle: u64) -> (usize, usize) {
        match self {
            Node::RunEnd {
                ends,
                values,
                offset,
                len,
            } => runend_bounds(ends, values, *offset, *len, needle),
            // Every stored value is `reference + encoded`, so none lies below
            // the reference.
            Node::FoR { reference, child } => match needle.checked_sub(*reference) {
                Some(encoded) => child.bounds(encoded),
                None => (0, 0),
            },
            Node::BitPacked(p) => {
                let (lo, past) = search_ascending_capped(p.len(), |i| p.value_at(i), needle);
                (
                    lo,
                    lo + partition(past - lo, |i| p.value_at(lo + i) <= needle),
                )
            }
            _ => (self.lower_bound(needle), self.upper_bound(needle)),
        }
    }

    pub(crate) fn collect_kinds(&self, out: &mut Vec<crate::NodeKind>) {
        use crate::NodeKind as K;
        match self {
            Node::Primitive(_) => out.push(K::Primitive),
            Node::Constant { .. } => out.push(K::Constant),
            Node::Sequence { .. } => out.push(K::Sequence),
            Node::RunEnd { ends, values, .. } => {
                out.push(K::RunEnd);
                ends.collect_kinds(out);
                values.collect_kinds(out);
            }
            Node::FoR { child, .. } => {
                out.push(K::FoR);
                child.collect_kinds(out);
            }
            Node::ChunkedFoR {
                references, child, ..
            } => {
                out.push(K::FoR);
                child.collect_kinds(out);
                references.collect_kinds(out);
            }
            Node::Sparse { patches, .. } => {
                out.push(K::Sparse);
                patches.indices.collect_kinds(out);
                patches.values.collect_kinds(out);
            }
            Node::Rle {
                values,
                indices,
                value_offsets,
                ..
            } => {
                out.push(K::Rle);
                values.collect_kinds(out);
                indices.collect_kinds(out);
                value_offsets.collect_kinds(out);
            }
            Node::BitPacked(p) => {
                out.push(K::BitPacked);
                if let Some(patches) = p.patches() {
                    out.push(K::Patches);
                    patches.indices.collect_kinds(out);
                    patches.values.collect_kinds(out);
                }
            }
            Node::Slice { child, .. } => {
                out.push(K::Slice);
                child.collect_kinds(out);
            }
            Node::Chunked { chunks, .. } => {
                out.push(K::Chunked);
                for c in chunks {
                    c.node.collect_kinds(out);
                }
            }
            Node::Dict { codes, values } => {
                out.push(K::Dict);
                codes.collect_kinds(out);
                values.collect_kinds(out);
            }
            Node::Delta(d) => d.collect_kinds(out),
        }
    }
}

/// [`Node::value_at`] of a [`Node::ChunkedFoR`]: the child's value plus the
/// reference of its FastLanes chunk. Out of line on purpose: written in
/// `value_at`'s match, the sum of two recursive reads lets the compiler turn
/// the recursion into a loop carrying an accumulator, which every node visit
/// then pays for (a stack slot zeroed on entry, a load and an add on return).
#[inline(never)]
fn chunked_for_value_at(references: &Node<'_>, offset: usize, child: &Node<'_>, i: usize) -> u64 {
    child
        .value_at(i)
        .wrapping_add(references.value_at((offset + i) / FL_CHUNK))
}

/// The chunks of a [`Node::Chunked`] that the window `start..start + len`
/// reaches, in order, each as `(node, from, take, done)`: the window's
/// `take` values from the chunk's `from`, after the `done` values before
/// them. Requires `0 < len` and `start + len <= ` the chunks' length.
fn chunk_windows<'n, 'a>(
    chunks: &'n [Chunk<'a>],
    start: usize,
    len: usize,
) -> impl Iterator<Item = (&'n Node<'a>, usize, usize, usize)> {
    let first = partition(chunks.len(), |c| chunks[c].start <= start) - 1;
    let mut done = 0;
    chunks[first..].iter().map_while(move |chunk| {
        (done < len).then(|| {
            let from = start + done - chunk.start;
            let take = (chunk.node.len() - from).min(len - done);
            let window = (&chunk.node, from, take, done);
            done += take;
            window
        })
    })
}

/// The runs of a [`Node::RunEnd`] that the window `start..start + len`
/// reaches, in order, each as its offsets in the window and its value: the
/// first run found once, the rest walked.
fn runs_in<'n>(
    ends: &'n Node<'_>,
    values: &'n Node<'_>,
    offset: usize,
    start: usize,
    len: usize,
) -> impl Iterator<Item = (Range<usize>, u64)> + 'n {
    let (first, last) = ((start + offset) as u64, (start + offset + len) as u64);
    let mut run = partition(ends.len(), |r| ends.value_at(r) <= first);
    let mut at = first;
    std::iter::from_fn(move || {
        (at < last).then(|| {
            let end = ends.value_at(run).min(last);
            // Run positions below `last` fit the window's `usize`.
            let window = (at - first) as usize..(end - first) as usize;
            let item = (window, values.value_at(run));
            at = end;
            run += 1;
            item
        })
    })
}

/// Position `q` among `len` positions, clamped to `len`. A quotient the index
/// type cannot hold lies past every position: on a 32-bit target a `u64`
/// quotient above `u32::MAX` clamps rather than wrapping to its low bits.
/// Generic over the index type so a 64-bit host can test the narrow case.
fn clamp_to_len<I: TryFrom<u64> + Ord + Copy>(q: u64, len: I) -> I {
    I::try_from(q).map_or(len, |n| n.min(len))
}

/// [`Node::bounds`] of a RunEnd node: the needle's first run by one search of
/// the window's run values, both bounds from `ends`.
fn runend_bounds(
    ends: &Node<'_>,
    values: &Node<'_>,
    offset: usize,
    len: usize,
    needle: u64,
) -> (usize, usize) {
    let (first, span) = window_runs(ends, offset, len);
    let k = search_ascending(span, |k| values.value_at(first + k), needle);
    // Adjacent runs may repeat a value, so the run past the needle's is
    // searched for unless the next run already holds a larger value.
    let past = if k == span || values.value_at(first + k) != needle {
        k
    } else if k + 1 == span || values.value_at(first + k + 1) > needle {
        k + 1
    } else {
        k + 1
            + partition(span - k - 1, |j| {
                values.value_at(first + k + 1 + j) <= needle
            })
    };
    (
        run_start(ends, first + k, offset, len),
        run_start(ends, first + past, offset, len),
    )
}

/// The runs overlapping the window `[offset, offset + len)`, as
/// `(first_run, run_count)`. Sortedness is asserted for the window only, so
/// run searches must stay inside these runs: a sliced RunEnd keeps its full
/// children, whose out-of-window values are in unspecified order.
fn window_runs(ends: &Node<'_>, offset: usize, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let nruns = ends.len();
    // An unsliced node: its last run ends at its length, so every run lies in
    // the window (the ends ascend).
    if offset == 0 && nruns > 0 && ends.value_at(nruns - 1) == len as u64 {
        return (0, nruns);
    }
    let first = partition(nruns, |r| ends.value_at(r) <= offset as u64);
    let last = partition(nruns, |r| ends.value_at(r) <= (offset + len - 1) as u64);
    (first, (last + 1).min(nruns) - first)
}

/// First row of run `run` in the sliced coordinate space: runs before it end at
/// `ends[run - 1] - offset`, clamped into `[0, len]` so a moved offset or a
/// shortened length (a sliced RunEnd keeps its full ends child) stays in range.
fn run_start(ends: &Node<'_>, run: usize, offset: usize, len: usize) -> usize {
    if run == 0 {
        0
    } else {
        (ends.value_at(run - 1) as usize)
            .saturating_sub(offset)
            .min(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(base: u64, multiplier: u64, len: usize) -> Node<'static> {
        Node::Sequence {
            base,
            multiplier,
            len,
        }
    }

    #[test]
    fn sequence_bounds_closed_form() {
        // 5, 8, 11, 14
        let n = seq(5, 3, 4);
        for (needle, lo, hi) in [
            (0, 0, 0),
            (4, 0, 0),
            (5, 0, 1),
            (6, 1, 1),
            (8, 1, 2),
            (13, 3, 3),
            (14, 3, 4),
            (15, 4, 4),
            (u64::MAX, 4, 4),
        ] {
            assert_eq!(n.lower_bound(needle), lo, "lower({needle})");
            assert_eq!(n.upper_bound(needle), hi, "upper({needle})");
        }
        assert_eq!(n.value_at(2), 11);
    }

    #[test]
    fn quotients_past_the_index_width_clamp_to_len() {
        // u32 stands in for a 32-bit target's usize: a quotient past
        // u32::MAX is past every position, never its low bits.
        let len = 10u32;
        for q in [1u64 << 32, (1 << 32) + 3, (1 << 33) + 9, u64::MAX] {
            assert_eq!(clamp_to_len(q, len), len, "q = {q}");
        }
        assert_eq!(clamp_to_len(3, len), 3);
        assert_eq!(clamp_to_len(u64::from(u32::MAX), len), len);
        // The native width.
        assert_eq!(clamp_to_len(u64::MAX, 7usize), 7);
        assert_eq!(clamp_to_len(4, 7usize), 4);
    }

    #[test]
    fn sequence_value_at_wide_domain() {
        let n = seq(u64::MAX - 10, 2, 6);
        assert_eq!(n.value_at(5), u64::MAX);
    }

    #[test]
    fn constant_bounds() {
        let n = Node::Constant { value: 7, len: 3 };
        assert_eq!((n.lower_bound(6), n.upper_bound(6)), (0, 0));
        assert_eq!((n.lower_bound(7), n.upper_bound(7)), (0, 3));
        assert_eq!((n.lower_bound(8), n.upper_bound(8)), (3, 3));
    }

    #[test]
    fn for_underflow_guard() {
        // reference 100 over [0, 0, 2] -> logical [100, 100, 102]
        let child = Node::Primitive(Words::U64(&[0, 0, 2]));
        let n = Node::FoR {
            reference: 100,
            child: Box::new(child),
        };
        assert_eq!((n.lower_bound(50), n.upper_bound(50)), (0, 0));
        assert_eq!((n.lower_bound(100), n.upper_bound(100)), (0, 2));
        assert_eq!((n.lower_bound(101), n.upper_bound(101)), (2, 2));
        assert_eq!((n.lower_bound(102), n.upper_bound(102)), (2, 3));
        assert_eq!(n.value_at(2), 102);
    }

    #[test]
    fn slice_clamp_rebase() {
        // child [1, 3, 3, 5, 7], window [1, 4) -> [3, 3, 5]
        let child = Node::Primitive(Words::U32(&[1, 3, 3, 5, 7]));
        let n = Node::Slice {
            child: Box::new(child),
            start: 1,
            len: 3,
        };
        assert_eq!((n.lower_bound(0), n.upper_bound(0)), (0, 0));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (0, 2));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (2, 3));
        assert_eq!((n.lower_bound(9), n.upper_bound(9)), (3, 3));
        assert_eq!(n.value_at(2), 5);
    }

    #[test]
    fn runend_offset_clamp() {
        // ends [4, 8, 10] over values [2, 5, 9]; sliced view offset=2 len=6
        // logical rows: positions 2..8 -> [2, 2, 5, 5, 5, 5]
        let ends = Node::Primitive(Words::U64(&[4, 8, 10]));
        let values = Node::Primitive(Words::U64(&[2, 5, 9]));
        let n = Node::RunEnd {
            ends: Box::new(ends),
            values: Box::new(values),
            offset: 2,
            len: 6,
        };
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (0, 2));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (2, 6));
        assert_eq!((n.lower_bound(9), n.upper_bound(9)), (6, 6));
        assert_eq!(n.value_at(0), 2);
        assert_eq!(n.value_at(2), 5);
        assert_eq!(n.value_at(5), 5);
    }

    #[test]
    fn runend_bounds_search_once_and_cross_repeated_values() {
        // ends [2, 3, 6, 7, 9, 12] over values [1, 4, 4, 4, 7, 9]: adjacent
        // runs repeat 4, which a RunEnd may hold although a writer merges
        // them. Logical rows: [1, 1, 4, 4, 4, 4, 4, 7, 7, 9, 9, 9].
        let ends = [2u64, 3, 6, 7, 9, 12];
        let values = [1u64, 4, 4, 4, 7, 9];
        let rows: Vec<u64> = values
            .iter()
            .zip(ends.iter())
            .scan(0u64, |start, (&v, &end)| {
                let run = (*start..end).map(move |_| v);
                *start = end;
                Some(run)
            })
            .flatten()
            .collect();
        // Unsliced, then three windows (one a single run, one ending mid-run).
        for (offset, len) in [(0usize, 12usize), (2, 6), (3, 4), (8, 1), (5, 0)] {
            let n = Node::RunEnd {
                ends: Box::new(Node::Primitive(Words::U64(&ends))),
                values: Box::new(Node::Primitive(Words::U64(&values))),
                offset,
                len,
            };
            let window = &rows[offset..offset + len];
            for needle in (0..=10).chain([u64::MAX]) {
                let want = (
                    window.partition_point(|&v| v < needle),
                    window.partition_point(|&v| v <= needle),
                );
                assert_eq!(
                    n.bounds(needle),
                    want,
                    "offset {offset} len {len} needle {needle}"
                );
                assert_eq!(
                    (n.lower_bound(needle), n.upper_bound(needle)),
                    want,
                    "offset {offset} len {len} needle {needle}"
                );
            }
        }
    }

    #[test]
    fn chunked_boundary_selection() {
        // chunks [1, 2, 2] and [2, 2, 3]: an equal run spans the boundary
        let a = Node::Primitive(Words::U32(&[1, 2, 2]));
        let b = Node::Primitive(Words::U32(&[2, 2, 3]));
        let n = Node::Chunked {
            chunks: vec![Chunk::new(0, a), Chunk::new(3, b)],
            len: 6,
        };
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (1, 5));
        assert_eq!((n.lower_bound(1), n.upper_bound(1)), (0, 1));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (5, 6));
        assert_eq!((n.lower_bound(4), n.upper_bound(4)), (6, 6));
        assert_eq!(n.value_at(4), 2);
        assert_eq!(n.value_at(5), 3);
    }

    #[test]
    fn words_needle_above_width_max() {
        let n = Node::Primitive(Words::U8(&[1, 2, 3]));
        assert_eq!(n.lower_bound(300), 3);
        assert_eq!(n.upper_bound(300), 3);
    }

    #[test]
    fn dict_bounds_through_permuted_values() {
        // codes [1, 1, 3, 3, 0, 2, 2] over values [5, 1, 7, 2]
        // -> logical [1, 1, 2, 2, 5, 7, 7] (sorted; values are not)
        let codes = Node::Primitive(Words::U8(&[1, 1, 3, 3, 0, 2, 2]));
        let values = Node::Primitive(Words::U32(&[5, 1, 7, 2]));
        let n = Node::Dict {
            codes: Box::new(codes),
            values: Box::new(values),
        };
        assert_eq!(n.len(), 7);
        assert_eq!((n.lower_bound(0), n.upper_bound(0)), (0, 0));
        assert_eq!((n.lower_bound(1), n.upper_bound(1)), (0, 2));
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (2, 4));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (4, 4));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (4, 5));
        assert_eq!((n.lower_bound(7), n.upper_bound(7)), (5, 7));
        assert_eq!((n.lower_bound(8), n.upper_bound(8)), (7, 7));
        assert_eq!(n.value_at(4), 5);
        assert_eq!(n.value_at(6), 7);
    }
}
