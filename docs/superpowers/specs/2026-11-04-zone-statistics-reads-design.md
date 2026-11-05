# Reads that use Vortex's statistics: design

Status: draft for review. Builds on #145 (a located run streamed, #140's memo removed). §13 lists what vortex-rdf built that Vortex already offers, and where the plan replaces it.

## 1. Goal

Make counts, offset reads and scans of bound patterns cheaper, on every store type (no index, by-copy, by-reference) and on both backends (file-backed and in-memory), by asking the statistics Vortex already keeps before reading rows.

Rules:
- **Reuse, don't re-invent.** Every "can this match?" or "does all of this match?" decision is a Vortex statistics proof (`falsify` or `satisfy`), evaluated by Vortex over Vortex's own metadata. vortex-rdf adds rules and statistics only through Vortex's extension points (`StatsRewriteRule`, `AggregateFnVTable`), and replaces the statistic structures it built itself (§13).
- **No query results kept.** Nothing a query computes is kept between calls (python/README.md, "Bytes & files"; docs/file-format.md §8).
- **Same answers.** Every read returns the same rows in the same order as today; only the work changes.

Motivation, from a local probe on #145 (4M quads by-copy, a 2M-row `(type, Taxon)` run over 4 graphs, `(? type Taxon g1)` = 500K matches; release build, warm cache):

| Graph layout in the run | Read | #145 today | zone maps + count on encoded data |
|---|---|---|---|
| alternating row by row | count / 32 offset reads / paging | 19 ms / 294 ms / 1,074 ms | 0.93 ms / 16 ms / 88 ms |
| 1,000-row stretches | same | 14 ms / 252 ms / 1,014 ms | 0.11 ms / 4.8 ms / 34 ms |
| 4 long stretches | same | 7.2 ms / 96 ms / 411 ms | 0.005 ms / 1.2 ms / 12 ms |

## 2. What Vortex already provides (verified on Vortex 0.90 and our files)

| Level | Vortex facility | In our files today |
|---|---|---|
| Whole column | Footer statistics, `VortexFile::file_stats()`: exact `Min`, `Max`, `Sum`, `NullCount` per top-level column | Present for the quad table's `s`, `p`, `o`, `g`; vortex-rdf never reads them |
| Whole array (in memory) | Array statistics (`statistics()`: min, max, is-constant, ...) computed once and cached on the array | Available on every in-memory column |
| Zone (8,192 rows) | Zoned layouts: per-zone `max`, `min`, `nan_count`, `null_count` | On every id column (`s`, `p`, `o`, `g`, `val`, `rid`) of every table: quad table, `index:posg`, `index:ospg`, `index:ref-p`, `index:ref-o` (`io/container/sources.rs`, `id_column_strategy`) |
| Whole file (pruning) | `VortexFile::can_prune(filter)`: the filter's falsifier against the footer statistics | Never called; level 1 below uses it |
| Zone pruning (file) | `LayoutReader::pruning_evaluation(range, expr, mask)` over the reader's cached zone map. The reader decodes its zone table once and caches, per bound filter tree, the falsifier and its zone mask; it keeps both private and hands back a row mask | Already used for filtered scans (`scan/file_filter.rs`) |
| Zone pruning (any table) | `ZoneMap::try_new(dtype, stats_table, aggregate_fns, zone_len, rows)` and `ZoneMap::prune(proof)`, both public: one bit per zone, for a falsifier or a satisfier | Not used; in-memory stores use vortex-rdf's own `CodeZones` instead |
| Proofs | `BoundExpression::falsify` (nothing in the scope matches) and `BoundExpression::satisfy` (every row matches), built from the rules registered in the session (`StatsRewriteRule`, `StatsSession::register_rewrite`; rules for one function are OR-ed). Built in: `x = c` is falsified when `c` lies outside min–max, `x ≠ c` when `min = max = c`. There is no built-in satisfier for `x = c`, and no reader evaluates satisfiers | — |
| Binding proofs | `StatBinder` and `bind_stats`: bind a proof's statistic references to any source (Vortex's own binders, for zone tables and footer statistics, are private) | — |
| Encoded data | Compute kernels (compare, then count of true values) that work on encodings (constant, run-end, dictionary, bit-packed) | — |
| Custom zone statistics | `AggregateFnVTable` (any return dtype, accumulators that may read encoded data), `AggregateFnSession::register`, `ZonedLayoutOptions::aggregate_fns`, editions (`EditionMember::aggregate`), `Accumulator` for in-memory computation | vortex-rdf already declares its own `vortexrdf` edition family for its root layout (`session.rs`) |

What Vortex does not provide: per-value counts in a zone. Min and max decide a zone only when it holds no matching row or nothing else. For graph columns, the custom zone statistic of §5, written into every store, records those counts, so mixed graph zones are answered without reading data too.

## 3. The decision ladder

Every read path asks, for the bound values it checks (one or more columns `x = c`), cheapest level first:

1. **Whole column.** Nothing matches: `VortexFile::can_prune` (file), or the falsifier bound to the column array's statistics with a `StatBinder` (in memory; footer values carried over at load). Every row matches: the satisfier of `x = c` (§3.1), bound the same way to the footer statistics or the array's. Index copies hold the quad table's values, so its footer statistics apply to them too. Example: on a single-graph store a graph check disappears entirely, and qEndpoint's start-up check (default-graph count against the store size) needs no read.
2. **Zones.** The column's Vortex zone map, asked for both proofs: the falsifier of `x = c` gives the zones where nothing matches ("none"); the satisfier gives the zones where every row matches ("all"). The rest are "mixed". Evaluated by Vortex:
   - **with `ZoneMap::prune`**, one bit per zone, for both proofs, on a Vortex `ZoneMap` of the column: on file, read from the column's zone table on first use (§12, point 2); in memory, the store's own (§4.4).
   - Filtered scans keep Vortex's own pruning (`pruning_evaluation`), unchanged.

   With several bound columns, a zone is "none" if any column says so, "all" if every column says so; a column that says "all" drops out of that zone's check.
3. **Mixed zones, recorded graph counts.** For a graph column, the zone's recorded graph counts (§5) give the count of `c` without reading data. A zone that recorded none (more than `K` graphs), and a run's partial first and last zones, go on to level 4.
4. **Mixed zones, encoded data.** Counted with Vortex's compare-and-count kernels on the encoded leaves, or vortex-rdf's encoded-search probe (`count_eq`, written for the probe experiment) where it is measurably faster (§8).
5. **Rows.** Read and checked only where the levels above leave rows undecided.

Levels 3 and 4 count a zone for one checked column; a zone that still has several columns to check after level 2 goes to level 5.

One component holds this ladder for a table and the columns a read checks: `ZoneDecider` (name open). Its backends:
- **File:** footer statistics (`VortexFile::can_prune`, and the satisfier on a one-zone `ZoneMap` of the footer's bounds), and a `ZoneMap` per checked column, read from its zone table on first use and kept with the file. That memory is bounded by the file's layout: two `u64` per 8,192-row zone of each column read.
- **In memory:** array statistics, and a Vortex `ZoneMap` per column (§4.4).

It answers: per zone, none / all / mixed; per zone, a count (from the zone size, or levels 3–5); and, for a row list, which rows are settled.

### 3.1 Rules vortex-rdf registers

Through `StatsSession::register_rewrite` on `VORTEX_SESSION`, Vortex's extension point for statistics proofs:
- **P1: a satisfier for `x = c`:** `min(x) = c AND max(x) = c`. Vortex 0.90 has none; vortex-rdf registers it and proposes it upstream (§12), then drops its own once Vortex ships one.
- **P2: proofs over the recorded graph counts (§5)**, for `g = c`: falsified when the zone recorded its graphs and `c` is not among them; satisfied when `c` is the only one. With the falsifier registered, Vortex's own pruning (every filtered scan, `pruning_evaluation`) drops mixed graph zones that don't hold `c`, on every read path, with no vortex-rdf code in the scan. It needs the proof to be written with Vortex's built-in scalar functions over the statistic's stored state (for instance `list_contains` over a list of the zone's graphs); P2's plan checks this first, and changes the state's type if needed. If a proof can't be written that way, P2 keeps the counts for the ladder only.

## 4. Read paths

### 4.1 Runs inside a sorted table (contiguous rows)

**File: located runs** (`located.rs`). By-copy patterns with a bound graph after a free position, such as `(? p o g)` in `index:posg`, and the subject-led runs every store answers from its quad table, such as `(s p ? g)`. The checked columns are the table's own, so the index's zone maps decide:
- **Count:** settled zones from their sizes, mixed zones by levels 3 to 5.
- **Offset window:** jump to the zone holding the window's first match by zone counts, then read from there.
- **Stream** (`match_code_chunks`): skip "none" zones, return "all" zones without checking rows, check mixed zones.
- **Cap:** P1 removes `GAP_ROWS` (8M rows) for table runs, since a long run no longer costs a full check per call. Reference runs keep it until P3 measures them.

**In memory: the residual compare over a subject's rows** (§6.3 of docs/matching.md). Same ladder on the base's zone map. Long subject ranges benefit; most fit in one zone.

### 4.2 In-memory index runs as contiguous runs

Today an in-memory by-copy run with a residual graph (`?P?G`, `?POG`) drops its serve plan and compares the graph at the run's base rows, scattered over the base. Instead, the run is checked inside the index component, where its rows are contiguous, as on file. This needs zone maps for the components' id columns, with the recorded graph counts (§5) on their graph columns:
- **From a file** (`from_file_in_memory`, `from_bytes`): the index children's zone tables, read with the rows.
- **From a build:** computed while the component's columns are compressed, with Vortex's accumulators.

Cost: about 16 bytes per 8,192 rows per id column, held only by in-memory stores, which hold everything in memory anyway.

### 4.3 By-reference runs: a graph column in the reference indexes

**Format.** Each reference index child stores the graph of the row it points to beside its `rid`: `{val, rid, g}`, still sorted by `(val, rid)`, written under the new `vortexrdf` edition (§5). The builder copies `g` from the quad it is indexing. The out-of-core build carries it in the spilled reference entries.

**File: by-reference runs.** A run checked against the graph (`(? p ? g)`, `(? ? o g)`, and the graph part of `(? p o g)`) reads its own contiguous `g` rows instead of the quad table at scattered rows. That column's zone maps and recorded counts (§5) decide it exactly as §4.1 decides a table run: counts, offset windows and streams, with no quad-table read for the graph. Checks against `p` or `o` (both bound) still read the run's rows against the other reference index, as today. That cost is inherent to by-reference indexes.

**Files written before** (no `g` column) keep today's path: the graph read from the quad table at each row.

**In memory:** index components carry the same column (§4.2), so in-memory by-reference runs check the graph the same way. Row ids of an in-memory index run that still go back to the base (a residual other than the graph) are decided per row by the base's zone map.

### 4.4 Whole-store filters (patterns no index serves)

**File: general-match counts** (term patterns from Python, JS and Java, and `countCodes` when no located run answers). Today Vortex's pruning drops "none" zones and the filter is evaluated on every other zone. Add the "all" side: zones where every bound value is the only one are counted from their sizes, without evaluating the filter. Implemented in vortex-rdf's count path through the same `pruning_evaluation` call with the negated predicates.

**In memory: the residual stage.**
- **Replace `CodeZones` and `runs_holding` with Vortex's `ZoneMap` and `prune`** (same data; the stats table read from the file at load or built with Vortex's accumulators). File and in-memory stores then prune with the same Vortex rules, and the "all" side comes for free.
- **Counts:** "all" zones counted from sizes; mixed zones counted on encoded data when only a count is asked.
- **Windows** (`PendingResidual`): jump over zones whose counts are known instead of comparing from the start.

### 4.5 Not changed

- Runs with nothing to check: the binary search already finds them exactly.
- General-match reads other than counts and windows: pruning already skips "none" zones, and comparing a uniform zone is cheap.
- No result of a query is kept: no memo (§6).

## 5. A per-zone graph count (Vortex custom zone statistic)

Written into every store from the new edition on (phase P2); there is no option to leave it out.

- **What:** a custom aggregate function `vortexrdf.value_counts` (name open) with an `AggregateFnVTable`. For a zone holding at most `K` distinct values (`K` = 16, an option serialized with the aggregate), it records each value with its row count, as a list of `(value u64, count u32)`. Past `K` it records null, so the table stays small.
- **Where:** in the zone maps of the graph columns of every table: the quad table, `index:posg`, `index:ospg`, and the reference indexes' new `g` column (§4.3). It's listed with the default aggregates in `ZonedLayoutOptions::aggregate_fns` for those columns in `id_column_strategy`. In memory it's computed with `Accumulator`.
- **Read:** the column's recorded counts, decoded once per open file from the zoned layout's auxiliary child and kept with the file's other metadata. That memory is bounded by the file's layout (about 12 bytes per graph per zone), like the zone maps Vortex's readers keep, and holds no query result. A zone table is written as one segment, so reading it on every call would decode every zone each time (about 40 µs on a 4M-row table in the spike, growing with the table). With it, a mixed zone's count of `c` needs no data read; only zones with a null entry fall back to §3 level 4. Counts and offset windows then cost one lookup per zone wherever zones record their graphs.
- **Registration and format:**
  - registered in `VORTEX_SESSION`'s aggregate registry;
  - declared as a member of a new `vortexrdf` edition (the next after `2026.10.0`, with `EditionMember::aggregate`), enabled for writing, so every store written from then on carries it.
- **Cost:** at most `K` × 12 bytes per 8,192-row zone of each graph column, plus the counting at write time (§9 measures both).
- **Compatibility:** a reader that doesn't know the aggregate refuses the file (vortex-rdf ≤ 0.14 does not allow unknown components), so new files need a vortex-rdf version that registers it. That is accepted: there is no requirement for older versions to read new files. Files written before the new edition keep reading; they lack the statistic, and the ladder skips level 3 for them.

## 6. No memo

Settled by the spikes (§10) for every store type. With the recorded graph counts (§5), and the graph column in the reference indexes (§4.3), graph checks of by-copy, by-reference and quad-table runs answer counts and offset reads close to what a memo gives, with no per-query memory. The counters (§7) show how many zones fall past `K` and still need their data read; if those turn out common, the lever is `K`, not a memo.

## 7. Counters and switch

- **Counters** in the existing `VORTEX_RDF_COUNTERS=1` set (Java `engineCounters()`), per backend:
  - checks settled by whole-column statistics;
  - zones settled "none" and "all";
  - mixed zones answered by recorded graph counts;
  - mixed zones counted on encoded data (past `K`, or partial), with their rows;
  - rows read for checks.

  Running the normal UniProt benchmark with counters on answers the question we couldn't send to the other agent: how often zones fall past `K` or need their rows read.
- **Ablation switch** `VORTEX_RDF_ZONES=0`, read once, like `VORTEX_RDF_LOCATE=0`: every path takes today's route, for the paper's comparisons and for equivalence tests.

## 8. Testing

- **Equivalence.** For every path in §4, on file and in memory, by-copy, by-reference and no index: counts, windows (pages of 1, 3 and 64), streams and offsets equal the term path's rows and order. These extend the existing `check_locate_codes` / `check_code_patterns` style. Fixture graph layouts: one graph only, long stretches, 1,000-row stretches, alternating rows, plus seven-row leaves.
- **Work done.** `observe!` events (as `LocatedBlock` in #145) pin that:
  - "none" zones read no rows;
  - "all" zones read no checked column;
  - a single-graph store's graph check reads nothing;
  - a by-reference graph check on a store with the reference graph column reads no quad-table graph.
- **Decisions.** Unit tests of the ladder against hand-made zone maps: none / all / mixed, several columns, partial edge zones, empty runs.
- **Encoded count.** `count_eq` checked against a plain count on every encoding fixture (`encoded-search/tests/common`, already written for the probe experiment). The same harness compares it with Vortex's kernels, to keep only the faster.
- **In-memory zone maps.** A store loaded from a file and the same store built in memory produce the same `ZoneMap`s, recorded graph counts included.
- **§5.**
  - The aggregate round-trips through write and read.
  - Reference children with the `g` column hold, at each `rid`, that quad's graph; both builders (in memory and out of core) agree.
  - Counts from recorded graph counts equal counts from the rows, on every fixture layout, including zones with more than `K` graphs.
  - A file written before the new edition reads as before, the ladder skipping level 3.
  - A file with it is refused by a session that doesn't register it, with the error naming the aggregate.

## 9. Benchmarks

The three probe stores (alternating, 1,000-row stretches, long stretches), plus a single-graph store. Each runs:
- **as:** file and in-memory; by-copy, by-reference and no index;
- **reads:** count, 32 spread offset reads, paging by 4,096, a stream, the first 64 rows;
- **memory:** anonymous memory after each phase;
- **writes:** file size and build time with the statistic, against #145.

Each phase is run with `VORTEX_RDF_ZONES=0` and without, reported against #145.

## 10. Spike: does the graph statistic make the memo unnecessary?

Run before P2, on the experiment branch (`exp/zone-counts`); its code is throwaway.

**Question.** With each zone's graph counts read from the file on every call (nothing cached), do counts and offset reads of long graph-bound runs cost about what a memo of per-zone counts costs, on every store type and graph layout?

**Probe.**
- A minimal `vortexrdf.value_counts` aggregate (§5) with `K` = 16: registered in the store session, declared in a draft `vortexrdf` edition, and written into the graph columns' zone maps.
- A fourth mode, `stats`, beside the experiment's `rows`, `zones` (encoded count) and `memo`. Mixed zones are answered from the recorded counts, read on each call through Vortex's layout reader (the zone-table rows of the zones the run covers). Zones past `K`, and partial edge zones, fall back to the encoded count.
- For by-reference runs, the same four modes over the quad table's graph zones, with the memo keeping per-block counts of the reference run.

**Stores.** 4M quads with a 2M-row `(type, Taxon)` run, as in §1, each written by-copy and by-reference, with four graph layouts in the run:
- alternating row by row, 4 graphs;
- 1,000-row stretches;
- 4 long stretches;
- a new graph every 100 rows, about 80 graphs per zone, past `K`: the case of one graph per source record.

**Measured, per store and mode.**
- Count, first and repeated; 32 one-row reads at spread offsets; paging through all matches by 4,096 rows; the first 64 rows.
- Anonymous memory after the reads.
- File size and build time with and without the statistic.
- All modes must return the same rows; checksums are compared as in the experiment.

**Decision.**
- **By-copy and quad-table runs:** drop the memo for good if, on every layout with at most `K` graphs per zone, `stats` stays within 2× of `memo` for repeated counts and for the 32 offset reads.
- **Past `K`:** if the encoded-count fallback costs more than 5 ms for a count of the 2M-row run, test a larger `K` against the zone-table size it costs before deciding.
- **By-reference runs:** report the gap between `zones` and `memo`. If it is large, choose between:
  - accepting it;
  - a memo for reference runs only, bounded per file;
  - storing the graph beside `rid` in the reference indexes, a format change that lets their own zone maps and statistic answer.
- **Writes:** accept the statistic if file size grows by under 1% and build time by under 10%; otherwise revisit `K`.

The outcome updates §5, §6 and P2 before P2's plan is written.

### Results (2026-11-04)

Release build, warm page cache, 4-core VM; repeated figures are medians. Every mode returned identical rows (checksums compared) on all 8 stores.

**By-copy, `(? type Taxon g1)`** (repeated count / 32 offset reads / paging, in ms). "Zones only" is §3 levels 1, 2, 4 and 5, without the statistic. "Statistic" is the recorded counts decoded once per open file.

| Graph layout | Zones only | Statistic | Memo |
|---|---|---|---|
| alternating, 4 graphs | 1.06 / 15.4 / 70 | 0.018 / 2.8 / 31 | 0.003 / 1.0 / 14 |
| 1,000-row stretches | 0.054 / 2.7 / 26 | 0.014 / 2.5 / 29 | 0.003 / 1.9 / 24 |
| 4 long stretches | 0.007 / 1.4 / 13 | 0.019 / 2.1 / 17 | 0.004 / 1.5 / 15 |
| 80 graphs per zone (past `K`) | 0.45 / 10.8 / 16 | 0.50 / 10.0 / 19 | 0.003 / 2.8 / 15 |

Reading the statistic on every call instead (nothing kept) cost about 40 µs more per call: repeated counts of 0.04–0.05 ms.

**By-reference, `(? type ? g1)`**: the statistic does not apply. "Zones" decides each row by its quad-table zone and each block by its `rid` range.

| Graph layout | #145 today | Zones | Memo |
|---|---|---|---|
| alternating, 4 graphs | 17.8 / 317 / 1,278 | 14.4 / 247 / 949 | 0.001 / 4.9 / 39 |
| 1,000-row stretches | 20.7 / 400 / 1,441 | 22.0 / 362 / 1,406 | 0.001 / 3.8 / 47 |
| 4 long stretches | 11.8 / 158 / 583 | 0.61 / 6.3 / 37 | 0.001 / 1.7 / 17 |
| 80 graphs per zone | 20.4 / 335 / 123 | 23.6 / 379 / 120 | 0.001 / 8.5 / 39 |

**Writes:** files grew by 0.7–6.6 KB (under 0.01%); build times differed only within run-to-run noise.

### Verdict

- **Table runs: no memo.** Repeated counts stay within 15 µs of the memo, and offset reads and paging within 1.2–2.7×. The remaining gap is per-call overhead in the spike's read of the recorded counts, which the real implementation can trim. The statistic must be decoded once per open file (§5).
- **Past `K`:** counts fall back to the encoded count, 0.5 ms for the 2M-row run, under the 5 ms threshold, so `K` = 16 stands.
- **Writes:** well under both thresholds.
- **By-reference runs: the gap is large** without a format change. With mixed zones, a memo is about 10,000× faster on repeated counts and 50–100× on offset reads and paging. Only long graph stretches close most of it, through `rid` ranges. A second spike tested storing each row's graph beside its `rid` in the reference indexes, below.

### Second spike: a graph column in the reference indexes (2026-11-04)

Same by-reference stores and pattern `(? type ? g1)`, rebuilt with `{val, rid, g}` reference children, the statistic written into their `g` column too. Repeated count / 32 offset reads / paging, in ms:

| Graph layout | No `g` column, today | No `g` column, memo | `g` column, statistic |
|---|---|---|---|
| alternating, 4 graphs | 17.8 / 317 / 1,278 | 0.001 / 4.9 / 39 | 0.017 / 2.1 / 20 |
| 1,000-row stretches | 20.7 / 400 / 1,441 | 0.001 / 3.8 / 47 | 0.024 / 4.3 / 49 |
| 4 long stretches | 11.8 / 158 / 583 | 0.001 / 1.7 / 17 | 0.028 / 3.0 / 27 |
| 80 graphs per zone (past `K`) | 20.4 / 335 / 123 | 0.001 / 8.5 / 39 | 0.45 / 10.3 / 18 |

On the new stores themselves, a memo measured 0.001 / 2.0–3.9 / 20–42 on the first three layouts, and 0.001 / 2.1 / 16 past `K`.

**Writes.** Against the same by-reference stores with the statistic but without the column:
- **File size:** +2.0% with 4 graphs alternating row by row (2 bits per entry, bit-packed); +0.01–0.19% when graphs come in stretches.
- **Build time:** unchanged within noise (three builds each: 11.2 s without, 10.6 s with).

All modes returned identical rows.

**Verdict:**
- **Drop the memo everywhere.** With the column, by-reference graph checks match by-copy ones: repeated counts within 30 µs of a memo, and offset reads within 1.0–1.2× of it wherever zones hold at most `K` graphs.
- **Past `K`:** offset reads are about 5× a memo's (10 ms for 32 on the 2M-row run, against 335 ms today), and counts take 0.45 ms, under the 5 ms threshold. So `K` = 16 stands, and the counters (§7) watch it.
- **Not covered:** checks against both `p` and `o` keep their per-row cost (§4.3).

## 11. Phases (one PR each, on top of #145)

| Phase | Content |
|---|---|
| P1 | The ladder (file backend), footer statistics level, the encoded count, file located table runs (§4.1), counters and switch. From §13: the `x = c` satisfier rule (S3) and `VortexFile::can_prune` at level 1 (S4). Plan: `docs/superpowers/plans/2026-11-05-zone-reads-p1.md` |
| S | Done: the spikes (§10) settled §6 (no memo) and added the reference indexes' graph column (§4.3) |
| P2 | The per-zone graph-count statistic and its edition (§5): aggregate, writer, file reads through the ladder. From §13: its proofs registered as rules, so Vortex's own pruning uses it (S9), if they can be written with built-in functions |
| P3 | The reference indexes' graph column (§4.3): format under P2's edition, both builders, by-reference graph checks from it |
| P4 | Whole-store filters: file general-match counts (§4.4); in-memory residual stage on Vortex `ZoneMap` with the recorded graph counts, array-statistics level. From §13: `CodeZones`, `ZoneBuilder`, `ZoneTable` and `runs_holding` replaced (S1), quad columns' zone tables read as `ZoneMap`s (S2), min and max stamped as array statistics (S10) |
| P5 | In-memory index components with zone maps (and the reference graph column); their runs checked as contiguous runs (§4.2, §4.3). From §13: Vortex's compressor in place of the hand-picked encodings, if measurements allow (S11) |
| R | Replacements outside the statistics work (§13), independent of P1–P5, in one or two PRs. Straight swaps: R2, R3, R5 (comment), R7, R8, R10, E6 (= D9), D10, E14, E15. Measured first, each kept only if it isn't slower: R9, E3, E5, E10, E11, E12 |

The implementation plan covers P1; the spike can run beside it. Each later phase gets its own plan once the one before it has landed and been measured. R can go at any time; its plan is written separately.

The bug found in the audit (§13.3: `quads()` and the one-shot reads on Vortex's own scan stream) is fixed in its own issue and PR, ahead of these phases.

Docs updated with each phase:
- `docs/matching.md` §6.3, §8 and §10 (paths);
- `docs/file-format.md` (P2 adds the aggregate to the zone maps and the new edition; P3 the reference indexes' `g` column; P5 adds component zone maps to what an in-memory store keeps, §8);
- `docs/interfaces.md` (counters, switch).

## 12. Open points

1. **Vortex keeps a reader's zone map private.** Pruning calls reuse it. Per-zone values (§5) need the zone table decoded by vortex-rdf, a second copy, unless Vortex exposes the reader's zone map (§13.5).
2. **Settled while planning P1: zone maps, not row masks.** `pruning_evaluation` returns a mask with a bit per row of the range it is asked about, so deciding a run that way allocates and fills its run's length in bits on every call: 62 MB for a 500M-row run. P1 decides zones on a `ZoneMap` of each checked column instead (§3), read from the zone table once per file: a bit per zone per call. The cost is a second copy of those columns' zone tables beside a reader that has pruned them (§13.5).
3. **The in-memory switch to `ZoneMap`** must keep today's cost rule in `runs_holding` (narrow only when the zones left hold at most half the base), or replace it with one measured in P4.
4. **Name** of the `ZoneDecider` component and of the §5 aggregate.

## 13. Vortex APIs in place of our own structures

An audit of what vortex-rdf built that Vortex 0.90 already offers, checked against Vortex's sources. Verdicts:
- **Replace:** Vortex covers it; the plan moves to Vortex's API.
- **Partial:** part of it moves to Vortex; the rest stays, for the reason given.
- **Keep:** Vortex has no equivalent, or ours exists because of a measured gap in Vortex; it goes when Vortex closes the gap (§13.5).

### 13.1 Statistics and pruning

| # | vortex-rdf structure | Vortex in its place | Verdict | Phase |
|---|---|---|---|---|
| S1 | `CodeZones`, `ZoneBuilder`, `ZoneTable`, `may_hold`, `runs_holding` (`store/zones.rs`): in-memory zone bounds and the residual's zone pruning | A `ZoneMap` per column, read from the file's zone table at load or built with Vortex's `Accumulator`s, pruned with `falsify`/`satisfy` and `ZoneMap::prune` | Replace | P4 |
| S2 | `ExactZoneMap::read_bounds` (`io/zone_map.rs`): a zone table read into vortex-rdf's own `(min, max)` vectors | The same read, handed back as `ZoneMap::try_new(..)` for the quad columns. The dictionary keeps its window bounds: it binary-searches sorted windows, which `ZoneMap::prune` (a pass over every zone) would make slower | Partial | P4 |
| S3 | "Every row matches" decided by vortex-rdf's ladder | A satisfier for `x = c` registered as a `StatsRewriteRule` (§3.1), evaluated by Vortex's `ZoneMap::prune` | Replace (new rule) | P1 |
| S4 | Whole-column decisions read by hand from the footer statistics (planned in §3) | `VortexFile::can_prune` for "nothing matches"; the satisfier bound with a `StatBinder` for "every row matches" | Replace (plan) | P1 |
| S5 | `pruning_envelopes` (`native_file.rs`): 512 filter shapes mapped to the row range zone pruning leaves | Vortex's reader already caches each filter's zone mask; this memo saves expanding it to a mask of the file's rows (one bit per row, every call) and collapsing that to a range. Computing the range per call at zone level would need zone maps of every quad column of the file, more memory than the memo (§12, point 2) | Keep until Vortex exposes a reader's zone-level masks (§13.5) | — |
| S6 | `BoundExprMemo` (one bound filter tree per shape) and the `ReaderTree` retired when it clears | None: Vortex's reader caches are keyed by the bound tree's identity and never evicted, so vortex-rdf pins identities and retires readers to bound them | Keep | — |
| S7 | The quad table's `splits` kept once per file | `VortexFile::splits()` recomputes on every call | Keep | — |
| S8 | Per-zone graph counts decoded by vortex-rdf (§5) | No public accessor for a reader's `ZoneMap` or a `ZoneMap`'s values | Keep (decoded once per file) | P2 |
| S9 | Graph zones that don't hold `c` pruned only by vortex-rdf's ladder | Rules over the recorded graph counts (§3.1) make Vortex's own pruning exact for graph zones, in every scan | Replace (new rule), if a proof can be written with built-in scalar functions | P2 |
| S10 | Min and max computed while compressing in-memory columns, then dropped (`resident.rs`) | Stamped as exact `Stat::Min`/`Stat::Max` on the array, so its statistics answer level 1 in memory | Replace | P4 |
| S11 | In-memory columns' encoding chosen by hand: constant, run-end or bit-packed (`compress_int_column`) | `BtrBlocksCompressorBuilder::empty()` with the schemes the encoded-search probe reads. Today's pass also yields the zone bounds; a compressor needs a second pass for them | Measure first | P5 |
| S12 | `IsSorted` stamped on sorted columns | Already Vortex's `Stat::IsSorted` | Keep (already Vortex) | — |

### 13.2 Encoded search and point reads

Vortex 0.90's sorted search (`search_sorted`) has no per-encoding kernels any more: it is one bisection over point reads (`SearchSortedArray` with `Scalar`s, or the typed `SearchSortedPrimitiveArray<T>`), each read a `Scalar` through a probe that needs an `ExecutionCtx` and lives on one thread. A run-end column bisects its run ends on every read; a delta column decodes a 1,024-value block on every read and keeps nothing.

| # | vortex-rdf structure | Vortex in its place | Verdict | Phase |
|---|---|---|---|---|
| E1 | `SortedProbe`, `Node`, `resolve_node` (`encoded-search`): a typed `u64` probe tree with bounds, window bounds, single and bulk reads, and no `ExecutionCtx`, `Scalar` or allocation per query | `SearchSortedPrimitiveArray<T>`: about log n point reads per search (log n × log runs on run-end), no search pushed into an encoding (frame-of-reference shift, a sequence's closed form, a chunk's last value), no window search, no bulk read, and a probe that can't live in shared caches | Keep | — |
| E2 | `search.rs`: partition and an interpolating capped search | `search_sorted_by` returns a `Result` per comparison and doesn't interpolate | Keep | — |
| E3 | Bulk reads of bit-packed leaves (`PackedNode::read_into`) and delta blocks (`DeltaNode::decode_block`), one value at a time | `UnpackedChunks::decode_into` unpacks whole 1,024-value blocks. Vortex's own take switches to it past `unpack_chunk_threshold` (160 values per block for `u64`); located runs read up to 1,024 contiguous codes | Replace for reads past the crossover; the per-block cache stays | R |
| E4 | `OwnedSortedProbe`, a probe shared across threads | None: Vortex's probes are single-thread | Keep | — |
| E5 | `ColumnChunks`, `LeafRows`: each leaf's fetched array and probe kept once; bounds by bisecting the leaves' last values | Vortex caches segment bytes only (nothing to gain over a mapping), its flat reader decodes again on every read, and it has no sorted search across a layout. Leaves end on zone boundaries, so the bisection could start from the zone maps' maxima instead of fetching leaves for their last values | Keep; measure starting from zone maxima | R |
| E6 | `fetch_flat`: fetches a flat leaf's segment and decodes it, a copy of the private `FlatReader::array_future` (also D9) | A flat layout's reader with `projection_evaluation` over the root expression, as Vortex's own dictionary reader loads a leaf. It costs a reader per fetch; leaves are kept after their first fetch | Replace | R |
| E7 | `flat_leaves`, `collect_leaves` | `depth_first_traversal` and `child_row_offsets` carry no row offsets and don't pair a dictionary's codes leaves with its values | Keep | — |
| E8 | `ColumnChunks::take` | Already Vortex's `take`, per leaf | Keep (already Vortex) | — |
| E9 | `StructProbes` | None: Vortex keeps no probes | Keep | — |
| E10 | The decoded cache of in-memory code columns (`shared_code_primitive`) | Already Vortex's `Shared` array. One difference: vortex-rdf's probe keeps reading the compressed source after the decoded copy exists, where Vortex's switches to the copy | Keep; measure switching | R |
| E11 | Point reads by row id up to 256 rows (`file_reads.rs`, `rid_reads.rs`, `gather.rs`) | `ScanBuilder::with_row_indices`. The threshold's premise, that a scan decodes every row of a split, is weaker in 0.90: the flat reader filters first at low density, masks are pushed under compares, and sparse ids get tight split ranges | Keep; re-measure the threshold | R |
| E12 | Sparse reads by take (`take_file_rows`, `TAKE_ROWS` = 16) | Vortex has no take-based projection for sparse selections; its run-end filter now switches to take on its own under 0.1 kept rows per run | Keep; re-measure `TAKE_ROWS` | R |
| E13 | Typed residual loops (`typed_eq.rs`), behind measured row gates | A compare against a constant after a take or filter, which is already the fallback | Keep | — |
| E14 | `search_sorted_bounds`' fallback for columns the probe declines: the `Scalar` search | The typed `SearchSortedPrimitiveArray<T>` for unsigned columns | Replace | R |
| E15 | The probe bench's Vortex arm uses the `Scalar` search, and the crate README says Vortex decodes a whole run-end column per search (no longer so in 0.90) | Bench against `SearchSortedPrimitiveArray<T>` and fix the README, so the case for E1 rests on current numbers | Fix | R |
| E16 | Read counters (`encoded-search/src/lib.rs`) | `vortex_metrics::Counter`; the bindings read ours as process-wide statics | Keep | — |

### 13.3 Scans and file reads

| # | vortex-rdf structure | Vortex in its place | Verdict | Phase |
|---|---|---|---|---|
| R1 | Split drivers: `ScanDriver`, `read_all_rows`, `collect_row_splits`, `stream_rows`, `stream_split_reads` (`store/scan/file_scans.rs`), `inline_splits`, `spawned_splits` (`io/read.rs`). At most 42 splits run inline, more are spawned; inline when there is no tokio runtime | `RepeatedScan::execute_stream` spawns the same way, but always: it panics without a tokio runtime and has no split count to choose by. Measured: a by-copy store's replay took 4,408 ms with every split spawned against 3,404 ms with the limit, and 195 MB against 105 MB of anonymous memory | Keep | — |
| R2 | `AVAILABLE_PARALLELISM`, `SPLITS_IN_FLIGHT_PER_CORE` | `vortex_utils::parallelism::get_available_parallelism()` and the scan builder's default concurrency (4) | Replace | R |
| R3 | `collect_chunks`, `drain_chunks`, `collect_scan`, `chunked_or_single` | `ArrayStreamExt::read_all` | Replace | R |
| R4 | Whole-reader scans (`scan_all_reader` and its twins, index row-id scans) | Vortex's whole-scan helpers spawn or `block_on`; these reads must poll with no runtime (wasm, bytes, synchronous calls) | Keep | — |
| R5 | `RowSelection::restrict_scan` | Already Vortex's `with_row_range`, `with_selection`, `with_row_indices`. Its comment says Vortex can't intersect a row range with an index selection; 0.90 can | Keep (fix the comment) | R |
| R6 | The masks-first filter engine: `prune_mask`, `ConjunctOrder`, `evaluate_filter_split`, the split folds (`store/scan/file_filter.rs`) | Vortex's split filter (`FilterExpr`, `build_filter_mask`) is private, and a scan evaluates its filter only together with its projection. Measured: masks first took `code_chunks` for an `O` pattern from 74.6 to 23.0 µs; the column-rank prior for conjunct order, 2.92 to 1.66 ms; growing pieces for early exits, `exists` from 60 to 20 µs | Keep | — |
| R7 | `ReaderTree`'s per-component readers | The public `LazyReaderChildren` does the same | Replace (small) | R |
| R8 | The quad table's splits, kept once per file (S7), but not passed to vortex-rdf's own scans, so each scan walks the layout again | `ScanBuilder::with_natural_splits`, set in `FileReadScope::scan` | Replace (use) | R |
| R9 | Serve plans: a run's scan cut by `SplitBy::RowCount` | `SplitBy::LayoutSubSplitting { max_rows }` keeps the worker-aware size and aligns splits to chunks. The row-count split was chosen on Vortex 0.85; re-measure on 0.90 | Measure first | R |
| R10 | Serve plans' `columns_at` fallback: scans `first..last+1`, then takes the positions | `ScanBuilder::with_row_indices(positions)` reads only the chunks that hold them | Replace | R |

**Found on the way, a bug rather than a re-invention** (read in the code, not reproduced yet). When the reads by id decline, `quads_vec` and `shared_quads_vec` read their rows with Vortex's own `ScanBuilder::into_stream` (`store/read/quads.rs`, `exact_chunks` → `scan_chunks`) instead of the store's driver (R1). That stream always spawns its work on the session's tokio handle. docs/file-format.md §8 lists these one-shot reads among the scans that run up to 42 splits inline and run inline without a tokio runtime; instead they ignore the limit, and a poll outside a runtime panics, the failure commit c4be435 fixed for the other scans. `quads()` streams the same way, as §8 says, so it also panics outside a runtime. The fix reads them through the store's driver (`collect_row_splits`, `stream_split_reads`), keeping each chunk's decode in its split task. It belongs in its own issue and PR, not in this plan's phases.

### 13.4 Dictionary, container and indexes

| # | vortex-rdf structure | Vortex in its place | Verdict | Phase |
|---|---|---|---|---|
| D1 | The store root layout `vortex-rdf.store.v2`: the quad table plus named children with their own rows | A custom layout with auxiliary children is Vortex's extension point. The closest built-in, the `vortex.indexed` prototype, has the same shape but ignores unknown index kinds (the store must refuse a required child it doesn't know), and holds neither the dictionary nor `quads_sorted` | Keep | — |
| D2 | The children's inventory: sortedness and roles | File statistics drop `IsSorted`, and file metadata segments sit outside the layout tree | Keep | — |
| D3 | `RdfStoreWriteStrategy` | `IndexedStrategy` builds indexes from the data stream and holds them in memory; the store's index children are sorted streams from the out-of-core merge | Keep | — |
| D4 | `id_column_strategy` (`io/container/sources.rs`): step for step `WriteStrategyBuilder::build()`, with two leaf targets (1 MiB for dictionary codes, 2 MiB for plain `u64` columns) | `WriteStrategyBuilder` with `with_data_block_target_bytes` and `with_field_writer`, if both columns could share one target. But the builder has no option for zone aggregates, which P2 needs on graph columns; the copy is where P2 lists them | Keep until Vortex's builder takes zone aggregates and a separate target for dictionary codes (§13.5) | — |
| D5 | The dictionary child's strategy, the exact-bound zone aggregates, the `vortexrdf` edition family | Already Vortex's own strategies, aggregates and editions | Keep (already Vortex) | — |
| D6 | `TermDictionary`: one sorted dictionary shared by every code column, a term's code its rank | `DictLayout` works per column and caps entries at 65,535; `DictArray` has no value→code lookup; `DictEncoder` shares codes but is unsorted and in memory only | Keep | — |
| D7 | FSST windows, decoded one term at a time into a reused buffer | FSST's own `scalar_at` allocates per call, which a binary search pays on every step. Windows are built with Vortex's `fsst_train_compressor` and `fsst_compress` | Keep | — |
| D8 | The file-backed dictionary's `WindowCache` (4 MiB) | Vortex's segment cache holds raw bytes, which the mapping already gives; Vortex keeps no decoded arrays | Keep | — |
| D9 | `fetch_flat` (`encoded-search/src/layout.rs`): fetches a flat leaf's segment and decodes it, as Vortex's private `FlatReader::array_future` does | A flat layout's reader with `projection_evaluation` over the root expression (the same change as E6). A reader per fetch costs little beside a window's rebuild, and leaves are kept after their first fetch | Replace | R |
| D10 | `ExactZoneMap::of` finds the zone table at slot 1 | Match the child whose type is the `zones` auxiliary child | Replace (small) | R |
| D11 | Reference children `index:ref-p`, `index:ref-o` and their resolvers | The `vortex.indexed` prototype has this shape (its own `ExactValueIndex` test kind), but is in no writable edition, locates rows with 32-bit bitmaps, decodes whole chunks (cached without bound) where the store bisects encoded ones, returns masks rather than runs (no counts from a run's width, no windows), builds in memory, and plans one conjunct at a time | Watch: revisit when Vortex closes those gaps. P3's `g` column stands meanwhile | — |
| D12 | Copy children `index:posg`, `index:ospg` and their serve plans | `vortex.indexed` always reads the data child for projections, so it can't serve a match from the index's own rows. The serve plans already use Vortex's row ranges, filters and `row_idx()` | Keep | — |

### 13.5 What to propose upstream

Each "keep" above that exists because of a gap in Vortex, with what Vortex would need. When Vortex ships one, vortex-rdf drops its own code for it.

| Gap in Vortex 0.90 | What vortex-rdf drops once it's closed |
|---|---|
| A built-in satisfier for `x = c` (`min = c AND max = c`), and a file-level satisfier check beside `VortexFile::can_prune` | Its own rule (§3.1) and the satisfier binding for footer statistics (S4) |
| Access to a zoned reader's `ZoneMap`, and to the values a `ZoneMap` holds per aggregate | The second copy of the graph columns' zone tables (S8; §12, point 1) |
| Readers that evaluate satisfiers, or return zone-level masks | The second copy of checked columns' zone tables (§12, point 2), and `pruning_envelopes` (S5) |
| A public in-memory zone-map builder (the zoned writer's accumulator and stats-table dtype are crate-private) | Building in-memory zone maps from `Accumulator`s by hand (S1) |
| Reader caches that are bounded or keyed by structure, or a way to clear them | `BoundExprMemo` and the reader tree's retirement (S6) |
| `VortexFile::splits()` kept once per file | The splits kept by the file handle (S7) |
| A scan driver that runs inline without a tokio runtime and exposes its split count | The store's split drivers (R1) |
| A public split filter (`FilterExpr`) with an order hint, and a mask-only split execution | The masks-first filter engine (R6) |
| Zone aggregates and a separate leaf target for dictionary codes in `WriteStrategyBuilder` | The copy of its pipeline, `id_column_strategy` (D4) |
| A persisted, searchable dictionary shared across columns | The term dictionary (D6), and plain Vortex readers would see terms instead of codes |
| `vortex.indexed`: a writable edition, 64-bit row locators, probes on encoded chunks, runs as results, streaming builds, plans over several conjuncts | The reference children's own resolvers (D11) |
| Typed native reads in probes, a per-encoding search hook (`OperationsVTable`), and probe state that can be shared across threads | The encoded-search probe tree (E1, E4) |
| A public block decode (`decompress_primitive` is crate-private) | The probe's own decode of delta blocks (E3) |
| Decoded leaves kept by flat readers, and a sorted search across a layout | `ColumnChunks`' leaf cache and leaf bisection (E5) |
| A take-based projection for sparse selections | Sparse reads by take (E12) |
