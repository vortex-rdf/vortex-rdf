# The interfaces a query engine reads the store through

This document is the contract between `vortex-rdf-core` and a query engine
built outside this repository — a SPARQL planner on Apache DataFusion, say,
that treats a store as a set of tables of term codes. The engine owns
parsing, algebra, planning and execution; this crate owns the data: how a
view's rows stream out as columns, which runtime a read needs, what a view
promises about itself before it is read, which identities tell the engine
when its cached codes and plans stop applying, and how a store file opens
through vortex's own reader. Nothing here depends on DataFusion; core's own
tests pin each interface
([`tests/chunks.rs`](../core/src/tests/chunks.rs),
[`tests/metadata.rs`](../core/src/tests/metadata.rs),
[`tests/mapping.rs`](../core/src/tests/mapping.rs)).

| What the engine needs | What it calls |
|---|---|
| Rows as columns, without Arrow | [`row_chunks`](../core/src/store/read/chunks.rs#L85), [`code_chunks`](../core/src/store/read/chunks.rs#L121) |
| Row counts, sort order, code bounds, partitions, what answered a match | [`view_statistics`](../core/src/store/read/metadata.rs#L160), [`index_components`](../core/src/store/read/metadata.rs#L232), [`partitions`](../core/src/store/query/partitions.rs#L29) |
| Term ↔ code, term predicates, kind ranges, the default graph's code | [`DictReader`](../core/src/store/layouts/dictionary/handles.rs#L246) (from [`dict_reader`](../core/src/store/mod.rs#L193)), see [matching.md §16.4](./matching.md#164-predicates-and-the-dictionary-handle) |
| Term-level constraints pushed below a pattern | `keep`, `window`, `size_capped`, `exists`, `match_many` — [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern) |
| When codes and plans stop applying | [`generation`](../core/src/store/read/metadata.rs#L149), [`DictReader::dictionary_id`](../core/src/store/layouts/dictionary/handles.rs#L270) |
| A store file through vortex's own reader | [`vortex_session`](../core/src/lib.rs#L80) ([§6](#6-a-store-file-through-vortexs-reader)) |

---

## 1. Rows as columns, without Arrow

Two streams read a view's rows as chunks, lazily (nothing is gathered or
scanned until the first poll), in the view's order, every chunk holding at
most `batch_rows` rows: a served view's (a match a `secondary-by-copy`
index answers, and every window, keep, partition and chained match of one)
in that index's order, any other view's in base row order. `sort_order` in
`view_statistics` says which ([§3](#3-what-a-view-promises-before-it-is-read)):

- `row_chunks(batch_rows)` — struct chunks in the layout's primary dtype;
  under a string layout the tail's live rows follow the base.
- `code_chunks(columns, batch_rows)` — `u64` code columns
  (`Buffer<TermCode>`), one buffer per requested `QuadColumn` in the
  requested order; Dictionary layout, empty tail. In memory only the
  requested columns are read: decoded into their shared caches, or gathered
  from the base cut to them.

In memory, chunks are exactly `batch_rows` long but the last of the base
(and of the tail); a file-backed view's scan chunks follow the file's
splits, cut to the cap. In memory a contiguous selection without tombstones
is sliced from the base, zero-copy over its canonical code columns, and any
other selection is gathered one window of `batch_rows` live ids per pulled
chunk, both reading only the requested columns. On file a selection taken
by id is taken one window of ids per pulled chunk; the scan reads the file
split by split, a bounded window of splits in flight ahead of the pulls
([§2](#2-which-runtime-a-read-needs)); and a selection small enough for
point reads is read at once, at the first poll. The scan, and a read by id
with no filter pending, read only the requested columns. A served view's
chunks come in its index's order, and its row ids are never sorted: in
memory they are gathered off the base at the run's row ids, one window of
ids per chunk, or, over a Dictionary base whose columns stay encoded (a
store read off a file or adopted from bytes, which every take decodes
again), read off the index component, a window of the run's live rows per
chunk; on file they are the index child's own chunks (its located
range, its kept rows' positions, or its filtered scan), except a
TypedObject store's `row_chunks`, taken from the base a window of ids at a
time.

`code_columns_gathered` is the one-shot form, for a match the caller has
counted and will consume whole: one call answers all four columns,
zero-copy over the base's canonical code columns for a contiguous selection
without tombstones. On a file view whose pending filter no read by id
serves, it evaluates the filter's masks split by split first and then reads
only the rows they keep, point by point, by one take, or by projecting each
split at its mask. The streams read such a view the same way, a split at a
time as they are pulled. The chunk stream is the form for a match consumed
incrementally or of unknown size, and the bindings reach it through
`match_code_chunks` (Python) and `matchCodeChunks` (JS and Java), and
Java's `iterateQuads`, Python's `iter_quads` and JS's `match()` as terms, a
chunk decoded at a time. A Python `match_codes` call given a cancel token
returns its result when its gather completes before the cancel is observed,
and raises `Cancelled` when the token is cancelled before the call or while
the call waits for data.

A `CodePattern` streams through
[`match_code_chunks`](../core/src/store/query/located/mod.rs#L159):
`match_code_pattern`'s rows in its order, as chunks of all four code
columns. A located run that answers the pattern is read a block of
`batch_rows` of its rows at a time, the rows each block keeps checked as it
is read, so the run is read once and the stream holds only its place in it;
any other pattern streams the general match's `code_chunks`. Java's
`matchCodeChunks` is built on it. Two calls let a binding build its own
chunked read of a pattern:
[`code_probe`](../core/src/store/query/located/mod.rs#L187) turns a `CodePattern` into
the `Probe` that `match_code_pattern` runs when no located run answers it
(`None` when no quad can match), for `run_probe` and then `code_chunks`;
and [`decode_row_chunk`](../core/src/store/read/chunks.rs#L148) decodes one of
a string layout's `row_chunks` into `SharedQuad`s, each distinct term of the
chunk once, as `shared_quads_vec` decodes it (a Dictionary store's chunks
hold codes, which `dict_reader` decodes). Java's `iterateQuads` is built on
the second for a store without a dictionary.

**To Arrow.** vortex converts a chunk through its own session:
`vortex_session().arrow().execute_arrow(chunk, None, &mut ctx)` (the
`ArrowSessionExt` of `vortex-arrow`) yields an Arrow `StructArray` whose
code columns are `UInt64` (a canonical column converts without a copy) and
whose `utf8` columns are `Utf8View`; `to_arrow_schema(chunk.dtype())`
gives the matching schema, and `RecordBatch::from(struct_array)` the batch.
An engine also builds two encodings *from* codes:

- **Codes as ids.** `Int64` is the zero-copy id form: a `code_chunks`
  buffer reinterprets as an Arrow `Int64` column without a copy
  (`into_byte_buffer().into_arrow_buffer()`, from vortex-buffer's `arrow`
  feature, as a `ScalarBuffer<i64>`), bit-identical to the codes below
  2^63. `Int32` ids cost a narrowing copy, valid while the dictionary holds
  at most `i32::MAX` terms (`DictReader::len`), which the build guarantees
  but the format does not.
- **Terms.** `DictReader::decode_many(codes)` decodes a batch's distinct
  codes once; the engine builds a dictionary-encoded Arrow array (keys = the
  batch's code ranks, values = the decoded spellings) or a plain string
  array.

**The default graph.** Its spelling is the empty string, so it is code 0
exactly when any quad is in the default graph
(`DictReader::kind_ranges().default_graph`); an engine that wants `NULL`
for it maps that code.

## 2. Which runtime a read needs

Every read is lazy, and a stream's first poll decides how it runs:

- An in-memory view's reads — `row_chunks`, `code_chunks` — are CPU work on
  the polling task, and need no runtime.
- A file-backed view reads its rows by id or scans the file's splits, as
  it decides at that first poll. A read by id, a scan of at most 42 splits
  ([`MAX_INLINE_SPLITS`](../core/src/store/scan/file_scans.rs#L65)), and a
  scan first polled where no tokio runtime is current run inline on the
  polling task. Any other scan spawns its splits
  ([`ScanDriver`](../core/src/store/scan/file_scans.rs#L29)) onto the tokio
  runtime it is polled in, and vortex looks that runtime up
  (`Handle::current()`) at every spawn, so every later poll of the stream
  must run inside a tokio runtime too. The reads that decode quads from a
  scan — `quads()` and its twins, `quads_vec`, `shared_quads_vec` and the
  N-Triples/N-Quads export — spawn every split, however few, and under a
  file-backed dictionary every chunk's decode, whenever that first poll
  finds a runtime, and run inline without one
  ([file-format.md §8](file-format.md#8-reading-a-file)). A filtered file view's capped count,
  window and one-shot reads evaluate the filter split by split under the
  same rule. Its uncapped count, a delete's search for the rows it
  tombstones and a `keep`'s pass spawn their splits, as `quads_vec` and
  `shared_quads_vec` spawn their split reads, whenever there is more than
  one and a runtime is current: each such call, once it has spawned, must
  be polled to completion inside that runtime.

An engine that polls on tokio, as DataFusion does, meets every one of
these. A consumer polling outside tokio gets the inline driver on a view's
scan.

## 3. What a view promises before it is read

`view_statistics()` answers without I/O; a count that would need a scan is
a bound, a code range that would need a read is `None`. It returns a
`Result`: on an in-memory view whose selection is a pending index run over
a base with tombstones, it decodes the run's row ids to count its live rows
and caches them on the view — no I/O, but not a pure read, and that decode
is the one way it fails.

| Field | Meaning |
|---|---|
| `rows: RowCountHint` | `exact` when the count is known without reading: every in-memory view, and every file view without a pending filter except one whose index run is unread and of unknown width or under tombstones; a filter a window has evaluated, finding it cuts no row, counts as none. Otherwise `upper_bound` only: the rows a pending filter has yet to test, or an unread index run's width, or the base's rows when that width is unknown. The tail's live rows count in both. |
| `sort_order` | The order every read of the view follows (`quads`, `row_chunks`, `code_chunks`, the exports), when it is a known sort and the view reads no tail row. `Some(Posg)` for a view the `index:posg` copy serves with the object unbound (`?p??`, and on file `?p?g`) and for its windows, keeps, partitions and chained matches; `Some(Spog)` for any other view over a globally sorted base (every base this crate writes), a served one's index order being base order within its bound terms. `None` for a view that reads tail rows (they follow the base in append order) and for a served file view whose index child is not globally sorted. |
| `code_bounds[4]` | Per column, the inclusive `TermCode` range the view's base rows lie in when it is free to know: a subject run's first and last code (in memory, over a tombstone-free contiguous run of a sorted base); the single code of each column a file view's pushed-down filter binds by equality, while the view still carries the filter (pending, or evaluated by a window that kept its masks). |
| `selection` | `All`, one `Range`, explicit `Ids`, or a `PendingIndexRun` an index resolved but never materialized. |
| `served_component` | The index child that answered the match, if any (`index:posg`, …): every read of the view, and of its windows, keeps, partitions and chained matches, follows that child's order. |
| `pending_filter` | Whether every read runs a pushed-down file filter. A filter a window has evaluated over every row, finding it cuts none, is not pending: its matches are counted exactly and read without running it again, and the view's selection reads as `Ids`. |
| `tombstones`, `tail_rows` | Rows tombstoned in the base (never counted) and live rows in the append tail (counted, read last). |
| `file_backed`, `generation` | Where the base lives, and the data's identity ([§5](#5-snapshots-and-mutation)). |

`index_components()` lists the store's index children, each with its
component name, its `IndexType`, its sort order (`Posg`, `Ospg`, or none
for the reference pairs, whose rows are in `(val, rid)` order), and `rows: usize`, the
base's row count, since every index child holds one row per base quad. It returns a `Result`, and it classifies
a child by its implementation slug, leaving out one under a slug this
version does not know. On a view a match or a narrowing proved empty the
backends differ: in memory the view holds no children and lists none; on
file it lists the file's children, as every file view does.

`partitions(n)` cuts a view into at most `n` views over disjoint,
contiguous pieces of its rows, in the view's order: reading them in turn
reads the view. A served view is cut in its index's order into served
windows of about equal live rows, each keeping the view's `sort_order`.
Any other view is cut in base order, each partition applying the view's
filter and tombstones to its own rows only, its size about equal *before*
filtering. For `n` = 1 the one partition is the view itself, its serve plan
and pending selection included. The append tail rides with the last
partition, so every partition but the last keeps the view's order, and the
last keeps it only when the view reads no tail row; a view selecting no
base row is its own one partition. Each partition is an ordinary view, so
an engine reads one per worker through its chunk streams.

## 4. Narrowing natively

Everything in [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern)
is the engine's pushdown vocabulary: `keep`/`keep_many` (a code set or range
per column, applied through the store's probes, zone maps or a scan
conjunct), `window`/`size_capped`/`exists` (`LIMIT`, `OFFSET`, `ASK` without
reading past the answer), `match_many`/`count_many` (a batch of probes;
`run_probe`/`count_probe` for one), and the dictionary handle's
`encode_many`, `prefix_range`, `filter_codes` and `kind_ranges`.
`encode_many` looks its terms up in byte order, each search running only
past the one before, and reads each window of a file-backed dictionary once
for all the terms it holds.
`filter_codes(predicate, candidates)` answers `(passed, undecided)`, both
ascending subsets of the candidate codes: a candidate is decided only where
the native rule is certain
([matching.md §16.4](./matching.md#164-predicates-and-the-dictionary-handle)),
and one in neither set fails. An engine keeps `passed` (as a `keep`),
evaluates `undecided` itself, and drops the rest.

The engine's rule of thumb: resolve the pattern with `match_pattern`, turn
term-level constraints into `keep`s (a `VALUES` block, the codes a
predicate's `filter_codes` passes, a namespace's `prefix_range`), apply
`window` for `LIMIT`/`OFFSET`, and read the resulting view's chunks
([§1](#1-rows-as-columns-without-arrow)), filtering only what is left.

An engine that already holds term codes, as a nested-loop join's
per-binding probe does, asks `count_code_pattern(CodePattern)` and
`match_code_pattern(CodePattern, offset, limit)`. When the bound codes are a
prefix of a sorted table's key order (the quad table's `(s, p, o, g)`, the
copy index's `index:posg` `(p, o, s, g)` or `index:ospg` `(o, s, p, g)`),
one located run answers the match's size and the window's code columns, with
no dictionary search, view or gather, in that table's order. With the
reference index, a bound `p` or `o` is a run of `index:ref-p` or
`index:ref-o` listing quad-table rows: the shorter one answers, in
quad-table order, and any other bound slot is checked at its rows. A bound
graph is checked on the reference child's own rows, where the child holds
each row's graph (`g`, in files written from this version on): as the run's
only check, it is decided a zone at a time from those rows' zone maps and
recorded graphs, as below, at any length. Otherwise, and in files written
before, the run is checked row by row, for runs of at most 8,388,608 rows
(`GAP_ROWS`). With a free predicate, a bound subject's run of a
subject-sorted quad table (below) is read instead unless a shorter
reference run answers in its place, so `s?o` reads the subject's few rows
rather than a common object's many, and a rare object's few rather than a
hub subject's many.
Any other pattern goes through the general match of the codes' terms. Which of the two answers depends on the
store and the pattern alone, so every window of one pattern comes in the
same order. A code in a position its term kind cannot take matches nothing.

Bound slots that are no such prefix, a bound slot after a free one
(`(? p o g)` on `index:posg`, `(s p ? g)` or `(s ? o ?)` on the quad table),
are the run of the table's longest bound prefix, its rows checked against
the slots past it, unless a reference run is strictly shorter. Those checks
are decided a zone at a time, by the checked columns' exact zone bounds (no
row holds a code outside them, every row holds one both equal) and, for the
graph, by the graphs each zone records. A zone where no row can hold the
checked code is
skipped, a zone where every row holds it is taken whole without reading
the column, and only the zones holding it among other codes are read. A
count takes such a zone's count from the graphs it records when the graph is
the one checked slot left in it and the zone lies whole within the run,
counts it on the column's encoding otherwise when one checked slot is left,
reading no rows either way, and checks its rows when two are; a window finds
its first zone from those counts and reads from there. A graph column
records each zone's graphs with their rows up to 16 of them (files written
since the `vortexrdf 2026.11.0` edition); a zone of more, or a file written
before, keeps its bounds alone.
A run shorter than a zone checks its few rows directly, as a join's probe of
a row or two is answered faster that way. A longer run asks the statistics
only when a read needs them: a first window that the run's first 4,096 rows
fill, such as a 64-row first page, is read from those rows without them.
The general match, which no located run answers, uses the same statistics.
On file, a filter of code equalities takes the zones where every row holds
every code without evaluating itself, and prunes the rest by the zones'
bounds and recorded graphs. In memory, the residual compare asks the
columns' whole bounds first, then classes the zones it touches once:
it skips the zones that cannot hold its codes, takes the zones that hold
only them, and compares the rest; a count takes a mixed zone from its
recorded graphs or the column's encoding, and a window skips the zones
before it by their counts.
`VORTEX_RDF_ZONES=0` (read once)
checks every row instead, and leaves the general match's zone pruning to the
bounds, without the recorded graphs.
With `VORTEX_RDF_COUNTERS=1` the engine counters count a located run's
decisions, not the general match's or the residual compare's: the slots the
footer settles (`zone_column`), the zones settled none and all (`zone_none`,
`zone_all`), the mixed zones counted from their recorded graphs
(`zone_recorded`) or on their encoding (`zone_counted`) and the rows read to
check (`check_read`: a run's span at its first check, the rows kept so far at
each later one, and a reference run's rows at the quad table), each with its
rows.

`locate_codes(CodePattern, offset, limit)` is the located run alone: it
answers only a whole file-backed Dictionary store (no tail, tombstones or
narrowing) and returns `None` otherwise.

## 5. Snapshots and mutation

A `VortexRdfStore` value is a snapshot: views share an immutable base, and
every read of a view sees the same rows. `add_quads`, `delete_quad`,
`delete_matching` and `compact` return new stores and leave the old ones
valid.

- `generation()` identifies the data behind a view. Views derived by
  `match_pattern`, `keep`, `window` and `partitions` keep it; a build, an
  open, `add_quads`, a delete and `compact` each take a new one from one
  process-wide counter (an `add_quads` that adds nothing returns the store
  as it is). Views of one generation select from the same base rows, codes
  and tail — the key for cached plans and statistics. A generation is never
  persisted.
- `DictReader::dictionary_id()` (and `DictSnapshot::dictionary_id`)
  identifies the code vocabulary. Every view of one store shares it; a
  `compact` re-encodes against a new dictionary and changes it, as does
  opening the file again. Equal ids promise equal codes; different ids
  promise nothing, so an engine that interns codes re-encodes its constants
  when the id changes.
- Under the Dictionary layout an append tail holds strings whose terms have
  no code: `row_chunks`, `code_chunks` and `keep` refuse such a view, and
  `code_columns_gathered` and `dict_reader()` answer `None`. A
  transactional engine therefore compacts on commit, or serves strings
  (`quads`, `row_chunks` on a string layout) while a tail exists, and
  re-takes the dictionary handle after the compaction.
- A store opened by [`from_file`](../core/src/store/persist/open.rs#L261)
  reads its memory-mapped file in place, and so does every view, partition
  and stream derived from it: the file must not be truncated or rewritten
  while any of them lives. Replacing it by a rename
  (as `compact` and the file writers do) is fine on Unix, where the mapping
  keeps the old file; Windows refuses to rename over a mapped file.
  [`from_file_in_memory`](../core/src/store/persist/open.rs#L271) loads the
  file whole instead and never reads it again.

## 6. A store file through vortex's reader

[`vortex_session`](../core/src/lib.rs#L80) holds this crate's store layout,
encodings and write editions. vortex's own reader opens a store file through
it, and the file scans as the quad table: the root hands its dtype, row
count and scan to child 0
([file-format.md §3](./file-format.md#3-the-store-root-vortex-rdfstorev2)),
so a scan reads `{s, p, o, g}` in base row order, `UInt64` codes under the
Dictionary layout, which the
[`DictReader`](../core/src/store/layouts/dictionary/handles.rs#L246) decodes.
That is the file's base alone: a view's narrowing, tombstones and tail live
in the store, and its rows come out through the streams of
[§1](#1-rows-as-columns-without-arrow). An engine on vortex's DataFusion
integration can register the file through vortex's own data source over it
(`VortexFile::data_source`); this repository tests the open and the scan
([`tests/mapping.rs`](../core/src/tests/mapping.rs)), not the integration.

## 7. Source map

| Concern | File |
|---|---|
| `count_code_pattern`, `match_code_pattern`, `match_code_chunks`, `code_probe`, `locate_codes`, `CodePattern`, `LocatedCodes` | [`core/src/store/query/located/mod.rs`](../core/src/store/query/located/mod.rs) |
| `row_chunks`, `code_chunks`, `decode_row_chunk` | [`core/src/store/read/chunks.rs`](../core/src/store/read/chunks.rs) |
| `view_statistics`, `index_components`, `generation` | [`core/src/store/read/metadata.rs`](../core/src/store/read/metadata.rs) |
| `partitions` | [`core/src/store/query/partitions.rs`](../core/src/store/query/partitions.rs) |
| The scan driver of a file view's reads | [`core/src/store/scan/file_scans.rs`](../core/src/store/scan/file_scans.rs) |
| `RowSelection::split` | [`core/src/store/view/selection.rs`](../core/src/store/view/selection.rs) |
| `DictReader`, `DictSnapshot`, `dictionary_id`, `filter_codes` | [`core/src/store/layouts/dictionary/handles.rs`](../core/src/store/layouts/dictionary/handles.rs), [`term_dict.rs`](../core/src/store/layouts/dictionary/term_dict.rs), [`file_backed/mod.rs`](../core/src/store/layouts/dictionary/file_backed/mod.rs) |
| `vortex_session` | [`core/src/lib.rs`](../core/src/lib.rs) |
| The interfaces' tests | [`core/src/tests/chunks.rs`](../core/src/tests/chunks.rs), [`metadata.rs`](../core/src/tests/metadata.rs), [`mapping.rs`](../core/src/tests/mapping.rs) |
