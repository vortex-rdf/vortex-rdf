# The interfaces a query engine reads the store through

This document is the contract between `vortex-rdf-core` and a query engine
built outside this repository — a SPARQL planner on Apache DataFusion, say,
that treats a store as a set of tables of term codes. The engine owns
parsing, algebra, planning and execution; this crate owns the data: how a
view is exposed as a vortex `DataSource`, what the store's persisted children
look like as tables, how rows stream out without Arrow, which runtime a read
needs, what a view promises about itself before it is read, and which
identities tell the engine when its cached codes and plans stop applying.
Nothing here depends on DataFusion; the conformance crate
([`interface-tests/tests/datafusion.rs`](../interface-tests/tests/datafusion.rs))
drives every interface through vortex's own DataFusion integration, which
is the path such an engine takes.

| What the engine needs | What it calls |
|---|---|
| A table of quads to scan with projection, filter and limit pushdown | [`data_source`](../core/src/store/read/data_source.rs#L100) on any view, registered with vortex's `VortexTable` |
| The index copies in their own sort orders, and the dictionary, as tables | [`component_data_source`](../core/src/store/read/data_source.rs#L127) |
| Rows as columns, without Arrow | [`row_chunks`](../core/src/store/read/chunks.rs#L52), [`code_chunks`](../core/src/store/read/chunks.rs#L77) |
| Row counts, sort order, code bounds, partitions, what answered a match | [`view_statistics`](../core/src/store/read/metadata.rs#L148), [`index_components`](../core/src/store/read/metadata.rs#L262), [`partitions`](../core/src/store/read/metadata.rs#L300) |
| Term ↔ code, term predicates, kind ranges, the default graph's code | [`DictReader`](../core/src/store/layouts/dictionary/handles.rs#L276) (from [`dict_reader`](../core/src/store/mod.rs#L270)), see [matching.md §16.4](./matching.md#164-predicates-and-the-dictionary-handle) |
| Term-level constraints pushed below a pattern | `keep`, `window`, `size_capped`, `exists`, `match_many` — [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern) |
| When codes and plans stop applying | [`generation`](../core/src/store/read/metadata.rs#L139), [`DictReader::dictionary_id`](../core/src/store/layouts/dictionary/handles.rs#L309) |
| The session to hand vortex's integrations | [`vortex_session`](../core/src/lib.rs#L79), with [`vortex_scan`](../core/src/lib.rs#L72) re-exported |

---

## 1. A view as a `DataSource`

`data_source()` wraps **any** view — the whole store, a `match_pattern`
result, a `keep`, a `window`, a partition — as a
[`vortex_scan::DataSource`](https://docs.rs/vortex-scan). The call is
synchronous and does no I/O. It takes its row count from
`view_statistics()` ([§5](#5-what-a-view-promises-before-it-is-read)), so it
does what that call does: on an in-memory view whose selection is a pending
index run over a base with tombstones, it decodes the run's row ids and
caches them on the view. The source is a snapshot: it holds the view, and
the view holds its base immutably.

**Dtype.** The layout's primary struct, non-nullable: `{s, p, o, g}` of
`UInt64` codes (`TermCode`) under the Dictionary layout, of `utf8`
N-Triples spellings under `Default`, and the seven-column split form under
`TypedObject`. The index columns are never part of it.

**Rows.** The view's rows in **base row order** — the `(s, p, o, g)` order of
every base this crate writes — with the view's narrowing applied: the
pattern, keeps, windows and tombstones. A view an index served
(`view_statistics().served_component`) is read in base order too; its row
ids materialize on the first poll. Under a string layout the append tail's
live rows follow the base.

**Errors.** `data_source` errors for a Dictionary-layout view with a
non-empty tail, which has no source: its tail holds terms without codes, so
`compact` first ([§7](#7-snapshots-and-mutation)). It also errors where
`view_statistics` does, when the row-id decode above fails.

**The scan request.**

| Field | How the source applies it |
|---|---|
| `projection` | Bound once against the dtype and applied to every chunk after the filter and the limit: a `select`, a `pack` of `get_item`s (renames, reorders) or any computed expression vortex evaluates. The scan's dtype is the projection's return dtype. |
| `filter` | Bound once and evaluated on every chunk *exactly* — the engine may drop a filter it pushed; a row the filter evaluates to null is dropped. Any expression over the columns: equality, comparisons, `and`/`or`, `in_list`, `like` on string layouts. |
| `limit` | Enforced exactly after filtering. |
| `row_range`, `selection` | Addressed in the view's **output** rows (after narrowing, before the filter), applied before the filter. |
| `ordered` | Rows always come in base order. |
| `partition_range`, `partition_selection` | The source has one partition, index 0; a window that leaves it out yields no partitions. |

**Statistics.** `row_count()` is the
[`view_statistics`](#5-what-a-view-promises-before-it-is-read) count:
`Exact` when that is known without reading, otherwise `Inexact` with its
upper bound. `byte_size()` is the
rows' uncompressed width under the Dictionary layout, four `u64` codes (32
bytes) a row, and absent under a string layout. The partition reports its
own: its row count (a bound once the request filters or selects, otherwise
cut by `row_range` and `limit`) times the width of a *projected* row.
`field_statistics` reports `NullCount` = 0 for every non-nullable column
and, when the row count is exact, the `UncompressedSizeInBytes` of a
primitive column (8 bytes a row for a code); min/max are absent — code
bounds come from `view_statistics`.

**What to push where.** A vortex filter on a code column is correct and
evaluated chunk by chunk; a *native* narrowing is cheaper when it maps to a
code set or range the store can apply through its probes, zone maps and
indexes. The engine's rule of thumb: resolve the pattern with
`match_pattern`, turn term-level constraints into `keep`s (a `VALUES` block,
the codes a predicate's `filter_codes` passes, a namespace's
`prefix_range`), apply `window` for `LIMIT`/`OFFSET`, and hand the resulting
view to DataFusion as the table — leaving to the scan request only what is
left.

## 2. The persisted children as tables

`component_data_source(name)` serves one of the store's children as a plain
table:

| Name | Columns | Order | Rows |
|---|---|---|---|
| `index:posg` | `s, p, o, g, rid` (`UInt64` codes or strings, as the layout; `rid: UInt64`) | `(p, o, s, g)` | one per base quad |
| `index:ospg` | the same | `(o, s, p, g)` | one per base quad |
| `index:ref-p`, `index:ref-o` | `val, rid` | by `val` | one per base quad |
| `dictionary` | `_dict_term` (`utf8`, the N-Triples spelling) | lexicographic | row *i* = the term with code *i* |

`rid` is the base row id, so a join on it reaches the quad table's row. The
reference children's `val` holds the predicate or object; the copy children
are complete quad copies, which is what gives an engine a merge-join order
on `p` or `o` without a sort. `index_components()` lists which children a
store holds ([§5](#5-what-a-view-promises-before-it-is-read)).

The children are **raw**: they do not apply a view's narrowing, tombstones
or append tail. Use them over a store without tombstones or tail (`compact`
produces one), or anti-join their `rid` against the view's own rows.
`None` is returned for a name the store has no child of; the `dictionary`
table needs a `file-io` or wasm build. On a view that a match or a
narrowing proved empty, the backends differ, as they do for
`index_components` ([§5](#5-what-a-view-promises-before-it-is-read)). In
memory, such a view holds no index children, so every `index:…` name
returns `None` (the `dictionary` table stays). On file, every view serves
the file's children. An in-memory store's tables read its resident
children. On file, each child is vortex's own `LayoutReaderDataSource` over
the child's layout reader, so a scan reads only the child's bytes, and it
needs a tokio runtime ([§4](#4-which-runtime-a-read-needs)).

## 3. Rows as columns, without Arrow

Two streams read a view's rows as chunks, lazily (nothing is gathered or
scanned until the first poll), in base row order, every chunk holding at
most `batch_rows` rows:

- `row_chunks(batch_rows)` — struct chunks in the layout's primary dtype;
  under a string layout the tail's live rows follow the base.
- `code_chunks(columns, batch_rows)` — `u64` code columns
  (`Buffer<TermCode>`), one buffer per requested `QuadColumn` in the
  requested order; Dictionary layout, empty tail. In memory the buffers are
  slices of the base's canonical code columns, zero-copy for a contiguous
  selection without tombstones; on file the scan projects only the
  requested columns.

In memory, chunks are exactly `batch_rows` long but the last of the base
(and of the tail); a file-backed view's scan chunks follow the file's
splits, cut to the cap. `code_columns_gathered` remains the one-shot form.

**To Arrow.** vortex converts a chunk through its own session:
`vortex_session().arrow().execute_arrow(chunk, None, &mut ctx)` (the
`ArrowSessionExt` of `vortex-arrow`) yields an Arrow `StructArray` whose
code columns are `UInt64` (a canonical column converts without a copy) and
whose `utf8` columns are `Utf8View`; `to_arrow_schema(source.dtype())`
gives the matching schema, and `RecordBatch::from(struct_array)` the batch.
The DataFusion integration does exactly this to every chunk a `DataSource`
partition emits. The conformance crate's
[`tests/arrow.rs`](../interface-tests/tests/arrow.rs) shows the recipe,
including the two encodings an engine builds *from* codes:

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

## 4. Which runtime a read needs

Every read is lazy, and a stream's first poll decides how it runs:

- An in-memory view's reads — its data source's scan, `row_chunks`,
  `code_chunks` — and an in-memory store's component tables are CPU work on
  the polling task, and need no runtime. One exception runs earlier. An
  adopted store (`from_bytes`, `from_bytes_owned`, `from_file_in_memory`)
  holds its index children deferred, and `component_data_source`
  materializes the child it serves when it is called, not at the first
  poll. For a byte-adopted child that means scanning the buffer and then
  decoding it. Every view of the store reuses the result, and a failure is
  the call's error.
- A file-backed view reads its rows by id or scans the file's splits, as
  it decides at that first poll. A read by id, a scan of at most 42 splits
  ([`MAX_INLINE_SPLITS`](../core/src/store/scan/file_reads.rs#L64)), and a
  scan first polled where no tokio runtime is current run inline on the
  polling task. Any other scan spawns its splits
  ([`ScanDriver`](../core/src/store/scan/file_reads.rs#L40)) onto the tokio
  runtime it is polled in, and vortex looks that runtime up
  (`Handle::current()`) at every spawn, so every later poll of the stream
  must run inside a tokio runtime too.
- A file-backed store's component tables are vortex's own
  `LayoutReaderDataSource`, whose scan always spawns: read them inside a
  tokio runtime.

DataFusion runs on tokio, so a query through its integration meets every
one of these. A consumer polling outside tokio gets the inline driver on a
view's scan and cannot read a file store's component tables.

## 5. What a view promises before it is read

`view_statistics()` answers without I/O; a count that would need a scan is
a bound, a code range that would need a read is `None`. It returns a
`Result`: on an in-memory view whose selection is a pending index run over
a base with tombstones, it decodes the run's row ids to count its live rows
and caches them on the view — no I/O, but not a pure read, and that decode
is the one way it fails.

| Field | Meaning |
|---|---|
| `rows: RowCountHint` | `exact` when the count is known without reading: every in-memory view, and every file view without a pending filter except one whose index run is unread and of unknown width or under tombstones. Otherwise `upper_bound` only: the rows a pending filter has yet to test, or the base's rows. The tail's live rows count in both. |
| `sort_order` | `Some(Spog)` when the base is globally sorted (every base this crate writes) and the view reads no tail row — the order `row_chunks`, `code_chunks` and the data source produce, whatever answered the match. A view that reads tail rows has none: they follow the base in append order. |
| `code_bounds[4]` | Per column, the inclusive `TermCode` range the view's base rows lie in when it is free to know: a subject run's first and last code (in memory, over a tombstone-free contiguous run of a sorted base); the single code of each column a pending file filter binds by equality. |
| `selection` | `All`, one `Range`, explicit `Ids`, or a `PendingIndexRun` an index resolved but never materialized. |
| `served_component` | The index child that answered the match, if any (`index:posg`, …; `quads()` on such a view decodes in that child's order). |
| `pending_filter` | Whether every read runs a pushed-down file filter. |
| `tombstones`, `tail_rows` | Rows tombstoned in the base (never counted) and live rows in the append tail (counted, read last). |
| `file_backed`, `generation` | Where the base lives, and the data's identity ([§7](#7-snapshots-and-mutation)). |

`index_components()` lists the store's index children, each with its
component name (the one `component_data_source` takes), its `IndexType`, its
sort order (`Posg`, `Ospg`, or none for the reference pairs), its sortedness
provenance, and `rows: usize`, the base's row count, since every index
child holds one row per base quad. It returns a `Result`, and it classifies
a child by its implementation slug, leaving out one under a slug this
version does not know. On a view a match or a narrowing proved empty the
backends differ: in memory the view holds no children and lists none; on
file it lists the file's children, as every file view does.

`partitions(n)` cuts a view into at most `n` views over disjoint,
contiguous pieces of its base rows, in base order: reading them in turn
reads the view, and each applies the view's filter and tombstones to its own
rows only. For `n` = 1 the one partition is the view itself, its serve plan
and pending selection included. For a larger `n`, a served match's row ids
are read first and no partition keeps the serve plan. The append tail rides
with the last partition, so over a sorted base every partition but the last
keeps the `Spog` order, and the last keeps it only when the view reads no
tail row. Sizes are about equal *before* filtering; a view selecting no base
row is its own one partition. Each partition is an ordinary view —
`data_source` on each gives an engine one source per worker.

## 6. Narrowing natively

Everything in [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern)
is the engine's pushdown vocabulary: `keep`/`keep_many` (a code set or range
per column, applied through the store's probes, zone maps or a scan
conjunct), `window`/`size_capped`/`exists` (`LIMIT`, `OFFSET`, `ASK` without
reading past the answer), `match_many`/`count_many` (a batch of probes),
and the dictionary handle's `encode_many`, `prefix_range`, `filter_codes`
and `kind_ranges`. `filter_codes(predicate, candidates)` answers
`(passed, undecided)`, both ascending subsets of the candidate codes: a
candidate is decided only where the native rule is certain
([matching.md §16.4](./matching.md#164-predicates-and-the-dictionary-handle)),
and one in neither set fails. An engine keeps `passed` (as a `keep`),
evaluates `undecided` itself, and drops the rest.

## 7. Snapshots and mutation

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
  no code: `row_chunks`, `code_chunks`, `data_source` and `keep` refuse such
  a view, and `code_columns_gathered` and `dict_reader()` answer `None`. A
  transactional engine therefore compacts on commit, or serves strings
  (`quads`, `row_chunks` on a string layout) while a tail exists, and
  re-takes the dictionary handle after the compaction.
- The persisted children ([§2](#2-the-persisted-children-as-tables)) are
  the base's, untouched by tombstones and tails.
- A store opened by [`from_file`](../core/src/store/persist/open.rs#L209)
  reads its memory-mapped file in place, and so does every view, data
  source, partition and stream derived from it: the file must not be
  truncated or rewritten while any of them lives. Replacing it by a rename
  (as `compact` and the file writers do) is fine on Unix, where the mapping
  keeps the old file; Windows refuses to rename over a mapped file.
  [`from_file_in_memory`](../core/src/store/persist/open.rs#L220) loads the
  file whole instead and never reads it again.

## 8. Through DataFusion

A view registers with vortex's table provider (`vortex-datafusion`'s `v2`):

```rust
let source = view.data_source()?;
let schema = Arc::new(vortex_session().arrow().to_arrow_schema(source.dtype())?);
ctx.register_table("quads", Arc::new(VortexTable::new(source, vortex_session().clone(), schema)))?;
```

vortex's integration pushes the projection, the filters it can convert
(comparisons, `AND`/`OR`, `IN` lists as `in_list`, `LIKE`, casts) and the
limit into the scan request, reads the one partition, and converts every
chunk to a `RecordBatch`. It reports one partition to DataFusion; set
`target_partitions` to 1 when the scan's order must survive into the
results, or use `partitions(n)` and register one table per piece.

**Integer literals.** SQL parses a bare integer (`WHERE p = 5`) as an
`Int64` literal. DataFusion's coercion and simplification turn its
comparison with a `UInt64` code column back into one against a `UInt64`
literal, so the scan node receives `$.p = 5u64`. A range arrives the same
way, an `IN` of more than three values as an `in_list` of `UInt64` values,
and a shorter one as `OR`ed equalities.

The suite ([`tests/datafusion.rs`](../interface-tests/tests/datafusion.rs))
checks, on in-memory, byte-adopted and file-backed stores (mapped, and
loaded whole): `COUNT(*)` against `size`, the whole table and projections
against `code_chunks`, equality, `IN`, range and conjunction filters
against brute force, bare integer literals on every code column (their
counts against brute force, their predicates on the plan's scan node),
limits with and without filters, `ORDER BY` on the base order, `DISTINCT`,
a view as its own table and a join between two views, the `index:posg`
child's order and `rid`s, the `dictionary` child's rows and a term-level
`LIKE` over it, and a string layout's `Utf8View` columns.
[`examples/datafusion_table.rs`](../interface-tests/examples/datafusion_table.rs)
is the starting point: run it with
`cargo run --manifest-path interface-tests/Cargo.toml --example datafusion_table`.

The crate is a workspace of its own, so the DataFusion dependency tree
stays out of `cargo test --workspace`. CI runs it in the `interface-tests`
job, `--locked` against the crate's own `Cargo.lock`, and
`scripts/ci-check.sh` mirrors that job.

## 9. Source map

| Concern | File |
|---|---|
| `DataSource` over a view, component tables, the request plan | [`core/src/store/read/data_source.rs`](../core/src/store/read/data_source.rs) |
| `row_chunks`, `code_chunks` | [`core/src/store/read/chunks.rs`](../core/src/store/read/chunks.rs) |
| `view_statistics`, `index_components`, `partitions`, `generation` | [`core/src/store/read/metadata.rs`](../core/src/store/read/metadata.rs) |
| The scan driver of a file view's reads | [`core/src/store/scan/file_reads.rs`](../core/src/store/scan/file_reads.rs) |
| `RowSelection::split` | [`core/src/store/view/selection.rs`](../core/src/store/view/selection.rs) |
| `DictReader`, `DictSnapshot`, `dictionary_id`, `filter_codes` | [`core/src/store/layouts/dictionary/handles.rs`](../core/src/store/layouts/dictionary/handles.rs), [`term_dict.rs`](../core/src/store/layouts/dictionary/term_dict.rs), [`file_backed.rs`](../core/src/store/layouts/dictionary/file_backed.rs) |
| `vortex_session`, the `vortex_scan` re-export | [`core/src/lib.rs`](../core/src/lib.rs) |
| Conformance through DataFusion, the Arrow recipe, the example | [`interface-tests/Cargo.toml`](../interface-tests/Cargo.toml), [`tests/datafusion.rs`](../interface-tests/tests/datafusion.rs), [`tests/arrow.rs`](../interface-tests/tests/arrow.rs), [`examples/datafusion_table.rs`](../interface-tests/examples/datafusion_table.rs) |
