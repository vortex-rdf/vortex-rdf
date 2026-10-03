# The interfaces a query engine reads the store through

This document is the contract between `vortex-rdf-core` and a query engine
built outside this repository — a SPARQL planner on Apache DataFusion, say,
that treats a store as a set of tables of term codes. The engine owns
parsing, algebra, planning and execution; this crate owns the data: how a
view is exposed as a vortex `DataSource`, what the store's persisted children
look like as tables, how rows stream out without Arrow, what a view promises
about itself before it is read, and which identities tell the engine when
its cached codes and plans stop applying. Nothing here depends on DataFusion;
the conformance crate ([`interface-tests/tests/datafusion.rs`](../interface-tests/tests/datafusion.rs)) drives every
interface through vortex's own DataFusion integration, which is the path such
an engine takes.

| What the engine needs | What it calls |
|---|---|
| A table of quads to scan with projection, filter and limit pushdown | [`VortexRdfStore::data_source`](../core/src/store/read/data_source.rs) on any view, registered with vortex's `VortexTable` |
| The index copies in their own sort orders, and the dictionary, as tables | [`VortexRdfStore::component_data_source`](../core/src/store/read/data_source.rs) |
| Rows as columns, without Arrow | [`row_chunks`](../core/src/store/read/chunks.rs), [`code_chunks`](../core/src/store/read/chunks.rs) |
| Row counts, sort order, code bounds, partitions, what answered a match | [`view_statistics`](../core/src/store/read/metadata.rs), [`index_components`](../core/src/store/read/metadata.rs), [`partitions`](../core/src/store/read/metadata.rs) |
| Term ↔ code, term predicates, kind ranges, the default graph's code | [`DictReader`](../core/src/store/layouts/dictionary/term_dict.rs) (`dict_reader`), see [matching.md §16.4](./matching.md#164-predicates-and-the-dictionary-handle) |
| Term-level constraints pushed below a pattern | `keep`, `window`, `size_capped`, `exists`, `match_many` — [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern) |
| When codes and plans stop applying | [`generation`](../core/src/store/read/metadata.rs), [`DictReader::dictionary_id`](../core/src/store/layouts/dictionary/term_dict.rs) |
| The session to hand vortex's integrations | [`vortex_session()`](../core/src/lib.rs), with `vortex_scan` re-exported |

---

## 1. A view as a `DataSource`

`data_source()` wraps **any** view — the whole store, a `match_pattern`
result, a `keep`, a `window`, a partition — as a
[`vortex_scan::DataSource`](https://docs.rs/vortex-scan). The source is a
snapshot: it holds the view, and the view holds its base immutably.

**Dtype.** The layout's primary struct, non-nullable: `{s, p, o, g}` of
`u32` codes under the Dictionary layout, of `utf8` N-Triples spellings under
`Default`, and the seven-column split form under `TypedObject`. The index
columns are never part of it.

**Rows.** The view's rows in **base row order** — the `(s, p, o, g)` order of
every base this crate writes — with the view's narrowing applied: the
pattern, keeps, windows and tombstones. A view an index served
(`view_statistics().served_component`) is read in base order too; its row
ids materialize on the first poll. Under a string layout the append tail's
live rows follow the base. A Dictionary-layout view with a non-empty tail
has no source: its tail holds terms without codes, so `compact` first
([§6](#6-snapshots-and-mutation)).

**The scan request.**

| Field | How the source applies it |
|---|---|
| `projection` | Bound once against the dtype and applied to every chunk after the filter: a `select`, a `pack` of `get_item`s (renames, reorders) or any computed expression vortex evaluates. The scan's dtype is the projection's return dtype. |
| `filter` | Bound once and evaluated on every chunk *exactly* — the engine may drop a filter it pushed. Any expression over the columns: equality, comparisons, `and`/`or`, `list_contains`, `like` on string layouts. |
| `limit` | Enforced exactly after filtering. |
| `row_range`, `selection` | Addressed in the view's **output** rows (after narrowing, before the filter), applied before the filter. |
| `ordered` | Rows always come in base order. |
| `partition_range`, `partition_selection` | The source has one partition, index 0; a window that leaves it out yields no partitions. |

**Statistics.** `row_count()` is `Exact` unless a pushed-down file filter is
still pending, then `Inexact` with the rows the filter has yet to test (the
same numbers as `view_statistics().rows`). `byte_size()` is the uncompressed
footprint of four `u32` columns under the Dictionary layout, absent
otherwise. `field_statistics` reports `NullCount` = 0 for every non-nullable
column and the `UncompressedSizeInBytes` of a `u32` column; min/max are
absent — code bounds come from
[`view_statistics`](#4-what-a-view-promises-before-it-is-read).

**What to push where.** A vortex filter on a code column is correct and
evaluated chunk by chunk; a *native* narrowing is cheaper when it maps to a
code set or range the store can apply through its probes, zone maps and
indexes. The engine's rule of thumb: resolve the pattern with
`match_pattern`, turn term-level constraints into `keep`s (a `VALUES` block,
a predicate's `filter_codes`, a namespace's `prefix_range`), apply `window`
for `LIMIT`/`OFFSET`, and hand the resulting view to DataFusion as the
table — leaving to the scan request only what is left.

## 2. The persisted children as tables

`component_data_source(name)` serves one of the store's children as a plain
table:

| Name | Columns | Order | Rows |
|---|---|---|---|
| `index:posg` | `s, p, o, g, rid` (`u32` codes or strings, as the layout; `rid: u64`) | `(p, o, s, g)` | one per base quad |
| `index:ospg` | the same | `(o, s, p, g)` | one per base quad |
| `index:ref-p`, `index:ref-o` | `val, rid` | by `val` | one per base quad |
| `dictionary` | `_dict_term` (`utf8`, the N-Triples spelling) | lexicographic | row *i* = the term with code *i* |

`rid` is the base row id, so a join on it reaches the quad table's row. The
reference children's `val` holds the predicate or object; the copy children
are complete quad copies, which is what gives an engine a merge-join order
on `p` or `o` without a sort. `index_components()` lists which children a
store holds ([§4](#4-what-a-view-promises-before-it-is-read)).

The children are **raw**: they do not apply a view's narrowing, tombstones
or append tail. Use them over a store without tombstones or tail (`compact`
produces one), or anti-join their `rid` against the view's own rows.
`None` is returned for a name the store has no child of; the `dictionary`
table is reachable when the dictionary is resident or the store is
file-backed. On file, each child is served through its own layout reader,
so a scan reads only the child's bytes.

## 3. Rows as columns, without Arrow

Two streams read a view's rows as chunks, lazily (nothing is gathered or
scanned until the first poll), in base row order, every chunk holding at
most `batch_rows` rows:

- `row_chunks(batch_rows)` — struct chunks in the layout's primary dtype;
  under a string layout the tail's live rows follow the base.
- `code_chunks(columns, batch_rows)` — `u32` code columns, one buffer per
  requested `QuadColumn` in the requested order; Dictionary layout, empty
  tail. In memory the buffers are zero-copy slices of the base's columns; on
  file the scan projects only the requested columns.

An in-memory view's chunks are exactly `batch_rows` long but the last; a
file-backed view's follow the file's natural splits cut to the cap.
`code_columns_gathered` remains the one-shot form.

**To Arrow.** vortex converts a chunk through its own session:
`vortex_session().arrow().execute_arrow(chunk, None, &mut ctx)` (the
`ArrowSessionExt` of `vortex-arrow`) yields an Arrow `StructArray` whose
`u32` columns are zero-copy and whose `utf8` columns are `Utf8View`;
`to_arrow_schema(source.dtype())` gives the matching schema, and
`RecordBatch::from(struct_array)` the batch. The DataFusion integration does
exactly this to every chunk a `DataSource` partition emits. The conformance
crate's [`tests/arrow.rs`](../interface-tests/tests/arrow.rs) shows the
recipe, including the two encodings an engine builds *from* codes:

- **Codes as ids.** `u32` codes reinterpreted as `Int32` object ids fit as
  long as the dictionary has at most `i32::MAX` terms (`DictReader::len`).
- **Terms.** `DictReader::decode_many(codes)` decodes a batch's distinct
  codes once; the engine builds a dictionary-encoded Arrow array (keys = the
  batch's code ranks, values = the decoded spellings) or a plain string
  array.

**The default graph.** Its spelling is the empty string, so it is code 0
exactly when any quad is in the default graph
(`DictReader::kind_ranges().default_graph`); an engine that wants `NULL`
for it maps that code.

## 4. What a view promises before it is read

`view_statistics()` answers without I/O; a count that would need a scan is
a bound, a code range that would need a read is `None`:

| Field | Meaning |
|---|---|
| `rows: RowCountHint` | `exact` when nothing but an in-memory gather is pending (every in-memory view; a file view with no pushed-down filter; a served file run), else `upper_bound` only — the rows a pending file filter has yet to test. |
| `sort_order` | `Some(Spog)` when the base is globally sorted (every base this crate writes) — the order `row_chunks`, `code_chunks` and the data source produce, whatever answered the match. |
| `code_bounds[4]` | Per column, the inclusive code range the view's rows lie in when it is free to know: a subject run's first and last code (in memory, over a tombstone-free contiguous run of a sorted base); the single code of each column a pending file filter binds by equality. |
| `selection` | `All`, one `Range`, explicit `Ids`, or a `PendingIndexRun` an index resolved but never materialized. |
| `served_component` | The index child that answered the match, if any (`quads()` on such a view decodes in that child's order). |
| `pending_filter` | Whether every read runs a pushed-down file filter. |
| `tombstones`, `tail_rows` | Rows tombstoned in the base (never counted) and live rows in the append tail (counted, read last). |
| `file_backed`, `generation` | Where the base lives, and the data's identity ([§6](#6-snapshots-and-mutation)). |

`index_components()` lists the index children with their `IndexType`, sort
order (`Posg`, `Ospg`, or none for the reference pairs), sortedness
provenance, row count when known without reading, and residency.

`partitions(n)` cuts a view into at most `n` views over disjoint,
contiguous pieces of its base rows, in base order: reading them in turn
reads the view, and each applies the view's filter and tombstones to its own
rows only. The append tail rides with the last partition. Sizes are about
equal *before* filtering. Each partition is an ordinary view — `data_source`
on each gives an engine one source per worker.

## 5. Narrowing natively

Everything in [matching.md §16](./matching.md#16-narrowing-beyond-a-pattern)
is the engine's pushdown vocabulary: `keep`/`keep_many` (a code set or range
per column, applied through the store's probes, zone maps or a scan
conjunct), `window`/`size_capped`/`exists` (`LIMIT`, `OFFSET`, `ASK` without
reading past the answer), `match_many`/`count_many` (a batch of probes),
and the dictionary handle's `encode_many`, `prefix_range`, `filter_codes`
and `kind_ranges`. A `TermPredicate`'s verdicts are conservative: `True`
and `False` only where the rule is total, `Unknown` otherwise, so an engine
pushes the definite codes as a keep and evaluates the unknown ones itself.

## 6. Snapshots and mutation

A `VortexRdfStore` value is a snapshot: views share an immutable base, and
every read of a view sees the same rows. `add_quads`, `delete_quad` and
`compact` return new stores and leave the old ones valid.

- `generation()` identifies the data behind a view. Views derived by
  `match_pattern`, `keep`, `window` and `partitions` keep it; a mutation, a
  compaction and a fresh open or build take a new one. Equal generations
  mean the same rows, codes and tail — the key for cached plans and
  statistics.
- `DictReader::dictionary_id()` (and `DictSnapshot::dictionary_id`)
  identifies the code vocabulary. Every view of one store shares it; a
  `compact` re-encodes against a new dictionary and changes it, as does
  opening the file again. Equal ids promise equal codes; different ids
  promise nothing, so an engine that interns codes re-encodes its constants
  when the id changes.
- Under the Dictionary layout an append tail holds strings whose terms have
  no code: `code_chunks`, `data_source`, `keep` and `code_columns_gathered`
  refuse such a view (`dict_reader()` is `None`). A transactional engine
  therefore compacts on commit, or serves strings (`quads`,
  `row_chunks` on a string layout) while a tail exists, and re-takes the
  dictionary handle after the compaction.
- The persisted children ([§2](#2-the-persisted-children-as-tables)) are
  the base's, untouched by tombstones and tails.

## 7. Through DataFusion

The conformance crate registers a view with vortex's table provider:

```rust
let source = view.data_source().await?;
let schema = Arc::new(vortex_session().arrow().to_arrow_schema(source.dtype())?);
ctx.register_table("quads", Arc::new(VortexTable::new(source, vortex_session().clone(), schema)))?;
```

vortex's integration pushes the projection, the filters it can convert
(comparisons, `AND`/`OR`, `IN` lists, `LIKE`, casts) and the limit into the
scan request, reads the one partition, and converts every chunk to a
`RecordBatch`. It reports one partition to DataFusion; set
`target_partitions` to 1 when the scan's order must survive into the
results, or use `partitions(n)` and register one table per piece. The
suite ([`tests/datafusion.rs`](../interface-tests/tests/datafusion.rs))
checks, on in-memory, byte-adopted and file-backed stores: `COUNT(*)`
against `size`, the whole table and projections against `code_chunks`,
equality, `IN`, range and conjunction filters against brute force, limits
with and without filters, `ORDER BY` on the base order, `DISTINCT`, a view
as its own table and a join between two views, the `index:posg` child's
order and `rid`s, the `dictionary` child's rows and a term-level `LIKE`
over it, and a string layout's `Utf8View` columns.
[`examples/datafusion_table.rs`](../interface-tests/examples/datafusion_table.rs)
is the starting point: run it with
`cargo run --manifest-path interface-tests/Cargo.toml --example datafusion_table`.

The crate is a workspace of its own so the DataFusion dependency tree stays
out of `cargo test --workspace`; CI runs it in the `interface-tests` job and
`scripts/ci-check.sh` mirrors it.

## 8. Source map

| Concern | File |
|---|---|
| `DataSource` over a view, component tables, the request plan | [`core/src/store/read/data_source.rs`](../core/src/store/read/data_source.rs) |
| `row_chunks`, `code_chunks` | [`core/src/store/read/chunks.rs`](../core/src/store/read/chunks.rs) |
| `view_statistics`, `index_components`, `partitions`, `generation` | [`core/src/store/read/metadata.rs`](../core/src/store/read/metadata.rs) |
| `RowSelection::split` | [`core/src/store/view/selection.rs`](../core/src/store/view/selection.rs) |
| `dictionary_id` | [`core/src/store/layouts/dictionary/term_dict.rs`](../core/src/store/layouts/dictionary/term_dict.rs), [`file_backed.rs`](../core/src/store/layouts/dictionary/file_backed.rs) |
| `vortex_session`, the `vortex_scan` re-export | [`core/src/lib.rs`](../core/src/lib.rs) |
| Conformance through DataFusion, the Arrow recipe, the example | [`interface-tests/Cargo.toml`](../interface-tests/Cargo.toml), [`tests/datafusion.rs`](../interface-tests/tests/datafusion.rs), [`tests/arrow.rs`](../interface-tests/tests/arrow.rs), [`examples/datafusion_table.rs`](../interface-tests/examples/datafusion_table.rs) |
