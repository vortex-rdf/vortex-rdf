# Experiment: point lookups from the file's zone maps and RunEnd runs

Branch `exp/zone-map-runend-point-lookup`, based on `v0.12.1`. It is a prototype to look at and integrate, not a
change to merge as is. Names carry `debug_` and the code sits in one module so that the diff is easy to read.

## Why

qEndpoint (RDF4J) running on a vortex-rdf store evaluates joins as nested loops of point lookups, issued through a
JNI binding, one `run_probe` + `code_columns_gathered` per binding. On qEndpoint's real BSBM 10K calls (96% are
`sp?`), one lookup costs about 10.9 us in `vortex-rdf-core`. HDT takes 0.81 us on the same calls. Measured split
of one `sp?` lookup (file-backed, copy index):

| Stage | Code | 10K | 100K |
|---|---|---|---|
| Parse the terms | `common/terms` | 0.8 us | 0.8 us |
| Term to code (dictionary search) | `matching.rs:178-186` -> `layouts/mod.rs:704` | 3.2 | 3.4 |
| Subject bisection | `file_scan.rs:607` -> `ColumnChunks::bounds` | 2.9 | 3.8 |
| Predicate within the run | `file_scan.rs:636` -> `bounds_in` | 0.6 | 0.7 |
| Index routing, view, window | `matching.rs:583-788`, `narrowing.rs:366` | 1.0 | 0.9 |
| Gather of about 1 row | `rows.rs:315` -> `:336` -> `:407` | 3.1 | 3.2 |

## What the files already hold, and what is not used

Inspected on the BSBM 10K/100K and WatDiv 10M stores (format v2, Vortex 0.88.0):

- **Subject column `s`:** chunks of 262,144 rows, `vortex.runend`, with ends and values `fastlanes.for` +
  `fastlanes.bitpacked`: one run per subject, 16-50 K runs per chunk. `p` is `vortex.dict`, `o` is BitPacked and
  `g` is Constant.
- **Zone maps:** every column, of the quad table and of `index:posg` / `index:ospg`, is `vortex.zoned`. The zone
  map has min, max and null_count per 8,192 rows.
- **The zone maps are skipped for point lookups.** `ColumnChunks` steps through the zoned wrapper and never reads
  its zones child (`encoded-search/src/layout.rs`, `unwrap_zoned`).
- **The RunEnd structure is searched many times instead of once.**
  - Each chunk-bisection step reads a chunk's first or last value with `value_at`, which on RunEnd binary-searches
    all the run ends (`node.rs`, `Node::value_at`).
  - `lower_bound` and `upper_bound` then each run `window_runs` (two searches over the ends) plus a search over the
    values.
  - Measured: 8 bisection steps at about 205 ns each, plus 1.0 us inside the chunk.

## What the branch adds

- `encoded-search`:
  - `debug_zone_dir` reads a column's zone map (one flat segment) into plain `u64` minima and maxima.
  - `ColumnChunks::debug_point_bounds` bisects the zone maxima to pick the chunk, then finds the run with one
    search: `Node::debug_run_bounds`, a single search over the RunEnd values, with both boundaries from `ends`.
  - It falls back to `bounds` when a run reaches the chunk's end.
  - Also `debug_chunk_rows` / `debug_chunk_array`, for inspection.
- `core::store::inspect::Proto`: a lookup by codes `(Option<s>, Option<p>, Option<o>)` with an offset and a limit.
  - Subject-bound patterns use the quad table: subject run, then `bounds_in` on `p` (and `o`).
  - `?p?` / `?po` use `index:posg`, and `??o` uses `index:ospg`.
  - Unbound columns are read with `ColumnChunks::value_at`.
  - It returns `None` (caller falls back) for `???`, `s?o`, and for components without all four columns (the
    reference index).
- `layout_report` and `sample_codes`: inspection helpers.

## Measured so far

Direct calls, warm, same process; today's `ColumnChunks` path compared with the prototype on the same calls:

| Calls | Subject: today -> prototype | Predicate (`bounds_in`, unchanged) | Read `o`, `g` (today: view + window + gather, about 4 us) |
|---|---|---|---|
| BSBM 10K, qEndpoint's `sp?` calls (19,997) | 2.54 -> **0.28 us** | 0.40 us | **0.12 us** |
| BSBM 100K, same calls (20,000) | 3.31 -> **0.20 us** | 0.56 us | **0.18 us** |
| WatDiv 10M, 20,000 `(s, p)` sampled from rows | 2.72 -> **0.25 us** | (sample biased to long runs) | (idem) |

- **Correctness:** row ranges equal today's `bounds` + `bounds_in` on every call. The `(o, g)` codes equal those of
  `run_probe` + `code_columns_gathered` on every call (first 64 rows).
- **The binding:** the qEndpoint binding's tests run every pattern shape (counts and searched rows against a full
  scan, paging, impossible patterns) with `Proto` in place, on the BSBM 1K copy and reference stores. All pass.
- **Sum of stages:** with codes given, the measured stages add up to 0.80 us (10K) and 0.94 us (100K) per `sp?`
  lookup, against 10.9 and 12.2 us today and 0.81 us for HDT. This is a sum of stages measured separately, not one
  timed path.
- **One idea refuted:** a linear pass over the run for the predicate, through `value_at`, was slower than
  `bounds_in` (0.92 vs 0.40 us).

Still being measured (results to follow on this branch): the index shapes and WatDiv's real calls (from a call log
of the server), Vortex's generic `search_sorted` on the same chunks, and the whole path end to end in the server
over HTTP, with result fingerprints checked.

## Suggested integration

1. A lookup that takes `TermCode`s, so the binding stops decoding codes that `prepare_pattern` then searches for
   again (removes 4.8 us).
2. A point-lookup path that returns the located run and reads only the unbound columns, skipping the view, the
   window and the gather (removes about 4 us).
3. In `ColumnChunks`: read the zoned child once and keep its minima and maxima. Pick the chunk from them, and on a
   RunEnd chunk search once (2.5-4.4 us -> 0.2-0.4 us).
