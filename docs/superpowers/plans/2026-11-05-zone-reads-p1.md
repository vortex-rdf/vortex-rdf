# Zone-statistics reads, P1 (file located table runs) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Counts, offset windows and streams of a located table run whose rows are checked against bound slots decide whole zones from Vortex statistics, and count only the mixed zones, on encoded data. Today every row of the run is read.

**Architecture:**
- **Proofs.** A new `ZoneDecider` (`core/src/store/query/zone_decider.rs`) classifies a run's zones as none, all or mixed. It evaluates Vortex's own proofs (`BoundExpression::falsify`, and `satisfy` through a vortex-rdf `StatsRewriteRule` for `x = c`) with `ZoneMap::prune`.
- **Zone maps.** The decider prunes Vortex `ZoneMap`s of the checked columns (`ColumnZones`, `core/src/io/zone_map.rs`), each read from its zone table on first use and kept with the file.
- **Located runs.** `LocatedRun` (`core/src/store/query/located.rs`) uses the classes:
  - a count adds zone sizes, and counts mixed zones with `ColumnChunks::count_eq`;
  - a window finds its first zone by those counts;
  - a block skips none zones, keeps all zones unread, and checks mixed zones.
- **Footer.** Before any zone, the footer statistics settle a checked slot for the whole file: `VortexFile::can_prune`, and the satisfier on a one-zone `ColumnZones` of the footer's bounds.

**Tech Stack:** Rust 2024; Vortex 0.90:
- `vortex-array`: `stats::rewrite::StatsRewriteRule`, `stats::session::StatsSessionExt`, `stats::expr::bound::stat`, `expr::bound::{and, eq, lit}`;
- `vortex-layout`: `layouts::zoned::zone_map::ZoneMap`;
- `vortex-file`: `VortexFile::can_prune`, `file_stats`;
- the workspace's `vortex-rdf-encoded-search` crate.

Tests are tokio tests; benches use divan.

**Spec:** `docs/superpowers/specs/2026-11-04-zone-statistics-reads-design.md`. This plan implements §3 (levels 1, 2, 4, 5), §3.1 (the P1 rule), §4.1, §7, §8 and §9 as they apply to P1, §12 point 2, and §13.1 S3 and S4.

## Global Constraints

- **Same answers.** Every count, window and stream returns the same rows in the same order as with `VORTEX_RDF_ZONES=0`, which is today's route (#145).
- **Nothing kept between calls.** A file keeps only each read column's zone map, bounded by its layout: two `u64` per 8,192-row zone. A run's classes live with the run, which a call or a stream holds.
- **Decisions are Vortex proofs evaluated by Vortex:** `falsify` / `satisfy` with `ZoneMap::prune`, and `VortexFile::can_prune`. vortex-rdf adds rules only through `StatsRewriteRule`.
- **`VORTEX_RDF_ZONES=0`**, read once like `VORTEX_RDF_LOCATE=0`, puts every path on today's route, the 8,388,608-row `GAP_ROWS` cap included.
- **Counters** join the `VORTEX_RDF_COUNTERS=1` set (`vortex_rdf_encoded_search::counters`), which Java's `engineCounters()` and Python's `engine_counters()` read by name.
- **Scope.** Located table runs (`RunRows::Table`) with checked slots, on file stores. Reference runs, in-memory stores and the general match are unchanged (spec P3–P5). `pruning_envelopes` stays (spec §13.1 S5).
- **Build.** File-only code is under `#[cfg(feature = "file-io")]`. `scripts/ci-check.sh docs lint rust-tests` stays clean: fmt, clippy `-D warnings` for the workspace and wasm32, and doc anchors.
- **Branch.** `perf/zone-reads-p1`, from `ccr-999602bb-9tajls` (#145).

## Review Focus

1. **A checked column with no zone map** takes today's row path, and `GAP_ROWS` still caps its run. Examples: a file written without zoned columns (the seven-row-leaf fixture), or a zone table without exact `max` and `min`. Tests: Task 3 (`read` gives `None`), Task 6 (same answers on the seven-row-leaf store) and Task 7 (the cap still declines it).
2. **A code equal to a zone's min or max, or `u64::MAX`.** The proofs' boundaries: equality is neither none nor all unless `min = max = c`. Tests: Task 4 and Task 5.
3. **Runs that start or end inside a zone, runs shorter than a zone, and empty runs.** A zone's class is the whole zone's; a count takes only the run's rows of it. Tests: Task 5 (partial ranges) and Task 6 (40,000-row runs whose table zones straddle them).
4. **Two checked slots** (`(hub ? o g)` on a store without indexes). A zone is none if either slot says so, and all only if both do; a mixed zone with both slots unsettled reads rows. Tests: Task 5 and Task 6.
5. **A file whose footer has no statistics for the checked column.** Level 1 settles nothing, and zones still decide. Test: Task 7 (`footer_decision` with no statistics).

---

### Task 1: Count a value on the encoding (`count_eq`)

**Files:**
- Modify: `encoded-search/src/node.rs`, `encoded-search/src/lib.rs`, `encoded-search/src/layout.rs`
- Test: `encoded-search/tests/common/mod.rs`, `encoded-search/tests/layout.rs`

**Interfaces:**
- Produces:
  - `SortedProbe::count_eq(&self, start: usize, len: usize, needle: u64) -> usize` (lib.rs; panics past `len()`).
  - `ColumnChunks::count_eq(&self, rows: Range<u64>, needle: u64, source: &Arc<dyn SegmentSource>, session: &VortexSession) -> VortexResult<u64>` (layout.rs, `layout` feature). It counts each leaf through its probe, and decodes a leaf whose probe declines.

The code exists on local commit `2ea916b` (branch `exp/zone-counts`): `git show 2ea916b -- encoded-search/`. Take its `count_eq` hunks only, not `leaf_node_kinds`.

- [ ] **Step 1: Write the failing tests.**
  - Add `2ea916b`'s `count_eq` loop to `assert_bulk_reads` in `tests/common/mod.rs`. It compares `probe.count_eq(start, width, needle)` with a plain count, for widths `[1, 7, 1023, 1024, 1025, 3000, data.len()]` and needles drawn from the data plus `u64::MAX`.
  - In `column_chunks_match_canonical` (`tests/layout.rs`), compare `column.count_eq(rows, needle, &source, &session)` with a plain count of `data[rows]` for each needle of `[0, max / 2, max, max + 1]`, over these row ranges:
    - `0..600_000`;
    - `199_990..200_010` (across the leaf boundary);
    - `5..9`;
    - `399_999..400_001`.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-encoded-search --all-features`.** Expected: compile errors, `count_eq` not found.
- [ ] **Step 3: Port the `count_eq` methods from `2ea916b`:** `node.rs` (all of its hunks), `lib.rs` (`SortedProbe::count_eq`) and `layout.rs` (`ColumnChunks::count_eq`).
- [ ] **Step 4: Run the same command.** Expected: PASS.
- [ ] **Step 5: Commit:** `feat(encoded-search): count a value on the encoding (count_eq)`.

### Task 2: Keep the faster of `count_eq` and Vortex's compare kernel

Spec §3 level 4 and §8: count on encoded data with whichever of the two is faster.

**Files:**
- Modify: `encoded-search/benches/probe.rs`; possibly `encoded-search/src/layout.rs` (Step 3)

**Interfaces:**
- Consumes: Task 1's `count_eq`.
- Produces: `ColumnChunks::count_eq`'s signature, unchanged whichever body wins.

- [ ] **Step 1: Add two divan benches over `SHAPES`, each counting 20 needles from `needles(&fixture.data)` in every 8,192-value window of the fixture:**
  - `count_eq_probe`: `SortedProbe::resolve(&fixture.encoded)` then `count_eq`;
  - `count_eq_vortex`: the window's slice, `apply_bound` of `eq(root(), lit(needle))` bound to the fixture's dtype, executed to a `Mask`, then `true_count()`.
- [ ] **Step 2: Run `cargo bench -p vortex-rdf-encoded-search --bench probe -- count_eq`.** Record each shape's medians.
- [ ] **Step 3: Decide.**
  - **Keep `count_eq`** if its median is lower on every shape, or lower on all but one and at most 1.2× the Vortex path's there.
  - **Otherwise** make `ColumnChunks::count_eq` count each leaf's slice through the Vortex path, delete `SortedProbe::count_eq` and the node methods, and keep Task 1's tests on `ColumnChunks::count_eq`.
- [ ] **Step 4: Run `cargo test -p vortex-rdf-encoded-search --all-features`.** Expected: PASS.
- [ ] **Step 5: Commit** the benches and any switch: `bench(encoded-search): count_eq against Vortex's compare kernel`. The commit body gives both medians per shape and the decision.

### Task 3: A column's zone map as a Vortex `ZoneMap` (`ColumnZones`)

**Files:**
- Modify: `core/src/io/zone_map.rs`
- Modify: `core/src/store/persist/native_file.rs`. `LocatedTables` gains its zones cells. `table_column_layout` comes from `2ea916b`'s `native_file.rs` hunk.
- Test: unit tests in `core/src/io/zone_map.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct ColumnZones`: a non-nullable `u64` column's zone table, keeping only its exact `max` and `min` fields, in `exact_bound_aggregates()` order.
  - `ColumnZones::from_bounds(mins: Buffer<u64>, maxes: Buffer<u64>, zone_len: u64, row_count: u64) -> Result<Self>`.
  - `ColumnZones::read(column: &LayoutRef, source: &Arc<dyn SegmentSource>) -> Result<Option<Self>>`. It goes through `ExactZoneMap::of`, then `read_bounds::<PrimitiveArray>`. It is `None` for a column that is not zoned with exact bounds, or whose bound columns hold a null.
  - `ColumnZones::zone_len(&self) -> u64`; `ColumnZones::zones(&self) -> u64`, the number of zones.
  - `ColumnZones::prune(&self, zones: Range<u64>, proof: &BoundExpression) -> Result<Mask>`. It returns one bit per zone of `zones`, set where `proof` holds. It is `ZoneMap::try_new(dtype, table.slice(zones), exact_bound_aggregates(), zone_len, rows_of(zones))?.prune(proof, &VORTEX_SESSION)`.
  - `LocatedTables::new(sorted, references) -> Self` and the field `zones: [[OnceLock<Option<Arc<ColumnZones>>>; 4]; 3]`, per sorted table and `(s, p, o, g)` slot.
  - `NativeStoreFile::table_column_layout(&self, component: Option<&str>, column: &str) -> Option<LayoutRef>`.

- [ ] **Step 1: Write the failing tests** in `zone_map.rs`:
  - `column_zones_prune_with_vortex_falsifier`. `from_bounds` with mins `[5, 4, 5, 6, 1]`, maxes `[5, 5, 6, 7, 3]`, `zone_len` 8,192 and `row_count` 36,000. With `eq(root(), lit(5u64))` bound to `u64` and its `falsify(&VORTEX_SESSION)`:
    - `prune(0..5, ..)` is `[false, false, false, true, true]`;
    - `prune(1..4, ..)` is `[false, false, true]`.
  - `column_zones_read_from_a_written_column`. Write 20,000 quads `make_quad("http://example.org/s{i:05}", "http://example.org/p", "o", numbered_graph(1 + i / 10_000))` (`write_store_file`, Dictionary, no index), so the quad table's rows run `g1` then `g2`. `ColumnZones::read` of its `g` layout (`table_column_layout(None, "g")`) has `zones() == 3` and `zone_len() == 8192`. With the falsifier of `g = <g1's code>` (the first row's `g` in `row_list`), `prune(0..3, ..)` is `[false, false, true]`.
  - `column_zones_absent_without_zone_maps`. For the seven-row-leaf store (`write_chunked_store_file(&quads, 7)`), `read` is `None`.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-core --lib column_zones`.** Expected: compile errors.
- [ ] **Step 3: Implement** `ColumnZones`, `LocatedTables::new` with empty cells, `table_column_layout`, and the `LocatedTables { .. }` literal in `located_run` replaced by `LocatedTables::new`.
- [ ] **Step 4: Run the same command.** Expected: PASS.
- [ ] **Step 5: Commit:** `feat(core): read a column's zone table as a Vortex ZoneMap`.

### Task 4: Prove "every row equals c" (the `x = c` satisfier rule)

**Files:**
- Create: `core/src/stats_rules.rs` (declared in `core/src/lib.rs`)
- Modify: `core/src/session.rs`, which registers the rule in `VORTEX_SESSION`'s initializer
- Test: unit tests in `core/src/stats_rules.rs`

**Interfaces:**
- Consumes: `ColumnZones` (Task 3).
- Produces:
  - `pub(crate) struct EqSatisfier`, a `vortex_array::stats::rewrite::StatsRewriteRule` with `scalar_fn_id() = Binary.id()`. `satisfy` answers `Some` only for `Operator::Eq` with one child a non-null literal and the other a non-nullable integer-typed expression `x`. Its proof is `and(eq(stat(x, Stat::Min.aggregate_fn()), lit), eq(stat(x, Stat::Max.aggregate_fn()), lit))`, from `vortex_array::stats::expr::bound::stat` and `vortex_array::expr::bound::{and, eq}`. It answers `None` for every other operator or dtype: a float zone's NaNs and a nullable zone's nulls would make min = max = c no proof.
  - `pub(crate) fn register(session: &VortexSession)`: `session.stats().register_rewrite(EqSatisfier)`.
  - `pub(crate) struct EqProofs { pub(crate) none: Option<BoundExpression>, pub(crate) all: Option<BoundExpression> }`.
  - `pub(crate) fn eq_proofs(column: &DType, code: u64) -> Result<EqProofs>`. It binds `eq(root(), lit(code))` against `column`. `none` is its `falsify`, and `all` its `satisfy`, both through `&VORTEX_SESSION`.

- [ ] **Step 1: Write the failing tests:**
  - `satisfier_holds_only_where_min_equals_max_equals_c`. With `ColumnZones` bounds mins `[5, 4, 5, 6, 1]`, maxes `[5, 5, 6, 7, 3]`, the `eq_proofs(u64, 5)` masks over `0..5` are:
    - `all`: `[true, false, false, false, false]`;
    - `none`: `[false, false, false, true, true]`.
  - `satisfier_at_the_top_of_u64`. With bounds `(u64::MAX, u64::MAX)` and `(u64::MAX - 1, u64::MAX)`, `eq_proofs(u64, u64::MAX).all` gives `[true, false]`.
  - `no_satisfier_for_floats_or_nullable`. `eq_proofs` with `DType::Primitive(F64, NonNullable)`, and with nullable `u64`, gives `all == None`.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-core --lib stats_rules`.** Expected: compile errors.
- [ ] **Step 3: Implement `stats_rules.rs`, and call `register` in `VORTEX_SESSION`'s initializer**, beside `crate::kernels::register`.
- [ ] **Step 4: Run the same command.** Expected: PASS.
- [ ] **Step 5: Commit:** `feat(core): prove "every row equals c" with a stats rewrite rule`.

### Task 5: The zone decider and its switch

**Files:**
- Create: `core/src/store/query/zone_decider.rs` (`#[cfg(feature = "file-io")]`, declared in the query module)
- Modify: `core/src/store/test_hooks.rs` (`ForcedZones`)
- Test: unit tests in `zone_decider.rs`

**Interfaces:**
- Consumes: `ColumnZones` (Task 3) and `eq_proofs` (Task 4).
- Produces:
  - `pub(crate) enum ZoneClass { None, All, Mixed { unsettled: u8 } }`. `unsettled` holds the bits `1 << slot` of the checked slots that are neither none nor all in that zone.
  - `pub(crate) struct CheckedColumn { pub(crate) slot: usize, pub(crate) code: TermCode, pub(crate) zones: Arc<ColumnZones> }`.
  - `ZoneDecider::new(columns: Vec<CheckedColumn>) -> Option<ZoneDecider>`. It is `None` for no columns, or for columns whose `zone_len` or `zones()` differ.
  - `ZoneDecider::zone_len(&self) -> u64`.
  - `ZoneDecider::classify(&self, zones: Range<u64>) -> Result<Vec<ZoneClass>>`. Its output has one class per zone, in order. A zone is `None` if any column's `none` proof holds, and `All` if every column's `all` proof holds; otherwise it is `Mixed` with the other columns' bits. Each column is pruned once per call.
  - `pub(crate) fn zones_enabled() -> bool`. It reads `VORTEX_RDF_ZONES` once (`!= "0"`), and under `cfg(test)` it gives `test_hooks::forced_zones()` first.
  - `test_hooks::ForcedZones::set(on: bool) -> ForcedZones` (a thread-local `Override<bool>` guard) and `test_hooks::forced_zones() -> Option<bool>`.

- [ ] **Step 1: Write the failing tests** (`ZONE` = 8,192 rows, bounds as `(min, max)` per zone):
  - `one_column_classes`. `g` (slot 3) has bounds `[(5,5), (1,4), (6,9), (5,6), (4,5)]` and code 5. `classify(0..5)` is `[All, None, None, Mixed{1<<3}, Mixed{1<<3}]`.
  - `two_columns_combine`. `o` (slot 2) has bounds `[(7,7), (7,7), (1,9), (8,9), (6,8), (7,7)]` and code 7. `g` has bounds `[(5,5), (1,4), (6,9), (5,6), (4,6), (5,6)]` and code 5. The classes are `[All, None, None, None, Mixed{1<<2 | 1<<3}, Mixed{1<<3}]`.
  - `partial_and_empty_ranges`. `classify(1..3)` gives the classes of zones 1 and 2, and `classify(2..2)` is empty.
  - `new_declines`. An empty column list gives `None`, and so do columns of different `zone_len`.
  - `forced_zones_override`. Under `ForcedZones::set(false)`, `zones_enabled()` is `false`; after the guard drops, it is the environment's value again.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-core --lib zone_decider`.** Expected: compile errors.
- [ ] **Step 3: Implement** `zone_decider.rs` and the `ForcedZones` hook.
- [ ] **Step 4: Run the same command.** Expected: PASS.
- [ ] **Step 5: Commit:** `feat(core): decide a run's zones with Vortex proofs (ZoneDecider)`.

### Task 6: Located table runs decided zone by zone

**Files:**
- Modify: `core/src/store/query/located.rs`
- Modify: `core/src/observe.rs`, which gains the events `ZoneCounted(u64)` and `CheckRead(u64)`
- Create: `core/src/tests/located_zones.rs` (declared beside `mod file_backed;` in `core/src/tests/mod.rs`)
- Modify: `core/src/tests/file_backed.rs` (`check_long_graph_bound_run`'s count assertion)
- Modify: `docs/file-format.md` §8 ("What a file store keeps") and `docs/interfaces.md` (the code-pattern paragraphs)

**Interfaces:**
- Consumes:
  - `ZoneDecider`, `ZoneClass`, `CheckedColumn` and `zones_enabled` (Task 5);
  - `ColumnZones::read` and `LocatedTables.zones` (Task 3);
  - `ColumnChunks::count_eq` (Tasks 1–2).
- Produces (all private to `located.rs`):
  - `struct RunZones { zone_len: u64, first: u64, classes: Vec<ZoneClass> }`: the classes of the run's table zones `first..first + classes.len()`;
  - `LocatedRun.zones: Option<RunZones>`;
  - `async fn column_zones(file, tables, table: usize, component: Option<&str>, slot: usize, source: &Source) -> Result<Option<Arc<ColumnZones>>>`, which fills `tables.zones[table][slot]` on first use, as `2ea916b`'s `zone_bounds` did;
  - `async fn zone_count(&self, zone: u64) -> Result<u64>`.

**Behavior:**
- **Building the run** (`located_run`, gap path). When `zones_enabled()` and every checked slot's `column_zones` is `Some`, build a `ZoneDecider` and set `zones` to `classify(run.start / Z .. run.end.div_ceil(Z))`. Otherwise `zones` is `None` and today's code runs unchanged.
- **`block(at)`, for a run with `zones`.** Cut `at` (positions in the run) at zone boundaries. For each piece:
  - `None`: nothing;
  - `All`: every row of the piece, unread;
  - `Mixed { unsettled }`: the rows that hold every unsettled slot's code, checked in `checked` order. The first slot reads the piece with `read_codes`, the rest use `holding`. Emit `CheckRead(rows)` for each piece read.

  Return `Rows::Ids` ascending, and emit `LocatedBlock(at.len())` once, as today.
- **`zone_count(zone)`** counts the run's rows within the zone:
  - `None`: 0;
  - `All`: the rows;
  - `Mixed` with one unsettled slot: `columns[slot].count_eq(rows, code, ..)`, emitting `ZoneCounted(rows)`;
  - otherwise: the number of rows `block` keeps over them.
- **`window_of_matches`, for a run with `zones`.** This algorithm is `2ea916b`'s `zoned_window` without its memo:

  ```text
  from = offset; to = limit.map(|l| from + l)
  z = first; before = 0
  if to != Some(from):                         // a window with rows
      while z < past:
          n = zone_count(z); if before + n > from { break }; before += n; z += 1
  seen = before; window = []
  while z < past && to.map_or(true, |to| seen < to):
      kept = block(piece of zone z, as run positions)
      append kept[from - seen ..  to - seen] (clamped) to window; seen += kept.len(); z += 1
  total = if whole { seen + Σ zone_count(z..past) } else { seen }
  ```

**Test fixture** (`located_zones.rs`). `zone_quads(graphs: Graphs) -> Vec<Quad>` holds `RUN = 40_000` quads `make_quad("http://example.org/s{i:05}", "http://example.org/type", "Taxon", numbered_graph(g(i)))` and `RUN` quads `make_quad("http://example.org/hub", "http://example.org/link", "o{i:05}", numbered_graph(g(i)))`. `g(i)` by `Graphs`:
- `One` → 1;
- `Long` → `1 + i / (RUN / 2)`;
- `Stretches` → `1 + (i / 1000) % 4`;
- `Alternating` → `1 + i % 4`.

Stores: each `Graphs` written with `IndexConfig::NoIndex` and with `IndexConfig::Copy` (Dictionary, mapped), plus `write_chunked_store_file(&zone_quads(Stretches), 7)` (no zone maps).

Patterns, each for `g1` and for the store's last graph:
- `(? type Taxon g)`: the `index:posg` run, on Copy stores;
- `(hub link ? g)`: the quad table's `(s, p)` run;
- `(hub ? o{i} g)` for `i ∈ {0, 20_000}`: two checked slots without an index.

- [ ] **Step 1: Write the failing tests:**
  - `zoned_runs_answer_as_rows_do`. For every store and pattern, with `ForcedZones::set(true)` and then `ForcedZones::set(false)`, these must be equal between the two settings and equal to the term path (`run_probe` of the decoded terms):
    - `count_code_pattern`;
    - `locate_codes(pattern, offset, Some(limit))` for limit ∈ {1, 3, 64} and offset ∈ {0, 1, total/2, total−1, total}, plus 32 offsets spread over `0..total` with limit 1;
    - `streamed_codes(store, pattern, batch)` for batch ∈ {999, 4096}, against `match_code_pattern(pattern, 0, None)`.
  - `zoned_runs_skip_settled_zones`. Zones on, Copy store, `(? type Taxon g1)`, recorded with a `Recorder`:
    - `Long`: a count emits no `CheckRead`, and the sum of `ZoneCounted` is `< 40_000`; a stream emits `CheckRead` summing `< 40_000`;
    - `Alternating`: a count emits no `CheckRead`, and the sum of `ZoneCounted` is `== 40_000`;
    - the seven-row-leaf store: a count of `(hub link ? g1)` emits `LocatedBlock` summing `40_000` and no `ZoneCounted`.
  - In `check_long_graph_bound_run` (`file_backed.rs`), for `IndexConfig::Copy` with zones enabled, a count emits `ZoneCounted` summing 40,000 and no `LocatedBlock`. The `Reference` case keeps `LocatedBlock == 40_000`.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-core --lib locate`.** It matches `located_zones` and the `test_locate_codes_*` tests. Expected: FAIL, because the events never fire and the counts read blocks.
- [ ] **Step 3: Implement** `RunZones`, `column_zones`, `zone_count`, the zoned `block` and `window_of_matches`, and the events.
- [ ] **Step 4: Run `cargo test -p vortex-rdf-core --lib locate`.** Expected: PASS.
- [ ] **Step 5: Update the docs:**
  - `docs/file-format.md` §8 "What a file store keeps": add a row for the zone maps of located runs' checked columns, read on first use, bounded at two `u64` per 8,192-row zone of each column read.
  - `docs/interfaces.md`: describe the gap path (a bound graph after a free slot), how its checked rows are decided zone by zone, and that counts no longer read the rows of settled zones.
  - Run `scripts/ci-check.sh docs` and fix any `located.rs#L…` anchors that moved.
- [ ] **Step 6: Commit:** `perf(core): decide a located run's checked rows zone by zone`.

### Task 7: The footer level, and `GAP_ROWS` only where zones cannot decide

**Files:**
- Modify: `core/src/store/query/located.rs`
- Modify: `core/src/store/test_hooks.rs` (`ForcedGapRows`)
- Test: `core/src/tests/located_zones.rs`

**Interfaces:**
- Consumes: `eq_proofs` (Task 4), `ColumnZones::from_bounds` (Task 3) and Task 6's run building.
- Produces:
  - `enum FooterDecision { Nothing, Everything, Unknown }`.
  - `fn footer_decision(file: &NativeStoreFile, slot: usize, code: TermCode) -> Result<FooterDecision>`:
    - `Nothing` when `file.can_prune(&eq(get_item(PRIMARY_COLUMNS[slot], root()), lit(code)))?`;
    - `Everything` when `bounds_settle_every_row(footer_bounds, code, row_count)?`. Here `footer_bounds` is the footer's exact `Min` and `Max` of that field (`file.file_stats()`, `get(slot)`), and `None` when the file has no statistics or no exact bounds for it;
    - `Unknown` otherwise.
  - `fn bounds_settle_every_row(bounds: Option<(u64, u64)>, code: TermCode, row_count: u64) -> Result<bool>`. It is `false` for `None`; otherwise it is the single bit of `eq_proofs(u64, code).all` on `ColumnZones::from_bounds([min], [max], row_count, row_count).prune(0..1, ..)`.

    An index table's slot uses the quad table's footer, since a by-copy index holds the same values.
  - `fn gap_rows() -> u64`, which gives `GAP_ROWS` or, under `cfg(test)`, `test_hooks::forced_gap_rows()`.
  - `test_hooks::ForcedGapRows::set(rows: u64)` and `forced_gap_rows() -> Option<u64>`.

**Behavior** (`located_run`, gap path, with zones enabled; zones off keeps today's code):
1. For each checked slot, `footer_decision`:
   - `Nothing`: the run is `RunRows::Table(found.start..found.start)` with no checks, so it answers 0 rows from the same table;
   - `Everything`: the slot leaves `checked`.
2. A run that still has checks needs a `ZoneDecider` (Task 6) or a length `<= gap_rows()`; otherwise the gap path declines, as it does today past `GAP_ROWS`.

- [ ] **Step 1: Write the failing tests:**
  - `footer_settles_a_whole_file`. Zones on, Copy store, recorded with a `Recorder`:
    - `Graphs::One`: a count of `(? type Taxon g1)` is 40,000, with no `CheckRead`, `ZoneCounted` or `LocatedBlock`;
    - every store: `(? type Taxon <code of http://example.org/type>)`, a graph code no `g` holds, counts 0 with no events, and `locate_codes` still answers `Some`.
  - `footer_bounds_decide_every_row`. `bounds_settle_every_row` gives:
    - `false` for `None` (a file without statistics) or `Some((4, 5))`, with code 5;
    - `true` for `Some((5, 5))`, with code 5;
    - `true` for `Some((u64::MAX, u64::MAX))`, with code `u64::MAX`.
  - `gap_cap_only_where_zones_cannot_decide`. With `ForcedGapRows::set(1_000)`:
    - Copy `Alternating` store, `(? type Taxon g1)`: `locate_codes` is `Some` with zones on and `None` with `ForcedZones::set(false)`;
    - the seven-row-leaf store's `(hub link ? g1)`: `None` with zones on;
    - the counts equal the term path in every case.
- [ ] **Step 2: Run `cargo test -p vortex-rdf-core --lib located_zones`.** Expected: FAIL.
- [ ] **Step 3: Implement** `footer_decision`, `gap_rows` and `ForcedGapRows`, and the two run-building steps.
- [ ] **Step 4: Run `cargo test -p vortex-rdf-core --lib locate`.** Expected: PASS.
- [ ] **Step 5: Commit:** `perf(core): settle a checked slot from the footer, cap only runs zones cannot decide`.

### Task 8: Counters and the switch, documented

**Files:**
- Modify: `encoded-search/src/lib.rs` (`counters`)
- Modify: `core/src/store/scan/typed_eq.rs` (its `[u64; 18]`)
- Modify: `core/src/store/query/located.rs` (the notes)
- Modify: `java/README.md` and `docs/interfaces.md`
- Test: `core/src/tests/located_zones.rs`

**Interfaces:**
- Produces:
  - `counters::LEN = 28`;
  - the sites `ZONE_COLUMN = 18`, `ZONE_NONE = 20`, `ZONE_ALL = 22`, `ZONE_COUNTED = 24` and `CHECK_READ = 26`, named `zone_column(_values)`, `zone_none(_values)`, `zone_all(_values)`, `zone_counted(_values)` and `check_read(_values)`;
  - `NAMES: [&str; LEN]` and `snapshot() -> [u64; LEN]`.

**Notes:** `note(site, rows)` at each site:
- `ZONE_COLUMN`: a footer decision of `Nothing` or `Everything` (the run's rows);
- `ZONE_NONE` and `ZONE_ALL`: each zone piece a block or count settles;
- `ZONE_COUNTED`: each `count_eq`;
- `CHECK_READ`: each mixed piece `block` reads.

- [ ] **Step 1: Write the failing test** `zone_counters_count_decisions`, which returns early unless `counters::on()`, as `typed_eq_binds_are_counted` does. A count of `(? type Taxon g1)` on the Copy `Long` store raises `ZONE_ALL`, `ZONE_NONE` and `ZONE_COUNTED`.
- [ ] **Step 2: Run `VORTEX_RDF_COUNTERS=1 cargo test -p vortex-rdf-core --lib zone_counters`.** Expected: compile error, `ZONE_ALL` not found.
- [ ] **Step 3: Implement** the counters and notes, and switch `typed_eq.rs` to `[u64; LEN]`.
- [ ] **Step 4: Run the test with and without `VORTEX_RDF_COUNTERS=1`.** Expected: PASS.
- [ ] **Step 5: Update the docs:** in `java/README.md` (the measurement switches paragraph) and `docs/interfaces.md`, add `VORTEX_RDF_ZONES=0` and the zone counters.
- [ ] **Step 6: Commit:** `feat(core): count zone decisions; document VORTEX_RDF_ZONES`.

### Task 9: Measure P1, and check the branch

**Files:** none in the repo. The harness is the scratch `zonebench` behind the spec's §10 numbers, not committed.

- [ ] **Step 1: Build the stores.** Add a `one` shape (one graph) beside the harness's `spread`, `clustered` and `located` shapes, and build each as by-copy: 4M quads, a 2M-row `(type, Taxon)` run. Also build each without indexes, with the other 2M quads `(hub, p{i % 50}, o{i}, g(i))` under the same graph layout, so that `(hub ? ? g1)` is a 2M-row checked run of the quad table.
- [ ] **Step 2: Measure each store**, with `VORTEX_RDF_ZONES=0` (#145's route) and without, in a release build:
  - count, first and repeated;
  - 32 one-row reads at spread offsets;
  - paging by 4,096;
  - a stream in chunks of 65,536;
  - the first 64 rows;
  - anonymous RSS after the reads.

  Every mode must return the same rows (checksums).
- [ ] **Step 3: Run `scripts/ci-check.sh docs lint rust-tests java-tests`.** Expected: all pass.
- [ ] **Step 4: Draft the PR description** (Why / Changes / Numbers / Checks, as #145's) in the session's scratchpad. Push and open it only once the user has said where.
