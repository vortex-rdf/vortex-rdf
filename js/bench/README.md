# JS benchmarks

Three instruments, kept separate because they answer different questions and are trustworthy in different ways. Run them from `js/`. `npm run bench:codspeed` needs `npm run build`. `npm run bench` and `bench:dict-memory` run on `npm run build:bench-wasm`, the bench's own build of the bindings in `pkg/bench`. `npm run bench` also needs `hdt-pkg/` (`npm run build:hdt-wasm`), `oxigraph-pkg/` (`npm run build:oxigraph-wasm`) and the HDT file. `scripts/refresh.sh --only js` builds `pkg/bench`, `hdt-pkg/`, `oxigraph-pkg/` and the HDT file.

| Command | Question | Output |
| --- | --- | --- |
| `npm run bench` | How does the wasm store compare to other JS RDF stores? | `target/bench/js.json` at the repository root, beside the other suites' results, [rendered to the dashboard](https://vortex-rdf.github.io/vortex-rdf/#js) |
| `npm run bench:codspeed` | Did this change regress anything? | uploaded to CodSpeed |
| `npm run bench:dict-memory` | Where is wasm memory going? | printed; `bench/dict-memory.json` |

Both JSON files are generated and git-ignored. `scripts/refresh.sh` runs every suite and renders the dashboard (`--only js,render` re-measures this one); it needs GNU coreutils (`sha256sum`, `sort -z`).

## Files

| File | Role |
| --- | --- |
| `compare.bench.ts` | Comparative wall-clock suite (tinybench): the Vortex build variants vs [rdf-stores.js](https://github.com/rubensworks/rdf-stores.js), [oxigraph](https://github.com/oxigraph/oxigraph) (its JS binding, [built from source](./oxigraph-wasm/build.sh)) and this [hdt-wasm](https://github.com/KonradHoeffner/hdt#web-assembly) library ([built locally](./hdt-wasm/Cargo.toml)), over the eight matching patterns (S/P/O/G/SP/PO/SPO/SPOG), the full scan, mutation and serialization. Every quad store is read through RDF/JS `match()`, consumed through its `data` events, except oxigraph, whose `match()` returns an array, and hdt, which reads all matched ids, then their strings window by window. A query run times three builds from the dataset, whose strings are flattened once after generation, and queries the last. Orchestrator only: it spawns one `compare.worker.ts` process per adapter and role and collects the rows into the JSON that `scripts/render_bench_dashboard.py` turns into the dashboard's JavaScript tab. Never uploaded to CodSpeed |
| `compare.worker.ts` | One adapter's full lifecycle in its own process, so a crash loses only that adapter's rows and its peak RSS is attributable to it alone, and the `memory` role's runs (see [Memory runs](#memory-runs)) |
| `codspeed.bench.ts` | Library-only instrumented suite, the JS counterpart of `core/benches/benchmark.rs`: layout × secondary index swept across the routing patterns, plus build, read-back and mutation. Runs under the CodSpeed action in instrumentation mode; locally it prints wall-clock numbers. Single process; CodSpeed ids each task as `<file that calls bench.add>::<task name>`, so task names never change and the `bench.add` calls stay in this file |
| `dict-memory.bench.ts` | Attributes wasm linear memory to the term dictionary. Wasm memory never shrinks, so it measures by differentials — several stores held live at once in one `dict-memory.worker.ts` process per config, memory read after each, slope against store count — and either checks one config against the reference figures in its header or sweeps term cardinality (`DICT_MEM_RATIOS`) to fit the per-term cost |
| `dict-memory.worker.ts` | One dictionary-memory config in its own process |
| `datasets.ts` | The synthetic dataset generator (`genDataset`) and probe patterns, pure so the instrumented suite can import it. Term cardinality is an explicit config value: distinct subjects and objects scale with rows, and the Python suite's `bench/datasets.py` is a port with the same moduli and term spellings, so rows on the two dashboard tabs describe the same data |
| `shared.ts` | Store adapters, the run configuration and the memory readers shared by the comparative and dict-memory instruments |
| `vortex.ts` | The Vortex bindings the comparative and dict-memory workers load: the bench's own build in `pkg/bench` (`npm run build:bench-wasm`, the js crate's `memory-stats` feature), loaded as `entry/node.js` loads `pkg/web`; the published build never carries the counter |
| `util.ts` | Pure consume/timing helpers, the memory runs' self-check and figures, and the check that the oxigraph build is the version `bench-compare/Cargo.lock` pins (`checkOxigraphVersion`); imports nothing but Node builtins so the CodSpeed suite can load it without a foreign wasm module in the process |
| `hdt-wasm/` | Wasm build of the `hdt` crate for the comparative suite (`npm run build:hdt-wasm` → `hdt-pkg/`); read-only, over an artifact built natively |
| `oxigraph-wasm/` | Builds oxigraph's official JS binding from its GitHub source at the version `bench-compare/Cargo.lock` pins, with the bench's live-byte counter patched in (`memory-stats.patch`): `npm run build:oxigraph-wasm` → `oxigraph-pkg/`. The suite stops at start when that build's version is not the pinned one. `build.sh` regenerates the committed `oxigraph-pkg/web.d.ts` on every build, so re-commit it after a version bump |
| `memory-stats/` | The bench-only counting global allocator (`bench-memory-stats`): live and peak heap bytes behind `setMemoryCounting`, `memoryStats` and `resetMemoryPeak`. The js crate's `memory-stats` feature installs it; only `pkg/bench` (`npm run build:bench-wasm`) enables that feature, and it is never published. `hdt-wasm/` always links it, and so does the oxigraph build (`oxigraph-wasm/`), through its patch |

## Environment variables

### Shared knobs

These knobs mean the same in every suite that reads one, in all four languages. The suites, by the names the table uses: JS compare (`compare.bench.ts`), JS CodSpeed (`codspeed.bench.ts`), JS dict-memory (`dict-memory.bench.ts`), Python compare (`python/bench/run.py`), Python CodSpeed (`python/bench/test_codspeed.py`), Java compare (`java/bench/run.py`), Rust compare (`bench-compare/benches/compare.rs`) and the Rust internals (`core/benches/benchmark.rs`, `match_lazy.rs`). Each suite's own knobs follow the table.

| Var | Default | Read by | Meaning |
| --- | --- | --- | --- |
| `BENCH_SIZE` | 1,048,576; 32,768 in the Rust internals | JS, Python, Java and Rust compare; the Rust internals | rows |
| `BENCH_DIM` | unset | JS, Python and Java compare | optional cube shorthand, `D³` rows; an explicit `BENCH_SIZE` wins |
| `BENCH_GRAPHS_QUADS` | 8 | JS, Python and Java compare | named graphs the comparative bench asks for, before the generator's coprimality nudge |
| `MUT_BATCH` | 10000; 512 in `benchmark.rs`, which clamps it to 1-4,095 | JS, Python, Java and Rust compare; `benchmark.rs` | quads per add/delete batch |
| `BENCH_TIMEOUT_MS` | 30000 | JS, Python, Java and Rust compare | the time limit for query cells, the same for every library: a match or count (the full scan included) whose first run takes longer than this is not measured, and reads `timeout`; builds, opens and mutations are always measured; `0` disables it |
| `BENCH_MEMORY_RUNS` | 3 | JS, Python, Java and Rust compare | memory runs per adapter, each in a fresh process after the timed runs; `0` runs none, and the page then shows the old peak table |
| `BENCH_SUBJ_RATIO` | 0.1 | JS and Python compare and CodSpeed; Java compare | distinct subjects / rows — the reciprocal of triples per subject |
| `BENCH_OBJ_RATIO` | 0.5 | JS and Python compare and CodSpeed; Java compare | distinct objects / rows |
| `BENCH_PREDICATES` | 32 | JS and Python compare and CodSpeed; JS dict-memory; Java compare | distinct predicates (a closed vocabulary) |
| `BENCH_GRAPHS` | 1 | JS and Python CodSpeed; JS dict-memory; JS compare for triple-only stores | distinct named graphs in the generator; 1 means default graph only (the comparative quad stores take `BENCH_GRAPHS_QUADS`) |
| `BENCH_LITERAL_FRAC` | 0.4 | JS and Python compare and CodSpeed; JS dict-memory; Java compare | fraction of objects that are literals |
| `CODSPEED_BENCH_DIM` | 32 | JS and Python CodSpeed | triples dataset is `D³` rows |
| `CODSPEED_BENCH_DIM_QUADS` | 13 | JS and Python CodSpeed | quads dataset is `D⁴` rows |

The dataset knobs, `BENCH_SUBJ_RATIO` through `BENCH_LITERAL_FRAC`, are read by the generator `datasets.ts` and its Python port `python/bench/datasets.py` (which the Java suite writes its data through too), so the suites built on them describe the same data; JS dict-memory sets both ratios itself, per point. The two ratios decide how much of the dataset is *terms*: the dictionary holds `subjectRatio + objectRatio` of them per quad. `BENCH_SUBJ_RATIO=1.0` gives every row its own subject — a dictionary-stress configuration (what `bench:dict-memory` uses) but a poor default for the comparison, since most probes then match two rows or fewer and the timings become query setup with the decode path barely exercised.

### Comparative suite (`compare.bench.ts`)

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_OUT` | `target/bench/js.json` | where the run's JSON is written; a relative path resolves against `bench/` |
| `CONSUME_BUDGET_MS` | 120000 | a consume loop past this budget throws and lands in the run's `failures` as a failed cell |
| `WORKER_TIMEOUT_MS` | 1800000 | hard kill for a worker stalled inside a single wasm call |
| `HDT_FILE` | `target/bench_js/data.hdt` | the natively built HDT artifact the hdt adapter opens; `scripts/refresh.sh --only js` builds it |

Each adapter process peaks at several GB at the default scale — check available memory before running, because a swapping run produces numbers that look like regressions.

### CodSpeed suite (`codspeed.bench.ts`)

| Var | Default | Meaning |
| --- | --- | --- |
| `CODSPEED_MUT_N` | 500 | add/delete batch size |

### Dictionary-memory instrument (`dict-memory.bench.ts` / `dict-memory.worker.ts`)

| Var | Default | Meaning |
| --- | --- | --- |
| `DICT_MEM_N` | 200000 | rows, held fixed across a sweep |
| `DICT_MEM_SLUGS` | `vortex_dict` | build variants to run; add `vortex_default` (no dictionary) to isolate everything that is not the dictionary |
| `DICT_MEM_RATIOS` | `1.0` | subject ratio per point (object ratio tracks it at half); several values sweep cardinality |
| `DICT_MEM_STORES` | 4 | concurrently live stores the retained-cost slope is read from |
| `DICT_MEM_DELETES` | 5 | deletes run before re-querying, exercising the dictionary-view rebuild |
| `DICT_MEM_OUT` | `bench/dict-memory.json` | where the points are written |

## Memory runs

The dashboard's Store memory panels come from separate memory runs, not from the timed process. That process's peak is mostly harness: it holds collected reads and in-process builds, counts mapped pages twice (as anonymous and as file memory), includes the mutations, and moves by about 60 MB from one run to the next. All four comparative suites (JS, Python, Java and Rust compare) therefore measure each adapter's store on its own, the same way.

After the timed runs, each suite makes `BENCH_MEMORY_RUNS` memory runs per adapter (default 3), each in a fresh process. One run:

1. checks its counters: it holds an 8 MiB block and requires each counter to see it, so a counter that misses memory fails the run instead of reporting 0. Java's native counter must see at least 7 MiB of the block, since the JVM's own threads allocate natively during the check; every other counter sees all 8 MiB. A run that fails the check exits non-zero, naming the counter;
2. takes a baseline before the store exists;
3. obtains the store the way the timed queries use it, which the entry's `obtained` records: `mapped` (memory-mapped from its file), `loaded` (read from a file into memory), `built` (from the triples) or `opened` (an on-disk database), reads its live bytes, disposes of it before any read (the JS suite says "frees"), and reads them again. That gives Store;
4. obtains the store again, the same way, and reads every probe pattern the adapter answers, the full scan included, once;
5. disposes of it.

The figures are in MiB to one decimal, clamped at 0, and `null` where one does not apply:

- **Store:** the live bytes right after the store is obtained minus those after its dispose, on a store no read has touched, so it leaves out the caches reads fill. The reads then run on a second store, obtained the same way. The suites' own sections give the figures.
- **Peaks:** the highest memory reached during the reads, above the baseline taken before the first open. Rust, Python and Java sample `/proc/self/status` every 2 ms and report `RssAnon` (process memory) and `RssFile` (mapped pages) at the sample with the highest sum; JS reports the peak of its live bytes. Mapped pages include the program's own code pages that the open and the reads first touch, a few MB to a few tens of MB by library, and the same for a library's file and in-memory rows. A store held in memory therefore reads that much there, not 0, and the gap between a pair's rows is the file's own pages.

With `BENCH_MEMORY_RUNS=0` no suite writes memory entries, and the page shows the old table of query peaks. With 1 or more, every adapter gets an entry even if all its runs fail, and its row then reads "—".

On the page, each cell is the median over the runs, and its tooltip gives the range, the number of runs and how the store was obtained. Each column is ranked with the smallest green and the others as a multiple of it; a figure under 1 MB reads "< 1 MB" and ranks as 1 MB, and "—" is never ranked. A "≈" marks a file / in-memory pair whose medians differ by less than the wider of their two ranges, which is run-to-run spread. Result files written without memory runs keep the query-peak table. A query entry's `peakRssMb` stays in the JSON, as the timed process's peak, for the memory-pressure warning.

Each suite documents its own run:

- Rust compare: `bench-compare/benches/compare.rs`, in `# Memory runs`;
- Python: `python/bench/README.md`, in `## Memory runs`;
- Java: `java/bench/README.md`, in `## Memory runs`;
- JS: the subsection below.

### The JavaScript suite

After the timed roles, `compare.bench.ts` runs each adapter's `memory` role `BENCH_MEMORY_RUNS` times (default 3), each in a `compare.worker.ts` process of its own. A run reads live bytes: the store module's counter (the bench's own Vortex build, `hdt-wasm` and the oxigraph build all link it) plus `heapUsed` after `global.gc()`. rdf-stores has no wasm module, so it has the JS heap only. One run:

- **Self-check:** before anything else, each counter must see an 8 MiB block held, from the module's own allocator for a wasm counter and an array of doubles for the JS heap. A run that fails it exits 1, naming the counter.
- **The store:** built from the flattened dataset, which is passed inline so nothing keeps it alive; hdt is loaded from its HDT file. It is the store the timed cells query, which `obtained` names: `built` for Vortex, rdf-stores and oxigraph, `loaded` for hdt. The run obtains it twice: first to measure Store, then freed before any read, and again for the reads.
- **The reads:** on the second store, each probe and the full scan once, read the way the timed cells read them, with their row counts recorded. A `fromBytes` store is loaded again and given the same reads, which must count the built store's rows, or the run fails.
- **Settling:** before each reading and at each probe's start, rounds of GC, a 50 ms event-loop turn and GC again, repeated until a round frees less than 1 MiB of live bytes, and 10 rounds at most. The turn lets FinalizationRegistry callbacks run: `free()` leaves a Vortex store's term dictionary to one, and every oxigraph quad and term a read returns is freed by one. V8 runs one registry's callbacks per task, so after a large read one turn is not enough: after oxigraph's full scan at 1,048,576 rows they take seconds and several rounds.
- **Vortex:** once the built store's reads are done, the run takes its `toBytes()` before freeing it, loads that with `fromBytes`, measures the loaded store right after the load with no read, frees it, and loads it again for the same reads, whose counts must be the built store's.

The figures, in MiB to one decimal, clamped at 0 and `null` where they do not apply:

- **Store, built from the triples** (`storeBuiltMb`: Vortex, rdf-stores and oxigraph) and **Store, loaded from a file** (`storeLoadedMb`: Vortex's `fromBytes`, and hdt): the live bytes right after the store is built or loaded, minus those after its free, both settled, on a store no read has touched, so it leaves out the caches reads fill. The reads then run on a second store, obtained the same way. Vortex Dict at 1,048,576 rows reads about 23 MiB built and 17 MiB loaded, the store's own wasm memory. The reads then fill two caches that Store leaves out and the peak includes:
  - **the term cache:** about 70 MiB of term strings in the JS heap. The store's `LazyDict` keeps each term a read decoded, and the full scan decodes all 629k;
  - **a built store's decoded code columns:** 32 MiB of wasm. A build keeps its four code columns compressed behind `vortex.shared` wrappers, and the first `match()` decodes each into its wrapper's cache, at 8 bytes per row. A `fromBytes` store keeps its columns encoded and decodes them on every read, so it holds no such cache.
- **Peak: live memory** (`peakLiveMb`): per probe, `resetMemoryPeak()`, the read, GC, then the counter's peak plus `heapUsed`; the highest of these above the baseline, which is read before the first store is obtained. oxigraph's `match()` collects its result, which the run holds until that reading. A streaming read's quads are collected by then, so its current chunk's JS objects are not seen.
- No RSS figure: wasm memory never shrinks, so a freed store's pages stay with the process.

An adapter whose query worker failed, or counted nothing, gets no memory runs: there are no counts to check them against. Otherwise its runs stop at its first failed worker, which `failures` records, and at its first run whose row counts differ from its own query and full-scan runs': that run is dropped, and the difference is a count warning. Every adapter still gets its entry, with the runs that finished, so its row reads "—" when none did; its `obtained` is then `null`. With `BENCH_MEMORY_RUNS=0` the suite writes no memory entries.

## Running

```bash
npm run build:bench-wasm                    # pkg/bench: the bindings the comparative and dict-memory suites load
npm run build:hdt-wasm                      # bench/hdt-pkg (the HDT file comes from scripts/refresh.sh --only js)
npm run build:oxigraph-wasm                 # bench/oxigraph-pkg, at the version bench-compare pins
npm run build                               # pkg/web: the published build, which the CodSpeed suite loads
npm run bench                               # full comparative run
BENCH_DIM=25 MUT_BATCH=2000 npm run bench   # quick local check (25³ rows)
npm run bench:codspeed                      # instrumented suite, wall-clock locally
CODSPEED_BENCH_DIM=48 npm run bench:codspeed
npm run bench:dict-memory                   # one config, regression check
DICT_MEM_RATIOS=0.001,0.01,0.1,0.5,1.0 DICT_MEM_SLUGS=vortex_dict,vortex_default npm run bench:dict-memory
npm run typecheck                           # also type-checks this directory (bench/tsconfig.json)
```
