# JS benchmarks

Three instruments, kept separate because they answer different questions and are trustworthy in different ways. All require `npm run build` first (run from `js/`).

| Command | Question | Output |
| --- | --- | --- |
| `npm run bench` | How does the wasm store compare to other JS RDF stores? | `bench/results.json`, [rendered to the dashboard](https://vortex-rdf.github.io/vortex-rdf/#js) |
| `npm run bench:codspeed` | Did this change regress anything? | uploaded to CodSpeed |
| `npm run bench:dict-memory` | Where is wasm memory going? | printed; `bench/dict-memory.json` |
| `npm run bench:dict-form` | What does an adopted store's dictionary form (`fromBytes(bytes, { dictionary })`) cost per read? | printed |

`results.json` and `dict-memory.json` are generated and git-ignored.

## Files

| File | Role |
| --- | --- |
| `compare.bench.ts` | Comparative wall-clock suite (tinybench): the Vortex build variants vs [rdf-stores.js](https://github.com/rubensworks/rdf-stores.js), [oxigraph](https://github.com/oxigraph/oxigraph) and this [hdt-wasm](https://github.com/KonradHoeffner/hdt#web-assembly) library ([built locally](./hdt-wasm/Cargo.toml)), over the eight matching patterns (S/P/O/G/SP/PO/SPO/SPOG), the full scan, mutation and serialization. Orchestrator only: it spawns one `compare.worker.ts` process per adapter and role and collects the rows into the JSON that `scripts/render_bench_dashboard.py` turns into the dashboard's JavaScript tab. Never uploaded to CodSpeed |
| `compare.worker.ts` | One adapter's full lifecycle in its own process, so a crash loses only that adapter's rows and its peak RSS is attributable to it alone |
| `codspeed.bench.ts` | Library-only instrumented suite, the JS counterpart of `core/benches/benchmark.rs`: layout × secondary index swept across the routing patterns, plus build, read-back and mutation. Runs under the CodSpeed action in instrumentation mode; locally it prints wall-clock numbers. Single process; task names are CodSpeed ids and never change |
| `dict-memory.bench.ts` | Attributes wasm linear memory to the term dictionary. Wasm memory never shrinks, so it measures by differentials — several stores held live at once in one `dict-memory.worker.ts` process per config, memory read after each, slope against store count — and either checks one config against the reference figures in its header or sweeps term cardinality (`DICT_MEM_RATIOS`) to fit the per-term cost |
| `dict-memory.worker.ts` | One dictionary-memory config in its own process |
| `dict-form.bench.ts` | Prices the two resident forms of an adopted store's term dictionary (`'as-written'`, `'plaintext'`) on what a wasm consumer does — adopt, probe, decode, scan, export terms — as wall-clock timings (tinybench). Memory is the counting-allocator example's job (`core/examples/dict_memory.rs`): wasm linear memory only grows, so an adopted store's footprint hides inside the build transient |
| `datasets.ts` | The synthetic dataset generator (`genDataset`) and probe patterns, pure so the instrumented suite can import it. Term cardinality is an explicit config value: distinct subjects and objects scale with rows, and the Python suite's `bench/datasets.py` is a port with the same moduli and term spellings, so rows on the two dashboard tabs describe the same data |
| `shared.ts` | Store adapters, the run configuration and the memory readers shared by the comparative and dict-memory instruments |
| `util.ts` | Pure consume/timing helpers; imports nothing but Node builtins so the CodSpeed suite can load it without a foreign wasm module in the process |
| `hdt-wasm/` | Wasm build of the `hdt` crate for the comparative suite (`npm run build:hdt-wasm` → `hdt-pkg/`); read-only, over an artifact built natively |

## Environment variables

Comparative suite (`compare.bench.ts`, read in `shared.ts`/`datasets.ts`/`util.ts`):

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_SIZE` | 1,048,576 | rows (value shared with the Rust and Python suites) |
| `BENCH_DIM` | unset | optional cube shorthand, `D³` rows; an explicit `BENCH_SIZE` wins |
| `BENCH_GRAPHS_QUADS` | 8 | named graphs the comparative bench asks for, before the generator's coprimality nudge |
| `MUT_BATCH` | 10000 | quads per add/delete batch |
| `BENCH_SUBJ_RATIO` | 0.1 | distinct subjects / rows — the reciprocal of triples per subject |
| `BENCH_OBJ_RATIO` | 0.5 | distinct objects / rows |
| `BENCH_PREDICATES` | 32 | distinct predicates (a closed vocabulary) |
| `BENCH_GRAPHS` | 1 | distinct named graphs in the generator; 1 means default graph only |
| `BENCH_LITERAL_FRAC` | 0.4 | fraction of objects that are literals |
| `BENCH_OUT` | `bench/results.json` | where the run's JSON is written |
| `BENCH_SLOW_PHASE_MS` | 30000 | a phase slower than this is measured once, without warmup (`samples: 1` in the row) |
| `CONSUME_BUDGET_MS` | 120000 | a consume loop past this budget throws and lands in the run's `failures` as a failed cell |
| `WORKER_TIMEOUT_MS` | 1800000 | hard kill for a worker stalled inside a single wasm call |
| `HDT_FILE` | `target/bench_compare/hdt/data.hdt` | the natively built HDT artifact the hdt adapter opens |

The two ratios decide how much of the dataset is *terms*: the dictionary holds `subjectRatio + objectRatio` of them per quad. `BENCH_SUBJ_RATIO=1.0` gives every row its own subject — a dictionary-stress configuration (what `bench:dict-memory` uses) but a poor default for the comparison, since most probes then match two rows or fewer and the timings become query setup with the decode path barely exercised.

Each adapter process peaks at several GB at the default scale — check available memory before running, because a swapping run produces numbers that look like regressions.

CodSpeed suite (`codspeed.bench.ts`):

| Var | Default | Meaning |
| --- | --- | --- |
| `CODSPEED_BENCH_DIM` | 32 | triples dataset is `D³` rows |
| `CODSPEED_BENCH_DIM_QUADS` | 13 | quads dataset is `D⁴` rows |
| `CODSPEED_MUT_N` | 500 | add/delete batch size |

Dictionary-memory instrument (`dict-memory.bench.ts` / `dict-memory.worker.ts`):

| Var | Default | Meaning |
| --- | --- | --- |
| `DICT_MEM_N` | 200000 | rows, held fixed across a sweep |
| `DICT_MEM_SLUGS` | `vortex_dict` | build variants to run; add `vortex_default` (no dictionary) to isolate everything that is not the dictionary |
| `DICT_MEM_RATIOS` | `1.0` | subject ratio per point (object ratio tracks it at half); several values sweep cardinality |
| `DICT_MEM_STORES` | 4 | concurrently live stores the retained-cost slope is read from |
| `DICT_MEM_DELETES` | 5 | deletes run before re-querying, exercising the dictionary-view rebuild |
| `DICT_MEM_OUT` | `bench/dict-memory.json` | where the points are written |

## Running

```bash
npm run build
npm run bench                               # full comparative run
BENCH_DIM=25 MUT_BATCH=2000 npm run bench   # quick local check (25³ rows)
npm run bench:codspeed                      # instrumented suite, wall-clock locally
CODSPEED_BENCH_DIM=48 npm run bench:codspeed
npm run bench:dict-memory                   # one config, regression check
DICT_MEM_RATIOS=0.001,0.01,0.1,0.5,1.0 DICT_MEM_SLUGS=vortex_dict,vortex_default npm run bench:dict-memory
npm run typecheck                           # also type-checks this directory (bench/tsconfig.json)
```
