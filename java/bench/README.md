# Java benchmarks

`run.py` measures the Java bindings against [hdt-java](https://github.com/rdfhdt/hdt-java), [qEndpoint](https://github.com/the-qa-company/qEndpoint)'s HDT library (`qendpoint-core`), [Apache Jena](https://jena.apache.org/) (its in-memory dataset and TDB2) and [Eclipse RDF4J](https://rdf4j.org/) (MemoryStore and the LMDB store). The workload is file → store → query, and the results go to `target/bench/java.json` at the repository root for [the dashboard's Java tab](https://vortex-rdf.github.io/vortex-rdf/#java). `scripts/refresh.sh` runs every suite and renders the dashboard; `--only java,render` re-measures this one.

```bash
scripts/refresh.sh --only java              # build what the suite needs, then the full run
python3 java/bench/run.py                   # the full run, once the builds below are done
BENCH_DIM=16 python3 java/bench/run.py      # quick pilot
mvn -f java/bench/pom.xml test -Dvortexrdf.library=$PWD/target/release/libvortex_rdf_jni.so \
    -Dvortexrdf.cli=$PWD/target/release/vortex-rdf-cli   # every adapter against the committed fixture
```

Requirements:
- JDK 25 or newer, with `javac`. RDF4J 6 needs it, and the same JDK runs every worker.
- Linux with glibc: the Store figure reads glibc's allocator statistics.
- Maven.
- `scripts/refresh.sh` checks both before its first stage. A run of `run.py` alone needs three builds first:
  - `cargo build --release -p vortex-rdf-java`;
  - `cargo build --release -p vortex-rdf-cli`, a separate invocation, so the CLI's allocator feature stays out of the JNI library, whose native memory the Store figure reads through glibc;
  - `mvn -f java/bench/pom.xml package -DskipTests`.

Harness design:
- **The dataset and the orchestration are the Python suite's.** `run.py` writes its data through `python/bench/datasets.py` and runs through `python/bench/suite.py`, so this tab measures the Python tab's data by the same rules.
- **Isolation.** Each adapter runs in its own JVM, on its library's own classpath (`target/<module>.jar` and `target/lib/`): the counterpart of the Python suite's one virtualenv per library.
- **The worker.** `harness/.../Worker.java` holds the phases, including a JIT warm-up before any pattern is timed. It writes its result to a file, never to stdout, which some libraries log to.
- **Memory and size.**
  - Each query and mutate worker's peak memory comes from `wait4`, so the Vortex CLI that a build runs counts too. That peak still drives the dashboard's memory-pressure warning.
  - The memory runs measure the store alone, in `BENCH_MEMORY_RUNS` more JVMs per adapter (see [Memory runs](#memory-runs)). Store comes from `Footprint` around the dispose, with the store on a thread of its own. The two peaks come from a 2 ms sampler during the reads, above a baseline read before the open. Each run first checks that both of `Footprint`'s counters see a block held.
  - Sizes on disk are allocated blocks.
- **The fixture and the adapters.** Every adapter must pass `AdapterContract` on the committed 512-row fixture, which `run.py --fixture` regenerates. Each library's setup is in its module's adapter.

The knobs this suite shares with the others (`BENCH_SIZE`, `BENCH_DIM`, `BENCH_GRAPHS_QUADS`, `MUT_BATCH`, `BENCH_TIMEOUT_MS` and the dataset shape) are in [the shared table](../../js/bench/README.md#shared-knobs). This suite's own:

| Var | Default | Meaning |
| --- | --- | --- |
| `JAVA_BENCH_QUERY_ITERS` / `JAVA_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `JAVA_BENCH_HEAVY_ITERS` / `JAVA_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
| `JAVA_BENCH_JIT_WARMUP_MS` | 2000 | the JIT warm-up budget, split evenly between the patterns: each runs, discarded, for its share before any is timed |
| `JAVA_BENCH_XMX` | 6g | every worker JVM's `-Xmx` |

## Memory runs

Each adapter's memory role runs in a JVM of its own, `BENCH_MEMORY_RUNS` times (default 3; the shared section in [the JS README](../../js/bench/README.md#memory-runs) describes the panels), after its query and mutate workers. A run opens the store the query worker built, reads every probe pattern once, then disposes the store, and reports:

- **Store:** the live bytes released by disposing it, read after the reads and again after the dispose, each after a full collection (`Footprint`): the Java heap in use, plus glibc's `mallinfo2` (`uordblks + hblkhd`) less the JVM's own allocations, which its native memory tracking (`-XX:NativeMemoryTracking=summary`) reports;
- **two peaks:** `RssAnon` and `RssFile`, the sample with the highest sum out of a 2 ms `/proc/self/status` sampler during the reads, minus a baseline taken before the store was opened.

The store lives on a thread of its own, which ends with the dispose, as the query worker's warm handle does: a library may leave a store's data reachable from the thread that queried it, as Jena's in-memory dataset does through a thread-local, and that data is the store's. Before opening anything, the run holds an 8 MiB array and 8 MiB of native memory, and checks that both counters saw them. The heap's must rise by the whole 8 MiB, and the native one by at least 7 MiB of it: the JVM's own threads (the JIT's, the collector's) allocate natively during the check, while a counter that misses the block misses it by about 8 MiB. A run that fails the check, or meets any other error, exits non-zero with the reason on stderr, and that adapter's runs end there while the suite goes on.

The Vortex file builds are mapped and the memory ones loaded; hdt-java and qEndpoint are mapped; Jena TIM and RDF4J's MemoryStore are built from the triples; Jena TDB2 and RDF4J's LMDB store are opened. The Vortex read goes through `iterateQuads`, at most 65,536 rows a chunk. The mapped-pages peak includes the code that the open and the reads run for the first time: for the Vortex rows, their JNI library's, about 15 MB.

The peaks follow G1's heap sizing and the JIT's compilations as well as the store, so Store is the figure to compare between libraries.
