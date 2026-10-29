# Java benchmarks

`run.py` measures the Java bindings against [hdt-java](https://github.com/rdfhdt/hdt-java), [qEndpoint](https://github.com/the-qa-company/qEndpoint)'s HDT library (`qendpoint-core`), [Apache Jena](https://jena.apache.org/) (its in-memory dataset and TDB2) and [Eclipse RDF4J](https://rdf4j.org/) (MemoryStore, NativeStore and the LMDB store). The workload is file → store → query, and the results go to `target/bench/java.json` at the repository root for [the dashboard's Java tab](https://vortex-rdf.github.io/vortex-rdf/#java). `scripts/refresh.sh` runs every suite and renders the dashboard; `--only java,render` re-measures this one.

```bash
scripts/refresh.sh --only java              # build what the suite needs, then the full run
python3 java/bench/run.py                   # the full run, once the builds below are done
BENCH_DIM=16 python3 java/bench/run.py      # quick pilot
mvn -f java/bench/pom.xml test -Dvortexrdf.library=$PWD/target/release/libvortex_rdf_jni.so \
    -Dvortexrdf.cli=$PWD/target/release/vortex-rdf-cli   # every adapter against the committed fixture
```

Requirements:
- JDK 25 or newer, with `javac`. RDF4J 6 needs it, and the same JDK runs every worker.
- Linux with glibc: the retained-memory figure reads glibc's allocator statistics.
- Maven.
- `scripts/refresh.sh` checks both before its first stage. A run of `run.py` alone needs three builds first:
  - `cargo build --release -p vortex-rdf-java`;
  - `cargo build --release -p vortex-rdf-cli`, a separate invocation, so the CLI's allocator feature stays out of the JNI library, whose native memory the retained figure reads through glibc;
  - `mvn -f java/bench/pom.xml package -DskipTests`.

Harness design:
- **The dataset and the orchestration are the Python suite's.** `run.py` writes its data through `python/bench/datasets.py` and runs through `python/bench/suite.py`, so this tab measures the Python tab's data by the same rules.
- **Isolation.** Each adapter runs in its own JVM, on its library's own classpath (`target/<module>.jar` and `target/lib/`): the counterpart of the Python suite's one virtualenv per library.
- **The worker.** `harness/.../Worker.java` holds the phases, including a JIT warm-up before any pattern is timed. It writes its result to a file, never to stdout, which some libraries log to.
- **Memory and size.**
  - Each worker's peak memory comes from `wait4`, so the Vortex CLI that a build runs counts too.
  - What the open store retains is what disposing its warm handle releases, sampled after a full collection on either side: the live heap, and glibc's bytes in use less the JVM's own allocations, which its native memory tracking (`-XX:NativeMemoryTracking=summary`) reports. The warm handle lives on a thread of its own, since a library may leave a store's data reachable from the querying thread's thread-locals, as Jena's in-memory dataset does.
  - Store sizes are allocated blocks.
- **The fixture and the adapters.** Every adapter must pass `AdapterContract` on the committed 512-row fixture, which `run.py --fixture` regenerates. Each library's setup is in its module's adapter.

The knobs this suite shares with the others (`BENCH_SIZE`, `BENCH_DIM`, `BENCH_GRAPHS_QUADS`, `MUT_BATCH`, `BENCH_SLOW_PHASE_MS` and the dataset shape) are in [the shared table](../../js/bench/README.md#shared-knobs). This suite's own:

| Var | Default | Meaning |
| --- | --- | --- |
| `JAVA_BENCH_QUERY_ITERS` / `JAVA_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `JAVA_BENCH_HEAVY_ITERS` / `JAVA_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
| `JAVA_BENCH_JIT_WARMUP_MS` | 2000 | the JIT warm-up budget, split evenly between the patterns: each runs, discarded, for its share before any is timed |
| `JAVA_BENCH_XMX` | 6g | every worker JVM's `-Xmx` |
