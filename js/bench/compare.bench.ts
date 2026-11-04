// Comparative benchmark: VortexRdfStore (JS/WASM) config variants vs. rdf-stores.js,
// oxigraph and hdt (read-only wasm over a natively pre-built artifact — see the
// hdt section in shared.ts). Emits dashboard-shaped JSON, target/bench/js.json, consumed
// by scripts/render_bench_dashboard.py (the JavaScript tab of the GitHub Pages dashboard).
//
// Run (after `npm run build:bench-wasm`, `build:hdt-wasm` and `build:oxigraph-wasm`,
// which write pkg/bench, bench/hdt-pkg and bench/oxigraph-pkg; `scripts/refresh.sh
// --only js` runs all three and writes the HDT file the hdt adapter opens):
//   npm run bench                         # full run (1,048,576 triples by default)
//   BENCH_DIM=25 MUT_BATCH=2000 npm run bench   # quick local check (25³ rows)
//
// The dataset is synthetic but realistic in the dimension that matters for a store
// comparison: distinct terms scale with rows (ten triples per subject, a small closed
// predicate vocabulary, a mix of IRI and literal objects), so each library's term
// handling — dictionaries, interning, string storage — is actually exercised. It is
// driven across the eight routing shapes the Rust dashboard also uses
// (S/P/O/G/SP/PO/SPO/SPOG) plus the full scan. Note this diverges from rdf-stores.js's own harness,
// whose dataset draws D^3 rows from only D distinct IRIs.
//
// This file is the ORCHESTRATOR only — it spawns one child process per adapter
// (compare.worker.ts) and collects the results. Sharing one process lets an
// earlier adapter's peak WASM/JS memory contaminate later ones: a wasm module
// can trap with `unreachable` purely because other multi-million-quad stores
// already grew and were freed in the same process, while running clean in
// isolation on the same dataset. Process-per-adapter also means a crash in one
// adapter loses only that adapter's rows, not the whole run, and gives each
// adapter a trustworthy, uncontaminated peak-RSS reading (see shared.ts's
// `peakRssMb`).
//
// Uses tinybench. `withCodSpeed` is not applied here: this suite is wall-clock
// and never uploaded — the instrumented, uploaded JS suite is codspeed.bench.ts.

import { createRequire } from 'node:module';
import { cpus } from 'node:os';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import {
    ADAPTERS, MUT_ADAPTERS, N_TRIPLES, GRAPHS, MUT_BATCH,
    QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS, moduli, unsupportedRow,
    type Obtained, type Row, type WorkerOutput,
} from './shared.js';
import {
    BENCH_COMPARE_LOCK, OXIGRAPH_PKG_DIR, checkOxigraphVersion, runWorkerProcess,
    type JsMemoryRun,
} from './util.js';

/** The run's dataset shape: term cardinality per role, from which every
 *  probe's selectivity follows. */
const SHAPE = moduli(N_TRIPLES, { graphs: GRAPHS });

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const workerPath = resolve(here, 'compare.worker.ts');

const OUT = resolve(here, process.env.BENCH_OUT ?? '../../target/bench/js.json');

/** Memory runs per adapter, each in a fresh process after the timed roles:
 *  the store alone, in live bytes (see runMemory in compare.worker.ts). `0`
 *  runs none and writes no memory entries. */
const MEMORY_RUNS = Number(process.env.BENCH_MEMORY_RUNS ?? 3);

/** One entry of the results file's `memory` list. */
type MemoryRow =
    | {
        slug: string; label: string; role: 'query' | 'mutate';
        /** The timed run's whole-process high-water mark, which the page's
         *  memory-pressure note reads, and its peak table when the file has
         *  no memory runs. */
        peakRssMb: number | null;
    }
    | {
        slug: string; label: string; role: 'memory';
        /** How the store the timed cells and the peak use was obtained; null
         *  when no run was kept. */
        obtained: Obtained | null;
        /** The runs that finished with the adapter's own counts. */
        runs: JsMemoryRun[];
    };

type Role = 'query' | 'querycold' | 'fullscan' | 'mutate' | 'memory';

/** One adapter, one role, one process. A worker that cannot finish is skipped:
 *  the remaining adapters still run and still produce their rows. The caller
 *  records a failure for a null return, so the missing rows stay attributed on
 *  the page as failures, not as benchmarks nobody ran. */
function runWorker(slug: string, role: Role): WorkerOutput | null {
    return runWorkerProcess<WorkerOutput>(workerPath, [slug, role], `${slug}/${role}`);
}

async function main(): Promise<void> {
    // oxigraph runs from the bench's own build (bench/oxigraph-wasm), which
    // must be the version the Rust comparison pins: a missing or stale build
    // stops the run here, before any worker starts.
    const oxigraphVersion = checkOxigraphVersion(BENCH_COMPARE_LOCK, OXIGRAPH_PKG_DIR);
    console.log(
        `Dataset shape: ${N_TRIPLES.toLocaleString()} quads over ${SHAPE.nGraph} named graphs ` +
        `(${SHAPE.terms.toLocaleString()} distinct terms)…`);

    const results: Row[] = [];
    const memory: MemoryRow[] = [];
    const matched: Record<string, number> = {};
    // Phases an adapter could not complete. Recorded so a missing row on the
    // dashboard is visibly attributed to a store that could not do the work,
    // not read as a benchmark nobody ran.
    const failures: { slug: string; label: string; role: string; phase: string; error: string }[] = [];

    // Every adapter queries the same dataset with the same probes, so their match
    // counts must agree. A disagreement is a correctness bug in one of them, not a
    // benchmarking detail — surface it; the first adapter's count is kept as
    // the reference. Applied to every role that counts anything: the full
    // scan reports from its own process, and it is the strongest check of the set.
    const countWarnings: string[] = [];
    const mergeMatched = (label: string, out: WorkerOutput): void => {
        for (const [pat, n] of Object.entries(out.matched)) {
            if (matched[pat] === undefined) matched[pat] = n;
            else if (matched[pat] !== n) {
                // Into the results file, not just the console: the dashboard
                // renders these, so a disagreement is visible where the numbers
                // are read, not only in the console output.
                const msg = `${label} matched ${n} rows for '${pat}', an earlier adapter matched ${matched[pat]}`;
                // Once: an adapter's memory runs repeat its timed runs' counts.
                if (countWarnings.includes(msg)) continue;
                countWarnings.push(msg);
                console.error(`  !! ${msg}`);
            }
        }
    };

    /** Each adapter's own counts, from its query and full-scan runs: what its
     *  memory runs must count too. */
    const ownMatched: Record<string, Record<string, number>> = {};
    const keepOwn = (a: { slug: string }, out: WorkerOutput): void => {
        Object.assign(ownMatched[a.slug] ??= {}, out.matched);
    };
    /** The adapters whose query worker reported counts: only these get memory
     *  runs, whose counts are checked against them. */
    const queried = new Set<string>();

    /** Run `role` for each of `adapters`, each in its own process, `times`
     *  times in a row: keep its rows and failures, and hand its output to
     *  `after` for what the role adds. An adapter's runs stop at its first
     *  worker that fails, and at a run `after` returns false for. */
    const runRole = (
        adapters: { slug: string; label: string }[], role: Role,
        after?: (a: { slug: string; label: string }, out: WorkerOutput) => boolean | void,
        times = 1,
    ): void => {
        for (const a of adapters) {
            for (let k = 0; k < times; k++) {
                const out = runWorker(a.slug, role);
                if (!out) {
                    // A worker that never produced output — crash or the
                    // WORKER_TIMEOUT_MS kill — still gets a failures entry, so its
                    // absent rows are attributed in the results file, exactly like
                    // an in-worker phase failure.
                    failures.push({
                        slug: a.slug, label: a.label, role, phase: 'worker',
                        error: 'worker process did not finish (crash or WORKER_TIMEOUT_MS backstop) — see the run log',
                    });
                    break;
                }
                results.push(...out.rows);
                for (const f of out.failures) {
                    failures.push({ slug: a.slug, label: a.label, role, ...f });
                    console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
                }
                if (after?.(a, out) === false) break;
            }
        }
    };

    runRole(ADAPTERS, 'query', (a, out) => {
        memory.push({
            slug: a.slug, label: a.label, role: 'query',
            peakRssMb: out.peakRssMb,
        });
        keepOwn(a, out);
        if (Object.keys(out.matched).length > 0) queried.add(a.slug);
        mergeMatched(a.label, out);
        console.log(`[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB`);
    });

    // The cold regime gets its own process per adapter: every iteration adopts a
    // fresh store, and the first query on one retains its whole buffer past
    // free() in wasm memory that never shrinks. Sharing the query process piles
    // those adoptions on top of a live store until the wasm allocator traps.
    // Adapters with no persistent form return no rows here (see runQueryCold).
    runRole(ADAPTERS, 'querycold');

    // The full scan gets its own process per adapter, for the same reason each
    // adapter gets one: it is heavy enough that a store's retained memory makes
    // it order-dependent, so sharing a process with the query and ingest phases
    // measures the residue of those on top of the scan. See runFullScan.
    runRole(ADAPTERS, 'fullscan', (a, out) => {
        keepOwn(a, out);
        mergeMatched(a.label, out);
    });

    runRole(MUT_ADAPTERS, 'mutate', (a, out) => {
        memory.push({ slug: a.slug, label: a.label, role: 'mutate', peakRssMb: out.peakRssMb });
        console.log(`[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB`);
    });

    // The memory runs: MEMORY_RUNS fresh processes per adapter, each measuring
    // the store alone (see runMemory in compare.worker.ts). An adapter's runs
    // stop at its first failed worker, and at its first run whose counts
    // differ from its own query and full-scan runs': that run is dropped, and
    // the difference is a count warning. An adapter whose query worker failed,
    // or counted nothing, gets none: there is nothing to check its counts by.
    const memoryRuns: Record<string, JsMemoryRun[]> = {};
    const mb = (x: number | null) => (x === null ? '—' : x.toFixed(1));
    if (MEMORY_RUNS > 0) {
        for (const a of ADAPTERS) {
            if (!queried.has(a.slug)) {
                console.error(`  !! ${a.label}: no memory runs, as its query worker reported no counts`);
            }
        }
    }
    runRole(ADAPTERS.filter((a) => queried.has(a.slug)), 'memory', (a, out) => {
        const runs = (memoryRuns[a.slug] ??= []);
        const own = ownMatched[a.slug] ?? {};
        const differ = Object.entries(own).filter(([pat, n]) => out.matched[pat] !== n);
        if (differ.length > 0) {
            const msg = `${a.slug}: memory run ${runs.length + 1} matched `
                + differ.map(([pat, n]) => `${out.matched[pat]} rows for '${pat}' where its timed runs matched ${n}`)
                    .join(', ');
            countWarnings.push(msg);
            console.error(`  !! ${msg} — no more memory runs for it`);
            return false;
        }
        if (!out.memoryRun) {
            failures.push({
                slug: a.slug, label: a.label, role: 'memory', phase: 'worker',
                error: 'the memory worker wrote no memory run — see the run log',
            });
            return false;
        }
        mergeMatched(a.label, out);
        const run = out.memoryRun;
        runs.push(run);
        console.log(`[${a.label}] memory: store built ${mb(run.storeBuiltMb)} / loaded ${mb(run.storeLoadedMb)} MB, `
            + `peak ${mb(run.peakLiveMb)} MB`);
    }, MEMORY_RUNS);
    // Every adapter gets an entry, even with no run kept, so its row reads "—"
    // rather than vanishing; with BENCH_MEMORY_RUNS=0 there are none at all,
    // and the page keeps the query-peak table.
    if (MEMORY_RUNS > 0) {
        for (const a of ADAPTERS) {
            const runs = memoryRuns[a.slug] ?? [];
            memory.push({
                slug: a.slug, label: a.label, role: 'memory',
                obtained: runs.length > 0 ? a.obtained : null, runs,
            });
        }
    }

    // A library whose model rules mutation out entirely (hdt: the file is
    // immutable once built) is not in MUT_ADAPTERS, but its cells should still
    // say why, so they read as unsupported, not as benchmarks nobody ran.
    for (const a of ADAPTERS) {
        if (!a.mutationUnsupported) continue;
        for (const id of ['add', 'add_batch', 'delete']) {
            results.push(unsupportedRow(`${id}::${a.slug}`, a.mutationUnsupported));
        }
    }

    const config = {
        triplesCount: N_TRIPLES,
        // Term cardinality is an explicit property of the dataset, and
        // selectivity follows from it — record it so a timing can be read
        // against the number of rows it actually touched.
        cardinality: SHAPE,
        matchedRows: matched,
        countWarnings,
        mutBatch: MUT_BATCH,
        queryIterations: QUERY_OPTS.iterations,
        heavyIterations: HEAVY_OPTS.iterations,
        fullScanIterations: FULL_SCAN_OPTS.iterations,
    };

    mkdirSync(dirname(OUT), { recursive: true });
    writeFileSync(OUT, JSON.stringify({ provenance: provenance(oxigraphVersion), results, memory, config, failures }, null, 2));
    console.log(`\nWrote ${results.length} benchmark rows, ${memory.length} memory readings`
        + `${failures.length ? `, ${failures.length} unmeasurable phase(s)` : ''} → ${OUT}`);
    for (const f of failures) console.log(`  missing: ${f.label} / ${f.role} / ${f.phase}`);
}

function depVersion(pkg: string): string {
    // Some packages' `exports` block a subpath import of package.json; fall back to
    // reading the installed manifest directly.
    try { return require(`${pkg}/package.json`).version as string; } catch { /* try fs */ }
    try {
        const path = resolve(here, '..', 'node_modules', pkg, 'package.json');
        return JSON.parse(readFileSync(path, 'utf8')).version as string;
    } catch { return '?'; }
}

function provenance(oxigraphVersion: string): string {
    // UTC, to the minute — the format every dashboard tab dates itself in
    // (`scripts/render_bench_dashboard.py`, `python/bench/run.py`), so one page
    // never shows three formats or three timezones for one run.
    const measured = `${new Date().toISOString().slice(0, 16).replace('T', ' ')} UTC`;
    const cpu = cpus()[0]?.model ?? 'unknown CPU';
    return (
        `Measured ${measured} · node ${process.version} · ${cpu}, ${cpus().length} threads · ` +
        `${N_TRIPLES.toLocaleString()} quads over ${SHAPE.nGraph} named graphs ` +
        `(${SHAPE.terms.toLocaleString()} terms; hdt reads their triples projection), ` +
        `MUT_BATCH=${MUT_BATCH.toLocaleString()} · tinybench ${depVersion('tinybench')}, wall-clock · ` +
        `vortex-rdf-store ${require('../package.json').version}, rdf-stores ${depVersion('rdf-stores')}, oxigraph ${oxigraphVersion} (built from source), hdt 0.7 (wasm, read-only) · ` +
        `one adapter per process, isolated`
    );
}

main().catch((e) => { console.error(e); process.exit(1); });
