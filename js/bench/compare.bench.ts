// Comparative benchmark: VortexRdfStore (JS/WASM) config variants vs. rdf-stores.js,
// oxigraph and hdt (read-only wasm over a natively pre-built artifact — see the
// hdt section in shared.ts). Emits dashboard-shaped JSON consumed by
// scripts/render_bench_dashboard.py (the JavaScript tab of the GitHub Pages dashboard).
//
// Run (after `npm run build` to produce pkg/web):
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
import { writeFileSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import {
    ADAPTERS, MUT_ADAPTERS, N_TRIPLES, GRAPHS, MUT_BATCH,
    QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS, moduli, unsupportedRow,
    type Row, type WorkerOutput,
} from './shared.js';
import { runWorkerProcess } from './util.js';

/** The run's dataset shape: term cardinality per role, from which every
 *  probe's selectivity follows. */
const SHAPE = moduli(N_TRIPLES, { graphs: GRAPHS });

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const workerPath = resolve(here, 'compare.worker.ts');

const OUT = resolve(here, process.env.BENCH_OUT ?? 'results.json');

interface MemoryRow {
    slug: string; label: string; role: string;
    /** Whole-process high-water mark: the only figure comparable across a wasm
     *  store and a pure-JS one. This is the headline memory number. */
    peakRssMb: number | null;
    /** See `WorkerOutput.storeFootprint` in shared.ts. */
    storeFootprint?: WorkerOutput['storeFootprint'];
}

/** One adapter, one role, one process. A worker that cannot finish is skipped:
 *  the remaining adapters still run and still produce their rows. The caller
 *  records a failure for a null return, so the missing rows stay attributed on
 *  the page as failures, not as benchmarks nobody ran. */
/** Views over wasm memory need the module's buffer to grow in place, which
 *  Node keeps behind this flag (24.5+). Without it every Arrow parse copies
 *  instead — the same API, a slower cell — so the Arrow role asks for it, and
 *  the worker prints which it got. It is passed to that role alone: it changes
 *  how the module's memory behaves, and no other cell on the page should move
 *  because the Arrow role exists. */
const ARROW_FLAGS = ['--experimental-wasm-rab-integration'];

function runWorker(
    slug: string, role: 'query' | 'querycold' | 'fullscan' | 'arrow' | 'mutate',
): WorkerOutput | null {
    const flags = role === 'arrow' ? ARROW_FLAGS : [];
    return runWorkerProcess<WorkerOutput>(workerPath, [slug, role], `${slug}/${role}`, flags);
}

async function main(): Promise<void> {
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
    // A worker that never produced output — crash or the WORKER_TIMEOUT_MS
    // kill — still gets a failures entry, so its absent rows are attributed
    // in the results file, exactly like an in-worker phase failure.
    const workerLost = (a: { slug: string; label: string }, role: string): void => {
        failures.push({
            slug: a.slug, label: a.label, role, phase: 'worker',
            error: 'worker process did not finish (crash or WORKER_TIMEOUT_MS backstop) — see the run log',
        });
    };
    const mergeMatched = (label: string, out: WorkerOutput): void => {
        for (const [pat, n] of Object.entries(out.matched)) {
            if (matched[pat] === undefined) matched[pat] = n;
            else if (matched[pat] !== n) {
                // Into the results file, not just the console: the dashboard
                // renders these, so a disagreement is visible where the numbers
                // are read, not only in the console output.
                const msg = `${label} matched ${n} rows for '${pat}', an earlier adapter matched ${matched[pat]}`;
                countWarnings.push(msg);
                console.error(`  !! ${msg}`);
            }
        }
    };

    for (const a of ADAPTERS) {
        const out = runWorker(a.slug, 'query');
        if (!out) { workerLost(a, 'query'); continue; }
        results.push(...out.rows);
        memory.push({
            slug: a.slug, label: a.label, role: 'query',
            peakRssMb: out.peakRssMb, storeFootprint: out.storeFootprint,
        });
        for (const f of out.failures) {
            failures.push({ slug: a.slug, label: a.label, role: 'query', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
        mergeMatched(a.label, out);
        console.log(
            `[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB` +
            `, store: ${out.storeFootprint?.rssMb ?? '?'} MB RSS` +
            ` / ${out.storeFootprint?.wasmHeapMb ?? '-'} MB wasm`,
        );
    }

    // The cold regime gets its own process per adapter: every iteration adopts a
    // fresh store, and the first query on one retains its whole buffer past
    // free() in wasm memory that never shrinks. Sharing the query process piles
    // those adoptions on top of a live store until the wasm allocator traps.
    // Adapters with no persistent form return no rows here (see runQueryCold).
    for (const a of ADAPTERS) {
        const out = runWorker(a.slug, 'querycold');
        if (!out) { workerLost(a, 'querycold'); continue; }
        results.push(...out.rows);
        for (const f of out.failures) {
            failures.push({ slug: a.slug, label: a.label, role: 'querycold', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
    }

    // The full scan gets its own process per adapter, for the same reason each
    // adapter gets one: it is heavy enough that a store's retained memory makes
    // it order-dependent, so sharing a process with the query and ingest phases
    // measures the residue of those on top of the scan. See runFullScan.
    for (const a of ADAPTERS) {
        const out = runWorker(a.slug, 'fullscan');
        if (!out) { workerLost(a, 'fullscan'); continue; }
        results.push(...out.rows);
        mergeMatched(a.label, out);
        for (const f of out.failures) {
            failures.push({ slug: a.slug, label: a.label, role: 'fullscan', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
    }

    // The Arrow read of the same probes, for the stores that export any. Its
    // own process per adapter: the role loads apache-arrow and runs under the
    // zero-copy flag, neither of which should touch the measurements above.
    for (const a of ADAPTERS.filter((x) => x.arrowMatch)) {
        const out = runWorker(a.slug, 'arrow');
        if (!out) { workerLost(a, 'arrow'); continue; }
        results.push(...out.rows);
        for (const f of out.failures) {
            failures.push({ slug: a.slug, label: a.label, role: 'arrow', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
    }

    for (const a of MUT_ADAPTERS) {
        const out = runWorker(a.slug, 'mutate');
        if (!out) { workerLost(a, 'mutate'); continue; }
        results.push(...out.rows);
        memory.push({ slug: a.slug, label: a.label, role: 'mutate', peakRssMb: out.peakRssMb });
        for (const f of out.failures) {
            failures.push({ slug: a.slug, label: a.label, role: 'mutate', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
        console.log(`[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB`);
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

    writeFileSync(OUT, JSON.stringify({ provenance: provenance(), results, memory, config, failures }, null, 2));
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

function provenance(): string {
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
        `vortex-rdf-store ${require('../package.json').version}, rdf-stores ${depVersion('rdf-stores')}, oxigraph ${depVersion('oxigraph')}, hdt 0.7 (wasm, read-only) · ` +
        `one adapter per process, isolated`
    );
}

main().catch((e) => { console.error(e); process.exit(1); });
