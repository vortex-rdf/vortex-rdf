// Comparative benchmark: VortexRdfStore (JS/WASM) config variants vs. rdf-stores.js
// and oxigraph. Emits dashboard-shaped JSON consumed by
// scripts/render_bench_dashboard.py (the JavaScript tab of the GitHub Pages dashboard).
//
// Run (after `npm run build` to produce pkg/web):
//   npm run bench                         # full rdf-stores-scale run (D=128 — heavy)
//   BENCH_DIM=25 BENCH_DIM_QUADS=10 MUT_BATCH=2000 npm run bench   # quick local check
//
// The dataset is synthetic but realistic in the dimension that matters for a store
// comparison: distinct terms scale with rows (near-unique subject IRIs, a small closed
// predicate vocabulary, a mix of IRI and literal objects), so each library's term
// handling — dictionaries, interning, string storage — is actually exercised. It is
// driven across the six routing shapes the Rust dashboard also uses (S/P/O/PO/G/SPOG)
// plus fully-bound and full-scan. Note this diverges from rdf-stores.js's own harness,
// whose dataset draws D^3 rows from only D distinct IRIs; results before the cardinality
// switch are not comparable with results after it.
//
// This file is the ORCHESTRATOR only — it spawns one child process per adapter
// (compare.worker.ts) and collects the results. Adapters used to share this process,
// but that let earlier adapters' peak WASM/JS memory contaminate later ones (observed:
// oxigraph's own wasm module trapping with `unreachable` only after several other
// multi-million-quad stores had already grown and been freed in the same process — it
// runs clean in isolation with the exact same dataset). Process-per-adapter also means
// a crash in one adapter loses only that adapter's rows, not the whole run, and gives
// each adapter a trustworthy, uncontaminated peak-RSS reading (see shared.ts's
// `peakRssMb`).
//
// Uses tinybench + @codspeed/tinybench-plugin — the same harness rdf-stores.js uses.
// `withCodSpeed` is a no-op when not run under the CodSpeed action, so this produces
// plain wall-clock results and is NEVER uploaded to CodSpeed (no JS CodSpeed workflow
// exists; this is never run via CodSpeedHQ/action). CodSpeed stays Rust-only.

import { createRequire } from 'node:module';
import { cpus } from 'node:os';
import { writeFileSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import {
    ADAPTERS, MUT_ADAPTERS, D, DQ, N_TRIPLES, N_QUADS, MUT_BATCH,
    QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS, moduli, type Row,
} from './shared.js';
import { runWorkerProcess } from './util.js';

const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));
const workerPath = resolve(here, 'compare.worker.ts');

const OUT = resolve(here, process.env.BENCH_OUT ?? 'results.json');

interface MemoryRow {
    slug: string; label: string; role: string;
    /** Whole-process high-water mark: the only figure comparable across a wasm
     *  store and a pure-JS one. This is the headline memory number. */
    peakRssMb: number | null;
    /** What building the store cost over and above the shared input `Quad[]`.
     *  `wasmHeapMb` is Vortex/oxigraph-only and is null for pure-JS adapters. */
    storeFootprint?: Record<string, number | null>;
}
interface WorkerOutput {
    rows: Row[];
    peakRssMb: number | null;
    storeFootprint?: Record<string, number | null>;
    matched?: Record<string, number>;
    cardinality?: Record<string, unknown>;
    failures?: { phase: string; error: string }[];
}

/** One adapter, one role, one process. A worker that cannot finish is skipped:
 *  the remaining adapters still run and still produce their rows. */
function runWorker(slug: string, role: 'query' | 'fullscan' | 'mutate'): WorkerOutput | null {
    return runWorkerProcess<WorkerOutput>(workerPath, [slug, role], `${slug}/${role}`);
}

async function main(): Promise<void> {
    const tm = moduli(N_TRIPLES);
    const qm = moduli(N_QUADS, { graphs: 8 });
    console.log(
        `Dataset shape: triples D=${D} (${N_TRIPLES.toLocaleString()} quads, ` +
        `${tm.terms.toLocaleString()} distinct terms), ` +
        `quads Dq=${DQ} (${N_QUADS.toLocaleString()} quads, ${qm.terms.toLocaleString()} distinct terms)…`);

    const results: Row[] = [];
    const memory: MemoryRow[] = [];
    const matched: Record<string, number> = {};
    // Phases an adapter could not complete. Recorded so a missing row on the
    // dashboard is visibly attributed to a store that could not do the work,
    // rather than looking like a benchmark nobody ran.
    const failures: { slug: string; label: string; role: string; phase: string; error: string }[] = [];

    for (const a of ADAPTERS) {
        const out = runWorker(a.slug, 'query');
        if (!out) continue;
        results.push(...out.rows);
        memory.push({
            slug: a.slug, label: a.label, role: 'query',
            peakRssMb: out.peakRssMb, storeFootprint: out.storeFootprint,
        });
        for (const f of out.failures ?? []) {
            failures.push({ slug: a.slug, label: a.label, role: 'query', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
        // Every adapter queries the same dataset with the same probes, so their
        // match counts must agree. A disagreement is a correctness bug in one of
        // them, not a benchmarking detail — surface it rather than silently
        // keeping whichever adapter reported last.
        if (out.matched) {
            for (const [pat, n] of Object.entries(out.matched)) {
                if (matched[pat] === undefined) matched[pat] = n;
                else if (matched[pat] !== n) {
                    console.error(
                        `  !! ${a.label} matched ${n} rows for '${pat}', ` +
                        `but an earlier adapter matched ${matched[pat]}`,
                    );
                }
            }
        }
        console.log(
            `[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB` +
            `, store: ${out.storeFootprint?.rssMb ?? '?'} MB RSS` +
            ` / ${out.storeFootprint?.wasmHeapMb ?? '-'} MB wasm`,
        );
    }

    // The full scan gets its own process per adapter, for the same reason each
    // adapter gets one: it is heavy enough that a store's retained memory makes
    // it order-dependent, so sharing a process with the query and ingest phases
    // measures the residue of those rather than the scan. See runFullScan.
    for (const a of ADAPTERS) {
        const out = runWorker(a.slug, 'fullscan');
        if (!out) continue;
        results.push(...out.rows);
        for (const f of out.failures ?? []) {
            failures.push({ slug: a.slug, label: a.label, role: 'fullscan', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
    }

    for (const a of MUT_ADAPTERS) {
        const out = runWorker(a.slug, 'mutate');
        if (!out) continue;
        results.push(...out.rows);
        memory.push({ slug: a.slug, label: a.label, role: 'mutate', peakRssMb: out.peakRssMb });
        for (const f of out.failures ?? []) {
            failures.push({ slug: a.slug, label: a.label, role: 'mutate', ...f });
            console.error(`  !! ${a.label} could not complete '${f.phase}': ${f.error}`);
        }
        console.log(`[${a.label}] peak RSS: ${out.peakRssMb ?? 'unknown'} MB`);
    }

    const config = {
        triplesCount: N_TRIPLES,
        quadsCount: N_QUADS,
        // Term cardinality is now an explicit property of the dataset, and
        // selectivity follows from it — record both so a timing can be read
        // against the number of rows it actually touched.
        cardinality: { triples: moduli(N_TRIPLES), quads: moduli(N_QUADS, { graphs: 8 }) },
        matchedRows: matched,
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
    const date = new Date().toISOString().slice(0, 10);
    const cpu = cpus()[0]?.model ?? 'unknown CPU';
    return (
        `Measured ${date} · node ${process.version} · ${cpu}, ${cpus().length} threads · ` +
        `triples D=${D} (${N_TRIPLES.toLocaleString()} quads, ${moduli(N_TRIPLES).terms.toLocaleString()} terms), ` +
        `quads Dq=${DQ} (${N_QUADS.toLocaleString()} quads, ${moduli(N_QUADS, { graphs: 8 }).terms.toLocaleString()} terms), ` +
        `MUT_BATCH=${MUT_BATCH.toLocaleString()} · tinybench ${depVersion('tinybench')}, wall-clock · ` +
        `vortex-rdf-store ${require('../package.json').version}, rdf-stores ${depVersion('rdf-stores')}, oxigraph ${depVersion('oxigraph')} · ` +
        `one adapter per process, isolated`
    );
}

main().catch((e) => { console.error(e); process.exit(1); });
