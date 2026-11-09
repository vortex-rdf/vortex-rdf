// Runs ONE adapter's full lifecycle in its own process, spawned by compare.bench.ts.
// Process-level isolation, not just in-process dispose+gc: a crash in one adapter
// can't take down the whole run, and each adapter gets a clean, uncontaminated
// peak-memory reading (see shared.ts's `peakRssMb`) instead of one polluted by
// whatever every earlier adapter left resident. The `memory` role is one of an
// adapter's memory runs, each in a process of its own too (see runMemory).
import { Bench, type BenchOptions, type Fn, type FnOptions } from 'tinybench';
import { writeFileSync } from 'node:fs';

import type { Quad } from '@rdfjs/types';

import {
    ADAPTERS, MUT_ADAPTERS, N_TRIPLES, GRAPHS, MUT_BATCH,
    genDataset, flattenTerms, genFresh, genDatasetPrefix, datasetProbes, moduli,
    FULL_SCAN_PATTERN, QUERY_OPTS, COLD_QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS,
    reclaim, collect, timeoutRow, unsupportedRow, peakRssMb, heapUsedBytes, holdJsHeap, settle,
    type Row, type Pat, type StoreAdapter, type WorkerOutput,
} from './shared.js';
import {
    checkCounter, errorText, fmtNs, making, makingLast, memoryRunFigures, onFresh,
    type JsMemoryRun, type StoreReadings,
} from './util.js';

const [, , slug, role, outFile] = process.argv;
if (!slug || !role || !outFile) {
    console.error('usage: compare.worker.ts <slug> <query|querycold|fullscan|mutate|memory> <out-file>');
    process.exit(1);
}

/** Phases that could not be measured, and why. */
const failures: { phase: string; error: string }[] = [];

/**
 * Run one benchmark group, keeping a failure local to it.
 *
 * A store that cannot survive one phase can still be measured on every other,
 * and that partial result is worth far more than nothing: without this, a single
 * trap loses the adapter's entire query worker. The failure is recorded rather
 * than swallowed, so a missing row is visibly attributed instead of looking like
 * a benchmark that was never run.
 */
async function bench(
    phase: string, rows: Row[], opts: BenchOptions, add: (b: Bench) => void,
    regime?: 'cold' | 'warm',
): Promise<void> {
    try {
        const b = new Bench(opts);
        add(b);
        await b.run();
        collect(b, rows, regime);
    } catch (e) {
        const error = errorText(e);
        console.error(`  !! phase '${phase}' failed: ${error}`);
        failures.push({ phase, error });
    }
}

/** Rows each probe matches, recorded beside the timings: selectivity follows
 *  the dataset's term cardinality, so a timing is only interpretable next to
 *  its match count. */
const matched: Record<string, number> = {};

/** The time limit for query cells, the same for every library: a match or
 *  count cell, warm or cold, the full scan included, whose one probing run
 *  takes longer than this is not measured, and reads "timeout", with that
 *  run's time as its tooltip. `0` disables the limit. Builds, opens and
 *  mutations are always measured.
 *
 *  Every query cell runs once before it is measured (see `measure`), so the
 *  limit costs a timed-out cell one run, never its repetitions: a 34 s cell at
 *  10 iterations + 5 warmups would be 8½ minutes for one number, three orders
 *  of magnitude slower than the rest of its column. */
const TIMEOUT_MS = Number(process.env.BENCH_TIMEOUT_MS ?? 30_000);

/** One run's result, and why it is a timeout when it took longer than the limit. */
interface Probe<T> { value: T; late?: string }

/** Run `run` once, timed against [`TIMEOUT_MS`]. */
async function timeOnce<T>(run: () => Promise<T> | T): Promise<Probe<T>> {
    const t0 = performance.now();
    const value = await run();
    const ms = performance.now() - t0;
    return TIMEOUT_MS > 0 && ms > TIMEOUT_MS
        ? { value, late: `not measured: one run took ${fmtNs(ms * 1e6)}, past the ${TIMEOUT_MS / 1000} s limit` }
        : { value };
}

/** `ids` as query cells that timed out in `regime`. */
function timeouts(ids: string[], reason: string, regime: 'warm' | 'cold'): Row[] {
    console.log(`  [${ids.join(', ')}] timeout — ${reason}`);
    return ids.map((id) => ({ ...timeoutRow(id, reason), regime }));
}

/** How a phase reaches its store: `probe` runs one call on it, timed against
 *  the limit, and `task` makes the benchmark task that repeats the call. */
interface Access {
    regime: 'warm' | 'cold';
    probe<T>(run: (h: unknown) => Promise<T> | T): Promise<Probe<T>>;
    task(run: (h: unknown) => unknown): [Fn, FnOptions?];
}

/** The warm regime: every call on the one open store `h`. */
function warmAccess(h: unknown): Access {
    return {
        regime: 'warm',
        probe: <T>(run: (h: unknown) => Promise<T> | T) => timeOnce(() => run(h)),
        task: (run) => [async () => { await run(h); }],
    };
}

/** The cold regime: every call on a store freshly adopted from `snap`, the
 *  adoption untimed and the store disposed after. */
function coldAccess(a: StoreAdapter, snap: unknown): Access {
    const open = () => a.open!(snap);
    const dispose = (h: unknown) => a.dispose?.(h);
    return {
        regime: 'cold',
        probe: async <T>(run: (h: unknown) => Promise<T> | T) => {
            const h = await open();
            try {
                return await timeOnce(() => run(h));
            } finally {
                dispose(h);
            }
        },
        task: (run) => onFresh(open, run, dispose),
    };
}

/**
 * Measure `pats` through `access`: probe every pattern once, then benchmark
 * each cell that came in under the time limit with `opts`, a benchmark of its
 * own, so a cell that fails takes no other with it.
 *
 * The probe is the cross-adapter count check and the time limit at once. The
 * count-only run goes first, since a count past the limit times out both of
 * the pattern's cells (materializing does the same work and more), while a
 * materialized run past it times out that cell alone. A pattern that fails
 * its probe is recorded as a failure and benchmarked in neither unit. The
 * probe runs the same consume the benches run, so it is also where a
 * consume-budget breach surfaces first. Failures are named as the page keys
 * the cell: its id after the slug, `#cold` in the cold regime.
 */
async function measure(a: StoreAdapter, pats: Pat[], access: Access, opts: BenchOptions, rows: Row[]): Promise<void> {
    const { regime } = access;
    const key = (cell: string) => (regime === 'cold' ? `${cell}#cold` : cell);
    const cells: { name: string; run: (h: unknown) => unknown }[] = [];
    for (const p of pats) {
        const id = `${a.slug}::${p.name}`;
        try {
            const count = await access.probe((h) => a.countOnly(h, p));
            matched[p.name] = count.value;
            if (count.late) {
                rows.push(...timeouts([id, `${id}::count`], count.late, regime));
                continue;
            }
            const match = await access.probe((h) => a.countMatch(h, p));
            matched[p.name] = match.value;
            // A count path that resolves differently must fail loudly here,
            // not time the wrong work in the benches below.
            if (count.value !== match.value) {
                throw new Error(`countOnly disagrees: ${count.value} vs ${match.value}`);
            }
            if (match.late) rows.push(...timeouts([id], match.late, regime));
            else cells.push({ name: p.name, run: (h) => a.countMatch(h, p) });
            cells.push({ name: `${p.name}::count`, run: (h) => a.countOnly(h, p) });
        } catch (e) {
            const error = errorText(e);
            console.error(`  !! probe '${p.name}' failed: ${error}`);
            failures.push({ phase: key(p.name), error });
        }
    }
    for (const { name, run } of cells) {
        await bench(key(name), rows, opts, (b) => { b.add(`${a.slug}::${name}`, ...access.task(run)); }, regime);
    }
}

/** The dataset options a store gets: the graph-bearing dataset where its model
 *  has graphs, and the same rows with the graph dropped where it does not. One
 *  dataset either way, so every store counts the same rows for every pattern
 *  that binds no graph. */
function datasetOptsFor(a: StoreAdapter) {
    return a.quadsUnsupported ? undefined : { graphs: GRAPHS };
}

/** Every pattern a store answers: the triple probes always, the graph probes
 *  where its model has graphs. */
function probesFor(a: StoreAdapter): Pat[] {
    const probes = datasetProbes(N_TRIPLES, datasetOptsFor(a));
    return a.quadsUnsupported ? probes.triples : [...probes.triples, ...probes.quads];
}

/** The graph probes' rows for a store whose model has no graphs, `reason`
 *  saying so per probe, so the cells read as unsupported, not as benchmarks
 *  nobody ran. Such a store answers the triple patterns on the projection of
 *  the same rows. */
function graphProbesUnsupported(slug: string, reason: string, regime: 'warm' | 'cold'): Row[] {
    return datasetProbes(N_TRIPLES, { graphs: GRAPHS }).quads.flatMap((p) =>
        [`${slug}::${p.name}`, `${slug}::${p.name}::count`].map((id) => ({ ...unsupportedRow(id, reason), regime })));
}

/** The dataset for `a`, its strings flattened once so that no timed build pays
 *  V8's rope flattening. */
function inputFor(a: StoreAdapter): Quad[] {
    const triples = genDataset(N_TRIPLES, datasetOptsFor(a));
    flattenTerms(triples);
    return triples;
}

async function runQuery(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const pats = probesFor(a);

    console.log(`[${a.label}] query…`);
    const triples = inputFor(a);
    let th: unknown;
    if (a.ingestUnsupported) {
        rows.push(unsupportedRow(`ingest::${a.slug}`, a.ingestUnsupported));
        th = await a.build(triples); // hdt opens its file, untimed
    } else {
        // Three timed builds; the last one is the store the probes query.
        console.log(`[${a.label}] ingest…`);
        const builds = makingLast(() => a.build(triples), (h) => reclaim(a, h));
        await bench('ingest', rows, HEAVY_OPTS, (b) => {
            b.add(`ingest::${a.slug}`, ...builds.task);
        });
        th = builds.last() ?? await a.build(triples);
    }

    await measure(a, pats, warmAccess(th), QUERY_OPTS, rows);
    reclaim(a, th);
    th = null;

    if (a.quadsUnsupported) rows.push(...graphProbesUnsupported(a.slug, a.quadsUnsupported, 'warm'));

    return rows;
}

/**
 * The cold query regime, plus the open it deliberately excludes — in a process
 * of its own.
 *
 * Each iteration answers the FIRST query on a freshly adopted store. The adopt
 * happens in tinybench's `beforeEach`, which is untimed, so the column isolates
 * what a query costs against empty caches, excluding the cost of building a
 * store. Opening is measured separately as `open::<slug>`,
 * mirroring the Python tab, so the two are attributable on their own.
 *
 * The process isolation is not stylistic. The first query on an adopted store
 * retains its whole buffer past `free()` (adoption alone is flat), and wasm
 * linear memory never returns to the OS. Sharing a process with the warm phase
 * piles a live multi-million-quad store plus every adoption into one address
 * space until the wasm allocator traps. Here the built store is freed the moment
 * its snapshot exists, so the whole budget belongs to the measurements.
 */
async function runQueryCold(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    if (!a.snapshot || !a.open) {
        // No persistent form: nothing to reopen, so no cold rows — and no open
        // either. Say so in the cell; a bare dash would read as a measurement
        // nobody took. Getting this store queryable in a
        // fresh process is a rebuild from quads, which the Ingest column reports.
        rows.push(unsupportedRow(
            `open::${a.slug}`,
            'no artifact to reopen: a fresh process rebuilds from quads, which is the Ingest column',
        ));
        return rows;
    }

    const dispose = (h: unknown) => a.dispose?.(h);

    console.log(`[${a.label}] query cold…`);
    const triples = inputFor(a);
    const pats = probesFor(a);
    let th: unknown = await a.build(triples);
    const tSnap = await a.snapshot(th);
    reclaim(a, th);
    th = null;

    // Open, on its own: adopting the snapshot into a queryable store, with no
    // query behind it. It matches the Python tab's `open::<slug>`, and without
    // it the cold column's context — how much of a cold start is the open — is
    // not on the page.
    await bench('open', rows, HEAVY_OPTS, (b) => {
        b.add(`open::${a.slug}`, ...making(() => a.open!(tSnap), dispose));
    });

    // Adopt per iteration (untimed), query (timed).
    await measure(a, pats, coldAccess(a, tSnap), COLD_QUERY_OPTS, rows);

    if (a.quadsUnsupported) rows.push(...graphProbesUnsupported(a.slug, a.quadsUnsupported, 'cold'));

    return rows;
}

/**
 * The full scan, in its own process — the same isolation argument this file
 * already makes for adapters, one level down.
 *
 * Materializing every row of a multi-million-quad dataset with realistic term
 * cardinality is by far the heaviest phase, and a store's own retention makes it
 * order-dependent: a store can hold hundreds of megabytes per full scan without
 * reclaiming them within its lifetime, and wasm linear memory never shrinks back
 * to the engine. Sharing a process with the query and ingest phases can therefore
 * leave a module too grown to scan at all — trapping on a freshly built store,
 * and reporting "this store cannot full-scan the dataset" when a clean process
 * does it comfortably. Measuring it here reports the scan, not the residue of
 * everything that ran before it.
 */
async function runFullScan(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    console.log(`[${a.label}] full scan…`);
    const triples = inputFor(a);
    let h: unknown = await a.build(triples);
    // Its probes record what the scan matches, as the pattern probes do — the
    // strongest cross-adapter check there is: every store must return the
    // whole dataset.
    await measure(a, [FULL_SCAN_PATTERN], warmAccess(h), FULL_SCAN_OPTS, rows);
    if (a.snapshot && a.open) {
        await measure(a, [FULL_SCAN_PATTERN], coldAccess(a, await a.snapshot(h)), FULL_SCAN_OPTS, rows);
    }
    reclaim(a, h);
    h = null;
    return rows;
}

async function runMutate(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const fresh = genFresh(MUT_BATCH);
    const delSlice = genDatasetPrefix(N_TRIPLES, MUT_BATCH);

    console.log(`\n[${a.label}] add (${MUT_BATCH})…`);
    const dispose = (h: unknown) => reclaim(a, h);
    await bench('add', rows, HEAVY_OPTS, (b) => {
        b.add(`add::${a.slug}`, ...making(() => a.newEmpty(), dispose, (h) => a.addAll(h, fresh)));
        if (a.addBatch) {
            b.add(`add_batch::${a.slug}`, ...making(() => a.newEmpty(), dispose, (h) => a.addBatch!(h, fresh)));
        }
    });

    console.log(`[${a.label}] delete (${MUT_BATCH})…`);
    await bench('delete', rows, HEAVY_OPTS, (b) => {
        b.add(`delete::${a.slug}`, ...onFresh(() => a.build(delSlice), (h) => a.deleteAll(h, delSlice), dispose));
    });

    return rows;
}

/** A collected read's result, kept reachable at module scope until the run
 *  has read the live bytes it holds. */
let held: unknown = null;

/**
 * One memory run: the store alone, in a process of its own, run
 * BENCH_MEMORY_RUNS times per adapter by compare.bench.ts.
 *
 * Live bytes are the store module's wasm counter plus the JS heap's
 * `heapUsed` after a collection; rdf-stores has no wasm module, so the JS heap
 * alone. Both counters must first see an 8 MiB block held, or the run throws
 * and the worker exits 1. The store is obtained as `a.obtained` says (built
 * from the flattened dataset, which is passed inline so nothing keeps it
 * alive, or hdt's file loaded), measured and freed before any read; a second
 * store obtained the same way is then read once per probe and full scan, the
 * counts recorded, and freed. Each reading is taken after `settle(live)`,
 * whose rounds of collection and an event-loop turn repeat until one frees
 * less than 1 MiB, so the finalizers a free or a read left behind have run. A
 * built store with a persistent form (Vortex) is also measured as `fromBytes`
 * loads it, from the bytes `toBytes()` gave before the second store was
 * freed, then loaded again and given the same reads, whose counts must be the
 * built store's. A failed free, or a loaded store that counts other rows,
 * throws as a failed self-check does.
 *
 * - Store: live bytes right after the store is obtained minus those after its
 *   free, on a store no read has touched, so it leaves out the caches reads
 *   fill (Vortex's term cache, and a built Vortex store's decoded code
 *   columns).
 * - Peak: per probe, the counter's peak reset, the read, a collection, then
 *   the counter's peak plus `heapUsed`; the highest above the baseline. A
 *   collected result (oxigraph) is held until that reading; a streaming
 *   read's current chunk cannot be seen.
 */
async function runMemory(a: StoreAdapter): Promise<JsMemoryRun> {
    console.log(`[${a.label}] memory…`);
    const counter = (await a.counter?.()) ?? null;
    counter?.setCounting(true);
    if (counter) checkCounter(`${counter.name} wasm`, () => counter.stats().liveBytes, counter.hold);
    checkCounter('JS heap (heapUsed)', heapUsedBytes, holdJsHeap);
    const live = () => heapUsedBytes() + (counter?.stats().liveBytes ?? 0);
    await settle(live);
    const baseline = live();
    // Store: a store of its own, obtained and freed before any read, so it
    // holds no cache a read fills. Not through `reclaim`, which swallows a
    // failed free(): that would leave Store at about 0, so it fails the run.
    let h: unknown = await a.build(inputFor(a)); // built, or hdt's file loaded
    await settle(live);
    const opened = live();
    a.dispose?.(h);
    h = null;
    await settle(live);
    const first: StoreReadings = { opened, disposed: live() };
    // The reads, on a second store obtained the same way.
    h = await a.build(inputFor(a));
    await settle(live);
    const probeTotals: number[] = [];
    for (const p of [...probesFor(a), FULL_SCAN_PATTERN]) {
        await settle(live);
        counter?.resetPeak();
        const read = a.matchHeld ? a.matchHeld(h, p) : { rows: await a.countMatch(h, p), result: null };
        held = read.result;
        matched[p.name] = read.rows;
        probeTotals.push(heapUsedBytes() + (counter?.stats().peakLiveBytes ?? 0));
        held = null;
    }
    await settle(live);
    const loads = a.obtained === 'built' && !!a.snapshot && !!a.open; // Vortex
    let bytes: unknown = loads ? await a.snapshot!(h) : null; // before the free
    a.dispose?.(h);
    h = null;
    await settle(live);
    let loaded: StoreReadings | null = a.obtained === 'loaded' ? first : null;
    if (loads) {
        // The loaded store as `fromBytes` gives it, measured before any read.
        let l: unknown = await a.open!(bytes);
        await settle(live);
        const lOpened = live();
        a.dispose?.(l);
        l = null;
        await settle(live);
        loaded = { opened: lOpened, disposed: live() };
        // Then the same reads, which must count the built store's rows.
        l = await a.open!(bytes);
        bytes = null;
        for (const p of [...probesFor(a), FULL_SCAN_PATTERN]) {
            const rows = await a.countMatch(l, p);
            if (rows !== matched[p.name]) {
                throw new Error(
                    `the store loaded from the snapshot matched ${rows} rows for '${p.name}', `
                    + `the built store ${matched[p.name]}`);
            }
        }
        a.dispose?.(l);
    }
    return memoryRunFigures({ baseline, built: a.obtained === 'built' ? first : null, loaded, probeTotals });
}

async function main(): Promise<void> {
    const list = role === 'mutate' ? MUT_ADAPTERS : ADAPTERS;
    const a = list.find((x) => x.slug === slug);
    if (!a) {
        console.error(`unknown adapter slug '${slug}' for role '${role}'`);
        process.exit(1);
    }
    // Untimed, before any phase: no timed build pays the module's compile.
    await a.init?.();

    // A memory run that fails, its self-check included, throws out of here:
    // the worker exits 1 with the reason, and writes nothing.
    const memoryRun = role === 'memory' ? await runMemory(a) : undefined;
    const rows = role === 'memory' ? []
        : role === 'query' ? await runQuery(a)
            : role === 'querycold' ? await runQueryCold(a)
                : role === 'fullscan' ? await runFullScan(a)
                    : await runMutate(a);
    const out: WorkerOutput = {
        rows,
        peakRssMb: peakRssMb(),
        matched,
        failures,
        cardinality: role === 'query' ? moduli(N_TRIPLES, { graphs: GRAPHS }) : undefined,
        memoryRun,
    };
    writeFileSync(outFile, JSON.stringify(out));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
