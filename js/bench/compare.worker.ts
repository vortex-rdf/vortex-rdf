// Runs ONE adapter's full lifecycle in its own process, spawned by compare.bench.ts.
// Process-level isolation, not just in-process dispose+gc: a crash in one adapter
// can't take down the whole run, and each adapter gets a clean, uncontaminated
// peak-memory reading (see shared.ts's `peakRssMb`) instead of one polluted by
// whatever every earlier adapter left resident.
import { Bench, type BenchOptions } from 'tinybench';
import { writeFileSync } from 'node:fs';

import {
    ADAPTERS, MUT_ADAPTERS, N_TRIPLES, GRAPHS, MUT_BATCH,
    genDataset, genFresh, genDatasetPrefix, datasetProbes, moduli,
    FULL_SCAN_PATTERN, QUERY_OPTS, COLD_QUERY_OPTS, HEAVY_OPTS, FULL_SCAN_OPTS,
    reclaim, collect, unsupportedRow, peakRssMb, rssMb, jsHeapMb, wasmHeapMb,
    installArrow,
    type Row, type Pat, type StoreAdapter, type WorkerOutput,
} from './shared.js';

const [, , slug, role, outFile] = process.argv;
if (!slug || !role || !outFile) {
    console.error('usage: compare.worker.ts <slug> <query|querycold|fullscan|arrow|mutate> <out-file>');
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
        const error = e instanceof Error ? e.message : String(e);
        console.error(`  !! phase '${phase}' failed: ${error}`);
        failures.push({ phase, error });
    }
}

/** Rows each probe matches, recorded beside the timings: selectivity follows
 *  the dataset's term cardinality, so a timing is only interpretable next to
 *  its match count. */
const matched: Record<string, number> = {};

/** A phase whose single execution already exceeds this runs ONCE, not the
 *  full repetition count; `0` disables the rule.
 *
 *  Repetitions exist to average out noise that is small relative to the
 *  measurement — on a cell that takes tens of seconds, the noise the extra
 *  runs would remove is three orders of magnitude below the reading, while
 *  the runs themselves cost minutes (a 34 s cell at 10 iterations + 5 warmups
 *  is 8½ minutes for one number). The reduced count is reported honestly:
 *  the row's `samples` field says `1`, exactly as the page displays it. */
const SLOW_PHASE_MS = Number(process.env.BENCH_SLOW_PHASE_MS ?? 30_000);

/** The repetition plan for a slow phase: one measured run, no warmup. Warmup
 *  is not lost — every pattern measured this way already executed once in the
 *  count pre-pass, so its caches are as warm as one run makes them. */
const ONE_SHOT: BenchOptions = {
    time: 0, iterations: 1, warmup: false, warmupIterations: 0, throws: true,
};

/** Split probes by what the count pre-pass measured: patterns under the cutoff
 *  keep the full repetition plan, patterns over it run once. A pattern with no
 *  pre-pass timing failed its count — benching it again would fail again, and
 *  under `throws: true` would take the whole group's surviving tasks with it. */
function splitBySpeed(pats: Pat[], costMs: Record<string, number>): { fast: Pat[]; slow: Pat[] } {
    const fast: Pat[] = [];
    const slow: Pat[] = [];
    for (const p of pats) {
        const c = costMs[p.name];
        if (c === undefined) continue;
        if (SLOW_PHASE_MS > 0 && c > SLOW_PHASE_MS) {
            console.log(`  [${p.name}] one run took ${(c / 1000).toFixed(0)}s — measuring 1 sample (BENCH_SLOW_PHASE_MS)`);
            slow.push(p);
        } else {
            fast.push(p);
        }
    }
    return { fast, slow };
}

/** What building the store cost, over and above the input `Quad[]`. */
const storeFootprint: Record<string, number | null> = {};

/** `wasm` is Vortex's linear memory specifically — oxigraph has its own module
 *  this cannot see, and rdf-stores has none — so it is only read for Vortex
 *  adapters and reported as null elsewhere (a 0 would misreport it). */
async function memSnapshot(isVortex: boolean): Promise<{ rss: number | null; js: number; wasm: number | null }> {
    // jsHeapMb forces a collection first, so the RSS read after it is post-GC too.
    const js = jsHeapMb();
    return { rss: rssMb(), js, wasm: isVortex ? await wasmHeapMb() : null };
}

const delta = (a: number | null, b: number | null) => (a === null || b === null ? null : a - b);

/** Count every probe once (the cross-adapter agreement check), timing each —
 *  the timings drive [`splitBySpeed`]. A probe that cannot be counted is
 *  recorded as a failure and excluded from the benches; the worker carries
 *  on. This pre-pass runs the same consume the benches run, so it is also
 *  where a consume-budget breach surfaces first. */
async function countAll(a: StoreAdapter, h: unknown, pats: Pat[]): Promise<Record<string, number>> {
    const costMs: Record<string, number> = {};
    for (const p of pats) {
        const t0 = performance.now();
        try {
            matched[p.name] = await a.countMatch(h, p);
            costMs[p.name] = performance.now() - t0;
            // A count path that resolves differently must fail loudly here,
            // not time the wrong work in the benches below.
            const c = await a.countOnly(h, p);
            if (c !== matched[p.name]) {
                throw new Error(`countOnly disagrees: ${c} vs ${matched[p.name]}`);
            }
        } catch (e) {
            const error = e instanceof Error ? e.message : String(e);
            console.error(`  !! count '${p.name}' failed: ${error}`);
            failures.push({ phase: `count:${p.name}`, error });
        }
    }
    return costMs;
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

async function runQuery(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const pats = probesFor(a);
    const qProbes = datasetProbes(N_TRIPLES, { graphs: GRAPHS });

    console.log(`[${a.label}] query…`);
    const triples = genDataset(N_TRIPLES, datasetOptsFor(a));
    // Measure across the build only. The input array is ~a gigabyte of JS objects
    // and is identical for every adapter, so a delta isolates the store's own
    // footprint far better than any absolute reading — and unlike dropping the
    // array it does not force us to regenerate it for the ingest phase.
    const isVortex = a.slug.startsWith('vortex');
    const before = await memSnapshot(isVortex);
    let th: unknown = await a.build(triples);
    const after = await memSnapshot(isVortex);
    Object.assign(storeFootprint, {
        rssMb: delta(after.rss, before.rss),
        jsHeapMb: after.js - before.js,
        wasmHeapMb: delta(after.wasm, before.wasm),
    });

    const tCost = await countAll(a, th, pats);
    const tSplit = splitBySpeed(pats, tCost);
    if (tSplit.fast.length) {
        await bench('query_triples', rows, QUERY_OPTS, (b) => {
            for (const p of tSplit.fast) {
                b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(th, p); });
                b.add(`${a.slug}::${p.name}::count`, async () => { await a.countOnly(th, p); });
            }
        }, 'warm');
    }
    if (tSplit.slow.length) {
        await bench('query_triples_slow', rows, ONE_SHOT, (b) => {
            for (const p of tSplit.slow) {
                b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(th, p); });
                b.add(`${a.slug}::${p.name}::count`, async () => { await a.countOnly(th, p); });
            }
        }, 'warm');
    }
    reclaim(a, th);
    th = null;

    if (a.quadsUnsupported) {
        // No graph in the model at all — say so per probe, so the cells
        // read as unsupported, not as benchmarks nobody ran. This store answered the
        // triple patterns above, on the projection of the same rows.
        for (const p of qProbes.quads) {
            for (const id of [`${a.slug}::${p.name}`, `${a.slug}::${p.name}::count`]) {
                const r = unsupportedRow(id, a.quadsUnsupported);
                r.regime = 'warm';
                rows.push(r);
            }
        }
    }

    if (a.ingestUnsupported) {
        rows.push(unsupportedRow(`ingest::${a.slug}`, a.ingestUnsupported));
    } else {
        console.log(`[${a.label}] ingest…`);
        await bench('ingest', rows, HEAVY_OPTS, (b) => {
            // Dispose in `afterEach` (untimed), so freeing the previous store
            // never pollutes the measured ingest cost.
            let h: unknown;
            b.add(`ingest::${a.slug}`, async () => { h = await a.build(triples); }, {
                afterEach: () => { reclaim(a, h); h = undefined; },
            });
        });
    }

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

    // One dataset's cold pass: adopt per iteration (untimed), query (timed).
    const coldPass = async (
        phase: string, snap: unknown, probes: Pat[],
    ): Promise<void> => {
        await bench(phase, rows, COLD_QUERY_OPTS, (b) => {
            for (const p of probes) {
                let h: unknown;
                b.add(`${a.slug}::${p.name}`, async () => { await a.countMatch(h, p); }, {
                    beforeEach: async () => { h = await a.open!(snap); },
                    afterEach: () => { a.dispose?.(h); h = undefined; },
                });
                let hc: unknown;
                b.add(`${a.slug}::${p.name}::count`, async () => { await a.countOnly(hc, p); }, {
                    beforeEach: async () => { hc = await a.open!(snap); },
                    afterEach: () => { a.dispose?.(hc); hc = undefined; },
                });
            }
        }, 'cold');
    };

    console.log(`[${a.label}] query cold…`);
    const triples = genDataset(N_TRIPLES, datasetOptsFor(a));
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
        let h: unknown;
        b.add(`open::${a.slug}`, async () => { h = await a.open!(tSnap); }, {
            afterEach: () => { a.dispose?.(h); h = undefined; },
        });
    });

    await coldPass('query_cold', tSnap, pats);

    if (a.quadsUnsupported) {
        for (const p of datasetProbes(N_TRIPLES, { graphs: GRAPHS }).quads) {
            for (const id of [`${a.slug}::${p.name}`, `${a.slug}::${p.name}::count`]) {
                const r = unsupportedRow(id, a.quadsUnsupported);
                r.regime = 'cold';
                rows.push(r);
            }
        }
    }

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
    const triples = genDataset(N_TRIPLES, datasetOptsFor(a));
    let h: unknown = await a.build(triples);
    // Record what the scan matches, as the pattern probes do — the strongest
    // cross-adapter check there is: every store must return the whole dataset.
    //
    // Guarded: this pre-pass runs the same consume the benches below run, so a
    // store that cannot scan at all — a trap, or the consume budget — fails
    // HERE, once, and the benches are skipped; they would otherwise fail three
    // more times. Its timing also picks the repetition plan.
    let preMs: number | null = null;
    try {
        const t0 = performance.now();
        matched.full = await a.countMatch(h, FULL_SCAN_PATTERN);
        preMs = performance.now() - t0;
    } catch (e) {
        const error = e instanceof Error ? e.message : String(e);
        console.error(`  !! full scan failed: ${error}`);
        failures.push({ phase: 'full', error });
    }
    if (preMs !== null) {
        const opts = SLOW_PHASE_MS > 0 && preMs > SLOW_PHASE_MS ? ONE_SHOT : FULL_SCAN_OPTS;
        if (opts === ONE_SHOT) {
            console.log(`  [full] one scan took ${(preMs / 1000).toFixed(0)}s — measuring 1 sample (BENCH_SLOW_PHASE_MS)`);
        }
        await bench('full', rows, opts, (b) => {
            b.add(`${a.slug}::full`, async () => { await a.countMatch(h, FULL_SCAN_PATTERN); });
        }, 'warm');
        // The count twin gets its own repetition plan: a store with a real
        // count path answers orders of magnitude below the consuming scan.
        await bench('full_count', rows, FULL_SCAN_OPTS, (b) => {
            b.add(`${a.slug}::full::count`, async () => { await a.countOnly(h, FULL_SCAN_PATTERN); });
        }, 'warm');
        if (a.snapshot && a.open) {
            const snap = await a.snapshot(h);
            await bench('full_cold', rows, opts, (b) => {
                let fresh: unknown;
                b.add(`${a.slug}::full`, async () => { await a.countMatch(fresh, FULL_SCAN_PATTERN); }, {
                    beforeEach: async () => { fresh = await a.open!(snap); },
                    afterEach: () => { a.dispose?.(fresh); fresh = undefined; },
                });
            }, 'cold');
            await bench('full_count_cold', rows, FULL_SCAN_OPTS, (b) => {
                let fresh: unknown;
                b.add(`${a.slug}::full::count`, async () => { await a.countOnly(fresh, FULL_SCAN_PATTERN); }, {
                    beforeEach: async () => { fresh = await a.open!(snap); },
                    afterEach: () => { a.dispose?.(fresh); fresh = undefined; },
                });
            }, 'cold');
        }
    }
    reclaim(a, h);
    h = null;
    return rows;
}

/**
 * The Arrow read of the same probes, in its own process.
 *
 * Two encodings against one baseline: `::arrow` exports the matched rows as
 * `string_view` columns and reads all four terms of every row — the same
 * values `::<pattern>` reads out of quads, so the pair is what the Arrow
 * surface is worth for the same delivered data. `::arrow_codes` exports the
 * `u32` code columns and reads their typed arrays, decoding no term at all:
 * not a like-for-like of the other two, but the currency a query engine joins
 * on, and the reason the interface exists.
 *
 * Its own process for two reasons. `apache-arrow` and the FFI parser are
 * multi-megabyte module graphs, and the zero-copy flag this role is spawned
 * with changes how the module's memory behaves — loading either into the query
 * worker would move every memory reading and every timing on the page. Only
 * the adapters with `arrowMatch` are ever spawned for it.
 */
async function runArrow(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    if (!a.arrowMatch) return rows;
    const zeroCopy = await installArrow();
    console.log(`[${a.label}] arrow… (zeroCopy=${zeroCopy})`);

    const h: unknown = await a.build(genDataset(N_TRIPLES, datasetOptsFor(a)));
    const pats = [...probesFor(a), FULL_SCAN_PATTERN];
    const costMs: Record<string, number> = {};
    // The same pre-pass the other roles run: every probe answered once, its
    // row count checked against the count path (a disagreement means the Arrow
    // export resolved something else, which would time the wrong work), and
    // its cost picking the repetition plan.
    for (const p of pats) {
        try {
            const t0 = performance.now();
            const n = a.arrowMatch(h, p, 'strings');
            costMs[p.name] = performance.now() - t0;
            const want = await a.countOnly(h, p);
            if (n !== want) throw new Error(`arrow rows disagree: ${n} vs ${want}`);
            const codes = a.arrowMatch(h, p, 'codes');
            if (codes !== want) throw new Error(`arrow codes rows disagree: ${codes} vs ${want}`);
        } catch (e) {
            const error = e instanceof Error ? e.message : String(e);
            console.error(`  !! arrow '${p.name}' failed: ${error}`);
            failures.push({ phase: `arrow:${p.name}`, error });
            delete costMs[p.name];
        }
    }
    const ok = pats.filter((p) => costMs[p.name] !== undefined);
    const { fast, slow } = splitBySpeed(ok, costMs);
    for (const [phase, group, opts] of [
        ['arrow', fast, QUERY_OPTS], ['arrow_slow', slow, ONE_SHOT],
    ] as const) {
        if (!group.length) continue;
        await bench(phase, rows, opts, (b) => {
            for (const p of group) {
                b.add(`${a.slug}::${p.name}::arrow`, () => { a.arrowMatch!(h, p, 'strings'); });
                b.add(`${a.slug}::${p.name}::arrow_codes`, () => { a.arrowMatch!(h, p, 'codes'); });
            }
        }, 'warm');
    }
    reclaim(a, h);
    return rows;
}

async function runMutate(a: StoreAdapter): Promise<Row[]> {
    const rows: Row[] = [];
    const fresh = genFresh(MUT_BATCH);
    const delSlice = genDatasetPrefix(N_TRIPLES, MUT_BATCH);

    console.log(`\n[${a.label}] add (${MUT_BATCH})…`);
    await bench('add', rows, HEAVY_OPTS, (b) => {
        let h: unknown;
        b.add(`add::${a.slug}`, async () => {
            h = await a.newEmpty();
            await a.addAll(h, fresh);
        }, { afterEach: () => { reclaim(a, h); h = undefined; } });
        if (a.addBatch) {
            let hb: unknown;
            b.add(`add_batch::${a.slug}`, async () => {
                hb = await a.newEmpty();
                await a.addBatch!(hb, fresh);
            }, { afterEach: () => { reclaim(a, hb); hb = undefined; } });
        }
    });

    console.log(`[${a.label}] delete (${MUT_BATCH})…`);
    let dh: unknown = null;
    await bench('delete', rows, HEAVY_OPTS, (b) => {
        b.add(`delete::${a.slug}`, async () => { await a.deleteAll(dh, delSlice); }, {
            beforeEach: async () => { dh = await a.build(delSlice); },
            afterEach: () => { reclaim(a, dh); dh = null; },
        });
    });

    return rows;
}

async function main(): Promise<void> {
    const list = role === 'mutate' ? MUT_ADAPTERS : ADAPTERS;
    const a = list.find((x) => x.slug === slug);
    if (!a) {
        console.error(`unknown adapter slug '${slug}' for role '${role}'`);
        process.exit(1);
    }

    const rows = role === 'query' ? await runQuery(a)
        : role === 'querycold' ? await runQueryCold(a)
            : role === 'fullscan' ? await runFullScan(a)
                : role === 'arrow' ? await runArrow(a)
                    : await runMutate(a);
    const out: WorkerOutput = {
        rows,
        peakRssMb: peakRssMb(),
        storeFootprint,
        matched,
        failures,
        cardinality: role === 'query' ? moduli(N_TRIPLES, { graphs: GRAPHS }) : undefined,
    };
    writeFileSync(outFile, JSON.stringify(out));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
