// The Arrow surface priced per consumer step: a match parsed as views over
// wasm memory (`matchArrow`, zero-copy under `--experimental-wasm-rab-integration`,
// which `npm run bench:arrow` passes; a parse-time copy without it), its
// JS-owned copy (`toTable()`), its IPC bytes (`toIPC()`, the Worker
// currency), and what an engine then does with the columns — `toArray()` on
// the codes, or a read of every value — per encoding, on a point pattern, a
// predicate pattern and the full scan. Prints whether the views were
// zero-copy, and asserts they survive memory growth when they are.
//
//   BENCH_SIZE=262144 npm run bench:arrow
import { Bench } from 'tinybench';
import type { Table } from 'apache-arrow';

import init from '../pkg/web/vortex_rdf.js';
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store/arrow';
import { FULL_SCAN_PATTERN, datasetProbes, genDataset, type Pat } from './datasets.js';
import { fmtNs } from './util.js';

const N = Number(process.env.BENCH_SIZE ?? 0) || 262_144;
const ENCODINGS = ['codes', 'terms', 'strings'] as const;
type Encoding = (typeof ENCODINGS)[number];

const { memory } = await init();
const store = await VortexRdfStore.fromQuads(genDataset(N), { layout: 'dictionary' });
const probes = datasetProbes(N).triples;
const patterns: Pat[] = [
    probes.find((p) => p.name === 'S')!,
    probes.find((p) => p.name === 'P')!,
    FULL_SCAN_PATTERN,
];
const args = (pat: Pat) => [pat.s, pat.p, pat.o, pat.g] as const;

/** Touch every value of the `s` column, so views and copies pay the same read. */
function consume(table: Table): number {
    const column = table.getChild('s')!;
    let acc = 0;
    for (const value of column) {
        acc += typeof value === 'number' ? value : (value as string).length;
    }
    return acc;
}

/** The typed-array read an engine does: `toArray()` on the codes column. */
function typed(table: Table): number {
    return table.getChild('s')!.toArray().length;
}

// ── zero-copy status, and views across growth ─────────────────────────────
{
    const view = store.matchArrow(...args(FULL_SCAN_PATTERN));
    console.log(`rows=${N}  zeroCopy=${view.zeroCopy}  memory pages=${memory.buffer.byteLength / 65536}`);
    if (view.zeroCopy) {
        const before = Array.from(view.table.getChild('s')!.toArray() as Uint32Array);
        const buffer = memory.buffer;
        const other = await VortexRdfStore.fromQuads(genDataset(N * 2), { layout: 'dictionary' });
        memory.grow(1);
        const after = Array.from(view.table.getChild('s')!.toArray() as Uint32Array);
        if (memory.buffer !== buffer || before.length !== after.length || before.some((v, i) => v !== after[i])) {
            throw new Error('zero-copy views did not survive memory growth');
        }
        console.log(`views survived growth to ${memory.buffer.byteLength / 65536} pages`);
        other.free();
    }
    view.free();
}

// ── timings ──────────────────────────────────────────────────────────────
console.log('(mean per call)');
for (const pat of patterns) {
    for (const encoding of ENCODINGS) {
        const bench = new Bench({ time: 800, warmupTime: 150 });
        const tag = `${pat.name}/${encoding}`;
        const view = (use: (t: Table) => unknown) => {
            const v = store.matchArrow(...args(pat), { encoding });
            use(v.table);
            v.free();
        };
        const copy = (use: (t: Table) => unknown) => {
            const v = store.matchArrow(...args(pat), { encoding });
            use(v.toTable());
            v.free();
        };
        bench.add(`${tag} view`, () => { view((t) => t.numRows); });
        bench.add(`${tag} toTable`, () => { copy((t) => t.numRows); });
        bench.add(`${tag} toIPC`, () => {
            const v = store.matchArrow(...args(pat), { encoding });
            v.toIPC();
            v.free();
        });
        if (encoding === 'codes') {
            bench.add(`${tag} view +toArray`, () => { view(typed); });
            bench.add(`${tag} toTable +toArray`, () => { copy(typed); });
        }
        bench.add(`${tag} view +read`, () => { view(consume); });
        bench.add(`${tag} toTable +read`, () => { copy(consume); });
        await bench.run();
        for (const task of bench.tasks) {
            const r = task.result;
            if (!r || !('latency' in r) || !r.latency) continue;
            console.log(`  ${task.name.padEnd(30)} ${fmtNs(r.latency.mean * 1e6)}`);
        }
    }
}
store.free();
