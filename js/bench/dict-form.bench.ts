// Side experiment: the resident form of an adopted store's term dictionary —
// 'as-written' (the file's FSST chunks, decoded one term per read) against
// 'plaintext' (decoded once at `fromBytes`) — priced on what a wasm consumer
// does: adopt from bytes, probe, decode, scan, export terms. Memory is not
// measured here (wasm linear memory only grows, and the build transient hides
// an adopted store's footprint); the counting-allocator example
// `core/examples/dict_memory.rs` gives the exact bytes.
//
//   BENCH_SIZE=262144 node --import tsx --expose-gc --experimental-wasm-rab-integration bench/dict-form.bench.ts
import { Bench } from 'tinybench';

import { VortexRdfStore, type DictForm } from '@vortex-rdf/vortex-rdf-store/arrow';
import { FULL_SCAN_PATTERN, datasetProbes, genDataset, moduli, subjectTerm, type Pat } from './datasets.js';
import { decodeAll, fmtNs } from './util.js';

const N = Number(process.env.BENCH_SIZE ?? 0) || 262_144;
const FORMS: DictForm[] = ['as-written', 'plaintext'];

const built = await VortexRdfStore.fromQuads(genDataset(N), { layout: 'dictionary' });
const bytes = await built.toBytes();
built.free();

const probes = datasetProbes(N).triples;
const S = probes.find((p) => p.name === 'S')!;
const P = probes.find((p) => p.name === 'P')!;
const { nSubj: subjects, terms } = moduli(N);
const args = (pat: Pat) => [pat.s, pat.p, pat.o, pat.g] as const;

async function adoptMs(form: DictForm): Promise<number> {
    const times: number[] = [];
    for (let i = 0; i < 3; i++) {
        const start = performance.now();
        const store = await VortexRdfStore.fromBytes(bytes, { dictionary: form });
        times.push(performance.now() - start);
        store.free();
    }
    return times.sort((a, b) => a - b)[1];
}

console.log(`rows=${N} terms=${terms} bytes=${(bytes.byteLength / 1048576).toFixed(1)} MiB  (mean per call)`);
for (const form of FORMS) {
    console.log(`\n${form}`);
    console.log(`  fromBytes (median of 3)            ${fmtNs((await adoptMs(form)) * 1e6)}`);
    const store = await VortexRdfStore.fromBytes(bytes, { dictionary: form });
    const dict = store.termDict()!;
    let next = 0;
    const bench = new Bench({ time: 800, warmupTime: 150 });
    bench.add('countQuads S, distinct subjects', () => {
        store.countQuads(subjectTerm(next++ % subjects), null, null, null);
    });
    bench.add('encode, distinct subjects', () => {
        dict.encode(`<${subjectTerm(next++ % subjects).value}>`);
    });
    bench.add('decode 1k distinct codes', () => {
        for (let c = 0; c < 1000; c++) dict.decode((c * 7919) % terms);
    });
    bench.add('getQuads S + decode', () => { decodeAll(store.getQuads(...args(S))); });
    bench.add('getQuads P + decode', () => { decodeAll(store.getQuads(...args(P))); });
    bench.add('getQuads full + decode', () => { decodeAll(store.getQuads(...args(FULL_SCAN_PATTERN))); });
    bench.add('matchArrow codes, full', () => {
        store.matchArrow(...args(FULL_SCAN_PATTERN), { encoding: 'codes' }).free();
    });
    bench.add('matchArrow terms, full', () => {
        store.matchArrow(...args(FULL_SCAN_PATTERN), { encoding: 'terms' }).free();
    });
    await bench.run();
    for (const task of bench.tasks) {
        const r = task.result;
        if (!r || !('latency' in r) || !r.latency) continue;
        console.log(`  ${task.name.padEnd(34)} ${fmtNs(r.latency.mean * 1e6)}`);
    }
    dict.free();
    store.free();
}
