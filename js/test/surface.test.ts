import { describe, test, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// The generated pkg/web/vortex_rdf.d.ts lists every `#[wasm_bindgen]` method
// as an `InitOutput` key (`vortexrdfstore_<name>` / `termdict_<name>`); the
// hand-written src/api.d.ts must declare each of them on its class.

const read = (rel: string) => readFileSync(fileURLToPath(new URL(rel, import.meta.url)), 'utf8');

function wasmMethods(generated: string, prefix: string): string[] {
    const block = generated.slice(generated.indexOf('export interface InitOutput'));
    const re = new RegExp(`readonly ${prefix}_([A-Za-z0-9_]+):`, 'g');
    return [...block.matchAll(re)].map((m) => m[1]).filter((n) => !n.startsWith('__wbg'));
}

function declaredMethods(api: string, className: string): Set<string> {
    const start = api.indexOf(`export class ${className} {`);
    const end = api.indexOf('\n}', start);
    const block = api.slice(start, end);
    const names = new Set<string>();
    for (const m of block.matchAll(/^\s+(?:static\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\(/gm)) names.add(m[1]);
    return names;
}

describe('api.d.ts covers the wasm exports', () => {
    const generated = read('../pkg/web/vortex_rdf.d.ts');
    const api = read('../src/api.d.ts');

    for (const [prefix, className] of [['vortexrdfstore', 'VortexRdfStore'], ['termdict', 'TermDict'], ['arrowffi', 'ArrowFFI']] as const) {
        test(className, () => {
            const exported = wasmMethods(generated, prefix);
            expect(exported.length).toBeGreaterThan(0);
            const declared = declaredMethods(api, className);
            const missing = exported.filter((name) => !declared.has(name));
            expect(missing).toEqual([]);
        });
    }
});

// The three entries must re-export the same names from the generated module.
function reexports(source: string): string[] {
    const names: string[] = [];
    for (const m of source.matchAll(/export\s*\{([^}]*)\}\s*from\s*'\.\.\/pkg\/web\/vortex_rdf\.js'/g)) {
        for (const raw of m[1].split(',')) {
            const name = raw.trim();
            if (name) names.push(name);
        }
    }
    return names.sort();
}

describe('the entries re-export the same names', () => {
    test('entry/node.js, entry/browser.js and entry/types.d.ts agree', () => {
        const node = reexports(read('../entry/node.js'));
        const browser = reexports(read('../entry/browser.js'));
        const types = reexports(read('../entry/types.d.ts'));
        expect(node.length).toBeGreaterThan(0);
        expect(browser).toEqual(node);
        expect(types).toEqual(node);
    });
});

// The two /arrow entries wrap their base entry the same way: re-export all of
// it, add MatchView, and install matchArrow off the same module.
describe('the /arrow entries agree', () => {
    test('entry/arrow.node.js and entry/arrow.browser.js differ only in their base entry', () => {
        const node = read('../entry/arrow.node.js');
        const browser = read('../entry/arrow.browser.js');
        expect(node).toContain("export * from './node.js';");
        expect(browser).toContain("export * from './browser.js';");
        for (const source of [node, browser]) {
            expect(source).toContain('export { MatchView };');
            expect(source).toContain('attachMatchArrow(VortexRdfStore, memory, enableZeroCopy(memory));');
        }
        expect(read('../entry/arrow.d.ts')).toContain("export * from './types.js';");
    });
});
