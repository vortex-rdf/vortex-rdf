import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, test, expect } from 'vitest';

import { BENCH_COMPARE_LOCK, OXIGRAPH_PKG_DIR, checkOxigraphVersion, lockedVersion } from '../bench/util.js';

/** A Cargo.lock's `[[package]]` block. */
const block = (name: string, version: string) => `[[package]]\nname = "${name}"\nversion = "${version}"\n`;

describe('the oxigraph build matches the version bench-compare pins', () => {
    test("lockedVersion reads the named package's version, not a neighbour's", () => {
        const lock = `version = 4\n\n${block('oxigraph', '0.5.13')}\n${block('oxrocksdb-sys', '0.5.12')}`;
        expect(lockedVersion(lock, 'oxigraph')).toBe('0.5.13');
    });

    test('lockedVersion throws unless the lock pins exactly one version', () => {
        const lock = `${block('oxigraph', '0.5.13')}\n${block('oxrocksdb-sys', '0.5.12')}`;
        expect(() => lockedVersion(lock, 'sophia')).toThrow(/pins no sophia/);
        expect(() => lockedVersion(`${lock}\n${block('oxigraph', '0.4.11')}`, 'oxigraph'))
            .toThrow(/pins 2 versions of oxigraph/);
    });

    test('checkOxigraphVersion passes the pinned build and names both versions otherwise', () => {
        const dir = mkdtempSync(join(tmpdir(), 'bench-oxigraph-version-'));
        try {
            const lockPath = join(dir, 'Cargo.lock');
            const pkgDir = join(dir, 'pkg');
            const manifest = join(pkgDir, 'package.json');
            writeFileSync(lockPath, block('oxigraph', '0.5.13'));
            mkdirSync(pkgDir);
            writeFileSync(manifest, '{"name":"oxigraph-js","version":"0.5.13"}');
            expect(checkOxigraphVersion(lockPath, pkgDir)).toBe('0.5.13');

            writeFileSync(manifest, '{"name":"oxigraph-js","version":"0.5.12"}');
            expect(() => checkOxigraphVersion(lockPath, pkgDir))
                .toThrow(/holds oxigraph 0\.5\.12 but .*Cargo\.lock pins 0\.5\.13/);

            rmSync(manifest);
            expect(() => checkOxigraphVersion(lockPath, pkgDir))
                .toThrow(/no oxigraph build at .*npm run build:oxigraph-wasm/);
        } finally {
            rmSync(dir, { recursive: true, force: true });
        }
    });

    test("the check reads bench-compare's lock and bench/oxigraph-pkg, the version build.sh builds", () => {
        expect(BENCH_COMPARE_LOCK).toBe(fileURLToPath(new URL('../../bench-compare/Cargo.lock', import.meta.url)));
        expect(OXIGRAPH_PKG_DIR).toBe(fileURLToPath(new URL('../bench/oxigraph-pkg', import.meta.url)));
        const printed = spawnSync(
            'bash', [fileURLToPath(new URL('../bench/oxigraph-wasm/build.sh', import.meta.url)), '--print-version'],
            { encoding: 'utf8' },
        ).stdout.trim();
        const pinned = lockedVersion(readFileSync(BENCH_COMPARE_LOCK, 'utf8'), 'oxigraph');
        expect(printed).toBe(pinned);
        expect(pinned).toMatch(/^\d+\.\d+\.\d+$/);
    });
});
