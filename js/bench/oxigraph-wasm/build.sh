#!/usr/bin/env bash
# Builds oxigraph's official JS binding, the `js/` crate of its repository,
# from source, with the bench's live-byte counter patched in, into
# js/bench/oxigraph-pkg: web.js, web_bg.wasm, web.d.ts, and wasm-pack's
# package.json, whose "version" compare.bench.ts checks against the pin.
#
#   bash js/bench/oxigraph-wasm/build.sh                  # the version bench-compare/Cargo.lock pins
#   bash js/bench/oxigraph-wasm/build.sh 0.5.13           # release v0.5.13 (refresh.sh passes the pin)
#   bash js/bench/oxigraph-wasm/build.sh --print-version  # print the pinned version, build nothing
#
# oxigraph's npm package has no counter, so the bench builds the binding
# itself, at the version the Rust comparison pins: both tabs then measure one
# release. The release tag is fetched with a shallow `git clone` into
# target/oxigraph-src, afresh on every run; memory-stats.patch makes
# bench-memory-stats (../memory-stats) the binding's global allocator; then
# upstream's web build runs, its output written out of tree.
# `npm run build:oxigraph-wasm` runs this script, and scripts/refresh.sh's js
# stage runs that when the build's stamp no longer matches.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../../.." && pwd)
# The npm-pinned wasm-pack (0.15.1), the one `npm run build:hdt-wasm` uses.
PATH="$root/js/node_modules/.bin:$PATH"

pinned=$(sed -n '/^name = "oxigraph"$/{n;s/^version = "\(.*\)"$/\1/p;}' "$root/bench-compare/Cargo.lock")
case $pinned in
  '' | *$'\n'*)
    echo "bench-compare/Cargo.lock pins no single oxigraph version" >&2
    exit 1
    ;;
esac
if [ "${1:-}" = --print-version ]; then
  echo "$pinned"
  exit 0
fi
version=${1:-$pinned}

# The patch's path to the counter crate assumes this fixed location. No
# submodules are needed: the binding uses none of them, since RocksDB (with
# its lz4) is a native-only dependency and the rest are test suites and tools.
src=$root/target/oxigraph-src
rm -rf "$src"
git -c advice.detachedHead=false clone -q --depth 1 --branch "v$version" https://github.com/oxigraph/oxigraph.git "$src"
patch -d "$src" -p1 < "$here/memory-stats.patch"

# Upstream's web build (the `build` script in its js/package.json). The
# RUSTFLAGS are upstream's own, from its nodejs build; setting them also
# overrides this repository's .cargo/config.toml (+simd128), which cargo would
# otherwise find above target/. CARGO_TARGET_DIR keeps the compiled crates
# across fresh clones. No --locked: the patch adds an entry to oxigraph's lock.
cd "$src/js"
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' CARGO_TARGET_DIR="$root/target/oxigraph-target" \
  wasm-pack build --release --target web --out-name web --weak-refs --reference-types \
  --out-dir "$root/js/bench/oxigraph-pkg"

# wasm-pack's .gitignore would hide web.d.ts, which is committed; its
# package.json stays, since it carries the version the suite checks.
rm -f "$root/js/bench/oxigraph-pkg/.gitignore"
