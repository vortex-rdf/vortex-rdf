#!/usr/bin/env bash
# Mirrors the jobs in .github/workflows/ci.yml so failures are caught before
# they reach GitHub. Run directly (`./scripts/ci-check.sh`) or let the
# pre-push hook (scripts/hooks/pre-push) invoke it automatically.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m==>\033[0m %s\n' "$1"; }

# Jobs that ran clean vs. jobs skipped for a missing tool, so the closing
# summary names both instead of hard-coding one shape.
passed=()
skipped=()
join_list() {
  local out="" item
  for item in "$@"; do
    [ -n "$out" ] && out+=", "
    out+="$item"
  done
  printf '%s' "$out"
}

# --- lint job ---
info "cargo fmt --check"
cargo fmt --check

info "cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

info "cargo clippy -p vortex-rdf-core --no-default-features --all-targets -- -D warnings"
cargo clippy -p vortex-rdf-core --no-default-features --all-targets -- -D warnings
passed+=(lint)

# --- rust-tests job ---
info "cargo test --workspace"
cargo test --workspace

info "cargo test -p vortex-rdf-core --no-default-features"
cargo test -p vortex-rdf-core --no-default-features
passed+=(rust-tests)

# --- python-tests job ---
# Skipped (with a warning, not a failure) when uv is absent, so the hook stays
# usable on a clone that never touches the bindings.
if command -v uv >/dev/null 2>&1; then
  info "uv sync --locked && uv run pytest tests -q (python)"
  (cd python && uv sync --locked && uv run pytest tests -q)
  passed+=(python-tests)
else
  warn "python: skipping python-tests — uv not found (install uv to mirror it)."
  skipped+=("python-tests (no uv)")
fi

# --- js-tests job ---
# Mirrors ci.yml's js-tests with two deliberate differences:
#
#   * `build:fast` (wasm-pack --no-opt) skips wasm-opt. Measured on a warm
#     cache, wasm-opt costs ~70s of the full build's ~78s and *no* extra
#     memory (peak is ~1.7 GB either way) — it buys wall clock, not headroom.
#     The trade is that the tests then run against an unoptimized binary, so
#     CI stays the source of truth for the exact artifact that ships.
#
#   * CARGO_BUILD_JOBS is capped. Memory here is spent by rustc, not wasm-opt:
#     a warm incremental rebuild is one process peaking ~1.7 GB, but a cold
#     wasm build compiles ~900 dependency crates and cargo would otherwise
#     fan out to one rustc per core. That fan-out — not the optimizer — is
#     what made this job unrunnable locally before. ci.yml pins it to 1 for
#     4-core/16 GB runners; 4 is the local compromise. Override by exporting
#     CARGO_BUILD_JOBS yourself.
#
# Rebuilds js/pkg/web in place (gitignored, so the tree stays clean), which
# replaces any wasm-opt'd build left there by a previous `npm run build`.
if ! command -v npm >/dev/null 2>&1; then
  warn "js: skipping js-tests — npm not found."
  skipped+=("js-tests (no npm)")
elif [ ! -d js/node_modules ]; then
  warn "js: skipping js-tests — js/node_modules missing (run \`cd js && npm ci\`)."
  skipped+=("js-tests (no node_modules)")
elif command -v rustup >/dev/null 2>&1 &&
  ! rustup target list --installed 2>/dev/null | grep -qx wasm32-unknown-unknown; then
  warn "js: skipping js-tests — run \`rustup target add wasm32-unknown-unknown\`."
  skipped+=("js-tests (no wasm32 target)")
else
  info "npm run build:fast && npm run typecheck && npm test (js)"
  (
    cd js
    export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"
    npm run build:fast
    # Typechecks after the build: the tsconfigs resolve against the generated
    # pkg/web/vortex_rdf.d.ts.
    npm run typecheck
    npm test
  )
  passed+=(js-tests)
fi

# --- summary ---
info "Passed: $(join_list "${passed[@]}")."
if [ ${#skipped[@]} -gt 0 ]; then
  warn "Not run: $(join_list "${skipped[@]}")."
fi
