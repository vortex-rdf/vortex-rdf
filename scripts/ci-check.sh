#!/usr/bin/env bash
# Mirrors the jobs in .github/workflows/ci.yml so failures are caught before
# they reach GitHub. Run directly (`./scripts/ci-check.sh`) or let the
# pre-push hook (scripts/hooks/pre-push) invoke it automatically.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }

# --- lint job ---
info "cargo fmt --check"
cargo fmt --check

info "cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

# --- rust-tests job ---
info "cargo test --workspace"
cargo test --workspace

info "cargo test -p vortex-rdf-core --no-default-features"
cargo test -p vortex-rdf-core --no-default-features

# The js-tests job (wasm-pack build + npm test) is intentionally not mirrored
# here: the wasm32 build is memory-hungry enough that it needs the
# CARGO_BUILD_JOBS=1 / CODEGEN_UNITS=1 / OPT_LEVEL=s tuning ci.yml applies
# even to run reliably in CI, and it got SIGTERM'd locally even in dev mode.
# Run it manually with `(cd js && npm run build && npm test)` before pushing
# JS/wasm changes; GitHub CI is the source of truth for this job.
info "All CI checks passed (js-tests job not run locally; see comment above)."
