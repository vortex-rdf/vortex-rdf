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

# --- js-tests job ---
# The WASM build + npm test is the slowest check, so it only runs when files
# that can affect the JS package have changed relative to $CI_CHECK_BASE
# (set by the pre-push hook to the remote's current sha). Override with
# VORTEX_JS_CHECK=always or VORTEX_JS_CHECK=never.
base_ref="${CI_CHECK_BASE:-}"
if [[ -z "$base_ref" ]] && git rev-parse --verify --quiet origin/main >/dev/null; then
  base_ref="$(git merge-base HEAD origin/main 2>/dev/null || true)"
fi

run_js=0
case "${VORTEX_JS_CHECK:-auto}" in
  always) run_js=1 ;;
  never) run_js=0 ;;
  auto)
    if [[ -z "$base_ref" ]]; then
      # No base ref to diff against (e.g. no origin/main yet) - be safe and run it.
      run_js=1
    elif git diff --name-only "$base_ref"...HEAD | grep -qE '^(js/|core/|Cargo\.toml$|Cargo\.lock$)'; then
      run_js=1
    fi
    ;;
esac

if [[ "$run_js" -eq 1 ]]; then
  info "js-tests: build wasm + npm test (js/, core/, or Cargo files changed)"
  (cd js && npm run build && npm test)
else
  info "js-tests: skipped, no changes under js/, core/, Cargo.toml or Cargo.lock (force with VORTEX_JS_CHECK=always)"
fi

info "All CI checks passed."
