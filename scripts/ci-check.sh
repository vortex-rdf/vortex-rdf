#!/usr/bin/env bash
# The commands of every CI job, one function per job. The workflows run their
# jobs through this script (.github/workflows/ci.yml, and docs.yml for docs),
# and the pre-push hook (scripts/hooks/pre-push) runs all of them, so a push
# fails here the way it would on GitHub.
#
#   scripts/ci-check.sh                  # every job, in this order
#   scripts/ci-check.sh lint rust-tests  # only the named jobs
#
# Jobs: docs, lint, rust-tests, interface-tests, python-tests, js-tests, java-tests.
#
# Under CI=true (GitHub Actions sets it) every job runs in full and a missing
# tool fails it. A local run differs in three ways:
#   * A step whose tool is missing is skipped with a warning, and the summary
#     names it: the wasm32 clippy and js-tests need the wasm32-unknown-unknown
#     target, python-tests needs uv, js-tests needs npm and js/node_modules,
#     java-tests needs a JDK and Maven.
#   * js-tests builds with `npm run build:fast` (wasm-pack without wasm-opt,
#     which dominates the full build's wall clock and buys no memory headroom)
#     in the installed js/node_modules, where CI runs `npm ci` and
#     `npm run build`. The tests then run against an unoptimized binary, so CI
#     stays the source of truth for the artifact that ships.
#   * interface-tests and js-tests compile with at most 4 jobs unless
#     CARGO_BUILD_JOBS is set: a cold build compiles DataFusion's or the wasm
#     build's dependency tree, and one rustc per core can exhaust a small
#     machine's memory. CI sets no cap.
set -euo pipefail

JOBS=(docs lint rust-tests interface-tests python-tests js-tests java-tests)

case "${1:-}" in
  -h | --help)
    # The header above, without its comment markers.
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
    exit 0
    ;;
esac
for job in "$@"; do
  case " ${JOBS[*]} " in
    *" $job "*) ;;
    *)
      printf 'ci-check.sh: unknown job "%s" (jobs: %s)\n' "$job" "${JOBS[*]}" >&2
      exit 2
      ;;
  esac
done

cd "$(git rev-parse --show-toplevel)"

info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m==>\033[0m %s\n' "$1"; }

in_ci() { [ "${CI:-}" = true ]; }

# Prints a command and runs it; `run -C dir …` runs it inside dir.
run() {
  if [ "$1" = -C ]; then
    local dir=$2
    shift 2
    info "$dir: $*"
    (cd "$dir" && "$@")
  else
    info "$*"
    "$@"
  fi
}

# A missing tool. Under CI it fails the job. Locally it skips one step
# (skip_step) or the whole job (skip_job) with a warning, and the summary
# names what did not run.
skip_step() { # <step> <what is missing> <how to get it>
  if in_ci; then
    printf '\033[1;31m==>\033[0m %s\n' "$1: $2; under CI=true a missing tool fails the job." >&2
    exit 1
  fi
  warn "$1: skipped, $2 ($3)."
  skipped+=("$1 ($2)")
}
skip_job() { # <what is missing> <how to get it>
  skip_step "$current_job" "$1" "$2"
  job_skipped=true
}

# Whether rustup lists the wasm32 target. Without rustup there is nothing to
# ask, so the wasm32 steps run (and fail loudly if the target is missing).
have_wasm32_target() {
  ! command -v rustup >/dev/null 2>&1 ||
    rustup target list --installed 2>/dev/null | grep -x wasm32-unknown-unknown >/dev/null
}

# The doc-anchor check. Cheap and tool-free (bash and python3), so it runs
# first: a broken `#L` anchor fails before the compilers are even warm.
job_docs() {
  run scripts/check-doc-anchors.sh
}

job_lint() {
  run cargo fmt --check
  # Proves bench-compare's lock still resolves against core's dependencies; builds nothing.
  info "cargo metadata --locked --manifest-path bench-compare/Cargo.toml --format-version 1"
  cargo metadata --locked --manifest-path bench-compare/Cargo.toml --format-version 1 > /dev/null
  run cargo clippy --workspace --all-targets -- -D warnings
  # Core's other build configuration: the wasm bindings take it without
  # `file-io`, a dead-code surface of its own (items only reachable through
  # that feature), which the native run cannot see.
  if have_wasm32_target; then
    run cargo clippy -p vortex-rdf --target wasm32-unknown-unknown --all-targets -- -D warnings
  else
    skip_step "the wasm32 clippy" "no wasm32-unknown-unknown target" \
      "rustup target add wasm32-unknown-unknown"
  fi
}

job_rust_tests() {
  run cargo test --workspace
}

# The DataFusion conformance crate is a workspace of its own
# (interface-tests/Cargo.toml), checked against its own Cargo.lock.
job_interface_tests() {
  if ! in_ci; then local -x CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"; fi
  run cargo fmt --manifest-path interface-tests/Cargo.toml --check
  run cargo clippy --manifest-path interface-tests/Cargo.toml --all-targets --locked -- -D warnings
  run cargo test --manifest-path interface-tests/Cargo.toml --locked
}

# `bench/test_codspeed.py` runs too: without `--codspeed` the plugin calls each
# benchmark once instead of instrumenting it, a seconds-long smoke test of every
# benchmark against the current bindings. The CodSpeed workflow is otherwise
# the only thing that imports it, so a signature change would land as a red
# benchmark job on a later pull request rather than as a test failure here.
job_python_tests() {
  if ! command -v uv >/dev/null 2>&1; then
    skip_job "no uv" "install uv to run it"
    return 0
  fi
  run -C python uv sync --locked
  run -C python uv run --no-sync pytest tests bench/test_codspeed.py bench/test_suite.py -q
}

job_js_tests() {
  if ! command -v npm >/dev/null 2>&1; then
    skip_job "no npm" "install Node.js, version in .tool-versions"
    return 0
  fi
  if ! in_ci && [ ! -d js/node_modules ]; then
    skip_job "no js/node_modules" "run \`cd js && npm ci\`"
    return 0
  fi
  if ! have_wasm32_target; then
    skip_job "no wasm32-unknown-unknown target" "rustup target add wasm32-unknown-unknown"
    return 0
  fi
  if in_ci; then
    # wasm-pack is a pinned npm devDependency: `npm ci` installs it, and
    # `npm run` puts node_modules/.bin ahead of anything on PATH.
    run -C js npm ci
    run -C js npm run build
  else
    local -x CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"
    # Rebuilds js/pkg/web in place (git-ignored, so the tree stays clean),
    # replacing any wasm-opt'd build already there.
    run -C js npm run build:fast
  fi
  # Typechecks after the build: the tsconfigs resolve against the generated
  # pkg/web/vortex_rdf.d.ts.
  run -C js npm run typecheck
  run -C js npm test
}

# The Java bindings: the crate's own Rust tests run under rust-tests; this job
# builds the JNI library and the stores and oracle the JUnit suite reads (the
# crate's `fixtures` example), then runs Maven.
job_java_tests() {
  if ! command -v java >/dev/null 2>&1 || ! command -v mvn >/dev/null 2>&1; then
    skip_job "no JDK or Maven" "install JDK 11 or later and Maven"
    return 0
  fi
  local target="${CARGO_TARGET_DIR:-target}" lib
  case $target in /*) ;; *) target="$PWD/$target" ;; esac
  run cargo build -p vortex-rdf-java
  run cargo run -p vortex-rdf-java --example fixtures -- java/target/fixtures
  lib=$(compgen -G "$target/debug/libvortex_rdf_jni.*" | grep -E '\.(so|dylib)$' | head -n 1)
  run -C java mvn -B test "-Dvortexrdf.library=$lib" "-Dvortexrdf.fixtures=$PWD/java/target/fixtures"
}

join_list() {
  local out="" item
  for item in "$@"; do
    [ -n "$out" ] && out+=", "
    out+="$item"
  done
  printf '%s' "$out"
}

[ $# -gt 0 ] || set -- "${JOBS[@]}"
passed=()
skipped=()
for job in "$@"; do
  current_job=$job
  job_skipped=false
  "job_${job//-/_}"
  "$job_skipped" || passed+=("$job")
done

if [ ${#passed[@]} -gt 0 ]; then
  info "Passed: $(join_list "${passed[@]}")."
fi
if [ ${#skipped[@]} -gt 0 ]; then
  warn "Not run: $(join_list "${skipped[@]}")."
fi
