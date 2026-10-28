#!/usr/bin/env bash
# One-command dashboard refresh, whole or partial. The dashboard workflow
# (.github/workflows/bench-dashboard.yml) runs it too.
#
#   scripts/refresh.sh                     # all six stages (over an hour at full scale)
#   scripts/refresh.sh --only js,render    # re-measure one surface, re-render
#   scripts/refresh.sh --only render       # template-only changes: no re-measurement
#   scripts/refresh.sh --force-build       # rebuild the bindings even if their stamps match
#
# Stages, in the order they run: rust (the internals targets, benchmark +
# match_lazy), compare (the cross-library suite: bench-compare/, a workspace of
# its own that this script formats, lints and benches), js, python, java,
# render. Every output lands in target/bench/: the suites' results (rust.txt,
# rust-compare.json, js.json, python.json, java.json) and the page,
# public/index.html. The java stage needs Maven running on JDK 25 or newer;
# that is checked before the first stage, so a missing tool fails in seconds.
#
# Measurement stages run strictly sequentially on purpose: parallel stages
# contend for CPU and memory and contaminate each other's timings — the
# process-per-adapter isolation inside each suite exists for the same reason.
#
# The wasm, Python and Java bindings, and the CLI the Java suite builds its
# Vortex stores with, are rebuilt only when their stamp no longer matches (see
# stamp()); wasm-pack alone costs ~10 minutes, which a harness- or
# template-only iteration should never pay.
#
# Needs GNU coreutils for the stamps (sha256sum, sort -z).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# The dashboard scale — the indicative-overview default every suite shares
# (all of them read BENCH_SIZE): 2^20 rows, one dataset. MUT_BATCH is the
# comparative suites' add/delete batch; the internals suite keeps its own
# (see the rust stage). Same numbers .github/workflows/bench-dashboard.yml pins.
export BENCH_SIZE="${BENCH_SIZE:-1048576}"
export MUT_BATCH="${MUT_BATCH:-10000}"
OUT=target/bench

ONLY="rust,compare,js,python,java,render"
FORCE_BUILD=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$2"; shift 2 ;;
    --only=*) ONLY="${1#*=}"; shift ;;
    --force-build) FORCE_BUILD=1; shift ;;
    -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"; exit 0 ;;
    *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
  esac
done

for s in ${ONLY//,/ }; do
  case "$s" in rust|compare|js|python|java|render) ;; *)
    echo "unknown stage: $s (stages: rust, compare, js, python, java, render)" >&2; exit 2 ;;
  esac
done

has() { case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# The java stage's tools, checked before the first stage runs rather than when
# the stage starts, after the stages before it. Maven compiles with the JDK it
# reports running on, and the suite's workers run on that JDK too.
if has java; then
  java_major=$(mvn -v 2>/dev/null | sed -n 's/^Java version: \([0-9]*\).*/\1/p' || true)
  if [ -z "$java_major" ] || [ "$java_major" -lt 25 ]; then
    echo "the java stage needs Maven running on JDK 25 or newer (found: ${java_major:-no mvn on PATH}); install them or leave java out of --only" >&2
    exit 2
  fi
fi
t_start=$(date +%s)
stage() { echo; echo "══ $1 · $(date +%H:%M:%S) ══"; }

# A binding build leaves a stamp beside its artifact, ARTIFACT.sha256: one
# sha256 over the build profile (the command that made the artifact, and the
# compiler's `rustc -V`), the artifact's bytes and the content of every file it
# is built from. A changed command, compiler or source invalidates the stamp,
# and so does any other build that rewrites the artifact with different bytes
# (the wasm `npm run build:fast` makes without wasm-opt, a debug `maturin develop`).
stamp() { # PROFILE ARTIFACT DEP...
  local profile="$1"; shift
  {
    echo "$profile"
    rustc -V
    find "$@" -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
  } | sha256sum | cut -d' ' -f1
}

# fresh PROFILE ARTIFACT DEP... — true when ARTIFACT's stamp still matches, i.e.
# the build can be skipped.
fresh() {
  [ "$FORCE_BUILD" = 0 ] && [ -f "$2" ] && [ -f "$2.sha256" ] &&
    [ "$(stamp "$@")" = "$(cat "$2.sha256")" ]
}

# build PROFILE ARTIFACT DEP... — runs PROFILE, a plain command line, unless
# ARTIFACT is fresh, and stamps ARTIFACT once PROFILE succeeds.
build() {
  if fresh "$@"; then
    echo "$2 matches its stamp — skipping $1 (--force-build overrides)"
  else
    $1 && stamp "$@" > "$2.sha256"
  fi
}

# What a binding is built from, relative to its own directory (js/, python/):
# its crate, the crates it links, the workspace manifest and lock file, and the
# cargo config (the wasm32 rustflags).
BINDING_DEPS=(src Cargo.toml ../core/src ../core/Cargo.toml
  ../encoded-search/src ../encoded-search/Cargo.toml ../Cargo.toml ../Cargo.lock
  ../.cargo/config.toml)

if has rust; then
  stage "Rust internals (benchmark + match_lazy → $OUT/rust.txt, BENCH_SIZE=$BENCH_SIZE)"
  mkdir -p "$OUT"
  # The internals suite keeps its own clamped mutate batch, not MUT_BATCH.
  env -u MUT_BATCH cargo bench --bench benchmark | tee "$OUT/rust.txt"
  env -u MUT_BATCH cargo bench --bench match_lazy | tee -a "$OUT/rust.txt"
fi

if has compare; then
  stage "Rust cross-library (compare → $OUT/rust-compare.json)"
  # bench-compare/ is a workspace of its own, so oxigraph's RocksDB and the
  # other libraries under comparison build here only: lint it where it is
  # built, then bench it.
  cargo fmt --manifest-path bench-compare/Cargo.toml --check
  cargo clippy --manifest-path bench-compare/Cargo.toml --all-targets --locked -- -D warnings
  cargo bench --manifest-path bench-compare/Cargo.toml
fi

if has js; then
  stage "JavaScript comparative (→ $OUT/js.json, BENCH_SIZE=$BENCH_SIZE)"
  (
    cd js
    [ -d node_modules ] || npm ci
    build "npm run build" pkg/web/vortex_rdf_bg.wasm "${BINDING_DEPS[@]}"
    build "npm run build:hdt-wasm" bench/hdt-pkg/hdt_wasm_bench_bg.wasm \
      bench/hdt-wasm/src bench/hdt-wasm/Cargo.toml bench/hdt-wasm/Cargo.lock \
      ../.cargo/config.toml
    npm run bench
  )
fi

if has python; then
  stage "Python comparative (→ $OUT/python.json, BENCH_SIZE=$BENCH_SIZE)"
  (
    cd python
    if [ ! -x .venv/bin/maturin ]; then
      uv venv .venv --python 3.13
      uv pip install --python .venv/bin/python maturin
    fi
    # An out-of-date .so is the classic silent trap: run.py reaches the
    # bindings via PYTHONPATH and would happily measure a stale extension.
    VIRTUAL_ENV="$PWD/.venv" build ".venv/bin/maturin develop --release" \
      vortex_rdf/_native.abi3.so "${BINDING_DEPS[@]}" pyproject.toml
    python3 bench/run.py
  )
fi

if has java; then
  stage "Java comparative (→ $OUT/java.json, BENCH_SIZE=$BENCH_SIZE)"
  # Two cargo invocations, not one: building both packages together would
  # unify core's features, and the CLI's mimalloc would reach the JNI library.
  (cd java && build "cargo build --release -p vortex-rdf-java" ../target/release/libvortex_rdf_jni.so "${BINDING_DEPS[@]}")
  (cd cli && build "cargo build --release -p vortex-rdf-cli" ../target/release/vortex-rdf-cli "${BINDING_DEPS[@]}")
  mvn -B -q -f java/bench/pom.xml package -DskipTests
  python3 java/bench/run.py
fi

if has render; then
  stage "Render ($OUT/public/index.html)"
  args=("$OUT/rust.txt" "$OUT/public/index.html")
  for suite in rust-compare js python java; do
    if [ -f "$OUT/$suite.json" ]; then args+=("--$suite" "$OUT/$suite.json"); fi
  done
  python3 scripts/render_bench_dashboard.py "${args[@]}"
fi

echo
echo "done in $(( ($(date +%s) - t_start) / 60 )) min ($ONLY)"
