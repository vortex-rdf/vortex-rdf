#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="python"
BENCHMARK_SCRIPT="dbbench_rdflib_benchmark.py"
DATASET="dbpedia"
GROUP="TP"
JOIN_SIZE_SMALL="small"
JOIN_SIZE_BIG="big"
WARMUP_RUNS="1"
MEASURED_RUNS="5"
MAX_QUERIES=""
ONLY_FILE_CONTAINS=""
QUERY_ROOT=""
COTTAS_PATH=""
VORTEX_IDS_PATH=""
VORTEX_STRINGS_PATH=""
OUT_DIR=""
NO_SILENCE_STDOUT="0"

# Whole benchmark-config watchdog. This prevents one engine/layout run from
# hanging forever if the Python process blocks inside PyO3/Rust and its own
# SIGALRM/query timeout cannot interrupt it.
# 0 disables the outer watchdog.
BENCHMARK_TIMEOUT_SECONDS="1800"
TIMEOUT_KILL_AFTER_SECONDS="10"

# Native-id materialization strategy default. The Rust side can override this.
# Kept here so DBBench runs do not accidentally fall back to the old OR path
# after you switch to the no-OR cottas_native_ids.rs.
export VORTEX_RDF_NATIVE_ID_LOOKUP_SINGLE_EQ_IDS="${VORTEX_RDF_NATIVE_ID_LOOKUP_SINGLE_EQ_IDS:-64}"

die() {
  echo "ERROR: $*" >&2
  exit 2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_file() {
  [[ -f "$1" ]] || die "File not found: $1"
}

require_int() {
  [[ "$1" =~ ^[0-9]+$ ]] || die "Expected non-negative integer, got: $1"
}

usage() {
  cat <<EOF
Usage:
  $0 --query-root PATH --cottas-path PATH --vortex-ids-path PATH --vortex-strings-path PATH [options]

Options:
  --python PATH
  --benchmark-script PATH
  --dataset NAME
  --groups GROUPS
  --warmup-runs N
  --measured-runs N
  --max-queries N
  --only-file-contains SUBSTR
  --out-dir DIR
  --benchmark-timeout-seconds N     Outer watchdog per engine/layout run. 0 disables. Default: ${BENCHMARK_TIMEOUT_SECONDS}
  --timeout-kill-after-seconds N    Seconds after TERM before KILL. Default: ${TIMEOUT_KILL_AFTER_SECONDS}
  --no-silence-stdout
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      PYTHON_BIN="$2"; shift 2 ;;
    --benchmark-script)
      BENCHMARK_SCRIPT="$2"; shift 2 ;;
    --query-root)
      QUERY_ROOT="$2"; shift 2 ;;
    --dataset)
      DATASET="$2"; shift 2 ;;
    --groups)
      GROUP="$2"; shift 2 ;;
    --cottas-path)
      COTTAS_PATH="$2"; shift 2 ;;
    --vortex-ids-path)
      VORTEX_IDS_PATH="$2"; shift 2 ;;
    --vortex-strings-path)
      VORTEX_STRINGS_PATH="$2"; shift 2 ;;
    --warmup-runs)
      WARMUP_RUNS="$2"; shift 2 ;;
    --measured-runs)
      MEASURED_RUNS="$2"; shift 2 ;;
    --max-queries)
      MAX_QUERIES="$2"; shift 2 ;;
    --only-file-contains)
      ONLY_FILE_CONTAINS="$2"; shift 2 ;;
    --out-dir)
      OUT_DIR="$2"; shift 2 ;;
    --benchmark-timeout-seconds)
      BENCHMARK_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --timeout-kill-after-seconds)
      TIMEOUT_KILL_AFTER_SECONDS="$2"; shift 2 ;;
    --no-silence-stdout)
      NO_SILENCE_STDOUT="1"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

[[ -n "$QUERY_ROOT" ]] || die "--query-root is required"
[[ -n "$COTTAS_PATH" ]] || die "--cottas-path is required"
[[ -n "$VORTEX_IDS_PATH" ]] || die "--vortex-ids-path is required"
[[ -n "$VORTEX_STRINGS_PATH" ]] || die "--vortex-strings-path is required"

require_cmd "$PYTHON_BIN"
require_cmd timeout
require_file "$BENCHMARK_SCRIPT"
require_int "$WARMUP_RUNS"
require_int "$MEASURED_RUNS"
require_int "$BENCHMARK_TIMEOUT_SECONDS"
require_int "$TIMEOUT_KILL_AFTER_SECONDS"
[[ -z "$MAX_QUERIES" ]] || require_int "$MAX_QUERIES"

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="dbbench_runs/$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUT_DIR/logs" || die "Failed to create output directory: $OUT_DIR"
MANIFEST="$OUT_DIR/manifest.txt"

{
  echo "Started: $(date)"
  echo "PWD: $(pwd)"
  echo "Python: $PYTHON_BIN"
  echo "Benchmark script: $BENCHMARK_SCRIPT"
  echo "Dataset: $DATASET"
  echo "Group: $GROUP"
  echo "Query root: $QUERY_ROOT"
  echo "COTTAS path: $COTTAS_PATH"
  echo "Vortex IDs path: $VORTEX_IDS_PATH"
  echo "Vortex strings path: $VORTEX_STRINGS_PATH"
  echo "Warmup runs: $WARMUP_RUNS"
  echo "Measured runs: $MEASURED_RUNS"
  echo "Max queries: ${MAX_QUERIES:-}"
  echo "Only file contains: ${ONLY_FILE_CONTAINS:-}"
  echo "Out dir: $OUT_DIR"
  echo "Benchmark timeout seconds: $BENCHMARK_TIMEOUT_SECONDS"
  echo "Timeout kill-after seconds: $TIMEOUT_KILL_AFTER_SECONDS"
  echo "VORTEX_RDF_NATIVE_ID_LOOKUP_SINGLE_EQ_IDS: ${VORTEX_RDF_NATIVE_ID_LOOKUP_SINGLE_EQ_IDS}"
  echo
} | tee "$MANIFEST"

FAILED=0

run_config() {
  local LABEL="$1"
  local ENGINE="$2"
  local VORTEX_PATH="$3"
  local VORTEX_LAYOUT="$4"

  local PREFIX="${OUT_DIR}/${LABEL}"
  local LOG="${OUT_DIR}/logs/${LABEL}.log"
  local STATUS=0
  local START_EPOCH END_EPOCH ELAPSED

  local CMD=(
    "$PYTHON_BIN" "$BENCHMARK_SCRIPT"
    --query-root "$QUERY_ROOT"
    --dataset "$DATASET"
    --groups "$GROUP"
    --join-sizes "$JOIN_SIZE_SMALL" "$JOIN_SIZE_BIG"
    --cottas-path "$COTTAS_PATH"
    --vortex-path "$VORTEX_PATH"
    --vortex-layout "$VORTEX_LAYOUT"
    --engines "$ENGINE"
    --warmup-runs "$WARMUP_RUNS"
    --measured-runs "$MEASURED_RUNS"
    --out-prefix "$PREFIX"
  )

  [[ -n "$MAX_QUERIES" ]] && CMD+=(--max-queries "$MAX_QUERIES")
  [[ -n "$ONLY_FILE_CONTAINS" ]] && CMD+=(--only-file-contains "$ONLY_FILE_CONTAINS")
  [[ "$NO_SILENCE_STDOUT" == "1" ]] && CMD+=(--no-silence-stdout)

  {
    echo ""
    echo "LABEL:  $LABEL"
    echo "ENGINE: $ENGINE"
    echo "LAYOUT: $VORTEX_LAYOUT"
    echo "PATH:   $VORTEX_PATH"
    echo "START:  $(date)"
    echo "CMD:    ${CMD[*]}"
    if [[ "$BENCHMARK_TIMEOUT_SECONDS" -gt 0 ]]; then
      echo "OUTER TIMEOUT: ${BENCHMARK_TIMEOUT_SECONDS}s; kill-after=${TIMEOUT_KILL_AFTER_SECONDS}s"
    else
      echo "OUTER TIMEOUT: disabled"
    fi
    echo ""
  } | tee -a "$MANIFEST" "$LOG"

  START_EPOCH=$(date +%s)
  set +e
  if [[ "$BENCHMARK_TIMEOUT_SECONDS" -gt 0 ]]; then
    timeout --preserve-status --kill-after="${TIMEOUT_KILL_AFTER_SECONDS}s" "${BENCHMARK_TIMEOUT_SECONDS}s" \
      "${CMD[@]}" 2>&1 | tee -a "$LOG"
    STATUS=${PIPESTATUS[0]}
  else
    "${CMD[@]}" 2>&1 | tee -a "$LOG"
    STATUS=${PIPESTATUS[0]}
  fi
  set -e
  END_EPOCH=$(date +%s)
  ELAPSED=$((END_EPOCH - START_EPOCH))

  {
    echo ""
    echo "END:     $(date)"
    echo "ELAPSED: ${ELAPSED}s"
    echo "STATUS:  ${STATUS}"
    if [[ "$BENCHMARK_TIMEOUT_SECONDS" -gt 0 && "$STATUS" -eq 143 ]]; then
      echo "STATUS-MEANING: timeout sent SIGTERM after ${BENCHMARK_TIMEOUT_SECONDS}s"
    elif [[ "$BENCHMARK_TIMEOUT_SECONDS" -gt 0 && "$STATUS" -eq 137 ]]; then
      echo "STATUS-MEANING: timeout escalated to SIGKILL"
    elif [[ "$BENCHMARK_TIMEOUT_SECONDS" -gt 0 && "$STATUS" -eq 124 ]]; then
      echo "STATUS-MEANING: timeout expired"
    fi
    echo "------------------------------------------------------------"
  } | tee -a "$MANIFEST" "$LOG"

  if [[ "$STATUS" -ne 0 ]]; then
    FAILED=1
  fi

  # Continue with the next engine/layout even after timeout/failure.
  return 0
}

run_config "cottas" "cottas" "$VORTEX_STRINGS_PATH" "cottas-native-strings"
run_config "vortex_native_ids" "vortex" "$VORTEX_IDS_PATH" "cottas-native-ids"
run_config "vortex_native_strings" "vortex" "$VORTEX_STRINGS_PATH" "cottas-native-strings"

{
  echo "Finished: $(date)"
  if [[ "$FAILED" -ne 0 ]]; then
    echo "Overall status: FAILED - check $OUT_DIR/logs"
  else
    echo "Overall status: OK"
  fi
} | tee -a "$MANIFEST"

exit "$FAILED"
