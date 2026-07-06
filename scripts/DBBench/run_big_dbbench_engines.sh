#!/usr/bin/env bash
set -eo pipefail

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
  [[ "$1" =~ ^[0-9]+$ ]] || die "Expected integer, got: $1"
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
    --no-silence-stdout)
      NO_SILENCE_STDOUT="1"; shift ;;
    -h|--help)
      echo "Usage:"
      echo "$0 --query-root PATH --cottas-path PATH --vortex-ids-path PATH --vortex-strings-path PATH [--max-queries N] [--out-dir DIR]"
      exit 0 ;;
    *)
      die "Unknown argument: $1" ;;
  esac
done

# --- Required args ---
[[ -n "$QUERY_ROOT" ]] || die "--query-root is required"
[[ -n "$COTTAS_PATH" ]] || die "--cottas-path is required"
[[ -n "$VORTEX_IDS_PATH" ]] || die "--vortex-ids-path is required"
[[ -n "$VORTEX_STRINGS_PATH" ]] || die "--vortex-strings-path is required"

# --- Validate environment ---
require_cmd "$PYTHON_BIN"
require_file "$BENCHMARK_SCRIPT"

require_int "$WARMUP_RUNS"
require_int "$MEASURED_RUNS"
[[ -z "$MAX_QUERIES" ]] || require_int "$MAX_QUERIES"

# --- Output dir ---
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="dbbench_runs/${DATASET}_big_$(date +%Y%m%d_%H%M%S)"
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
  echo "Max queries: ${MAX_QUERIES:-<none>}"
  echo "Only file contains: ${ONLY_FILE_CONTAINS:-<none>}"
  echo "Out dir: $OUT_DIR"
  echo
} | tee "$MANIFEST"

FAILED=0

run_config() {
  local LABEL="$1"
  local ENGINE="$2"
  local VORTEX_PATH="$3"
  local VORTEX_LAYOUT="$4"

  local PREFIX="$OUT_DIR/${DATASET}_${LABEL}"
  local LOG="$OUT_DIR/logs/${LABEL}.log"

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
    echo "============================================================"
    echo "LABEL:  $LABEL"
    echo "ENGINE: $ENGINE"
    echo "LAYOUT: $VORTEX_LAYOUT"
    echo "PATH:   $VORTEX_PATH"
    echo "START:  $(date)"
    echo "CMD:    ${CMD[*]}"
    echo "============================================================"
  } | tee -a "$MANIFEST" "$LOG"

  set +e
  "${CMD[@]}" | tee -a "$LOG"
  local STATUS=$?
  set -e

    {
    echo "END:    $(date)"
    echo "STATUS: $STATUS"
    echo
  } | tee -a "$MANIFEST" "$LOG"

  if [[ "$STATUS" -ne 0 ]]; then
    FAILED=1
  fi

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