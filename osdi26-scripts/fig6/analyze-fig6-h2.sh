#!/bin/bash
# analyze-fig6-h2.sh — extract H2 fig6 RT-curve metrics into plot/h2-data/.
#
# For each GC variant (g1, shenandoah, dgc-shm, dgc-rdma):
#   1. Glob ${RESULTS_BASE}/*_h2_<gc>_*  (filtered by --tag if provided)
#   2. Run parse-fig6-dacapo.py to aggregate the runs into one CSV
#   3. Drop the CSV at plot/h2-data/<gc>-output.csv
#
# Default RESULTS_BASE matches fig6-h2-run.sh:
#   ${AE_DIR}/results/${USER}/fig6-result
#
# Usage:
#   ./analyze-fig6-h2.sh                    # use all matching runs
#   ./analyze-fig6-h2.sh --tag fig6-h2-20260427_120000   # only this tag
#   GC_LIST="dgc-shm dgc-rdma" ./analyze-fig6-h2.sh      # subset of GCs
#   RESULTS_BASE=/path/to/results ./analyze-fig6-h2.sh   # custom base

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLI_TAG="${TAG:-}"
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig6-result}"
PLOT_DIR="${AE_DIR}/plot/h2-data"
PARSER="${SCRIPT_DIR}/parse-fig6-dacapo.py"

GC_LIST="${GC_LIST:-g1 shenandoah dgc-shm dgc-rdma}"

# CSV filename per GC (matches existing plot/h2-data/*.csv).
declare -A CSV_NAME=(
    [g1]=g1gc-output.csv
    [shenandoah]=shenandoah-output.csv
    [dgc-shm]=shm-dgc-output.csv
    [dgc-rdma]=rdma-dgc-output.csv
)

# ccmt/ccet/pcore values per GC (from conf/workloads/h2.conf).
# Baseline (g1, shenandoah) uses 12 cores; DGC variants use 8.
_h2_args() {
    case "$1" in
        g1|shenandoah) echo "12 12 12" ;;
        dgc-shm|dgc-rdma) echo "8 8 8" ;;
        *) echo "0 0 0" ;;
    esac
}

mkdir -p "$PLOT_DIR"

for GC in $GC_LIST; do
    [ -n "${CSV_NAME[$GC]:-}" ] || { echo "[skip] unknown GC: $GC" >&2; continue; }
    pattern="${RESULTS_BASE}/*_h2_${GC}"
    [ -n "$CLI_TAG" ] && pattern="${pattern}_${CLI_TAG}*" || pattern="${pattern}_*"
    # shellcheck disable=SC2206
    runs=( $(compgen -G "$pattern" || true) )
    if [ ${#runs[@]} -eq 0 ]; then
        echo "[warn] no runs match: $pattern" >&2
        continue
    fi
    read -r ccmt ccet pcore <<<"$(_h2_args "$GC")"
    echo "[run] h2/$GC: ${#runs[@]} run(s) → ${PLOT_DIR}/${CSV_NAME[$GC]}"
    python3 "$PARSER" \
        h2 "$GC" 2.0 "$ccmt" "$ccet" "$pcore" 2 \
        "${PLOT_DIR}/${CSV_NAME[$GC]}" \
        "${runs[@]}"
done
