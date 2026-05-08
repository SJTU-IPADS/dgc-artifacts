#!/bin/bash
# analyze-fig6-tradesoap.sh — extract tradesoap fig6 RT-curve metrics
#                             into plot/tradesoap-data/.
#
# Same pipeline as analyze-fig6-h2.sh but for the DayTrader (tradesoap)
# DaCapo benchmark. The dacapo parser allows the META workload field to be
# either `tradesoap` or `tradesoap-vlarge-640` etc., so this driver covers
# all conf variants of tradesoap on ds00 (default + vlarge-640 + big-final).

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
PLOT_DIR="${AE_DIR}/plot/tradesoap-data"
PARSER="${SCRIPT_DIR}/parse-fig6-dacapo.py"

GC_LIST="${GC_LIST:-g1 shenandoah dgc-shm dgc-rdma}"

declare -A CSV_NAME=(
    [g1]=g1gc-output.csv
    [shenandoah]=shenandoah-output.csv
    [dgc-shm]=shm-dgc-output.csv
    [dgc-rdma]=rdma-dgc-output.csv
)

# tradesoap.conf: baseline=10 cores, dgc=8 host + 4 client.
_tradesoap_args() {
    case "$1" in
        g1|shenandoah) echo "10 10 10" ;;
        dgc-shm|dgc-rdma) echo "8 8 8" ;;
        *) echo "0 0 0" ;;
    esac
}

mkdir -p "$PLOT_DIR"

for GC in $GC_LIST; do
    [ -n "${CSV_NAME[$GC]:-}" ] || { echo "[skip] unknown GC: $GC" >&2; continue; }
    # Match `tradesoap`, `tradesoap-vlarge-640`, etc.
    pattern="${RESULTS_BASE}/*_tradesoap*_${GC}"
    [ -n "$CLI_TAG" ] && pattern="${pattern}_${CLI_TAG}*" || pattern="${pattern}_*"
    # shellcheck disable=SC2206
    runs=( $(compgen -G "$pattern" || true) )
    if [ ${#runs[@]} -eq 0 ]; then
        echo "[warn] no runs match: $pattern" >&2
        continue
    fi
    read -r ccmt ccet pcore <<<"$(_tradesoap_args "$GC")"
    echo "[run] tradesoap/$GC: ${#runs[@]} run(s) → ${PLOT_DIR}/${CSV_NAME[$GC]}"
    python3 "$PARSER" \
        tradesoap "$GC" 2.0 "$ccmt" "$ccet" "$pcore" 2 \
        "${PLOT_DIR}/${CSV_NAME[$GC]}" \
        "${runs[@]}"
done
