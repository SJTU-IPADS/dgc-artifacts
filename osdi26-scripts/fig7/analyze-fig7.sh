#!/bin/bash
# analyze-fig7.sh — extract fig7 plotting CSVs from a fig7 PRESET run set.
#
# Outputs (under plot/fig7-data/):
#   preset.csv           per-run rows
#   preset.agg.csv       mean ± 95% per (gc, cache_size_mb)
#   p99.csv              type,p99_ms          (consumed by fig7-plot.py)
#   memory_usage.csv     type,memory_gb       (consumed by fig7-plot.py)
#   rdma_transport.csv   type,rdma_gb         (consumed by fig7-plot.py)
#
# Usage:
#   ./analyze-fig7.sh                                 # all fig7-* runs
#   ./analyze-fig7.sh --tag fig7-20260427_180000      # specific tag base
#
# Environment:
#   RESULTS_BASE   override (default ${AE_DIR}/results/${USER}/fig7-result)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLI_TAG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig7-result}"
PLOT_DIR="${AE_DIR}/plot/fig7-data"
PARSER="${SCRIPT_DIR}/parse-fig7-preset.py"

if [ -n "$CLI_TAG" ]; then
    pattern="${RESULTS_BASE}/*_specjbb-preset_*_${CLI_TAG}*"
else
    pattern="${RESULTS_BASE}/*_specjbb-preset_*_fig7-*"
fi
# shellcheck disable=SC2206
runs=( $(compgen -G "$pattern" || true) )
if [ ${#runs[@]} -eq 0 ]; then
    echo "[err] no run dirs match: $pattern" >&2
    exit 1
fi

echo "[run] analysing ${#runs[@]} fig7 runs → ${PLOT_DIR}/"
mkdir -p "$PLOT_DIR"
python3 "$PARSER" "$PLOT_DIR" "${runs[@]}"

echo ""
echo "Now plot:  python3 ${AE_DIR}/plot/fig7-plot.py"
