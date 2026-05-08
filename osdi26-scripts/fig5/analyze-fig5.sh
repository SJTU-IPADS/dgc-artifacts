#!/bin/bash
# analyze-fig5.sh — extract fig5 plotting CSVs from the latest fig5 run.
#
# Pipeline:
#   1. Find a matching run dir under ${RESULTS_BASE} (filterable by --tag,
#      --run-dir, or --rep). Defaults to the most recent fig5 run.
#   2. Call parse-fig5-latency.py to emit windows.csv / gc.csv / meta.csv
#      under plot/fig5-data/.
#
# After this finishes, run plot/fig5-plot.py to render the figure.
#
# Usage:
#   ./analyze-fig5.sh                              # newest fig5-* run
#   ./analyze-fig5.sh --tag fig5-20260427_153000   # specific tag
#   ./analyze-fig5.sh --run-dir /path/to/run       # explicit run dir
#   ./analyze-fig5.sh --rep 2                      # fig5 rep #2
#
# Environment:
#   RESULTS_BASE   override results base (default: ${AE_DIR}/results/${USER}/fig5-result)
#   GID           backend group id to analyse (default 0)
#   FIG5_IR       target IR for steady-state detection (default 6819)
#   FIG5_PLOT_START_IDX / FIG5_PLOT_END_IDX
#                 cycle index window to plot (default 0..10)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLI_TAG=""
CLI_RUN_DIR=""
CLI_REP=""
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        --run-dir) CLI_RUN_DIR="$2"; shift 2 ;;
        --rep) CLI_REP="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig5-result}"
PLOT_DIR="${AE_DIR}/plot/fig5-data"
PARSER="${SCRIPT_DIR}/parse-fig5-latency.py"
GID="${GID:-0}"

if [ -n "$CLI_RUN_DIR" ]; then
    RUN_DIR="$CLI_RUN_DIR"
else
    pattern="${RESULTS_BASE}/*_specjbb-preset_dgc-shm"
    if [ -n "$CLI_TAG" ]; then
        pattern="${pattern}_${CLI_TAG}"
    else
        pattern="${pattern}_fig5-*"
    fi
    if [ -n "$CLI_REP" ]; then
        pattern="${pattern}-rep${CLI_REP}*"
    else
        pattern="${pattern}*"
    fi
    # shellcheck disable=SC2206
    candidates=( $(compgen -G "$pattern" || true) )
    if [ ${#candidates[@]} -eq 0 ]; then
        echo "[err] no run dir matches: $pattern" >&2
        exit 1
    fi
    # Pick newest by mtime.
    RUN_DIR=$(ls -dt "${candidates[@]}" | head -1)
fi

if [ ! -d "$RUN_DIR" ]; then
    echo "[err] run dir not found: $RUN_DIR" >&2
    exit 1
fi

echo "[run] analysing: $(basename "$RUN_DIR")"
echo "[run] backend gid=${GID}, output → ${PLOT_DIR}"
mkdir -p "$PLOT_DIR"
python3 "$PARSER" "$RUN_DIR" "$PLOT_DIR" "$GID"

echo ""
echo "Now plot:  python3 ${AE_DIR}/plot/fig5-plot.py"
