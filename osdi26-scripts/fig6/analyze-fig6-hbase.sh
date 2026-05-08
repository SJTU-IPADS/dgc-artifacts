#!/bin/bash
# analyze-fig6-hbase.sh — extract HBase fig6 RT-curve metrics into
#                        plot/hbase-{workloada,read-insert-half-workload}-data/.
#
# Drives parse-fig6-hbase.py for both YCSB workloads (workloada / readinsert)
# across all four GC variants. Plot directory names match what
# osdi26{,_plot_hbase_h2_tradesoap_combined}-fig6-plot-rt-curve.py expects.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLI_TAG="${TAG:-}"
WORKLOAD_LIST="${WORKLOAD_LIST:-hbase-workloada hbase-readinsert}"
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        --workload) WORKLOAD_LIST="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig6-result}"
PARSER="${SCRIPT_DIR}/parse-fig6-hbase.py"

GC_LIST="${GC_LIST:-g1 shenandoah dgc-shm dgc-rdma}"

# GC stem in CSV filename (e.g. "shenandoah" → "shenandoah-<suffix>-output.csv").
# fig6-plot.py builds paths as: f'./{workload_name}-data/{stem}-{suffix}-output.csv'
declare -A GC_STEM=(
    [g1]=g1gc
    [shenandoah]=shenandoah
    [dgc-shm]=shm-dgc
    [dgc-rdma]=rdma-dgc
)

# plot/<dir>/ + filename suffix per workload — matches fig6-plot.py:
#   hbase-workloada  → ./hbase-workloada-data/<stem>-workloada_2host-output.csv
#   hbase-readinsert → ./hbase-read-insert-half-workload-data/
#                        <stem>-read_insert_half_workload-output.csv
_plot_dir_for() {
    case "$1" in
        hbase-workloada) echo "${AE_DIR}/plot/hbase-workloada-data" ;;
        hbase-readinsert) echo "${AE_DIR}/plot/hbase-read-insert-half-workload-data" ;;
        *) echo "" ;;
    esac
}
_csv_suffix_for() {
    case "$1" in
        hbase-workloada) echo "workloada_2host" ;;
        hbase-readinsert) echo "read_insert_half_workload" ;;
        *) echo "" ;;
    esac
}

# hbase-{workloada,readinsert}.conf: baseline=10 cores, dgc=4 (mark) / 8 (proc).
_hbase_args() {
    case "$1" in
        g1|shenandoah) echo "10 10 10" ;;
        dgc-shm|dgc-rdma) echo "4 4 8" ;;
        *) echo "0 0 0" ;;
    esac
}

# Loop time / iteration count column. HBase has no warmup re-run; use 1.
LOOP_TIME=1

for WORKLOAD in $WORKLOAD_LIST; do
    PLOT_DIR=$(_plot_dir_for "$WORKLOAD")
    SUFFIX=$(_csv_suffix_for "$WORKLOAD")
    if [ -z "$PLOT_DIR" ] || [ -z "$SUFFIX" ]; then
        echo "[skip] unknown workload: $WORKLOAD" >&2
        continue
    fi
    mkdir -p "$PLOT_DIR"
    for GC in $GC_LIST; do
        STEM="${GC_STEM[$GC]:-}"
        [ -n "$STEM" ] || { echo "[skip] unknown GC: $GC" >&2; continue; }
        pattern="${RESULTS_BASE}/*_${WORKLOAD}_${GC}"
        [ -n "$CLI_TAG" ] && pattern="${pattern}_${CLI_TAG}*" || pattern="${pattern}_*"
        # shellcheck disable=SC2206
        runs=( $(compgen -G "$pattern" || true) )
        if [ ${#runs[@]} -eq 0 ]; then
            echo "[warn] no runs match: $pattern" >&2
            continue
        fi
        read -r ccmt ccet pcore <<<"$(_hbase_args "$GC")"
        # Canonical name (fig6-plot.py): <stem>-<suffix>-output.csv.
        canonical_csv="${PLOT_DIR}/${STEM}-${SUFFIX}-output.csv"
        echo "[run] $WORKLOAD/$GC: ${#runs[@]} run(s) → ${canonical_csv}"
        python3 "$PARSER" \
            "$WORKLOAD" "$GC" 2.0 "$ccmt" "$ccet" "$pcore" "$LOOP_TIME" \
            "${canonical_csv}" \
            "${runs[@]}"
        # Compatibility alias for the older combined plotter
        # (osdi26_plot_hbase_h2_tradesoap_combined_rt_curve.py), which reads
        # <stem>-output.csv with no workload suffix.
        cp -f "${canonical_csv}" "${PLOT_DIR}/${STEM}-output.csv"
    done
done
