#!/bin/bash
# analyze-fig6-all.sh — one-shot driver for all fig6 RT-curve analyses.
#
# Runs the per-benchmark analyzers in sequence (h2 / tradesoap / hbase) so
# that after a fig6 sweep finishes, a single command repopulates every
# CSV under plot/{h2,tradesoap,hbase-*}-data/ that the fig6 plot scripts
# read.
#
# Usage:
#   ./analyze-fig6-all.sh                        # all benchmarks, all runs
#   ./analyze-fig6-all.sh --tag fig6-hbase-20260427_120000   # specific tag
#   BENCH_LIST="h2 tradesoap" ./analyze-fig6-all.sh          # subset
#   GC_LIST="dgc-shm dgc-rdma" ./analyze-fig6-all.sh         # subset of GCs
#
# Environment passthroughs (forwarded to every sub-analyzer):
#   RESULTS_BASE, GC_LIST
#
# Exit code is 0 even if some sub-analyses produce empty CSVs (a missing
# GC variant is logged with [warn] but doesn't fail the whole run).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CLI_TAG="${TAG:-}"
BENCH_LIST="${BENCH_LIST:-h2 tradesoap hbase}"
while [ $# -gt 0 ]; do
    case "$1" in
        --tag)   CLI_TAG="$2"; shift 2 ;;
        --bench) BENCH_LIST="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# Build the arg list once so all four sub-analyzers receive the same tag.
sub_args=()
[ -n "$CLI_TAG" ] && sub_args+=( --tag "$CLI_TAG" )

echo "==========================================="
echo "  fig6 analysis driver"
echo "  Benchmarks: $BENCH_LIST"
echo "  Tag filter: ${CLI_TAG:-<none>}"
echo "  GC filter:  ${GC_LIST:-<all>}"
echo "  Results:    ${RESULTS_BASE:-<artifact>/results/$USER/fig6-result}"
echo "==========================================="

for B in $BENCH_LIST; do
    case "$B" in
        h2)
            "${SCRIPT_DIR}/analyze-fig6-h2.sh" "${sub_args[@]}" || true
            ;;
        tradesoap)
            "${SCRIPT_DIR}/analyze-fig6-tradesoap.sh" "${sub_args[@]}" || true
            ;;
        hbase)
            "${SCRIPT_DIR}/analyze-fig6-hbase.sh" "${sub_args[@]}" || true
            ;;
        *)
            echo "[skip] unknown bench: $B" >&2
            ;;
    esac
done

echo "==========================================="
echo "  fig6 analysis driver finished at $(date)"
echo "==========================================="
