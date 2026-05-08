#!/bin/bash
# fig6-tradesoap-run.sh — Figure 6: DayTrader (DaCapo tradesoap) RT-curve test
#
# Runs the full fig6 tradesoap-vlarge sweep in one shot:
#   3 GCs (shenandoah, dgc-shm, dgc-rdma)
#   × 5 throttle rates (1000..5000 step 1000, baked into vlarge-640.conf)
#   × 3 reps
#   = 9 ./run.sh invocations × 5 throttles each = 45 test points
#
# tradesoap-vlarge-640.conf pins:
#   DACAPO_SIZE="vlarge"  (s=13, 8192 sessions — same data scale as ds03 archive)
#   MIN_HEAP=640, HEAP_MULTIPLIER=2.0  → Xmx=1280m (2× minheap)
#   HOST_COUNT=2 (relies on the (hid+1)*100 port-offset shift in lib/workload-dacapo.sh)
#   ITERATIONS=5, TERMINAL_NUM=4
#   THROTTLES="1000 2000 3000 4000 5000"
#   SNIC_RDMA_BATCH_FETCHKLASS_dgc=true (workaround for mlx5 fw 32.42.1000 LOC_QP_OP_ERR)
#
# Usage:
#   ./osdi26-scripts/fig6/fig6-tradesoap-run.sh                  # run all 9 combos
#   ./osdi26-scripts/fig6/fig6-tradesoap-run.sh "shenandoah"     # one GC, all reps
#   GC_LIST="dgc-rdma" REPS="1" ./osdi26-scripts/fig6/fig6-tradesoap-run.sh
#
# Environment overrides:
#   RESULTS_BASE   override (default ${AE_DIR}/results/${USER}/fig6-result)
#   GC_LIST        space-separated GCs (default "shenandoah dgc-shm dgc-rdma")
#   REPS           space-separated rep ids (default "1 2 3")
#   THROTTLES      override conf throttles (default uses conf value)
#   WORKLOAD       conf variant (default "tradesoap-vlarge-640"; alternates:
#                  "tradesoap-big-final" for big16 single-host setup)
#

set -euo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."
AE_DIR="$(pwd)"

# Default results destination = per-user subdir under the artifact's results/.
export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig6-result}"
mkdir -p "$RESULTS_BASE"

GC_LIST="${GC_LIST:-${1:-shenandoah dgc-shm dgc-rdma}}"
REPS="${REPS:-1 2 3}"
WORKLOAD="${WORKLOAD:-tradesoap-vlarge-640}"

TAG="fig6-tradesoap-$(date +%Y%m%d_%H%M%S)"

# Build the optional --throttle CLI override only if user supplied one
THROTTLE_ARGS=()
if [ -n "${THROTTLES:-}" ]; then
    THROTTLE_ARGS=(--throttle "$THROTTLES")
fi

echo "==============================================="
echo "  fig6 tradesoap sweep starting at $(date)"
echo "  Tag:        ${TAG}"
echo "  Workload:   ${WORKLOAD}"
echo "  GCs:        ${GC_LIST}"
echo "  Reps:       ${REPS}"
echo "  Throttles:  ${THROTTLES:-<from conf>}"
echo "  Results:    ${RESULTS_BASE}/<run-id>"
echo "==============================================="

for REP in $REPS; do
    for GC in $GC_LIST; do
        echo ""
        echo "----- [$(date +%H:%M:%S)] REP=${REP}  ${WORKLOAD}  ${GC} -----"
        ./run.sh "$WORKLOAD" "$GC" --tag "${TAG}-rep${REP}" "${THROTTLE_ARGS[@]}"
    done
done

echo ""
echo "==============================================="
echo "  fig6 tradesoap sweep finished at $(date)"
echo "  Tag: ${TAG}"
echo "==============================================="
