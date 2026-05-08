#!/bin/bash
# fig6-hbase-run.sh — Figure 6: HBase RT-curve test
#
# Runs the full fig6 HBase sweep in one shot:
#   3 GCs (shenandoah, dgc-shm, dgc-rdma)
#   × 2 workloads (hbase-workloada, hbase-readinsert)
#   × 10 throttle rates (4500..45000 step 4500, baked into the workload confs)
#   × 3 reps
#   = 18 ./run.sh invocations × 10 IR rates each = 180 test points
#
# Each workload conf already pins:
#   HEAP=3072m   (= 2× min-heap label "2.0")
#   HOST_NUM=2   (two HBase region servers)
#   YCSB_THREADS=60, WARMUP=30s, RUN=60s
#   IR_RATES="4500 9000 13500 18000 22500 27000 31500 36000 40500 45000"
#
# Usage:
#   ./osdi26-scripts/fig6/fig6-hbase-run.sh                  # run all 18 combos
#   ./osdi26-scripts/fig6/fig6-hbase-run.sh "shenandoah"     # only shenandoah, both workloads
#   GC_LIST="dgc-rdma" REPS="1" ./osdi26-scripts/fig6/fig6-hbase-run.sh   # one rep, one GC
#
# Environment overrides:
#   RESULTS_BASE   override (default ${AE_DIR}/results/${USER}/fig6-result)
#   GC_LIST        space-separated GCs (default "shenandoah dgc-shm dgc-rdma")
#   WORKLOAD_LIST  space-separated workloads (default "hbase-workloada hbase-readinsert")
#   REPS           space-separated rep ids (default "1 2 3")
#

set -euo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."
AE_DIR="$(pwd)"

# Default results destination = per-user subdir under the artifact's results/.
export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig6-result}"
mkdir -p "$RESULTS_BASE"

GC_LIST="${GC_LIST:-${1:-shenandoah dgc-shm dgc-rdma}}"
WORKLOAD_LIST="${WORKLOAD_LIST:-hbase-workloada hbase-readinsert}"
REPS="${REPS:-1 2 3}"
# Optional IR-rate override forwarded as --throttle to run.sh. Lets a
# time-constrained sweep cut the default 10-point IR list down. Empty leaves
# the conf default in place.
IR_RATES_OVERRIDE="${IR_RATES_OVERRIDE:-}"

TAG="fig6-hbase-$(date +%Y%m%d_%H%M%S)"

THROTTLE_ARGS=()
if [ -n "$IR_RATES_OVERRIDE" ]; then
    THROTTLE_ARGS=(--throttle "$IR_RATES_OVERRIDE")
fi

echo "==============================================="
echo "  fig6 HBase sweep starting at $(date)"
echo "  Tag:        ${TAG}"
echo "  GCs:        ${GC_LIST}"
echo "  Workloads:  ${WORKLOAD_LIST}"
echo "  Reps:       ${REPS}"
echo "  Results:    ${RESULTS_BASE}/<run-id>"
echo "==============================================="

for REP in $REPS; do
    for WORKLOAD in $WORKLOAD_LIST; do
        for GC in $GC_LIST; do
            echo ""
            echo "----- [$(date +%H:%M:%S)] REP=${REP}  ${WORKLOAD}  ${GC} -----"
            ./run.sh "$WORKLOAD" "$GC" --tag "${TAG}-rep${REP}" "${THROTTLE_ARGS[@]}"
        done
    done
done

echo ""
echo "==============================================="
echo "  fig6 HBase sweep finished at $(date)"
echo "  Tag: ${TAG}"
echo "==============================================="
