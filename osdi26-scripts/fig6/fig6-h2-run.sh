#!/bin/bash
# fig6-h2-run.sh — Figure 6: H2 TPC-C RT-curve test
#
# Runs the full fig6 H2 sweep in one shot:
#   3 GCs (shenandoah, dgc-shm, dgc-rdma)
#   × 5 throttle rates (200..1000 step 200, matches ds03 archive)
#   × 3 reps
#   = 9 ./run.sh invocations × 5 throttles each = 45 test points
#
# h2.conf already pins:
#   MIN_HEAP=677, HEAP_MULTIPLIER=2.0  → actual heap = 1354 MB (2× min)
#   ITERATIONS=5
#   TERMINAL_NUM=8
#   HOST_COUNT=2 (default dual-homog)
#
# Throttles (200..1000) override h2.conf default (200..1000 with extra points
# we don't need for fig6) — matches osdi26-test-archive fig6-h2-rt-curve-test.
#
# Usage:
#   ./osdi26-scripts/fig6/fig6-h2-run.sh                  # run all 9 combos
#   ./osdi26-scripts/fig6/fig6-h2-run.sh "shenandoah"     # one GC, all reps
#   GC_LIST="dgc-rdma" REPS="1" ./osdi26-scripts/fig6/fig6-h2-run.sh
#
# Environment overrides:
#   RESULTS_BASE   override (default ${AE_DIR}/results/${USER}/fig6-result)
#   GC_LIST        space-separated GCs (default "shenandoah dgc-shm dgc-rdma")
#   REPS           space-separated rep ids (default "1 2 3")
#   THROTTLES      space-separated throttles (default "200 400 600 800 1000")
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
THROTTLES="${THROTTLES:-200 400 600 800 1000}"

TAG="fig6-h2-$(date +%Y%m%d_%H%M%S)"

echo "==============================================="
echo "  fig6 H2 sweep starting at $(date)"
echo "  Tag:        ${TAG}"
echo "  GCs:        ${GC_LIST}"
echo "  Reps:       ${REPS}"
echo "  Throttles:  ${THROTTLES}"
echo "  Results:    ${RESULTS_BASE}/<run-id>"
echo "==============================================="

for REP in $REPS; do
    for GC in $GC_LIST; do
        echo ""
        echo "----- [$(date +%H:%M:%S)] REP=${REP}  h2  ${GC} -----"
        ./run.sh h2 "$GC" --tag "${TAG}-rep${REP}" --throttle "$THROTTLES"
    done
done

echo ""
echo "==============================================="
echo "  fig6 H2 sweep finished at $(date)"
echo "  Tag: ${TAG}"
echo "==============================================="
