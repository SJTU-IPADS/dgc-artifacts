#!/bin/bash
# fig4-run.sh — Figure 4: SPECjbb HBIR_RT 2-host across 4 GC configs.
#
# Drives the unified `./run.sh` framework with workload=specjbb-hbir
# (SPECJBB_GROUPS=2 = 2 backends/hosts) for each of:
#   g1            G1GC baseline
#   shenandoah    Shenandoah baseline
#   dgc-shm       DGC + shared memory transport
#   dgc-rdma      DGC + RDMA transport
#
# HBIR_RT auto-ramps load until the controller finds the high-bound IR (HBIR)
# and then builds the response-time curve. Each GC run takes 30+ minutes.
#
# Usage:
#   ./osdi26-scripts/fig4/fig4-run.sh                         # all 4 GCs
#   ./osdi26-scripts/fig4/fig4-run.sh g1 shenandoah           # subset
#   GC_LIST="dgc-shm dgc-rdma" ./osdi26-scripts/fig4/fig4-run.sh
#
# Pass-through args (forwarded to ./run.sh after `specjbb-hbir <gc>`):
#   --profile <name>       use env.d/profiles/<name>.env
#   --jdk <path>           override JDK path
#   --tag <suffix>         add tag suffix to result-dir name
#   --dry-run              print config and skip
#
# Results land in osdi26-ae/results/adhoc/<run_id>/ as usual.

set -uo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."
AE_DIR="$(pwd)"

# Route results to a dedicated fig4 dir under the per-user results root
# (override env.sh's default of ${AE_DIR}/results/${USER}/adhoc). Each
# ./run.sh invocation reads RESULTS_BASE at framework init and writes
# ${RESULTS_BASE}/${RUN_ID}/.
export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig4-result}"
mkdir -p "$RESULTS_BASE"

# Default GC list and split into args vs forwarded options.
declare -a CLI_GC=()
declare -a FWD_ARGS=()
seen_dashdash=0
while [ $# -gt 0 ]; do
    if [ "$seen_dashdash" -eq 0 ] && [[ "$1" != -* ]]; then
        CLI_GC+=("$1")
    else
        seen_dashdash=1
        FWD_ARGS+=("$1")
    fi
    shift
done

if [ ${#CLI_GC[@]} -gt 0 ]; then
    GC_LIST="${CLI_GC[*]}"
else
    GC_LIST="${GC_LIST:-g1 shenandoah dgc-shm dgc-rdma}"
fi

TAG="fig4-$(date +%Y%m%d_%H%M%S)"

echo "==============================================="
echo "  Figure 4 SPECjbb HBIR_RT 2-host run"
echo "  Started: $(date)"
echo "  Tag:     ${TAG}"
echo "  GCs:     ${GC_LIST}"
[ ${#FWD_ARGS[@]} -gt 0 ] && echo "  Extra args: ${FWD_ARGS[*]}"
echo "  Results: ${RESULTS_BASE}/<run_id>/"
echo "==============================================="

overall_rc=0
for GC in $GC_LIST; do
    case "$GC" in
        g1|g1gc)            gc_arg="g1" ;;
        shenandoah)         gc_arg="shenandoah" ;;
        dgc-shm|shm-dgc)    gc_arg="dgc-shm" ;;
        dgc-rdma|rdma-dgc)  gc_arg="dgc-rdma" ;;
        *)
            echo "!!! [skip $GC] unknown GC; expected g1/shenandoah/dgc-shm/dgc-rdma"
            overall_rc=1
            continue
            ;;
    esac

    echo ""
    echo "----- [$(date +%H:%M:%S)] Fig4 / specjbb-hbir / ${gc_arg} -----"
    bash "${AE_DIR}/run.sh" specjbb-hbir "$gc_arg" --tag "${TAG}-${gc_arg}" "${FWD_ARGS[@]}"
    rc=$?
    echo "----- [$(date +%H:%M:%S)] specjbb-hbir/${gc_arg} exit=${rc} -----"
    [ $rc -ne 0 ] && overall_rc=1
done

echo ""
echo "==============================================="
echo "  Figure 4 run finished at $(date)"
echo "  Tag:    ${TAG}"
echo "  Status: $([ $overall_rc -eq 0 ] && echo OK || echo "some failed/skipped")"
echo "  Browse: ls ${RESULTS_BASE}/ | grep ${TAG}"
echo "==============================================="
exit $overall_rc
