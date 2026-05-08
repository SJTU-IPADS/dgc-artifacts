#!/bin/bash
# table3-run.sh — Table 3: SPECjbb+HBase mix workload across 3 GC configs
#
# Drives the inner scripts under osdi26-scripts/table3/inner/
# end-to-end. By default runs all three configurations sequentially:
#   shenandoah  — vanilla Shenandoah baseline (paper-fair 10-core budget)
#   dgc-shm     — DGC with shared-memory transport
#   dgc-rdma    — DGC with RDMA transport
# Each runs the same workload: 2 SPECjbb backends + 2 HBase regionservers
# under YCSB load.
#
# After the run completes, point the analyzer at the result tag:
#   ./osdi26-scripts/table3/table3-analyze.sh <run_tag>   # or last run
#
# Usage:
#   ./osdi26-scripts/table3/table3-run.sh                          # all 3 GCs
#   ./osdi26-scripts/table3/table3-run.sh shenandoah               # only shenandoah
#   ./osdi26-scripts/table3/table3-run.sh dgc-rdma                 # only dgc-rdma
#   GC_LIST="shenandoah dgc-shm" ./osdi26-scripts/table3/table3-run.sh
#
# Environment overrides (forwarded to inner scripts):
#   DEPS_BASE      base dir for runtime deps -- expects sibling layout
#                  (default ${AE_DIR}/.., where the prebuilt JDK and benchmark
#                  trees live next to this artifact)
#   NODE_IP        machine NIC IP to embed in SnicCoorHostAddrPortList /
#                  SNICAddr / HostAddr. Default 127.0.0.1 (works for SHM).
#                  For RDMA, set to the IB-attached interface IP via env or
#                  env.d/$(hostname -s).env.
#   CDS_JSA        Shenandoah CDS archive (default $JAVA_HOME/lib/server/classes.jsa)
#   LOG_PATH       result root; per-GC log dirs are created beneath it
#                  (default $RESULTS_BASE/$TAG)
#   RESULTS_BASE   override result root parent
#                  (default ${AE_DIR}/results/${USER}/table3-result)
#

set -uo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."

AE_DIR="$(pwd)"
INNER_DIR="${AE_DIR}/osdi26-scripts/table3/inner"

GC_LIST="${GC_LIST:-${1:-shenandoah dgc-shm dgc-rdma}}"
TAG="table3-$(date +%Y%m%d_%H%M%S)"

export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/table3-result}"
export LOG_PATH="${LOG_PATH:-${RESULTS_BASE}/${TAG}}"
export DEPS_BASE="${DEPS_BASE:-${AE_DIR}/..}"
export NODE_IP="${NODE_IP:-${DGC_HOST_ADDR:-${DGC_ADDR:-127.0.0.1}}}"
# Export DGC_JDK + JAVA_HOME so the HBase scripts (hbase-env.sh) and SPECjbb
# launchers all converge on the same prebuilt image. hbase-env.sh defaults to
# /data2/${USER}/jdk17-snic-gc/build/... when JAVA_HOME isn't set, which is
# meaningless under our shared /data2/dgc-ae layout.
export DGC_JDK="${DGC_JDK:-${AE_DIR}/../jdk17-snic-gc-prebuilt/jdk}"
export JAVA_HOME="${JAVA_HOME:-${DGC_JDK}}"

mkdir -p "$LOG_PATH"

echo "==============================================="
echo "  Table 3 SPECjbb+HBase mix run starting at $(date)"
echo "  Tag:      ${TAG}"
echo "  GCs:      ${GC_LIST}"
echo "  DEPS_BASE: ${DEPS_BASE}"
echo "  NODE_IP:  ${NODE_IP}"
echo "  Results:  ${LOG_PATH}/<gc>"
echo "==============================================="

# Sanity: the SHM HBase install must already exist under DEPS_BASE
shm_hbase="${DEPS_BASE}/hbase-test/multi-regionserver-hbase-0"
rdma_hbase="${DEPS_BASE}/hbase-test/rdma-multi-regionserver-hbase"

run_one() {
    local gc="$1"
    local script
    case "$gc" in
        dgc-shm)
            script="${INNER_DIR}/table3-dgc-shm-specjbb-hbase-mix-run.sh"
            if [ ! -d "$shm_hbase" ]; then
                echo "!!! [skip dgc-shm] missing $shm_hbase"
                echo "!!! Place the SHM HBase install at ${shm_hbase} (see README.md section 3.3)"
                return 2
            fi
            ;;
        dgc-rdma)
            script="${INNER_DIR}/table3-dgc-rdma-specjbb-hbase-mix-run.sh"
            if [ ! -d "$rdma_hbase" ]; then
                echo "!!! [skip dgc-rdma] missing $rdma_hbase"
                return 2
            fi
            ;;
        shenandoah)
            script="${INNER_DIR}/table3-shenandoah-specjbb-hbase-mix-run.sh"
            if [ ! -d "$shm_hbase" ]; then
                echo "!!! [skip shenandoah] missing $shm_hbase"
                return 2
            fi
            ;;
        *)
            echo "!!! [skip $gc] unknown GC; expected dgc-shm, dgc-rdma, or shenandoah"
            return 2
            ;;
    esac

    local gc_log_path="${LOG_PATH}/${gc}"
    mkdir -p "$gc_log_path"
    echo ""
    echo "----- [$(date +%H:%M:%S)] GC=${gc} log=${gc_log_path} -----"

    # Inner scripts cd into their own dir (where ./config/specjbb2015.props lives).
    # Forward LOG_PATH as the per-GC result root so output never lands in the script dir.
    LOG_PATH="$gc_log_path" \
    DEPS_BASE="$DEPS_BASE" \
    NODE_IP="$NODE_IP" \
    bash "$script" 2>&1 | tee -a "${gc_log_path}/runner.log"
    local rc=${PIPESTATUS[0]}
    echo "----- [$(date +%H:%M:%S)] GC=${gc} exit=${rc} -----"
    return $rc
}

overall_rc=0
for GC in $GC_LIST; do
    if ! run_one "$GC"; then
        overall_rc=1
    fi
done

echo ""
echo "==============================================="
echo "  Table 3 run finished at $(date)"
echo "  Tag:    ${TAG}"
echo "  Status: $([ $overall_rc -eq 0 ] && echo OK || echo "some failed/skipped")"
echo "==============================================="
exit $overall_rc
