#!/bin/bash
# fig7-run.sh — Figure 7: SPECjbb PRESET IR=10356 — DGC-RDMA cache-size
#               sweep + baseline GC comparison points
#
# Goal: at a fixed injection rate (IR=10356), show how DGC-RDMA's address-
# translated client memory pool size affects request latency, and place
# DGC-SHM / Shenandoah / G1 reference points on the same chart so we can
# see how the four GC variants compare on the same workload.
#
# Pipeline (7 runs total by default):
#   1. dgc-rdma × CACHE_SIZES (default 4 cache values)
#   2. dgc-shm   (1 reference point)
#   3. shenandoah (1 reference point)
#   4. g1         (1 reference point)
#
# Each run uses specjbb-preset.conf (PRESET mode, fixed IR + duration).
# The conf is patched with fig7 overrides at the start and restored on
# exit (same approach as fig5-run.sh).
#
# Usage:
#   ./osdi26-scripts/fig7/fig7-run.sh                                   # full sweep
#   CACHE_SIZES="4096 1024" ./osdi26-scripts/fig7/fig7-run.sh           # subset
#   GC_LIST="dgc-rdma dgc-shm" ./osdi26-scripts/fig7/fig7-run.sh        # skip baselines
#   IR=8192 DURATION=60000 ./osdi26-scripts/fig7/fig7-run.sh            # different IR
#   REPS="1 2 3" ./osdi26-scripts/fig7/fig7-run.sh                      # 3 reps each
#
# Environment overrides:
#   IR             injection rate (default 10356)
#   DURATION       PRESET duration ms (default 120000)
#   GROUPS         backend groups (default 2)
#   CACHE_SIZES    space-separated MB values for SnicGCLocalMemoryPoolSize
#                  (default "4096 3072 2048 1024", DGC-RDMA only)
#   GC_LIST        which GC variants to run; the cache sweep applies to
#                  dgc-rdma if it appears in this list, others run once.
#                  (default "dgc-rdma dgc-shm shenandoah g1")
#   REPS           rep ids (default "1")
#   RESULTS_BASE   override (default ${AE_DIR}/results/${USER}/fig7-result)

set -euo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."

AE_DIR="$(pwd)"

CLI_TAG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

IR="${IR:-10356}"
DURATION="${DURATION:-60000}"
BACKEND_GROUPS="${BACKEND_GROUPS:-2}"
CACHE_SIZES="${CACHE_SIZES:-4096 3072 2048 1024}"
GC_LIST="${GC_LIST:-dgc-rdma dgc-shm shenandoah g1}"
REPS="${REPS:-1}"

# Default results destination = per-user subdir under the artifact's results/.
export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig7-result}"
mkdir -p "$RESULTS_BASE"

TS=$(date +%Y%m%d_%H%M%S)
TAG_BASE="${CLI_TAG:-fig7-${TS}}"

# Patch specjbb-preset.conf with fig7 IR / DURATION / GROUPS for the whole
# sweep; restore on exit. Env vars don't override conf values, so we have
# to edit the file in place.
CONF="${AE_DIR}/conf/workloads/specjbb-preset.conf"
CONF_BACKUP=$(mktemp)
# Avoid `cp -p` here: when a co-evaluator (different uid, same group) runs
# fig7-run.sh, restoring with -p tries to chown/chtime a file the running
# user does not own → cp prints a non-fatal warning. We only need content
# preservation; mode/owner stay correct because the conf already has the
# right group + perms.
cat "$CONF" > "$CONF_BACKUP"

# fig7-specific COOR_CCMT_ARGS override for dgc-rdma — region-eviction at
# small caches makes DGC marking much slower (≈1500ms) so coordinator's
# CCMT model needs different b values. Restored on exit.
RDMA_CONF="${AE_DIR}/conf/gc/dgc-rdma.conf"
RDMA_CONF_BACKUP=$(mktemp)
cat "$RDMA_CONF" > "$RDMA_CONF_BACKUP"

restore_conf() {
    cat "$CONF_BACKUP" > "$CONF"
    rm -f "$CONF_BACKUP"
    cat "$RDMA_CONF_BACKUP" > "$RDMA_CONF"
    rm -f "$RDMA_CONF_BACKUP"
}
trap restore_conf EXIT

{
    echo ""
    echo "# === FIG7 OVERRIDE (added by fig7-run.sh; restored on exit) ==="
    echo "SPECJBB_GROUPS=${BACKEND_GROUPS}"
    echo "SPECJBB_IR=${IR}"
    echo "SPECJBB_DURATION=${DURATION}"
} >> "$CONF"

# Append fig7 COOR_CCMT_ARGS override after conf file's own assignment so
# bash sourcing picks our value. Cache-eviction thrashing slows DGC marking
# significantly, so the model needs different b values:
#   fallback marking ~ 500ms (vs default 550)
#   DGC marking under thrashing ~ 1500ms (vs default 475)
{
    echo ""
    echo "# === FIG7 OVERRIDE (added by fig7-run.sh; restored on exit) ==="
    echo 'COOR_CCMT_ARGS="0:0:500;4:0:1500"'
} >> "$RDMA_CONF"

# fig7-specific JVM flag overrides (consumed by lib/runner-dgc.sh).
# These match the dgc-rdma-evict-preset run config that produced the
# paper-matching p99 across all four cache sizes (incl. the 1/4 cache
# thrashing scenario).
export EVICTION_CONTROL=0          # vs default 50: disables hotness-based eviction blocking
export EST_OBJECT_SIZE=10           # vs default 256: smaller obj-size estimate for cache modeling
export CLIENT_RDMA_COPY_TIME=20     # vs default 60: client expects faster RDMA
export HOST_RDMA_COPY_TIME=80       # vs default 60: host expects slower (gives DGC more headroom)
export COOR_RDMA_COPY_TIME=20       # vs default 60: coordinator timing aligned with client
export COOR_AVG_MARK_AMPLIFY=2.0    # coordinator amplifies avg mark time by 2x for safety
export COOR_FROZEN_UPPER=150        # vs default 80: frozen-DGC window wider tolerates jitter

echo "==============================================="
echo "  fig7 SPECjbb PRESET IR=${IR} sweep starting at $(date)"
echo "  Tag base:    ${TAG_BASE}"
echo "  GCs:         ${GC_LIST}"
echo "  Reps:        ${REPS}"
echo "  Groups:      ${BACKEND_GROUPS}"
echo "  IR:          ${IR}    Duration: ${DURATION}ms"
echo "  Cache sizes: ${CACHE_SIZES}  (DGC-RDMA only)"
echo "  Results:     ${RESULTS_BASE}/<run-id>"
echo "==============================================="

run_one() {
    local rep="$1" gc="$2" mem="$3"
    local rep_tag
    if [ -n "$mem" ]; then
        rep_tag="${TAG_BASE}-mem${mem}-rep${rep}"
    else
        rep_tag="${TAG_BASE}-rep${rep}"
    fi

    echo ""
    if [ -n "$mem" ]; then
        echo "----- [$(date +%H:%M:%S)] REP=${rep}  ${gc}  CLIENT_MEM_BUDGET=${mem}MB -----"
    else
        echo "----- [$(date +%H:%M:%S)] REP=${rep}  ${gc}  -----"
    fi

    # CLIENT_MEM_BUDGET only meaningful for dgc-rdma; the runner-dgc.sh
    # fig7 hook ignores it for SHM/baseline.
    if [ -n "$mem" ]; then
        CLIENT_MEM_BUDGET="$mem" ./run.sh specjbb-preset "$gc" --tag "$rep_tag" || \
            echo "[warn] run failed for ${gc} mem=${mem}MB rep=${rep}"
    else
        ./run.sh specjbb-preset "$gc" --tag "$rep_tag" || \
            echo "[warn] run failed for ${gc} rep=${rep}"
    fi
}

for rep in $REPS; do
    for gc in $GC_LIST; do
        if [ "$gc" = "dgc-rdma" ]; then
            for mem in $CACHE_SIZES; do
                run_one "$rep" "$gc" "$mem"
            done
        else
            run_one "$rep" "$gc" ""
        fi
    done
done

echo ""
echo "==============================================="
echo "  fig7 sweep finished at $(date)"
echo "  Tag base: ${TAG_BASE}"
echo "  Run analysis:  ./osdi26-scripts/fig7/analyze-fig7.sh --tag ${TAG_BASE}"
echo "==============================================="
