#!/bin/bash
# run.sh — DGC test framework unified entry point
#
# Usage:
#   ./run.sh <workload> <gc> [options]
#
# Workloads:
#   h2                  H2 TPC-C throttle benchmark
#   tradesoap           DayTrader (DaCapo tradesoap)
#   hbase-workloada     HBase + YCSB workload-A (read/update)
#   hbase-readinsert    HBase + YCSB read-insert-half
#   specjbb-preset      SPECjbb2015 PRESET mode
#   specjbb-hbir        SPECjbb2015 HBIR_RT mode
#   *-survey            DGC benefit survey (no throttle, max throughput)
#                       h2-survey, lusearch-survey, tomcat-survey, kafka-survey,
#                       cassandra-survey, tradebeans-survey, tradesoap-survey, jme-survey
#
# GC variants:
#   g1                  G1GC baseline
#   shenandoah          Shenandoah baseline
#   dgc-shm             DGC shared memory
#   dgc-rdma            DGC RDMA
#   all                 Run all 4 variants sequentially
#
# Options:
#   --jdk <path>        Override JDK path
#   --profile <name>    Use env.d/profiles/<name>.env
#   --throttle "t1 t2"  Override throttle/IR rates
#   --tag <name>        Add tag to result directory name
#   --dry-run           Print config and exit without running
#   --iter <n>          Override DaCapo iteration count
#
# Examples:
#   ./run.sh h2 g1                              # H2 + G1 baseline
#   ./run.sh h2 all                             # H2 + all 4 GC variants
#   ./run.sh tradesoap shenandoah --profile branch-A
#   ./run.sh hbase-workloada g1 --throttle "4500 18000 45000"
#   ./run.sh specjbb-preset dgc-shm

set -euo pipefail

# Multi-evaluator setups expect every artifact-tree dir/file to be writable
# by the shared group (the artifact root is setgid). Default umask 0022
# would create per-evaluator dirs that block their group, so anyone running
# the AE under a different uid in the same group can't write into them
# (e.g. mkdir results/<other_user>). Force a group-writable umask up front.
umask 0002

AE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Resolve SHM_PREFIX as early as env.sh would, so cleanup helpers below can scope
# rm globs to the calling user's files. Mirrors logic in env.sh layer 0.
SHM_PREFIX="${SHM_PREFIX:-${DGC_PROFILE:-${USER:-default}}}"

# ============================================================
# Run lock — atomic via flock(2)
# ============================================================
# Default lock lives at the artifact root so multiple evaluators sharing the
# same checkout serialize their AE runs. The lock is per-checkout: in the
# expected AE deployment all evaluators share one $AE_DIR, so this is also
# effectively per-host.
#
# Why not /tmp? Modern Linux (Ubuntu 22.04 default kernel) sets
# `fs.protected_regular=2`, which blocks open-for-write on regular files in
# sticky world-writable dirs (like /tmp) when the file is owned by another
# user — even at mode 0666. So a lock file in /tmp is not safely writable
# across users. The artifact root is a setgid group dir shared by all
# evaluators, which sidesteps the protected_regular check.
#
# LOCK_FILE may be overridden when an in-house parallel driver wants multiple
# run.sh instances on disjoint resource scopes (e.g., separate NUMA nodes,
# separate ports), or when evaluators do NOT share a checkout (point all of
# them at a common path on a non-sticky dir writable by every evaluator).

LOCK_FILE="${LOCK_FILE:-${AE_DIR}/.run.lock}"

# Make sure the lock file exists and is group-writable so every evaluator
# (assumed to share the artifact's group) can flock it. The umask-0002
# subshell creates new files at mode 0664; chmod-ing already-existing files
# is a no-op for foreign owners (and unnecessary if everyone is in the
# artifact's setgid group).
if [ ! -e "$LOCK_FILE" ]; then
    ( umask 0002 && : > "$LOCK_FILE" ) 2>/dev/null || true
fi
if [ -e "$LOCK_FILE" ] && [ ! -w "$LOCK_FILE" ]; then
    echo "ERROR: Lock file $LOCK_FILE is not writable by ${USER:-?}." >&2
    echo "  All evaluators must share the artifact's group (setgid on $AE_DIR)." >&2
    echo "  Verify with: ls -ld '$AE_DIR' '$LOCK_FILE'" >&2
    exit 2
fi

# Open the lock file on fd 9. The fd stays open through the whole run; flock
# is auto-released by the kernel when the holder process exits.
exec 9>>"$LOCK_FILE"

# ============================================================
# Kill leftover benchmark/DGC processes
# ============================================================

_kill_zombies() {
    echo "Cleaning up leftover processes..."
    # Match both specjbb2015.jar and specjbb-output-latency-with-start.jar
    # (the latency-instrumented variant used by fig5/fig7). The "java.*"
    # prefix keeps pkill from matching the calling shell's own cmdline,
    # which contains "specjbb-preset"/"specjbb-hbir" when called via a
    # workload arg.
    pkill -9 -f "java.*specjbb" 2>/dev/null || true
    pkill -9 -f "SnicGCClient" 2>/dev/null || true
    pkill -9 -f "SnicGCCoordinator" 2>/dev/null || true
    pkill -9 -f "dacapo" 2>/dev/null || true
    pkill -9 -f "HRegionServer" 2>/dev/null || true
    pkill -9 -f "HMaster" 2>/dev/null || true
    # Scope the SHM cleanup to the caller's prefix so a concurrent run by
    # another user (with a different prefix) is not affected. With SHM_PREFIX
    # defaulting to $USER (env.sh), the prefix is naturally per-user.
    if [ -n "${SHM_PREFIX:-}" ] && [ "${SHM_PREFIX}" != "default" ]; then
        rm -f /dev/shm/${SHM_PREFIX}_share_* /dev/shm/${SHM_PREFIX}_coor_* /dev/shm/${SHM_PREFIX}_virtual_node_* 2>/dev/null || true
    else
        # Legacy fallback: SHM_PREFIX explicitly set to "default" wipes only
        # unprefixed segments. tmpfs perms keep the rm scoped to your own files.
        rm -f /dev/shm/share_* /dev/shm/coor_* /dev/shm/virtual_node_* 2>/dev/null || true
    fi
    sleep 1
    echo "Cleanup done."
}

# ============================================================
# Handle "unlock" subcommand
# ============================================================
# With flock-based locking, the kernel auto-releases the lock when its holder
# process exits. The 'unlock' subcommand therefore only kills leftover
# benchmark processes and wipes the holder-info content; it does NOT (and
# cannot safely) forcibly evict an active flock.

if [ "${1:-}" = "unlock" ]; then
    _kill_zombies
    : > "$LOCK_FILE" 2>/dev/null || true
    echo "Cleanup done. Any active flock will be auto-released when its holder exits."
    exit 0
fi

# ============================================================
# Acquire exclusive lock (non-blocking, atomic via flock(2))
# ============================================================

# Wait up to 10s for the lock. Without a wait, a chained driver (fig*-run.sh
# back-to-back ./run.sh calls) hits a transient race: the previous run.sh has
# exited and the kernel marked the OFD for release, but Java child processes
# that inherited fd 9 (bash does not set FD_CLOEXEC on script-level redirects)
# may still be in their teardown path, keeping the OFD refcount > 0. The
# 10-second window covers this without affecting the case where another
# evaluator is genuinely holding the lock for a long run — in that case the
# wait still terminates with the same friendly error.
if ! flock -w 10 9; then
    holder=$(head -1 "$LOCK_FILE" 2>/dev/null || echo "")
    echo "ERROR: Another AE test is running on $(hostname -s)." >&2
    if [ -n "$holder" ]; then
        echo "  Lock holder: $holder" >&2
    fi
    echo "  Lock file:   $LOCK_FILE" >&2
    echo "  Waited 10s; either the holder is doing real work, or its child JVMs are stuck." >&2
    echo "  Use './run.sh unlock' (only safe if no AE is actually running) to recover." >&2
    exit 1
fi

# Record holder info inside the lock file for friendlier diagnostics. fd 9
# still holds the flock; truncating the content via a separate redirect does
# not release it (flock is per-inode at the kernel level, not per-content).
echo "$USER $$ $(date '+%Y-%m-%d %H:%M:%S') $(hostname -s)" > "$LOCK_FILE"
trap ': > "$LOCK_FILE" 2>/dev/null || true' EXIT

# ============================================================
# Parse CLI arguments
# ============================================================

WORKLOAD=""
GC_TYPE=""
CLI_JDK=""
CLI_THROTTLE=""
CLI_TAG=""
CLI_ITER=""
DRY_RUN=false

usage() {
    head -35 "$0" | grep '^#' | sed 's/^# \?//'
    exit 1
}

[ $# -lt 2 ] && usage

WORKLOAD="$1"; shift
GC_TYPE="$1"; shift

while [ $# -gt 0 ]; do
    case "$1" in
        --jdk)      CLI_JDK="$2"; shift 2 ;;
        --profile)  export DGC_PROFILE="$2"; shift 2 ;;
        --throttle) CLI_THROTTLE="$2"; shift 2 ;;
        --tag)      CLI_TAG="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=true; shift ;;
        --iter)     CLI_ITER="$2"; shift 2 ;;
        -h|--help)  usage ;;
        *)          echo "Unknown option: $1" >&2; usage ;;
    esac
done

# ============================================================
# Load config layers
# ============================================================

# Layer 0-3: env.sh handles defaults → machine → user → profile
source "${AE_DIR}/env.sh"

# Layer 4: CLI overrides
[ -n "$CLI_JDK" ] && DGC_JDK="$CLI_JDK" && JAVA="${DGC_JDK}/bin/java"

# Load workload config
WORKLOAD_CONF="${AE_DIR}/conf/workloads/${WORKLOAD}.conf"
[ -f "$WORKLOAD_CONF" ] || { echo "ERROR: Unknown workload '${WORKLOAD}' (no ${WORKLOAD_CONF})" >&2; exit 1; }
source "$WORKLOAD_CONF"

# ============================================================
# Dispatch: handle "all" or single GC
# ============================================================

if [ "$GC_TYPE" = "all" ]; then
    echo "=== Running all GC variants for ${WORKLOAD} ==="
    for gc in g1 shenandoah dgc-shm dgc-rdma; do
        echo ""
        echo "========================================"
        echo "  ${WORKLOAD} / ${gc}"
        echo "========================================"
        "$0" "$WORKLOAD" "$gc" \
            ${CLI_JDK:+--jdk "$CLI_JDK"} \
            ${DGC_PROFILE:+--profile "$DGC_PROFILE"} \
            ${CLI_THROTTLE:+--throttle "$CLI_THROTTLE"} \
            ${CLI_TAG:+--tag "$CLI_TAG"} \
            ${CLI_ITER:+--iter "$CLI_ITER"} \
            ${DRY_RUN:+--dry-run} || true
    done
    exit 0
fi

# Load GC config
GC_CONF="${AE_DIR}/conf/gc/${GC_TYPE}.conf"
[ -f "$GC_CONF" ] || { echo "ERROR: Unknown GC '${GC_TYPE}' (no ${GC_CONF})" >&2; exit 1; }
source "$GC_CONF"

# Re-source workload config so it can override GC defaults (e.g. LOCAL_DGC_GC_CORES, GC_FLAGS)
source "$WORKLOAD_CONF"

# Layer 5: Profile can override workload/GC config values too
if [ -n "${DGC_PROFILE:-}" ] && [ -f "${AE_DIR}/env.d/profiles/${DGC_PROFILE}.env" ]; then
    source "${AE_DIR}/env.d/profiles/${DGC_PROFILE}.env"
fi

# ============================================================
# Resolve final parameters (workload × GC mode)
# ============================================================

source "${AE_DIR}/lib/common.sh"

# Resolve per-mode parameters
CCMT=$(resolve_var CCMT "$GC_MODE")
PCORE=$(resolve_var PCORE "$GC_MODE")
CLIENT_CCMT=$(resolve_var CLIENT_CCMT "$GC_MODE")
# COORD_CCMT = ConcGCThreads on the coordinator JVM (small, stable — coord
# just runs CP-SAT on its own tiny heap).
# CLIENT_OWN_CCMT = ConcGCThreads on the client JVM for its OWN heap (NOT
# the marker count that works on host's heap — that's ShmClientMarkerNum,
# driven by CLIENT_CCMT).
#
# Before 2026-04-24 both defaulted to CLIENT_CCMT, so bumping CLIENT_CCMT
# for marking parallelism silently multiplied coord/client GC threads too
# — crashed coord on small-heap workloads via native SIGABRT from
# OR-Tools. See campaigns/dual-homog/KNOWN_ISSUES.md §14.
COORD_CCMT=$(resolve_var COORD_CCMT "$GC_MODE")
: "${COORD_CCMT:=4}"
CLIENT_OWN_CCMT=$(resolve_var CLIENT_OWN_CCMT "$GC_MODE")
: "${CLIENT_OWN_CCMT:=4}"
export COORD_CCMT CLIENT_OWN_CCMT
HEAP_SIZE=$(compute_heap "$GC_MODE")
BENCH_JAR=$(resolve_var BENCH_JAR "$GC_MODE")
[ -z "$BENCH_JAR" ] && BENCH_JAR="${BENCH_JAR_default:-}"
HOST_COUNT=$(resolve_var HOST_COUNT "$GC_MODE")
: "${HOST_COUNT:=2}"

# CLI overrides for throttle/IR and iterations
if [ -n "$CLI_THROTTLE" ]; then
    THROTTLES="$CLI_THROTTLE"
    IR_RATES="$CLI_THROTTLE"
fi
[ -n "$CLI_ITER" ] && ITERATIONS="$CLI_ITER"

# ============================================================
# Create result directory
# ============================================================

_ts=$(date +"%Y%m%d_%H%M%S")
_label="${WORKLOAD}_${GC_NAME}"
[ -n "${DGC_PROFILE:-}" ] && _label="${_label}_${DGC_PROFILE}"
[ -n "${CLI_TAG:-}" ] && _label="${_label}_${CLI_TAG}"
_label="${_label}_$(whoami)"

RUN_ID="${_ts}_${_label}"
RUN_DIR="${RESULTS_BASE}/${RUN_ID}"
mkdir -p "${RUN_DIR}/logs" "${RUN_DIR}/raw"

# ============================================================
# Record metadata & command log
# ============================================================

source "${AE_DIR}/lib/metadata.sh"
write_meta_json "$RUN_DIR" "$RUN_ID" "$WORKLOAD" "$GC_TYPE"
set_log_file "${RUN_DIR}/logs/framework.log"
set_cmd_log_file "${RUN_DIR}/logs/commands.log"

log "============================================"
log "DGC Test Framework"
log "============================================"
log "Run ID:    ${RUN_ID}"
log "Workload:  ${WORKLOAD} (runner: ${BENCH_RUNNER})"
log "GC:        ${GC_NAME} (mode: ${GC_MODE})"
log "JDK:       ${DGC_JDK}"
log "Heap:      ${HEAP_SIZE}m"
log "CCMT:      ${CCMT}, PCORE: ${PCORE}"
log "Profile:   ${DGC_PROFILE:-none}"
log "Results:   ${RUN_DIR}"
log "============================================"
log ""

# ============================================================
# Dry-run check
# ============================================================

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "--- Dry run: config resolved, not executing ---"
    echo "JAVA=${JAVA}"
    echo "BENCH_JAR=${BENCH_JAR}"
    echo "HEAP_SIZE=${HEAP_SIZE}"
    echo "CCMT=${CCMT} PCORE=${PCORE}"
    echo "THROTTLES=${THROTTLES:-} IR_RATES=${IR_RATES:-}"
    echo "RUN_DIR=${RUN_DIR}"
    # Remove the empty result dir
    rm -rf "$RUN_DIR"
    exit 0
fi

# ============================================================
# Dispatch to workload runner
# ============================================================

WORKLOAD_RC=0
case "${BENCH_RUNNER}" in
    dacapo)
        source "${AE_DIR}/lib/workload-dacapo.sh"
        run_dacapo || WORKLOAD_RC=$?
        ;;
    hbase)
        source "${AE_DIR}/lib/workload-hbase.sh"
        HEAP_SIZE="${HEAP:-${HEAP_SIZE}}"
        run_hbase || WORKLOAD_RC=$?
        ;;
    specjbb)
        source "${AE_DIR}/lib/workload-specjbb.sh"
        HEAP_SIZE="${HEAP:-${HEAP_SIZE}}"
        run_specjbb || WORKLOAD_RC=$?
        ;;
    *)
        die "Unknown runner: ${BENCH_RUNNER}"
        ;;
esac

# ============================================================
# Finalize
# ============================================================

if [ "$WORKLOAD_RC" -eq 0 ]; then
    finalize_meta_json "$RUN_DIR" "completed"
    log ""
    log "============================================"
    log "Test completed. Results: ${RUN_DIR}"
    log "============================================"
else
    finalize_meta_json "$RUN_DIR" "failed"
    log ""
    log "============================================"
    log "Test FAILED (rc=${WORKLOAD_RC}). Results: ${RUN_DIR}"
    log "============================================"
fi
exit $WORKLOAD_RC
