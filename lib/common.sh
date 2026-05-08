#!/bin/bash
# common.sh — Shared utility functions for DGC test framework

set -euo pipefail

# ============================================================
# Logging
# ============================================================

# Guard against re-sourcing resetting the log file path
: "${_LOG_FILE:=}"

log() {
    local msg="[$(date '+%H:%M:%S')] $*"
    echo "$msg"
    [ -n "$_LOG_FILE" ] && echo "$msg" >> "$_LOG_FILE" || true
}

log_error() {
    local msg="[$(date '+%H:%M:%S')] ERROR: $*"
    echo "$msg" >&2
    [ -n "$_LOG_FILE" ] && echo "$msg" >> "$_LOG_FILE" || true
}

die() {
    log_error "$@"
    exit 1
}

# Set the log file for this run
set_log_file() {
    _LOG_FILE="$1"
    mkdir -p "$(dirname "$_LOG_FILE")"
}

# Commands log file (separate from framework.log for easy parsing)
: "${_CMD_LOG_FILE:=}"

set_cmd_log_file() {
    _CMD_LOG_FILE="$1"
}

# Log a process launch command line.
# Usage: log_cmd <component_name> <pid> <cmdline...>
# Writes to both framework.log and a dedicated commands.log
log_cmd() {
    local name="$1"; shift
    local pid="$1"; shift
    local cmd="$*"
    log "  CMD [${name}] (PID=${pid}): ${cmd}"
    if [ -n "$_CMD_LOG_FILE" ]; then
        printf '[%s] %s (PID=%s)\n%s\n\n' "$(date '+%H:%M:%S')" "$name" "$pid" "$cmd" >> "$_CMD_LOG_FILE"
    fi
}

# ============================================================
# Process management
# ============================================================

# Kill java processes matching a pattern, ignoring permission errors.
# Uses pgrep+kill instead of pkill -f to avoid killing the calling SSH session
# (pkill -f matches the entire command line, including the ssh command that contains the pattern).
safe_pkill() {
    local pattern="$1"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null | while read pid; do
        # Only kill java processes, not bash/ssh/pgrep
        local cmd
        cmd=$(ps -p "$pid" -o comm= 2>/dev/null)
        case "$cmd" in java|numactl) echo "$pid" ;; esac
    done || true)
    [ -n "$pids" ] && kill -9 $pids 2>/dev/null || true
}

# Wait for a process to be ready (simple sleep-based)
wait_ready() {
    local seconds="${1:-5}"
    sleep "$seconds"
}

# Check if a PID is still alive
is_alive() {
    kill -0 "$1" 2>/dev/null
}

# ============================================================
# Port allocation
# ============================================================

# Get a random available port in range [base, base+range)
random_port() {
    local base="${1:-1200}"
    local range="${2:-64000}"
    echo $(( base + RANDOM % range ))
}

# ============================================================
# Config resolution helpers
# ============================================================

# Resolve a variable that may have per-mode suffixes.
# Usage: resolve_var BENCH_JAR baseline → returns BENCH_JAR_baseline or BENCH_JAR
resolve_var() {
    local varname="$1"
    local mode="$2"
    local specific="${varname}_${mode}"
    local value="${!specific:-}"
    if [ -z "$value" ]; then
        value="${!varname:-}"
    fi
    echo "$value"
}

# Compute heap size from MIN_HEAP and HEAP_MULTIPLIER
compute_heap() {
    local mode="$1"    # baseline or dgc
    local min_heap
    min_heap=$(resolve_var MIN_HEAP "$mode")
    [ -z "$min_heap" ] && min_heap="${MIN_HEAP:-0}"
    local multiplier="${HEAP_MULTIPLIER:-2.0}"

    # If HEAP is set directly, use it
    if [ -n "${HEAP:-}" ]; then
        echo "$HEAP"
        return
    fi

    # Compute: floor(min_heap * multiplier)
    echo "$min_heap" "$multiplier" | awk '{printf "%d", $1 * $2}'
}

# ============================================================
# Cleanup
# ============================================================

cleanup_shm() {
    # Clean SHM files matching current profile prefix
    # Default (no profile): share_*, coor_*, virtual_node_*
    # With profile: brA_share_*, brA_coor_*, etc.
    if [ -n "${SHM_PREFIX:-}" ] && [ "${SHM_PREFIX}" != "default" ]; then
        rm -f /dev/shm/${SHM_PREFIX}_share_* /dev/shm/${SHM_PREFIX}_coor_* /dev/shm/${SHM_PREFIX}_virtual_node_* 2>/dev/null || true
    else
        rm -f /dev/shm/share_* /dev/shm/coor_* /dev/shm/virtual_node_* 2>/dev/null || true
    fi
}

cleanup_hbase() {
    safe_pkill "org.apache.hadoop.hbase.regionserver.HRegionServer"
    safe_pkill "org.apache.hadoop.hbase.master.HMaster"
    safe_pkill "org.apache.hadoop.hbase.zookeeper"
}
