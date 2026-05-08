#!/bin/bash
# cpu.sh — CPU pinning helpers
#
# DS03 topology (HT disabled):
#   Socket 0: cores 0,2,4,...,62   (32 physical cores, even numbers)
#   Socket 1: cores 64,66,...,126  (32 physical cores, even numbers)
#   Odd-numbered "cores" (1,3,5,...) are available when HT is off on this machine
#   Total usable: 0-127 (64 physical cores × 2 sockets, step 2)
#
# Convention: all CPU lists use step=2 (physical cores only)
#
# NUMA mapping (numactl -m must match the CPU's NUMA node!):
#
#   Function            CPUs        NUMA node   numactl -m
#   ─────────────────   ─────────   ─────────   ──────────
#   cpu_baseline()      even        0           -m0
#   cpu_dgc_host()      odd         1           -m1
#   cpu_dgc_client()    even        0           -m0
#   cpu_controller()    odd         1           -m1
#   cpu_ycsb_client()   even        0           -m0
#
# Cross-NUMA memory access (e.g. CPU on node 1 with -m0) causes ~50ns
# extra latency per access. Always keep CPU and memory on the same node.

# Generate a CPU list: N cores starting from $start, step 2, ascending
# Usage: cpu_list_asc <start> <count>
# Example: cpu_list_asc 0 8 → "0,2,4,6,8,10,12,14"
cpu_list_asc() {
    local start=$1 count=$2
    local cpus="" c=$start
    for ((i = 0; i < count; i++)); do
        [ -n "$cpus" ] && cpus="${cpus},"
        cpus="${cpus}${c}"
        c=$((c + 2))
    done
    echo "$cpus"
}

# Generate a CPU list: N cores starting from $start, step -2, descending
# Usage: cpu_list_desc <start> <count>
# Example: cpu_list_desc 126 4 → "126,124,122,120"
cpu_list_desc() {
    local start=$1 count=$2
    local cpus="" c=$start
    for ((i = 0; i < count; i++)); do
        [ -n "$cpus" ] && cpus="${cpus},"
        cpus="${cpus}${c}"
        c=$((c - 2))
    done
    echo "$cpus"
}

# ============================================================
# Standard allocation patterns (matching paper configuration)
# ============================================================

# Baseline: single JVM, all cores in a contiguous block
# host_id=0 → cores from low end of socket
cpu_baseline() {
    local cores=$1
    local host_id=${2:-0}
    local start=$((host_id * cores * 2))
    cpu_list_asc "$start" "$cores"
}

# DGC Host: odd cores from low end (matching original scripts)
# host_id=0 → 1,3,5,...,15; host_id=1 → 17,19,...,31
cpu_dgc_host() {
    local cores=$1
    local host_id=${2:-0}
    local start=$((host_id * cores * 2 + 1))
    cpu_list_asc "$start" "$cores"
}

# DGC Client: ALL clients share the SAME core pool (paper design: shared DGC resource pool)
# client_id is ignored — both clients bind to the same 4 cores
cpu_dgc_client() {
    local cores=$1
    local client_id=${2:-0}  # unused: shared pool
    local start=126
    cpu_list_desc "$start" "$cores"
}

# Controller / TxInjector: odd-numbered cores (socket 1 territory)
cpu_controller() {
    local cores=$1
    local cpus="" c=1
    for ((i = 0; i < cores; i++)); do
        [ -n "$cpus" ] && cpus="${cpus},"
        cpus="${cpus}${c}"
        c=$((c + 2))
    done
    echo "$cpus"
}

# Local DGC: GC-dedicated cores (above app cores, same parity)
# Usage: cpu_local_dgc_gc <gc_cores> <app_cores> [host_id] [numa_node]
# numa_node=0 → even cores (Node 0), numa_node=1 → odd cores (Node 1, default)
# Example: cpu_local_dgc_gc 4 8 0 1 → "17,19,21,23" (odd, after 8 odd app cores)
# Example: cpu_local_dgc_gc 4 8 0 0 → "16,18,20,22" (even, after 8 even app cores)
cpu_local_dgc_gc() {
    local gc_cores=$1
    local app_cores=$2
    local host_id=${3:-0}
    local numa=${4:-1}
    local parity=$((numa == 0 ? 0 : 1))
    local start=$((host_id * app_cores * 2 + app_cores * 2 + parity))
    cpu_list_asc "$start" "$gc_cores"
}

# YCSB client (HBase tests): high even cores on socket 1
cpu_ycsb_client() {
    local cores=$1
    cpu_list_desc 126 "$cores"
}
