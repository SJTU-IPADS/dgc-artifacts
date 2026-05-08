#!/bin/bash
# runner-baseline.sh — Run a single-JVM baseline test (G1 or Shenandoah)
#
# Called by run.sh after all config is loaded.
# Expects: JAVA, GC_FLAGS, COMMON_FLAGS, PCORE, CCMT, NUMA_NODE,
#          HEAP_SIZE, RUN_DIR, and workload-specific variables.

source "$(dirname "${BASH_SOURCE[0]}")/cpu.sh"

# Run baseline test for a single load point
# Args: $1=throttle_or_ir  $2=host_id (default 0)
# Returns: the numactl + java command prefix (without -jar and workload flags)
run_baseline_single() {
    local load_point="$1"
    local host_id="${2:-0}"

    # Unlock flags MUST come before any experimental options
    local unlock="-XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions"

    local cpu_list gc_option

    if [ "${LOCAL_DGC:-false}" = "true" ]; then
        # Local DGC: app cores + dedicated GC cores (same NUMA node)
        # NUMA_NODE=0 → even cores (Node 0), NUMA_NODE=1 → odd cores (Node 1)
        local gc_cores=${LOCAL_DGC_GC_CORES:-4}
        local numa=${NUMA_NODE:-1}
        local app_cpu gc_cpu
        if [ "$numa" = "0" ]; then
            app_cpu=$(cpu_baseline "$PCORE" "$host_id")
        else
            app_cpu=$(cpu_dgc_host "$PCORE" "$host_id")
        fi
        gc_cpu=$(cpu_local_dgc_gc "$gc_cores" "$PCORE" "$host_id" "$numa")
        if [ "${LOCAL_DGC_STRICT}" = "true" ]; then
            cpu_list="${app_cpu}"   # strict: only app cores in numactl; GC threads escape via pthread_setaffinity_np
        else
            cpu_list="${app_cpu},${gc_cpu}"
        fi

        gc_option="${GC_FLAGS} -XX:-SnicGCHost \
            -XX:ParallelGCThreads=${PCORE} \
            -XX:ConcGCThreads=${gc_cores} \
            -XX:ShenandoahMaxSATBBufferFlushes=${SATB_FLUSHES:-5} \
            -XX:GCThreadsCores=${gc_cpu} \
            -XX:MutatorCores=${app_cpu}"
    else
        # PAR_BL_CPU_0 / PAR_BL_CPU_1 env vars override per-host CPU lists
        # for parallel-sweep mode (physical cores only).
        local _par_bl_cpu_var="PAR_BL_CPU_${host_id}"
        if [ -n "${!_par_bl_cpu_var:-}" ]; then
            cpu_list="${!_par_bl_cpu_var}"
        else
            cpu_list=$(cpu_baseline "$PCORE" "$host_id")
        fi

        gc_option="${GC_FLAGS} -XX:-SnicGCHost \
            -XX:ParallelGCThreads=${PCORE} \
            -XX:ConcGCThreads=${CCMT} \
            -XX:ShenandoahMaxSATBBufferFlushes=${SATB_FLUSHES:-5}"
    fi

    local snic_baseline="-XX:-SnicGCShareMemEnabled \
        -XX:SNICAddr=${DGC_ADDR} -XX:HostAddr=${DGC_HOST_ADDR} \
        -XX:RDMAPort=$(random_port 2000 60000) -XX:RPCPort=$(random_port)"

    local heap_base_addresses=("0x500000000" "0x300000000" "0x100000000")
    local heap_base="-XX:HeapBaseMinAddress=${heap_base_addresses[$host_id]:-0x500000000}"

    local log_dir="${RUN_DIR}/logs"
    mkdir -p "$log_dir"
    local gc_log="${log_dir}/gc_${load_point}_host${host_id}.log"
    local gc_log_opt="-Xlog:gc*=info:file=${gc_log}:timemillis:filesize=100M"

    # PAR_BL_NUMA env var overrides NUMA_NODE for parallel-sweep mode.
    local _bl_numa="${PAR_BL_NUMA:-${NUMA_NODE:-1}}"

    # Build command: numactl + java + flags (caller appends -jar and workload flags)
    local full_cmd="numactl -C ${cpu_list} -m${_bl_numa} \
        ${JAVA} ${gc_log_opt} ${unlock} ${gc_option} ${snic_baseline} ${heap_base} ${COMMON_FLAGS}"

    echo "$full_cmd"
}
