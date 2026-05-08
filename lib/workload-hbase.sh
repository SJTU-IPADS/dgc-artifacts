#!/bin/bash
# workload-hbase.sh — HBase + YCSB benchmark runner
#
# Supports baseline (single JVM) and DGC (coordinator + client + host) modes.
# DGC mode uses runner-dgc.sh for coordinator/client orchestration,
# and the "framework" test_mode in regionserver scripts for host JVM flags.

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/cpu.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runner-baseline.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runner-dgc.sh"

_preflight_hbase() {
    local ok=true
    if [ ! -d "${HBASE_DIR}/lib" ]; then
        log_error "HBase not found at ${HBASE_DIR}/lib"
        log_error "  Download HBase 2.5.11 and apply overlay — see hbase-conf/SETUP.md"
        ok=false
    fi
    if [ ! -f "${HBASE_DIR}/bin/self-regionserver-start" ]; then
        log_error "Custom regionserver script missing: ${HBASE_DIR}/bin/self-regionserver-start"
        log_error "  Copy from hbase-conf/bin/ — see hbase-conf/SETUP.md"
        ok=false
    fi
    if [ ! -d "${YCSB_DIR}/bin" ]; then
        log_error "YCSB not found at ${YCSB_DIR}"
        log_error "  Download YCSB 0.18.0 — see hbase-conf/SETUP.md"
        ok=false
    fi
    if [ ! -f "${YCSB_DIR}/workloads/${YCSB_WORKLOAD}" ]; then
        log_error "YCSB workload file missing: ${YCSB_DIR}/workloads/${YCSB_WORKLOAD}"
        log_error "  Copy from hbase-conf/ycsb-workloads/ — see hbase-conf/SETUP.md"
        ok=false
    fi
    [ "$ok" = true ] || die "HBase pre-flight check failed. See hbase-conf/SETUP.md for setup instructions."
}

run_hbase() {
    _preflight_hbase
    local mode="$GC_MODE"

    # Export JAVA_HOME so HBase scripts use the framework's JDK
    export JAVA_HOME="$DGC_JDK"

    log "=== HBase ${YCSB_WORKLOAD} / ${GC_NAME} (${mode}) ==="
    log "Heap: ${HEAP_SIZE}m, Threads: ${YCSB_THREADS}, Host count: ${HOST_NUM}"
    log "IR rates: ${IR_RATES}"
    log ""

    for ir_rate in ${IR_RATES}; do
        log "--- IR rate: ${ir_rate} ---"

        local point_dir="${RUN_DIR}/raw/${ir_rate}"
        mkdir -p "$point_dir"

        # Cleanup
        cleanup_hbase
        safe_pkill "SnicGCClient"
        safe_pkill "SnicGCCoordinator"
        cleanup_shm
        rm -rf "${RUN_DIR}/tmp" 2>/dev/null || true

        if [ "$mode" = "baseline" ]; then
            _run_hbase_baseline "$ir_rate" "$point_dir"
        else
            _run_hbase_dgc "$ir_rate" "$point_dir"
        fi

        # Final cleanup
        cleanup_hbase
        if [ "$mode" = "dgc" ]; then
            cleanup_dgc
        fi
        log "IR rate ${ir_rate} done."
        echo ""
    done
}

# ============================================================
# Common: start HBase infrastructure (ZK + Master + init)
# ============================================================

_start_hbase_infra() {
    local rpc_port="$1" point_dir="$2"

    # Clean stale HBase data (hbase-site.xml: hbase.tmp.dir=./tmp_0)
    rm -rf ./tmp_0 2>/dev/null || true

    local workdir
    workdir=$(get_workdir "$RUN_DIR" "hbase")
    rm -rf "${workdir}/data/default" 2>/dev/null || true

    # Wipe any inherited HBASE_OPTS so ZK / master do not pick up
    # SnicGCHost / RPCPort / SnicShm* flags left over from the prior IR's
    # regionserver export. Symptom of the leak: master.log starts with
    # "DGC LOG: Failed to connect to server, ... port:<old-IR's RPC>" and
    # exits at 0.022 s; subsequent table init then fails with NoNode for
    # /hbase/master and the run silently degenerates into 0 ops/s.
    log "Starting ZooKeeper..."
    HBASE_OPTS="" bash "${HBASE_DIR}/bin/self-zookeeper-start" --config "${HBASE_DIR}/conf" \
        zookeeper "${rpc_port}" "${CCMT}" "${CCMT}" "${PCORE}" "${HEAP_SIZE}" framework start \
        > "${point_dir}/zookeeper.log" 2>&1 &
    wait_ready 5

    log "Starting HBase master..."
    HBASE_OPTS="" bash "${HBASE_DIR}/bin/self-master-start" --config "${HBASE_DIR}/conf" \
        master "${rpc_port}" "${CCMT}" "${CCMT}" "${PCORE}" "${HEAP_SIZE}" framework start \
        > "${point_dir}/master.log" 2>&1 &
    wait_ready 5
}

_init_hbase_tables() {
    local point_dir="$1"
    log "Initializing HBase tables..."
    # Clear HBASE_OPTS so hbase shell doesn't inherit DGC/GC flags from regionserver
    # Retry up to N times: master may not have registered with ZK yet on the
    # first try, surfacing as `NoNode for /hbase/master` when creating the
    # usertable. The downstream YCSB load then silently sees 0 operations,
    # which leaves the regionservers idle and the coordinator's CP-SAT in a
    # MODEL_INVALID feedback loop forcing GC every ~40 ms.
    local attempt
    for attempt in 1 2 3 4 5; do
        HBASE_OPTS="" "${HBASE_DIR}/bin/hbase" shell < "${HBASE_DIR}/test.txt" \
            > "${point_dir}/init.log" 2>&1
        if ! grep -qE "NoNode|ERROR:" "${point_dir}/init.log"; then
            return 0
        fi
        log "  Init tables attempt ${attempt} hit ZK error, retrying in 10s..."
        sleep 10
    done
    log_error "  HBase init failed after 5 attempts; see ${point_dir}/init.log"
    return 1
}

# ============================================================
# Common: YCSB load → warmup → run
# ============================================================

_run_ycsb() {
    local ir_rate="$1" point_dir="$2" ycsb_cpu_list="$3"

    local latency_file="${point_dir}/ycsb_latency.txt"
    local ycsb_output="${point_dir}/ycsb_output.txt"
    local ycsb_base_params="-p hosts=localhost -threads ${YCSB_THREADS}"

    log "YCSB load..."
    numactl -C "$ycsb_cpu_list" -m0 python3 "${YCSB_DIR}/bin/ycsb" load hbase2 -s \
        -P "${YCSB_DIR}/workloads/${YCSB_WORKLOAD}" ${ycsb_base_params} \
        -cp "${HBASE_DIR}/conf" -p table=usertable -p columnfamily=family \
        -p status.interval=10000 >> "$ycsb_output" 2>&1

    log "YCSB warmup (${YCSB_WARMUP_SEC}s)..."
    numactl -C "$ycsb_cpu_list" -m0 python3 "${YCSB_DIR}/bin/ycsb" run hbase2 -s \
        -P "${YCSB_DIR}/workloads/${YCSB_WORKLOAD}" ${ycsb_base_params} \
        -p maxexecutiontime=${YCSB_WARMUP_SEC} \
        -cp "${HBASE_DIR}/conf" -p table=usertable -p columnfamily=family \
        -p status.interval=10000 >> "$ycsb_output" 2>&1

    log "YCSB run (${YCSB_RUN_SEC}s, target=${ir_rate})..."
    numactl -C "$ycsb_cpu_list" -m0 python3 "${YCSB_DIR}/bin/ycsb" run hbase2 -s \
        -P "${YCSB_DIR}/workloads/${YCSB_WORKLOAD}" ${ycsb_base_params} \
        -p measurementtype=raw -p measurement.raw.output_file="${latency_file}" \
        -target "${ir_rate}" -p maxexecutiontime=${YCSB_RUN_SEC} \
        -cp "${HBASE_DIR}/conf" -p table=usertable -p columnfamily=family \
        -p status.interval=10000 >> "$ycsb_output" 2>&1

    local tput
    tput=$(grep '\[OVERALL\].*Throughput' "$ycsb_output" 2>/dev/null | tail -1 | awk -F, '{printf "%.0f", $3}')
    log "Throughput: ${tput:-N/A} ops/sec"
}

# ============================================================
# Baseline mode: single JVM per regionserver
# ============================================================

_run_hbase_baseline() {
    local ir_rate="$1" point_dir="$2"

    local ycsb_cpu_list
    ycsb_cpu_list=$(cpu_ycsb_client "$PCORE")

    local rs_script="self-regionserver-start"
    [ "${GC_NAME}" = "g1" ] && rs_script="g1gc-regionserver-start"

    local rpc_port
    rpc_port=$(random_port)
    local rs_port=$((RANDOM % 10000 + 16000))

    # Build baseline HBASE_OPTS via framework
    local unlock="-XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions"
    local gc_flags="${GC_FLAGS} -XX:-SnicGCHost \
        -XX:ParallelGCThreads=${PCORE} -XX:ConcGCThreads=${CCMT} \
        -XX:ShenandoahMaxSATBBufferFlushes=${SATB_FLUSHES:-5}"
    local snic_base="-XX:-SnicGCShareMemEnabled \
        -XX:SNICAddr=${DGC_ADDR} -XX:HostAddr=${DGC_HOST_ADDR} \
        -XX:RDMAPort=$(random_port 2000 60000) -XX:RPCPort=$(random_port)"

    _start_hbase_infra "$rpc_port" "$point_dir"

    # Start region servers with framework mode
    for ((rs = 0; rs < HOST_NUM; rs++)); do
        local cpu_list
        cpu_list=$(cpu_baseline "$PCORE" "$rs")
        local gc_log="${RUN_DIR}/logs/gc_${ir_rate}_rs${rs}.log"
        local gc_log_opt="-Xlog:gc*=info:file=${gc_log}:timemillis:filesize=100M"
        local heap_base_addresses=("0x500000000" "0x300000000" "0x100000000")

        export HBASE_OPTS="${gc_log_opt} ${unlock} ${gc_flags} ${snic_base} ${COMMON_FLAGS} \
            -XX:HeapBaseMinAddress=${heap_base_addresses[$rs]:-0x500000000} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m"
        export FRAMEWORK_CPU_LIST="$cpu_list"
        export FRAMEWORK_NUMA_NODE=0

        log "Starting RegionServer ${rs} (cores=${cpu_list})..."
        bash "${HBASE_DIR}/bin/${rs_script}" --config "${HBASE_DIR}/conf" \
            regionserver "${rpc_port}" "${CCMT}" "${CCMT}" "${PCORE}" "${HEAP_SIZE}" \
            "${rs}" "${COOR_RDMA_PORT:-9999}" "${CLIENT_CCMT:-4}" framework \
            -Dhbase.regionserver.port=$((rs_port + rs)) \
            -Dhbase.regionserver.info.port=$((rs_port + rs + 10)) start \
            > "${point_dir}/regionserver_${rs}.log" 2>&1 &
        log_cmd "regionserver_${rs}" "$!" "HBASE_OPTS='${HBASE_OPTS}' FRAMEWORK_CPU_LIST='${cpu_list}' bash ${HBASE_DIR}/bin/${rs_script} ... framework"
        wait_ready 5
    done

    _init_hbase_tables "$point_dir"
    _run_ycsb "$ir_rate" "$point_dir" "$ycsb_cpu_list"
}

# ============================================================
# DGC mode: Coordinator + Clients + Host RegionServers
# ============================================================

_run_hbase_dgc() {
    local ir_rate="$1" point_dir="$2"

    local ycsb_cpu_list
    ycsb_cpu_list=$(cpu_ycsb_client "$PCORE")
    local host_count="${HOST_COUNT:-$HOST_NUM}"

    # Port allocation
    RPC_PORT=$(random_port)
    RDMA_BASE_PORT=$(random_port 2000 60000)
    COOR_RDMA_PORT=$(random_port 2000 60000)
    local rs_port=$((RANDOM % 10000 + 16000))

    # Coordinator/client need a JAR to run — use specjbb2015.jar as dummy (same as legacy)
    local saved_bench_jar="${BENCH_JAR:-}"
    BENCH_JAR="${SPECJBB_DIR}/specjbb2015.jar"

    # ---- 1. Start DGC Coordinator ----
    if [ "${COORDINATOR:-false}" = "true" ]; then
        start_dgc_coordinator "$host_count"
    fi

    # ---- 2. Start DGC Clients ----
    for ((rs = 0; rs < host_count; rs++)); do
        start_dgc_client "$rs"
    done
    wait_ready 5

    # Restore BENCH_JAR
    BENCH_JAR="$saved_bench_jar"

    # ---- 3. Start HBase infra (ZK + Master) ----
    _start_hbase_infra "$RPC_PORT" "$point_dir"

    # ---- 4. Start RegionServers as DGC hosts ----
    for ((rs = 0; rs < host_count; rs++)); do
        local host_gc
        host_gc=$(build_dgc_host_flags "$rs")

        local cpu_list
        cpu_list=$(cpu_dgc_host "$PCORE" "$rs")
        local gc_log="${RUN_DIR}/logs/gc_${ir_rate}_rs${rs}.log"
        local gc_log_opt="-Xlog:gc*=info:file=${gc_log}:timemillis:filesize=100M"

        export HBASE_OPTS="${gc_log_opt} ${host_gc} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m"
        export FRAMEWORK_CPU_LIST="$cpu_list"
        export FRAMEWORK_NUMA_NODE=1

        log "Starting RegionServer ${rs} as DGC host (cores=${cpu_list})..."
        bash "${HBASE_DIR}/bin/self-regionserver-start" --config "${HBASE_DIR}/conf" \
            regionserver "${RPC_PORT}" "${CCMT}" "${CCMT}" "${PCORE}" "${HEAP_SIZE}" \
            "${rs}" "${COOR_RDMA_PORT}" "${CLIENT_CCMT:-4}" framework \
            -Dhbase.regionserver.port=$((rs_port + rs)) \
            -Dhbase.regionserver.info.port=$((rs_port + rs + 10)) start \
            > "${point_dir}/regionserver_${rs}.log" 2>&1 &
        log_cmd "regionserver_${rs}" "$!" "HBASE_OPTS='${HBASE_OPTS}' FRAMEWORK_CPU_LIST='${cpu_list}' bash self-regionserver-start ... framework"
        wait_ready 5
    done

    # ---- 5. Init + YCSB ----
    _init_hbase_tables "$point_dir"
    _run_ycsb "$ir_rate" "$point_dir" "$ycsb_cpu_list"
}
