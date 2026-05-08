#!/bin/bash
# workload-specjbb.sh — SPECjbb2015 benchmark runner
#
# Supports two modes:
#   COMPOSITE: single JVM per group (baseline tests)
#   MULTI: Controller + TxInjector + Backend per group (DGC tests)

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/cpu.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runner-dgc.sh"

run_specjbb() {
    local mode="$GC_MODE"
    local bench_jar
    bench_jar=$(resolve_var BENCH_JAR "$mode")
    [ -z "$bench_jar" ] && bench_jar="${BENCH_JAR_default}"

    local groups="${SPECJBB_GROUPS:-2}"
    local ctrl_port="${SPECJBB_CONTROLLER_PORT:-24000}"

    log "=== SPECjbb ${SPECJBB_MODE} / ${GC_NAME} (${mode}) ==="
    log "JAR: ${bench_jar}"
    log "Groups: ${groups}, Heap: ${HEAP_SIZE}m"
    if [ "${SPECJBB_MODE}" = "PRESET" ]; then
        log "IR: ${SPECJBB_IR}, Duration: ${SPECJBB_DURATION}ms"
    fi
    log ""

    # SPECjbb needs config/specjbb2015.props relative to cwd
    cd "${SPECJBB_DIR}" || { log_error "Cannot cd to ${SPECJBB_DIR}"; return 1; }

    # Timestamp marker: only collect results created after this point
    _SPECJBB_START_MARKER=$(mktemp "${RUN_DIR}/.specjbb_start_XXXXXX")

    # Cleanup: kill leftover SPECjbb and DGC processes, remove stale SHM files
    # Match both specjbb2015.jar and specjbb-output-latency-with-start.jar.
    # safe_pkill only kills processes whose `comm` is java or numactl, so
    # broadening the pattern can't take out the calling shell or ssh.
    safe_pkill "specjbb"
    safe_pkill "SnicGCClient"
    safe_pkill "SnicGCCoordinator"
    cleanup_shm
    sleep 2

    if [ "$mode" = "baseline" ]; then
        _run_specjbb_baseline "$bench_jar" "$groups" "$ctrl_port"
    else
        _run_specjbb_dgc "$bench_jar" "$groups" "$ctrl_port"
    fi
}

_run_specjbb_baseline() {
    local jar="$1" groups="$2" ctrl_port="$3"

    local shm_flag="${BASELINE_SHM_ENABLED:-false}"
    local gc_option="${GC_FLAGS} \
        -XX:ParallelGCThreads=${PCORE} \
        -XX:ConcGCThreads=${CCMT} \
        -XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions"
    if [ "$shm_flag" = "true" ]; then
        gc_option="${gc_option} -XX:+SnicGCShareMemEnabled"
    else
        gc_option="${gc_option} -XX:-SnicGCShareMemEnabled"
    fi

    if [ "${SPECJBB_MULTI:-false}" = "true" ]; then
        _run_specjbb_baseline_multi "$jar" "$groups" "$ctrl_port" "$gc_option"
    else
        _run_specjbb_baseline_composite "$jar" "$groups" "$ctrl_port" "$gc_option"
    fi
}

_run_specjbb_baseline_composite() {
    local jar="$1" groups="$2" ctrl_port="$3" gc_option="$4"

    local pids=()
    for ((gid = 0; gid < groups; gid++)); do
        local cpu_list
        cpu_list=$(cpu_baseline "$PCORE" "$gid")
        local gc_log="${RUN_DIR}/logs/gc_group${gid}.log"
        local out_file="${RUN_DIR}/logs/group${gid}.log"

        local props=""
        if [ "${SPECJBB_MODE}" = "PRESET" ]; then
            props="-Dspecjbb.controller.type=PRESET \
                -Dspecjbb.controller.preset.ir=${SPECJBB_IR} \
                -Dspecjbb.controller.preset.duration=${SPECJBB_DURATION}"
        fi

        log "Starting COMPOSITE group ${gid} (cores=${cpu_list})..."
        local _comp_cmd="numactl -C ${cpu_list} -m0 \
            ${JAVA} -Xlog:gc*=info:file=${gc_log}:timemillis \
            ${gc_option} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            ${props} \
            -jar ${jar} -m COMPOSITE -ikv"
        numactl -C "$cpu_list" -m0 \
            ${JAVA} -Xlog:gc*=info:file=${gc_log}:timemillis \
            ${gc_option} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            ${props} \
            -jar "$jar" -m COMPOSITE -ikv \
            > "$out_file" 2>&1 &
        pids+=($!)
        log_cmd "composite_${gid}" "${pids[-1]}" "$_comp_cmd"
        wait_ready 3
    done

    log "Waiting for all groups to finish..."
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    log "All groups done."

    _collect_specjbb_results
}

_run_specjbb_baseline_multi() {
    local jar="$1" groups="$2" ctrl_port="$3" gc_option="$4"

    # TxInjector + Backend pairs
    local backend_pids=()
    for ((gid = 0; gid < groups; gid++)); do
        local group_name="group$((gid + 1))"

        # TxInjector
        local txi_cpu
        txi_cpu=$(cpu_list_desc $((127 - gid * PCORE * 2)) "$PCORE")
        local txi_log="${RUN_DIR}/logs/txi_${gid}.log"

        log "Starting TxInjector ${gid} (cores=${txi_cpu})..."
        local _bl_txi_cmd="numactl -C ${txi_cpu} -m1 \
            ${JAVA} -Xlog:gc*=info:stdout:timemillis \
            ${gc_option} ${COMMON_FLAGS} \
            -Xmx${TXI_HEAP:-8192}m -Xms${TXI_HEAP:-8192}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar ${jar} -m TXINJECTOR -ikv -G=${group_name} -J=JVM1"
        numactl -C "$txi_cpu" -m1 \
            ${JAVA} -Xlog:gc*=info:stdout:timemillis \
            ${gc_option} ${COMMON_FLAGS} \
            -Xmx${TXI_HEAP:-8192}m -Xms${TXI_HEAP:-8192}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar "$jar" -m TXINJECTOR -ikv -G=${group_name} -J=JVM1 \
            > "$txi_log" 2>&1 &
        log_cmd "txi_${gid}" "$!" "$_bl_txi_cmd"
        wait_ready 3

        # Backend
        local be_cpu
        be_cpu=$(cpu_baseline "$PCORE" "$gid")
        local gc_log="${RUN_DIR}/logs/gc_backend_${gid}.log"
        local be_log="${RUN_DIR}/logs/backend_${gid}.log"

        local be_extra="${BASELINE_BACKEND_FLAGS:-}"
        [ -n "$be_extra" ] || be_extra="-XX:ShenandoahMaxSATBBufferFlushes=5 -XX:HeapBaseMinAddress=0x500000000"

        log "Starting Backend ${gid} (cores=${be_cpu})..."
        local _bl_be_cmd="numactl -C ${be_cpu} -m0 \
            ${JAVA} -Xlog:gc*=info:file=${gc_log}:timemillis \
            ${gc_option} ${be_extra} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar ${jar} -m BACKEND -ikv -G=${group_name} -J=JVM2 -v"
        numactl -C "$be_cpu" -m0 \
            ${JAVA} -Xlog:gc*=info:file=${gc_log}:timemillis \
            ${gc_option} ${be_extra} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar "$jar" -m BACKEND -ikv -G=${group_name} -J=JVM2 -v \
            > "$be_log" 2>&1 &
        backend_pids+=($!)
        log_cmd "backend_${gid}" "${backend_pids[-1]}" "$_bl_be_cmd"
    done
    wait_ready 5

    # Controller (background, starts first to accept connections)
    local ctrl_cpu
    ctrl_cpu=$(cpu_controller "$PCORE")
    local ctrl_log="${RUN_DIR}/logs/controller.log"

    local ctrl_props="-Dspecjbb.group.count=${groups} -Dspecjbb.controller.port=${ctrl_port}"
    if [ "${SPECJBB_MODE}" = "PRESET" ]; then
        ctrl_props="${ctrl_props} -Dspecjbb.controller.type=PRESET \
            -Dspecjbb.controller.preset.ir=${SPECJBB_IR} \
            -Dspecjbb.controller.preset.duration=${SPECJBB_DURATION}"
    fi

    log "Starting Controller..."
    local _bl_ctrl_cmd="numactl -C ${ctrl_cpu} -m1 \
        ${JAVA} ${gc_option} ${COMMON_FLAGS} \
        -Xmx4096m -Xms4096m \
        ${ctrl_props} \
        -jar ${jar} -m MULTICONTROLLER -ikv -l 3 -v"
    numactl -C "$ctrl_cpu" -m1 \
        ${JAVA} ${gc_option} ${COMMON_FLAGS} \
        -Xmx4096m -Xms4096m \
        ${ctrl_props} \
        -jar "$jar" -m MULTICONTROLLER -ikv -l 3 -v \
        > "$ctrl_log" 2>&1 &
    local ctrl_pid=$!
    log_cmd "controller" "$ctrl_pid" "$_bl_ctrl_cmd"

    # Watchdog: timeout only (do NOT kill on backend exit — backends exit before report finishes)
    # Honor SPECJBB_TIMEOUT_SEC env override (HBIR_RT auto-ramps and may take 30+ min,
    # the SPECJBB_DURATION-derived default of 360s would kill it prematurely).
    local timeout_sec
    if [ -n "${SPECJBB_TIMEOUT_SEC:-}" ]; then
        timeout_sec=${SPECJBB_TIMEOUT_SEC}
    else
        timeout_sec=$(( ${SPECJBB_DURATION:-60000} / 1000 * 4 + 120 ))
    fi
    (
        local elapsed=0
        while is_alive $ctrl_pid 2>/dev/null; do
            sleep 3
            elapsed=$((elapsed + 3))
            if [ $elapsed -ge $timeout_sec ]; then
                log_error "Watchdog: timeout after ${timeout_sec}s, killing controller"
                kill $ctrl_pid 2>/dev/null
                exit 2
            fi
        done
    ) &
    local watchdog_pid=$!

    wait $ctrl_pid 2>/dev/null || true
    local ctrl_exit=$?
    kill $watchdog_pid 2>/dev/null || true

    if [ $ctrl_exit -eq 0 ]; then
        log "Controller finished successfully."
    else
        log_error "Controller exited with code ${ctrl_exit}"
    fi

    # Wait for backends
    for pid in "${backend_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    _collect_specjbb_results
}

_run_specjbb_dgc() {
    local jar="$1" groups="$2" ctrl_port="$3"

    RPC_PORT=$(random_port)
    RDMA_BASE_PORT=$(random_port 2000 60000)
    COOR_RDMA_PORT=$(random_port 2000 60000)
    BENCH_JAR="$jar"
    HOST_COUNT="$groups"
    trap_dgc_cleanup

    # 1. Coordinator
    if [ "${COORDINATOR:-false}" = "true" ]; then
        start_dgc_coordinator "$groups"
    fi

    # 2. DGC Clients
    for ((gid = 0; gid < groups; gid++)); do
        start_dgc_client "$gid"
    done
    wait_ready 5

    # 3. Controller (background, starts first to accept connections)
    local ctrl_cpu
    ctrl_cpu=$(cpu_controller "$PCORE")
    local ctrl_log="${RUN_DIR}/logs/controller.log"
    local ctrl_gc="${GC_FLAGS} -XX:-SnicGCHost \
        -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 \
        ${SNIC_FLAGS} -XX:ShenandoahMaxSATBBufferFlushes=5"

    local ctrl_props="-Dspecjbb.group.count=${groups} -Dspecjbb.controller.port=${ctrl_port}"
    if [ "${SPECJBB_MODE}" = "PRESET" ]; then
        ctrl_props="${ctrl_props} -Dspecjbb.controller.type=PRESET \
            -Dspecjbb.controller.preset.ir=${SPECJBB_IR} \
            -Dspecjbb.controller.preset.duration=${SPECJBB_DURATION}"
    fi

    log "Starting Controller..."
    local _ctrl_cmd="numactl -C ${ctrl_cpu} -m1 \
        ${JAVA} ${ctrl_gc} ${COMMON_FLAGS} \
        -Xmx4096m -Xms4096m \
        ${ctrl_props} \
        -jar ${jar} -m MULTICONTROLLER -ikv -l 3 -v"
    numactl -C "$ctrl_cpu" -m1 \
        ${JAVA} ${ctrl_gc} ${COMMON_FLAGS} \
        -Xmx4096m -Xms4096m \
        ${ctrl_props} \
        -jar "$jar" -m MULTICONTROLLER -ikv -l 3 -v \
        > "$ctrl_log" 2>&1 &
    local ctrl_pid=$!
    log_cmd "controller" "$ctrl_pid" "$_ctrl_cmd"
    wait_ready 3

    # 4. TxInjector + Backend pairs
    local backend_pids=()
    for ((gid = 0; gid < groups; gid++)); do
        local group_name="group$((gid + 1))"
        local host_rpc_port=$((RPC_PORT + gid))
        local rdma_port=$((${RDMA_BASE_PORT:-2340} + gid))

        # TxInjector
        local txi_cpu
        txi_cpu=$(cpu_list_desc $((127 - gid * PCORE * 2)) "$PCORE")
        local txi_log="${RUN_DIR}/logs/txi_${gid}.log"
        local txi_gc="${GC_FLAGS} -XX:-SnicGCHost \
            -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 \
            ${SNIC_FLAGS} -XX:ShenandoahMaxSATBBufferFlushes=5"

        log "Starting TxInjector ${gid}..."
        local _txi_cmd="numactl -C ${txi_cpu} -m1 \
            ${JAVA} -Xlog:gc*=info:stdout:timemillis \
            ${txi_gc} ${COMMON_FLAGS} \
            -Xmx${TXI_HEAP:-8192}m -Xms${TXI_HEAP:-8192}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar ${jar} -m TXINJECTOR -ikv -G=${group_name} -J=JVM1"
        numactl -C "$txi_cpu" -m1 \
            ${JAVA} -Xlog:gc*=info:stdout:timemillis \
            ${txi_gc} ${COMMON_FLAGS} \
            -Xmx${TXI_HEAP:-8192}m -Xms${TXI_HEAP:-8192}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar "$jar" -m TXINJECTOR -ikv -G=${group_name} -J=JVM1 \
            > "$txi_log" 2>&1 &
        log_cmd "txi_${gid}" "$!" "$_txi_cmd"
        wait_ready 3

        # Backend
        local be_cpu
        be_cpu=$(cpu_dgc_host "$PCORE" "$gid")
        local be_gc_log="${RUN_DIR}/logs/gc_backend_${gid}.log"
        local be_log="${RUN_DIR}/logs/backend_${gid}.log"
        local be_gc
        be_gc=$(build_dgc_host_flags "$gid")

        log "Starting Backend ${gid} (cores=${be_cpu})..."
        local _be_cmd="numactl -C ${be_cpu} -m1 \
            ${JAVA} -Xlog:gc*=info:file=${be_gc_log}:timemillis \
            ${be_gc} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar ${jar} -m BACKEND -ikv -G=${group_name} -J=JVM2 -v"
        numactl -C "$be_cpu" -m1 \
            ${JAVA} -Xlog:gc*=info:file=${be_gc_log}:timemillis \
            ${be_gc} ${COMMON_FLAGS} \
            -Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m \
            -Dspecjbb.controller.port=${ctrl_port} \
            -jar "$jar" -m BACKEND -ikv -G=${group_name} -J=JVM2 -v \
            > "$be_log" 2>&1 &
        backend_pids+=($!)
        log_cmd "backend_${gid}" "${backend_pids[-1]}" "$_be_cmd"

        # Check backend survived startup
        sleep 3
        if ! is_alive "${backend_pids[-1]}"; then
            log_error "Backend ${gid} died during startup! Check ${be_log}"
            tail -3 "$be_log" >&2
            kill $ctrl_pid 2>/dev/null || true
            cleanup_dgc
            return 1
        fi
    done
    wait_ready 5

    # 5. Monitor: background watchdog kills controller ONLY on timeout.
    # Do NOT kill controller when backends exit — in MULTI PRESET mode, backends
    # exit normally after the test while the controller continues generating reports.
    # Killing the controller here causes RT curve files to not be generated.
    # Honor SPECJBB_TIMEOUT_SEC env override for HBIR_RT mode (which auto-ramps
    # and can run 30+ min — the default 360s would cut it short).
    local timeout_sec
    if [ -n "${SPECJBB_TIMEOUT_SEC:-}" ]; then
        timeout_sec=${SPECJBB_TIMEOUT_SEC}
    else
        timeout_sec=$(( ${SPECJBB_DURATION:-60000} / 1000 * 4 + 120 ))
    fi
    (
        local elapsed=0
        while is_alive $ctrl_pid 2>/dev/null; do
            sleep 3
            elapsed=$((elapsed + 3))
            if [ $elapsed -ge $timeout_sec ]; then
                log_error "Watchdog: timeout after ${timeout_sec}s, killing controller"
                kill $ctrl_pid 2>/dev/null
                exit 2
            fi
        done
    ) &
    local watchdog_pid=$!

    # Block on controller
    wait $ctrl_pid 2>/dev/null || true
    local ctrl_exit=$?
    kill $watchdog_pid 2>/dev/null || true

    if [ $ctrl_exit -eq 0 ]; then
        log "Controller finished successfully."
    else
        log_error "Controller exited with code ${ctrl_exit}"
        # Check which backend died
        for ((gid = 0; gid < ${#backend_pids[@]}; gid++)); do
            if ! is_alive "${backend_pids[$gid]}" 2>/dev/null; then
                log_error "  Backend ${gid} is dead. Check ${RUN_DIR}/logs/backend_${gid}.log"
            fi
        done
    fi

    # Wait for all backend processes to exit. In MULTI mode, the reporter runs
    # inside a backend JVM (not as a separate process), so we must wait for
    # backends to finish generating RT curve files before collecting results.
    log "Waiting for backends to finish reporting..."
    for ((gid = 0; gid < ${#backend_pids[@]}; gid++)); do
        wait "${backend_pids[$gid]}" 2>/dev/null || true
    done
    log "All backends exited."
    # Also wait for any remaining specjbb java processes (TxInjectors, etc.)
    local _wait=0
    while pgrep -f "specjbb2015" >/dev/null 2>&1 && [ $_wait -lt 30 ]; do
        sleep 1; _wait=$((_wait + 1))
    done
    [ $_wait -gt 0 ] && log "Remaining SPECjbb processes exited after ${_wait}s"

    _collect_specjbb_results
    cleanup_dgc
}

# ============================================================
# Result collection & summary
# ============================================================

_collect_specjbb_results() {
    mkdir -p "${RUN_DIR}/raw"

    # Find specjbb result dirs created after our start marker
    local collected=false
    if [ -f "${_SPECJBB_START_MARKER:-}" ]; then
        local new_results
        new_results=$(find result/ -maxdepth 1 -name 'specjbb2015-*' -type d -newer "$_SPECJBB_START_MARKER" 2>/dev/null)
        if [ -n "$new_results" ]; then
            for d in $new_results; do
                cp -r "$d" "${RUN_DIR}/raw/"
                log "Collected result: $(basename "$d")"
                collected=true
            done
        fi
        rm -f "$_SPECJBB_START_MARKER"
    else
        # Fallback: grab the latest one
        local latest_result
        latest_result=$(ls -dt result/specjbb2015-*/ 2>/dev/null | head -1)
        if [ -n "$latest_result" ]; then
            cp -r "$latest_result" "${RUN_DIR}/raw/"
            log "Collected result: $(basename "$latest_result")"
            collected=true
        fi
    fi

    if [ "$collected" = false ]; then
        log_error "WARNING: No SPECjbb report directory found for this run!"
        log_error "  SPECjbb may have exited before generating a report."
        log_error "  Check controller.log for details."
    fi

    # Print RT summary from report
    local rt_file
    rt_file=$(find "${RUN_DIR}/raw" -name '*overall-throughput-rt.txt' 2>/dev/null | head -1)
    if [ -n "$rt_file" ]; then
        log "=== RT Summary ==="
        while IFS=';' read -r iter min med p90 p95 p99 max; do
            [[ "$iter" == *Iteration* || "$iter" == *===* ]] && continue
            [ -z "$iter" ] && continue
            log "  iter ${iter%.*}: p50=$(printf '%.0f' "$med")us  p90=$(printf '%.0f' "$p90")us  p95=$(printf '%.0f' "$p95")us  p99=$(printf '%.0f' "$p99")us  max=$(printf '%.0f' "$max")us"
        done < "$rt_file"
    fi

    # Print GC stats
    local gc_logs
    gc_logs=$(find "${RUN_DIR}/logs" -name 'gc_*.log' 2>/dev/null)
    if [ -n "$gc_logs" ]; then
        for gc_log in $gc_logs; do
            local name=$(basename "$gc_log" .log)
            local degen=$(grep -c 'Pause Degenerated' "$gc_log" 2>/dev/null; true)
            local full=$(grep -c 'Pause Full' "$gc_log" 2>/dev/null; true)
            [ "$degen" -gt 0 ] || [ "$full" -gt 0 ] && log "  ${name}: degen=${degen} full=${full}"
        done
    fi

    # Steady-state analysis (per backend)
    local analyze_script
    analyze_script="$(dirname "${BASH_SOURCE[0]}")/analyze-specjbb.sh"
    if [ -x "$analyze_script" ] && [ "${SPECJBB_MODE}" = "PRESET" ]; then
        local groups="${SPECJBB_GROUPS:-2}"
        for ((gid = 0; gid < groups; gid++)); do
            log ""
            log "=== Steady-State Analysis: Backend ${gid} ==="
            "$analyze_script" "$RUN_DIR" "$gid" 2>&1 | while IFS= read -r line; do log "  $line"; done
        done
    fi
}
