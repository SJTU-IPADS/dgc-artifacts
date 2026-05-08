#!/bin/bash
# workload-dacapo.sh — DaCapo benchmark runner (H2, DayTrader)
#
# Handles both baseline (single JVM) and DGC (multi-process) modes.
# DGC orchestration: Coordinator → Clients → Hosts (all run the same JAR)

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/cpu.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runner-baseline.sh"
source "$(dirname "${BASH_SOURCE[0]}")/isolation.sh"

# _select_spring_requests_variant — swap spring's requests.txt based on
# the DACAPO_REQUESTS_VARIANT env/conf var. Needed because spring has
# multiple owner-count data generations (stock 8k; big 80k), and the
# requests.txt line N references a SPECIFIC (owner, pet) pair that only
# matches if requests.txt was generated with the SAME RNG seed as data.sql.
#
# Keying by DACAPO_SIZE doesn't work because spring.cnf has no "big" size
# (we don't inject one — spring-big uses the default-size cnf with
# overridden args[0] → requests count, not size-name). So we need a
# separate control knob.
#
# Variants live alongside the active requests.txt:
#   requests.txt.stock   — 8192 owners × 8 sessions × 2 rows
#   requests.txt.big     — matches data-80000.sql (80000 owners × ...)
# Setup: one-time per machine:
#   cp requests.txt.orig requests.txt.stock
#   cp /path/to/requests-80000.txt requests.txt.big
# Usage in conf/workloads/*.conf:
#   DACAPO_REQUESTS_VARIANT="big"     # for spring-big
#   (unset/default "stock"            # for spring)
# If the variant file is missing, logs WARNING and leaves current file.
_select_spring_requests_variant() {
    [ "$BENCH_NAME" = "spring" ] || return 0
    local req="${DACAPO_DIR}/dacapo-23.11-chopin/dat/spring/requests.txt"
    local variant="${DACAPO_REQUESTS_VARIANT:-stock}"
    local src="${req}.${variant}"
    if [ -f "$src" ]; then
        if ! cmp -s "$src" "$req" 2>/dev/null; then
            cp -f "$src" "$req" && log "spring: switched requests.txt to variant '${variant}'"
        fi
    elif [ "$variant" != "stock" ]; then
        log "WARNING: spring requests.txt variant '${variant}' not found at $src (running with whatever requests.txt currently holds)"
    fi
}

run_dacapo() {
    local mode="$GC_MODE"   # baseline or dgc
    local bench_jar
    bench_jar=$(resolve_var BENCH_JAR "$mode")

    # -t N for DaCapo workload = PCORE (pinned-core count). Until 2026-04-24
    # confs set TERMINAL_NUM=8 across the board, so a baseline with
    # PCORE_baseline=10 ended up with 8 app threads on 10 cores — 2 idle
    # cores that GC could still use, asymmetrically favoring baseline over
    # DGC (whose 8-host-core config naturally had PCORE==-t==8).
    # Now: -t always equals PCORE so every pinned core has an app thread.
    # TERMINAL_NUM in workload conf is ignored (kept for tooling that greps
    # it, but no longer authoritative). Override via TERMINAL_NUM_OVERRIDE
    # env var if a specific run really needs over/under-subscription.
    local terminal_num="${TERMINAL_NUM_OVERRIDE:-${PCORE:-8}}"

    local dacapo_size
    dacapo_size=$(resolve_var DACAPO_SIZE "$mode")

    # Spring-specific: pick the correct requests.txt variant based on DACAPO_SIZE
    _select_spring_requests_variant

    local iterations="${ITERATIONS:-5}"
    local extra_flags="${DACAPO_EXTRA_FLAGS:---no-validation --latency-csv}"
    local throttle_flag="${DACAPO_THROTTLE_FLAG:-}"

    log "=== DaCapo ${BENCH_NAME} / ${GC_NAME} (${mode}) ==="
    log "JAR: ${bench_jar}"
    log "Heap: ${HEAP_SIZE}m, Terminals: ${terminal_num}, Iterations: ${iterations}"
    local throttle_list="${THROTTLES:-nothrottle}"
    log "Throttle points: ${throttle_list}"
    log ""

    local overall_fail=0
    for throttle in ${throttle_list}; do
        log "--- Throttle: ${throttle} ---"

        local point_dir="${RUN_DIR}/raw/${throttle}"
        mkdir -p "$point_dir"

        # Cleanup from previous run of THIS benchmark instance only. In a
        # parallel-sweep (two tracks running the same BENCH_NAME on separate
        # NUMAs, e.g. par-shm vs par-rdma), the bench-name-scoped patterns
        # below match the sibling track's processes too and cause cross-track
        # kills. SHM_PREFIX is guaranteed unique per track (baked into JVM
        # -XX:SnicShmMemPath=/${SHM_PREFIX}_... flags), so prefer it when set.
        if [ -n "${SHM_PREFIX:-}" ] && [ "${SHM_PREFIX}" != "default" ]; then
            safe_pkill "${SHM_PREFIX}_"
        else
            safe_pkill "dacapo.*${BENCH_NAME}"
            safe_pkill "Xmx${HEAP_SIZE}m.*${BENCH_NAME}"
            safe_pkill "SnicGCClient.*${BENCH_NAME}"
            safe_pkill "SnicGCCoordinator.*${BENCH_NAME}"
        fi
        cleanup_shm
        sleep 2

        if [ "$mode" = "baseline" ]; then
            _run_dacapo_baseline "$bench_jar" "$throttle" "$terminal_num" \
                "$dacapo_size" "$iterations" "$extra_flags" "$throttle_flag" "$point_dir" \
                || overall_fail=1
        else
            _run_dacapo_dgc "$bench_jar" "$throttle" "$terminal_num" \
                "$dacapo_size" "$iterations" "$extra_flags" "$throttle_flag" "$point_dir" \
                || overall_fail=1
        fi

        log "Throttle ${throttle} done."
        echo ""
    done
    return $overall_fail
}

# ============================================================
# Baseline mode: 1 or N co-running JVMs (HOST_COUNT, default 1)
# ============================================================

_run_dacapo_baseline() {
    local jar="$1" throttle="$2" terminals="$3" size="$4"
    local iterations="$5" extra_flags="$6" throttle_flag="$7" point_dir="$8"

    local host_count="${HOST_COUNT:-1}"

    # Throttle plumbing — empirically determined (2026-05-06 on ds00):
    #   * The JAR honours only -Ddacapo.throttle (verified by `strings` on the
    #     class files; -Ddacapo.daytrader.issue.rate is silently ignored).
    #   * -Ddacapo.throttle=N is PER-THREAD: with -t T threads achieved rps = N×T.
    # When DACAPO_THROTTLE_FLAG is set in conf, treat THROTTLES values as JVM-
    # global rps and divide by terminals before passing to -Ddacapo.throttle.
    # When unset (h2 path), pass throttle as-is (per-thread already).
    local throttle_pt="$throttle"
    if [ -n "$throttle_flag" ] && [ "$throttle" != "nothrottle" ]; then
        throttle_pt=$(( throttle / terminals ))
        [ "$throttle_pt" -lt 1 ] && throttle_pt=1
    fi

    local host_pids=()
    local host_logs=()

    for ((hid = 0; hid < host_count; hid++)); do
        local base_cmd
        base_cmd=$(run_baseline_single "$throttle" "$hid")

        local testcase="-Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m"
        # Plumb workload-specific JVM flags (e.g. -Ddacapo.daytrader.logIterSessions)
        # set via DACAPO_JVM_EXTRA_FLAGS in conf/workloads/*.conf. Must come before
        # -jar so JVM parses them (not DaCapo's CLI).
        [ -n "${DACAPO_JVM_EXTRA_FLAGS:-}" ] && testcase="${testcase} ${DACAPO_JVM_EXTRA_FLAGS}"
        if [ "$throttle" != "nothrottle" ]; then
            testcase="${testcase} -Ddacapo.throttle=${throttle_pt}"
        fi
        testcase="${testcase} -jar ${jar} ${BENCH_NAME} -n ${iterations} ${extra_flags}"
        testcase="${testcase} --log-directory ${point_dir} -t ${terminals}"
        [ -n "$size" ] && testcase="${testcase} -s ${size}"
        if [ "$throttle" != "nothrottle" ]; then
            [ -n "$throttle_flag" ] || testcase="${testcase} --throttle ${throttle_pt}"
        fi
        mkdir -p "${RUN_DIR}/tmp/scratch${hid}" "${point_dir}/host${hid}"
        testcase="${testcase} --scratch-directory ${RUN_DIR}/tmp/scratch${hid}"
        # Override log-directory so the two hosts do not overwrite each other's latency CSV
        testcase=$(echo "$testcase" | sed "s|--log-directory ${point_dir}|--log-directory ${point_dir}/host${hid}|")

        # Per-benchmark port offset (mirror DGC path)
        case "$BENCH_NAME" in
          tradesoap|tradebeans)
            # (hid+1)*100 not hid*100: DaCapoServerRunner.shutdown() execs
            # jboss-cli.sh --connect which hardcodes localhost:9990. With hid*100,
            # host_0 binds management on 9990 and host_1's shutdown CLI kills
            # host_0 when the test finishes. Shifting both off 9990 makes the
            # CLI fail silently (no listener). See FIXES_DS00.md 修 4.
            testcase="-Djboss.socket.binding.port-offset=$(((hid + 1) * 100)) ${testcase}" ;;
          tomcat)
            testcase="-Ddacapo.tomcat.port=$((8080 + hid * 100)) ${testcase}" ;;
          spring)
            testcase="-Dserver.port=$((8080 + hid * 100)) ${testcase}" ;;
        esac

        local log_file="${point_dir}/host_${hid}.log"
        host_logs+=("$log_file")
        local full_cmd="${base_cmd} ${testcase}"

        echo "Commandline: ${full_cmd}" > "$log_file"
        eval "$full_cmd" >> "$log_file" 2>&1 &
        local pid=$!
        host_pids+=("$pid")
        log_cmd "baseline_host${hid}" "$pid" "$full_cmd"
    done

    _wait_dacapo_hosts host_pids host_logs "$iterations"

    local any_fail=0
    for ((hid = 0; hid < host_count; hid++)); do
        _check_dacapo_result "${host_logs[$hid]}" "$point_dir" "$iterations" "host${hid}" || any_fail=1
        local simple_csv="${point_dir}/host${hid}/dacapo-latency-usec-simple-$((iterations - 1)).csv"
        local metered_csv="${point_dir}/host${hid}/dacapo-latency-usec-metered-$((iterations - 1)).csv"
        [ -f "$simple_csv" ] && mv "$simple_csv" "${point_dir}/latency_host${hid}.csv"
        [ -f "$metered_csv" ] && mv "$metered_csv" "${point_dir}/latency_metered_host${hid}.csv"
    done
    return $any_fail
}

# ============================================================
# DGC mode: Coordinator + Clients + Hosts
# ============================================================

_run_dacapo_dgc() {
    local jar="$1" throttle="$2" terminals="$3" size="$4"
    local iterations="$5" extra_flags="$6" throttle_flag="$7" point_dir="$8"

    # Same global→per-thread division as in _run_dacapo_baseline; see comment there.
    local throttle_pt="$throttle"
    if [ -n "$throttle_flag" ] && [ "$throttle" != "nothrottle" ]; then
        throttle_pt=$(( throttle / terminals ))
        [ "$throttle_pt" -lt 1 ] && throttle_pt=1
    fi

    local host_count="${HOST_COUNT:-2}"
    local rpc_port
    rpc_port=$(random_port)
    local coor_rdma_port
    coor_rdma_port=$(random_port 2000 60000)
    local client_ccmt="${CLIENT_CCMT:-8}"
    local transport="${DGC_TRANSPORT:-shm}"

    # Per-role log levels. Only the DGC client honours DGC_CLIENT_LOG_LEVEL;
    # host/coord stay at info because their gc=debug output is enormous
    # (host_*.log can balloon to 6+ GB per iter at debug).
    local log_opt="-Xlog:gc*=info:stdout:timemillis"
    local client_log_opt="-Xlog:gc*=${DGC_CLIENT_LOG_LEVEL:-info}:stdout:timemillis"
    local dgc_pids=()

    # SNIC base flags (differs between shm and rdma)
    local snic_base="-XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions"
    if [ "$transport" = "shm" ]; then
        snic_base="${snic_base} -XX:+SnicGCShareMemEnabled"
    else
        snic_base="${snic_base} -XX:-SnicGCShareMemEnabled"
    fi
    local rdma_base_port
    rdma_base_port=$(random_port 2000 60000)
    snic_base="${snic_base} -XX:RDMAPort=${rdma_base_port}"
    snic_base="${snic_base} -XX:SNICAddr=${DGC_ADDR} -XX:HostAddr=${DGC_HOST_ADDR}"
    snic_base="${snic_base} -XX:-ShenandoahVerify"

    # Workload flags for coordinator and clients (no latency measurement)
    local coor_workload="-Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m -jar ${jar} ${BENCH_NAME} -n ${iterations} --no-validation"

    # ---- 1. Coordinator (background, skip for AlwaysDGC) ----
    if [ "${ALWAYS_DGC:-false}" = "true" ]; then
        log "[1/3] Skipping Coordinator (AlwaysDGC mode)..."
    else
        log "[1/3] Starting Coordinator..."
        local coor_log="${point_dir}/coordinator.log"
        # workload-dacapo.sh builds its own coor cmdline (does not call
        # start_dgc_coordinator from runner-dgc.sh), so the COOR_FLAGS
        # set in conf/gc/dgc-{shm,rdma}.conf doesn't reach this path.
        # Default-on the adaptive controller here too — to disable, append
        # -XX:-SnicCoorAdaptiveCCMT via COOR_EXTRA_FLAGS env (last wins).
        local coor_gc="${snic_base} -XX:+SnicGCCoorHeuristic -XX:SnicGCCoorSHMPath=$(shm_path_coor)"
        coor_gc="${coor_gc} -XX:SnicShmGlobalPacerPath=$(shm_path_pacer)"
        coor_gc="${coor_gc} -XX:SnicGCCoorClientNum=${host_count} -XX:+SnicGCCoordinator"
        coor_gc="${coor_gc} -XX:+SnicCoorAdaptiveCCMT"
        coor_gc="${coor_gc} -XX:SnicAvgMarkTimeAmplifyRate=2.0 -XX:RDMAPortForCoor=${coor_rdma_port}"
        if [ -n "${COOR_EXTRA_FLAGS:-}" ]; then
            coor_gc="${coor_gc} ${COOR_EXTRA_FLAGS}"
        fi
        # Coord runs CP-SAT on its own small heap. Use COORD_CCMT (default 4)
        # NOT client_ccmt — the two used to share a variable, which silently
        # crashed coord at CLIENT_CCMT > 4 on small-heap workloads.
        # See KNOWN_ISSUES.md §14.
        local coord_ccmt="${COORD_CCMT:-4}"
        # ParallelGCThreads on coord doubles as CP-SAT cumulative capacity
        # (snicCoordinator.cc:443: int C = ParallelGCThreads). Must be ≥ the
        # max marker demand (R) in SNIC_COOR_CCMT_ARGS, or DGC plans become
        # infeasible and CP-SAT silently picks fallback only. Tie it to
        # client_ccmt (= marker count). ConcGCThreads stays at COORD_CCMT
        # so coord's own concurrent GC remains lightweight.
        coor_gc="${coor_gc} -XX:ConcGCThreads=${coord_ccmt} -XX:ParallelGCThreads=${client_ccmt}"
        coor_gc="${coor_gc} -XX:SnicGCEstimatedRDMACopyTime=60 -XX:SnicCoorFrozenDGCUpperBound=50"
        # SnicCoorHostAddrPortList only exists in region-reclaim-2+ branches
        if [ "$transport" = "rdma" ] && ${JAVA} -XX:+UnlockExperimentalVMOptions -XX:+PrintFlagsFinal -version 2>&1 | grep -q SnicCoorHostAddrPortList; then
            coor_gc="${coor_gc} '-XX:SnicCoorHostAddrPortList='"
        fi

        local _coor_cmd="${JAVA} ${log_opt} ${coor_gc} ${COMMON_FLAGS} ${coor_workload}"
        eval "$_coor_cmd" &> "$coor_log" &
        dgc_pids+=($!)
        log_cmd "coordinator" "${dgc_pids[-1]}" "$_coor_cmd"
        sleep 5
        if ! is_alive "${dgc_pids[-1]}"; then
            log_error "Coordinator died during startup! Check ${coor_log}"
            tail -3 "$coor_log" >&2
            return 1
        fi
    fi

    # ---- 2. DGC Clients (background, one per host) ----
    log "[2/3] Starting ${host_count} DGC Clients..."
    for ((hid = 0; hid < host_count; hid++)); do
        local client_log="${point_dir}/client_${hid}.log"
        local client_rpc=$((rpc_port + hid))
        local client_rdma=$((rdma_base_port + hid))

        # Client CPU pool: a SINGLE shared pool of N cores (default 8 = paper's
        # "8 client cores" convention). All host_count clients pin to the SAME
        # N CPUs, stacking 2:1 when host_count=2 (coord's CP-SAT must ensure
        # only one client is actively marking at a time — see KNOWN_ISSUES §14
        # note on coord scheduling).
        #
        # Before 2026-04-24 the pool was `client_ccmt × host_count` which
        # linearly grew with host_count — e.g. HOST_COUNT=2 with CLIENT_CCMT=8
        # gave 16 client cores (over-allocating vs paper's 8), breaking
        # fair-core-budget comparisons. CLIENT_POOL_CORES env var (default 8)
        # now controls the pool size regardless of host_count.
        local client_pool_cores="${CLIENT_POOL_CORES:-8}"
        local client_cpu
        if [ -n "${PAR_CLIENT_CPU:-}" ]; then
            client_cpu="${PAR_CLIENT_CPU}"
        else
            client_cpu=$(cpu_list_desc 127 ${client_pool_cores})
        fi

        # Client's OWN GC threads — for its tiny heap's GC, not for marking
        # the host's heap. That's ShmClientMarkerNum below (= client_ccmt).
        # CLIENT_OWN_CCMT default 4; tie to CLIENT_CCMT only if you really
        # want over-subscription of client's own GC.
        local client_own_ccmt="${CLIENT_OWN_CCMT:-4}"
        local client_gc="-XX:+UseShenandoahGC -XX:+SnicGCClient"
        client_gc="${client_gc} -XX:ParallelGCThreads=${client_own_ccmt} -XX:ConcGCThreads=${client_own_ccmt}"
        client_gc="${client_gc} -XX:-UseDynamicNumberOfGCThreads ${snic_base}"
        client_gc="${client_gc} -XX:+SnicConcCopyRegion -XX:RPCPort=${client_rpc} -XX:SnicGCHostNum=1"
        client_gc="${client_gc} $(build_shm_flags_client $hid)"
        client_gc="${client_gc} -XX:RDMAPort=${client_rdma}"
        if [ "${ALWAYS_DGC:-false}" = "true" ]; then
            client_gc="${client_gc} -XX:-SnicGCCoorHeuristic"
        else
            client_gc="${client_gc} -XX:+SnicGCCoorHeuristic -XX:SnicGCCoorSHMPath=$(shm_path_coor)"
            client_gc="${client_gc} -XX:SnicGCCoorClientNum=${host_count} -XX:SnicGCCoorClientId=${hid}"
        fi

        # Transport-specific client flags
        if [ "$transport" = "shm" ]; then
            client_gc="${client_gc} -XX:ShmClientMarkerNum=${client_ccmt}"
            client_gc="${client_gc} -XX:SnicGCEstimatedRDMACopyTime=60"
        else
            client_gc="${client_gc} -XX:SnicSATBRootsSplitPartNum=3"
            client_gc="${client_gc} -XX:SNICTransRegionGroupNum=200"
            client_gc="${client_gc} -XX:SnicSATBRootsForceDeltaNum=5"
            client_gc="${client_gc} -XX:DpuClientMarkerNum=${client_ccmt}"
            client_gc="${client_gc} -XX:SnicGCEstimatedRDMACopyTime=60"
            # SnicGCRDMABatchFetchKlass: opt-in workaround for the mlx5 fw
            # 32.42.1000 LOC_QP_OP_ERR bug on the per-class RPC 6 path.
            # Enabled per-workload via SNIC_RDMA_BATCH_FETCHKLASS_dgc=true
            # (see tradesoap/tradebeans/spring confs).
            local _batch_fk=$(resolve_var SNIC_RDMA_BATCH_FETCHKLASS dgc)
            if [ "$_batch_fk" = "true" ]; then
                client_gc="${client_gc} -XX:+SnicGCRDMABatchFetchKlass"
            fi
        fi

        # setarch -R: disable ASLR so the client's mmap targets (dictated by
        # the host over RPC) land at predictable addresses.
        # PAR_CLIENT_NUMA env var overrides -m1 for parallel-sweep mode.
        local _client_numa="${PAR_CLIENT_NUMA:-1}"
        local _client_cmd="setarch \$(uname -m) -R numactl -C ${client_cpu} -m${_client_numa} ${JAVA} ${client_log_opt} ${client_gc} ${COMMON_FLAGS} ${coor_workload}"
        eval "$_client_cmd" &> "$client_log" &
        dgc_pids+=($!)
        log_cmd "dgc_client_${hid}" "${dgc_pids[-1]}" "$_client_cmd"
        sleep 5
        if ! is_alive "${dgc_pids[-1]}"; then
            log_error "Client ${hid} died during startup! Check ${client_log}"
            tail -3 "$client_log" >&2
            return 1
        fi
    done

    # ---- 3. Host JVMs with workload (all in background) ----
    log "[3/3] Starting ${host_count} Host JVMs (${transport} mode)..."

    local host_pids=()
    local host_logs=()
    for ((hid = 0; hid < host_count; hid++)); do
        local host_log="${point_dir}/host_${hid}.log"
        host_logs+=("$host_log")
        local host_rpc=$((rpc_port + hid))
        local host_rdma=$((rdma_base_port + hid))

        # PAR_HOST_CPU_0 / PAR_HOST_CPU_1 env vars override per-host CPU lists
        # for parallel-sweep mode (physical cores only).
        local host_cpu
        local _par_host_cpu_var="PAR_HOST_CPU_${hid}"
        if [ -n "${!_par_host_cpu_var:-}" ]; then
            host_cpu="${!_par_host_cpu_var}"
        else
            host_cpu=$(cpu_dgc_host "$PCORE" "$hid")
        fi

        # Host workload flags (with latency measurement)
        local host_workload="-Xmx${HEAP_SIZE}m -Xms${HEAP_SIZE}m"
        # Plumb workload-specific JVM flags (e.g. -Ddacapo.daytrader.logIterSessions)
        # set via DACAPO_JVM_EXTRA_FLAGS in conf/workloads/*.conf.
        [ -n "${DACAPO_JVM_EXTRA_FLAGS:-}" ] && host_workload="${host_workload} ${DACAPO_JVM_EXTRA_FLAGS}"
        if [ "$throttle" != "nothrottle" ]; then
            host_workload="${host_workload} -Ddacapo.throttle=${throttle_pt}"
        fi
        host_workload="${host_workload} -jar ${jar} ${BENCH_NAME} -n ${iterations} --no-validation --latency-csv"
        host_workload="${host_workload} --log-directory ${point_dir}/host${hid} -t ${terminals}"
        [ -n "$size" ] && host_workload="${host_workload} -s ${size}"
        if [ "$throttle" != "nothrottle" ]; then
            [ -n "$throttle_flag" ] || host_workload="${host_workload} --throttle ${throttle_pt}"
        fi
        host_workload="${host_workload} --scratch-directory ${RUN_DIR}/tmp/scratch${hid}"
        mkdir -p "${RUN_DIR}/tmp/scratch${hid}" "${point_dir}/host${hid}"

        # Co-running host JVMs bind the same HTTP/RMI ports by default.
        # Each benchmark exposes its own port property — apply the right one
        # so host 1 uses port 8180 while host 0 uses 8080.
        case "$BENCH_NAME" in
          tradesoap|tradebeans)
            # See baseline path above for (hid+1)*100 rationale.
            host_workload="-Djboss.socket.binding.port-offset=$(((hid + 1) * 100)) ${host_workload}" ;;
          tomcat)
            host_workload="-Ddacapo.tomcat.port=$((8080 + hid * 100)) ${host_workload}" ;;
          spring)
            host_workload="-Dserver.port=$((8080 + hid * 100)) ${host_workload}" ;;
        esac

        local host_gc="-XX:+UseShenandoahGC -XX:ParallelGCThreads=${CCMT} -XX:ConcGCThreads=${CCMT}"
        # DGC mode: disable Shenandoah's mutator pacer so it does not
        # compete with the coordinator's CP-SAT scheduler. A workload
        # conf can set SHEN_PACING_dgc=true to keep the JVM's default
        # (pacer on) — used historically when a heavy reflection
        # workload over RDMA out-paced marking.
        local _pacing=$(resolve_var SHEN_PACING dgc)
        if [ "$_pacing" = "true" ]; then
            host_gc="${host_gc} ${snic_base} -XX:ShenandoahMaxSATBBufferFlushes=5"
        else
            host_gc="${host_gc} ${snic_base} -XX:ShenandoahMaxSATBBufferFlushes=5 -XX:-ShenandoahPacing"
        fi
        host_gc="${host_gc} -XX:RPCPort=${host_rpc} -XX:+SnicGCHost -XX:HeapBaseMinAddress=0x500000000"
        host_gc="${host_gc} $(build_shm_flags_host $hid)"
        host_gc="${host_gc} -XX:RDMAPort=${host_rdma}"
        if [ "${ALWAYS_DGC:-false}" = "true" ]; then
            host_gc="${host_gc} -XX:-SnicGCCoorHeuristic"
        else
            host_gc="${host_gc} -XX:SnicGCCoorClientId=${hid} -XX:+SnicGCCoorHeuristic"
        fi
        host_gc="${host_gc} -XX:+SnicConcCopyRegion -XX:RDMAPortForCoor=${coor_rdma_port}"
        host_gc="${host_gc} -XX:+SnicGCRDMAPrefetchEnabled -XX:SnicGCEstimatedRDMACopyTime=60"

        # Transport-specific host flags
        if [ "$transport" = "shm" ]; then
            host_gc="${host_gc} -XX:ShmClientMarkerNum=${client_ccmt}"
        else
            host_gc="${host_gc} -XX:SnicSATBRootsSplitPartNum=3"
            host_gc="${host_gc} -XX:SNICTransRegionGroupNum=200"
            host_gc="${host_gc} -XX:SnicSATBRootsForceDeltaNum=5"
            host_gc="${host_gc} -XX:DpuClientMarkerNum=${client_ccmt}"
            # SnicGCRDMABatchFetchKlass — see client-side block above; must be
            # enabled on both host (to emit RPC 12) and client (to handle it).
            local _batch_fk_host=$(resolve_var SNIC_RDMA_BATCH_FETCHKLASS dgc)
            if [ "$_batch_fk_host" = "true" ]; then
                host_gc="${host_gc} -XX:+SnicGCRDMABatchFetchKlass"
            fi
        fi
        host_gc="${host_gc} -XX:+DGCNoPenalty -XX:SnicGCIntervalUnderEstimation=20"

        # SnicFallbackCCMT / SnicDGCCCMT / COOR_FROZEN_UPPER (from workload config)
        local _fallback_ccmt=$(resolve_var SNIC_FALLBACK_CCMT dgc)
        local _dgc_ccmt_val=$(resolve_var SNIC_DGC_CCMT dgc)
        local _frozen_upper=$(resolve_var COOR_FROZEN_UPPER dgc)
        [ -n "$_fallback_ccmt" ] && host_gc="${host_gc} -XX:SnicFallbackCCMT=${_fallback_ccmt}"
        [ -n "$_dgc_ccmt_val" ] && host_gc="${host_gc} -XX:SnicDGCCCMT=${_dgc_ccmt_val}"
        [ -n "$_frozen_upper" ] && host_gc="${host_gc} -XX:SnicCoorFrozenDGCUpperBound=${_frozen_upper}"

        # Coordinator CCMT scheduling args (workload-specific)
        local coor_args_var="SNIC_COOR_CCMT_ARGS_${transport}"
        local coor_args="${!coor_args_var:-}"
        if [ -n "$coor_args" ]; then
            host_gc="${host_gc} -XX:SnicCoorCCMTArgs=\"${coor_args}\""
        fi

        rm -f "${point_dir}"/dacapo-latency-usec* 2>/dev/null || true

        # setarch -R disables ASLR so the host's SHM mmap regions don't
        # collide with the JVM's own mmap targets (heap, CDS archive,
        # libjvm/libjli code) — same fix as dgc-shm-campaign.sh applies to the
        # client. Without this, universe_init() hits SIGBUS BUS_ADRERR at a
        # virtual address that overlaps our SHM mapping.
        # PAR_HOST_NUMA env var overrides -m1 for parallel-sweep mode.
        local _host_numa="${PAR_HOST_NUMA:-1}"
        local full_cmd="setarch \$(uname -m) -R numactl -C ${host_cpu} -m${_host_numa} ${JAVA} ${log_opt} ${host_gc} ${COMMON_FLAGS} ${host_workload}"
        echo "Commandline: ${full_cmd}" > "$host_log"

        eval "$full_cmd" >> "$host_log" 2>&1 &
        host_pids+=($!)
        log_cmd "host_${hid}" "${host_pids[-1]}" "$full_cmd"
    done

    # Monitor progress until all hosts finish
    _wait_dacapo_hosts host_pids host_logs "$iterations"

    # Collect results
    local any_fail=0
    for ((hid = 0; hid < host_count; hid++)); do
        _check_dacapo_result "${host_logs[$hid]}" "$point_dir" "$iterations" "host${hid}" || any_fail=1
    done

    # Collect latency CSVs (each host writes to its own log-directory)
    for ((hid = 0; hid < host_count; hid++)); do
        local latency_csv="${point_dir}/host${hid}/dacapo-latency-usec-simple-$((iterations - 1)).csv"
        if [ -f "$latency_csv" ]; then
            mv "$latency_csv" "${point_dir}/latency_host${hid}.csv"
        fi
        local metered_csv="${point_dir}/host${hid}/dacapo-latency-usec-metered-$((iterations - 1)).csv"
        if [ -f "$metered_csv" ]; then
            mv "$metered_csv" "${point_dir}/latency_metered_host${hid}.csv"
        fi
    done

    # Cleanup all DGC processes (coordinator, clients, hosts)
    # Kill process trees: numactl → java, so kill children too
    log "Cleaning up DGC processes..."
    for pid in "${dgc_pids[@]}" "${host_pids[@]}"; do
        # Kill the process and all its children
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 2
    for pid in "${dgc_pids[@]}" "${host_pids[@]}"; do
        pkill -9 -P "$pid" 2>/dev/null || true
        kill -9 "$pid" 2>/dev/null || true
    done
    # Safety net: kill any remaining java processes belonging to this
    # benchmark instance. Same parallel-sweep concern as the start-of-
    # throttle cleanup above — scope by SHM_PREFIX when available so we
    # don't terminate the sibling track's ongoing iteration.
    if [ -n "${SHM_PREFIX:-}" ] && [ "${SHM_PREFIX}" != "default" ]; then
        safe_pkill "${SHM_PREFIX}_"
    else
        safe_pkill "Xmx${HEAP_SIZE}m.*${BENCH_NAME}"
    fi
    cleanup_isolated
    return $any_fail
}

# ============================================================
# Result checking
# ============================================================

# Poll host logs for DaCapo iteration progress; return when all hosts exit.
# DaCapo prints "completed warmup N" for iterations 1..(n-1), then "PASSED/FAILED" for the last.
_wait_dacapo_hosts() {
    local -n _pids=$1 _logs=$2
    local total_iter=$3
    local count=${#_pids[@]}

    # Track last-reported iteration per host
    local -a reported=()
    for ((i = 0; i < count; i++)); do reported+=(0); done

    while true; do
        local all_done=true
        for ((i = 0; i < count; i++)); do
            if ! is_alive "${_pids[$i]}" 2>/dev/null; then
                # Report finish once
                if [ "${reported[$i]}" != "done" ]; then
                    if grep -q "PASSED in" "${_logs[$i]}" 2>/dev/null; then
                        reported[$i]="done"
                    elif grep -Eq "FAILED|SIGSEGV|fatal error" "${_logs[$i]}" 2>/dev/null; then
                        log "  [host${i}] CRASHED (check ${_logs[$i]})"
                        reported[$i]="done"
                    else
                        reported[$i]="done"
                    fi
                fi
                continue
            fi
            all_done=false

            # Count completed iterations from log
            local iter_now
            iter_now=$(grep -Ec "completed warmup|PASSED in" "${_logs[$i]}" 2>/dev/null; true)
            if [ "$iter_now" -gt "${reported[$i]}" ]; then
                # Show the latest iteration line
                local latest
                latest=$(grep -E "completed warmup|PASSED in" "${_logs[$i]}" 2>/dev/null | tail -1 \
                    | grep -oP '(completed warmup \d+ in \d+ msec|PASSED in \d+ msec)')
                log "  [host${i}] iter ${iter_now}/${total_iter}: ${latest}"
                reported[$i]=$iter_now
            fi
        done
        $all_done && break
        sleep 5
    done
}

_check_dacapo_result() {
    local log_file="$1" point_dir="$2" iterations="$3" label="${4:-}"

    if grep -q "PASSED in" "$log_file" 2>/dev/null; then
        local pass_time
        pass_time=$(grep "PASSED in" "$log_file" | tail -1 | grep -oP '\d+(?= msec)' || echo "?")
        local p99
        p99=$(grep "simple tail latency:" "$log_file" 2>/dev/null | tail -1 | grep -oP '(?<=, )99% \K\d+' || echo "?")
        log "  ${label:+[${label}] }PASSED in ${pass_time}ms, p99=${p99}us"
        return 0
    fi
    log_error "${label:+[${label}] }Test did not PASS (check ${log_file})"
    return 1
}
