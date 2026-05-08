#!/bin/bash
# table3-dgc-shm-specjbb-hbase-mix-run.sh
# DGC-SHM SPECjbb+HBase mix workload for Table 3 reproduction.
# Honors env from wrapper: LOG_PATH, DEPS_BASE, NODE_IP, CDS_JSA, DGC_JDK.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# HOME_DIR points to dir containing specjbb-1.0.4, hbase-test, jdk17-snic-gc.
HOME_DIR="${DEPS_BASE:-$SCRIPT_DIR/../../../..}"
NODE_IP="${NODE_IP:-${DGC_HOST_ADDR:-${DGC_ADDR:-127.0.0.1}}}"

SPECJBB_JAR_PATH="${SPECJBB_JAR_PATH:-$HOME_DIR/specjbb-1.0.4/specjbb-output-latency-with-start.jar}"
HBASE_BASE_PATH="$HOME_DIR/hbase-test/multi-regionserver-hbase-0"
YCSB_BASE_PATH="$HOME_DIR/hbase-test/ycsb-0.18.0"

LOG_PATH="${LOG_PATH:-$(pwd)}"

# SPECjbb reads ./config/specjbb2015.props relative to cwd; chdir into the
# inner script dir so this resolves regardless of where the wrapper invokes us.
cd "$SCRIPT_DIR"

JAVA_HOME="${DGC_JDK:-$HOME_DIR/jdk17-snic-gc/build/linux-x86_64-server-release/images/jdk}"
CDS_JSA="${CDS_JSA:-$JAVA_HOME/lib/server/classes.jsa}"
CDS_OPTION="-XX:SharedArchiveFile=$CDS_JSA -Xshare:on"
SNIC_GC_OPTION="-XX:RDMAPort=2340 -XX:SNICAddr=$NODE_IP -XX:HostAddr=$NODE_IP -XX:HeapBaseMinAddress=0x500000000 -XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions -XX:-ShenandoahVerify -XX:+SnicGCShareMemEnabled"
COMMON_FLAGS="$CDS_OPTION -XX:MaxMetaspaceSize=1024m -XX:MetaspaceSize=1024m -XX:-ClassUnloading -XX:+UseCompressedOops -XX:+UseCompressedClassPointers"

HEAP_ID=1
HBASE_IR_RATE=45000
HBASE_YCSB_WORKLOAD="workloada_2host"

# CCMT marking time model: "<CCMT_fallback>:<a>:<b>;<CCMT_dgc>:<a>:<b>"
# Constant form (a=0): fallback=650ms, DGC=350ms (legacy default).
# SnicCoorCCMTArgs seed: empty inherits the JDK default ("0:0:500;4:0:500"),
# which adaptive EWMA replaces after a few cycles. Set CCMT_ARGS in the env
# to pin a hand-tuned seed (note: shell escaping requires "0:0:X\;4:0:Y").
CCMT_ARGS="${CCMT_ARGS:-}"
COOR_FROZEN_UPPER="${COOR_FROZEN_UPPER:-80}"

tmux_count=0
declare -A min_heaps
min_heaps["specjbb"]=2048
min_heaps["hbase"]=1536

heap_config_names=("1.5" "2.0" "4.0" "1.3" "1.0")
heap_config_name=${heap_config_names[$HEAP_ID]}

specjbb_min_heap=${min_heaps["specjbb"]}
specjbb_heap_configs=($((specjbb_min_heap * 3 / 2)) $((specjbb_min_heap * 2)) $((specjbb_min_heap * 4)) $((specjbb_min_heap * 13 / 10)) $((specjbb_min_heap)) )
specjbb_heapsize=${specjbb_heap_configs[$HEAP_ID]}

hbase_min_heap=${min_heaps["hbase"]}
hbase_heap_configs=($((hbase_min_heap * 3 / 2)) $((hbase_min_heap * 2)) $((hbase_min_heap * 4)) $((hbase_min_heap * 13 / 10)) $((hbase_min_heap)) )
hbase_heapsize=${hbase_heap_configs[$HEAP_ID]}

function log_both() {
    echo "$1"
    echo "$1" >> "$2"
}

function print_usage() {
    ps aux --sort=-%cpu | awk 'NR<=15 {print $0}'
}

function print_and_run() {
    echo "Commandline:" >> $2
    echo "$1" >> "$2"
    print_usage >> "$2"
    eval "$1" &>> "$2"
}

function controller_print_and_run() {
    echo "Commandline:" >> $2
    echo "$1" >> "$2"
    print_usage >> "$2"
    eval "$1" >> "$2" 2>$3
}

function run_cmd() {
    local cmd=$1
    eval "$cmd"
}

get_isolate_client_cpu_list() {
    local cores=$1
    local host_id=$2
    local cpus=""
    local start=$((126-host_id*cores*2))
    for ((i=0; i<cores; i++)); do
        cpus="$cpus$start,"
        start=$((start-2))
    done
    cpus=${cpus%,}
    echo $cpus
}

get_specjbb_backend_cpu_list() {
    local cores=$1
    local group_id=$2
    local cpus=""
    local start=$((1+cores*group_id*2))
    for ((i=0; i<cores; i++)); do
        cpus="$cpus$start,"
        start=$((start+2))
    done
    cpus=${cpus%,}
    echo $cpus
}

get_specjbb_txi_cpu_list() {
    local cores=$1
    local group_id=$2
    local cpus=""
    local start=$((16+cores*group_id*2))
    for ((i=0; i<cores; i++)); do
        cpus="$cpus$start,"
        start=$((start+2))
    done
    cpus=${cpus%,}
    echo $cpus
}

get_specjbb_controller_cpu_list() {
    local cores=$1
    local cpus=""
    local start=16
    for ((i=0; i<cores; i++)); do
        cpus="$cpus$start,"
        start=$((start+2))
    done
    cpus=${cpus%,}
    echo $cpus
}

function check_test_result() {
    local log_file=$1
    if grep -q "unexpected error" "$log_file"; then
        echo "Error: testcase failed !!!"
    else
        echo "PASSED"
    fi
}

init_hbase_for_ycsb() {
    $HBASE_BASE_PATH/bin/hbase shell < $HBASE_BASE_PATH/test.txt
}

get_ycsb_terminal_cpu_list() {
    local cores=$1
    local cpus=""
    local start=126
    for ((i=cores-1; i>=0; i--)); do
        cpus="$cpus$start,"
        start=$((start-2))
    done
    cpus=${cpus%,}
    echo $cpus
}

hbase_ycsb_queries_load_and_warmup() {
    local log_file_prefix=$1
    local loop_time=$2
    local test_type=$3
    local daemon_log_file=$4
    local pcore=$5
    local ir_rate=$6
    local workload_file_name=$7
    terminal_cpu_list=$(get_ycsb_terminal_cpu_list $pcore)
    local query_output_file="${log_file_prefix}_${test_type}.txt"
    local log_file_latency="${log_file_prefix}_ycsb_latency_${test_type}_${loop_time}.txt"
    local terminal_num=60
    echo "logging ycsb output to $query_output_file"
    echo "logging ycsb latency to $log_file_latency"
    echo "logging ycsb process to $daemon_log_file"
    echo "terminal cpu list: $terminal_cpu_list"
    local max_warmup_time=30
    local max_run_time=60
    throttle_rate="-target $ir_rate"
    if [ $ir_rate -eq 0 ]; then
        throttle_rate=""
    fi
    local load_ycsb_param="-p hosts=localhost -threads $terminal_num"
    local warmup_ycsb_param="-p hosts=localhost -threads $terminal_num -p maxexecutiontime=${max_warmup_time}"
    local run_ycsb_param="-p hosts=localhost -p measurementtype=raw -p measurement.raw.output_file=$log_file_latency -threads $terminal_num $throttle_rate -p maxexecutiontime=${max_run_time}"
    default_output_interval=10000
    echo "start load $loop_time $(date +%s%3N)ms" >> $query_output_file
    numactl -C $terminal_cpu_list -m0 python3 $YCSB_BASE_PATH/bin/ycsb load hbase2 -s -P $YCSB_BASE_PATH/workloads/$workload_file_name $load_ycsb_param -cp $HBASE_BASE_PATH/conf -p table=usertable -p columnfamily=family -p status.interval=${default_output_interval} &>> $query_output_file
    echo "finish load $loop_time $(date +%s%3N)ms" >> $query_output_file
    echo "start warmup $loop_time $(date +%s%3N)ms" >> $query_output_file
    numactl -C $terminal_cpu_list -m0 python3 $YCSB_BASE_PATH/bin/ycsb run hbase2 -s -P $YCSB_BASE_PATH/workloads/$workload_file_name $warmup_ycsb_param -cp $HBASE_BASE_PATH/conf -p table=usertable -p columnfamily=family -p status.interval=${default_output_interval} &>> $query_output_file
    echo "finish warmup $loop_time $(date +%s%3N)ms" >> $query_output_file
    init_hbase_for_ycsb
    numactl -C $terminal_cpu_list -m0 python3 $YCSB_BASE_PATH/bin/ycsb load hbase2 -s -P $YCSB_BASE_PATH/workloads/$workload_file_name $load_ycsb_param -cp $HBASE_BASE_PATH/conf -p table=usertable -p columnfamily=family -p status.interval=${default_output_interval} &>> $query_output_file
}

hbase_ycsb_queries_reload_and_run() {
    local log_file_prefix=$1
    local loop_time=$2
    local test_type=$3
    local daemon_log_file=$4
    local pcore=$5
    local ir_rate=$6
    local workload_file_name=$7
    terminal_cpu_list=$(get_ycsb_terminal_cpu_list $pcore)
    local query_output_file="${log_file_prefix}_${test_type}.txt"
    local log_file_latency="${log_file_prefix}_ycsb_latency_${test_type}_${loop_time}.txt"
    local terminal_num=60
    local max_warmup_time=30
    local max_run_time=60
    throttle_rate="-target $ir_rate"
    if [ $ir_rate -eq 0 ]; then
        throttle_rate=""
    fi
    local load_ycsb_param="-p hosts=localhost -threads $terminal_num"
    local warmup_ycsb_param="-p hosts=localhost -threads $terminal_num -p maxexecutiontime=${max_warmup_time}"
    local run_ycsb_param="-p hosts=localhost -p measurementtype=raw -p measurement.raw.output_file=$log_file_latency -threads $terminal_num $throttle_rate -p maxexecutiontime=${max_run_time}"
    default_output_interval=10000
    echo "start run $loop_time $(date +%s%3N)ms" >> $query_output_file
    numactl -C $terminal_cpu_list -m0 python3 $YCSB_BASE_PATH/bin/ycsb run hbase2 -s -P $YCSB_BASE_PATH/workloads/$workload_file_name $run_ycsb_param -cp $HBASE_BASE_PATH/conf -p table=usertable -p columnfamily=family -p status.interval=${default_output_interval} &>> $query_output_file
    echo "finish run $loop_time $(date +%s%3N)ms" >> $query_output_file
}

for CCMT_THREAD in 4
do
    for CCET_THREAD in 4
    do
        for PCORE in 8
        do
            for GROUP_NUM in 3
            do
                RPC_PORT=$((1024 + RANDOM % 64000))
                COOR_RDMA_PORT=9999
                RDMA_PORT=2340
                log_path="$LOG_PATH/$heap_config_name/${CCMT_THREAD}/${CCET_THREAD}/${PCORE}/${GROUP_NUM}/shm"
                function run_coordinator {
                    gc_name=$1
                    java_cmd=$2
                    log_file_prefix=$3
                    mkdir $log_path -p
                    log_file="${log_file_prefix}_SNICCoor.txt"
                    TESTCASE_FLAGS="-jar $SPECJBB_JAR_PATH -m COMPOSITE"
                    COOR_CCMT_THREAD=$((CCMT_THREAD*2))
                    GC_OPTION="-XX:+SnicGCCoorHeuristic -XX:SnicGCCoorSHMPath=/${USER}_coor_heuristic -XX:SnicGCCoorClientNum=4 -XX:+SnicGCCoordinator -XX:+SnicCoorAdaptiveCCMT -XX:SnicAvgMarkTimeAmplifyRate=2.0 -XX:SnicShmGlobalPacerPath=/${USER}_share_global_pacer -XX:ParallelGCThreads=${COOR_CCMT_THREAD} -XX:ConcGCThreads=${COOR_CCMT_THREAD} -XX:SnicCoorFrozenDGCUpperBound=${COOR_FROZEN_UPPER} -XX:SnicCoorHostAddrPortList=\"$NODE_IP:9999;$NODE_IP:10000;$NODE_IP:10001;$NODE_IP:10002\""
                    echo Start Coordinator, logging into $log_file
                    log_both "gc name: $gc_name" $log_file
                    log_both "heap config name: ${heap_config_name}" $log_file
                    print_and_run "$java_cmd $GC_OPTION $COMMON_FLAGS $TESTCASE_FLAGS"  $log_file &
                }
                function run_specjbb_client {
                    gc_name=$1
                    java_cmd=$2
                    log_file_prefix=$3
                    host_id=$4
                    client_rpc_port=$((host_id+RPC_PORT))
                    rdma_port=$((host_id+RDMA_PORT))
                    mkdir $log_path -p
                    log_file="${log_file_prefix}_SNICClient_${host_id}.txt"
                    TESTCASE_FLAGS="-Xmx${specjbb_heapsize}m -Xms${specjbb_heapsize}m -jar $SPECJBB_JAR_PATH -m COMPOSITE"
                    GC_OPTION="-XX:+UseShenandoahGC -XX:+SnicGCClient -XX:ParallelGCThreads=${CCMT_THREAD} -XX:ConcGCThreads=${CCMT_THREAD} -XX:-UseDynamicNumberOfGCThreads $SNIC_GC_OPTION -XX:+SnicConcCopyRegion -XX:RPCPort=${client_rpc_port} -XX:SnicGCHostNum=1 -XX:SnicShmMemPath=/${USER}_share_heap_${host_id} -XX:SnicShmRootsPath=/${USER}_share_roots_${host_id} -XX:SnicShmGlobalPacerPath=/${USER}_share_global_pacer -XX:SnicShmRegionTamsPath=/${USER}_share_region_tams_${host_id} -XX:SnicShmBitmapPath=/${USER}_share_bitmap_${host_id} -XX:SnicShmLivenessPath=/${USER}_share_liveness_${host_id} -XX:SnicShmVirtualNodePath=/${USER}_virtual_node_${host_id} -XX:-SnicGCGlobalPacer -XX:RDMAPort=${rdma_port} -XX:+SnicGCCoorHeuristic -XX:SnicGCCoorSHMPath=/${USER}_coor_heuristic -XX:SnicGCCoorClientNum=4 -XX:SnicGCCoorClientId=${host_id} -XX:ShmClientMarkerNum=${CCMT_THREAD}"
                    cpu_list=$(get_isolate_client_cpu_list $((CCMT_THREAD*2)) 0)
                    echo Start SPECjbb client $host_id, logging into $log_file
                    log_both "gc name: $gc_name" $log_file
                    log_both "bench name: specjbb" $log_file
                    log_both "heap config size: ${specjbb_heapsize}" $log_file
                    log_both "heap config name: ${heap_config_name}" $log_file
                    print_and_run "numactl -C $cpu_list $java_cmd $GC_OPTION $COMMON_FLAGS $TESTCASE_FLAGS" $log_file &
                    tmux_count=$((tmux_count+1))
                }
                function run_hbase_client {
                    gc_name=$1
                    java_cmd=$2
                    log_file_prefix=$3
                    host_id=$4
                    client_rpc_port=$((host_id+RPC_PORT))
                    rdma_port=$((host_id+RDMA_PORT))
                    mkdir $log_path -p
                    log_file="${log_file_prefix}_SNICClient_${host_id}.txt"
                    TESTCASE_FLAGS="-Xmx${specjbb_heapsize}m -Xms${specjbb_heapsize}m -jar $SPECJBB_JAR_PATH -m COMPOSITE"
                    GC_OPTION="-XX:+UseShenandoahGC -XX:+SnicGCClient -XX:ParallelGCThreads=${CCMT_THREAD} -XX:ConcGCThreads=${CCMT_THREAD} -XX:-UseDynamicNumberOfGCThreads $SNIC_GC_OPTION -XX:+SnicConcCopyRegion -XX:RPCPort=${client_rpc_port} -XX:SnicGCHostNum=1 -XX:SnicShmMemPath=/${USER}_share_heap_${host_id} -XX:SnicShmRootsPath=/${USER}_share_roots_${host_id} -XX:SnicShmGlobalPacerPath=/${USER}_share_global_pacer -XX:SnicShmRegionTamsPath=/${USER}_share_region_tams_${host_id} -XX:SnicShmBitmapPath=/${USER}_share_bitmap_${host_id} -XX:SnicShmLivenessPath=/${USER}_share_liveness_${host_id} -XX:SnicShmVirtualNodePath=/${USER}_virtual_node_${host_id} -XX:-SnicGCGlobalPacer -XX:RDMAPort=${rdma_port} -XX:+SnicGCCoorHeuristic -XX:SnicGCCoorSHMPath=/${USER}_coor_heuristic -XX:SnicGCCoorClientNum=4 -XX:SnicGCCoorClientId=${host_id} -XX:ShmClientMarkerNum=${CCMT_THREAD}"
                    cpu_list=$(get_isolate_client_cpu_list $((CCMT_THREAD*2)) 0)
                    echo Start HBase client $host_id, logging into $log_file
                    log_both "gc name: $gc_name" $log_file
                    log_both "bench name: HBase" $log_file
                    log_both "heap config size: ${hbase_heapsize}" $log_file
                    log_both "heap config name: ${heap_config_name}" $log_file
                    print_and_run "numactl -C $cpu_list $java_cmd $GC_OPTION $COMMON_FLAGS $TESTCASE_FLAGS" $log_file &
                    tmux_count=$((tmux_count+1))
                }
                function run_specjbb_txi_and_backend {
                    gc_name=$1
                    java_cmd=$2
                    gc_log_option=$3
                    log_file_prefix=$4
                    group_id=$5
                    group_name_id=$((group_id+1))
                    host_rpc_port=$((RPC_PORT+group_id))
                    rdma_port=$((group_id+RDMA_PORT))
                    controller_log_file="${log_file_prefix}_specjbb_controller_snic.txt"
                    mkdir $log_path -p
                    backend_log_file="${log_file_prefix}_specjbb_backend_snic_${group_id}.txt"
                    txi_log_file="${log_file_prefix}_specjbb_txi_snic_${group_id}.txt"
                    txi_cpu_list=$(get_specjbb_txi_cpu_list $PCORE $group_id)
                    TXI_TESTCASE_FLAGS="-Dspecjbb.controller.port=24000 -jar $SPECJBB_JAR_PATH -m TXINJECTOR -ikv -G=group${group_name_id} -J=JVM1"
                    TXI_GC_OPTION="-XX:+UseShenandoahGC -XX:-SnicGCHost -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 $SNIC_GC_OPTION -XX:ShenandoahMaxSATBBufferFlushes=5 -Xmx8192m -Xms8192m"
                    BACKEND_TESTCASE_FLAGS="-Xmx${specjbb_heapsize}m -Xms${specjbb_heapsize}m -Dspecjbb.controller.port=24000 -jar $SPECJBB_JAR_PATH -m BACKEND -ikv -G=group${group_name_id} -J=JVM2 -v"
                    backend_cpu_list=$(get_specjbb_backend_cpu_list $PCORE $group_id)
                    BACKEND_GC_OPTION="-XX:+UseShenandoahGC -XX:ParallelGCThreads=${CCMT_THREAD} -XX:ConcGCThreads=${CCMT_THREAD} $SNIC_GC_OPTION -XX:ShenandoahMaxSATBBufferFlushes=5 -XX:RPCPort=${host_rpc_port} -XX:+SnicGCHost -XX:HeapBaseMinAddress=0x500000000 -XX:-ShenandoahVerify -XX:SnicShmMemPath=/${USER}_share_heap_${group_id} -XX:SnicShmRootsPath=/${USER}_share_roots_${group_id} -XX:SnicShmGlobalPacerPath=/${USER}_share_global_pacer -XX:SnicShmRegionTamsPath=/${USER}_share_region_tams_${group_id} -XX:SnicShmBitmapPath=/${USER}_share_bitmap_${group_id} -XX:SnicShmLivenessPath=/${USER}_share_liveness_${group_id} -XX:SnicShmVirtualNodePath=/${USER}_virtual_node_${group_id} -XX:-SnicGCGlobalPacer -XX:RDMAPort=${rdma_port} -XX:SnicGCCoorClientId=${group_id} -XX:+SnicGCCoorHeuristic -XX:-ShenandoahPacing -XX:+DGCNoPenalty ${CCMT_ARGS:+-XX:SnicCoorCCMTArgs=\"$CCMT_ARGS\"} -XX:ShmClientMarkerNum=${CCMT_THREAD}"
                    echo Start SPECjbb TXInjector, logging into $txi_log_file
                    log_both "TXI gc name: $gc_name" $txi_log_file
                    log_both "TXI bench name: $bench_name" $txi_log_file
                    print_and_run "numactl -C $txi_cpu_list -m0 $java_cmd $TXI_GC_OPTION $COMMON_FLAGS $TXI_TESTCASE_FLAGS" $txi_log_file &
                    sleep 3
                    echo Start SPECjbb Backend $group_id, logging into $backend_log_file
                    echo BACKEND Logging into $backend_log_file
                    log_both "BACKEND gc name: $gc_name" $backend_log_file
                    log_both "BACKEND bench name: specjbb" $backend_log_file
                    log_both "BACKEND heap config size: ${specjbb_heapsize}" $backend_log_file
                    log_both "BACKEND heap config name: ${heap_config_name}" $backend_log_file
                    # Per-backend cwd so latency-with-start JAR's ./latency.txt
                    # doesn't collide between groups.
                    backend_cwd="${log_path}/backend_${group_id}_cwd"
                    mkdir -p "$backend_cwd"
                    cp -r "$SCRIPT_DIR/config" "$backend_cwd/" 2>/dev/null
                    print_and_run "cd '$backend_cwd' && numactl -C $backend_cpu_list -m1 $java_cmd $gc_log_option $BACKEND_GC_OPTION $COMMON_FLAGS $BACKEND_TESTCASE_FLAGS" $backend_log_file $backend_log_file &
                    tmux_count=$((tmux_count+1))
                }
                function run_specjbb_controller {
                    gc_name=$1
                    java_cmd=$2
                    gc_log_option=$3
                    log_file_prefix=$4
                    mkdir $log_path -p
                    controller_log_file="${log_file_prefix}_specjbb_controller_snic.txt"
                    controller_err_file="${log_file_prefix}_specjbb_controller_snic.log"
                    controller_cpu_list=$(get_specjbb_controller_cpu_list $PCORE)
                    CONTROLLER_GC_OPTION="-XX:+UseShenandoahGC -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 $SNIC_GC_OPTION -XX:ShenandoahMaxSATBBufferFlushes=5 -XX:RPCPort=${RPC_PORT}"
                    CONTROLLER_TESTCASE_FLAGS="-Dspecjbb.group.count=2 -Dspecjbb.controller.port=24000 -jar $SPECJBB_JAR_PATH -m MULTICONTROLLER -ikv -l 3 -v"
                    echo Start SPECjbb controller, logging into $controller_log_file
                    log_both "gc name: $gc_name" $controller_log_file
                    log_both "bench name: specjbb" $controller_log_file
                    controller_print_and_run "numactl -C $controller_cpu_list -m0 $java_cmd $CONTROLLER_GC_OPTION $COMMON_FLAGS $CONTROLLER_TESTCASE_FLAGS" $controller_log_file $controller_err_file
                    check_test_result $controller_log_file
                }
                function run_hbase_components {
                    gc_name=$1
                    java_cmd=$2
                    host_log_option=$3
                    log_file_prefix=$4
                    mkdir $log_path -p
                    zookeeper_log_file="${log_file_prefix}_hbase_zookeeper.txt"
                    daemon_log_file="${log_file_prefix}_hbase_daemon_snic.txt"
                    daemon_stderr_file="${log_file_prefix}_hbase_daemon_stderr_snic.txt"
                    regionserver_0_log_file="${log_file_prefix}_hbase_regionserver_snic_0.txt"
                    regionserver_1_log_file="${log_file_prefix}_hbase_regionserver_snic_1.txt"
                    log_file="${log_file_prefix}_SNICHost.txt"
                    echo Start HBase components, logging into $log_file
                    log_both "gc name: $gc_name" $log_file
                    log_both "bench name: HBase" $log_file
                    log_both "heap config size: ${hbase_heapsize}" $log_file
                    log_both "heap config name: ${heap_config_name}" $log_file
                    region_server_port=$((RANDOM%10000+16000))
                    rm -rf ${SCRIPT_DIR}/tmp/hbase/data/default
                    run_cmd "bash $HBASE_BASE_PATH/bin/self-zookeeper-start --config $HBASE_BASE_PATH/conf zookeeper ${RPC_PORT} ${CCMT_THREAD} ${CCET_THREAD} ${PCORE} ${hbase_heapsize} snic start" >> $zookeeper_log_file 2>&1 &
                    sleep 3
                    run_cmd "bash $HBASE_BASE_PATH/bin/specjbb-hbase-mix-master-start --config $HBASE_BASE_PATH/conf master ${RPC_PORT} ${CCMT_THREAD} ${CCET_THREAD} ${PCORE} ${hbase_heapsize} snic start" >> $daemon_log_file 2>&1 &
                    sleep 3
                    run_cmd "bash $HBASE_BASE_PATH/bin/specjbb-hbase-mix-regionserver-start --config $HBASE_BASE_PATH/conf regionserver ${RPC_PORT} ${CCMT_THREAD} ${CCET_THREAD} ${PCORE} ${hbase_heapsize} 2 ${COOR_RDMA_PORT} ${CCMT_THREAD} snic -Dhbase.regionserver.port=$region_server_port -Dhbase.regionserver.info.port=$((region_server_port+10)) start" >> $regionserver_0_log_file 2>&1 &
                    sleep 3
                    run_cmd "bash $HBASE_BASE_PATH/bin/specjbb-hbase-mix-regionserver-start --config $HBASE_BASE_PATH/conf regionserver ${RPC_PORT} ${CCMT_THREAD} ${CCET_THREAD} ${PCORE} ${hbase_heapsize} 3 ${COOR_RDMA_PORT} ${CCMT_THREAD} snic -Dhbase.regionserver.port=$((region_server_port+1)) -Dhbase.regionserver.info.port=$((region_server_port+11)) start" >> $regionserver_1_log_file 2>&1 &
                    sleep 3
                    init_hbase_for_ycsb
                }
                function run_hbase_ycsb {
                    gc_name=$1
                    java_cmd=$2
                    host_log_option=$3
                    log_file_prefix=$4
                    mkdir $log_path -p
                    daemon_log_file="${log_file_prefix}_hbase_daemon_snic.txt"
                    log_file="${log_file_prefix}_SNICHost.txt"
                    region_server_port=$((RANDOM%10000+16000))
                    loop_time=0
                    hbase_ycsb_queries_load_and_warmup $log_file_prefix $loop_time SNICHost $daemon_log_file $PCORE $HBASE_IR_RATE $HBASE_YCSB_WORKLOAD
                    for((i=0;i<2;i++))
                    do
                        run_specjbb_txi_and_backend "snicgc" "$JAVA_HOME/bin/java" "$LOG_OPTION" "$log_file_prefix" $i
                    done
                    sleep 3
                    run_specjbb_controller "snicgc" "$JAVA_HOME/bin/java" "$LOG_OPTION" "$log_file_prefix" &
                    controller_pid=$!
                    sleep 1
                    echo "start to wait finish of SPECjbb warmup..."
                    target_specjbb_controller_log_file="${log_file_prefix}_specjbb_controller_snic.txt"
                    while ! grep -q "Ramping up completed!" "$target_specjbb_controller_log_file" 2>/dev/null; do
                        sleep 1
                    done
                    echo "start to really execute YCSB queries!!!"
                    hbase_ycsb_queries_reload_and_run $log_file_prefix $loop_time SNICHost $daemon_log_file $PCORE $HBASE_IR_RATE $HBASE_YCSB_WORKLOAD
                    pkill -9 -f org.apache.hadoop.hbase.regionserver.HRegionServer
                    pkill -9 -f org.apache.hadoop.hbase.master.HMaster
                    pkill -9 -f org.apache.hadoop.hbase.zookeeper
                    echo "Waiting for SPECjbb controller to finish..."
                    wait $controller_pid
                    echo PASSED
                }
                export current_datetime=$(date +"%Y%m%d_%H%M%S")
                mkdir $log_path -p
                log_file_prefix="$log_path/${current_datetime}"
                export LOG_OPTION="-Xlog:gc*=info:stdout:timemillis"
                pkill -9 -f "java.*specjbb"
                pkill -9 -f org.apache.hadoop.hbase.regionserver.HRegionServer
                pkill -9 -f org.apache.hadoop.hbase.master.HMaster
                pkill -9 -f org.apache.hadoop.hbase.zookeeper
                rm -f /dev/shm/${USER}_*
                rm -rf result/*
                rm -f *.data.gz
                # Master aborts on stale MasterData / regionserver port mismatch
                rm -rf "${SCRIPT_DIR}/tmp_0"
                rm -rf "${SCRIPT_DIR}/tmp"
                # Latency JAR appends to ./latency.txt; remove stale data
                rm -f "${SCRIPT_DIR}/latency.txt"
                run_coordinator "snicgc" "$JAVA_HOME/bin/java $LOG_OPTION" "$log_file_prefix"
                sleep 5
                for((i=0;i<2;i++))
                do
                    run_specjbb_client "snicgc" "$JAVA_HOME/bin/java $LOG_OPTION" "$log_file_prefix" $i
                done
                for((i=2;i<4;i++))
                do
                    run_hbase_client "snicgc" "$JAVA_HOME/bin/java $LOG_OPTION" "$log_file_prefix" $i
                done
                run_hbase_components "snicgc" "$JAVA_HOME/bin/java" "$LOG_OPTION" "$log_file_prefix"
                run_hbase_ycsb "snicgc" "$JAVA_HOME/bin/java" "$LOG_OPTION" "$log_file_prefix"
            done
        done
    done
done
