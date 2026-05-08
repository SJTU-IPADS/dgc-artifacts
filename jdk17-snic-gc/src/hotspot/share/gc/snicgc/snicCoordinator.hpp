

#ifndef SNIC_COORDINATOR_HPP
#define SNIC_COORDINATOR_HPP

#include <thread>
#include <stack>
#include <queue>
#include <map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <pthread.h>
#include <fstream>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iomanip>
#include <sys/mman.h>
#include "oops/oopsHierarchy.hpp"
#include "oops/compressedOops.inline.hpp"
#include "oops/oop.inline.hpp"
#include "oops/instanceKlass.hpp"
#include "oops/instanceRefKlass.inline.hpp"
#include "gc/shenandoah/shenandoahMarkBitMap.hpp"
#include "gc/shenandoah/shenandoahHeap.hpp"
#include "gc/shenandoah/shenandoahWorkGroup.hpp"
#include "gc/shenandoah/shenandoahOopClosures.inline.hpp"
#include "gc/shenandoah/shenandoahHeapRegion.hpp"
#include "gc/shared/workgroup.hpp"
#include "gc/shared/gc_globals.hpp"
#include "runtime/atomic.hpp"
#include "memory/virtualspace.hpp"
#include "memory/memRegion.hpp"
#include "memory/universe.hpp"
#include "classfile/javaClasses.hpp"
#include "gc/shared/taskTerminator.hpp"
#include "rdma/lib.hh"

enum ClientStates {
    INIT_CLIENT_STATE,
    RESET_GLOBAL_PACER,
    UPDATE_GLOBAL_PACER_LIVENESS,
    SCHEDULE_GLOBAL_PACER,
    CLIENT_HANDLE_RPC_12,
    CLIENT_HANDLE_RPC_13,
    GC_START,
    GC_END,
    CLIENT_EXIT,
    RDMA_CLIENT_INIT,
    STATE_UPDATE_FINISHED, // when a client sees this flag, new client state and related data are updated by coordinator.
};

struct CoorState{
    int client_num;
    int client_id;
    volatile int client_state;
    volatile unsigned long long client_liveness_to_update;
    volatile int client_gc_id;
    volatile unsigned long long client_avg_mark_time;
    volatile unsigned long long gc_start_time;
    volatile int cores_in_use;
    volatile long long marked_liveness;
    volatile int force_gc_ccmt;
};

struct HostLiveData{
    int hostId;
    uint64_t next_gc_time;
    uint64_t planed_next_gc_time;
    uint64_t avg_mark_time;
    uint64_t historyLiveness;

    friend void swap(HostLiveData& a, HostLiveData& b) noexcept {
        using std::swap;
        swap(a.hostId, b.hostId);
        swap(a.next_gc_time, b.next_gc_time);
        swap(a.planed_next_gc_time, b.planed_next_gc_time);
        swap(a.avg_mark_time, b.avg_mark_time);
        swap(a.historyLiveness, b.historyLiveness);
    }
};

// struct GlobalPacerDataPerHost {
//     // in words
//     unsigned long long historyLiveness;
//     // in words means, this will be added to global pacer's budget every round update
//     unsigned long long budgetIncreasing;
//     // in ms
//     unsigned long long averageGCTime;
//     // in ms
//     unsigned long long nextGCTime;
//     unsigned long long forceGC;
//     unsigned long long forceGCTriggerTimestamp;
//     unsigned long long startPacer;
//     unsigned long long pacerWorkAhead;
//     long budgetsToIncreaseDuringMark;
//     // special feilds designed for coordinator
//     unsigned long long CCMT;
//     unsigned long long client_gc_id;
//     unsigned long long nonmarking_time_prediction;
// };

// struct GlobalPacerData{
//     unsigned long long round;
//     unsigned long long free;
//     GlobalPacerDataPerHost hosts_global_pacer_data[MAX_HOST_NUM];
// };

class SnicCoordinator{
public:
    CoorState *coor_state;
    GlobalPacerData *backend_states;
    char* remote_global_pacer_data_base_addrs[MAX_HOST_NUM];
    char* remote_force_gc_data_base_addrs[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::qp::RC> host_global_pacer_queue_pairs[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::rmem::RMem> host_global_pacer_local_mems[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::rmem::RegHandler> host_global_pacer_local_mrs[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::qp::RC> host_force_gc_queue_pairs[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::rmem::RMem> host_force_gc_local_mems[MAX_HOST_NUM];
    rdmaio::Arc<rdmaio::rmem::RegHandler> host_force_gc_local_mrs[MAX_HOST_NUM];
    RDMAForceGCData* host_force_gc_data[MAX_HOST_NUM];
    volatile bool host_initialized[MAX_HOST_NUM];
    std::queue<int> coor_schedule_host_ids;
    bool coor_schedule_global_pacer_decided;
    int cur_alive_client_num;
    // TEST ONLY fields.
    int test_force_gc_host_id = 0;
    int* force_gc_cnts;

    long long JavaTimeDelta[MAX_HOST_NUM];

    int fallback_counter;

    // Adaptive CCMT model state (SnicCoorAdaptiveCCMT). Tracks per-host
    // marking-time observations to EWMA-update coor_ccmt_b[] in-place.
    // last_compaction_ts_seen[i]: last value of enter_compaction_timestamp; new
    // transition into Compacting → marking-just-finished → record sample.
    // last_state_seen[i]: previous host_GC_state, so we know which path
    // (FallbackMarking vs Marking=DGC) to credit the sample to.
    // n_samples[i][n]: count of samples accumulated for coor_ccmt_b[n].
    unsigned long long adaptive_last_compaction_ts[MAX_HOST_NUM];
    unsigned long long adaptive_last_state[MAX_HOST_NUM];
    int adaptive_n_samples[MAX_HOST_NUM][MAX_COOR_CONFIG_NUM];
    void update_adaptive_ccmt_b();

    SnicCoordinator();
    ~SnicCoordinator();
    void run();
    void init();
    void coordinator_reset_global_pacer();
    void coordinator_update_global_pacer_liveness(int clientId);
    void coordinator_schedule_global_pacer();
    void coordinator_handle_rpc_12(int clientId);
    void coordinator_handle_rpc_13(int clientId);
    void coordinator_handle_gc_start(int clientId);
    void coordinator_handle_gc_end(int clientId);
    void coordinator_handle_client_exit(int clientId);
    void coordinator_handle_rdma_client_init(int clientId);
    void do_schedule();
    void print_state();
    void update_force_gc_state(int hostId, int CCMT, unsigned long long forceGCTriggerTimestamp);
    int decide_force_gc_type(int hostId);
    void ResourceSchedulingProblem();

    void call_or_tools();
    char* get_global_pacer_addr_for_host(int hostId);
    void read_global_pacer_data_for_host(int hostId);
    void write_force_gc_data_for_host(int hostId);
    void wait_global_pacer_data_rdma_op_finish(int hostId);
    void wait_force_gc_data_rdma_op_finish(int hostId);
    void sync_update_global_pacer_data();

    long long coor_get_prev_forceGC_time(int hostId);
    unsigned long long coor_get_prev_forceGC_type(int hostId);
    unsigned long long coor_get_prev_forceGC_ccmt(int hostId);
    std::vector<int64_t> computeRanks(const std::vector<int64_t>& input);

    void coor_prepare_CPSAT_default(int hostId, std::vector<std::vector<int64_t>>& R, std::vector<std::vector<int64_t>>& J);
    void coor_prepare_CPSAT_predefined(int hostId, std::vector<std::vector<int64_t>>& R, std::vector<std::vector<int64_t>>& J, int64_t cores_in_use, int64_t time_to_release_core);
};

#endif
