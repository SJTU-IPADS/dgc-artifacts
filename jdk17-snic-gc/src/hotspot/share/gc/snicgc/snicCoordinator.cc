#include "snicCoordinator.hpp"
#include "shareMemSnicClient.hpp"

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

#include <vector>
#include <algorithm>
#include <chrono>
#include "runtime/globals.hpp"
#include <string>

// #ifdef USE_ORTOOLS
#include "ortools/base/logging.h"
#include "ortools/sat/cp_model.h"
#include "ortools/sat/cp_model.pb.h"
#include "ortools/sat/cp_model_solver.h"
// #endif

SnicCoordinator::SnicCoordinator() {

}

SnicCoordinator::~SnicCoordinator() {

}

// Adaptive CCMT model (SnicCoorAdaptiveCCMT).
//
// Detect per-host marking-just-completed transitions via
// enter_compaction_timestamp changes (compaction starts when marking ends).
// For each new transition, EWMA-update coor_ccmt_b[i][n] in-place where:
//   n=0 (R=0)      → fallback path; observed = compaction_ts - fallback_ts.
//   n=dgc_idx (R=SnicDGCCCMT) → DGC path; observed = client_avg_mark_time
//                              (already EWMA'd on the client side).
// Alpha schedule: α = max(2/(n+1), 0.05) — first sample writes through
// (α=1), later samples blend in slowly with floor 0.05 to keep the model
// responsive to phase shifts.
void SnicCoordinator::update_adaptive_ccmt_b() {
    if (!SnicCoorAdaptiveCCMT) return;

    for (int i = 0; i < SnicGCCoorClientNum; ++i) {
        unsigned long long curr_compaction_ts =
            Atomic::load(&(backend_states->hosts_global_pacer_data[i].enter_compaction_timestamp));
        unsigned long long curr_state =
            Atomic::load(&(backend_states->hosts_global_pacer_data[i].host_GC_state));

        // New compaction transition iff timestamp moved forward.
        if (curr_compaction_ts != adaptive_last_compaction_ts[i] && curr_compaction_ts > 0) {
            unsigned long long prev_state = adaptive_last_state[i];

            if (prev_state == Universe::HostGCState::FallbackMarking) {
                unsigned long long fallback_ts =
                    Atomic::load(&(backend_states->hosts_global_pacer_data[i].enter_fallback_timestamp));
                if (fallback_ts > 0 && curr_compaction_ts > fallback_ts) {
                    unsigned long long t_observed = curr_compaction_ts - fallback_ts;
                    int n = ++adaptive_n_samples[i][0];
                    double alpha = MAX2(2.0 / ((double)n + 1.0), 0.05);
                    double old_b = backend_states->hosts_global_pacer_data[i].coor_ccmt_b[0];
                    double new_b = (1.0 - alpha) * old_b + alpha * (double)t_observed;
                    backend_states->hosts_global_pacer_data[i].coor_ccmt_b[0] = new_b;
                    log_info(gc)("DGC LOG: ADAPTIVE host=%d Fallback obs=%llums old_b=%.1f new_b=%.1f n=%d alpha=%.3f",
                                 i, t_observed, old_b, new_b, n, alpha);
                }
            } else if (prev_state == Universe::HostGCState::Marking) {
                unsigned long long t_observed =
                    Atomic::load(&(coor_state[i].client_avg_mark_time));
                if (t_observed > 0) {
                    int dgc_idx = -1;
                    for (int n = 0; n < (int)backend_states->hosts_global_pacer_data[i].coor_ccmt_num; n++) {
                        if (backend_states->hosts_global_pacer_data[i].coor_ccmt_R[n] == (unsigned long long)SnicDGCCCMT) {
                            dgc_idx = n;
                            break;
                        }
                    }
                    if (dgc_idx >= 0) {
                        int n = ++adaptive_n_samples[i][dgc_idx];
                        double alpha = MAX2(2.0 / ((double)n + 1.0), 0.05);
                        double old_b = backend_states->hosts_global_pacer_data[i].coor_ccmt_b[dgc_idx];
                        double new_b = (1.0 - alpha) * old_b + alpha * (double)t_observed;
                        backend_states->hosts_global_pacer_data[i].coor_ccmt_b[dgc_idx] = new_b;
                        log_info(gc)("DGC LOG: ADAPTIVE host=%d DGC obs=%llums old_b=%.1f new_b=%.1f n=%d alpha=%.3f",
                                     i, t_observed, old_b, new_b, n, alpha);
                    }
                }
            }
            adaptive_last_compaction_ts[i] = curr_compaction_ts;
        }
        adaptive_last_state[i] = curr_state;
    }
}

void SnicCoordinator::run() {
    while (true) {
        os::naked_short_sleep(3);
        if(!SnicGCShareMemEnabled) {
          sync_update_global_pacer_data();
        }
        print_state();
        // loop to check every client's state to act different actions in coordinator.
        for (int i = 0; i < SnicGCCoorClientNum; ++i) {
            bool clientStateUpdated = true;
            switch (coor_state[i].client_state) {
                case ClientStates::CLIENT_EXIT:
                    log_debug(gc)("DGC LOG: coordinator handle client exit for client %d", coor_state[i].client_id);
                    coordinator_handle_client_exit(coor_state[i].client_id);
                    break;
                case ClientStates::RDMA_CLIENT_INIT:
                    log_debug(gc)("DGC LOG: coordinator handle rdma client init for client %d", coor_state[i].client_id);
                    coordinator_handle_rdma_client_init(coor_state[i].client_id);
                    break;
                default:
                    clientStateUpdated = false;
                    // log_error(gc)("DGC LOG: coordinator handle client state %d for client %d", coor_state[i].client_state, coor_state[i].client_id);
                    break;
            }
            if (clientStateUpdated) {
                coor_state[i].client_state = ClientStates::STATE_UPDATE_FINISHED;
            }
            // do_schedule();
        }

        update_adaptive_ccmt_b();
        call_or_tools();
    }
}

void SnicCoordinator::print_state() {
    log_debug(gc)("DGC LOG: coordinator print state");
    for (int i = 0; i < SnicGCCoorClientNum; i++) {

      unsigned long long backend_during_gc = Atomic::load(&(backend_states->hosts_global_pacer_data[i].host_GC_state));

      double marking_process = (double)(coor_state[i].marked_liveness) / ((double)backend_states->hosts_global_pacer_data[i].historyLiveness);
      unsigned long long time_to_release_core = (1.0 - MIN(marking_process, 0.95)) * coor_state[i].client_avg_mark_time;
      if(backend_during_gc == Universe::HostGCState::Idle){
        // time_to_release_core = 0;
        log_debug(gc)("DGC LOG: client %d (Idle) gc_finish_ddl %llu", coor_state[i].client_id, backend_states->hosts_global_pacer_data[i].gc_finish_ddl);
      }
      else if(backend_during_gc == Universe::HostGCState::Marking){
        // time_to_release_core = 1000000;
        log_debug(gc)("DGC LOG: client %d (DGC Marking), time to release core %llu, marked_liveness %llu, client_avg_mark_time %llu, historyLiveness %llu", coor_state[i].client_id, time_to_release_core, coor_state[i].marked_liveness, coor_state[i].client_avg_mark_time, backend_states->hosts_global_pacer_data[i].historyLiveness);
      }
      else if(backend_during_gc == Universe::HostGCState::FallbackMarking){
        log_debug(gc)("DGC LOG: client %d (FallbackMarking), time to release core %llu", coor_state[i].client_id, time_to_release_core);
      }
      else if(backend_during_gc == Universe::HostGCState::Compacting){
        log_debug(gc)("DGC LOG: client %d (Compacting), time to release core %llu", coor_state[i].client_id, time_to_release_core);
      }
      // log_debug(gc)("DGC LOG: client %d state %d, cores in use %d, time to next gc time %llu", coor_state[i].client_id, coor_state[i].client_state, coor_state[i].cores_in_use, backend_states->hosts_global_pacer_data[i].nextGCTime - os::javaTimeMillis());
    }
    log_debug(gc)("DGC LOG: coordinator print state end");
}

void SnicCoordinator::init() {
    fallback_counter = 0;
    cur_alive_client_num = SnicGCCoorClientNum;
    force_gc_cnts = new int[SnicGCCoorClientNum];

    // Adaptive CCMT model state
    for (int i = 0; i < MAX_HOST_NUM; ++i) {
        adaptive_last_compaction_ts[i] = 0;
        adaptive_last_state[i] = (unsigned long long)Universe::HostGCState::Idle;
        for (int n = 0; n < MAX_COOR_CONFIG_NUM; ++n) {
            adaptive_n_samples[i][n] = 0;
        }
    }
    // build shm
    char coor_shm_path[1024];
    // strcpy(curSnicShmMemPath, SnicShmMemPath);
    sprintf(coor_shm_path, "%s", SnicGCCoorSHMPath);
    log_info(gc)("coor_shm_path: %s", coor_shm_path);
    int shm_fd = shm_open(coor_shm_path, O_CREAT | O_RDWR, 0666);
    if(shm_fd == -1){
        log_error(gc)("DGC LOG: shm_open failed for coor_shm_path:%s", coor_shm_path);
        perror("shm_open");
        exit(1);
    }
    auto trunc_result = ftruncate(shm_fd, sizeof(CoorState) * SnicGCCoorClientNum);
    if (trunc_result == -1) {
        log_error(gc)("DGC LOG: failed to set size of coordinator shm");
        exit(0);
    }
    void* shm_ptr = mmap(NULL, sizeof(CoorState) * SnicGCCoorClientNum, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    coor_state = (CoorState*)shm_ptr;
    for(int i = 0; i < SnicGCCoorClientNum; i++){
        coor_state[i].client_id = i;
        coor_state[i].client_num = SnicGCCoorClientNum;
        coor_state[i].client_state = ClientStates::INIT_CLIENT_STATE;
        coor_state[i].client_liveness_to_update = 0ULL;
        coor_state[i].client_gc_id = -2;
        coor_state[i].client_avg_mark_time = 0ULL;
        force_gc_cnts[i] = 0;
    }
    for (int i = 0; i < MAX_HOST_NUM; ++i) {
        host_initialized[i] = false;
        host_force_gc_data[i] = new RDMAForceGCData();
        // host_force_gc_data[i]->jvmStartTime = Universe::get_jvm_start_time();
    }
    if(SnicGCShareMemEnabled){
        auto globalPacerFileFD = shm_open(SnicShmGlobalPacerPath, O_CREAT | O_RDWR, 0666);
        if (globalPacerFileFD < 0) {
            log_error(gc)("DGC LOG: create global pacer shm failed");
            exit(0);
        }
        if (ftruncate(globalPacerFileFD, sizeof(GlobalPacerData)) == -1) {
            log_error(gc)("DGC LOG: failed to set size of global pacer shm");
            exit(0);
        }
        backend_states = (GlobalPacerData*) Universe::hostMmap(NULL, sizeof(GlobalPacerData), 0, globalPacerFileFD, 0);
        log_debug(gc)("DGC LOG: mmap global pacer data at %p", backend_states);
        memset(backend_states, 0, sizeof(GlobalPacerData));
        for(int i = 0; i < MAX_HOST_NUM; i++){
          Atomic::store(&(backend_states->hosts_global_pacer_data[i].client_gc_id), (long long)(-2));
        }
        // only for test:
        backend_states->round = 6357;
        Atomic::store(&(backend_states->free), (unsigned long long)(MAX_HOST_NUM));
    }
    else{
        // build some RDMA connection
        backend_states = (GlobalPacerData*) Universe::hostMmap(NULL, sizeof(GlobalPacerData), 0, -1, 0);
        memset(backend_states, 0, sizeof(GlobalPacerData));
        for(int i = 0; i < MAX_HOST_NUM; i++){
          Atomic::store(&(backend_states->hosts_global_pacer_data[i].client_gc_id), (long long)(-2));
        }
        backend_states->round = 6357;
        Atomic::store(&(backend_states->free), (unsigned long long)(MAX_HOST_NUM));
    }
}

// Deprecated
void SnicCoordinator::coordinator_reset_global_pacer() {
    // for (int i = 0; i < SnicGCCoorClientNum; i++) {
    //     Atomic::store(&(backend_states->hosts_global_pacer_data[i].forceGC), (unsigned long long)0);
    //     Atomic::store(&(backend_states->hosts_global_pacer_data[i].forceGCTriggerTimestamp), (unsigned long long)0);
    //     Atomic::store(&(backend_states->hosts_global_pacer_data[i].startPacer), (unsigned long long)0);
    //     Atomic::store(&(backend_states->hosts_global_pacer_data[i].nextGCTime), (unsigned long long)9999999999999);
    //     log_debug(gc, ergo)("DGC LOG: set nextGCTime coordinator_reset_global_pacer: %lu", 9999999999999);
    // }
}

// Deprecated
void SnicCoordinator::do_schedule() {
    // // no need to return a host id to handle for this coordinator schedule function, because there is only a host for a client.
    // // the utility of this function is to reschedule next gc time for all hosts if required.
    // bool all_next_gc_time_valid = true;
    // HostLiveData hostLiveData[MAX_HOST_NUM];
    // for (int i = 0; i < SnicGCCoorClientNum; i++){
    //     hostLiveData[i].hostId = i;
    //     hostLiveData[i].next_gc_time = Atomic::load(&(backend_states->nextGCTime[i]));
    //     if (hostLiveData[i].next_gc_time == 9999999999999) {
    //         all_next_gc_time_valid = false;
    //         return;
    //     }
    //     hostLiveData[i].planed_next_gc_time = hostLiveData[i].next_gc_time;
    //     auto origin_avg_mark_time = Atomic::load(&(backend_states->averageGCTime[i]));
    //     hostLiveData[i].avg_mark_time = origin_avg_mark_time * SnicAvgMarkTimeAmplifyRate;
    //     hostLiveData[i].historyLiveness = Atomic::load(&(backend_states->historyLiveness[i]));
    // }
    // std::sort(hostLiveData, hostLiveData + SnicGCCoorClientNum, [](HostLiveData a, HostLiveData b)
    //           { return a.next_gc_time < b.next_gc_time; });
    // for (int i = SnicGCCoorClientNum - 1; i > 0; i--) {
    //     if (hostLiveData[i - 1].planed_next_gc_time + hostLiveData[i - 1].avg_mark_time > hostLiveData[i].planed_next_gc_time) {
    //         hostLiveData[i - 1].planed_next_gc_time = hostLiveData[i].planed_next_gc_time - hostLiveData[i - 1].avg_mark_time;
    //     }
    // }
    // unsigned long long cur_time = os::javaTimeMillis();
    // if (hostLiveData[0].planed_next_gc_time < cur_time) {
    //     hostLiveData[0].planed_next_gc_time = cur_time;
    //     auto cur_mark_host = Atomic::cmpxchg(&(backend_states->free), (unsigned long long)(MAX_HOST_NUM), (unsigned long long)(hostLiveData[0].hostId), memory_order_relaxed);
    //     if (cur_mark_host != (unsigned long long)(MAX_HOST_NUM)) {
    //         return;
    //     }
    //     auto targetHostId = hostLiveData[0].hostId;
    //     auto force_gc_type = decide_force_gc_type(targetHostId);
    //     update_force_gc_state(targetHostId, force_gc_type);
    //     for (int i = 0; i < SnicGCCoorClientNum; i++) {
    //         log_debug(gc)("DGC LOG: host %d next gc time %lu, planed next gc time %lu, avg mark time %lu", hostLiveData[i].hostId, hostLiveData[i].next_gc_time, hostLiveData[i].planed_next_gc_time, hostLiveData[i].avg_mark_time);
    //     }
    // }

    // // sleep for a period.
    // os::naked_short_sleep(100);
    // auto target_host_id = test_force_gc_host_id;
    // test_force_gc_host_id = (test_force_gc_host_id + 1) % SnicGCCoorClientNum;
    // // auto cur_mark_host = Atomic::cmpxchg(&(backend_states->free), (unsigned long long)(MAX_HOST_NUM), (unsigned long long)(test_force_gc_host_id), memory_order_relaxed);
    // // if (cur_mark_host != (unsigned long long)(MAX_HOST_NUM)) {
    // //     return;
    // // }
    // auto force_gc_type = decide_force_gc_type(target_host_id);
    // update_force_gc_state(target_host_id, force_gc_type);
}

void SnicCoordinator::coordinator_handle_client_exit(int clientId) {
    auto old_alive_num = Atomic::sub(&(cur_alive_client_num), 1);
    if (old_alive_num == 1) {
        // make coordinator exit if all clients have exited.
        exit(0);
    }
}

// CCMT == 0 --> FallBack
// CCMT != 0 --> DGC
void SnicCoordinator::update_force_gc_state(int hostId, int CCMT, unsigned long long forceGCTriggerTimestamp) {
    log_debug(gc)("DGC LOG: update force gc state for host %d, with CCMT %d, forceGCTriggerTimestamp %llu", hostId, CCMT, forceGCTriggerTimestamp);
    if(CCMT != 0){
      Atomic::store(&(coor_state[hostId].force_gc_ccmt), (int)CCMT);
    }
    if (!SnicGCShareMemEnabled) {
        if (!host_initialized[hostId]) {
            return ;
        }
        if(CCMT == 0) {
          host_force_gc_data[hostId]->forceGCType = (unsigned long long)Universe::ForceGCTypes::FallBack;
        }
        else{
          host_force_gc_data[hostId]->forceGCType = (unsigned long long)Universe::ForceGCTypes::DGC;
        }
        host_force_gc_data[hostId]->forceGCTriggerTimestamp = forceGCTriggerTimestamp;
        host_force_gc_data[hostId]->forceGCId = (unsigned long long) (Atomic::load(&(backend_states->hosts_global_pacer_data[hostId].client_gc_id)) + 1);
        host_force_gc_data[hostId]->forceGCCCMT = (unsigned long long) CCMT;
        // send to host using RDMA.
        write_force_gc_data_for_host(hostId);
        // wait RDMA writes finish.
        wait_force_gc_data_rdma_op_finish(hostId);
        // log_debug(gc)("DGC LOG: update rdma force gc state for host %d, with CCMT %d", hostId, CCMT);
        return ;
    }
    Atomic::store(&(backend_states->hosts_global_pacer_data[hostId].forceGCTriggerTimestamp), (unsigned long long)forceGCTriggerTimestamp);
    auto expected_force_gc_id = Atomic::load(&(backend_states->hosts_global_pacer_data[hostId].client_gc_id)) + 1;
    Atomic::store(&(backend_states->hosts_global_pacer_data[hostId].forceGCId), (unsigned long long)expected_force_gc_id);
    Atomic::store(&(backend_states->hosts_global_pacer_data[hostId].forceGCCCMT), (unsigned long long)CCMT);
    if(CCMT == 0){
      Atomic::store(&(backend_states->hosts_global_pacer_data[hostId].forceGCType), (unsigned long long)Universe::ForceGCTypes::FallBack);
    }
    else{
      Atomic::store(&(backend_states->hosts_global_pacer_data[hostId].forceGCType), (unsigned long long)Universe::ForceGCTypes::DGC);
    }
}

long long SnicCoordinator::coor_get_prev_forceGC_time(int hostId){
  if(SnicGCShareMemEnabled){
    return (long long)Atomic::load(&(backend_states->hosts_global_pacer_data[hostId].forceGCTriggerTimestamp));
  }
  else{
    return (long long)Atomic::load(&(host_force_gc_data[hostId]->forceGCTriggerTimestamp));
  }
}


unsigned long long SnicCoordinator::coor_get_prev_forceGC_type(int hostId){
  if(SnicGCShareMemEnabled){
    return Atomic::load(&(backend_states->hosts_global_pacer_data[hostId].forceGCType));
  }
  else{
    return Atomic::load(&(host_force_gc_data[hostId]->forceGCType));
  }
}

unsigned long long SnicCoordinator::coor_get_prev_forceGC_ccmt(int hostId){
  if(SnicGCShareMemEnabled){
    return Atomic::load(&(backend_states->hosts_global_pacer_data[hostId].forceGCCCMT));
  }
  else{
    return Atomic::load(&(host_force_gc_data[hostId]->forceGCCCMT));
  }
}


// Superseded by the CP-SAT path in coor_handle_one_solver_iteration(),
// which decides DGC vs fallback per-host inside the schedule, not via
// a dedicated dispatcher. Kept as a no-op in case a stale call site
// is still wired up; see coor_handle_one_solver_iteration() for the
// live force-GC selection.
int SnicCoordinator::decide_force_gc_type(int hostId) {
    log_error(gc)("DGC LOG: decide_force_gc_type called on host %d but the CP-SAT scheduler is the live decision path", hostId);
    exit(0);
}

void SnicCoordinator::coordinator_handle_rdma_client_init(int clientId) {
    // 1. create a local QP to use
    auto nic = RNic::create(RNicInfo::query_dev_names().at(0)).value();
    // 2. create the pair QP at server using CM
    char HostAddrPort[128];
    // parse SnicCoorHostAddrPortList and select entry by clientId
    const char* list_cstr = SnicCoorHostAddrPortList;
    std::string selected;
    if (list_cstr != nullptr && list_cstr[0] != '\0') {
        std::string list_str(list_cstr);
        std::vector<std::string> entries;
        size_t start = 0;
        while (start <= list_str.size()) {
            size_t sep = list_str.find(';', start);
            if (sep == std::string::npos) sep = list_str.size();
            std::string item = list_str.substr(start, sep - start);
            // trim spaces
            size_t l = item.find_first_not_of(' ');
            size_t r = item.find_last_not_of(' ');
            if (l != std::string::npos && r != std::string::npos) {
                item = item.substr(l, r - l + 1);
            }
            if (!item.empty()) entries.push_back(item);
            if (sep == list_str.size()) break;
            start = sep + 1;
        }
        if (!entries.empty()) {
            if (clientId < (int)entries.size()) {
                selected = entries[clientId];
            } else {
                // fallback: use modulo to avoid crash, but log an error
                log_error(gc)("SnicCoorHostAddrPortList size %d is smaller than clientId %d, using modulo fallback", (int)entries.size(), clientId);
                selected = entries[clientId % entries.size()];
            }
        }
    }
    if (selected.empty()) {
        // fallback to previous behavior if list not provided
        sprintf(HostAddrPort, "%s:%d", HostAddr, (RDMAPortForCoor + clientId));
    } else {
        snprintf(HostAddrPort, sizeof(HostAddrPort), "%s", selected.c_str());
    }
    printf("DGC LOG: coordinator handle rdma client init for client %d, HostAddrPort: %s\n", clientId, HostAddrPort);
    auto cm = new ConnectManager(HostAddrPort);
    if (cm->wait_ready(1000000, 2) == IOCode::Timeout) {
        // wait 1 second for server to ready, retry 2 times
        RDMA_ASSERT(false) << "cm connect to server timeout";
    }
    // 3. init qp for corresponding host's global pacer data.
    host_global_pacer_queue_pairs[clientId] = RC::create(nic, QPConfig()).value();
    auto qp_res = cm->cc_rc("qp-host-global-pacer-data", host_global_pacer_queue_pairs[clientId], 0, QPConfig());
    RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
    auto fetch_res = cm->fetch_remote_mr(6 + clientId * 200);
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
    host_global_pacer_queue_pairs[clientId]->bind_remote_mr(remote_attr);
    remote_global_pacer_data_base_addrs[clientId] = (char *)remote_attr.buf;
    char *local_addr = get_global_pacer_addr_for_host(clientId);
    host_global_pacer_local_mems[clientId] = Arc<RMem>(new RMem(local_addr, sizeof(GlobalPacerDataPerHost)));
    host_global_pacer_local_mrs[clientId] = RegHandler::create(host_global_pacer_local_mems[clientId], nic).value();
    host_global_pacer_queue_pairs[clientId]->bind_local_mr(host_global_pacer_local_mrs[clientId]->get_reg_attr().value());
    log_debug(gc)("DGC LOG: coordinator init global pacer qp for host %d, remote buffer base:%p, remote buffer size:%lu", clientId, remote_global_pacer_data_base_addrs[clientId], sizeof(GlobalPacerDataPerHost));
    // 4. init qp for corresponding host's force gc data.
    host_force_gc_queue_pairs[clientId] = RC::create(nic, QPConfig()).value();
    auto qp_res_force_gc = cm->cc_rc("qp-host-force-gc-data", host_force_gc_queue_pairs[clientId], 0, QPConfig());
    RDMA_ASSERT(qp_res_force_gc == IOCode::Ok) << std::get<0>(qp_res_force_gc.desc);
    auto fetch_res_force_gc = cm->fetch_remote_mr(5 + clientId * 200);
    RDMA_ASSERT(fetch_res_force_gc == IOCode::Ok) << std::get<0>(fetch_res_force_gc.desc);
    rmem::RegAttr remote_attr_force_gc = std::get<1>(fetch_res_force_gc.desc);
    host_force_gc_queue_pairs[clientId]->bind_remote_mr(remote_attr_force_gc);
    remote_force_gc_data_base_addrs[clientId] = (char *)remote_attr_force_gc.buf;
    host_force_gc_local_mems[clientId] = Arc<RMem>(new RMem(host_force_gc_data[clientId], sizeof(RDMAForceGCData)));
    host_force_gc_local_mrs[clientId] = RegHandler::create(host_force_gc_local_mems[clientId], nic).value();
    host_force_gc_queue_pairs[clientId]->bind_local_mr(host_force_gc_local_mrs[clientId]->get_reg_attr().value());
    log_debug(gc)("DGC LOG: coordinator init force gc qp for host %d, remote buffer base:%p, remote buffer size:%lu", clientId, remote_force_gc_data_base_addrs[clientId], sizeof(RDMAForceGCData));
    host_initialized[clientId] = true;
}

char* SnicCoordinator::get_global_pacer_addr_for_host(int hostId) {
    char* start_addr = (char*) (backend_states);
    return start_addr + sizeof(unsigned long long) * 2 + sizeof(GlobalPacerDataPerHost) * hostId;
}

void SnicCoordinator::read_global_pacer_data_for_host(int hostId) {
    if (!host_initialized[hostId]) {
      return ;
    }
    auto res_s = host_global_pacer_queue_pairs[hostId]->send_normal_direct(
        {.op = IBV_WR_RDMA_READ,
         .flags = IBV_SEND_SIGNALED,
         .len = (unsigned int)(sizeof(GlobalPacerDataPerHost)),
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(get_global_pacer_addr_for_host(hostId)),
         .remote_addr = (unsigned long long)remote_global_pacer_data_base_addrs[hostId],
         .imm_data = 0});
    if (res_s != IOCode::Ok) {
      log_error(gc)("DGC LOG: read global pacer data for host %d failed, res_s: %d", hostId, res_s);
      exit(0);
    }
    log_debug(gc)("DGC LOG: read global pacer data for host %d, local addr: %p, remote addr: %p", hostId, get_global_pacer_addr_for_host(hostId), remote_global_pacer_data_base_addrs[hostId]);
}

void SnicCoordinator::wait_global_pacer_data_rdma_op_finish(int hostId) {
    if (!host_initialized[hostId]) {
      return ;
    }
    log_debug(gc)("DGC LOG: waiting for global pacer data write finish for host %d, qp: %p", hostId, host_global_pacer_queue_pairs[hostId].get());
    auto res_p = host_global_pacer_queue_pairs[hostId]->wait_one_comp();
    RDMA_ASSERT(res_p == IOCode::Ok);
}

void SnicCoordinator::write_force_gc_data_for_host(int hostId) {
    auto res_s = host_force_gc_queue_pairs[hostId]->send_normal_direct(
        {.op = IBV_WR_RDMA_WRITE,
         .flags = IBV_SEND_SIGNALED,
         .len = (unsigned int)(sizeof(RDMAForceGCData)),
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(host_force_gc_data[hostId]),
         .remote_addr = (unsigned long long)remote_force_gc_data_base_addrs[hostId],
         .imm_data = 0});
    RDMA_ASSERT(res_s == IOCode::Ok);
    log_debug(gc)("DGC LOG: write force gc data for host %d, local addr: %p, remote addr: %p", hostId, host_force_gc_data[hostId], remote_force_gc_data_base_addrs[hostId]);
}

void SnicCoordinator::wait_force_gc_data_rdma_op_finish(int hostId) {
    log_debug(gc)("DGC LOG: waiting for force gc data write finish for host %d, qp: %p", hostId, host_force_gc_queue_pairs[hostId].get());
    auto res_p = host_force_gc_queue_pairs[hostId]->wait_one_comp();
    RDMA_ASSERT(res_p == IOCode::Ok);
}


void SnicCoordinator::ResourceSchedulingProblem() {
  using namespace operations_research::sat;
  // 问题参数
  int N = SnicGCCoorClientNum;  // 任务数量
  const int M = 2;  // 每个任务的资源选项数量
  int C = ParallelGCThreads;  // 资源容量上限
  const int timewindow = 10;
  const int inf = 1000000;

  std::vector<int64_t> snapshot_timestamps;
  std::vector<int64_t> during_gc_states;
  for(int i = 0; i < N; i++) {
    snapshot_timestamps.push_back(Atomic::load(&(backend_states->hosts_global_pacer_data[i].data_timestamp)));
    during_gc_states.push_back(Atomic::load(&(backend_states->hosts_global_pacer_data[i].host_GC_state)));
  }

  CpModelBuilder cp_model;

  // 创建区间变量和资源分配变量
  std::vector<std::vector<IntervalVar>> intervals(N);
  std::vector<std::vector<BoolVar>> resource_assign_vars(N);
  std::vector<IntVar> start_vars(N);
  std::vector<IntVar> end_vars(N);

  // 累积约束的需求量
  std::vector<std::vector<IntVar>> demands(N);
  // 资源需求
  std::vector<std::vector<int64_t>> R;

  std::vector<std::vector<int64_t>> J;

  std::vector<int64_t> d;

  std::vector<bool> frozen_gc_flag(N, false);


  int64_t horizon = 0;
  for (int i = 0; i < N; i++) {
    if(Atomic::load(&(backend_states->hosts_global_pacer_data[i].backend_connected)) == 0) {
      // backend has not connected, skip this round.
      return;
    }
  }
  bool BE_preparing_GC0 = false;
  for (int i = 0; i < N; i++) {
    if(Atomic::load(&(backend_states->hosts_global_pacer_data[i].client_gc_id)) > 0){ //BE is preparing GC0, only collect the relative timestamp
      if(Atomic::load(&(backend_states->hosts_global_pacer_data[i].host_GC_state)) == Universe::HostGCState::Idle){
        JavaTimeDelta[i] = ((long long)os::javaTimeMillis() - (long long)backend_states->hosts_global_pacer_data[i].data_timestamp);
        log_debug(gc)("DGC LOG: BE %d delta time: %llu", i, JavaTimeDelta[i]);
      }
    }
    log_info(gc)("KKK: %d %lld", i, JavaTimeDelta[i]);
  }

  // decide ddl R and J
  for (int i = 0; i < N; i++) {
    // not during GC
    if(during_gc_states[i] == Universe::HostGCState::Idle) {
      // here we require delta time > -10 to prevent get one deprecate force GC
      if(coor_get_prev_forceGC_time(i) - (long long)(backend_states->hosts_global_pacer_data[i].data_timestamp) < SnicCoorFrozenDGCUpperBound && coor_get_prev_forceGC_time(i) - (long long)(backend_states->hosts_global_pacer_data[i].data_timestamp) > -1* SnicCoorFrozenDGCLowerBound && coor_get_prev_forceGC_type(i) == Universe::ForceGCTypes::DGC){
        frozen_gc_flag[i] = true;
        if(coor_get_prev_forceGC_type(i) == Universe::ForceGCTypes::DGC){
          // unsigned long long time_to_release_core = coor_state[i].client_avg_mark_time;
          unsigned long long time_to_release_core = 0;
          for(int n = 0; n < backend_states->hosts_global_pacer_data[i].coor_ccmt_num; n++) {
            if(backend_states->hosts_global_pacer_data[i].coor_ccmt_R[n] == coor_get_prev_forceGC_ccmt(i)) {
              time_to_release_core = backend_states->hosts_global_pacer_data[i].coor_ccmt_a[n] * backend_states->hosts_global_pacer_data[i].historyLiveness + backend_states->hosts_global_pacer_data[i].coor_ccmt_b[n];
              break;
            }
          }
          if(time_to_release_core == 0) {
            time_to_release_core = coor_state[i].client_avg_mark_time;
            log_warning(gc)("DGC LOG: Frozen DGC, but no ccmt found for host %d, use client avg mark time: %llu", i, time_to_release_core);
          }
          d.push_back(time_to_release_core + coor_get_prev_forceGC_time(i) - (long long)(backend_states->hosts_global_pacer_data[i].data_timestamp));
          horizon = std::max(horizon, d.back());
          coor_prepare_CPSAT_predefined(i, R, J, coor_get_prev_forceGC_ccmt(i), time_to_release_core);
          log_debug(gc)("DGC LOG: ddl for host %d(Frozen DGC): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
        }else{
          // unsigned long long fallback_marking_time = backend_states->hosts_global_pacer_data[i].coor_ccmt_a[0] * backend_states->hosts_global_pacer_data[i].historyLiveness + backend_states->hosts_global_pacer_data[i].coor_ccmt_b[0];
          // unsigned long long time_to_fallback_finish = fallback_marking_time;
          // unsigned long long next_gc_ddl = time_to_fallback_finish + backend_states->hosts_global_pacer_data[i].host_GC_interval;
          // d.push_back(next_gc_ddl);
          // horizon = std::max(horizon, d.back());
          // coor_prepare_CPSAT_default(i, R, J);
          // log_debug(gc)("DGC LOG: ddl for host %d(Frozen Fallback): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
        }
      }else{
        d.push_back(MAX((long long)backend_states->hosts_global_pacer_data[i].gc_finish_ddl - (long long)backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, 0));
        horizon = std::max(horizon, d.back());
        coor_prepare_CPSAT_default(i, R, J);
        log_debug(gc)("DGC LOG: gc_finish_ddl:%llu,nonmarking_time_prediction:%llu", backend_states->hosts_global_pacer_data[i].gc_finish_ddl, backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction);
        log_debug(gc)("DGC LOG: ddl for host %d(IDLE, next forceGC:%lld): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, coor_get_prev_forceGC_time(i) - backend_states->hosts_global_pacer_data[i].data_timestamp, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
      }
    } else if(during_gc_states[i] == Universe::HostGCState::Marking) {
      unsigned long long est_mark_time = 0;
      for(int n = 0; n < backend_states->hosts_global_pacer_data[i].coor_ccmt_num; n++) {
        if(backend_states->hosts_global_pacer_data[i].coor_ccmt_R[n] == coor_get_prev_forceGC_ccmt(i)) {
          est_mark_time = backend_states->hosts_global_pacer_data[i].coor_ccmt_a[n] * backend_states->hosts_global_pacer_data[i].historyLiveness + backend_states->hosts_global_pacer_data[i].coor_ccmt_b[n];
          break;
        }
      }
      if(est_mark_time == 0) {
        est_mark_time = coor_state[i].client_avg_mark_time;
      }

      // during DGC marking
      int cores_in_use = Atomic::load(&(coor_state[i].cores_in_use));
      if(cores_in_use > 0) { // dgc client has started
        double marking_process = (double)(coor_state[i].marked_liveness) / ((double)backend_states->hosts_global_pacer_data[i].historyLiveness);
        unsigned long long time_to_release_core = (1.0 - MIN(marking_process, 0.95)) * est_mark_time;
        d.push_back(time_to_release_core);
        horizon = std::max(horizon, d.back());
        coor_prepare_CPSAT_predefined(i, R, J, cores_in_use, time_to_release_core);
        log_debug(gc)("DGC LOG: ddl for host %d(during marking): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
      }
      // about to start DGC marking
      else{ // dgc client is going to start; don't try fetch client's state, use host side state instead
        // unsigned long long time_to_release_core = coor_state[i].client_avg_mark_time;
        d.push_back(est_mark_time);
        horizon = std::max(horizon, d.back());
        coor_prepare_CPSAT_predefined(i, R, J, coor_get_prev_forceGC_ccmt(i), est_mark_time);
        log_debug(gc)("DGC LOG: ddl for host %d(about to start marking): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
      }
    } else if(during_gc_states[i] == Universe::HostGCState::Compacting) {
      unsigned long long time_since_compaction_start = os::javaTimeMillis() - (JavaTimeDelta[i] + Atomic::load(&(backend_states->hosts_global_pacer_data[i].enter_compaction_timestamp)));
      unsigned long long time_to_compaction_finish = MAX(backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction - time_since_compaction_start, 0);
      unsigned long long next_gc_ddl = MAX((long long)(time_to_compaction_finish + backend_states->hosts_global_pacer_data[i].host_GC_interval) - (long long)backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, 0);
      d.push_back(next_gc_ddl);
      horizon = std::max(horizon, d.back());
      coor_prepare_CPSAT_default(i, R, J);
      log_debug(gc)("DGC LOG: ddl for host %d(during compaction): gcddl=%llu, nonmarking_time_prediction=%llu,(data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].data_timestamp);
    } else if(during_gc_states[i] == Universe::HostGCState::FallbackMarking){
      // during fallback marking
      unsigned long long time_since_fallback_start = os::javaTimeMillis() - (JavaTimeDelta[i] + Atomic::load(&(backend_states->hosts_global_pacer_data[i].enter_fallback_timestamp)));
      unsigned long long fallback_marking_time = backend_states->hosts_global_pacer_data[i].coor_ccmt_a[0] * backend_states->hosts_global_pacer_data[i].historyLiveness + backend_states->hosts_global_pacer_data[i].coor_ccmt_b[0];
      unsigned long long time_to_fallback_finish = MAX((long long)(fallback_marking_time) - (long long)time_since_fallback_start, 0);
      unsigned long long next_gc_ddl = time_to_fallback_finish + backend_states->hosts_global_pacer_data[i].host_GC_interval;
      d.push_back(next_gc_ddl);
      horizon = std::max(horizon, d.back());
      coor_prepare_CPSAT_default(i, R, J);
      log_debug(gc)("DGC LOG: ddl for host %d(during fallback): gcddl=%llu, nonmarking_time_prediction=%llu, interval=%llu, (data timestamp=%llu)", i, d.back(), backend_states->hosts_global_pacer_data[i].nonmarking_time_prediction, backend_states->hosts_global_pacer_data[i].host_GC_interval, backend_states->hosts_global_pacer_data[i].data_timestamp);
    }
  }
  auto ddl_ranks = computeRanks(d);

  // 时间范围
  // int64_t horizon = *std::max_element(d.begin(), d.end()) + 600;


  // 创建可选区间和需求变量
  for (int i = 0; i < N; ++i) {
    start_vars[i] = cp_model.NewIntVar({0, horizon});
    end_vars[i] = cp_model.NewIntVar({0, horizon});

    for (int n = 0; n < R[i].size(); ++n) {
      // 创建资源分配选择变量
      resource_assign_vars[i].push_back(cp_model.NewBoolVar());

      // 创建可选区间变量
      IntervalVar interval = cp_model.NewOptionalIntervalVar(
          start_vars[i], J[i][n], end_vars[i], resource_assign_vars[i][n]);
      intervals[i].push_back(interval);

      // 创建需求变量
      demands[i].push_back(cp_model.NewIntVar({0, C}));
      cp_model.AddEquality(demands[i][n], R[i][n]).OnlyEnforceIf(resource_assign_vars[i][n]);
      cp_model.AddEquality(demands[i][n], 0).OnlyEnforceIf(Not(resource_assign_vars[i][n]));
    }

    // 每个任务必须选择一种资源分配方式
    cp_model.AddExactlyOne(resource_assign_vars[i]);

    // 截止时间约束
    cp_model.AddLessOrEqual(end_vars[i], d[i]);
  }



  // 添加累积约束（确保任何时刻资源使用不超过容量）
  CumulativeConstraint cumulative = cp_model.AddCumulative(C);
  for (int i = 0; i < N; i++) {
    for (int n = 0; n < R[i].size(); n++) {
      cumulative.AddDemand(intervals[i][n], demands[i][n]);
    }
  }

  // 目标函数: 最小化所有任务的起始时间（尽早执行 DGC marking,
  // 否则 trigger_ts 落在 deadline 末端,backend 等不到 current_ts > trigger_ts
  // 就被 allocation-failure 触发自己的 fallback GC,DGC 卸载实际从未生效)。
  // 同时仍然惩罚使用 fallback CCMT(R[i][0],即 ccmt=0 的方案)。
  //
  // Adaptive mode (+SnicCoorAdaptiveCCMT): hardcode "always-prefer-DGC"
  //   regardless of observed marking-time advantage. DGC's value is mutator
  //   CPU isolation (marking off-host), not marking speed. The existing
  //   ddl_ranks*5000 penalty doesn't fire for rank-0 tasks, so single-host
  //   workloads regress when EWMA learns fallback is faster.
  //   Add a large baseline penalty (1M) so fallback is only chosen when
  //   CP-SAT returns INFEASIBLE (DGC truly can't fit). Sign matches the
  //   Minimize objective: adding bias to fallback assignment makes
  //   Minimize avoid it.
  const int64_t adaptive_dgc_bias = SnicCoorAdaptiveCCMT ? 1000000LL : 0LL;
  LinearExpr objective_expr;
  for (int i = 0; i < N; ++i) {
    objective_expr += start_vars[i];
    objective_expr += ddl_ranks[i] * 5000 * resource_assign_vars[i][0];  // 惩罚使用 fallback
    objective_expr += adaptive_dgc_bias * resource_assign_vars[i][0];     // adaptive: always prefer DGC
  }
  cp_model.Minimize(objective_expr);

  // 求解器参数设置
  SatParameters parameters;
  parameters.set_max_time_in_seconds(30);
  parameters.set_num_search_workers(8);  // 使用多线程
  // parameters.set_log_search_progress(true);
  parameters.set_log_to_stdout(false);

  // 求解问题
  LOG(INFO) << "Solving problem with CP-SAT";
  auto start_time = std::chrono::high_resolution_clock::now();

  const CpSolverResponse response = SolveWithParameters(cp_model.Build(), parameters);

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  LOG(INFO) << "Time taken: " << duration.count() << " milliseconds";

  // 输出结果

  // if(!SnicGCShareMemEnabled) {
  //   sync_update_global_pacer_data();
  // }

  if (response.status() == CpSolverStatus::OPTIMAL ||
      response.status() == CpSolverStatus::FEASIBLE) {
    LOG(INFO) << "Objective value: " << response.objective_value();

    // 输出任务调度方案
    std::cout << "\nTask schedule:" << std::endl;
    for (int i = 0; i < N; ++i) {
      int64_t start_val = SolutionIntegerValue(response, start_vars[i]);
      int64_t end_val = SolutionIntegerValue(response, end_vars[i]);
      int64_t duration_val = end_val - start_val;
      if (d[i] == 0) {
        log_debug(gc)("DGC LOG: invalid ddl 0 for host %d when OPTIMAL", i);
      }
      std::cout << "Task " << i << "(" << (during_gc_states[i]==Universe::HostGCState::Idle?"Idle":"during gc") << ")"<< ": Start=" << start_val
                << ", End=" << end_val
                << ", Duration=" << duration_val
                << ", DDL=" << d[i] << ", Resources=[";

      int CCMT_rst = -1;
      for (int n = 0; n < R[i].size(); ++n) {
        if (SolutionBooleanValue(response, resource_assign_vars[i][n])) {
          std::cout << "{CCMT=" << R[i][n] << "(dur=" << J[i][n] << ")} ";
          CCMT_rst = R[i][n];
        }
      }
      std::cout << "]" << std::endl;

      if(CCMT_rst != -1 && during_gc_states[i] == Universe::HostGCState::Idle && !frozen_gc_flag[i]) {
        update_force_gc_state(i, CCMT_rst, snapshot_timestamps[i] + start_val);
      }

    }
  } else {
    // LOG(INFO) << "No solution found.";
    LOG(INFO) << "Status: " << CpSolverStatus_Name(response.status());
    fallback_counter ++;
    if(fallback_counter > SnicCoorFallbackTrigger) {
      fallback_counter = 0;
      // if INFEASIBLE, sned forceGC to the latest ddl task
      int latest_ddl_task_id = -1;
      for(int i = 0; i < N; i++) {
        if(frozen_gc_flag[i] == false && during_gc_states[i] == Universe::HostGCState::Idle) {
          latest_ddl_task_id = i;
          break;
        }
      }
      for(int i = 0; i < N; i++) {
        if(d[i] < d[latest_ddl_task_id] && frozen_gc_flag[i] == false && during_gc_states[i] == Universe::HostGCState::Idle) {
          latest_ddl_task_id = i;
        }
      }
      if(latest_ddl_task_id != -1){
        unsigned long long fallback_marking_time = backend_states->hosts_global_pacer_data[latest_ddl_task_id].coor_ccmt_a[0] * backend_states->hosts_global_pacer_data[latest_ddl_task_id].historyLiveness + backend_states->hosts_global_pacer_data[latest_ddl_task_id].coor_ccmt_b[0];
        if (d[latest_ddl_task_id] < fallback_marking_time) {
          log_debug(gc)("DGC LOG: force fallback gc to host %d when INFEASIBLE, ccmt=%ld, ddl=%ld, fallback_marking_time=%llu", latest_ddl_task_id, R[latest_ddl_task_id][0], d[latest_ddl_task_id], fallback_marking_time);
        }
        update_force_gc_state(latest_ddl_task_id, R[latest_ddl_task_id][0], snapshot_timestamps[latest_ddl_task_id] + (d[latest_ddl_task_id] - fallback_marking_time));
      }
    }

  }

  // 输出统计信息
  LOG(INFO) << "Statistics";
  LOG(INFO) << CpSolverResponseStats(response);
}

void SnicCoordinator::call_or_tools() {
    ResourceSchedulingProblem();
}

void SnicCoordinator::sync_update_global_pacer_data() {
  for(int i = 0; i < SnicGCCoorClientNum; i++) {
    read_global_pacer_data_for_host(i);
  }
  for(int i = 0; i < SnicGCCoorClientNum; i++) {
    wait_global_pacer_data_rdma_op_finish(i);
  }
}



std::vector<int64_t> SnicCoordinator::computeRanks(const std::vector<int64_t>& input) {
    const size_t size = input.size();
    std::vector<int64_t> ranks(size);

    // 创建索引数组并排序（按 input 的值升序）
    std::vector<size_t> indices(size);
    for (size_t i = 0; i < size; ++i) {
        indices[i] = i;
    }

    // 按 input 的值排序 indices
    std::sort(indices.begin(), indices.end(),
        [&input](size_t a, size_t b) { return input[a] > input[b]; });

    // 填充排名（从 0 或 1 开始）
    for (size_t i = 0; i < size; ++i) {
        // 处理相同值的情况：相同值的排名相同
        if (i > 0 && input[indices[i]] == input[indices[i - 1]]) {
            ranks[indices[i]] = ranks[indices[i - 1]];
        } else {
            ranks[indices[i]] = i + 1; // 排名从 1 开始
            // 如果希望从 0 开始，直接赋值 i 即可
        }
    }

    return ranks;
}

void SnicCoordinator::coor_prepare_CPSAT_default(int hostId, std::vector<std::vector<int64_t>>& R, std::vector<std::vector<int64_t>>& J){
  std::vector<int64_t> R_tmp;
  std::vector<int64_t> J_tmp;
  for(int plan = 0; plan < backend_states->hosts_global_pacer_data[hostId].coor_ccmt_num; plan++) {
    R_tmp.push_back(backend_states->hosts_global_pacer_data[hostId].coor_ccmt_R[plan]);
    J_tmp.push_back((backend_states->hosts_global_pacer_data[hostId].coor_ccmt_a[plan] * backend_states->hosts_global_pacer_data[hostId].historyLiveness + backend_states->hosts_global_pacer_data[hostId].coor_ccmt_b[plan]));
  }
  R.push_back(R_tmp);
  J.push_back(J_tmp);
}

void SnicCoordinator::coor_prepare_CPSAT_predefined(int hostId, std::vector<std::vector<int64_t>>& R, std::vector<std::vector<int64_t>>& J, int64_t cores_in_use, int64_t time_to_release_core){
  std::vector<int64_t> R_tmp;
  std::vector<int64_t> J_tmp;
  R_tmp.push_back(cores_in_use);
  J_tmp.push_back(time_to_release_core);
  R.push_back(R_tmp);
  J.push_back(J_tmp);
}
