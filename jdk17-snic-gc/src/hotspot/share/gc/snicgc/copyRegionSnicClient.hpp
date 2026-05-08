
/*
 * Copyright (c) 2013, 2021, Red Hat, Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 *
 */

#include "rdma/lib.hh"
#include "snicClient.hpp"

#include <mutex>

#define MAX_ULL 18446744073709551615ULL
#define REGION_NOT_COPIED 0
#define REGION_ON_EVICTION 32
#define REGION_ON_TRANS 64
#define REGION_COPIED 128

#define REGION_STATE_MASK 31

// #define REGION_IN_CRITICAL_SECTION 3
#define CHECK_PENDING_THRESHOLD 1000
#define SATB_ROOTS_NORMAL_ACK 1
#define SATB_ROOTS_FINAL_ACK 2
#define REMOTE_GC_ROOTS_COPY_FINISH_ACK 3
#define TASK_QUEUE_ROOTS_FINISH_ACK 4
#define SATB_ROOTS_FINISH_ACK 5
#define RDMA_DGC_LIVENESS_UPDATE_THRESHOLD 10000

struct SnicVirtualSpaceNode{
    int hostId;
    int map_fd; // SHM fd backing the VSN (dual-mapped at base and real_local_base); -1 for CDS (file-backed by JVM itself).
    int index;
    unsigned long long base;           // VA the host uses, also aliased in the client for narrow-klass / pointer decoding.
    unsigned long long top;
    unsigned long long sz;
    unsigned long long cur_top;
    unsigned long long real_local_base; // Client-picked VA where the NIC MR is registered. Decoupled from `base` so ibv_reg_mr never collides with the client JVM's own CCS/metaspace VMAs.
    rdmaio::Arc<rdmaio::qp::RC> qp;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem;
};
class CopyRegionSnicClient;
class WorkerPendingMap;

// HEAD WorkerPendingMap class removed; using theirs definition below (with 4-arg push)

class RegionArrivalQueue {
private:
    int* _region_arrival_queue;
    alignas(64) volatile int _head; // marker use only
    alignas(64) volatile int _tail; // controller write only, marker read only
    alignas(64) volatile bool _is_empty; // shared
    // pthread_mutex_t _mutex;
public:
    RegionArrivalQueue() = default;
    RegionArrivalQueue(uint region_num);
    ~RegionArrivalQueue();
    void push(uint region_idx);
    int pop();
    bool is_empty();
    void clear();
    // void lock();
    // void unlock();
    // int try_lock();
};

struct RemoteAddressTranslationEntry {
    unsigned short local_region_id;  // Local host id
};
struct LocalAddressTranslationEntry {
    unsigned short remote_region_id;  // Remote host id
};

class CopyRegionSnicClient : public SnicClient {
public:
    // rdma related data fields.
    rdmaio::ConnectManager* cm;
    rdmaio::Arc<rdmaio::RNic> nic;
    rdmaio::Arc<rdmaio::RNic> nic2;
    rdmaio::Arc<rdmaio::qp::RC> qp_heap;
    rdmaio::Arc<rdmaio::qp::RC> qp_heap2;
    rdmaio::Arc<rdmaio::qp::RC> qp_bitmap;
    rdmaio::Arc<rdmaio::qp::RC> qp_liveness;
    rdmaio::Arc<rdmaio::qp::RC> qp_gc_roots_buffer;
    rdmaio::Arc<rdmaio::qp::RC> qp_force_gc_by_dpu_client;
    rdmaio::Arc<rdmaio::qp::RC> qp_rdma_prefetch_finish_flag;
    // unsigned long long heapBase;
    // unsigned long long heapSize;
    unsigned long long remoteBitmapBase;
    unsigned long long remoteBitmapSize;
    unsigned long long remoteLivenessBase;
    unsigned long long remoteLivenessSize;
    unsigned long long remoteGCRootsBufferBase;
    unsigned long long remoteGCRootsBufferSize;
    unsigned long long remoteForceGCByDPUClientBase;
    unsigned long long remoteForceGCByDPUClientSize;
    unsigned long long remoteRdmaPrefetchFinishFlagBase;
    unsigned long long remoteRdmaPrefetchFinishFlagSize;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_heap;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_heap2;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_heap;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_heap2;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_bitmap;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_bitmap;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_liveness;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_liveness;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_gc_roots_buffer;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_gc_roots_buffer;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_force_gc_by_dpu_client;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_force_gc_by_dpu_client;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_rdma_prefetch_finish_flag;
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_rdma_prefetch_finish_flag;
    std::vector<SnicHeapRegion> region_info;
    std::vector<int> pending_count_table;
    std::vector<int> ongoing_mark_count_table;
    volatile unsigned long long* region_copied_bitmap;
    // WorkGang* snic_gc_workers;
    // ShenandoahObjToScanQueueSet* task_queues;
    // a random number used to distinguish different host.
    int host_random_num = 0;
    uint _gc_id;
    // uint64_t** live_data_caches;
    size_t on_trans_region_number;

    size_t on_trans_region_number1;
    size_t on_trans_region_number2;

    size_t done_trans_region_number;

    size_t done_trans_region_number1;
    size_t done_trans_region_number2;

    bool is_one_gc_final_stage = false;
    WorkerPendingMap** workerPendingMaps;
    RegionArrivalQueue** regionArrivalQueue;
    pthread_t wait_copy_finish_thread_id;
    pthread_t wait_pre_copy_finish_thread_id;
    pthread_mutex_t wait_copy_finish_mutex;
    pthread_cond_t wait_copy_finish_cond;
    pthread_cond_t wait_pre_copy_finish_cond;
    pthread_mutex_t wait_pre_copy_finish_mutex;
    int received_region_num;
    int received_region_num1;
    int received_region_num2;
    int satb_handle_cnt = 0;
    bool handling_task_queue_roots = false;
    bool handling_satb_roots = false;
    bool sent_satb_roots_req = false;
    char* gc_roots_buffer;
    int* local_force_gc_by_dpu_client;
    int* gc_roots_copy_finish_flag;
    int* local_rdma_prefetch_finish_flag;
    size_t regionTopIdx = 0;
    // ShenandoahMarkBitMap* bitmap;
    SnicHeapRegion* regions;
    // uint64_t* live_count;
    std::vector<SnicVirtualSpaceNode*> virtualSpaceNodes;
    uint64_t total_sent_liveness = 0;
    int finished_worker_count = 0;
    std::mutex task_count_lock;
    size_t* try_mark_counts;
    size_t* success_mark_counts;
    int cur_handling_rpc_type = 0;
    int normal_satb_roots_handle_count = 0;
    int force_terminate_counts = 0;
    unsigned long long is_prefetched = 0;
    unsigned long long time_to_wait_for_prefetched_gc = 0;
    uint64_t* prev_total_liveness;
    alignas(64) volatile int should_force_tasks_finish = 0;
    alignas(64) volatile int under_last_round_mark_count = 0;
    alignas(64) volatile int force_finished_task_cnt = 0;
    alignas(64) volatile int terminate_condition_for_address_translation = 0; // 0:resume, 1: term
    int local_mem_pool_free_region_count = 0;

    LocalAddressTranslationEntry* local_addr_translation_table;  // Address translation table
    int local_memory_region_alloacted;
    void* local_memory_pool_base;                                   // Base address of local memory pool
    size_t local_memory_pool_size;                                  // Size of local memory pool
    size_t local_memory_pool_used;                                 // Used size of local memory pool
    size_t local_memory_pool_region_limit;
    // int local_memory_pool_region_limit;
    std::mutex memory_pool_lock;                                   // Lock for memory pool management
    unsigned long long local_heap_base;                            // Local heap base (when using translation)
    rdmaio::Arc<rdmaio::rmem::RMem> local_mem_pool;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_pool;
    rdmaio::Arc<rdmaio::rmem::RegHandler> local_mr_pool2;


    CopyRegionSnicClient();
    // int runRPCServer();
    int runRDMAClient(int hostId);
    int runRDMAClientWithTranslation(int hostId);
    // void resolveRPC(void* buffer) override;
    void handleRPC(int rpcType, int hostId, int bufferSize, void* payload) override;
    void reserveMemRegion(unsigned long long addr, unsigned long long length);
    void copyRegion(int bufferSize, void* payload);
    void handleRoot(int bufferSize, void* payload);
    void handleRootAndCommit(int bufferSize, void* payload);
    void handleNewVirtualSpaceNode(int hostId, int bufferSize, void* payload);
    void fetchKlass(int hostId, int bufferSize, void* payload);
    // SnicGCRDMABatchFetchKlass path — one bulk RDMA READ over the CCS
    // delta since the last sync, on a freshly-recreated QP. See case 12
    // in handleRPC and the host-side send_rpc(12) before the
    // copy_region_metadata RPC in shenandoahConcurrentMark.cpp.
    void bulkSyncCcs(int hostId, unsigned long long ccs_hwm, unsigned long long ccs_base);

    void initBitmapQP(void* ptr, size_t sz, int hostId);
    void initLivenessQP(void* ptr, size_t sz, int hostId);
    void initGCRootsBufferQP(void* ptr, size_t sz, int hostId);
    void initForceGCByDPUClientQP(void* ptr, size_t sz, int hostId);
    void initRdmaPrefetchFinishFlagQP(void* ptr, size_t sz, int hostId);
    void writeBitmapRDMA();
    void waitBitmapCQ();

    void writeLivenessRDMA();
    void waitLivenessCQ();
    void writeForceGCByDPUClientRDMA();
    void waitForceGCByDPUClientCQ();
    // Override of SnicClient::forceGCByDPUClient — sets the local flag
    // and issues the RDMA write+CQ wait. Inlined so snicClient.cc can
    // dispatch via virtual call without seeing the implementation.
    void forceGCByDPUClient() override {
        local_force_gc_by_dpu_client[0] = 1;
        writeForceGCByDPUClientRDMA();
        waitForceGCByDPUClientCQ();
    }
    void writeRdmaPrefetchFinishFlagRDMA();
    void waitRdmaPrefetchFinishFlagCQ();
    void snic_do_compressed_oops(uint worker_id);
    void snic_do_compressed_oops_address_translation(uint worker_id);
    void checkRegionArrivalQueue(uint worker_id);
    void checkRegionArrivalQueueAddressTranslation(uint worker_id);
    void snic_do_compressed_oop(narrowOop* p, uint worker_id, int type = -1, bool should_count_liveness = true);
    bool check_is_narrow_oop(HeapWord* p);
    void snic_concurrent_inc_liveness(unsigned long long r, intptr_t start, size_t s, uint worker_id);
    void snic_concurrent_inc_ongoing_mark(unsigned long long r, intptr_t start, int64_t s, uint worker_id);
    void snic_concurrent_dec_ongoing_mark(unsigned long long r, intptr_t start, int64_t s, uint worker_id);
    // void copy_one_normal_region_async(unsigned long long region_idx, bool is_prefetch = false);
    // int copy_one_region_async_auto_huge(unsigned long long region_idx, bool is_prefetch = false);
    int copy_one_region_async_auto_huge(unsigned long long region_idx, bool is_prefetch = false, bool force = true);
    int copy_one_normal_region_async(unsigned long long region_idx, bool is_prefetch, bool force = true);

    int copy_one_region_async_no_address_translation(unsigned long long region_idx, bool is_prefetch = false);
    int copy_one_region_async_address_translation(unsigned long long region_idx, unsigned long long local_region_cache_index, bool is_prefetch = false);
    int copy_one_huge_region_async(unsigned long long region_idx, int huge_region_size, bool is_prefetch = false, bool force = true);
    void issue_fetch_region_by_pending_length_heap1(uint candidate_region_number);
    void issue_fetch_region_by_pending_length_heap2(uint candidate_region_number);
    int issue_fetch_region_by_pending_length_heap1_address_translation(uint candidate_region_number);
    int issue_fetch_region_by_pending_length_heap2_address_translation(uint candidate_region_number);

    ShenandoahObjToScanQueue* get_queue(uint worker_id);
    void initialize();
    // void start_copy_region_thread();
    void copy_region_metadata(int bufferSize, void* payload);
    void conc_copy_region_handle_root(int bufferSize, void* payload, int is_prefetched);
    void wait_part_copy_finish_work();
    void start_wait_copy_finish_thread();
    void wait_pre_copy_finish_work();
    void start_wait_pre_copy_finish_thread();
    void wait_thread_complete();
    void wait_pre_copy_thread_complete();
    void start_handle_task_queue_roots();
    void finish_handle_task_queue_roots();
    bool during_handle_task_queue_roots();
    void start_handle_satb_roots();
    void finish_handle_satb_roots();
    bool during_handle_satb_roots();
    void handle_satb_roots(int bufferSize, void* payload);
    void handle_satb_roots_commit(int bufferSize, void* payload);
    int copy_remote_gc_roots_buffer(int bufferSize, void* payload);
    bool oop_is_humongous(oop obj);
    bool humongous_oop_regions_copy_finished(oop obj);
    bool humongous_oop_regions_copy_finished(oop obj, int* region_idx);
    bool humongous_oop_regions_copy_finished_address_translation(oop obj);
    bool humongous_oop_regions_copy_finished_address_translation(oop obj, int* region_idx);
    bool checkMarkerTerminateCondition(uint worker_id);
    bool checkRDMAControllerTerminateCondition();
    size_t compute_mr_idx(int hostId, size_t idx);
    void read_back_cur_host_virtual_nodes(int hostId);
    void unmapMemSpace(void* addr, unsigned long long length, int hostId);
    void* mapMemSpace(int fd, unsigned long long addr, unsigned long long length, int hostId);
    void unmapCurHostMemSpaces(int hostId);
    void remapCurHostMemSpaces(int hostId);
    int decide_force_terminate_counts();

    unsigned long long translate_remote_to_local(unsigned long long remote_addr);
    unsigned long long translate_local_to_remote(unsigned long long local_addr);
    bool ensure_region_mapped(unsigned long long region_idx);


    void initialize_address_translation();
    int select_local_cache_index_simple();
    int select_local_cache_index_range_simple(int region_count);
    // int select_local_cache_index();
    // int select_local_cache_index_range(int region_count, int candidate_hotness = 0);
    int select_local_cache_range(int region_count, int candidate_hotness);
    int select_local_cache_normal(int candidate_hotness);
    int select_local_cache_huge(int region_count);
    // int select_region_to_evict(int required_region_count);

    // lock from REGION_COPIED(def 128) to +1
    // int lock_read_region_bitmap(unsigned long long region_idx);
    // int unlock_read_region_bitmap(unsigned long long region_idx);
    // int evict_region_bitmap(unsigned long long region_idx);
    // // 0:succ 1:failed
    // int set_region_bitmap(unsigned long long region_idx, int target);
    // int loop_until_change_region_bitmap(unsigned long long region_idx, int prev, int target);


    void reset_address_translation_simple();

    inline oop translate_oop(oop obj){
        unsigned long long remote_addr = (unsigned long long)obj;
        int remote_region_idx = (remote_addr - heapBase) >> regionSizeShift;
        unsigned long long remote_addr_delta = remote_addr - region_info[remote_region_idx].bottom;
        unsigned short local_region_id = region_info[remote_region_idx].local_region_id;
        unsigned long long local_addr = (unsigned long long)(local_memory_pool_base) + ((unsigned long long)local_region_id << regionSizeShift) + remote_addr_delta;
        // log_info(gc)("LHT LOG: translate_oop, remote_addr=%llx, remote_region_idx=%d, remote_addr_delta=%llx, local_region_id=%d, local_addr=%llx", remote_addr, remote_region_idx, remote_addr_delta, local_region_id, local_addr);
        if(local_addr_translation_table[local_region_id].remote_region_id != remote_region_idx){
            log_info(gc)("LHT LOG: ASSERT! local_addr_translation_table[%d].remote_region_id != %d, remote_region_state=%d", local_region_id, remote_region_idx, region_copied_bitmap[remote_region_idx]);
            log_info(gc)("LHT LOG: obj=%p, klass=%p, size=%d", obj, obj->klass(), obj->size());
        }
        return (oop)(local_addr);
    }

    std::mutex region_bitmap_add_read_lock_mutex; // lock for region_bitmap_add_read_lock

    inline bool region_is_local(unsigned long long region_idx){
        auto region_state = Atomic::load(&region_copied_bitmap[region_idx]);
        return confirm_region_bitmap(REGION_COPIED, region_state);
    }

    inline bool region_is_uncopied(unsigned long long region_idx){
        auto region_state = Atomic::load(&region_copied_bitmap[region_idx]);
        return confirm_region_bitmap(REGION_NOT_COPIED, region_state);
    }

    inline unsigned long long region_bitmap_add_read_lock(unsigned long long region_idx){
        return Atomic::add(&region_copied_bitmap[region_idx], (unsigned long long)1);

        // int retry_times = 0;
        // while(true){
        //     auto prev = Atomic::load(&region_copied_bitmap[region_idx]);
        //     if(!confirm_region_bitmap(target_state, prev)){
        //         return false;
        //     }
        //     auto res = Atomic::cmpxchg(&region_copied_bitmap[region_idx], prev, prev + 1);
        //     if(res == prev){
        //         return true;
        //     }
        //     else{
        //         retry_times++;
        //         if(retry_times > 1000000000){
        //             log_error(gc)("LHT LOG: region_bitmap_add_read_lock_COPIED retry times > 1000000000, region_idx=%lu, prev=%llu", region_idx, prev);
        //             exit(0);
        //         }
        //     }
        // }
        // return false;
    }

    inline bool region_bitmap_dec_read_lock(unsigned long long region_idx){
        Atomic::dec(&region_copied_bitmap[region_idx]);
        return true;
    }

    inline bool region_bitmap_set_value_with_write_lock(unsigned long long region_idx, unsigned long long prev_state, unsigned long long new_state){
        int retry_times = 0;
        while(true){

            auto res = Atomic::cmpxchg(&region_copied_bitmap[region_idx], prev_state, new_state);
            if(res == prev_state){
                return true;
            }
            else{
                retry_times++;
                if(retry_times > 1000000000){
                    log_error(gc)("LHT LOG: region_bitmap_add_read_lock_COPIED retry times > 1000000000, region_idx=%lu, prev=%llu, new_state=%llu", region_idx, res, new_state);
                    exit(0);
                }
                if(!confirm_region_bitmap(prev_state, res)){
                    log_error(gc)("LHT LOG: ASSERT! region_bitmap_set_value_with_write_lock failed, region_idx=%lu, current_state=%llu, prev_state=%llu, new_state=%llu", region_idx, res, prev_state, new_state);
                    os::abort();
                }
            }
        }
        return false;
    }

    // inline bool region_bitmap_set_value(unsigned long long region_idx, unsigned long long prev, unsigned long long value){

    //     unsigned long long prev = Atomic::cmpxchg(&region_copied_bitmap[region_idx], prev, value);
    //     return prev ==
    // }

    inline bool confirm_region_bitmap(unsigned long long target_state, unsigned long long actual_state){
        return get_region_bitmap_base_state(actual_state) == target_state;
    }

    inline unsigned long long get_region_bitmap_base_state(unsigned long long state){
        return state - (state & REGION_STATE_MASK);
    }

    inline bool check_region_bitmap_read_lock(unsigned long long region_idx, unsigned long long target_state){
        auto cur = Atomic::load(&region_copied_bitmap[region_idx]);
        if(!confirm_region_bitmap(target_state, cur)){
            log_error(gc)("LHT LOG: ASSERT! check_region_bitmap_read_lock failed, region_idx=%lu, cur=%llu, target_state=%llu", region_idx, cur, target_state);
            os::abort();
        }
        if(cur != target_state && cur <= target_state + snic_gc_workers->active_workers() + 1){
            return true;
        }
        return false;
    }

    inline void deprecated_function(){
        log_error(gc)("ASSERT: deprecated function is called");
        os::abort();
    }
    // inline bool check_region_bitmap_read_lock(unsigned long long region_idx, unsigned long long target_state){
    //     auto cur = Atomic::load(&region_copied_bitmap[region_idx]);
    //     if(cur != target_state && )
    //     return true;
    // }
};



class WorkerPendingMap {
    public:
        alignas(64) volatile int worker_marked_count = 0;
        std::vector<ShenandoahObjToScanQueue*> workerLocalVec;
        WorkerPendingMap() = default;
        WorkerPendingMap(uint region_num) {
            for (uint i = 0; i < region_num; ++i) {
                auto que = new ShenandoahObjToScanQueue();
                que->initialize();
                workerLocalVec.push_back(que);
            }
        }
        bool push(uint region_idx, ShenandoahMarkTask task, volatile unsigned long long* region_copied_bitmap, CopyRegionSnicClient* snic_client) {
            auto res = Atomic::load(&region_copied_bitmap[region_idx]);
            if(snic_client->get_region_bitmap_base_state(res) == REGION_NOT_COPIED || snic_client->get_region_bitmap_base_state(res) == REGION_ON_EVICTION || snic_client->get_region_bitmap_base_state(res) == REGION_ON_TRANS){
                workerLocalVec[region_idx]->push(task);
                return true;
            }
            else{
                log_error(gc)("LHT LOG: ASSERT! push failed, region_idx=%lu, current_state=%llu", region_idx, res);
                os::abort();
                return false;
            }
        }
        // 3-arg overload from HEAD (no AddressTranslation): retry-loop CAS to enqueue while
        // region is in-flight, abort if region already in COPIED state.
        bool push(uint region_idx, ShenandoahMarkTask task, volatile unsigned long long* region_copied_bitmap) {
            if(region_copied_bitmap[region_idx] == (unsigned long long) (REGION_NOT_COPIED)){
                workerLocalVec[region_idx]->push(task);
                return true;
            }
            auto prev_val = region_copied_bitmap[region_idx];
            auto res = prev_val;
            int retry_times = 0;
            while(true){
                retry_times++;
                if(retry_times > 1000000000){
                    log_error(gc)("DGC LOG: retry times > 1000000000, region_idx=%u, prev_val=%llu", region_idx, prev_val);
                    exit(0);
                }
                prev_val = region_copied_bitmap[region_idx];
                if(prev_val == (char) (REGION_COPIED)){
                    break;
                }
                res = Atomic::cmpxchg(&region_copied_bitmap[region_idx], (unsigned long long) prev_val, (unsigned long long) (prev_val + 1));
                if(res == prev_val){
                    workerLocalVec[region_idx]->push(task);
                    Atomic::dec(&region_copied_bitmap[region_idx]);
                    return true;
                }
            }
            return false;
        }
        ShenandoahMarkTask pop(uint region_idx) {
            ShenandoahMarkTask task;
            auto res = workerLocalVec[region_idx]->pop(task);
            if (res) {
                return task;
            }
            return ShenandoahMarkTask(NULL, false, false);
        }
        void clear() {
            for (auto& queue : workerLocalVec) {
                queue->clear();
            }
            worker_marked_count = 0;
        }
    };



class ShenandoahSNICCMTask : public AbstractGangTask {
private:
    int _buf_size;
    unsigned long long* _msg_buf;
    CopyRegionSnicClient* const _snic_client;
    std::vector<int> indexes;
    int started_worker_count = 0;
    int worker_num = 0;
    bool is_SATB = false;
public:
    ShenandoahSNICCMTask(int size, unsigned long long* buf, CopyRegionSnicClient* cli, std::vector<int> parts, bool is_SATB) :
        AbstractGangTask("SNIC Concurrent Mark"), _buf_size(size), _msg_buf(buf), _snic_client(cli), indexes(parts), is_SATB(is_SATB) {
        worker_num = (int) (_snic_client->snic_gc_workers->active_workers());
    }
    void work(uint worker_id) {
        if (worker_id >= _snic_client->snic_gc_workers->active_workers()) {
            return ;
        }
        log_debug(gc)("GC(%u,%d) DGC LOG: worker %u starts ShenandoahSNICCMTask", _snic_client->_gc_id, _snic_client->clientId, worker_id);
        auto worker_queue = _snic_client->get_queue(worker_id);
        uint index_num = (uint)(indexes.size() - 1);
        uint process_id = worker_id;
        int push_count = 0;
        while (process_id < index_num) {
            int start = indexes[process_id];
            int end = indexes[process_id + 1];
            for (int handle_idx = start; handle_idx < end; ++handle_idx) {
                if (_msg_buf[handle_idx] == (unsigned long long) (-1)) {
                    continue;
                }
                oop obj = (oop) (_msg_buf[handle_idx]);
                // bool upgraded = false;
                // bool mark_rst = _snic_client->bitmap->mark_strong((HeapWord*) (obj), upgraded);
                // // _snic_client->try_mark_counts[(worker_id + 1) * 8 - 1] += 1;
                // if (!mark_rst) {
                //     continue;
                // }
                if(is_SATB){
                  bool upgraded = false;
                  bool bitmap_mark_rst = _snic_client->bitmap->mark_strong((HeapWord*) (obj), upgraded);
                  if (!bitmap_mark_rst) {
                      continue;
                  }
                }
                bool skip_live = !is_SATB;

                unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj)
                    - (unsigned long long)(_snic_client->heapBase)) >> (_snic_client->regionSizeShift);
                if (r_idx_o >= _snic_client->heapRegionNumber) {
                    log_error(gc)("DGC LOG: worker %u handle oop in invalid region,r_idx_o=%llu,regionNumber=%zu,obj=%p,idx=%d,start=%d,end=%d",
                        worker_id, r_idx_o, _snic_client->heapRegionNumber, obj, handle_idx, start, end);
                    exit(1);
                }
                if (_snic_client->region_info[r_idx_o].top == 0) {
                    if (_snic_client->is_prefetched == 1) {
                        log_debug(gc)("DGC LOG: worker %u handles obj in null region %llu", worker_id, r_idx_o);
                    }
                    continue;
                }
                // logic to support concurrent copy region
                if (SnicConcCopyRegion && _snic_client->region_copied_bitmap[r_idx_o] != (unsigned long long) (REGION_COPIED)) {
                    ShenandoahMarkTask oneTask(obj, skip_live, false);
                    if(_snic_client->workerPendingMaps[worker_id]->push(r_idx_o, oneTask, _snic_client->region_copied_bitmap, _snic_client)){
                        if (_snic_client->is_prefetched == 1) {
                            log_debug(gc)("DGC LOG: worker %u push region %llu to pending map (1), is_prefetched=%llu", worker_id, r_idx_o, _snic_client->is_prefetched);
                        }
                        continue;
                    }
                }
                if (_snic_client->oop_is_humongous(obj)) {
                    int uncopied_region_idx = -1;
                    if (!_snic_client->humongous_oop_regions_copy_finished(obj, &uncopied_region_idx)) {
                        ShenandoahMarkTask oneTask(obj, skip_live, false);
                        if (_snic_client->workerPendingMaps[worker_id]->push(uncopied_region_idx, oneTask, _snic_client->region_copied_bitmap, _snic_client)) {
                            if (_snic_client->is_prefetched == 1) {
                                log_debug(gc)("DGC LOG: worker %u push region %d to pending map (2), is_prefetched=%llu", worker_id, uncopied_region_idx, _snic_client->is_prefetched);
                            }
                            continue;
                        }
                    }
                }
                // Skip above-TAMS objects: they are implicitly live and MUST NOT
                // have a bitmap bit. Also skip objects in regions the host didn't
                // send (region_info==NULL means bottom==TAMS, i.e. the entire
                // region is above TAMS or empty — nothing to mark).
                if (_snic_client->region_info[r_idx_o].top == 0 ||
                    (unsigned long long)obj >= _snic_client->region_info[r_idx_o].top) {
                    continue;
                }
                bool upgraded = false;
                bool mark_rst = _snic_client->bitmap->mark_strong((HeapWord*) (obj), upgraded);
                if (mark_rst) {
                    push_count += 1;
                    ShenandoahMarkTask oneTask(obj, upgraded, false);
                    worker_queue->push(oneTask);
                    _snic_client->snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
                }
            }
            process_id += _snic_client->snic_gc_workers->active_workers();
        }
        // Atomic::add(&started_worker_count, 1);
        // // wait until all other workers are ready to start do_oops.
        // while (Atomic::load(&started_worker_count) < worker_num) {}
        log_debug(gc)("DGC LOG:worker %u ready to do_oops, push count: %d", worker_id, push_count);
        _snic_client->snic_do_compressed_oops(worker_id);
    }
};
/*@end lyh*/


class ShenandoahSNICCMTaskAddressTranslation : public AbstractGangTask {
private:
    int _buf_size;
    unsigned long long* _msg_buf;
    CopyRegionSnicClient* const _snic_client;
    std::vector<int> indexes;
    int started_worker_count = 0;
    int worker_num = 0;
    bool is_SATB = false;
public:
    ShenandoahSNICCMTaskAddressTranslation(int size, unsigned long long* buf, CopyRegionSnicClient* cli, std::vector<int> parts, bool is_SATB) :
        AbstractGangTask("SNIC Concurrent Mark"), _buf_size(size), _msg_buf(buf), _snic_client(cli), indexes(parts), is_SATB(is_SATB) {
        worker_num = (int) (_snic_client->snic_gc_workers->active_workers());
    }
    void work(uint worker_id) {
        if (worker_id >= _snic_client->snic_gc_workers->active_workers()) {
            return ;
        }
        log_info(gc)("GC(%u,%d) LHT LOG: worker %u starts ShenandoahSNICCMTask", _snic_client->_gc_id, _snic_client->clientId, worker_id);
        auto worker_queue = _snic_client->get_queue(worker_id);
        uint index_num = (uint)(indexes.size() - 1);
        uint process_id = worker_id;
        int push_count = 0;
        while (process_id < index_num) {
            int start = indexes[process_id];
            int end = indexes[process_id + 1];
            for (int handle_idx = start; handle_idx < end; ++handle_idx) {
                if (_msg_buf[handle_idx] == (unsigned long long) (-1)) {
                    continue;
                }

                oop obj = (oop) (_msg_buf[handle_idx]);
                unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(_snic_client->heapBase)) >> (_snic_client->regionSizeShift);

                if (r_idx_o >= _snic_client->heapRegionNumber) {
                    log_error(gc)("LHT LOG: worker %u handle oop in invalid region,r_idx_o=%llu,regionNumber=%zu,obj=%p,idx=%d,start=%d,end=%d",
                        worker_id, r_idx_o, _snic_client->heapRegionNumber, obj, handle_idx, start, end);
                    exit(1);
                }
                if (_snic_client->region_info[r_idx_o].top == 0 || _msg_buf[handle_idx] >= _snic_client->region_info[r_idx_o].top) {
                    continue;
                }

                // skip marked obj
                if(is_SATB){
                  bool upgraded = false;
                  bool bitmap_mark_rst = _snic_client->bitmap->mark_strong((HeapWord*) (obj), upgraded);
                  if (!bitmap_mark_rst) {
                      continue;
                  }
                }

                auto res = _snic_client->region_bitmap_add_read_lock(r_idx_o);
                // log_info(gc)("LHT LOG: region_bitmap_add_read_lock (4), r_idx_o=%lu, res=%llu", r_idx_o, res);
                if(_snic_client->get_region_bitmap_base_state(res) != REGION_COPIED){
                    // if(!is_SATB){
                    //     log_error(gc)("LHT LOG: ASSERT! region_bitmap_add_read_lock failed, is_SATB=false, r_idx_o=%lu, res=%llu", r_idx_o, res);
                    //     os::abort();
                    // }
                    ShenandoahMarkTask oneTask(obj, false, false);
                    if(_snic_client->workerPendingMaps[worker_id]->push(r_idx_o, oneTask, _snic_client->region_copied_bitmap, _snic_client)){
                        _snic_client->snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)SnicGCEstimatedObjectSize, worker_id);
                        _snic_client->region_bitmap_dec_read_lock(r_idx_o);
                        // log_info(gc)("LHT LOG: region_bitmap_dec_read_lock (7), r_idx_o=%lu", r_idx_o);
                        continue;
                    }
                    else{
                        log_error(gc)("LHT LOG: ASSERT! workerPendingMaps[worker_id]->push failed, is_SATB=false, r_idx_o=%lu, res=%llu", r_idx_o, res);
                        os::abort();
                    }
                }


                oop local_cached_obj = _snic_client->translate_oop(obj);

                // if (_snic_client->oop_is_humongous(obj)) {
                //     int uncopied_region_idx = -1;
                //     if (!_snic_client->humongous_oop_regions_copy_finished_address_translation(obj, &uncopied_region_idx)) {
                //         log_error(gc)("LHT LOG: ASSERT! humongous_oop_regions_copy_finished_address_translation failed, r_idx_o=%lu", r_idx_o);
                //         os::abort();
                //         ShenandoahMarkTask oneTask(obj, false, false);
                //         if (_snic_client->workerPendingMaps[worker_id]->push(uncopied_region_idx, oneTask, _snic_client->region_copied_bitmap, _snic_client)) {
                //             if (_snic_client->is_prefetched == 1) {
                //                 log_info(gc)("LHT LOG: worker %u push region %d to pending map (2), is_prefetched=%llu, copy_region_bitmap=%llu", worker_id, uncopied_region_idx, _snic_client->is_prefetched, _snic_client->region_copied_bitmap[uncopied_region_idx]);
                //             }
                //             log_info(gc)("LHT LOG: worker %u push region %d to pending map (2), is_prefetched=%llu, copy_region_bitmap=%llu", worker_id, uncopied_region_idx, _snic_client->is_prefetched, _snic_client->region_copied_bitmap[uncopied_region_idx]);
                //             if(is_SATB){
                //               _snic_client->snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)SnicGCEstimatedObjectSize, worker_id);
                //             }
                //             continue;
                //         }
                //     }
                // }

                push_count += 1;
                ShenandoahMarkTask oneTask(obj, true, false);
                worker_queue->push(oneTask);
                if(is_SATB){
                  _snic_client->snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)local_cached_obj->size(), worker_id);
                  _snic_client->snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(local_cached_obj->size()), worker_id);
                }
                _snic_client->region_bitmap_dec_read_lock(r_idx_o);
                // log_info(gc)("LHT LOG: region_bitmap_dec_read_lock (8), r_idx_o=%lu", r_idx_o);
            }
            process_id += _snic_client->snic_gc_workers->active_workers();
        }
        // Atomic::add(&started_worker_count, 1);
        // // wait until all other workers are ready to start do_oops.
        // while (Atomic::load(&started_worker_count) < worker_num) {}
        if(_snic_client->cur_handling_rpc_type == 10){
          Atomic::add(&_snic_client->under_last_round_mark_count, 1);
        }
        log_info(gc)("LHT LOG:worker %u ready to do_oops, push count: %d", worker_id, push_count);
        _snic_client->snic_do_compressed_oops_address_translation(worker_id);
    }
};



/*@end lht*/
