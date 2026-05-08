#include "shareMemSnicClient.hpp"
#include <fstream>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include "oops/compressedOops.inline.hpp"
#include "oops/oop.inline.hpp"
#include "gc/shenandoah/shenandoahOopClosures.inline.hpp"
#include "oops/instanceKlass.hpp"
#include "memory/virtualspace.hpp"
#include "memory/memRegion.hpp"
#include "gc/shenandoah/shenandoahHeapRegion.hpp"
#include "memory/universe.hpp"
#include "classfile/javaClasses.hpp"
#include "oops/instanceRefKlass.inline.hpp"
#include <mutex>
#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>

static inline void futex_wake(volatile unsigned long long* addr) {
  syscall(SYS_futex, addr, FUTEX_WAKE, 1, NULL, NULL, 0);
}

std::mutex send_liveness_ack_mtx;
std::mutex coor_update_liveness_mtx;

GlobalPacerData* ShareMemSnicClient::clientGlobalPacerData = nullptr;
bool ShareMemSnicClient::schedule_global_pacer_decided = false;
std::queue<int> ShareMemSnicClient::schedule_host_ids;

ShareMemSnicClient::ShareMemSnicClient() : SnicClient() {}

ShareMemSnicClient::~ShareMemSnicClient() {}

void ShareMemSnicClient::initialize() {
    _gc_id = -1;
    virtual_space_regions.clear();
    defaultPageSize = (size_t)os::vm_page_size();
    char curSnicShmMemPath[1024];
    // strcpy(curSnicShmMemPath, SnicShmMemPath);
    sprintf(curSnicShmMemPath, "%s_%d", SnicShmMemPath, clientId);
    shareHeapFD = shm_open(curSnicShmMemPath, O_RDWR | O_CREAT, 0666);
    if (shareHeapFD < 0) {
        log_error(gc)("DGC LOG: create shareHeapFD failed");
        exit(0);
    }
    char curSnicShmRootsPath[1024];
    // strcpy(curSnicShmRootsPath, SnicShmRootsPath);
    sprintf(curSnicShmRootsPath, "%s_%d", SnicShmRootsPath, clientId);
    shareRootsFileFD = shm_open(curSnicShmRootsPath, O_RDWR | O_CREAT, 0666);
    if (shareRootsFileFD < 0) {
        log_error(gc)("DGC LOG: create shareRootsFileFD failed");
        exit(0);
    }
    log_debug(gc)("DGC LOG: share mem client open shm files:%s,%d;%s,%d;%s,%d", curSnicShmMemPath, shareHeapFD, curSnicShmRootsPath, shareRootsFileFD, SnicShmGlobalPacerPath, globalPacerFileFD);

    // Map lock-free DGC control region
    if (SnicShmLockFreeMarking) {
        char curControlPath[1024];
        sprintf(curControlPath, "%s_%d", SnicShmControlPath, clientId);
        int controlFD = shm_open(curControlPath, O_RDWR, 0666);
        if (controlFD < 0) {
            log_error(gc)("DGC LOG: open control shm failed, path: %s (host must create it first)", curControlPath);
            exit(0);
        }
        dgcControl = (Universe::ShmDGCControl*) mmap(NULL, sizeof(Universe::ShmDGCControl), PROT_READ | PROT_WRITE, MAP_SHARED, controlFD, 0);
        if (dgcControl == MAP_FAILED) {
            log_error(gc)("DGC LOG: mmap control shm failed");
            exit(0);
        }
        log_debug(gc)("DGC LOG: mapped DGC control SHM at %p, path: %s", dgcControl, curControlPath);

        // Pre-allocate roots SHM at the SAME address as host (MAP_FIXED)
        // Wait for host to publish pre-alloc info
        while (Atomic::load_acquire(&dgcControl->prealloc_host_addr) == 0) {
            usleep(1000);
        }
        unsigned long long host_addr = Atomic::load(&dgcControl->prealloc_host_addr);
        unsigned long long prealloc_sz = Atomic::load(&dgcControl->prealloc_size);
        // ftruncate client-side copy of roots file (same SHM file)
        ftruncate(shareRootsFileFD, prealloc_sz);
        void* client_map = mmap((void*)host_addr, prealloc_sz,
                                PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED,
                                shareRootsFileFD, 0);
        if (client_map == MAP_FAILED || client_map != (void*)host_addr) {
            log_error(gc)("DGC LOG: client pre-alloc mmap failed at %p (got %p)", (void*)host_addr, client_map);
            // Fallback: map at arbitrary addr (less efficient, roots pointers won't match)
            client_map = mmap(NULL, prealloc_sz, PROT_READ | PROT_WRITE, MAP_SHARED, shareRootsFileFD, 0);
        }
        log_debug(gc)("DGC LOG: client pre-alloc roots at %p (host=%p), size=%lluMB",
                     client_map, (void*)host_addr, prealloc_sz / (1024*1024));
    }
}

// Lock-free SHM marking loop: polls control struct, processes roots/SATB directly.
// Called from TCP RPC handler case 5 after initial setup completes.
//
// IMPORTANT: returns when dgcControl->shutdown_requested is set by the host.
// This is necessary because this function is called from the TCP RPC dispatch
// thread (case 5 of handleRPC). While in this loop, the RPC thread cannot
// process incoming TCP RPCs — specifically, it cannot receive RPC 10 which
// is sent by the host during JVM shutdown (java.cpp before_exit). If this
// function never returned, host's recv_int_ack() would deadlock waiting for
// the client's ACK to RPC 10, and the SHM loop would busy-spin forever.
//
// On shutdown: return to case 5, which returns to the outer TCP RPC dispatch
// loop, which then receives RPC 10 and executes its exit path (ACKs host,
// closes sockets, exit(0)).
void ShareMemSnicClient::runShmMarkingLoop() {
    // Start after the first cycle which was handled by TCP (seqno=1)
    unsigned long long last_seqno = Atomic::load_acquire(&dgcControl->marking_seqno);
    log_debug(gc)("DGC LOG: entering SHM marking loop (zero-TCP mode, starting after seqno=%llu)", last_seqno);

    // Liveness heartbeat: refresh every ~64K poll iterations so the host
    // can detect a dead/hung client at GC start. Throttled by counter to
    // avoid one os::javaTimeMillis() syscall per busy-poll iteration.
    unsigned int hb_ticker = 0;
    Atomic::release_store(&dgcControl->client_heartbeat_ms,
                          (unsigned long long)os::javaTimeMillis());

    while (true) {
        // 1. Poll for new marking request (host sets marking_seqno).
        //    Also poll shutdown_requested so JVM exit can unblock us.
        unsigned long long seqno;
        while ((seqno = Atomic::load_acquire(&dgcControl->marking_seqno)) <= last_seqno) {
            if (Atomic::load_acquire(&dgcControl->shutdown_requested) != 0) {
                log_debug(gc)("DGC LOG: shutdown_requested set, exiting SHM marking loop");
                return;
            }
            if ((++hb_ticker & 0xFFFF) == 0) {
                Atomic::release_store(&dgcControl->client_heartbeat_ms,
                                      (unsigned long long)os::javaTimeMillis());
            }
            // Dedicated client cores — busy-poll is fine
        }
        // Tick once unconditionally on cycle entry so the host sees fresh
        // liveness even when poll wake-up was prompt enough to skip the
        // throttle in the inner loop.
        Atomic::release_store(&dgcControl->client_heartbeat_ms,
                              (unsigned long long)os::javaTimeMillis());
        last_seqno = seqno;

        // 2. Read root metadata
        unsigned long long roots_addr = Atomic::load(&dgcControl->roots_shm_addr);
        unsigned long long roots_size = Atomic::load(&dgcControl->roots_size);
        _gc_id = (int)Atomic::load(&dgcControl->roots_gc_id);

        log_debug(gc)("GC(%u,%d) DGC LOG: SHM-loop combined start (seqno=%llu, roots=%lluB)",
                     _gc_id, clientId, seqno, roots_size);

        // 3. Combined marking + SATB in one run_task (no worker re-scheduling)
        remapVirtualNodesForCurHost();
        memset(live_count, 0, heapRegionNumber * sizeof(uint64_t));
        handleInlineMarkingFromSHM(seqno);
        Commit();
        unmapVirtualNodesForCurHost();
        log_debug(gc)("GC(%u,%d) DGC LOG: SHM-loop combined done (seqno=%llu)", _gc_id, clientId, seqno);
    }
}

// Combined roots + SATB in a single run_task (workers stay alive, no re-scheduling)
void ShareMemSnicClient::handleCombinedMarkingFromSHM(unsigned long long seqno) {
    // Parse roots from pre-alloc SHM
    unsigned long long roots_addr = Atomic::load(&dgcControl->roots_shm_addr);
    unsigned long long roots_size = Atomic::load(&dgcControl->roots_size);
    unsigned long long* data = (unsigned long long*)roots_addr;
    int rootNum = (int)(roots_size / sizeof(unsigned long long));
    std::vector<int> queue_indexes;
    int first_data = (int)(data[0]);
    queue_indexes.push_back(first_data);
    for (int i = 0; i < first_data - 1; ++i) {
        queue_indexes.push_back(data[i + 1]);
    }

    task_queues->reserve(snic_gc_workers->active_workers());
    finished_task_count = 0;  // Reset for Phase 1 do_oops termination

    // Combined task: workers do roots + barrier + poll satb_seqno + SATB in one go
    // Last worker acts as "feeder" — polls satb_seqno and provides SATB data
    // No extra thread needed (avoids CPU starvation on 4-core client)
    ShmCombinedMarkTask task(this, dgcControl, seqno, data, rootNum, queue_indexes);
    snic_gc_workers->run_task(&task);  // blocks until all workers return (one park)
    // satb_done is signaled by Commit() (after liveness aggregation)
}

// Inline SATB streaming: single-phase marking with SATB consumed from SHM stream
void ShareMemSnicClient::handleInlineMarkingFromSHM(unsigned long long seqno) {
    unsigned long long roots_addr = Atomic::load(&dgcControl->roots_shm_addr);
    unsigned long long roots_size = Atomic::load(&dgcControl->roots_size);
    unsigned long long* data = (unsigned long long*)roots_addr;
    std::vector<int> queue_indexes;
    int first_data = (int)(data[0]);
    queue_indexes.push_back(first_data);
    for (int i = 0; i < first_data - 1; ++i) {
        queue_indexes.push_back(data[i + 1]);
    }

    task_queues->reserve(snic_gc_workers->active_workers());
    finished_task_count = 0;

    unsigned long long prealloc_base = Atomic::load(&dgcControl->prealloc_host_addr);
    auto satb_buf = (unsigned long long*)(prealloc_base + Universe::PREALLOC_SATB_OFFSET);

    ShmInlineMarkTask task(this, dgcControl, seqno, data, queue_indexes, satb_buf);
    snic_gc_workers->run_task(&task);
    // satb_done is signaled by Commit()
}

int ShareMemSnicClient::mmapShareMemSpace(void* addr, size_t length, off_t offset, int fd, int rpcType) {
    unsigned long long addr_num = (unsigned long long)addr;
    unsigned long long target_addr_num = addr_num;
    size_t target_len = length;
    if (target_addr_num % defaultPageSize != 0) {
        auto delta = target_addr_num % defaultPageSize;
        target_addr_num -= delta;
        target_len += delta;
    }
    if (target_len % defaultPageSize != 0) {
        auto delta = target_len % defaultPageSize;
        target_len -= delta;
        target_len += defaultPageSize;
    }
    // If the target addr falls inside the pre-reserved SHM arena, use
    // MAP_FIXED so the kernel overlays our own PROT_NONE reservation —
    // guaranteed to land exactly where the host asked. Outside the arena
    // we stick with hint-mode mmap + the old collision error for
    // backwards compat with non-arena call sites.
    int fixed = 0;
    if (Universe::shmArenaBase != nullptr &&
        target_addr_num >= (unsigned long long) Universe::shmArenaBase &&
        target_addr_num + target_len <= Universe::shmArenaEnd) {
        fixed = MAP_FIXED;
    }
    void *res = mmap((void *)target_addr_num, target_len, PROT_READ | PROT_WRITE, MAP_SHARED | fixed, fd, offset);
    if (res == (void *) -1) {
        log_debug(gc)("DGC LOG: mmap failed, %d, %s, rpc type = %d", errno, strerror(errno), rpcType);
        exit(0);
    }
    if (res != (void *)target_addr_num) {
        // Non-arena fallback: kernel gave a different address because the
        // target was already occupied by libc/libjvm/stack. Returning a
        // "skip" here just defers the crash to a mysterious SIGSEGV in
        // downstream memset, so fail loud instead. If you hit this, set
        // -XX:SnicShmArenaSize large enough that host bump-allocates all
        // its SHM regions inside the arena.
        unmapShareMemSpace(res, target_len);
        log_error(gc)("DGC LOG: mmap address collision — got %p, wanted target addr %p (rpc=%d). "
                      "Client ASLR put an existing mapping in the way; need to restart the client.",
                      res, addr, rpcType);
        exit(2);
    }
    log_debug(gc)("DGC LOG: mmap success, addr %p, length %zu, offset %ld, target addr %p, target length %zu, rpcType %d", addr, length, offset, (void*) (target_addr_num), target_len, rpcType);
    return 0;
}

void ShareMemSnicClient::unmapShareMemSpace(void* addr, size_t length) {
    auto res = munmap(addr, length);
    if (res == -1) {
        log_error(gc)("DGC LOG: munmap failed, errno:%d, err reason:%s,addr:%p,length:%zu", errno, strerror(errno), addr, length);
        exit(0);
    }
}

void ShareMemSnicClient::handleRPC(int rpcType, int hostId, int sz, void* buffer) {
    log_debug(gc)("DGC LOG: shmClient handles rpc, type %d, hostId %d, sz %d", rpcType, hostId, sz);
    switch (rpcType) {
        case 1:
        {
            // mmap java heap.
            handleJavaHeap(sz, buffer);
            break;
        }
        case 2:
        {
            // mmap for bitmap->_map.
            handleBitmap(sz, buffer);
            break;
        }
        case 3:
        {
            // mmap for global live count.(not live_cache)
            handleLiveCount(sz, buffer);
            break;
        }
        case 4:
        {
            // read RPC data for region tams info.
            handleRegionTamsInfo(sz, buffer);
            break;
        }
        case 5:
        {
            _gc_id = ((unsigned long long*)buffer)[3];
            if (SnicGCCoorHeuristic) {
                if (coor_state->force_gc_ccmt != 0) {
                    ConcGCThreads = coor_state->force_gc_ccmt;
                }
                snic_gc_workers->update_active_workers(ConcGCThreads);
                log_debug(gc)("DGC LOG: set active workers to %d", snic_gc_workers->active_workers());
            }
            // read mmap data for task queues information and mark for them.
            if (!SnicShmLockFreeMarking || dgcControl == nullptr) {
                send_back_int_ack(1);
            }
            // if (!SnicGCCoorHeuristic && SnicGCGlobalPacer && SnicGCShareMemEnabled && !schedule_host_ids.empty()) {
            //     if (schedule_host_ids.front() != clientId) {
            //         log_error(gc)("DGC LOG: schedule_host_ids.front() != clientId, %d != %d", schedule_host_ids.front(), clientId);
            //         exit(0);
            //     }
            // }
            currentMarkStartTime = os::javaTimeMillis();
            // if(!SnicGCCoorHeuristic && SnicGCGlobalPacer){
            //     Atomic::store(&(ShareMemSnicClient::clientGlobalPacerData->curMarkStartTimeInMs), (unsigned long long)(currentMarkStartTime));
            //     // Atomic::store(&(ShareMemSnicClient::clientGlobalPacerData->free), (unsigned long long)0);
            //     reset_global_pacer();
            // }
            if (SnicGCCoorHeuristic) {
                // waitCoordinatorUpdateFinished();
                // updateClientState(ClientStates::RESET_GLOBAL_PACER);
                coor_state->gc_start_time = os::javaTimeMillis();
                coor_state->client_gc_id = _gc_id;
                coor_state->cores_in_use = ConcGCThreads;
                log_debug(gc)("DGC LOG: set active workers to %d", snic_gc_workers->active_workers());
            }
            remapVirtualNodesForCurHost();
            handleTaskQueueRoots(sz, buffer);
            // After first cycle with lock-free, enter SHM polling loop for all subsequent cycles
            if (SnicShmLockFreeMarking && dgcControl != nullptr) {
                runShmMarkingLoop();  // never returns — all subsequent marking via SHM
            }
            break;
        }
        case 6:
        {
            // read mmap data for satb roots and mark for them.
            handleSATBRoots(sz, buffer, 6);
            break;
        }
        case 7:
        {
            // mmap app cds in client side using SharedArchiveFile.
            handleAppCDS(sz, buffer);
            break;
        }
        case 8:
        {
            handleVirtualSpace(sz, buffer);
            break;
        }
        case 9:
        {
            handleSATBRootsCommit(sz, buffer);
            unmapVirtualNodesForCurHost();
            // if (!SnicGCCoorHeuristic && SnicGCGlobalPacer) {
            //     // auto cur_mark_host = Atomic::load(&(ShareMemSnicClient::clientGlobalPacerData->free));
            //     // if (cur_mark_host != (unsigned long long)(clientId)) {
            //     //     log_error(gc, ergo)("DGC LOG: free should be maintained by host %d, but is maintained by host %llu", clientId, cur_mark_host);
            //     //     exit(0);
            //     // }
            //     Atomic::store(&(ShareMemSnicClient::clientGlobalPacerData->free), (unsigned long long)(MAX_HOST_NUM));
            //     if(ShareMemSnicClient::schedule_global_pacer_decided){
            //         schedule_host_ids.pop();
            //         if(schedule_host_ids.empty()) {
            //             schedule_global_pacer_decided = false;
            //         }
            //         else {
            //             auto cur_mark_host = Atomic::cmpxchg(&(ShareMemSnicClient::clientGlobalPacerData->free), (unsigned long long)(MAX_HOST_NUM), (unsigned long long)(ShareMemSnicClient::schedule_host_ids.front()), memory_order_relaxed);
            //             if (cur_mark_host != (unsigned long long)(MAX_HOST_NUM)) {
            //                 while (!schedule_host_ids.empty()) {
            //                     schedule_host_ids.pop();
            //                 }
            //                 schedule_global_pacer_decided = false;
            //             } else {
            //                 Atomic::store(&(clientGlobalPacerData->forceGC[ShareMemSnicClient::schedule_host_ids.front()]), (unsigned long long)(clients[ShareMemSnicClient::schedule_host_ids.front()]->_gc_id + 1));
            //                 log_debug(gc)("DGC LOG: set host %d to force gc(global pacer)", ShareMemSnicClient::schedule_host_ids.front());
            //             }
            //         }
            //     }
            // }
            auto new_mark_time = os::javaTimeMillis() - currentMarkStartTime;
            historyMarkTime.push_back(new_mark_time);
            if (SnicGCCoorHeuristic) {
                // waitCoordinatorUpdateFinished();
                coor_state->client_avg_mark_time = get_avg_mark_time();
                // updateClientState(ClientStates::GC_END);
            }
            break;
        }
        case 10:
        {
            // used to stop rpc server.
            // close(shareHeapFD);
            // close(shareRootsFileFD);
            // close(clientSocket);
            Atomic::sub(&(SnicClient::alive_clients_num), 1);
            int cur_alive_clients_num = Atomic::load(&(SnicClient::alive_clients_num));
            log_debug(gc)("DGC LOG: some jvm exit,alive clients num %d", cur_alive_clients_num);
            if (cur_alive_clients_num == 0) {
                log_debug(gc)("DGC LOG: rpc server exit!");
                for(int i = 0; i < SnicGCHostNum; i++){
                    clients[i]->send_back_int_ack(1);
                }
                close(serverSocket);
                if (SnicGCCoorHeuristic) {
                    waitCoordinatorUpdateFinished();
                    updateClientState(ClientStates::CLIENT_EXIT);
                }
                exit(0);
            }
            break;
        }
        case 11:
        {
            handleClassSpaceBase(sz, buffer);
            break;
        }
        case 12:
        {
            // if (!SnicGCCoorHeuristic && SnicGCGlobalPacer) {
            //     // TEST ONLY
            //     if (schedule_host_ids.empty()) {
            //         log_debug(gc)("DGC LOG: schedule_host_ids is empty before reset");
            //     }
            //     // reset global pacer
            //     while(!schedule_host_ids.empty()){
            //         log_debug(gc)("DGC LOG: schedule_host_ids.front() before reset = %d", schedule_host_ids.front());
            //         schedule_host_ids.pop();
            //     }
            //     schedule_global_pacer_decided = false;
            // }
            _gc_id++;
            if (SnicGCCoorHeuristic) {
                // waitCoordinatorUpdateFinished();
                coor_state->client_gc_id = _gc_id;
                // updateClientState(ClientStates::CLIENT_HANDLE_RPC_12);
            }
            log_debug(gc)("DGC LOG: reset global pacer");
            break;
        }
        case 13:
        {
            // if (!SnicGCCoorHeuristic && SnicGCGlobalPacer) {
            //     // reset global pacer when a forceGC is skipped.
            //     while (!schedule_host_ids.empty()) {
            //         schedule_host_ids.pop();
            //     }
            //     schedule_global_pacer_decided = false;
            // }
            // if (SnicGCCoorHeuristic) {
            //     // waitCoordinatorUpdateFinished();
            //     // updateClientState(ClientStates::CLIENT_HANDLE_RPC_13);
            // }
            break;
        }
    }
}

void ShareMemSnicClient::handleJavaHeap(int bufferSize, void* buffer) {
    if (heapBase != 0) {
        log_debug(gc)("DGC LOG: handle java heap, already initialized:%llx", heapBase);
        return ;
    }
    initialize();
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | start addr | length | offset | region number
    if (N != 4) {
        log_error(gc)("DGC LOG: handle java heap buffer size error, %d", N);
        exit(0);
    }
    unsigned long long *message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];
    heapRegionSize = (size_t)message[3];
    heapRegionNumber = length / heapRegionSize;
    mmapShareMemSpace(addr, length, offset, shareHeapFD, 1);
    heapBase = reinterpret_cast<unsigned long long>(addr);
    heapSize = length;
    log_debug(gc)("DGC LOG: handle java heap, base %p, size %zu, region number %zu", addr, length, heapRegionNumber);
    // init live cache.
    live_data_caches = new uint64_t*[(snic_gc_workers->active_workers())];
    for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
        live_data_caches[i] = new uint64_t[heapRegionNumber];
        memset(live_data_caches[i], 0, heapRegionNumber * sizeof(uint64_t));
    }
    // init region tams info.
    region_tams_info = new unsigned long long[heapRegionNumber];
    memset(region_tams_info, 0, heapRegionNumber * sizeof(unsigned long long));
}

void ShareMemSnicClient::handleBitmap(int bufferSize, void* buffer) {
    char curShareBitmapPath[1024];
    // strcpy(curShareBitmapPath, "/share_bitmap");
    sprintf(curShareBitmapPath, "%s_%d", SnicShmBitmapPath, clientId);
    int bitmapFD = shm_open(curShareBitmapPath, O_RDWR | O_CREAT, 0666);
    if (bitmapFD < 0) {
        log_error(gc)("DGC LOG: shm_open failed for bitmapFD, errno:%d, %s, path:%s", errno, strerror(errno), curShareBitmapPath);
        exit(0);
    }
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | start addr | length | offset
    if (N != 3) {
        log_error(gc)("DGC LOG: handle bitmap buffer size error, %d", N);
        exit(0);
    }
    unsigned long long *message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];
    mmapShareMemSpace(addr, length, offset, bitmapFD, 2);
    MemRegion bitmap_region = MemRegion((HeapWord*)addr, length / HeapWordSize);
    bitmap = new ShenandoahMarkBitMap(MemRegion((HeapWord*)heapBase, (size_t)(heapSize / HeapWordSize)),  bitmap_region);
    close(bitmapFD);
}

void ShareMemSnicClient::handleLiveCount(int bufferSize, void* buffer) {
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | start addr | length | offset
    if (N != 3) {
        log_error(gc)("DGC LOG: handle live count buffer size error, %d", N);
        exit(0);
    }
    unsigned long long *message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];

    char curShareLivenessPath[1024];
    // strcpy(curShareLivenessPath, "/share_liveness");
    sprintf(curShareLivenessPath, "%s_%d", SnicShmLivenessPath, clientId);
    int livenessFD = shm_open(curShareLivenessPath, O_RDWR | O_CREAT, 0666);
    if (livenessFD < 0) {
        log_error(gc)("DGC LOG: shm_open failed for livenessFD, errno:%d, %s, path:%s", errno, strerror(errno), curShareLivenessPath);
        exit(0);
    }
    mmapShareMemSpace(addr, length, offset, livenessFD, 3);
    live_count = (uint64_t*) (addr);
    close(livenessFD);
}

void ShareMemSnicClient::inc_liveness(unsigned long long r, intptr_t start, size_t s, uint worker_id) {
    auto end_addr = (unsigned long long)(start) + s * 8;
    auto old_live_data = live_data_caches[worker_id][r];
    auto region_end = heapBase + heapRegionSize * (r + 1);
    while (region_end < end_addr) {
        size_t delta = (size_t)((region_end - start) / HeapWordSize);
        live_data_caches[worker_id][r] += (uint64_t)(delta);
        start = region_end;
        r += 1;
        region_end = heapBase + heapRegionSize * (r + 1);
    }
    size_t final_delta = (size_t)((end_addr - start) / HeapWordSize);
    live_data_caches[worker_id][r] += (uint64_t)(final_delta);
}

// Defensive oop validation. A stale mark bit or a corrupt task-queue entry in
// the lock-free SHM path can hand us a pointer that isn't actually in the
// Java heap, or is inside the heap but whose klass header is junk / null.
// Dereferencing such a pointer (via obj->klass(), obj->size(), or even
// bitmap->is_marked on a wild address) crashes the JVM with SIGSEGV.
//
// Validate in three steps, cheapest first, before any read from obj:
//   1. heap bounds — obj must be within [heapBase, heapBase + heapSize)
//   2. alignment — obj must be HeapWord-aligned
//   3. klass — obj->klass_or_null() must be non-null
// All three must hold to read obj->size() / obj->klass()->id() safely.
bool ShareMemSnicClient::is_oop_safe_to_mark(oop obj) const {
    if (obj == nullptr) return false;
    unsigned long long addr = (unsigned long long)obj;
    if (addr < heapBase || addr >= heapBase + heapSize) return false;
    if ((addr & (HeapWordSize - 1)) != 0) return false;
    if (obj->klass_or_null() == nullptr) return false;
    return true;
}

void ShareMemSnicClient::do_oop(narrowOop* p, uint worker_id, int type) {
    narrowOop o = RawAccess<>::oop_load(p);
    auto worker_queue = get_queue(worker_id);
    if (!CompressedOops::is_null(o)) {
        oop obj = cast_to_oop((uintptr_t)(COMPRESSED_OOP_BASE) + ((uintptr_t)(o) << COMPRESSED_OOP_SHIFT));
        bool upgraded = false;
        bool rst = bitmap->is_marked((HeapWord *)obj);
        if (!rst) {
            unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)heapBase) / heapRegionSize;
            if (r_idx_o >= heapRegionNumber) return;
            unsigned long long obj_addr = (unsigned long long)obj;
            if (obj_addr < region_tams_info[r_idx_o]) {
                bool mark_rst = bitmap->mark_strong((HeapWord *)obj, upgraded);
                if (mark_rst) {
                    ShenandoahMarkTask pushTask(obj, upgraded, false);
                    worker_queue->push(pushTask);
                    inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
                }
            }
        }
    }
}

bool ShareMemSnicClient::checkMarkerTerminateCondition(uint worker_id) {
    bool ret = false;
    uint active_worker_num = snic_gc_workers->active_workers();
    task_count_lock.lock();
    finished_task_count++;
    if ((uint) (finished_task_count) == active_worker_num) {
        task_count_lock.unlock();
        return true;
    }
    // compute all tasks num in all queues.
    int left_tasks_num = 0;
    for (uint i = 0; i < active_worker_num; ++i) {
        auto que = get_queue(i);
        left_tasks_num += que->size();
    }
    if (left_tasks_num == 0) {
        ret = true;
    } else {
        // case: a worker fail to steal/pop new task, but still some tasks left, return false to try to steal again.
        finished_task_count--;
    }
    task_count_lock.unlock();
    return ret;
}

void ShareMemSnicClient::do_oops(uint worker_id) {
    auto worker_queue = get_queue(worker_id);
    ShenandoahMarkTask popTask;
    uint work_count = 0;
    int steal_count = 0;
    // while (worker_queue->pop(popTask) || task_queues->steal(worker_id, popTask)) {
    while (true) {
        bool pop_result = worker_queue->pop(popTask);
        if (!pop_result) {
            pop_result = task_queues->steal(worker_id, popTask);
            steal_count++;
        }
        if (!pop_result) {
            if (checkMarkerTerminateCondition(worker_id)) {
                break;
            } else {
                continue;
            }
        }
        oop obj = popTask.obj();
        work_count++;
        if (work_count % SEND_LIVENESS_THRESHOLD == 0) {
            uint64_t total_liveness_tmp = 0;
            for (size_t i = 0; i < heapRegionNumber; ++i) {
                uint64_t liveness_tmp = live_data_caches[worker_id][i];
                Atomic::add(&live_count[i], liveness_tmp);
                live_data_caches[worker_id][i] = 0;
                total_liveness_tmp += liveness_tmp;
            }
            // if(!SnicGCCoorHeuristic && SnicGCGlobalPacer){
            //     // send back liveness to global pacer
            //     for(int i = 0; i < SnicGCHostNum; i++){
            //         if(Atomic::load(&(clientGlobalPacerData->startPacer[i]))){
            //             Atomic::add(&(clientGlobalPacerData->budgetIncreasing[i]), total_liveness_tmp);
            //         }
            //     }
            //     Atomic::add(&(clientGlobalPacerData->budgetsToIncreaseDuringMark[clientId]), (long)total_liveness_tmp);
            // } else {
            if (SnicGCCoorHeuristic) {
                add_marked_liveness(total_liveness_tmp);
            } else if (SnicShmLockFreeMarking) {
                // Lock-free mode: skip TCP liveness ack, data is in mmap'd live_count
            } else {
                send_back_int_ack(total_liveness_tmp);
            }
            // }
        }
        if (obj->klass()->id() == ObjArrayKlassID) {
            objArrayOop a = objArrayOop(obj);
            narrowOop *p = (narrowOop *)(a->base());
            narrowOop *const end = p + a->length();
            for (; p < end; p++) {
                do_oop(p, worker_id, 1);
            }
        } else if (InstanceKlass::cast(obj->klass())->reference_type() != REF_NONE) {
            OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
            OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
            for (; map < end_map; ++map) {
                narrowOop *p = (narrowOop *)obj->field_addr(map->offset());
                narrowOop *end = p + map->count();
                for (; p < end; ++p) {
                    do_oop(p, worker_id, 2);
                }
            }
            narrowOop *referent_addr = NULL;
            if (UseCompressedClassPointers) {
                referent_addr = (narrowOop *)((unsigned long long)(obj) + 12);
            } else {
                referent_addr = (narrowOop *)((unsigned long long)(obj) + 16);
            }
            if (true) {
                do_oop(referent_addr, worker_id, 3);
            }
        } else {
            OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
            OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
            for (; map < end_map; ++map) {
                narrowOop *p = (narrowOop *)obj->field_addr(map->offset());
                narrowOop *end = p + map->count();
                for (; p < end; ++p) {
                    do_oop(p, worker_id, 2);
                }
            }
            // special check for Mirror Klass's static field
            if (obj->klass()->id() == InstanceMirrorKlassID) {
                narrowOop *p = (narrowOop *)((InstanceMirrorKlass *)obj->klass())->start_of_static_fields(obj);
                narrowOop *const end = p + java_lang_Class::static_oop_field_count_raw(obj);
                for (; p < end; ++p) {
                    do_oop(p, worker_id, 4);
                }
            }
        }
    }
    log_debug(gc)("GC(%u,%d) DGC LOG: worker %u finish do_oops, work_count:%u, steal count:%d,for host %d", _gc_id, clientId, worker_id, work_count, steal_count, clientId);
}

void ShareMemSnicClient::Commit() {
    uint64_t client_total_liveness = 0;
    uint64_t client_recent_liveness = 0;
    // no need to commit bitmap now because of shared memory.
    // aggregate local live cache to global live count.
    for (int i = 0; i < (int)(snic_gc_workers->active_workers()); ++i) {
        for (size_t j = 0; j < heapRegionNumber; ++j) {
            if (live_data_caches[i][j] > 0) {
                live_count[j] += (uint64_t)(live_data_caches[i][j]);
                client_recent_liveness += (uint64_t)(live_data_caches[i][j]);
                live_data_caches[i][j] = 0;
            }
        }
    }
    // no need to commit global live count now because of shared memory.
    // send an ack to notify the finish of client-side mark.
    if (SnicShmLockFreeMarking && dgcControl != nullptr) {
        Atomic::release_store(&dgcControl->satb_done_seqno, (unsigned long long)_gc_id + 1);
        futex_wake(&dgcControl->satb_done_seqno);
        log_debug(gc)("DGC LOG: lock-free SATB done + futex_wake (seqno=%d)", _gc_id + 1);
    } else {
        send_back_int_ack(1);
    }
    // compute client total liveness to get client mark rate.
    for (size_t i = 0; i < heapRegionNumber; ++i) {
        client_total_liveness += live_count[i];
    }
    // if(!SnicGCCoorHeuristic && SnicGCGlobalPacer){
    //     // send back liveness to global pacer
    //     for(int i = 0; i < SnicGCHostNum; i++){
    //         if(Atomic::load(&(clientGlobalPacerData->startPacer[i]))){
    //             Atomic::add(&(clientGlobalPacerData->budgetIncreasing[i]), client_recent_liveness);
    //         }
    //     }
    //     log_debug(gc)("GC(%u,%d) DGC LOG: global pacer recent liveness %lu", _gc_id, clientId, client_recent_liveness);
    //     Atomic::add(&(clientGlobalPacerData->budgetsToIncreaseDuringMark[clientId]), (long)client_recent_liveness);
    // }
    if (SnicGCCoorHeuristic) {
        // log_debug(gc)("DGC LOG: client %d update state from %d to %d in Commit function", coor_state->client_id, coor_state->client_state, ClientStates::UPDATE_GLOBAL_PACER_LIVENESS);
        // waitCoordinatorUpdateFinished();
        // coor_state->client_liveness_to_update = (unsigned long long) client_recent_liveness;
        // updateClientState(ClientStates::UPDATE_GLOBAL_PACER_LIVENESS);
        coor_state->cores_in_use = 0;
    }
    log_debug(gc)("GC(%u,%d) DGC LOG: client total liveness %lu", _gc_id, clientId, client_total_liveness);
    set_marked_liveness(0);
}

void ShareMemSnicClient::handleRoots(int bufferSize, void* buffer, int rpcType) {
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | start addr | length | offset | gc id
    if (N != 4) {
        log_error(gc)("DGC LOG: handle task queue roots buffer size error, %d", N);
        exit(0);
    }
    unsigned long long *message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];
    if (addr == (void*) (nullptr)) {
        log_debug(gc)("GC(%u,%d) DGC LOG: handle rpc %d roots, addr is nullptr", _gc_id, clientId, rpcType);
        return ;
    }
    // In pre-alloc mode, region is already mapped — skip per-cycle mmap/unmap
    bool skip_mmap = (SnicShmLockFreeMarking && dgcControl != nullptr &&
                      Atomic::load(&dgcControl->prealloc_host_addr) != 0);
    if (!skip_mmap) {
        mmapShareMemSpace(addr, length, offset, shareRootsFileFD, rpcType);
    }
    unsigned long long* data = (unsigned long long*) (addr);
    int rootNum = (int) (length / sizeof(unsigned long long));
    std::vector<int> queue_indexes;
    int first_data = (int) (data[0]);
    queue_indexes.push_back(first_data);
    for (int i = 0; i < first_data - 1; ++i) {
        queue_indexes.push_back(data[i + 1]);
    }
    task_queues->reserve(snic_gc_workers->active_workers());
    finished_task_count = 0;
    shareMemSnicConcurrentMarkTask task(rootNum, data, this, queue_indexes, rpcType);
    snic_gc_workers->run_task(&task);
    if (!skip_mmap) {
        unmapShareMemSpace(addr, length);
    }
}

void ShareMemSnicClient::handleTaskQueueRoots(int bufferSize, void* buffer) {
    // if (!SnicGCCoorHeuristic && SnicGCGlobalPacer) {
    //     Atomic::store(&(clientGlobalPacerData->startPacer[clientId]), (unsigned long long)0);
    // }
    // output a log to notify start of client-side concurrent mark, used to generate client mark rate.
    log_debug(gc)("GC(%u,%d) DGC LOG: start client-side concurrent mark for host %d", _gc_id, clientId, clientId);
    // // memset for bitmap.
    // memset(bitmap->_map, 0, bitmap->_size / 8);
    // memset for live count.
    memset(live_count, 0, heapRegionNumber * sizeof(uint64_t));
    log_debug(gc)("DGC LOG: finish memset in handleTaskQueueRoots");
    handleRoots(bufferSize, buffer, 5);
    if (SnicShmLockFreeMarking && dgcControl != nullptr) {
        // Signal marking done via SHM + futex wake (host futex_wait unblocks)
        Atomic::release_store(&dgcControl->marking_done_seqno, (unsigned long long)_gc_id + 1);
        futex_wake(&dgcControl->marking_done_seqno);
        log_debug(gc)("DGC LOG: lock-free marking done + futex_wake (seqno=%d) for host %d", _gc_id + 1, clientId);

        // Poll SHM for SATB roots (NO TCP RPC 9 — host writes directly to SHM)
        unsigned long long target_seqno = (unsigned long long)_gc_id + 1;
        while (Atomic::load_acquire(&dgcControl->satb_seqno) < target_seqno) {
            // Client dedicated cores — polling is fine
        }
        // Read SATB metadata from control struct, construct buffer for handleRoots
        unsigned long long satb_addr = Atomic::load(&dgcControl->satb_roots_shm_addr);
        unsigned long long satb_size = Atomic::load(&dgcControl->satb_roots_size);
        unsigned long long satb_offset = Atomic::load(&dgcControl->satb_roots_offset);
        unsigned long long satb_gcid = Atomic::load(&dgcControl->satb_gc_id);
        // Build metadata buffer (same format as TCP RPC payload)
        unsigned long long satb_meta[4] = {satb_addr, satb_size, satb_offset, satb_gcid};
        log_debug(gc)("DGC LOG: lock-free SATB from SHM (addr=%p, size=%llu)", (void*)satb_addr, satb_size);
        handleSATBRoots(sizeof(satb_meta), (void*)satb_meta, 9);
        Commit();
        unmapVirtualNodesForCurHost();
    } else {
        send_back_int_ack(1);
    }
    log_debug(gc)("DGC LOG: finish handleTaskQueueRoots for host %d", clientId);
}

void ShareMemSnicClient::handleSATBRoots(int bufferSize, void* buffer, int rpcType) {
    log_debug(gc)("DGC LOG: start handleSATBRoots for host %d", clientId);
    handleRoots(bufferSize, buffer, rpcType);
    log_debug(gc)("DGC LOG: finish handleSATBRoots for host %d", clientId);
}

void ShareMemSnicClient::handleSATBRootsCommit(int bufferSize, void* buffer) {
    log_debug(gc)("DGC LOG: start handleSATBRootsCommit for host %d", clientId);
    handleSATBRoots(bufferSize, buffer, 9);
    Commit();
    log_debug(gc)("DGC LOG: finish handleSATBRootsCommit for host %d", clientId);
}

void ShareMemSnicClient::handleRegionTamsInfo(int bufferSize, void* buffer) {
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | mmap addr | mmap length | mmap file offset
    if (N != 3) {
        log_error(gc)("DGC LOG: handle region tams info buffer size error, %d, expected 3", N);
        exit(0);
    }
    char curSnicShmRootsPath[1024];
    // strcpy(curSnicShmRootsPath, "/share_region_tams");
    sprintf(curSnicShmRootsPath, "%s_%d", SnicShmRegionTamsPath, clientId);
    int shareRegionTamsFileFD = shm_open(curSnicShmRootsPath, O_RDWR | O_CREAT, 0666);
    if (shareRegionTamsFileFD < 0) {
        log_error(gc)("DGC LOG: shm_open failed for shareRegionTamsFileFD, errno:%d, %s, path:%s", errno, strerror(errno), curSnicShmRootsPath);
        exit(0);
    }
    unsigned long long* message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];
    // unsigned long long* received_data = (unsigned long long*) (addr);
    // mmapShareMemSpace(addr, length, offset, shareRegionTamsFileFD, 4);
    // for (size_t i = 0; i < heapRegionNumber; ++i) {
    //     region_tams_info[i] = received_data[i];
    // }
    // unmapShareMemSpace(addr, length);
    region_tams_info = (unsigned long long*) (addr);
    mmapShareMemSpace(addr, length, offset, shareRegionTamsFileFD, 4);
}

void ShareMemSnicClient::handleAppCDS(int bufferSize, void* buffer) {
    // do nothing.
    // no need to unmap-remap app cds memory space because both host and client map the same jsa file.
}

void ShareMemSnicClient::remapVirtualNodesForCurHost() {
    for (size_t i = 0; i < virtual_space_regions.size(); ++i) {
        auto& mr = virtual_space_regions[i];
        void* region_start = (void*) (mr.start);
        // unmap
        unmapShareMemSpace(region_start, mr.size);
        // remap
        mmapShareMemSpace(region_start, mr.size, mr.fileOffset, mr.fd, 8);
    }
}

void ShareMemSnicClient::unmapVirtualNodesForCurHost() {
    for (size_t i = 0; i < virtual_space_regions.size(); ++i) {
        auto& mr = virtual_space_regions[i];
        void* region_start = (void*) (mr.start);
        // unmap
        unmapShareMemSpace(region_start, mr.size);
    }
}

void ShareMemSnicClient::handleVirtualSpace(int bufferSize, void* buffer) {
    int virtual_node_id = virtual_space_regions.size();
    char path[1024];
    sprintf(path, "%s_%d_%d", SnicShmVirtualNodePath, clientId, virtual_node_id);
    int virtual_node_fd = shm_open(path, O_CREAT | O_RDWR, 0666);
    if (virtual_node_fd < 0) {
        log_error(gc)("DGC LOG: create virtual node shm failed");
        exit(0);
    }
    log_debug(gc)("DGC LOG: create virtual node shm, fd %d, path %s", virtual_node_fd, path);
    // metaspace is also reserved memory space, so after we mmap host metaspace, no need to fetch klass.
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | class space base addr | class space length | class space backup file offset
    if (N != 3) {
        log_error(gc)("DGC LOG: handle virtual space buffer size error, %d", N);
        exit(0);
    }
    unsigned long long* message = (unsigned long long *)(buffer);
    void* addr = (void *)message[0];
    size_t length = (size_t)message[1];
    off_t offset = (off_t)message[2];
    // auto ret = mmapShareMemSpace(addr, length, offset, virtual_node_fd, 8);
    // if (ret != 0) {
    //     // some virtual space have been mapped during init of  snic client, act unmap-remap here.
    //     unmapShareMemSpace(addr, length);
    //     mmapShareMemSpace(addr, length, offset, virtual_node_fd, 8);
    // }
    SnicMemRegion mr((unsigned long long) addr, length, offset, virtual_node_fd);
    virtual_space_regions.push_back(mr);
    log_debug(gc)("DGC LOG: handle virtual space, base %p, length %zu, end %p", addr, length, (char*) (addr) + length);
}

ShenandoahObjToScanQueue* ShareMemSnicClient::get_queue(uint worker_id) {
    return task_queues->queue(worker_id);
}

void ShareMemSnicClient::handleClassSpaceBase(int bufferSize, void* buffer) {
    int N = bufferSize / sizeof(unsigned long long);
    // 2 int headers | class space base addr
    if (N != 1) {
        log_error(gc)("DGC LOG: handle class space base buffer size error, %d", N);
        exit(0);
    }
    unsigned long long* message = (unsigned long long *)(buffer);
    class_space_base = (size_t)message[0];
    // CompressedKlassPointers::set_base((address)class_space_base);
    log_debug(gc)("DGC LOG: handle class space base, base %p", (void*)class_space_base);
}

// No-op stub kept for symbol compatibility with the prior global-pacer
// scheduling path. Left empty because the live pacing logic now runs
// inside the coordinator (see SnicCoordinator::ResourceSchedulingProblem).
void ShareMemSnicClient::schedule_global_pacer() {
    // if(schedule_global_pacer_decided){
    //     return;
    // }
    // bool all_next_gc_time_valid = true;
    // HostLiveData hostLiveData[MAX_HOST_NUM];
    // for(int i = 0; i < SnicGCHostNum; i++){
    //     hostLiveData[i].hostId = i;
    //     hostLiveData[i].next_gc_time = Atomic::load(&(clientGlobalPacerData->hosts_global_pacer_data[i].nextGCTime));
    //     if(hostLiveData[i].next_gc_time == 9999999999999){
    //         all_next_gc_time_valid = false;
    //         return ;
    //     }
    //     hostLiveData[i].planed_next_gc_time = hostLiveData[i].next_gc_time;
    //     auto origin_avg_mark_time = ((ShareMemSnicClient *)clients[i])->get_avg_mark_time();
    //     hostLiveData[i].avg_mark_time = origin_avg_mark_time * 1.1;
    //     Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[i].averageGCTime), (unsigned long long)(origin_avg_mark_time));
    //     hostLiveData[i].historyLiveness = Atomic::load(&(clientGlobalPacerData->hosts_global_pacer_data[i].historyLiveness));
    // }
    // std::sort(hostLiveData, hostLiveData + SnicGCHostNum, [](HostLiveData a, HostLiveData b){
    //     return a.next_gc_time < b.next_gc_time;
    // });
    // for(int i = SnicGCHostNum - 1; i > 0; i--){
    //     if(hostLiveData[i - 1].planed_next_gc_time + hostLiveData[i - 1].avg_mark_time > hostLiveData[i].planed_next_gc_time){
    //         hostLiveData[i - 1].planed_next_gc_time = hostLiveData[i].planed_next_gc_time - hostLiveData[i - 1].avg_mark_time;
    //     }
    // }
    // // int force_gc = false;
    // unsigned long long cur_time = os::javaTimeMillis();
    // if(hostLiveData[0].planed_next_gc_time < cur_time){
    //     // force_gc = true;
    //     // force_gc
    //     schedule_global_pacer_decided = true;

    //     if(!schedule_host_ids.empty()){
    //         log_error(gc)("DGC LOG: schedule_host_ids is not empty, but schedule_global_pacer_decided is true");
    //         exit(0);
    //     }
    //     schedule_host_ids.push(hostLiveData[0].hostId);

    //     log_debug(gc)("DGC LOG: force gc to host %d", hostLiveData[0].hostId);
    //     hostLiveData[0].planed_next_gc_time = cur_time;
    //     auto cur_mark_host = Atomic::cmpxchg(&(clientGlobalPacerData->free), (unsigned long long)(MAX_HOST_NUM), (unsigned long long)(hostLiveData[0].hostId), memory_order_relaxed);
    //     if (cur_mark_host != (unsigned long long)(MAX_HOST_NUM)) {
    //         return ;
    //     }
    //     Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[hostLiveData[0].hostId].forceGC), (unsigned long long)(clients[hostLiveData[0].hostId]->_gc_id + 1));
    //     unsigned long long total_work_ahead = 0;
    //     for(int i = 0; i < SnicGCHostNum - 1; i++){
    //         if(hostLiveData[i+1].planed_next_gc_time < hostLiveData[i].planed_next_gc_time + hostLiveData[i].avg_mark_time){ // need to schedule
    //             schedule_host_ids.push(hostLiveData[i+1].hostId);
    //             log_debug(gc)("DGC LOG: add host %d to schedule_host_ids", hostLiveData[i+1].hostId);
    //             total_work_ahead += hostLiveData[i].historyLiveness;
    //             hostLiveData[i+1].planed_next_gc_time = hostLiveData[i].planed_next_gc_time + hostLiveData[i].avg_mark_time;
    //             if(hostLiveData[i + 1].planed_next_gc_time > hostLiveData[i + 1].next_gc_time){ // need to delay
    //                 Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[hostLiveData[i + 1].hostId].pacerWorkAhead), (unsigned long long)total_work_ahead);
    //                 Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[hostLiveData[i + 1].hostId].startPacer), (unsigned long long)1);
    //                 log_debug(gc)("DGC LOG: set host %d to start pacer", hostLiveData[i + 1].hostId);
    //             }
    //         }
    //         else{
    //             // total_work_ahead = 0;
    //             // only schedule these about to oom hosts
    //             break;
    //         }
    //     }
    //     for(int i = 0; i < SnicGCHostNum; i++){
    //         log_debug(gc)("DGC LOG: host %d next gc time %lu, planed next gc time %lu, avg mark time %lu", hostLiveData[i].hostId, hostLiveData[i].next_gc_time, hostLiveData[i].planed_next_gc_time, hostLiveData[i].avg_mark_time);
    //     }
    // }
}

// No-op stub kept for symbol compatibility; pacer state reset now happens
// implicitly via the coordinator's per-cycle CP-SAT solve.
void ShareMemSnicClient::reset_global_pacer(){
    // schedule_global_pacer_decided = false;
    // for(int i = 0; i < SnicGCHostNum; i++){
    //     Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[i].forceGC), (unsigned long long)0);
    //     Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[i].startPacer), (unsigned long long)0);
    //     Atomic::store(&(clientGlobalPacerData->hosts_global_pacer_data[i].nextGCTime), (unsigned long long)9999999999999);
    //     log_debug(gc, ergo)("DGC LOG: set nextGCTime reset_global_pacer: %lu", 9999999999999);
    // }
}
