#ifndef SHARE_GC_SNICGC_SNICCLIENT_HPP
#define SHARE_GC_SNICGC_SNICCLIENT_HPP

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
#include <netinet/tcp.h>
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
#include "snicCoordinator.hpp"

enum SnicRegionState {
    _empty_uncommitted,      // region is empty and has memory uncommitted
    _empty_committed,        // region is empty and has memory committed
    _regular,                // region is for regular allocations
    _humongous_start,        // region is the humongous start
    _humongous_cont,         // region is the humongous continuation
    _pinned_humongous_start, // region is both humongous start and pinned
    _cset,                   // region is in collection set
    _pinned,                 // region is pinned
    _pinned_cset,            // region is pinned and in cset (evac failure path)
    _trash,                  // region contains only trash
    _REGION_STATES_NUM       // last
};

class SnicHeapRegion {
public:
    // Brought back by the noTrans (pre-merge) DGC-RDMA client which
    // still needs region->index for region_info lookups. The merged
    // CopyRegionSnicClient stopped using this field but doesn't conflict.
    unsigned long long index;
    unsigned long long bottom;
    unsigned long long top;
    unsigned long long end;
    volatile size_t _live_data;
    volatile int64_t _marked_data;
    int64_t ongoing_mark_size = 0;
    SnicRegionState state;
    unsigned short local_region_id = USHRT_MAX;
    unsigned short kick_out_region_id = USHRT_MAX;
};

class RPCMsg {
public:
    int rpcType;
    int hostId;
    int sz;
    void* payload;
    RPCMsg(int rpcType, int hostId, int sz, void* input_buffer) : rpcType(rpcType), hostId(hostId), sz(sz) {
        this->payload = (char*)new char[sz + 1];
        // strncpy((char*)(this->payload), (char*)input_buffer, sz);
        memcpy((char*)(this->payload), (char*)input_buffer, sz);
    }
    ~RPCMsg() {
        // delete[] (char*)payload;
    }
};

class SnicClient {
public:
    CoorState* coor_state;
    int coor_client_id;
    static int serverSocket;
    static SnicClient **clients;
    static int currentHostId;
    static std::vector<std::queue<RPCMsg*>> rpcMsgQueues;
    static int alive_clients_num;
    int _gc_id = -1;
    int clientSocket;
    int clientId;
    size_t heapRegionNumber;
    size_t heapRegionNumber_half;
    unsigned long long heapRegionSize;
    unsigned long long heapBase = 0;
    unsigned long long heapBase2 = 0;
    unsigned long long heapSize;
    size_t regionSizeShift = 18;
    uint64_t** live_data_caches;
    int64_t** ongoing_mark_caches;
    ShenandoahMarkBitMap* bitmap;
    uint64_t* live_count;
    WorkGang *snic_gc_workers;
    ShenandoahObjToScanQueueSet *task_queues;
    // used to collect and calculate avg mark time.
    std::vector<unsigned long long> historyMarkTime;
    unsigned long long currentMarkStartTime;

    // Lock-free SHM control (mapped by client, written by both sides)
    Universe::ShmDGCControl* dgcControl = nullptr;

public:
    SnicClient();
    static int runRPCServer();
    static void resolveRPC(void* buffer);

    void send_back_ack(char msg);
    virtual void handleRPC(int rpcType, int hostId, int bufferSize, void* payload);
    // Trigger a "force GC" RPC over the data plane (RDMA) to the
    // corresponding host. Subclasses that talk RDMA override this; the
    // default base impl is a no-op so callers in snicClient.cc don't
    // need to know the concrete subclass type.
    virtual void forceGCByDPUClient() {}
    void send_back_int_ack(size_t ack);
    void updateClientState(int clientState);
    void waitCoordinatorUpdateFinished();
    void set_marked_liveness(unsigned long long liveness);
    void add_marked_liveness(unsigned long long liveness);
    unsigned long long get_avg_mark_time();
};

// Factory for the pre-merge (region-reclaim-2) DGC-RDMA client used
// when SnicGCRegionAddressTranslation is OFF. Defined in
// noTransSnicCopyRegionClient.cc; declared here so snicClient.cc can
// dispatch without including the noTrans hpp (which has its own
// internal macros and helper classes).
SnicClient* create_no_trans_snic_copy_region_client();

#endif
