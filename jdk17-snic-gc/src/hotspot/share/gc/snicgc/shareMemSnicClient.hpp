#include "snicClient.hpp"
#include "snicCoordinator.hpp"
#include <linux/futex.h>
#include <sys/syscall.h>

#define SEND_LIVENESS_THRESHOLD 10000



class SnicMemRegion {
public:
    unsigned long long start;
    unsigned long long size;
    unsigned long long fileOffset;
    int fd;
    SnicMemRegion(unsigned long long s, unsigned long long len, unsigned long long f, int fd) : start(s), size(len), fileOffset(f), fd(fd) {}
    bool contains(unsigned long long addr) {
        return addr >= start && addr < start + size;
    }
};

class ShareMemSnicClient : public SnicClient {
public:
    size_t defaultPageSize = 0;
    unsigned long long appCDSBase = 0;
    unsigned long long appCDSSize = 0;
    // int clientSocket;
    int shareHeapFD = 0;
    int shareRootsFileFD = 0;
    int cdsFD = 0;
    int globalPacerFileFD = 0;
    int started_task_count = 0;
    int finished_task_count = 0;
    std::mutex task_count_lock;
    // WorkGang *snic_gc_workers;
    // ShenandoahObjToScanQueueSet *task_queues;
    // uint64_t* live_count;
    // uint64_t** live_data_caches;
    // ShenandoahMarkBitMap* bitmap;
    std::vector<SnicHeapRegion> region_info;
    unsigned long long* region_tams_info;
    std::vector<SnicMemRegion> virtual_space_regions;
    size_t class_space_base = 0;
    static GlobalPacerData* clientGlobalPacerData;

    static bool schedule_global_pacer_decided;
    static std::queue<int> schedule_host_ids;
public:
    ShareMemSnicClient();
    ~ShareMemSnicClient();
    void initialize();
    // int runRPCServer();
    int mmapShareMemSpace(void* addr, size_t length, off_t offset, int fd, int rpcType);
    void unmapShareMemSpace(void* addr, size_t length);
    // void resolveRPC(void* buffer) override;
    void handleRPC(int rpcType, int hostId, int sz, void* buffer) override;
    void handleJavaHeap(int bufferSize, void* buffer);
    void handleBitmap(int bufferSize, void* buffer);
    void handleLiveCount(int bufferSize, void* buffer);
    void handleRegionTamsInfo(int bufferSize, void *buffer);
    void handleRoots(int bufferSize, void* buffer, int rpcType);
    void handleTaskQueueRoots(int bufferSize, void* buffer);
    void handleSATBRoots(int bufferSize, void* buffer, int rpcType);
    void handleSATBRootsCommit(int bufferSize, void* buffer);
    void runShmMarkingLoop();  // lock-free SHM polling loop (replaces TCP for marking)
    void handleCombinedMarkingFromSHM(unsigned long long seqno);  // combined roots+SATB in one run_task
    void handleInlineMarkingFromSHM(unsigned long long seqno);   // inline SATB streaming (single-phase)
    void handleAppCDS(int bufferSize, void* buffer);
    void handleVirtualSpace(int bufferSize, void* buffer);
    void inc_liveness(unsigned long long r, intptr_t start, size_t s, uint worker_id);
    void do_oop(narrowOop* p, uint worker_id, int type = -1);
    void do_oops(uint worker_id);
    bool is_oop_safe_to_mark(oop obj) const;  // heap-bounds + alignment + non-null klass
    bool checkMarkerTerminateCondition(uint worker_id);
    void Commit();
    ShenandoahObjToScanQueue* get_queue(uint worker_id);
    void handleClassSpaceBase(int bufferSize, void* buffer);
    void remapVirtualNodesForCurHost();
    void unmapVirtualNodesForCurHost();
    static void schedule_global_pacer();
    static void reset_global_pacer();
    // unsigned long long get_avg_mark_time();
};

class shareMemSnicConcurrentMarkTask : public AbstractGangTask {
private:
    unsigned long long _root_num;
    unsigned long long* _root_buf;
    ShareMemSnicClient* _snic_client;
    std::vector<int> _indexes;
    int started_task_count = 0;
    int worker_num = 0;
    int _rpc_type;
public:
    shareMemSnicConcurrentMarkTask(unsigned long long num, unsigned long long* buf, ShareMemSnicClient* client, std::vector<int> parts, int rpc_type) :
        AbstractGangTask("Share Mem SNIC Concurrent Mark"), _root_num(num), _root_buf(buf), _snic_client(client), _indexes(parts), _rpc_type(rpc_type) {
        worker_num = (int) (_snic_client->snic_gc_workers->active_workers());
    }
    void work(uint worker_id) {
        log_debug(gc)("GC(%u,%d) DGC LOG: worker %u starts shareMemSnicConcurrentMarkTask", _snic_client->_gc_id, _snic_client->clientId, worker_id);
        auto worker_queue = _snic_client->get_queue(worker_id);
        uint index_num = (uint)(_indexes.size() - 1);
        uint process_id = worker_id;
        int push_count = 0;
        while (process_id < index_num) {
            log_debug(gc)("GC(%u,%d) DGC LOG: worker %u handles input queue %u for host %d", _snic_client->_gc_id, _snic_client->clientId, worker_id, process_id, _snic_client->clientId);
            int start = _indexes[process_id];
            int end = _indexes[process_id + 1];
            for (int handle_idx = start; handle_idx < end; ++handle_idx) {
                if (_root_buf[handle_idx] == (unsigned long long)(-1)) {
                    continue;
                }
                oop obj = (oop)(_root_buf[handle_idx]);
                bool upgraded = false;
                unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(_snic_client->heapBase)) / _snic_client->heapRegionSize;
                if (r_idx_o >= _snic_client->heapRegionNumber) {
                    log_error(gc)("GC(%u,%d) DGC LOG: worker %u handle oop in invalid region,r_idx_o=%llu,regionNumber=%zu,obj=%p,idx=%d,start=%d,end=%d",
                                  _snic_client->_gc_id, _snic_client->clientId, worker_id, r_idx_o, _snic_client->heapRegionNumber, obj, handle_idx, start, end);
                    exit(1);
                }
                // Above-TAMS objects are implicitly live in Shenandoah; NEVER
                // set a bitmap bit for them. Setting a bit here leaves a stale
                // mark that next cycle's clear_bitmap (which only clears
                // [bottom, top_bitmap)) won't wipe, violating the invariant
                // caught by Shenandoah Verifier ("Before Evacuation, Marked:
                // Must be marked in complete bitmap"). The verifier crashed
                // h2 with exactly this: a Long[] allocated_after_mark_start
                // had a bitmap bit set via this bootstrap path.
                unsigned long long obj_addr = (unsigned long long)obj;
                if (obj_addr >= _snic_client->region_tams_info[r_idx_o]) {
                    log_warning(gc)("GC(%u,%d) DGC LOG: shareMemSnicConcurrentMarkTask worker %u skip above-tams oop %p (tams=%p region=%llu rpc=%d)",
                                    _snic_client->_gc_id, _snic_client->clientId, worker_id,
                                    obj, (void*)_snic_client->region_tams_info[r_idx_o], r_idx_o, _rpc_type);
                    continue;
                }
                bool mark_rst = _snic_client->bitmap->mark_strong((HeapWord *)(obj), upgraded);
                if (mark_rst || _rpc_type == 5) {
                    push_count += 1;
                    ShenandoahMarkTask oneTask(obj, upgraded, false);
                    worker_queue->push(oneTask);
                    _snic_client->inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
                }
            }
            process_id += _snic_client->snic_gc_workers->active_workers();
        }
        log_debug(gc)("GC(%u,%d) DGC LOG: worker %u starts do_oops, push %d oops into task queue", _snic_client->_gc_id, _snic_client->clientId, worker_id, push_count);
        Atomic::add(&started_task_count, 1);
        while (Atomic::load(&started_task_count) < worker_num) {}
        _snic_client->do_oops(worker_id);
    }
};

// Combined marking + SATB in a single run_task() — avoids worker re-scheduling
class ShmCombinedMarkTask : public AbstractGangTask {
private:
    ShareMemSnicClient* _client;
    Universe::ShmDGCControl* _ctrl;
    unsigned long long _seqno;
    int _worker_num;
    // Phase 1 (roots) data
    unsigned long long* _roots_buf;
    int _roots_num;
    std::vector<int> _roots_indexes;
    // Phase 2 (SATB) data — filled by signaling worker after host provides it
    volatile unsigned long long* _satb_buf;
    volatile int _satb_num;
    std::vector<int> _satb_indexes;
    // Synchronization
    volatile int _phase1_push_done;
    volatile int _phase1_mark_done;
    volatile bool _satb_ready;
    volatile int _phase2_push_done;

    void parse_and_push(unsigned long long* buf, std::vector<int>& indexes, uint worker_id, int rpc_type) {
        auto worker_queue = _client->get_queue(worker_id);
        uint index_num = (uint)(indexes.size() - 1);
        uint process_id = worker_id;
        while (process_id < index_num) {
            int start = indexes[process_id];
            int end = indexes[process_id + 1];
            for (int handle_idx = start; handle_idx < end; ++handle_idx) {
                if (buf[handle_idx] == (unsigned long long)(-1)) continue;
                oop obj = (oop)(buf[handle_idx]);
                unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(_client->heapBase)) / _client->heapRegionSize;
                if (r_idx_o >= _client->heapRegionNumber) {
                    log_error(gc)("DGC LOG: combined task oop in invalid region, r_idx_o=%llu", r_idx_o);
                    exit(1);
                }
                // See shareMemSnicConcurrentMarkTask::work — never set a
                // bitmap bit for an allocated-after-mark-start object.
                unsigned long long obj_addr = (unsigned long long)obj;
                if (obj_addr >= _client->region_tams_info[r_idx_o]) {
                    log_warning(gc)("DGC LOG: ShmCombinedMarkTask worker %u skip above-tams oop %p (tams=%p region=%llu rpc=%d)",
                                    worker_id, obj, (void*)_client->region_tams_info[r_idx_o], r_idx_o, rpc_type);
                    continue;
                }
                bool upgraded = false;
                bool mark_rst = _client->bitmap->mark_strong((HeapWord*)(obj), upgraded);
                if (mark_rst || rpc_type == 5) {
                    ShenandoahMarkTask oneTask(obj, upgraded, false);
                    worker_queue->push(oneTask);
                    _client->inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
                }
            }
            process_id += _worker_num;
        }
    }

public:
    ShmCombinedMarkTask(ShareMemSnicClient* client, Universe::ShmDGCControl* ctrl,
                        unsigned long long seqno,
                        unsigned long long* roots_buf, int roots_num, std::vector<int> roots_idx)
        : AbstractGangTask("SHM Combined Mark"),
          _client(client), _ctrl(ctrl), _seqno(seqno),
          _roots_buf(roots_buf), _roots_num(roots_num), _roots_indexes(roots_idx),
          _satb_buf(nullptr), _satb_num(0),
          _phase1_push_done(0), _phase1_mark_done(0), _satb_ready(false), _phase2_push_done(0) {
        _worker_num = (int)(client->snic_gc_workers->active_workers());
    }

    void set_satb_data(unsigned long long* buf, int num, std::vector<int> idx) {
        _satb_indexes = idx;
        _satb_num = num;
        Atomic::release_store((volatile unsigned long long*)&_satb_buf, (unsigned long long)buf);
        Atomic::release_store(&_satb_ready, true);
    }

    void work(uint worker_id) {
        // === Phase 1: mark initial roots ===
        parse_and_push(_roots_buf, _roots_indexes, worker_id, 5);
        // Barrier: wait all workers finish pushing
        Atomic::add(&_phase1_push_done, 1);
        while (Atomic::load(&_phase1_push_done) < _worker_num) {}
        // Mark (traverse heap)
        _client->do_oops(worker_id);
        // Barrier: wait all workers finish marking
        int done = Atomic::add(&_phase1_mark_done, 1);
        if (done == _worker_num) {
            // Last worker: signal marking_done to host via futex
            Atomic::release_store(&_ctrl->marking_done_seqno, _seqno);
            syscall(SYS_futex, &_ctrl->marking_done_seqno, FUTEX_WAKE, 1, NULL, NULL, 0);
            log_debug(gc)("GC(%u,%d) DGC LOG: combined-task marking done + futex_wake (seqno=%llu)",
                         _client->_gc_id, _client->clientId, _seqno);
        } else {
            // Other workers: yield CPU while waiting (not spin-burn)
            while (!Atomic::load_acquire(&_satb_ready)) {
                sched_yield();
            }
        }

        if (done == _worker_num) {
            // === Last worker: poll satb_seqno + parse SATB data ===
            // (no extra thread needed — this worker is already awake)
            while (Atomic::load_acquire(&_ctrl->satb_seqno) < _seqno) {}

            auto satb_addr = (unsigned long long*)Atomic::load(&_ctrl->satb_roots_shm_addr);
            auto satb_size = (int)(Atomic::load(&_ctrl->satb_roots_size) / sizeof(unsigned long long));
            std::vector<int> satb_idx;
            int first = (int)(satb_addr[0]);
            satb_idx.push_back(first);
            for (int i = 0; i < first - 1; ++i) {
                satb_idx.push_back(satb_addr[i + 1]);
            }
            _satb_indexes = satb_idx;
            _satb_num = satb_size;
            _client->finished_task_count = 0;  // Reset for Phase 2 do_oops termination
            Atomic::release_store((volatile unsigned long long*)&_satb_buf, (unsigned long long)satb_addr);
            Atomic::release_store(&_satb_ready, true);
        }

        // === Phase 2: mark SATB roots ===
        while (!Atomic::load_acquire(&_satb_ready)) { sched_yield(); }
        auto satb_buf_local = (unsigned long long*)Atomic::load((volatile unsigned long long*)&_satb_buf);
        parse_and_push(satb_buf_local, _satb_indexes, worker_id, 9);
        Atomic::add(&_phase2_push_done, 1);
        while (Atomic::load(&_phase2_push_done) < _worker_num) {}
        _client->do_oops(worker_id);
    }
};

// Single-phase marking with inline SATB consumption from SHM stream
class ShmInlineMarkTask : public AbstractGangTask {
private:
    ShareMemSnicClient* _client;
    Universe::ShmDGCControl* _ctrl;
    unsigned long long _seqno;
    int _worker_num;
    unsigned long long* _roots_buf;
    std::vector<int> _roots_indexes;
    unsigned long long* _satb_buf;
    volatile unsigned long long _satb_read_cursor;
    volatile int _marking_done_signaled;
    volatile int _push_done;

    static const int SATB_CLAIM_BATCH = 32;

    void parse_and_push(unsigned long long* buf, std::vector<int>& indexes, uint worker_id) {
        auto worker_queue = _client->get_queue(worker_id);
        uint index_num = (uint)(indexes.size() - 1);
        uint process_id = worker_id;
        while (process_id < index_num) {
            int start = indexes[process_id];
            int end = indexes[process_id + 1];
            for (int handle_idx = start; handle_idx < end; ++handle_idx) {
                if (buf[handle_idx] == (unsigned long long)(-1)) continue;
                oop obj = (oop)(buf[handle_idx]);
                unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(_client->heapBase)) / _client->heapRegionSize;
                if (r_idx_o >= _client->heapRegionNumber) continue;
                // TAMS check is load-bearing: setting a bitmap bit above TAMS
                // leaves a stale mark that clear_bitmap won't wipe next cycle.
                if ((unsigned long long)obj >= _client->region_tams_info[r_idx_o]) continue;
                bool upgraded = false;
                _client->bitmap->mark_strong((HeapWord*)(obj), upgraded);
                ShenandoahMarkTask oneTask(obj, upgraded, false);
                worker_queue->push(oneTask);
                _client->inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
            }
            process_id += _worker_num;
        }
    }

    void mark_satb_oop(oop obj, uint worker_id) {
        auto wq = _client->get_queue(worker_id);
        unsigned long long r_idx = ((unsigned long long)obj - (unsigned long long)_client->heapBase) / _client->heapRegionSize;
        if (r_idx >= _client->heapRegionNumber) return;
        if ((unsigned long long)obj >= _client->region_tams_info[r_idx]) return;
        bool upgraded = false;
        if (_client->bitmap->mark_strong((HeapWord*)obj, upgraded)) {
            ShenandoahMarkTask task(obj, upgraded, false);
            wq->push(task);
            _client->inc_liveness(r_idx, (intptr_t)obj, (size_t)(obj->size()), worker_id);
        }
    }

    int drain_satb_stream(uint worker_id, unsigned long long avail) {
        int consumed = 0;
        while (true) {
            unsigned long long cursor = Atomic::load(&_satb_read_cursor);
            if (cursor >= avail) break;
            unsigned long long claim_end = cursor + SATB_CLAIM_BATCH;
            if (claim_end > avail) claim_end = avail;
            if (Atomic::cmpxchg(&_satb_read_cursor, cursor, claim_end) != cursor) continue;
            for (unsigned long long i = cursor; i < claim_end; i++) {
                oop obj = (oop)(_satb_buf[i]);
                if (obj != nullptr) {
                    mark_satb_oop(obj, worker_id);
                    consumed++;
                }
            }
        }
        return consumed;
    }

public:
    ShmInlineMarkTask(ShareMemSnicClient* client, Universe::ShmDGCControl* ctrl,
                      unsigned long long seqno,
                      unsigned long long* roots_buf, std::vector<int> roots_idx,
                      unsigned long long* satb_buf)
        : AbstractGangTask("SHM Inline Mark"),
          _client(client), _ctrl(ctrl), _seqno(seqno),
          _roots_buf(roots_buf), _roots_indexes(roots_idx),
          _satb_buf(satb_buf),
          _satb_read_cursor(0), _marking_done_signaled(0), _push_done(0) {
        _worker_num = (int)(client->snic_gc_workers->active_workers());
    }

    void work(uint worker_id) {
        // === Step 1: Push initial roots ===
        parse_and_push(_roots_buf, _roots_indexes, worker_id);
        Atomic::add(&_push_done, 1);
        while (Atomic::load(&_push_done) < _worker_num) {}

        // === Step 2: Single-phase marking loop with inline SATB drain ===
        // Matches baseline ShenandoahMark::mark_loop_work: process a full
        // stride of tasks before re-checking SATB stream or termination.
        // Stride = 1000 (= ShenandoahMarkLoopStride default). Amortizes
        // the per-iteration SHM volatile read + atomic load overhead over
        // 1000 tasks, matching baseline CCMT cost.
        static const uint STRIDE = 1000;
        auto wq = _client->get_queue(worker_id);
        ShenandoahMarkTask t;
        uint work_count = 0;

        while (true) {
            // Drain any SATB from the SHM stream once per stride
            unsigned long long avail = Atomic::load_acquire(&_ctrl->satb_stream_write_idx);
            drain_satb_stream(worker_id, avail);

            uint work = 0;
            for (uint i = 0; i < STRIDE; i++) {
                bool got = wq->pop(t);
                if (!got) got = _client->task_queues->steal(worker_id, t);
                if (!got) break;

                oop obj = t.obj();
                work++;
                work_count++;
                // Periodic liveness flush
                if (work_count % SEND_LIVENESS_THRESHOLD == 0) {
                    for (size_t ri = 0; ri < _client->heapRegionNumber; ++ri) {
                        uint64_t lv = _client->live_data_caches[worker_id][ri];
                        if (lv > 0) {
                            Atomic::add(&_client->live_count[ri], lv);
                            _client->live_data_caches[worker_id][ri] = 0;
                        }
                    }
                }
                // Object traversal (same as do_oops body)
                if (obj->klass()->id() == ObjArrayKlassID) {
                    objArrayOop a = objArrayOop(obj);
                    narrowOop *p = (narrowOop *)(a->base());
                    narrowOop *const end = p + a->length();
                    for (; p < end; p++) _client->do_oop(p, worker_id, 1);
                } else if (InstanceKlass::cast(obj->klass())->reference_type() != REF_NONE) {
                    OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
                    OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
                    for (; map < end_map; ++map) {
                        narrowOop *p = (narrowOop *)obj->field_addr(map->offset());
                        narrowOop *end = p + map->count();
                        for (; p < end; ++p) _client->do_oop(p, worker_id, 2);
                    }
                    narrowOop *referent_addr = UseCompressedClassPointers
                        ? (narrowOop *)((unsigned long long)(obj) + 12)
                        : (narrowOop *)((unsigned long long)(obj) + 16);
                    _client->do_oop(referent_addr, worker_id, 3);
                } else {
                    OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
                    OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
                    for (; map < end_map; ++map) {
                        narrowOop *p = (narrowOop *)obj->field_addr(map->offset());
                        narrowOop *end = p + map->count();
                        for (; p < end; ++p) _client->do_oop(p, worker_id, 2);
                    }
                    if (obj->klass()->id() == InstanceMirrorKlassID) {
                        narrowOop *p = (narrowOop *)((InstanceMirrorKlass *)obj->klass())->start_of_static_fields(obj);
                        narrowOop *const end = p + java_lang_Class::static_oop_field_count_raw(obj);
                        for (; p < end; ++p) _client->do_oop(p, worker_id, 4);
                    }
                }
            }

            if (work == 0) {
                // Stride produced no tasks — check termination
                unsigned long long flush_done = Atomic::load_acquire(&_ctrl->satb_flush_complete);
                avail = Atomic::load_acquire(&_ctrl->satb_stream_write_idx);
                unsigned long long read = Atomic::load(&_satb_read_cursor);

                if (flush_done && read >= avail) {
                    if (_client->checkMarkerTerminateCondition(worker_id)) break;
                } else if (!flush_done && read >= avail) {
                    // Signal host to do final SATB flush (once)
                    if (Atomic::load(&_marking_done_signaled) == 0) {
                        if (Atomic::cmpxchg(&_marking_done_signaled, 0, 1) == 0) {
                            Atomic::release_store(&_ctrl->marking_done_seqno, _seqno);
                            syscall(SYS_futex, &_ctrl->marking_done_seqno, FUTEX_WAKE, 1, NULL, NULL, 0);
                            log_debug(gc)("GC(%u,%d) DGC LOG: inline-task marking_done (seqno=%llu)",
                                         _client->_gc_id, _client->clientId, _seqno);
                        }
                    }
                    sched_yield();
                }
            }
        }
        log_debug(gc)("GC(%u,%d) DGC LOG: worker %u inline-task done, work_count=%u",
                     _client->_gc_id, _client->clientId, worker_id, work_count);
    }
};
