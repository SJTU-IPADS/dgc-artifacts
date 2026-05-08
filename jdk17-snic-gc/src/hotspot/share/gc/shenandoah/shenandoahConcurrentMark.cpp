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

#include "precompiled.hpp"

#include "gc/shared/satbMarkQueue.hpp"
#include "gc/shared/strongRootsScope.hpp"
#include "gc/shared/taskTerminator.hpp"
#include "gc/shenandoah/shenandoahBarrierSet.inline.hpp"
#include "gc/shenandoah/shenandoahClosures.inline.hpp"
#include "gc/shenandoah/shenandoahConcurrentMark.hpp"
#include "gc/shenandoah/shenandoahHeap.inline.hpp"
#include "gc/shenandoah/shenandoahMark.inline.hpp"
#include "gc/shenandoah/shenandoahReferenceProcessor.hpp"
#include "gc/shenandoah/shenandoahRootProcessor.inline.hpp"
#include "gc/shenandoah/shenandoahOopClosures.inline.hpp"
#include "gc/shenandoah/shenandoahPhaseTimings.hpp"
#include "gc/shenandoah/shenandoahStringDedup.hpp"
#include "gc/shenandoah/shenandoahTaskqueue.inline.hpp"
#include "gc/shenandoah/shenandoahUtils.hpp"
#include "memory/iterator.inline.hpp"
#include "memory/metaspace/virtualSpaceList.hpp"
#include "memory/resourceArea.hpp"
#include "oops/compressedOops.hpp"
#include "gc/snicgc/copyRegionSnicClient.hpp"
#include <sys/stat.h>
#include <sys/mman.h>
#include <vector>
#include <pthread.h>
#include <sched.h>
#include <numeric>
#include <linux/futex.h>
#include <sys/syscall.h>

// futex helpers for cross-process signaling on SHM
static inline void futex_wait(volatile unsigned long long* addr, unsigned long long expected) {
  // Wait if *addr == expected; wakes on futex_wake or spuriously
  syscall(SYS_futex, addr, FUTEX_WAIT, (int)expected, NULL, NULL, 0);
}
static inline void futex_wake(volatile unsigned long long* addr) {
  syscall(SYS_futex, addr, FUTEX_WAKE, 1, NULL, NULL, 0);
}
static inline void futex_wait_timed(volatile unsigned long long* addr, unsigned long long expected, long timeout_us) {
  struct timespec ts;
  ts.tv_sec = timeout_us / 1000000;
  ts.tv_nsec = (timeout_us % 1000000) * 1000;
  syscall(SYS_futex, addr, FUTEX_WAIT, (int)expected, &ts, NULL, 0);
}

// DIAG: per-cycle counters for SATB flow sanity check (see finish_mark_work).
// _streamed_to_client: non-null oops written to SHM by SATBToShmClosure
// _drained_by_host:    non-null oops pulled from completed SATB buffers by the
//                      host's local ShenandoahSATBBufferClosure in final_mark
// Both reset at cycle start (in concurrent_mark lock-free branch).
// File-scope (non-anonymous) so ShenandoahSATBBufferClosure in
// shenandoahMark.inline.hpp can declare it via `extern`.
volatile uint64_t g_satb_streamed_to_client = 0;
volatile uint64_t g_satb_drained_by_host    = 0;

// DIAG: count of SATB oops that overflowed the SHM buffer and fell back to
// host-local marking. If this is non-zero for a cycle, we know the static
// PREALLOC_ROOTS_SIZE is too small for that workload.
volatile uint64_t g_satb_overflow_to_host = 0;

// Closure that drains a completed SATB buffer. The fast path streams the oops
// into the SHM flat array for the client to consume. If the SHM buffer
// saturates (PREALLOC_ROOTS_SIZE / 8 entries), the closure falls back to
// marking the overflowed oops LOCALLY on the host: it goes through
// mark_context->mark_strong (which honours allocated_after_mark_start / TAMS)
// and pushes them onto worker 0's task queue, so final_mark's mark_loop will
// transitively drain them. This avoids silently dropping SATB entries — the
// root cause of an earlier SIGSEGV in h2 where the verifier reported
// "Must be marked in complete bitmap" for an object whose SATB pre-write
// value had been dropped on buffer saturation.
class SATBToShmClosure : public SATBBufferClosure {
private:
  unsigned long long*          _dest;
  unsigned long long&          _count;
  unsigned long long           _capacity;
  ShenandoahObjToScanQueue*    _overflow_queue;
  ShenandoahMarkingContext*    _mark_context;
public:
  SATBToShmClosure(unsigned long long* dest, unsigned long long& count, unsigned long long capacity,
                   ShenandoahObjToScanQueue* overflow_queue,
                   ShenandoahMarkingContext* mark_context)
    : _dest(dest), _count(count), _capacity(capacity),
      _overflow_queue(overflow_queue), _mark_context(mark_context) {}
  void do_buffer(void** buffer, size_t size) {
    for (size_t i = 0; i < size; i++) {
      if (buffer[i] == nullptr) continue;
      if (_count < _capacity) {
        _dest[_count++] = (unsigned long long)buffer[i];
        Atomic::inc(&g_satb_streamed_to_client);
      } else {
        // SHM buffer saturated — fall back to host-local marking. This
        // path goes through mark_context->mark_strong which honours
        // allocated_after_mark_start (skips above-TAMS) so we never set
        // stale bits in the bitmap.
        oop obj = cast_to_oop(buffer[i]);
        bool upgraded = false;
        if (_mark_context != nullptr && _mark_context->mark_strong(obj, upgraded)) {
          if (_overflow_queue != nullptr) {
            _overflow_queue->push(ShenandoahMarkTask(obj, upgraded, false));
          }
        }
        Atomic::inc(&g_satb_overflow_to_host);
      }
    }
  }
};

class ShenandoahUpdateRootsTask : public AbstractGangTask {
private:
  ShenandoahRootUpdater*  _root_updater;
  bool                    _check_alive;
public:
  ShenandoahUpdateRootsTask(ShenandoahRootUpdater* root_updater, bool check_alive) :
    AbstractGangTask("Shenandoah Update Roots"),
    _root_updater(root_updater),
    _check_alive(check_alive){
  }

  void work(uint worker_id) {
    assert(ShenandoahSafepoint::is_at_shenandoah_safepoint(), "Must be at a safepoint");
    ShenandoahParallelWorkerSession worker_session(worker_id);

    ShenandoahHeap* heap = ShenandoahHeap::heap();
    ShenandoahUpdateRefsClosure cl;
    if (_check_alive) {
      ShenandoahForwardedIsAliveClosure is_alive;
      _root_updater->roots_do<ShenandoahForwardedIsAliveClosure, ShenandoahUpdateRefsClosure>(worker_id, &is_alive, &cl);
    } else {
      AlwaysTrueClosure always_true;;
      _root_updater->roots_do<AlwaysTrueClosure, ShenandoahUpdateRefsClosure>(worker_id, &always_true, &cl);
    }
  }
};

void set_gc_thread_affinity() {
  if(strlen(GCThreadsCores) > 0){
    log_info(os, thread)("GCThreadsCores: %s", GCThreadsCores);
    // 设置线程的CPU亲和性
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpu_numbers = {};
    int gc_core_len = strlen(GCThreadsCores);
    int i = 0;
    int core_idx = 0;
    while(i < gc_core_len){
      if(GCThreadsCores[i] == ','){
        CPU_SET(core_idx, &cpuset);
        cpu_numbers.push_back(core_idx);
        core_idx = 0;
      }
      else{
        core_idx *= 10;
        core_idx += GCThreadsCores[i] - '0';
      }
      i += 1;
    }
    CPU_SET(core_idx, &cpuset);
    cpu_numbers.push_back(core_idx);

    pthread_t thread = pthread_self();
    pid_t tid = gettid();
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    // ret = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
      log_warning(os, thread)("Failed to set gc thread affinity - pthread_setaffinity_np failed (%d=%s)",
        ret, os::errno_name(ret));
    } else {
      std::string cpu_numbers_str = "";
      for(unsigned int i = 0; i < cpu_numbers.size(); i++){
        cpu_numbers_str += std::to_string(cpu_numbers[i]);
        if(i != cpu_numbers.size() - 1){
          cpu_numbers_str += ",";
        }
      }
      log_info(os, thread)("Successfully set gc thread %i affinity to %s", tid, cpu_numbers_str.c_str());
    }
  }
}


void unset_gc_thread_affinity() {
  if(strlen(MutatorCores) > 0){
    log_info(os, thread)("MutatorCores: %s", MutatorCores);
    // 设置线程的CPU亲和性
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    std::vector<int> cpu_numbers = {};
    int gc_core_len = strlen(MutatorCores);
    int i = 0;
    int core_idx = 0;
    while(i < gc_core_len){
      if(MutatorCores[i] == ','){
        CPU_SET(core_idx, &cpuset);
        cpu_numbers.push_back(core_idx);
        core_idx = 0;
      }
      else{
        core_idx *= 10;
        core_idx += MutatorCores[i] - '0';
      }
      i += 1;
    }
    CPU_SET(core_idx, &cpuset);
    cpu_numbers.push_back(core_idx);

    pthread_t thread = pthread_self();
    pid_t tid = gettid();
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    // ret = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
      log_warning(os, thread)("Failed to set gc thread affinity - pthread_setaffinity_np failed (%d=%s)",
        ret, os::errno_name(ret));
    } else {
      std::string cpu_numbers_str = "";
      for(unsigned int i = 0; i < cpu_numbers.size(); i++){
        cpu_numbers_str += std::to_string(cpu_numbers[i]);
        if(i != cpu_numbers.size() - 1){
          cpu_numbers_str += ",";
        }
      }
      log_info(os, thread)("Successfully unset gc thread %i affinity to %s", tid, cpu_numbers_str.c_str());
    }
  }
}


class ShenandoahConcurrentMarkingTask : public AbstractGangTask {
private:
  ShenandoahConcurrentMark* const _cm;
  TaskTerminator* const           _terminator;

public:
  ShenandoahConcurrentMarkingTask(ShenandoahConcurrentMark* cm, TaskTerminator* terminator) :
    AbstractGangTask("Shenandoah Concurrent Mark"), _cm(cm), _terminator(terminator) {
  }

  void work(uint worker_id) {
    if(!SnicGCHost){
      set_gc_thread_affinity();
    }
    if(MultiJVMForceBindCoreEnabled){
      set_gc_thread_affinity();
    }
    ShenandoahHeap* heap = ShenandoahHeap::heap();
    ShenandoahConcurrentWorkerSession worker_session(worker_id);
    ShenandoahSuspendibleThreadSetJoiner stsj(ShenandoahSuspendibleWorkers);
    ShenandoahObjToScanQueue* q = _cm->get_queue(worker_id);
    ShenandoahReferenceProcessor* rp = heap->ref_processor();
    assert(rp != NULL, "need reference processor");
    _cm->mark_loop(worker_id, _terminator, rp,
                   true /*cancellable*/,
                   ShenandoahStringDedup::is_enabled() ? ENQUEUE_DEDUP : NO_DEDUP);
    if(!SnicGCHost){
      unset_gc_thread_affinity();
    }
    if(MultiJVMForceBindCoreEnabled){
      unset_gc_thread_affinity();
    }
  }
};

class SnicCollectSATBRootsTask : public AbstractGangTask {
private:
  SATBMarkQueueSet &_satb_mq_set;
  ShenandoahConcurrentMark* _cm;
public:
  std::vector<std::vector<unsigned long long>> _oop_vecs;
  std::vector<std::vector<size_t>> _buf_sizes_vecs;
public:
  SnicCollectSATBRootsTask(SATBMarkQueueSet &satb_mq_set, ShenandoahConcurrentMark* cm) : AbstractGangTask("Snic Collect SATBRoots Task"), _satb_mq_set(satb_mq_set), _cm(cm) {
    _oop_vecs = std::vector<std::vector<unsigned long long>>(ShenandoahHeap::heap()->workers()->active_workers());
    _buf_sizes_vecs = std::vector<std::vector<size_t>>(ShenandoahHeap::heap()->workers()->active_workers());
  }
  void work(uint worker_id) {
    auto queue = _cm->get_queue(worker_id);
    while (_satb_mq_set.completed_buffers_num() > 0) {
      SnicCollectSATBRootsToTaskQueueClosure cl(queue, &_oop_vecs[worker_id], &_buf_sizes_vecs[worker_id]);
      _satb_mq_set.apply_closure_to_completed_buffer(&cl);
    }
  }
};

class ShenandoahSATBAndRemarkThreadsClosure : public ThreadClosure {
private:
  SATBMarkQueueSet& _satb_qset;
  OopClosure* const _cl;
  uintx _claim_token;

public:
  ShenandoahSATBAndRemarkThreadsClosure(SATBMarkQueueSet& satb_qset, OopClosure* cl) :
    _satb_qset(satb_qset),
    _cl(cl),
    _claim_token(Threads::thread_claim_token()) {}

  void do_thread(Thread* thread) {
    if (thread->claim_threads_do(true, _claim_token)) {
      // Transfer any partial buffer to the qset for completed buffer processing.
      _satb_qset.flush_queue(ShenandoahThreadLocalData::satb_mark_queue(thread));
      if (thread->is_Java_thread()) {
        if (_cl != NULL) {
          ResourceMark rm;
          thread->oops_do(_cl, NULL);
        }
      }
    }
  }
};

class ShenandoahFinalMarkingTask : public AbstractGangTask {
private:
  ShenandoahConcurrentMark* _cm;
  TaskTerminator*           _terminator;
  bool                      _dedup_string;

public:
  ShenandoahFinalMarkingTask(ShenandoahConcurrentMark* cm, TaskTerminator* terminator, bool dedup_string) :
    AbstractGangTask("Shenandoah Final Mark"), _cm(cm), _terminator(terminator), _dedup_string(dedup_string) {
  }

  void work(uint worker_id) {
    ShenandoahHeap* heap = ShenandoahHeap::heap();

    ShenandoahParallelWorkerSession worker_session(worker_id);
    ShenandoahReferenceProcessor* rp = heap->ref_processor();
    // First drain remaining SATB buffers.
    {
      ShenandoahObjToScanQueue* q = _cm->get_queue(worker_id);
      // TEST ONLY
      q->queue_worker_id = worker_id;

      ShenandoahSATBBufferClosure cl(q);
      SATBMarkQueueSet& satb_mq_set = ShenandoahBarrierSet::satb_mark_queue_set();
      // log_debug(gc)("DGC LOG: remaining satb buffer num:%lu", satb_mq_set.completed_buffers_num());
      while (satb_mq_set.apply_closure_to_completed_buffer(&cl)) {}
      assert(!heap->has_forwarded_objects(), "Not expected");

      ShenandoahMarkRefsClosure<NO_DEDUP> mark_cl(q, rp);
      ShenandoahSATBAndRemarkThreadsClosure tc(satb_mq_set, ShenandoahIUBarrier ? &mark_cl : NULL);
      Threads::threads_do(&tc);
    }
    _cm->mark_loop(worker_id, _terminator, rp,
                   false /*not cancellable*/,
                   _dedup_string ? ENQUEUE_DEDUP : NO_DEDUP);
    assert(_cm->task_queues()->is_empty(), "Should be empty");
  }
};

ShenandoahConcurrentMark::ShenandoahConcurrentMark() :
  ShenandoahMark() {}

// Mark concurrent roots during concurrent phases
class ShenandoahMarkConcurrentRootsTask : public AbstractGangTask {
private:
  SuspendibleThreadSetJoiner          _sts_joiner;
  ShenandoahConcurrentRootScanner     _root_scanner;
  ShenandoahObjToScanQueueSet* const  _queue_set;
  ShenandoahReferenceProcessor* const _rp;

public:
  ShenandoahMarkConcurrentRootsTask(ShenandoahObjToScanQueueSet* qs,
                                    ShenandoahReferenceProcessor* rp,
                                    ShenandoahPhaseTimings::Phase phase,
                                    uint nworkers);
  void work(uint worker_id);
};

ShenandoahMarkConcurrentRootsTask::ShenandoahMarkConcurrentRootsTask(ShenandoahObjToScanQueueSet* qs,
                                                                     ShenandoahReferenceProcessor* rp,
                                                                     ShenandoahPhaseTimings::Phase phase,
                                                                     uint nworkers) :
  AbstractGangTask("Shenandoah Concurrent Mark Roots"),
  _root_scanner(nworkers, phase),
  _queue_set(qs),
  _rp(rp) {
  assert(!ShenandoahHeap::heap()->has_forwarded_objects(), "Not expected");
}

void ShenandoahMarkConcurrentRootsTask::work(uint worker_id) {
  ShenandoahConcurrentWorkerSession worker_session(worker_id);
  ShenandoahObjToScanQueue* q = _queue_set->queue(worker_id);
  // TEST ONLY
  q->queue_worker_id = worker_id;
  // Cannot enable string deduplication during root scanning. Otherwise,
  // may result lock inversion between stack watermark and string dedup queue lock.
  ShenandoahMarkRefsClosure<NO_DEDUP> cl(q, _rp);
  _root_scanner.roots_do(&cl, worker_id);
}

void ShenandoahConcurrentMark::mark_concurrent_roots() {
  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  assert(!heap->has_forwarded_objects(), "Not expected");
  WorkGang *workers = heap->workers();
  auto origin_nworkers = workers->active_workers();
  TASKQUEUE_STATS_ONLY(task_queues()->reset_taskqueue_stats());
  // if (SnicGCHost && !SnicGCShareMemEnabled) {
  //   workers->update_active_workers(workers->total_workers());
  // }
  Universe::start_mark_roots();
  ShenandoahReferenceProcessor* rp = heap->ref_processor();
  task_queues()->reserve(workers->active_workers());
  log_debug(gc)("active worker num before conc mark roots:%u", workers->active_workers());
  ShenandoahMarkConcurrentRootsTask task(task_queues(), rp, ShenandoahPhaseTimings::conc_mark_roots, workers->active_workers());
  workers->run_task(&task);
  Universe::finish_mark_roots();
  // if (SnicGCHost && !SnicGCShareMemEnabled) {
  //   workers->update_active_workers(origin_nworkers);
  // }
}

class ShenandoahFlushSATBHandshakeClosure : public HandshakeClosure {
private:
  SATBMarkQueueSet& _qset;
public:
  ShenandoahFlushSATBHandshakeClosure(SATBMarkQueueSet& qset) :
    HandshakeClosure("Shenandoah Flush SATB Handshake"),
    _qset(qset) {}

  void do_thread(Thread* thread) {
    _qset.flush_queue(ShenandoahThreadLocalData::satb_mark_queue(thread));
  }
};

void ShenandoahConcurrentMark::wait_client_copy_gc_roots_finish(int i) {
  auto heap = ShenandoahHeap::heap();
  while (heap->_snic_gc_roots_copy_finish_flag[0] != i) {
    // log_info(gc)("wait for finish flag:%d, current flag:%d", i, heap->_snic_gc_roots_copy_finish_flag[0]);
    continue;
  }
  log_debug(gc)("DGC LOG: client copy gc roots finish:%d", i);
}

void ShenandoahConcurrentMark::wait_client_copy_gc_roots_finish_with_ack(std::queue<int> &rpc_normal_ack_queue, std::queue<int> &rpc_final_ack_queue) {
  while (true) {
    // int ack_type = Universe::get_ack_type();
    int ack_type = (int) (Universe::recv_int_ack());
    if (ack_type == (int)(REMOTE_GC_ROOTS_COPY_FINISH_ACK)) {
      break;
    } else {
      if (ack_type == (int)(SATB_ROOTS_NORMAL_ACK)) {
        rpc_normal_ack_queue.push(ack_type);
      } else if (ack_type == (int)(SATB_ROOTS_FINAL_ACK)) {
        rpc_final_ack_queue.push(ack_type);
      }
    }
  }
}

void ShenandoahConcurrentMark::wait_client_liveness_and_finish_ack(std::queue<int> &rpc_normal_ack_queue, std::queue<int> &rpc_final_ack_queue, size_t *total_liveness_received, int rpc_type) {
  int liveness_update_count = 0;
  while (true) {
    size_t ack = Universe::recv_int_ack();
    log_info(gc)("LHT LOG: wait_client_liveness_and_finish_ack, get ack=%llu", ack);
    if (ack == (size_t)(SATB_ROOTS_FINISH_ACK) && (rpc_type == 9 || rpc_type == 10)) {
      break;
    }
    if (ack == (size_t)(TASK_QUEUE_ROOTS_FINISH_ACK) && (rpc_type == 2)) {
      break;
    }
    if (ack == (size_t) (SATB_ROOTS_NORMAL_ACK)) {
      rpc_normal_ack_queue.push(ack);
    } else if (ack == (size_t) (SATB_ROOTS_FINAL_ACK)) {
      rpc_final_ack_queue.push(ack);
    } else if (ack > (size_t) (SATB_ROOTS_FINISH_ACK)) {
      *total_liveness_received += (size_t)(ack);
      if (ShenandoahPacing) {
        if (liveness_update_count < 16) {
          ShenandoahHeap::heap()->pacer()->report_mark((size_t)(ack * 16));
        } else {
          ShenandoahHeap::heap()->pacer()->report_mark((size_t)(ack));
        }
      }
      liveness_update_count++;
    }
  }
  log_debug(gc)("liveness update count %d during client handling rpc %d", liveness_update_count, rpc_type);
}

std::vector<int> ShenandoahConcurrentMark::decide_host_splits_by_dpu_client_num() {
  int dpu_client_marker_num = DpuClientMarkerNum;
  uint active_worker_num = ShenandoahHeap::heap()->workers()->active_workers();
  std::vector<int> host_splits;
  for (uint i = 0; i < active_worker_num; i++) {
    host_splits.push_back(1);
  }
  // int rest = dpu_client_marker_num - active_worker_num;
  int rest = active_worker_num - dpu_client_marker_num;
  int idx = 0;
  while (rest > 0) {
    host_splits[idx]++;
    idx = (idx + 1) % active_worker_num;
    rest--;
  }
  return host_splits;
}

void ShenandoahConcurrentMark::flush_task_queues_and_send(char* buffer, int rpc_type, bool is_prefetched) {
  int top = 1;
  int count = 0;
  auto heap = ShenandoahHeap::heap();
  auto ull_gc_roots_buffer = (unsigned long long*) (heap->_snic_gc_roots_buffer);
  uint total_worker_num = heap->workers()->active_workers();
  unsigned long long *message = (unsigned long long *)(buffer + 2 * sizeof(int));
  ShenandoahMarkTask t;
  ull_gc_roots_buffer[0] = (unsigned long long)(1);
  auto host_splits = decide_host_splits_by_dpu_client_num();
  unsigned long calculated_root_count = 0;
  for (uint i = 0; i < total_worker_num; i++) {
    int host_split = host_splits[i];
    auto que = get_queue(i);
    auto que_size = que->size() + que->overflow_stack()->size() + 1;
    calculated_root_count += que_size;
    int pop_count = 0;
    auto root_num_per_split = que_size / host_split;
    auto target_pop_count = root_num_per_split;
    for (int j = 0; j < host_split; ++j) {
      pop_count = 0;
      if (j == host_split - 1) {
        target_pop_count = que_size - root_num_per_split * j;
      }
      while (que->pop(t)) {
        ull_gc_roots_buffer[top] = (unsigned long long)(t.obj());
        top++;
        pop_count++;
        if ((unsigned long) pop_count == target_pop_count) {
          break;
        }
      }
      ull_gc_roots_buffer[top] = (unsigned long long)(-1);
      top++;
    }
  }
  // for (uint wid = 0; wid < total_worker_num; ++wid) {
  //   auto que = get_queue(wid);
  //   while (que->pop(t)) {
  //     auto top_oop = t.obj();
  //     ull_gc_roots_buffer[top] = (unsigned long long)(top_oop);
  //     top++;
  //     count++;
  //   }
  //   // put a placeholder at the end of queue data
  //   ull_gc_roots_buffer[top] = (unsigned long long)(-1);
  //   top++;
  // }
  message[0] = top;
  message[1] = is_prefetched ? 1ULL : 0ULL;
  Universe::send_rpc(rpc_type, 2 * sizeof(int) + 2 * sizeof(unsigned long long), (void *)buffer);
  log_debug(gc)("DGC LOG: task queue roots count:%d, calculated roots count:%lu\n", top, calculated_root_count);
}

std::vector<int> ShenandoahConcurrentMark::decide_host_splits_by_shm_client_marker_num() {
  int shm_client_marker_num = ShmClientMarkerNum;
  uint active_worker_num = ShenandoahHeap::heap()->workers()->active_workers();
  std::vector<int> host_splits;
  for (uint i = 0; i < active_worker_num; i++) {
    host_splits.push_back(1);
  }
  int rest = shm_client_marker_num - active_worker_num;
  int idx = 0;
  while (rest > 0) {
    host_splits[idx]++;
    idx = (idx + 1) % active_worker_num;
    rest--;
  }
  return host_splits;
}

void ShenandoahConcurrentMark::snic_shm_send_roots(int rpcType) {
  int shm_client_marker_num = ShmClientMarkerNum;
  std::vector<int> host_splits = decide_host_splits_by_shm_client_marker_num();
  auto heap = ShenandoahHeap::heap();
  int total_root_count = 0;
  std::vector<int> queue_indexes;
  // compute roots count before creating shm to remove an extra memcpy operations.
  for (uint i = 0; i < heap->workers()->active_workers(); i++) {
    auto que = get_queue(i);
    total_root_count += que->size();
    total_root_count += que->overflow_stack()->size();
    // log_debug(gc)("DGC LOG: init queue %d size:%d, overflow_stack size:%lu", i, que->size(), que->overflow_stack()->size());
    total_root_count += 1; // for buffered first item.
    // total_root_count += 1; // for -1 used to notify the end of this queue.
    // total_root_count += 1; // for queue index.
  }
  total_root_count += 1; // for queues number.
  total_root_count += shm_client_marker_num; // for -1 used to notify the end of this queue.
  total_root_count += shm_client_marker_num; // for queue indexes.
  off_t roots_file_size = 0;
  struct stat shm_stat;
  fstat(Universe::get_hostShareRootFD(), &shm_stat);
  off_t cur_file_size = shm_stat.st_size;
  if (rpcType != 5) {
    roots_file_size = shm_stat.st_size;
  }
  unsigned long long* root_addr = (unsigned long long*) Universe::hostMmap(nullptr, total_root_count * sizeof(unsigned long long), roots_file_size, Universe::get_hostShareRootFD(), rpcType);
  // ftruncate shm file.
  auto new_file_size = roots_file_size + total_root_count * sizeof(unsigned long long);
  auto single_page_size = os::vm_page_size();
  if (new_file_size % single_page_size != 0) {
    new_file_size = new_file_size + single_page_size - new_file_size % single_page_size;
  }
  if ((unsigned long) cur_file_size < new_file_size) {
    Universe::ftruncateFile(Universe::get_hostShareRootFD(), (size_t)(new_file_size));
  }
  log_debug(gc)("finish truncate file %d for rpc type %d in func snic_shm_send_roots", Universe::get_hostShareRootFD(), rpcType);
  log_debug(gc)("DGC LOG: shm task queue roots size is %lu B", total_root_count * sizeof(unsigned long long));
  // fill the shm space with roots in queues.
  ShenandoahMarkTask t;
  uint active_workers = heap->workers()->active_workers();
  // int top = (int) (active_workers) + 1;
  int top = shm_client_marker_num + 1;
  for (uint i = 0; i < active_workers; i++) {
    // auto que = get_queue(i);
    // while (que->pop(t)) {
    //   root_addr[top++] = (unsigned long long)t.obj();
    // }
    // root_addr[top] = (unsigned long long)(-1);
    // queue_indexes.push_back(top);
    // top++;
    int host_split = host_splits[i];
    auto que = get_queue(i);
    auto que_size = que->size() + que->overflow_stack()->size() + 1;
    int pop_count = 0;
    auto root_num_per_split = que_size / host_split;
    auto target_pop_count = root_num_per_split;
    for (int j = 0; j < host_split; ++j) {
      pop_count = 0;
      if (j == host_split - 1) {
        target_pop_count = que_size - root_num_per_split * j;
      }
      while (que->pop(t)) {
        root_addr[top] = (unsigned long long)(t.obj());
        top++;
        pop_count++;
        if ((unsigned long) pop_count == target_pop_count) {
          break;
        }
      }
      root_addr[top] = (unsigned long long) (-1);
      queue_indexes.push_back(top);
      top += 1;
    }
  }
  // root_addr[0] = (unsigned long long) (active_workers + 1);
  // for (uint i = 0; i < active_workers; i++) {
  root_addr[0] = (unsigned long long) (shm_client_marker_num + 1);
  for (int i = 0; i < shm_client_marker_num; i++) {
    root_addr[i + 1] = (unsigned long long) queue_indexes[i];
  }
  // then send rpc to snic client.
  char* buffer = new char[1024];
  unsigned long long* message = (unsigned long long*)(buffer + 2 * sizeof(int));
  message[0] = (unsigned long long)root_addr;
  message[1] = (unsigned long long)total_root_count * sizeof(unsigned long long);
  message[2] = (unsigned long long)roots_file_size;
  message[3] = (unsigned long long)GCId::current();
  Universe::send_rpc(rpcType, 2 * sizeof(int) + 4 * sizeof(unsigned long long), (void *)buffer);
  log_debug(gc)("DGC LOG: finish send rpc %d in func snic_shm_send_roots", rpcType);
  delete(buffer);
}

void ShenandoahConcurrentMark::snic_collect_satb_roots_and_send(std::vector<std::vector<unsigned long long>> *oop_vecs, int rpcType) {
  int shm_client_marker_num = ShmClientMarkerNum;
  std::vector<int> host_splits = decide_host_splits_by_shm_client_marker_num();
  auto heap = ShenandoahHeap::heap();
  int total_root_count = 0;
  std::vector<int> queue_indexes;
  // compute roots count before creating shm to remove an extra memcpy operations.
  for (uint i = 0; i < heap->workers()->active_workers(); i++) {
    total_root_count += oop_vecs->at(i).size();
    // total_root_count += 1; // for -1 used to notify the end of this queue.
    // total_root_count += 1; // for queue index.
  }
  total_root_count += 1; // for queues number.
  total_root_count += shm_client_marker_num; // for -1 used to notify the end of this queue.
  total_root_count += shm_client_marker_num; // for queue indexes.
  off_t roots_file_size = 0;
  struct stat shm_stat;
  fstat(Universe::get_hostShareRootFD(), &shm_stat);
  off_t cur_file_size = shm_stat.st_size;
  // if (rpcType != 5) {
  //   roots_file_size = shm_stat.st_size;
  // }
  unsigned long long *root_addr = (unsigned long long *)Universe::hostMmap(nullptr, total_root_count * sizeof(unsigned long long), roots_file_size, Universe::get_hostShareRootFD(), rpcType);
  // ftruncate shm file.
  auto new_file_size = roots_file_size + total_root_count * sizeof(unsigned long long);
  auto single_page_size = os::vm_page_size();
  if (new_file_size % single_page_size != 0) {
    new_file_size = new_file_size + single_page_size - new_file_size % single_page_size;
  }
  if ((unsigned long) cur_file_size < new_file_size) {
    Universe::ftruncateFile(Universe::get_hostShareRootFD(), (size_t)(new_file_size));
  }
  log_debug(gc)("finish truncate file %d for rpc type %d in func snic_collect_satb_roots_and_send", Universe::get_hostShareRootFD(), rpcType);
  log_debug(gc)("DGC LOG: shm satb roots size is %lu B", total_root_count * sizeof(unsigned long long));
  // fill the shm space with roots in queues.
  uint active_workers = heap->workers()->active_workers();
  // int top = (int)(active_workers) + 1;
  int top = shm_client_marker_num + 1;
  for (uint i = 0; i < active_workers; i++) {
    // memcpy(root_addr + top, oop_vecs->at(i).data(), oop_vecs->at(i).size() * sizeof(unsigned long long));
    // top += oop_vecs->at(i).size();
    // root_addr[top] = (unsigned long long)(-1);
    // queue_indexes.push_back(top);
    // top++;
    int host_split = host_splits[i];
    auto root_num_per_split = oop_vecs->at(i).size() / host_split;
    for (int j = 0; j < host_split; j++) {
      auto copy_num = root_num_per_split;
      if (j == host_split - 1) {
        copy_num = oop_vecs->at(i).size() - root_num_per_split * j;
      }
      memcpy(root_addr + top, oop_vecs->at(i).data() + root_num_per_split * j, copy_num * sizeof(unsigned long long));
      top += copy_num;
      root_addr[top] = (unsigned long long) (-1);
      queue_indexes.push_back(top);
      top += 1;
    }
  }
  // root_addr[0] = (unsigned long long)(active_workers + 1);
  // for (uint i = 0; i < active_workers; i++) {
  root_addr[0] = (unsigned long long)(shm_client_marker_num + 1);
  for (int i = 0; i < shm_client_marker_num; i++) {
    root_addr[i + 1] = (unsigned long long)queue_indexes[i];
  }
  // then send rpc to snic client.
  char *buffer = new char[1024];
  unsigned long long *message = (unsigned long long *)(buffer + 2 * sizeof(int));
  message[0] = (unsigned long long)root_addr;
  message[1] = (unsigned long long)total_root_count * sizeof(unsigned long long);
  message[2] = (unsigned long long)roots_file_size;
  message[3] = (unsigned long long)GCId::current();
  Universe::send_rpc(rpcType, 2 * sizeof(int) + 4 * sizeof(unsigned long long), (void *)buffer);
  delete (buffer);
}

void ShenandoahConcurrentMark::snic_shm_send_satb_roots(int rpcType) {
  ShenandoahSATBMarkQueueSet &qset = ShenandoahBarrierSet::satb_mark_queue_set();
  ShenandoahFlushSATBHandshakeClosure flush_satb(qset);
  // flush all local queues into global queue set.
  Handshake::execute(&flush_satb);
  // run SnicCollectSATBRootsTask to collect all satb roots into every worker's task queue.
  uint active_worker_num = ShenandoahHeap::heap()->workers()->active_workers();
  SnicCollectSATBRootsTask task(qset, this);
  ShenandoahHeap::heap()->workers()->run_task(&task);
  // create shm space satb roots and send rpc 9 to client side.
  // snic_shm_send_roots(rpcType);
  snic_collect_satb_roots_and_send(&task._oop_vecs, rpcType);
}

// Write roots to pre-allocated SHM region (no mmap, no ftruncate, no TCP)
// Returns total bytes written
size_t ShenandoahConcurrentMark::snic_shm_write_roots_prealloc(unsigned long long* root_addr) {
  int shm_client_marker_num = ShmClientMarkerNum;
  std::vector<int> host_splits = decide_host_splits_by_shm_client_marker_num();
  auto heap = ShenandoahHeap::heap();
  int total_root_count = 0;
  std::vector<int> queue_indexes;
  for (uint i = 0; i < heap->workers()->active_workers(); i++) {
    auto que = get_queue(i);
    total_root_count += que->size() + que->overflow_stack()->size() + 1;
  }
  total_root_count += 1 + shm_client_marker_num * 2;

  ShenandoahMarkTask t;
  uint active_workers = heap->workers()->active_workers();
  int top = shm_client_marker_num + 1;
  for (uint i = 0; i < active_workers; i++) {
    int host_split = host_splits[i];
    auto que = get_queue(i);
    auto que_size = que->size() + que->overflow_stack()->size() + 1;
    int pop_count = 0;
    auto root_num_per_split = que_size / host_split;
    auto target_pop_count = root_num_per_split;
    for (int j = 0; j < host_split; ++j) {
      pop_count = 0;
      if (j == host_split - 1) {
        target_pop_count = que_size - root_num_per_split * j;
      }
      while (que->pop(t)) {
        root_addr[top] = (unsigned long long)(t.obj());
        top++;
        pop_count++;
        if ((unsigned long) pop_count == target_pop_count) break;
      }
      root_addr[top] = (unsigned long long)(-1);
      queue_indexes.push_back(top);
      top += 1;
    }
  }
  root_addr[0] = (unsigned long long)(shm_client_marker_num + 1);
  for (int i = 0; i < shm_client_marker_num; i++) {
    root_addr[i + 1] = (unsigned long long)queue_indexes[i];
  }
  return (size_t)top * sizeof(unsigned long long);
}

// Write SATB roots to pre-allocated SHM region (no mmap, no TCP)
size_t ShenandoahConcurrentMark::snic_shm_write_satb_prealloc(unsigned long long* root_addr) {
  ShenandoahSATBMarkQueueSet &qset = ShenandoahBarrierSet::satb_mark_queue_set();
  ShenandoahFlushSATBHandshakeClosure flush_satb(qset);
  Handshake::execute(&flush_satb);
  SnicCollectSATBRootsTask task(qset, this);
  ShenandoahHeap::heap()->workers()->run_task(&task);

  int shm_client_marker_num = ShmClientMarkerNum;
  std::vector<int> host_splits = decide_host_splits_by_shm_client_marker_num();
  auto heap = ShenandoahHeap::heap();
  int total_root_count = 0;
  std::vector<int> queue_indexes;
  for (uint i = 0; i < heap->workers()->active_workers(); i++) {
    total_root_count += task._oop_vecs[i].size();
  }
  total_root_count += 1 + shm_client_marker_num * 2;

  uint active_workers = heap->workers()->active_workers();
  int top = shm_client_marker_num + 1;
  for (uint i = 0; i < active_workers; i++) {
    int host_split = host_splits[i];
    auto root_num_per_split = task._oop_vecs[i].size() / host_split;
    for (int j = 0; j < host_split; j++) {
      auto copy_num = root_num_per_split;
      if (j == host_split - 1) {
        copy_num = task._oop_vecs[i].size() - root_num_per_split * j;
      }
      memcpy(root_addr + top, task._oop_vecs[i].data() + root_num_per_split * j, copy_num * sizeof(unsigned long long));
      top += copy_num;
      root_addr[top] = (unsigned long long)(-1);
      queue_indexes.push_back(top);
      top += 1;
    }
  }
  root_addr[0] = (unsigned long long)(shm_client_marker_num + 1);
  for (int i = 0; i < shm_client_marker_num; i++) {
    root_addr[i + 1] = (unsigned long long)queue_indexes[i];
  }
  return (size_t)top * sizeof(unsigned long long);
}

// Lock-free variant: collects SATB roots into SHM, stores metadata in control struct, NO TCP send
void ShenandoahConcurrentMark::snic_shm_send_satb_roots_no_tcp() {
  ShenandoahSATBMarkQueueSet &qset = ShenandoahBarrierSet::satb_mark_queue_set();
  ShenandoahFlushSATBHandshakeClosure flush_satb(qset);
  Handshake::execute(&flush_satb);
  uint active_worker_num = ShenandoahHeap::heap()->workers()->active_workers();
  SnicCollectSATBRootsTask task(qset, this);
  ShenandoahHeap::heap()->workers()->run_task(&task);

  // Write SATB roots to SHM (same as snic_collect_satb_roots_and_send but skip send_rpc)
  int shm_client_marker_num = ShmClientMarkerNum;
  std::vector<int> host_splits = decide_host_splits_by_shm_client_marker_num();
  auto heap = ShenandoahHeap::heap();
  int total_root_count = 0;
  std::vector<int> queue_indexes;
  for (uint i = 0; i < heap->workers()->active_workers(); i++) {
    total_root_count += task._oop_vecs[i].size();
  }
  total_root_count += 1 + shm_client_marker_num * 2;
  off_t roots_file_size = 0;
  struct stat shm_stat;
  fstat(Universe::get_hostShareRootFD(), &shm_stat);
  off_t cur_file_size = shm_stat.st_size;
  unsigned long long *root_addr = (unsigned long long *)Universe::hostMmap(nullptr, total_root_count * sizeof(unsigned long long), roots_file_size, Universe::get_hostShareRootFD(), 9);
  auto new_file_size = roots_file_size + total_root_count * sizeof(unsigned long long);
  auto single_page_size = os::vm_page_size();
  if (new_file_size % single_page_size != 0) {
    new_file_size = new_file_size + single_page_size - new_file_size % single_page_size;
  }
  if ((unsigned long) cur_file_size < new_file_size) {
    Universe::ftruncateFile(Universe::get_hostShareRootFD(), (size_t)(new_file_size));
  }
  uint active_workers = heap->workers()->active_workers();
  int top = shm_client_marker_num + 1;
  for (uint i = 0; i < active_workers; i++) {
    int host_split = host_splits[i];
    auto root_num_per_split = task._oop_vecs[i].size() / host_split;
    for (int j = 0; j < host_split; j++) {
      auto copy_num = root_num_per_split;
      if (j == host_split - 1) {
        copy_num = task._oop_vecs[i].size() - root_num_per_split * j;
      }
      memcpy(root_addr + top, task._oop_vecs[i].data() + root_num_per_split * j, copy_num * sizeof(unsigned long long));
      top += copy_num;
      root_addr[top] = (unsigned long long) (-1);
      queue_indexes.push_back(top);
      top += 1;
    }
  }
  root_addr[0] = (unsigned long long)(shm_client_marker_num + 1);
  for (int i = 0; i < shm_client_marker_num; i++) {
    root_addr[i + 1] = (unsigned long long)queue_indexes[i];
  }
  // Store metadata in SHM control struct (instead of TCP)
  auto ctrl = Universe::get_shmDGCControl();
  Atomic::release_store(&ctrl->satb_roots_shm_addr, (unsigned long long)root_addr);
  Atomic::release_store(&ctrl->satb_roots_size, (unsigned long long)total_root_count * sizeof(unsigned long long));
  Atomic::release_store(&ctrl->satb_roots_offset, (unsigned long long)roots_file_size);
  Atomic::release_store(&ctrl->satb_gc_id, (unsigned long long)GCId::current());
}

void ShenandoahConcurrentMark::handle_liveness_ack(size_t ack) {
  auto heap = ShenandoahHeap::heap();
  if(ShenandoahPacing){
    heap->pacer()->report_mark((size_t) (ack));
  }
}

void ShenandoahConcurrentMark::concurrent_mark() {
  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  if (SnicGCHost && !SnicGCShareMemEnabled) {
    heap->_snic_gc_roots_copy_finish_flag[0] = 0;
    Universe::set_dpuCurTimeDuringMarkInSec(os::elapsedTime());
  }
  // DGC fault handling — idle-disconnect detection.
  // If the SHM client's heartbeat is stale at the start of the cycle,
  // skip the DGC marking path and do a local Shenandoah cycle instead.
  // start_SnicGCFallback_silent() avoids the RPC-12 send_rpc that the
  // regular start_SnicGCFallback() would issue (which exit(0)s on TCP
  // failure and would take the host down with the dead client).
  if (SnicGCHost && SnicGCShareMemEnabled && SnicShmLockFreeMarking
      && SnicDGCFaultHandling
      && !Universe::during_SnicGCFallback()
      && !Universe::snic_dgc_client_alive()) {
    log_warning(gc)("DGC LOG: SHM client heartbeat stale at GC start; falling back to local Shenandoah marking for this cycle");
    Universe::start_SnicGCFallback_silent();
    heap->workers()->update_active_workers(SnicFallbackCCMT);
    ConcGCThreads = heap->workers()->active_workers();
  }
  WorkGang* workers = heap->workers();
  uint origin_nworkers = workers->active_workers();
  // if (SnicGCHost && Universe::during_SnicGCFallback() && SnicGCCoorHeuristic) {
  //   workers->update_active_workers(workers->total_workers());
  // }
  uint nworkers = workers->active_workers();
  log_debug(gc)("DGC LOG: active worker num before starting ccmark:%u", workers->active_workers());
  task_queues()->reserve(nworkers);
  Universe::start_ccmark();

  ShenandoahSATBMarkQueueSet& qset = ShenandoahBarrierSet::satb_mark_queue_set();
  ShenandoahFlushSATBHandshakeClosure flush_satb(qset);
  char *buffer = new char[MAX_RPC_BUFFER_SIZE];
  int N = (MAX_RPC_BUFFER_SIZE - sizeof(int) * 2) / sizeof(unsigned long long);
  unsigned long long *message = (unsigned long long *)(buffer + 2 * sizeof(int));
  int top = 0;
  int count = 0;
  ShenandoahMarkTask t;
  uint64_t total_liveness_from_client = 0;
  if (!SnicGCHost || Universe::during_SnicGCFallback()) {
    for (uint flushes = 0; flushes < ShenandoahMaxSATBBufferFlushes; flushes++) {
      TaskTerminator terminator(nworkers, task_queues());
      ShenandoahConcurrentMarkingTask task(this, &terminator);
      workers->run_task(&task);
      if (heap->cancelled_gc()) {
        // GC is cancelled, break out.
        break;
      }
      size_t before = qset.completed_buffers_num();
      Handshake::execute(&flush_satb);
      size_t after = qset.completed_buffers_num();
      if (before == after) {
        // No more retries needed, break out.
        break;
      }
    }
    delete (buffer);
    // if (SnicGCCoorHeuristic) {
    //   workers->update_active_workers(origin_nworkers);
    // }
  } else {
    // workers->update_active_workers(ConcGCThreads);
    if (!SnicGCShareMemEnabled) {
      bool is_prefetched = false;
      // if (SnicGCRDMAPrefetchEnabled && heap->_rdma_prefetch_finish_flag[0] == 1) {
      //   is_prefetched = true;
      //   heap->_rdma_prefetch_finish_flag[0] = 0;
      // }
      size_t total_liveness_received = 0;
      log_debug(gc)("DGC LOG: start copyRegion RPC");
      for (size_t i = ShenandoahHeap::heap()->num_regions(); i > 0; i--) {
        ShenandoahHeapRegion *r = ShenandoahHeap::heap()->get_region(i - 1);
        HeapWord *r_tams = heap->marking_context()->top_at_mark_start(r);
        if (r->bottom() != r_tams) {
          auto region_state = (int)(r->state());
          message[top * 5] = (unsigned long long)r->index();
          message[top * 5 + 1] = (unsigned long long)r->bottom();
          message[top * 5 + 2] = (unsigned long long)(ShenandoahHeap::heap()->marking_context()->top_at_mark_start(r));
          message[top * 5 + 3] = (unsigned long long)r->end();
          message[top * 5 + 4] = (unsigned long long)region_state;
          top++;
          if (top * 5 >= N) {
            log_error(gc)("Buffer not big enough!");
            exit(0);
          }
        }
      }
      message[top * 5] = (unsigned long long)(0);
      if (Universe::get_start_rdma_prefetch()) {
        Universe::set_start_rdma_prefetch(false);
        auto cur_ts = (unsigned long long)os::javaTimeMillis();
        if (heap->_prefetch_tmp_rdma_force_gc_data->forceGCTriggerTimestamp > cur_ts) {
          auto delta_time_to_wait = heap->_prefetch_tmp_rdma_force_gc_data->forceGCTriggerTimestamp - (unsigned long long) cur_ts;
          log_debug(gc)("DGC LOG: delta_time_to_wait=%llu", delta_time_to_wait);
          message[top * 5 + 1] = (unsigned long long)delta_time_to_wait;
        } else {
          message[top * 5 + 1] = (unsigned long long)0;
        }
        heap->reset_prefetch_tmp_rdma_force_gc_data();
      } else {
        message[top * 5 + 1] = (unsigned long long)0;
      }
      // SnicGCRDMABatchFetchKlass — before the first RPC of the DGC cycle,
      // push the current CCS high-water mark to the client so it can do one
      // bulk RDMA READ of everything the host has allocated since the last
      // sync (on a freshly-recreated QP, to sidestep the mlx5 fw 32.42.1000
      // LOC_QP_OP_ERR on back-to-back reads). Client must ack before we
      // proceed; RPC 12 is synchronous.
      //
      // A lambda is used because we emit RPC 12 both before RPC 8 (copy
      // region metadata) and immediately before RPC 2 (handle root / start
      // of marking) — any new class the host defined between the two
      // needs to be pulled before the marker decodes its narrow klass.
      auto send_ccs_bulk_sync = []() {
        if (!SnicGCRDMABatchFetchKlass) return;
        // RPC 12 payload: {ccs_hwm, ccs_base}. The client uses
        // ccs_base to look up the CCS VirtualSpaceNode (so the bulk RDMA
        // read targets the right MR) and ccs_hwm to bound the read range.
        //
        // The base must come from VirtualSpaceList::vslist_class()->
        // base_of_first_node() — i.e. the actual reservation base of the
        // class space — not from CompressedKlassPointers::base(), which
        // is the narrow-klass encoding base and on x86 coincides with the
        // CDS archive base. Using the encoding base would target the CDS
        // VSN's MR instead of the CCS VSN's, producing mlx5 wc.status=10
        // (remote access error) at the client.
        MetaWord* ccs_base = metaspace::VirtualSpaceList::vslist_class() != NULL
                             ? metaspace::VirtualSpaceList::vslist_class()->base_of_first_node()
                             : NULL;
        char sync_buffer[2 * sizeof(int) + 2 * sizeof(unsigned long long)];
        unsigned long long* sync_msg = (unsigned long long*)(sync_buffer + 2 * sizeof(int));
        sync_msg[0] = Universe::get_ccs_hwm();
        sync_msg[1] = (unsigned long long) ccs_base;
        Universe::send_rpc(12, 2 * sizeof(int) + 2 * sizeof(unsigned long long), sync_buffer);
        Universe::recv_int_ack();
      };
      send_ccs_bulk_sync();
      // concurrent copy region logic.
      if (SnicConcCopyRegion) {
        Universe::send_rpc(8, 2 * sizeof(int) + (top * 5 + 2) * sizeof(unsigned long long), buffer);
        auto time_after_send_rpc_8 = os::javaTimeMillis();
        Universe::recv_int_ack();
        auto time_after_recv_int_ack = os::javaTimeMillis();
        auto recv_int_ack_used_time = time_after_recv_int_ack - time_after_send_rpc_8;
        if (recv_int_ack_used_time > 30) {
          log_debug(gc)("DGC LOG: client ack used time is too long for rpc 8: %ld", recv_int_ack_used_time);
        }
      } else {
        Universe::send_rpc(3, 2 * sizeof(int) + (top * 5 + 2) * sizeof(unsigned long long), buffer);
      }
      // Second sync: pulls any klasses the host defined between RPC 8 and
      // the imminent RPC 2 (marker starts processing roots on ack).
      send_ccs_bulk_sync();
      // task queue roots logic.
      top = 0;
      std::queue<int> rpc_normal_ack_queue;
      std::queue<int> rpc_final_ack_queue;
      flush_task_queues_and_send(buffer, 2, is_prefetched); // send RPC 2 here.
      auto time_after_send_rpc_2 = os::javaTimeMillis();
      wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
      auto time_after_dpu_client_ack = os::javaTimeMillis();
      auto dpu_client_ack_used_time = time_after_dpu_client_ack - time_after_send_rpc_2;
      if (dpu_client_ack_used_time > 30) {
        log_debug(gc)("DGC LOG: dpu client ack used time is too long: %ld", dpu_client_ack_used_time);
      }
      log_debug(gc)("DGC LOG: finish copyRegion RPC, satb roots split part num:%d", SnicSATBRootsSplitPartNum);
      // receive task queue roots handling finish ack here.
      wait_client_liveness_and_finish_ack(rpc_normal_ack_queue, rpc_final_ack_queue, &total_liveness_received, 2);
      // Universe::recv_int_ack();
      int i = 0;
      // always send RPC 9 when gc roots buffer is full.
      int buffer_full_rpc_type = 9;
      while(true) {
        int rpc_type = 9;
        log_debug(gc)("DGC LOG: start to get ack for %d times", i);
        int ack_type = -1;
        if (!rpc_normal_ack_queue.empty()) {
          ack_type = rpc_normal_ack_queue.front();
          rpc_normal_ack_queue.pop();
        } else if (!rpc_final_ack_queue.empty()) {
          ack_type = rpc_final_ack_queue.front();
          rpc_final_ack_queue.pop();
        } else {
          // ack_type = Universe::get_ack_type();
          ack_type = (int) (Universe::recv_int_ack());
        }
        log_debug(gc)("DGC LOG: get ack type %d", ack_type);
        if (ack_type == (int) (SATB_ROOTS_FINAL_ACK)) {
          rpc_type = 10;
        }
        // flush all items inside the satb queues into task queues.
        Handshake::execute(&flush_satb);
        // drain satb roots into task queues.
        top = 1;
        auto heap = ShenandoahHeap::heap();
        SnicSATBBufferClosure snicCl(&top, (size_t) (MAX_RPC_BUFFER_SIZE));
        ShenandoahSATBMarkQueueSet &satb_set = ShenandoahBarrierSet::satb_mark_queue_set();
        auto origin_que_num = satb_set.completed_buffers_num();
        auto satb_buf_size = satb_set.buffer_size();
        int buf_count = 0;
        int gc_roots_buffer_limit = (MAX_RPC_BUFFER_SIZE - sizeof(unsigned long long)) / sizeof(unsigned long long);
        // put version number at the top of the buffer.
        auto ull_gc_roots_buffer = (unsigned long long *)(heap->_snic_gc_roots_buffer);
        ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
        // collect all satb roots into different worker's vector concurrently.
        SnicCollectSATBRootsTask copyRegionCollectSATBRootsTask(satb_set, this);
        heap->workers()->run_task(&copyRegionCollectSATBRootsTask);
        size_t roots_to_send_count = 0;
        for (uint i = 0; i < heap->workers()->active_workers(); i++) {
          roots_to_send_count += copyRegionCollectSATBRootsTask._oop_vecs[i].size();
          roots_to_send_count += 1; // for -1 used to mark the end of a single worker's vector.
        }
        if (roots_to_send_count + 1 + DpuClientMarkerNum < (size_t) (gc_roots_buffer_limit)) {
          log_debug(gc)("DGC LOG: buffer is enough to send all satb roots, roots to send count:%lu,buffer limit:%d", roots_to_send_count, gc_roots_buffer_limit);
          top = 1;
          buf_count = 0;
          auto host_splits = decide_host_splits_by_dpu_client_num();
          for (uint j = 0; j < heap->workers()->active_workers(); j++) {
            int host_split = host_splits[j];
            auto vec_size = copyRegionCollectSATBRootsTask._oop_vecs[j].size();
            auto buf_sizes = copyRegionCollectSATBRootsTask._buf_sizes_vecs[j];
            auto buf_cnt_per_split = buf_sizes.size() / host_split;
            std::vector<size_t> root_nums_per_split;
            root_nums_per_split.push_back(0);
            // compute root num for every split.
            for (int k = 0; k < host_split; ++k) {
              auto start_idx = k * buf_cnt_per_split;
              if (k != host_split - 1) {
                auto end_idx = (k + 1) * buf_cnt_per_split;
                auto root_num = std::accumulate(buf_sizes.begin() + start_idx, buf_sizes.begin() + end_idx, 0);
                root_nums_per_split.push_back(root_num);
              }
              else{
                auto root_num = std::accumulate(buf_sizes.begin() + start_idx, buf_sizes.end(), 0);
                root_nums_per_split.push_back(root_num);
              }
            }
            // store all roots into buffer.
            int cur_root_num = 0;
            for (int k = 0; k < host_split; ++k) {
              memcpy(ull_gc_roots_buffer + top, copyRegionCollectSATBRootsTask._oop_vecs[j].data() + cur_root_num, root_nums_per_split[k + 1] * sizeof(unsigned long long));
              cur_root_num += root_nums_per_split[k + 1];
              top += root_nums_per_split[k + 1];
              ull_gc_roots_buffer[top] = (unsigned long long)(-1);
              top++;
              buf_count++;
            }
            log_debug(gc)("DGC LOG:worker %u top:%d,buf_count:%d,real root num:%d,total root num:%lu", j, top, buf_count, cur_root_num, vec_size);
            // auto root_num_per_split = vec_size / host_split;
            // for (int k = 0; k < host_split; k++) {
            //   if (k != host_split - 1) {
            //     memcpy(ull_gc_roots_buffer + top, copyRegionCollectSATBRootsTask._oop_vecs[j].data() + root_num_per_split * k, root_num_per_split * sizeof(unsigned long long));
            //     top += root_num_per_split;
            //     ull_gc_roots_buffer[top] = (unsigned long long)(-1);
            //     top++;
            //   }
            //   else{
            //     memcpy(ull_gc_roots_buffer + top, copyRegionCollectSATBRootsTask._oop_vecs[j].data() + root_num_per_split * k, vec_size * sizeof(unsigned long long));
            //     top += vec_size;
            //     ull_gc_roots_buffer[top] = (unsigned long long)(-1);
            //     top++;
            //   }
            //   buf_count++;
            //   vec_size -= root_num_per_split;
            // }
          }
          ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
          message[0] = (unsigned long long) (top);
          message[1] = 0ULL;
          Universe::send_rpc(rpc_type, 2 * sizeof(int) + 2 * sizeof(unsigned long long), (void *)buffer);
          log_debug(gc)("DGC LOG: success send rpc %d", rpc_type);
          wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
          log_debug(gc)("DGC LOG: finish send RPC %d for %d times,size = %d,origin queue num=%lu,counted queue num=%d", rpc_type, i, top, origin_que_num, buf_count);
          if (DpuClientLivenessUpdateEnabled) {
            wait_client_liveness_and_finish_ack(rpc_normal_ack_queue, rpc_final_ack_queue, &total_liveness_received, rpc_type);
          }
        } else {
          // collected root counts > ull buffer size, send these roots for multiple of times.
          uint sent_index = 0;
          top = 1;
          buf_count = 0;
          while (sent_index < heap->workers()->active_workers()) {
            if (top + copyRegionCollectSATBRootsTask._oop_vecs[sent_index].size() + 1 < (size_t)(gc_roots_buffer_limit)) {
              memcpy(ull_gc_roots_buffer + top, copyRegionCollectSATBRootsTask._oop_vecs[sent_index].data(), copyRegionCollectSATBRootsTask._oop_vecs[sent_index].size() * sizeof(unsigned long long));
              top += copyRegionCollectSATBRootsTask._oop_vecs[sent_index].size();
              ull_gc_roots_buffer[top] = (unsigned long long)(-1);
              top++;
              buf_count++;
              sent_index++;
            } else {
              // send rpc here.
              ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
              message[0] = (unsigned long long) (top);
              message[1] = 0ULL;
              Universe::send_rpc(buffer_full_rpc_type, 2 * sizeof(int) + 2 * sizeof(unsigned long long), (void *)buffer);
              wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
              log_debug(gc)("DGC LOG: finish send RPC %d for %d times when buffer is full,size = %d,origin queue num=%lu,counted queue num=%d", buffer_full_rpc_type, i, top, origin_que_num, buf_count);
              // wait_client_liveness_and_finish_ack(rpc_normal_ack_queue, rpc_final_ack_queue, &total_liveness_received, buffer_full_rpc_type);
              top = 1;
              buf_count = 0;
              i++;
              ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
            }
          }
          if (buf_count > 0) {
            // send the remaining roots.
            ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
            message[0] = (unsigned long long) (top);
            message[1] = 0ULL;
            Universe::send_rpc(rpc_type, 2 * sizeof(int) + 2 * sizeof(unsigned long long), (void *)buffer);
            wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
            log_debug(gc)("DGC LOG: finish send RPC %d for %d times,size = %d,origin queue num=%lu,counted queue num=%d", rpc_type, i, top, origin_que_num, buf_count);
            // wait_client_liveness_and_finish_ack(rpc_normal_ack_queue, rpc_final_ack_queue, &total_liveness_received, rpc_type);
          }
        }
        // while (true) {
        //   if (top + satb_buf_size + 1 >= (size_t)(gc_roots_buffer_limit)) {
        //     log_debug(gc)("DGC LOG: start to send RPC %d for %d times when buffer is full", buffer_full_rpc_type, i);
        //     message[0] = (unsigned long long) (top);
        //     // Universe::send_rpc(rpc_type, 2 * sizeof(int) + top * sizeof(unsigned long long), (void *)buffer);
        //     ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
        //     Universe::send_rpc(buffer_full_rpc_type, 2 * sizeof(int) + sizeof(unsigned long long), (void *)buffer);
        //     wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
        //     log_debug(gc)("DGC LOG: finish send RPC %d for %d times when buffer is full,size = %d,cur buf count:%d", buffer_full_rpc_type, i, top, buf_count);
        //     top = 1;
        //     i++;
        //     // update version number for the new send routine.
        //     ull_gc_roots_buffer[0] = (unsigned long long)(i + 2);
        //   }
        //   auto apply_res = satb_set.apply_closure_to_completed_buffer(&snicCl);
        //   if (!apply_res) {
        //     log_debug(gc)("DGC LOG: no more satb buffer to apply, queue count = %d, satb buffer size=%lu, N=%d", buf_count, satb_buf_size, gc_roots_buffer_limit);
        //     break;
        //   }
        //   buf_count++;
        // }
        // log_debug(gc)("DGC LOG: start to send RPC %d for %d times", rpc_type, i);
        // message[0] = (unsigned long long) (top);
        // Universe::send_rpc(rpc_type, 2 * sizeof(int) + sizeof(unsigned long long), (void *)buffer);
        // wait_client_copy_gc_roots_finish_with_ack(rpc_normal_ack_queue, rpc_final_ack_queue);
        // log_debug(gc)("DGC LOG: finish send RPC %d for %d times,size = %d,origin queue num=%lu,counted queue num=%d", rpc_type, i, top, origin_que_num, buf_count);
        if(rpc_type == 10){
          break;
        }
        else{
          i++;
        }
      }
      // free buffer.
      delete (buffer);
      if (!DpuClientLivenessUpdateEnabled) {
        // wait for last part copy ack.
        Universe::recv_int_ack();
      }
      uint64_t final_total_liveness = 0;
      for (size_t region_idx = 0; region_idx < heap->num_regions(); ++region_idx) {
        ShenandoahHeapRegion *region = heap->get_region(region_idx);
        uint64_t remote_liveness = heap->_remote_liveness_cache[(int)(region_idx)];
        total_liveness_from_client += remote_liveness;
        region->internal_increase_live_data(remote_liveness);
        final_total_liveness += remote_liveness;
      }
      final_total_liveness -= total_liveness_received;
      if (ShenandoahPacing) {
        heap->pacer()->report_mark(final_total_liveness);
      }
      log_debug(gc)("DGC LOG: SATB roots handling is finished");
    } else {
      // log_info(gc)("get into SnicGCShareMemEnabled = true case");
      // // empty the share mem roots file.
      // auto trunc_res_0 = ftruncate(Universe::get_hostShareRootFD(), (off_t)(0));
      // if (trunc_res_0 != 0) {
      //   log_error(gc)("DGC LOG: truncate share mem roots file failed, %d", trunc_res_0);
      //   exit(0);
      // }
      uint64_t total_liveness_received = 0;
      log_debug(gc)("DGC LOG: truncate share roots file to zero size successfully");
      for (uint i = 0; i < heap->num_regions(); i++) {
        heap->_regions_tams_info[i] = (unsigned long long) (heap->marking_context()->top_at_mark_start(heap->get_region(i)));
      }

      if (SnicShmLockFreeMarking && Universe::get_shmDGCControl() != nullptr && Universe::preallocRootsAddr != nullptr) {
        // ========== Lock-free SHM: inline SATB streaming ==========
        // Reset per-cycle SATB counters.
        Atomic::store(&g_satb_streamed_to_client, (uint64_t)0);
        Atomic::store(&g_satb_drained_by_host,   (uint64_t)0);

        auto ctrl = Universe::get_shmDGCControl();
        unsigned long long seqno = (unsigned long long) GCId::current() + 1;
        auto prealloc = Universe::preallocRootsAddr;

        // 1. Write roots to pre-alloc region (unchanged)
        size_t roots_bytes = snic_shm_write_roots_prealloc(prealloc);
        Atomic::release_store(&ctrl->roots_shm_addr, (unsigned long long)prealloc);
        Atomic::release_store(&ctrl->roots_size, (unsigned long long)roots_bytes);
        Atomic::release_store(&ctrl->roots_gc_id, (unsigned long long)GCId::current());

        if (seqno == 1) {
          // First cycle: TCP RPC 5 bootstrap + old two-phase protocol
          char* rpc_buf = new char[1024];
          unsigned long long* msg = (unsigned long long*)(rpc_buf + 2 * sizeof(int));
          msg[0] = (unsigned long long)prealloc;
          msg[1] = (unsigned long long)roots_bytes;
          msg[2] = 0ULL;
          msg[3] = (unsigned long long)GCId::current();
          Universe::send_rpc(5, 2 * sizeof(int) + 4 * sizeof(unsigned long long), rpc_buf);
          delete rpc_buf;
          Atomic::release_store(&ctrl->marking_seqno, seqno);
          // Old protocol: wait marking_done → batch SATB → wait satb_done.
          // Timed wait + cancelled_gc() check so an OOM-driven alloc
          // failure (which calls cancel_gc) can interrupt the wait and
          // route the cycle into Degenerated GC even if the SHM client
          // is hung mid-marking.
          while (Atomic::load_acquire(&ctrl->marking_done_seqno) < seqno) {
            if (SnicDGCFaultHandling && heap->cancelled_gc()) {
              log_warning(gc)("DGC LOG: cancelled_gc() during cycle-1 marking_done wait (seqno=%llu); aborting DGC wait so degen path can run", seqno);
              break;
            }
            futex_wait_timed(&ctrl->marking_done_seqno, Atomic::load(&ctrl->marking_done_seqno), SnicDGCWaitSliceUs);
          }
          auto satb_dest = (unsigned long long*)((char*)prealloc + Universe::PREALLOC_SATB_OFFSET);
          size_t satb_bytes = snic_shm_write_satb_prealloc(satb_dest);
          Atomic::release_store(&ctrl->satb_roots_shm_addr, (unsigned long long)satb_dest);
          Atomic::release_store(&ctrl->satb_roots_size, (unsigned long long)satb_bytes);
          Atomic::release_store(&ctrl->satb_gc_id, (unsigned long long)GCId::current());
          Atomic::release_store(&ctrl->satb_seqno, seqno);
          while (Atomic::load_acquire(&ctrl->satb_done_seqno) < seqno) {
            if (SnicDGCFaultHandling && heap->cancelled_gc()) {
              log_warning(gc)("DGC LOG: cancelled_gc() during cycle-1 satb_done wait (seqno=%llu); aborting DGC wait", seqno);
              break;
            }
            futex_wait_timed(&ctrl->satb_done_seqno, Atomic::load(&ctrl->satb_done_seqno), SnicDGCWaitSliceUs);
          }
          log_debug(gc)("DGC LOG: lock-free cycle-1 two-phase done (seqno=%llu, roots=%luB, satb=%luB)", seqno, roots_bytes, satb_bytes);
        } else {
          // Cycle 2+: inline SATB streaming
          // 2. Reset streaming state
          Atomic::release_store(&ctrl->satb_stream_write_idx, (unsigned long long)0);
          Atomic::release_store(&ctrl->satb_flush_complete, (unsigned long long)0);
          // 3. Signal client to start marking
          Atomic::release_store(&ctrl->marking_seqno, seqno);

          // 4. Stream completed SATB buffers to SHM while client marks
          ShenandoahSATBMarkQueueSet& stream_qset = ShenandoahBarrierSet::satb_mark_queue_set();
          auto satb_dest = (unsigned long long*)((char*)prealloc + Universe::PREALLOC_SATB_OFFSET);
          unsigned long long satb_capacity = Universe::PREALLOC_ROOTS_SIZE / sizeof(unsigned long long);
          unsigned long long total_written = 0;
          // Reset overflow counter each cycle.
          Atomic::store(&g_satb_overflow_to_host, (uint64_t)0);
          // Host-local overflow queue: worker 0's task queue. final_mark_work's
          // mark_loop will drain it transitively.
          ShenandoahObjToScanQueue* overflow_q = get_queue(0);
          SATBToShmClosure satb_closure(satb_dest, total_written, satb_capacity,
                                        overflow_q, heap->marking_context());

          while (Atomic::load_acquire(&ctrl->marking_done_seqno) < seqno) {
            while (stream_qset.completed_buffers_num() > 0) {
              stream_qset.apply_closure_to_completed_buffer(&satb_closure);
            }
            Atomic::release_store(&ctrl->satb_stream_write_idx, total_written);
            if (SnicDGCFaultHandling && heap->cancelled_gc()) {
              log_warning(gc)("DGC LOG: cancelled_gc() during inline-SATB streaming (seqno=%llu); aborting DGC wait", seqno);
              break;
            }
            futex_wait_timed(&ctrl->marking_done_seqno, Atomic::load(&ctrl->marking_done_seqno), 500);
          }

          // 5. Final flush: Handshake forces thread-local SATB queue drain
          ShenandoahFlushSATBHandshakeClosure flush_satb(stream_qset);
          Handshake::execute(&flush_satb);
          while (stream_qset.completed_buffers_num() > 0) {
            stream_qset.apply_closure_to_completed_buffer(&satb_closure);
          }
          Atomic::release_store(&ctrl->satb_stream_write_idx, total_written);
          Atomic::release_store(&ctrl->satb_flush_complete, (unsigned long long)1);

          // 6. Wait for client to finish all marking (including final SATB).
          // Timed + cancellable so OOM alloc-failure can interrupt a
          // hung client and let Degenerated GC run.
          while (Atomic::load_acquire(&ctrl->satb_done_seqno) < seqno) {
            if (SnicDGCFaultHandling && heap->cancelled_gc()) {
              log_warning(gc)("DGC LOG: cancelled_gc() during inline-SATB satb_done wait (seqno=%llu); aborting DGC wait so degen path can run", seqno);
              break;
            }
            futex_wait_timed(&ctrl->satb_done_seqno, Atomic::load(&ctrl->satb_done_seqno), SnicDGCWaitSliceUs);
          }
          log_debug(gc)("DGC LOG: lock-free inline-SATB done (seqno=%llu, roots=%luB, satb_oops=%llu)", seqno, roots_bytes, total_written);
        }

        // 7. Collect liveness from mmap'd cache (common path)
        uint64_t final_liveness = 0;
        for (size_t region_idx = 0; region_idx < heap->num_regions(); ++region_idx) {
          ShenandoahHeapRegion *region = heap->get_region(region_idx);
          uint64_t remote_liveness = heap->_remote_liveness_cache[(int)(region_idx)];
          final_liveness += remote_liveness;
          total_liveness_from_client += remote_liveness;
          region->internal_increase_live_data(remote_liveness);
        }
        if (ShenandoahPacing) {
          heap->pacer()->report_mark(final_liveness);
        }
        delete (buffer);
      } else {
        // ========== Original TCP-based SHM marking path ==========
        // send task queue roots using shm rpc 5.
        snic_shm_send_roots(5);
        auto start_time = os::javaTimeMillis();
        Universe::recv_int_ack();
        auto end_time = os::javaTimeMillis();
        log_info(gc)("Global Pacer: client response time: %ld ms", end_time - start_time);
        if(end_time - start_time > 30){
          log_warning(gc)("Global Pacer: client response time is too long!!");
        }

        log_debug(gc)("DGC LOG: send rpc 5 to share mem client");
        while (true) {
          size_t ack = Universe::recv_int_ack();
          if (ack == 1) {
            break;
          } else {
            total_liveness_received += ack;
            if(ShenandoahPacing){
              heap->pacer()->report_mark(ack);
            }
          }
        }

        snic_shm_send_satb_roots(9);
        log_debug(gc)("DGC LOG: send rpc 9 to share mem client");
        top = 0;
        while (true) {
          size_t ack = Universe::recv_int_ack();
          if (ack == 1) {
            break;
          }
          else {
            total_liveness_received += ack;
            if(ShenandoahPacing){
              heap->pacer()->report_mark(ack);
            }
          }
        }

        log_debug(gc)("DGC LOG: finish satb roots handling");
        uint64_t final_liveness = 0;
        for (size_t region_idx = 0; region_idx < heap->num_regions(); ++region_idx) {
          ShenandoahHeapRegion *region = heap->get_region(region_idx);
          uint64_t remote_liveness = heap->_remote_liveness_cache[(int)(region_idx)];
          final_liveness += remote_liveness;
          total_liveness_from_client += remote_liveness;
          region->internal_increase_live_data(remote_liveness);
        }
        final_liveness -= total_liveness_received;
        if(ShenandoahPacing){
          heap->pacer()->report_mark(final_liveness);
        }
        log_debug(gc)("DGC LOG: finish local liveness cache update");
        delete (buffer);
      }
    }
  }

  Universe::finish_SnicGCFallback();
  // print liveness data here
  uint64_t total_liveness = 0;
  for(size_t i=0; i< heap->num_regions(); i++){
    auto region = heap->get_region(i);
    total_liveness += heap->get_region(i)->get_live_data_bytes();
  }
  log_debug(gc)("DGC LOG: get total live data %lu bytes during ccmark", total_liveness);
  if(SnicGCCoorHeuristic){
    // Universe::record_liveness_and_report(total_liveness);
    Universe::record_liveness_and_report(total_liveness_from_client * HeapWordSize);
  }
  // auto bitmap_start = heap->marking_context()->_mark_bit_map._map;
  // unsigned long total_bitmap_val = 0;
  // for (size_t i = 0; i < heap->marking_context()->_mark_bit_map._size / 8; i++) {
  //   total_bitmap_val += bitmap_start[i];
  // }
  // log_info(gc)("host bitmap checksum %lu", total_bitmap_val);

  Universe::finish_ccmark();
  // if (SnicGCHost && !SnicGCShareMemEnabled) {
  //   workers->update_active_workers(origin_nworkers);
  // }
  assert(task_queues()->is_empty() || heap->cancelled_gc(), "Should be empty when not cancelled");
}

void ShenandoahConcurrentMark::snic_final_mark() {
  assert(ShenandoahSafepoint::is_at_shenandoah_safepoint(), "Must be at a safepoint");
  assert(Thread::current()->is_VM_thread(), "Must by VM Thread");
  snic_final_mark_work();
  // close SATB Barrier here.
  ShenandoahHeap::heap()->set_concurrent_mark_in_progress(false);
}

void ShenandoahConcurrentMark::finish_mark() {
  assert(ShenandoahSafepoint::is_at_shenandoah_safepoint(), "Must be at a safepoint");
  assert(Thread::current()->is_VM_thread(), "Must by VM Thread");
  // // TEST ONLY: check bitmap to compute sum of marked oop size, and compare to live count sum.
  // auto complete_ctx = ShenandoahHeap::heap()->marking_context();
  // for (size_t region_idx = 0; region_idx < ShenandoahHeap::heap()->num_regions(); region_idx++) {
  //   ShenandoahHeapRegion *region = ShenandoahHeap::heap()->get_region(region_idx);
  //   if (region->bottom() == ShenandoahHeap::heap()->marking_context()->top_at_mark_start(region)) {
  //     continue;
  //   }
  //   if (region->is_humongous()) {
  //     continue;
  //   }
  //   if (region->end() != ShenandoahHeap::heap()->marking_context()->top_at_mark_start(region)) {
  //     continue;
  //   }
  //   auto region_liveness = region->get_live_data_words();
  //   int region_marked_size = 0;
  //   HeapWord *old_marked_addr = NULL;
  //   if (!region->is_humongous() && !region->is_trash()) {
  //     // HeapWord *tams = complete_ctx->top_at_mark_start(region);
  //     HeapWord *tams = region->end();
  //     if (tams > region->bottom()) {
  //       HeapWord *start = region->bottom();
  //       HeapWord *addr = ShenandoahHeap::heap()->marking_context()->get_next_marked_addr(start, tams);
  //       old_marked_addr = addr;
  //       while (addr < tams) {
  //         oop obj = cast_to_oop(addr);
  //         int size = obj->size();
  //         region_marked_size += size;
  //         addr += 1;
  //         if (addr < tams) {
  //           addr = ShenandoahHeap::heap()->marking_context()->get_next_marked_addr(addr, tams);
  //           if (old_marked_addr + size > addr) {
  //             log_debug(gc)("DGC LOG:final mark overlap in %p and %p, size:%d", old_marked_addr, addr, size);
  //             exit(0);
  //           }
  //           old_marked_addr = addr;
  //         }
  //       }
  //     }
  //   }
  //   if (region_liveness != (uint64_t)(region_marked_size)) {
  //     log_debug(gc)("DGC LOG:region %lu (state %d) marked size %d != local_liveness_size %lu", region_idx, region->state_ordinal(),
  //                  region_marked_size, region_liveness);
  //   }
  // }
  finish_mark_work();
  assert(task_queues()->is_empty(), "Should be empty");
  TASKQUEUE_STATS_ONLY(task_queues()->print_taskqueue_stats());
  TASKQUEUE_STATS_ONLY(task_queues()->reset_taskqueue_stats());

  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  heap->set_concurrent_mark_in_progress(false);
  heap->mark_complete_marking_context();
}

void ShenandoahConcurrentMark::snic_final_mark_work() {
  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  char *buffer = new char[MAX_RPC_BUFFER_SIZE];
  int N = (MAX_RPC_BUFFER_SIZE - sizeof(int) * 2) / sizeof(unsigned long long);
  unsigned long long *message = (unsigned long long *)(buffer + 2 * sizeof(int));
  int top = 0;
  int count = 0;
  ShenandoahMarkTask t;
  ShenandoahSATBMarkQueueSet& qset = ShenandoahBarrierSet::satb_mark_queue_set();
  ShenandoahFlushSATBHandshakeClosure flush_satb(qset);
  // copy region RPC logic.
  log_debug(gc)("DGC LOG: start copyRegion RPC");
  for (size_t i = ShenandoahHeap::heap()->num_regions(); i > 0; i--) {
    ShenandoahHeapRegion *r = ShenandoahHeap::heap()->get_region(i - 1);
    HeapWord *r_tams = heap->marking_context()->top_at_mark_start(r);
    if (r->bottom() != r_tams) {
      message[top * 4] = (unsigned long long)r->index();
      message[top * 4 + 1] = (unsigned long long)r->bottom();
      message[top * 4 + 2] = (unsigned long long)(ShenandoahHeap::heap()->marking_context()->top_at_mark_start(r));
      message[top * 4 + 3] = (unsigned long long)r->end();
      top++;
      if (top * 4 >= N) {
        log_error(gc)("Buffer not big enough!");
        exit(0);
      }
    }
  }
  Universe::send_rpc(3, 2 * sizeof(int) + top * 4 * sizeof(unsigned long long), buffer);
  top = 0;
  log_debug(gc)("DGC LOG: finish copyRegion RPC");
  log_debug(gc)("DGC LOG: start send RPC 2");
  // task queue roots logic.
  flush_task_queues_and_send(buffer, 2, false); // send RPC 2 here.
  // recv ack
  Universe::recv_ack();
  log_debug(gc)("DGC LOG: finish send RPC 2");
  // flush all items inside the satb queues into task queues.
  Handshake::execute(&flush_satb);
  // send all satb roots to snic client using RPC 7.
  log_debug(gc)("DGC LOG: start send RPC 7");
  top = 0;
  SnicSATBBufferClosure snicCl(&top, (size_t) (MAX_RPC_BUFFER_SIZE));
  ShenandoahSATBMarkQueueSet &satb_set = ShenandoahBarrierSet::satb_mark_queue_set();
  while (satb_set.apply_closure_to_completed_buffer(&snicCl)) {}
  Universe::send_rpc(7, 2 * sizeof(int) + top * sizeof(unsigned long long), (void *)buffer);
  Universe::recv_ack();
  log_debug(gc)("DGC LOG: finish send RPC 7");
  // free buffer.
  delete (buffer);
  // update local liveness data here.
  for (size_t region_idx = 0; region_idx < heap->num_regions(); ++region_idx) {
    ShenandoahHeapRegion *region = heap->get_region(region_idx);
    uint64_t remote_liveness = heap->_remote_liveness_cache[(int)(region_idx)];
    region->increase_live_data_gc_words(remote_liveness);
  }
}

void ShenandoahConcurrentMark::finish_mark_work() {
  // Finally mark everything else we've got in our queues during the previous steps.
  // It does two different things for concurrent vs. mark-compact GC:
  // - For concurrent GC, it starts with empty task queues, drains the remaining
  //   SATB buffers, and then completes the marking closure.
  // - For mark-compact GC, it starts out with the task queues seeded by initial
  //   root scan, and completes the closure, thus marking through all live objects
  // The implementation is the same, so it's shared here.
  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  ShenandoahGCPhase phase(ShenandoahPhaseTimings::finish_mark);
  uint origin_workers = heap->workers()->active_workers();
  // heap->workers()->update_active_workers(1);
  uint nworkers = heap->workers()->active_workers();
  task_queues()->reserve(nworkers);

  StrongRootsScope scope(nworkers);
  TaskTerminator terminator(nworkers, task_queues());
  // all queues should be empty before final mark processes SATB queues.
  uint que_size = task_queues()->size();
  log_debug(gc)("DGC LOG:queue num before final mark SATB:%u", que_size);
  for (uint i = 0; i < que_size; ++i) {
    auto que = get_queue(i);
    if (que->size() > 0) {
      log_debug(gc)("DGC LOG:que %u not empty before SATB", i);
    }
  }
  ShenandoahFinalMarkingTask task(this, &terminator, ShenandoahStringDedup::is_enabled());
  heap->workers()->run_task(&task);

  // DIAG: residual state check at end of finish_mark_work.
  // By this point every SATB entry that existed at any time during this cycle
  // must have been consumed either by the client (in concurrent phase via
  // lock-free SHM streaming) or by the host's local mark_loop above. Nothing
  // should remain in the global completed SATB set, and host task queues
  // must be empty.
  if (SnicGCHost) {
    SATBMarkQueueSet& qset = ShenandoahBarrierSet::satb_mark_queue_set();
    size_t leftover_completed = qset.completed_buffers_num();
    size_t leftover_tasks = 0;
    for (uint i = 0; i < task_queues()->size(); i++) {
      auto que = get_queue(i);
      leftover_tasks += que->size();
    }
    uint64_t streamed = Atomic::load(&g_satb_streamed_to_client);
    uint64_t drained  = Atomic::load(&g_satb_drained_by_host);
    uint64_t overflow = Atomic::load(&g_satb_overflow_to_host);
    if (leftover_completed > 0 || leftover_tasks > 0) {
      log_error(gc)("DGC LOG: finish_mark_work RESIDUAL STATE — completed_satb_buffers=%zu host_task_queues_total=%zu streamed=%llu drained=%llu overflow=%llu",
                    leftover_completed, leftover_tasks,
                    (unsigned long long)streamed, (unsigned long long)drained,
                    (unsigned long long)overflow);
    } else {
      log_debug(gc)("DGC LOG: finish_mark_work clean: streamed=%llu drained=%llu overflow=%llu",
                   (unsigned long long)streamed, (unsigned long long)drained,
                   (unsigned long long)overflow);
    }
  }

  assert(task_queues()->is_empty(), "Should be empty");
}


void ShenandoahConcurrentMark::cancel() {
  clear();
  ShenandoahReferenceProcessor* rp = ShenandoahHeap::heap()->ref_processor();
  rp->abandon_partial_discovery();
}
