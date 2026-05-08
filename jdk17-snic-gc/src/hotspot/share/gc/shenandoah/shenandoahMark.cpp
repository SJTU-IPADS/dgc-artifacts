/*
 * Copyright (c) 2021, Red Hat, Inc. All rights reserved.
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

#include "gc/shenandoah/shenandoahBarrierSet.hpp"
#include "gc/shenandoah/shenandoahClosures.inline.hpp"
#include "gc/shenandoah/shenandoahMark.inline.hpp"
#include "gc/shenandoah/shenandoahOopClosures.inline.hpp"
#include "gc/shenandoah/shenandoahReferenceProcessor.hpp"
#include "gc/shenandoah/shenandoahTaskqueue.inline.hpp"
#include "gc/shenandoah/shenandoahUtils.hpp"
#include "gc/shenandoah/shenandoahVerifier.hpp"
#include "memory/iterator.hpp"
#include "memory/universe.hpp"

#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#define MAX_ULL 18446744073709551615ULL


ShenandoahMarkRefsSuperClosure::ShenandoahMarkRefsSuperClosure(ShenandoahObjToScanQueue* q,  ShenandoahReferenceProcessor* rp) :
  MetadataVisitingOopIterateClosure(rp),
  _stringDedup_requests(),
  _queue(q),
  _mark_context(ShenandoahHeap::heap()->marking_context()),
  _weak(false)
{ }

ShenandoahMark::ShenandoahMark() :
  _task_queues(ShenandoahHeap::heap()->marking_context()->task_queues()) {
}

void ShenandoahMark::clear() {
  // Clean up marking stacks.
  ShenandoahObjToScanQueueSet* queues = ShenandoahHeap::heap()->marking_context()->task_queues();
  queues->clear();

  // Cancel SATB buffers.
  ShenandoahBarrierSet::satb_mark_queue_set().abandon_partial_marking();
}

template <bool CANCELLABLE, StringDedupMode STRING_DEDUP>
void ShenandoahMark::mark_loop_prework(uint w, TaskTerminator *t, ShenandoahReferenceProcessor *rp) {
  ShenandoahObjToScanQueue* q = get_queue(w);

  ShenandoahHeap* const heap = ShenandoahHeap::heap();
  ShenandoahLiveData* ld = heap->get_liveness_cache(w);

  // TODO: We can clean up this if we figure out how to do templated oop closures that
  // play nice with specialized_oop_iterators.
  //log_dev_debug(gc)("DGC LOG: heap->unload_classes(): %d , heap->has_forwarded_objects(): %d ", int(heap->unload_classes()) , heap->has_forwarded_objects());
  if (heap->unload_classes()) {
    if (heap->has_forwarded_objects()) {
      using Closure = ShenandoahMarkUpdateRefsMetadataClosure<STRING_DEDUP>;
      Closure cl(q, rp);
      mark_loop_work<Closure, CANCELLABLE>(&cl, ld, w, t);
    } else {
      using Closure = ShenandoahMarkRefsMetadataClosure<STRING_DEDUP>;
      Closure cl(q, rp);
      mark_loop_work<Closure, CANCELLABLE>(&cl, ld, w, t);
    }
  } else {
    if (heap->has_forwarded_objects()) {
      using Closure = ShenandoahMarkUpdateRefsClosure<STRING_DEDUP>;
      Closure cl(q, rp);
      mark_loop_work<Closure, CANCELLABLE>(&cl, ld, w, t);
    } else {
      using Closure = ShenandoahMarkRefsClosure<STRING_DEDUP>;
      Closure cl(q, rp);
      mark_loop_work<Closure, CANCELLABLE>(&cl, ld, w, t);
    }
  }

  heap->flush_liveness_cache(w);
}

void ShenandoahMark::mark_loop(uint worker_id, TaskTerminator* terminator, ShenandoahReferenceProcessor *rp,
               bool cancellable, StringDedupMode dedup_mode) {
  if (cancellable) {
    switch(dedup_mode) {
      case NO_DEDUP:
        mark_loop_prework<true, NO_DEDUP>(worker_id, terminator, rp);
        break;
      case ENQUEUE_DEDUP:
        mark_loop_prework<true, ENQUEUE_DEDUP>(worker_id, terminator, rp);
        break;
      case ALWAYS_DEDUP:
        mark_loop_prework<true, ALWAYS_DEDUP>(worker_id, terminator, rp);
        break;
    }
  } else {
    switch(dedup_mode) {
      case NO_DEDUP:
        mark_loop_prework<false, NO_DEDUP>(worker_id, terminator, rp);
        break;
      case ENQUEUE_DEDUP:
        mark_loop_prework<false, ENQUEUE_DEDUP>(worker_id, terminator, rp);
        break;
      case ALWAYS_DEDUP:
        mark_loop_prework<false, ALWAYS_DEDUP>(worker_id, terminator, rp);
        break;
    }
  }
}

template <class T, bool CANCELLABLE>
void ShenandoahMark::mark_loop_work(T* cl, ShenandoahLiveData* live_data, uint worker_id, TaskTerminator *terminator) {
  if(SnicGCHost){
    log_debug(gc)("DGC LOG: enter mark_loop_work");
  }
  
  uintx stride = ShenandoahMarkLoopStride;

  ShenandoahHeap* heap = ShenandoahHeap::heap();
  /*start @lyh*/
  unsigned long long heap_start_addr = (unsigned long long) (heap->reserved_region().start());
  auto first_heap_region = heap->get_region(0);
  unsigned long long one_region_size = (unsigned long long) ((first_heap_region->end() - first_heap_region->bottom()) * HeapWordSize);
  /*end @lyh*/
  ShenandoahObjToScanQueueSet* queues = task_queues();
  ShenandoahObjToScanQueue* q;
  ShenandoahMarkTask t;

  heap->ref_processor()->set_mark_closure(worker_id, cl);

  /*
   * Process outstanding queues, if any.
   *
   * There can be more queues than workers. To deal with the imbalance, we claim
   * extra queues first. Since marking can push new tasks into the queue associated
   * with this worker id, we come back to process this queue in the normal loop.
   */
  assert(queues->get_reserved() == heap->workers()->active_workers(),
         "Need to reserve proper number of queues: reserved: %u, active: %u", queues->get_reserved(), heap->workers()->active_workers());

  q = queues->claim_next();
  while (q != NULL) {
    if (CANCELLABLE && heap->check_cancelled_gc_and_yield()) {
      return;
    }
    // log_debug(gc)("DGC LOG: gc outstanding queue init size: %d", q->size());
    for (uint i = 0; i < stride; i++) {
      if (q->pop(t)) {
        do_task<T>(q, cl, live_data, &t);
      } else {
        assert(q->is_empty(), "Must be empty");
        q = queues->claim_next();
        break;
      }
    }
  }
  q = get_queue(worker_id);

  ShenandoahSATBBufferClosure drain_satb(q);
  SATBMarkQueueSet& satb_mq_set = ShenandoahBarrierSet::satb_mark_queue_set();

  /*
   * Normal marking loop:
   */
  // log_debug(gc)("DGC LOG:oop_size_offset:%d,offset_of_static_fields:%d,static_oop_field_count_offset:%d",
  //   java_lang_Class::_oop_size_offset, InstanceMirrorKlass::_offset_of_static_fields, java_lang_Class::_static_oop_field_count_offset);
  if(false) {
    // send region info
    char* buffer = new char[MAX_RPC_BUFFER_SIZE];
    int N = (MAX_RPC_BUFFER_SIZE - sizeof(int) * 2) / sizeof(unsigned long long);
    unsigned long long *message = (unsigned long long *)(buffer + 2 * sizeof(int));
    int top = 0;
    /*start @lyh*/
    char* heap_region_buf = new char[MAX_RPC_BUFFER_SIZE];
    unsigned long long* heap_region_msg = (unsigned long long*) (heap_region_buf + 2 * sizeof(int));
    int region_cnt = 0;
    std::map<unsigned long long, ShenandoahHeapRegion*> dedup_map;
    //log_dev_debug(gc)("DGC LOG:host heap:start=%p,end=%p,size=%lu", heap->reserved_region().start(),
      // heap->reserved_region().end(), heap->reserved_region().byte_size());
    /*end @lyh*/

    char rpcTypeByte = 3;  // 调用类型为3的RPC
    log_debug(gc)("DGC LOG: do RPC 3");
    // if (send(Universe::get_rpc_desc(), &rpcTypeByte, sizeof(rpcTypeByte), 0) == -1) {
    //   std::cerr << "Failed to send RPC 3 to server." << std::endl;
    //   exit(0);
    // }

    for (size_t i = ShenandoahHeap::heap()->num_regions(); i > 0; i--) { 
      ShenandoahHeapRegion* r = ShenandoahHeap::heap()->get_region(i - 1);
      /*start @lyh*/
      HeapWord* r_tams =  heap->marking_context()->top_at_mark_start(r);
      if(r->bottom() != r_tams) {
      /*end @lyh*/
        //log_dev_debug(gc)("DGC LOG: get nonempty region, idx=%lu, bottom=%llx, top=%llx",
          // (unsigned long)r->index(), (unsigned long long)r->bottom(), (unsigned long long)r->top());
        message[top * 4] = (unsigned long long)r->index();
        message[top * 4 + 1] = (unsigned long long)r->bottom();
        // message[top * 4 + 2] = (unsigned long long)r->top();
        message[top * 4 + 2] = (unsigned long long)(ShenandoahHeap::heap()->marking_context()->top_at_mark_start(r));
        message[top * 4 + 3] = (unsigned long long)r->end();
        top++;
        if(top * 4 >= N){
          log_error(gc)("Buffer not big enough!");
          exit(0);
        }
      }
    }
    Universe::send_rpc(3, 2*sizeof(int) + top * 4 * sizeof(unsigned long long), buffer);
    top = 0;
    log_debug(gc)("DGC LOG: Start sending RPC 2 to server. ");
    
    int count = 0;
    /*start @lyh*/
    // worker 0 pop every worker queue sequentally, and split them inside message buffer.
    uint total_worker_num = heap->workers()->total_workers();
    for (uint wid = 0; wid < total_worker_num; ++wid) {
      auto que = get_queue(wid);
      log_debug(gc)("DGC LOG: worker_queue %u size:%d", wid, que->size());
      while (que->pop(t)) {
        auto top_oop = t.obj();
        message[top] = (unsigned long long) (top_oop);
        top++;
        count++;
      }
      // put a placeholder at the end of queue data
      message[top] = (unsigned long long) (-1);
      top++;
      log_debug(gc)("DGC LOG:worker_queue %u top:%d", wid, top);
    }
    // then send RPC 2 to SNIC Client
    Universe::send_rpc(2, 2 * sizeof(int) + top * sizeof(unsigned long long), (void*)buffer);
    Universe::recv_ack();
    /*start @lyh*/
    // disable all SATB Barriers
    // satb_mq_set.set_active_all_threads(false, false);
    // process all satb queues and send RPC
    log_info(gc)("send RPC 7 for satb queues");
    top = 0;
    // flush all thread local satb queues to global satb queue
    SnicSATBThreadsClosure snicThreadCl(satb_mq_set);
    Threads::threads_do(&snicThreadCl);
    // SnicSATBBufferClosure snicCl(message, &top);
    // while (satb_mq_set.apply_closure_to_completed_buffer(&snicCl)) {}
    Universe::send_rpc(7, 2 * sizeof(int) + top * sizeof(unsigned long long), buffer);
    log_debug(gc)("DGC LOG:finish to send RPC 7");
    Universe::recv_ack();
    delete (buffer);
    log_info(gc)("finish SNIC GC HOST!");
    /*end @lyh*/
    // update local liveness data
    size_t total_live_data = 0;
    for (size_t region_idx = 0; region_idx < heap->num_regions(); ++region_idx) {
      ShenandoahHeapRegion* region = heap->get_region(region_idx);
      uint64_t remote_liveness = heap->_remote_liveness_cache[(int) (region_idx)];
      region->increase_live_data_gc_words(remote_liveness);
      total_live_data += region->get_live_data_bytes();
    }
    log_debug(gc)("DGC LOG:host side total live data:%lu", total_live_data);
    return ;
    /*end @lyh*/
  }
  else{
    // modified @lht
    // std::cout<<"DGC LOG: gc queue init size: " << q->size() << std::endl;
    // log_debug(gc)("DGC LOG: worker %u gc queue init size: %d", worker_id, q->size());
    int work_count = 0;
    int init_que_size = 0;
    init_que_size += (int)(q->size());
    init_que_size += (int)(q->overflow_stack()->size());
    // log_debug(gc)("DGC LOG: worker %u init que size %d", worker_id, init_que_size);
    while (true) {
      if (CANCELLABLE && heap->check_cancelled_gc_and_yield()) {
        return;
      }

      while (satb_mq_set.completed_buffers_num() > 0) {
        satb_mq_set.apply_closure_to_completed_buffer(&drain_satb);
      }
      uint work = 0;
      for (uint i = 0; i < stride; i++) {
        if (q->pop(t) ||
          queues->steal(worker_id, t)) {
          do_task<T>(q, cl, live_data, &t);
          work++;
          work_count++;
        } else {
          break;
        }
      }

      if (work == 0) {
        // log_debug(gc)("DGC LOG: worker %u total work done %d", worker_id, work_count);
        // No work encountered in current stride, try to terminate.
        // Need to leave the STS here otherwise it might block safepoints.
        ShenandoahSuspendibleThreadSetLeaver stsl(CANCELLABLE && ShenandoahSuspendibleWorkers);
        ShenandoahTerminatorTerminator tt(heap);
        if (terminator->offer_termination(&tt)) return;
      }
    }
  }
}
