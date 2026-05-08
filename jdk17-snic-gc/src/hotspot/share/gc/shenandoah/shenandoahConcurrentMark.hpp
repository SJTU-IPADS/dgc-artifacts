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

#ifndef SHARE_GC_SHENANDOAH_SHENANDOAHCONCURRENTMARK_HPP
#define SHARE_GC_SHENANDOAH_SHENANDOAHCONCURRENTMARK_HPP

#include "gc/shenandoah/shenandoahMark.hpp"
#include <stack>
#include <queue>

class ShenandoahConcurrentMarkingTask;
class ShenandoahFinalMarkingTask;

class ShenandoahConcurrentMark: public ShenandoahMark {
  friend class ShenandoahConcurrentMarkingTask;
  friend class ShenandoahFinalMarkingTask;

public:
  ShenandoahConcurrentMark();
  // Concurrent mark roots
  void mark_concurrent_roots();
  // Concurrent mark
  void concurrent_mark();
  /*start @lyh*/
  std::vector<int> decide_host_splits_by_dpu_client_num();
  void flush_task_queues_and_send(char* buffer, int rpc_type, bool is_prefetched);
  void snic_final_mark();
  void snic_final_mark_work();
  void wait_client_copy_gc_roots_finish(int i);
  void wait_client_copy_gc_roots_finish_with_ack(std::queue<int> &rpc_normal_ack_queue, std::queue<int> &rpc_final_ack_queue);
  void wait_client_liveness_and_finish_ack(std::queue<int> &rpc_normal_ack_queue, std::queue<int> &rpc_final_ack_queue, size_t* total_liveness_received, int rpc_type);
  void snic_shm_send_roots(int rpcType);
  void snic_collect_satb_roots_and_send(std::vector<std::vector<unsigned long long>> *oop_vecs, int rpcType);
  void snic_shm_send_satb_roots(int rpcType);
  void snic_shm_send_satb_roots_no_tcp();
  size_t snic_shm_write_roots_prealloc(unsigned long long* dest);
  size_t snic_shm_write_satb_prealloc(unsigned long long* dest);
  void handle_liveness_ack(size_t ack);
  std::vector<int> decide_host_splits_by_shm_client_marker_num();
  /*end @lyh*/
  // Finish mark at a safepoint
  void finish_mark();

  static void cancel();

private:
  void finish_mark_work();
};

#endif // SHARE_GC_SHENANDOAH_SHENANDOAHCONCURRENTMARK_HPP
