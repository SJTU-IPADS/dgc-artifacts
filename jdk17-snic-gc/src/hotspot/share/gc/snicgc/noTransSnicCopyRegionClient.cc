
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

#include "noTransSnicCopyRegionClient.hpp"
#include <fstream>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <iomanip>
#include <atomic>
#include <sys/mman.h>
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
#include <numeric>
#include <errno.h>

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

// File-scope to avoid colliding with copyRegionSnicClient.cc's `mtx`.
static std::mutex mtx;

NoTransSnicCopyRegionClient::NoTransSnicCopyRegionClient() : SnicClient() {
  // generate a random number to distinguish different host.
  srand(time(NULL));
  host_random_num = rand() % 10000 + 10000;
  virtualSpaceNodes.clear();
}

// function to handle RPCs.
void NoTransSnicCopyRegionClient::handleRPC(int rpcType, int hostId, int bufferSize, void* payload) {
  if (rpcType != 6) {
    log_debug (gc)("DGC LOG: host %d handling RPC of type %d", hostId, rpcType);
  }
  cur_handling_rpc_type = rpcType;
  switch (rpcType)
  {
  // register rdma
  case 1:
    {
      unsigned long long* message = (unsigned long long*) payload;
      unsigned long long region_size = message[0];
      regionSizeShift = __builtin_ctz(region_size);
      log_debug(gc)("DGC LOG: received host %d region size shift:%lu, received region size:%llu", hostId, regionSizeShift, region_size / 1024);
      // handle RPC type 1, used to establish RDMA connection.
      runRDMAClient(hostId);
      send_back_int_ack(1);
      break;
    }
  case 2:
    {
      if (SnicGCCoorHeuristic) {
        if (coor_state->force_gc_ccmt != 0) {
          ConcGCThreads = coor_state->force_gc_ccmt;
        }
        snic_gc_workers->update_active_workers(ConcGCThreads);
        log_debug(gc)("DGC LOG: set active workers to %d", snic_gc_workers->active_workers());
      }
      if (is_prefetched == 1) {
        if (SnicGCCoorHeuristic) {
          coor_state->gc_start_time = (unsigned long long)os::javaTimeMillis();
          coor_state->client_gc_id = _gc_id;
          coor_state->cores_in_use = ConcGCThreads;
        }
        currentMarkStartTime = (unsigned long long)os::javaTimeMillis();
      }
      // handle RPC type 2, used to receive root objects' addresses.
      should_force_tasks_finish = 0;
      force_finished_task_cnt = 0;
      finished_worker_count = 0;
      normal_satb_roots_handle_count = 0;
      if (!SnicConcCopyRegion) {
        log_debug(gc)("GC(%u,%d)DGC LOG: Start handling root objects and process references", _gc_id, hostId);
        // handleRoot(bufferSize, payload);
        int len = copy_remote_gc_roots_buffer(bufferSize, payload);
        handleRoot(len, (void*) (gc_roots_buffer));
        // send back an ack to tell client case 2 is finished.
        send_back_int_ack(NT_TASK_QUEUE_ROOTS_FINISH_ACK);
        log_debug(gc)("GC(%u,%d)DGC LOG: Done handling root objects and references", _gc_id, hostId);
      } else {
        // read_back_cur_host_virtual_nodes(hostId);
        remapCurHostMemSpaces(hostId);
        // memset(try_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
        // memset(success_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
        unsigned long long *message = (unsigned long long *)payload;
        is_prefetched = message[1];
        log_debug(gc)("GC(%u,%d)DGC LOG: conc copy region handle root, is_prefetched:%d", _gc_id, hostId, is_prefetched);
        int len = copy_remote_gc_roots_buffer(bufferSize, payload);
        conc_copy_region_handle_root(len, (void*) (gc_roots_buffer), (int) is_prefetched);
        if (is_prefetched == 1) {
          send_back_int_ack(NT_SATB_ROOTS_FINAL_ACK);
        }
        // FIX (noTrans-isolation): host's wait_client_liveness_and_finish_ack
        // returns on TASK_QUEUE_ROOTS_FINISH_ACK and then enters a while-loop
        // that blocks at recv_int_ack() until it sees a SATB_ROOTS_NORMAL_ACK
        // or _FINAL_ACK to decide between RPC 9 and RPC 10. With
        // SnicSATBRootsSplitPartNum=1 and a marker that didn't push any
        // pending region fetches (e.g. all roots already in already-copied
        // regions), wait_part_copy_finish_work spins without ever sending
        // FINAL_ACK -> host hangs. Send one FINAL_ACK here so the host can
        // make forward progress and request the final SATB roots via RPC 10.
        log_info(gc)("GC(%u,%d) DGC LOG: notrans send FINAL_ACK before TASK_QUEUE_ROOTS_FINISH_ACK", _gc_id, hostId);
        send_back_int_ack(NT_SATB_ROOTS_FINAL_ACK);
        // send back an ack to tell client case 2 is finished.
        send_back_int_ack(NT_TASK_QUEUE_ROOTS_FINISH_ACK);
        size_t total_try_mark_count = 0;
        size_t total_success_mark_count = 0;
        // for (uint i = 0; i < snic_gc_workers->active_workers() * 8; ++i) {
        //   total_try_mark_count += try_mark_counts[i];
        //   total_success_mark_count += success_mark_counts[i];
        // }
        log_debug(gc)("GC(%u,%d)DGC LOG: Done conc copy region handle root, total try mark count: %lu, total success mark count: %lu", _gc_id, hostId, total_try_mark_count, total_success_mark_count);
      }
      break;
    }
  case 3:
    {
      _gc_id++;
      // handle RPC type 3, used to copy heap.
      log_debug(gc)("GC(%u,%d)DGC LOG: Start copyRegion", _gc_id, hostId);
      copyRegion(bufferSize, payload);
      log_debug(gc)("GC(%u,%d)DGC LOG: Done copyRegion", _gc_id, hostId);
      break;
    }
  case 4:
    {
      exit(0);
      break;
    }
  case 5:
    {
      handleNewVirtualSpaceNode(hostId, bufferSize, payload);
      // ACK after ibv_reg_mr completes so the host blocks until we're ready.
      // Without this, the host's first GC can send RPC 8 while we're still
      // pinning pages for a 4GB Metaspace MR (~800ms), causing the RPC 8 to
      // queue behind ibv_reg_mr and delaying GC(0) by hundreds of ms.
      send_back_int_ack(1);
      break;
    }
  case 6:
    {
      fetchKlass(hostId, bufferSize, payload);
      break;
    }
  case 7:
    {
      log_debug(gc)("GC(%u,%d)DGC LOG: Start handling SATB objects and process references", _gc_id, hostId);
      handleRootAndCommit(bufferSize, payload);
      log_debug(gc)("GC(%u,%d)DGC LOG: Done handling SATB objects and references", _gc_id, hostId);
      break;
    }
  case 8:
    {
      _gc_id++;
      log_debug(gc)("GC(%u,%d)DGC LOG: Start copy region metadata", _gc_id, hostId);
      if (SnicGCCoorHeuristic) {
        if (coor_state->force_gc_ccmt != 0) {
          ConcGCThreads = coor_state->force_gc_ccmt;
        }
        snic_gc_workers->update_active_workers(ConcGCThreads);
        log_debug(gc)("DGC LOG: set active workers to %d", snic_gc_workers->active_workers());
      }
      copy_region_metadata(bufferSize, payload);
      if (is_prefetched == 0) {
        if (SnicGCCoorHeuristic) {
          coor_state->gc_start_time = (unsigned long long)os::javaTimeMillis();
          coor_state->client_gc_id = _gc_id;
          coor_state->cores_in_use = ConcGCThreads;
        }
        currentMarkStartTime = (unsigned long long)os::javaTimeMillis();
      } else {
        if (SnicGCCoorHeuristic) {
          coor_state->client_gc_id = _gc_id;
        }
      }
      log_debug(gc)("GC(%u,%d)DGC LOG: Done copy region metadata", _gc_id, hostId);
      break;
    }
  case 9:
    {
      // memset(try_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
      // memset(success_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
      force_finished_task_cnt = 0;
      finished_worker_count = 0;
      normal_satb_roots_handle_count += 1;
      log_debug(gc)("GC(%u,%d)DGC LOG: Start handle normal satb roots, handle count:%d", _gc_id, hostId, normal_satb_roots_handle_count);
      // handle_satb_roots(bufferSize, payload);
      int len = copy_remote_gc_roots_buffer(bufferSize, payload);
      handle_satb_roots(len, (void*) (gc_roots_buffer));
      if (DpuClientLivenessUpdateEnabled) {
        send_back_int_ack((size_t)(NT_SATB_ROOTS_FINISH_ACK));
      }
      size_t total_try_mark_count = 0;
      size_t total_success_mark_count = 0;
      // for (uint i = 0; i < snic_gc_workers->active_workers() * 8; ++i) {
      //   total_try_mark_count += try_mark_counts[i];
      //   total_success_mark_count += success_mark_counts[i];
      // }
      log_debug(gc)("GC(%u,%d)DGC LOG: Done handle normal satb roots, len=%d, total try mark count: %lu, total success mark count: %lu", _gc_id, hostId, len, total_try_mark_count, total_success_mark_count);
      break;
    }
  case 10:
    {
      // memset(try_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
      // memset(success_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
      force_finished_task_cnt = 0;
      finished_worker_count = 0;
      log_debug(gc)("GC(%u,%d)DGC LOG: Start handle final satb roots", _gc_id, hostId);
      // handle_satb_roots_commit(bufferSize, payload);
      int len = copy_remote_gc_roots_buffer(bufferSize, payload);
      handle_satb_roots_commit(len, (void*) (gc_roots_buffer));
      size_t total_try_mark_count = 0;
      size_t total_success_mark_count = 0;
      // for (uint i = 0; i < snic_gc_workers->active_workers() * 8; ++i) {
      //   total_try_mark_count += try_mark_counts[i];
      //   total_success_mark_count += success_mark_counts[i];
      // }
      if (SnicGCCoorHeuristic) {
        coor_state->cores_in_use = 0;
      }
      auto new_mark_time = os::javaTimeMillis() - currentMarkStartTime;
      historyMarkTime.push_back(new_mark_time);
      if (SnicGCCoorHeuristic) {
        coor_state->client_avg_mark_time = get_avg_mark_time();
      }
      set_marked_liveness(0);
      // FIX (noTrans-isolation): host (line ~1281 in shenandoahConcurrentMark.cpp)
      // calls one final Universe::recv_int_ack() after breaking out of the
      // SATB-RPC loop when DpuClientLivenessUpdateEnabled=false. The merged
      // client unblocks this via worker liveness updates streamed from
      // wait_part_copy_finish_work; in noTrans mode that thread may have
      // exited without sending a terminator. Send one ack here so the host
      // can return from concurrent marking instead of hanging.
      log_info(gc)("GC(%u,%d) DGC LOG: notrans send terminator ack at end of case 10", _gc_id, hostId);
      send_back_int_ack(1);
      log_debug(gc)("GC(%u,%d)DGC LOG: Done handle final satb roots, len=%d, total try mark count: %lu, total success mark count: %lu", _gc_id, hostId, len, total_try_mark_count, total_success_mark_count);
      // unmapCurHostMemSpaces(hostId);
      break;
    }
    case 11:
    {
      // this rpc is used to handle rdma regions prefetch.
      log_debug(gc)("DGC LOG: start to prefetch for host %d", hostId);
      start_wait_pre_copy_finish_thread();
      send_back_int_ack(1);
      break;
    }
    case 12:
    {
      // SnicGCRDMABatchFetchKlass bulk-sync. Payload:
      //   u64 ccs_hwm   -- high-water mark of the host's CompressedClassSpace
      //   u64 ccs_base  -- VirtualSpaceList::vslist_class()->base_of_first_node()
      // The client uses ccs_base to locate the matching SnicVirtualSpaceNode
      // (so the bulk RDMA read targets the right MR) and ccs_hwm to bound
      // the read range.
      unsigned long long* m = (unsigned long long*)payload;
      unsigned long long ccs_hwm = m[0];
      unsigned long long ccs_base = m[1];
      bulkSyncCcs(hostId, ccs_hwm, ccs_base);
      send_back_int_ack(1);
      break;
    }
  default:
    // unknown RPC type
    log_warning(gc)("Unknown RPC type:%d", rpcType);
  }
}

int NoTransSnicCopyRegionClient::runRDMAClient(int hostId)
{
  // 1. create a local QP to use
  nic = RNic::create(RNicInfo::query_dev_names().at(0)).value();
  nic2 = RNic::create(RNicInfo::query_dev_names().at(1)).value();
  // 2. create the pair QP at server using CM
  char HostAddrPort[30];
  sprintf(HostAddrPort, "%s:%d", HostAddr, (RDMAPort + hostId));
  log_debug(gc)("DGC LOG: host %d runRDMAClient, HostAddrPort: %s", hostId, HostAddrPort);
  cm = new ConnectManager(HostAddrPort);

  if (cm->wait_ready(1000000, 2) ==
      IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout";

  //log_dev_debug(gc)("DGC LOG: cm connected");
  qp_heap = RC::create(nic, QPConfig()).value();
  qp_heap2 = RC::create(nic2, QPConfig()).value();
  std::string qp_name_1 = "client-qp-heap-" + std::to_string(hostId);
  auto qp_res = cm->cc_rc(qp_name_1, qp_heap, 0, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  std::string qp_name_2 = "client-qp-heap2-" + std::to_string(hostId);
  qp_res = cm->cc_rc(qp_name_2, qp_heap2, 1, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);

  //log_dev_debug(gc)("DGC LOG: qp_heap get");
  auto key = std::get<1>(qp_res.desc);
  //log_dev_debug(gc)("DGC LOG: key get");

  {
    auto target_mr_idx_1 = compute_mr_idx(hostId, 0);
    log_debug(gc)("DGC LOG: host %d fetch remote heap1 mr idx: %lu", hostId, target_mr_idx_1);
    auto fetch_res = cm->fetch_remote_mr(target_mr_idx_1);
    if (fetch_res != IOCode::Ok) {
      log_debug(gc)("DGC LOG: host %d fetch remote heap1 mr idx: %lu failed, %s", hostId, target_mr_idx_1, std::get<0>(fetch_res.desc).c_str());
    }
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
    //log_dev_debug(gc)("DGC LOG: start bind_remote_mr");
    qp_heap->bind_remote_mr(remote_attr);
    heapBase = remote_attr.buf;
    heapSize = remote_attr.sz;
    heapRegionNumber_half = heapSize >> regionSizeShift;

    log_debug(gc)("DGC LOG: reserve Heap from %llx to %llx,size=%llu for host %d", heapBase, heapBase+heapSize, heapSize, hostId);
    reserveMemRegion(heapBase, heapSize);
    local_mem_heap = Arc<RMem>(new RMem((void*)heapBase, heapSize));
    local_mr_heap = RegHandler::create(local_mem_heap, nic).value();
    qp_heap->bind_local_mr(local_mr_heap->get_reg_attr().value());

    auto res = qp_heap->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = (unsigned int)(16),
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(heapBase),
        .remote_addr = heapBase,
        .imm_data = 0});
    RDMA_ASSERT(res == IOCode::Ok);
    auto res2 = qp_heap->wait_one_comp();
    RDMA_ASSERT(res2 == IOCode::Ok);
    log_debug(gc)("DGC LOG: send rdma read to heapBase for host %d done, get msg: %llx, %llx", hostId, *(long long*)heapBase, *(long long*)(heapBase+8));
  }

  {
    auto target_mr_idx_2 = compute_mr_idx(hostId, 999);
    log_debug(gc)("DGC LOG: host %d fetch remote heap2 mr idx: %lu", hostId, target_mr_idx_2);
    auto fetch_res = cm->fetch_remote_mr(target_mr_idx_2);
    if (fetch_res != IOCode::Ok) {
      log_debug(gc)("DGC LOG: host %d fetch remote heap2 mr idx: %lu failed, %s", hostId, target_mr_idx_2, std::get<0>(fetch_res.desc).c_str());
    }
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
    //log_dev_debug(gc)("DGC LOG: start bind_remote_mr");
    qp_heap2->bind_remote_mr(remote_attr);
    heapBase2 = remote_attr.buf;
    unsigned long long heapSize2 = remote_attr.sz;

    log_debug(gc)("DGC LOG: host %d heapsize1: %llu, heapsize2: %llu", hostId, heapSize, heapSize2);
    heapSize += heapSize2;
    heapRegionNumber = heapSize >> regionSizeShift;

    log_debug(gc)("DGC LOG: host %d heapsize: %llu, heapRegionNumber: %lu, regionSizeShift: %lu", hostId, heapSize, heapRegionNumber, regionSizeShift);

    log_debug(gc)("DGC LOG: reserve Heap from %llx to %llx,size=%llu for host %d", heapBase2, heapBase2+heapSize2, heapSize2, hostId);
    reserveMemRegion(heapBase2, heapSize2);
    local_mem_heap2 = Arc<RMem>(new RMem((void*)heapBase2, heapSize2));
    local_mr_heap2 = RegHandler::create(local_mem_heap2, nic2).value();
    qp_heap2->bind_local_mr(local_mr_heap2->get_reg_attr().value());


    auto res = qp_heap2->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = (unsigned int)(16),
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(heapBase2),
        .remote_addr = heapBase2,
        .imm_data = 0});
    RDMA_ASSERT(res == IOCode::Ok);
    auto res2 = qp_heap2->wait_one_comp();
    RDMA_ASSERT(res2 == IOCode::Ok);
    log_debug(gc)("DGC LOG: send rdma read to heapBase for host %d done, get msg: %llx, %llx", hostId, *(long long*)heapBase2, *(long long*)(heapBase2+8));
  }

  size_t bitmap_page_size = (size_t)os::vm_page_size();
  size_t bitmap_size = ShenandoahMarkBitMap::compute_size(heapSize);
  bitmap_size = align_up(bitmap_size, bitmap_page_size);
  ReservedSpace bitmap_space(bitmap_size, bitmap_page_size);

  MemRegion bitmap_region = MemRegion((HeapWord*) bitmap_space.base(), bitmap_space.size() / HeapWordSize);
  os::commit_memory_or_exit((char *) bitmap_region.start(), bitmap_space.size(), bitmap_page_size, false,
                              "Cannot commit bitmap memory");
  bitmap = new ShenandoahMarkBitMap(MemRegion((HeapWord*)heapBase, (size_t)(heapSize / HeapWordSize)),  bitmap_region);
  memset(bitmap->_map, 0, bitmap->_size / 8);
  live_count = new uint64_t[heapRegionNumber];
  memset(live_count, 0, heapRegionNumber * sizeof(uint64_t));
  initBitmapQP((void*)bitmap_region.start(), bitmap_space.size(), hostId);
  initLivenessQP((void*)live_count, heapRegionNumber * sizeof(uint64_t), hostId);
  gc_roots_buffer = new char[MAX_RPC_BUFFER_SIZE];
  memset(gc_roots_buffer, (char) (0), MAX_RPC_BUFFER_SIZE);
  initGCRootsBufferQP((void*)gc_roots_buffer, MAX_RPC_BUFFER_SIZE, hostId);
  if (!SnicGCCoorHeuristic) {
    local_force_gc_by_dpu_client = new int[1];
    memset(local_force_gc_by_dpu_client, 0, sizeof(int));
    initForceGCByDPUClientQP((void*)local_force_gc_by_dpu_client, sizeof(int), hostId);
  } else {
    waitCoordinatorUpdateFinished();
    updateClientState(ClientStates::RDMA_CLIENT_INIT);
  }
  local_rdma_prefetch_finish_flag = new int[1];
  memset(local_rdma_prefetch_finish_flag, 0, sizeof(int));
  initRdmaPrefetchFinishFlagQP((void*)local_rdma_prefetch_finish_flag, sizeof(int), hostId);
  initialize();
  return 0;
}

void NoTransSnicCopyRegionClient::reserveMemRegion(unsigned long long addr, unsigned long long length)
{

  log_info(gc)("reserveMemRegion");
  int prot = PROT_READ | PROT_WRITE;                   // the protection of memory page
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED; // the mapping flags
  int fd = -1;                                         // the file descriptor used in file mapping
  off_t offset = 0;                                    // the offset in file mapping, here not used, set to 0

  // apply memory by mmap()
  void *ptr = mmap((void*)addr, length, prot, flags, fd, offset);
  if (ptr == MAP_FAILED) {
    log_error(gc)("DGC LOG: reserveMemRegion failed");
    exit(0);
  }
  log_info(gc)("Success reserveMemRegion at %p, length: %llu", ptr, length);
}

// Superseded by conc_copy_region_handle_root + the worker-driven async
// copy pipeline. The body exit(0)s at the top so any stale call site
// fails loudly rather than silently re-entering the older sync path.
void NoTransSnicCopyRegionClient::copyRegion(int bufferSize, void* payload){
  log_debug(gc)("DGC LOG: copyRegion called but the active path is conc_copy_region_handle_root");
  exit(0);
  // memset bitmap
  memset(bitmap->_map, 0, bitmap->_size / 8);
  // memset livedata
  memset(live_count, 0, heapRegionNumber * sizeof(live_count[0]));
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    region_info[i] = NULL;
  }
  regionTopIdx = 0;

  unsigned long long* message = (unsigned long long*)payload;
  int wait_complete_count = 0;
  received_region_num = (int) (bufferSize / sizeof(unsigned long long) / 4);
  size_t shift_num = 0;
  for(int i = 0; i < received_region_num; i++){
      regionTopIdx = std::max((unsigned long long)regionTopIdx, message[i*4]);
      SnicHeapRegion* cur_region = new SnicHeapRegion();
      cur_region->index = message[i * 4];
      cur_region->bottom = message[i * 4 + 1];
      cur_region->top = message[i * 4 + 2];
      cur_region->end = message[i * 4 + 3];
      if (shift_num == 0) {
        shift_num = __builtin_ctz(cur_region->end - cur_region->bottom);
      }
      region_info[cur_region->index] = cur_region;
      auto res_s = qp_heap->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = (unsigned int)(message[i * 4 + 2] - message[i * 4 + 1]),
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(message[i * 4 + 1]),
        .remote_addr = message[i * 4 + 1],
        .imm_data = 0});
      RDMA_ASSERT(res_s == IOCode::Ok);
      wait_complete_count += 1;
  }
  regionSizeShift = shift_num;
  heapRegionSize = (size_t) (1UL << regionSizeShift);
  log_debug(gc)("DGC LOG: regionSizeShift=%lu,heapRegionNumber=%lu,heapRegionSize=%llu", regionSizeShift, heapRegionNumber, heapRegionSize);
  while(wait_complete_count > 0) {
    wait_complete_count -= 1;
    auto res_p = qp_heap->wait_one_comp();
    RDMA_ASSERT(res_p == IOCode::Ok);
  }
  send_back_int_ack(1);
}

void NoTransSnicCopyRegionClient::wait_pre_copy_finish_work() {
  std::vector<int> wait_types;
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    if (region_info[i] == NULL) {
      continue;
    }
    if (region_copied_bitmap[i] == (char) (NT_REGION_COPIED)) {
      continue;
    }
    copy_one_region_async(i, true);
    auto region_bottom = region_info[i]->bottom;
    auto region_top = region_info[i]->top;
    if (region_top <= heapBase2) {
      wait_types.push_back(0);
    } else if (region_bottom >= heapBase2) {
      wait_types.push_back(1);
    }
  }
  int region_num_to_copy = wait_types.size();
  for (int i = 0; i< region_num_to_copy; ++i) {
    if (wait_types[i] == 0) {
      auto res_p = qp_heap->wait_one_comp();
      RDMA_ASSERT(res_p == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (char) (NT_REGION_COPIED);
      on_trans_region_number1 -= 1;
      done_trans_region_number1 += 1;
    } else if (wait_types[i] == 1) {
      auto res_p = qp_heap2->wait_one_comp();
      RDMA_ASSERT(res_p == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (char) (NT_REGION_COPIED);
      on_trans_region_number2 -= 1;
      done_trans_region_number2 += 1;
    }
    on_trans_region_number -= 1;
  }
  // update rdma_finish_prefetch_flag to notify the end of prefetch for host side.
  local_rdma_prefetch_finish_flag[0] = 1;
  writeRdmaPrefetchFinishFlagRDMA();
  waitRdmaPrefetchFinishFlagCQ();
  log_debug(gc)("DGC LOG: wait_pre_copy_finish_work done, copied region num:%d", region_num_to_copy);
}

void NoTransSnicCopyRegionClient::wait_part_copy_finish_work() {
  // for (int i = SnicSATBRootsSplitPartNum; i > 0; --i) {
  // int start = split_regions_parts_arr[i];
  // int end = split_regions_parts_arr[i - 1];
  // for (int j = start; j < end; ++j) {
  log_debug(gc)("DGC LOG: wait part copy finish work");
  unsigned long long start_time = os::javaTimeMillis();
  int SnicSATBRootsSplitPartCount = 0;
  int SATBFlashInterval = received_region_num / SnicSATBRootsSplitPartNum;
  int checkForceFinishInterval = SATBFlashInterval + SnicSATBRootsForceDeltaNum;

  // OPT#5: batch-drain up to POLL_BATCH completions per ibv_poll_cq call on
  // each QP. Previously we called wait_one_comp() which does
  // ibv_poll_cq(cq, 1, &wc) in a busy loop — one completion per poll. For
  // graphchi (1209 regions) that's 1209 individual polls of the CQ doorbell,
  // each with an atomic decrement on out_signaled. Batching pays off
  // because the NIC pipelines many RDMA READs and frequently has multiple
  // CQEs ready at once.
  //
  // All per-CQE work (CAS region_copied_bitmap, enqueue regionArrivalQueue,
  // counter updates, SATB ack threshold) is extracted into a lambda so it
  // runs the same whether the CQE came from a batch poll or from the
  // blocking wait_one_comp fallback.
  constexpr int POLL_BATCH = 16;
  auto process_cqe = [&](int side, uint64_t wr_id) {
    uint64_t recv_region_idx = (wr_id >> 16) & 0xFFFF;
    unsigned long long prev = 0;
    int retry_times = 0;
    while (true) {
      retry_times++;
      if (retry_times > 1000000000) {
        log_error(gc)("DGC LOG: retry times > 1000000000, recv_region_idx=%lu, prev=%llu", recv_region_idx, prev);
        exit(0);
      }
      prev = region_copied_bitmap[recv_region_idx];
      if (prev > NT_REGION_ON_TRANS) continue;
      auto res = Atomic::cmpxchg(&region_copied_bitmap[recv_region_idx],
                                 (unsigned long long) prev,
                                 (unsigned long long) (NT_REGION_COPIED));
      if (res == prev) break;
    }
    int min_region_idx = 0;
    int min_region_size = task_queues->queue(0)->size();
    for (int i = 1; i < (int) snic_gc_workers->active_workers(); ++i) {
      auto task_queues_i = task_queues->queue(i);
      if ((uint) min_region_size > task_queues_i->size()) {
        min_region_size = task_queues_i->size();
        min_region_idx = i;
      }
    }
    regionArrivalQueue[min_region_idx]->push(recv_region_idx);

    if (side == 1) {
      on_trans_region_number1--;
      done_trans_region_number1++;
    } else {
      on_trans_region_number2--;
      done_trans_region_number2++;
    }
    on_trans_region_number--;
    done_trans_region_number++;

    if (done_trans_region_number % (size_t) SATBFlashInterval == 0) {
      if (SnicSATBRootsSplitPartNum == 1) {
        send_back_int_ack(NT_SATB_ROOTS_FINAL_ACK);
      } else if (SnicSATBRootsSplitPartCount < SnicSATBRootsSplitPartNum - 1) {
        send_back_int_ack(NT_SATB_ROOTS_NORMAL_ACK);
      }
      SnicSATBRootsSplitPartCount++;
    }
    if (done_trans_region_number % (size_t) checkForceFinishInterval == 0 && is_prefetched == 0) {
      Atomic::add(&should_force_tasks_finish, 1);
      log_debug(gc)("DGC LOG: ready to force workers termination 1, done_trans_num:%lu,target_num:%d", done_trans_region_number, received_region_num);
    }
  };

  while (done_trans_region_number < (size_t) received_region_num) {
    if (on_trans_region_number1 < (size_t)(SNICTransRegionGroupNum) && received_region_num1 - done_trans_region_number1 - on_trans_region_number1 > 0) {
      int candidate_region_number1 = std::min(2 * SNICTransRegionGroupNum - on_trans_region_number1, size_t(received_region_num1 - done_trans_region_number1 - on_trans_region_number1));
      issue_fetch_region_by_pending_length_heap1(candidate_region_number1);
    }
    if (on_trans_region_number2 < (size_t)(SNICTransRegionGroupNum) && received_region_num2 - done_trans_region_number2 - on_trans_region_number2 > 0) {
      int candidate_region_number2 = std::min(2 * SNICTransRegionGroupNum - on_trans_region_number2, size_t(received_region_num2 - done_trans_region_number2 - on_trans_region_number2));
      issue_fetch_region_by_pending_length_heap2(candidate_region_number2);
    }

    // Try batch-polling both QPs first (non-blocking). Whatever completions
    // are already waiting get drained in a single ibv_poll_cq call.
    ibv_wc wc_batch[POLL_BATCH];
    int processed_this_iter = 0;
    int n1 = qp_heap->poll_send_comp_batch(POLL_BATCH, wc_batch);
    for (int i = 0; i < n1; i++) {
      RDMA_ASSERT(wc_batch[i].status == IBV_WC_SUCCESS)
        << ibv_wc_status_str(wc_batch[i].status);
      process_cqe(1, wc_batch[i].wr_id);
      processed_this_iter++;
    }
    int n2 = qp_heap2->poll_send_comp_batch(POLL_BATCH, wc_batch);
    for (int i = 0; i < n2; i++) {
      RDMA_ASSERT(wc_batch[i].status == IBV_WC_SUCCESS)
        << ibv_wc_status_str(wc_batch[i].status);
      process_cqe(2, wc_batch[i].wr_id);
      processed_this_iter++;
    }

    // If both CQs were empty and we still have outstanding reads, fall back
    // to the blocking wait_one_comp on whichever side has the most in-flight.
    // This preserves the original "progress at least one per iteration"
    // semantic and avoids a tight spin burning a CPU when the NIC is slow.
    if (processed_this_iter == 0 && on_trans_region_number > 0) {
      if (on_trans_region_number1 >= on_trans_region_number2 && on_trans_region_number1 > 0) {
        auto res_p = qp_heap->wait_one_comp();
        RDMA_ASSERT(res_p == IOCode::Ok);
        process_cqe(1, (uint64_t) res_p.desc.wr_id);
      } else if (on_trans_region_number2 > 0) {
        auto res_p = qp_heap2->wait_one_comp();
        RDMA_ASSERT(res_p == IOCode::Ok);
        process_cqe(2, (uint64_t) res_p.desc.wr_id);
      }
    }
  }
  if (SnicSATBRootsSplitPartNum != 1 && is_prefetched == 0) {
    send_back_int_ack(NT_SATB_ROOTS_FINAL_ACK);
    // force termination after copying all target regions.
    Atomic::add(&should_force_tasks_finish, 1);
    log_debug(gc)("DGC LOG: ready to force workers termination 2, done_trans_num:%lu,target_num:%d", done_trans_region_number, received_region_num);
  }
  log_debug(gc)("DGC LOG: wait part copy finish work done, time cost: %llu ms", os::javaTimeMillis() - start_time);
  // verify the copied regions
  for(size_t i = 0; i < heapRegionNumber; i++){
    if(region_info[i] != NULL && region_copied_bitmap[i] != (char) (NT_REGION_COPIED)){
      log_debug(gc)("DGC LOG: region %lu is not copied", i);
      exit(0);
    }
  }
}

static void* wait_copy_finish_thread_func(void* arg) {
  NoTransSnicCopyRegionClient* client = (NoTransSnicCopyRegionClient*) arg;
  client->wait_part_copy_finish_work();
  return NULL;
}

static void* wait_pre_copy_finish_func(void* arg) {
  NoTransSnicCopyRegionClient* client = (NoTransSnicCopyRegionClient*) (arg);
  client->wait_pre_copy_finish_work();
  return NULL;
}

void NoTransSnicCopyRegionClient::start_wait_copy_finish_thread() {
  pthread_mutex_init(&wait_copy_finish_mutex, NULL);
  pthread_cond_init(&wait_copy_finish_cond, NULL);
  int ret = pthread_create(&wait_copy_finish_thread_id, NULL, wait_copy_finish_thread_func, this);
  if (ret != 0) {
    log_error(gc)("DGC LOG: failed to create wait copy finish thread");
    exit(0);
  }
  int detach_res = pthread_detach(wait_copy_finish_thread_id);
  if (detach_res != 0) {
    log_error(gc)("DGC LOG: failed to detach wait copy finish thread");
    exit(0);
  }
}

void NoTransSnicCopyRegionClient::start_wait_pre_copy_finish_thread() {
  pthread_mutex_init(&wait_pre_copy_finish_mutex, NULL);
  pthread_cond_init(&wait_pre_copy_finish_cond, NULL);
  int ret = pthread_create(&wait_pre_copy_finish_thread_id, NULL, wait_pre_copy_finish_func, this);
  if (ret != 0) {
    log_error(gc)("DGC LOG: failed to create wait pre copy finish thread");
    exit(0);
  }
  int detach_res = pthread_detach(wait_pre_copy_finish_thread_id);
  if (detach_res != 0) {
    log_error(gc)("DGC LOG: failed to detach wait pre copy finish thread");
    exit(0);
  }
}

void NoTransSnicCopyRegionClient::wait_thread_complete() {
  pthread_mutex_lock(&wait_copy_finish_mutex);
  while (on_trans_region_number > 0) {
    pthread_cond_wait(&wait_copy_finish_cond, &wait_copy_finish_mutex);
  }
  pthread_mutex_unlock(&wait_copy_finish_mutex);
  log_debug(gc)("DGC LOG: wait thread complete done");
}

void NoTransSnicCopyRegionClient::wait_pre_copy_thread_complete() {
  pthread_mutex_lock(&wait_pre_copy_finish_mutex);
  while (on_trans_region_number > 0) {
    pthread_cond_wait(&wait_pre_copy_finish_cond, &wait_pre_copy_finish_mutex);
  }
  pthread_mutex_unlock(&wait_pre_copy_finish_mutex);
  log_debug(gc)("DGC LOG: wait pre copy thread complete done");
}

void NoTransSnicCopyRegionClient::copy_region_metadata(int bufferSize, void* payload) {
  is_one_gc_final_stage = false;
  satb_handle_cnt = 0;
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    region_info[i] = NULL;
    // reset copied bitmap here, because snicClient will be initialized only once
    region_copied_bitmap[i] = (char) (NT_REGION_NOT_COPIED);
  }
  regionTopIdx = 0;
  log_debug(gc)("DGC LOG: start update region_info");
  unsigned long long* message = (unsigned long long*)payload;
  int wait_complete_count = 0;
  received_region_num = (int) (bufferSize / sizeof(unsigned long long) / 5);
  received_region_num1 = 0;
  received_region_num2 = 0;
  size_t shift_num = 0;
  for(int i = 0; i < received_region_num; i++){
      regionTopIdx = std::max((unsigned long long)regionTopIdx, message[i*5]);
      SnicHeapRegion* cur_region = new SnicHeapRegion();
      cur_region->index = message[i * 5];
      cur_region->bottom = message[i * 5 + 1];
      cur_region->top = message[i * 5 + 2];
      cur_region->end = message[i * 5 + 3];
      cur_region->state = (SnicRegionState) message[i * 5 + 4];
      if (shift_num == 0) {
        shift_num = __builtin_ctz(cur_region->end - cur_region->bottom);
      }
      region_info[cur_region->index] = cur_region;


      if(cur_region->top <= heapBase2){
        received_region_num1++;
      }
      else{
        received_region_num2++;
      }
  }
  is_prefetched = message[received_region_num * 5];
  time_to_wait_for_prefetched_gc = message[received_region_num * 5 + 1];
  log_debug(gc)("DGC LOG: regionSizeShift=%lu,heapRegionNumber=%lu,received_region_num=%d,received_is_prefetched=%llu,received_time_to_wait_for_prefetched_gc=%llu", regionSizeShift, heapRegionNumber, received_region_num, is_prefetched, time_to_wait_for_prefetched_gc);
  send_back_int_ack(1);
}

void NoTransSnicCopyRegionClient::handleRoot(int bufferSize, void* payload){
  log_debug(gc)("DGC LOG:on_trans_region_number:%lu before handle root", on_trans_region_number);
  // int N = bufferSize / sizeof(unsigned long long);
  int N = bufferSize;
  unsigned long long* message = (unsigned long long*) (gc_roots_buffer);
  unsigned long long version = message[0];
  log_debug(gc)("DGC LOG: received version:%llu", version);
  // can not assume host queue num = client queue num
  uint active_worker = snic_gc_workers->active_workers();
  std::vector<int> queue_indexes;
  queue_indexes.push_back(1);
  for (int i = 1; i < N; ++i) {
    if (message[i] != (unsigned long long) (-1)) {
      // TEST ONLY
      unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(message[i]) - (unsigned long long)(heapBase)) >> (regionSizeShift);
      if (r_idx_o >= heapRegionNumber) {
        log_error(gc)("DGC LOG: handle oop 0x%llx in invalid region,r_idx_o=%llu,heapRegionNumber=%zu,idx=%d", message[i], r_idx_o, heapRegionNumber, i);
      }
      continue;
    }
    queue_indexes.push_back(i);
    // log_debug(gc)("DGC LOG: receive queue:%d", i);
  }
  log_debug(gc)("GC(%u)DGC LOG: NoTransSnicCopyRegionClient receives root num:%d,queue count:%lu", _gc_id, N, queue_indexes.size() - 1);
  task_queues->reserve(snic_gc_workers->active_workers());
  for (uint i = 0; i < (snic_gc_workers->active_workers()); ++i) {
    auto worker_i = snic_gc_workers->worker(i);
    worker_i->set_gc_id(_gc_id);
  }
  NoTransShenandoahSNICCMTask task(N, message, this, queue_indexes);
  //log_dev_debug(gc)("DGC LOG:ready to do NoTransShenandoahSNICCMTask");
  snic_gc_workers->run_task(&task);
}

void NoTransSnicCopyRegionClient::conc_copy_region_handle_root(int bufferSize, void* payload, int is_prefetched) {
  // int N = bufferSize / sizeof(unsigned long long);
  int N = bufferSize;
  unsigned long long* message = (unsigned long long*) (gc_roots_buffer);
  unsigned long long version = message[0];
  log_debug(gc)("DGC LOG: received version:%llu", version);
  std::map<unsigned long long, int> region_map;
  std::vector<int> queue_indexes;
  queue_indexes.push_back(1);
  if (is_prefetched == 0) {
    on_trans_region_number = 0;
    done_trans_region_number = 0;
    done_trans_region_number1 = 0;
    done_trans_region_number2 = 0;
    for (size_t i = 0; i < heapRegionNumber; ++i) {
      region_copied_bitmap[i] = (char)(NT_REGION_NOT_COPIED);
    }
  }
  for (int i = 1; i < N; ++i) {
    if (message[i] == (unsigned long long)(-1)) {
      queue_indexes.push_back(i);
      continue;
    }
    oop obj = (oop)message[i];
    unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(heapBase)) >> regionSizeShift;
    if (region_info[r_idx_o] == NULL) {
      continue;
    }
    if (region_copied_bitmap[r_idx_o] == (char)(NT_REGION_COPIED)) {
      continue;
    }
    region_map[r_idx_o] += 1;
  }
  log_debug(gc)("DGC LOG: sync copy region count: %lu, received queue count:%lu", region_map.size(), queue_indexes.size());
  // send copy request related to roots.
  for (auto &pair : region_map) {
    copy_one_region_async(pair.first);
  }
  // split region into parts.
  int left_region_num = received_region_num - region_map.size();
  // update_split_regions_parts_arr(left_region_num);
  // wait synchronously for the copy region requests to finish.
  while (on_trans_region_number > 0) {
    if (on_trans_region_number1 > on_trans_region_number2) {
      auto recv_res = qp_heap->wait_one_comp();
      RDMA_ASSERT(recv_res == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(recv_res.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (char)(NT_REGION_COPIED);
      on_trans_region_number1--;
      done_trans_region_number1++;
    } else {
      auto recv_res = qp_heap2->wait_one_comp();
      RDMA_ASSERT(recv_res == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(recv_res.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (char)(NT_REGION_COPIED);
      on_trans_region_number2--;
      done_trans_region_number2++;
    }
    on_trans_region_number -= 1;
    done_trans_region_number += 1;
  }
  log_debug(gc)("DGC LOG: finish sync init root regions copy, copy region num:%lu", on_trans_region_number);
  force_terminate_counts = decide_force_terminate_counts();
  // wait asynchronously for the copy region requests to finish.
  log_debug(gc)("DGC LOG: finish to send rest of requests, start to wait copy finish");
  if (is_prefetched == 0) {
    start_wait_copy_finish_thread();
    if (time_to_wait_for_prefetched_gc > 0) {
      log_debug(gc)("GC(%u) DGC LOG: wait for prefetched gc, time_to_wait_for_prefetched_gc=%llu", _gc_id, time_to_wait_for_prefetched_gc);
      os::naked_short_sleep(time_to_wait_for_prefetched_gc);
      
    }
  }
  start_handle_task_queue_roots();
  // handle roots normally here.
  task_queues->reserve(snic_gc_workers->active_workers());
  for (uint i = 0; i < (snic_gc_workers->active_workers()); ++i) {
    auto worker_i = snic_gc_workers->worker(i);
    worker_i->set_gc_id(_gc_id);
  }
  NoTransShenandoahSNICCMTask task(N, message, this, queue_indexes);
  //log_dev_debug(gc)("DGC LOG:ready to do NoTransShenandoahSNICCMTask");
  snic_gc_workers->run_task(&task);
  finish_handle_task_queue_roots();
}

void NoTransSnicCopyRegionClient::handle_satb_roots(int bufferSize, void* payload) {
  start_handle_satb_roots();
  sent_satb_roots_req = false;
  handleRoot(bufferSize, payload);
  finish_handle_satb_roots();
}

void NoTransSnicCopyRegionClient::handle_satb_roots_commit(int bufferSize, void* payload) {
  handle_satb_roots(bufferSize, payload);
  // check whether all queues are empty before commit.
  for (uint i = 0; i < (snic_gc_workers->active_workers()); ++i) {
    auto task_queue = get_queue(i);
    if (!task_queue->is_empty()) {
      log_debug(gc)("DGC LOG: queue %u is not empty, size:%u", i, task_queue->size());
      // exit(0);
    }
    auto pending_map = workerPendingMaps[i];
    for (size_t j = 0; j < heapRegionNumber; ++j) {
      if (!pending_map->workerLocalVec[j]->is_empty()) {
        log_debug(gc)("DGC LOG: pending queue for worker %u region %lu is not empty, size:%u", i, j, pending_map->workerLocalVec[j]->size());
        // exit(0);
      }
    }
  }
  // commit client side local data.
  for (int i = 0; i < (int)(snic_gc_workers->active_workers()); ++i) {
    for (size_t j = 0; j < heapRegionNumber; ++j) {
      if (live_data_caches[i][j] > 0) {
        region_info[j]->_live_data += (size_t)(live_data_caches[i][j]);
        live_data_caches[i][j] = 0;
      }
    }
  }
  int total_live = 0;
  for (int i = 0; i < (int)heapRegionNumber; ++i) {
    if (region_info[i] == NULL) {
      continue;
    }
    live_count[i] = (uint64_t)(region_info[i]->_live_data);
    total_live += live_count[i];
  }
  writeBitmapRDMA();
  writeLivenessRDMA();
  waitBitmapCQ();
  waitLivenessCQ();
  send_back_int_ack((size_t)(NT_SATB_ROOTS_FINISH_ACK));
  log_debug(gc)("GC(%u)DGC LOG: liveness count: %d B.", _gc_id, total_live * HeapWordSize);
  log_debug(gc)("DGC LOG:start memset after commit liveness, bitmap and gc_roots_buffer");
  // memset bitmap
  memset(bitmap->_map, 0, bitmap->_size / 8);
  // memset livedata
  memset(live_count, 0, heapRegionNumber * sizeof(live_count[0]));
  // memset gc_roots_buffer
  size_t gc_roots_buffer_len = sizeof(gc_roots_buffer);
  memset(gc_roots_buffer, (char) (0), gc_roots_buffer_len);
  log_debug(gc)("DGC LOG:finish memset after commit liveness, bitmap and gc_roots_buffer");
  for(int i = 0; i < (int) snic_gc_workers->active_workers(); ++i){
    regionArrivalQueue[i]->clear();
  }
}

void NoTransSnicCopyRegionClient::handleRootAndCommit(int bufferSize, void* payload) {
  start_handle_satb_roots();
  sent_satb_roots_req = false;
  satb_handle_cnt += 1;
  handleRoot(bufferSize, payload);
  if (SnicConcCopyRegion) {
    is_one_gc_final_stage = true;
  }
  finish_handle_satb_roots();
  if (satb_handle_cnt == SnicSATBRootsSplitPartNum) {
    log_debug(gc)("DGC LOG: finish final satb part handling logic");
    wait_thread_complete();
    // aagregate every heapRegion's live count.
    for (int i = 0; i < (int)(snic_gc_workers->active_workers()); ++i) {
      for (size_t j = 0; j < heapRegionNumber; ++j) {
        if (live_data_caches[i][j] > 0) {
          region_info[j]->_live_data += (size_t)(live_data_caches[i][j]);
          live_data_caches[i][j] = 0;
        }
      }
    }
    int total_live = 0;
    for (int i = 0; i < (int)heapRegionNumber; ++i) {
      if (region_info[i] == NULL) {
        continue;
      }
      live_count[i] = (uint64_t)(region_info[i]->_live_data);
      // log_debug(gc)("DGC LOG:region %u live count:%lu", i, live_count[i]);
      total_live += live_count[i];
    }
    log_debug(gc)("GC(%u)DGC LOG: liveness count: %d B", _gc_id, total_live * HeapWordSize);
    writeBitmapRDMA();
    writeLivenessRDMA();
    waitBitmapCQ();
    waitLivenessCQ();
    send_back_int_ack(1);
  }
}

void NoTransSnicCopyRegionClient::initBitmapQP(void* ptr, size_t sz, int hostId) {
  qp_bitmap = RC::create(nic, QPConfig()).value();

  auto qp_res2 = cm->cc_rc("client-qp-bitmap", qp_bitmap, 0, QPConfig());
  RDMA_ASSERT(qp_res2 == IOCode::Ok) << std::get<0>(qp_res2.desc);
  //log_dev_debug(gc)("DGC LOG: qp_bitmap get");
  auto key2 = std::get<1>(qp_res2.desc);
  //log_dev_debug(gc)("DGC LOG: key get");

  //log_dev_debug(gc)("DGC LOG: start fetch remote mr for bitmap");
  auto fetch_res_bitmap = cm->fetch_remote_mr(compute_mr_idx(hostId, 1));
  RDMA_ASSERT(fetch_res_bitmap == IOCode::Ok) << std::get<0>(fetch_res_bitmap.desc);
  rmem::RegAttr remote_attr_bitmap = std::get<1>(fetch_res_bitmap.desc);
  //log_dev_debug(gc)("DGC LOG: start bind_remote_mr for bitmap");

  qp_bitmap->bind_remote_mr(remote_attr_bitmap);
  remoteBitmapBase = remote_attr_bitmap.buf;
  remoteBitmapSize = remote_attr_bitmap.sz;

  local_mem_bitmap = Arc<RMem>(new RMem(ptr, sz));
  local_mr_bitmap = RegHandler::create(local_mem_bitmap, nic).value();
  qp_bitmap->bind_local_mr(local_mr_bitmap->get_reg_attr().value());
}

void NoTransSnicCopyRegionClient::initLivenessQP(void* ptr, size_t sz, int hostId) {
  qp_liveness = RC::create(nic, QPConfig()).value();

  auto qp_res2 = cm->cc_rc("client-qp-liveness", qp_liveness, 0, QPConfig());
  RDMA_ASSERT(qp_res2 == IOCode::Ok) << std::get<0>(qp_res2.desc);
  //log_dev_debug(gc)("DGC LOG: qp liveness get");
  auto key2 = std::get<1>(qp_res2.desc);
  //log_dev_debug(gc)("DGC LOG: key get");

  //log_dev_debug(gc)("DGC LOG: start fetch remote mr for liveness");
  auto fetch_res = cm->fetch_remote_mr(compute_mr_idx(hostId, 2));
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  //log_dev_debug(gc)("DGC LOG: start bind_remote_mr for liveness");

  qp_liveness->bind_remote_mr(remote_attr);
  remoteLivenessBase = remote_attr.buf;
  remoteLivenessSize = remote_attr.sz;

  local_mem_liveness = Arc<RMem>(new RMem(ptr, sz));
  local_mr_liveness = RegHandler::create(local_mem_liveness, nic).value();
  qp_liveness->bind_local_mr(local_mr_liveness->get_reg_attr().value());
}

void NoTransSnicCopyRegionClient::initGCRootsBufferQP(void* ptr, size_t sz, int hostId) {
  qp_gc_roots_buffer = RC::create(nic, QPConfig()).value();
  auto qp_res3 = cm->cc_rc("client-qp-gc-roots-buffer", qp_gc_roots_buffer, 0, QPConfig());
  RDMA_ASSERT(qp_res3 == IOCode::Ok) << std::get<0>(qp_res3.desc);
  auto key3 = std::get<1>(qp_res3.desc);
  auto fetch_res = cm->fetch_remote_mr(compute_mr_idx(hostId, 3));
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  qp_gc_roots_buffer->bind_remote_mr(remote_attr);
  remoteGCRootsBufferBase = remote_attr.buf;
  remoteGCRootsBufferSize = remote_attr.sz;
  local_mem_gc_roots_buffer = Arc<RMem>(new RMem(ptr, sz));
  local_mr_gc_roots_buffer = RegHandler::create(local_mem_gc_roots_buffer, nic).value();
  qp_gc_roots_buffer->bind_local_mr(local_mr_gc_roots_buffer->get_reg_attr().value());
  log_debug(gc)("DGC LOG: client local gc roots buffer:%p,remote buffer base:%llx,remote buffer size:%llu", gc_roots_buffer, remoteGCRootsBufferBase, remoteGCRootsBufferSize);
}

void NoTransSnicCopyRegionClient::initForceGCByDPUClientQP(void* ptr, size_t sz, int hostId) {
  qp_force_gc_by_dpu_client = RC::create(nic, QPConfig()).value();
  auto qp_res4 = cm->cc_rc("client-qp-force-gc-by-dpu-client", qp_force_gc_by_dpu_client, 0, QPConfig());
  RDMA_ASSERT(qp_res4 == IOCode::Ok) << std::get<0>(qp_res4.desc);
  auto key4 = std::get<1>(qp_res4.desc);
  auto fetch_res = cm->fetch_remote_mr(compute_mr_idx(hostId, 5));
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  qp_force_gc_by_dpu_client->bind_remote_mr(remote_attr);
  remoteForceGCByDPUClientBase = remote_attr.buf;
  remoteForceGCByDPUClientSize = remote_attr.sz;
  local_mem_force_gc_by_dpu_client = Arc<RMem>(new RMem(ptr, sz));
  local_mr_force_gc_by_dpu_client = RegHandler::create(local_mem_force_gc_by_dpu_client, nic).value();
  qp_force_gc_by_dpu_client->bind_local_mr(local_mr_force_gc_by_dpu_client->get_reg_attr().value());
  log_debug(gc)("DGC LOG: client local force gc by dpu client:%p,remote buffer base:%llx,remote buffer size:%llu", local_force_gc_by_dpu_client, remoteForceGCByDPUClientBase, remoteForceGCByDPUClientSize);
}

void NoTransSnicCopyRegionClient::initRdmaPrefetchFinishFlagQP(void* ptr, size_t sz, int hostId) {
  qp_rdma_prefetch_finish_flag = RC::create(nic, QPConfig()).value();
  auto qp_res = cm->cc_rc("client-qp-rdma-prefetch-finish-flag", qp_rdma_prefetch_finish_flag, 0, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  auto key = std::get<1>(qp_res.desc);
  auto fetch_res = cm->fetch_remote_mr(compute_mr_idx(hostId, 7));
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
  qp_rdma_prefetch_finish_flag->bind_remote_mr(remote_attr);
  remoteRdmaPrefetchFinishFlagBase = remote_attr.buf;
  remoteRdmaPrefetchFinishFlagSize = remote_attr.sz;
  local_mem_rdma_prefetch_finish_flag = Arc<RMem>(new RMem(ptr, sz));
  local_mr_rdma_prefetch_finish_flag = RegHandler::create(local_mem_rdma_prefetch_finish_flag, nic).value();
  qp_rdma_prefetch_finish_flag->bind_local_mr(local_mr_rdma_prefetch_finish_flag->get_reg_attr().value());
  log_debug(gc)("DGC LOG: client local rdma prefetch finish flag:%p,remote buffer base:%llx,remote buffer size:%llu", local_rdma_prefetch_finish_flag, remoteRdmaPrefetchFinishFlagBase, remoteRdmaPrefetchFinishFlagSize);
}

void NoTransSnicCopyRegionClient::writeForceGCByDPUClientRDMA() {
  auto res_s = qp_force_gc_by_dpu_client->send_normal_direct(
      {.op = IBV_WR_RDMA_WRITE,
       .flags = IBV_SEND_SIGNALED,
       .len = (unsigned int)(remoteForceGCByDPUClientSize),
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_force_gc_by_dpu_client),
       .remote_addr = remoteForceGCByDPUClientBase,
       .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
}

void NoTransSnicCopyRegionClient::waitForceGCByDPUClientCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for force gc by dpu client cq", _gc_id);
  auto res_p = qp_force_gc_by_dpu_client->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void NoTransSnicCopyRegionClient::writeBitmapRDMA(){
  auto res_s = qp_bitmap->send_normal_direct(
    {.op = IBV_WR_RDMA_WRITE,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(bitmap->_size / 8),
      .wr_id = 0},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(bitmap->_map),
      .remote_addr = remoteBitmapBase,
      .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
}
void NoTransSnicCopyRegionClient::waitBitmapCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for bitmap cq", _gc_id);
  auto res_p = qp_bitmap->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void NoTransSnicCopyRegionClient::writeLivenessRDMA(){
  auto res_s = qp_liveness->send_normal_direct(
    {.op = IBV_WR_RDMA_WRITE,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(remoteLivenessSize),
      .wr_id = 0},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(live_count),
      .remote_addr = remoteLivenessBase,
      .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
}

void NoTransSnicCopyRegionClient::waitLivenessCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for liveness cq", _gc_id);
  auto res_p = qp_liveness->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void NoTransSnicCopyRegionClient::writeRdmaPrefetchFinishFlagRDMA() {
  auto res_s = qp_rdma_prefetch_finish_flag->send_normal_direct(
      {.op = IBV_WR_RDMA_WRITE,
       .flags = IBV_SEND_SIGNALED,
       .len = (unsigned int)(remoteRdmaPrefetchFinishFlagSize),
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_rdma_prefetch_finish_flag),
       .remote_addr = remoteRdmaPrefetchFinishFlagBase,
       .imm_data = 0});
  if (res_s != IOCode::Ok) {
    log_error(gc)("GC(%u)DGC LOG: write rdma prefetch finish flag failed", _gc_id);
    exit(0);
  }
}

void NoTransSnicCopyRegionClient::waitRdmaPrefetchFinishFlagCQ() {
  log_debug(gc)("GC(%u)DGC LOG: waiting for rdma prefetch finish flag cq", _gc_id);
  auto res_p = qp_rdma_prefetch_finish_flag->wait_one_comp();
  if (res_p != IOCode::Ok) {
    log_error(gc)("GC(%u)DGC LOG: wait rdma prefetch finish flag failed", _gc_id);
    exit(0);
  }
}

void NoTransSnicCopyRegionClient::handleNewVirtualSpaceNode(int hostId, int bufferSize, void* payload){
  // int N = 1;
  assert(bufferSize == sizeof(int))
  int* message = (int*)payload;
  SnicVirtualSpaceNode* newNode = new SnicVirtualSpaceNode();
  newNode->hostId = hostId;
  newNode->index = (int)message[0];

  newNode->qp = RC::create(nic, QPConfig()).value();
  std::string qp_name = std::string("client-qp-virtualNode-") + std::to_string(hostId) + std::string("-") + std::to_string(newNode->index);
  auto qp_res = cm->cc_rc(qp_name, newNode->qp, 0, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  //log_dev_debug(gc)("DGC LOG: virtual node qp get");
  auto key = std::get<1>(qp_res.desc);
  //log_dev_debug(gc)("DGC LOG: key get");

  //log_dev_debug(gc)("DGC LOG: start fetch remote mr for virtual node");
  auto target_mr_idx = compute_mr_idx(hostId, newNode->index);
  log_debug(gc)("DGC LOG: target mr in rpc 5:%lu", target_mr_idx);
  auto fetch_res_virtualspace = cm->fetch_remote_mr(target_mr_idx);
  RDMA_ASSERT(fetch_res_virtualspace == IOCode::Ok) << std::get<0>(fetch_res_virtualspace.desc);
  rmem::RegAttr remote_attr_virtualNode = std::get<1>(fetch_res_virtualspace.desc);
  //log_dev_debug(gc)("DGC LOG: start bind_remote_mr for virtual node");

  newNode->qp->bind_remote_mr(remote_attr_virtualNode);
  newNode->base = remote_attr_virtualNode.buf;
  newNode->sz = remote_attr_virtualNode.sz;
  newNode->top = newNode->base + newNode->sz;
  newNode->cur_top = newNode->base;
  log_debug(gc)("DGC LOG: host %d gets remote vsn at base=%p, top=%p, size=%llu", hostId, (void*)(newNode->base), (void*)(newNode->top), newNode->sz);
  newNode->map_fd = -1;
  newNode->real_local_base = newNode->base;

  // CDS archive at 0x800000000 is already loaded by the client JVM via
  // classes.jsa — its physical pages exist and can be registered directly at
  // the same VA. No shm dual-map needed.
  //
  // Everything else (CCS at 0x880000000, non-class Metaspace at a
  // kernel-picked VA like 0x7ffe80000000, and the legacy 0x801000000 layout)
  // collides with the client JVM's own reservations (CCS) or pins pages on
  // ranges whose VMA state the client JVM still owns. The old
  // `mmap(MAP_FIXED|MAP_ANONYMOUS)` overlay left the mlx5 MR translation in
  // an inconsistent state: the first 1-2 RDMA READs on the QP worked, then a
  // subsequent READ targeting the low offsets of CCS (where the client JVM
  // had committed classes before the overlay) failed with
  // IBV_WC_LOC_QP_OP_ERR / mlx5 vendor_err=0x68. See campaigns/dual-homog/
  // KNOWN_ISSUES.md §1.
  //
  // Fix: for every non-CDS VSN, create a POSIX shared-memory file of the
  // required size and map it twice — once at a client-picked VA where the
  // NIC MR is registered (no collision with client JVM mappings), and once
  // at newNode->base (for narrow-klass decoding from inside the DGC marker).
  // Both VAs alias the same physical pages, so RDMA READs land where the
  // marker expects.
  if (newNode->base != 0x800000000) {
    std::string shm_path = "/dpu_virtual_node_" + std::to_string(hostId)
                         + "_" + std::to_string(newNode->index)
                         + "_" + std::to_string(host_random_num);
    int fd = shm_open(shm_path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
    if (fd == -1 && errno == EEXIST) {
      // Stale file from a previous aborted run — clobber and retry.
      shm_unlink(shm_path.c_str());
      fd = shm_open(shm_path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
    }
    if (fd == -1) {
      log_error(gc)("DGC LOG: host %d shm_open(%s) failed: %s",
                    hostId, shm_path.c_str(), strerror(errno));
      exit(0);
    }
    if (ftruncate(fd, newNode->sz) == -1) {
      log_error(gc)("DGC LOG: host %d ftruncate(%s, %llu) failed: %s",
                    hostId, shm_path.c_str(), newNode->sz, strerror(errno));
      exit(0);
    }
    // Map 1: client-picked VA where ibv_reg_mr registers the MR. No
    // MAP_FIXED — let the kernel find free VA far from anything the client
    // JVM already uses.
    void* reg_at = mmap(NULL, newNode->sz, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    if (reg_at == MAP_FAILED) {
      log_error(gc)("DGC LOG: host %d mmap(NULL, %llu) failed: %s",
                    hostId, newNode->sz, strerror(errno));
      exit(0);
    }
    newNode->real_local_base = (unsigned long long) reg_at;
    // Map 2: aliased at newNode->base so narrow-klass decoding and any
    // direct pointer dereference from host VAs still works. MAP_FIXED here
    // atomically replaces whatever the client JVM had at that VA (e.g. its
    // own CCS reservation at 0x880000000).
    void* alias_at = mmap((void*)newNode->base, newNode->sz,
                          PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_FIXED, fd, 0);
    if (alias_at == MAP_FAILED || alias_at != (void*)newNode->base) {
      log_error(gc)("DGC LOG: host %d mmap alias at 0x%llx failed: got %p, %s",
                    hostId, newNode->base, alias_at, strerror(errno));
      exit(0);
    }
    // Unlink the name — the fd and the two mappings keep the inode alive.
    // Cleanup is automatic when the client process exits.
    shm_unlink(shm_path.c_str());
    newNode->map_fd = fd;
    log_debug(gc)("DGC LOG: host %d vsn[%d] dual-mapped: base=0x%llx reg_at=%p sz=%llu",
                  hostId, newNode->index, newNode->base, reg_at, newNode->sz);
  }

  newNode->local_mem = Arc<RMem>(new RMem((void *)(newNode->real_local_base), newNode->sz));
  auto reg_opt = RegHandler::create(newNode->local_mem, nic);
  if (!reg_opt.has_value()) {
    log_error(gc)("DGC LOG: host %d RegHandler::create failed for vsn[%d] at %p sz=%llu",
                  hostId, newNode->index, (void*)(newNode->real_local_base), newNode->sz);
    exit(0);
  }
  newNode->local_mr = reg_opt.value();
  newNode->qp->bind_local_mr(newNode->local_mr->get_reg_attr().value());
  virtualSpaceNodes.push_back(newNode);
}

// SnicGCRDMABatchFetchKlass — RPC 12 handler. Re-pull the *entire* host-side
// committed CCS range in a single RDMA READ per call. Works around the mlx5
// fw 32.42.1000 LOC_QP_OP_ERR bug by creating a brand-new QP for each
// bulk-sync (so the WR is always the 1st on its QP; the firmware quirk
// only trips on the 2nd+ WR).
//
// We re-read the whole range (not just a delta) because the host's class-
// linking layer can fill in a Klass's fields (vtable pointer, itable,
// method pointers) AFTER resolve_from_stream returns. A delta that
// snapshots the Klass at resolve time would freeze a partial copy on the
// client forever; a full re-read self-heals on each GC cycle once linking
// completes on the host side.
//
// Cost: ~sizeof-committed-CCS per GC cycle. For WildFly-sized apps that
// is ~10-50 MB → <5 ms at 100 Gbps RDMA, comfortably cheaper than a GC
// round. In steady state (no new classes) each sync still re-reads, but
// the data is mostly cache-hot on the host so end-to-end latency is
// dominated by the NIC round trip rather than DMA throughput.
void NoTransSnicCopyRegionClient::bulkSyncCcs(int hostId, unsigned long long ccs_hwm, unsigned long long ccs_base) {
  // Locate the CCS VirtualSpaceNode by the base address the host sent
  // alongside the HWM, so this lookup is robust to JVM ergonomics
  // placing CCS at any base derived from CDS layout / chunk alignment.
  SnicVirtualSpaceNode* ccs_node = nullptr;
  for (auto* node : virtualSpaceNodes) {
    if (node->base == ccs_base) { ccs_node = node; break; }
  }
  if (ccs_node == nullptr) {
    log_error(gc)("DGC LOG: bulkSyncCcs(host %d): no CCS VSN found at base=0x%llx (registered VSNs differ)",
                  hostId, ccs_base);
    return;
  }

  // Read from vsn->base up to the host's current HWM. Must re-read each
  // time — see function comment for why a delta is unsafe.
  unsigned long long from = ccs_node->base;
  if (ccs_hwm <= from) {
    log_debug(gc)("DGC LOG: bulkSyncCcs(host %d): no-op, hwm=0x%llx <= base=0x%llx", hostId, ccs_hwm, from);
    return;
  }
  unsigned long long delta = ccs_hwm - from;
  log_debug(gc)("DGC LOG: bulkSyncCcs(host %d): fetch whole CCS [0x%llx..0x%llx) = %llu bytes",
                hostId, from, ccs_hwm, delta);

  // Chunked transfer: mlx5 RC QPs cap RDMA READ to ~1 GiB per message (IB
  // max_msg_sz). For CCS that runs up to 2 GB on class-loading-heavy apps
  // (spring hit 2146959912 bytes on first run → IBV_WC_LOC_LEN_ERR=1).
  // Also: firmware bug forbids 2nd+ WR on a given QP. So each chunk uses
  // its own fresh QP.
  //
  // 512 MB is a safe headroom below the mlx5 advertised message size, and
  // 4 chunks of 512 MB covers the full 2 GB CCS. Cost of 4 QP creations
  // (~5 ms each) is ~20 ms per sync — paid only when CCS grew past the
  // single-chunk threshold.
  static const unsigned long long kChunk = 512ull * 1024ull * 1024ull; // 512 MiB
  static std::atomic<int> sync_epoch{0};

  unsigned long long cursor = from;
  while (cursor < ccs_hwm) {
    unsigned long long this_len = ccs_hwm - cursor;
    if (this_len > kChunk) this_len = kChunk;

    int epoch = sync_epoch.fetch_add(1) + 1;
    auto fresh_qp = rdmaio::qp::RC::create(nic, rdmaio::qp::QPConfig()).value();
    std::string qp_name = std::string("client-qp-ccs-sync-") + std::to_string(hostId)
                        + "-" + std::to_string(epoch);
    auto qp_res = cm->cc_rc(qp_name, fresh_qp, 0, rdmaio::qp::QPConfig());
    if (qp_res != rdmaio::IOCode::Ok) {
      log_error(gc)("DGC LOG: bulkSyncCcs cc_rc(%s) failed: %s",
                    qp_name.c_str(), std::get<0>(qp_res.desc).c_str());
      exit(0);
    }
    auto remote_fetch = cm->fetch_remote_mr(compute_mr_idx(hostId, ccs_node->index));
    if (remote_fetch != rdmaio::IOCode::Ok) {
      log_error(gc)("DGC LOG: bulkSyncCcs fetch_remote_mr failed: %s",
                    std::get<0>(remote_fetch.desc).c_str());
      exit(0);
    }
    rdmaio::rmem::RegAttr remote_attr = std::get<1>(remote_fetch.desc);
    fresh_qp->bind_remote_mr(remote_attr);
    fresh_qp->bind_local_mr(ccs_node->local_mr->get_reg_attr().value());

    unsigned long long local_addr = cursor - ccs_node->base + ccs_node->real_local_base;
    auto res_s = fresh_qp->send_normal_direct(
        {.op = IBV_WR_RDMA_READ,
         .flags = IBV_SEND_SIGNALED,
         .len = (unsigned int)this_len,
         .wr_id = 0},
        {.local_addr = reinterpret_cast<rdmaio::rmem::RMem::raw_ptr_t>(local_addr),
         .remote_addr = cursor,
         .imm_data = 0});
    if (res_s != rdmaio::IOCode::Ok) {
      log_error(gc)("DGC LOG: bulkSyncCcs ibv_post_send failed hostId=%d cursor=0x%llx len=%llu",
                    hostId, cursor, this_len);
      exit(0);
    }
    auto res_p = fresh_qp->wait_one_comp();
    if (res_p != rdmaio::IOCode::Ok) {
      ibv_wc wc = res_p.desc;
      log_error(gc)("DGC LOG: bulkSyncCcs wait_one_comp FAIL hostId=%d cursor=0x%llx len=%llu wc.status=%d(%s) wc.vendor_err=0x%x wc.qp_num=%u",
                    hostId, cursor, this_len,
                    wc.status, ibv_wc_status_str(wc.status), wc.vendor_err, wc.qp_num);
      exit(0);
    }

    cursor += this_len;
  }

  log_debug(gc)("DGC LOG: bulkSyncCcs(host %d) ok, ccs_hwm=0x%llx, delta=%llu",
                hostId, ccs_hwm, delta);
}

void NoTransSnicCopyRegionClient::fetchKlass(int hostId, int bufferSize, void* payload){

  // SnicGCRDMABatchFetchKlass: per-class RPC 6 is never sent by the host in
  // batch mode; defensive no-op if we somehow receive one (e.g. mixed-mode
  // replay of a pre-flag run or coordinator misconfiguration).
  if (SnicGCRDMABatchFetchKlass) {
    log_debug(gc)("DGC LOG: fetchKlass received in batch mode — ignoring (host-side gate should have skipped RPC 6)");
    return;
  }

  unsigned long long *message = (unsigned long long *)payload;
  unsigned long long klass_ptr = message[0];
  unsigned long long klass_sz = message[1];
  //log_dev_debug(gc)("fetch klass with klass_ptr=%llx, sz=%llx", klass_ptr, klass_sz);

  rdmaio::Arc<rdmaio::qp::RC> qp_vsn = nullptr;
  int target_node_idx = -1;
  int nodes_size = virtualSpaceNodes.size();
  for(int i=0; i < nodes_size; i++){
    if(klass_ptr >= virtualSpaceNodes[i]->base && klass_ptr < virtualSpaceNodes[i]->top){
      qp_vsn = virtualSpaceNodes[i]->qp;
      target_node_idx = i;
      break;
    }
  }
  if(qp_vsn == nullptr){
    log_error(gc)("Fail to find matched vsn");
    exit(0);
  }
  unsigned long long local_addr = klass_ptr
      - virtualSpaceNodes[target_node_idx]->base
      + virtualSpaceNodes[target_node_idx]->real_local_base;
  auto res_s = qp_vsn->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
       .flags = IBV_SEND_SIGNALED,
       .len = (unsigned int)(klass_sz),
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_addr),
       .remote_addr = klass_ptr,
       .imm_data = 0});
  if (res_s != IOCode::Ok) {
    log_error(gc)("DGC LOG: fetchKlass ibv_post_send failed hostId=%d vsn_idx=%d klass=0x%llx sz=0x%llx local=0x%llx",
                  hostId, target_node_idx, klass_ptr, klass_sz, local_addr);
    exit(0);
  }

  auto res_p = qp_vsn->wait_one_comp();
  if (res_p != IOCode::Ok) {
    ibv_wc wc = res_p.desc;
    log_error(gc)("DGC LOG: fetchKlass wait_one_comp FAIL hostId=%d vsn_idx=%d vsn.base=0x%llx klass=0x%llx sz=0x%llx local=0x%llx wc.status=%d(%s) wc.vendor_err=0x%x wc.wr_id=0x%llx wc.qp_num=%u",
                  hostId, target_node_idx,
                  virtualSpaceNodes[target_node_idx]->base,
                  klass_ptr, klass_sz, local_addr,
                  wc.status, ibv_wc_status_str(wc.status),
                  wc.vendor_err,
                  (unsigned long long)wc.wr_id, wc.qp_num);
    exit(0);
  }

  unsigned long long klass_end = klass_ptr + klass_sz;
  if (klass_end > virtualSpaceNodes[target_node_idx]->cur_top) {
    virtualSpaceNodes[target_node_idx]->cur_top = klass_end;
  }
}

void NoTransSnicCopyRegionClient::checkRegionArrivalQueue(uint worker_id) {
  int cur_region_idx = -1;
  if(!regionArrivalQueue[worker_id]->is_empty()){
    cur_region_idx = regionArrivalQueue[worker_id]->pop();
  }
  auto worker_queue = get_queue(worker_id);
  while(cur_region_idx != -1){
    for (uint src_worker_id = 0; src_worker_id < snic_gc_workers->active_workers(); ++src_worker_id) {
      // push all pending tasks of this region into this worker's task queue.
      ShenandoahMarkTask task;
      auto taskQueue = workerPendingMaps[src_worker_id]->workerLocalVec[cur_region_idx];
      // std::queue<ShenandoahMarkTask> tmp_task_queue;
      // log_debug(gc)("DGC LOG: src_worker_id: %u, cur_region_idx: %d", src_worker_id, cur_region_idx);
      while (taskQueue->pop(task)) {
        bool upgraded = false;
        oop obj = task.obj();
        // printf("%p\n", obj);
        if((unsigned long long)obj < (unsigned long long)heapBase || (unsigned long long)obj > (unsigned long long)heapBase + (unsigned long long)heapSize){
          log_error(gc)("DGC LOG: ASSERT! obj_addr=%llx, worker_id=%u, src_worker_id=%u, cur_region_idx=%u", (unsigned long long)obj, worker_id, src_worker_id, cur_region_idx);
          exit(0);
        }
        if (region_info[cur_region_idx] == NULL ||
            (unsigned long long)obj >= region_info[cur_region_idx]->top) {
          continue;
        }
        bool is_humongous_obj = false;
        int uncopied_region_idx = -1;
        if (oop_is_humongous(obj)) {
          bool push_success = false;
          while (!humongous_oop_regions_copy_finished(obj, &uncopied_region_idx)) {
            if(workerPendingMaps[worker_id]->push(uncopied_region_idx, task, region_copied_bitmap)){
              push_success = true;
              break;
            }
          }
          if (push_success) {
            continue;
          }
        }
        bool mark_rst = bitmap->mark_strong((HeapWord*) (obj), upgraded);
        if (mark_rst || is_humongous_obj) {
          ShenandoahMarkTask oneTask(obj, upgraded, false);
          worker_queue->push(oneTask);
          if (mark_rst) {
            snic_concurrent_inc_liveness(cur_region_idx, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
          }
        }
      }
      // while (!tmp_task_queue.empty()) {
      //   taskQueue->push(tmp_task_queue.front());
      //   tmp_task_queue.pop();
      // }
    }
    cur_region_idx = regionArrivalQueue[worker_id]->pop();
  }
}


bool NoTransSnicCopyRegionClient::checkTerminateCondition(uint worker_id) {
  bool ret = false;
  uint active_worker_num = snic_gc_workers->active_workers();
  task_count_lock.lock();
  finished_worker_count++;
  if ((uint)(finished_worker_count) == active_worker_num) {
    task_count_lock.unlock();
    return true;
  }
  // compute all tasks num in all queues.
  int left_tasks_num = 0;
  for (uint i = 0; i < active_worker_num; ++i) {
    auto que = get_queue(i);
    left_tasks_num += que->size();
    left_tasks_num += que->overflow_stack()->size();
  }
  // FIX (noTrans-isolation): also account for in-flight region copies and
  // arrivals. If wait_part_copy_finish_work is still streaming regions
  // into regionArrivalQueue, the marker MUST wait — otherwise it
  // terminates with an incomplete bitmap, the host marks 92%+ of heap
  // as dead garbage, frees regions whose objects are reachable via
  // late-arriving references, and the mutator subsequently dereferences
  // one of those freed objects (the JVM_ArrayCopy / null-dst crash).
  // We also block on outstanding regionArrivalQueue entries because the
  // worker that polls a queue may not be `worker_id`.
  if (left_tasks_num == 0 && cur_handling_rpc_type == 2) {
    if (on_trans_region_number > 0 ||
        done_trans_region_number < (size_t)received_region_num) {
      // Still copying. Don't terminate yet.
      finished_worker_count--;
      task_count_lock.unlock();
      return false;
    }
    for (uint i = 0; i < active_worker_num; ++i) {
      if (!regionArrivalQueue[i]->is_empty()) {
        // A region just arrived; some worker will pick it up. Spin again.
        finished_worker_count--;
        task_count_lock.unlock();
        return false;
      }
    }
  }
  if (left_tasks_num == 0) {
    ret = true;
  } else {
    // case: a worker fail to steal/pop new task, but still some tasks left, return false to try to steal again.
    finished_worker_count--;
  }
  task_count_lock.unlock();
  return ret;
}

void NoTransSnicCopyRegionClient::snic_do_compressed_oops(uint worker_id) {
  auto worker_queue = get_queue(worker_id);
  ShenandoahMarkTask popTask;
  uint work_count = 0;
  uint steal_count = 0;
  bool checked_worker_pending_maps = false;
  uint64_t worker_total_liveness = 0;
  int has_sleeped = 0;
  while (true) {
    auto should_force_finish = Atomic::load(&should_force_tasks_finish);
    if (is_prefetched == 0 && !SnicGCCoorHeuristic) {
      if(should_force_finish > 0 && cur_handling_rpc_type != 10) {
        if (Atomic::add(&force_finished_task_cnt, 1) == (int) (snic_gc_workers->active_workers())) {
          log_debug(gc)("DGC LOG: force workers terminate, last worker:%u", worker_id);
          // Atomic::store(&should_force_tasks_finish, should_force_finish - 1);
          Atomic::sub(&should_force_tasks_finish, 1);
          // Atomic::store(&force_finished_task_cnt, 0);
          for (uint j = 0; j < snic_gc_workers->active_workers(); ++j) {
            auto que = get_queue(j);
            auto left_size = que->size() + que->overflow_stack()->size();
            log_debug(gc)("DGC LOG: worker %u left size:%lu", j, left_size);
          }
        }
        break;
      } else {
        if (should_force_finish == 0 && has_sleeped > 0 && normal_satb_roots_handle_count >= force_terminate_counts) {
          break;
        }
      }
    }
    if(!regionArrivalQueue[worker_id]->is_empty()){
      checkRegionArrivalQueue(worker_id);
    }
    if (!worker_queue->pop(popTask)) {
      if (!task_queues->steal(worker_id, popTask)) {
        if (SnicConcCopyRegion) {
          if (is_prefetched == 0) {
            // logic to support concurrent copy region
            if (!SnicGCCoorHeuristic) {
              if (cur_handling_rpc_type == 10) {
                if (checkTerminateCondition(worker_id)) {
                  break;
                }
                os::naked_short_sleep(1);
                continue;
              }
              os::naked_short_sleep(1);
              has_sleeped += 1;
              // log_info(gc)("cur terminate:%d,cur finish count:%d", should_force_tasks_finish, force_finished_task_cnt);
              if (has_sleeped % 10000 == 0) {
                log_debug(gc)("DGC LOG: worker %u has slept for %u times", worker_id, has_sleeped);
                exit(0);
              }
              continue;
            } else {
              if (cur_handling_rpc_type == 10 || cur_handling_rpc_type == 2) {
                if (checkTerminateCondition(worker_id)) {
                  break;
                }
                // OPT#2: yield instead of 1ms sleep so termination detection is
                // prompt. With 1ms naked_short_sleep, each idle worker waited up
                // to 1ms before re-checking; for small benchmarks the whole
                // marking phase spent 10-30ms in these polls. naked_yield gives
                // other runnable threads (RPC handler, other workers) a chance
                // and re-runs us within microseconds.
                os::naked_yield();
                continue;
              } else {
                break;
              }
            }
          } else {
            break;
          }
        } else {
          if (checkTerminateCondition(worker_id)) {
            if (cur_handling_rpc_type == 10) {
              break;
            }
            os::naked_short_sleep(1);
            has_sleeped += 1;
          }
          continue;
        }
      } else {
        steal_count += 1;
      }
    }
    oop obj = popTask.obj();
    work_count++;
    if (DpuClientLivenessUpdateEnabled && work_count % DpuClientLivenessUpdateThreshold == 0) {
      uint64_t total_liveness_tmp = 0;
      for (size_t i = 0; i < heapRegionNumber; ++i) {
        total_liveness_tmp += live_data_caches[worker_id][i];
        // Atomic::add(&(region_info[i]->_live_data), live_data_caches[worker_id][i]);
        // live_data_caches[worker_id][i] = 0;
      }
      log_debug(gc)("DGC LOG: worker %u sends back liveness %lu", worker_id, (size_t) (total_liveness_tmp - worker_total_liveness));
      send_back_int_ack((size_t) (total_liveness_tmp - worker_total_liveness));
      worker_total_liveness = total_liveness_tmp;
    }
    if (SnicGCCoorHeuristic && work_count % NT_RDMA_DGC_LIVENESS_UPDATE_THRESHOLD == 0) {
      uint64_t total_liveness_tmp = 0;
      for (size_t i = 0; i < heapRegionNumber; ++i) {
        total_liveness_tmp += live_data_caches[worker_id][i];
      }
      auto target_liveness = total_liveness_tmp - prev_total_liveness[worker_id];
      prev_total_liveness[worker_id] = total_liveness_tmp;
      add_marked_liveness(target_liveness);
    }
    bool should_count_liveness = popTask.count_liveness();
    if (obj->klass()->id() == ObjArrayKlassID) {
      objArrayOop a = objArrayOop(obj);
      unsigned long long start_region_idx = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)heapBase) >> regionSizeShift;
      // log_debug(gc)("DGC LOG:objArrayObj:%p,%d", a, a->length());
      narrowOop* p = (narrowOop*)(a->base());
      narrowOop* const end = p + a->length();
      // if (oop_is_humongous(obj)) {
      //   if (!humongous_oop_regions_copy_finished(obj)) {
      //     ShenandoahMarkTask oneTask(obj, false, false);
      //     workerPendingMaps[worker_id]->push(start_region_idx, oneTask);
      //     continue;
      //   }
      // }
      for (; p < end; p++) {
        // unsigned long long r_idx = (reinterpret_cast<unsigned long long>(p) - (unsigned long long)heapBase) >> regionSizeShift;
        // if (region_info[r_idx] != NULL) {
        //   if (region_copied_bitmap[r_idx] != (char) (NT_REGION_COPIED)) {
        //     log_info(gc)("worker %u push obj array %p to pending queue %llu due to empty region %llu", worker_id, obj, start_region_idx, r_idx);
        //     // push the entire ObjArrayOop to pending queue.
        //     ShenandoahMarkTask oneTask(obj, false, false);
        //     workerPendingMaps[worker_id]->push(start_region_idx, oneTask);
        //     break;
        //   } else {
        //     snic_do_compressed_oop(p, worker_id, 1, should_count_liveness);
        //   }
        // }
        snic_do_compressed_oop(p, worker_id, 1, should_count_liveness);
      }
    } else if (InstanceKlass::cast(obj->klass())->reference_type() != REF_NONE) {
      // log_debug(gc)("DGC LOG:InstanceRefObj:%p", obj);
      OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
      OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
      for (; map < end_map; ++map) {
        narrowOop* p = (narrowOop *)obj->field_addr(map->offset());
        // log_debug(gc)("DGC LOG:first field addr 1:%p, map count:%u,map off:%d", p, map->count(), map->offset());
        narrowOop* end = p + map->count();
        for (; p < end; ++p) {
          // log_debug(gc)("DGC LOG:instanceObj field 1:%p", p);
          snic_do_compressed_oop(p, worker_id, 2, should_count_liveness);
        }
      }
      narrowOop* referent_addr = NULL;
      if (UseCompressedClassPointers) {
        referent_addr = (narrowOop *)((unsigned long long) (obj) + 12);
      } else {
        referent_addr = (narrowOop *)((unsigned long long) (obj) + 16);
      }
      if (true) {
        // log_debug(gc)("DGC LOG:do_compressed_oop for referent_addr %p", referent_addr);
        snic_do_compressed_oop(referent_addr, worker_id, 3, should_count_liveness);
      }
    } else {
      // log_debug(gc)("DGC LOG:normalInstanceObj:%p,%p", obj, CompressedOops::decode_not_null(obj));
      OopMapBlock *map = ((InstanceKlass *)obj->klass())->start_of_nonstatic_oop_maps();
      OopMapBlock *end_map = map + ((InstanceKlass *)obj->klass())->nonstatic_oop_map_count();
      // log_debug(gc)("DGC LOG: OopMapBlock map=%p, end_map=%p, oopMapBlock size=%lu", map, end_map, sizeof(OopMapBlock));
      for (; map < end_map; ++map) {
        narrowOop *p = (narrowOop*) obj->field_addr(map->offset());
        // log_debug(gc)("DGC LOG:first field addr:%p,map count:%u", p, map->count());
        narrowOop *end = p + map->count();
        for (; p < end; ++p) {
          // log_debug(gc)("DGC LOG:instanceObj field:%p", p);
          snic_do_compressed_oop(p, worker_id, 2, should_count_liveness);
        }
      }
      // special check for Mirror Klass's static field
      if (obj->klass()->id() == InstanceMirrorKlassID) {
        // log_debug(gc)("DGC LOG:normalInstanceMirrorObj:%p", obj);
        narrowOop *p = (narrowOop*) ((InstanceMirrorKlass *)obj->klass())->start_of_static_fields(obj);
        narrowOop *const end = p + java_lang_Class::static_oop_field_count_raw(obj);
        // log_debug(gc)("DGC LOG:start_of_static_fields for %p:%p,static field count:%d", obj, p, java_lang_Class::static_oop_field_count_raw(obj));
        for (; p < end; ++p) {
          snic_do_compressed_oop(p, worker_id, 4, should_count_liveness);
        }
      }
    }
    // if (popTask.count_liveness()) {
    //   unsigned long long r_idx = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)heapBase) >> regionSizeShift;
    //   snic_concurrent_inc_liveness(r_idx, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
    // }
  }
  log_debug(gc)("GC(%u,%d) DGC LOG: worker %u finish snic_do_compressed_oops, work_count:%u,steal count:%u", _gc_id, clientId, worker_id, work_count, steal_count);
}

void NoTransSnicCopyRegionClient::snic_do_compressed_oop(narrowOop* p, uint worker_id, int type, bool should_count_liveness) {
  narrowOop o = RawAccess<>::oop_load(p);
  auto worker_queue = get_queue(worker_id);
  if (!CompressedOops::is_null(o)) {
    // try_mark_counts[(worker_id + 1) * 8 - 1]++;
    // oop obj = CompressedOops::decode_not_null(o);
    oop obj = cast_to_oop((uintptr_t) (COMPRESSED_OOP_BASE) + ((uintptr_t)(o) << COMPRESSED_OOP_SHIFT));
    bool upgraded = false;
    bool rst = bitmap->is_marked((HeapWord *)obj);
    if (!rst) {
      unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)heapBase) >> regionSizeShift;
      // auto it = region_info.find(r_idx_o);
      auto it = region_info[r_idx_o];
      if (it != NULL) {
        unsigned long long obj_addr = (unsigned long long)obj;
        if (obj_addr < it->top) {
          // logic to support concurrent copy region

          if (SnicConcCopyRegion && region_copied_bitmap[r_idx_o] != (char) (NT_REGION_COPIED)) {
            ShenandoahMarkTask oneTask(obj, false, false);
            if (workerPendingMaps[worker_id]->push(r_idx_o, oneTask, region_copied_bitmap)) {
              if (is_prefetched == 1) {
                log_debug(gc)("DGC LOG: worker %u push region %llu to pending map (3), is_prefetched=%llu", worker_id, r_idx_o, is_prefetched);
              }
              return;
            }
          }
          if (oop_is_humongous(obj)) {
            int uncopied_region_idx = -1;
            if (!humongous_oop_regions_copy_finished(obj, &uncopied_region_idx)) {
              ShenandoahMarkTask oneTask(obj, false, false);
              if (workerPendingMaps[worker_id]->push(uncopied_region_idx, oneTask, region_copied_bitmap)) {
                if (is_prefetched == 1) {
                  log_debug(gc)("DGC LOG: worker %u push region %d to pending map (4), is_prefetched=%llu", worker_id, uncopied_region_idx, is_prefetched);
                }
                return;
              }
            }
          }
          if (region_info[r_idx_o] == NULL ||
              (unsigned long long)obj >= region_info[r_idx_o]->top) {
            return;
          }
          bool mark_rst = bitmap->mark_strong((HeapWord *)obj, upgraded);
          if (mark_rst) {
            ShenandoahMarkTask pushTask(obj, upgraded, false);
            worker_queue->push(pushTask);
            snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
          }
        }
      }
    }
  }
}

bool NoTransSnicCopyRegionClient::check_is_narrow_oop(HeapWord* p) {
  uint32_t first_uint_val = *(uint32_t*) (p);
  // min_narrow_val = 0xa0000000 when HeapBase = 0x500000000
  uint32_t min_narrow_val = (uint32_t) ((heapBase - (unsigned long long) (COMPRESSED_OOP_BASE)) >> (unsigned long long) (COMPRESSED_OOP_SHIFT));
  if (min_narrow_val > first_uint_val) {
    return false;
  }
  return true;
}

void NoTransSnicCopyRegionClient::snic_concurrent_inc_liveness(unsigned long long r, intptr_t start_addr, size_t s, uint worker_id) {
  // NOTE: huge objects will use multiple regions, CAN NOT put the entire size into a region's live_data.
  unsigned long long actual_r = ((unsigned long long)start_addr - (unsigned long long)heapBase) >> regionSizeShift;
  unsigned long long end_addr = (unsigned long long) (start_addr) + s * 8;
  while (region_info[actual_r] != NULL && region_info[actual_r]->end < end_addr) {
    size_t delta = (size_t)((region_info[actual_r]->end - start_addr) / HeapWordSize);
    live_data_caches[worker_id][actual_r] += (uint64_t) (delta);
    start_addr = region_info[actual_r]->end;
    actual_r += 1;
  }
  size_t final_delta = (size_t)((end_addr - start_addr) / HeapWordSize);
  live_data_caches[worker_id][actual_r] += (uint64_t) (final_delta);
}

void NoTransSnicCopyRegionClient::copy_one_region_async(unsigned long long region_idx, bool is_prefetch) {
  auto region_bottom = region_info[region_idx]->bottom;
  auto region_top = region_info[region_idx]->top;
  if( region_top <= heapBase2){
    auto res_s = qp_heap->send_normal_direct(
    {.op = IBV_WR_RDMA_READ,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(region_top - region_bottom),
      .wr_id = (uint64_t) (region_idx)},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(region_bottom),
      .remote_addr = region_bottom,
      .imm_data = 0});
    RDMA_ASSERT(res_s == IOCode::Ok);
    on_trans_region_number1 += 1;
  }
  else if( region_bottom >= heapBase2){
    auto res_s = qp_heap2->send_normal_direct(
    {.op = IBV_WR_RDMA_READ,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(region_top - region_bottom),
      .wr_id = (uint64_t) (region_idx)},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(region_bottom),
      .remote_addr = region_bottom,
      .imm_data = 0});
    RDMA_ASSERT(res_s == IOCode::Ok);
    on_trans_region_number2 += 1;
  }
  else{
    log_error(gc)("DGC LOG: no, this region is between heap1 and heap2");
    log_error(gc)("DGC LOG: region_bottom: %llu, region_top: %llu, heapBase1: %llu, heapBase2: %llu", region_bottom, region_top, heapBase, heapBase2);
    exit(0);
  }
  on_trans_region_number += 1;
}

ShenandoahObjToScanQueue* NoTransSnicCopyRegionClient::get_queue(uint worker_id) {
  return task_queues->queue(worker_id);
}

void NoTransSnicCopyRegionClient::initialize() {
  prev_total_liveness = new uint64_t[snic_gc_workers->active_workers()];
  memset(prev_total_liveness, 0, sizeof(uint64_t) * snic_gc_workers->active_workers());
  try_mark_counts = new size_t[snic_gc_workers->active_workers() * 8];
  success_mark_counts = new size_t[snic_gc_workers->active_workers() * 8];
  memset(try_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
  memset(success_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
  _gc_id = -1;
  pending_count_table.resize(heapRegionNumber);
  workerPendingMaps = new NoTransWorkerPendingMap*[snic_gc_workers->active_workers()];
  for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
    workerPendingMaps[i] = new NoTransWorkerPendingMap(heapRegionNumber);
  }

  regionArrivalQueue = new NoTransRegionArrivalQueue*[snic_gc_workers->active_workers()];
  for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
    regionArrivalQueue[i] = new NoTransRegionArrivalQueue(heapRegionNumber);
    regionArrivalQueue[i]->clear();
  }


  region_copied_bitmap = new unsigned long long[heapRegionNumber];
  // char* tmp_region_copied_bitmap = (char*)region_copied_bitmap;
  memset((unsigned long long*)region_copied_bitmap, (unsigned long long) (NT_REGION_NOT_COPIED), sizeof(unsigned long long) * heapRegionNumber);
  live_data_caches = new uint64_t*[(int) (ConcGCThreads)];
  for (int i = 0; i < (int) (ConcGCThreads); ++i) {
    live_data_caches[i] = new uint64_t[heapRegionNumber];
    memset(live_data_caches[i], 0, sizeof(uint64_t) * heapRegionNumber);
  }
  // init region_info here.
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    region_info.push_back(NULL);
  }
  log_debug(gc)("DGC LOG: finish initialize func of NoTransSnicCopyRegionClient, region_info size:%lu", region_info.size());
  // TEST ONLY: output some contants here to check whether they are correct
  log_debug(gc)("DGC LOG:arrayOop::base_offset:%d, length_offset:%d", arrayOopDesc::base_offset_in_bytes(T_OBJECT), arrayOopDesc::length_offset_in_bytes());
  log_debug(gc)("DGC LOG:referent_offset:%d", java_lang_ref_Reference::referent_offset());
}

void NoTransSnicCopyRegionClient::issue_fetch_region_by_pending_length_heap1(uint candidate_region_number) {
  // log_debug(gc)("DGC LOG: issue_fetch_region_by_pending_length, candidate_region_number:%u", candidate_region_number);
  memset(pending_count_table.data(), 0, sizeof(int) * heapRegionNumber);
  for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
    for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
      auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[region_idx];
      pending_count_table[region_idx] += taskQueue->size();
    }
  }
  for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
    if(region_info[region_idx] == NULL || region_copied_bitmap[region_idx] != (char) (NT_REGION_NOT_COPIED)){
      pending_count_table[region_idx] = -1;
    }
  }
  // if (candidate_region_number < 20) {
    // find the region with the maximum pending count.
  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_debug(gc)("DGC LOG: %d", j);
    if(region_info[j] == NULL || region_info[j]->bottom >= heapBase2){
      continue;
    }
    if (pending_count_table[j] > threshold) {
      if(candidate_list.size() < candidate_region_number){
        candidate_list.push_back(std::make_pair(pending_count_table[j], j));
      }
      else{
        candidate_list[candidate_region_number - 1] = std::make_pair(pending_count_table[j], j);
      }
      int idx = candidate_list.size() - 1;
      while(idx >= 1 && candidate_list[idx-1].first < candidate_list[idx].first){
        std::swap(candidate_list[idx-1], candidate_list[idx]);
        --idx;
      }
      if(candidate_list.size() == candidate_region_number){
        threshold = candidate_list[candidate_region_number - 1].first;
      }
      else{
        threshold = -1;
      }
    }
  }
  if (threshold != -1) {
    for(uint i = 0; i < candidate_region_number; ++i){
      copy_one_region_async(candidate_list[i].second);
      // region_copied_bitmap[candidate_list[i].second] = (char) (NT_REGION_ON_TRANS);

      int target_region_idx = candidate_list[i].second;
      int retry_times = 0;
      unsigned long long prev;
      while(true){
        retry_times++;
        if(retry_times > 1000000000){
          log_error(gc)("DGC LOG: retry times > 1000000000, target_region_idx=%d, prev=%llu", target_region_idx, prev);
          exit(0);
        }
        prev = region_copied_bitmap[target_region_idx];
        if(prev > NT_REGION_ON_TRANS){
          continue;
        }
        auto res = Atomic::cmpxchg(&region_copied_bitmap[target_region_idx], (unsigned long long) prev, (unsigned long long) (NT_REGION_ON_TRANS));
        if(res == prev){
          // log_debug(gc)("DGC LOG: success cmpxchg region_idx=%d, prev_val=%d, res=%d set to NT_REGION_ON_TRANS", target_region_idx, prev, res);
          break;
        }
        else{
          // log_debug(gc)("DGC LOG: failed cmpxchg region_idx=%d, prev_val=%d, res=%d set to NT_REGION_ON_TRANS", target_region_idx, prev, res);
        }
      }

    }
  }
  else{
    log_error(gc)("DGC LOG: no region to copy in heap1");
    log_error(gc)("DGC LOG: done_trans_region_number1: %lu, done_trans_region_number2: %lu, on_trans_region_number1: %lu, on_trans_region_number2: %lu, received_region_num1: %d, received_region_num2: %d, received_region_num: %d", done_trans_region_number1, done_trans_region_number2, on_trans_region_number1, on_trans_region_number2, received_region_num1, received_region_num2, received_region_num);
    exit(0);
  }
}



void NoTransSnicCopyRegionClient::issue_fetch_region_by_pending_length_heap2(uint candidate_region_number) {
  // log_debug(gc)("DGC LOG: issue_fetch_region_by_pending_length, candidate_region_number:%u", candidate_region_number);
  memset(pending_count_table.data(), 0, sizeof(int) * heapRegionNumber);
  for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
    for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
      auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[region_idx];
      pending_count_table[region_idx] += taskQueue->size();
    }
  }
  for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
    if(region_info[region_idx] == NULL || region_copied_bitmap[region_idx] != (char) (NT_REGION_NOT_COPIED)){
      pending_count_table[region_idx] = -1;
    }
  }
  // if (candidate_region_number < 20) {
    // find the region with the maximum pending count.
  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_debug(gc)("DGC LOG: %d", j);
    if(region_info[j] == NULL || region_info[j]->top <= heapBase2){
      continue;
    }
    if (pending_count_table[j] > threshold) {
      if(candidate_list.size() < candidate_region_number){
        candidate_list.push_back(std::make_pair(pending_count_table[j], j));
      }
      else{
        candidate_list[candidate_region_number - 1] = std::make_pair(pending_count_table[j], j);
      }
      int idx = candidate_list.size() - 1;
      while(idx >= 1 && candidate_list[idx-1].first < candidate_list[idx].first){
        std::swap(candidate_list[idx-1], candidate_list[idx]);
        --idx;
      }
      if(candidate_list.size() == candidate_region_number){
        threshold = candidate_list[candidate_region_number - 1].first;
      }
      else{
        threshold = -1;
      }
    }
  }
  if (threshold != -1) {
    for(uint i = 0; i < candidate_region_number; ++i){
      copy_one_region_async(candidate_list[i].second);
      // region_copied_bitmap[candidate_list[i].second] = (char) (NT_REGION_ON_TRANS);

      int target_region_idx = candidate_list[i].second;
      int retry_times = 0;
      unsigned long long prev;
      while(true){
        retry_times++;
        if(retry_times > 1000000000){
          log_error(gc)("DGC LOG: retry times > 1000000000, target_region_idx=%d, prev=%llu", target_region_idx, prev);
          exit(0);
        }
        prev = region_copied_bitmap[target_region_idx];
        if(prev > NT_REGION_ON_TRANS){
          continue;
        }
        auto res = Atomic::cmpxchg(&region_copied_bitmap[target_region_idx], (unsigned long long) prev, (unsigned long long) (NT_REGION_ON_TRANS));
        if(res == prev){
          // log_debug(gc)("DGC LOG: success cmpxchg region_idx=%d, prev_val=%d, res=%d set to NT_REGION_ON_TRANS", target_region_idx, prev, res);
          break;
        }
        else{
          // log_debug(gc)("DGC LOG: failed cmpxchg region_idx=%d, prev_val=%d, res=%d set to NT_REGION_ON_TRANS", target_region_idx, prev, res);
        }
      }
    }
  }
  else{
    log_error(gc)("DGC LOG: no region to copy in heap2");
    log_error(gc)("DGC LOG: done_trans_region_number1: %lu, done_trans_region_number2: %lu, on_trans_region_number1: %lu, on_trans_region_number2: %lu, received_region_num1: %d, received_region_num2: %d, received_region_num: %d", done_trans_region_number1, done_trans_region_number2, on_trans_region_number1, on_trans_region_number2, received_region_num1, received_region_num2, received_region_num);
    exit(0);
  }
}



void NoTransSnicCopyRegionClient::start_handle_task_queue_roots() {
  handling_task_queue_roots = true;
}
void NoTransSnicCopyRegionClient::finish_handle_task_queue_roots() {
  handling_task_queue_roots = false;
}

bool NoTransSnicCopyRegionClient::during_handle_task_queue_roots() {
  return handling_task_queue_roots;
}

void NoTransSnicCopyRegionClient::start_handle_satb_roots() {
  handling_satb_roots = true;
}
void NoTransSnicCopyRegionClient::finish_handle_satb_roots() {
  handling_satb_roots = false;
}

bool NoTransSnicCopyRegionClient::during_handle_satb_roots() {
  return handling_satb_roots;
}

int NoTransSnicCopyRegionClient::copy_remote_gc_roots_buffer(int bufferSize, void* payload) {
  // memset(gc_roots_buffer, (char) (0), MAX_RPC_BUFFER_SIZE);
  int N = bufferSize / sizeof(unsigned long long);
  unsigned long long *message = (unsigned long long *)payload;
  if (N != 2) {
    log_error(gc)("DGC LOG: RPC content to copy gc roots buffer should have size 2");
    exit(0);
  }
  int len = (int) (message[0]);
  auto res_s = qp_gc_roots_buffer->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
       .flags = IBV_SEND_SIGNALED,
       .len = (unsigned int)(len * sizeof(unsigned long long)),
       .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(gc_roots_buffer),
       .remote_addr = remoteGCRootsBufferBase,
       .imm_data = 0});
  RDMA_ASSERT(res_s == IOCode::Ok);
  if (res_s != IOCode::Ok) {
    log_error(gc)("DGC LOG: failed to copy gc roots buffer");
    exit(0);
  }
  auto res_p = qp_gc_roots_buffer->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
  if (res_p != IOCode::Ok) {
    log_error(gc)("DGC LOG: failed to wait for gc roots buffer");
    exit(0);
  }
  // send back ack to host to notify the host that the gc roots buffer has been copied.
  send_back_int_ack(NT_REMOTE_GC_ROOTS_COPY_FINISH_ACK);
  log_debug(gc)("DGC LOG: received len to copy:%d", len);
  return len;
}

bool NoTransSnicCopyRegionClient::oop_is_humongous(oop obj) {
  auto region_size = (unsigned long long)(1UL << regionSizeShift);
  auto obj_size = obj->size() * HeapWordSize;
  if ((unsigned long long) obj_size > region_size) {
    return true;
  }
  return false;
  // auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  // if (region_info[start_region_idx] == NULL) {
  //   return false;
  // }
  // return region_info[start_region_idx]->state == SnicRegionState::_humongous_start;
}

bool NoTransSnicCopyRegionClient::humongous_oop_regions_copy_finished(oop obj) {
  auto size = obj->size();
  auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  objArrayOop a = objArrayOop(obj);
  narrowOop *p = (narrowOop *)(a->base());
  narrowOop *const end = p + a->length();
  auto end_region_idx = ((unsigned long long) (end) - (unsigned long long) heapBase) >> regionSizeShift;
  for (unsigned long long i = start_region_idx; i <= end_region_idx; ++i) {
    if (region_info[i] == NULL) {
      continue;
    }
    if (region_copied_bitmap[i] != (char) (NT_REGION_COPIED)) {
      // log_debug(gc)("DGC LOG: find humongous oop %p, region %llu not copied,start:%llu,end:%llu,size:%d", obj, i, start_region_idx, end_region_idx, size);
      return false;
    }
  }
  // log_debug(gc)("DGC LOG: find humongous oop %p, all regions are copied,start:%llu,end:%llu,size:%d", obj, start_region_idx, end_region_idx, size);
  return true;
}

bool NoTransSnicCopyRegionClient::humongous_oop_regions_copy_finished(oop obj, int* region_idx) {
  auto size = obj->size();
  auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  // objArrayOop a = objArrayOop(obj);
  // narrowOop *p = (narrowOop *)(a->base());
  auto end = obj + size;
  unsigned long long regionSize = 1UL << regionSizeShift;
  unsigned long long delta = (unsigned long long) end - (unsigned long long) obj;
  auto end_region_idx = ((unsigned long long) (end) - (unsigned long long) heapBase) >> regionSizeShift;
  if (delta % regionSize == 0) {
    end_region_idx -= 1;
  }
  if (end_region_idx >= heapRegionNumber) {
    printf("DGC LOG: end_region_idx out of range, end_region_idx: %llu, heapRegionNumber: %lu, obj:%p, end:%p\n", end_region_idx, heapRegionNumber, obj, end);
    // int res = 1 / 0;
    end_region_idx = heapRegionNumber - 1;
  }
  for (unsigned long long i = start_region_idx; i <= end_region_idx; ++i) {
    if (region_info[i] == NULL) {
      continue;
    }
    if (region_copied_bitmap[i] != (char) (NT_REGION_COPIED)) {
      // log_debug(gc)("DGC LOG: find humongous oop %p, region %llu not copied,start:%llu,end:%llu,size:%d", obj, i, start_region_idx, end_region_idx, size);
      if (i >= heapRegionNumber) {
        printf("DGC LOG: region idx out of range, region idx: %llu, heapRegionNumber: %lu, obj:%p, end:%p\n", i, heapRegionNumber, obj, end);
        int res = 1 / 0;
      }
      *region_idx = i;
      return false;
    }
  }
  // log_debug(gc)("DGC LOG: find humongous oop %p, all regions are copied,start:%llu,end:%llu,size:%d", obj, start_region_idx, end_region_idx, size);
  return true;
}

NoTransRegionArrivalQueue::NoTransRegionArrivalQueue(uint region_num) {
    _region_arrival_queue = new int[region_num];
    _head = 0;
    _tail = 0;
    _is_empty = true;
}
NoTransRegionArrivalQueue::~NoTransRegionArrivalQueue() {
    delete[] _region_arrival_queue;
}
void NoTransRegionArrivalQueue::push(uint region_idx) {
    _region_arrival_queue[_tail] = region_idx;
    // modify is_empty first to avoid concurrent error
    Atomic::store(&_is_empty, false);
    Atomic::add(&_tail, 1);
    // log_debug(gc)("DGC LOG: push region_idx: %d, head: %d, tail: %d", region_idx, _head, _tail);
}
int NoTransRegionArrivalQueue::pop() {
    int local_head = Atomic::load(&_head);
    int local_tail = Atomic::load(&_tail);
    // printf("DGC LOG: pop, head: %d, tail: %d\n", local_head, local_tail);
    // log_debug(gc)("DGC LOG: pop, head: %d, tail: %d", Atomic::load(&_head), Atomic::load(&_tail));
    if(local_head < local_tail){
        int region_idx = _region_arrival_queue[local_head];
        local_head = (local_head + 1);
        Atomic::store(&_head, local_head);
        if (local_head == local_tail) {
            // Atomic::store(&is_empty, true);
            Atomic::cmpxchg(&_is_empty, false, true);
        }
        // log_debug(gc)("DGC LOG: pop region_idx: %d, head: %d, tail: %d", region_idx, Atomic::load(&_head), Atomic::load(&_tail));
        return region_idx;
    }
    return -1;
}
bool NoTransRegionArrivalQueue::is_empty() {
    return _is_empty;
}
void NoTransRegionArrivalQueue::clear() {
    _head = 0;
    _tail = 0;
    _is_empty = true;
}

size_t NoTransSnicCopyRegionClient::compute_mr_idx(int hostId, size_t idx) {
  return hostId * 50 + idx;
}

void NoTransSnicCopyRegionClient::read_back_cur_host_virtual_nodes(int hostId) {
  for (auto node : virtualSpaceNodes) {
    if (node->base == 0x800000000) {
      continue;
    }
    auto real_length = node->cur_top - node->base;
    if (real_length == 0) {
      continue;
    }
    log_debug(gc)("DGC LOG: host %d re-bind for virtual node,addr:%llx,length:%llu,real length:%llu", hostId, (unsigned long long) node->base, node->sz, real_length);
    // re-bind this memory space for RDMA.
    auto target_mr_idx = compute_mr_idx(hostId, node->index);
    auto fetch_res_virtualspace = cm->fetch_remote_mr(target_mr_idx);
    RDMA_ASSERT(fetch_res_virtualspace == IOCode::Ok) << std::get<0>(fetch_res_virtualspace.desc);
    rmem::RegAttr remote_attr_virtualNode = std::get<1>(fetch_res_virtualspace.desc);
    node->qp->bind_remote_mr(remote_attr_virtualNode);
    log_debug(gc)("DGC LOG: host %d bind remote mr success", hostId);
    node->local_mem = Arc<RMem>(new RMem((void *)(node->base), node->sz));
    node->local_mr = RegHandler::create(node->local_mem, nic).value();
    node->qp->bind_local_mr(node->local_mr->get_reg_attr().value());

    log_debug(gc)("DGC LOG: host %d read back virtual node,addr:%llx,length:%llu,real length:%llu", hostId, (unsigned long long) node->base, node->sz, real_length);
    auto res_s = node->qp->send_normal_direct(
        {.op = IBV_WR_RDMA_READ,
         .flags = IBV_SEND_SIGNALED,
         .len = (unsigned int)(node->cur_top - node->base),
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(node->base),
         .remote_addr = node->base,
         .imm_data = 0});
    if (res_s != IOCode::Ok) {
      log_debug(gc)("DGC LOG: host %d send read request failed:addr:%llx,length:%llu", hostId, (unsigned long long) node->base, node->sz);
      exit(0);
    }
    auto read_res = node->qp->wait_one_comp();
    if (read_res != IOCode::Ok) {
      log_debug(gc)("DGC LOG: host %d wait read request finished failed:addr:%llx,length:%llu", hostId, (unsigned long long) node->base, node->sz);
      exit(0);
    }
  }
}

void NoTransSnicCopyRegionClient::unmapMemSpace(void* addr, unsigned long long length, int hostId) {
  int res = munmap(addr, length);
  if (res == -1) {
    log_debug(gc)("DGC LOG: host %d munmap failed:addr:%llx,length:%llu,errno:%s", hostId, (unsigned long long) addr, length, strerror(errno));
    exit(0);
  }
}

void* NoTransSnicCopyRegionClient::mapMemSpace(int fd, unsigned long long addr, unsigned long long length, int hostId) {
  void* ptr = NULL;
  if (fd == -1) {
    ptr = mmap((void *)addr, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
  }
  else {
    ptr = mmap((void*)addr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  }
  if (ptr == MAP_FAILED) {
    log_error(gc)("DGC LOG: host %d mmap failed:addr:%llx,length:%llu,fd:%d,errno:%s", hostId, addr, length, fd, strerror(errno));
    return NULL;
  }
  if ((unsigned long long) ptr != addr && fd != -1) {
    log_debug(gc)("DGC LOG: host %d mmap result mismatched:target addr:%llx,result addr:%llx,length:%llu,fd:%d", hostId, addr, (unsigned long long) ptr, length, fd);
    return ptr;
  }
  log_debug(gc)("DGC LOG: host %d mmap success:addr:%llx,real addr:%llx,length:%llu,fd:%d", hostId, addr, (unsigned long long) ptr, length, fd);
  return ptr;
}

void NoTransSnicCopyRegionClient::unmapCurHostMemSpaces(int hostId) {
  for (auto node : virtualSpaceNodes) {
    // CDS is owned by the JVM; leave it alone.
    if (node->base == 0x800000000) continue;
    // Drop only the alias mapping at node->base. The client-picked
    // real_local_base mapping (where the MR is pinned) stays live; the
    // fd keeps the SHM inode alive across remap cycles.
    unmapMemSpace((void*)node->base, node->sz, hostId);
  }
}

void NoTransSnicCopyRegionClient::remapCurHostMemSpaces(int hostId) {
  for (auto node : virtualSpaceNodes) {
    // CDS is owned by the JVM; leave it alone.
    if (node->base == 0x800000000) continue;
    // Only shm-backed VSNs (map_fd != -1) participate in remap cycling.
    if (node->map_fd == -1) continue;
    // unmap any existing alias at node->base, then reinstate the SHM alias.
    unmapMemSpace((void*)node->base, node->sz, hostId);
    void* alias = mmap((void*)node->base, node->sz,
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_FIXED, node->map_fd, 0);
    if (alias == MAP_FAILED || alias != (void*)node->base) {
      log_error(gc)("DGC LOG: host %d remap alias at 0x%llx failed: got %p, %s",
                    hostId, node->base, alias, strerror(errno));
      exit(0);
    }
  }
}

int NoTransSnicCopyRegionClient::decide_force_terminate_counts() {
  int SATBFlashInterval = received_region_num / SnicSATBRootsSplitPartNum;
  int checkForceFinishInterval = SATBFlashInterval + SnicSATBRootsForceDeltaNum;
  return received_region_num / checkForceFinishInterval - done_trans_region_number / checkForceFinishInterval + 1;
}

// Factory used from snicClient.cc when SnicGCRegionAddressTranslation is
// OFF. Defined here so callers don't need to pull this hpp (and its
// internal NT_-prefixed macros + helper classes) into their own
// translation unit.
SnicClient* create_no_trans_snic_copy_region_client() {
    return new NoTransSnicCopyRegionClient();
}
