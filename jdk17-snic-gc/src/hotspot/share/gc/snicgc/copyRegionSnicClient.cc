
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

#include "copyRegionSnicClient.hpp"
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
#include <map>

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

std::map<unsigned long long, int> obj_trace_map;
std::map<unsigned long long, int> obj_trace_map2;
std::map<unsigned long long, int64_t> obj_trace_map3;
std::map<int64_t, int> root_map;
int64_t total_ongoing_mark_size = 0;
int total_ongoing_oop = 0;
std::mutex mtx2;
int debug_counter_read_lock = 0;
int debug_trigger = 0;
int debug_trigger_rdma = 0;
int region_trans_counter = 0;
// for now, use a mutex to sync
std::mutex mtx;

CopyRegionSnicClient::CopyRegionSnicClient() : SnicClient() {
  // generate a random number to distinguish different host.
  srand(time(NULL));
  host_random_num = rand() % 10000 + 10000;
  virtualSpaceNodes.clear();

  // Initialize address translation related members
  local_memory_pool_base = nullptr;
  local_memory_pool_size = 0;
  local_memory_pool_used = 0;
  local_memory_region_alloacted = 0;
}

// function to handle RPCs.
void CopyRegionSnicClient::handleRPC(int rpcType, int hostId, int bufferSize, void* payload) {
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
      heapRegionSize = region_size;
      regionSizeShift = __builtin_ctz(region_size);
      log_debug(gc)("DGC LOG: received host %d region size shift:%lu, received region size:%llu", hostId, regionSizeShift, region_size / 1024);
      // handle RPC type 1, used to establish RDMA connection.
      if (SnicGCRegionAddressTranslation) {
        runRDMAClientWithTranslation(hostId);
      } else {
        runRDMAClient(hostId);
      }
      send_back_int_ack(1);
      break;
    }
  case 2:
    {
      region_trans_counter = 0;
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
      for (int i = 0; i < (int) (ConcGCThreads); ++i) {
        // live_data_caches[i] = new uint64_t[heapRegionNumber];
        memset(live_data_caches[i], 0, sizeof(uint64_t) * heapRegionNumber);
        // ongoing_mark_caches[i] = new int64_t[heapRegionNumber];
        memset(ongoing_mark_caches[i], 0, sizeof(int64_t) * heapRegionNumber);
      }
      // handle RPC type 2, used to receive root objects' addresses.
      should_force_tasks_finish = 0;
      under_last_round_mark_count = 0;
      terminate_condition_for_address_translation = 0;
      force_finished_task_cnt = 0;
      finished_worker_count = 0;
      normal_satb_roots_handle_count = 0;
      local_mem_pool_free_region_count = local_memory_pool_region_limit;
      // memset(region_copied_bitmap, 0, sizeof(unsigned long long) * heapRegionNumber);
      // memset(pending_count_table, 0, sizeof(int) * heapRegionNumber);
      if (!SnicConcCopyRegion) {
        log_debug(gc)("GC(%u,%d)DGC LOG: Start handling root objects and process references", _gc_id, hostId);
        // handleRoot(bufferSize, payload);
        int len = copy_remote_gc_roots_buffer(bufferSize, payload);
        handleRoot(len, (void*) (gc_roots_buffer));
        // send back an ack to tell client case 2 is finished.
        send_back_int_ack(TASK_QUEUE_ROOTS_FINISH_ACK);
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
          send_back_int_ack(SATB_ROOTS_FINAL_ACK);
        }
        // send back an ack to tell client case 2 is finished.
        send_back_int_ack(TASK_QUEUE_ROOTS_FINISH_ACK);
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
      terminate_condition_for_address_translation = 0;
      force_finished_task_cnt = 0;
      finished_worker_count = 0;
      normal_satb_roots_handle_count += 1;
      log_debug(gc)("GC(%u,%d)DGC LOG: Start handle normal satb roots, handle count:%d", _gc_id, hostId, normal_satb_roots_handle_count);
      // handle_satb_roots(bufferSize, payload);
      int len = copy_remote_gc_roots_buffer(bufferSize, payload);
      handle_satb_roots(len, (void*) (gc_roots_buffer));
      if (DpuClientLivenessUpdateEnabled) {
        send_back_int_ack((size_t)(SATB_ROOTS_FINISH_ACK));
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
      terminate_condition_for_address_translation = 0;
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
      log_debug(gc)("GC(%u,%d)DGC LOG: Done handle final satb roots, len=%d, total try mark count: %lu, total success mark count: %lu", _gc_id, hostId, len, total_try_mark_count, total_success_mark_count);
      // unmapCurHostMemSpaces(hostId);
      log_info(gc)("GC(%u,%d)LHT LOG: region trans counter: %d", _gc_id, hostId, region_trans_counter);
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

int CopyRegionSnicClient::runRDMAClient(int hostId)
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

int CopyRegionSnicClient::runRDMAClientWithTranslation(int hostId)
{
  // 1. create a local QP to use
  nic = RNic::create(RNicInfo::query_dev_names().at(0)).value();
  nic2 = RNic::create(RNicInfo::query_dev_names().at(1)).value();
  // 2. create the pair QP at server using CM
  char HostAddrPort[30];
  sprintf(HostAddrPort, "%s:%d", HostAddr, (RDMAPort + hostId));
  log_info(gc)("LHT LOG: host %d runRDMAClient, HostAddrPort: %s", hostId, HostAddrPort);
  cm = new ConnectManager(HostAddrPort);

  if (cm->wait_ready(1000000, 2) ==
      IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout";

  //log_dev_debug(gc)("LHT LOG: cm connected");
  if(SnicGCLocalMemoryPoolSize == 0) {
    log_error(gc)("LHT LOG: SnicGCLocalMemoryPoolSize is 0, please set it to a non-zero value");
    exit(0);
  }

  local_memory_pool_base = (void*)SnicGCLocalMemoryPoolMinAddress;
  local_memory_pool_size = SnicGCLocalMemoryPoolSize * 1024 * 1024;
  reserveMemRegion(SnicGCLocalMemoryPoolMinAddress, local_memory_pool_size);
  local_mem_pool = Arc<RMem>(new RMem(local_memory_pool_base, local_memory_pool_size));

  qp_heap = RC::create(nic, QPConfig()).value();
  qp_heap2 = RC::create(nic2, QPConfig()).value();
  std::string qp_name_1 = "client-qp-heap-" + std::to_string(hostId);
  auto qp_res = cm->cc_rc(qp_name_1, qp_heap, 0, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  std::string qp_name_2 = "client-qp-heap2-" + std::to_string(hostId);
  qp_res = cm->cc_rc(qp_name_2, qp_heap2, 1, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);

  //log_dev_debug(gc)("LHT LOG: qp_heap get");
  auto key = std::get<1>(qp_res.desc);
  //log_dev_debug(gc)("LHT LOG: key get");

  {
    auto target_mr_idx_1 = compute_mr_idx(hostId, 0);
    log_info(gc)("LHT LOG: host %d fetch remote heap1 mr idx: %lu", hostId, target_mr_idx_1);
    auto fetch_res = cm->fetch_remote_mr(target_mr_idx_1);
    if (fetch_res != IOCode::Ok) {
      log_info(gc)("LHT LOG: host %d fetch remote heap1 mr idx: %lu failed, %s", hostId, target_mr_idx_1, std::get<0>(fetch_res.desc).c_str());
    }
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
    //log_dev_debug(gc)("LHT LOG: start bind_remote_mr");
    qp_heap->bind_remote_mr(remote_attr);
    heapBase = remote_attr.buf;
    heapSize = remote_attr.sz;
    heapRegionNumber_half = heapSize >> regionSizeShift;
    log_info(gc)("LHT LOG: remote heap1 base: %llx, size: %llu for host %d", remote_attr.buf, remote_attr.sz, hostId);
    log_info(gc)("LHT LOG: reserve mem pool from %llx to %llx, size=%llu for host %d", local_memory_pool_base, local_memory_pool_base+local_memory_pool_size, local_memory_pool_size, hostId);
    local_mr_pool = RegHandler::create(local_mem_pool, nic).value();
    qp_heap->bind_local_mr(local_mr_pool->get_reg_attr().value());

    auto res = qp_heap->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = (unsigned int)(16),
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_memory_pool_base),
        .remote_addr = heapBase,
        .imm_data = 0});
    RDMA_ASSERT(res == IOCode::Ok);
    auto res2 = qp_heap->wait_one_comp();
    RDMA_ASSERT(res2 == IOCode::Ok);
    log_info(gc)("LHT LOG: send rdma read to heapBase for host %d done", hostId);
  }

  {
    auto target_mr_idx_2 = compute_mr_idx(hostId, 999);
    log_info(gc)("LHT LOG: host %d fetch remote heap2 mr idx: %lu", hostId, target_mr_idx_2);
    auto fetch_res = cm->fetch_remote_mr(target_mr_idx_2);
    if (fetch_res != IOCode::Ok) {
      log_info(gc)("LHT LOG: host %d fetch remote heap2 mr idx: %lu failed, %s", hostId, target_mr_idx_2, std::get<0>(fetch_res.desc).c_str());
    }
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);
    //log_dev_debug(gc)("LHT LOG: start bind_remote_mr");
    qp_heap2->bind_remote_mr(remote_attr);
    heapBase2 = remote_attr.buf;
    unsigned long long heapSize2 = remote_attr.sz;

    log_info(gc)("LHT LOG: host %d heapsize1: %llu, heapsize2: %llu", hostId, heapSize, heapSize2);
    heapSize += heapSize2;
    heapRegionNumber = heapSize >> regionSizeShift;

    log_info(gc)("LHT LOG: host %d heapsize: %llu, heapRegionNumber: %lu, regionSizeShift: %lu", hostId, heapSize, heapRegionNumber, regionSizeShift);

    local_mr_pool2 = RegHandler::create(local_mem_pool, nic2).value();
    qp_heap2->bind_local_mr(local_mr_pool2->get_reg_attr().value());

    log_info(gc)("LHT LOG: test rdma read to heapBase2 for host %d", hostId);

    auto res = qp_heap2->send_normal_direct(
      {.op = IBV_WR_RDMA_READ,
        .flags = IBV_SEND_SIGNALED,
        .len = (unsigned int)(16),
        .wr_id = 0},
      {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_memory_pool_base),
        .remote_addr = heapBase2,
        .imm_data = 0});
    RDMA_ASSERT(res == IOCode::Ok);
    auto res2 = qp_heap2->wait_one_comp();
    RDMA_ASSERT(res2 == IOCode::Ok);
    log_info(gc)("LHT LOG: test rdma read to heapBase2 for host %d done", hostId);
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
  initialize_address_translation();
  log_info(gc)("LHT LOG: initialize address translation for host %d done", hostId);
  return 0;
}


void CopyRegionSnicClient::reserveMemRegion(unsigned long long addr, unsigned long long length)
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
void CopyRegionSnicClient::copyRegion(int bufferSize, void* payload){
  log_debug(gc)("DGC LOG: copyRegion called but the active path is conc_copy_region_handle_root");
  exit(0);
  // memset bitmap
  memset(bitmap->_map, 0, bitmap->_size / 8);
  // memset livedata
  memset(live_count, 0, heapRegionNumber * sizeof(live_count[0]));
  // for (int i = 0; i < (int) (heapRegionNumber); ++i) {
  //   region_info[i] = NULL;
  // }
  region_info.clear();
  region_info.resize(heapRegionNumber);
  for(int i = 0; i < heapRegionNumber; i++){
    // region_info[i].index = i;
    region_info[i].local_region_id = USHRT_MAX;
    region_info[i].bottom = 0;
    region_info[i].top = 0;
    region_info[i].end = 0;
    region_info[i].state = _empty_uncommitted;
  }
  regionTopIdx = 0;

  unsigned long long* message = (unsigned long long*)payload;
  int wait_complete_count = 0;
  received_region_num = (int) (bufferSize / sizeof(unsigned long long) / 4);
  size_t shift_num = 0;
  for(int i = 0; i < received_region_num; i++){
      regionTopIdx = std::max((unsigned long long)regionTopIdx, message[i*4]);
      // SnicHeapRegion* cur_region = new SnicHeapRegion();
      // cur_region->index = message[i * 4];
      int idx = (int)message[i * 4];
      region_info[idx].bottom = message[i * 4 + 1];
      region_info[idx].top = message[i * 4 + 2];
      region_info[idx].end = message[i * 4 + 3];
      // if (shift_num == 0) {
      //   shift_num = __builtin_ctz(cur_region->end - cur_region->bottom);
      // }
      // region_info[cur_region->index] = cur_region;
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

void CopyRegionSnicClient::wait_pre_copy_finish_work() {
  deprecated_function();
  std::vector<int> wait_types;
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    if (region_info[i].top == 0) {
      continue;
    }
    if (region_copied_bitmap[i] == (unsigned long long) (REGION_COPIED) || region_copied_bitmap[i] == (unsigned long long) (REGION_ON_TRANS) || region_copied_bitmap[i] == (unsigned long long) (REGION_ON_EVICTION)) {
      continue;
    }
    int huge_region_size = copy_one_region_async_auto_huge(i, true);
    for (int j = 0; j < huge_region_size; j++){
      auto region_bottom = region_info[i + j].bottom;
      auto region_top = region_info[i + j].top;
      if (region_top <= heapBase2) {
        wait_types.push_back(0);
      } else if (region_bottom >= heapBase2) {
        wait_types.push_back(1);
      }
    }
    i += (huge_region_size - 1);
  }
  int region_num_to_copy = wait_types.size();
  for (int i = 0; i< region_num_to_copy; ++i) {
    if (wait_types[i] == 0) {
      auto res_p = qp_heap->wait_one_comp();
      RDMA_ASSERT(res_p == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (unsigned long long) (REGION_COPIED);
      log_info(gc)("LHT LOG: wait_pre_copy_finish_work recv_region_idx=%llu", recv_region_idx);
      on_trans_region_number1 -= 1;
      done_trans_region_number1 += 1;
    } else if (wait_types[i] == 1) {
      auto res_p = qp_heap2->wait_one_comp();
      RDMA_ASSERT(res_p == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (unsigned long long) (REGION_COPIED);
      log_info(gc)("LHT LOG: wait_pre_copy_finish_work recv_region_idx=%llu", recv_region_idx);
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

void CopyRegionSnicClient::wait_part_copy_finish_work() {
  log_debug(gc)("DGC LOG: wait part copy finish work");
  unsigned long long start_time = os::javaTimeMillis();
  unsigned long long next_try_time = 0;
  int SnicSATBRootsSplitPartCount = 0;
  int SATBFlashInterval = received_region_num / SnicSATBRootsSplitPartNum;
  int checkForceFinishInterval = SATBFlashInterval + SnicSATBRootsForceDeltaNum;
  (void)next_try_time;

  if (SnicGCRegionAddressTranslation) {
    // ===== region-reclaim-2 address translation path =====
    int finalSended = 0;
    int timeout_counter = 0;
    while (true) {
      if (os::javaTimeNanos() > next_try_time || on_trans_region_number == 0) {
        int planed_region_number = 0;

        if (on_trans_region_number1 < (size_t)(SNICTransRegionGroupNum) || on_trans_region_number2 < (size_t)(SNICTransRegionGroupNum)) {
          memset(pending_count_table.data(), 0, sizeof(int) * heapRegionNumber);
          for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
            for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
              auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[region_idx];
              pending_count_table[region_idx] += taskQueue->size() + (int)(! taskQueue->is_empty()) + (int)(! taskQueue->is_empty());
            }
          }
          memset(ongoing_mark_count_table.data(), 0, sizeof(int) * heapRegionNumber);
          for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
            for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
              ongoing_mark_count_table[region_idx] += ongoing_mark_caches[worker_id][region_idx];
            }
            if (ongoing_mark_count_table[region_idx] < 0) {
              ongoing_mark_count_table[region_idx] = 0;
            }
          }
        }
        if (on_trans_region_number1 < (size_t)(SNICTransRegionGroupNum)) {
          int candidate_region_number1 = (2 * SNICTransRegionGroupNum - on_trans_region_number1);
          planed_region_number += issue_fetch_region_by_pending_length_heap1_address_translation(candidate_region_number1);
        }

        if (on_trans_region_number2 < (size_t)(SNICTransRegionGroupNum)) {
          int candidate_region_number2 = (2 * SNICTransRegionGroupNum - on_trans_region_number2);
          planed_region_number += issue_fetch_region_by_pending_length_heap2_address_translation(candidate_region_number2);
        }
        if (planed_region_number == 0) {
          next_try_time = os::javaTimeNanos() + 1000000;
          if (on_trans_region_number == 0) {
            if (checkRDMAControllerTerminateCondition()) {
              os::naked_short_sleep(1);
              if (checkRDMAControllerTerminateCondition() == false) {
                log_error(gc)("LHT LOG: ASSERT! checkRDMAControllerTerminateCondition() == false");
              } else {
                if (finalSended == 0) {
                  log_info(gc)("LHT LOG: send SATB_ROOTS_FINAL_ACK");
                  send_back_int_ack(SATB_ROOTS_FINAL_ACK);
                  finalSended = 1;
                  Atomic::add(&terminate_condition_for_address_translation, 1);
                  log_info(gc)("LHT LOG: terminate_condition_for_address_translation=%d", Atomic::load(&terminate_condition_for_address_translation));
                  log_info(gc)("LHT LOG: under_last_round_mark_count=%d", Atomic::load(&under_last_round_mark_count));
                  while (true) {
                    os::naked_short_sleep(1);
                    if (Atomic::load(&under_last_round_mark_count) == snic_gc_workers->active_workers()) {
                      break;
                    }
                  }
                  continue;
                } else if (cur_handling_rpc_type == 10) {
                  Atomic::store(&terminate_condition_for_address_translation, 1);
                  log_info(gc)("LHT LOG: terminate_condition_for_address_translation=1");
                  break;
                } else {
                  log_error(gc)("LHT LOG: ASSERT! finalSended=%d, cur_handling_rpc_type=%d", finalSended, cur_handling_rpc_type);
                  exit(0);
                }
              }
            }
            timeout_counter++;
            if (timeout_counter %= 100) {
              // (verbose debug logs omitted)
            }
            os::naked_short_sleep(1);
            continue;
          }
        }
      }
      uint64_t recv_region_idx = UINT64_MAX;
      if (on_trans_region_number1 > on_trans_region_number2) {
        auto res_p = qp_heap->wait_one_comp();
        RDMA_ASSERT(res_p == IOCode::Ok);
        recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
        on_trans_region_number1--;
        done_trans_region_number1++;
      } else {
        auto res_p = qp_heap2->wait_one_comp();
        RDMA_ASSERT(res_p == IOCode::Ok);
        recv_region_idx = ((uint64_t)(res_p.desc.wr_id) >> 16) & 0xFFFF;
        on_trans_region_number2--;
        done_trans_region_number2++;
      }
      on_trans_region_number--;
      done_trans_region_number++;

      auto kick_out_region_idx = region_info[recv_region_idx].kick_out_region_id;
      local_addr_translation_table[region_info[recv_region_idx].local_region_id].remote_region_id = recv_region_idx;
      if (kick_out_region_idx != USHRT_MAX) {
        region_info[kick_out_region_idx].local_region_id = USHRT_MAX;
        region_bitmap_set_value_with_write_lock(kick_out_region_idx, REGION_ON_EVICTION, REGION_NOT_COPIED);
        region_info[recv_region_idx].kick_out_region_id = USHRT_MAX;
      }
      region_bitmap_set_value_with_write_lock(recv_region_idx, REGION_ON_TRANS, REGION_COPIED);

      int recv_region_pending_count = 0;
      for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
        auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[recv_region_idx];
        recv_region_pending_count += taskQueue->size() + (int)(! taskQueue->is_empty());
      }
      if (recv_region_pending_count > 0) {
        int min_region_idx = 0;
        int min_region_size = task_queues->queue(0)->size();
        for (int i = 1; i < (int) snic_gc_workers->active_workers(); ++i) {
          auto task_queues_i = task_queues->queue(i);
          if ((uint) min_region_size > task_queues_i->size()) {
            min_region_size = task_queues_i->size();
            min_region_idx = i;
          }
        }
        auto res = region_bitmap_add_read_lock(recv_region_idx);
        if (get_region_bitmap_base_state(res) != REGION_COPIED) {
          log_error(gc)("LHT LOG: ASSERT! region_bitmap_add_read_lock failed, recv_region_idx=%lu, res=%llu", recv_region_idx, res);
          exit(0);
        }
        regionArrivalQueue[min_region_idx]->push(recv_region_idx);
      }
      continue;
    }
  } else {
    // ===== HEAD batch-poll OPT#5 path =====
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
        if (prev > REGION_ON_TRANS) continue;
        auto res = Atomic::cmpxchg(&region_copied_bitmap[recv_region_idx],
                                   (unsigned long long) prev,
                                   (unsigned long long) (REGION_COPIED));
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
          send_back_int_ack(SATB_ROOTS_FINAL_ACK);
        } else if (SnicSATBRootsSplitPartCount < SnicSATBRootsSplitPartNum - 1) {
          send_back_int_ack(SATB_ROOTS_NORMAL_ACK);
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
      send_back_int_ack(SATB_ROOTS_FINAL_ACK);
      Atomic::add(&should_force_tasks_finish, 1);
      log_debug(gc)("DGC LOG: ready to force workers termination 2, done_trans_num:%lu,target_num:%d", done_trans_region_number, received_region_num);
    }
  }

  log_debug(gc)("DGC LOG: wait part copy finish work done, time cost: %llu ms", os::javaTimeMillis() - start_time);
  if (SnicGCRegionAddressTranslation) {
    return;
  }
  for (size_t i = 0; i < heapRegionNumber; i++) {
    if (region_info[i].top != 0 && !region_is_local(i)) {
      log_debug(gc)("DGC LOG: region %lu is not copied", i);
      exit(0);
    }
  }
}

static void* wait_copy_finish_thread_func(void* arg) {
  CopyRegionSnicClient* client = (CopyRegionSnicClient*) arg;
  client->wait_part_copy_finish_work();
  return NULL;
}

static void* wait_pre_copy_finish_func(void* arg) {
  CopyRegionSnicClient* client = (CopyRegionSnicClient*) (arg);
  client->wait_pre_copy_finish_work();
  return NULL;
}

void CopyRegionSnicClient::start_wait_copy_finish_thread() {
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

void CopyRegionSnicClient::start_wait_pre_copy_finish_thread() {
  deprecated_function();
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

void CopyRegionSnicClient::wait_thread_complete() {
  pthread_mutex_lock(&wait_copy_finish_mutex);
  while (on_trans_region_number > 0) {
    pthread_cond_wait(&wait_copy_finish_cond, &wait_copy_finish_mutex);
  }
  pthread_mutex_unlock(&wait_copy_finish_mutex);
  log_debug(gc)("DGC LOG: wait thread complete done");
}

void CopyRegionSnicClient::wait_pre_copy_thread_complete() {
  pthread_mutex_lock(&wait_pre_copy_finish_mutex);
  while (on_trans_region_number > 0) {
    pthread_cond_wait(&wait_pre_copy_finish_cond, &wait_pre_copy_finish_mutex);
  }
  pthread_mutex_unlock(&wait_pre_copy_finish_mutex);
  log_debug(gc)("DGC LOG: wait pre copy thread complete done");
}

void CopyRegionSnicClient::copy_region_metadata(int bufferSize, void* payload) {
  is_one_gc_final_stage = false;
  satb_handle_cnt = 0;
  region_info.clear();
  region_info.resize(heapRegionNumber);
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    region_info[i].top = 0;
    region_info[i].state = _empty_uncommitted;

    region_copied_bitmap[i] = (unsigned long long) (REGION_NOT_COPIED);
  }
  if(SnicGCRegionAddressTranslation){
    for(int i = 0; i < (int) (heapRegionNumber); ++i) {
      local_addr_translation_table[i].remote_region_id = USHRT_MAX;
    }
  }
  regionTopIdx = 0;
  log_debug(gc)("DGC LOG: start update region_info");
  unsigned long long* message = (unsigned long long*)payload;
  int wait_complete_count = 0;
  received_region_num = (int) (bufferSize / sizeof(unsigned long long) / 5);
  received_region_num1 = 0;
  received_region_num2 = 0;
  // size_t shift_num = 0;
  for(int i = 0; i < received_region_num; i++){
      regionTopIdx = std::max((unsigned long long)regionTopIdx, message[i*5]);
      // SnicHeapRegion* cur_region = new SnicHeapRegion();
      // cur_region->index = message[i * 5];
      int index = (int)message[i * 5];
      region_info[index].bottom = message[i * 5 + 1];
      region_info[index].top = message[i * 5 + 2];
      region_info[index].end = message[i * 5 + 3];
      region_info[index].state = (SnicRegionState) message[i * 5 + 4];
      // if (shift_num == 0) {
      //   shift_num = __builtin_ctz(cur_region->end - cur_region->bottom);
      // }
      // region_info[cur_region->index] = cur_region;


      if(region_info[index].top <= heapBase2){
        received_region_num1++;
      }
      else{
        received_region_num2++;
      }
  }
  if(SnicGCRegionAddressTranslation){
    reset_address_translation_simple();
  }
  is_prefetched = message[received_region_num * 5];
  time_to_wait_for_prefetched_gc = message[received_region_num * 5 + 1];
  log_debug(gc)("DGC LOG: regionSizeShift=%lu,heapRegionNumber=%lu,received_region_num=%d,received_is_prefetched=%llu,received_time_to_wait_for_prefetched_gc=%llu", regionSizeShift, heapRegionNumber, received_region_num, is_prefetched, time_to_wait_for_prefetched_gc);
  send_back_int_ack(1);
}

void CopyRegionSnicClient::handleRoot(int bufferSize, void* payload){
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
  log_debug(gc)("GC(%u)DGC LOG: CopyRegionSnicClient receives root num:%d,queue count:%lu", _gc_id, N, queue_indexes.size() - 1);
  task_queues->reserve(snic_gc_workers->active_workers());
  for (uint i = 0; i < (snic_gc_workers->active_workers()); ++i) {
    auto worker_i = snic_gc_workers->worker(i);
    worker_i->set_gc_id(_gc_id);
  }
  if (SnicGCRegionAddressTranslation) {
    ShenandoahSNICCMTaskAddressTranslation task(N, message, this, queue_indexes, true);
    snic_gc_workers->run_task(&task);
  } else {
    ShenandoahSNICCMTask task(N, message, this, queue_indexes, true);
    snic_gc_workers->run_task(&task);
  }

}

void CopyRegionSnicClient::conc_copy_region_handle_root(int bufferSize, void* payload, int is_prefetched) {
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
      region_copied_bitmap[i] = (unsigned long long)(REGION_NOT_COPIED);
    }
  }
  for (int i = 1; i < N; ++i) {
    if (message[i] == (unsigned long long)(-1)) {
      queue_indexes.push_back(i);
      continue;
    }
    oop obj = (oop)message[i];
    unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(heapBase)) >> regionSizeShift;
    if (region_info[r_idx_o].top == 0 || (uint64_t)obj >= region_info[r_idx_o].top) {
      continue;
    }
    // if (region_copied_bitmap[r_idx_o] == (unsigned long long)(REGION_COPIED)) {
    //   continue;
    // }
    region_map[r_idx_o] += 1;
  }
  for (size_t i = 0; i < heapRegionNumber; ++i) {
    if (region_info[i].state == _humongous_start) {
      region_map[i] += 1;
    }
  }

  log_debug(gc)("DGC LOG: sync copy region count: %lu, received queue count:%lu", region_map.size(), queue_indexes.size());
  // send copy request related to roots.
  for (auto &pair : region_map) {
    copy_one_region_async_auto_huge(pair.first);
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
      region_copied_bitmap[recv_region_idx] = (unsigned long long)(REGION_COPIED);
      log_info(gc)("LHT LOG: wait_copy_finish_work recv_region_idx=%llu", recv_region_idx);
      on_trans_region_number1--;
      done_trans_region_number1++;
    } else {
      auto recv_res = qp_heap2->wait_one_comp();
      RDMA_ASSERT(recv_res == IOCode::Ok);
      uint64_t recv_region_idx = ((uint64_t)(recv_res.desc.wr_id) >> 16) & 0xFFFF;
      region_copied_bitmap[recv_region_idx] = (unsigned long long)(REGION_COPIED);
      log_info(gc)("LHT LOG: wait_copy_finish_work recv_region_idx=%llu", recv_region_idx);
      on_trans_region_number2--;
      done_trans_region_number2++;
    }
    on_trans_region_number -= 1;
    done_trans_region_number += 1;
  }
  log_debug(gc)("DGC LOG: finish sync init root regions copy, copy region num:%lu", on_trans_region_number);

  for (int i = 1; i < N; ++i) {
    if (message[i] == (unsigned long long)(-1)) {
      continue;
    }
    oop obj = (oop)message[i];
    unsigned long long r_idx_o = (reinterpret_cast<unsigned long long>(obj) - (unsigned long long)(heapBase)) >> regionSizeShift;
    if (region_info[r_idx_o].top == 0 || (uint64_t)obj >= region_info[r_idx_o].top) {
      continue;
    }
    bool upgraded = false;
    bool bitmap_mark_rst = bitmap->mark_strong((HeapWord*) (obj), upgraded);
    if (!bitmap_mark_rst) {
      continue;
    }

    if (SnicGCRegionAddressTranslation) {
      oop local_cached_obj = translate_oop(obj);
      snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(local_cached_obj->size()), 0);
      snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)(local_cached_obj->size()), 0);
    } else {
      snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(obj->size()), 0);
    }
  }

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
  if (SnicGCRegionAddressTranslation) {
    ShenandoahSNICCMTaskAddressTranslation task(N, message, this, queue_indexes, false);
    snic_gc_workers->run_task(&task);
  } else {
    ShenandoahSNICCMTask task(N, message, this, queue_indexes, false);
    snic_gc_workers->run_task(&task);
  }

  finish_handle_task_queue_roots();
}

void CopyRegionSnicClient::handle_satb_roots(int bufferSize, void* payload) {
  start_handle_satb_roots();
  sent_satb_roots_req = false;
  handleRoot(bufferSize, payload);
  finish_handle_satb_roots();
}

void CopyRegionSnicClient::handle_satb_roots_commit(int bufferSize, void* payload) {
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
        region_info[j]._live_data += (size_t)(live_data_caches[i][j]);
        live_data_caches[i][j] = 0;
      }
      // if (ongoing_mark_caches[i][j] > 0) {
      //   region_info[j]._marked_data += (int64_t)(ongoing_mark_caches[i][j]);
      //   ongoing_mark_caches[i][j] = 0;
      // }
    }
  }
  int total_live = 0;
  for (int i = 0; i < (int)heapRegionNumber; ++i) {
    if (region_info[i].top == 0) {
      continue;
    }
    live_count[i] = (uint64_t)(region_info[i]._live_data);
    total_live += live_count[i];
  }
  writeBitmapRDMA();
  writeLivenessRDMA();
  waitBitmapCQ();
  waitLivenessCQ();
  send_back_int_ack((size_t)(SATB_ROOTS_FINISH_ACK));
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

void CopyRegionSnicClient::handleRootAndCommit(int bufferSize, void* payload) {
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
          region_info[j]._live_data += (size_t)(live_data_caches[i][j]);
          live_data_caches[i][j] = 0;
        }
        // if (ongoing_mark_caches[i][j] > 0) {
        //   region_info[j]._marked_data += (int64_t)(ongoing_mark_caches[i][j]);
        //   ongoing_mark_caches[i][j] = 0;
        // }
      }
    }
    int total_live = 0;
    for (int i = 0; i < (int)heapRegionNumber; ++i) {
      if (region_info[i].top == 0) {
        continue;
      }
      live_count[i] = (uint64_t)(region_info[i]._live_data);
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

void CopyRegionSnicClient::initBitmapQP(void* ptr, size_t sz, int hostId) {
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

void CopyRegionSnicClient::initLivenessQP(void* ptr, size_t sz, int hostId) {
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

void CopyRegionSnicClient::initGCRootsBufferQP(void* ptr, size_t sz, int hostId) {
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

void CopyRegionSnicClient::initForceGCByDPUClientQP(void* ptr, size_t sz, int hostId) {
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

void CopyRegionSnicClient::initRdmaPrefetchFinishFlagQP(void* ptr, size_t sz, int hostId) {
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

void CopyRegionSnicClient::writeForceGCByDPUClientRDMA() {
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

void CopyRegionSnicClient::waitForceGCByDPUClientCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for force gc by dpu client cq", _gc_id);
  auto res_p = qp_force_gc_by_dpu_client->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void CopyRegionSnicClient::writeBitmapRDMA(){
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
void CopyRegionSnicClient::waitBitmapCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for bitmap cq", _gc_id);
  auto res_p = qp_bitmap->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void CopyRegionSnicClient::writeLivenessRDMA(){
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

void CopyRegionSnicClient::waitLivenessCQ(){
  log_debug(gc)("GC(%u)DGC LOG: waiting for liveness cq", _gc_id);
  auto res_p = qp_liveness->wait_one_comp();
  RDMA_ASSERT(res_p == IOCode::Ok);
}

void CopyRegionSnicClient::writeRdmaPrefetchFinishFlagRDMA() {
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

void CopyRegionSnicClient::waitRdmaPrefetchFinishFlagCQ() {
  log_debug(gc)("GC(%u)DGC LOG: waiting for rdma prefetch finish flag cq", _gc_id);
  auto res_p = qp_rdma_prefetch_finish_flag->wait_one_comp();
  if (res_p != IOCode::Ok) {
    log_error(gc)("GC(%u)DGC LOG: wait rdma prefetch finish flag failed", _gc_id);
    exit(0);
  }
}

void CopyRegionSnicClient::handleNewVirtualSpaceNode(int hostId, int bufferSize, void* payload){
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
void CopyRegionSnicClient::bulkSyncCcs(int hostId, unsigned long long ccs_hwm, unsigned long long ccs_base) {
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

void CopyRegionSnicClient::fetchKlass(int hostId, int bufferSize, void* payload){

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
  if (target_node_idx == 0) {
    return;
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

void CopyRegionSnicClient::checkRegionArrivalQueue(uint worker_id) {
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
        if((unsigned long long)obj < (unsigned long long)heapBase || (unsigned long long)obj > (unsigned long long)heapBase + (unsigned long long)heapSize){
          log_error(gc)("DGC LOG: ASSERT! obj_addr=%llx, worker_id=%u, src_worker_id=%u, cur_region_idx=%u", (unsigned long long)obj, worker_id, src_worker_id, cur_region_idx);
          exit(0);
        }
        if (region_info[cur_region_idx].top == 0 ||
            (unsigned long long)obj >= region_info[cur_region_idx].top) {
          continue;
        }
        bool is_humongous_obj = false;
        int uncopied_region_idx = -1;
        if (SnicGCRegionAddressTranslation) {
          ShenandoahMarkTask oneTask(obj, upgraded, false);
          worker_queue->push(oneTask);
          if (task.count_liveness()) {
            snic_concurrent_inc_liveness(cur_region_idx, (intptr_t)(obj), (size_t)(obj->size()), worker_id);
          }
        } else {
          if (oop_is_humongous(obj)) {
            bool push_success = false;
            while (!humongous_oop_regions_copy_finished(obj, &uncopied_region_idx)) {
              if (workerPendingMaps[worker_id]->push(uncopied_region_idx, task, region_copied_bitmap)) {
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
      }
    }
    cur_region_idx = regionArrivalQueue[worker_id]->pop();
  }
}

void CopyRegionSnicClient::checkRegionArrivalQueueAddressTranslation(uint worker_id) {
  // regionArrivalQueue[worker_id]->lock();
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
      while (taskQueue->pop(task)) {
        // bool upgraded = false;
        oop obj = task.obj();
        if((unsigned long long)obj < (unsigned long long)heapBase || (unsigned long long)obj > (unsigned long long)heapBase + (unsigned long long)heapSize){
          log_error(gc)("LHT LOG: ASSERT! obj_addr=%llx, worker_id=%u, src_worker_id=%u, cur_region_idx=%u", (unsigned long long)obj, worker_id, src_worker_id, cur_region_idx);
          exit(0);
        }
        bool is_humongous_obj = false;
        int uncopied_region_idx = -1;
        // if (oop_is_humongous(obj)) {
        //   bool push_success = false;
        //   while (!humongous_oop_regions_copy_finished_address_translation(obj, &uncopied_region_idx)) {
        //     if(workerPendingMaps[worker_id]->push(uncopied_region_idx, task, region_copied_bitmap, this)){
        //       push_success = true;
        //       break;
        //     }
        //   }
        //   if (push_success) {
        //     continue;
        //   }
        // }
        ShenandoahMarkTask oneTask(obj, true, false);
        worker_queue->push(oneTask);
        if (task.count_liveness()) {
          oop local_cached_obj = translate_oop(obj);
          snic_concurrent_inc_liveness(cur_region_idx, (intptr_t)(obj), (size_t)(local_cached_obj->size()), worker_id);
          snic_concurrent_inc_ongoing_mark(cur_region_idx, (intptr_t)(obj), (int64_t)(local_cached_obj->size() - SnicGCEstimatedObjectSize), worker_id);
          // if(root_map.find((int64_t)obj) == root_map.end()){
          //   // log_info(gc)("LHT LOG: snic_concurrent_inc_ongoing_mark for non-root object, obj=%llx, est s=%llu", (uint64_t)obj, (int64_t)SnicGCEstimatedObjectSize);
          // }else{
          //   log_info(gc)("LHT LOG: snic_concurrent_inc_ongoing_mark for root object (2), obj=%llx, act s=%llu, delta s=%llu", (uint64_t)obj, (int64_t)local_cached_obj->size(), (int64_t)(local_cached_obj->size() - SnicGCEstimatedObjectSize));
          // }
        }
      }
    }

    // done push all tasks of this region into this worker's task queue.
    // unset read lock to region.
    // //debug
    // {
    //   Atomic::dec(&debug_counter_read_lock);
    //   log_info(gc)("LHT LOG: debug_counter_read_lock=%d, worker_id=%u, cur_region_idx=%d, cur_region_idx=%lu", Atomic::load(&debug_counter_read_lock), worker_id, cur_region_idx, cur_region_idx);
    // }
    region_bitmap_dec_read_lock(cur_region_idx);
    // LOCK
    // region_bitmap_dec_read_lock(cur_region_idx);
    cur_region_idx = regionArrivalQueue[worker_id]->pop();
  }
  // regionArrivalQueue[worker_id]->unlock();
  return;
}

bool CopyRegionSnicClient::checkMarkerTerminateCondition(uint worker_id) {
  if(SnicGCRegionAddressTranslation){
    return Atomic::load(&terminate_condition_for_address_translation);
  }
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
  if (left_tasks_num == 0) {
    ret = true;
  } else {
    // case: a worker fail to steal/pop new task, but still some tasks left, return false to try to steal again.
    finished_worker_count--;
  }
  task_count_lock.unlock();
  return ret;
}

bool CopyRegionSnicClient::checkRDMAControllerTerminateCondition() {
  // first check all region arrival queues are empty
  int rst = true;
  // for(int i = 0; i < snic_gc_workers->active_workers(); i++){
  //   // if(regionArrivalQueue[i]->try_lock() != 0){
  //   //   rst = false;
  //   //   break;
  //   // }
  // }
  int reason = -1;
  if(rst){
    for(int i = 0; i < snic_gc_workers->active_workers(); i++){
      if(!regionArrivalQueue[i]->is_empty()){
        rst = false;
        reason = 1;
        break;
      }
    }
  }
  if(rst){
    for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
      auto que = get_queue(i);
      if(que->size() + que->overflow_stack()->size() > 0){
        rst = false;
        reason =2;
        break;
      }
    }
  }
  if(rst){
    for(int i = 0; i < snic_gc_workers->active_workers(); i++){
      for(int j = 0; j < (int) heapRegionNumber; j++){
        auto taskQueue = workerPendingMaps[i]->workerLocalVec[j];
        if(taskQueue->is_empty() == false){
          rst = false;
          reason =3;
          break;
        }
      }
    }
  }
  // for(int i = 0; i < snic_gc_workers->active_workers(); i++){
  //   regionArrivalQueue[i]->unlock();
  // }
  if(rst){
    log_info(gc)("LHT LOG: checkRDMAControllerTerminateCondition, rst=true");
  }

  debug_trigger_rdma++;
  if(debug_trigger_rdma % 1000000 == 0){
    int total_ongoing = 0;
    for(int i = 0; i < heapRegionNumber; i++){
      total_ongoing += ongoing_mark_count_table[i];
    }
    log_info(gc)("LHT LOG: checkRDMAControllerTerminateCondition, total_ongoing=%d, reason=%d", total_ongoing, reason);
    // log_info(gc)("LHT LOG: checkRDMAControllerTerminateCondition, reason=%d", reason);
  }
  return rst;
}

void CopyRegionSnicClient::snic_do_compressed_oops(uint worker_id) {
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
                if (checkMarkerTerminateCondition(worker_id)) {
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
                if (checkMarkerTerminateCondition(worker_id)) {
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
          if (checkMarkerTerminateCondition(worker_id)) {
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
      }
      log_debug(gc)("DGC LOG: worker %u sends back liveness %lu", worker_id, (size_t) (total_liveness_tmp - worker_total_liveness));
      send_back_int_ack((size_t) (total_liveness_tmp - worker_total_liveness));
      worker_total_liveness = total_liveness_tmp;
    }
    if (SnicGCCoorHeuristic && work_count % RDMA_DGC_LIVENESS_UPDATE_THRESHOLD == 0) {
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
      for (; p < end; p++) {
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


void CopyRegionSnicClient::snic_do_compressed_oops_address_translation(uint worker_id) {
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
          log_info(gc)("LHT LOG: force workers terminate, last worker:%u", worker_id);
          Atomic::sub(&should_force_tasks_finish, 1);
          for (uint j = 0; j < snic_gc_workers->active_workers(); ++j) {
            auto que = get_queue(j);
            auto left_size = que->size() + que->overflow_stack()->size();
            log_info(gc)("LHT LOG: worker %u left size:%lu", j, left_size);
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
      checkRegionArrivalQueueAddressTranslation(worker_id);
    }
    if (!worker_queue->pop(popTask)) {
      if (!task_queues->steal(worker_id, popTask)) {
        if (SnicConcCopyRegion) {
          if (is_prefetched == 0) {
            if (!SnicGCCoorHeuristic) {
              if (cur_handling_rpc_type == 10) {
                if (checkMarkerTerminateCondition(worker_id)) {
                  break;
                }
                os::naked_short_sleep(1);
                continue;
              }
              os::naked_short_sleep(1);
              has_sleeped += 1;
              if (has_sleeped % 10000 == 0) {
                log_info(gc)("LHT LOG: worker %u has slept for %u times", worker_id, has_sleeped);
                exit(0);
              }
              continue;
            } else {
              if (cur_handling_rpc_type == 10 || cur_handling_rpc_type == 2) {
                if (checkMarkerTerminateCondition(worker_id)) {
                  break;
                }
                os::naked_short_sleep(1);
                continue;
              } else {
                break;
              }
            }
          } else {
            break;
          }
        } else {
          if (checkMarkerTerminateCondition(worker_id)) {
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
    oop remote_obj = popTask.obj();
    // log_info(gc)("LHT LOG: do_oops remote_obj=%p", remote_obj);
    int remote_region_idx = (reinterpret_cast<unsigned long long>(remote_obj) - (unsigned long long)heapBase) >> regionSizeShift;

    // lock start
    // int locked = true;
    unsigned long long res = 0;
    // if(confirm_region_bitmap(REGION_COPIED, Atomic::load(&region_copied_bitmap[remote_region_idx]))){
    //   locked = false;
    // }else{
    res = region_bitmap_add_read_lock(remote_region_idx);
    // log_info(gc)("LHT LOG: region_bitmap_add_read_lock (2), recv_region_idx=%lu, res=%llu", remote_region_idx, res);
    if(get_region_bitmap_base_state(res) != REGION_COPIED){
      ShenandoahMarkTask oneTask(remote_obj, true, false);
      if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
        region_bitmap_dec_read_lock(remote_region_idx);
        // log_info(gc)("LHT LOG: region_bitmap_dec_read_lock (2), remote_region_idx=%lu", remote_region_idx);
        continue;
      }
      else{
        log_error(gc)("LHT LOG: ASSERT! workerPendingMaps[worker_id]->push failed, remote_region_idx=%lu, res=%llu", remote_region_idx, res);
        os::abort();
      }
    }
    // }

    //since lock is confirmed, no check is necessary here
    oop local_cached_obj = translate_oop(remote_obj);

    work_count++;
    if (DpuClientLivenessUpdateEnabled && work_count % DpuClientLivenessUpdateThreshold == 0) {
      uint64_t total_liveness_tmp = 0;
      for (size_t i = 0; i < heapRegionNumber; ++i) {
        total_liveness_tmp += live_data_caches[worker_id][i];
      }
      log_info(gc)("LHT LOG: worker %u sends back liveness %lu", worker_id, (size_t) (total_liveness_tmp - worker_total_liveness));
      send_back_int_ack((size_t) (total_liveness_tmp - worker_total_liveness));
      worker_total_liveness = total_liveness_tmp;
    }
    if (SnicGCCoorHeuristic && work_count % RDMA_DGC_LIVENESS_UPDATE_THRESHOLD == 0) {
      uint64_t total_liveness_tmp = 0;
      for (size_t i = 0; i < heapRegionNumber; ++i) {
        total_liveness_tmp += live_data_caches[worker_id][i];
      }
      auto target_liveness = total_liveness_tmp - prev_total_liveness[worker_id];
      prev_total_liveness[worker_id] = total_liveness_tmp;
      add_marked_liveness(target_liveness);
    }
    bool should_count_liveness = popTask.count_liveness();
    if (local_cached_obj->klass()->id() == ObjArrayKlassID) {
      objArrayOop a = objArrayOop(local_cached_obj);
      narrowOop* p = (narrowOop*)(a->base());
      narrowOop* const end = p + a->length();
      for (; p < end; p++) {
        // if(!region_is_local(remote_region_idx)){
        //   log_error(gc)("LHT LOG: ASSERT! objArrayKlassID is not local, remote_region_idx=%lu", remote_region_idx);
        //   os::abort();
        //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
        //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
        //   //   break;
        //   // }
        // }
        snic_do_compressed_oop(p, worker_id, 1, should_count_liveness);
      }
    } else if (InstanceKlass::cast(local_cached_obj->klass())->reference_type() != REF_NONE) {
      OopMapBlock *map = ((InstanceKlass *)local_cached_obj->klass())->start_of_nonstatic_oop_maps();
      OopMapBlock *end_map = map + ((InstanceKlass *)local_cached_obj->klass())->nonstatic_oop_map_count();
      for (; map < end_map; ++map) {
        narrowOop* p = (narrowOop *)local_cached_obj->field_addr(map->offset());
        narrowOop* end = p + map->count();
        for (; p < end; ++p) {
          // if(!region_is_local(remote_region_idx)){
          //   log_error(gc)("LHT LOG: ASSERT! non-static oop map is not local, remote_region_idx=%lu", remote_region_idx);
          //   os::abort();
          //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
          //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
          //   //   break;
          //   // }
          // }
          snic_do_compressed_oop(p, worker_id, 2, should_count_liveness);
        }
      }
      narrowOop* referent_addr = NULL;
      if (UseCompressedClassPointers) {
        referent_addr = (narrowOop *)((unsigned long long) (local_cached_obj) + 12);
      } else {
        referent_addr = (narrowOop *)((unsigned long long) (local_cached_obj) + 16);
      }
      if (true) {
        // if(!region_is_local(remote_region_idx)){
        //   log_error(gc)("LHT LOG: ASSERT! referent is not local, remote_region_idx=%lu", remote_region_idx);
        //   os::abort();
        //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
        //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
        //   //   continue;
        //   // }
        // }
        snic_do_compressed_oop(referent_addr, worker_id, 3, should_count_liveness);
      }
    } else {
      OopMapBlock *map = ((InstanceKlass *)local_cached_obj->klass())->start_of_nonstatic_oop_maps();
      OopMapBlock *end_map = map + ((InstanceKlass *)local_cached_obj->klass())->nonstatic_oop_map_count();
      for (; map < end_map; ++map) {
        narrowOop *p = (narrowOop*) local_cached_obj->field_addr(map->offset());
        narrowOop *end = p + map->count();
        for (; p < end; ++p) {
          // if(!region_is_local(remote_region_idx)){
          //   log_error(gc)("LHT LOG: ASSERT! non-static oop map is not local, remote_region_idx=%lu", remote_region_idx);
          //   os::abort();
          //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
          //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
          //   //   break;
          //   // }
          // }
          snic_do_compressed_oop(p, worker_id, 2, should_count_liveness);
        }
        // if(!region_is_local(remote_region_idx)){
        //   log_error(gc)("LHT LOG: ASSERT! non-static oop map is not local, remote_region_idx=%lu", remote_region_idx);
        //   os::abort();
        //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
        //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
        //   //   break;
        //   // }
        // }
      }
      if (local_cached_obj->klass()->id() == InstanceMirrorKlassID) {
        narrowOop *p = (narrowOop*) ((InstanceMirrorKlass *)local_cached_obj->klass())->start_of_static_fields(local_cached_obj);
        narrowOop *const end = p + java_lang_Class::static_oop_field_count_raw(local_cached_obj);
        for (; p < end; ++p) {
          // if(!region_is_local(remote_region_idx)){
          //   log_error(gc)("LHT LOG: ASSERT! static field is not local, remote_region_idx=%lu", remote_region_idx);
          //   os::abort();
          //   // ShenandoahMarkTask oneTask(remote_obj, true, false);
          //   // if(workerPendingMaps[worker_id]->push(remote_region_idx, oneTask, region_copied_bitmap, this)){
          //   //   break;
          //   // }
          // }
          snic_do_compressed_oop(p, worker_id, 4, should_count_liveness);
        }
      }
    }
    snic_concurrent_dec_ongoing_mark(remote_region_idx, (intptr_t)(remote_obj), (int64_t)(local_cached_obj->size()), worker_id);
    // lock end
    // if(locked){
    region_bitmap_dec_read_lock(remote_region_idx);
    // }
    // log_info(gc)("LHT LOG: region_bitmap_dec_read_lock (5), remote_region_idx=%lu", remote_region_idx);
  }
  log_info(gc)("GC(%u,%d) LHT LOG: worker %u finish snic_do_compressed_oops, work_count:%u,steal count:%u", _gc_id, clientId, worker_id, work_count, steal_count);
}

void CopyRegionSnicClient::snic_do_compressed_oop(narrowOop* p, uint worker_id, int type, bool should_count_liveness) {
  narrowOop o = RawAccess<>::oop_load(p);
  // {
  //   unsigned long long local_region_idx = (reinterpret_cast<unsigned long long>(p) - (unsigned long long)local_memory_pool_base) >> regionSizeShift;
  //   unsigned long long remote_region_idx = local_addr_translation_table[local_region_idx].remote_region_id;
    // log_info(gc)("LHT LOG: snic_do_compressed_oop, p=%p, local_region_idx=%llu, remote_region_idx=%llu, region_copied_bitmap=%llu", p, local_region_idx, remote_region_idx, region_copied_bitmap[remote_region_idx]);
  // }
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
      // auto it = region_info[r_idx_o];
      if (region_info[r_idx_o].top != 0) {
        unsigned long long obj_addr = (unsigned long long)obj;
        if (obj_addr >= region_info[r_idx_o].top) {
          return;
        }
        bool mark_rst = bitmap->mark_strong((HeapWord *)obj, upgraded);
        if (!mark_rst) {
          return;
        }
        // logic to support concurrent copy region
        if (SnicConcCopyRegion) {
          auto res = region_bitmap_add_read_lock(r_idx_o);
          // log_info(gc)("LHT LOG: region_bitmap_add_read_lock (3), recv_region_idx=%lu, res=%llu", r_idx_o, res);
          if(get_region_bitmap_base_state(res) != REGION_COPIED){
            ShenandoahMarkTask oneTask(obj, false, false);
            if (SnicGCRegionAddressTranslation) {
              if (workerPendingMaps[worker_id]->push(r_idx_o, oneTask, region_copied_bitmap, this)) {
                snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)(SnicGCEstimatedObjectSize), worker_id);
                region_bitmap_dec_read_lock(r_idx_o);
                return;
              } else {
                log_error(gc)("LHT LOG: ASSERT! workerPendingMaps[worker_id]->push failed, r_idx_o=%lu, res=%llu", r_idx_o, res);
                os::abort();
              }
            } else {
              if (workerPendingMaps[worker_id]->push(r_idx_o, oneTask, region_copied_bitmap)) {
                if (is_prefetched == 1) {
                  log_debug(gc)("DGC LOG: worker %u push region %llu to pending map (3), is_prefetched=%llu", worker_id, r_idx_o, is_prefetched);
                }
                return;
              }
            }
          }
          if (SnicGCRegionAddressTranslation) {
            ShenandoahMarkTask pushTask(obj, true, false);
            worker_queue->push(pushTask);
            oop local_cached_obj = translate_oop(obj);
            snic_concurrent_inc_liveness(r_idx_o, (intptr_t)(obj), (size_t)(local_cached_obj->size()), worker_id);
            snic_concurrent_inc_ongoing_mark(r_idx_o, (intptr_t)(obj), (int64_t)(local_cached_obj->size()), worker_id);
            region_bitmap_dec_read_lock(r_idx_o);
          } else {
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
            if (region_info[r_idx_o].top == 0 ||
                (unsigned long long)obj >= region_info[r_idx_o].top) {
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
}



bool CopyRegionSnicClient::check_is_narrow_oop(HeapWord* p) {
  uint32_t first_uint_val = *(uint32_t*) (p);
  // min_narrow_val = 0xa0000000 when HeapBase = 0x500000000
  uint32_t min_narrow_val = (uint32_t) ((heapBase - (unsigned long long) (COMPRESSED_OOP_BASE)) >> (unsigned long long) (COMPRESSED_OOP_SHIFT));
  if (min_narrow_val > first_uint_val) {
    return false;
  }
  return true;
}

void CopyRegionSnicClient::snic_concurrent_inc_liveness(unsigned long long r, intptr_t start_addr, size_t s, uint worker_id) {
  // NOTE: huge objects will use multiple regions, CAN NOT put the entire size into a region's live_data.
  unsigned long long actual_r = ((unsigned long long)start_addr - (unsigned long long)heapBase) >> regionSizeShift;
  unsigned long long end_addr = (unsigned long long) (start_addr) + s * 8;
  while (region_info[actual_r].top != 0 && region_info[actual_r].end < end_addr) {
    size_t delta = (size_t)((region_info[actual_r].end - start_addr) / HeapWordSize);
    live_data_caches[worker_id][actual_r] += (uint64_t) (delta);
    start_addr = region_info[actual_r].end;
    actual_r += 1;
  }
  size_t final_delta = (size_t)((end_addr - start_addr) / HeapWordSize);
  live_data_caches[worker_id][actual_r] += (uint64_t) (final_delta);
}

void CopyRegionSnicClient::snic_concurrent_inc_ongoing_mark(unsigned long long r, intptr_t start_addr, int64_t s, uint worker_id) {
  // mtx2.lock();

  unsigned long long actual_r = ((unsigned long long)start_addr - (unsigned long long)heapBase) >> regionSizeShift;
  Atomic::add(&(ongoing_mark_caches[worker_id][actual_r]), s);



  // Atomic::add(&total_ongoing_mark_size, s);
  // if(obj_trace_map.find(start_addr) == obj_trace_map.end()){
  //   // new
  //   if(s == SnicGCEstimatedObjectSize){
  //     obj_trace_map[start_addr] = 1; // guess
  //     obj_trace_map2[start_addr] = s;
  //   }
  //   else{
  //     obj_trace_map[start_addr] = 2; // actual
  //     obj_trace_map2[start_addr] = s;
  //   }
  // }
  // else{
  //   if(obj_trace_map[start_addr] == 1){
  //     obj_trace_map[start_addr] = 2;  // actual
  //     obj_trace_map2[start_addr] += s;
  //   }
  //   else{
  //     //err
  //     log_error(gc)("LHT ERR: snic_concurrent_inc_ongoing_mark, start_addr=%llx, s=%lld, obj_trace_map[%llx]=%d", start_addr, s, start_addr, obj_trace_map[start_addr]);
  //     exit(0);
  //   }
  // }
  // Atomic::store(&total_ongoing_oop, (int)obj_trace_map.size());
  // if(obj_trace_map3.find(actual_r) == obj_trace_map3.end()){
  //   obj_trace_map3[actual_r] = s;
  // }
  // else{
  //   obj_trace_map3[actual_r] += s;
  // }
  // int64_t total_ongoing = 0;
  // for(int i = 0; i < snic_gc_workers->active_workers(); ++i){
  //   total_ongoing += ongoing_mark_caches[i][actual_r];
  // }

  // mtx2.unlock();


}

void CopyRegionSnicClient::snic_concurrent_dec_ongoing_mark(unsigned long long r, intptr_t start_addr, int64_t s, uint worker_id) {
  // mtx2.lock();

  unsigned long long actual_r = ((unsigned long long)start_addr - (unsigned long long)heapBase) >> regionSizeShift;
  Atomic::add(&(ongoing_mark_caches[worker_id][actual_r]), -s);


  // Atomic::add(&total_ongoing_mark_size, -s);
  // if(obj_trace_map.find(start_addr) == obj_trace_map.end()){
  //   log_error(gc)("LHT ERR: snic_concurrent_dec_ongoing_mark, start_addr=%llx, s=%lld, obj_trace_map[%llx] not found", start_addr, s, start_addr);
  //   exit(0);
  // }
  // else{
  //   if(obj_trace_map[start_addr] == 1){
  //     if(s != SnicGCEstimatedObjectSize){
  //       log_error(gc)("LHT ERR: snic_concurrent_dec_ongoing_mark, start_addr=%llx, s=%lld, obj_trace_map[%llx]=1, s=%lld", start_addr, s, start_addr, s);
  //       exit(0);
  //     }
  //   }
  //   if(s != obj_trace_map2[start_addr]){
  //     log_error(gc)("LHT ERR: snic_concurrent_dec_ongoing_mark, start_addr=%llx, s=%lld, obj_trace_map2[%llx]=%d, s=%lld", start_addr, s, start_addr, obj_trace_map2[start_addr], s);
  //     if(root_map.find((int64_t)start_addr) == root_map.end()){
  //       log_error(gc)("LHT ERR: snic_concurrent_dec_ongoing_mark, obj is not root object");
  //       // exit(0);
  //     }else{
  //       log_info(gc)("LHT LOG: snic_concurrent_dec_ongoing_mark, obj is root object");
  //     }
  //     exit(0);
  //   }
  //   obj_trace_map.erase(start_addr);
  //   obj_trace_map2.erase(start_addr);
  // }

  // Atomic::store(&total_ongoing_oop, (int)obj_trace_map.size());

  // if(obj_trace_map3.find(actual_r) == obj_trace_map3.end()){
  //   obj_trace_map3[actual_r] = - s;
  // }
  // else{
  //   obj_trace_map3[actual_r] -= s;
  // }
  // int64_t total_ongoing = 0;
  // for(int i = 0; i < snic_gc_workers->active_workers(); ++i){
  //   total_ongoing += ongoing_mark_caches[i][actual_r];
  // }
  // mtx2.unlock();

}


int CopyRegionSnicClient::copy_one_region_async_auto_huge(unsigned long long region_idx, bool is_prefetch, bool force) {
  if (region_info[region_idx].state != _humongous_start){
    return copy_one_normal_region_async(region_idx, is_prefetch, force);
  }
  else{
    int huge_start = region_idx;
    int huge_size = 0;
    do{
      region_idx++;
      huge_size++;
    } while (region_info[region_idx].state == _humongous_cont);
    log_info(gc)("LHT LOG: copy_one_region_async_auto_huge, huge_start=%d, huge_size=%d", huge_start, huge_size);
    return copy_one_huge_region_async(huge_start, huge_size, true, force);
  }
}


int CopyRegionSnicClient::copy_one_normal_region_async(unsigned long long region_idx, bool is_prefetch, bool force) {
  if(SnicGCRegionAddressTranslation){
    int candidate_hotness = 0;
    if(force){
      candidate_hotness = INT_MAX;
    }else{
      // candidate_hotness = region_info[region_idx].ongoing_mark_size;
      candidate_hotness = ongoing_mark_count_table[region_idx];
      // if(candidate_hotness == 0 && !force){
      //   return 0;
      // }
    }
    int loacl_region_cache_index = select_local_cache_normal(candidate_hotness);
    if(loacl_region_cache_index == -1){
      if(force){
        log_error(gc)("LHT LOG: no available region range with size %d, abort", region_info[region_idx].ongoing_mark_size);
        os::abort();
      }
      else{
        return 0;
      }
    }
    return copy_one_region_async_address_translation(region_idx, loacl_region_cache_index, is_prefetch);
  }
  else{
    return copy_one_region_async_no_address_translation(region_idx, is_prefetch);
  }
}

int CopyRegionSnicClient::copy_one_region_async_no_address_translation(unsigned long long region_idx, bool is_prefetch) {
  // metadata update
  // region_copied_bitmap[region_idx] = REGION_ON_TRANS;
  region_bitmap_set_value_with_write_lock(region_idx, REGION_NOT_COPIED, REGION_ON_TRANS);
  // lock_read_region_bitmap(region_idx);
  // loop_until_change_region_bitmap(region_idx, REGION_NOT_COPIED, REGION_ON_TRANS);
  // set_region_bitmap(region_idx, REGION_ON_TRANS);

  // issue RDMA
  auto region_bottom = region_info[region_idx].bottom;
  auto region_top = region_info[region_idx].top;
  if(region_top <= heapBase2){

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
  else if (region_bottom >= heapBase2) {
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
    log_error(gc)("LHT LOG: no, this region is between heap1 and heap2");
    log_error(gc)("LHT LOG: region_bottom: %llu, region_top: %llu, heapBase1: %llu, heapBase2: %llu", region_bottom, region_top, heapBase, heapBase2);
    exit(0);
  }
  on_trans_region_number += 1;
  return 1;
}

int CopyRegionSnicClient::copy_one_region_async_address_translation(unsigned long long region_idx, unsigned long long loacl_region_cache_index, bool is_prefetch) {
  region_trans_counter ++;
  // metadata update
  if(local_addr_translation_table[loacl_region_cache_index].remote_region_id != USHRT_MAX){
    auto evicted_region_idx = local_addr_translation_table[loacl_region_cache_index].remote_region_id;
    region_info[region_idx].kick_out_region_id = evicted_region_idx;
    // region_copied_bitmap[evicted_region_idx] = REGION_ON_EVICTION;
    region_bitmap_set_value_with_write_lock(evicted_region_idx, REGION_COPIED, REGION_ON_EVICTION);
  }
  else{
    local_addr_translation_table[loacl_region_cache_index].remote_region_id = region_idx;
    local_mem_pool_free_region_count --;
  }
  region_info[region_idx].local_region_id = loacl_region_cache_index;
  // region_copied_bitmap[region_idx] = REGION_ON_TRANS;
  region_bitmap_set_value_with_write_lock(region_idx, REGION_NOT_COPIED, REGION_ON_TRANS);

  // issue RDMA
  auto remote_region_bottom = region_info[region_idx].bottom;
  auto remote_region_top = region_info[region_idx].top;
  auto local_region_bottom = (unsigned long long)(local_memory_pool_base) + ((unsigned long long)loacl_region_cache_index << regionSizeShift);
  // log_info(gc)("LHT LOG: copy_one_region_async_address_translation, region_idx=%llu, loacl_region_cache_index=%llu, remote_region_bottom=%llx, remote_region_top=%llx, local_region_bottom=%llx", region_idx, loacl_region_cache_index, remote_region_bottom, remote_region_top, local_region_bottom);
  if(remote_region_top <= heapBase2){
    auto res_s = qp_heap->send_normal_direct(
    {.op = IBV_WR_RDMA_READ,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(remote_region_top - remote_region_bottom),
      .wr_id = (uint64_t) (region_idx)},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_region_bottom),
      .remote_addr = remote_region_bottom,
      .imm_data = 0});
    RDMA_ASSERT(res_s == IOCode::Ok);
    on_trans_region_number1 += 1;
  }
  else if(remote_region_bottom >= heapBase2){
    auto res_s = qp_heap2->send_normal_direct(
    {.op = IBV_WR_RDMA_READ,
      .flags = IBV_SEND_SIGNALED,
      .len = (unsigned int)(remote_region_top - remote_region_bottom),
      .wr_id = (uint64_t) (region_idx)},
    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_region_bottom),
      .remote_addr = remote_region_bottom,
      .imm_data = 0});
    RDMA_ASSERT(res_s == IOCode::Ok);
    on_trans_region_number2 += 1;
  }
  else{
    log_error(gc)("LHT LOG: no, this region is between heap1 and heap2");
    log_error(gc)("LHT LOG: region_bottom: %llu, region_top: %llu, heapBase1: %llu, heapBase2: %llu", remote_region_bottom, remote_region_top, heapBase, heapBase2);

    exit(0);
  }

  on_trans_region_number += 1;
  return 1;
}


int CopyRegionSnicClient::copy_one_huge_region_async(unsigned long long region_idx, int huge_region_size, bool is_prefetch, bool force) {
  // assert region_idx is _humongous_start
  if(region_info[region_idx].state != _humongous_start){
    log_error(gc)("LHT LOG: ASSERT! region_idx=%llu, state=%d", region_idx, region_info[region_idx].state);
    1/0;
    os::abort();
  }
  if(SnicGCRegionAddressTranslation){
    int candidate_hotness = 0;
    int loacl_region_cache_index_start = select_local_cache_huge(huge_region_size);
    if(loacl_region_cache_index_start == -1){
      log_info(gc)("LHT LOG: no available region range with size %d, abort", huge_region_size);
      os::abort();
    }
    for(int i = 0; i < huge_region_size; ++i){
      copy_one_region_async_address_translation(region_idx + i, loacl_region_cache_index_start + i, is_prefetch);
    }
    return huge_region_size;
  }
  else{
    for(int i = 0; i < huge_region_size; ++i){
      copy_one_region_async_no_address_translation(region_idx + i, is_prefetch);
    }
    return huge_region_size;
  }
}


ShenandoahObjToScanQueue* CopyRegionSnicClient::get_queue(uint worker_id) {
  return task_queues->queue(worker_id);
}

void CopyRegionSnicClient::initialize() {
  prev_total_liveness = new uint64_t[snic_gc_workers->active_workers()];
  memset(prev_total_liveness, 0, sizeof(uint64_t) * snic_gc_workers->active_workers());
  try_mark_counts = new size_t[snic_gc_workers->active_workers() * 8];
  success_mark_counts = new size_t[snic_gc_workers->active_workers() * 8];
  memset(try_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
  memset(success_mark_counts, 0, sizeof(size_t) * snic_gc_workers->active_workers() * 8);
  _gc_id = -1;
  pending_count_table.resize(heapRegionNumber);
  ongoing_mark_count_table.resize(heapRegionNumber);
  workerPendingMaps = new WorkerPendingMap*[snic_gc_workers->active_workers()];
  for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
    workerPendingMaps[i] = new WorkerPendingMap(heapRegionNumber);
  }

  regionArrivalQueue = new RegionArrivalQueue*[snic_gc_workers->active_workers()];
  for (uint i = 0; i < snic_gc_workers->active_workers(); ++i) {
    regionArrivalQueue[i] = new RegionArrivalQueue(heapRegionNumber);
    regionArrivalQueue[i]->clear();
  }


  region_copied_bitmap = new unsigned long long[heapRegionNumber];
  memset((unsigned long long*)region_copied_bitmap, (unsigned long long) (REGION_NOT_COPIED), sizeof(unsigned long long) * heapRegionNumber);
  live_data_caches = new uint64_t*[(int) (ConcGCThreads)];
  ongoing_mark_caches = new int64_t*[(int) (ConcGCThreads)];
  for (int i = 0; i < (int) (ConcGCThreads); ++i) {
    live_data_caches[i] = new uint64_t[heapRegionNumber];
    memset(live_data_caches[i], 0, sizeof(uint64_t) * heapRegionNumber);
    ongoing_mark_caches[i] = new int64_t[heapRegionNumber];
    memset(ongoing_mark_caches[i], 0, sizeof(int64_t) * heapRegionNumber);
  }
  // init region_info here.
  for (int i = 0; i < (int) (heapRegionNumber); ++i) {
    region_info.push_back(SnicHeapRegion());
  }
  log_debug(gc)("DGC LOG: finish initialize func of CopyRegionSnicClient, region_info size:%lu", region_info.size());
  // TEST ONLY: output some contants here to check whether they are correct
  log_debug(gc)("DGC LOG:arrayOop::base_offset:%d, length_offset:%d", arrayOopDesc::base_offset_in_bytes(T_OBJECT), arrayOopDesc::length_offset_in_bytes());
  log_debug(gc)("DGC LOG:referent_offset:%d", java_lang_ref_Reference::referent_offset());
}

void CopyRegionSnicClient::issue_fetch_region_by_pending_length_heap1(uint candidate_region_number) {
  // log_debug(gc)("DGC LOG: issue_fetch_region_by_pending_length, candidate_region_number:%u", candidate_region_number);
  memset(pending_count_table.data(), 0, sizeof(int) * heapRegionNumber);
  for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
    for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
      auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[region_idx];
      pending_count_table[region_idx] += taskQueue->size() + (int)(! taskQueue->is_empty());
    }
  }
  for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
    if(region_info[region_idx].top == 0 || !region_is_uncopied(region_idx)){
      pending_count_table[region_idx] = -1;
    }
  }
  // if (candidate_region_number < 20) {
    // find the region with the maximum pending count.
  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_debug(gc)("DGC LOG: %d", j);
    if (region_info[j].top == 0 || region_info[j].bottom >= heapBase2) {
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
      int huge_size = copy_one_region_async_auto_huge(candidate_list[i].second, false, false);
      (void)huge_size;


    }
  }
  else{
    log_error(gc)("DGC LOG: no region to copy in heap1");
    log_error(gc)("DGC LOG: done_trans_region_number1: %lu, done_trans_region_number2: %lu, on_trans_region_number1: %lu, on_trans_region_number2: %lu, received_region_num1: %d, received_region_num2: %d, received_region_num: %d", done_trans_region_number1, done_trans_region_number2, on_trans_region_number1, on_trans_region_number2, received_region_num1, received_region_num2, received_region_num);
    exit(0);
  }
}



void CopyRegionSnicClient::issue_fetch_region_by_pending_length_heap2(uint candidate_region_number) {
  // log_debug(gc)("DGC LOG: issue_fetch_region_by_pending_length, candidate_region_number:%u", candidate_region_number);
  memset(pending_count_table.data(), 0, sizeof(int) * heapRegionNumber);
  for (uint worker_id = 0; worker_id < snic_gc_workers->active_workers(); ++worker_id) {
    for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
      auto taskQueue = workerPendingMaps[worker_id]->workerLocalVec[region_idx];
      pending_count_table[region_idx] += taskQueue->size() + (int)(! taskQueue->is_empty());
    }
  }
  for (uint region_idx = 0; region_idx < heapRegionNumber; ++region_idx) {
    if(region_info[region_idx].top == 0 || !region_is_uncopied(region_idx)){
      pending_count_table[region_idx] = -1;
    }
  }
  // if (candidate_region_number < 20) {
    // find the region with the maximum pending count.
  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_debug(gc)("DGC LOG: %d", j);
    if (region_info[j].top == 0 || region_info[j].top <= heapBase2) {
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
      int huge_size = copy_one_region_async_auto_huge(candidate_list[i].second, false, false);
      (void)huge_size;

    }
  }
  else{
    log_error(gc)("DGC LOG: no region to copy in heap2");
    log_error(gc)("DGC LOG: done_trans_region_number1: %lu, done_trans_region_number2: %lu, on_trans_region_number1: %lu, on_trans_region_number2: %lu, received_region_num1: %d, received_region_num2: %d, received_region_num: %d", done_trans_region_number1, done_trans_region_number2, on_trans_region_number1, on_trans_region_number2, received_region_num1, received_region_num2, received_region_num);
    exit(0);
  }
}



int CopyRegionSnicClient::issue_fetch_region_by_pending_length_heap1_address_translation(uint candidate_region_number) {

  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  if(local_mem_pool_free_region_count == 0){
    int threshold = 0;
  }
  if(local_mem_pool_free_region_count < 0){
    log_error(gc)("LHT LOG: local_mem_pool_free_region_count < 0, abort");
    os::abort();
  }
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_info(gc)("LHT LOG: %d", j);
    if(!region_is_uncopied(j) || region_info[j].top == 0 || region_info[j].bottom >= heapBase2 || region_info[j].state == _humongous_cont){
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
      // else{
      //   threshold = -1;
      // }
    }
  }
  int copied_region_number = 0;
  if (candidate_list.size() > 0) {
    for(uint i = 0; i < candidate_region_number && i < candidate_list.size(); ++i){
      int huge_size = copy_one_region_async_auto_huge(candidate_list[i].second, false, false);
      copied_region_number += huge_size;
    }
  }
  return copied_region_number;
}



int CopyRegionSnicClient::issue_fetch_region_by_pending_length_heap2_address_translation(uint candidate_region_number) {

  std::vector<std::pair<int, int>> candidate_list;
  int threshold = -1;
  if(local_mem_pool_free_region_count == 0){
    int threshold = 0;
  }
  if(local_mem_pool_free_region_count < 0){
    log_error(gc)("LHT LOG: local_mem_pool_free_region_count < 0, abort");
    os::abort();
  }
  for(uint j = 0; j < heapRegionNumber; ++j) {
    // log_info(gc)("LHT LOG: %d", j);
    if(!region_is_uncopied(j) || region_info[j].top == 0 || region_info[j].top <= heapBase2 || region_info[j].state == _humongous_cont){
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
      // else{
      //   threshold = -1;
      //   if(local_mem_pool_free_region_count == 0){
      //     int threshold = 0;
      //   }
      // }
    }
  }
  int copied_region_number = 0;
  if (candidate_list.size() > 0) {
    for(uint i = 0; i < candidate_region_number && i < candidate_list.size(); ++i){
      int huge_size = copy_one_region_async_auto_huge(candidate_list[i].second, false, false);
      copied_region_number += huge_size;
    }
  }
  return copied_region_number;
}


void CopyRegionSnicClient::start_handle_task_queue_roots() {
  handling_task_queue_roots = true;
}
void CopyRegionSnicClient::finish_handle_task_queue_roots() {
  handling_task_queue_roots = false;
}

bool CopyRegionSnicClient::during_handle_task_queue_roots() {
  return handling_task_queue_roots;
}

void CopyRegionSnicClient::start_handle_satb_roots() {
  handling_satb_roots = true;
}
void CopyRegionSnicClient::finish_handle_satb_roots() {
  handling_satb_roots = false;
}

bool CopyRegionSnicClient::during_handle_satb_roots() {
  return handling_satb_roots;
}

int CopyRegionSnicClient::copy_remote_gc_roots_buffer(int bufferSize, void* payload) {
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
  send_back_int_ack(REMOTE_GC_ROOTS_COPY_FINISH_ACK);
  log_debug(gc)("DGC LOG: received len to copy:%d", len);
  return len;
}

bool CopyRegionSnicClient::oop_is_humongous(oop obj) {
  if(SnicGCRegionAddressTranslation){
    obj = translate_oop(obj);
  }
  auto region_size = (unsigned long long)(1UL << regionSizeShift);
  auto obj_size = obj->size() * HeapWordSize;
  if ((unsigned long long) obj_size > region_size) {
    return true;
  }
  return false;
  // auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  // if (region_info[start_region_idx].top == 0) {
  //   return false;
  // }
  // return region_info[start_region_idx].state == SnicRegionState::_humongous_start;
}

bool CopyRegionSnicClient::humongous_oop_regions_copy_finished(oop obj) {
  auto size = obj->size();
  auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  objArrayOop a = objArrayOop(obj);
  narrowOop *p = (narrowOop *)(a->base());
  narrowOop *const end = p + a->length();
  auto end_region_idx = ((unsigned long long) (end) - (unsigned long long) heapBase) >> regionSizeShift;
  for (unsigned long long i = start_region_idx; i <= end_region_idx; ++i) {
    if (region_info[i].top == 0) {
      continue;
    }
    if (!region_is_local(i)) {
      // log_debug(gc)("DGC LOG: find humongous oop %p, region %llu not copied,start:%llu,end:%llu,size:%d", obj, i, start_region_idx, end_region_idx, size);
      return false;
    }
  }
  // log_debug(gc)("DGC LOG: find humongous oop %p, all regions are copied,start:%llu,end:%llu,size:%d", obj, start_region_idx, end_region_idx, size);
  return true;
}

bool CopyRegionSnicClient::humongous_oop_regions_copy_finished(oop obj, int* region_idx) {
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
    if (region_info[i].top == 0) {
      continue;
    }
    if (!region_is_local(i)) {
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

bool CopyRegionSnicClient::humongous_oop_regions_copy_finished_address_translation(oop obj) {
  oop local_cached_obj = translate_oop(obj);
  auto size = local_cached_obj->size();
  auto start_region_idx = ((unsigned long long) (obj) - (unsigned long long) heapBase) >> regionSizeShift;
  objArrayOop a = objArrayOop(local_cached_obj);
  narrowOop *p = (narrowOop *)(a->base());
  narrowOop *const end = p + a->length();
  auto end_region_idx = ((unsigned long long) (end) - (unsigned long long) heapBase) >> regionSizeShift;
  for (unsigned long long i = start_region_idx; i <= end_region_idx; ++i) {
    if (region_info[i].top == 0) {
      continue;
    }
    if (!region_is_local(i)) {
      log_info(gc)("LHT LOG: find humongous oop %p, region %llu not copied,start:%llu,end:%llu,size:%d", obj, i, start_region_idx, end_region_idx, size);
      return false;
    }
  }
  // log_info(gc)("LHT LOG: find humongous oop %p, all regions are copied,start:%llu,end:%llu,size:%d", obj, start_region_idx, end_region_idx, size);
  return true;
}

bool CopyRegionSnicClient::humongous_oop_regions_copy_finished_address_translation(oop obj, int* region_idx) {
  oop local_cached_obj = translate_oop(obj);
  auto size = local_cached_obj->size();
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
    printf("LHT LOG: end_region_idx out of range, end_region_idx: %llu, heapRegionNumber: %lu, obj:%p, end:%p\n", end_region_idx, heapRegionNumber, obj, end);
    // int res = 1 / 0;
    end_region_idx = heapRegionNumber - 1;
  }
  for (unsigned long long i = start_region_idx; i <= end_region_idx; ++i) {
    if (region_info[i].top == 0) {
      continue;
    }
    if (!region_is_local(i)) {
      log_info(gc)("LHT LOG: find humongous oop %p, region %llu not copied,start:%llu,end:%llu,size:%d", obj, i, start_region_idx, end_region_idx, size);
      if (i >= heapRegionNumber) {
        printf("LHT LOG: region idx out of range, region idx: %llu, heapRegionNumber: %lu, obj:%p, end:%p\n", i, heapRegionNumber, obj, end);
        int res = 1 / 0;
      }
      *region_idx = i;
      return false;
    }
  }
  // log_info(gc)("LHT LOG: find humongous oop %p, all regions are copied,start:%llu,end:%llu,size:%d", obj, start_region_idx, end_region_idx, size);
  return true;
}


RegionArrivalQueue::RegionArrivalQueue(uint region_num) {
    _region_arrival_queue = new int[region_num*100];
    _head = 0;
    _tail = 0;
    _is_empty = true;
    // pthread_mutex_init(&_mutex, NULL);
}
RegionArrivalQueue::~RegionArrivalQueue() {
    // delete[] _region_arrival_queue;
    // pthread_mutex_destroy(&_mutex);
}
void RegionArrivalQueue::push(uint region_idx) {
    _region_arrival_queue[_tail] = region_idx;
    // modify is_empty first to avoid concurrent error
    Atomic::store(&_is_empty, false);
    Atomic::add(&_tail, 1);
    // log_debug(gc)("DGC LOG: push region_idx: %d, head: %d, tail: %d", region_idx, _head, _tail);
}
int RegionArrivalQueue::pop() {
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
bool RegionArrivalQueue::is_empty() {
    return _is_empty;
}
void RegionArrivalQueue::clear() {
    _head = 0;
    _tail = 0;
    _is_empty = true;
}

// void RegionArrivalQueue::lock() {
//     pthread_mutex_lock(&_mutex);
// }
// void RegionArrivalQueue::unlock() {
//     pthread_mutex_unlock(&_mutex);
// }
// int RegionArrivalQueue::try_lock() {
//     return pthread_mutex_trylock(&_mutex);
// }
size_t CopyRegionSnicClient::compute_mr_idx(int hostId, size_t idx) {
  return hostId * 50 + idx;
}

void CopyRegionSnicClient::read_back_cur_host_virtual_nodes(int hostId) {
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

void CopyRegionSnicClient::unmapMemSpace(void* addr, unsigned long long length, int hostId) {
  int res = munmap(addr, length);
  if (res == -1) {
    log_debug(gc)("DGC LOG: host %d munmap failed:addr:%llx,length:%llu,errno:%s", hostId, (unsigned long long) addr, length, strerror(errno));
    exit(0);
  }
}

void* CopyRegionSnicClient::mapMemSpace(int fd, unsigned long long addr, unsigned long long length, int hostId) {
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

void CopyRegionSnicClient::unmapCurHostMemSpaces(int hostId) {
  for (auto node : virtualSpaceNodes) {
    // CDS is owned by the JVM; leave it alone.
    if (node->base == 0x800000000) continue;
    // Drop only the alias mapping at node->base. The client-picked
    // real_local_base mapping (where the MR is pinned) stays live; the
    // fd keeps the SHM inode alive across remap cycles.
    unmapMemSpace((void*)node->base, node->sz, hostId);
  }
}

void CopyRegionSnicClient::remapCurHostMemSpaces(int hostId) {
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

int CopyRegionSnicClient::decide_force_terminate_counts() {
  int SATBFlashInterval = received_region_num / SnicSATBRootsSplitPartNum;
  int checkForceFinishInterval = SATBFlashInterval + SnicSATBRootsForceDeltaNum;
  return received_region_num / checkForceFinishInterval - done_trans_region_number / checkForceFinishInterval + 1;
}


void CopyRegionSnicClient::initialize_address_translation() {
  if (!SnicGCRegionAddressTranslation) {
    log_error(gc)("LHT LOG: address translation is not enabled");
    exit(0);
  }

  local_memory_pool_region_limit = local_memory_pool_size >> regionSizeShift;
  log_info(gc)("LHT LOG: local memory pool region limit: %d", local_memory_pool_region_limit);
  local_addr_translation_table = new LocalAddressTranslationEntry[local_memory_pool_region_limit];
  for (int i = 0; i < local_memory_pool_region_limit; i++) {
    local_addr_translation_table[i].remote_region_id = USHRT_MAX;
  }

  // Memory pool should already be allocated in runRDMAClient
  // Just verify it exists
  if (local_memory_pool_base == nullptr) {
    log_error(gc)("LHT LOG: Memory pool not allocated in initialize_address_translation!");
    exit(0);
  }


  log_info(gc)("LHT LOG: Initialized address translation table, pool base: %p, size: %zu",
               local_memory_pool_base, local_memory_pool_size);
}

// int CopyRegionSnicClient::select_region_to_evict() {
//   log_error(gc)("LHT LOG: select_region_to_evict not implemented");
//   exit(0);
//   return -1;
// }

int CopyRegionSnicClient::select_local_cache_index_simple() {
  int ret = local_memory_region_alloacted;
  local_memory_region_alloacted += 1;
  return ret;
}

int CopyRegionSnicClient::select_local_cache_index_range_simple(int region_count) {
  int ret = local_memory_region_alloacted;
  local_memory_region_alloacted += region_count;
  return ret;
}


void CopyRegionSnicClient::reset_address_translation_simple() {
  local_memory_region_alloacted = 0;
}


// int CopyRegionSnicClient::select_local_cache_index() {
//   return select_local_cache_index_range(1);
// }



// this range need to satisfy:
// 1. the regions are not used or copied (don't interrupt the ongoing copy)
// 2. choose the range with smallest sum of access frequency
int CopyRegionSnicClient::select_local_cache_range(int region_count, int candidate_hotness) {
  // candidate_hotness = max(1, candidate_hotness);
  if(local_mem_pool_free_region_count == 0 && candidate_hotness == 0){
    return -1;
  }

  candidate_hotness = std::max((int)0, candidate_hotness);
  int min_sum_access_frequency = candidate_hotness;
  if (candidate_hotness != 0 && candidate_hotness * SnicGCEvictionHotnessControl / 100 < 1) {
    min_sum_access_frequency = 1;
  } else {
    min_sum_access_frequency = candidate_hotness * SnicGCEvictionHotnessControl / 100;
  }
  // int min_sum_access_frequency = candidate_hotness * SnicGCEvictionHotnessControl / 100;

  // unsigned int min_sum_access_frequency = 1;
  // unsigned int min_sum_access_frequency = UINT_MAX;

  int debug_counter0 = 0;
  int debug_counter1 = 0;
  int debug_counter2 = 0;
  int debug_counter3 = 0;


  int min_sum_access_frequency_index = -1;
  for(int i = 0; i + region_count - 1 < local_memory_pool_region_limit; ++i){
    int current_range_hotness = 0;
    int current_range_not_used = 1;
    // here we unfold the range scan since most huge region only requires a few regions.
    for(int j = 0; j < region_count; j++){
      unsigned short remote_region_id = local_addr_translation_table[i + j].remote_region_id;
      if(remote_region_id == USHRT_MAX){
        continue;
      }
      else{
        current_range_not_used = 0;
      }
      if(region_is_uncopied(remote_region_id)){
        log_error(gc)("LHT LOG: ASSERT! remote_region_idx=%d, region_copied_bitmap=%d, local_id=%d", remote_region_id, region_copied_bitmap[remote_region_id], i + j);
        1/0;
        exit(0);
      }
      if(!region_is_local(remote_region_id)){
        current_range_hotness = -1;
        debug_counter0 ++;
        break;
      }
      // skip locked region
      if(check_region_bitmap_read_lock(remote_region_id, REGION_COPIED)){
        current_range_hotness = -1;
        debug_counter1 ++;
        break;
      }

      // in real word app, huge region is small and rare, simply ignore them when evict
      if(region_info[remote_region_id].state == _humongous_start || region_info[remote_region_id].state == _humongous_cont){
        current_range_hotness = -1;
        debug_counter2 ++;
        break;
      }
      if(ongoing_mark_count_table[remote_region_id] < 0){
        log_error(gc)("LHT LOG: ASSERT! region_idx=%d, ongoing_mark_count_table=%d", remote_region_id, ongoing_mark_count_table[remote_region_id]);
        exit(0);
      }
      current_range_hotness += ongoing_mark_count_table[remote_region_id];
    }
    if(current_range_hotness == -1){
      // debug_counter3 ++;
      continue;
    }
    if(current_range_hotness < min_sum_access_frequency){
      min_sum_access_frequency = current_range_hotness;
      min_sum_access_frequency_index = i;
      // if(current_range_hotness == 0){
      //   break;
      // }
    }
    if(current_range_not_used){
      min_sum_access_frequency_index = i;
      break;
    }
  }
  if(min_sum_access_frequency_index == -1){
    debug_trigger += 1;
    if(debug_trigger % 1000000 == 0){
      log_info(gc)("LHT LOG: no available region range with size %d and candidate hotness %d, abort", region_count, candidate_hotness);
      log_info(gc)("LHT LOG: debug_counter0=%d, debug_counter1=%d, debug_counter2=%d, debug_counter3=%d", debug_counter0, debug_counter1, debug_counter2, debug_counter3);
    }
    // log_info(gc)("LHT LOG: no available region range with size %d and candidate hotness %d, abort", region_count, candidate_hotness);
    // log_info(gc)("LHT LOG: debug_counter0=%d, debug_counter1=%d, debug_counter2=%d, debug_counter3=%d", debug_counter0, debug_counter1, debug_counter2, debug_counter3);
    return -1;
  }

  // remove Eviction logic here, now do this in copy_one_region_async_address_translation
  // for(int i = 0; i < region_count; i++){
  //   region_copied_bitmap[min_sum_access_frequency_index + i] = REGION_ON_EVICTION;
  // }
  for(int i = min_sum_access_frequency_index; i < min_sum_access_frequency_index + region_count; i++){
    unsigned short remote_region_id = local_addr_translation_table[i].remote_region_id;
    if(remote_region_id != USHRT_MAX && (region_info[remote_region_id].state == _humongous_start || region_info[remote_region_id].state == _humongous_cont)){
      log_error(gc)("LHT LOG: ASSERT! humongous region %d is evicted, local id=%d", remote_region_id, i);
      os::abort();
    }
  }
  return min_sum_access_frequency_index;
}

int CopyRegionSnicClient::select_local_cache_normal(int candidate_hotness) {
  return select_local_cache_range(1, candidate_hotness);
}

int CopyRegionSnicClient::select_local_cache_huge(int region_count) {
  return select_local_cache_range(region_count, INT_MAX);
}
