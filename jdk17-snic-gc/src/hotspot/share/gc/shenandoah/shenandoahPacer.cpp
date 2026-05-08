/*
 * Copyright (c) 2018, 2019, Red Hat, Inc. All rights reserved.
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

#include "gc/shenandoah/shenandoahFreeSet.hpp"
#include "gc/shenandoah/shenandoahHeap.inline.hpp"
#include "gc/shenandoah/shenandoahPacer.hpp"
#include "gc/shenandoah/shenandoahPhaseTimings.hpp"
#include "runtime/atomic.hpp"
#include "runtime/mutexLocker.hpp"
#include "runtime/threadSMR.hpp"
#include "gc/snicgc/shareMemSnicClient.hpp"

/*
 * In normal concurrent cycle, we have to pace the application to let GC finish.
 *
 * Here, we do not know how large would be the collection set, and what are the
 * relative performances of the each stage in the concurrent cycle, and so we have to
 * make some assumptions.
 *
 * For concurrent mark, there is no clear notion of progress. The moderately accurate
 * and easy to get metric is the amount of live objects the mark had encountered. But,
 * that does directly correlate with the used heap, because the heap might be fully
 * dead or fully alive. We cannot assume either of the extremes: we would either allow
 * application to run out of memory if we assume heap is fully dead but it is not, and,
 * conversely, we would pacify application excessively if we assume heap is fully alive
 * but it is not. So we need to guesstimate the particular expected value for heap liveness.
 * The best way to do this is apparently recording the past history.
 *
 * For concurrent evac and update-refs, we are walking the heap per-region, and so the
 * notion of progress is clear: we get reported the "used" size from the processed regions
 * and use the global heap-used as the baseline.
 *
 * The allocatable space when GC is running is "free" at the start of phase, but the
 * accounted budget is based on "used". So, we need to adjust the tax knowing that.
 */

void ShenandoahPacer::setup_for_mark() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  size_t live = update_and_get_progress_history();
  size_t free = _heap->free_set()->available();

  size_t non_taxable = free * ShenandoahPacingCycleSlack / 100;
  size_t taxable = free - non_taxable;

  double tax = 1.0 * live / taxable; // base tax for available free space
  tax *= 1;                          // mark can succeed with immediate garbage, claim all available space
  // Skip surcharge only when actually doing DGC (client-side marking). In
  // coor-RDMA fallback mode the host does local marking like baseline, so
  // the surcharge should apply to throttle mutator properly.
  if (!(SnicGCHost && !SnicGCShareMemEnabled && Universe::during_ccmark() && !Universe::during_SnicGCFallback())) {
    tax *= ShenandoahPacingSurcharge;  // additional surcharge to help unclutter heap
  }

  restart_with(non_taxable, tax);

  log_info(gc, ergo)("Pacer for Mark. Expected Live: " SIZE_FORMAT "%s, Free: " SIZE_FORMAT "%s, "
                     "Non-Taxable: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx",
                     byte_size_in_proper_unit(live),        proper_unit_for_byte_size(live),
                     byte_size_in_proper_unit(free),        proper_unit_for_byte_size(free),
                     byte_size_in_proper_unit(non_taxable), proper_unit_for_byte_size(non_taxable),
                     tax);


  // if(SnicGCGlobalPacer && SnicGCShareMemEnabled) {
  //   int hostId = Universe::get_CoorHostId();
  //   Atomic::store(&(Universe::get_hostShareGlobalPacerData()->historyLiveness[hostId]), (unsigned long long)(live));
  //   /*start @lyh*/
  //   // init shared mark budget for the current host.
  //   size_t initial = (size_t)(non_taxable * tax) >> LogHeapWordSize;
  //   Atomic::store(&(Universe::get_hostShareGlobalPacerData()->budgetsToIncreaseDuringMark[hostId]), (long) (initial));
  //   /*end @lyh*/
  // }
}

void ShenandoahPacer::setup_for_evac() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  size_t used = _heap->collection_set()->used();
  size_t free = _heap->free_set()->available();

  size_t non_taxable = free * ShenandoahPacingCycleSlack / 100;
  size_t taxable = free - non_taxable;

  double tax = 1.0 * used / taxable; // base tax for available free space
  tax *= 2;                          // evac is followed by update-refs, claim 1/2 of remaining free
  tax = MAX2<double>(1, tax);        // never allocate more than GC processes during the phase
  tax *= ShenandoahPacingSurcharge;  // additional surcharge to help unclutter heap

  restart_with(non_taxable, tax);

  log_info(gc, ergo)("Pacer for Evacuation. Used CSet: " SIZE_FORMAT "%s, Free: " SIZE_FORMAT "%s, "
                     "Non-Taxable: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx",
                     byte_size_in_proper_unit(used),        proper_unit_for_byte_size(used),
                     byte_size_in_proper_unit(free),        proper_unit_for_byte_size(free),
                     byte_size_in_proper_unit(non_taxable), proper_unit_for_byte_size(non_taxable),
                     tax);
}

void ShenandoahPacer::setup_for_updaterefs() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  size_t used = _heap->used();
  size_t free = _heap->free_set()->available();

  size_t non_taxable = free * ShenandoahPacingCycleSlack / 100;
  size_t taxable = free - non_taxable;

  double tax = 1.0 * used / taxable; // base tax for available free space
  tax *= 1;                          // update-refs is the last phase, claim the remaining free
  tax = MAX2<double>(1, tax);        // never allocate more than GC processes during the phase
  tax *= ShenandoahPacingSurcharge;  // additional surcharge to help unclutter heap

  restart_with(non_taxable, tax);

  log_info(gc, ergo)("Pacer for Update Refs. Used: " SIZE_FORMAT "%s, Free: " SIZE_FORMAT "%s, "
                     "Non-Taxable: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx",
                     byte_size_in_proper_unit(used),        proper_unit_for_byte_size(used),
                     byte_size_in_proper_unit(free),        proper_unit_for_byte_size(free),
                     byte_size_in_proper_unit(non_taxable), proper_unit_for_byte_size(non_taxable),
                     tax);
}

/*
 * In idle phase, we have to pace the application to let control thread react with GC start.
 *
 * Here, we have rendezvous with concurrent thread that adds up the budget as it acknowledges
 * it had seen recent allocations. It will naturally pace the allocations if control thread is
 * not catching up. To bootstrap this feedback cycle, we need to start with some initial budget
 * for applications to allocate at.
 */

void ShenandoahPacer::setup_for_idle() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  size_t initial = _heap->max_capacity() / 100 * ShenandoahPacingIdleSlack;
  double tax = 1;

  restart_with(initial, tax);

  log_info(gc, ergo)("Pacer for Idle. Initial: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx",
                     byte_size_in_proper_unit(initial), proper_unit_for_byte_size(initial),
                     tax);
}

size_t ShenandoahPacer::compute_predicted_alloc_rate() {
  auto cur_available = _heap->free_set()->available();
  size_t free = 0;
  auto free_headroom_history = Universe::get_free_headroom_history();
  if (free_headroom_history->size() > 0) {
    int headroom_history_size = free_headroom_history->size();
    for (int k = 0; k < headroom_history_size; k++) {
      free += free_headroom_history->at(k);
    }
    free /= free_headroom_history->size();
  }
  if (free < cur_available) {
    // convert to words/ms
    free = (cur_available - free) / 8;
  } else {
    free = 0;
  }
  auto cur_time_ms = os::javaTimeMillis();
  auto planned_next_time = Atomic::load(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].nextGCTime));
  double predicted_alloc_rate = 0;
  if (planned_next_time != 9999999999999) {
    auto time_diff = planned_next_time - (unsigned long long)(cur_time_ms);
    if (time_diff > 0) {
      predicted_alloc_rate = (double)free / time_diff;
    }
  }
  Atomic::store(&_predicted_alloc_rate, predicted_alloc_rate);
  return free;
}

void ShenandoahPacer::setup_for_global_pacer() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  log_debug(gc, ergo)("DGC LOG: setup_for_global_pacer");

  size_t free = _heap->free_set()->available();
  // size_t free = compute_predicted_alloc_rate();

  size_t non_taxable = free * SnicGCGlobalPacerInitTaxFree / 100;

  size_t taxable = free - non_taxable;

  unsigned long long mark_ahead = Atomic::load(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].pacerWorkAhead));
  double tax = 1.0 * mark_ahead / taxable * SnicGCGlobalPacerSurcharge;
  // double tax = SnicGCGlobalPacerSurcharge;

  restart_with(non_taxable, tax);

  log_info(gc, ergo)("Pacer for global pacer. Initial: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx, Predicted Alloc Rate: %.1f bytes/ms",
                     byte_size_in_proper_unit(non_taxable), proper_unit_for_byte_size(non_taxable),
                     tax, _predicted_alloc_rate);
}

void ShenandoahPacer::setup_for_global_pacer_Client_Occupied() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  log_debug(gc, ergo)("DGC LOG: setup_for_global_pacer_Client_Occupied");

  size_t free = _heap->free_set()->available();

  size_t non_taxable = free * 0.1;

  size_t taxable = free - non_taxable;

  double tax = 1.5;

  restart_with(non_taxable, tax);

  log_info(gc, ergo)("Pacer for global pacer (Client Occupied). Initial: " SIZE_FORMAT "%s, Alloc Tax Rate: %.1fx",
                     byte_size_in_proper_unit(non_taxable), proper_unit_for_byte_size(non_taxable),
                     tax);
}

/*
 * There is no useful notion of progress for these operations. To avoid stalling
 * the allocators unnecessarily, allow them to run unimpeded.
 */

void ShenandoahPacer::setup_for_reset() {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  size_t initial = _heap->max_capacity();
  restart_with(initial, 1.0);

  log_info(gc, ergo)("Pacer for Reset. Non-Taxable: " SIZE_FORMAT "%s",
                     byte_size_in_proper_unit(initial), proper_unit_for_byte_size(initial));
}

size_t ShenandoahPacer::update_and_get_progress_history() {
  if (_progress == -1) {
    // First initialization, report some prior
    Atomic::store(&_progress, (intptr_t)PACING_PROGRESS_ZERO);
    return (size_t) (_heap->max_capacity() * 0.1);
  } else {
    // Record history, and reply historical data
    _progress_history->add(_progress);
    Atomic::store(&_progress, (intptr_t)PACING_PROGRESS_ZERO);
    return (size_t) (_progress_history->avg() * HeapWordSize);
  }
}

void ShenandoahPacer::restart_with(size_t non_taxable_bytes, double tax_rate) {
  size_t initial = (size_t)(non_taxable_bytes * tax_rate) >> LogHeapWordSize;
  STATIC_ASSERT(sizeof(size_t) <= sizeof(intptr_t));
  Atomic::xchg(&_budget, (intptr_t)initial, memory_order_relaxed);
  Atomic::store(&_tax_rate, tax_rate);
  Atomic::inc(&_epoch);

  // Shake up stalled waiters after budget update.
  _need_notify_waiters.try_set();
}

bool ShenandoahPacer::claim_for_alloc(size_t words, bool force) {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  intptr_t tax = MAX2<intptr_t>(1, words * Atomic::load(&_tax_rate));

  intptr_t cur = 0;
  intptr_t new_val = 0;
  /*start @lyh*/
  // if (!(SnicGCHost && SnicGCShareMemEnabled && SnicGCGlobalPacer && Universe::during_ccmark() && !Universe::during_SnicGCFallback())) {
    do {
      cur = Atomic::load(&_budget);
      if (cur < tax && !force) {
        // Progress depleted, alas.
        return false;
      }
      new_val = cur - tax;
    } while (Atomic::cmpxchg(&_budget, cur, new_val, memory_order_relaxed) != cur);
  // } else {
  //   int hostId = Universe::get_CoorHostId();
  //   do {
  //     cur = Atomic::load(&Universe::get_hostShareGlobalPacerData()->budgetsToIncreaseDuringMark[hostId]);
  //     if (cur < tax && !force) {
  //       // Progress depleted, alas.
  //       return false;
  //     }
  //     new_val = cur - tax;
  //   } while (Atomic::cmpxchg(&Universe::get_hostShareGlobalPacerData()->budgetsToIncreaseDuringMark[hostId], cur, new_val, memory_order_relaxed) != cur);
  // }
  /*end @lyh*/
  return true;
}

void ShenandoahPacer::unpace_for_alloc(intptr_t epoch, size_t words) {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  if (Atomic::load(&_epoch) != epoch) {
    // Stale ticket, no need to unpace.
    return;
  }

  size_t tax = MAX2<size_t>(1, words * Atomic::load(&_tax_rate));
  add_budget(tax);
}

intptr_t ShenandoahPacer::epoch() {
  return Atomic::load(&_epoch);
}

void ShenandoahPacer::pace_for_alloc(size_t words) {
  assert(ShenandoahPacing, "Only be here when pacing is enabled");

  /*start @lyh*/
  // The rate-based predictive budget consumption is for DGC (client-side)
  // marking where the host itself doesn't report actual allocations as
  // progress. In fallback mode the host does local marking and the normal
  // claim_for_alloc path tracks progress correctly, so skip this path.
  if (SnicGCHost && !SnicGCShareMemEnabled && Universe::during_ccmark() && !Universe::during_SnicGCFallback()) {
    double cur_time = os::elapsedTime();
    if (Universe::get_dpuCurTimeDuringMarkInSec() == 0) {
      Universe::set_dpuCurTimeDuringMarkInSec(cur_time);
    } else {
      double time_diff = cur_time - Universe::get_dpuCurTimeDuringMarkInSec();
      Universe::set_dpuCurTimeDuringMarkInSec(cur_time);
      double avg_alloc_rate = Universe::get_dpuAverageAllocRate();
      size_t predicted_word_size = (size_t)(avg_alloc_rate * time_diff);
      report_alloc(predicted_word_size);
    }
  }
  /*end @lyh*/

  // Fast path: try to allocate right away
  bool claimed = claim_for_alloc(words, false);
  if (claimed) {
    return;
  }

  // Forcefully claim the budget: it may go negative at this point, and
  // GC should replenish for this and subsequent allocations. After this claim,
  // we would wait a bit until our claim is matched by additional progress,
  // or the time budget depletes.
  claimed = claim_for_alloc(words, true);
  assert(claimed, "Should always succeed");

  // Threads that are attaching should not block at all: they are not
  // fully initialized yet. Blocking them would be awkward.
  // This is probably the path that allocates the thread oop itself.
  if (JavaThread::current()->is_attaching_via_jni()) {
    return;
  }

  double start = os::elapsedTime();

  size_t max_ms = ShenandoahPacingMaxDelay;
  size_t total_ms = 0;

  while (true) {
    // We could instead assist GC, but this would suffice for now.
    size_t cur_ms = (max_ms > total_ms) ? (max_ms - total_ms) : 1;
    wait(cur_ms);

    double end = os::elapsedTime();
    total_ms = (size_t)((end - start) * 1000);

    /*start @lyh*/
    bool should_break = false;
    // if (!(SnicGCHost && SnicGCShareMemEnabled && SnicGCGlobalPacer && Universe::during_ccmark() && !Universe::during_SnicGCFallback())) {
      should_break = total_ms > max_ms || Atomic::load(&_budget) >= 0;
    // } else {
    //   int hostId = Universe::get_CoorHostId();
    //   should_break = total_ms > max_ms || Atomic::load(&Universe::get_hostShareGlobalPacerData()->budgetsToIncreaseDuringMark[hostId]) >= 0;
    // }
    if (should_break) {
    /*end @lyh*/
      // Exiting if either:
      //  a) Spent local time budget to wait for enough GC progress.
      //     Breaking out and allocating anyway, which may mean we outpace GC,
      //     and start Degenerated GC cycle.
      //  b) The budget had been replenished, which means our claim is satisfied.
      double t = end - start;
      ShenandoahThreadLocalData::add_paced_time(JavaThread::current(), t);
      if (ShenandoahHeap::heap()->is_concurrent_mark_in_progress()) {
        ShenandoahThreadLocalData::add_paced_mark_time(JavaThread::current(), t);
      } else if (ShenandoahHeap::heap()->is_evacuation_in_progress()) {
        ShenandoahThreadLocalData::add_paced_evac_time(JavaThread::current(), t);
      } else if (ShenandoahHeap::heap()->is_update_refs_in_progress()) {
        ShenandoahThreadLocalData::add_paced_update_time(JavaThread::current(), t);
      }else if (ShenandoahHeap::heap()->is_idle()) {
        ShenandoahThreadLocalData::add_paced_idle_time(JavaThread::current(), t);
      }
      
      break;
    }
  }
}

void ShenandoahPacer::wait(size_t time_ms) {
  // Perform timed wait. It works like like sleep(), except without modifying
  // the thread interruptible status. MonitorLocker also checks for safepoints.
  assert(time_ms > 0, "Should not call this with zero argument, as it would stall until notify");
  assert(time_ms <= LONG_MAX, "Sanity");
  MonitorLocker locker(_wait_monitor);
  _wait_monitor->wait((long)time_ms);
}

void ShenandoahPacer::notify_waiters() {
  if (_need_notify_waiters.try_unset()) {
    MonitorLocker locker(_wait_monitor);
    _wait_monitor->notify_all();
  }
}

void ShenandoahPacer::flush_stats_to_cycle() {
  double sum = 0;
  for (JavaThreadIteratorWithHandle jtiwh; JavaThread *t = jtiwh.next(); ) {
    sum += ShenandoahThreadLocalData::paced_time(t);
  }
  ShenandoahHeap::heap()->phase_timings()->record_phase_time(ShenandoahPhaseTimings::pacing, sum);
}

void ShenandoahPacer::print_cycle_on(outputStream* out) {
  MutexLocker lock(Threads_lock);

  double now = os::elapsedTime();
  double total = now - _last_time;
  _last_time = now;

  out->cr();
  out->print_cr("Allocation pacing accrued:");

  size_t threads_total = 0;
  size_t threads_nz = 0;
  double sum = 0;
  double sum_mark = 0;
  double sum_evac = 0;
  double sum_update = 0;
  double sum_idle = 0;
  for (JavaThreadIteratorWithHandle jtiwh; JavaThread *t = jtiwh.next(); ) {
    double d = ShenandoahThreadLocalData::paced_time(t);
    if (d > 0) {
      threads_nz++;
      sum += d;
      sum_mark += ShenandoahThreadLocalData::paced_mark_time(t);
      sum_evac += ShenandoahThreadLocalData::paced_evac_time(t);
      sum_update += ShenandoahThreadLocalData::paced_update_time(t);
      sum_idle += ShenandoahThreadLocalData::paced_idle_time(t);
      out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): %s",
              d * 1000, total * 1000, d/total*100, t->name());
    }
    threads_total++;
    ShenandoahThreadLocalData::reset_paced_time(t);
    ShenandoahThreadLocalData::reset_paced_mark_time(t);
    ShenandoahThreadLocalData::reset_paced_evac_time(t);
    ShenandoahThreadLocalData::reset_paced_update_time(t);
    ShenandoahThreadLocalData::reset_paced_idle_time(t);
  }
  out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <total>",
          sum * 1000, total * 1000, sum/total*100);
  out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <idle>",
          sum_idle * 1000, total * 1000, sum_idle/total*100);
  out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <mark>",
          sum_mark * 1000, total * 1000, sum_mark/total*100);
  out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <evac>",
          sum_evac * 1000, total * 1000, sum_evac/total*100);
  out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <update>",
          sum_update * 1000, total * 1000, sum_update/total*100);


  if (threads_total > 0) {
    out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <average total>",
            sum / threads_total * 1000, total * 1000, sum / threads_total / total * 100);
  }
  if (threads_nz > 0) {
    out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <average non-zero>",
            sum / threads_nz * 1000, total * 1000, sum / threads_nz / total * 100);
    out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <average mark>",
            sum_mark / threads_nz * 1000, total * 1000, sum_mark / threads_nz / total * 100);
    out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <average evac>",
            sum_evac / threads_nz * 1000, total * 1000, sum_evac / threads_nz / total * 100);
    out->print_cr("  %5.0f of %5.0f ms (%5.1f%%): <average update>",
            sum_update / threads_nz * 1000, total * 1000, sum_update / threads_nz / total * 100);
  }

  out->cr();
}
