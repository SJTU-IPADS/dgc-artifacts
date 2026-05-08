/*
 * Copyright (c) 1997, 2021, Oracle and/or its affiliates. All rights reserved.
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
#include "cds/heapShared.hpp"
#include "cds/metaspaceShared.hpp"
#include "classfile/classLoader.hpp"
#include "classfile/classLoaderDataGraph.hpp"
#include "classfile/javaClasses.hpp"
#include "classfile/stringTable.hpp"
#include "classfile/symbolTable.hpp"
#include "classfile/systemDictionary.hpp"
#include "classfile/vmClasses.hpp"
#include "classfile/vmSymbols.hpp"
#include "code/codeBehaviours.hpp"
#include "code/codeCache.hpp"
#include "compiler/oopMap.hpp"
#include "gc/shared/collectedHeap.inline.hpp"
#include "gc/shared/gcArguments.hpp"
#include "gc/shared/gcConfig.hpp"
#include "gc/shared/gcLogPrecious.hpp"
#include "gc/shared/gcTraceTime.inline.hpp"
#include "gc/shared/oopStorageSet.hpp"
#include "gc/shared/stringdedup/stringDedup.hpp"
#include "gc/shared/tlab_globals.hpp"
#include "logging/log.hpp"
#include "logging/logStream.hpp"
#include "memory/metadataFactory.hpp"
#include "memory/metaspaceClosure.hpp"
#include "memory/metaspaceCounters.hpp"
#include "memory/metaspaceUtils.hpp"
#include "memory/oopFactory.hpp"
#include "memory/resourceArea.hpp"
#include "memory/universe.hpp"
#include "oops/compressedOops.hpp"
#include "oops/instanceKlass.hpp"
#include "oops/instanceMirrorKlass.hpp"
#include "oops/klass.inline.hpp"
#include "oops/objArrayOop.inline.hpp"
#include "oops/oop.inline.hpp"
#include "oops/oopHandle.inline.hpp"
#include "oops/typeArrayKlass.hpp"
#include "prims/resolvedMethodTable.hpp"
#include "runtime/arguments.hpp"
#include "runtime/atomic.hpp"
#include "runtime/flags/jvmFlagLimit.hpp"
#include "runtime/handles.inline.hpp"
#include "runtime/init.hpp"
#include "runtime/java.hpp"
#include "runtime/jniHandles.hpp"
#include "runtime/thread.inline.hpp"
#include "runtime/timerTrace.hpp"
#include "services/memoryService.hpp"
#include "utilities/align.hpp"
#include "utilities/autoRestore.hpp"
#include "utilities/debug.hpp"
#include "utilities/formatBuffer.hpp"
#include "utilities/macros.hpp"
#include "utilities/ostream.hpp"
#include "utilities/preserveException.hpp"
#include "rdma/lib.hh"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <iomanip>
#include <vector>
#include "../gc/snicgc/copyRegionSnicClient.hpp"
#include "../gc/snicgc/shareMemSnicClient.hpp"

// LHT modified
rdmaio::RCtrl* Universe::ctrl = NULL;
rdmaio::RCtrl* Universe::ctrl_for_coor = NULL;
int Universe::rpc_desc = 0;
int Universe::SnicHostId = 0;
bool Universe::_during_ccmark = false;
bool Universe::_during_global_pacer = false;
bool Universe::_during_global_pacer_Client_Occupied = false;
bool Universe::_SnicGCFallback = false;
bool Universe::_last_cycle_was_dgc = false;
bool Universe::_during_final_mark = false;
bool Universe::_during_mark_roots = false;
int Universe::hostShareHeapFD = -1;
int Universe::hostShareRootFD = -1;
int Universe::hostShareGlobalPacerFD = -1;
int Universe::hostShareControlFD = -1;
GlobalPacerData* Universe::hostShareGlobalPacerData = nullptr;
Universe::ShmDGCControl* Universe::shmDGCControl = nullptr;
unsigned long long* Universe::preallocRootsAddr = nullptr;
void* Universe::shmArenaBase = nullptr;
unsigned long long Universe::shmArenaNext = 0;
unsigned long long Universe::shmArenaEnd = 0;
GlobalPacerDataPerHost* Universe::hostPrivateGlobalPacerData = nullptr;
int Universe::virtualSpaceNodeCount = 0;
bool Universe::rpcServerShouldStop = false;
std::vector<size_t> Universe::free_headroom_history;
unsigned long long Universe::nonmarking_time_start = 0;
// unsigned long long Universe::nonmarking_time_end = 0;
TruncatedSeq* Universe::nonmarking_time_history = nullptr;
TruncatedSeq* Universe::liveness_history = nullptr;
TruncatedSeq* Universe::gc_interval_history = nullptr;
long Universe::dpuCurTimeDuringMarkInMs = 0;
double Universe::dpuAverageAllocRate = 0.0;
double Universe::dpuCurTimeDuringMarkInSec = 0.0;
unsigned long long Universe::_jvm_start_time = 0;
bool Universe::_start_rdma_prefetch = false;
bool Universe::_during_rdma_prefetch = false;
unsigned long long Universe::_estimated_rdma_copy_time = 0; // in ms.

// Known objects
Klass* Universe::_typeArrayKlassObjs[T_LONG+1]        = { NULL /*, NULL...*/ };
Klass* Universe::_objectArrayKlassObj                 = NULL;
OopHandle Universe::_mirrors[T_VOID+1];

OopHandle Universe::_main_thread_group;
OopHandle Universe::_system_thread_group;
OopHandle Universe::_the_empty_class_array;
OopHandle Universe::_the_null_string;
OopHandle Universe::_the_min_jint_string;

OopHandle Universe::_the_null_sentinel;

// _out_of_memory_errors is an objArray
enum OutOfMemoryInstance { _oom_java_heap,
                           _oom_c_heap,
                           _oom_metaspace,
                           _oom_class_metaspace,
                           _oom_array_size,
                           _oom_gc_overhead_limit,
                           _oom_realloc_objects,
                           _oom_retry,
                           _oom_count };

OopHandle Universe::_out_of_memory_errors;
OopHandle Universe::_delayed_stack_overflow_error_message;
OopHandle Universe::_preallocated_out_of_memory_error_array;
volatile jint Universe::_preallocated_out_of_memory_error_avail_count = 0;

OopHandle Universe::_null_ptr_exception_instance;
OopHandle Universe::_arithmetic_exception_instance;
OopHandle Universe::_virtual_machine_error_instance;

OopHandle Universe::_reference_pending_list;

Array<Klass*>* Universe::_the_array_interfaces_array = NULL;
LatestMethodCache* Universe::_finalizer_register_cache = NULL;
LatestMethodCache* Universe::_loader_addClass_cache    = NULL;
LatestMethodCache* Universe::_throw_illegal_access_error_cache = NULL;
LatestMethodCache* Universe::_throw_no_such_method_error_cache = NULL;
LatestMethodCache* Universe::_do_stack_walk_cache     = NULL;

bool Universe::_verify_in_progress                    = false;
long Universe::verify_flags                           = Universe::Verify_All;

Array<int>* Universe::_the_empty_int_array            = NULL;
Array<u2>* Universe::_the_empty_short_array           = NULL;
Array<Klass*>* Universe::_the_empty_klass_array     = NULL;
Array<InstanceKlass*>* Universe::_the_empty_instance_klass_array  = NULL;
Array<Method*>* Universe::_the_empty_method_array   = NULL;

// These variables are guarded by FullGCALot_lock.
debug_only(OopHandle Universe::_fullgc_alot_dummy_array;)
debug_only(int Universe::_fullgc_alot_dummy_next = 0;)

// Heap
int             Universe::_verify_count = 0;

// Oop verification (see MacroAssembler::verify_oop)
uintptr_t       Universe::_verify_oop_mask = 0;
uintptr_t       Universe::_verify_oop_bits = (uintptr_t) -1;

int             Universe::_base_vtable_size = 0;
bool            Universe::_bootstrapping = false;
bool            Universe::_module_initialized = false;
bool            Universe::_fully_initialized = false;

OopStorage*     Universe::_vm_weak = NULL;
OopStorage*     Universe::_vm_global = NULL;

CollectedHeap*  Universe::_collectedHeap = NULL;

int             Universe::_mr_index = 1000;
volatile unsigned long long Universe::_ccs_hwm = 0;

objArrayOop Universe::the_empty_class_array ()  {
  return (objArrayOop)_the_empty_class_array.resolve();
}

oop Universe::main_thread_group()                 { return _main_thread_group.resolve(); }
void Universe::set_main_thread_group(oop group)   { _main_thread_group = OopHandle(vm_global(), group); }

oop Universe::system_thread_group()               { return _system_thread_group.resolve(); }
void Universe::set_system_thread_group(oop group) { _system_thread_group = OopHandle(vm_global(), group); }

oop Universe::the_null_string()                   { return _the_null_string.resolve(); }
oop Universe::the_min_jint_string()               { return _the_min_jint_string.resolve(); }

oop Universe::null_ptr_exception_instance()       { return _null_ptr_exception_instance.resolve(); }
oop Universe::arithmetic_exception_instance()     { return _arithmetic_exception_instance.resolve(); }
oop Universe::virtual_machine_error_instance()    { return _virtual_machine_error_instance.resolve(); }

oop Universe::the_null_sentinel()                 { return _the_null_sentinel.resolve(); }

oop Universe::int_mirror()                        { return check_mirror(_mirrors[T_INT].resolve()); }
oop Universe::float_mirror()                      { return check_mirror(_mirrors[T_FLOAT].resolve()); }
oop Universe::double_mirror()                     { return check_mirror(_mirrors[T_DOUBLE].resolve()); }
oop Universe::byte_mirror()                       { return check_mirror(_mirrors[T_BYTE].resolve()); }
oop Universe::bool_mirror()                       { return check_mirror(_mirrors[T_BOOLEAN].resolve()); }
oop Universe::char_mirror()                       { return check_mirror(_mirrors[T_CHAR].resolve()); }
oop Universe::long_mirror()                       { return check_mirror(_mirrors[T_LONG].resolve()); }
oop Universe::short_mirror()                      { return check_mirror(_mirrors[T_SHORT].resolve()); }
oop Universe::void_mirror()                       { return check_mirror(_mirrors[T_VOID].resolve()); }

oop Universe::java_mirror(BasicType t) {
  assert((uint)t < T_VOID+1, "range check");
  return check_mirror(_mirrors[t].resolve());
}

// Used by CDS dumping
void Universe::replace_mirror(BasicType t, oop new_mirror) {
  Universe::_mirrors[t].replace(new_mirror);
}

void Universe::basic_type_classes_do(void f(Klass*)) {
  for (int i = T_BOOLEAN; i < T_LONG+1; i++) {
    f(_typeArrayKlassObjs[i]);
  }
}

void Universe::basic_type_classes_do(KlassClosure *closure) {
  for (int i = T_BOOLEAN; i < T_LONG+1; i++) {
    closure->do_klass(_typeArrayKlassObjs[i]);
  }
}

void LatestMethodCache::metaspace_pointers_do(MetaspaceClosure* it) {
  it->push(&_klass);
}

void Universe::metaspace_pointers_do(MetaspaceClosure* it) {
  for (int i = 0; i < T_LONG+1; i++) {
    it->push(&_typeArrayKlassObjs[i]);
  }
  it->push(&_objectArrayKlassObj);

  it->push(&_the_empty_int_array);
  it->push(&_the_empty_short_array);
  it->push(&_the_empty_klass_array);
  it->push(&_the_empty_instance_klass_array);
  it->push(&_the_empty_method_array);
  it->push(&_the_array_interfaces_array);

  _finalizer_register_cache->metaspace_pointers_do(it);
  _loader_addClass_cache->metaspace_pointers_do(it);
  _throw_illegal_access_error_cache->metaspace_pointers_do(it);
  _throw_no_such_method_error_cache->metaspace_pointers_do(it);
  _do_stack_walk_cache->metaspace_pointers_do(it);
}

// Serialize metadata and pointers to primitive type mirrors in and out of CDS archive
void Universe::serialize(SerializeClosure* f) {

#if INCLUDE_CDS_JAVA_HEAP
  {
    oop mirror_oop;
    for (int i = T_BOOLEAN; i < T_VOID+1; i++) {
      if (f->reading()) {
        f->do_oop(&mirror_oop); // read from archive
        assert(oopDesc::is_oop_or_null(mirror_oop), "is oop");
        // Only create an OopHandle for non-null mirrors
        if (mirror_oop != NULL) {
          _mirrors[i] = OopHandle(vm_global(), mirror_oop);
        }
      } else {
        if (HeapShared::is_heap_object_archiving_allowed()) {
          mirror_oop = _mirrors[i].resolve();
        } else {
          mirror_oop = NULL;
        }
        f->do_oop(&mirror_oop); // write to archive
      }
      if (mirror_oop != NULL) { // may be null if archived heap is disabled
        java_lang_Class::update_archived_primitive_mirror_native_pointers(mirror_oop);
      }
    }
  }
#endif

  for (int i = 0; i < T_LONG+1; i++) {
    f->do_ptr((void**)&_typeArrayKlassObjs[i]);
  }

  f->do_ptr((void**)&_objectArrayKlassObj);
  f->do_ptr((void**)&_the_array_interfaces_array);
  f->do_ptr((void**)&_the_empty_int_array);
  f->do_ptr((void**)&_the_empty_short_array);
  f->do_ptr((void**)&_the_empty_method_array);
  f->do_ptr((void**)&_the_empty_klass_array);
  f->do_ptr((void**)&_the_empty_instance_klass_array);
  _finalizer_register_cache->serialize(f);
  _loader_addClass_cache->serialize(f);
  _throw_illegal_access_error_cache->serialize(f);
  _throw_no_such_method_error_cache->serialize(f);
  _do_stack_walk_cache->serialize(f);
}


void Universe::check_alignment(uintx size, uintx alignment, const char* name) {
  if (size < alignment || size % alignment != 0) {
    vm_exit_during_initialization(
      err_msg("Size of %s (" UINTX_FORMAT " bytes) must be aligned to " UINTX_FORMAT " bytes", name, size, alignment));
  }
}

void initialize_basic_type_klass(Klass* k, TRAPS) {
  Klass* ok = vmClasses::Object_klass();
#if INCLUDE_CDS
  if (UseSharedSpaces) {
    ClassLoaderData* loader_data = ClassLoaderData::the_null_class_loader_data();
    assert(k->super() == ok, "u3");
    if (k->is_instance_klass()) {
      InstanceKlass::cast(k)->restore_unshareable_info(loader_data, Handle(), NULL, CHECK);
    } else {
      ArrayKlass::cast(k)->restore_unshareable_info(loader_data, Handle(), CHECK);
    }
  } else
#endif
  {
    k->initialize_supers(ok, NULL, CHECK);
  }
  k->append_to_sibling_list();
}

void Universe::genesis(TRAPS) {
  ResourceMark rm(THREAD);
  HandleMark   hm(THREAD);

  { AutoModifyRestore<bool> temporarily(_bootstrapping, true);

    { MutexLocker mc(THREAD, Compile_lock);

      java_lang_Class::allocate_fixup_lists();

      // determine base vtable size; without that we cannot create the array klasses
      compute_base_vtable_size();

      if (!UseSharedSpaces) {
        for (int i = T_BOOLEAN; i < T_LONG+1; i++) {
          _typeArrayKlassObjs[i] = TypeArrayKlass::create_klass((BasicType)i, CHECK);
        }

        ClassLoaderData* null_cld = ClassLoaderData::the_null_class_loader_data();

        _the_array_interfaces_array     = MetadataFactory::new_array<Klass*>(null_cld, 2, NULL, CHECK);
        _the_empty_int_array            = MetadataFactory::new_array<int>(null_cld, 0, CHECK);
        _the_empty_short_array          = MetadataFactory::new_array<u2>(null_cld, 0, CHECK);
        _the_empty_method_array         = MetadataFactory::new_array<Method*>(null_cld, 0, CHECK);
        _the_empty_klass_array          = MetadataFactory::new_array<Klass*>(null_cld, 0, CHECK);
        _the_empty_instance_klass_array = MetadataFactory::new_array<InstanceKlass*>(null_cld, 0, CHECK);
      }
    }

    vmSymbols::initialize();

    SystemDictionary::initialize(CHECK);

    // Create string constants
    oop s = StringTable::intern("null", CHECK);
    _the_null_string = OopHandle(vm_global(), s);
    s = StringTable::intern("-2147483648", CHECK);
    _the_min_jint_string = OopHandle(vm_global(), s);


#if INCLUDE_CDS
    if (UseSharedSpaces) {
      // Verify shared interfaces array.
      assert(_the_array_interfaces_array->at(0) ==
             vmClasses::Cloneable_klass(), "u3");
      assert(_the_array_interfaces_array->at(1) ==
             vmClasses::Serializable_klass(), "u3");
    } else
#endif
    {
      // Set up shared interfaces array.  (Do this before supers are set up.)
      _the_array_interfaces_array->at_put(0, vmClasses::Cloneable_klass());
      _the_array_interfaces_array->at_put(1, vmClasses::Serializable_klass());
    }

    initialize_basic_type_klass(boolArrayKlassObj(), CHECK);
    initialize_basic_type_klass(charArrayKlassObj(), CHECK);
    initialize_basic_type_klass(floatArrayKlassObj(), CHECK);
    initialize_basic_type_klass(doubleArrayKlassObj(), CHECK);
    initialize_basic_type_klass(byteArrayKlassObj(), CHECK);
    initialize_basic_type_klass(shortArrayKlassObj(), CHECK);
    initialize_basic_type_klass(intArrayKlassObj(), CHECK);
    initialize_basic_type_klass(longArrayKlassObj(), CHECK);
  } // end of core bootstrapping

  {
    Handle tns = java_lang_String::create_from_str("<null_sentinel>", CHECK);
    _the_null_sentinel = OopHandle(vm_global(), tns());
  }

  // Create a handle for reference_pending_list
  _reference_pending_list = OopHandle(vm_global(), NULL);

  // Maybe this could be lifted up now that object array can be initialized
  // during the bootstrapping.

  // OLD
  // Initialize _objectArrayKlass after core bootstraping to make
  // sure the super class is set up properly for _objectArrayKlass.
  // ---
  // NEW
  // Since some of the old system object arrays have been converted to
  // ordinary object arrays, _objectArrayKlass will be loaded when
  // SystemDictionary::initialize(CHECK); is run. See the extra check
  // for Object_klass_loaded in objArrayKlassKlass::allocate_objArray_klass_impl.
  _objectArrayKlassObj = InstanceKlass::
    cast(vmClasses::Object_klass())->array_klass(1, CHECK);
  // OLD
  // Add the class to the class hierarchy manually to make sure that
  // its vtable is initialized after core bootstrapping is completed.
  // ---
  // New
  // Have already been initialized.
  _objectArrayKlassObj->append_to_sibling_list();

  #ifdef ASSERT
  if (FullGCALot) {
    // Allocate an array of dummy objects.
    // We'd like these to be at the bottom of the old generation,
    // so that when we free one and then collect,
    // (almost) the whole heap moves
    // and we find out if we actually update all the oops correctly.
    // But we can't allocate directly in the old generation,
    // so we allocate wherever, and hope that the first collection
    // moves these objects to the bottom of the old generation.
    int size = FullGCALotDummies * 2;

    objArrayOop    naked_array = oopFactory::new_objArray(vmClasses::Object_klass(), size, CHECK);
    objArrayHandle dummy_array(THREAD, naked_array);
    int i = 0;
    while (i < size) {
        // Allocate dummy in old generation
      oop dummy = vmClasses::Object_klass()->allocate_instance(CHECK);
      dummy_array->obj_at_put(i++, dummy);
    }
    {
      // Only modify the global variable inside the mutex.
      // If we had a race to here, the other dummy_array instances
      // and their elements just get dropped on the floor, which is fine.
      MutexLocker ml(THREAD, FullGCALot_lock);
      if (_fullgc_alot_dummy_array.is_empty()) {
        _fullgc_alot_dummy_array = OopHandle(vm_global(), dummy_array());
      }
    }
    assert(i == ((objArrayOop)_fullgc_alot_dummy_array.resolve())->length(), "just checking");
  }
  #endif
}

void Universe::initialize_basic_type_mirrors(TRAPS) {
#if INCLUDE_CDS_JAVA_HEAP
    if (UseSharedSpaces &&
        HeapShared::open_archive_heap_region_mapped() &&
        _mirrors[T_INT].resolve() != NULL) {
      assert(HeapShared::is_heap_object_archiving_allowed(), "Sanity");

      // check that all mirrors are mapped also
      for (int i = T_BOOLEAN; i < T_VOID+1; i++) {
        if (!is_reference_type((BasicType)i)) {
          oop m = _mirrors[i].resolve();
          assert(m != NULL, "archived mirrors should not be NULL");
        }
      }
    } else
      // _mirror[T_INT} could be NULL if archived heap is not mapped.
#endif
    {
      for (int i = T_BOOLEAN; i < T_VOID+1; i++) {
        BasicType bt = (BasicType)i;
        if (!is_reference_type(bt)) {
          oop m = java_lang_Class::create_basic_type_mirror(type2name(bt), bt, CHECK);
          _mirrors[i] = OopHandle(vm_global(), m);
        }
      }
    }
}

void Universe::fixup_mirrors(TRAPS) {
  // Bootstrap problem: all classes gets a mirror (java.lang.Class instance) assigned eagerly,
  // but we cannot do that for classes created before java.lang.Class is loaded. Here we simply
  // walk over permanent objects created so far (mostly classes) and fixup their mirrors. Note
  // that the number of objects allocated at this point is very small.
  assert(vmClasses::Class_klass_loaded(), "java.lang.Class should be loaded");
  HandleMark hm(THREAD);

  if (!UseSharedSpaces) {
    // Cache the start of the static fields
    InstanceMirrorKlass::init_offset_of_static_fields();
  }

  GrowableArray <Klass*>* list = java_lang_Class::fixup_mirror_list();
  int list_length = list->length();
  for (int i = 0; i < list_length; i++) {
    Klass* k = list->at(i);
    assert(k->is_klass(), "List should only hold classes");
    java_lang_Class::fixup_mirror(k, CATCH);
  }
  delete java_lang_Class::fixup_mirror_list();
  java_lang_Class::set_fixup_mirror_list(NULL);
}

#define assert_pll_locked(test) \
  assert(Heap_lock->test(), "Reference pending list access requires lock")

#define assert_pll_ownership() assert_pll_locked(owned_by_self)

oop Universe::reference_pending_list() {
  if (Thread::current()->is_VM_thread()) {
    assert_pll_locked(is_locked);
  } else {
    assert_pll_ownership();
  }
  return _reference_pending_list.resolve();
}

void Universe::clear_reference_pending_list() {
  assert_pll_ownership();
  _reference_pending_list.replace(NULL);
}

bool Universe::has_reference_pending_list() {
  assert_pll_ownership();
  return _reference_pending_list.peek() != NULL;
}

oop Universe::swap_reference_pending_list(oop list) {
  assert_pll_locked(is_locked);
  return _reference_pending_list.xchg(list);
}

#undef assert_pll_locked
#undef assert_pll_ownership

static void reinitialize_vtables() {
  // The vtables are initialized by starting at java.lang.Object and
  // initializing through the subclass links, so that the super
  // classes are always initialized first.
  for (ClassHierarchyIterator iter(vmClasses::Object_klass()); !iter.done(); iter.next()) {
    Klass* sub = iter.klass();
    sub->vtable().initialize_vtable();
  }
}


static void initialize_itable_for_klass(InstanceKlass* k) {
  k->itable().initialize_itable();
}


static void reinitialize_itables() {
  MutexLocker mcld(ClassLoaderDataGraph_lock);
  ClassLoaderDataGraph::dictionary_classes_do(initialize_itable_for_klass);
}


bool Universe::on_page_boundary(void* addr) {
  return is_aligned(addr, os::vm_page_size());
}

// the array of preallocated errors with backtraces
objArrayOop Universe::preallocated_out_of_memory_errors() {
  return (objArrayOop)_preallocated_out_of_memory_error_array.resolve();
}

objArrayOop Universe::out_of_memory_errors() { return (objArrayOop)_out_of_memory_errors.resolve(); }

oop Universe::out_of_memory_error_java_heap() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_java_heap));
}

oop Universe::out_of_memory_error_c_heap() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_c_heap));
}

oop Universe::out_of_memory_error_metaspace() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_metaspace));
}

oop Universe::out_of_memory_error_class_metaspace() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_class_metaspace));
}

oop Universe::out_of_memory_error_array_size() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_array_size));
}

oop Universe::out_of_memory_error_gc_overhead_limit() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_gc_overhead_limit));
}

oop Universe::out_of_memory_error_realloc_objects() {
  return gen_out_of_memory_error(out_of_memory_errors()->obj_at(_oom_realloc_objects));
}

// Throw default _out_of_memory_error_retry object as it will never propagate out of the VM
oop Universe::out_of_memory_error_retry()              { return out_of_memory_errors()->obj_at(_oom_retry);  }
oop Universe::delayed_stack_overflow_error_message()   { return _delayed_stack_overflow_error_message.resolve(); }


bool Universe::should_fill_in_stack_trace(Handle throwable) {
  // never attempt to fill in the stack trace of preallocated errors that do not have
  // backtrace. These errors are kept alive forever and may be "re-used" when all
  // preallocated errors with backtrace have been consumed. Also need to avoid
  // a potential loop which could happen if an out of memory occurs when attempting
  // to allocate the backtrace.
  objArrayOop preallocated_oom = out_of_memory_errors();
  for (int i = 0; i < _oom_count; i++) {
    if (throwable() == preallocated_oom->obj_at(i)) {
      return false;
    }
  }
  return true;
}


oop Universe::gen_out_of_memory_error(oop default_err) {
  // generate an out of memory error:
  // - if there is a preallocated error and stack traces are available
  //   (j.l.Throwable is initialized), then return the preallocated
  //   error with a filled in stack trace, and with the message
  //   provided by the default error.
  // - otherwise, return the default error, without a stack trace.
  int next;
  if ((_preallocated_out_of_memory_error_avail_count > 0) &&
      vmClasses::Throwable_klass()->is_initialized()) {
    next = (int)Atomic::add(&_preallocated_out_of_memory_error_avail_count, -1);
    assert(next < (int)PreallocatedOutOfMemoryErrorCount, "avail count is corrupt");
  } else {
    next = -1;
  }
  if (next < 0) {
    // all preallocated errors have been used.
    // return default
    return default_err;
  } else {
    JavaThread* current = JavaThread::current();
    Handle default_err_h(current, default_err);
    // get the error object at the slot and set set it to NULL so that the
    // array isn't keeping it alive anymore.
    Handle exc(current, preallocated_out_of_memory_errors()->obj_at(next));
    assert(exc() != NULL, "slot has been used already");
    preallocated_out_of_memory_errors()->obj_at_put(next, NULL);

    // use the message from the default error
    oop msg = java_lang_Throwable::message(default_err_h());
    assert(msg != NULL, "no message");
    java_lang_Throwable::set_message(exc(), msg);

    // populate the stack trace and return it.
    java_lang_Throwable::fill_in_stack_trace_of_preallocated_backtrace(exc);
    return exc();
  }
}

// Setup preallocated OutOfMemoryError errors
void Universe::create_preallocated_out_of_memory_errors(TRAPS) {
  InstanceKlass* ik = vmClasses::OutOfMemoryError_klass();
  objArrayOop oa = oopFactory::new_objArray(ik, _oom_count, CHECK);
  objArrayHandle oom_array(THREAD, oa);

  for (int i = 0; i < _oom_count; i++) {
    oop oom_obj = ik->allocate_instance(CHECK);
    oom_array->obj_at_put(i, oom_obj);
  }
  _out_of_memory_errors = OopHandle(vm_global(), oom_array());

  Handle msg = java_lang_String::create_from_str("Java heap space", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_java_heap), msg());

  msg = java_lang_String::create_from_str("C heap space", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_c_heap), msg());

  msg = java_lang_String::create_from_str("Metaspace", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_metaspace), msg());

  msg = java_lang_String::create_from_str("Compressed class space", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_class_metaspace), msg());

  msg = java_lang_String::create_from_str("Requested array size exceeds VM limit", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_array_size), msg());

  msg = java_lang_String::create_from_str("GC overhead limit exceeded", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_gc_overhead_limit), msg());

  msg = java_lang_String::create_from_str("Java heap space: failed reallocation of scalar replaced objects", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_realloc_objects), msg());

  msg = java_lang_String::create_from_str("Java heap space: failed retryable allocation", CHECK);
  java_lang_Throwable::set_message(oom_array->obj_at(_oom_retry), msg());

  // Setup the array of errors that have preallocated backtrace
  int len = (StackTraceInThrowable) ? (int)PreallocatedOutOfMemoryErrorCount : 0;
  objArrayOop instance = oopFactory::new_objArray(ik, len, CHECK);
  _preallocated_out_of_memory_error_array = OopHandle(vm_global(), instance);
  objArrayHandle preallocated_oom_array(THREAD, instance);

  for (int i=0; i<len; i++) {
    oop err = ik->allocate_instance(CHECK);
    Handle err_h(THREAD, err);
    java_lang_Throwable::allocate_backtrace(err_h, CHECK);
    preallocated_oom_array->obj_at_put(i, err_h());
  }
  _preallocated_out_of_memory_error_avail_count = (jint)len;
}

intptr_t Universe::_non_oop_bits = 0;

void* Universe::non_oop_word() {
  // Neither the high bits nor the low bits of this value is allowed
  // to look like (respectively) the high or low bits of a real oop.
  //
  // High and low are CPU-specific notions, but low always includes
  // the low-order bit.  Since oops are always aligned at least mod 4,
  // setting the low-order bit will ensure that the low half of the
  // word will never look like that of a real oop.
  //
  // Using the OS-supplied non-memory-address word (usually 0 or -1)
  // will take care of the high bits, however many there are.

  if (_non_oop_bits == 0) {
    _non_oop_bits = (intptr_t)os::non_memory_address_word() | 1;
  }

  return (void*)_non_oop_bits;
}

bool Universe::contains_non_oop_word(void* p) {
  return *(void**)p == non_oop_word();
}

static void initialize_global_behaviours() {
  CompiledICProtectionBehaviour::set_current(new DefaultICProtectionBehaviour());
}

jint universe_init() {
  Universe::_jvm_start_time = (unsigned long long) os::javaTimeMillis();
  assert(!Universe::_fully_initialized, "called after initialize_vtables");
  guarantee(1 << LogHeapWordSize == sizeof(HeapWord),
         "LogHeapWordSize is incorrect.");
  guarantee(sizeof(oop) >= sizeof(HeapWord), "HeapWord larger than oop?");
  guarantee(sizeof(oop) % sizeof(HeapWord) == 0,
            "oop size is not not a multiple of HeapWord size");

  TraceTime timer("Genesis", TRACETIME_LOG(Info, startuptime));

  initialize_global_behaviours();

  GCLogPrecious::initialize();

  // Reserve the SHM region arena before any GC/SHM code runs. Both host
  // and client must do this at the same point in startup so libjvm/libc
  // haven't settled into the target VA range yet.
  Universe::reserveShmArena();

  if(SnicGCCoordinator){
    log_debug(gc)("DGC LOG: SnicGCCoordinator is enabled");
    SnicCoordinator coordinator;
    coordinator.init();
    coordinator.run();
    exit(0);
  }
  if(SnicGCClient){
    Metaspace::global_initialize();

    MetaspaceCounters::initialize_performance_counters();
    Universe::_finalizer_register_cache = new LatestMethodCache();
    Universe::_loader_addClass_cache    = new LatestMethodCache();
    Universe::_throw_illegal_access_error_cache = new LatestMethodCache();
    Universe::_throw_no_such_method_error_cache = new LatestMethodCache();
    Universe::_do_stack_walk_cache = new LatestMethodCache();
    if(UseSharedSpaces){
      MetaspaceShared::initialize_shared_spaces();
    }
    // if (!SnicGCShareMemEnabled) {
    //   auto client = new CopyRegionSnicClient();
    //   client->runRPCServer();
    // } else {
    //   auto shareMemClient = new ShareMemSnicClient();
    //   shareMemClient->runRPCServer();
    // }
    SnicClient::runRPCServer();


    exit(0);
  }
  if(SnicGCHost) {
    if (!SnicGCShareMemEnabled) {
      // Universe::start_rdma_server();
      Universe::connect_rpc_server();
      Universe::start_rdma_server();
      if (SnicGCCoorHeuristic && !SnicGCShareMemEnabled) {
        Universe::start_rdma_server_for_coordinator();
      }
    } else {
      Universe::connect_rpc_server();
      char curSnicShmMemPath[1024];
      // strcpy(curSnicShmMemPath, SnicShmMemPath);
      sprintf(curSnicShmMemPath, "%s_%d", SnicShmMemPath, Universe::SnicHostId);
      Universe::hostShareHeapFD = shm_open(curSnicShmMemPath, O_RDWR | O_CREAT, 0666);
      if(Universe::hostShareHeapFD < 0) {
        log_error(gc)("DGC LOG: create share mem file failed, path: %s", curSnicShmMemPath);
        exit(0);
      }
      else{
        log_debug(gc)("DGC LOG: create share mem file success, path: %s, fd: %d", curSnicShmMemPath, Universe::hostShareHeapFD);
      }

      char curSnicShmRootsPath[1024];
      // strcpy(curSnicShmRootsPath, SnicShmRootsPath);
      sprintf(curSnicShmRootsPath, "%s_%d", SnicShmRootsPath, Universe::SnicHostId);
      Universe::hostShareRootFD = shm_open(curSnicShmRootsPath, O_RDWR | O_CREAT, 0666);
      if(Universe::hostShareRootFD < 0) {
        log_error(gc)("DGC LOG: create share mem file failed, path: %s", curSnicShmRootsPath);
        exit(0);
      }
      else{
        log_debug(gc)("DGC LOG: create share mem file success, path: %s, fd: %d", curSnicShmRootsPath, Universe::hostShareRootFD);
      }

      if(SnicGCCoorHeuristic && SnicGCShareMemEnabled){
        Universe::hostShareGlobalPacerFD = shm_open(SnicShmGlobalPacerPath, O_CREAT | O_RDWR, 0666);
        if (Universe::hostShareGlobalPacerFD < 0) {
          log_error(gc)("DGC LOG: create global pacer shm failed");
          exit(0);
        }
        else{
          log_debug(gc)("DGC LOG: create global pacer shm success, path: %s, fd: %d", SnicShmGlobalPacerPath, Universe::hostShareGlobalPacerFD);
        }
        Universe::hostShareGlobalPacerData = (GlobalPacerData*) Universe::hostMmap(NULL, sizeof(GlobalPacerData), 0, Universe::hostShareGlobalPacerFD, 0);
        // only for test:
        log_debug(gc)("DGC LOG: global pacer data round: %llu", Universe::hostShareGlobalPacerData->round);
      }

      // Lock-free DGC control region (replaces TCP acks in marking hot path)
      if (SnicShmLockFreeMarking) {
        char curControlPath[1024];
        sprintf(curControlPath, "%s_%d", SnicShmControlPath, Universe::SnicHostId);
        Universe::hostShareControlFD = shm_open(curControlPath, O_RDWR | O_CREAT, 0666);
        if (Universe::hostShareControlFD < 0) {
          log_error(gc)("DGC LOG: create control shm failed, path: %s", curControlPath);
          exit(0);
        }
        ftruncate(Universe::hostShareControlFD, sizeof(Universe::ShmDGCControl));
        Universe::shmDGCControl = (Universe::ShmDGCControl*) Universe::hostMmap(NULL, sizeof(Universe::ShmDGCControl), 0, Universe::hostShareControlFD, 0);
        memset((void*)Universe::shmDGCControl, 0, sizeof(Universe::ShmDGCControl));
        log_debug(gc)("DGC LOG: created DGC control SHM at %p, path: %s", Universe::shmDGCControl, curControlPath);

        // Pre-allocate roots SHM region (avoid per-cycle mmap)
        ftruncate(Universe::hostShareRootFD, Universe::PREALLOC_TOTAL_SIZE);
        Universe::preallocRootsAddr = (unsigned long long*) Universe::hostMmap(
            NULL, Universe::PREALLOC_TOTAL_SIZE, 0, Universe::hostShareRootFD, 0);
        if (Universe::preallocRootsAddr == MAP_FAILED || Universe::preallocRootsAddr == nullptr) {
          log_error(gc)("DGC LOG: pre-alloc roots mmap failed");
          exit(0);
        }
        // Publish pre-alloc info in control struct for client
        Atomic::release_store(&Universe::shmDGCControl->prealloc_host_addr,
                              (unsigned long long)Universe::preallocRootsAddr);
        Atomic::release_store(&Universe::shmDGCControl->prealloc_size,
                              (unsigned long long)Universe::PREALLOC_TOTAL_SIZE);
        log_debug(gc)("DGC LOG: pre-alloc roots SHM at %p, size=%luMB",
                     Universe::preallocRootsAddr, Universe::PREALLOC_TOTAL_SIZE / (1024*1024));
      }
    }
  }

  GCConfig::arguments()->initialize_heap_sizes();
  jint status = Universe::initialize_heap();
  if (status != JNI_OK) {
    return status;
  }

  if (SnicGCCoorHeuristic && SnicGCHost) {
    Universe::nonmarking_time_history = new TruncatedSeq(10, ShenandoahAdaptiveDecayFactor);
    Universe::liveness_history = new TruncatedSeq(10, ShenandoahAdaptiveDecayFactor);
    Universe::gc_interval_history = new TruncatedSeq(10, ShenandoahAdaptiveDecayFactor);
    Universe::init_coor_args();
  }

  // if(SnicGCHost && !SnicGCShareMemEnabled) {
  //   int message[2];
  //   log_debug(gc)("DGC LOG: send RPC 1 to copyRegion snic client");
  //   Universe::send_rpc(1, sizeof(int) * 2, message);
  //   Universe::recv_int_ack();
  //   log_debug(gc)("DGC LOG: Success get response of RPC 1");
  // }

  Universe::initialize_tlab();

  Metaspace::global_initialize();

  // Initialize performance counters for metaspaces
  MetaspaceCounters::initialize_performance_counters();

  // Checks 'AfterMemoryInit' constraints.
  if (!JVMFlagLimit::check_all_constraints(JVMFlagConstraintPhase::AfterMemoryInit)) {
    return JNI_EINVAL;
  }

  // Create memory for metadata.  Must be after initializing heap for
  // DumpSharedSpaces.
  ClassLoaderData::init_null_class_loader_data();

  // We have a heap so create the Method* caches before
  // Metaspace::initialize_shared_spaces() tries to populate them.
  Universe::_finalizer_register_cache = new LatestMethodCache();
  Universe::_loader_addClass_cache    = new LatestMethodCache();
  Universe::_throw_illegal_access_error_cache = new LatestMethodCache();
  Universe::_throw_no_such_method_error_cache = new LatestMethodCache();
  Universe::_do_stack_walk_cache = new LatestMethodCache();

#if INCLUDE_CDS
  if (UseSharedSpaces) {
    // Read the data structures supporting the shared spaces (shared
    // system dictionary, symbol table, etc.).  After that, access to
    // the file (other than the mapped regions) is no longer needed, and
    // the file is closed. Closing the file does not affect the
    // currently mapped regions.
    MetaspaceShared::initialize_shared_spaces();
    StringTable::create_table();
  } else
#endif
  {
    SymbolTable::create_table();
    StringTable::create_table();
  }

#if INCLUDE_CDS
  if (Arguments::is_dumping_archive()) {
    MetaspaceShared::prepare_for_dumping();
  }
#endif

  if (strlen(VerifySubSet) > 0) {
    Universe::initialize_verify_flags();
  }

  ResolvedMethodTable::create_table();

  return JNI_OK;
}

jint Universe::initialize_heap() {
  assert(_collectedHeap == NULL, "Heap already created");
  _collectedHeap = GCConfig::arguments()->create_heap();

  log_info(gc)("Using %s", _collectedHeap->name());
  return _collectedHeap->initialize();
}

void Universe::initialize_tlab() {
  ThreadLocalAllocBuffer::set_max_size(Universe::heap()->max_tlab_size());
  if (UseTLAB) {
    ThreadLocalAllocBuffer::startup_initialization();
  }
}

ReservedHeapSpace Universe::reserve_heap(size_t heap_size, size_t alignment) {

  assert(alignment <= Arguments::conservative_max_heap_alignment(),
         "actual alignment " SIZE_FORMAT " must be within maximum heap alignment " SIZE_FORMAT,
         alignment, Arguments::conservative_max_heap_alignment());

  size_t total_reserved = align_up(heap_size, alignment);
  assert(!UseCompressedOops || (total_reserved <= (OopEncodingHeapMax - os::vm_page_size())),
      "heap size is too big for compressed oops");

  size_t page_size = os::vm_page_size();
  if (UseLargePages && is_aligned(alignment, os::large_page_size())) {
    page_size = os::large_page_size();
  } else {
    // Parallel is the only collector that might opt out of using large pages
    // for the heap.
    assert(!UseLargePages || UseParallelGC , "Wrong alignment to use large pages");
  }

  // Now create the space.
  ReservedHeapSpace total_rs(total_reserved, alignment, page_size, AllocateHeapAt);

  if (total_rs.is_reserved()) {
    assert((total_reserved == total_rs.size()) && ((uintptr_t)total_rs.base() % alignment == 0),
           "must be exactly of required size and alignment");
    // We are good.

    // LHT modified
    log_debug(gc)("DGC LOG: get heap config, size=%lld, base=%llx", (unsigned long long)total_rs.size(), (unsigned long long)total_rs.base());

    if (AllocateHeapAt != NULL) {
      log_info(gc,heap)("Successfully allocated Java heap at location %s", AllocateHeapAt);
    }

    if (UseCompressedOops) {
      CompressedOops::initialize(total_rs);
    }

    Universe::calculate_verify_data((HeapWord*)total_rs.base(), (HeapWord*)total_rs.end());

    return total_rs;
  }

  vm_exit_during_initialization(
    err_msg("Could not reserve enough space for " SIZE_FORMAT "KB object heap",
            total_reserved/K));

  // satisfy compiler
  ShouldNotReachHere();
  return ReservedHeapSpace(0, 0, os::vm_page_size());
}

OopStorage* Universe::vm_weak() {
  return Universe::_vm_weak;
}

OopStorage* Universe::vm_global() {
  return Universe::_vm_global;
}

void Universe::oopstorage_init() {
  Universe::_vm_global = OopStorageSet::create_strong("VM Global", mtInternal);
  Universe::_vm_weak = OopStorageSet::create_weak("VM Weak", mtInternal);
}

void universe_oopstorage_init() {
  Universe::oopstorage_init();
}

void initialize_known_method(LatestMethodCache* method_cache,
                             InstanceKlass* ik,
                             const char* method,
                             Symbol* signature,
                             bool is_static, TRAPS)
{
  TempNewSymbol name = SymbolTable::new_symbol(method);
  Method* m = NULL;
  // The klass must be linked before looking up the method.
  if (!ik->link_class_or_fail(THREAD) ||
      ((m = ik->find_method(name, signature)) == NULL) ||
      is_static != m->is_static()) {
    ResourceMark rm(THREAD);
    // NoSuchMethodException doesn't actually work because it tries to run the
    // <init> function before java_lang_Class is linked. Print error and exit.
    vm_exit_during_initialization(err_msg("Unable to link/verify %s.%s method",
                                 ik->name()->as_C_string(), method));
  }
  method_cache->init(ik, m);
}

void Universe::initialize_known_methods(TRAPS) {
  // Set up static method for registering finalizers
  initialize_known_method(_finalizer_register_cache,
                          vmClasses::Finalizer_klass(),
                          "register",
                          vmSymbols::object_void_signature(), true, CHECK);

  initialize_known_method(_throw_illegal_access_error_cache,
                          vmClasses::internal_Unsafe_klass(),
                          "throwIllegalAccessError",
                          vmSymbols::void_method_signature(), true, CHECK);

  initialize_known_method(_throw_no_such_method_error_cache,
                          vmClasses::internal_Unsafe_klass(),
                          "throwNoSuchMethodError",
                          vmSymbols::void_method_signature(), true, CHECK);

  // Set up method for registering loaded classes in class loader vector
  initialize_known_method(_loader_addClass_cache,
                          vmClasses::ClassLoader_klass(),
                          "addClass",
                          vmSymbols::class_void_signature(), false, CHECK);

  // Set up method for stack walking
  initialize_known_method(_do_stack_walk_cache,
                          vmClasses::AbstractStackWalker_klass(),
                          "doStackWalk",
                          vmSymbols::doStackWalk_signature(), false, CHECK);
}

void universe2_init() {
  EXCEPTION_MARK;
  Universe::genesis(CATCH);
}

// Set after initialization of the module runtime, call_initModuleRuntime
void universe_post_module_init() {
  Universe::_module_initialized = true;
}

bool universe_post_init() {
  assert(!is_init_completed(), "Error: initialization not yet completed!");
  Universe::_fully_initialized = true;
  EXCEPTION_MARK;
  if (!UseSharedSpaces) {
    reinitialize_vtables();
    reinitialize_itables();
  }

  HandleMark hm(THREAD);
  // Setup preallocated empty java.lang.Class array for Method reflection.

  objArrayOop the_empty_class_array = oopFactory::new_objArray(vmClasses::Class_klass(), 0, CHECK_false);
  Universe::_the_empty_class_array = OopHandle(Universe::vm_global(), the_empty_class_array);

  // Setup preallocated OutOfMemoryError errors
  Universe::create_preallocated_out_of_memory_errors(CHECK_false);

  oop instance;
  // Setup preallocated cause message for delayed StackOverflowError
  if (StackReservedPages > 0) {
    instance = java_lang_String::create_oop_from_str("Delayed StackOverflowError due to ReservedStackAccess annotated method", CHECK_false);
    Universe::_delayed_stack_overflow_error_message = OopHandle(Universe::vm_global(), instance);
  }

  // Setup preallocated NullPointerException
  // (this is currently used for a cheap & dirty solution in compiler exception handling)
  Klass* k = SystemDictionary::resolve_or_fail(vmSymbols::java_lang_NullPointerException(), true, CHECK_false);
  instance = InstanceKlass::cast(k)->allocate_instance(CHECK_false);
  Universe::_null_ptr_exception_instance = OopHandle(Universe::vm_global(), instance);

  // Setup preallocated ArithmeticException
  // (this is currently used for a cheap & dirty solution in compiler exception handling)
  k = SystemDictionary::resolve_or_fail(vmSymbols::java_lang_ArithmeticException(), true, CHECK_false);
  instance = InstanceKlass::cast(k)->allocate_instance(CHECK_false);
  Universe::_arithmetic_exception_instance = OopHandle(Universe::vm_global(), instance);

  // Virtual Machine Error for when we get into a situation we can't resolve
  k = vmClasses::VirtualMachineError_klass();
  bool linked = InstanceKlass::cast(k)->link_class_or_fail(CHECK_false);
  if (!linked) {
     tty->print_cr("Unable to link/verify VirtualMachineError class");
     return false; // initialization failed
  }
  instance = InstanceKlass::cast(k)->allocate_instance(CHECK_false);
  Universe::_virtual_machine_error_instance = OopHandle(Universe::vm_global(), instance);

  Handle msg = java_lang_String::create_from_str("/ by zero", CHECK_false);
  java_lang_Throwable::set_message(Universe::arithmetic_exception_instance(), msg());

  Universe::initialize_known_methods(CHECK_false);

  // This needs to be done before the first scavenge/gc, since
  // it's an input to soft ref clearing policy.
  {
    MutexLocker x(THREAD, Heap_lock);
    Universe::heap()->update_capacity_and_used_at_gc();
  }

  // ("weak") refs processing infrastructure initialization
  Universe::heap()->post_initialize();

  MemoryService::add_metaspace_memory_pools();

  MemoryService::set_universe_heap(Universe::heap());
#if INCLUDE_CDS
  MetaspaceShared::post_initialize(CHECK_false);
#endif
  return true;
}


void Universe::compute_base_vtable_size() {
  _base_vtable_size = ClassLoader::compute_Object_vtable();
}

void Universe::print_on(outputStream* st) {
  GCMutexLocker hl(Heap_lock); // Heap_lock might be locked by caller thread.
  st->print_cr("Heap");
  heap()->print_on(st);
}

void Universe::print_heap_at_SIGBREAK() {
  if (PrintHeapAtSIGBREAK) {
    print_on(tty);
    tty->cr();
    tty->flush();
  }
}

void Universe::initialize_verify_flags() {
  verify_flags = 0;
  const char delimiter[] = " ,";

  size_t length = strlen(VerifySubSet);
  char* subset_list = NEW_C_HEAP_ARRAY(char, length + 1, mtInternal);
  strncpy(subset_list, VerifySubSet, length + 1);
  char* save_ptr;

  char* token = strtok_r(subset_list, delimiter, &save_ptr);
  while (token != NULL) {
    if (strcmp(token, "threads") == 0) {
      verify_flags |= Verify_Threads;
    } else if (strcmp(token, "heap") == 0) {
      verify_flags |= Verify_Heap;
    } else if (strcmp(token, "symbol_table") == 0) {
      verify_flags |= Verify_SymbolTable;
    } else if (strcmp(token, "string_table") == 0) {
      verify_flags |= Verify_StringTable;
    } else if (strcmp(token, "codecache") == 0) {
      verify_flags |= Verify_CodeCache;
    } else if (strcmp(token, "dictionary") == 0) {
      verify_flags |= Verify_SystemDictionary;
    } else if (strcmp(token, "classloader_data_graph") == 0) {
      verify_flags |= Verify_ClassLoaderDataGraph;
    } else if (strcmp(token, "metaspace") == 0) {
      verify_flags |= Verify_MetaspaceUtils;
    } else if (strcmp(token, "jni_handles") == 0) {
      verify_flags |= Verify_JNIHandles;
    } else if (strcmp(token, "codecache_oops") == 0) {
      verify_flags |= Verify_CodeCacheOops;
    } else if (strcmp(token, "resolved_method_table") == 0) {
      verify_flags |= Verify_ResolvedMethodTable;
    } else if (strcmp(token, "stringdedup") == 0) {
      verify_flags |= Verify_StringDedup;
    } else {
      vm_exit_during_initialization(err_msg("VerifySubSet: \'%s\' memory sub-system is unknown, please correct it", token));
    }
    token = strtok_r(NULL, delimiter, &save_ptr);
  }
  FREE_C_HEAP_ARRAY(char, subset_list);
}

bool Universe::should_verify_subset(uint subset) {
  if (verify_flags & subset) {
    return true;
  }
  return false;
}

void Universe::verify(VerifyOption option, const char* prefix) {
  // The use of _verify_in_progress is a temporary work around for
  // 6320749.  Don't bother with a creating a class to set and clear
  // it since it is only used in this method and the control flow is
  // straight forward.
  _verify_in_progress = true;

  COMPILER2_PRESENT(
    assert(!DerivedPointerTable::is_active(),
         "DPT should not be active during verification "
         "(of thread stacks below)");
  )

  Thread* thread = Thread::current();
  ResourceMark rm(thread);
  HandleMark hm(thread);  // Handles created during verification can be zapped
  _verify_count++;

  FormatBuffer<> title("Verifying %s", prefix);
  GCTraceTime(Info, gc, verify) tm(title.buffer());
  if (should_verify_subset(Verify_Threads)) {
    log_debug(gc, verify)("Threads");
    Threads::verify();
  }
  if (should_verify_subset(Verify_Heap)) {
    log_debug(gc, verify)("Heap");
    heap()->verify(option);
  }
  if (should_verify_subset(Verify_SymbolTable)) {
    log_debug(gc, verify)("SymbolTable");
    SymbolTable::verify();
  }
  if (should_verify_subset(Verify_StringTable)) {
    log_debug(gc, verify)("StringTable");
    StringTable::verify();
  }
  if (should_verify_subset(Verify_CodeCache)) {
    MutexLocker mu(CodeCache_lock, Mutex::_no_safepoint_check_flag);
    log_debug(gc, verify)("CodeCache");
    CodeCache::verify();
  }
  if (should_verify_subset(Verify_SystemDictionary)) {
    log_debug(gc, verify)("SystemDictionary");
    SystemDictionary::verify();
  }
  if (should_verify_subset(Verify_ClassLoaderDataGraph)) {
    log_debug(gc, verify)("ClassLoaderDataGraph");
    ClassLoaderDataGraph::verify();
  }
  if (should_verify_subset(Verify_MetaspaceUtils)) {
    log_debug(gc, verify)("MetaspaceUtils");
    DEBUG_ONLY(MetaspaceUtils::verify();)
  }
  if (should_verify_subset(Verify_JNIHandles)) {
    log_debug(gc, verify)("JNIHandles");
    JNIHandles::verify();
  }
  if (should_verify_subset(Verify_CodeCacheOops)) {
    log_debug(gc, verify)("CodeCache Oops");
    CodeCache::verify_oops();
  }
  if (should_verify_subset(Verify_ResolvedMethodTable)) {
    log_debug(gc, verify)("ResolvedMethodTable Oops");
    ResolvedMethodTable::verify();
  }
  if (should_verify_subset(Verify_StringDedup)) {
    log_debug(gc, verify)("String Deduplication");
    StringDedup::verify();
  }

  _verify_in_progress = false;
}


#ifndef PRODUCT
void Universe::calculate_verify_data(HeapWord* low_boundary, HeapWord* high_boundary) {
  assert(low_boundary < high_boundary, "bad interval");

  // decide which low-order bits we require to be clear:
  size_t alignSize = MinObjAlignmentInBytes;
  size_t min_object_size = CollectedHeap::min_fill_size();

  // make an inclusive limit:
  uintptr_t max = (uintptr_t)high_boundary - min_object_size*wordSize;
  uintptr_t min = (uintptr_t)low_boundary;
  assert(min < max, "bad interval");
  uintptr_t diff = max ^ min;

  // throw away enough low-order bits to make the diff vanish
  uintptr_t mask = (uintptr_t)(-1);
  while ((mask & diff) != 0)
    mask <<= 1;
  uintptr_t bits = (min & mask);
  assert(bits == (max & mask), "correct mask");
  // check an intermediate value between min and max, just to make sure:
  assert(bits == ((min + (max-min)/2) & mask), "correct mask");

  // require address alignment, too:
  mask |= (alignSize - 1);

  if (!(_verify_oop_mask == 0 && _verify_oop_bits == (uintptr_t)-1)) {
    assert(_verify_oop_mask == mask && _verify_oop_bits == bits, "mask stability");
  }
  _verify_oop_mask = mask;
  _verify_oop_bits = bits;
}

// Oop verification (see MacroAssembler::verify_oop)

uintptr_t Universe::verify_oop_mask() {
  return _verify_oop_mask;
}

uintptr_t Universe::verify_oop_bits() {
  return _verify_oop_bits;
}

uintptr_t Universe::verify_mark_mask() {
  return markWord::lock_mask_in_place;
}

uintptr_t Universe::verify_mark_bits() {
  intptr_t mask = verify_mark_mask();
  intptr_t bits = (intptr_t)markWord::prototype().value();
  assert((bits & ~mask) == 0, "no stray header bits");
  return bits;
}
#endif // PRODUCT


void LatestMethodCache::init(Klass* k, Method* m) {
  if (!UseSharedSpaces) {
    _klass = k;
  }
#ifndef PRODUCT
  else {
    // sharing initilization should have already set up _klass
    assert(_klass != NULL, "just checking");
  }
#endif

  _method_idnum = m->method_idnum();
  assert(_method_idnum >= 0, "sanity check");
}


Method* LatestMethodCache::get_method() {
  if (klass() == NULL) return NULL;
  InstanceKlass* ik = InstanceKlass::cast(klass());
  Method* m = ik->method_with_idnum(method_idnum());
  assert(m != NULL, "sanity check");
  return m;
}

#ifdef ASSERT
// Release dummy object(s) at bottom of heap
bool Universe::release_fullgc_alot_dummy() {
  MutexLocker ml(FullGCALot_lock);
  objArrayOop fullgc_alot_dummy_array = (objArrayOop)_fullgc_alot_dummy_array.resolve();
  if (fullgc_alot_dummy_array != NULL) {
    if (_fullgc_alot_dummy_next >= fullgc_alot_dummy_array->length()) {
      // No more dummies to release, release entire array instead
      _fullgc_alot_dummy_array.release(Universe::vm_global());
      return false;
    }

    // Release dummy at bottom of old generation
    fullgc_alot_dummy_array->obj_at_put(_fullgc_alot_dummy_next++, NULL);
  }
  return true;
}

bool Universe::is_gc_active() {
  return heap()->is_gc_active();
}

bool Universe::is_in_heap(const void* p) {
  return heap()->is_in(p);
}

#endif // ASSERT

void Universe::start_rdma_server(){
  log_debug(gc)("DGC LOG: START reserve heap");
  ctrl = new rdmaio::RCtrl((RDMAPort + Universe::get_SnicHostId()));

  auto nic =
      rdmaio::RNic::create(rdmaio::RNicInfo::query_dev_names().at(0)).value();
  auto nic2 =
      rdmaio::RNic::create(rdmaio::RNicInfo::query_dev_names().at(1)).value();
  log_debug(gc)("DGC LOG: create nic success, host:%d, port:%d", Universe::get_SnicHostId(), RDMAPort + Universe::get_SnicHostId());
  // register the nic with name 0 to the ctrl
  ctrl->opened_nics.reg(0, nic);
  ctrl->opened_nics.reg(1, nic2);
  ctrl->start_daemon();
}

void Universe::start_rdma_server_for_coordinator() {
  ctrl_for_coor = new rdmaio::RCtrl(RDMAPortForCoor + SnicGCCoorClientId);
  auto nic = rdmaio::RNic::create(rdmaio::RNicInfo::query_dev_names().at(0)).value();
  log_debug(gc)("DGC LOG: create nic for coordinator success, host id for coor:%d, port for coor:%d", SnicGCCoorClientId, RDMAPortForCoor + SnicGCCoorClientId);
  // register the nic with name 0 to the ctrl
  ctrl_for_coor->opened_nics.reg(0, nic);
  ctrl_for_coor->start_daemon();
}

size_t Universe::compute_mr_idx_for_cur_host(size_t idx) {
  auto cur_host_id = Universe::get_SnicHostId();
  return cur_host_id * 50 + idx;
}

int Universe::add_mr(void* base, size_t sz){
  //log_dev_debug(gc)("DGC LOG: add mr with base=%p, size=%lld", base, sz);
  _mr_index += 1;
  add_mr(base, sz, _mr_index);
  return _mr_index;
}

// SnicGCRDMABatchFetchKlass — update the host-side CCS HWM. Called from the
// class-loader paths on every Klass definition (see systemDictionary.cpp /
// classLoader.cpp / objArrayKlass.cpp / typeArrayKlass.cpp). Monotonic max.
void Universe::update_ccs_hwm(unsigned long long klass_end) {
  unsigned long long cur;
  do {
    cur = Atomic::load(&_ccs_hwm);
    if (klass_end <= cur) return;
  } while (Atomic::cmpxchg(&_ccs_hwm, cur, klass_end) != cur);
}

void Universe::add_mr(void* base, size_t sz, size_t idx){
  auto updated_idx = compute_mr_idx_for_cur_host(idx);
  log_debug(gc)("DGC LOG: host %d add_mr origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
  auto rm = new rdmaio::rmem::RMem(base, (rdmaio::u64)sz);
  auto add_mr_res = ctrl->registered_mrs.create_then_reg(
    updated_idx, rdmaio::Arc<rdmaio::rmem::RMem>(rm),
    ctrl->opened_nics.query(0).value());
  if (!add_mr_res.has_value()) {
    log_debug(gc)("DGC LOG: host %d add_mr failed, origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
    exit(0);
  }
  auto check = ctrl->registered_mrs.query(updated_idx);
  if (!check.has_value()) {
    log_debug(gc)("DGC LOG: host %d adds mr %lu (origin idx:%lu), but fail to find it", Universe::get_SnicHostId(), updated_idx, idx);
    exit(0);
  }
  log_debug(gc)("DGC LOG: host %d add_mr success, origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
}

void Universe::add_mr_for_coor(void* base, size_t sz, size_t idx) {
  auto updated_idx = compute_mr_idx_for_cur_host(idx);
  log_debug(gc)("DGC LOG: host %d add_mr_for_coor origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
  auto rm = new rdmaio::rmem::RMem(base, (rdmaio::u64)sz);
  auto add_mr_res = ctrl_for_coor->registered_mrs.create_then_reg(
      updated_idx, rdmaio::Arc<rdmaio::rmem::RMem>(rm),
      ctrl_for_coor->opened_nics.query(0).value());
  if (!add_mr_res.has_value()) {
    log_debug(gc)("DGC LOG: host %d add_mr_for_coor failed, origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
    exit(0);
  }
  auto check = ctrl_for_coor->registered_mrs.query(updated_idx);
  if (!check.has_value()) {
    log_debug(gc)("DGC LOG: host %d add_mr_for_coor %lu (origin idx:%lu), but fail to find it", Universe::get_SnicHostId(), updated_idx, idx);
    exit(0);
  }
}

void Universe::add_mr_nic2(void* base, size_t sz, size_t idx){
  auto updated_idx = compute_mr_idx_for_cur_host(idx);
  log_debug(gc)("DGC LOG: host %d add_mr_nic2 origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
  auto rm = new rdmaio::rmem::RMem(base, (rdmaio::u64)sz);
  auto add_mr_res = ctrl->registered_mrs.create_then_reg(
    updated_idx, rdmaio::Arc<rdmaio::rmem::RMem>(rm),
    ctrl->opened_nics.query(1).value());
  if (!add_mr_res.has_value()) {
    log_debug(gc)("DGC LOG: host %d add_mr_nic2 failed, origin idx:%lu, target idx:%lu", Universe::get_SnicHostId(), idx, updated_idx);
    exit(0);
  }
  auto check = ctrl->registered_mrs.query(updated_idx);
  if (!check.has_value()) {
    log_debug(gc)("DGC LOG: host %d adds mr_nic2 %lu (origin idx:%lu), but fail to find it", Universe::get_SnicHostId(), updated_idx, idx);
    exit(0);
  }
}

void Universe::connect_rpc_server(){
  // std::string serverIP = "127.0.0.1";
  // int serverPort = 9999;

  // 创建 socket
  int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (clientSocket == -1) {
      log_error(gc)("DGC LOG: Failed to create socket.");
      exit(0);
  }

  // 设置服务器的 IP 和端口
  sockaddr_in serverAddress{};
  serverAddress.sin_family = AF_INET;
  serverAddress.sin_port = htons(RPCPort);
  if (inet_pton(AF_INET, SNICAddr, &serverAddress.sin_addr) <= 0) {
      log_error(gc)("DGC LOG: Invalid server IP address.");
      exit(0);
  }

  // 连接到服务器
  int connect_result = -1;
  log_debug(gc)("DGC LOG: try to connect to rpc server, clientSocket is %d", clientSocket);
  if ((connect_result = connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress))) < 0) {
    close(clientSocket);
      log_error(gc)("DGC LOG: Failed to connect to server, %d, %s, server rpc port:%d, connect clientSocket:%d", connect_result, strerror(errno), RPCPort, clientSocket);
      exit(0);
  }
  // OPT#3: disable Nagle on the host side too. Host sends small RPC headers
  // and small ack reads interact with Nagle on the client's side. Setting
  // TCP_NODELAY on both ends eliminates the 40ms delayed-ack interlock that
  // was dominating per-cycle rdmaMark.
  int tcp_nodelay = 1;
  if (setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay)) < 0) {
    log_debug(gc)("DGC LOG: setsockopt TCP_NODELAY failed on host rpc_desc: %s", strerror(errno));
  } else {
    log_debug(gc)("DGC LOG: TCP_NODELAY enabled on host rpc_desc fd=%d", clientSocket);
  }
  rpc_desc = clientSocket;
  log_debug(gc)("DGC LOG: success connect to rpc server, fd is %d", rpc_desc);
  Universe::SnicHostId = (int)recv_ack();
  log_debug(gc)("DGC LOG: SnicHostId is %d", Universe::SnicHostId);
}

void Universe::send_rpc(int rpcType, int sz, void* buffer){
  //log_dev_debug(gc)("DGC LOG: Send RPC %d to server.", rpcType);
  int* message = (int*)buffer;
  short* msg_shorts = (short*)message;
  msg_shorts[0] = (short)rpcType;
  msg_shorts[1] = (short)Universe::SnicHostId;
  message[1] = sz;
  int send_result = send(get_rpc_desc(), buffer, sz, 0);
  if (send_result == -1) {
    log_error(gc)("DGC LOG: Failed to send RPC to server.");
    exit(0);
  } else {
    if (send_result == 0) {
      log_error(gc)("DGC LOG: connection is closed");
      exit(0);
    }
  }
}

char Universe::recv_ack(){
  int count=0;
  char notify;
  while(recv(Universe::get_rpc_desc(), &notify, sizeof(notify), 0) <= 0){
    log_debug(gc)("DGC LOG: Fail to get notification, %d, %s", errno, strerror(errno));
    count++;
    if(count >=100){
      exit(0);
    }
  }
  // if (notify == (char) (1)) {
  //   log_debug(gc)("DGC LOG: Success get notification:1");
  // }
  log_debug(gc)("DGC LOG: Success get notification %d", notify);
  return notify;
}

size_t Universe::recv_int_ack() {
  int count=0;
  size_t notify;
  while(recv(Universe::get_rpc_desc(), &notify, sizeof(notify), 0) <= 0){
    // log_debug(gc)("DGC LOG: Fail to get notification, %d, %s", errno, strerror(errno));
    count++;
    if (count >= 100) {
      exit(0);
    }
  }
  if (notify == (size_t)(1)) {
    log_debug(gc)("DGC LOG: Success get notification:1");
  }
  return notify;
}

int Universe::get_ack_type() {
  int count=0;
  char notify;
  while(recv(Universe::get_rpc_desc(), &notify, sizeof(notify), 0) <= 0){
    log_debug(gc)("DGC LOG: Fail to get notification, %d, %s", errno, strerror(errno));
    count++;
    if(count >=100){
      exit(0);
    }
  }
  return (int) (notify);
}

void Universe::start_ccmark(){
  _during_ccmark = true;
}
void Universe::finish_ccmark(){
  _during_ccmark = false;
}
bool Universe::during_ccmark(){
  return _during_ccmark;
}

void Universe::start_global_pacer(){
  _during_global_pacer = true;
}
void Universe::finish_global_pacer(){
  _during_global_pacer = false;
}
bool Universe::during_global_pacer(){
  return _during_global_pacer;
}


void Universe::start_global_pacer_Client_Occupied(){
  _during_global_pacer_Client_Occupied = true;
}
void Universe::finish_global_pacer_Client_Occupied(){
  _during_global_pacer_Client_Occupied = false;
}
bool Universe::during_global_pacer_Client_Occupied(){
  return _during_global_pacer_Client_Occupied;
}

bool Universe::during_SnicGCFallback(){
  return _SnicGCFallback;
}
void Universe::start_SnicGCFallback(){
  log_debug(gc)("DGC LOG: start SnicGCFallback");
  _SnicGCFallback = true;
  if (SnicGCShareMemEnabled) {
    // reset global pacer data so this host stops blocking new force-GCs
    reset_force_gc_state(Universe::get_CoorHostId());
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].startPacer), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].nextGCTime), (unsigned long long)9999999999999);
    log_debug(gc, ergo)("DGC LOG: set nextGCTime start_SnicGCFallback: %lu", 9999999999999);
    // then send rpc to snic client.
    char* buffer = new char[1024];
    Universe::send_rpc(12, 2 * sizeof(int), (void *)buffer);
    delete(buffer);
  } else {
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->forceGCTriggerTimestamp), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->forceGCId), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->startPacer), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->nextGCTime), (unsigned long long)9999999999999);
    log_debug(gc, ergo)("DGC LOG: rdma host %d set nextGCTime start_SnicGCFallback: %lu", Universe::get_SnicHostId(), 9999999999999);
  }
}

void Universe::start_SnicGCFallback_silent(){
  // Used when the SHM client is presumed dead — the regular
  // start_SnicGCFallback() would call send_rpc(12) which exit(0)s on TCP
  // failure, taking the host down with the client. Reset just the local
  // state and SHM-resident pacer fields (these are mmap stores, no IPC).
  if (_SnicGCFallback) return;
  log_debug(gc)("DGC LOG: start SnicGCFallback (silent, no RPC 12)");
  _SnicGCFallback = true;
  if (SnicGCShareMemEnabled) {
    if (Universe::get_hostShareGlobalPacerData() != nullptr) {
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].startPacer),
                    (unsigned long long)0);
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].nextGCTime),
                    (unsigned long long)9999999999999);
    }
  } else if (Universe::get_hostPrivateGlobalPacerData() != nullptr) {
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->forceGCTriggerTimestamp), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->forceGCId), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->startPacer), (unsigned long long)0);
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->nextGCTime), (unsigned long long)9999999999999);
  }
}

bool Universe::snic_dgc_client_alive(){
  if (!SnicDGCFaultHandling) return true;
  if (shmDGCControl == nullptr) return true;
  unsigned long long hb = Atomic::load(&shmDGCControl->client_heartbeat_ms);
  if (hb == 0) return true;  // client not yet running SHM loop; trust bootstrap
  unsigned long long now = (unsigned long long)os::javaTimeMillis();
  if (now < hb) return true;  // wall-clock skew; do not falsely flag
  return (now - hb) <= (unsigned long long)SnicDGCHealthTimeoutMs;
}

void Universe::finish_SnicGCFallback(){
  if(_SnicGCFallback){
    log_debug(gc)("DGC LOG: finish SnicGCFallback");
    _SnicGCFallback = false;
  }
}

void Universe::start_final_mark() {
  _during_final_mark = true;
}
void Universe::finish_final_mark() {
  _during_final_mark = false;
}
bool Universe::during_final_mark() {
  return _during_final_mark;
}
void Universe::start_mark_roots() {
  _during_mark_roots = true;
}
void Universe::finish_mark_roots() {
  _during_mark_roots = false;
}
bool Universe::during_mark_roots() {
  return _during_mark_roots;
}
void Universe::empty_test_func() {
  log_info(gc)("get into Universe::empty_test_func");
}

// Reserve the SHM region arena [SnicShmArenaBase, SnicShmArenaBase+Size).
// Both host and client JVMs call this at universe_init() so every mmap
// landing in the arena can use MAP_FIXED — overlaying our PROT_NONE
// reservation instead of fighting ASLR-placed libjvm/libc/stack pages.
//
// Kernel guarantees: MAP_FIXED_NOREPLACE refuses if the target range is
// already in use, so if this succeeds nothing else in our own address
// space is there. The two JVMs are independent processes, so their
// reservations are independent; we just need each to reserve the SAME
// numerical range so RPC'd addresses mean the same thing to both.
void Universe::reserveShmArena() {
  if (!SnicGCShareMemEnabled) {
    return;
  }
  if (shmArenaBase != nullptr) {
    return;  // already reserved (idempotent)
  }
  void* base = (void*) (uintptr_t) SnicShmArenaBase;
  size_t sz = (size_t) SnicShmArenaSize;
  void* r = mmap(base, sz, PROT_NONE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
  if (r == MAP_FAILED || r != base) {
    log_error(gc)("DGC LOG: SHM arena reserve failed, wanted %p got %p errno=%d %s; "
                  "try a different -XX:SnicShmArenaBase", base, r, errno, strerror(errno));
    exit(0);
  }
  shmArenaBase = base;
  shmArenaNext = (unsigned long long) base;
  shmArenaEnd  = (unsigned long long) base + sz;
  log_debug(gc)("DGC LOG: reserved SHM arena [%p, %p) size=%luMB",
               base, (void*) shmArenaEnd, (unsigned long) (sz >> 20));
}

// Bump-pointer allocate inside the arena. Returns nullptr (not exit) if
// the arena was never reserved (e.g. non-SHM JVM) so legacy NULL-addr
// mmap callers keep working. Exits on arena exhaustion — that's a config
// bug (arena too small), not a recoverable runtime failure.
//
// `alignment` (must be a power of two) is applied to BOTH the returned
// address and the next-free pointer so the next allocation can satisfy
// callers with their own alignment requirement. Pass 0 or 1 for
// page-alignment. Metaspace VSNs in particular require
// MAX_CHUNK_BYTE_SIZE (typically 16 MB) alignment or Metachunk address
// arithmetic wedges the commit bitmap.
void* Universe::bumpAllocInShmArena(size_t length, size_t alignment) {
  if (shmArenaBase == nullptr) {
    return nullptr;
  }
  size_t page_sz = (size_t) os::vm_page_size();
  if (alignment < page_sz) {
    alignment = page_sz;
  }
  // Round shmArenaNext up to `alignment`.
  unsigned long long start = shmArenaNext;
  if (start & (alignment - 1)) {
    start = (start + alignment - 1) & ~(alignment - 1);
  }
  size_t aligned_len = (length + page_sz - 1) & ~(page_sz - 1);
  if (start + aligned_len > shmArenaEnd) {
    log_error(gc)("DGC LOG: SHM arena exhausted (next=%p end=%p want=%luB align=%lu); "
                  "raise -XX:SnicShmArenaSize", (void*) start,
                  (void*) shmArenaEnd, (unsigned long) aligned_len, (unsigned long) alignment);
    exit(0);
  }
  void* r = (void*) start;
  shmArenaNext = start + aligned_len;
  return r;
}

void* Universe::hostMmap(void* addr, size_t length, off_t offset, int fd, int rpcType) {
  void * map_res = NULL;
  bool use_fixed = false;

  // NULL addr + reserved arena -> carve a deterministic address out of the
  // arena and MAP_FIXED overlay our PROT_NONE reservation. Preserves the
  // old NULL-means-kernel-picks behaviour when no arena is set up.
  if (addr == nullptr && shmArenaBase != nullptr) {
    void* bump = bumpAllocInShmArena(length);
    if (bump != nullptr) {
      addr = bump;
      use_fixed = true;
    }
  }

  int fixed = use_fixed ? MAP_FIXED : 0;
  if (fd != -1) {
    map_res = mmap(addr, length, PROT_READ | PROT_WRITE, MAP_SHARED | fixed, fd, offset);
  } else {
    map_res = mmap(addr, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | fixed, -1, 0);
  }
  if (map_res == (void*) (-1)) {
    log_debug(gc)("DGC LOG: host mmap failed, %s,args:%p,%lu,%lu,%d,%d", strerror(errno), addr, length, offset, fd, rpcType);
    exit(0);
  }
  if (addr != nullptr && map_res != addr) {
    log_debug(gc)("DGC LOG: host mmap res mismatch, %p,%p,%lu,%lu,%d,%d", map_res, addr, length, offset, fd, rpcType);
    exit(0);
  }
  log_debug(gc)("DGC LOG: host mmap success:%p,%p,%lu,%lu,%d,%d", map_res, addr, length, offset, fd, rpcType);
  return map_res;
}

int Universe::hostMsync(void* addr, size_t length, int flags) {
  unsigned long long start = (unsigned long long)addr;
  unsigned long long size = (unsigned long long) (length);
  unsigned long long end = start + size;
  auto single_page_size = os::vm_page_size();
  if (start % single_page_size != 0) {
    start = start - start % single_page_size;
  }
  if (end % single_page_size != 0) {
    end = end - end % single_page_size + single_page_size;
  }
  return msync((void*) (start), (size_t) (end - start), flags);
}

void Universe::ftruncateFile(int fd, size_t length) {
  auto trunc_res = ftruncate(fd, (off_t)(length));
  if (trunc_res != 0) {
    log_error(gc)("DGC LOG: truncate share mem roots file failed, %d", trunc_res);
    exit(0);
  }
}

int Universe::get_hostShareHeapFD() {
  return hostShareHeapFD;
}

int Universe::get_hostShareRootFD() {
  return hostShareRootFD;
}

char* Universe::generate_virtual_node_shm_path() {
  char* path = (char*) malloc(1024);
  sprintf(path, "%s_%d_%d", SnicShmVirtualNodePath, Universe::SnicHostId, virtualSpaceNodeCount);
  virtualSpaceNodeCount++;
  return path;
}

GlobalPacerData* Universe::get_hostShareGlobalPacerData() {
  return hostShareGlobalPacerData;
}

GlobalPacerDataPerHost* Universe::get_hostPrivateGlobalPacerData() {
  return hostPrivateGlobalPacerData;
}

void Universe::set_hostPrivateGlobalPacerData(GlobalPacerDataPerHost* data) {
  hostPrivateGlobalPacerData = data;
}

void Universe::shm_exit() {
  // unlink share space for java heap.
  shm_unlink("/share_heap");
  // unlink share space for roots.
  shm_unlink("/share_roots");
  // unlink share space for bitmap.
  shm_unlink("/share_bitmap");
  // unlink share space for liveness.
  shm_unlink("/share_liveness");
  // unlink share space for region tams information.
  shm_unlink("/share_region_tams");
  // unlink all share spaces for VirtalNode. Must match the format used by
  // generate_virtual_node_shm_path(): "<SnicShmVirtualNodePath>_<host>_<node>".
  // A prior version of this code unlinked "/virtual_node_%d" which never
  // matched the real filenames (e.g. /virtual_node_0_3), so 65+ GB of stale
  // tmpfs files were leaking across runs and causing ibv_reg_mr on the next
  // 4 GB metaspace MR to stall under kernel memory pressure.
  char* path = (char*) (malloc(1024));
  for (int i = 0; i < virtualSpaceNodeCount; ++i) {
    sprintf(path, "%s_%d_%d", SnicShmVirtualNodePath, Universe::SnicHostId, i);
    shm_unlink(path);
  }
  free(path);
}

bool Universe::rpc_server_should_stop() {
  return rpcServerShouldStop;
}

void Universe::set_rpc_server_should_stop(bool should_stop) {
  rpcServerShouldStop = should_stop;
}

int Universe::get_SnicHostId() {
  return SnicHostId;
}

// this function will return host id seen by SnicCoordinator.
int Universe::get_CoorHostId() {
  if (SnicGCCoorHeuristic) {
    return SnicGCCoorClientId;
  }
  return SnicHostId;
}

std::vector<size_t>* Universe::get_free_headroom_history() {
  return &free_headroom_history;
}

long Universe::get_dpuCurTimeDuringMarkInMs() {
  return dpuCurTimeDuringMarkInMs;
}

double Universe::get_dpuAverageAllocRate() {
  return dpuAverageAllocRate;
}

void Universe::set_dpuCurTimeDuringMarkInMs(long curTime) {
  dpuCurTimeDuringMarkInMs = curTime;
}

void Universe::set_dpuAverageAllocRate(double allocRate) {
  dpuAverageAllocRate = allocRate;
}

double Universe::get_dpuCurTimeDuringMarkInSec() {
  return dpuCurTimeDuringMarkInSec;
}

void Universe::set_dpuCurTimeDuringMarkInSec(double curTime) {
  dpuCurTimeDuringMarkInSec = curTime;
}


void Universe::record_nonmarking_time_start() {
  nonmarking_time_start = os::javaTimeMillis();
}

void Universe::record_nonmarking_time_end_and_report() {
  auto nonmarking_time = os::javaTimeMillis() - nonmarking_time_start;
  nonmarking_time_history->add((double)nonmarking_time);
  auto avg_nonmarking_time = nonmarking_time_history->davg();
  auto dsd_nonmarking_time = nonmarking_time_history->dsd();
  auto nonmarking_prediction = avg_nonmarking_time + dsd_nonmarking_time * ShenandoahAdaptiveInitialConfidence;
  log_debug(gc)("DGC LOG: nonmarking_prediction: %lld (avg: %lld, dsd: %lld)", (unsigned long long)nonmarking_prediction, (unsigned long long)avg_nonmarking_time, (unsigned long long)dsd_nonmarking_time);
  // consider the dsd
  // log_debug(gc)("DGC LOG: avg nonmarking time: %llu", avg_nonmarking_time);
  if (SnicGCShareMemEnabled) {
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].nonmarking_time_prediction), (unsigned long long)nonmarking_prediction);
  } else {
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->nonmarking_time_prediction), (unsigned long long)nonmarking_prediction);
  }
}


void Universe::record_liveness_and_report(unsigned long long liveness) {
  liveness_history->add((double)liveness);
  auto avg_liveness = liveness_history->davg();
  auto dsd_liveness = liveness_history->dsd();
  auto liveness_prediction = avg_liveness; //+ dsd_liveness * ShenandoahAdaptiveInitialConfidence;
  log_debug(gc)("DGC LOG: liveness_prediction: %lld (avg: %lld, dsd: %lld)", (unsigned long long)liveness_prediction, (unsigned long long)avg_liveness, (unsigned long long)dsd_liveness);
  if(SnicGCShareMemEnabled){
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].historyLiveness), (unsigned long long)liveness_prediction);
  }
  else{
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->historyLiveness), (unsigned long long)liveness_prediction);
  }
}

void Universe::reset_force_gc_state(int hostId) {
  Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].forceGCTriggerTimestamp), (unsigned long long)0);
  Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].forceGCId), (unsigned long long)0);
  Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].forceGCType), (unsigned long long)Universe::ForceGCTypes::EMPTY);
  Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].forceGCCCMT), (unsigned long long)0);
}


void Universe::init_coor_args() {
  if (SnicGCCoorHeuristic) {

    char* input = (char*)SnicCoorCCMTArgs;
    int int_array[MAX_COOR_CONFIG_NUM];
    double double_array1[MAX_COOR_CONFIG_NUM];
    double double_array2[MAX_COOR_CONFIG_NUM];
    int coor_ccmt_num = 0;

    // 先复制字符串，因为strtok会修改原字符串
    char* input_copy = strdup(input);
    if (input_copy == NULL) {
        log_error(gc)("DGC LOG: strdup failed");
        exit(0);
        // return 1;
    }

    // 第一次分割，用分号';'分隔每一对
    char* pair_token = strtok(input_copy, ";");
    while (pair_token != NULL && coor_ccmt_num < MAX_COOR_CONFIG_NUM) {
        int int_part;
        double double_part;
        double double_part2;

        // 用sscanf解析"int:double"格式
        if (sscanf(pair_token, "%d:%lf:%lf", &int_part, &double_part, &double_part2) == 3) {
            int_array[coor_ccmt_num] = int_part;
            double_array1[coor_ccmt_num] = double_part;
            double_array2[coor_ccmt_num] = double_part2;
            coor_ccmt_num++;
        } else {
            // printf("Failed to parse pair: %s\n", pair_token);
            log_error(gc)("DGC LOG: Failed to parse pair: %s", pair_token);
            exit(0);
        }

        pair_token = strtok(NULL, ";");
    }

    free(input_copy);  // 释放复制的字符串

    // 打印结果
    // log_debug(gc)("DGC LOG: Int array: ");
    // for (int i = 0; i < coor_ccmt_num; i++) {
    //     log_debug(gc)("DGC LOG: %d ", int_array[i]);
    // }

    // log_debug(gc)("DGC LOG: Double array1: ");
    // for (int i = 0; i < coor_ccmt_num; i++) {
    //     log_debug(gc)("DGC LOG: %.1f ", double_array1[i]);
    // }

    // log_debug(gc)("DGC LOG: Double array2: ");
    // for (int i = 0; i < coor_ccmt_num; i++) {
    //     log_debug(gc)("DGC LOG: %.1f ", double_array2[i]);
    // }

    // Adaptive override: in adaptive mode, drop liveness slope (a=0) and
    // initialize b to SnicCoorAdaptiveInitB (default 500ms — high enough
    // that EWMA below converges downward, not stuck above the cliff).
    if (SnicCoorAdaptiveCCMT) {
        for (int i = 0; i < coor_ccmt_num; i++) {
            double_array1[i] = 0.0;
            double_array2[i] = (double)SnicCoorAdaptiveInitB;
        }
        log_info(gc)("DGC LOG: ADAPTIVE init — coor_ccmt_a=0 coor_ccmt_b=%lu (SnicCoorAdaptiveInitB) for all %d configs", SnicCoorAdaptiveInitB, coor_ccmt_num);
    }

    if(SnicGCShareMemEnabled){
      Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].coor_ccmt_num = coor_ccmt_num;
      for (int i = 0; i < coor_ccmt_num; i++) {
        Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].coor_ccmt_R[i]), (unsigned long long)int_array[i]);
        Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].coor_ccmt_a[i]), (double)double_array1[i]);
        Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].coor_ccmt_b[i]), (double)double_array2[i]);
      }
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[Universe::get_CoorHostId()].backend_connected), (unsigned long long)1);
    }
    else{
      Universe::get_hostPrivateGlobalPacerData()->coor_ccmt_num = coor_ccmt_num;
      for (int i = 0; i < coor_ccmt_num; i++) {
        Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->coor_ccmt_R[i]), (unsigned long long)int_array[i]);
        Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->coor_ccmt_a[i]), (double)double_array1[i]);
        Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->coor_ccmt_b[i]), (double)double_array2[i]);
      }
      Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->backend_connected), (unsigned long long)1);
    }
    // init coor args

  }
}

void Universe::coor_set_host_GC_state(Universe::HostGCState state){
  if (state == Universe::HostGCState::Marking) {
    _last_cycle_was_dgc = true;
  } else if (state == Universe::HostGCState::FallbackMarking) {
    _last_cycle_was_dgc = false;
  }
  int hostId = Universe::get_CoorHostId();
  if(SnicGCShareMemEnabled){
    if(state == Universe::HostGCState::Compacting){
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].enter_compaction_timestamp), (unsigned long long)os::javaTimeMillis());
    }
    if(state == Universe::HostGCState::FallbackMarking){
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].enter_fallback_timestamp), (unsigned long long)os::javaTimeMillis());
    }
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].data_timestamp), (unsigned long long)os::javaTimeMillis());
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].host_GC_state), (unsigned long long)(state));
  }
  else{
    if(state == Universe::HostGCState::Compacting){
      Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->enter_compaction_timestamp), (unsigned long long)os::javaTimeMillis());
    }
    if(state == Universe::HostGCState::FallbackMarking){
      Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->enter_fallback_timestamp), (unsigned long long)os::javaTimeMillis());
    }
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->data_timestamp), (unsigned long long)os::javaTimeMillis());
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->host_GC_state), (unsigned long long)(state));
  }
}

// void Universe::coor_set_GCfinish_host(){
//   int hostId = Universe::get_CoorHostId();
//   if(SnicGCShareMemEnabled){
//     Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].data_timestamp), (unsigned long long)os::javaTimeMillis());
//     Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].host_GC_state), (unsigned long long)(0));
//   }
//   else{
//     Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->data_timestamp), (unsigned long long)os::javaTimeMillis());
//     Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->host_GC_state), (unsigned long long)(0));
//   }
// }

void Universe::coor_unset_forceGC(){
  int hostId = Universe::get_CoorHostId();
  if(SnicGCShareMemEnabled){
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].forceGCType), (unsigned long long)(Universe::ForceGCTypes::EMPTY));
  }
  else{
    ShenandoahHeap::heap()->_rdma_force_gc_data->forceGCType = (unsigned long long)Universe::ForceGCTypes::EMPTY;
  }
}

void Universe::coor_set_CCMT(unsigned long long CCMT){
  int hostId = Universe::get_CoorHostId();
  if(SnicGCShareMemEnabled){
    Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].dgc_ccmt), (unsigned long long)(CCMT));
  }
  else{
    Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->dgc_ccmt), (unsigned long long)(CCMT));
  }
}

void Universe::update_coor_GCId(long long GCId){
    if(SnicGCCoorHeuristic){
    int hostId = Universe::get_CoorHostId();
    if (GCId != (unsigned int)(-1)) {
      if (SnicGCShareMemEnabled) {
        Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].client_gc_id), (long long)(GCId));
      } else {
        Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->client_gc_id), (long long)(GCId));
      }
    }
  }
}

unsigned long long Universe::get_jvm_start_time() {
  return _jvm_start_time;
}

bool Universe::get_start_rdma_prefetch() {
  return _start_rdma_prefetch;
}

void Universe::set_start_rdma_prefetch(bool start) {
  _start_rdma_prefetch = start;
}

bool Universe::get_during_rdma_prefetch() {
  return _during_rdma_prefetch;
}

void Universe::set_during_rdma_prefetch(bool during) {
  _during_rdma_prefetch = during;
}

unsigned long long Universe::get_estimated_rdma_copy_time() {
  return _estimated_rdma_copy_time;
}

void Universe::set_estimated_rdma_copy_time(unsigned long long estimated_rdma_copy_time) {
  _estimated_rdma_copy_time = estimated_rdma_copy_time;
}

void Universe::try_record_host_GC_interval(unsigned long long next_gc_ddl){
  // next_gc_ddl = SnicGCIntervalUnderEstimation;
  int hostId = Universe::get_CoorHostId();
  unsigned long long current_state;
  if(SnicGCShareMemEnabled){
    current_state = Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].host_GC_state;
  }
  else{
    current_state = Universe::get_hostPrivateGlobalPacerData()->host_GC_state;
  }
  if(current_state == Universe::HostGCState::Compacting){
    gc_interval_history->add((double)next_gc_ddl);
    auto avg_gc_interval = gc_interval_history->davg();
    // auto dsd_gc_interval = gc_interval_history->dsd();
    auto gc_interval_prediction = avg_gc_interval; // + dsd_gc_interval * ShenandoahAdaptiveInitialConfidence;
    log_debug(gc)("DGC LOG: gc_interval_prediction: %lld (avg: %lld, dsd: %lld)", (unsigned long long)gc_interval_prediction);
    if(SnicGCShareMemEnabled){
      Atomic::store(&(Universe::get_hostShareGlobalPacerData()->hosts_global_pacer_data[hostId].host_GC_interval), (unsigned long long)gc_interval_prediction);
    }
    else{
      Atomic::store(&(Universe::get_hostPrivateGlobalPacerData()->host_GC_interval), (unsigned long long)gc_interval_prediction);
    }
  }

}
