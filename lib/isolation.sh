#!/bin/bash
# isolation.sh — Resource isolation for concurrent test runs
#
# SHM paths: the JDK uses shm_open() which takes a NAME (not a full path).
# The name should start with "/" and NOT contain additional slashes.
# shm_open("/foo") → creates /dev/shm/foo
#
# For isolation between concurrent profiles, we prefix the name with the profile.
# Default prefix is empty (matching old script behavior: /share_heap_0, etc.)

# Get the SHM name prefix for isolation
# Default (no profile) → empty prefix (compatible with old scripts)
# With profile → "brA_" prefix
_shm_name_prefix() {
    if [ -n "${SHM_PREFIX:-}" ] && [ "${SHM_PREFIX}" != "default" ]; then
        echo "${SHM_PREFIX}_"
    fi
}

# SHM name functions — return names for shm_open(), e.g. "/share_heap_0"
shm_path_heap()    { echo "/$(_shm_name_prefix)share_heap_$1"; }
shm_path_roots()   { echo "/$(_shm_name_prefix)share_roots_$1"; }
shm_path_pacer()   { echo "/$(_shm_name_prefix)share_global_pacer"; }
shm_path_tams()    { echo "/$(_shm_name_prefix)share_region_tams_$1"; }
shm_path_bitmap()  { echo "/$(_shm_name_prefix)share_bitmap_$1"; }
shm_path_liveness(){ echo "/$(_shm_name_prefix)share_liveness_$1"; }
shm_path_vnode()   { echo "/$(_shm_name_prefix)virtual_node_$1"; }
shm_path_coor()    { echo "/$(_shm_name_prefix)coor_heuristic"; }

# Build the SHM JVM flags for a DGC host or client
#
# SnicGCGlobalPacer: the pacer also owns the lifecycle of the SHM files
# listed below, including sizing (ftruncate) for the pacer path itself. If
# the path is passed but the pacer is disabled, the host mmap's a 0-byte
# backing and the JVM hits SIGBUS BUS_ADRERR in universe_init().
# Legacy fig6 shm_run.sh kept pacer ON; dgc-shm-campaign.sh sidesteps by
# dropping the path + using lock-free marking. We follow the legacy path
# so multi-host coordinator mode keeps working.
build_shm_flags_host() {
    local host_id=$1
    echo "-XX:SnicShmMemPath=$(shm_path_heap $host_id) \
-XX:SnicShmRootsPath=$(shm_path_roots $host_id) \
-XX:SnicShmGlobalPacerPath=$(shm_path_pacer) \
-XX:SnicShmRegionTamsPath=$(shm_path_tams $host_id) \
-XX:SnicShmBitmapPath=$(shm_path_bitmap $host_id) \
-XX:SnicShmLivenessPath=$(shm_path_liveness $host_id) \
-XX:SnicShmVirtualNodePath=$(shm_path_vnode $host_id) \
-XX:+SnicGCGlobalPacer"
}

build_shm_flags_client() {
    local host_id=$1
    build_shm_flags_host "$host_id"
}

# Get an isolated working directory
get_workdir() {
    local run_dir="$1"
    local component="${2:-default}"
    local dir="${run_dir}/tmp/${component}"
    mkdir -p "$dir"
    echo "$dir"
}

# Cleanup SHM files for this profile
cleanup_isolated() {
    local prefix="$(_shm_name_prefix)"
    rm -f /dev/shm/${prefix}share_* /dev/shm/${prefix}coor_* /dev/shm/${prefix}virtual_node_* 2>/dev/null || true
}
