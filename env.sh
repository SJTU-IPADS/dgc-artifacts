#!/bin/bash
# env.sh — DGC test framework environment configuration
#
# Loading order (later overrides earlier):
#   1. Defaults (below)
#   2. Machine-level:  env.d/$(hostname -s).env
#   3. User-level:     env.d/$(whoami).env
#   4. Profile-level:  env.d/profiles/${DGC_PROFILE}.env  (set via --profile)
#   5. CLI overrides:  --jdk, --throttle, etc.
#
# All variables use the ": ${VAR:=default}" pattern so they can be
# pre-set by the caller or by earlier config layers.

# Group-writable umask so multi-evaluator setups (one shared $AE_DIR with
# setgid + group-writable mode) can mkdir into each other's adjacent
# subtrees. Without this, results/<userA>/ created at umask 0022 (mode 0755)
# would block <userB> in the same group from creating results/<userB>/.
umask 0002

_AE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ============================================================
# Layer 0: Defaults
# ============================================================
# Dependencies (prebuilt DGC JDK + benchmark suites) are expected to live
# as siblings of the artifact root, so the same shared install can serve
# every evaluator without per-user copies. Override per host / per user
# via env.d/ if a host stores them somewhere else.

# --- JDK ---
: "${DGC_JDK:=${_AE_DIR}/../jdk17-snic-gc-prebuilt/jdk}"

# --- Benchmark suites ---
: "${DACAPO_DIR:=${_AE_DIR}/../dacapo-test/dacapo}"
: "${HBASE_DIR:=${_AE_DIR}/../hbase-test/rdma-multi-regionserver-hbase}"
: "${YCSB_DIR:=${_AE_DIR}/../hbase-test/ycsb-0.18.0}"
: "${SPECJBB_DIR:=${_AE_DIR}/../specjbb-1.0.4}"

# --- Network (DGC data plane) ---
# Loopback default works for the SHM transport (default GC mode). For the
# RDMA transport (dgc-rdma), the evaluator must point these at the IB
# interface IP via env.d/$(hostname -s).env — see env.d/ds00.env for an
# example. Loopback is intentionally the AE default so a fresh checkout
# does not silently reach for an SJTU lab IP that does not exist on the
# evaluator's machine.
: "${DGC_ADDR:=127.0.0.1}"
: "${DGC_HOST_ADDR:=127.0.0.1}"

# --- Common JVM flags ---
: "${COMMON_FLAGS:=-XX:MaxMetaspaceSize=4096m -XX:MetaspaceSize=1024m -XX:-ClassUnloading -XX:+UseCompressedOops -XX:+UseCompressedClassPointers}"

# --- Output ---
# Results go to results/${USER}/ so multiple evaluators sharing the same
# artifact tree stay isolated from each other. Untagged runs land in the
# adhoc/ subdir; per-figure drivers override RESULTS_BASE to their own
# figX-result subdir under the same per-user root.
: "${RESULTS_BASE:=${_AE_DIR}/results/${USER:-anonymous}/adhoc}"

# ============================================================
# Layer 1: Machine-level (auto-detected by hostname)
# ============================================================
_host_env="${_AE_DIR}/env.d/$(hostname -s).env"
if [ -f "$_host_env" ]; then
    source "$_host_env"
fi

# ============================================================
# Layer 2: User-level (auto-detected by whoami)
# ============================================================
_user_env="${_AE_DIR}/env.d/$(whoami).env"
if [ -f "$_user_env" ]; then
    source "$_user_env"
fi

# ============================================================
# Layer 3: Profile-level (set via --profile or DGC_PROFILE env)
# ============================================================
if [ -n "${DGC_PROFILE:-}" ]; then
    _profile_env="${_AE_DIR}/env.d/profiles/${DGC_PROFILE}.env"
    if [ -f "$_profile_env" ]; then
        source "$_profile_env"
    else
        echo "WARNING: profile '${DGC_PROFILE}' not found at ${_profile_env}" >&2
    fi
fi

# ============================================================
# Derived paths (computed after all layers are loaded)
# ============================================================
JAVA="${DGC_JDK}/bin/java"

# JDK source repo: walk up from the JDK image to find the git root
# Path: .../jdk17-snic-gc/build/linux-x86_64-server-release/images/jdk
#       → 4 levels up to reach the git repo root
_jdk_repo_dir=""
for _up in "/../../../.." "/../../.."; do
    _try="$(cd "${DGC_JDK}${_up}" 2>/dev/null && pwd || echo "")"
    if [ -e "${_try}/.git" ] 2>/dev/null; then
        _jdk_repo_dir="$_try"
        break
    fi
done
: "${DGC_JDK_REPO:=${_jdk_repo_dir}}"

# Shared memory prefix for isolation. Default order:
#   --profile <name>  → uses that profile's namespace
#   else              → per-user namespace (${USER}_share_*) so two users running
#                       AE concurrently do not collide on /dev/shm or accidentally
#                       reuse each other's stale GC SHM segments.
#   last fallback     → "default" (resolved to empty prefix in lib/isolation.sh,
#                       used for legacy single-user setups).
: "${SHM_PREFIX:=${DGC_PROFILE:-${USER:-default}}}"
