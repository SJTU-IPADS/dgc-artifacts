#!/bin/bash
# metadata.sh — Collect and record run metadata

# Collect JDK info: version, commit hash, branch, dirty status
collect_jdk_info() {
    local jdk_path="$1"
    local out_file="$2"

    local java="${jdk_path}/bin/java"
    local version
    version=$("$java" -version 2>&1 | head -1) || version="unknown"

    # Try to find the git repo for this JDK build
    local repo_dir="${DGC_JDK_REPO:-}"
    local commit="unknown" commit_short="unknown" subject="unknown"
    local branch="unknown" dirty="unknown" build_ts="unknown"

    if [ -e "${repo_dir}/.git" ]; then
        commit=$(git -C "$repo_dir" rev-parse HEAD 2>/dev/null || echo "unknown")
        commit_short="${commit:0:12}"
        branch=$(git -C "$repo_dir" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
        subject=$(git -C "$repo_dir" log -1 --format='%s' 2>/dev/null || echo "unknown")
        # Escape characters that would break the raw JSON embedding below.
        subject=${subject//\\/\\\\}
        subject=${subject//\"/\\\"}
        if git -C "$repo_dir" diff --quiet HEAD 2>/dev/null; then
            dirty="false"
        else
            dirty="true"
        fi
    fi
    build_ts=$(stat -c '%y' "${jdk_path}/lib/server/libjvm.so" 2>/dev/null | cut -d. -f1 || echo "unknown")

    cat > "$out_file" <<EOF
{
  "path": "${jdk_path}",
  "version": "${version}",
  "source_repo": "${repo_dir}",
  "commit": "${commit}",
  "commit_short": "${commit_short}",
  "subject": "${subject}",
  "branch": "${branch}",
  "dirty": ${dirty},
  "libjvm_mtime": "${build_ts}"
}
EOF
}

# Collect system info
collect_system_info() {
    local out_file="$1"
    local hostname kernel cpu_model cores mem_gb
    hostname=$(hostname -f 2>/dev/null || hostname)
    kernel=$(uname -r)
    cpu_model=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs || echo "unknown")
    cores=$(nproc 2>/dev/null || echo "unknown")
    mem_gb=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "unknown")

    cat > "$out_file" <<EOF
{
  "hostname": "${hostname}",
  "kernel": "${kernel}",
  "cpu_model": "${cpu_model}",
  "cores": ${cores},
  "memory_gb": ${mem_gb}
}
EOF
}

# Write the full META.json for a run
write_meta_json() {
    local run_dir="$1"
    local run_id="$2"
    local workload="$3"
    local gc="$4"

    mkdir -p "$run_dir"

    # Collect sub-files
    collect_jdk_info "$DGC_JDK" "${run_dir}/.jdk_info.json"
    collect_system_info "${run_dir}/.system_info.json"

    local start_time
    start_time=$(date -Iseconds)
    local user
    user=$(whoami)
    local profile="${DGC_PROFILE:-none}"

    # Build META.json
    cat > "${run_dir}/META.json" <<EOF
{
  "run_id": "${run_id}",
  "user": "${user}",
  "profile": "${profile}",
  "start_time": "${start_time}",
  "end_time": null,
  "workload": "${workload}",
  "gc": "${gc}",
  "jdk": $(cat "${run_dir}/.jdk_info.json"),
  "system": $(cat "${run_dir}/.system_info.json"),
  "config": {},
  "status": "running"
}
EOF

    # Snapshot all env vars used
    env | sort > "${run_dir}/config.env"

    # Cleanup temp files
    rm -f "${run_dir}/.jdk_info.json" "${run_dir}/.system_info.json"
}

# Update META.json at test completion
finalize_meta_json() {
    local run_dir="$1"
    local status="${2:-completed}"
    local end_time
    end_time=$(date -Iseconds)

    if [ -f "${run_dir}/META.json" ]; then
        # Update end_time and status using sed (no jq dependency)
        sed -i "s/\"end_time\": null/\"end_time\": \"${end_time}\"/" "${run_dir}/META.json"
        sed -i "s/\"status\": \"running\"/\"status\": \"${status}\"/" "${run_dir}/META.json"
    fi
}
