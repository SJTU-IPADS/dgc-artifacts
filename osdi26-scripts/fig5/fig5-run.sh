#!/bin/bash
# fig5-run.sh — Figure 5: DGC-SHM SPECjbb latency-details test
#
# Replays the archive's fig5-dgc-shm-latency-details-test on the new
# osdi26-ae framework. Goal: capture per-request latency + GC events
# from a steady-state DGC-SHM SPECjbb PRESET run, so the plotter can
# show how DGC marking phases overlap with mutator latency without the
# pause penalty visible to the application.
#
# Pipeline:
#   1. Override specjbb-preset.conf to use the latency-instrumented JAR
#      (specjbb-output-latency-with-start.jar) which appends each
#      request's <start_us>,<latency_us>,<req_type> to ./latency.txt.
#   2. Drive ./run.sh with --tag fig5-<timestamp>.
#   3. After the run, salvage ${SPECJBB_DIR}/latency.txt into the run
#      directory (the bench writes it relative to its CWD).
#
# The archive used 1 backend group (HOST_NUM=1); we keep the same so the
# resulting plot matches the paper's fig5 layout (5 GC cycles per panel).
#
# Usage:
#   ./osdi26-scripts/fig5/fig5-run.sh                     # default: 1 group, IR=6819
#   BACKEND_GROUPS=1 IR=6819 DURATION=120000 ./osdi26-scripts/fig5/fig5-run.sh
#   ./osdi26-scripts/fig5/fig5-run.sh --tag rep1          # custom tag suffix
#
# Environment overrides:
#   BACKEND_GROUPS         number of backend groups (default: 1, matches archive)
#   IR             injection rate per backend (default: from specjbb-preset.conf, 6819)
#   DURATION       PRESET duration ms (default: 120000 — long enough for ≥10 GCs)
#   RESULTS_BASE   override results dir (default: ${AE_DIR}/results/${USER}/fig5-result)
#   GC_LIST        space-separated GCs to run (default: dgc-shm only — fig5 is shm-specific)
#   REPS           rep ids (default: 1) — fig5 typically only needs 1 successful rep

set -euo pipefail
umask 0002          # group-writable creates so co-evaluators in same group can interleave
cd "$(dirname "$0")/../.."

AE_DIR="$(pwd)"

CLI_TAG=""
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) CLI_TAG="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

BACKEND_GROUPS="${BACKEND_GROUPS:-1}"
IR="${IR:-6819}"
DURATION="${DURATION:-120000}"
GC_LIST="${GC_LIST:-dgc-shm}"
REPS="${REPS:-1}"

# Default results destination = per-user subdir under the artifact's results/.
export RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/fig5-result}"
mkdir -p "$RESULTS_BASE"

TS=$(date +%Y%m%d_%H%M%S)
TAG="${CLI_TAG:-fig5-${TS}}"

# Source specjbb-preset.conf to learn SPECJBB_DIR + latency JAR path.
# The conf is sourced in a subshell to keep our env clean.
SPECJBB_DIR_RESOLVED=$(
    set -e
    source "${AE_DIR}/env.sh" 2>/dev/null || true
    source "${AE_DIR}/conf/workloads/specjbb-preset.conf"
    echo "${SPECJBB_DIR}"
)
LATENCY_JAR="${SPECJBB_DIR_RESOLVED}/specjbb-output-latency-with-start.jar"
DEFAULT_JAR="${SPECJBB_DIR_RESOLVED}/specjbb2015.jar"

if [ ! -f "$LATENCY_JAR" ]; then
    echo "ERROR: latency JAR not found at ${LATENCY_JAR}" >&2
    echo "Build it from the SPECjbb sources or copy it into ${SPECJBB_DIR_RESOLVED}/" >&2
    exit 1
fi

# Temporarily rewrite the JAR path in specjbb-preset.conf so workload-specjbb.sh
# resolves BENCH_JAR_dgc to the latency JAR. Env vars don't override conf
# values, so we have to edit the file in place and restore afterwards.
# A trap ensures restore runs even on test failure.
CONF="${AE_DIR}/conf/workloads/specjbb-preset.conf"
CONF_BACKUP=$(mktemp)
# Avoid `cp -p` here: when a co-evaluator (different uid, same group) runs
# fig5-run.sh, restoring with -p tries to chown/chtime a file the running
# user does not own → cp prints a non-fatal warning. We only need content
# preservation; mode/owner stay correct because the conf already has the
# right group + perms.
cat "$CONF" > "$CONF_BACKUP"

restore_conf() {
    cat "$CONF_BACKUP" > "$CONF"
    rm -f "$CONF_BACKUP"
}
trap restore_conf EXIT

# Inject fig5-specific overrides as a "FIG5 OVERRIDE" block at end of conf.
# (Appending is safer than sed-replacing existing keys, which may not exist.)
{
    echo ""
    echo "# === FIG5 OVERRIDE (added by fig5-run.sh; restored on exit) ==="
    echo "BENCH_JAR_dgc=\"${LATENCY_JAR}\""
    echo "BENCH_JAR_baseline=\"${LATENCY_JAR}\""
    echo "BENCH_JAR_default=\"${LATENCY_JAR}\""
    echo "SPECJBB_GROUPS=${BACKEND_GROUPS}"
    echo "SPECJBB_IR=${IR}"
    echo "SPECJBB_DURATION=${DURATION}"
} >> "$CONF"

echo "==============================================="
echo "  fig5 SPECjbb DGC-SHM sweep starting at $(date)"
echo "  Tag:        ${TAG}"
echo "  GCs:        ${GC_LIST}"
echo "  Reps:       ${REPS}"
echo "  Groups:     ${BACKEND_GROUPS}    IR=${IR}    Duration=${DURATION}ms"
echo "  Latency JAR: ${LATENCY_JAR}"
echo "  Results:    ${RESULTS_BASE}/<run-id>"
echo "==============================================="

run_one() {
    local rep="$1" gc="$2"
    local rep_tag="${TAG}-rep${rep}"

    echo ""
    echo "----- [$(date +%H:%M:%S)] REP=${rep}  specjbb-preset  ${gc} -----"

    # Clear any old latency.txt before the run so we know the post-run file is fresh.
    rm -f "${SPECJBB_DIR_RESOLVED}/latency.txt"

    # Invoke the shared run.sh; it will pick up the conf overrides above.
    ./run.sh specjbb-preset "$gc" --tag "$rep_tag"

    # Salvage latency.txt into the run dir. run.sh does not know about it.
    local run_dir
    run_dir=$(ls -dt "${RESULTS_BASE}"/*_specjbb-preset_"${gc}"_"${rep_tag}"* 2>/dev/null | head -1)
    if [ -z "$run_dir" ]; then
        echo "[warn] could not locate run dir for rep=${rep} gc=${gc}" >&2
        return
    fi
    if [ -f "${SPECJBB_DIR_RESOLVED}/latency.txt" ]; then
        cp "${SPECJBB_DIR_RESOLVED}/latency.txt" "${run_dir}/raw/specjbb_latency.txt"
        echo "[ok] salvaged latency.txt → ${run_dir}/raw/specjbb_latency.txt"
    else
        echo "[warn] ${SPECJBB_DIR_RESOLVED}/latency.txt missing — was the latency JAR loaded?" >&2
    fi
}

for rep in $REPS; do
    for gc in $GC_LIST; do
        run_one "$rep" "$gc"
    done
done

echo ""
echo "==============================================="
echo "  fig5 sweep finished at $(date)"
echo "  Tag: ${TAG}"
echo "  Run analysis:  ./osdi26-scripts/fig5/analyze-fig5.sh --tag ${TAG}"
echo "==============================================="
