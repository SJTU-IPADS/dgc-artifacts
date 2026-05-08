#!/bin/bash
# table3-analyze.sh — extract p99 from a Table 3 run to a CSV under plot/
#
# Walks the 3 GC subdirs of a Table 3 run tag (shenandoah / dgc-shm / dgc-rdma),
# computes:
#   - YCSB READ/UPDATE p50/p95/p99 from raw latency files
#   - SPECjbb steady-state p99 from latency.txt (windowed, skipping warmup)
#   - degen counts from each backend / regionserver GC log
# and writes a single tidy CSV at plot/table3-data/p99.csv.
#
# Usage:
#   ./osdi26-scripts/table3/table3-analyze.sh                       # auto-pick newest tag
#   ./osdi26-scripts/table3/table3-analyze.sh table3-20260429_xxxx  # specific tag
#   ./osdi26-scripts/table3/table3-analyze.sh /path/to/table3-result/table3-...
#   RESULTS_BASE=/path/to/table3-result ./table3-analyze.sh
#
# Output:
#   plot/table3-data/p99.csv  — one row per GC

set -uo pipefail
cd "$(dirname "$0")/../.."
AE_DIR="$(pwd)"

RESULTS_BASE="${RESULTS_BASE:-${AE_DIR}/results/${USER}/table3-result}"
PLOT_DIR="${AE_DIR}/plot/table3-data"
CSV="${PLOT_DIR}/p99.csv"

mkdir -p "$PLOT_DIR"

# Resolve the run dir
RUN_ARG="${1:-}"
if [ -z "$RUN_ARG" ]; then
    # newest table3-* tag under RESULTS_BASE
    RUN_DIR=$(ls -1dt "${RESULTS_BASE}"/table3-* 2>/dev/null | head -1)
elif [ -d "$RUN_ARG" ]; then
    RUN_DIR="$RUN_ARG"
elif [ -d "${RESULTS_BASE}/$RUN_ARG" ]; then
    RUN_DIR="${RESULTS_BASE}/$RUN_ARG"
else
    echo "!! cannot resolve run: $RUN_ARG" >&2
    exit 1
fi

if [ -z "$RUN_DIR" ] || [ ! -d "$RUN_DIR" ]; then
    echo "!! no run dir under $RESULTS_BASE; pass a tag or path" >&2
    exit 1
fi

echo "Analyzing run: $RUN_DIR"

# extract_ycsb_pXX <latency.txt> <op> <pXX> -> us
extract_ycsb() {
    local file="$1"
    [ ! -f "$file" ] && return
    awk -F',' -v op="READ" -v p50=0 -v p95=0 -v p99=0 -v median=0 '
    $1 == op {a[++n]=$3}
    END {
        if (n==0) { print "0,0,0,0"; exit }
        asort(a)
        printf "%d,%d,%d,%d\n", n, a[int(n*0.5)], a[int(n*0.95)], a[int(n*0.99)]
    }
    ' "$file"
}

# extract_specjbb_p99 <latency.txt> -> windowed p99 (skip first 10s ramp settle)
extract_specjbb_p99() {
    local file="$1"
    [ ! -f "$file" ] && { echo "0,0"; return; }
    # latency.txt format: start_us, latency_us, success
    awk -F',' '
    NR==1 { start=$1 }
    {
        bucket = int(($1-start)/1e7)*10
        if (bucket >= 10) {
            v[++n] = $2
            sum += $2
        }
    }
    END {
        if (n==0) { print "0,0"; exit }
        asort(v)
        printf "%d,%d\n", n, v[int(n*0.99)]
    }
    ' "$file"
}

# count_degens <gc_log>
# grep -c always prints a count; suppress its non-zero exit on "0 matches".
count_degens() {
    local f="$1"
    [ ! -f "$f" ] && { echo 0; return; }
    local n
    n=$(grep -cE 'Pause Degenerated GC' "$f" 2>/dev/null)
    echo "${n:-0}"
}

# count_marking_cycles <gc_log>
count_marking() {
    local f="$1"
    [ ! -f "$f" ] && { echo 0; return; }
    local n
    n=$(grep -cE 'Concurrent marking [0-9.]+ms' "$f" 2>/dev/null)
    echo "${n:-0}"
}

# Resolve YCSB latency file in a GC dir (shenandoah uses BaselineHost suffix,
# DGC uses SNICHost).
find_ycsb_file() {
    local gc_dir="$1"
    find -L "$gc_dir" -maxdepth 8 -name '*ycsb_latency_*Host_*.txt' 2>/dev/null | head -1
}

# Resolve specjbb latency.txt (per-backend cwd or combined). The combined one
# is the controller-aggregated file (works for the multi-backend case).
find_specjbb_latency() {
    local gc_dir="$1"
    local f
    f=$(find -L "$gc_dir" -maxdepth 8 -name 'specjbb_latency_combined.txt' 2>/dev/null | head -1)
    if [ -n "$f" ] && [ -f "$f" ]; then echo "$f"; return; fi
    f=$(find -L "$gc_dir" -maxdepth 10 -name 'latency.txt' 2>/dev/null | head -1)
    [ -n "$f" ] && echo "$f"
}

find_backend_log() {
    local gc_dir="$1" idx="$2"
    find -L "$gc_dir" -maxdepth 8 -name "*specjbb_backend_*_${idx}.txt" 2>/dev/null | head -1
}
find_regionserver_log() {
    local gc_dir="$1" idx="$2"
    find -L "$gc_dir" -maxdepth 8 -name "*hbase_regionserver_*_${idx}.txt" 2>/dev/null | head -1
}

# Header
{
    echo "gc,run_tag,ycsb_read_n,ycsb_read_p50_us,ycsb_read_p95_us,ycsb_read_p99_us,ycsb_update_n,ycsb_update_p50_us,ycsb_update_p95_us,ycsb_update_p99_us,specjbb_n,specjbb_p99_us,backend0_marking,backend0_degens,backend1_marking,backend1_degens,rs0_marking,rs0_degens,rs1_marking,rs1_degens"
} > "$CSV"

run_tag=$(basename "$RUN_DIR")

for gc in shenandoah dgc-shm dgc-rdma; do
    gc_dir="${RUN_DIR}/${gc}"
    if [ ! -d "$gc_dir" ]; then
        echo "  [skip $gc] no $gc_dir"
        continue
    fi
    echo "  [$gc] scanning $gc_dir"

    ycsb=$(find_ycsb_file "$gc_dir")
    if [ -z "$ycsb" ]; then
        echo "    !! no ycsb_latency file"
        continue
    fi

    # YCSB READ p50/p95/p99
    read_csv=$(awk -F',' '
        $1 == "READ" { a[++n] = $3 }
        END {
            if (n == 0) { print "0,0,0,0"; exit }
            asort(a)
            printf "%d,%d,%d,%d\n", n, a[int(n*0.5)], a[int(n*0.95)], a[int(n*0.99)]
        }' "$ycsb")
    update_csv=$(awk -F',' '
        $1 == "UPDATE" { a[++n] = $3 }
        END {
            if (n == 0) { print "0,0,0,0"; exit }
            asort(a)
            printf "%d,%d,%d,%d\n", n, a[int(n*0.5)], a[int(n*0.95)], a[int(n*0.99)]
        }' "$ycsb")

    # SPECjbb p99 (windowed: skip first 10s)
    spec_lat=$(find_specjbb_latency "$gc_dir")
    if [ -n "$spec_lat" ]; then
        spec_csv=$(extract_specjbb_p99 "$spec_lat")
    else
        spec_csv="0,0"
    fi

    # Per-backend / per-regionserver counts
    b0=$(find_backend_log "$gc_dir" 0)
    b1=$(find_backend_log "$gc_dir" 1)
    rs0=$(find_regionserver_log "$gc_dir" 0)
    rs1=$(find_regionserver_log "$gc_dir" 1)

    b0_marking=$(count_marking "$b0")
    b0_deg=$(count_degens "$b0")
    b1_marking=$(count_marking "$b1")
    b1_deg=$(count_degens "$b1")
    rs0_marking=$(count_marking "$rs0")
    rs0_deg=$(count_degens "$rs0")
    rs1_marking=$(count_marking "$rs1")
    rs1_deg=$(count_degens "$rs1")

    echo "${gc},${run_tag},${read_csv},${update_csv},${spec_csv},${b0_marking},${b0_deg},${b1_marking},${b1_deg},${rs0_marking},${rs0_deg},${rs1_marking},${rs1_deg}" >> "$CSV"

    # Echo human-readable summary
    read_p99=$(echo "$read_csv" | cut -d, -f4)
    update_p99=$(echo "$update_csv" | cut -d, -f4)
    spec_p99=$(echo "$spec_csv" | cut -d, -f2)
    echo "    YCSB READ p99 = ${read_p99} us"
    echo "    YCSB UPDATE p99 = ${update_p99} us"
    echo "    SPECjbb steady p99 = ${spec_p99} us"
    echo "    Backend degens: ${b0_deg} / ${b1_deg}; RS degens: ${rs0_deg} / ${rs1_deg}"
done

echo ""
echo "Wrote: $CSV"
column -t -s, < "$CSV" | head -20
