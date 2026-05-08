#!/bin/bash
# analyze-specjbb.sh — Analyze SPECjbb PRESET steady-state GC behavior
#
# Finds the measurement window (second IR=target → IR=0), then reports:
#   - Steady-state time range
#   - GC count by type (normal, degen, full)
#   - Degen breakdown by phase (Outside of Cycle, Update Refs, Evacuation, ...)
#   - What GC type preceded each degen
#   - Average marking duration (DGC vs fallback, via coordinator)
#   - Coordinator INFEASIBLE/OPTIMAL ratio
#
# Usage:
#   ./lib/analyze-specjbb.sh <result_dir> [backend_id]
#   backend_id defaults to 0

set -euo pipefail

RUN_DIR="${1:?Usage: analyze-specjbb.sh <result_dir> [backend_id]}"
BE="${2:-0}"

LOG="$RUN_DIR/logs"
GC="$LOG/gc_backend_${BE}.log"
BACKEND="$LOG/backend_${BE}.log"
COOR="$LOG/coordinator.log"

if [ ! -f "$GC" ] || [ ! -f "$BACKEND" ]; then
    echo "ERROR: Missing gc_backend_${BE}.log or backend_${BE}.log in $LOG" >&2
    exit 1
fi

# ============================================================
# 1. Find steady-state window
# ============================================================

# SPECjbb PRESET: find IR=<target> lines. The second "→ Running (IR=<target>)"
# after an "IR=0.0" is the measurement start. The next "IR=0.0" is the end.
# Pattern: IR=0 → IR=target (measurement) → IR=0

TARGET_IR=$(grep -oP 'IR=\K[0-9.]+' "$BACKEND" | grep -v '^0\.0$' | sort | uniq -c | sort -rn | head -1 | awk '{print $2}')
if [ -z "$TARGET_IR" ]; then
    echo "ERROR: Cannot determine target IR from $BACKEND" >&2
    exit 1
fi

# Find: after the first IR=0.0 → IR=target → IR=0.0 cycle, the second IR=0→IR=target is measurement start
MEASURE_START=""
MEASURE_END=""
_state="init"
while IFS= read -r line; do
    ts=$(echo "$line" | grep -oP '^\<[^>]+\>' | head -1)
    ir=$(echo "$line" | grep -oP 'IR=\K[0-9.]+' | tail -1)
    [ -z "$ir" ] && continue

    case "$_state" in
        init)
            # Wait for first target IR (warmup start)
            if [ "$ir" = "$TARGET_IR" ]; then _state="warmup_running"; fi
            ;;
        warmup_running)
            # Wait for IR=0 (warmup end)
            if [ "$ir" = "0.0" ]; then _state="warmup_done"; fi
            ;;
        warmup_done)
            # Next IR=target is measurement start
            if [ "$ir" = "$TARGET_IR" ]; then
                MEASURE_START="$ts"
                _state="measuring"
            fi
            ;;
        measuring)
            # Next IR=0 is measurement end
            if [ "$ir" = "0.0" ]; then
                MEASURE_END="$ts"
                break
            fi
            ;;
    esac
done < <(grep "NEW STATE: Running" "$BACKEND")

if [ -z "$MEASURE_START" ] || [ -z "$MEASURE_END" ]; then
    echo "ERROR: Cannot find measurement window in $BACKEND" >&2
    echo "  Found target IR=${TARGET_IR}" >&2
    exit 1
fi

# Convert wall-clock timestamps to epoch ms
# Format: <Mon Apr 06 00:14:00 CST 2026>
_parse_ts() {
    local raw="$1"
    raw=$(echo "$raw" | sed 's/^<//;s/>$//')
    TZ=Asia/Shanghai date -d "$raw" +%s 2>/dev/null
}

S=$(_parse_ts "$MEASURE_START")000
E=$(_parse_ts "$MEASURE_END")000
DUR=$(( (E - S) / 1000 ))

echo "============================================"
echo "SPECjbb Steady-State Analysis"
echo "============================================"
echo "Result:    $(basename "$RUN_DIR")"
echo "Backend:   ${BE}"
echo "Target IR: ${TARGET_IR}"
echo "Window:    ${MEASURE_START} → ${MEASURE_END}"
echo "           ${S} → ${E} (${DUR}s)"
echo ""

# ============================================================
# 2. GC count and type breakdown
# ============================================================

echo "=== GC Cycles ==="

NORMAL_N=$(awk -F'[][]' -v S="$S" -v E="$E" '
/Concurrent reset [0-9]/ { ts=$2+0; if(ts>=S && ts<=E) n++ }
END { print n+0 }' "$GC")

DEGEN_N=$(awk -F'[][]' -v S="$S" -v E="$E" '
/Pause Degenerated GC \(/ && /M\(/ { ts=$2+0; if(ts>=S && ts<=E) n++ }
END { print n+0 }' "$GC")

FULL_N=$(awk -F'[][]' -v S="$S" -v E="$E" '
/Pause Full GC/ && /M\(/ { ts=$2+0; if(ts>=S && ts<=E) n++ }
END { print n+0 }' "$GC")

TOTAL=$((NORMAL_N + DEGEN_N + FULL_N))
echo "  Normal concurrent: ${NORMAL_N}"
echo "  Degenerated:       ${DEGEN_N}"
echo "  Full:              ${FULL_N}"
echo "  Total:             ${TOTAL}"
if [ "$TOTAL" -gt 0 ]; then
    DEGEN_PCT=$(( DEGEN_N * 100 / TOTAL ))
    echo "  Degen ratio:       ${DEGEN_PCT}%"
fi
echo ""

# ============================================================
# 3. Degen breakdown by phase
# ============================================================

echo "=== Degen Breakdown by Phase ==="
awk -F'[][]' -v S="$S" -v E="$E" '
/Pause Degenerated GC \(/ && /M\(/ {
    ts=$2+0
    if(ts>=S && ts<=E) {
        match($0, /Pause Degenerated GC \(([^)]+)\)/, a)
        phase[a[1]]++
    }
}
END {
    for(p in phase) printf "  %-25s %d\n", p, phase[p]
}' "$GC" | sort -t' ' -k2 -rn
echo ""

# ============================================================
# 4. What preceded each degen
# ============================================================

echo "=== Degen Preceded By ==="
awk -F'[][]' -v S="$S" -v E="$E" '
# Track previous GC type by GC ID
/Concurrent marking [0-9]/ {
    match($0, /GC\(([0-9]+)\)/, g)
    gc_type[g[1]] = "fallback"  # default, overridden below
}
/finish truncate file.*snic_shm_send_roots/ {
    match($0, /GC\(([0-9]+)\)/, g)
    gc_type[g[1]] = "DGC"
}
/LHT LOG: finish SnicGCFallback/ {
    match($0, /GC\(([0-9]+)\)/, g)
    gc_type[g[1]] = "fallback"
}
/Pause Degenerated GC \(/ && /M\(/ {
    ts=$2+0
    if(ts>=S && ts<=E) {
        match($0, /GC\(([0-9]+)\)/, g)
        degen_id = g[1]+0
        prev_id = degen_id - 1
        if(prev_id in gc_type)
            preceded[gc_type[prev_id]]++
        else
            preceded["unknown"]++
    }
}
END {
    for(p in preceded) printf "  after %-15s %d\n", p, preceded[p]
}' "$GC" | sort -t' ' -k3 -rn
echo ""

# ============================================================
# 5. Average marking duration (from GC log)
# ============================================================

echo "=== Marking Duration (from GC log) ==="
awk -F'[][]' -v S="$S" -v E="$E" '
/Concurrent marking [0-9]/ {
    ts=$2+0
    if(ts>=S && ts<=E) {
        match($0, /([0-9]+\.[0-9]+)ms/, a)
        sum+=a[1]; n++
    }
}
END {
    if(n>0) printf "  All marking: avg=%.0fms n=%d\n", sum/n, n
    else print "  No marking data"
}' "$GC"
echo ""

# ============================================================
# 6. Coordinator: DGC vs fallback duration (if coordinator log exists)
# ============================================================

if [ -f "$COOR" ]; then
    echo "=== Coordinator: DGC vs Fallback (host ${BE}) ==="
    awk -F'[][]' -v S="$S" -v E="$E" -v H="$BE" '
    $0 ~ "ddl for host "H {
        ts=$2+0; if(ts<S || ts>E) next
        if(/during marking/) state="DGC"
        else if(/during fallback/) state="FB"
        else if(/during compaction/) state="CMP"
        else if(/IDLE/) state="IDLE"
        else next
        if(state!=prev) {
            if(prev=="DGC" && (state=="CMP"||state=="IDLE")){d=ts-ets; ds+=d; dn++}
            if(prev=="FB" && (state=="CMP"||state=="IDLE")){d=ts-ets; fs+=d; fn++}
            ets=ts; prev=state
        }
    }
    END {
        if(dn>0) printf "  DGC marking:      avg=%dms  n=%d\n", ds/dn, dn
        else print "  DGC marking:      none"
        if(fn>0) printf "  Fallback marking: avg=%dms  n=%d\n", fs/fn, fn
        else print "  Fallback marking: none"
        total=dn+fn
        if(total>0) printf "  DGC ratio:        %d%%\n", dn*100/total
    }' "$COOR"

    echo ""
    echo "=== Coordinator: State Distribution (host ${BE}) ==="
    awk -F'[][]' -v S="$S" -v E="$E" -v H="$BE" '
    $0 ~ "ddl for host "H {
        ts=$2+0; if(ts<S || ts>E) next
        if(/IDLE/) idle++
        else if(/during marking/) dgc++
        else if(/during fallback/) fb++
        else if(/during compaction/) cmp++
        else if(/Frozen/) frz++
    }
    END {
        total=idle+dgc+fb+cmp+frz
        if(total>0) {
            printf "  Idle=%d(%d%%) DGC=%d(%d%%) Fallback=%d(%d%%) Compaction=%d(%d%%) Frozen=%d(%d%%)\n",
                idle, idle*100/total, dgc, dgc*100/total, fb, fb*100/total, cmp, cmp*100/total, frz, frz*100/total
        }
    }' "$COOR"

    echo ""
    echo "=== Coordinator: Solver Stats ==="
    awk -F'[][]' -v S="$S" -v E="$E" '
    /INFEASIBLE/ { ts=$2+0; if(ts>=S && ts<=E) inf++ }
    /OPTIMAL/    { ts=$2+0; if(ts>=S && ts<=E) opt++ }
    END { printf "  OPTIMAL=%d  INFEASIBLE=%d\n", opt+0, inf+0 }' "$COOR"
fi

echo ""
echo "============================================"
