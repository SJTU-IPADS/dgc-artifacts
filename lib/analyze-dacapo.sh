#!/bin/bash
# analyze-dacapo.sh — Analyze DaCapo DGC steady-state marking behavior
#
# Extracts the measurement iteration (last of N iterations), then reports:
#   - Steady-state GC count (total, degen, full)
#   - DGC vs Fallback marking time averages
#   - DGC success ratio
#
# Designed for calibrating COOR_CCMT_ARGS (the coordinator's marking time model).
#
# Usage:
#   ./lib/analyze-dacapo.sh <result_dir> [host_id]
#   host_id defaults to 0
#
# Accepts both framework results (results/<run_id>/) and raw throttle point dirs.
#
# For framework results:
#   ./lib/analyze-dacapo.sh results/20260405_115908_h2_dgc-shm_lht
#   → analyzes all throttle points, prints per-point + summary
#
# For a single throttle point:
#   ./lib/analyze-dacapo.sh results/20260405_115908_h2_dgc-shm_lht/raw/400 0
#
# Output (when used for calibration):
#   CCMT_ARGS suggestion at the end: "0:0:<fallback_avg>;<N>:0:<dgc_avg>"

set -euo pipefail

RUN_DIR="${1:?Usage: analyze-dacapo.sh <result_dir> [host_id]}"
HOST="${2:-0}"

# ============================================================
# Detect input type: framework result dir or single throttle dir
# ============================================================

_analyze_one_point() {
    local host_log="$1"
    local label="${2:-}"
    local gc_log="${3:-}"        # optional separate GC log (for baseline)

    if [ ! -f "$host_log" ]; then
        echo "  [${label}] SKIP: no host log"
        return
    fi

    if ! grep -q "PASSED" "$host_log" 2>/dev/null; then
        echo "  [${label}] SKIP: test did not PASS"
        return
    fi

    # Determine the log that contains GC data.
    # For DGC mode: host_0.log has both DaCapo output and GC log (stdout).
    # For baseline: run.log has DaCapo output, gc_*.log has GC log (separate file).
    local marking_log="$host_log"
    if [ -n "$gc_log" ] && [ -f "$gc_log" ]; then
        marking_log="$gc_log"
    fi

    # Find the last iteration start timestamp from the DaCapo output log.
    local start_ts
    start_ts=$(grep -E "starting$|starting \[" "$host_log" | grep -v "warmup" | tail -1 | grep -oP '\[(\d+)ms\]' | grep -oP '\d+' || true)

    # Fallback: if the "starting" line is split across lines (interleaved with GC log),
    # search for the last non-warmup, non-ccmark "starting" pattern.
    if [ -z "$start_ts" ]; then
        start_ts=$(grep "starting" "$host_log" | grep -v "warmup\|ccmark" | tail -1 | grep -oP '\[(\d+)ms\]' | grep -oP '\d+' || true)
    fi

    # If GC log is separate and has no timestamps matching, use percentage-based fallback:
    # take the last 1/N of GC events (N = iteration count, default 5).
    local use_tail_pct=false
    if [ -z "$start_ts" ] && [ "$marking_log" != "$host_log" ]; then
        use_tail_pct=true
    fi

    if [ -z "$start_ts" ] && [ "$use_tail_pct" = false ]; then
        echo "  [${label}] SKIP: cannot find measurement iteration start"
        return
    fi

    # Extract marking times, separated by DGC vs Fallback.
    # Pattern: "start SnicGCFallback" before "Concurrent marking Xms" → fallback
    # No "start SnicGCFallback" → DGC (remote marking succeeded)
    # For baseline (no fallback markers), all marking is classified as "baseline" → reported as fallback.
    local result
    if [ "$use_tail_pct" = true ]; then
        # Baseline with separate GC log: take last 1/5 of all marking events
        local total_marks
        total_marks=$(grep -c "Concurrent marking [0-9]" "$marking_log" 2>/dev/null || echo 0)
        total_marks=$(echo "$total_marks" | grep -oP '\d+')
        local tail_count=$(( total_marks / 5 ))
        [ "$tail_count" -lt 5 ] && tail_count=5
        result=$(grep -oP 'Concurrent marking (\d+\.\d+)ms' "$marking_log" | grep -oP '\d+\.\d+' | tail -"$tail_count" | awk '
        { sum += $1; n++ }
        END {
            avg = (n > 0) ? sum/n : 0
            printf "fb_avg=%.0f fb_n=%d dgc_avg=0 dgc_n=0 total=%d dgc_pct=0 degen=0 full=0\n", avg, n, n
        }')
    else
        result=$(awk -v S="$start_ts" '
        {
            if (match($0, /\[([0-9]+)ms\]/, ts_arr))
                ts = ts_arr[1]+0
            else
                next
            if (ts < S) next
        }
        /start SnicGCFallback/ { in_fallback = 1 }
        /Concurrent marking [0-9]/ && !/roots/ {
            match($0, /Concurrent marking ([0-9]+\.[0-9]+)ms/, m)
            t = m[1]+0
            if (in_fallback) {
                fb_sum += t; fb_n++; in_fallback = 0
            } else {
                dgc_sum += t; dgc_n++
            }
        }
        /Pause Degenerated GC/ && /M\(/ { degen_n++ }
        /Pause Full GC/ && /M\(/ { full_n++ }
        END {
            fb_avg  = (fb_n  > 0) ? fb_sum/fb_n   : 0
            dgc_avg = (dgc_n > 0) ? dgc_sum/dgc_n : 0
            total = fb_n + dgc_n
            dgc_pct = (total > 0) ? dgc_n * 100 / total : 0
            printf "fb_avg=%.0f fb_n=%d dgc_avg=%.0f dgc_n=%d total=%d dgc_pct=%d degen=%d full=%d\n", \
                fb_avg, fb_n, dgc_avg, dgc_n, total, dgc_pct, degen_n+0, full_n+0
        }' "$marking_log")
    fi

    # Parse result into variables
    eval "$result"

    printf "  [%-6s] GC=%d (degen=%d full=%d) | Fallback: avg=%dms n=%d | DGC: avg=%dms n=%d | DGC%%=%d%%\n" \
        "$label" "$total" "$degen" "$full" "$fb_avg" "$fb_n" "$dgc_avg" "$dgc_n" "$dgc_pct"

    # Accumulate for summary
    _total_fb_sum=$(( _total_fb_sum + fb_avg * fb_n ))
    _total_fb_n=$(( _total_fb_n + fb_n ))
    _total_dgc_sum=$(( _total_dgc_sum + dgc_avg * dgc_n ))
    _total_dgc_n=$(( _total_dgc_n + dgc_n ))
}

# ============================================================
# Main
# ============================================================

echo "============================================"
echo "DaCapo DGC Marking Time Analysis"
echo "============================================"
echo "Result: $(basename "$RUN_DIR")"
echo "Host:   ${HOST}"
echo ""

_total_fb_sum=0
_total_fb_n=0
_total_dgc_sum=0
_total_dgc_n=0

if [ -d "$RUN_DIR/raw" ]; then
    # Framework result dir — iterate throttle points
    echo "=== Per-throttle marking times (measurement iteration) ==="
    for point_dir in $(ls -d "$RUN_DIR/raw"/*/ 2>/dev/null | sort -t/ -k$(echo "$RUN_DIR/raw/1/" | tr '/' '\n' | wc -l) -n); do
        throttle=$(basename "$point_dir")
        host_log="${point_dir}/host_${HOST}.log"
        gc_log=""
        if [ ! -f "$host_log" ]; then
            # Baseline mode: DaCapo output in run.log, GC in logs/gc_<throttle>_host<N>.log
            host_log="${point_dir}/run.log"
            gc_log="${RUN_DIR}/logs/gc_${throttle}_host${HOST}.log"
        fi
        _analyze_one_point "$host_log" "$throttle" "$gc_log"
    done
else
    # Single throttle point dir
    host_log="${RUN_DIR}/host_${HOST}.log"
    gc_log=""
    if [ ! -f "$host_log" ]; then
        host_log="${RUN_DIR}/run.log"
    fi
    _analyze_one_point "$host_log" "point" "$gc_log"
fi

echo ""
echo "=== Summary ==="

if [ "$_total_fb_n" -gt 0 ]; then
    _fb_global_avg=$(( _total_fb_sum / _total_fb_n ))
    echo "  Fallback marking: avg=${_fb_global_avg}ms  (total n=${_total_fb_n})"
else
    _fb_global_avg=0
    echo "  Fallback marking: no data"
fi

if [ "$_total_dgc_n" -gt 0 ]; then
    _dgc_global_avg=$(( _total_dgc_sum / _total_dgc_n ))
    echo "  DGC marking:      avg=${_dgc_global_avg}ms  (total n=${_total_dgc_n})"
else
    _dgc_global_avg=0
    echo "  DGC marking:      no data"
fi

_total_all=$(( _total_fb_n + _total_dgc_n ))
if [ "$_total_all" -gt 0 ]; then
    echo "  DGC ratio:        $(( _total_dgc_n * 100 / _total_all ))%"
fi

echo ""
echo "=== Suggested COOR_CCMT_ARGS ==="
echo "  Format: CCMT_fallback:0:<fallback_avg>;<CCMT_dgc>:0:<dgc_avg>"
if [ "$_fb_global_avg" -gt 0 ] && [ "$_dgc_global_avg" -gt 0 ]; then
    echo "  → \"0:0:${_fb_global_avg};4:0:${_dgc_global_avg}\""
elif [ "$_fb_global_avg" -gt 0 ]; then
    echo "  → Only fallback data available. Use baseline Shenandoah marking avg as DGC estimate."
    echo "  → \"0:0:${_fb_global_avg};4:0:<shenandoah_avg>\""
else
    echo "  → Insufficient data. Run more tests."
fi

echo ""
echo "============================================"
