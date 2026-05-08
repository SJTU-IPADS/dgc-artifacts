#!/usr/bin/env python3
# parse-fig6-dacapo.py — extract DaCapo fig6 metrics into a CSV.
#
# Mirrors the archive's parse-{shenandoah,shm,rdma,g1}-output.py family
# (osdi26-test-archive/dacapo-tests/fig6-{h2,daytrader}-rt-curve-test/),
# adapted to the new osdi26-ae result layout:
#
#   results/<base>/<run-id>/
#     META.json                      (workload, gc, status)
#     raw/<throttle>/host_<i>.log    (DaCapo host JVM stdout + Shenandoah GC log)
#
# For each (gc, throttle) point we collect across reps:
#   throughput, host-app-time, simple p50/p90/p99, metered p50/p90/p99
#   plus (for shenandoah/dgc-shm/dgc-rdma) Shenandoah marking/evac/final-mark
#   times and a derived host-mark-rate (live_data / mark_time).
#
# Strategy: DaCapo prints `simple/metered tail latency` and `processed N
# requests` lines once per iteration; the LAST occurrence is the post-PASSED
# summary for the final iteration (matches archive parse-g1-output.py).
#
# Usage:
#   parse-fig6-dacapo.py <workload> <gc> <heap_label> <ccmt> <ccet> <pcore> \
#                        <host_count> <output_csv> <run_dir>...
#
# Example:
#   parse-fig6-dacapo.py h2 dgc-shm 2.0 8 8 8 2 \
#       plot/h2-data/shm-dgc-output.csv \
#       results/adhoc-fig6/*_h2_dgc-shm_fig6-h2-*

import math
import os
import re
import statistics
import sys
from glob import glob

# ----- regexes ---------------------------------------------------------------

_SIMPLE_LATENCY = re.compile(
    r'DaCapo simple tail latency: 50% (\d+) usec, 90% (\d+) usec, 99% (\d+) usec'
)
_METERED_LATENCY = re.compile(
    r'DaCapo metered tail latency: 50% (\d+) usec, 90% (\d+) usec, 99% (\d+) usec'
)
_PROCESSED = re.compile(
    r'DaCapo processed (\d+) requests in (\d+) msec, (\d+) requests per second'
)
_PASSED = re.compile(r'PASSED in (\d+) msec')
_STARTING = re.compile(r'starting ')
_LIVE_DATA = re.compile(r'get total live data (\d+) bytes during ccmark')
_CONC_MARK_END = re.compile(r'Concurrent marking (\d+\.\d+)ms')
_CONC_EVAC_END = re.compile(r'Concurrent evacuation (\d+\.\d+)ms')
_FINAL_MARK = re.compile(r'Pause Final Mark (\d+\.\d+)ms')

# META.json fields are extracted via regex — current osdi26-ae META.json has
# an unescaped `"openjdk version "17-internal" ..."` string in the jdk.version
# field that breaks strict json.loads.
_META_FIELD = re.compile(r'"(workload|gc|status)"\s*:\s*"([^"]+)"')


# ----- aggregation helpers ---------------------------------------------------

def _mean_ci95(values):
    if not values:
        return -1, -1
    if len(values) == 1:
        return round(values[0], 3), -1
    var = statistics.variance(values)
    if var <= 0:
        return round(sum(values) / len(values), 3), 0.0
    ci = 1.96 * math.sqrt(var / math.sqrt(len(values)))
    return round(sum(values) / len(values), 3), round(ci, 3)


# ----- per-host extraction ---------------------------------------------------

def _extract_dacapo_summary(log_text):
    """Return (throughput, app_time, sp50, sp90, sp99, mp50, mp90, mp99).

    Last occurrence wins — DaCapo prints these per-iteration and once more
    after PASSED, so the final value is the steady-state summary.
    """
    throughput = app_time = None
    sp50 = sp90 = sp99 = None
    mp50 = mp90 = mp99 = None
    for line in log_text.splitlines():
        m = _PROCESSED.search(line)
        if m:
            app_time = int(m.group(2))
            throughput = int(m.group(3))
            continue
        m = _SIMPLE_LATENCY.search(line)
        if m:
            sp50, sp90, sp99 = int(m.group(1)), int(m.group(2)), int(m.group(3))
            continue
        m = _METERED_LATENCY.search(line)
        if m:
            mp50, mp90, mp99 = int(m.group(1)), int(m.group(2)), int(m.group(3))
            continue
    return throughput, app_time, sp50, sp90, sp99, mp50, mp90, mp99


def _extract_steady_state_gc(log_text, loop_time):
    """Extract Shenandoah GC metrics from the final iteration window.

    State machine: count `starting ` markers; at the LOOP_TIME-th, enter
    steady-state collection until PASSED. Returns (mark_rate_mbps,
    total_evac_ms, total_mark_ms, avg_final_mark_ms, gc_count) — None if
    insufficient data.
    """
    live_bytes = []
    mark_times = []
    evac_times = []
    final_marks = []
    state = 0
    cnt = 0
    for line in log_text.splitlines():
        if state == 0:
            if _STARTING.search(line):
                cnt += 1
                if cnt >= loop_time:
                    state = 1
            continue
        # state == 1: in final iteration window.
        if _PASSED.search(line):
            break
        m = _LIVE_DATA.search(line)
        if m:
            live_bytes.append(int(m.group(1)))
            continue
        m = _CONC_MARK_END.search(line)
        if m:
            mark_times.append(float(m.group(1)))
            continue
        m = _CONC_EVAC_END.search(line)
        if m:
            evac_times.append(float(m.group(1)))
            continue
        m = _FINAL_MARK.search(line)
        if m:
            final_marks.append(float(m.group(1)))
            continue
    if not mark_times or not live_bytes:
        return None
    n = min(len(live_bytes), len(mark_times))
    total_live = sum(live_bytes[:n])
    total_mark = sum(mark_times[:n])
    total_evac = sum(evac_times[:n]) if evac_times else 0.0
    avg_final_mark = (sum(final_marks) / len(final_marks)) if final_marks else 0.0
    mark_rate_mbps = (total_live / total_mark) / 1_000_000 if total_mark else 0.0
    return mark_rate_mbps, total_evac, total_mark, avg_final_mark, n


# ----- per-run-dir collection ------------------------------------------------

def _read_meta(run_dir):
    """Extract workload/gc/status from META.json without strict JSON parsing."""
    meta_path = os.path.join(run_dir, "META.json")
    if not os.path.exists(meta_path):
        return None
    try:
        with open(meta_path) as f:
            text = f.read()
    except OSError:
        return None
    out = {}
    for m in _META_FIELD.finditer(text):
        out.setdefault(m.group(1), m.group(2))
    return out or None


def _scan_run(run_dir, loop_time, has_gc_details):
    """Scan one run-id dir, yield (throttle_int, host_metrics_dict)."""
    raw_dir = os.path.join(run_dir, "raw")
    if not os.path.isdir(raw_dir):
        return
    for entry in sorted(os.listdir(raw_dir),
                        key=lambda s: int(s) if s.isdigit() else -1):
        sub = os.path.join(raw_dir, entry)
        if not os.path.isdir(sub) or not entry.isdigit():
            continue
        throttle = int(entry)
        host_logs = sorted(glob(os.path.join(sub, "host_*.log")))
        if not host_logs:
            continue
        for host_log in host_logs:
            try:
                with open(host_log, errors="replace") as f:
                    text = f.read()
            except OSError:
                continue
            tput, app_time, sp50, sp90, sp99, mp50, mp90, mp99 = _extract_dacapo_summary(text)
            if tput is None or sp99 is None:
                continue
            row = {
                "throughput": tput,
                "app_time": app_time,
                "p50": sp50, "p90": sp90, "p99": sp99,
                "metered_p50": mp50, "metered_p90": mp90, "metered_p99": mp99,
            }
            if has_gc_details:
                gc = _extract_steady_state_gc(text, loop_time)
                if gc is not None:
                    rate, evac, mark, fmark, gc_n = gc
                    row.update({
                        "host_mark_rate": rate,
                        "host_evac_time": evac,
                        "host_mark_time": mark,
                        "host_final_mark_time": fmark,
                        "gc_count": gc_n,
                    })
            yield throttle, row


# ----- CSV emission ----------------------------------------------------------

# Column order matches existing osdi26-ae/plot/{h2,tradesoap}-data/*.csv
# headers, lifted from osdi26-test-archive's parse-*.py.
_HEADER_BASE = (
    "hostId,timestamp,heapsize,benchname,ccmt,ccet,pcore,throttle,sample-size,"
)
_HEADER_GC = (
    "host-mark-rate,±95%,host-evac-time,±95%,host-mark-time,±95%,"
    "host-final-mark-time,±95%,gc-count,±95%,"
)
_HEADER_TIMES_LAT = (
    "host-app-time,±95%,throughput,±95%,"
    "p50,±95%,p99,±95%,metered_p50,±95%,metered_p99,±95%"
)
_HEADER_TIMES_LAT_P90 = (
    "host-app-time,±95%,throughput,±95%,"
    "p50,±95%,p90,±95%,p99,±95%,"
    "metered_p50,±95%,metered_p90,±95%,metered_p99,±95%"
)


def _write_csv(out_path, rows, schema):
    has_gc = schema["has_gc"]
    has_p90 = schema["has_p90"]
    header = _HEADER_BASE
    if has_gc:
        header += _HEADER_GC
    header += _HEADER_TIMES_LAT_P90 if has_p90 else _HEADER_TIMES_LAT

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        f.write(header + "\n")
        for r in rows:
            cells = [
                "0", "0", schema["heap"], schema["bench"],
                schema["ccmt"], schema["ccet"], schema["pcore"],
                str(r["throttle"]), str(r["sample_size"]),
            ]
            if has_gc:
                cells += [
                    f"{r['mark_rate'][0]}", f"{r['mark_rate'][1]}",
                    f"{r['evac_time'][0]}", f"{r['evac_time'][1]}",
                    f"{r['mark_time'][0]}", f"{r['mark_time'][1]}",
                    f"{r['final_mark'][0]}", f"{r['final_mark'][1]}",
                    f"{r['gc_count'][0]}", f"{r['gc_count'][1]}",
                ]
            cells += [
                f"{r['app_time'][0]}", f"{r['app_time'][1]}",
                f"{r['throughput'][0]}", f"{r['throughput'][1]}",
                f"{r['p50'][0]}", f"{r['p50'][1]}",
            ]
            if has_p90:
                cells += [f"{r['p90'][0]}", f"{r['p90'][1]}"]
            cells += [
                f"{r['p99'][0]}", f"{r['p99'][1]}",
                f"{r['metered_p50'][0]}", f"{r['metered_p50'][1]}",
            ]
            if has_p90:
                cells += [f"{r['metered_p90'][0]}", f"{r['metered_p90'][1]}"]
            cells += [
                f"{r['metered_p99'][0]}", f"{r['metered_p99'][1]}",
            ]
            f.write(",".join(cells) + "\n")


def _aggregate(by_throttle, has_gc, has_p90):
    out = []
    for throttle in sorted(by_throttle):
        samples = by_throttle[throttle]

        def col(key):
            vals = [s[key] for s in samples if s.get(key) is not None]
            return _mean_ci95(vals)

        row = {
            "throttle": throttle,
            "sample_size": len(samples),
            "throughput": col("throughput"),
            "app_time": col("app_time"),
            "p50": col("p50"),
            "p99": col("p99"),
            "metered_p50": col("metered_p50"),
            "metered_p99": col("metered_p99"),
        }
        if has_p90:
            row["p90"] = col("p90")
            row["metered_p90"] = col("metered_p90")
        if has_gc:
            row["mark_rate"] = col("host_mark_rate")
            row["evac_time"] = col("host_evac_time")
            row["mark_time"] = col("host_mark_time")
            row["final_mark"] = col("host_final_mark_time")
            row["gc_count"] = col("gc_count")
        out.append(row)
    return out


# ----- main ------------------------------------------------------------------

def main(argv):
    if len(argv) < 10:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    workload = argv[1]
    gc = argv[2]
    heap_label = argv[3]
    ccmt = argv[4]
    ccet = argv[5]
    pcore = argv[6]
    host_count = int(argv[7])  # informational; we walk all host_*.log
    output_csv = argv[8]
    run_dirs = argv[9:]

    has_gc_details = gc in ("shenandoah", "dgc-shm", "dgc-rdma")
    # Match archive's per-workload schema decisions.
    has_p90 = workload.startswith("tradesoap") or workload.startswith("tradebeans")

    # Loop count — matches conf/workloads/<bench>.conf ITERATIONS=5.
    loop_time = int(os.environ.get("ITERATIONS", "5"))

    by_throttle = {}
    used_runs = 0
    for run_dir in run_dirs:
        meta = _read_meta(run_dir)
        if meta is None:
            continue
        # Allow workload prefix match (e.g. tradesoap-vlarge-640 → tradesoap)
        # so the same script handles vlarge/big/default conf variants.
        meta_wl = meta.get("workload", "")
        if not (meta_wl == workload or meta_wl.startswith(workload + "-")):
            continue
        if meta.get("gc") != gc:
            continue
        if meta.get("status") != "completed":
            print(f"[skip] {os.path.basename(run_dir)}: status={meta.get('status')}",
                  file=sys.stderr)
            continue
        used_runs += 1
        for throttle, row in _scan_run(run_dir, loop_time, has_gc_details):
            by_throttle.setdefault(throttle, []).append(row)

    if not by_throttle:
        print(f"[warn] no data points for workload={workload} gc={gc}; "
              f"runs scanned={len(run_dirs)} matched={used_runs}",
              file=sys.stderr)

    rows = _aggregate(by_throttle, has_gc_details, has_p90)
    schema = {
        "heap": heap_label, "bench": workload,
        "ccmt": ccmt, "ccet": ccet, "pcore": pcore,
        "has_gc": has_gc_details, "has_p90": has_p90,
    }
    _write_csv(output_csv, rows, schema)
    print(f"[ok] {workload}/{gc}: wrote {len(rows)} rows from {used_runs} runs → "
          f"{output_csv}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
