#!/usr/bin/env python3
# parse-fig5-latency.py — extract fig5 plotting CSVs from a SPECjbb DGC-SHM run.
#
# Mirrors the archive's parse-snic-specjbb-test-output.py + fig5-plot-time-window.py
# parse logic, adapted to osdi26-ae's run-dir layout where the GC log is split
# from the SPECjbb stdout (and uses absolute epoch ms via -Xlog:...:timemillis).
#
#   <run_dir>/
#     META.json
#     logs/
#       backend_<gid>.log         specjbb backend stdout (state transitions
#                                 with `<Mon Apr 27 16:06:19 CST 2026>` CST stamps)
#       gc_backend_<gid>.log      Shenandoah GC log with `[<epoch_ms>ms]` stamps
#       controller.log
#     raw/
#       specjbb_latency.txt       <start_us>,<latency_us>,<req_type> per request
#                                 (start_us = epoch microseconds, salvaged by
#                                 fig5-run.sh from ${SPECJBB_DIR}/latency.txt)
#
# Pipeline:
#   1. From backend_<gid>.log find the wall-clock window where IR is at the
#      target rate: from `IR=target → IR=target` (steady-state confirmation)
#      to `IR=target → IR=0` (rampdown). Parse CST stamps as Asia/Shanghai
#      (UTC+8) — ds00 is in CST.
#   2. From gc_backend_<gid>.log extract Pause Init Mark / Concurrent marking /
#      Concurrent update references events whose epoch_ms falls in that window;
#      group into GC cycles (each cycle = init-mark → end-mark → end-update-refs).
#   3. From specjbb_latency.txt filter records of req_type=1 with start_us in
#      the window; pad +/- 100ms around the first/last GC cycle for plot context.
#   4. Bucket records into 100ms windows; emit p50/p90/p99 latency + throughput
#      time series CSV. Emit a separate GC-bands CSV for the plotter.
#
# Outputs (under <out_dir>/):
#   windows.csv   time_s,p50_us,p90_us,p99_us,throughput_kops
#   gc.csv        idx,phase,start_s,end_s        phase ∈ {mark, compaction}
#   meta.csv      target_ir,run_dir,gid,plot_start_idx,plot_end_idx
#
# Usage:
#   parse-fig5-latency.py <run_dir> <out_dir> [<gid>]

import glob
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))

# ----- regexes ---------------------------------------------------------------

# CST stamp at start of each backend stdout line: <Mon Apr 27 16:06:19 CST 2026>
_BACKEND_TS = re.compile(
    r'<(\w{3}) (\w{3}) (\d{1,2}) (\d{2}):(\d{2}):(\d{2}) CST (\d{4})>'
)
# IR state transitions (anywhere on the line).
_IR_TRANSITION = re.compile(
    r'IN STATE: Running \(IR=(\d+)\.0\), NEW STATE: Running \(IR=(\d+)\.0\)'
)

# Shenandoah GC log uses [<epoch_ms>ms] stamps thanks to -Xlog:...:timemillis.
_GC_TS = re.compile(r'\[(\d+)ms\]')
_PAUSE_INIT_MARK = re.compile(r'Pause Init Mark (\d+\.\d+)ms')
_CONC_MARK_END = re.compile(r'Concurrent marking (\d+\.\d+)ms')
_CONC_UPDATE_REFS = re.compile(r'Concurrent update references (\d+\.\d+)ms')
_TRIGGER_DEGEN = re.compile(r'Trigger: Handle Allocation Failure')

_MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


# ----- helpers ---------------------------------------------------------------

def _read(path):
    with open(path, errors="replace") as f:
        return f.read()


def _read_rotated(base_path):
    """Read base_path plus its `-Xlog:...:filesize=...` rotation siblings.

    Shenandoah rotates `gc_backend_0.log` to `gc_backend_0.log.0`, `.1`, etc.
    when the file exceeds the configured filesize. The `.0` suffix holds the
    OLDEST events; the un-suffixed file holds the NEWEST. We concatenate
    them in chronological order so callers can scan a single text blob.
    """
    parts = []
    rotations = sorted(glob.glob(base_path + ".*"),
                       key=lambda p: int(p.rsplit(".", 1)[-1])
                       if p.rsplit(".", 1)[-1].isdigit() else 1_000_000_000)
    for p in rotations + [base_path]:
        if os.path.exists(p):
            parts.append(_read(p))
    return "".join(parts)


def _parse_cst(line):
    """Return epoch ms for the leading CST timestamp on the line, or None."""
    m = _BACKEND_TS.search(line)
    if not m:
        return None
    _, mon, day, hh, mm, ss, year = m.groups()
    if mon not in _MONTHS:
        return None
    dt = datetime(int(year), _MONTHS[mon], int(day), int(hh), int(mm), int(ss),
                  tzinfo=CST)
    return int(dt.timestamp() * 1000)


# ----- steady-state window detection ----------------------------------------

def find_steady_state_window_ms(backend_text, target_ir):
    """Return (start_ms, end_ms) — wall-clock epoch ms of the steady-state window.

    start = first 'IR=target → IR=target' transition (controller confirmed at full rate)
    end   = first 'IR=target → IR=0' transition after start (rampdown)

    If end isn't found (test killed before rampdown), end = last CST timestamp
    in backend_text (best-effort).
    """
    start_ms = None
    end_ms = None
    last_ts = None
    for line in backend_text.splitlines():
        ts = _parse_cst(line)
        if ts is not None:
            last_ts = ts
        m = _IR_TRANSITION.search(line)
        if not m or ts is None:
            continue
        left, right = int(m.group(1)), int(m.group(2))
        if start_ms is None and left == target_ir and right == target_ir:
            start_ms = ts
            continue
        if start_ms is not None and left == target_ir and right == 0:
            end_ms = ts
            break
    if start_ms is not None and end_ms is None:
        # Test ended without rampdown — fall back to last CST stamp.
        end_ms = last_ts
    return start_ms, end_ms


# ----- GC phase extraction ---------------------------------------------------

def extract_gc_cycles(gc_text, start_ms, end_ms):
    """Return list of dicts {init_mark_ms, end_mark_ms, end_update_refs_ms}.

    A cycle is included only if all three phase timestamps appear in order
    within [start_ms, end_ms] AND no degenerate-GC trigger interrupted it.
    """
    cur = None
    cycles = []
    skip_cycle = False
    for line in gc_text.splitlines():
        m = _GC_TS.search(line)
        if not m:
            continue
        ts = int(m.group(1))
        if ts < start_ms:
            continue
        if ts > end_ms:
            break
        if _TRIGGER_DEGEN.search(line):
            skip_cycle = True
            continue
        if _PAUSE_INIT_MARK.search(line):
            # Finalize previous cycle if it was complete.
            if cur is not None and not cur.get("skip"):
                if all(k in cur for k in ("init_mark_ms", "end_mark_ms",
                                          "end_update_refs_ms")):
                    cycles.append(cur)
            cur = {"init_mark_ms": ts, "skip": skip_cycle}
            skip_cycle = False
            continue
        if cur is None:
            continue
        if _CONC_MARK_END.search(line) and "end_mark_ms" not in cur:
            cur["end_mark_ms"] = ts
        elif _CONC_UPDATE_REFS.search(line) and "end_update_refs_ms" not in cur:
            cur["end_update_refs_ms"] = ts
    # Tail flush.
    if cur is not None and not cur.get("skip"):
        if all(k in cur for k in ("init_mark_ms", "end_mark_ms",
                                  "end_update_refs_ms")):
            cycles.append(cur)
    return cycles


# ----- request-latency extraction --------------------------------------------

def load_latency_records(latency_path, first_us, last_us, req_type=1):
    """Read latency.txt rows in [first_us, last_us]; return [(start_us, end_us)]."""
    out = []
    with open(latency_path, errors="replace") as f:
        for line in f:
            parts = line.split(",")
            if len(parts) != 3:
                continue
            try:
                start = int(parts[0])
                lat = int(parts[1])
                rt = int(parts[2])
            except ValueError:
                continue
            if rt != req_type:
                continue
            if start < first_us or start > last_us:
                continue
            out.append((start, start + lat))
    out.sort(key=lambda p: p[0])
    return out


# ----- 100ms windowing -------------------------------------------------------

def percentile(sorted_values, q):
    if not sorted_values:
        return 0
    idx = int(len(sorted_values) * q)
    if idx >= len(sorted_values):
        idx = len(sorted_values) - 1
    return sorted_values[idx]


def window_metrics(records, start_us, end_us, window_us=100_000):
    """Bucket records; return list of (mid_s, p50_us, p90_us, p99_us, kops_per_s)."""
    rows = []
    cur = start_us
    idx = 0
    while cur < end_us:
        window_end = cur + window_us
        latencies = []
        while idx < len(records) and records[idx][0] < window_end:
            if records[idx][0] >= cur:
                latencies.append(records[idx][1] - records[idx][0])
            idx += 1
        latencies.sort()
        mid_s = (cur + window_us / 2 - start_us) / 1_000_000
        if latencies:
            rows.append((
                mid_s,
                percentile(latencies, 0.5),
                percentile(latencies, 0.9),
                percentile(latencies, 0.99),
                len(latencies) * 1_000_000 / window_us / 1000,
            ))
        else:
            rows.append((mid_s, 0, 0, 0, 0))
        cur += window_us
    return rows


# ----- main ------------------------------------------------------------------

def main(argv):
    if len(argv) < 3:
        print("Usage: parse-fig5-latency.py <run_dir> <out_dir> [<gid>]",
              file=sys.stderr)
        sys.exit(2)
    run_dir = argv[1]
    out_dir = argv[2]
    gid = int(argv[3]) if len(argv) > 3 else 0

    backend_log = os.path.join(run_dir, "logs", f"backend_{gid}.log")
    gc_log = os.path.join(run_dir, "logs", f"gc_backend_{gid}.log")
    latency_txt = os.path.join(run_dir, "raw", "specjbb_latency.txt")
    for path in (backend_log, gc_log, latency_txt):
        if not os.path.exists(path):
            print(f"[err] missing required file: {path}", file=sys.stderr)
            sys.exit(2)

    target_ir = int(os.environ.get("FIG5_IR", "6819"))

    backend_text = _read(backend_log)
    start_ms, end_ms = find_steady_state_window_ms(backend_text, target_ir)
    if start_ms is None or end_ms is None:
        print(f"[err] could not find steady-state window in {backend_log} "
              f"(IR={target_ir}). State transitions:", file=sys.stderr)
        for line in backend_text.splitlines():
            if "IN STATE" in line:
                print(f"  {line}", file=sys.stderr)
        sys.exit(3)
    print(f"[info] steady-state window: epoch_ms [{start_ms}, {end_ms}] "
          f"({(end_ms - start_ms) / 1000:.1f}s)", file=sys.stderr)

    gc_text = _read_rotated(gc_log)
    cycles = extract_gc_cycles(gc_text, start_ms, end_ms)
    if len(cycles) < 2:
        print(f"[err] only {len(cycles)} concurrent GC cycles observed in "
              f"steady-state — need ≥2 to plot fig5. Check {gc_log}.",
              file=sys.stderr)
        sys.exit(4)
    print(f"[info] {len(cycles)} concurrent GC cycles in window", file=sys.stderr)

    plot_start_idx = int(os.environ.get("FIG5_PLOT_START_IDX", "0"))
    default_end = min(plot_start_idx + 10, len(cycles))
    plot_end_idx = int(os.environ.get("FIG5_PLOT_END_IDX", str(default_end)))
    plot_cycles = cycles[plot_start_idx:plot_end_idx]
    if len(plot_cycles) < 2:
        plot_cycles = cycles
        plot_start_idx = 0
        plot_end_idx = len(cycles)
    print(f"[info] plotting cycles [{plot_start_idx}, {plot_end_idx})",
          file=sys.stderr)

    PAD_MS = 100  # 100ms padding before first / after last cycle
    origin_us = (plot_cycles[0]["init_mark_ms"] - PAD_MS) * 1000
    end_us = (plot_cycles[-1]["end_update_refs_ms"] + PAD_MS) * 1000

    records = load_latency_records(latency_txt, origin_us, end_us)
    print(f"[info] {len(records)} req_type=1 latency rows in window "
          f"({(end_us - origin_us) / 1_000_000:.1f}s)", file=sys.stderr)
    if not records:
        print(f"[err] no latency records — was specjbb_latency.txt empty? "
              f"first GC at epoch_us {origin_us}, last at {end_us}.",
              file=sys.stderr)
        sys.exit(5)

    rows = window_metrics(records, origin_us, end_us)

    os.makedirs(out_dir, exist_ok=True)
    win_csv = os.path.join(out_dir, "windows.csv")
    gc_csv = os.path.join(out_dir, "gc.csv")
    meta_csv = os.path.join(out_dir, "meta.csv")

    with open(win_csv, "w") as f:
        f.write("time_s,p50_us,p90_us,p99_us,throughput_kops\n")
        for mid_s, p50, p90, p99, kops in rows:
            f.write(f"{mid_s:.4f},{p50},{p90},{p99},{kops:.3f}\n")

    with open(gc_csv, "w") as f:
        f.write("idx,phase,start_s,end_s\n")
        for i, c in enumerate(plot_cycles):
            mark_start_s = (c["init_mark_ms"] * 1000 - origin_us) / 1_000_000
            mark_end_s = (c["end_mark_ms"] * 1000 - origin_us) / 1_000_000
            comp_end_s = (c["end_update_refs_ms"] * 1000 - origin_us) / 1_000_000
            f.write(f"{i},mark,{mark_start_s:.4f},{mark_end_s:.4f}\n")
            f.write(f"{i},compaction,{mark_end_s:.4f},{comp_end_s:.4f}\n")

    with open(meta_csv, "w") as f:
        f.write("target_ir,run_dir,gid,plot_start_idx,plot_end_idx\n")
        f.write(f"{target_ir},{run_dir},{gid},{plot_start_idx},{plot_end_idx}\n")

    print(f"[ok] wrote {win_csv} ({len(rows)} windows), "
          f"{gc_csv} ({len(plot_cycles)*2} bands), {meta_csv}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
