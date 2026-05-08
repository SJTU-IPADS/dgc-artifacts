#!/usr/bin/env python3
# parse-fig7-preset.py — extract fig7 metrics into per-run + aggregated CSVs,
# plus three "narrow" two-column CSVs that match the canonical fig7
# multi-axis plotting format consumed by plot/fig7-plot.py.
#
# Outputs under <out_dir>/:
#   preset.csv           per-run rows (full schema, for forensics)
#   preset.agg.csv       mean ± 95% per (gc, cache_size_mb)
#   p99.csv              type,p99_ms                  ← consumed by fig7-plot.py
#   memory_usage.csv     type,memory_gb               ← consumed by fig7-plot.py
#   rdma_transport.csv   type,rdma_gb                 ← consumed by fig7-plot.py
#
# `type` labels follow the paper / multi-axis plot convention:
#   G1, Shen, D-Shm, D-RDMA 1/1, D-RDMA 3/4, D-RDMA 1/2, D-RDMA 1/4
#
# Sources of truth:
#   - rt-curve last iteration   → p50/p90/p95/p99/max (us)
#   - controller.log            → max rIR (target IR), mean aIR at target
#   - gc_backend_*.log          → max post-cleanup heap usage (MB)
#   - coordinator.log           → CP-SAT solve `Time taken: N ms` mean
#   - dgc_client_*.log          → cumulative RDMA bytes (best-effort regex)
#
# Run-id tag convention (set by fig7-run.sh):
#   .._fig7-<base>-mem<MB>-rep<R>_<user>      for dgc-rdma cache point
#   .._fig7-<base>-rep<R>_<user>              for g1 / shen / dgc-shm
#
# Usage:
#   parse-fig7-preset.py <output_dir> <run_dir>...

import math
import os
import re
import statistics
import sys
from fractions import Fraction

# ----- regexes ---------------------------------------------------------------

_META_FIELD = re.compile(r'"(workload|gc|status|run_id)"\s*:\s*"([^"]+)"')
_MEM_TAG = re.compile(r'mem(\d+)')
_RT_ROW = re.compile(r'^(\d+(?:\.\d+)?);([\d.]+);([\d.]+);([\d.]+);'
                     r'([\d.]+);([\d.]+);([\d.]+)\s*$')
_AIR = re.compile(r'rIR:aIR:PR\s*=\s*(\d+):(\d+):(\d+)')
_CCLEANUP = re.compile(r'Concurrent cleanup (\d+)M->(\d+)M\((\d+)M\)')
_SOLVER = re.compile(r'Time taken:\s*(\d+) milliseconds')
# RDMA traffic source of truth: every per-region copy that the DGC client
# completes emits one `LHT LOG: wait_copy_finish_work recv_region_idx=N`
# line. Counting these and multiplying by the heap region size (parsed from
# `Heap Region Size: NM` in the host's GC log) approximates the cumulative
# RDMA bytes ferried client→host. The earlier regex set looked for
# "total ... rdma ... bytes" / "RDMA total = ... bytes" patterns that the
# JDK never actually emits, leaving rdma_transport.csv pinned at 0.
_WAIT_COPY_FINISH = re.compile(r'wait_copy_finish_work')
_REGION_SIZE = re.compile(r'Heap Region Size:\s*(\d+)M')


# ----- helpers ---------------------------------------------------------------

def _read_meta(run_dir):
    path = os.path.join(run_dir, "META.json")
    if not os.path.exists(path):
        return None
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return None
    out = {}
    for m in _META_FIELD.finditer(text):
        out.setdefault(m.group(1), m.group(2))
    return out or None


def _find_rt_curve(run_dir):
    raw_dir = os.path.join(run_dir, "raw")
    if not os.path.isdir(raw_dir):
        return None
    for jbb_dir in sorted(os.listdir(raw_dir)):
        if not jbb_dir.startswith("specjbb2015-"):
            continue
        for root, _, files in os.walk(os.path.join(raw_dir, jbb_dir)):
            for f in files:
                if f.endswith("-overall-throughput-rt.txt"):
                    return os.path.join(root, f)
    return None


def _gc_logs(run_dir):
    logs_dir = os.path.join(run_dir, "logs")
    if not os.path.isdir(logs_dir):
        return []
    out = []
    for f in sorted(os.listdir(logs_dir)):
        if f.startswith("gc_backend_") and (".log" in f):
            out.append(os.path.join(logs_dir, f))
    out.sort(key=lambda p: (1_000_000_000 if p.endswith(".log")
                            else int(p.rsplit(".", 1)[-1])))
    return out


def _client_logs(run_dir):
    logs_dir = os.path.join(run_dir, "logs")
    if not os.path.isdir(logs_dir):
        return []
    return [os.path.join(logs_dir, f) for f in os.listdir(logs_dir)
            if f.startswith("dgc_client_") and f.endswith(".log")]


# ----- per-run extraction ----------------------------------------------------

def extract_rt_metrics(rt_path):
    if rt_path is None or not os.path.exists(rt_path):
        return None, None, None, None, None
    last = None
    for line in open(rt_path, errors="replace").read().splitlines():
        m = _RT_ROW.match(line.strip())
        if m:
            last = m.groups()
    if last is None:
        return None, None, None, None, None
    _, _, p50, p90, p95, p99, mx = last
    return float(p50), float(p90), float(p95), float(p99), float(mx)


def extract_ir(controller_path):
    """target_ir = MAX rIR; achieved = mean aIR at lines where rIR == target."""
    if not os.path.exists(controller_path):
        return None, None
    samples = []
    target = 0
    for line in open(controller_path, errors="replace"):
        m = _AIR.search(line)
        if not m:
            continue
        ri = int(m.group(1))
        ai = int(m.group(2))
        samples.append((ri, ai))
        if ri > target:
            target = ri
    if target == 0:
        return None, None
    at_target = [a for r, a in samples if r == target and a > 0]
    if not at_target:
        return target, None
    return target, int(round(sum(at_target) / len(at_target)))


def extract_peak_mem_mb(gc_log_paths):
    peak = None
    for p in gc_log_paths:
        try:
            for line in open(p, errors="replace"):
                m = _CCLEANUP.search(line)
                if m:
                    used = int(m.group(2))
                    if peak is None or used > peak:
                        peak = used
        except OSError:
            continue
    return peak


def extract_avg_solver_ms(coord_path):
    if not os.path.exists(coord_path):
        return None
    samples = []
    try:
        for line in open(coord_path, errors="replace"):
            m = _SOLVER.search(line)
            if m:
                samples.append(int(m.group(1)))
    except OSError:
        return None
    if not samples:
        return None
    return sum(samples) / len(samples)


def extract_region_size_mb(gc_log_paths):
    """Region size in MB, parsed from `Heap Region Size: NM` in the host log."""
    for p in gc_log_paths:
        try:
            for line in open(p, errors="replace"):
                m = _REGION_SIZE.search(line)
                if m:
                    return int(m.group(1))
        except OSError:
            continue
    return None


def extract_rdma_traffic_gb(client_paths, region_size_mb):
    """Approximate RDMA bytes = sum_clients(wait_copy_finish_work events) * region_size.

    Each `LHT LOG: wait_copy_finish_work recv_region_idx=N` is one region
    copy received over RDMA. SHM mode never emits these (returns 0 → caller
    sees no RDMA traffic, which is correct for SHM/baseline). For RDMA mode
    each region is the heap region size (default 2 MB on a 4 GB heap)."""
    if not client_paths or region_size_mb is None:
        return None
    region_count = 0
    saw_anything = False
    for p in client_paths:
        try:
            n = 0
            for line in open(p, errors="replace"):
                if _WAIT_COPY_FINISH.search(line):
                    n += 1
            region_count += n
            saw_anything = True
        except OSError:
            continue
    if not saw_anything:
        return None
    bytes_total = region_count * region_size_mb * 1024 * 1024
    return bytes_total / 1024 / 1024 / 1024


def extract_cache_size_mb(run_id, gc):
    if gc != "dgc-rdma":
        return None
    m = _MEM_TAG.search(run_id)
    return int(m.group(1)) if m else None


# ----- type label ------------------------------------------------------------

def make_type_label(gc, cache_mb, max_cache_mb):
    """G1 / Shen / D-Shm / D-RDMA <fraction> — matches plot_multi_axis.py."""
    if gc == "g1":
        return "G1"
    if gc == "shenandoah":
        return "Shen"
    if gc == "dgc-shm":
        return "D-Shm"
    if gc == "dgc-rdma":
        if cache_mb and max_cache_mb:
            f = Fraction(int(cache_mb), int(max_cache_mb)).limit_denominator(8)
            return f"D-RDMA {f.numerator}/{f.denominator}"
        return "D-RDMA"
    return gc


# ----- aggregation -----------------------------------------------------------

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


# ----- main ------------------------------------------------------------------

def main(argv):
    if len(argv) < 3:
        print("Usage: parse-fig7-preset.py <output_dir> <run_dir>...",
              file=sys.stderr)
        sys.exit(2)
    out_dir = argv[1]
    run_dirs = argv[2:]

    rows = []
    for run_dir in run_dirs:
        meta = _read_meta(run_dir)
        if meta is None:
            print(f"[skip] {os.path.basename(run_dir)}: no META.json",
                  file=sys.stderr)
            continue
        if meta.get("workload") != "specjbb-preset":
            print(f"[skip] {os.path.basename(run_dir)}: workload="
                  f"{meta.get('workload')}", file=sys.stderr)
            continue
        gc = meta.get("gc", "")
        run_id = meta.get("run_id", os.path.basename(run_dir))
        cache_mb = extract_cache_size_mb(run_id, gc)

        rt = _find_rt_curve(run_dir)
        p50, p90, p95, p99, mx = extract_rt_metrics(rt)

        ctrl = os.path.join(run_dir, "logs", "controller.log")
        target_ir, ach_ir = extract_ir(ctrl)

        peak_mem_mb = extract_peak_mem_mb(_gc_logs(run_dir))

        coord = os.path.join(run_dir, "logs", "coordinator.log")
        solver_ms = extract_avg_solver_ms(coord) if gc.startswith("dgc-") else None

        if gc == "dgc-rdma":
            region_size_mb = extract_region_size_mb(_gc_logs(run_dir))
            rdma_gb = extract_rdma_traffic_gb(_client_logs(run_dir), region_size_mb)
        else:
            rdma_gb = None

        if p99 is None:
            print(f"[warn] {os.path.basename(run_dir)}: no rt-curve",
                  file=sys.stderr)
        rows.append((gc, cache_mb, run_id, p50, p90, p95, p99, mx,
                     target_ir, ach_ir, peak_mem_mb, rdma_gb, solver_ms))

    os.makedirs(out_dir, exist_ok=True)
    order = {"g1": 0, "shenandoah": 1, "dgc-shm": 2, "dgc-rdma": 3}

    def sort_key(r):
        gc, cache_mb = r[0], r[1]
        return (order.get(gc, 99),
                -1 if cache_mb is None else -cache_mb,
                r[2])

    # ----- preset.csv (per-run) -----
    out_csv = os.path.join(out_dir, "preset.csv")
    with open(out_csv, "w") as f:
        f.write("gc,cache_size_mb,run_id,p50_us,p90_us,p95_us,p99_us,max_us,"
                "target_ir,achieved_ir,peak_mem_mb,rdma_traffic_gb,"
                "solver_time_ms\n")
        for r in sorted(rows, key=sort_key):
            (gc, cache_mb, run_id, p50, p90, p95, p99, mx,
             t_ir, a_ir, peak_mb, rdma_gb, slv_ms) = r
            f.write(",".join([
                gc,
                "" if cache_mb is None else str(cache_mb),
                run_id,
                "" if p50 is None else f"{p50}",
                "" if p90 is None else f"{p90}",
                "" if p95 is None else f"{p95}",
                "" if p99 is None else f"{p99}",
                "" if mx is None else f"{mx}",
                "" if t_ir is None else str(t_ir),
                "" if a_ir is None else str(a_ir),
                "" if peak_mb is None else str(peak_mb),
                "" if rdma_gb is None else f"{rdma_gb:.4f}",
                "" if slv_ms is None else f"{slv_ms:.3f}",
            ]) + "\n")

    # ----- preset.agg.csv (aggregated by group) -----
    agg_csv = os.path.join(out_dir, "preset.agg.csv")
    by_group = {}
    for r in rows:
        by_group.setdefault((r[0], r[1]), []).append(r)
    with open(agg_csv, "w") as f:
        f.write("gc,cache_size_mb,sample_size,"
                "p50_mean,p50_ci95,p90_mean,p90_ci95,"
                "p99_mean,p99_ci95,max_mean,max_ci95,"
                "target_ir_mean,achieved_ir_mean,"
                "peak_mem_mb_mean,peak_mem_mb_ci95,"
                "rdma_traffic_gb_mean,rdma_traffic_gb_ci95,"
                "solver_time_ms_mean,solver_time_ms_ci95\n")

        def grp_key(g):
            gc, cache_mb = g
            return (order.get(gc, 99),
                    -1 if cache_mb is None else -cache_mb)

        for (gc, cache_mb) in sorted(by_group, key=grp_key):
            samples = by_group[(gc, cache_mb)]

            def col(idx):
                vals = [s[idx] for s in samples if s[idx] is not None]
                return _mean_ci95(vals)

            p50_m, p50_c = col(3); p90_m, p90_c = col(4)
            p99_m, p99_c = col(6); max_m, max_c = col(7)
            t_ir_m, _ = col(8); a_ir_m, _ = col(9)
            mem_m, mem_c = col(10)
            rdma_m, rdma_c = col(11)
            slv_m, slv_c = col(12)
            f.write(",".join([
                gc,
                "" if cache_mb is None else str(cache_mb),
                str(len(samples)),
                f"{p50_m}", f"{p50_c}",
                f"{p90_m}", f"{p90_c}",
                f"{p99_m}", f"{p99_c}",
                f"{max_m}", f"{max_c}",
                f"{t_ir_m}", f"{a_ir_m}",
                f"{mem_m}", f"{mem_c}",
                f"{rdma_m}", f"{rdma_c}",
                f"{slv_m}", f"{slv_c}",
            ]) + "\n")

    # ----- narrow CSVs for plot_multi_axis-style plot -----
    rdma_caches = [r[1] for r in rows if r[0] == "dgc-rdma" and r[1]]
    max_cache = max(rdma_caches) if rdma_caches else None

    # Aggregated mean per (gc, cache_mb), in plot order.
    p99_csv = os.path.join(out_dir, "p99.csv")
    mem_csv = os.path.join(out_dir, "memory_usage.csv")
    rdma_csv = os.path.join(out_dir, "rdma_transport.csv")
    with open(p99_csv, "w") as fp99, \
         open(mem_csv, "w") as fmem, \
         open(rdma_csv, "w") as frdma:
        fp99.write("type,p99_ms\n")
        fmem.write("type,memory_gb\n")
        frdma.write("type,rdma_gb\n")

        def grp_key(g):
            gc, cache_mb = g
            return (order.get(gc, 99),
                    -1 if cache_mb is None else -cache_mb)

        for (gc, cache_mb) in sorted(by_group, key=grp_key):
            samples = by_group[(gc, cache_mb)]
            label = make_type_label(gc, cache_mb, max_cache)

            def col(idx):
                vals = [s[idx] for s in samples if s[idx] is not None]
                return _mean_ci95(vals)[0]   # mean only

            p99_us = col(6)
            mem_mb = col(10)
            rdma_gb = col(11)
            p99_ms = (p99_us / 1000) if p99_us not in (-1, None) else 0
            mem_gb = (mem_mb / 1024) if mem_mb not in (-1, None) else 0
            rdma_gb_v = rdma_gb if rdma_gb not in (-1, None) else 0
            fp99.write(f"{label},{p99_ms:.3f}\n")
            fmem.write(f"{label},{mem_gb:.3f}\n")
            frdma.write(f"{label},{rdma_gb_v:.3f}\n")

    print(f"[ok] wrote {out_csv} ({len(rows)} runs), "
          f"{agg_csv} ({len(by_group)} groups)", file=sys.stderr)
    print(f"[ok] wrote {p99_csv}, {mem_csv}, {rdma_csv}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
