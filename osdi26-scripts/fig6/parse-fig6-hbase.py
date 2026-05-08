#!/usr/bin/env python3
# parse-fig6-hbase.py — extract HBase fig6 metrics into a CSV.
#
# Mirrors osdi26-test-archive/hbase-tests/fig6-hbase-rt-curve-test/parse-*.py,
# adapted to the new osdi26-ae result layout:
#
#   results/<base>/<run-id>/
#     META.json                         (workload=hbase-{workloada,readinsert})
#     raw/<ir_rate>/ycsb_output.txt     (YCSB driver stdout)
#     raw/<ir_rate>/regionserver_<i>.log (HBase region server log + Shenandoah GC)
#
# YCSB writes three [OVERALL] blocks: load → warmup → run. Per-op pNN
# latencies (e.g. `[READ], p99, 732`) only appear in the run phase, so
# "last value wins" gives us the run-phase numbers without phase tracking.
# We grab the last [OVERALL], Throughput value the same way.
#
# Usage:
#   parse-fig6-hbase.py <workload> <gc> <heap_label> <ccmt> <ccet> <pcore> \
#                       <loop_time> <output_csv> <run_dir>...
#
# <workload> selects which op pair we emit:
#   hbase-workloada  → read + update
#   hbase-readinsert → read + insert

import math
import os
import re
import statistics
import sys

# ----- regexes ---------------------------------------------------------------

_THROUGHPUT = re.compile(r'\[OVERALL\], Throughput\(ops/sec\), (\d+(?:\.\d+)?)')
_RUNTIME = re.compile(r'\[OVERALL\], RunTime\(ms\), (\d+)')
# YCSB op latency lines: `[READ], p99, 1234`. The load phase prints these as
# `99thPercentileLatency(us)` instead, so this regex naturally only matches
# the run phase output.
_OP_LATENCY = re.compile(r'\[(READ|UPDATE|INSERT)\], (p\d+), (\d+(?:\.\d+)?)')

# META.json fields are extracted via regex — current osdi26-ae META.json has
# an unescaped `"openjdk version "17-internal" ..."` string in the jdk.version
# field that breaks strict json.loads, so we just pull the top-level fields
# we need (workload / gc / status) from the surrounding JSON text.
_META_FIELD = re.compile(r'"(workload|gc|status)"\s*:\s*"([^"]+)"')


# ----- aggregation helpers ---------------------------------------------------

def _mean_ci95(values):
    """Mean and 95% CI half-width (matches archive's gen_mean_and_sd)."""
    if not values:
        return -1, -1
    if len(values) == 1:
        return round(values[0], 3), -1
    var = statistics.variance(values)
    if var <= 0:
        return round(sum(values) / len(values), 3), 0.0
    ci = 1.96 * math.sqrt(var / math.sqrt(len(values)))
    return round(sum(values) / len(values), 3), round(ci, 3)


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


def _parse_ycsb(text, second_op):
    """Return (throughput, runtime_ms, {op: {pNN: float}}) for the run phase.

    Strategy: take the LAST [OVERALL], Throughput value (= run phase) and the
    LAST [READ]/[UPDATE]/[INSERT], pNN value for each percentile. Returns None
    if the run phase wasn't found or didn't include both ops.
    """
    throughputs = _THROUGHPUT.findall(text)
    runtimes = _RUNTIME.findall(text)
    if not throughputs or not runtimes:
        return None
    ops = {}  # {op: {pNN: float}}
    for line in text.splitlines():
        m = _OP_LATENCY.search(line)
        if m:
            op, pct, val = m.group(1), m.group(2), float(m.group(3))
            ops.setdefault(op, {})[pct] = val   # last value wins
    if "READ" not in ops or second_op not in ops:
        return None
    return float(throughputs[-1]), int(runtimes[-1]), ops


def _scan_run(run_dir, second_op):
    """Yield (ir_rate_int, sample_dict) for each successful throttle point."""
    raw_dir = os.path.join(run_dir, "raw")
    if not os.path.isdir(raw_dir):
        return
    for entry in sorted(os.listdir(raw_dir),
                        key=lambda s: int(s) if s.isdigit() else -1):
        sub = os.path.join(raw_dir, entry)
        if not os.path.isdir(sub) or not entry.isdigit():
            continue
        ycsb_path = os.path.join(sub, "ycsb_output.txt")
        if not os.path.exists(ycsb_path):
            continue
        try:
            with open(ycsb_path, errors="replace") as f:
                text = f.read()
        except OSError:
            continue
        parsed = _parse_ycsb(text, second_op)
        if parsed is None:
            continue
        tput, runtime_ms, ops = parsed
        sample = {
            "throughput": tput,
            "runtime_ms": runtime_ms,
            "read_p50": ops["READ"].get("p50"),
            "read_p90": ops["READ"].get("p90"),
            "read_p95": ops["READ"].get("p95"),
            "read_p99": ops["READ"].get("p99"),
            "second_p50": ops[second_op].get("p50"),
            "second_p90": ops[second_op].get("p90"),
            "second_p95": ops[second_op].get("p95"),
            "second_p99": ops[second_op].get("p99"),
        }
        yield int(entry), sample


# ----- CSV emission ----------------------------------------------------------

def _write_csv(out_path, rows, schema, second_op_label):
    header = (
        "timestamp,heapsize,benchname,ccmt,ccet,pcore,loop-time,ir-rate,sample-size,"
        "throughput,±95%,"
        "read-p50(us),±95%,read-p90(us),±95%,read-p95(us),±95%,read-p99(us),±95%,"
        f"{second_op_label}-p50(us),±95%,{second_op_label}-p90(us),±95%,"
        f"{second_op_label}-p95(us),±95%,{second_op_label}-p99(us),±95%"
    )
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        f.write(header + "\n")
        for r in rows:
            cells = [
                "0", schema["heap"], schema["bench"],
                schema["ccmt"], schema["ccet"], schema["pcore"],
                schema["loop_time"], str(r["ir_rate"]), str(r["sample_size"]),
                f"{r['throughput'][0]}", f"{r['throughput'][1]}",
                f"{r['read_p50'][0]}", f"{r['read_p50'][1]}",
                f"{r['read_p90'][0]}", f"{r['read_p90'][1]}",
                f"{r['read_p95'][0]}", f"{r['read_p95'][1]}",
                f"{r['read_p99'][0]}", f"{r['read_p99'][1]}",
                f"{r['second_p50'][0]}", f"{r['second_p50'][1]}",
                f"{r['second_p90'][0]}", f"{r['second_p90'][1]}",
                f"{r['second_p95'][0]}", f"{r['second_p95'][1]}",
                f"{r['second_p99'][0]}", f"{r['second_p99'][1]}",
            ]
            f.write(",".join(cells) + "\n")


def _aggregate(by_ir):
    out = []
    for ir in sorted(by_ir):
        samples = by_ir[ir]

        def col(key):
            vals = [s[key] for s in samples if s.get(key) is not None]
            return _mean_ci95(vals)

        out.append({
            "ir_rate": ir,
            "sample_size": len(samples),
            "throughput": col("throughput"),
            "read_p50": col("read_p50"),
            "read_p90": col("read_p90"),
            "read_p95": col("read_p95"),
            "read_p99": col("read_p99"),
            "second_p50": col("second_p50"),
            "second_p90": col("second_p90"),
            "second_p95": col("second_p95"),
            "second_p99": col("second_p99"),
        })
    return out


# ----- main ------------------------------------------------------------------

def main(argv):
    if len(argv) < 10:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    workload = argv[1]   # hbase-workloada | hbase-readinsert
    gc = argv[2]
    heap_label = argv[3]
    ccmt = argv[4]
    ccet = argv[5]
    pcore = argv[6]
    loop_time = argv[7]  # informational column, not used for parsing
    output_csv = argv[8]
    run_dirs = argv[9:]

    if workload == "hbase-workloada":
        second_op = "UPDATE"
        second_op_label = "update"
    elif workload == "hbase-readinsert":
        second_op = "INSERT"
        second_op_label = "insert"
    else:
        print(f"[err] unknown workload: {workload}", file=sys.stderr)
        sys.exit(2)

    by_ir = {}
    used_runs = 0
    for run_dir in run_dirs:
        meta = _read_meta(run_dir)
        if meta is None:
            continue
        if meta.get("workload") != workload or meta.get("gc") != gc:
            continue
        if meta.get("status") != "completed":
            print(f"[skip] {os.path.basename(run_dir)}: status={meta.get('status')}",
                  file=sys.stderr)
            continue
        used_runs += 1
        for ir, sample in _scan_run(run_dir, second_op):
            by_ir.setdefault(ir, []).append(sample)

    if not by_ir:
        print(f"[warn] no data points for workload={workload} gc={gc}; "
              f"runs scanned={len(run_dirs)} matched={used_runs}",
              file=sys.stderr)

    rows = _aggregate(by_ir)
    schema = {
        "heap": heap_label, "bench": "hbase",
        "ccmt": ccmt, "ccet": ccet, "pcore": pcore,
        "loop_time": loop_time,
    }
    _write_csv(output_csv, rows, schema, second_op_label)
    print(f"[ok] {workload}/{gc}: wrote {len(rows)} rows from {used_runs} runs → "
          f"{output_csv}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
