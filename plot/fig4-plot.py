#!/usr/bin/env python3
"""Plot Figure 4 (SPECjbb2015 P99 latency curve & critical-jOPS) for the
fig4-run.sh sweep. Style mirrors the paper figure
pics/specjbb-rt-diffgc/asplos26-specjbb-P99-rt-curve-logscale.pdf.

Usage:
    python fig4-plot.py [--results-dir <artifact>/results/${USER}/fig4-result] \
                       [--out fig4-specjbb-p99-rt-curve.pdf]
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys

import matplotlib.pyplot as plt


GC_ORDER = ["g1", "shenandoah", "dgc-shm", "dgc-rdma"]
GC_LABEL = {
    "g1": "G1",
    "shenandoah": "Shen",
    "dgc-shm": "DGC-SHM",
    "dgc-rdma": "DGC-RDMA",
}


def read_rt_curve(path):
    """Return (jops, p99us, critical_jops, max_jops) for a SPECjbb rt-curve txt."""
    jops, p99 = [], []
    critical = max_j = None
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if line.startswith("Domain Marker;critical-jOPS;"):
                critical = float(line.split(";")[2])
                continue
            if line.startswith("Domain Marker;max-jOPS;"):
                max_j = float(line.split(";")[2])
                continue
            if line.startswith("jOPS;") or "===" in line:
                continue
            parts = line.split(";")
            if len(parts) < 7:
                continue
            try:
                ir = float(parts[0])
                p99us = float(parts[5])
            except ValueError:
                continue
            if ir == 0.0 or p99us != p99us:
                continue
            jops.append(ir)
            p99.append(p99us)
    return jops, p99, critical, max_j


def find_rt_curve_file(run_dir):
    """Return path to overall-throughput-rt.txt under a run dir, or None."""
    candidates = glob.glob(
        os.path.join(
            run_dir,
            "raw",
            "specjbb*",
            "report-*",
            "data",
            "rt-curve",
            "specjbb*-overall-throughput-rt.txt",
        )
    )
    return candidates[0] if candidates else None


def detect_gc(run_dir_name):
    """Pick GC name out of a run dir like
    20260430_003005_specjbb-hbir_<gc>_fig4-..._${USER}."""
    m = re.search(r"specjbb-hbir_([^_]+(?:-[^_]+)?)_fig4", run_dir_name)
    if not m:
        return None
    gc = m.group(1)
    return gc if gc in GC_LABEL else None


def collect(results_dir):
    """Return {gc: (jops, p99, crit, maxj, run_dir)} for the latest run per GC."""
    runs = {}
    for entry in sorted(os.listdir(results_dir)):
        run_dir = os.path.join(results_dir, entry)
        if not os.path.isdir(run_dir):
            continue
        gc = detect_gc(entry)
        if gc is None:
            continue
        rt_file = find_rt_curve_file(run_dir)
        if rt_file is None:
            print(f"[warn] no rt-curve file under {run_dir}", file=sys.stderr)
            continue
        jops, p99, crit, maxj = read_rt_curve(rt_file)
        if not jops:
            print(f"[warn] empty rt-curve {rt_file}", file=sys.stderr)
            continue
        runs[gc] = (jops, p99, crit, maxj, run_dir)
    return runs


def plot(runs, out_path):
    plt.rc("font", size=16)
    plt.rc("axes", titlesize=16, labelsize=16, labelweight="bold")
    plt.rc("xtick", labelsize=14)
    plt.rc("ytick", labelsize=14)
    plt.rc("legend", fontsize=14)
    plt.rc("lines", linewidth=1.5)

    tab20c = plt.get_cmap("tab20c").colors
    tab20b = plt.get_cmap("tab20b").colors
    palette = {
        "dgc-rdma": tab20c[4],
        "dgc-shm": tab20c[0],
        "g1": tab20c[8],
        "shenandoah": tab20b[12],
    }
    markers = {
        "dgc-rdma": "v",
        "dgc-shm": "$*$",
        "g1": "x",
        "shenandoah": "+",
    }

    fig, ax = plt.subplots(figsize=(8, 4.5))

    for gc in GC_ORDER:
        if gc not in runs:
            continue
        jops, p99, crit, maxj, _ = runs[gc]
        x = [j / 1000.0 for j in jops]
        y = [p / 1000.0 for p in p99]
        ax.plot(
            x,
            y,
            label=GC_LABEL[gc],
            color=palette[gc],
            marker=markers[gc],
            markersize=12,
            markevery=max(1, len(x) // 8),
            markerfacecolor="none",
            markeredgewidth=2,
            linewidth=2.0 if gc == "dgc-rdma" else 1.5,
        )
        if crit is not None:
            ax.axvline(
                crit / 1000.0,
                color=palette[gc],
                linestyle=":",
                linewidth=1,
                alpha=0.7,
            )

    ax.set_xlabel("Throughput (kjOPS)")
    ax.set_ylabel("P99 Response Time (ms)")
    ax.set_xlim(0, 22)
    ax.set_yscale("log")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(ncol=2, loc="upper left", frameon=True)

    plt.tight_layout()
    plt.savefig(out_path, format="pdf", dpi=300)
    png_path = os.path.splitext(out_path)[0] + ".png"
    plt.savefig(png_path, format="png", dpi=200)
    print(f"wrote {out_path}")
    print(f"wrote {png_path}")


def main():
    ap = argparse.ArgumentParser()
    _ae_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument(
        "--results-dir",
        default=os.path.join(_ae_dir, "results",
                             os.environ.get("USER", "anonymous"),
                             "fig4-result"),
        help="Directory containing fig4 run dirs",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="Output PDF path (default: ./fig4-specjbb-p99-rt-curve.pdf next to script)",
    )
    args = ap.parse_args()

    out = args.out
    if out is None:
        out = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "fig4-specjbb-p99-rt-curve.pdf",
        )

    runs = collect(args.results_dir)
    if not runs:
        print(f"[error] no usable run dirs in {args.results_dir}", file=sys.stderr)
        sys.exit(1)
    print("Collected runs:")
    for gc in GC_ORDER:
        if gc in runs:
            _, _, crit, maxj, run_dir = runs[gc]
            print(
                f"  {gc:<10}  critical-jOPS={crit:>7.0f}  max-jOPS={maxj:>7.0f}  "
                f"{os.path.basename(run_dir)}"
            )
    plot(runs, out)


if __name__ == "__main__":
    main()
