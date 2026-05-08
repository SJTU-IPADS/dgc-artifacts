#!/usr/bin/env python3
# fig5-plot.py — render Figure 5 (DGC-SHM SPECjbb latency-details).
#
# Reads three CSVs produced by osdi26-scripts/parse-fig5-latency.py from
# plot/fig5-data/:
#   windows.csv   time_s,p50_us,p90_us,p99_us,throughput_kops
#   gc.csv        idx,phase,start_s,end_s   (phase ∈ {mark, compaction})
#   meta.csv      target_ir,run_dir,gid,plot_start_idx,plot_end_idx
#
# Layout matches the archive's fig5-plot-time-window.py:
#   - Top panel: P50 latency (ms) over time
#   - Bottom panel: throughput (kreq/s) over time
#   - Mark / Compaction phases drawn as filled vertical bands
#   - Dashed reference line at the target injection rate on the throughput plot
#
# Outputs:
#   plot/fig5-time-window.png
#   plot/fig5-time-window.pdf

import csv
import os
import sys

import matplotlib.pyplot as plt

PLOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PLOT_DIR, "fig5-data")

# Y-axis caps — both panels share the 0-10 range so latency (ms) and
# throughput (kreq/s) line up visually for comparison.
LATENCY_MS_LIMIT = 10
THROUGHPUT_KOPS_LIMIT = 10


def _read_csv(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        return list(reader)


def main():
    win_path = os.path.join(DATA_DIR, "windows.csv")
    gc_path = os.path.join(DATA_DIR, "gc.csv")
    meta_path = os.path.join(DATA_DIR, "meta.csv")
    for p in (win_path, gc_path, meta_path):
        if not os.path.exists(p):
            print(f"[err] missing {p} — run osdi26-scripts/analyze-fig5.sh first.",
                  file=sys.stderr)
            sys.exit(1)

    windows = _read_csv(win_path)
    gc_bands = _read_csv(gc_path)
    meta = _read_csv(meta_path)[0]

    target_ir = int(meta["target_ir"])
    target_kops = target_ir / 1000.0

    times = [float(w["time_s"]) for w in windows]
    p50_ms = [float(w["p50_us"]) / 1000 for w in windows]
    kops = [float(w["throughput_kops"]) for w in windows]
    if not times:
        print("[err] windows.csv is empty", file=sys.stderr)
        sys.exit(2)

    x_min = times[0]
    x_max = times[-1]
    throughput_limit = THROUGHPUT_KOPS_LIMIT

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(10, 4), sharex=True,
        gridspec_kw={"height_ratios": [1, 1]},
    )

    tab20b = plt.get_cmap("tab20b").colors
    tab20c = plt.get_cmap("tab20c").colors
    line_color = tab20b[13]   # warm reddish brown — matches archive
    mark_color = tab20c[1]    # mark band — peach
    compaction_color = tab20c[7]  # compaction band — slate blue

    # Top: latency
    ax1.plot(times, p50_ms, color=line_color, linewidth=2)
    ax1.set_ylabel("Latency\n(ms)", color="black", fontsize=20)
    ax1.yaxis.set_label_coords(-0.06, 0.5)
    ax1.set_ylim(0, LATENCY_MS_LIMIT)
    ax1.set_xlim(x_min, x_max)
    ax1.spines["bottom"].set_visible(True)
    ax1.xaxis.set_ticks_position("none")
    ax1.xaxis.tick_top()
    ax1.set_yticks([0, 5, 10])
    ax1.tick_params(axis="y", labelsize=18)

    # Bottom: throughput
    ax2.plot(times, kops, color=line_color, linewidth=2)
    ax2.axhline(y=target_kops, color="black", linestyle="--", linewidth=2, alpha=0.6)
    text_x = x_min + (x_max - x_min) * 0.83
    ax2.text(text_x, target_kops * 1.02, "Injection\n  Rate",
             color="black", fontsize=18, ha="left", va="bottom")
    ax2.set_ylabel("Throughput\n(kreq/s)", color="black", fontsize=20)
    ax2.yaxis.set_label_coords(-0.06, 0.5)
    ax2.tick_params(axis="y", labelsize=18)
    ax2.tick_params(axis="x", labelsize=18)
    ax2.set_yticks([0, 5, 10])
    ax2.set_xlabel("Time (s)", color="black", fontsize=20)
    ax2.spines["top"].set_visible(True)
    ax2.xaxis.tick_bottom()
    ax2.set_ylim(0, throughput_limit)
    ax2.set_xlim(x_min, x_max)

    # GC phase bands
    first_mark = True
    first_comp = True
    for band in gc_bands:
        start = float(band["start_s"])
        end = float(band["end_s"])
        if band["phase"] == "mark":
            label = "Mark" if first_mark else None
            ax1.fill_betweenx([0, LATENCY_MS_LIMIT], start, end,
                              color=mark_color, alpha=0.8, label=label)
            ax2.fill_betweenx([0, throughput_limit], start, end,
                              color=mark_color, alpha=0.8)
            first_mark = False
        else:  # compaction
            label = "Compaction" if first_comp else None
            ax1.fill_betweenx([0, LATENCY_MS_LIMIT], start, end,
                              color=compaction_color, alpha=0.8, label=label)
            ax2.fill_betweenx([0, throughput_limit], start, end,
                              color=compaction_color, alpha=0.8)
            first_comp = False

    legend = ax1.legend(
        loc="upper center", bbox_to_anchor=(0.5, 1.35),
        ncol=2, frameon=False, prop={"size": 18},
    )
    for handle in legend.legend_handles:
        handle.set_linewidth(1.0)
        handle.set_edgecolor("black")

    png_path = os.path.join(PLOT_DIR, "fig5-time-window.png")
    pdf_path = os.path.join(PLOT_DIR, "fig5-time-window.pdf")
    plt.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0)
    plt.savefig(pdf_path, dpi=300, format="pdf", bbox_inches="tight", pad_inches=0)
    print(f"[ok] wrote {png_path}")
    print(f"[ok] wrote {pdf_path}")


if __name__ == "__main__":
    main()
