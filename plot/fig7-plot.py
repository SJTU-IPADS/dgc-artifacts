#!/usr/bin/env python3
# fig7-plot.py — render Figure 7 as a multi-axis bar chart matching the
# paper layout (one bar group per GC variant, three stacked metrics).
#
# Reads three narrow CSVs from plot/fig7-data/ (produced by
# osdi26-scripts/analyze-fig7.sh -> parse-fig7-preset.py):
#   p99.csv              type,p99_ms
#   memory_usage.csv     type,memory_gb
#   rdma_transport.csv   type,rdma_gb
#
# All three CSVs share the same `type` column ordering — one row per
# tested config. Labels: G1, Shen, D-Shm, D-RDMA 1/1, D-RDMA 3/4, ...
#
# Output: plot/fig7-multi-axis.{png,pdf}

import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

PLOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PLOT_DIR, "fig7-data")


def main():
    p99_path = os.path.join(DATA_DIR, "p99.csv")
    mem_path = os.path.join(DATA_DIR, "memory_usage.csv")
    rdma_path = os.path.join(DATA_DIR, "rdma_transport.csv")
    for p in (p99_path, mem_path, rdma_path):
        if not os.path.exists(p):
            print(f"[err] missing {p} — run osdi26-scripts/analyze-fig7.sh first.",
                  file=sys.stderr)
            sys.exit(1)

    p99_df = pd.read_csv(p99_path)
    mem_df = pd.read_csv(mem_path)
    rdma_df = pd.read_csv(rdma_path)

    types = p99_df["type"].values
    p99_values = p99_df.iloc[:, 1].values
    mem_values = mem_df.iloc[:, 1].values
    rdma_values = rdma_df.iloc[:, 1].values

    fig, ax1 = plt.subplots(figsize=(10, 4.5))
    tab20b_colors = plt.get_cmap("tab20b").colors
    tab20c_colors = plt.get_cmap("tab20c").colors
    colors = [tab20c_colors[4], tab20c_colors[0], tab20c_colors[8], tab20b_colors[12]]

    # First y-axis: P99 (ms)
    color1 = colors[3]
    ax1.set_ylabel("P99 (ms) ●", color=color1, fontsize=20)
    ax1.plot(types, p99_values, color=color1, marker="o",
             linewidth=2, markersize=14, label="P99 (ms)")
    ax1.tick_params(axis="y", labelcolor=color1, labelsize=20, pad=0)
    ax1.tick_params(axis="x", labelsize=20, rotation=45)
    ax1.set_ylim(0, 80)

    # Second y-axis: Mem Usg (GB)
    ax2 = ax1.twinx()
    color2 = colors[2]
    ax2.set_ylabel("Mem Usg (GB) ■", color=color2, fontsize=20)
    ax2.plot(types, mem_values, color=color2, marker="s",
             linewidth=2, markersize=14, label="Mem Usg (GB)")
    ax2.tick_params(axis="y", labelcolor=color2, labelsize=20, pad=0)
    ax2.set_yticks([0, 4, 8, 12])

    # Third y-axis: RDMA Traffic (GB) — offset outward to avoid overlap.
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("outward", 60))
    color3 = colors[1]
    ax3.set_ylabel("RDMA Traffic (GB) ▲", color=color3, fontsize=20)
    ax3.plot(types, rdma_values, color=color3, marker="^",
             linewidth=2, markersize=14, label="RDMA Traffic")
    ax3.tick_params(axis="y", labelcolor=color3, labelsize=20)
    ax3.set_ylim(0, 25)

    ax1.set_xticks(range(len(types)))
    ax1.set_xticklabels(types)

    # Vertical separator between Shen (baseline) and D-Shm (DGC).
    if "D-Shm" in list(types) and "Shen" in list(types):
        shm_index = list(types).index("D-Shm")
        shen_index = list(types).index("Shen")
        middle = (shen_index + shm_index) / 2.0
        ax1.axvline(x=middle, color="black", linestyle="--", linewidth=2)

    plt.tight_layout()
    png_path = os.path.join(PLOT_DIR, "fig7-multi-axis.png")
    pdf_path = os.path.join(PLOT_DIR, "fig7-multi-axis.pdf")
    plt.savefig(png_path, dpi=300, bbox_inches="tight")
    plt.savefig(pdf_path, bbox_inches="tight")
    print(f"[ok] wrote {png_path}")
    print(f"[ok] wrote {pdf_path}")


if __name__ == "__main__":
    main()
