import pandas as pd
import matplotlib.pyplot as plt


def _safe_read_csv(path):
    """Return DataFrame or None if path is missing / unreadable / empty."""
    import os
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
    except (pd.errors.EmptyDataError, OSError):
        return None
    if df.empty or 'ccmt' not in df.columns:
        return None
    return df


def plot_hbase_curve(ax, plot_target, workload_name, colors, markers):
    name_suffix = ""
    if workload_name == "hbase-read-insert-half-workload":
        name_suffix = "read_insert_half_workload"
    elif workload_name == "hbase-workloada":
        name_suffix = "workloada_2host"
    shenandoah_file_path = f'./{workload_name}-data/shenandoah-{name_suffix}-output.csv'
    rdma_dgc_file_path = f'./{workload_name}-data/rdma-dgc-{name_suffix}-output.csv'
    shm_dgc_file_path = f'./{workload_name}-data/shm-dgc-{name_suffix}-output.csv'
    g1gc_file_path = f'./{workload_name}-data/g1gc-{name_suffix}-output.csv'
    target_col = plot_target + "(us)"

    # Skip GC variants whose CSV is missing / empty / malformed (e.g. an
    # AE evaluator who only ran a subset). The panel still renders with
    # whatever GCs DID produce data.
    shenandoah_data = _safe_read_csv(shenandoah_file_path)
    rdma_dgc_data   = _safe_read_csv(rdma_dgc_file_path)
    shm_dgc_data    = _safe_read_csv(shm_dgc_file_path)
    g1gc_data       = _safe_read_csv(g1gc_file_path)

    shenandoah_ccmt_values = shenandoah_data['ccmt'].unique() if shenandoah_data is not None else []
    rdma_dgc_ccmt_values   = rdma_dgc_data['ccmt'].unique()   if rdma_dgc_data   is not None else []
    shm_dgc_ccmt_values    = shm_dgc_data['ccmt'].unique()    if shm_dgc_data    is not None else []
    g1gc_ccmt_values       = g1gc_data['ccmt'].unique()       if g1gc_data       is not None else []

    for i, ccmt in enumerate(rdma_dgc_ccmt_values):
        subset = rdma_dgc_data[rdma_dgc_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='DGC-RDMA',
            color=colors[0],
            linewidth=2,
            marker=markers[0],
            markerfacecolor='none',
            markersize=12,
            markeredgewidth=1,
        )

    for i, ccmt in enumerate(shm_dgc_ccmt_values):
        subset = shm_dgc_data[shm_dgc_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='DGC-SHM',
            color=colors[1],
            linewidth=2,
            marker=markers[1],
            markersize=12,
            markeredgewidth=1,
        )

    for i, ccmt in enumerate(g1gc_ccmt_values):
        subset = g1gc_data[g1gc_data['ccmt'] == ccmt]
        sorted_subset = subset.sort_values('throughput')[['throughput', target_col]]
        ax.plot(
            sorted_subset['throughput'] / 1000,
            sorted_subset[target_col] / 1000,
            label='G1',
            color=colors[2],
            linewidth=2,
            linestyle='--',
        )

    for i, ccmt in enumerate(shenandoah_ccmt_values):
        subset = shenandoah_data[shenandoah_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='Shen',
            color=colors[3],
            linewidth=2,
        )

    ax.set_xlabel('RPS (kops/sec)')
    label_text = plot_target.replace("update", "Update").replace("p99", "P99").replace("insert", "Insert").replace("read", "Read")
    # Move what would be the y-axis label up to the top of the subplot,
    # overlapping the top spine. We do this by drawing a text box at
    # axes coords (0.5, 1.05) instead of calling set_ylabel(), so the
    # label sits over the top edge of the panel.
    ax.text(
        0.5,
        1.05,
        f'{label_text} (ms)',
        transform=ax.transAxes,
        ha='center',
        va='center',
        fontsize=plt.rcParams.get('axes.labelsize', 20),
        bbox={
            'facecolor': 'white',
            'alpha': 1.0,
            'edgecolor': 'black',
            'linewidth': 1.0,
            'pad': 4,
        },
    )
    # ax.grid(True, linestyle='--')
    ax.set_yscale('log')


def plot_h2_tradesoap_curve(ax, data_dir, colors, markers):
    shenandoah_file_path = f'{data_dir}/shenandoah-output.csv'
    rdma_dgc_file_path = f'{data_dir}/rdma-dgc-output.csv'
    shm_dgc_file_path = f'{data_dir}/shm-dgc-output.csv'
    g1gc_file_path = f'{data_dir}/g1gc-output.csv'
    target_col = 'p99'

    shenandoah_data = _safe_read_csv(shenandoah_file_path)
    rdma_dgc_data   = _safe_read_csv(rdma_dgc_file_path)
    shm_dgc_data    = _safe_read_csv(shm_dgc_file_path)
    g1gc_data       = _safe_read_csv(g1gc_file_path)

    shenandoah_ccmt_values = shenandoah_data['ccmt'].unique() if shenandoah_data is not None else []
    rdma_dgc_ccmt_values   = rdma_dgc_data['ccmt'].unique()   if rdma_dgc_data   is not None else []
    shm_dgc_ccmt_values    = shm_dgc_data['ccmt'].unique()    if shm_dgc_data    is not None else []
    g1gc_ccmt_values       = g1gc_data['ccmt'].unique()       if g1gc_data       is not None else []

    for i, ccmt in enumerate(rdma_dgc_ccmt_values):
        subset = rdma_dgc_data[rdma_dgc_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='DGC-RDMA',
            color=colors[0],
            linewidth=2,
            marker=markers[0],
            markerfacecolor='none',
            markersize=12,
            markeredgewidth=1,
        )

    for i, ccmt in enumerate(shm_dgc_ccmt_values):
        subset = shm_dgc_data[shm_dgc_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='DGC-SHM',
            color=colors[1],
            linewidth=2,
            marker=markers[1],
            markersize=12,
            markeredgewidth=1,
        )

    for i, ccmt in enumerate(g1gc_ccmt_values):
        subset = g1gc_data[g1gc_data['ccmt'] == ccmt]
        sorted_subset = subset.sort_values('throughput')[['throughput', target_col]]
        ax.plot(
            sorted_subset['throughput'] / 1000,
            sorted_subset[target_col] / 1000,
            label='G1',
            color=colors[2],
            linewidth=2,
            linestyle='--',
        )

    for i, ccmt in enumerate(shenandoah_ccmt_values):
        subset = shenandoah_data[shenandoah_data['ccmt'] == ccmt]
        subset = subset.sort_values('throughput')
        ax.plot(
            subset['throughput'] / 1000,
            subset[target_col] / 1000,
            label='Shen',
            color=colors[3],
            linewidth=2,
        )

    ax.set_xlabel('RPS (kops/sec)')
    # Same trick as plot_hbase_curve: place the y-axis label at the top of the panel
    ax.text(
        0.5,
        1.05,
        'P99 (ms)',
        transform=ax.transAxes,
        ha='center',
        va='center',
        fontsize=plt.rcParams.get('axes.labelsize', 20),
        bbox={
            'facecolor': 'white',
            'alpha': 1.0,
            'edgecolor': 'black',
            'linewidth': 1.0,
            'pad': 4,
        },
    )
    # ax.grid(True, linestyle='--')
    ax.set_yscale('log')


if __name__ == "__main__":
    plt.rc('font', size=20)
    plt.rc('axes', titlesize=20)
    plt.rc('axes', labelsize=20)
    plt.rc('xtick', labelsize=16)
    plt.rc('ytick', labelsize=16)
    plt.rc('legend', fontsize=18)
    plt.rc('lines', linewidth=2)

    # HBase configs (the first four subplots)
    hbase_configs = [
        ('read-p99', 'hbase-read-insert-half-workload'),
        ('insert-p99', 'hbase-read-insert-half-workload'),
        ('read-p99', 'hbase-workloada'),
        ('update-p99', 'hbase-workloada'),
    ]

    # H2 and Tradesoap configs (the last two subplots)
    h2_tradesoap_configs = [
        'h2-data',
        'tradesoap-data',
    ]

    # Six subplots in one row, kept tightly spaced
    fig, axes = plt.subplots(1, 6, figsize=(20, 4.5))
    tab20c_colors = plt.get_cmap('tab20c').colors
    tab20b_colors = plt.get_cmap('tab20b').colors
    colors = [tab20c_colors[4], tab20c_colors[0], tab20c_colors[8], tab20b_colors[12]]
    markers = ["v", "x", "$*$", "|"]

    legend_handles = None
    legend_labels = None

    # First four subplots: HBase
    for i, (plot_target, workload_name) in enumerate(hbase_configs):
        plot_hbase_curve(axes[i], plot_target, workload_name, colors, markers)
        if legend_handles is None:
            legend_handles, legend_labels = axes[i].get_legend_handles_labels()

    # Last two subplots: H2 and Tradesoap
    for i, data_dir in enumerate(h2_tradesoap_configs):
        plot_h2_tradesoap_curve(axes[4 + i], data_dir, colors, markers)

    # Tradesoap subplot: only show y-ticks at 10^0 and 10^1
    axes[5].set_yticks([1, 10])

    # Shared legend across the top, no background frame
    fig.legend(legend_handles, legend_labels, loc='upper center', ncol=4, frameon=False)

    # Subplot group captions
    fig.text(0.1667, 0.02, '(a) HBase Read-Insert Test', ha='center', fontsize=24)
    fig.text(0.5, 0.02, '(b) HBase Read-Update Test', ha='center', fontsize=24)
    fig.text(0.75, 0.02, '(c) H2', ha='center', fontsize=24)
    fig.text(0.9167, 0.02, '(d) Daytrader', ha='center', fontsize=24)

    plt.tight_layout(rect=[0, 0.05, 1, 0.92])
    # Tighten horizontal spacing after tight_layout so it actually takes effect
    fig.subplots_adjust(wspace=0.2)
    fig.savefig('fig6-combined-rt-curve.pdf', format='pdf', dpi=300, bbox_inches='tight', pad_inches=0)
    fig.savefig('fig6-combined-rt-curve.png', format='png', dpi=150, bbox_inches='tight', pad_inches=0.05)
