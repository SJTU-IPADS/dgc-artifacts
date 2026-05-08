# OSDI'26 - DGC - Artifact Evaluation

This repository is the self-contained artifact evaluation tree for the OSDI'26
paper *Shaving the Peaks: Taming Tail Latency for Managed Workloads via
Disaggregated Garbage Collection*. It reproduces the figures and tables of the
paper by running the DGC-enabled OpenJDK 17 fork (bundled under
`jdk17-snic-gc/`) against SPECjbb2015, HBase + YCSB, and DaCapo workloads
under the four GC variants compared in the paper (G1, Shenandoah, DGC-SHM,
DGC-RDMA).

The paper PDF accompanying this artifact is dropped in at the repo root as
`osdi26-dgc.pdf` for cross-reference.

## 1. Repository Layout

```
osdi26-dgc-artifacts/                   ← This repo (AE workspace)
├── README.md                           This file
├── osdi26-dgc.pdf                      Paper PDF (camera-ready)
├── run.sh                              Unified test entry point
├── env.sh                              Default environment + config layering
├── env.d/                              Per-machine / per-user overrides
│   ├── ds00.env                          example machine-level override (auto-loaded by hostname)
│   └── (drop env.d/$(whoami).env here for user-level overrides)
├── conf/
│   ├── workloads/                      Workload knobs (heap, throttles, IRs, ...)
│   │   ├── h2.conf                       fig6 H2 TPC-C
│   │   ├── tradesoap-vlarge-640.conf     fig6 DayTrader (DaCapo tradesoap, vlarge)
│   │   ├── hbase-workloada.conf          fig6 HBase + YCSB workload-A
│   │   ├── hbase-readinsert.conf         fig6 HBase + YCSB read-insert-half
│   │   ├── specjbb-preset.conf           fig5 / fig7 SPECjbb PRESET
│   │   └── specjbb-hbir.conf             fig4 SPECjbb HBIR_RT
│   └── gc/                             GC profile knobs (CCMT, ParallelGC, flags)
│       ├── g1.conf
│       ├── shenandoah.conf
│       ├── dgc-shm.conf
│       └── dgc-rdma.conf
├── lib/                                Shared shell libraries
│   ├── common.sh                         logging / utility helpers
│   ├── cpu.sh                            CPU pinning + NUMA helpers
│   ├── isolation.sh                      /dev/shm + port-namespace isolation
│   ├── metadata.sh                       META.json + commands.log writers
│   ├── runner-baseline.sh                G1 / Shenandoah single-JVM runner
│   ├── runner-dgc.sh                     Coordinator + Client + Host orchestrator
│   ├── workload-{specjbb,hbase,dacapo}.sh  per-suite runners
│   └── analyze-{dacapo,specjbb}.sh       post-hoc analyzers
├── osdi26-scripts/                     Per-figure / per-table top-level drivers
│   ├── fig4/                             Figure 4 (SPECjbb HBIR_RT, 4 GCs)
│   │   └── fig4-run.sh
│   ├── fig5/                             Figure 5 (DGC-SHM SPECjbb latency details)
│   │   ├── fig5-run.sh
│   │   ├── analyze-fig5.sh
│   │   └── parse-fig5-latency.py
│   ├── fig6/                             Figure 6 (HBase / H2 / DayTrader RT-curves)
│   │   ├── fig6-h2-run.sh
│   │   ├── fig6-hbase-run.sh
│   │   ├── fig6-tradesoap-run.sh
│   │   ├── analyze-fig6-{all,h2,hbase,tradesoap}.sh
│   │   └── parse-fig6-{dacapo,hbase}.py
│   ├── fig7/                             Figure 7 (DGC-RDMA cache-size sweep)
│   │   ├── fig7-run.sh
│   │   ├── analyze-fig7.sh
│   │   └── parse-fig7-preset.py
│   └── table3/                           Table 3 (SPECjbb + HBase mix workload)
│       ├── table3-run.sh
│       ├── table3-analyze.sh
│       └── inner/                          Per-GC inner runner scripts + SPECjbb props
│           ├── table3-shenandoah-...-run.sh
│           ├── table3-dgc-shm-...-run.sh
│           ├── table3-dgc-rdma-...-run.sh
│           └── config/                       SPECjbb 2-group config + templates
├── hbase-conf/                         HBase + YCSB drop-in config
│   ├── test.txt                          HBase shell script: create the YCSB table
│   ├── bin/                              custom self-{master,regionserver,zookeeper}-start
│   │                                       + specjbb-hbase-mix-{master,regionserver}-start
│   ├── conf/                             hbase-env.sh, hbase-site.xml, regionservers
│   └── ycsb-workloads/                   workloada_2host, read_insert_half_workload
├── plot/                               Plotting scripts (consume CSVs in plot/<figX>-data/)
│   ├── fig4-plot.py
│   ├── fig5-plot.py
│   ├── fig6-plot.py
│   └── fig7-plot.py
└── jdk17-snic-gc/                      Bundled DGC OpenJDK 17 fork (source)
    ├── build-with-ortools.sh             configure wrapper (calls ./configure)
    ├── src/, make/, doc/, ...            standard OpenJDK 17 layout
    └── README.md
```

External dependencies — **not** bundled in this repo, must be present on the
test machine as **siblings of this artifact's parent directory**:

```
../jdk17-snic-gc-prebuilt/jdk/     Built DGC JDK image (rsync target)
../specjbb-1.0.4/                  SPECjbb2015 + latency-instrumented JAR
../hbase-test/                     HBase 2.5.11 + YCSB 0.18.0 (with patches)
../dacapo-test/                    DaCapo 23.11-chopin + throttle JARs
../or-tools_x86_64_…/              Google OR-Tools v9.10 (CP-SAT solver)
../jdk-17.0.7+7/                   Boot JDK used to build the DGC JDK
```

## 2. Hardware & Software Prerequisites

- **Hardware** (paper-fair core budget assumes a NUMA Intel x86_64 server):
  - CPU: dual-socket Intel Xeon Gold 6430 (32 physical cores per socket, HT
    disabled); even cores → NUMA node 0, odd cores → NUMA node 1.
  - Memory: 128 GB RAM (≥ 32 GB free per JVM is recommended).
  - Network: 200 Gbps NVIDIA BlueField-3 DPU (or any RoCEv2-capable RDMA NIC)
    is required for the DGC-RDMA experiments. The control plane is over
    Ethernet, the data plane is over RDMA.
  - Disk: ~200 GB free for the DGC JDK build, prebuilt dependencies, and
    per-run result archives.

- **Operating System**: Ubuntu 22.04 LTS. The build/run path has only been
  tested on this distribution; other Linux distributions are likely to work
  with minor dependency adjustments.

- **Workloads** (versions are pinned by the prebuilt dependency tree, see
  Section 3.3):
  - SPECjbb2015 v1.0.4 (HBIR_RT and PRESET modes).
  - HBase 2.5.11 + YCSB 0.18.0 (workloada read/update; read-insert-half).
  - DaCapo 23.11-chopin with throttle patches (H2 TPC-C, DayTrader
    `tradesoap`/`tradebeans`, plus the latency-critical sweep family
    `lusearch`/`tomcat`/`spring`/`kafka`/`cassandra`/`jme`).

If your team does not have a machine that meets the hardware requirements
(particularly the 200 Gbps RDMA NIC for fig7 / DGC-RDMA), see
**Section 7** for how to request access to the maintainers' test machine.

## 3. Setting Up the Environment Locally

The steps below walk through a clean install on your own host. Every command
below runs as a regular user; only the system-level package install (3.1)
and the optional system tuning (3.7) require `sudo`.

### 3.1 Install OS Dependencies

The DGC JDK build needs a boot JDK (HotSpot 17), Google OR-Tools (the CP-SAT
scheduler is linked into HotSpot), and the standard OpenJDK build toolchain.
On Ubuntu 22.04:

```shell
# Build toolchain (matches OpenJDK 17 build requirements)
sudo apt-get update
sudo apt-get install -y build-essential autoconf zip unzip git \
    libfreetype-dev libfontconfig1-dev libcups2-dev libx11-dev libxext-dev \
    libxrender-dev libxrandr-dev libxtst-dev libxt-dev libasound2-dev \
    libffi-dev pkg-config
# Python (used by analyzers/plotters)
sudo apt-get install -y python3 python3-pip
python3 -m pip install --user pandas matplotlib numpy
# RDMA stack (only needed for DGC-RDMA)
sudo apt-get install -y rdma-core libibverbs-dev librdmacm-dev ibverbs-utils
```

Verify the RDMA NIC and that RoCE v2 is enabled:

```shell
ibv_devices                # should list a Mellanox / BlueField device
rdma link                  # should show ACTIVE state
ibstatus                   # should show "rate: 200 Gb/sec" if BlueField-3
```

### 3.2 Get the Artifact

Pick a parent directory for the AE workspace (this becomes the
`<shared-base>` referenced throughout) and clone the repo into it:

```shell
mkdir -p ~/dgc-ae && cd ~/dgc-ae
git clone git@ipads.se.sjtu.edu.cn:liyh/osdi26-dgc-artifacts.git
cd osdi26-dgc-artifacts
```

The bundled `jdk17-snic-gc/` is the OpenJDK 17 source already on the
flex-CCMT snapshot used for the paper, checked into this repo directly —
there is no submodule fetch step.

### 3.3 Lay Out External Dependencies

The framework expects the DGC JDK build and the benchmark suites to live
as **siblings** of the artifact root, all under the same parent:

```text
<shared-base>/
├── osdi26-dgc-artifacts/            # this repo (cloned in 3.2)
├── jdk17-snic-gc-prebuilt/jdk       # built DGC JDK image (created in 3.4)
├── specjbb-1.0.4/                   # SPECjbb2015 + latency JAR
├── hbase-test/                      # HBase 2.5.11 + YCSB 0.18.0
│   ├── multi-regionserver-hbase-0/
│   ├── rdma-multi-regionserver-hbase/
│   └── ycsb-0.18.0/
├── dacapo-test/                     # DaCapo 23.11-chopin + throttle JARs
├── or-tools_x86_64_Ubuntu-22.04_cpp_v9.10.4067/    # Google OR-Tools (CP-SAT)
└── jdk-17.0.7+7/                    # Boot JDK (used to build the DGC JDK)
```

`env.sh` resolves all dependency paths through `${_AE_DIR}/../...`, so as
long as the artifact and its siblings sit under the same parent, the
defaults work out of the box. If your local layout differs, override
individual paths in `env.d/` (Section 3.6).

**Sourcing each dependency:**

- **OR-Tools**: download `or-tools_amd64_ubuntu-22.04_cpp_v9.10.4067.tar.gz`
  from <https://github.com/google/or-tools/releases/tag/v9.10>, untar into
  `<shared-base>/`, and verify the directory name matches what `env.sh`
  expects (or override `ORTOOLS_DIR` in your `env.d/`).

- **Boot JDK**: download Eclipse Temurin 17 (e.g.
  `OpenJDK17U-jdk_x64_linux_hotspot_17.0.7_7.tar.gz`) from
  <https://adoptium.net/temurin/releases/?version=17> and untar into
  `<shared-base>/`.

- **SPECjbb2015 v1.0.4** — *closed-source commercial software, must be
  prepared by you*. SPECjbb is licensed by SPEC and cannot be
  redistributed in this artifact. To obtain a copy, purchase / request a
  license through the SPEC website (<https://www.spec.org/jbb2015/>),
  unpack the v1.0.4 ISO into `<shared-base>/specjbb-1.0.4/`, and confirm
  `specjbb2015.jar` exists at the top level of that directory. The
  latency-instrumented build of `specjbb2015.jar` used by Figures 4 / 5 / 7
  (which records per-request start time + latency) is a small in-house
  patch on top of the SPEC release; once you can show proof of your SPEC
  license, contact the maintainers (Section 7) for the patched JAR.

- **DaCapo 23.11-chopin throttle fork** — *built from source already in
  this repo*. The full DaCapo source with the throttle-chasing fork
  applied is bundled under `dacapo-bench/` (no external download needed).
  Build it locally with the steps below:

    **(a) Install the two extra JDKs the DaCapo build requires.** Note
    these are *separate* from the boot JDK 17 used to build the DGC JDK:

    - **JDK 8** — DaCapo's harness builds at JDK 8 source level. You need
      a real JDK (with `javac`), **not** a JRE. The Ubuntu package
      `openjdk-8-jre-headless` is JRE-only and won't work.
      Recommended: download the portable Eclipse Temurin 8 build
      `OpenJDK8U-jdk_x64_linux_hotspot_8u482b08.tar.gz` from
      <https://adoptium.net/temurin/releases/?version=8> and untar to a
      directory you control (e.g. `/opt/jdk-8/`).
    - **JDK 11** — required because `jme`, `kafka`, `pmd`, `tomcat`,
      `tradebeans`, and `tradesoap` need JDK 11 source level for their
      ant subprojects. Same source: Eclipse Temurin 11 from Adoptium.

    Also install `ant` and `git` if not already present:

    ```shell
    sudo apt-get install -y ant git
    ```

    **(b) Tell the build where JDK 11 lives.** Edit (or create)
    `dacapo-bench/benchmarks/local.properties` and add the line:

    ```properties
    jdk.11.home=/opt/jdk-11        # absolute path to your JDK-11 install
    ```

    Without this, the `tradesoap` / `tradebeans` builds fail. (See
    `dacapo-bench/README.md` Section "Building" point 2.)

    **(c) Run the build.** Set `JAVA_HOME` to the JDK 8 install and
    invoke `ant dist`:

    ```shell
    export JAVA_HOME=/opt/jdk-8                # JDK 8, not JRE
    export PATH=$JAVA_HOME/bin:$PATH
    cd dacapo-bench/benchmarks
    ant dist                                   # ~10 min full build
    ```

    On success the fat JAR appears in `dacapo-bench/benchmarks/` named
    `dacapo-evaluation-git-<rev>.jar` (the `<rev>` is your local git
    short hash; you can also see it printed at the end of the ant log).

    For incremental rebuilds, `ant harness` (~50 s) rebuilds just the
    DaCapo harness and patches the fat JAR in place; per-benchmark
    targets like `ant h2` / `ant tradesoap` / `ant tradebeans` rebuild
    only the named workload. See `dacapo-bench/THROTTLE.md` for the full
    list of supported targets and a workaround if your network can't
    reach github / apache archives during the build.

    **(d) Stage the JAR(s) where the AE expects them.** The
    `conf/workloads/*.conf` files in this artifact reference *four
    distinct filenames* under `${DACAPO_DIR}` (`<shared-base>/dacapo-test/dacapo/`):

    | Target filename | Referenced by | Used by |
    |---|---|---|
    | `dacapo-23.11-chopin.jar` | `conf/workloads/h2.conf`, `tradesoap-*.conf`, `tradebeans-*.conf` (fall-back master) | all DaCapo workloads (master fat JAR) |
    | `h2-throttle-chasing-new.jar` | `conf/workloads/h2.conf` | fig6 H2 |
    | `dacapo-tradesoap-throttle.jar` | `conf/workloads/tradesoap-vlarge-640.conf` | fig6 tradesoap |
    | `dacapo-tradebeans-throttle.jar` | (alternate DayTrader run, optional) | optional |

    All four filenames can share the *same* fat-jar bytes — only the
    filename matters to the AE configs. After `ant dist` finishes,
    copy the produced JAR to all four target filenames:

    ```shell
    # cwd = dacapo-bench/benchmarks   (where ant dist ran)
    SRC=$(ls dacapo-evaluation-git-*.jar | head -1)
    DST=<shared-base>/dacapo-test/dacapo
    mkdir -p "$DST"
    cp "$SRC" "$DST/dacapo-23.11-chopin.jar"
    cp "$SRC" "$DST/h2-throttle-chasing-new.jar"
    cp "$SRC" "$DST/dacapo-tradesoap-throttle.jar"
    cp "$SRC" "$DST/dacapo-tradebeans-throttle.jar"
    ```

    Replace `<shared-base>` with the parent directory you chose in
    Section 3.2 (e.g. `~/dgc-ae`). Re-run this `cp` after every rebuild.

- **HBase 2.5.11**: download the `hbase-2.5.11-bin.tar.gz` release from
  the Apache HBase downloads page (<https://hbase.apache.org/downloads/>)
  and untar to create the two install trees the AE expects:

    ```shell
    cd <shared-base>
    mkdir -p hbase-test
    cd hbase-test
    # Untar twice — table3 shenandoah / dgc-shm read from one tree,
    # fig6 hbase + table3 dgc-rdma read from the other.
    tar xf /path/to/hbase-2.5.11-bin.tar.gz
    cp -r hbase-2.5.11 multi-regionserver-hbase-0
    cp -r hbase-2.5.11 rdma-multi-regionserver-hbase
    rm -rf hbase-2.5.11
    ```

  Then drop in the AE-side patches from `osdi26-dgc-artifacts/hbase-conf/`
  into **both** install trees. From the artifact root, run:

    ```shell
    AE_DIR=$(pwd)
    for HB in ../hbase-test/multi-regionserver-hbase-0 \
              ../hbase-test/rdma-multi-regionserver-hbase; do
        # 1) Custom start-up scripts (with DGC JVM flags baked in)
        cp $AE_DIR/hbase-conf/bin/*           $HB/bin/
        chmod +x $HB/bin/self-zookeeper-start \
                 $HB/bin/self-master-start \
                 $HB/bin/self-regionserver-start \
                 $HB/bin/g1gc-regionserver-start \
                 $HB/bin/specjbb-hbase-mix-master-start \
                 $HB/bin/specjbb-hbase-mix-regionserver-start

        # 2) Per-host HBase config (hbase-env.sh, hbase-site.xml, regionservers)
        cp $AE_DIR/hbase-conf/conf/*          $HB/conf/

        # 3) Table-creation HBase shell script (loaded once before YCSB load)
        cp $AE_DIR/hbase-conf/test.txt        $HB/
    done
    ```

  Both HBase trees use the stock 2.5.11 distribution; the `rdma-` prefix is
  a naming convention so fig6 hbase + DGC-RDMA and Table 3 + DGC-RDMA pick
  up the RDMA-tuned start-up scripts and per-tree SHM paths independently
  of the SHM-mode tree, even when both modes run in the same evaluator
  workspace.

- **YCSB 0.18.0**: download the prebuilt `ycsb-0.18.0.tar.gz` from the
  YCSB releases (<https://github.com/brianfrankcooper/YCSB/releases/tag/0.18.0>)
  and untar into `<shared-base>/hbase-test/ycsb-0.18.0/`. Drop in the
  workload files used by fig6 hbase and Table 3:

    ```shell
    cp $AE_DIR/hbase-conf/ycsb-workloads/*    ../hbase-test/ycsb-0.18.0/workloads/
    ```

### 3.4 Build the DGC JDK

The DGC JDK source is bundled under `jdk17-snic-gc/`. Configure once, then
`make`:

```shell
cd jdk17-snic-gc
bash build-with-ortools.sh                 # configures with --with-ortools=...
make images JOBS=$(nproc)                  # ~25 min on 64 cores
```

The build product lands in
`jdk17-snic-gc/build/linux-x86_64-server-release/images/jdk/`. Mirror it
to the prebuilt directory so the framework's `DGC_JDK` default picks it
up automatically:

```shell
TS=$(date +%Y%m%d_%H%M%S)
rsync -a build/linux-x86_64-server-release/images/jdk/ \
       ../../jdk17-snic-gc-prebuilt/jdk.${TS}/
ln -sfn jdk.${TS} ../../jdk17-snic-gc-prebuilt/jdk
```

Re-run this rsync + symlink update after every JDK rebuild; the framework
picks up the symlink via `DGC_JDK` automatically because the default
resolves to `<shared-base>/jdk17-snic-gc-prebuilt/jdk` relative to the
artifact root.

### 3.5 Configure the Per-Machine / Per-User Environment

`env.sh` loads three layers in order: built-in defaults → machine-level
(file `env.d/$(hostname -s).env`) → user-level (`env.d/$(whoami).env`).
The built-in defaults assume the sibling layout above and set `DGC_ADDR`
/ `DGC_HOST_ADDR` to `127.0.0.1` (loopback), which is sufficient for any
DGC-SHM run.

**For DGC-RDMA you must set `DGC_ADDR` and `DGC_HOST_ADDR` to the host's
IB interface IPv4 address.** Drop a per-machine override at
`env.d/$(hostname -s).env` — for example, on a host named `myhost` with
the IB interface bound to `192.168.1.10`:

```bash
# env.d/myhost.env
DGC_ADDR=192.168.1.10                        # IB / RoCE NIC IPv4 address
DGC_HOST_ADDR=192.168.1.10
```

Other shipped variables (`DGC_JDK`, `DACAPO_DIR`, `HBASE_DIR`,
`SPECJBB_DIR`) only need to be overridden if your install does not use
the sibling layout from §3.3. The shipped `env.d/ds00.env` is an
example file from the maintainers' test machine and is auto-loaded
only when the local hostname is `ds00`; you do **not** need to modify
it for your own host.

#### GC controller defaults

`conf/gc/dgc-shm.conf` and `conf/gc/dgc-rdma.conf` enable the
adaptive coordinator (`-XX:+SnicCoorAdaptiveCCMT`) by default. The
coordinator seeds the CP-SAT marking-time model from the JDK's
`SnicCoorCCMTArgs` default (`0:0:500;4:0:500`) and refines both
fallback and DGC `b` values per cycle by EWMA. To pin a hand-tuned
seed instead, append `-XX:-SnicCoorAdaptiveCCMT` plus an explicit
`-XX:SnicCoorCCMTArgs=...` to `COOR_FLAGS` in the GC conf (last
`-XX:` argument wins).

Runtime DGC fault detection (`-XX:+SnicDGCFaultHandling`, default on
in the JDK) covers two failure modes silently: if the SHM client's
heartbeat is stale at GC start the cycle falls back to local
Shenandoah marking, and if `cancel_gc()` fires while the host is
waiting on a SHM seqno the host re-checks every
`SnicDGCWaitSliceUs` (default 100 ms) so allocation-failure can
route the cycle into Degenerated GC even when the client is hung.

### 3.6 Topology + System Tuning (Recommended)

The paper assumes a hyperthread-disabled, performance-mode CPU layout:

```shell
# Disable HT and pin to performance governor (idempotent; takes effect on next boot)
echo off | sudo tee /sys/devices/system/cpu/smt/control
sudo cpupower frequency-set -g performance
# 2 MiB hugepages help SHM-DGC; 8192 pages = 16 GiB
echo 8192 | sudo tee /proc/sys/vm/nr_hugepages
# Increase RDMA registered-memory limit (DGC-RDMA registers 4 GiB MRs)
echo "* hard memlock unlimited" | sudo tee -a /etc/security/limits.conf
echo "* soft memlock unlimited" | sudo tee -a /etc/security/limits.conf
```

After tuning, log out and back in so the `memlock` ulimit takes effect.

## 4. Run Experiments

### 4.1 Reproduce a Paper Figure or Table

For each paper figure / table reproduced from this repo there is a per-figure
driver under `osdi26-scripts/<figX|tableX>/`:

| Paper artifact | Driver | Analyzer | Plotter |
|----------------|--------|----------|---------|
| Figure 4 (SPECjbb HBIR_RT, 4 GCs) | `osdi26-scripts/fig4/fig4-run.sh` | — (built into plot) | `plot/fig4-plot.py` |
| Figure 5 (DGC-SHM latency details) | `osdi26-scripts/fig5/fig5-run.sh` | `osdi26-scripts/fig5/analyze-fig5.sh` | `plot/fig5-plot.py` |
| Figure 6 (HBase / H2 / DayTrader RT-curves) | `osdi26-scripts/fig6/fig6-{h2,hbase,tradesoap}-run.sh` | `osdi26-scripts/fig6/analyze-fig6-all.sh` | `plot/fig6-plot.py` |
| Figure 7 (DGC-RDMA cache-size sweep) | `osdi26-scripts/fig7/fig7-run.sh` | `osdi26-scripts/fig7/analyze-fig7.sh` | `plot/fig7-plot.py` |
| Table 3 (SPECjbb + HBase mix) | `osdi26-scripts/table3/table3-run.sh` | `osdi26-scripts/table3/table3-analyze.sh` | (CSV consumed by paper-side plot) |

Each driver script contains a header comment with its full usage; the
walkthroughs below cover the common case. By default every driver writes its
runs to a per-figure result base under `results/${USER}/<figX|tableX>-result/`
inside the artifact tree, so multiple evaluators sharing the same install
stay isolated from each other.

#### 4.1.1 Figure 4 — SPECjbb HBIR_RT, 4 GCs

Runs SPECjbb HBIR_RT (auto-ramping mode) with 2 backends across G1,
Shenandoah, DGC-SHM and DGC-RDMA. Each GC variant takes 1.5–3 hours; the full
sweep is roughly 6–10 hours wall clock.

```shell
# All four GCs (default)
./osdi26-scripts/fig4/fig4-run.sh
# Subset
./osdi26-scripts/fig4/fig4-run.sh g1 dgc-shm
GC_LIST="dgc-rdma" ./osdi26-scripts/fig4/fig4-run.sh
```

After the sweep finishes, render the latency-vs-throughput chart:

```shell
cd plot && python3 fig4-plot.py
# Outputs: plot/fig4-specjbb-p99-rt-curve.{pdf,png}
```

#### 4.1.2 Figure 5 — DGC-SHM SPECjbb latency details

Runs SPECjbb PRESET in DGC-SHM with the latency-instrumented JAR so each
request's start time + latency is recorded. The plotter slices this into a
time-window view of latency overlapping with GC events.

```shell
./osdi26-scripts/fig5/fig5-run.sh                # 1 group, IR=6819, 120 s
GROUPS=1 IR=6819 DURATION=120000 ./osdi26-scripts/fig5/fig5-run.sh
./osdi26-scripts/fig5/analyze-fig5.sh            # writes plot/fig5-data/*.csv
cd plot && python3 fig5-plot.py
```

#### 4.1.3 Figure 6 — HBase / H2 / DayTrader RT-curves

Three sub-runs, one per workload; each exercises Shenandoah / DGC-SHM /
DGC-RDMA across a swept throttle (HBase) / IR (H2 / tradesoap) range. With
the default 3 reps each, the full sweep is roughly:

- HBase:     ~2.5 h × 3 GCs × 2 workloads × 3 reps = ~45 h
- H2:        ~30 min × 3 GCs × 3 reps = ~4.5 h
- tradesoap: ~2 h   × 3 GCs × 3 reps = ~18 h

Run them in any order (the analyzer aggregates per-workload):

```shell
./osdi26-scripts/fig6/fig6-h2-run.sh
./osdi26-scripts/fig6/fig6-tradesoap-run.sh
./osdi26-scripts/fig6/fig6-hbase-run.sh
# Subset / single rep:
GC_LIST="dgc-shm dgc-rdma" REPS="1" ./osdi26-scripts/fig6/fig6-h2-run.sh
```

Aggregate and plot:

```shell
./osdi26-scripts/fig6/analyze-fig6-all.sh        # writes plot/{h2,tradesoap,hbase-*}-data/*.csv
cd plot && python3 fig6-plot.py
```

The all-in-one analyzer dispatches to `analyze-fig6-h2.sh`,
`analyze-fig6-tradesoap.sh`, and `analyze-fig6-hbase.sh`; you can run any
of those individually and pass `--tag <tag>` to scope to a specific sweep.

#### 4.1.4 Figure 7 — DGC-RDMA cache-size sweep

Runs SPECjbb PRESET at a fixed IR (default 10356) and sweeps DGC-RDMA's
address-translated client memory pool across multiple sizes
(default 4096/3072/2048/1024 MB), then adds DGC-SHM / Shenandoah / G1
reference points on the same chart.

```shell
./osdi26-scripts/fig7/fig7-run.sh                # default sweep + reference points
CACHE_SIZES="4096 1024" ./osdi26-scripts/fig7/fig7-run.sh   # subset of cache sizes
GC_LIST="dgc-rdma dgc-shm" ./osdi26-scripts/fig7/fig7-run.sh
./osdi26-scripts/fig7/analyze-fig7.sh
cd plot && python3 fig7-plot.py
```

#### 4.1.5 Table 3 — SPECjbb + HBase Mix

Co-runs 2 SPECjbb backends and 2 HBase region servers under YCSB load on the
same NUMA node. Each GC variant runs for ~13 minutes; full Table 3 takes
roughly 45 minutes.

```shell
./osdi26-scripts/table3/table3-run.sh                  # all 3 GCs
./osdi26-scripts/table3/table3-run.sh shenandoah       # only baseline
GC_LIST="dgc-shm dgc-rdma" ./osdi26-scripts/table3/table3-run.sh
```

Table 3 is the only deliverable that is a numeric table rather than a
figure, so there is no in-tree plot script — the analyzer's CSV columns
map one-to-one to the paper's table cells (read it directly, paste into
LaTeX). Extract YCSB read/update p99, SPECjbb steady-state p99, and
per-host degen counts to a single CSV:

```shell
./osdi26-scripts/table3/table3-analyze.sh              # newest run
./osdi26-scripts/table3/table3-analyze.sh table3-2026… # specific tag
# Output: plot/table3-data/p99.csv
```

### 4.2 Result Layout

By default each driver writes its runs to `results/${USER}/<figX|tableX>-result/`
under the artifact root (one subdir per evaluator, so multiple users can run
concurrently without collision), exposed via the `RESULTS_BASE` environment
variable in every driver script. Override it to redirect a sweep:

```shell
RESULTS_BASE=/mnt/scratch/fig4-rerun ./osdi26-scripts/fig4/fig4-run.sh
```

A typical run directory has:

```
<RESULTS_BASE>/<run-id>/
├── META.json            JDK commit, conf snapshot, host info, start/stop times
├── config.env           full env var snapshot (reproducible cmdline)
├── logs/                framework + coordinator + client + host + controller logs
└── raw/                 workload-side raw output (SPECjbb .data.gz, YCSB *.txt, DaCapo CSVs, ...)
```

The framework logs (`logs/framework.log`) contain the exact `numactl` /
`numactl -m` / JVM cmdline used for every JVM, so even after re-tooling the
scripts you can reproduce the original cmdline.

## 5. Plotting

All plotters live in `plot/` and read CSVs that the analyzers materialise in
`plot/<figX>-data/`. Re-render any figure with:

```shell
cd plot
python3 fig4-plot.py                                    # Figure 4
python3 fig5-plot.py                                    # Figure 5
python3 fig6-plot.py                                    # Figure 6
python3 fig7-plot.py                                    # Figure 7
```

Each plotter writes a PDF (vector, used in the paper) plus a PNG (for quick
preview) into `plot/`.

## 6. Troubleshooting

- **`./run.sh` exits with `lock /tmp/osdi26-ae.lock held`** — a previous run
  did not clean up. Run `bash run.sh unlock` to release the lock (this also
  kills leftover JVMs and wipes prefixed `/dev/shm/` segments), then
  re-launch.
- **Coordinator or DGC client refuses to start with `bind: Address already
  in use`** — the previous run's JVMs survived. Kill them with
  `pkill -u $USER -9 java` and remove the SHM segments
  `rm -f /dev/shm/${USER}_*` before retrying.

## 7. Need a Test Machine?

If your group does not have a host that meets the hardware requirements
in Section 2 (in particular the 200 Gbps BlueField-3 RDMA NIC required
for the DGC-RDMA experiments in figs 6 / 7 and Table 3), or if you would
prefer to evaluate the artifact on a machine that already has the prebuilt
DGC JDK + SPECjbb / HBase / DaCapo dependency tree in place, please email
**<hongtaolyu@sjtu.edu.cn>**. We can grant SSH access to a pre-configured
test machine and assist with the per-evaluator environment setup
(Section 3.5) so you can jump straight to running the per-figure drivers
in Section 4.
