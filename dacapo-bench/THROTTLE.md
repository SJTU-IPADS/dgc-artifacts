# DaCapo Throttle Fork (`all-benchmarks-throttle` branch)

This fork of DaCapo 23.11-chopin adds deadline-chasing throttle support so a
benchmark can be driven at a prescribed ops/sec rate. Both the per-operation
wall-clock latency and the "metered" latency (start-of-deadline → end) are
recorded for tail-latency analysis.

## Supported Benchmarks

| Benchmark | Throttle mechanism | Flag | Domain |
|-----------|--------------------|------|--------|
| `h2` | Manual `LatencyReporter.startWithDeadline` in `TPCCSubmitter.runTransactionsWithThrottle` | `-Ddacapo.throttle=N` or `--throttle N` | TPC-C OLTP |
| `tradesoap` | Manual deadline-chasing in `DaCapoTrader` (DayTrader path) | `-Ddacapo.daytrader.issue.rate=N` | SOAP web services |
| `tradebeans` | Same as tradesoap (shares `DaCapoTrader`) | `-Ddacapo.daytrader.issue.rate=N` | EJB services |
| `lusearch` | Manual `startWithDeadline` in `Search`, deadline persisted across `QueryProcessor` instances | `-Ddacapo.throttle=N` | Lucene queries |
| `tomcat` | Manual `startWithDeadline` in `Client` HTTP sender | `-Ddacapo.throttle=N` | HTTP server |
| `spring` | Manual `startWithDeadline` in `Client` HTTP sender (WildFly deferred init) | `-Ddacapo.throttle=N` | HTTP services |
| `kafka` | **Auto** via patched `LatencyReporter.start()` | `-Ddacapo.throttle=N` | Message streaming |
| `jme` | **Auto** via patched `LatencyReporter.start()` | `-Ddacapo.throttle=N` | 3D game engine |

**Throttle unit**: the value is **per-thread ops/sec**, not total.  Total throughput
≈ `throttle × --terminals` (or the benchmark's own worker count). For h2 with `-t
8`, `--throttle 1000` is ~8k transactions/sec across the JVM. Under-throttling
below the steady-state capacity produces a throttle-bounded run where each thread
blocks via `LockSupport.parkNanos()` between deadlines.

**What NOT to expect**: benchmarks whose source does not call `LatencyReporter`
(most notably `cassandra`, `h2o`, `pmd`, `biojava`, `eclipse`, `xalan`, `batik`,
`graphchi`, `fop`, `jython`, `sunflow`, `luindex`, `avrora`, `zxing`) ignore
`-Ddacapo.throttle` entirely — they run at full speed and report only wall-clock
iteration time.

## Latency Modes

`LatencyReporter` produces two CSVs per iteration: `*-simple-N.csv` (operation
end − operation begin) and `*-metered-N.csv` (operation end − deadline). When a
workload runs near saturation the two diverge: simple captures in-flight
processing time, metered captures perceived latency from the client's
perspective (queuing delay included).

## Harness-Level Auto-Throttle

`harness/src/org/dacapo/harness/LatencyReporter.java:77-86` reads
`System.getProperty("dacapo.throttle")` in `initialize()`. If set, each
`LatencyReporter.start(tid)` call blocks until the next per-thread deadline and
then stamps the deadline on the report. Benchmarks that construct their own
deadlines via `startWithDeadline(tid, deadline)` bypass the auto path.

Per-thread deadlines are staggered at startup by `tickNs × tid / threads` so
the initial request burst is spread evenly.

## Building

Default `ant dist` builds all 22 benchmarks; this fails on machines that lack
`cvs`, `svn`, or network access to github releases / apache archives. For the
8 latency-critical benchmarks above, only `git` is strictly needed. Workarounds:

1. Comment out lines 104-111 of `benchmarks/build.xml` (`<check.dependency>`
   for `cvs/svn/python/node/npm` and `check-python-modules`).
2. Pre-populate `benchmarks/harness/downloads/` with
   `java-allocation-instrumenter-3.3.0.jar` and `HdrHistogram-2.1.12.zip`
   from https://github.com/... (the ant `<get>` tasks fail behind restrictive
   proxies).
3. Set `JAVA_HOME` to a JDK-8 (not JRE-8). `javac` must be on `PATH`.
4. `jdk.11.home=<JDK-11 path>` in `benchmarks/local.properties` — pointing at
   a JRE-only tree fails.

Per-benchmark incremental build: `ant lusearch`, `ant tomcat`, `ant spring`,
`ant h2`, `ant harness` all work independently and patch the fat JAR in place.
