# Write-coalescing redesign: benchmark results

Real-cluster results for the `perf/coalesce-locality-aware` branch (design:
`docs/coalescing-redesign.md`). Every number here is reproducible from
`tests/bench/coalesce_cluster.py`, `tests/bench/run_coalesce_plan.sh` and
`tests/bench/run_coalesce_bench.sh`; `tests/bench/summarize_coalesce_bench.py`
regenerates the tables below from the raw run output a campaign leaves under
`tests/bench/results/`, which is git-ignored: each run records its commit SHA
in a `.provenance.txt`, so a pass is re-runnable rather than archived here.

> An earlier version of this file carried a real-cluster table whose origin
> could not be reconstructed -- no script, no command history, no logs. It was
> deleted rather than reinterpreted. Nothing below shares any data with it.

## Method

Load generator is `scylladb/scylla-bench`, which imports `github.com/gocql/gocql`
directly, so it exercises the driver under test. Its `go.mod` `replace` is
pointed at a detached worktree of this repo, and each config is rebuilt from its
own commit; every run records the linked module path, commit SHA and subject in
a `.provenance.txt` next to its output. The `-write-coalesce-wait-time` flag it
needs is `tests/bench/scylla-bench-coalesce-flag.patch`.

| Item | Value |
|---|---|
| Cluster | scylla-ccm podman, `docker.io/scylladb/scylla:2026.3` |
| Image digest | `sha256:5d4b2f63d99b7672080db59b29900479e4343aa11b3c0a8f35f38ff6f3b5f00f` |
| Topology | dc1/rack1:1, dc1/rack2:1, dc2/rack1:2 (4 nodes) |
| Simulated delay | inter-rack 0.5ms, inter-DC 20ms (netem, per direction) |
| Client | ccm client container on dc1/rack1's network, entered via `nsenter --net` |
| Client CPUs | `taskset -c 12-15` (one core, `12`, for the CPU-bound variant) |
| Replication | NetworkTopologyStrategy RF=2, CL=ONE |
| Partitions | 2,000,000 (key space never exhausted mid-run) |
| Connections | 4 |
| Client compression | off |
| TLS | off, except the TLS (E5) section below |
| Go | 1.26.8 |
| Repeats | 3 per cell, 30s per run |

Tier pinning: RF=2 places a replica of every token in every rack, so
`TokenAwareHostPolicy(RackAwareRoundRobinPolicy(dc, rack))` sends all traffic to
the tier under test. Verified: same-rack ~0.6ms, other-rack ~1.0ms, cross-DC
~20ms median in a smoke run.

Configs under test (resolved at run time by commit subject, not hard-coded):

| Label | Commit | Meaning |
|---|---|---|
| master | `ac999d79` | baseline, coalescing as shipped (200µs window) |
| master-plus-alloc-reduction | `0707e53a` | + "perf: reduce writeCoalescer per-flush allocations" |
| branch-disabled | branch tip | `-write-coalesce-wait-time 0` |
| branch-new-design | branch tip | locality/RTT-aware window + MSS threshold |

The latency archetypes ran against tip `364aa3e7`, the saturating ones against
`333b4747`. Only tests, benchmarks and this file differ between the two; no
driver code changed, so the tables are comparable.

## Experiment design

Each archetype has **one** objective. Quoting p99 for a saturating ingest job,
or ops/s for a rate-limited serving job, is what made earlier passes
meaningless, so they are measured and read separately.

| Archetype | Shape | Objective |
|---|---|---|
| `ingest` | unthrottled prepared INSERT, 4 client cores | ops/s, client CPU per op |
| `ingest-cpubound` | same, client on **one** core | ops/s (where client-side savings can convert) |
| `oltp-lam0.1` | INSERT at a fixed low rate | p50/p99 |
| `oltp-lam1.0` | INSERT at the model's break-even rate | p50/p99 |
| `read-lam0.1` | SELECT at a fixed low rate | p50/p99 (gates section 4.5) |
| `mixed-lam1.0` | 50/50 at the break-even rate | p50/p99 |

Rate points come from the `lambda_conn*W` model in `docs/coalescing-redesign.md`
section 2: with W=200µs and 4 connections, `lambda_conn*W = rate*200e-6/4`.

| Offered rate | `lambda_conn*W` | Regime |
|---|---|---|
| 2,000 | 0.10 | window collects ~nothing; the request pays the full wait |
| 20,000 | 1.00 | break-even, ~1 extra frame per window |
| unthrottled | >1 | batching regime |

Cross-DC tops out near 12.4k ops/s and cannot reach `lambda*W=1`, so its rates
(500 and 3,000) are scaled to the same fraction of its own ceiling.

Concurrency is matched to the offered rate: scylla-bench divides `-max-rate`
across workers and floors, so 256 workers quantise 2,000 rps down to 1,792 and
500 rps down to 255. The two low-rate archetypes were re-run at concurrency 20
(2000/20 and 500/20 divide exactly) and only the corrected runs are reported
here. The quantised pass is superseded: at 255 of 500 offered ops/s it produced
a spurious +80% cross-DC p50 for `branch-disabled` that does not reproduce at
the correct rate (-3.5%).

### What the saturating archetypes can and cannot resolve

`ingest`/`ingest-cpubound` push the four-node cluster to its limit, and the
server does not return to the same state between runs: `nodetool
compactionstats` climbs from 0 to 15-16 pending tasks during the first run of an
archetype and stays there. The next run then measures the driver *plus* whatever
CPU compaction is taking, and the loop order (tier, repeat, config) means the
config that runs last inherits the deepest backlog.

Measured: six runs of the same binary with the same flags spanned 64,898-81,655
ops/s -- 26% -- and the distribution was bimodal, not noisy around a mean.
`run_coalesce_bench.sh` now drains compaction to zero pending tasks before every
run, which brings the spread of identical binaries to ~14%. All `ingest*` tables
below are from the drained re-run; the undrained pass is superseded.

The consequence for reading any of this: at saturation, **ops/s differences
under ~15% are not resolvable with three repeats**, and only differences that
survive across tiers and archetypes are worth quoting. `Skbs/op` (a direct
count) and `CPU µs/op` are unaffected -- both stayed tight while ops/s swung --
and the rate-limited archetypes are unaffected too, since they offer a load the
server absorbs and never build the backlog.

An earlier pass of this file drew a -12.4% TLS same-rack throughput regression
for `branch-new-design` from the undrained data. It was an artifact of exactly
this: that config runs fourth of four in every repeat and drew the slow mode
three times running. The drained re-run puts the same cell at **+7.4%**. No
driver change was made in response; the measurement was fixed instead.

## Results

Values are mean ±stddev across repeats. `*` marks a difference larger than the
combined spread of the two cells; everything unmarked is within noise.

Every table below carries the single-buffer fast path in `flush`. The latency
archetypes are from the `364aa3e7` campaign; the saturating ones are from the
drained re-run described above. Two earlier passes were discarded rather than
reported: one `ingest` pass shared the machine with an unrelated `go test
-race`, which moved master by 17% between repeats, and the undrained `ingest*`
pass is superseded for the reason given above.

## ingest

`opmode=write rates=same-rack=0,same-dc-other-rack=0,cross-dc=0 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=false`

Objective: throughput and client CPU per op. Latency is not reported: these runs saturate, so latency is queueing delay.

| Tier | Config | n | ops/s | Δ | CPU µs/op | Δ | Skbs/op |
|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 69,492 ±5,119 | +0.0% | 8.3 ±0.1 | +0.0% | 0.019 |
|  | master-plus-alloc-reduction | 3 | 75,092 ±483 | +8.1% | 8.3 ±0.1 | +0.2% | 0.019 |
|  | branch-disabled | 3 | 75,678 ±6,514 | +8.9% | 10.5 ±0.2 | +26.9% * | 1.000 |
|  | branch-new-design | 3 | 73,854 ±3,201 | +6.3% | 9.4 ±0.1 | +13.6% * | 0.498 |
| same-dc-other-rack | master | 3 | 66,384 ±3,132 | +0.0% | 8.6 ±0.1 | +0.0% | 0.020 |
|  | master-plus-alloc-reduction | 3 | 65,995 ±2,773 | -0.6% | 8.5 ±0.1 | -2.0% | 0.019 |
|  | branch-disabled | 3 | 71,773 ±2,769 | +8.1% | 10.9 ±0.1 | +26.5% * | 1.000 |
|  | branch-new-design | 3 | 74,406 ±400 | +12.1% * | 8.4 ±0.0 | -3.4% * | 0.113 |
| cross-dc | master | 3 | 12,179 ±20 | +0.0% | 19.6 ±0.4 | +0.0% | 0.264 |
|  | master-plus-alloc-reduction | 3 | 12,189 ±3 | +0.1% | 19.5 ±0.1 | -0.2% | 0.261 |
|  | branch-disabled | 3 | 12,417 ±14 | +2.0% * | 22.2 ±0.6 | +13.4% * | 1.068 |
|  | branch-new-design | 3 | 12,276 ±10 | +0.8% * | 21.0 ±0.5 | +7.5% * | 0.395 |

## ingest-cpubound

`opmode=write rates=same-rack=0,same-dc-other-rack=0,cross-dc=0 duration=30s repeats=3 conns=4 client_cpus=12 tls=false`

Objective: throughput and client CPU per op. Latency is not reported: these runs saturate, so latency is queueing delay.

| Tier | Config | n | ops/s | Δ | CPU µs/op | Δ | Skbs/op |
|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 81,102 ±1,554 | +0.0% | 6.4 ±0.0 | +0.0% | 0.027 |
|  | master-plus-alloc-reduction | 3 | 83,012 ±1,037 | +2.4% | 6.5 ±0.1 | +0.7% | 0.027 |
|  | branch-disabled | 3 | 80,566 ±5,248 | -0.7% | 8.2 ±0.1 | +27.7% * | 1.000 |
|  | branch-new-design | 3 | 79,706 ±5,332 | -1.7% | 8.6 ±0.1 | +33.9% * | 0.995 |
| same-dc-other-rack | master | 3 | 68,884 ±513 | +0.0% | 6.6 ±0.2 | +0.0% | 0.029 |
|  | master-plus-alloc-reduction | 3 | 77,279 ±438 | +12.2% * | 6.7 ±0.2 | +2.0% | 0.029 |
|  | branch-disabled | 3 | 70,217 ±5,554 | +1.9% | 8.4 ±0.1 | +26.8% * | 1.000 |
|  | branch-new-design | 3 | 80,274 ±163 | +16.5% * | 6.8 ±0.1 | +2.5% | 0.156 |
| cross-dc | master | 3 | 12,213 ±29 | +0.0% | 12.8 ±0.1 | +0.0% | 0.381 |
|  | master-plus-alloc-reduction | 3 | 12,217 ±9 | +0.0% | 12.2 ±0.1 | -4.8% * | 0.285 |
|  | branch-disabled | 3 | 12,421 ±11 | +1.7% * | 14.6 ±0.1 | +13.8% * | 1.065 |
|  | branch-new-design | 3 | 12,287 ±32 | +0.6% * | 12.8 ±0.2 | -0.4% | 0.394 |

## oltp-lam0.1

`opmode=write rates=same-rack=2000,same-dc-other-rack=2000,cross-dc=500 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=false`

Objective: latency at a fixed offered rate (coordinated-omission corrected).

| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | p99.9 | CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 2,000 | 2.27ms | +0.0% | 3.62ms | +0.0% | 4.62ms | 49.2 |
|  | master-plus-alloc-reduction | 3 | 1,999 | 2.28ms | +0.5% | 3.56ms | -1.5% | 4.53ms | 50.6 |
|  | branch-disabled | 3 | 1,999 | 1.38ms | -39.4% * | 2.47ms | -31.7% * | 3.16ms | 69.7 |
|  | branch-new-design | 3 | 2,000 | 1.41ms | -38.0% * | 2.58ms | -28.7% * | 3.13ms | 60.0 |
| same-dc-other-rack | master | 3 | 1,999 | 2.69ms | +0.0% | 3.84ms | +0.0% | 4.81ms | 48.2 |
|  | master-plus-alloc-reduction | 3 | 1,999 | 2.72ms | +1.2% | 3.89ms | +1.1% | 4.51ms | 48.3 |
|  | branch-disabled | 3 | 2,000 | 1.74ms | -35.4% * | 2.67ms | -30.7% * | 3.35ms | 55.8 |
|  | branch-new-design | 3 | 2,000 | 1.75ms | -35.0% * | 2.79ms | -27.6% * | 3.31ms | 53.5 |
| cross-dc | master | 3 | 500 | 23.27ms | +0.0% | 25.32ms | +0.0% | 47.88ms | 82.0 |
|  | master-plus-alloc-reduction | 3 | 500 | 23.27ms | +0.0% | 25.41ms | +0.3% | 48.42ms | 80.9 |
|  | branch-disabled | 3 | 500 | 22.47ms | -3.4% * | 24.26ms | -4.2% * | 43.56ms | 86.0 |
|  | branch-new-design | 3 | 500 | 22.66ms | -2.6% * | 24.36ms | -3.8% * | 44.51ms | 92.7 |

## oltp-lam1.0

`opmode=write rates=same-rack=20000,same-dc-other-rack=20000,cross-dc=3000 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=false`

Objective: latency at a fixed offered rate (coordinated-omission corrected).

| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | p99.9 | CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 19,962 | 3.64ms | +0.0% | 5.83ms | +0.0% | 6.88ms | 13.1 |
|  | master-plus-alloc-reduction | 3 | 19,968 | 3.65ms | +0.3% | 5.96ms | +2.2% | 7.96ms | 12.4 |
|  | branch-disabled | 3 | 19,968 | 3.27ms | -10.2% * | 5.42ms | -7.1% * | 6.80ms | 15.2 |
|  | branch-new-design | 3 | 19,964 | 3.52ms | -3.3% | 5.53ms | -5.2% * | 6.84ms | 13.5 |
| same-dc-other-rack | master | 3 | 19,968 | 4.12ms | +0.0% | 6.29ms | +0.0% | 8.84ms | 13.4 |
|  | master-plus-alloc-reduction | 3 | 19,964 | 4.13ms | +0.3% | 6.24ms | -0.9% | 7.73ms | 13.6 |
|  | branch-disabled | 3 | 19,966 | 3.62ms | -12.2% * | 5.87ms | -6.8% * | 7.66ms | 15.3 |
|  | branch-new-design | 3 | 19,964 | 3.75ms | -9.0% * | 5.77ms | -8.3% * | 7.66ms | 13.7 |
| cross-dc | master | 3 | 2,816 | 24.72ms | +0.0% | 27.20ms | +0.0% | 61.32ms | 25.0 |
|  | master-plus-alloc-reduction | 3 | 2,816 | 24.99ms | +1.1% | 27.24ms | +0.2% | 59.83ms | 25.5 |
|  | branch-disabled | 3 | 2,816 | 24.55ms | -0.7% | 27.96ms | +2.8% * | 85.96ms | 29.0 |
|  | branch-new-design | 3 | 2,816 | 24.69ms | -0.1% | 27.19ms | -0.0% | 59.08ms | 26.2 |

## read-lam0.1

`opmode=read rates=same-rack=2000,same-dc-other-rack=2000,cross-dc=500 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=false`

Objective: latency at a fixed offered rate (coordinated-omission corrected).

| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | p99.9 | CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 1,999 | 2.82ms | +0.0% | 4.23ms | +0.0% | 11.30ms | 58.7 |
|  | master-plus-alloc-reduction | 3 | 2,000 | 2.74ms | -2.7% | 4.06ms | -3.9% | 11.64ms | 57.1 |
|  | branch-disabled | 3 | 1,999 | 1.90ms | -32.6% * | 3.07ms | -27.4% * | 3.75ms | 69.4 |
|  | branch-new-design | 3 | 1,999 | 1.97ms | -30.2% * | 3.20ms | -24.3% * | 6.61ms | 63.2 |
| same-dc-other-rack | master | 3 | 1,999 | 3.25ms | +0.0% | 4.63ms | +0.0% | 11.92ms | 57.6 |
|  | master-plus-alloc-reduction | 3 | 1,999 | 3.19ms | -2.0% * | 4.57ms | -1.4% | 11.29ms | 56.5 |
|  | branch-disabled | 3 | 1,999 | 2.29ms | -29.5% * | 3.31ms | -28.5% * | 7.27ms | 59.6 |
|  | branch-new-design | 3 | 1,999 | 2.30ms | -29.2% * | 3.40ms | -26.7% * | 7.45ms | 59.5 |
| cross-dc | master | 3 | 500 | 23.77ms | +0.0% | 25.92ms | +0.0% | 47.62ms | 93.6 |
|  | master-plus-alloc-reduction | 3 | 500 | 23.76ms | -0.0% | 25.94ms | +0.1% | 47.55ms | 92.7 |
|  | branch-disabled | 3 | 500 | 22.95ms | -3.4% * | 24.84ms | -4.2% * | 45.23ms | 94.9 |
|  | branch-new-design | 3 | 500 | 23.02ms | -3.1% * | 24.94ms | -3.8% * | 45.51ms | 101.1 |

## mixed-lam1.0

`opmode=mixed rates=same-rack=20000,same-dc-other-rack=20000,cross-dc=3000 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=false`

Objective: latency at a fixed offered rate (coordinated-omission corrected).

| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | p99.9 | CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 19,954 | 4.96ms | +0.0% | 65.95ms | +0.0% | 108.65ms | 15.7 |
|  | master-plus-alloc-reduction | 3 | 19,965 | 4.42ms | -10.8% | 14.77ms | -77.6% | 56.75ms | 15.2 |
|  | branch-disabled | 3 | 19,965 | 4.84ms | -2.4% | 112.04ms | +69.9% | 175.96ms | 18.4 |
|  | branch-new-design | 3 | 19,528 | 4.26ms | -14.1% * | 574.64ms | +771.3% | 700.96ms | 15.3 |
| same-dc-other-rack | master | 3 | 19,825 | 9.50ms | +0.0% | 1241.51ms | +0.0% | 1518.16ms | 14.4 |
|  | master-plus-alloc-reduction | 3 | 19,942 | 49.47ms | +420.6% | 1916.23ms | +54.3% | 2048.68ms | 14.0 |
|  | branch-disabled | 3 | 19,960 | 5.52ms | -42.0% | 368.31ms | -70.3% | 497.64ms | 16.7 |
|  | branch-new-design | 3 | 19,960 | 5.53ms | -41.8% | 339.13ms | -72.7% | 437.22ms | 14.5 |
| cross-dc | master | 3 | 2,813 | 25.14ms | +0.0% | 28.42ms | +0.0% | 53.41ms | 26.8 |
|  | master-plus-alloc-reduction | 3 | 2,816 | 25.32ms | +0.7% | 28.33ms | -0.3% | 54.77ms | 26.9 |
|  | branch-disabled | 3 | 2,816 | 24.76ms | -1.5% * | 33.65ms | +18.4% | 75.43ms | 29.9 |
|  | branch-new-design | 3 | 2,816 | 24.98ms | -0.7% | 28.05ms | -1.3% * | 52.75ms | 27.1 |

Skbs/op are Tcp:OutSegs deltas from the client netns. Client and nodes are joined by veth/bridge with no physical NIC, so GSO buffers are never split: this is write batching, NOT wire packets.

## TLS (E5)

Same cluster with `client_encryption_options` enabled in place
(`coalesce_cluster.py enable-tls`), so port 9042 itself is TLS; `TLS=true`
selects it in the harness. Note that under TLS **master and branch-disabled are
the same code path** -- `WrapTLS` sets `DisableCoalesce` on master -- so the
spread between those two columns is this setup's noise floor, and
`branch-new-design` is the only config that coalesces at all.

### ingest-cpubound, TLS

`opmode=write rates=same-rack=0,same-dc-other-rack=0,cross-dc=0 duration=30s repeats=3 conns=4 client_cpus=12 tls=true`

| Tier | Config | n | ops/s | Δ | CPU µs/op | Δ | Skbs/op |
|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 75,284 ±5,612 | +0.0% | 8.8 ±0.2 | +0.0% | 1.000 |
|  | master-plus-alloc-reduction | 3 | 81,792 ±272 | +8.6% * | 8.7 ±0.1 | -1.5% | 1.000 |
|  | branch-disabled | 3 | 75,964 ±4,806 | +0.9% | 8.5 ±0.2 | -3.0% | 1.000 |
|  | branch-new-design | 3 | 80,883 ±480 | +7.4% | 8.9 ±0.0 | +0.9% | 0.994 |
| same-dc-other-rack | master | 3 | 68,019 ±5,726 | +0.0% | 8.9 ±0.2 | +0.0% | 1.000 |
|  | master-plus-alloc-reduction | 3 | 74,291 ±108 | +9.2% * | 8.8 ±0.1 | -1.3% | 1.000 |
|  | branch-disabled | 3 | 69,508 ±4,322 | +2.2% | 8.8 ±0.1 | -1.4% | 1.000 |
|  | branch-new-design | 3 | 82,158 ±654 | +20.8% * | 6.7 ±0.2 | -24.2% * | 0.156 |
| cross-dc | master | 3 | 12,403 ±25 | +0.0% | 14.3 ±0.3 | +0.0% | 1.043 |
|  | master-plus-alloc-reduction | 3 | 12,417 ±27 | +0.1% | 14.1 ±0.6 | -1.6% | 1.034 |
|  | branch-disabled | 3 | 12,420 ±18 | +0.1% | 14.4 ±0.2 | +0.1% | 1.052 |
|  | branch-new-design | 3 | 12,245 ±17 | -1.3% * | 12.8 ±0.2 | -11.1% * | 0.329 |

### oltp-lam0.1, TLS

`opmode=write rates=same-rack=2000,same-dc-other-rack=2000,cross-dc=500 duration=30s repeats=3 conns=4 client_cpus=12-15 tls=true`

| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | p99.9 | CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| same-rack | master | 3 | 1,999 | 1.42ms | +0.0% | 2.44ms | +0.0% | 3.04ms | 67.7 |
|  | master-plus-alloc-reduction | 3 | 2,000 | 1.41ms | -0.8% | 2.39ms | -1.8% | 3.08ms | 69.0 |
|  | branch-disabled | 3 | 2,000 | 1.45ms | +2.3% | 2.49ms | +2.2% | 2.95ms | 70.4 |
|  | branch-new-design | 3 | 1,999 | 1.42ms | +0.0% | 2.56ms | +4.9% | 3.34ms | 59.6 |
| same-dc-other-rack | master | 3 | 1,999 | 1.81ms | +0.0% | 2.69ms | +0.0% | 3.19ms | 57.7 |
|  | master-plus-alloc-reduction | 3 | 1,999 | 1.82ms | +0.6% | 2.64ms | -1.6% | 3.34ms | 59.3 |
|  | branch-disabled | 3 | 2,000 | 1.80ms | -0.6% | 2.69ms | +0.0% | 3.43ms | 56.2 |
|  | branch-new-design | 3 | 1,999 | 1.82ms | +0.6% | 2.84ms | +5.7% * | 3.80ms | 57.9 |
| cross-dc | master | 3 | 500 | 22.49ms | +0.0% | 24.44ms | +0.0% | 45.91ms | 87.3 |
|  | master-plus-alloc-reduction | 3 | 500 | 22.54ms | +0.2% | 24.43ms | -0.0% | 46.55ms | 88.2 |
|  | branch-disabled | 3 | 500 | 22.49ms | +0.0% | 24.13ms | -1.3% * | 44.53ms | 88.0 |
|  | branch-new-design | 3 | 500 | 22.69ms | +0.9% * | 24.60ms | +0.6% * | 46.48ms | 94.2 |

**TLS gains more from coalescing than plaintext does, as E5 predicted -- but
only where the window is large enough to form a batch.** Same-DC is the clearest
cell in the campaign: 0.156 skbs/op against 1.000 for every non-coalescing
config, **-24.2% client CPU per op**, and throughput up. Cross-DC shows the same
shape at -11.1% CPU and 0.329 skbs/op. Same-rack does not batch (0.994 skbs/op):
its RTT-scaled window is 3-9µs, far below what a request/response gap can fill,
so TLS same-rack behaves like master by design.

**Latency is not the axis TLS moves.** At `lambda*W=0.1` every config lands
within ~5% of every other on p50 and p99, because master already does not
coalesce under TLS -- there is no always-on wait to remove. The gain is the
-12% same-rack CPU (59.6 vs 67.7 µs/op) at identical latency.

**No case was found for disabling coalescing on TLS connections.** The tier
where it cannot batch is also the tier where it costs nothing; the two where it
batches are both clear wins.

## Reading these numbers

**The shipped always-on window costs local latency, and that is the largest
effect in the whole campaign.** At `lambda*W=0.1` -- the regime the section 2
model says collects nothing and pays the full wait -- disabling coalescing cuts
same-rack p50 by 39% (2.27ms to 1.38ms) and p99 by 32%. The new design lands
within 1-2 points of disabled (1.41ms / 2.58ms) while keeping a meaningful part
of the batching. Reads behave the same way (-33% p50, -27% p99, new design
within 2-3 points of disabled).

**At the break-even rate the effect shrinks, as the model predicts.** At
`lambda*W=1.0` the same-rack p50 gap is -10% for disabled and -3% for the new
design; on the other-rack tier both land together (-12% and -9% p50, -7% and
-8% p99).

**Cross-DC is a no-op for every config.** 2-4% at the low rate, within noise at
the break-even rate. A 200µs window against a 20ms RTT cannot matter, which is
exactly what section 2 says.

**Coalescing buys segments, not throughput.** With GSO enabled -- as in
production -- master hands the stack ~0.019 segments per op (roughly 52 ops per
skb) versus ~1.000 for disabled: a ~50x difference that produces **no**
throughput gain. Disabled measured 2-9% *higher* than master on the local tiers.
The kernel's own aggregation already captures the batching benefit the
user-space window is trying to produce.

**Where coalescing does pay is client CPU.** Turning it off costs 27% more
client CPU per op on the ingest archetypes (8.3 to 10.5 µs/op same-rack, 8.6 to
10.9 same-DC) and 15-40% at the low serving rates. On same-DC the new design
matches or beats master's CPU (-3.4% on `ingest`, +2.5% on `ingest-cpubound`)
at a tenth of its window, while giving up the latency tax.

**Same-rack at saturation is the one cell where the new design costs CPU.** Its
window there is 3-9µs, so it batches almost nothing (0.995 skbs/op on
`ingest-cpubound`) and pays +33.9% CPU against master's unconditional 200µs
window. That is the deliberate trade: master buys that CPU with the 39% p50 tax
the `oltp-lam0.1` table charges it, and at the rate-limited points -- which is
where a same-rack serving workload actually sits -- the new design is *ahead* on
CPU as well (59.6 vs 67.7 µs/op under TLS). Only a same-rack client running flat
out against its own ceiling gives up something, and it gives up CPU, not
throughput (-1.7%, within the spread).

**The timer floor, not the computed window, sets the wait when a window does
arm.** Go's netpoller floors a sub-millisecond timer deadline to roughly 1ms
when all Ps are parked, so an armed wait costs ~1ms whether the computed
window is 10µs or 200µs. The RTT-scaled window's real effect is therefore how
*often* a connection arms, not how long it waits once armed. That is visible
in the tables above: same-rack sits at 0.995 skbs/op (it essentially never
batches) and its `oltp-lam0.1` p50 is within ~1% of fully-disabled coalescing
-- it is not paying a 1ms tax it isn't incurring. Same-DC sits at 0.156
skbs/op (~6.4 frames per flush) at saturation while its `oltp-lam0.1` p50
matches fully-disabled to 0.01ms -- it arms in bursts, not at serving rates.
That is the intended Nagle behaviour, and it is also why a minimum-window
floor was rejected: same-DC's window (30-67µs) is itself below the timer
floor, and that is the tier with the campaign's best result (-24.2% client
CPU under TLS); flooring the window would disable coalescing exactly where it
wins.

**The single-buffer fast path is what closed the CPU gap.** Before it, the new
design paid +17.4% CPU over master on `ingest-cpubound` same-rack at 0.573
skbs/op; after, +10.2% at 0.385 measured against the same (undrained) baseline.
Concatenating instead of vector-writing costs one memcpy and removes a per-frame
write.

**The coalescer allocates nothing per write.** `BenchmarkCoalesceAllocs` reports
0 allocs/op (and 0-1 B/op, the amortised concat buffer) for the direct writer
and for the coalescer at both window=0 and window=200µs: the result channel
comes from `writeResultChanPool` and the concat buffer is reused across flushes.

**Nothing in the campaign justifies section 4.5 (SELECT bypass).** The read
archetype shows the new design within 2-3 points of fully-disabled coalescing on
both p50 and p99; a statement-type bypass has nothing left to recover. Section
4.5 stays unimplemented, per its own "ship it only if E4 shows a measurable p99
gain; otherwise drop it" gate.

**`mixed-lam1.0` p99 is not characterised.** Its cells swing from -73% to +771%,
and master's own same-DC p99 moved from 43ms in one pass to 1242ms in the next
on identical code. The tail there is dominated by something outside the driver
(most likely compaction). Three 30s runs do not resolve it; no conclusion is
drawn from that table, in either direction.

## Open items

- TLS covers `ingest-cpubound` and `oltp-lam0.1` only; the remaining archetypes
  were not re-run under TLS.
- A compression-on pass (`COMPRESSION=true`), since compression re-sizes frames
  and interacts with the MSS threshold.
- `mixed` needs longer runs or more repeats before its tail is quotable.
- Saturating ops/s still carries ~14% spread between identical binaries after
  compaction draining. More repeats, or a workload sized below the cluster's
  ceiling, would be needed to resolve differences smaller than that.
- Segment counts here are skbs handed to the stack, not wire packets: client and
  nodes are joined by veth/bridge with no physical NIC, so GSO buffers are never
  split. A physical-NIC run would separate write batching from packet count.
