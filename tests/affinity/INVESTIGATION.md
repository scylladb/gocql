# CPU affinity investigation checkpoint

Status: **parked at the user's request**. Checkpoint date: 2026-10-29.
No additional experiments or driver changes are queued by this investigation.

## Question and current decision

Does assigning an application metadata-cache shard and its worker goroutine to
a logical CPU improve load balance or throughput? Can socket receive processing
also be kept on that CPU?

Retain normal Go scheduling as the default. This simplified workload showed a
benefit from removing a shared mutable cache lock, but no TCP throughput benefit
from pinning its owner-local workers. A one-time SO_INCOMING_CPU setting did not
maintain the requested receive CPU. These findings do not establish general
database/driver performance or rule out useful affinity in another workload.

## Setup actually tested

This is a standalone Linux/amd64 cache + encoder + TCP echo experiment. It does
not use gocql, CQL, a database, TLS, compression or a physical NIC.

- Four workers share one mutable metadata LRU, or own separate quarters of the
  same total entry population. Every lookup updates LRU order. Sharding removes
  the shared mutex and shared LRU; this is a separate intervention from affinity.
- A pinned worker calls runtime.LockOSThread, then Linux sched_setaffinity with
  a single logical CPU. Original affinity is restored before unlocking. The
  same worker owns cache lookup, encoding, socket writes and socket reads.
- All client variants use GOMAXPROCS=4 and CPUs 8,10,12,14, in one 32 MiB L3
  domain. The echo server uses CPUs 17,18,20,23 on separate physical cores.
  Affinity restricts scheduling; these CPUs were not reserved exclusively.
- TCP cases use 64 MiB of entry storage, 1% cached-value updates, four persistent
  connections, and batches of 32 records of 256 bytes. Maps, input streams and
  sampling storage add memory beyond the entry-storage budget.
- The socket variant sets SO_INCOMING_CPU once, then observes it after warmup
  and during/after measurement. It is a feasibility probe, not an enforced
  receive-affinity treatment. No sustained socket-affinity comparison is emitted.

## Latest comparable results

The corrected TCP confirmation completed 30 successful runs: five cases, six
randomized repetitions each, two seconds of warmup and ten seconds measured.
Values below are median record throughput, not database query throughput.

| Configuration | Median loopback TCP throughput |
| --- | ---: |
| Shared cache, normal scheduling | 4.83 M records/s |
| Shared cache, pinned workers | 2.12 M records/s |
| Sharded cache, normal scheduling | **5.68 M records/s** |
| Sharded cache, pinned workers | 4.70 M records/s |
| Sharded cache, pinned + one-time socket option | 4.59 M records/s |

Paired within-repetition comparisons: sharding with normal scheduling increased
throughput about 17%; pinning after sharding reduced it about 17%. These paired
medians need not equal ratios of the independently calculated table medians.
All 24 socket-option workers already reported an incoming CPU different from
their target after warmup. Their observed receive CPUs belonged to the server
CPU set; observed NAPI IDs were zero.

The shared host denied perf counters (perf_event_paranoid=4). Sampled CPU
placement checks passed, but no hardware cache-miss or full migration counters
were collected. Worker completion balance is not attributed per-vCPU load;
overall CPU utilization includes unrelated processes. Six randomized blocks
provide exploratory local evidence, not a universal affinity result.

## Review corrections and evidence history

The benchmark review found biased periodic latency sampling, a socket treatment
that changed before measurement, incomplete matrix validation, and missing
source notices. Fixes added random sample positions plus a whole-run reservoir,
explicit socket feasibility status, strict case/repetition/configuration/seed
validation, and copyright/license notices. Go race tests, Go vet, eight Python
regression tests and the corrected runtime validation passed.

The original latency samples are phase-locked to repeated input positions. Their
p99 values and latency ratios, including the earlier **3.2x p99 claim, are
withdrawn**. Original raw results remain intact; corrected summaries withhold
those fields. Reports under pre-review directories are superseded history.
Do not mix source versions or short validation runs into the confirmation table.

| Artifact | Purpose |
| --- | --- |
| [Harness README](README.md) | Implementation details, controls and commands |
| [Original run, corrected interpretation](results/2026-10-29/conclusions.txt) | 144 original runs; throughput retained, biased latency withdrawn |
| [Original correction record](results/2026-10-29/review-corrections.json) | Raw identity and superseded-report status |
| [Mixed-L3 pilot](results/2026-10-29-pilot-mixed-llc/environment.json) | Interrupted pilot; excluded from final comparisons |
| [Post-review regression verification](results/post-review-validation/verification.json) | 48 short runs; 24 cases x two repetitions, 200 ms warmup and two seconds measured |
| [TCP confirmation report](results/post-review-tcp-confirmation/report.txt) | Latest full-duration five-case comparison |
| [TCP confirmation summary](results/post-review-tcp-confirmation/summary.json) | Machine-readable statistics and paired comparisons |
| [TCP confirmation raw results](results/post-review-tcp-confirmation/runs.jsonl) | Exact commands, all 30 outcomes and per-worker observations |
| [TCP confirmation environment](results/post-review-tcp-confirmation/environment.json) | Runtime/topology/settings plus measured source and binary identities |
| [TCP confirmation audit](results/post-review-tcp-confirmation/verification.json) | Successful matrix, source/placement/payload checks and server shutdown |

Recorded source hashes identify the snapshots used for each run. Subsequent
checkpoint/documentation changes do not retroactively alter those identities.

## Resume only if requested

Start with this checkpoint and the confirmation report. Possible next steps,
not currently scheduled:

1. Add an immutable prepared-handle or read-only-cache control so shared LRU
   contention is not assumed to represent the intended driver hot path.
2. Repeat the corrected full CPU/skew matrix if those outcomes matter; its
   post-review coverage so far is short regression validation. Add a thread-lock
   control without single-CPU affinity if separating those costs is necessary.
3. Investigate physical NIC receive-queue/RSS/RPS/RFS placement separately if
   sustained socket locality is the objective. Do not infer it from setsockopt
   success or loopback throughput.
4. Only then validate a promising change through the actual driver and a remote
   database with representative concurrency, payloads and latency measurements.

The latest TCP comparison can be reproduced from the repository root with:

```sh
python3 tests/affinity/run.py --phase tcp --repeats 6 --duration 10s --warmup 2s \
  --output tests/affinity/results/new-tcp-run
python3 tests/affinity/summarize.py tests/affinity/results/new-tcp-run
```

Choose permitted CPUs with the same topology constraints if using another host;
see the harness README. Always use a fresh output directory. The investigation
is saved locally; no commit or external publication is implied by this checkpoint.
