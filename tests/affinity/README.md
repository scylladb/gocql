# Local cache, worker and socket affinity experiment

**Status: parked.** The [investigation checkpoint](INVESTIGATION.md) records the
latest comparable results, review corrections, evidence links and possible
steps for resuming this work.

This standalone Linux/amd64 experiment tests whether CPU affinity adds value
after cache ownership is partitioned. It uses only the Go standard library and
introduces no new module. It does not execute CQL or the gocql transport/codecs.
Results establish properties of this synthetic workload, not a driver speedup.

Each entry has 128 bytes of metadata. Every lookup updates an intrusive LRU, like
a mutable prepared-statement cache. A shared cache uses one mutex; owner-local
shards have no cross-worker access and need no mutex. All modes hold the same
total entries and use the same deterministic, partitioned key streams. Metadata
updates alter existing entries without allocation; there are no cache misses,
eviction/refill costs or duplicate per-worker caches. A real application using
immutable prepared handles may avoid this LRU work entirely.

## Variants

| Name | Cache | Scheduling | Socket option |
| --- | --- | --- | --- |
| A | Shared mutex/LRU | Normal Go | Default |
| B | Shared mutex/LRU | Locked thread, one assigned CPU per worker | Default |
| C | Owner-local shards | Normal Go | Default |
| D | Owner-local shards | Locked thread, one assigned CPU per worker | Default |
| E | Owner-local shards | Locked thread, one assigned CPU per worker | One-time SO_INCOMING_CPU feasibility probe |

D/C isolates the practical benefit/cost of worker pinning after cache sharding.
C/A includes removing the global mutex and shared LRU; it cannot establish an
affinity benefit. E is labeled `E-sharded-pinned-socket-once` and tests whether
a one-time socket setting survives warmup and traffic. It is not a sustained
receive-affinity treatment. No E/D socket-affinity throughput comparison is
emitted: observed mismatches are reported as unsupported, while matching samples
remain unverified between probes. Thread locking plus Linux affinity is the tested pinning intervention;
there is no thread-lock-only control.

CPU mode performs lookups and fixed-width encoding into reusable storage. TCP
mode adds four persistent connections, one per worker, to a separate local echo
server. Each worker encodes 32 records of 256 bytes, sends that batch, reads the
full echo, and verifies every returned byte. The same goroutine owns that
connection's reads, writes, cache and encoding. TCP_NODELAY and batch size
are equal across variants. Echo workers run on a disjoint set of physical cores;
they are otherwise unchanged. This is a loopback experiment without TLS,
compression, native-protocol multiplexing or server-side database work.

## Matrix and execution

The default matrix has 24 cases and six randomized blocks (144 fresh client
processes). Each process warms for two seconds, synchronizes all workers, and
measures for ten seconds:

- CPU: A-D, 1/64 MiB total entry storage, 0/1 percent metadata updates: 16 cases.
- Balanced TCP: A-E, 64 MiB, 1 percent updates: five cases.
- Skewed TCP: C-E, same cache/workload, 80 percent of batches routed to worker
  zero: three cases. A dispatcher permits at most 64 outstanding batches and
  bounded per-worker queues. This phase includes dispatch/backpressure costs;
  compare variants within this phase, not directly with autonomous balanced
  workers. Unpinned owner-local workers do not steal another shard's work.

Entry sizes exclude map/index, key-stream and sampling storage; final Go heap
usage is reported separately. Warmup does not mutate metadata, and measured
input sequences restart from zero. Every partition's full key working set is
covered before the deterministic shuffled stream repeats.

Run from the repository root, choosing permitted CPUs on distinct physical
cores. The example is the CPU assignment used on the current host:

```sh
GO111MODULE=off GOWORK=off go test -race -count=1 ./tests/affinity
python3 tests/affinity/run.py --cpus 8,10,12,14 --server-cpus 17,18,20,23 \
  --output tests/affinity/results/new-run
python3 tests/affinity/summarize.py tests/affinity/results/new-run
```

Use a fresh output directory. `--phase smoke --repeats 1 --duration 1s
--warmup 200ms` exercises CPU, TCP, affinity, socket options and skewed shutdown.
Other phase choices are `cpu`, `tcp` and `skew`. The runner asserts equal CPU
sets, one shared last-level-cache domain for client workers, disjoint physical
client/server cores, successful clients, nonzero work and correct sampled CPU
placement of every pinned worker. Runner and summarizer share one case manifest
and require exactly one successful row per expected case/repetition, with matching
configuration and paired sampling seed. Duplicates, unknown cases and invalid
repetitions fail; missing rows prevent completion. `summarize.py --allow-incomplete`
is an explicit progress report and never marks a partial matrix complete. Every
phase, including smoke or a single selected phase, has its own expected manifest.
The runner always stops its
own echo server. It does not change host network/IRQ/sysctl configuration.

## Measurements and interpretation

Raw commands, source/binary SHA-256 identities, machine/runtime/cgroup details,
load averages, server CPU time and each result are retained in environment.json
and runs.jsonl. Each result records throughput, process CPU time, allocations,
heap usage, context switches, per-worker completions, sampled CPU locations,
socket incoming CPU/NAPI observations and latency of sampled 32-operation
batches. Balanced batch latency includes cache/encode/TCP response time; skewed
batch latency additionally includes time since dispatch. It is not per-query
p99, and this closed-loop test is not an open-loop overload test.

Latency sampling chooses one random position in each 256-batch window instead
of repeatedly selecting the same position in the power-of-two input stream.
Per-worker random streams use the recorded SampleSeed and worker ID; paired
variants in a repetition use identical seeds. A reservoir of at most 65,536
observations per worker represents the entire run, including late observations
when the reservoir is full. Seen/retained counts are saved. CPU placement probes
remain separate periodic checks. Socket-option probes follow sampled TCP batches,
outside their timed interval; their overhead still belongs to E's total throughput.

CPU locations/changes are sampled every 256 batches, not hardware migration
counters. CPU busy percentages include unrelated host processes; they do not
attribute utilization to this benchmark. Process CPU time is attributable to
the client. Server CPU time includes the client's setup and warmup as well as
measurement. The current host denies perf counters (perf_event_paranoid=4), so
cache misses and true CPU migrations are unavailable. No affinity/locality
mechanism should be inferred from throughput alone.

Compare paired repetitions and their variability. A practical initial criterion
is a repeatable approximately 10 percent D/C throughput gain without a latency
or skewed-workload regression. Six randomized blocks on a shared host are
exploratory evidence. Follow promising results with actual driver benchmarks
and physical NIC/RSS/RFS observations before changing the driver design.

Setting SO_INCOMING_CPU is checked immediately, at the start of measurement
after warmup, after sampled TCP batches, and at the end of traffic;
successful setsockopt is not evidence of fixed receive placement. A pinned
application I/O worker and kernel packet processing are separate observations.
Linux documentation: [socket options](https://man7.org/linux/man-pages/man7/socket.7.html),
[network scaling](https://docs.kernel.org/networking/scaling.html).

## Review corrections and validation

The original 2026-10-29 run used phase-locked latency sampling. Its raw counts
and commands are preserved, but the corrected summarizer withholds its latency
statistics and ratios. The earlier 3.2x p99 claim is withdrawn. All 48 original
socket-option workers already reported a different incoming CPU at measurement
start, so those cases provide feasibility observations, not maintained-affinity
throughput evidence. Original derived reports are retained under each run's
`pre-review/` directory and explicitly superseded by the corrected reports.

This benchmark deliberately uses a mutable LRU and does not model immutable
prepared-handle execution or the CQL stack. Removing that lock/LRU is a separate
intervention from CPU pinning. Neither the original results nor short regression
runs establish database throughput or general load-balancing behavior.

Run the regression tests with:

```sh
GO111MODULE=off GOWORK=off go test -race -count=1 ./tests/affinity
GO111MODULE=off GOWORK=off go vet ./tests/affinity
python3 -m unittest discover -s tests/affinity -p 'test_*.py' -v
```
