# One client vCPU driver comparison

**Parked at user request.** Read [the committed checkpoint](PARKED-CHECKPOINT.json)
for the archive, retained prototypes, validation blocker, and restore instructions.
Do not restart optimization or benchmark loads without an explicit request.

The current v2 optimization roadmap starts at `7e1ad6f`. Its independent
prototypes, measurements, decisions, and validation prerequisites are tracked
in [ROADMAP-STATE.json](ROADMAP-STATE.json). Historical comparisons and rejected
experiments below remain preserved separately. Use `roadmap_build.py` to freeze
v2 inputs, `roadmap_run.py` for paired observations, and `roadmap_audit.py` to
audit retained artifacts; local emulator results alone cannot grant acceptance.

The active v2-only optimization experiment uses separate 30-second profiling
runs and a fresh agent per iteration. See [optimization results and handoff](OPTIMIZATION.md)
for CPU/heap evidence, retained changes, and remaining workloads. The parked
comparison described below remains a separate historical experiment.

**Status: parked; incomplete.** Start with [overall progress](PROGRESS.md),
[findings](FINDINGS.md), [benchmark/test details](BENCHMARKS.md), and
[resume instructions](RESUME.md). Eleven of 36 primary runs and 22 final read
pilots completed. The benchmark cluster is stopped; retained data remain.
Only the point-read maximum-throughput comparison passed acceptance.

This Linux/amd64 experiment compares remote master
`1ceaac8172bda26e8b8d701ac356982d33729b27` with v2
`5a07badd5f3a567f3990e0c70aba6c8d28e7fbc0` using public APIs. It introduces
an independent benchmark module and does not change either driver.

The shared Go core validates IDs, exact row counts and SHA-256 of every returned
payload inside each operation. Old gocql reuses per-worker query templates,
argument and scan slices, with a public blob unmarshaler copying to reusable
owned storage. Its public WithContext call supplies each operation's deadline.
V2 loads full table metadata with RefreshMetadata before preparing typed statements, asserts token routing, binds immutable input with BindBorrowed, and
closes/waits for every binding. Buffered NextInto copies borrowed Bytes to the
same owned row shape; streaming uses seeded destinations. No row mapping or
unsafe borrowing is used. Reads consume all rows before returning; writes await
acknowledgement. Page size is 32 with prefetch disabled. Write coalescing is zero in both drivers; serial consistency is unset for these ordinary statements. Wire probes verify identical representative parameters, including metadata flags. A 128-row read uses five requests: four data pages and a terminal empty page.

Workloads are point SELECT / acknowledged upsert of a 1 KiB blob over 100,000
keys, and partition SELECT of 128 × 4 KiB rows over 1,024 partitions. Each complete
partition is one operation. Keys follow a deterministic permutation; payloads
are immutable deterministic variants indexed by key and row ID. Tables use
RF=3 in datacenter1, LOCAL_QUORUM, native v4, plaintext, no compression,
application retries or speculation. Both profiles use event-driven schema updates with periodic metadata polling disabled. Dataset resets truncate and reload the
entire target table before every pilot/recorded run; all keys are read and
validated before warmup. Preparation flush/compaction work must settle. Untimed loading is paced at 20,000 point rows/s or 5,000 paged rows/s. Only this benchmark keyspace’s snapshots are cleared between resets.

Run from the repository root on the specified host with passwordless sudo,
rootful Podman, Go 1.25.0, Python 3, tcpdump, tc and nsenter available:

```sh
sh tests/driver-comparison/build.sh
python3 tests/driver-comparison/provision.py start
python3 tests/driver-comparison/provision.py tune
python3 tests/driver-comparison/run.py --smoke \
  --output tests/driver-comparison/results/new-smoke
python3 tests/driver-comparison/run.py \
  --output tests/driver-comparison/results/new-run
```

Provisioning imports the existing Alternator script at
`/extra/scylladb/alternator-client-rust/scripts/run-alternator-overload.py`,
redirecting its names, network, cache and disk directories. It inventories and
stops running Scylla containers and the old experiment's Prometheus without
removing their configuration or mounts, and stops detected Alternator runners.
The isolated `codex-gocql-compare-*` nodes use CPUs 0–1, 4–5 and 6–7, two shards,
3 GiB Scylla memory and a 5 GiB container limit with swap disabled. NVMe
calibration supplies physical I/O properties; no experiment disk-budget file or
netem delay is used. The uncalibrated bootstrap data are retained separately.
Runner/monitoring processes use CPUs 2 and 18. All application clients, including
preparation and complete dataset warming, use taskset CPU 8 and GOMAXPROCS=1;
internal and external thread-affinity checks enforce it. Administrative
container commands run within server CPU sets.

`start` requires fresh benchmark container names. For another fresh experiment,
remove only the stopped `codex-gocql-compare-{1,2,3}` and `codex-gocql-iotune`
containers, and move the cache's `data` and `uncalibrated-data` directories aside.
Existing Alternator containers and their data must stay preserved. The runner
retains a lock against concurrent runs, checkpoints output and can reuse completed
run files while a benchmark cluster remains running. It stops the benchmark
cluster on full-run exit, including failure, so failed experiments require
`python3 tests/driver-comparison/provision.py resume` before resuming. It joins each client and verifies
endpoint shutdown and preservation of the old container mounts.

Pilots sweep 1, 4, 16, 64, 256 and 1,024 workers (5 s warmup + 15 s measured),
selecting the smallest concurrency within 3% of the best valid rate. Both v2 read
adapters receive a sweep and three observations at their selected concurrency;
streaming must exceed buffered median throughput by more than 3% to win. Choices
are frozen before recorded rounds. A still-rising 1,024-worker result is flagged.
Each of the 36 recorded runs has 30 s warmup and 120 s measurement, with driver
order alternating between rounds. Each phase has three rounds per driver. Sets
exceeding throughput sample CV 5% or p90/p95/p99 CV 10%, or failing sustainable delivery,
are repeated once, retaining originals and explicitly reporting continued
instability. Sustainable rates are 70% of each driver's own median maximum.

The open-loop scheduler uses evenly scheduled arrivals and a queue bounded to
the greater of 1,024 entries and four times concurrency. It retains scheduling lag, dispatch queueing, rejected
arrivals, misses over 1 ms and per-second queue depth. Sustainable latency starts
at scheduled arrival; dispatch latency is also retained. Successful completions
within the fixed measurement window determine throughput. Histograms include
all requests admitted during that window through a 15 s bounded drain; individual
operations have a 10 s context deadline. HDR histograms cover 1 µs–60 s with three
significant digits and explicit overflow counts. Warmup errors invalidate a run. The v2 `-metadata-refresh` diagnostic flag can enable periodic refresh to check metadata readiness.

Results include REPORT.md, rounds.csv, state.json, individual run JSON,
compressed raw histogram snapshots, server/host metric time series, client
thread-affinity observations, build and environment records, packet captures and
decoded requests. Server CPU and disk summaries use metric samples within the
measurement window. Client resource deltas include draining. Profiling and wire
probes run outside measurements. Sustainable latency is compared at different
absolute rates and must be interpreted with the corresponding target throughput.

Verification:

```sh
GOMAXPROCS=1 GOWORK=off taskset -c 8 go -C tests/driver-comparison test -race ./...
GOMAXPROCS=1 GOWORK=off taskset -c 8 go -C tests/driver-comparison test -race -tags old ./...
GOWORK=off go -C tests/driver-comparison vet ./...
GOWORK=off go -C tests/driver-comparison vet -tags old ./...
python3 -m unittest discover -s tests/driver-comparison -p 'test_*.py' -v
```

Audit a completed run with `python3 tests/driver-comparison/verify.py RESULTS_DIRECTORY`.

The retained metadata diagnostics show why full schema loading precedes v2 preparation: a handle created before table metadata is loaded has no routing token and is rejected after a refresh first supplies the table partitioner. Failed preliminary trials are excluded from the final comparison. A corrected handle passes frequent-refresh and long-running controls.
