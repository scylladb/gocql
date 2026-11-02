# Overall progress and parked checkpoint

Parked at the user’s request on 2026-11-01T14:15:38.255021+00:00. **The full experiment is unfinished.** Start here when resuming.

## State of the work

|Work item|Status|
|---|---|
|Pin remote master and v2, compiler/build records|Done|
|Shared workload/validation/pacing core and separate public adapters|Implemented|
|Provision fresh three-node NVMe cluster; preserve existing clusters|Done; all benchmark endpoints verified shut down|
|Correctness and representative wire/connection equivalence|Passed for final adapters|
|Harness Go race tests, vet, Python tests|Passed; rechecked at parking|
|Final point-read pilots|22 complete, zero errors|
|Final point-read maximum throughput|6/6 rounds complete; both sets accepted|
|Final point-read sustainable load|5/6 initial rounds complete; all completed rounds rejected arrivals|
|Required sustainable repeats|0/6 complete|
|Final point-write pilots and measurements|Not started|
|Final paged-read pilots and measurements|Not started|
|Final full-matrix audit / comparison report|Not complete|

**Completed primary rounds: 11/36 (30.6%). Remaining primary rounds: 25.** The interrupted v2 sustainable read round three has logs and a partial metric stream but no completed JSON or histogram result. It does not count as completed. A minimum of six additional repeat rounds is expected after the sustainable failure assessments. Progress is not a time estimate.

## Findings to carry forward

- Stable point-read maximum medians: legacy 134,605.8 ops/s, v2 68,929.29 ops/s; v2 is 48.8% slower. Median p99: 1.146 ms versus 2.293 ms.
- Both drivers use 64 workers; buffered NextInto won the v2 read-adapter pilot. Neither search hit a rising-capacity limit at 1,024 workers.
- Sustainable targets differ: legacy 94,224.06 ops/s and v2 48,250.50 ops/s. No sustainable set is accepted.
- V2 table metadata must be loaded before typed preparation so the immutable handles capture a valid partitioner and supply routing tokens.
- Final setup uses calibrated NVMe I/O properties, matched coalescing and serial-consistency settings, paced untimed loading, and benchmark-only snapshot cleanup.

Full numerical results and caveats: [FINDINGS.md](FINDINGS.md). Workload/test inventory: [BENCHMARKS.md](BENCHMARKS.md). Remaining actions: [RESUME.md](RESUME.md).

## Retained implementation

|File|Responsibility|
|---|---|
|[main.go](main.go)|Shared deterministic datasets, correctness, validation, complete warming, closed/open-loop phases, bounded HDR histograms, CPU/GC/allocation counters, affinity|
|[old.go](old.go)|Legacy public adapter and reusable owned blob unmarshaler|
|[v2.go](v2.go)|Typed prepared buffered/streaming adapters, readiness and binding drain|
|[build.sh](build.sh)|Pinned source/build setup and binary records|
|[provision.py](provision.py)|Reuse original provisioning helper with isolated names/network/data; physical calibration and resume/stop|
|[run.py](run.py)|Reset/flush/agreement/settling, pilot/round ordering, monitoring, checkpoints and report|
|[probe.py](probe.py), [wire.py](wire.py)|Outside-timing packet capture/decoding and connection inventory|
|[shutdown.py](shutdown.py)|Stop/join cluster; verify closed endpoints and preserved old container mounts|
|[core_test.go](core_test.go), [test_runner.py](test_runner.py)|Meaningful harness regressions|
|[verify.py](verify.py)|Full 36-run evidence audit; not yet applicable to this partial matrix|

## Repository and artifact state

All changes are under `tests/driver-comparison/`, currently untracked. Driver source was not edited. No commit, push, merge, issue publication or other external publication occurred. The old pinned worktree and compiled binaries are retained in `.codex-cache/driver-comparison/` (git-ignored). Raw result directories occupy approximately 618 MiB in total at parking. Do not delete them or mistake preflight directories for final results.

The original runner was no longer alive when parking was requested; its last incomplete run had no process completion record. The three benchmark nodes were still running. Parking invoked shutdown.py successfully and verified all three benchmark nodes stopped, all 12 checked endpoints closed, and all four preserved Alternator/monitor containers stopped with their mounts intact; [shutdown.json](results/2026-11-01/shutdown.json) is the authoritative final host state. Existing Alternator containers remain stopped with their original mounts/configuration, and unrelated services remain running.

## Evidence index

|Path|Meaning|
|---|---|
|[results/2026-11-01/](results/2026-11-01/)|Final configuration: 22 read pilots, 11 completed primary rounds, partial twelfth|
|[results/2026-11-01/PARK-VERIFICATION.md](results/2026-11-01/PARK-VERIFICATION.md)|Parking test results and evidence/shutdown checks|
|[results/2026-11-01/park-audit.json](results/2026-11-01/park-audit.json)|Parking audit of completed result accounting, histograms, affinity records, wire parameters, and original source snapshots|
|[results/2026-11-01/rounds.csv](results/2026-11-01/rounds.csv)|Completed per-round data|
|[results/2026-11-01/sources/](results/2026-11-01/sources/)|Exact source snapshot matching original environment hashes; keep frozen|
|[results/2026-11-01-metadata-diagnostics/](results/2026-11-01-metadata-diagnostics/)|Public metadata-readiness reproduction and corrected controls|
|[results/2026-11-01-metadata-preflight/](results/2026-11-01-metadata-preflight/)|Excluded pilots/long rounds prepared before metadata readiness|
|[results/2026-11-01-load-preflight/](results/2026-11-01-load-preflight/)|Excluded unpaced dataset-loading timeout; no new primary result|
|[results/2026-11-01-preflight/](results/2026-11-01-preflight/)|Earlier excluded setup/pilot flags and affinity-monitoring corrections|
|[results/2026-11-01-smoke/](results/2026-11-01-smoke/), [matched-smoke](results/2026-11-01-matched-smoke/)|Short end-to-end workload exercises on earlier settings, not capacity evidence|
|[results/2026-11-01-probes/](results/2026-11-01-probes/)|Earlier wire-probe development; final probes are inside final run directory|
