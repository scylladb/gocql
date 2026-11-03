# Parked driver convergence work

All optimization and benchmark work is parked at the user's explicit request.
Resume only after a new explicit instruction. No optimization was adopted and
measured convergence was not achieved. The retained production reference remains
`f363e371832e7d8260e2858fba867fe591f655a4`. Root source includes unadopted prototypes.

The local checkpoint branch is `park-driver-convergence-20261103T113413Z`.
The verified archive, Git bundle, receipts and full file inventories are in:

`/extra/scylladb/gocql-parks/convergence-20261103T113413Z`

Read `git-receipt.json` and `archive-verification.json` there before restoring.
The archive preserves the root source, harness, isolated worktree contents, frozen
sources/binaries, datasets and all result directories. The Git bundle preserves
repository refs including the checkpoint. Existing parking archives remain intact.

The original branch `optimize-convergence-20261102` remains at parked f363e37.
The three reference commits are master `1ceaac8`, original v2 `7e1ad6f`, and parked
redesign `f363e37`. All longer hashes and paths are in CONVERGENCE-STATE.json.

All comparison launchers, coordinators, clients and health monitors are stopped.
Containers `codex-gocql-compare-1`, `codex-gocql-compare-2`, and
`codex-gocql-compare-3` are paused after flushed snapshots tagged
`convergence-park-20261103T113413Z`; persistent data remain in place. The compatible
Cassandra v5 container `codex-gocql-v5-20261103T014000Z` remains paused.
Emulator PID 167949 is suspended with SIGSTOP, preserving its in-memory dataset.
Verify identity before any signal: host PIDs have wrapped during this session.
The native and emulator server CPU assignments overlap; serialize their loads.

At this checkpoint, four initial/confirmed screening decisions closed without
adoption. Ownership B has a confirmed buffered-partition CPU/op regression of
1.54% (95% CI +0.49%..+2.95%). Its 50 completed windows remain included.
Independent scalar fusion completed 80 screen windows but failed delivery and
cross-workload checks. Independent cancellation completed 16 maximum read windows
before unchanged reference writes failed with critical disk utilization.
The independent framing experiment passed full unit race, vet, 50 query lifetime
repetitions, and the combined 64 KiB idle-storage test. Its separate profiles and
interrupted maximum screen remain preserved; no retention decision is complete.
Read their current summary/state files; do not treat partial windows as completed.

Disk pressure reached 99% and Scylla rejected writes from the unchanged reference.
Free space recovered before the recorded cache cleanup command. The subsequent
30-second reference health check achieved 61,824.17 acknowledged writes/s with
zero errors, timeouts, rejections or affinity failures. This is environment health,
not a driver speedup. All failed runs and original target rates remain preserved.

If explicitly resumed on this same host, inspect container state, free storage,
CPU topology, boot ID and network delay before unpausing the benchmark nodes.
Keep the client on CPU 8 with GOMAXPROCS=1. Preserve the current endpoints and
backend identity when using a confirmation resume; otherwise obtain a new health
qualification and backend baseline, keeping all old rates/results. Use new output
directories. Never use provisioning start/tune to overwrite datasets or stop
unrelated services. Do not lower matched/historical rates or relax validation.

Most promising unfinished lead: native v4 unextended VOID acknowledgement fast
path, screened at roughly 5.3% less CPU and six fewer allocations versus the
preceding prototype. It still needs isolation from rejected ownership changes
and full confirmation. Framing and parameter scratch, value handles, validation
caches and stream scratch are also preserved experimental variants. No breaking
API experiment was adopted.

The remaining work is canonical per-change retention decisions, worker/scalability
and noise diagnostics, full emulator/local final matrices against all three refs,
final-source correctness and mixed-memory qualification, and two complete profile
passes without reproducible improvement. Current completed convergence passes: 0.
Master remains a comparison reference with no 10% winning gate. No release or push
is authorized by this parking request.
