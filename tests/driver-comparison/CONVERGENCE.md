# Driver optimization by measured convergence

This is the active optimization policy, replacing the former master-win targets.
Work resumed on `optimize-convergence-20261102`. The implementation and performance
qualification are incomplete. [State](CONVERGENCE-STATE.json) records the current
experiment, checks, evidence, and prerequisites. No release or push is authorized.

Master `1ceaac8172bda26e8b8d701ac356982d33729b27`, original v2
`7e1ad6f718929342200c5b9bc126e5a5e7fc7d1c`, and parked redesign
`f363e371832e7d8260e2858fba867fe591f655a4` are frozen references. Master is a
comparison reference, with no minimum winning margin. Compare each experiment
with the latest retained candidate, then report cumulative comparisons with all
three references. Keep historical branches, archives, failures, and rejected
experiments. Every invocation must use a new output directory.

## Measurements and decisions

Use the existing `redesign_compare.py`, `roadmap_run.py`, and `run.py` harness.
Production workloads retain full SHA-256 payload validation, native v4,
LOCAL_QUORUM, no TLS or compression, zero configured coalescing, page size 32,
prefetch disabled, matched connection budgets, and no application retries or
speculation. Point buffered reads and acknowledged writes use 64 workers; point
streaming and both complete-partition variants use four. Complete partitions
consume and validate all 128 rows. Master uses its buffered adapter for both
streaming comparisons, at the corresponding worker count.

`--stage baseline` obtains three master maxima for each selected case and worker
count. All observations must be healthy; capacity CV must not exceed the retained
5% baseline stability limit. It writes `frozen-rates.json` only after the whole
baseline set succeeds. The file binds 70% and 85% rates to master identity,
backend, endpoints, affinity, worker count, raw results, and harness provenance.
A failed or unstable baseline supplies no matched rates. Do not select favorable
observations from it or lower the historical controls.

The arrival producer now delivers every arrival scheduled inside the fixed
window, including timer wakeups just past its end, within the existing 15-second
drain deadline. Late completions remain excluded from window throughput and
retain their scheduled latency. Full-queue rejection and producer-deadline
rejection have separate counters. The overload exception applies only to actual
queue-capacity failures; ambiguous legacy rejections cannot establish overload.
The affected legacy matched-rate screen is retained as blocked harness evidence
under `results/convergence-local-scylla-20261102T233500Z`; corrected runs use
`results/convergence-arrival-correction-20261103T004400Z`. Maximum-throughput
observations and frozen capacities remain preserved.

For master rebuilt solely to correct the harness, provide the original
`--capacity-master-binary` and `--capacity-master-manifest` alongside the unchanged
rate file. The runner verifies the pinned production sources, compiler, unchanged
master adapter, and AST hashes of 13 canonical workload functions/constants.
It records both identities and the equivalence evidence without recalculating
any capacity or target rate. Both comparison clients use corrected delivery.

Screens have two alternating A–B/B–A pairs of 30 seconds. Confirmations have six
alternating pairs of 120 seconds. Each observation warms the full dataset, settles
for two seconds, and runs a five-second workload warmup by default. `--seconds`,
`--warmup`, and `--settle` are explicit and recorded. Nonstandard confirmation
durations, zero warmup, partial matrices, and multiprocessor diagnostics cannot
retain a candidate.

The default load matrix includes maximum throughput, 70% and 85% frozen master
capacity, and the unchanged historical absolute controls (including 52,503.01
writes/s). `--workers 1 4 16 64 256 1024` performs a separate sweep and requires
matching frozen baselines. `--gomaxprocs` and `--client-cpus` support separate
multiprocessor diagnostics. Canonical comparisons retain GOMAXPROCS=1.

For each metric, compute paired B/A ratios and a geometric mean with 20,000
bootstrap resamples of log ratios, reporting 95% confidence intervals. Benefits
must exclude 1 in the favorable direction. Maximum throughput and scheduled
arrival-to-completion p99 at matched rates are primary; maximum-load p99 and
fixed-rate throughput remain descriptive. CPU/op, bytes/op, and allocations/op
also participate in the benefit/regression decision. A confirmed regression in
any primary metric prevents retention. An inconclusive confidence interval is
not evidence of equivalence.

Outcomes are `retained`, `rejected`, `inconclusive`, or `blocked`. Retention
requires a valid, complete confirmation across all five canonical cases and all
four load modes. Screening cannot retain a change. Repeat a complete inconclusive
confirmation once using `--repeat-of ORIGINAL_DIRECTORY`; both sets are retained
and combined as 12 pairs. Continued ambiguity remains inconclusive. Backend-local
retention never grants final real-Scylla or release qualification.

Use `--resume-from INTERRUPTED_DIRECTORY` to finish an interrupted confirmation
in a new directory. Completed windows, including failures, remain in the combined
analysis. An attempt with no measurement result is archived separately; an
unprocessed result requires explicit recovery. Binary, measurement-code, rate,
duration, CPU, backend, boot, and matrix identities must match. Orchestration
changes remain visible in provenance. Resume does not repeat or replace a
completed unfavorable window.

The user authorized `--allow-reference-overload`: adoption may use valid confirmed
comparisons while overloaded-reference fixed-rate latency cases remain explicitly
blocked. Preserve every failed row and rate; do not infer benefits/regressions from
blocked metrics. Candidate-only delivery failures and unexpected errors still
prevent adoption. This exception does not qualify real Scylla.

Validity otherwise requires zero errors, timeouts, rejected arrivals, histogram overflow,
affinity violations, incomplete drains, or growing backlog. Fixed-rate throughput
must reach 99% of target. Warmup failures also invalidate observations. Queue
capacity remains `max(1024, 4 * workers)`, preserving the existing harness rule;
it cannot be raised to hide a failed baseline. All completed rows participate in
analysis, including unfavorable and invalid rows. A missing result or interrupted
warmup leaves an explicit incomplete observation and a blocked report.

## Reproduction

Freeze a v2 source tree with `roadmap_build.py --source SOURCE --output NEW_BUILD`.
Its manifest includes source and harness hashes, binary hash, and toolchain.
Copy the binary to a unique name directly in `.codex-cache/driver-comparison/`
for process affinity auditing. Keep the original frozen source and harness.
Pinned master can use the retained `redesign-master-wave3` binary and
`redesign-wave3/master-manifest.json`. Its harness identity is different from
new builds and is recorded explicitly. Rebuild references with a common harness
before final resource qualification.

For an emulator master baseline:

```sh
python3 tests/driver-comparison/redesign_compare.py \
  --master "$MASTER_BINARY" --master-manifest "$MASTER_MANIFEST" \
  --candidate "$CANDIDATE_BINARY" --candidate-manifest "$CANDIDATE_MANIFEST" \
  --stage baseline --emulator-pid "$EMULATOR_PID" --output "$NEW_BASELINE"
```

For an experiment against the parked reference (use `latest` for subsequent
retained candidates):

```sh
python3 tests/driver-comparison/redesign_compare.py \
  --baseline "$PARKED_BINARY" --baseline-manifest "$PARKED_MANIFEST" \
  --reference parked \
  --candidate "$CANDIDATE_BINARY" --candidate-manifest "$CANDIDATE_MANIFEST" \
  --rates "$NEW_BASELINE/frozen-rates.json" \
  --stage screen --emulator-pid "$EMULATOR_PID" --output "$NEW_SCREEN"
```

Use `--stage confirm` for confirmation and `--stage profile` for separate CPU
and heap diagnostics. Profiles run only at maximum load and cannot retain a
candidate. Unprofiled comparisons pass no profile prefix, force no collections,
and reject changed heap sampling. Raw histograms, process arguments, sampled RSS,
file-descriptor counts, source/harness/binary identities, execution order,
configuration, affinity, and profiling mode remain in each result directory.
New clients additionally report phase-boundary live heap and goroutine counts
without forcing GC. These snapshots do not establish a memory plateau.

The diagnostic `profile` stage also accepts `--synchronization-profiles` for
block/mutex sampling and `--syscall-trace` for a separately pinned strace
collector. Both options are rejected outside that stage. Tracing attaches only
to the coordinator's selected client and captures no payload bytes. Network
syscall completions are counted from measurement start through the recorded
drain end, with syscall counts/op, bytes/syscall, and write-batch byte
distributions retained beside the raw trace. Both binaries need the current
diagnostic harness. These observations never support performance retention.
`roadmap_build.py --driver old` freezes master with the same harness; the
default `--driver v2` covers both v2 references and experiments.

## Dedicated Scylla qualification

The user subsequently authorized local testing with the existing isolated
three-node cluster and network-delay script, with the client limited to one
vCPU. This supersedes the dedicated-host prerequisite for local evidence.
Use `provision.py resume --output UNIQUE_DIRECTORY --network-delay` to retain
the existing data. Server CPUs are `0-1`, `4-5`, and `6-7`; client CPU is `8`
with `GOMAXPROCS=1`. The existing helper verifies 200 µs one-way delay on every
client/node and node/node path. Preserve dataset snapshots before write runs.
Serialize emulator and Scylla load because their server CPU assignments overlap.

`local_cluster_receipt.py` captures the pinned image, RF3, two shards/node,
memory limits, storage configuration, CPU topology, original write controls,
complete dataset validation, and verified delay. Its receipt explicitly says
`dedicated_host: false`. Use `--local-cluster` with the runner to accept it;
the client must use exactly one CPU and GOMAXPROCS=1. Three healthy master
maxima per case are still required to freeze backend-specific matched rates.
Historical capacity failures remain blocked under the approved overload policy.
Local results do not establish dedicated-host qualification.

The runner requires `--backend scylla --hosts EXPLICIT_ENDPOINTS --nodes-json FILE
--qualification RECEIPT`. The JSON receipt must have `qualified: true`,
`dedicated_host: true`, the local `hostname`, matching `hosts`, and `references`
matching the three full revisions above. `cpu_roles` must assign nonempty CPU
lists to `client`, `server`, `monitoring`, and `development`. Roles must not share
physical cores, including SMT siblings. Use `--client-cpus` and `--monitor-cpus`
to match the qualified host's physical topology.

The receipt's `evidence` maps each of the following keys to a retained `path` and
`sha256`: `compiler`, `server_image`, `rf3_topology`, `cpu_topology`, `storage`,
`original_write_maxima`, `write_controls_52503_01`, `master_stability`, and
`wire_dataset_equivalence`. Paths resolve relative to the receipt. Qualification
must establish the pinned compiler/image, RF3, shards, memory and connection
budgets, calibrated dedicated storage and health, three healthy original-v2 write
maxima, three unchanged 52,503.01/s controls, stable master baselines, and complete
wire/dataset equivalence. Receipt hashes prevent evidence replacement; the
coordinator must verify the evidence's substantive qualification before marking
it qualified. The runner does not provision or certify a host from a declaration.

## Work remaining and convergence

Admission experiment A1 moves `InternalExecutionRequest` allocation into the
fallback branch. Focused counters show 13 → 12 allocations/op and 1,584 → 880
bytes/op, exactly 704 bytes saved. These are admission-only diagnostics and do
not establish a throughput or matched-rate latency improvement. Its patch,
profiles, compiler reports, and canonical screen are retained separately.

A1 was rejected at maximum-throughput screening because streaming partition
throughput decreased in both pairs. Its patch was restored out of the default
driver. See [the experiment report](results/convergence-admission-20261102/REPORT.md).
A2 parameter/value scratch was also rejected at its initial screen. All seven
implementation packages now have tested prototypes; later admission and parameter
scratch revisits remain separate experiments. Isolated variants cover value
handles, contiguous framing, cursor scratch and a checked VOID response fast
path. None has completed the retention policy. Root driver edits are experimental
and do not represent an accepted production candidate. The retained reference
remains the parked tree. See the state and resume documents for current manifests,
checks, confirmation jobs and the latest direct master screen.

Remaining work includes per-change confirmation decisions, the outstanding
transport/receive and scalability experiments, and final qualification of the
retained source. Reprofile after each retained change and preserve rejected
experiments separately. The two required complete convergence passes remain
unfinished.

Final qualification still requires the full emulator and authorized local-Scylla
matrices, cumulative results against all three references, syscall/batch/queue
and synchronization diagnostics, a 30-minute mixed-response RSS/live-heap test,
live reusable-query/paging correctness, and compatible-server live v5 coverage.
The historical server's v5 limitation remains explicit. No API experiment has
been adopted in this continuation, so no migration is yet required here.

Finish only after two consecutive complete passes over actionable profile costs
produce no reproducible benefit under this policy. Report remaining costs in
CPU/op (including application SHA-256 separately), retained-memory evidence,
server constraints, confidence intervals, and environmental noise. No convergence
or absolute-optimum claim is made at this checkpoint.
