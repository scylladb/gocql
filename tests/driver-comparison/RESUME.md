# Resume the parked comparison

The experiment is parked at the user's request. Do not start more work until
asked to resume. Read [PROGRESS.md](PROGRESS.md), [FINDINGS.md](FINDINGS.md), and
[BENCHMARKS.md](BENCHMARKS.md) first. The remaining work is substantial: 25 primary
runs, at least six sustainable-read repeat runs, write/paged pilots, and final
verification. No final write or paged-read performance conclusion exists.

## Preserve this checkpoint before resuming

Work from `/extra/scylladb/gocql`. Keep the final run directory and its frozen
`sources/` snapshot intact. The interrupted
`read-sustainable-v2-initial-3` has no result JSON: its partial logs and metrics
are evidence, not a completed round. The runner would overwrite those files
when retrying that name, so retain a snapshot first:

```sh
cp -a tests/driver-comparison/results/2026-11-01 \
  tests/driver-comparison/results/2026-11-01-parked-snapshot
```

Use a new snapshot name if that destination already exists. In particular,
preserve `environment.json`, `sources/`, `state-before-park.json`,
`REPORT-before-park.md`, `park-audit.json`, `shutdown.json`, and `run.log`.
The current report supersedes the premature sustainable acceptance label in
`REPORT-before-park.md`.

No driver source was edited. All benchmark additions are untracked under
`tests/driver-comparison/`; no commit, push or publication was made. Old source,
binaries, calibration, retained data, and provisioning logs are in the ignored
`.codex-cache/driver-comparison/` directory. Preserve it too.

## Check identities and environment

1. Verify the repository remains at v2
   `5a07badd5f3a567f3990e0c70aba6c8d28e7fbc0`, and the old worktree at
   `1ceaac8172bda26e8b8d701ac356982d33729b27`. These are the original pins;
   a later movement of remote master does not change this comparison.
2. Compare retained binaries with the SHA-256 values in
   [BENCHMARKS.md](BENCHMARKS.md) / the original environment record. Compare
   implementation files with the frozen source hashes. New checkpoint Markdown
   files were added after that source snapshot; do not rewrite the original
   snapshot to make them appear contemporaneous.
3. Check disk free space and competing host work. The final setup had only
   about 64.9 GiB free on a shared, 97%-full NVMe filesystem. Clear snapshots only
   for this benchmark's `bench_compare` keyspace, as the runner does. Existing
   Alternator data/configuration must remain untouched.
4. Inventory running Scylla and any associated experiment runners again.
   `provision.py resume` starts this benchmark but does not repeat `start`'s
   inventory/stop logic for other clusters. If someone has restarted other
   instances, preserve and stop them within the original authorization before
   continuing. Leave unrelated services running.
5. Restore server/client/monitor CPU assignments, physical I/O properties,
   container/ancestor limits, qdiscs and connection equivalence. All application
   clients, including setup/verification, must use CPU 8 and GOMAXPROCS=1.
   Monitor/server work must stay off CPUs 8 and 24.

The parked benchmark nodes and old Alternator containers are stopped. Their
data and mounts remain. The benchmark network remains. See the final
[shutdown evidence](results/2026-11-01/shutdown.json).

## Continue with the same implementation

The existing runner can reuse completed JSON files and the frozen point-read
selection. It will rerun setup correctness, complete the missing v2 sustainable
round, assess the initial sustainable sets, repeat affected sets once, and then
move to write and paged scenarios. A restarted container may receive a different
IP; `resume` records current addresses. The seven-connection count and original
host identities still need verification for the resumed session.

After preserving the checkpoint and checking the environment:

```sh
python3 tests/driver-comparison/provision.py resume
python3 tests/driver-comparison/run.py \
  --output tests/driver-comparison/results/2026-11-01 \
  > .codex-cache/driver-comparison/resumed-run.log 2>&1
```

Do not use `start` or `tune` on these retained container names/data. `start`
requires fresh names; `tune` is for initial calibration. `resume` reuses the
already calibrated properties. Rebuild with `build.sh` only if needed; compare
binary identities before mixing results. If implementation or workload settings
change, start a separately labeled experiment and retain these observations.

The runner will overwrite its mutable environment/source snapshot and cached
partial-run files on resume, which is why the parked copy is necessary. Its
status field is descriptive; it does not enforce a paused state. Do not launch
it merely to inspect results. The final wire probes are cached in `probes/`;
capture any resumed-session verification separately without overwriting the
parked captures.

## Outstanding measurements and acceptance

- Point read: retain the accepted 3+3 maximum rounds and the fixed 64-worker
  buffered choice. Finish sustainable v2 initial round three. Legacy's initial
  sustainable set already fails both rejection and p90/p95 variability rules;
  both completed v2 sustainable rounds also rejected arrivals. Run a complete
  three-round repeat for each affected driver. Do not silently enlarge the queue
  or lower rates to turn failures into passes. Any experimental change needs a
  separate labeled result set.
- Point write: pilot both drivers at 1/4/16/64/256/1,024 workers, select capacity,
  then three maximum rounds and three sustainable rounds per driver.
- Paged read: pilot legacy and both v2 adapters, run the median-based v2 adapter
  selection, then the same maximum/sustainable matrix. Count a complete partition
  as one operation and report rows/s and blob MiB/s as well.
- Repeat unstable three-round sets once, keeping originals. Continued errors,
  rejections, backlog or CV failures must be explicit in the final report.
- Keep alternating driver order and performing identical resets, flush/agreement/
  settle checks, and complete dataset warming before every measured run.

## Reporting and verification work still needed

The report generator used `not state['instability'].get(set)` as its stable flag.
An unassessed complete set could therefore appear stable before assessment.
The parked report and state now correctly mark legacy sustainable as failed,
but the implementation still has this reporting weakness. Before relying on
new reports, require an explicit completed acceptance assessment; unknown must
remain unknown. Keep the original source snapshot as evidence if changing it.

The runner's existence-based reuse does not check all source/configuration
identities automatically. Its full `verify.py` checks a completed matrix and
must not be presented as passed for this partial checkpoint. The parking audit
checked 11 completed rounds, 88 raw histogram snapshots, 22 pilots, 110 completed
process records and 15 source identities; see
[park-audit.json](results/2026-11-01/park-audit.json).

Re-run the harness checks listed in [BENCHMARKS.md](BENCHMARKS.md) if changing
the harness. When the full matrix is finished:

```sh
python3 tests/driver-comparison/verify.py \
  tests/driver-comparison/results/2026-11-01
```

Verify all 36 primary round files plus retained repeat sets, warmup/measurement
durations, histogram/accounting integrity, target derivation, source/binary
identities, routing, affinity and wire settings. Record a final report with
median/min/max throughput and latency, CVs, resources, failures, sustainable
absolute rates and limitations. Investigate performance causes with profiling
outside timed runs only if continuing that investigation is requested.

The runner normally calls shutdown on full exit. Still verify that no benchmark
runner/client remains and that `shutdown.json` confirms closed native,
shard-aware, monitoring and Alternator endpoints. Explicit cleanup if needed:

```sh
python3 tests/driver-comparison/shutdown.py \
  tests/driver-comparison/results/2026-11-01
```

Keep existing clusters stopped with preserved data/configuration unless the user
asks to restore them. No push, merge or publication belongs to this task.
