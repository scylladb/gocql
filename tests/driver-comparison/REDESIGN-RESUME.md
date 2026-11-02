# Resume the parked reusable-query redesign

Parked at the user's request on 2026-11-02T18:32:01.991837+00:00. Resume implementation or
benchmark loads only after an explicit request.

The code and report are preserved on `park-gocql-redesign-20261102T183201Z`. The original
branch `v2-17-release-verification` remains at `6129d9545d96e41d4dc8c11903dba7612cd6dcc0`.
The parking commit contains the experimental changes, not an accepted release.

Read [the report](REDESIGN.md), [state](REDESIGN-STATE.json), and
[checkpoint receipt](REDESIGN-PARKED.json) first. All five measured emulator
cases miss the 10% master target; the healthy original-v2 write control gate
also failed. Tests and live-v4 correctness passed; live v5 was unavailable on
the benchmark image. Earlier failed live attempts remain in the evidence.

The verified archive and Git bundle are in `/extra/scylladb/gocql-parks/redesign-20261102T183201Z`. Check the archive
SHA-256 and per-entry manifest before restoring files. Restore to a separate
checkout/directory first; do not overwrite parked evidence. The bundle includes
the final commit and parking branches. Benchmark server data remain at their
existing stopped container mounts. Unrelated services remain running.

No client or server load was restarted for parking. Use a new result directory
for future experiments. Dedicated benchmark storage or a host remains the
fallback prerequisite for real performance qualification. Continue the remaining
implementation and acceptance work listed in the report; do not infer acceptance
from the successful correctness checks.
