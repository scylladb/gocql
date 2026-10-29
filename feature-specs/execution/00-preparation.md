# Execution preparation

The repository was inspected on 2026-10-29 at `/extra/scylladb/gocql`.
There are no applicable AGENTS.md files in the repository or its ancestor
paths. The active toolchain is `go1.25.0 linux/amd64`.

The complete tracked/untracked implementation checkpoint was committed as
`661f169` (498 changed files), preserving existing scaffolding and evidence.
Ignored generated PKI fixtures are retained in the external workspace archive
recorded in STATUS.md and remain available in the shared working directory.

Read `plan.md`, detailed API/implementation contracts and the streaming receive
feature contract before each task. The plan's historical pause is superseded
by the user's current explicit execution request.

Database fixture configurations remain in
`/extra/scylladb/gocql-api-test-cluster`. Their health and exclusive ownership
must be revalidated before use; historic process/session IDs do not establish
live reservations. Baseline unit checks require no database mutation.

A preparation agent owns streaming API/task reconciliation before step 01.
Root owns this execution ledger. Implementation agents will run sequentially,
start their assigned branches from the accepted predecessor, preserve others'
edits, and commit only their assigned changes and evidence.

Baseline validation: `go test -race -tags unit -timeout=5m ./...` completed
with exit 0 before implementation edits. Output is retained in
`feature-specs/evidence/execution-preparation-unit-race.log`; the Go test cache
was enabled. This establishes a starting baseline, not final-tree verification.

Streaming preparation owner: `/root/streaming_design_preparation` settled the
public entry point, explicit streaming codec/readers/builders, capacity rules,
exclusive pool read ownership, supported modes and bounded budgets. The
requirements are assigned across the existing 17 sequential steps, preserving
all prior acceptance scope. No driver implementation was changed in preparation.

Read-only API preflight: `/root/streaming_api_preflight` checked signatures,
independent plans, nested storage/reset/null behavior, retries, exclusive read
ownership, cancellation, modes, transforms and no-buffered-fallback rules. It
reported no blocking contradiction and identified one metadata budget reset/
overlap ambiguity for correction before the preparation commit. These checks
are separate from, and do not extend, the historical ten design review rounds.

`python3 feature-specs/verify.py` completed with exit 0: 144 Rust modules,
418 pinned source links, 60/60 reference review reports, 174 baseline identities.
This verifier establishes traceability only; implementation semantics and
release readiness require their own gates.

The metadata budget clarification was applied to the API, implementation and
streaming feature contracts before acceptance. It limits simultaneously retained
metadata/control including replacement overlap, accumulates lengths within an
envelope, releases accounting with storage, and keeps paging history separate.
`git diff --check` passes for the resulting preparation changes.
