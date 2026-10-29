# Hot-path design review cycle 01: Fragmented ranges and physical row access

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Repeated fragmented slicing can become quadratic and allocate per cell.** The wire contract allowed multi-span descriptor allocation and supplied no traversal complexity bound. Required shared immutable endpoint ranges, embedded cursor state and O(N + S) sequential descriptor traversal. Existing `internal/wire/cursor.go` already illustrates starting Take at the current span; this is a preserved design requirement, not a claim that all callers meet it.

2. **Named/indexed rows can rescan or allocate for every field.** The row seam required indexed descriptors without an execution strategy. Required compiled physical-order assignment, a streaming ordinary path, and at most one reusable row-local index when random access is actually needed. Views cannot capture reusable scratch.

## Re-review

Read the full three-document design and the wire/row seams; checked split cells, rollback, omitted tuple suffixes, ignored columns, custom access, and retained page ownership. New requirements preserve checked framing and the public accessor model.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `daceac2c2c28d8201739fb0ed7e0b4341c8e27e7d7eb56e533cfbb77ae4bc202` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `154a2201c22e8a8ba744e4dfc903998a5addae32dd70ec559d31d88d6c4289f2` | `8a78c796b69648ae7f917f6a409654996b3da612d2456defe733e45fd2f45a50` |
| `feature-specs/gocql-v2-implementation.md` | `167f40e2d980934bf82351f5de01780910ca3d136f4eb000b6c4872397598524` | `b26f049b65eb9d5ecd7d82467edc45dc02c64e46fcd18d1b4dacf527874d5f3b` |
