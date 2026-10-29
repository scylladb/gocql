# Hot-path design review cycle 03: Lazy container work and dynamic access

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Lazy aggregate validation can recursively revisit entire subtrees.** “Enclosing framing/count” was not scoped, permitting depth-multiplied scanning before iteration. Required immediate-child framing only, lazy descendant validation and compiled O(1) fixed-vector shape checks. Full enclosing validation is retained, and unvisited nested semantics are explicitly outside a lazy-success claim.

2. **Repeated DynamicRow.Get/At can scan all preceding columns each time.** The API exposed random access without an index strategy. Required one compiled name lookup and at most one lazy physical-offset index per row, with row/page ownership distinct from reusable streaming-row scratch.

## Re-review

Re-reviewed compiled child bindings, named row ordering and shared ranges against lazy iteration, DynamicRow lifetime, Collect/Clone, ignored fields and malformed framing. Clarified that typed streaming scratch and dynamic row-owned indices have different lifetimes; no returned view can reference reused scratch.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `6ffbc1ac42d56df565824c8f549c18000864941ac7381965144b597ef7547add` | `f2d828a25aace8da65840664d59f957b165dea29ed7d7f5aa89c7d1eb2043999` |
| `feature-specs/gocql-v2-implementation.md` | `bd55b6dcdcb4c35f2a6a7dd1450e176f2f2fe8fc54cf1c069d54a1246eb1ca0e` | `522bb187763fafe4a882a47e92a73e01932b5fad7be385011c3e0db5b822babf` |
