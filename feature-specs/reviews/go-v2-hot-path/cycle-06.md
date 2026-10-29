# Hot-path design review cycle 06: Paging history and prepared-cache scope

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Complete paging-cycle detection can be quadratic or copy each token repeatedly.** Required hash-indexed exact equality, incremental allocation and one shared immutable owned token for internal history/sending. Count and byte budgets, empty continuations, A-B-A detection and explicit public copy semantics remain unchanged.

2. **Global metadata generations/LRU hits can make warm prepared execution cold or contended.** Required scoped dependency epochs, direct live-handle entry access and per-key lazy preparation coordination. Unrelated changes cannot cause a global reprepare storm; incomplete dependencies still use conservative validation.

## Re-review

Re-reviewed every paging-state, generation/cache, topology publication and reprepare passage, including live handles surviving eviction. Verified collision-safe equality, count/byte exhaustion before next fetch, retained response metadata and cancellation of lazy-prepare waiters.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `a0b3aafc30e1a340e75817fa9e2a9d5a42a2242d05a38bf52e41d1c74d6ca743` | `2960977e0ef50b2d29c9a5095e19d4e30d26db5d8306645d824f8d93acdaf593` |
| `feature-specs/gocql-v2-implementation.md` | `687f0384990006249d661f4f251cf8ba9037df42fa75b6c60463e0314fc38fa4` | `a3e0f792011b2e8144e8c760076bf6aa1527c76e85a3c69505c0ad5b014e0c09` |
