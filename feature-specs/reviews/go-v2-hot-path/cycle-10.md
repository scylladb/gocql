# Hot-path design review cycle 10: Whole-design cost and lifetime reconciliation

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Nested skips still revisit fragmented child bodies even after shallow validation.** Final cross-check found that cycle 03's broad linear-traversal claim did not follow from cycle 01's O(spans-crossed) Skip. Required a once-per-source shared span-offset index and endpoint jumps, and made the bound precise: O(N + S) for nonoverlapping streaming, plus O(J log S) for nested/random jumps. Updated API and scaling gates without allocating indices per cell or scanning descendants.

2. **Temporary row-index scratch wording conflicts with escaping DynamicRow ownership.** The earlier seam grouped custom/dynamic indexing under pager scratch reuse while the dynamic section correctly required row/page-owned indices. Restricted scratch to temporary Decode access and explicitly routed escaping DynamicRows to their own immutable index lifetime.

## Re-review

Read the complete final diff and cross-checked all cost/copy/allocation, compilation, cache-generation, paging, routing, feedback, ownership and implementation-gate passages. Re-reviewed both corrections and their API/gate references; reflowed only newly extended prose. Ten-cycle limit reached; all 19 identified findings were fixed. This is not an additional independent clean review or measured runtime validation.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `65080d730a3ea0b882775e69ff7595d7260c89d52206ce4eadb74c80181d25ad` | `65080d730a3ea0b882775e69ff7595d7260c89d52206ce4eadb74c80181d25ad` |
| `feature-specs/gocql-v2-api.md` | `d597f93615a6a1428eda5070ec4abd34d28fa460b1d9ff330f764fd2529b7bb7` | `12abb35eaa2f3b851202056c7964590676a84ed688a2fd7a2d0b7320ffde780d` |
| `feature-specs/gocql-v2-implementation.md` | `aedad4a9dad4ddfca90c26510eaa8adc85e30e12bf4de7ca0433eb5648bb29c7` | `15d1d0f220705630d20d0f89c65896b1bd65dd71f4a34e45782f4b3df8f6b51a` |
