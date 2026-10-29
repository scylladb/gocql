# Hot-path design review cycle 07: Admission, observation and scheduler overhead

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Immutable profiles/options can still be copied and revalidated on each attempt/page.** Required one resolved admission snapshot and one batch payload-descriptor copy, preserving all per-member input drains and per-attempt negotiated checks.

2. **Disabled features and metrics can impose steady scheduler/allocation/contention costs.** Required nil-observer event elision, no disabled-speculation scheduling or Metrics calls, demand-driven/reused pager workers and deadline timers, and bounded striped sampling with periodically published percentile summaries. Exact observer/policy callbacks and request counts remain required.

## Re-review

Re-reviewed zero Options/default prefetch, profile Replace races, custom payload drains, observer panic containment, metrics/Result fields, retry/speculation and session cancellation. No optional-work optimization can bypass admission identity, configured callbacks or close/wait obligations.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `2960977e0ef50b2d29c9a5095e19d4e30d26db5d8306645d824f8d93acdaf593` | `fbcf09754f990b546f0eb1cbd3fd3abbdb7036cef74fed249e39e53729cc62e7` |
| `feature-specs/gocql-v2-implementation.md` | `a3e0f792011b2e8144e8c760076bf6aa1527c76e85a3c69505c0ad5b014e0c09` | `d3fde18da440d41896e5dfe25d173beaa98e6f38cdd4a61e0786587767756bb0` |
