# Hot-path design review cycle 09: Enforce performance contracts during implementation

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

1 findings; 1 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Copy/allocation gates do not detect algorithmic regressions, and final-stage measurement is too late.** Expanded the normative gate with independent cell/span/host/history/concurrency scaling, type-plan/descriptor/refcount counters, scheduler and mutex/block evidence, TLS amplification and retained memory. Scoped performance checks now gate each implementation handoff; final agents compose and rerun them. Reconciled plan status with this separate design-only review ledger.

## Re-review

Re-reviewed all new cost requirements against milestones, the seventeen future agent responsibilities and the benchmark table. Existing correctness, all-protocol zero-copy, final-tree and 174-test requirements remain mandatory. This cycle changes verification design only; it does not report benchmark results.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `65080d730a3ea0b882775e69ff7595d7260c89d52206ce4eadb74c80181d25ad` |
| `feature-specs/gocql-v2-api.md` | `d597f93615a6a1428eda5070ec4abd34d28fa460b1d9ff330f764fd2529b7bb7` | `d597f93615a6a1428eda5070ec4abd34d28fa460b1d9ff330f764fd2529b7bb7` |
| `feature-specs/gocql-v2-implementation.md` | `d1f49e6a501e7cb91e0b4caff1df5364e4152e95a7595798cbdbca806f00506c` | `aedad4a9dad4ddfca90c26510eaa8adc85e30e12bf4de7ca0433eb5648bb29c7` |
