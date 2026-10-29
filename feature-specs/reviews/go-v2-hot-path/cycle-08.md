# Hot-path design review cycle 08: Feedback publication and ownership atomics

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Frequent tablet feedback can turn immutable snapshots into a cluster-copy workload.** Required exact validated no-op detection, structurally shared affected-range updates and owned bounded feedback independent of page leases. Opaque-version equality cannot bypass epochs or content comparison; overflow invalidation and leader order stay intact.

2. **Borrowed range/view construction can perform an atomic retain/release per cell.** Required page-level ownership, one reference per distinct segment, and no view/element refcount churn in internal held-owner loops. Explicit leases and public released-view detection are preserved.

## Re-review

Re-reviewed tablet v1/v2 feedback, overlap replacement, placement invalidation, topology generation and event publication against routing-index reuse. Also re-read every owner-transfer/view/Close/Wait contract to ensure refcount amortization does not pool stable identities, extend row lifetimes or release borrowed storage early.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `fbcf09754f990b546f0eb1cbd3fd3abbdb7036cef74fed249e39e53729cc62e7` | `d597f93615a6a1428eda5070ec4abd34d28fa460b1d9ff330f764fd2529b7bb7` |
| `feature-specs/gocql-v2-implementation.md` | `d3fde18da440d41896e5dfe25d173beaa98e6f38cdd4a61e0786587767756bb0` | `d1f49e6a501e7cb91e0b4caff1df5364e4152e95a7595798cbdbca806f00506c` |
