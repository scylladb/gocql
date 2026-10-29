# Hot-path design review cycle 04: First-target routing and latency selection

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Cheap-first-target intent does not prevent O(hosts) work on every request.** The disposition table mentioned a cheap target, but no concrete contract excluded host-map construction, public metadata copies or eager fallback plans. Required publication-time indices, logarithmic range lookup, replica-proportional first selection and lazy deduplicated fallback. Strict targets, deterministic LWT order and serialized speculative advancement remain intact.

2. **Latency awareness can add a global mutex and full-cluster sort per request.** Required per-host sampling plus periodically published decision snapshots, while timely concurrency-safe probes and acquisition-time eligibility prevent stale snapshots from admitting excluded hosts.

## Re-review

Re-reviewed all routing, public metadata copies, target/pool, leader/tablet, retry/speculation and latency-option passages against the new cost contract. Immutable replica order, unknown-tablet behavior, remote/locality exclusions and user policy callbacks remain authoritative.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `f2d828a25aace8da65840664d59f957b165dea29ed7d7f5aa89c7d1eb2043999` | `a0b3aafc30e1a340e75817fa9e2a9d5a42a2242d05a38bf52e41d1c74d6ca743` |
| `feature-specs/gocql-v2-implementation.md` | `522bb187763fafe4a882a47e92a73e01932b5fad7be385011c3e0db5b822babf` | `48da006d33c19304fecb590332599f7fbadf5e3dc0bddcfdb243de785ad9f4a7` |
