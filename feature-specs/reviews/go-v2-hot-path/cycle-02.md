# Hot-path design review cycle 02: Compiled codecs and result-plan reuse

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Nested built-in codecs can redo schema compilation per element.** Compile-once wording covered outer plans but not tuple/UDT and collection child bindings. Required recursively compiled internal typed adapters and immutable descriptor access; runtime semantic validation remains mandatory, and custom codec methods keep their contract.

2. **Every page can rebuild RowPlan despite unchanged metadata.** “Selects/recompiles” did not require a cache and allowed repeated name/type work. Required O(1) exact-entry hits, structural checks once for a new entry, handle/decoder-scoped bounded plan storage, and independently retained page plans. User compilation runs outside cache locks.

## Re-review

Re-reviewed the cycle-01 range/row changes and all CheckType/Compile, standalone, prepared-generation and empty-page passages. Verified that caching does not weaken full metadata compatibility, NO_METADATA fences, custom codec safety or schema-independent Clone.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `8a78c796b69648ae7f917f6a409654996b3da612d2456defe733e45fd2f45a50` | `6ffbc1ac42d56df565824c8f549c18000864941ac7381965144b597ef7547add` |
| `feature-specs/gocql-v2-implementation.md` | `b26f049b65eb9d5ecd7d82467edc45dc02c64e46fcd18d1b4dacf527874d5f3b` | `bd55b6dcdcb4c35f2a6a7dd1450e176f2f2fe8fc54cf1c069d54a1246eb1ca0e` |
