# Go v2 design review round 08: Configuration, security and observation

Reviewer: `/root/design_reconciliation/round08_config`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

API options/profile/lifecycle, callbacks and TLS/security paragraphs;
implementation Config, pool budgets, auth/dial/TLS hooks, routes and durations;
local configuration-security-observability reference. Root also independently
flagged the smaller-stream-cap budget scenario.

## Findings and dispositions

5 findings; 5 fixed; 0 unresolved.

1. **Unreachable orphan recovery (implementation:300–304,496–498).** Effective budget is min(configured budget, effective stream capacity minus one), preserving zero behavior and all v3/v4/v5 native IDs. Default configuration remains valid with smaller explicit request caps.
2. **Callback cancellation/reentrancy (API:717–719,880–883,962–969; implementation:401,409).** Made TLSForHost context-aware; all I/O hooks obey context. Defined shared concurrency, no driver locks around user callbacks, prompt/no-blocking-I/O synchronous hooks and no blocking Close/Wait reentry. Arbitrary caller code is not preemptible. Rechecked TLS snapshot depth and explicitly copied mutable trust/certificate descriptors while documenting shared provider/key/cache objects.
3. **Singleton-only client routes (implementation:462).** Added ConnectionIDs and hostname overrides, sticky ordered selection, deletion/failure replacement under one deadline, route-generation fences, no direct fallback, explicit incompatible translator/shard-source configuration and verified TLS requirements.
4. **Nil TLS precedence/downgrade (implementation:478–480; API:988–989).** Nil per-host config inherits base TLS; required TLS with no usable config fails, never downgrades. Per-connection snapshots and logical SNI are explicit. Extended late-opening fences to route/proxy/SNI/TLS generations.
5. **Duration/default ambiguity (implementation:389–395,448–457; API:661).** Added normative defaults/zero/negative validation table for setup/DNS/read/write/keepalive/coalescing/profile/metadata/agreement/linger. Local disabled timeouts do not disable caller/session cancellation; unsupported settings fail explicitly.

## Additional behaviors checked

Immutable profile/admission snapshots; raw target/source hook; fresh per-connection
auth exchanges/cleanup; rewritten-address TLS identity; explicit protocol options;
options payload drains; conservative speculative classification.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `d215bced05949448600172082b8095a424a60ddf0294b5fade04a89d35c54814` | `ee59260dbcd057c2cb1264a7a61705c7bbb9678b0ae7be6757d43403a029787e` |
| `feature-specs/gocql-v2-api.md` | `81041900ed119b70e4afdd14a5f7e707dad67837b15bd65b59cb89c37950a38f` | `0dac706a10a69f476510bb70327142b75f3459b8005bf83693516afa1919436b` |
| `feature-specs/gocql-v2-implementation.md` | `2982fd008a1027f2fcb691d1b18f1bafb8b263c44ef6d6e623cea2f7525f4985` | `3269b04a9e60f34c0179a161346da7c7f53b8bf09dcb91a7f5f8c6b7d4c67b9d` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
