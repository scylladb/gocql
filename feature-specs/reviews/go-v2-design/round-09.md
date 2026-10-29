# Go v2 design review round 09: Packages, migration and verification matrix

Reviewer: `/root/design_reconciliation/round09_migration`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

Plan module/dependency rules, verification matrix and all 17 agent rows;
implementation historical ownership, internal query seam and acceptance gates;
API facades and migration rules; baseline manifest identities/counts.

## Findings and dispositions

3 findings; 3 fixed; 0 unresolved.

1. **Contradictory old ownership (implementation:158–159,240–241,659–675; plan:591–616).** Removed runtime conn.go ownership and simultaneous exclusivity. Marked the old owner table as historical same-package responsibility seams; made the 17 sequential agent branches/gates authoritative, with explicit mapping and extraction by 11–12.
2. **Mismatched mandatory matrix (implementation:741–747; plan:503–513).** Replaced optional Scylla-v5-centric gate with stable/development Scylla v4, mandatory Cassandra v4/v5, auth/cloud/TLS/Unix/CCM/injected/race/fuzz lanes. Unsupported Scylla v5 is a skip, never a v5 pass. Preserved actual per-identity applicable passes, successful lane exits and final-tree evidence.
3. **Unsupported default server timeout (implementation:563).** Default ServerTimeout is now unset/automatic: negotiated 30-second extension only where supported, omitted on Cassandra. Explicit positive unsupported requests still error; explicit zero omits. The independent client deadline remains 31 seconds by default.

## Additional behaviors checked

Manifest remains 174 identities across 29 files; assertions and intentional API
translations retained; no skips counted as passes; final pinned-tree evidence;
benchmark-module consolidation; no facade serializer; injected nongeneric internal
query seam avoids metadata→execution→cluster dependency cycles.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `ee59260dbcd057c2cb1264a7a61705c7bbb9678b0ae7be6757d43403a029787e` | `ee59260dbcd057c2cb1264a7a61705c7bbb9678b0ae7be6757d43403a029787e` |
| `feature-specs/gocql-v2-api.md` | `0dac706a10a69f476510bb70327142b75f3459b8005bf83693516afa1919436b` | `0dac706a10a69f476510bb70327142b75f3459b8005bf83693516afa1919436b` |
| `feature-specs/gocql-v2-implementation.md` | `3269b04a9e60f34c0179a161346da7c7f53b8bf09dcb91a7f5f8c6b7d4c67b9d` | `3b51f7c3e420818a537550504326a76beffd643b912d27908182ece7c8f63d74` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
