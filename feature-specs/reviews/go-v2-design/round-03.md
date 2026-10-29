# Go v2 design review round 03: Prepared metadata and paging

Reviewer: `/root/design_reconciliation/round03_prepared`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

API preparation/binding/reprepare, result metadata selection, row/page/state and
profile sections; plan prepared-metadata and routing invariants; implementation
attemptRequest/responsePage, limits and runtime gates; local execution reference.

## Findings and dispositions

3 findings; 3 fixed; 0 unresolved.

1. **Mutable routing schema (API:497,519).** Reviewer: changing partition-key indexes selects different canonical cells and a different token. Made index order, table identity, partitioner and conditional classification part of compatibility; an old binding rejects changes before resend. Only compatible per-host IDs update independently.
2. **Undefined CAS fence (API:524; plan:437).** Opaque metadata IDs cannot order remote schemas. Defined exact captured entry/local revision/schema epoch comparison; failed publication is discarded rather than retried against a fresh entry. Each page uses matching retained metadata even when publication loses. Local stale-update prevention is explicitly distinct from global schema chronology.
3. **Unbounded or incomplete paging history (API:579).** Consecutive-only comparison misses A → B → A; unbounded history violates resource limits. Defined full-token comparison across the automatic pager's history, including empty continuations, under positive configurable count (65,536 default) and bytes (16 MiB default) budgets. Added ErrPagingStateLimit for exhaustion; manual page calls remain stateless.

## Additional behaviors checked

Host-specific IDs; live handles after cache eviction; no re-encoding on retry;
changed metadata on empty pages; complete-state rejection; nil continuation;
profile/topology snapshots; independent output page ownership.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `6a0b01dc6e7c21dc46e5f1283a42f7c7e862c5b868c95c3ef5a2a2305ccf1d50` | `cf30c37b5e1e0c50be195e2115974fd62392ca6c5f4fd31cbc140b93b6011ec4` |
| `feature-specs/gocql-v2-api.md` | `06e25f22ba5580606cdfa92a7f84ac7b3c5a4bbf5ace6c7484f4d22ebd96d195` | `ef1252941b1408da4ee778939b6b3e0fae940f72ba0b58a867eb4f0ae618fd2a` |
| `feature-specs/gocql-v2-implementation.md` | `42520cff8afad6d55afe603ab9d416ebd77c97d99134909eecd4b40c39c41eaa` | `58a8d6d2a1947a21d45c6637a170bcef5bc7f987f63d2d5b2650f04aba7991a9` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
