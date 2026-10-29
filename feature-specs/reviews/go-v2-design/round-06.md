# Go v2 design review round 06: Routing, tablets and LWT safety

Reviewer: `/root/design_reconciliation/round06_routing`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

API Statement/PreparedInfo/RoutingInfo, retry/speculation and policy target seams;
plan tablet/leader requirements; implementation feature disposition; local routing
reference v1/v2/leader/range rules and conditional metadata limitations.

## Findings and dispositions

4 findings; 4 fixed; 0 unresolved.

1. **Unknown LWT classification (API:453–476,480,721,821–823).** Cassandra and direct QUERY lack negotiated conditional markers. Added explicit unknown/ordinary/conditional StatementKind. Unknown disables speculation; confirmed conditional metadata overrides declarations; regular serial forces conditional; optional serial alone does not. Batches merge classifications conservatively. Ordinary declaration is an explicit correctness assertion, never a CQL parser heuristic.
2. **Strictness lost across policy seam (API:694–696,726–730,794).** Added Target.StrictShard and preserved it in attempts/pool selection. Missing/out-of-range/unavailable strict shards error without fallback; host pinning cannot cross hosts.
3. **Tablet/leader enumeration (plan:453–456; implementation:561).** Specified v2 precedence and EXECUTE-only version byte, opaque equality-only versions, nibble hints, checked nonwrapping ranges and overlap replacement, original replica-order leader provenance, strong-consistency/token/locality eligibility, unknown-tablet fallback and table/schema/placement-epoch feedback fencing. Leader implementation remains mandatory.
4. **Bounded feedback versus applied-once promise (API:824; implementation:561).** Replaced unconditional delivery with accepted-at-most-once coalescing and a guaranteed bounded invalidation signal on saturation. Mark affected/global placement unknown, surface a diagnostic and relearn; never block successful replies or retain known-stale mappings.

## Additional behaviors checked

Canonical custom-partitioner streams; unknown-partitioner diagnostics; immutable
reprepare routing; DC/rack and remote failover; deterministic conditional order;
unknown tablet ranges never use vnode placement; required leader path preserved.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `7de1ca35b5ca4514a85ee82804414dcfa40001dde6d7c15120cd277dc5bdee21` | `d215bced05949448600172082b8095a424a60ddf0294b5fade04a89d35c54814` |
| `feature-specs/gocql-v2-api.md` | `1347c5ab8bd0af0ddd0d6cefce0fa39aecae75ab22d3f5d27d8954a470528e0b` | `cfe5d88bb5b26f38b0aced78f006fee355123afd1ab4bca6479bc2795a564903` |
| `feature-specs/gocql-v2-implementation.md` | `103f91215126d2ad499c24637075a5ae0cbdb7d2dff663d11a7dcf07a979cdc2` | `4a39a982901c4b400ac81d1041eb4658bb8405687948fcf2f1e7c2310d181ebd` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
