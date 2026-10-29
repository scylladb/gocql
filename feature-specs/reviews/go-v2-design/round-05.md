# Go v2 design review round 05: Metadata, control and events

Reviewer: `/root/design_reconciliation/round05_metadata`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

Plan dependency rules and control/metadata invariants; API snapshot/event,
subscription and agreement contracts; implementation metadata options/query seams
and feature disposition; local metadata-control-events reference.

## Findings and dispositions

4 findings; 4 fixed; 0 unresolved.

1. **Overflow/resync gap (API:852–854,841–842).** A full channel could lose its own overflow marker and snapshot reads could race queued events. Defined positive bounded capacity, guaranteed marker after discarding deltas, then subscription termination. Resubscribe before snapshot/refresh and ignore publication events at or below its generation. Publish snapshots before events; Close never requires draining.
2. **Undefined drop/route fences (plan:430–434; API:857–860; implementation:525).** Defined fetch-start epochs, per-object and route-pair fences, durable tombstones, fresh recreation identity, and discarding superseded full/partial data. Recoverable errors cannot revive a dropped schema; unrelated failed reads cannot suppress tombstone publication.
3. **Internal query seam/import cycle (plan:387–391; implementation:435).** Added nongeneric injected InternalQuerier/encoded query/raw owned page seam in internal/model. Metadata uses typed codec adapters and does not import public Session, execution or cluster. Defined control targeting for topology/schema/routes, direct-host agreement, eligible-host tracing, page/input release, and subsystem-specific contexts without user profile leakage.
4. **Agreement scope (API:860–861; implementation MetadataOptions).** Defined enabled-host snapshot scope including zero-token hosts, filtered exclusions, missing/broken peer retry, no vacuous agreement, fresh snapshot per check and bounded deadlines. Automatic DDL requires its exact coordinator even after topology changes; manual wait does not refresh; post-success failures never replay the DDL.

## Additional behaviors checked

Defensive metadata copies; retained snapshots; incomplete topology and invalid
tokens; surfaced partial-refresh errors; metadata modes/filters; event variants;
retained pages independent of shutdown.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `b94fc0b5476ff81f71b8b13bf8098ead8e4a20f731142a4a2015f6683fb49d50` | `7de1ca35b5ca4514a85ee82804414dcfa40001dde6d7c15120cd277dc5bdee21` |
| `feature-specs/gocql-v2-api.md` | `c69d4a676de24b52af7380790af327c1d9550681c4f06477e8e2b624dd23acf9` | `1347c5ab8bd0af0ddd0d6cefce0fa39aecae75ab22d3f5d27d8954a470528e0b` |
| `feature-specs/gocql-v2-implementation.md` | `05ad43c52d10671535a6730e85a0cfa351000088afd8baaf8d0ffde18dde2655` | `103f91215126d2ad499c24637075a5ae0cbdb7d2dff663d11a7dcf07a979cdc2` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
