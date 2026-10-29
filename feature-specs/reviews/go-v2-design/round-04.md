# Go v2 design review round 04: Transport, pools and cancellation

Reviewer: `/root/design_reconciliation/round04_transport`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

Plan transport/pool invariants; API session lifecycle and retained pages;
implementation writer queue, spans, streams, Config/PoolOptions and feature
disposition; local connections reference cancellation, filling and shard fallback.

## Findings and dispositions

3 findings; 3 fixed; 0 unresolved.

1. **Session shutdown completion (API:834–835,586).** Defined admission rejection and Closed at shutdown initiation, cancellation, forced connection close to unblock I/O, accepted-writer/worker drain, undelivered page release, active row/page ErrClosed, idempotent/concurrent completion, and independence of application bindings/retained pages.
2. **Pool sizing ambiguity (implementation:368–375,424–426).** Positive per-host count overrides positive per-shard defaults; zero is unset for the host count, both zero/negative counts invalid, per-host mode disables targeted filling but retains shard selection. Defined stream range 1..32,768, excess zero, source-port bounds/fallback and terminal closed state. Endpoint/reshard generations fence late opening results.
3. **Orphan budget and queue cancellation (plan:414–423; implementation:372,492).** Defined stable request identity and persistent cancellation state across stream allocation; delayed cancellation cannot orphan a reused stream. Count all abandoned transmitted streams, without age exemption; exceeding the nonnegative budget closes/poisons/replaces the connection. Default 1,024; zero closes on first orphan. No budget recovery recycles outstanding streams.

## Additional behaviors checked

Canonical span identity; distinct input/stream/page owners; syscall completion
before input release; partial-write poisoning; coalescing/writev order; private
mutable net.Buffers scratch; raw selected target/source hook; one TLS application;
logical TLS identity under proxy routing; independent retained receive leases.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `cf30c37b5e1e0c50be195e2115974fd62392ca6c5f4fd31cbc140b93b6011ec4` | `b94fc0b5476ff81f71b8b13bf8098ead8e4a20f731142a4a2015f6683fb49d50` |
| `feature-specs/gocql-v2-api.md` | `ef1252941b1408da4ee778939b6b3e0fae940f72ba0b58a867eb4f0ae618fd2a` | `c69d4a676de24b52af7380790af327c1d9550681c4f06477e8e2b624dd23acf9` |
| `feature-specs/gocql-v2-implementation.md` | `58a8d6d2a1947a21d45c6637a170bcef5bc7f987f63d2d5b2650f04aba7991a9` | `05ad43c52d10671535a6730e85a0cfa351000088afd8baaf8d0ffde18dde2655` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
