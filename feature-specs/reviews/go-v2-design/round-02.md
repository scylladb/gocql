# Go v2 design review round 02: Ownership and leases

Reviewer: `/root/design_reconciliation/round02_ownership`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

Plan ownership and zero-copy sections; API standalone serialization, Bytes/Text,
lazy containers, dynamic values, Bound.Close/Wait, page leases and Options payloads;
implementation wire leases, writer admission and ownership acceptance gates.

## Findings and dispositions

2 findings; 2 fixed; 0 unresolved.

1. **Options payload drain gap (API:659–662,655,506).** Reviewer: “Callers receive no payload-specific drain handle,” while context return can precede queued/loser writes. Attached borrowed CustomPayload bytes to the same binding admission and explicitly extended Close + successful Wait to cover those references. Before-admission errors retain none; batches require all member waits; page-derived inputs require the source lease through that wait. Only entry descriptors are snapshotted. Rejected an implicit admission payload copy because the strict zero-copy contract requires explicit owned conversion; callers may explicitly Clone Bytes.
2. **Collect detachment ambiguity (API:340,348).** A slice of Text/Bytes/Value could otherwise retain page dependencies. Defined Collect as an explicit recursive ownership conversion using the iterator decoder's Clone, with no partial slice returned on error.

## Additional behaviors checked

Accepted-writer/loser/pager drains; timed-out waits; copying during owned encoding;
standalone external lease obligations; dynamic row borrowing; independent
segment/page references; retained pages after session close; schema-independent
recursive Clone; lazy clone without eager semantic decoding; expired-owner misuse
versus malformed-input validation.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `ced64106f4999647fcb9c600b3993214f0dfbdecac55b378ba2d80b99e2cd519` | `6a0b01dc6e7c21dc46e5f1283a42f7c7e862c5b868c95c3ef5a2a2305ccf1d50` |
| `feature-specs/gocql-v2-api.md` | `39bc6024e6d23e7990504f9ffb9cd1642f1546efa308e2feebf306e19be16b7e` | `06e25f22ba5580606cdfa92a7f84ac7b3c5a4bbf5ace6c7484f4d22ebd96d195` |
| `feature-specs/gocql-v2-implementation.md` | `42520cff8afad6d55afe603ab9d416ebd77c97d99134909eecd4b40c39c41eaa` | `42520cff8afad6d55afe603ab9d416ebd77c97d99134909eecd4b40c39c41eaa` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
