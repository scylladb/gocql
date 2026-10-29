# Go v2 design review round 01: API and signatures

Reviewer: `/root/design_reconciliation/round01_api`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

`plan.md` sections 2–5 and 11; `gocql-v2-api.md` type/codec, borrowed views,
dynamic values, statements and page-state sections; implementation Config shapes.
Anchors below identify the reviewed pre-fix text; later rounds can move lines.

## Findings and dispositions

5 findings; 5 fixed; 0 unresolved.

1. **Malformed text panic (API:300,341,409; plan:42).** Reviewer: “BorrowText(string) Text … explicitly require[s] a panic for invalid UTF-8”; this contradicted malformed-input errors. Changed BorrowText and TextValue to return `(value, error)` and made UTF-8 validation explicit.
2. **Missing raw host hook (plan:74; implementation:313).** Config lacked DialHost, and HostSnapshot could not convey source-port/shard dialing. Added DialTarget with selected network/address, source endpoint, host snapshot and shard hint. Added Config.DialHost, a matching Dialer interface, precedence, raw/TLS/auth responsibilities, and explicit rejection when hooks cannot honor selection.
3. **Unreleasable dynamic lease (API:374,558).** Reviewer: “Decoding retains the page lease,” but neither Value nor DynamicRow has Close. Replaced this with row-scoped borrowing without an independent reference; only explicit PageLease/Clone extends lifetime.
4. **Ambiguous page-state import (API:547,573).** Bytes did not distinguish three states. Chose continuation-only restoration: Restore(nil/empty) is More, zero state is start, Bytes copies only the token, start/complete are distinguished by predicates, and complete input returns ErrPagingComplete before I/O.
5. **Final versus temporary root layout (API:5; plan:46).** Marked root scaffolding as the current checkpoint and made plan section 5's extraction/dependency boundaries authoritative.

## Additional behaviors checked

Package-level generics; typed Param/Column accessors; schema-independent Clone;
unprepared declared metadata; prepared/bound signature agreement; explicit removal
of legacy public execution interfaces. No implementation-status claim was made.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `49edc5a332710c047207415fbcf9b0d95ff20ca1605ee98b7b7a68dc4faf72f1` | `ced64106f4999647fcb9c600b3993214f0dfbdecac55b378ba2d80b99e2cd519` |
| `feature-specs/gocql-v2-api.md` | `10479c85c36e2474e1b9725198ddb348c87cb6bbb51598d273d6ec485aebad50` | `39bc6024e6d23e7990504f9ffb9cd1642f1546efa308e2feebf306e19be16b7e` |
| `feature-specs/gocql-v2-implementation.md` | `8dd5723bdc8ae7e410e7066242b2a2f6b2bf86f69d9b4928750c2ce89fa3e951` | `42520cff8afad6d55afe603ab9d416ebd77c97d99134909eecd4b40c39c41eaa` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
