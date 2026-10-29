# Driver feature specifications

These specifications record the behavior of the ScyllaDB Rust driver, then define
and verify a Go API informed by that behavior. Language-independent contracts and
Rust implementation evidence are separate sections in each reference spec.

The target is **gocql v2**. The user explicitly requires no backward compatibility
with the existing Go API. Design a clean Go API with generic serialization and
explicit zero-copy ownership. Migrate existing integration tests to the v2 API
while preserving their behavioral assertions; legacy signatures, reflection-led
APIs, and compatibility adapters are not deliverables. Reusing correct transport
and routing internals is permissible when they satisfy the v2 contracts.

## Source revisions

- Rust driver: `scylladb/scylla-rust-driver` at
  [`f8b0ee928dc0dcb475247dc82cce4e42804b0621`](https://github.com/scylladb/scylla-rust-driver/tree/f8b0ee928dc0dcb475247dc82cce4e42804b0621).
  Local reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Go baseline: `scylladb/gocql` at
  [`1ceaac8172bda26e8b8d701ac356982d33729b27`](https://github.com/scylladb/gocql/tree/1ceaac8172bda26e8b8d701ac356982d33729b27).
- Format inspiration: [`dkropachev/codex` feature specs](https://github.com/dkropachev/codex/tree/1c8c12e6c4da3f0af204343cb89c129600beee08/codex-rs/feature-specs).

## Reference feature sections

1. [Routing, replicas, tablets, and LWT](routing.md)
2. [Connections, picking, and pool lifecycle](connections.md)
3. [Metadata, control connection, and events](metadata-control-events.md)
4. [Statements, execution, paging, retry, and speculation](execution.md)
5. [Protocol, types, serialization, and borrowing](serialization-protocol.md)
6. [Configuration, security, and observability](configuration-security-observability.md)

Each section includes summary, language-independent behavior, named subfeatures,
invariants, pinned Rust implementation links, test places, and edge cases. Source
behavior must be distinguished from proposed Go behavior. A feature catalog maps
all production Rust modules to these sections to check breadth.

- [Source catalog](source-catalog.md), with machine-readable assignments in
  [rust-source-catalog.json](rust-source-catalog.json).
- [Pinned integration test inventory](integration-baseline.json): 174 top-level
  tests across 29 integration/CCM-tagged Go files, including helper-only files.
- [Progress and completion gates](WORK-LOG.md).

## Go v2 feature proposals

- [Streaming receive decoding into caller storage](gocql-v2-streaming-receive.md):
  draft incremental socket-to-destination decoding without complete response/page
  buffering; includes transform limits, ownership, backpressure and acceptance
  criteria. Public signature reconciliation and implementation remain pending.

## Review contract

Every reference section receives ten sequential review-and-fix rounds, each by a
fresh agent. Each agent reads the actual pinned source, checks the entire assigned
section (including its language-independent and implementation-evidence parts),
fixes verified omissions or errors, and writes a report under `reviews/` recording
the source inspected, findings, fixes, and remaining concerns. A report claiming
no findings must identify the behaviors checked. Drafting is not a review round.
The review ledger records agent identity and report paths; completion is measured
from the actual reports and files, not planned rounds.

## Test places

- Rust unit tests: source-level algorithm, type, protocol, and state-machine tests.
- Rust integration tests: database/proxy tests of externally visible behavior.
- Go unit/race tests: API contracts, codec boundaries, concurrency, and ownership.
- Go integration tests: the existing gocql suite migrated to the new API.
- Go allocation benchmarks: encoding, decoding, request construction, paging,
  routing, and connection selection hot paths with explicit ownership boundaries.

Source test targets are evidence of intended coverage, not proof of a local test
run. Missing coverage stays explicit. Database runs must record environment,
commands, exit status, and skips. No mocked database result can establish that the
existing integration suite works.
