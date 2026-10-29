# Serialization/protocol review, round 08

- Section: `feature-specs/serialization-protocol.md`.
- Round: 08, whole-section review with additional caller/callee and cross-section focus.
- Agent identity: `/root/review_serialization_08`.
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`.
- Source commit verified by `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `79942f9062764b83f25bffb05646dcd610590a40a0a8d72263a2424ff7ef7e46`.
- After SHA-256: `292c340f72052f6d02d433dc4e0014668dbc51eb6cd76541f937cce26e7c43da`.

## Findings and fixes

### 1. Batch reuse must name statement index zero and iterator reconstruction

The previous phrase “first prepared statement's already serialized row” could
mean the first prepared member anywhere in the batch. The session selects only
`batch.statements.first()` and passes a prepared handle only when that exact
member is already prepared (`scylla/src/client/session.rs:1328-1333`).
`peek_first_token` serializes and caches that member's row only when one is
available, and `BatchValuesFirstSerializedIterator::serialize_next` appends those
bytes while skipping the original first row
(`scylla/src/statement/batch.rs:308-342,384-401`). Connection-local automatic
preparation happens later (`scylla/src/network/connection.rs:1252-1270,1323-1374`)
and does not retroactively create this cache.

The previous statement also described cloning as a property of all batch-value
providers. `BatchValues::batch_values_iter` is an iterator factory, with slice
and tuple providers constructing fresh iterators. The iterator-backed adapter
clones its stored iterator; the metadata adapter separately clones its context
iterator (`scylla-cql-core/src/serialize/batch.rs:13-25,123-206`;
`scylla-cql/src/serialize/raw_batch.rs:120-139`). The wire batch serializer gets
another iterator each time it is invoked and consumes any extra rows to report
the actual mismatch count (`scylla-cql/src/frame/request/batch.rs:86-136`).

Fixed the reuse condition and iterator wording, linked the execution section's
matching first-member routing boundary, and added pinned evidence for the
session/helper pair. This changes no execution or routing behavior specification.

### 2. A parsed custom payload is not an ordinary public query-result field

The wire section described the decoded payload map but omitted where its
visibility ends. `Connection::parse_response` retains it on internal
`QueryResponse` (`scylla/src/network/connection.rs:1487-1524`). EXECUTE consumes
tablet feedback before conversion, including its reprepare resend
(`connection.rs:1167-1174,1210-1216,2045-2067`).
`QueryResponse::into_non_error_query_response` copies response, tracing ID, and
warnings into a type with no payload field
(`scylla/src/response/request_response.rs:17-43`). Ordinary result construction
therefore cannot expose the custom payload.

Added this boundary and linked the already-correct configuration/observability
description for the different warning and tracing-ID retention paths. Added
pinned evidence for response conversion. No change to another section was needed.

### 3. Duplicate SUPPORTED keys differ from duplicate fields inside their lists

The parent relayed a connections-review observation, which this reviewer verified
directly: `Supported::deserialize` calls `read_string_multimap`, whose
`HashMap::insert` replaces an earlier value list when the same top-level key
appears again (`scylla-cql/src/frame/response/supported.rs:18-19`;
`scylla-cql/src/frame/types.rs:347-358`). Only after this step does
`ProtocolFeatures::get_cql_extension_field` choose the first exact `KEY=` string
inside the retained list (`scylla-cql-core/src/frame/protocol_features.rs:109-112`).

Clarified the last-top-level-entry/first-matching-list-field distinction beside
the existing malformed numeric-field rule. Added the duplicate top-level key
case to the explicit test gaps. This complements the connections section's
negotiation description without changing another agent's file.

## Source and boundaries inspected

Read the review instructions, README, complete assigned section, and relevant
execution, connections, metadata/control/events, routing, and
configuration/security/observability claims. Independently inspected these
implementation areas:

- `scylla-cql/src/frame/mod.rs:55-359`: request construction, header validation,
  owned body read, static VOID buffer, extension ordering, LZ4/Snappy transforms.
- `scylla-cql-core/src/frame/protocol_features.rs:38-145`: recognized feature
  parsing, first matching numeric field, presence-based support, tablet-v2
  priority, metadata IDs, and LWT mask matching.
- `scylla-cql/src/frame/request/{mod,query,execute,batch}.rs`: opcode inventory,
  exact size hints, auxiliary decoder deprecation, parameter flags/defaults,
  feature-aware EXECUTE fields, batch count/error framing.
- `scylla-cql/src/frame/response/{result,event,custom_type_parser}.rs`:
  `RawMetadataAndRawRows::{deserialize,deserialize_metadata}`,
  `deser_prepared_metadata`, `deser_prepared`, `EventV2::deserialize`, schema and
  route events, custom grammar, frozen propagation, vector dimensions.
- `scylla-cql/src/frame/response/supported.rs:18-19` and
  `scylla-cql/src/frame/types.rs:347-358`: SUPPORTED multimap decoding and
  duplicate top-level key replacement before extension interpretation.
- `scylla-cql-core/src/frame/{types,response/result,response/error}.rs`:
  consistency and length primitives, null/unset split, type inventory/vector
  widths, special-empty capability, database error families and rate-limit
  matching order.
- `scylla-cql-core/src/serialize/{row,value,batch,writers,mod}.rs`:
  named and positional rows, count checking, `SerializedValues`, rollback,
  scalar/aggregate/dynamic encoders, row-provider factories, cell completion
  proofs and no-size writers, shared/downcastable errors.
- `scylla-cql-core/src/deserialize/{value,frame_slice,result,mod}.rs` and
  `scylla-cql/src/deserialize/result.rs`: scalar null/length/range behavior,
  optional and special-empty values, aggregate iteration and error consumption,
  vector bulk/skip paths, tuple/UDT omissions, frame storage promotion, and raw,
  typed, and lending row traversal.
- `scylla-macros/src/{serialize,deserialize}/{row,value}.rs`: matching modes,
  flatten's internal trait requirement, rename/skip validation, allowed-missing
  and null/default distinction, extra UDT fields and ordered suffix behavior.
- `scylla-cql-core/src/value.rs`, `pretty.rs`, `utils/safe_format.rs`,
  `scylla-cql/src/{value,utils/parse}.rs`, and `scylla-cql-core/Cargo.toml`:
  native wrapper identity, timestamp arithmetic, formatter boundaries,
  compatibility decoder cursor, parser positions, and optional adapter features.
- `scylla/src/network/connection.rs:138-180,723-820,943-1230,1252-1374,1459-1525,2045-2067,2136-2290`:
  owned request enqueue, preparation/reprepare, metadata-ID handling, per-send
  snapshots, compression negotiation, and custom-payload/tablet processing.
- `scylla/src/client/session.rs:1300-1360`,
  `scylla/src/statement/{batch,bound,prepared}.rs`: first-member batch routing and
  serialized-row reuse, bound ownership, partition-key extraction, immutable
  bind contexts, result cache snapshots and unconditional replacement.
- `scylla/src/client/pager.rs:714-824,1170-1380`,
  `scylla/src/response/{request_response,query_result}.rs`: lending-page waits,
  type checks after page changes, stable owned row bounds, result conversion,
  and unstable raw-result accessor gates.
- `scylla/src/cluster/metadata/worker.rs:911-966`: event decoding's handoff to
  topology/schema/client-route scheduling, rather than immediate topology
  mutation in the wire codec.

## Explicit checked behaviors without additional findings

- Native-v4 header/version limits, extension/decompression order, incoming size
  guard absence, and unchecked outgoing whole-frame length remain accurately
  separated from proposed Go validation requirements.
- Numeric extension parsing, LWT mask-zero behavior, tablet-v2 byte presence,
  and metadata-ID independence agree between codecs and connection callers.
- Query/batch flags, ordinary versus serial consistency, length/count limits,
  empty paging tokens, partial writer errors, and incremental rollback agree
  with the lower-level encoders. Effective statement defaults remain delegated
  to execution.
- Scalar inventory and strict type matching, ASCII encoding asymmetry, time
  range checking, null/unset/empty differences, dynamic variant delegation,
  aggregate evolution, and vector fixed/variable classification remain accurate.
- Metadata cache selection never rescues malformed newly supplied metadata.
  Per-send shared snapshots agree with connection parsing and reprepare, and
  current-result replacement does not imply refreshed bind metadata.
- Borrowed, shared-owner, and owned values retain different lifetimes/copy
  costs. Row and collection iteration retain the documented count/error quirks;
  paging repeats destination checking for newly reached nonempty pages.
- Event payload vocabularies and invalid schema-change handling agree with the
  metadata consumer; connection negotiation and payload consumption agree with
  the configuration and routing descriptions.
- Derive policies, native wrapper identity/formatting, optional feature gates,
  auxiliary decoder limitations, and error wrapper ownership were checked
  without additional verified corrections.

## Test evidence and remaining gaps

No Rust, Go, database, or benchmark tests were executed in this round. The prior
round-03 execution statement remains attributed to that round.

A read-only source-reference check resolved all 79 named test/function targets
in 30 inline source references in the section. Existence does not establish test
coverage. Assertion bodies inspected included:

- Frame LZ4 round-trip/short-input and extension-negotiation tests; EXECUTE
  feature-aware construction/round-trip tests.
- `test_deserialize_bytes` (contents, not pointer identity),
  `metadata_does_not_bound_deserialized_values` (compile-time lifetime scope,
  errors explicitly permitted), and `test_null_errors` (non-null map fixture,
  not a null-scalar matrix).
- `test_vector_bulk_decoding_matches_element_by_element` (every truncation of
  one four-int payload), `test_collections_are_allocated_once` (length/capacity
  assertions for 100-element fixtures, not an allocation counter), and vector
  iterator skip fixtures.
- Integration vector fixture inventory and empty-collection typed assertions;
  metadata-ID tests' capability-dependent early returns; compression fixture
  sizes, outgoing wire-size measurements, and tolerated proxy shutdown error.

Added explicit missing-coverage entries for first-statement-only batch row reuse
across retries, custom-payload removal at ordinary result conversion, and
duplicate top-level SUPPORTED keys. Existing
gaps for hostile sizes, optional-feature builds, lifetime/cancellation races,
metadata-feature cross-products, malformed aggregate paths, and Go ownership
remain. No other-file issue requiring parent intervention was confirmed.

Only the assigned specification and this report were edited.
