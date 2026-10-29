# Serialization/protocol reference review — round 01

- Section: `feature-specs/serialization-protocol.md`
- Round: 01 — independent source trace and factual corrections
- Agent identity: `/root/review_serialization_01`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `72536514ef3c33636ceb0ee9e24041dd412004bff5182e6469a97582c2ae53af`
- After SHA-256: `d0ff04c6ade77fd48e62b9712004ff85c9c9731d88c7bf0a3051a62342ac3d7b`
- Changes: assigned specification and this report only; no implementation changes and no child agents.

## Verified findings and fixes

1. **Native time decoding does not preserve arbitrary signed wire values.**
   `scylla-cql-core/src/deserialize/value.rs:653–680`,
   `get_nanos_from_time_column` and the `CqlTime` implementation, reject values
   outside `0..=86399999999999`. The dynamic `CqlValue::Time` branch delegates to
   that decoder (`:153`). Conversely, `serialize/value.rs:169–173` writes the
   native wrapper's signed integer directly. Corrected the type table to state
   the exact decoding range and serialization asymmetry. Added the existing
   `test_deserialize_time` coverage location (`deserialize/value_tests.rs:2886`).

2. **Nested vectors can be fixed-width vector elements.**
   `scylla-cql-core/src/frame/response/result.rs:298–307`,
   `ColumnType::type_size_for_vector`, recursively multiplies a nested vector's
   fixed element width by its dimension. The draft classified every composite
   as variable-width. Corrected the classification; collections, tuples, UDTs,
   and vectors whose elements have no fixed vector width remain variable-width.
   Checked the consumers in `serialize/value.rs:1055–1100` and
   `deserialize/value.rs:1242–1390`, and the nested-vector integration fixtures
   in `scylla/tests/integration/types/cql_collections.rs:646–650`.

3. **Generated UDT serialization supports `allow_missing`.**
   `scylla-macros/src/serialize/value.rs:77–81` maps the attribute to
   `ignore_missing`; the name-matching branch checks required fields at
   `:411–429`, and ordered matching skips an allowed-missing source field
   without consuming an unmatched metadata field at `:478–529`. Corrected the
   unconditional claim that source fields absent from metadata fail. Checked
   the existing combined derive tests at `scylla-cql-core/src/macros_tests.rs:70`
   and `:141`, including missing fields and ordering variants, and added those
   test locations.

4. **The ownership description conflated containers and borrowed iterators.**
   `scylla-cql-core/src/deserialize/result.rs:13–17` stores a borrowed
   `FrameSlice`; `deserialize/row.rs:30–35` does likewise for columns.
   `scylla-cql/src/frame/response/result.rs:373–378` owns `Bytes`, while
   `scylla-cql/src/deserialize/result.rs:26–31` owns metadata and row bytes in
   `RawRowLendingIterator`. Clarified which objects retain ownership and which
   borrow an owner, preserving the distinct frame/metadata lifetime contract.

5. **Whole-frame outgoing size checking was omitted from the limit discussion.**
   `scylla-cql/src/frame/mod.rs:100–101` uses an unchecked `as u32` conversion
   for the complete outgoing body length. The per-cell checked `i32` conversion
   in `scylla-cql-core/src/serialize/writers.rs:130–137` does not establish a
   checked aggregate frame bound. Added the observed limitation without turning
   it into a proposed Go acceptance rule. No multi-gigabyte allocation was
   attempted. The preexisting incoming allocation-limit limitation remains.

6. **Public timestamp helper behavior was missing.**
   `scylla-cql-core/src/value.rs:773–887` exposes `MIN`, `MAX`, nonmonotonic
   wall-clock `now`, checked elapsed duration, and duration arithmetic. Added
   whole-millisecond truncation, result-overflow panic behavior, the checked
   difference's absent result for reversed ordering, and support for the full
   unsigned millisecond difference. Added source and existing boundary-test
   references (`value.rs:1676–1745`). These helpers do not change wire encoding.

7. **Batch reuse and unstable codec access needed precise boundaries.**
   The parent flagged these cross-section interfaces; both were traced here.
   `scylla/src/network/connection.rs:144` constructs a serialized request for
   each send; `scylla-cql/src/frame/request/batch.rs:84–115` calls the batch row
   provider. `scylla/src/statement/batch.rs:307–395` caches only the first row
   when it was serialized for the first prepared statement's routing token.
   Added that boundary to replayable batch values. Also recorded separately
   gated raw result access at `scylla/src/response/query_result.rs:431–437` and
   vector-element serialization helpers at
   `scylla-cql-core/src/serialize/value.rs:983–1053`, with their two required
   feature/cfg gates. Checked feature forwarding in all three Cargo manifests;
   these gates are separate from `full-serialization`.

8. **Some test descriptions exceeded their precise assertions.**
   `deserialize/value_tests.rs:1108–1140` checks every truncation of one
   four-element `vector<int>` fixture, not every vector element type.
   `:1143–1198` checks final capacity for typed list/vector and dynamic
   vector/map fixtures; it does not count allocations. Narrowed that entry.
   `serialize/value_tests.rs:185–198` tests propagated errors through `Some`
   and `MaybeUnset::Set`, not invalid null/unset states; corrected its label.

## Source inspected and behaviors checked

All paths below are relative to the pinned Rust checkout. The complete assigned
specification and `feature-specs/README.md` were read before the source audit.

- **Framing and negotiation:** `scylla-cql/src/frame/mod.rs:60–351`;
  `frame/request/mod.rs:36–120`; `frame/response/mod.rs:23–168`;
  `scylla-cql-core/src/frame/protocol_features.rs:35–151`. Checked v4 direction
  validation, opcode inventory, stream insertion, body completion and errors,
  static VOID optimization, decompression ownership, LZ4 short-prefix check,
  trace/warnings/custom-payload order, permissive numeric extension parsing,
  LWT mask semantics, tablet-v2 precedence, and independent metadata-ID gating.
- **Handshake/events:** `scylla-cql/src/frame/request/{options,startup,prepare,auth_response,register}.rs`,
  `frame/response/{supported,authenticate,event}.rs`, and
  `frame/server_event_type.rs`. Checked empty OPTIONS, CQL-version constant,
  authentication null tokens, event inventories, unknown-change behavior,
  schema target payloads, and matching connection/host counts with UUID parsing.
- **Query/batch wire format:** `scylla-cql/src/frame/request/query.rs:19–282`,
  `execute.rs:80–176`, `batch.rs:65–245`,
  `scylla-cql-core/src/frame/types.rs:21–251`,
  `frame/request/query.rs:28–106`, `serialize/batch.rs:13–207`, and
  `scylla-cql/src/serialize/raw_batch.rs:13–185`. Checked defaults and flag
  presence, empty versus absent paging tokens, serial-consistency restriction,
  named-value rejection, statement/value count checks, replayable providers,
  and extension byte ordering. Read the connection send boundary and first-row
  batch cache as noted above.
- **Encoding and value wrappers:** `scylla-cql-core/src/serialize/value.rs:93–1148`,
  `serialize/row.rs:24–230,515–709`, `serialize/writers.rs:13–258`, and relevant
  wrapper/conversion/formatting implementations in `value.rs:35–1465`.
  Checked scalar widths and exact metadata types, unvalidated ASCII encoding,
  null/unset bypass, special-empty capability, tuple suffix omission, dynamic
  UDT names and unknown fields, collection framing, vector formats, map-name
  diagnostics, row count limits, incremental rollback, and builder completion.
- **Decoding, laziness, and lifetimes:**
  `scylla-cql-core/src/deserialize/value.rs:48–2141`,
  `deserialize/frame_slice.rs:43–239`, `deserialize/result.rs:13–153`,
  `deserialize/row.rs:16–145`, `deserialize/README.md`, and
  `scylla-cql/src/deserialize/result.rs:19–110`. Checked strict scalar lengths,
  ASCII/UTF-8, null collections versus optional collections, dynamic empty
  differences, duration field conversions, tuple/UDT missing bytes, element
  checking, collection preallocation, vector truncation paths, separate metadata
  and frame lifetimes, borrowed/shared/copied blobs, failure-preserving
  `FrameSlice` reads, and row-count-bounded iteration.
- **Metadata and custom types:** `scylla-cql/src/frame/response/result.rs:517–1063`,
  `custom_type_parser.rs:29–350`,
  `scylla-cql-core/src/frame/response/result.rs:74–377`.
  Checked the complete column-type inventory, cached/no/sent metadata choices,
  incompatible metadata flags, prepared paging rejection, partition-key index
  sorting, native descriptor IDs, recursive types, custom frozen context, and
  whitespace/hex/parameter-count handling.
- **Derives:** attribute validation and generation paths in
  `scylla-macros/src/{serialize,deserialize}/{row,value}.rs`, especially
  serialization UDT branches at `serialize/value.rs:250–557`, deserialization
  UDT branches at `deserialize/value.rs:230–400,640–725`, and row flatten/name
  branches in `serialize/row.rs:136–169,254–389,438–455`.
  Checked skip/rename/order/null-default/missing-field distinctions and that
  flattening belongs to row serialization. The fixes above preserve these
  separate policies.
- **Errors, formatting, feature boundaries:**
  `scylla-cql-core/src/{serialize,deserialize}/mod.rs`,
  `frame/response/error.rs:34–218,242–445`, `pretty.rs`,
  `utils/safe_format.rs`, `scylla-cql/src/utils/parse.rs:13–146`,
  `scylla-cql/src/frame/types.rs:26–42`, and the core/CQL/driver Cargo features.
  Checked downcastable error wrappers, structured server error fields and
  unknown fallbacks, safe repeated iterator formatting, literal formatting,
  character error positions, serde versus CQL codec separation, and feature
  forwarding.

## Test coverage checks and remaining gaps

- Resolved every preexisting `path.rs:test_name` reference against the pinned
  checkout; all named functions exist. Inspected assertions or their called
  helpers for framing, extension bytes, prepared round-trip, named-row and
  count errors, scalar/aggregate errors, wrappers, lifetime compilation,
  collection capacity, row boundaries, derives, custom grammar, server errors,
  serde, and the integration families. The precise corrections are listed
  above; test names alone were not treated as execution evidence.
- Integration locations exercise actual database insert/selects, schema changes,
  proxy-observed extension IDs, and compressed wire sizes. The vector fixture
  includes nested vectors. `test_basic_metadata_update` returns early when the
  extension is unavailable and accepts a proxy driver-disconnection result;
  its presence is not evidence that extension coverage ran in any environment.
- `metadata_does_not_bound_deserialized_values` explicitly establishes a compile
  constraint, accepting errors from its empty input; it is not a runtime
  pointer-identity or allocation test. The spec retains its lifetime claim.
- No Rust unit tests, Rust integration tests, Go tests, or benchmarks were run.
  This was a source-and-document review. No server was configured for this round.
- Hostile-frame allocation limits, whole-frame overflow execution, deep nesting,
  all vector-type/truncation combinations, and universal zero-copy proofs remain
  unestablished. The Go implementation/test/benchmark entries remain explicitly
  missing. This review does not establish runtime acceptance of every malformed
  or server-invalid value.
- Cross-section batch/unstable API notes were communicated with the parent; no
  other section was edited.
