# Serialization/protocol review — round 06

- Section: `feature-specs/serialization-protocol.md`
- Round: 06 — omitted public/optional features and less-common paths, with a whole-section audit.
- Agent identity: `/root/review_serialization_06`
- Reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`, verified HEAD
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `efc9e66b1262ef44b19e4be2404d62fefee2aa8b4688e28d526728f3a32bdb15`
- After SHA-256: `c3e8f276586497f32a15b662aebd21623e9ba332094b40a5b22e6d3488df692e`
- Scope: only the assigned specification and this report were edited. No Go or
  Rust implementation changes, child agents, test executions, or database runs.

Read the complete section, README, review instructions, and source catalog.
The evidence below comes from the pinned implementation and test bodies;
earlier reports were not substituted for source inspection.

## Source inspected

Paths below are relative to the pinned reference root. Line numbers identify
the relevant implementation, not just an API declaration.

- `scylla-cql-core/src/value.rs`: `MaybeUnset` (34–51), `Emptiable` (63–110),
  timeuuid constructors/comparison/hash (134–331), varint constructors,
  normalization, and conversions (369–571), decimal equality and conversions
  (593–711), date/time/timestamp conversion/arithmetic, `CqlValue` helpers and
  formatting (1058–1494), and `Row` (1498).
- `scylla-cql-core/src/serialize/value.rs`: scalar/optional/container codecs,
  dynamic dispatch (613–721), UDT field mapping, tuple encoding, sequence and
  vector encoding, and gated vector-element helpers (983–1027).
  `serialize/row.rs`: positional/named rows, context, `SerializedValues`
  construction/rollback/frame import/iteration (502–687).
  `serialize/batch.rs`: provider and advancing iterator contracts (14–211),
  heterogeneous tuple implementations (219–320).
  `serialize/writers.rs`: framed versus no-size mode (75–220).
- `scylla-cql-core/src/deserialize/value.rs`: scalar/null/empty/dynamic codecs,
  external time/number adapters, secrecy adapters (804–882), collection/map
  iteration and collection, vector `into_vec`/`nth` (1299–1441), raw cell plus
  metadata (1674–1706), tuple/UDT suffix behavior, container ownership
  (1948–2035), and raw byte iterators (2100–2175).
  `deserialize/row.rs`: raw columns, remaining-column check, raw and dynamic
  destination implementations (17–225). `deserialize/result.rs` and
  `scylla-cql/src/deserialize/result.rs`: raw/typed/lending scan and error cursors.
  `deserialize/frame_slice.rs`: ownership, cursor mutation, split/chunk/read
  semantics (64–235); serialization/deserialization `mod.rs` error wrappers.
- `scylla-cql/src/frame/mod.rs`: complete framing/compression/extension path;
  request `mod.rs`, `query.rs`, `execute.rs`, `batch.rs`, `options.rs`,
  `startup.rs`, `prepare.rs`, `auth_response.rs`, and `register.rs`;
  response `mod.rs`, `result.rs`, `event.rs`, `authenticate.rs`, `supported.rs`,
  `custom_type_parser.rs`, and `server_event_type.rs`.
  Key less-common paths: `ExecuteV2` featureless/feature-aware decoders
  (123–174), response construction helpers in `result.rs` (1069–1298), and
  `SchemaChangeEvent::serialize` (`event.rs`:207–267).
- `scylla-cql-core/src/frame/protocol_features.rs`, `frame/types.rs`,
  `frame/frame_errors.rs`, and `frame/response/error.rs`: extension precedence,
  primitive/null/unset boundaries, and structured error parsing.
  `frame/response/result.rs`: complete native/aggregate enum and vector width
  table, special-empty capability, metadata ownership conversion (257–294),
  column/table specifications, and prepared metadata.
  Core request/response kind enums and core `frame/request/{query,batch}.rs`;
  protocol `frame/types.rs` primitive writers and prefix handling.
- `scylla-cql/src/serialize/raw_batch.rs`, compatibility `value.rs`, all three
  crate `lib.rs` export surfaces, serialization/deserialization module exports,
  `_macro_internal.rs` flattening traits and generated-code helpers;
  `pretty.rs`, parser utilities, and safe-format utilities.
- All four `scylla-macros/src/{serialize,deserialize}/{row,value}.rs` attribute
  structures and generators; `deserialize/mod.rs`, `parser.rs`, and `lib.rs`.
  Checked name/order matching, flattening, missing/null/default behavior,
  strictness, crate-path routing, and attribute validation.
- The three Cargo manifests; `scylla/src/lib.rs`:159–160;
  `response/query_result.rs`:130–139,393–424;
  `client/pager.rs`:715–824,1338–1367;
  `network/connection.rs`:138–178,995–1029,1105–1223,1459–1518;
  `statement/prepared.rs`:569–608; `statement/bound.rs`:25–32.
  These establish feature gates, request ownership, metadata snapshots, and
  lending/stream lifetime and cancellation boundaries.

## Verified findings and fixes

1. **Native wrapper identity was underspecified.** The spec described the wire
   representation but omitted observable equality/hash rules. `value.rs` uses
   timestamp-byte ordering with masked version bits and signed-byte tail order
   for timeuuid, and the same representation for equality/hash (219–331).
   Varint normalization removes leading zero bytes, preserves a necessary zero
   sign byte, treats empty/all-zero as zero, and does not normalize redundant
   negative sign extension (439–526). Decimal derives equality over the varint
   and exact scale (593–614). Added those boundaries while preserving the fact
   that serialization sends the original bytes.

2. **Dynamic source variants did not imply strict variant identity.**
   `serialize/value.rs`:627–721 delegates ASCII/text to string codecs and
   list/set/vector to `Vec<CqlValue>`, whose metadata-directed paths are at
   535–572. Added the permitted cross-variant encoding and dimension check.
   Also documented consuming/accessor helper compatibility, top-level nullable
   dynamic rows, and `MaybeUnset::from_option`/default behavior from `value.rs`.

3. **Deferred decoding and ownership helpers were only named or omitted.**
   `FrameSliceWithMetadata` accepts any type and preserves the optional cell
   without interpreting it (`deserialize/value.rs`:1674–1706).
   `ColumnIterator` is itself a row destination and checks remaining columns
   when asked (`deserialize/row.rs`:60–65,140–151). Added these contracts,
   exact Box/Arc/Cow behavior, FrameSlice split/chunk/cursor boundaries, and
   metadata ownership promotion costs. Corrected the claim that compatibility
   `value.rs` only establishes exports: its public `deser_cql_value` decodes a
   present cell without advancing its mutable slice argument (17–30).

4. **Batch/request and response-tooling APIs lacked a complete boundary.**
   Added the three batch kind bytes, supported batch provider forms, consuming
   iterator operations, raw adapter/context role, `SerializedValues` closure
   construction/frame import, and raw iterator's malformed-custom-output panic.
   Evidence: core `frame/request/batch.rs`:5–49,
   `serialize/batch.rs`:14–320, `serialize/row.rs`:537–687, and protocol
   `serialize/raw_batch.rs`:17–164. Added `SerializableRequest`/auxiliary
   `DeserializableRequest` and the deprecated Execute decoder distinction:
   featureless `ExecuteV2` assumes metadata ID and leaves tablet bytes unread
   (`request/execute.rs`:123–174). Clarified which low-level APIs require the
   CQL crates. Documented response tooling serializers and their unsupported
   vector metadata branch (`response/result.rs`:1069–1298).

5. **Optional external adapters needed behavior, not only feature names.**
   Added concrete chrono/time/BigInt/BigDecimal mappings and native-wrapper
   conversions. Secrecy 0.8 `Secret<T>` and 0.10 `SecretBox<T>` expose their
   inner value to the normal encoder, then wrap decoded values; 0.10 has
   additional String/Slice decoding paths. Evidence:
   `serialize/value.rs`:142–242, `deserialize/value.rs`:804–882,
   `value.rs`:533–711, and all three Cargo feature tables. The spec now makes
   the ordinary output-buffer and owned String/Vec representation explicit.

6. **Two unstable inter-driver result APIs were missing.** Added the
   `scylla_unstable` + `unstable-python-rs` optional borrowed metadata/rows
   getter on `QueryResult` (`query_result.rs`:130–139), and
   `scylla_unstable` + `unstable-cpp-rs` consuming `QueryRowsResult::into_inner`
   with its metadata/rows, trace, warnings, and coordinator tuple (393–417).
   Also recorded the C++ type re-export (`lib.rs`:159–160). Verified existing
   Node.js, Python vector-helper, and C# lending APIs and the fact that
   `full-serialization` does not enable these gates.

7. **Derive configuration omitted an actual public option and shared syntax.**
   Every derive accepts `#[scylla(crate = path)]` and routes through that
   path's `_macro_internal`; see the four attribute structures and
   `deserialize/mod.rs`:15–21. Added its purpose and the complete supported
   attribute distribution. Both serialize derives accept but ignore
   `default_when_null`, permitting shared deserialize annotations; flattening
   remains exclusive to row serialization. Existing semantic restrictions were
   retained after tracing generated name/order and UDT code.

8. **The completion/overflow statement excluded no public mode.**
   `CellWriter::new_without_size` and child no-size writers are public.
   `writers.rs`:95–131,163–179,200–219 shows that the builder in this mode has
   no `-3` placeholder and no accumulated-length check in `finish`, while
   `set_value` still checks its slice length and null/unset still write
   four-byte sentinels. Narrowed the original blanket statement, documented
   these paths, and removed the misleading word “bounded” from the generic
   custom-codec writer description.

9. **Vector nth-element skipping was absent.**
   `deserialize/value.rs`:1401–1440 implements fixed-width byte skipping
   without decoding skipped values, immediate exhaustion when `n >= remaining`,
   and variable-width iteration that discards skipped errors. Added this path
   and its validation boundary; retained the separately described bulk and
   ordinary iteration behavior.

## Whole-section checks without additional corrections

- v4 direction/version/header checks, opcode inventory, static VOID, body
  allocation, extension order, LZ4 short prefix and Snappy behavior.
- Optional extension parse failures, first-match duplicate behavior, zero LWT
  mask, tablet-v2 priority, and independent metadata-ID layout.
- Handshake/authentication/event payloads; schema Invalid versus unknown-target
  handling; ordinary/serial consistency and query defaults; paging token
  presence; named row reordering and deterministic leftover-name errors.
- Scalar widths, strict type compatibility, ASCII encode/decode asymmetry,
  temporal limits, null/unset/empty distinctions, tuple shortened suffixes,
  frozen metadata, collection counts, vector size classification, and nested
  error/cursor behavior.
- UDT name versus ordered evolution, missing-definition versus missing-value
  defaults, ignored-field framing, flattening trait restrictions, and invalid
  macro combinations.
- NO_METADATA cache/fallback selection, new-metadata precedence, count and
  partition-key-index limits, response-owned and cached metadata, shared blob
  retention, raw/lending iteration, stream metadata checks, cancellation-safe
  pending-page wait, and synchronous encoding before queue submission.
- Error stage separation, recognized malformed error fields, standard-code
  priority over rate-limit extension, unknown error/operation preservation,
  diagnostic formatting, and absence of universal no-copy/hostile-frame bounds.

## Test evidence and validation

No tests were executed. The earlier round-03 execution remains explicitly
attributed historical evidence, not a new run or optional-feature result.

- Mechanically verified all 79 named Rust test references in the final spec
  resolve to functions in their stated files. Checked every pinned source link
  for file existence and line bounds. These checks do not establish runtime
  coverage or correctness of every anchor's semantic scope.
- Read the newly cited varint equality/hash and unchanged-byte fixtures,
  timeuuid version-bit equality/hash case, valid float/text `nth` cases,
  Box/Arc/Cow contents, and gated secrecy encode/decode fixtures. Added their
  exact scope and explicitly omitted assertions: negative sign normalization,
  pointer identity, allocation counts, Cow variant, malformed skip behavior,
  and transport-buffer zeroization.
- Rechecked the existing blob and metadata-lifetime tests, collection-capacity
  assertions, misleading `test_null_errors`, custom-type internal/trailing
  whitespace test, and serde feature-forwarding body. They support the existing
  narrowed coverage claims, not broader null, zero-copy, or feature guarantees.
- Inspected integration vector fixtures, metadata-extension early returns and
  proxy simulation, and compression request feedback/fixture construction.
  They remain test locations rather than successful real-database executions.
- Remaining gaps stay explicit: no-size overflow/custom malformed rows,
  deferred-cell APIs, native decimal scale equality, inter-driver raw access,
  concurrent metadata replacement/retained ownership/cancellation matrices,
  hostile-frame budgets, and the listed future Go tests/benchmarks.

No cross-section source contradiction requiring another file edit was found.
The newly cataloged inter-driver result APIs were communicated to the parent
for coordination with the execution section. Only this section's spec and
round-06 report are deliverables from this agent.
