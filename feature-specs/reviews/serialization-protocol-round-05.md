# Serialization protocol review — round 05

- Section: `feature-specs/serialization-protocol.md`
- Round: 05 — whole-section audit, with additional focus on actual test assertions and guards.
- Agent identity: `/root/review_serialization_05`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, independently checked with `git rev-parse HEAD`: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `50882c6690ce1d2eff215e59dec53c468fd2b8f81a92ca72c827b43e7d4a43ae`
- After SHA-256: `efc9e66b1262ef44b19e4be2404d62fefee2aa8b4688e28d526728f3a32bdb15`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. Findings below come from the pinned source and actual
test bodies, not previous reports. Changed only the assigned specification and
this report. No Rust/Go implementation changes, child agents, builds, test runs,
or database runs were performed. The specification's round-03 execution record
is retained as an earlier record; it is not a round-05 execution claim.

## Findings and fixes

### 1. A null-rejection citation supplied no null

`scylla-cql-core/src/deserialize/value_tests.rs:2189–2200`, despite its name
`test_null_errors`, serializes a non-null map and asks the shared test helper to
deserialize it as `MaybeEmpty<i32>` against map metadata. The helper's type
check fails before value decoding; the test only calls `unwrap_err()` and never
supplies a null scalar. `test_native_errors` at 1480–1563 checks incompatible
types, fixed-length mismatches, and invalid text bytes, not direct null inputs.

Removed “nullability” and the misleading citation from that grouped coverage
claim. Explained what `test_null_errors` actually checks and explicitly marked
a direct scalar null-rejection matrix as unestablished by the listed targets.
The production behavior remains supported by the numeric decoder macro's
`ensure_not_null_slice` call (`deserialize/value.rs:328` onward), the native
scalar implementations, and the distinct `Option<T>` decoder at 251–267.

### 2. Test names concealed narrower fixture coverage

Read assertions rather than inferring coverage from the target names:

- `scylla-cql/src/frame/mod.rs:378–388` checks only payload sizes 0, 1, and 3
  for the short LZ4 prefix error; size 2 is not a fixture. The implementation's
  `try_get_u32` at 325–337 does reject every shorter-than-four-byte input. The
  spec now separates that source guarantee from the tested fixtures.
- `scylla-cql/src/frame/response/custom_type_parser.rs:659–670` is named
  `custom_cassandra_type_parser_accepts_middle_whitespace_for_complex`, but its
  assertion expects `UnexpectedTrailingCharacters` for trailing whitespace.
  It establishes that parsing reached the suffix after internal whitespace;
  it does not assert an `Ok` result. Added this distinction without weakening
  the independently checked grammar behavior (`parse`, `skip_blank`, and
  `get_type_parameters` at 55–160).
- `scylla/src/serde_tests.rs:5–30` checks exact JSON and round-trip equality for
  a struct containing `Counter` and `CqlDuration`, and is included only under
  `cfg(all(test, feature = "serde"))` in `scylla/src/lib.rs:299–300`. Replaced the
  broad wrapper description with those two concrete types and stated the gap
  for other wrappers/consistency types.
- The three cited EXECUTE tests (`request/execute.rs:212–272`) all use
  `result_metadata_id: None`; they do not establish the whole metadata-ID /
  tablet-v2 combination matrix. The rate-limit test (`core/frame/response/error.rs:
  761–797`) covers read/true and write/false with two custom codes, not standard
  code collisions or unknown operation bytes. Added these explicit coverage gaps.

### 3. Integration coverage needed feature and harness boundaries

- `scylla/tests/integration/types/cql_types.rs:380–445` conditionally calls
  chrono/time/bigint/BigDecimal adapter checks. The decimal column is created
  unconditionally, but its value checks run only with `bigdecimal-04`.
  `test_empty` at 1408–1454 checks only an empty `int`, inserted first through
  `blobasint(0x)` and then bound `CqlValue::Empty`. Documented these limits.
- `types/cql_collections.rs:568–902` contains 26 prepared vector round-trips,
  covering native values, collections, a tuple, and nested vectors. There is
  no UDT or counter-element fixture and no server-capability early return in
  that target. `test_deserialize_empty_collections` at 911–970 asserts empty
  destinations, while the list/set/map tests at 68–228 additionally assert
  optional destinations are `None` after empty inserts. Distinguished these
  assertions from direct wire inspection.
- `statements/result_metadata_extension.rs:430–559` simulates mixed support:
  a proxy removes `SCYLLA_USE_METADATA_ID` from one node's SUPPORTED message
  and injects bootstrapping errors to select prepare/execute destinations.
  Renamed the coverage description to simulated mixed extension support;
  this is not evidence of running different database versions. Preserved
  the early-return caveat at 291, 398, and 435.
- `session/compression.rs:43–72,92–201` checks six cases: no compression,
  Snappy, and LZ4 with 1,000/1,000,000 repeated `a` bytes. It asserts returned
  row contents, compression flags, and summed outgoing QUERY/EXECUTE wire-body
  size ranges. It does not measure response wire sizes or arbitrary data
  compression ratios. Added the concrete assertions and the accepted proxy
  `DriverDisconnected` return, also accepted by the metadata-ID targets.
  `utils.rs:160–207` awaits the test closure before calling `running_proxy.finish`,
  so this harness return does not bypass assertions in the closure.

### 4. The unstable public lending pager was omitted

`scylla/src/client/pager.rs:734–745` exposes
`QueryPager::next_column_iterator` under
`cfg(all(scylla_unstable, feature = "unstable-csharp-rs"))`.
`scylla/Cargo.toml:85` declares the feature. The wrapper forwards `next()`
at 714–731, returning a borrowing `ColumnIterator` plus a fresh-page Boolean;
it does not invoke a destination type check. The stable typed stream instead
checks initially (`pager.rs:1299–1313`) and after fresh pages (1338–1368).

Added the gated API, its caller's per-page type-check responsibility, the
forwarded cancellation behavior, a pinned source link, and the lack of cited
cancellation coverage for that API. Flagged this public pager surface to the
parent for cross-section consistency; no other section was edited.

## Whole-section implementation checks

The following files/symbols were inspected independently. Line locations refer
to the pinned Rust checkout. No additional discrepancy was established in these
behaviors:

- `scylla-cql/src/frame/mod.rs:53–345`: `SerializedRequest::make`,
  `read_response_frame`, `read_response_body`, extension parsing, compression.
  Confirmed v4/direction validation, nine-byte header, unchecked whole-body
  `u32` conversion, missing inbound allocation cap, VOID static buffer,
  decompression ownership, and trace/warnings/custom-payload order.
- `frame/request/mod.rs:36–78`, `frame/response/mod.rs:23–62`, request
  `options.rs`, `startup.rs`, `prepare.rs`, `auth_response.rs`,
  `frame/server_event_type.rs`, and `frame/response/event.rs:151–455`:
  opcode/event inventories, empty OPTIONS, CQL `4.0.0` constant, map/long-string
  and optional token framing, invalid schema-kind handling, topology/status
  values, client-route list lengths and host UUID parsing.
- `scylla-cql-core/src/frame/protocol_features.rs:65–141`:
  numeric parse fallback, first exact key match, presence-based support,
  tablet-v2 priority, metadata-ID independence, and all-bits LWT mask behavior.
- `scylla-cql/src/frame/request/query.rs:19–260`, `execute.rs:64–175`,
  `batch.rs:60–211`, `scylla-cql-core/src/frame/types.rs:21–254`:
  defaults, flags/order, distinct paging-state presence, named-value decoder
  rejection, serial consistency conversion, batch mismatch checks, and
  response negative lengths versus request null/unset parsing.
- `scylla-cql-core/src/serialize/row.rs:190–248,514–635`,
  `serialize/writers.rs:60–245`, and `scylla-cql/src/serialize/raw_batch.rs`:
  metadata order/repeated names, deterministic unused-name error,
  post-codec complete-row count check, pre-codec incremental count check,
  failed incremental append rollback, owned intermediate bytes, and builder
  completion/length proof. `statement/bound.rs:25–32` captures serialized values.
- `core/frame/response/result.rs:83–205`, `core/serialize/value.rs:98–222,
  369–433,750–855,1055–1137`, and `core/deserialize/value.rs:67–193,251–337,
  540–804,980–1070,1242–1444,1519–1605,1870–1945`: complete type inventory,
  fixed-vector widths, native/external scalar boundaries, null/unset bypass,
  dynamic empty handling, UDT names and missing fields, collection checks,
  vector lengths and exhaustion quirks, and lazy/error cursor behavior.
- `scylla-macros/src/serialize/{row,value}.rs` and
  `scylla-macros/src/deserialize/{row,value}.rs`: attribute validation,
  rename/skip/default/order rules, row-only flattening traits,
  `allow_missing`, suffix versus interior omissions, and separate null-default
  handling. Specifically inspected serialization generated branches at
  `serialize/value.rs:350–418,470–536` and deserialization at
  `deserialize/value.rs:410–522,775–839`.
- `core/deserialize/frame_slice.rs:43–245`, `core/deserialize/result.rs:13–143`,
  `scylla-cql/src/deserialize/result.rs:19–104`, and
  `scylla-cql/src/frame/response/result.rs:720–1025`: borrowed versus shared
  ownership, copy fallback, atomic individual reads versus partial row scans,
  once-per-iterator type checks, metadata cache selection, row-count failure,
  partition-key sequence/index conversion, and PREPARED paging rejection.
- `scylla/src/network/connection.rs:138–174,1105–1221,1487–1526`,
  `statement/prepared.rs:569–609`, and `client/pager.rs:714–837,1299–1368`:
  synchronous serialization before queue await, captured metadata snapshots,
  fresh snapshot after reprepare, unconditional cache store, normal response
  metadata decoding, and independent typed-stream ownership.
- `core/value.rs:773–881,1381–1463`, `core/pretty.rs`,
  `core/utils/safe_format.rs`, `core/{serialize,deserialize}/mod.rs`, and
  `core/frame/response/error.rs:36–174`: timestamp arithmetic and diagnostic
  formatting, cloned format iterators, shared error/downcast ownership,
  standard error-code precedence, unknown operation preservation, and
  recognized-field parse errors.
- `scylla-cql-core/Cargo.toml:24–94`, `scylla/Cargo.toml:66–85`, and
  `scylla/src/lib.rs:299–300`: optional adapter features, unconditional chrono
  diagnostic dependency, `full-serialization` scope, and serde test gating.

Here `core/` abbreviates `scylla-cql-core/src/`; paths beginning `frame/` in
the protocol paragraph abbreviate `scylla-cql/src/frame/`.

## Additional test assertions checked

- PREPARED round-trip (`scylla-cql/src/frame/response/result.rs:1360–1425`)
  loops over absent/present metadata IDs and compares statement ID, result ID,
  bind specs, result specs, and sorted partition-key index/sequence pairs.
- Named-row errors (`core/serialize/row_tests.rs:172–212`) check the field
  name and error kind for missing, unused, and incompatible cells. The count
  test at 266–285 accepts 65,535 values, rejects the next, and verifies retained
  contents; it does not count custom codec invocations. The rollback fixture
  at 355–403 appends through an unfinished builder, returns a custom error,
  and verifies both count and exact request bytes remain intact.
- Serialization tuple/UDT/wrapper tests inspect nested error kinds and field
  context. Present `Option`/`MaybeUnset` tests use wrapper helpers for metadata
  mismatch and a custom codec error; they do not exercise null/unset branches.
- Blob decoding (`core/deserialize/value_tests.rs:33–64`) compares contents,
  not pointers or storage retention. The metadata lifetime test at 2325–2391
  deliberately tolerates `Err`, making it a compile-time lifetime check.
- Null/empty fixtures at 808–872 distinguish value, null optional, and special
  empty wrappers. Vector bulk test at 1108–1137 compares successes or debug
  errors at all 17 truncation lengths of four `i32` elements. Capacity tests
  at 1143–1189 assert final capacity equals 100; no allocator instrumentation
  is present. The dynamic time test at 2886–2906 checks both range endpoints
  and four invalid values through `CqlValue`'s native decoder delegation.
- Generated UDT tests (`core/macros_tests.rs:69–215`) cover both matching
  flavors with missing-definition fields, skipped defaults, reordered fields,
  and excess suffixes. Row tests at 2540–2618,2637–2653,2766–2816 and
  3269 onward check name mismatch, rename bytes, flattened bytes, and error
  contexts. Missing raw column framing is distinct from a null field.
- Raw/lending iterator tests (`core/deserialize/result.rs:163–201` and
  `scylla-cql/src/deserialize/result.rs:122–173`) cover two complete rows and
  a declared second row absent from the buffer; they do not test continuation
  after an error in the middle of a row.
- Timestamp overflow tests (`core/value.rs:1667–1693`) have `should_panic`;
  the extreme checked-duration fixture at 1734–1742 expects
  `Duration::from_millis(u64::MAX)`.
- Integration UDT evolution (`types/cql_types.rs:1281–1404`) adds a field and
  asserts `None`; missing-field variants at 1457–1641 check suffix/interior
  omissions. The shortened nested tuple fixture (`types/cql_collections.rs:
  324–405`) checks typed and dynamic suffix nulls plus a 31-byte serialized
  payload. Dynamic UDT/duration tests in `types/cql_value.rs` compare complete
  expected values, including all three duration components.

## Remaining gaps and validation

The specification retains explicit missing Go implementation/race/integration/
allocation coverage. Rust gaps remain for allocation budgets, universal no-copy
claims, concurrent metadata replacement, retained blob lifetime across paging
and cancellation, malformed ignored UDT fields, nullable/trailing-empty vector
quirks, malformed duplicate extension fields, count/cache disagreement, and
post-error cursor continuation. Newly identified test limits are recorded in
the section rather than represented as successful coverage.

No test was run, and no earlier passing core test was repeated. Validation for
this documentation-only change checks the source commit, specification hashes,
existence of cited test symbols/local review links, and whitespace. Database
versions, optional-feature execution, and cross-version server interoperability
remain unverified by this round.
