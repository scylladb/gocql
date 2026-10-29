# Serialization protocol review — round 02

- Section: `feature-specs/serialization-protocol.md`
- Round: 02 — whole-section audit, with additional attention to defaults,
  exact conditions, type/count/length limits, validation order, and fallback.
- Agent: `/root/review_serialization_02`
- Reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`, verified HEAD
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`.
- Before SHA-256: `d0ff04c6ade77fd48e62b9712004ff85c9c9731d88c7bf0a3051a62342ac3d7b`
- After SHA-256: `b6edd77fbee2a1c50f4486cb808185896b0dbe5678d94a808b5df37f245914a9`
- Ownership: only the section and this report were edited. No Go or Rust source
  was changed. The reference checkout remained clean.

Read `feature-specs/reviews/REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`,
and the complete assigned spec. Findings below were independently traced to
the pinned source; the prior report was not used as proof.

## Findings and corrections

1. **Extension parsing omitted first-match and presence-only boundaries.**
   `scylla-cql-core/src/frame/protocol_features.rs:79-120` parses the first
   exact `KEY=` entry and then attempts the integer conversion. It does not
   retry a later duplicate after conversion fails. Tablet-v1/v2 and metadata-ID
   use `contains_key`, including empty value lists. The mask predicate at
   `143-148` accepts a zero LWT mask for every flags value. Added these exact
   conditions without changing the existing unsupported-on-parse-failure rule.

2. **Length limits and their enforcement points were incomplete.**
   `scylla-cql/src/frame/types.rs:141-172,214-244,269-281,322-371` uses checked
   unsigned-16-bit lengths for short strings/IDs and string-list/map counts,
   and checked nonnegative signed-32-bit lengths for long strings/bytes.
   String length counts bytes. `scylla-cql-core/src/serialize/writers.rs:31-45`
   allows a low-level row writer to exceed the protocol value count;
   `serialize/row.rs:544-565,605-630` checks after a complete row but before
   invoking a new cell codec in `add_value`. Batch checks occur after row
   serialization at `scylla-cql/src/frame/request/batch.rs:92-117`.
   Added these distinctions. Also documented that `QueryParameters::serialize`
   writes page size verbatim (`request/query.rs:165-167`), whereas
   `scylla/src/statement/mod.rs:65-75` validates positive sizes.

3. **The aggregate validation statement needed a decoding/encoding boundary.**
   Typed list/map decoding checks nested metadata before iteration
   (`scylla-cql-core/src/deserialize/value.rs:980-1010,1495-1521`). Encoding
   only invokes nested codecs while traversing actual values
   (`serialize/value.rs:932-980,1102-1149`), so empty aggregates do not check
   their absent element types. Null/unset encoders bypass their wrapped codec
   (`369-415`). Map keys are serialized before values. Named rows process
   requested metadata before checking unused names (`serialize/row.rs:190-228`);
   dynamic UDTs likewise check leftover names after serializing fields
   (`serialize/value.rs:774-813`). Added these ordering and bypass rules.

4. **Vector fallback was too broadly described as truncated-remainder errors.**
   `scylla-cql-core/src/deserialize/frame_slice.rs:210-236` returns `None` when
   its slice is already empty, regardless of the requested count. Vector
   iteration passes that to the element decoder
   (`deserialize/value.rs:1333-1390`); `Option<T>` accepts it (`251-266`).
   Thus missing fixed-width elements can become null for nullable destinations.
   A variable-width element with a zero length is present-empty if later bytes
   exist, but null if the length prefix exhausts the slice. The bulk path
   delegates its remainder to this same iterator (`1299-1331`). Also corrected
   error context: the variable-width element failure records `element_type`
   at `1383-1388`, while framing errors use the collection type. Finally,
   `custom_type_parser.rs:161-186` and `utils/parse.rs:88-96` accept dimension
   zero through unsigned-16-bit parsing. Added these limits and quirks without
   presenting them as server validity requirements.

5. **Metadata fallback and partition-key validation limits were missing.**
   `scylla-cql/src/frame/response/result.rs:881-921` selects cached or empty
   metadata for NO_METADATA without comparing the received column count.
   Raw row traversal then uses the selected specs
   (`scylla-cql-core/src/deserialize/result.rs:57-77`). The prepared parser
   reads an i32-length partition-key count, stores each wire index as u16,
   casts its ordinal with `i as u16`, and sorts without duplicate/range checks
   (`response/result.rs:936-963`). The original claim about retained sequence
   needed its u16 boundary. Added both points and documented i32 column/row
   counts, u16 native tuple/UDT field counts (`517-616`), and the absence of a
   separate nesting/allocation budget. Collection count-based reservation is
   visible at `deserialize/value.rs:933-938,1032-1043`.

6. **Flattening requires more than an arbitrary row codec.**
   `scylla-macros/src/serialize/row.rs:254-278,376-389,431-470` requires nested
   `SerializeRowByName` or `SerializeRowInOrder` according to the parent's
   flavor. `_macro_internal.rs:38-64` describes the name-matching restriction.
   Added that matching generated implementations normally supply these
   internal traits; implementing `SerializeRow` alone does not suffice.

7. **Rate-limit interpretation needed code-collision precedence.**
   `scylla-cql-core/src/frame/response/error.rs:47-154` matches standard database
   codes before the guarded negotiated rate-limit code. Added that a collision
   preserves standard error decoding, rather than overriding it with the
   extension.

8. **Optional features conceal a source-level availability inconsistency.**
   `scylla-cql-core/src/deserialize/value.rs:652-680` gates
   `get_nanos_from_time_column` on `chrono-04` or `time-03`, while its caller in
   the native `CqlTime` implementation is unconditional. Core Cargo features
   (`Cargo.toml:25-62`) define neither as a default. Added an explicit source
   observation and build-verification limitation. This is not claimed as a
   compiler-confirmed failure: the attempted check was rejected by the Rust
   version requirement before compilation, as recorded below.

9. **Metadata-ID integration coverage is conditional.**
   All three cited targets in
   `scylla/tests/integration/statements/result_metadata_extension.rs`
   (`286-293`, `394-401`, `430-439`) return early if metadata IDs are not
   negotiated. Added this qualification so a future successful process exit
   cannot alone be treated as feature coverage. Added explicit test gaps for
   the newly documented vector, extension-duplicate, cache-count, and build
   feature boundaries.

## Whole-section behaviors checked

- Frame v4 direction/version/opcode checks, nine-byte header, static VOID
  optimization, advertised-body allocation, unchecked outgoing total length,
  compression and trace/warning/payload order:
  `scylla-cql/src/frame/mod.rs:53-345`. Verified request dispatch constructs a
  complete serialized request before channel submission at
  `scylla/src/network/connection.rs:138-165`.
- Request/response inventories: `frame/request/mod.rs:38-81` and
  `frame/response/mod.rs:23-60`. OPTIONS, STARTUP, PREPARE, REGISTER, and
  authentication codecs: the corresponding request files, plus
  `frame/response/authenticate.rs` and `supported.rs`. CQL language default
  is `4.0.0` in `request/options.rs`; it is separate from native version 4.
- Event variants and unknown-name handling, schema Invalid fallback,
  CLIENT_ROUTES_CHANGE paired counts and UUID parsing:
  `scylla-cql/src/frame/response/event.rs:16-449`.
- Query flags, default LOCAL_QUORUM, empty values, initial paging state,
  no serial override/timestamp/page size, and metadata inclusion:
  `request/query.rs:107-271`, core `frame/types.rs:17-132`, and core
  `frame/request/query.rs`. EXECUTE metadata-ID/tablet-byte order and
  feature-aware decoder: `request/execute.rs:78-169`; the connection gate is
  `scylla/src/network/connection.rs:1137-1155`.
- Batch counts and mismatches, replayable providers and first-row reuse:
  `request/batch.rs:61-158`, core `serialize/batch.rs:30-165`,
  `scylla-cql/src/serialize/raw_batch.rs:90-171`, and
  `scylla/src/statement/batch.rs:285-339`.
- Native type inventory, fixed-vector-width classification, frozen behavior,
  and special-empty capability: core `frame/response/result.rs:72-375`.
  Scalar serializers and decoders, integer strictness, ASCII/UTF-8 asymmetry,
  decimal scale, time range, UUID lengths, duration vint parsing, null/unset,
  and dynamic empty conversion: core `serialize/value.rs:37-438` and
  `deserialize/value.rs:48-806`.
- Tuple suffixes, collection-null behavior, set/list restrictions, dynamic UDT
  names and field mapping, and raw UDT missing-vs-null states:
  core `serialize/value.rs:475-705,750-929` and
  `deserialize/value.rs:933-1184,1495-1668,1711-1943`.
- Generated name/order matching, strict UDT extras, allow_missing versus
  default_when_null, skipped fields and invalid attribute checks:
  `scylla-macros/src/lib.rs`, `serialize/{row,value}.rs`,
  `deserialize/{row,value}.rs`, especially UDT serialization `270-421,453-558`
  and UDT deserialization `236-354,405-522,654-735,775-823`.
- Frame ownership, `to_bytes` copy fallback, immutable borrowed/shared values,
  row scan boundaries and once-per-iterator type check:
  core `deserialize/README.md`, `frame_slice.rs:45-236`, `row.rs:16-95`,
  `result.rs:13-152`, and `scylla-cql/src/deserialize/result.rs:19-110`.
- Custom-type recursive grammar, frozen propagation, UTF-8/hex conversion and
  strict trailing-input check: `frame/response/custom_type_parser.rs:29-335`
  and `utils/parse.rs:15-135`. Typed error wrappers and downcasting:
  core `serialize/mod.rs:17-62` and `deserialize/mod.rs:15-106`.
- Timestamp helpers and timeuuid ordering, CQL diagnostic display, safe repeat
  formatting: core `value.rs:219-322,773-882,1381-1486`, `pretty.rs`, and
  `utils/safe_format.rs`. Feature exports and unstable raw API gates:
  core/CQL `lib.rs`, core `Cargo.toml`, and
  `scylla/src/response/query_result.rs:418-424`. Macro/book/serde harness roles
  were checked in `macros_doctests.rs`, `book_tests.rs`, and `serde_tests.rs`.

## Tests and verification

All explicitly named test file/symbol references in the section were checked
against the pinned tree; none was missing. Representative bodies and helpers
were inspected for the substantive expectations, including:

- LZ4 round-trip and short-prefix rejection; tablet negotiation and EXECUTE
  omission/round-trip; prepared result serialization.
- Named-row missing/extra/type errors and exactly 65,535 incremental values
  (`serialize/row_tests.rs:172-207,266-285`). Present Option/MaybeUnset tests
  propagate codec errors; they do not prove the absent-value path.
- The bulk-vector fixture uses four `int` values and all payload truncations
  (`deserialize/value_tests.rs:1108-1139`). It does not test nullable elements.
  The allocation fixture checks capacity/length 100 for typed list/vector and
  dynamic map/vector (`1143-1198`), not actual allocation counts.
- Borrowed/shared/owned blob content, metadata lifetime compile assertions,
  scalar errors, null/empty wrappers and native time boundaries; generated
  UDT allow_missing fixtures and row flatten/rename/order tests.
- Raw and lending row short-input tests; timestamp overflow/difference tests;
  custom grammar whitespace tests; structured database errors; serde forwarding
  for Counter/CqlDuration.
- Integration targets for scalar/UDT evolution, collections, shortened nested
  tuples and vectors, metadata IDs, and compression. The collection/vector
  helper asserts selected values equal inserted values. Compression checks
  values, flags and accumulated wire sizes. No database run was performed.

Attempted command, from the reference checkout:

```text
cargo check -p scylla-cql-core --no-default-features --target-dir /tmp/gocql-rust-check-target
```

Observed exit status: **101**, before compilation:

```text
error: rustc 1.93.0 is not supported by the following packages:
  scylla-cql-core@1.9.0 requires rustc 1.94
  scylla-macros@1.9.0 requires rustc 1.94
```

No Rust unit/integration tests or Go tests were executed. The default-feature
build observation remains source-only, and the documented adversarial cases
remain unverified by executable tests. Hostile allocation budgets and universal
zero-copy guarantees remain explicitly absent. Final SHA-256 was recomputed
after the edits; no cross-section file was edited.
