# Serialization/protocol review — round 03

- Section: `feature-specs/serialization-protocol.md`
- Round: 03 — errors, partial parsing, fallback, and recovery; whole-section audit
- Agent: `/root/review_serialization_03`
- Reference: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `b6edd77fbee2a1c50f4486cb808185896b0dbe5678d94a808b5df37f245914a9`
- After SHA-256: `c2d11951cebf216e8046e0daa652a0270d96243ca6d3f781c4f16360e5b34477`

Read the review instructions, section README, and complete assigned section.
Checked the reference HEAD and clean working tree independently. Only the
assigned specification and this report were edited; no Go or Rust source was
changed and no child agents were used. Earlier findings were not treated as
proof: the feature-gating concern below was contradicted by direct inspection.

## Source inspected

Paths in this table are relative to the reference checkout. Line numbers refer
to the pinned commit.

| Files | Symbols and paths checked |
| --- | --- |
| `scylla-cql/src/frame/mod.rs:63–345` | `SerializedRequest::make`, header/version/opcode checks, static VOID body, partial body reads, extension parsing, LZ4/Snappy |
| `scylla-cql-core/src/frame/protocol_features.rs` | Numeric extension parsing, key presence, tablet-v2 priority, LWT mask matching |
| `scylla-cql/src/frame/request/{mod,options,startup,prepare,auth_response,query,execute,batch}.rs` | Request inventory, option/token framing, query flags/defaults, feature-aware EXECUTE, batch count/error paths |
| `scylla-cql/src/frame/response/{event,custom_type_parser,result}.rs` | Schema event fallback, event families, custom parser EOF/aliases/frozen context; metadata parsing/cache branches at `result.rs:720–1054` |
| `scylla-cql-core/src/frame/{types,response/error}.rs` and `scylla-cql/src/frame/types.rs` | Null/unset distinction, integer lengths, vint prefix consumption, consistency, inet, `parse_prefix`; database error parsing at `error.rs:22–174` |
| `scylla-cql-core/src/serialize/{mod,row,value,writers,batch}.rs`, `scylla-cql/src/serialize/raw_batch.rs` | Error wrappers, row order/counts, `SerializedValues::add_value`, tuple/UDT/null encoding, completion proof, replayable providers and adapter |
| `scylla-cql-core/src/deserialize/{mod,frame_slice,result,row,value}.rs` | Error wrappers; owner promotion; raw/typed row and column iteration; scalar type checks; native time helper; tuple/UDT/list/map/vector failure paths and collection helpers |
| `scylla-cql/src/deserialize/result.rs:25–112` | Lending iterator cursor/count updates after success and failure |
| `scylla-macros/src/{serialize,deserialize}/{row,value}.rs` | Attribute validation, flatten trait references, name/order matching, missing/default policies; generated UDT read loops at `deserialize/value.rs:495–565,860–894` |
| `scylla-cql-core/Cargo.toml`, `scylla-cql-core/src/{lib,value,pretty}.rs`, `scylla-cql-core/src/utils/safe_format.rs`, `scylla-cql/src/utils/parse.rs` | Feature/dependency declarations, wrapper exports, timestamp arithmetic/formatting, repeated formatting, parser helpers |
| Core serialization/deserialization tests, core `macros_tests.rs`, protocol inline tests, and named integration files | Named-target existence; focused assertions described below; coverage versus execution distinction |

## Verified findings and fixes

### 1. The no-feature native-time concern was factually incorrect

The previous specification said `get_nanos_from_time_column` was gated on a time
adapter. It is unconditional at
[`deserialize/value.rs:653`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L653),
as is `CqlTime`'s call at line 676. The feature gate at line 608 belongs to the
different date-adapter helper. `chrono-04` is a nonoptional dependency for
formatting in `scylla-cql-core/Cargo.toml:75–78`; its named feature gates CQL
adapters, not that dependency.

Removed the nonexistent gating inconsistency and the unresolved-build claim.
The earlier default-Rust-1.93 attempt stopped before compilation and proved no
compiler failure in this feature combination. This round used installed Rust
1.94.1 and successfully compiled and tested the core without default features.

### 2. Iterator recovery and partial consumption were underspecified

[`RawRowIterator::next`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/result.rs#L56)
decrements the remaining row count before scanning and preserves consumption of
successful columns before a failure. Individual `FrameSlice` reads are
transactional (`frame_slice.rs:193–236`), but the row scan is not. Later calls
remain at the failing bytes and restart their diagnostic column index at zero.
`TypedRowIterator::next` at lines 134–138 advances the raw row first, so a value
decoder failure permits the next fully framed row to be decoded. The lending
implementation updates `at` per successfully scanned column and has the same
boundary (`scylla-cql/src/deserialize/result.rs:54–89`).

For collections, `ListlikeIterator::next` (`deserialize/value.rs:1053–1067`)
consumes a successful frame before invoking the value decoder.
[`MapIterator::next`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L1563)
reads both raw frames before decoding the key; therefore value framing errors
precede key decoding errors. The count-bounded raw iterator consumes a slot on
every call, even after framing fails (`value.rs:2127–2135`). Variable-vector
length prefixes mutate the slice separately (`value.rs:1354–1380`,
`frame/types.rs:281–296`), so the complete vector-element read has no rollback.
The public `BytesSequenceIterator` has no count bound and repeatedly returns an
error for malformed nonempty input without moving (`value.rs:2168–2173`).

Added these distinctions, first-error behavior of collected destinations, and
the unbounded raw-iterator error case. The specification no longer lets the
single-read rollback statement imply whole-row or whole-element recovery.

### 3. Ignored UDT fields have different malformed-input behavior by flavor

Name-matching generated decoding iterates all metadata fields and applies `?`
to their framing result before testing whether the field is used
([`deserialize/value.rs:868–884`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/deserialize/value.rs#L868)).
It therefore rejects malformed framing in ignored fields, but does not run
their value decoders. Ordered generation returns once its own field finalizers
have run (`deserialize/value.rs:545–565`), leaving an allowed suffix unread.

Qualified the existing statement that extra database fields are ignored. This
does not change the documented missing-definition, missing-byte, or null-default
policies.

### 4. Cached metadata is not a recovery fallback for malformed sent metadata

The cache branches in
[`RawMetadataAndRawRows::deserialize_metadata`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/result.rs#L877)
only use cached/empty metadata when `no_metadata()` is true. The sent-metadata
branch propagates deserialization failure at line 915. Row-count parsing occurs
after selecting/decoding metadata and propagates its own error at lines 923–925.

Added that malformed sent metadata never causes fallback to the cache, and that
a later row-count error does not return a partially usable metadata/rows result.

### 5. Error-context claims were stronger than actual framing diagnostics

Typed tuple decoding wraps a framing read directly in `DeserializationError`
at [`deserialize/value.rs:1750`](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L1750);
the tuple position is only added for the nested value decoder at lines
1753–1760. Replaced the blanket nested-context statement with that distinction.

Also made the database-error fallback boundary explicit:
`frame/response/error.rs:22–31,46–155` wraps malformed known-error fields with
family/field context and propagates failure; it does not substitute
`DbError::Other`. Unknown rate-limit operation bytes are retained as
`OperationType::Other` at lines 167–174. The existing standard-code precedence
over the negotiated rate-limit code was independently confirmed.

## Checked behaviors retained without findings

- Native-v4 framing, direction/version rejection, body EOF staging, complete
  body ownership, static VOID optimization, and lack of a frame allocation cap.
- Compression-prefix rejection and new decompression allocation; tracing,
  warnings, then custom-payload ordering; no compression fallback on parse errors.
- Extension malformed-number fallback, first matching advertised value,
  key-presence capabilities, zero LWT mask, and feature-aware EXECUTE layout.
- Query defaults/flags, serial consistency restriction, bind-marker ordering,
  count limits, deterministic leftover-name errors, and independent batch rows.
- Partial low-level writes versus complete-request failure; incremental row
  rollback; `WrittenCellProof`/unfinished builder boundaries.
- Scalar strict type/length checks, native time range, date/timestamp wrappers,
  tuple suffix/null behavior, special-empty handling, frozen metadata, dynamic
  UDT names, and map key/value ordering.
- Vector metadata dimension, fixed/variable encoding, bulk-decoding fallback,
  nullable exhausted-element quirks, and ignored trailing bytes.
- Row/UDT derive name/order/default rules and flattening's restricted trait
  surface; frame/metadata lifetime separation and owner-promotion copy fallback.
- Deferred metadata parsing, cached metadata count mismatch behavior,
  metadata-changed/no-metadata rejection, prepared paging-state rejection, and
  partition-key index sorting without additional index validation.
- Custom-type EOF validation, diagnostic formatting boundaries, and timestamp
  overflow/checked-duration behavior. Go tests/benchmarks remain future work.

## Test evidence and remaining gaps

Executed in the clean reference checkout, using a target directory outside it:

```text
cargo +1.94.1 check -p scylla-cql-core --no-default-features --target-dir /tmp/gocql-rust-check-target
exit 0; Finished dev profile in 10.23s

cargo +1.94.1 test -p scylla-cql-core --no-default-features --lib --target-dir /tmp/gocql-rust-check-target
exit 0; 228 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

This is a core-only compilation and library-test result. Protocol-crate tests,
doctests, the optional-feature matrix, database integration tests, and Go tests
were not executed. The reference working tree stayed clean.

Resolved all 66 referenced Rust test names to function definitions. Focused
assertion inspection confirmed that `serialize/row_tests.rs:serialized_values`
appends through a failing custom codec and checks unchanged count and request
bytes afterward; added this previously omitted rollback coverage. The raw and
lending row short-result tests exercise a missing next row, not continuation
after a failure in the middle of a row. The vector bulk test covers all 17
lengths of a four-int payload and compares success/error results; the collection
allocation test checks capacity, not allocator call counts. Tuple and map tests
check nested value errors but do not establish all framing-context behavior.

Integration test names and metadata-ID early-return gates were checked from
source only. They establish candidate coverage locations, not a successful
database run. Continuation after mid-row framing errors, malformed ignored UDT
fields, variable-vector prefix recovery, the unbounded raw-iterator error case,
and metadata/cache count disagreement still lack execution evidence in the
cited targeted tests. No broader rejection or zero-copy guarantee was inferred.

Validated the edited Markdown for trailing whitespace and resolved the test
references. No cross-section edit was needed. The successful no-feature build
and correction of the earlier inference were reported to the parent agent.
