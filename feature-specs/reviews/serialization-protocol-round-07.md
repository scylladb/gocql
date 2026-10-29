# Serialization and protocol review — round 07

- Section: `feature-specs/serialization-protocol.md`
- Round: 07, source-link/anchor accuracy and implementation-versus-contract wording
- Agent identity: `/root/review_serialization_07`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Verified source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `c3e8f276586497f32a15b662aebd21623e9ba332094b40a5b22e6d3488df692e`
- After SHA-256: `79942f9062764b83f25bffb05646dcd610590a40a0a8d72263a2424ff7ef7e46`

Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
assigned specification. This was an independent source audit; earlier reports
were not used as proof. Only the assigned specification and this report were
edited. No Go or Rust implementation was changed, and no tests were executed.

## Findings and fixes

1. **Evidence anchors did not consistently identify the advertised entry point.**
   Thirteen anchors were adjusted after reading their surrounding logic. The
   largest mismatches were `serialize/raw_batch.rs:90`, a slice iterator's
   `count` implementation, replaced by the `RawBatchValues` declaration at 17;
   `pager.rs:734`, inside private `QueryPager::next`, replaced by the public
   `next_column_iterator` wrapper at 748; and protocol `response/result.rs:720`,
   the prepared-result metadata helper, replaced by the advertised
   `RawMetadataAndRawRows::deserialize_metadata` method at 877.
   Other anchors now identify `RequestOpcode` (42), `ProtocolFeatures` (38),
   `ExecuteV2` (60), batch `do_serialize` (63), `ColumnType` (74), `FrameSlice`
   (46), `ColumnIterator` (31), `CustomTypeParser::parse` (55), the frame owner
   contract (55), and the core Cargo features section (23).
   This corrects navigation; the linked implementations were also checked for
   their stated conditions, rather than treating file existence as evidence.

2. **Custom-type acceptance rules were incomplete.**
   `scylla-cql/src/frame/response/custom_type_parser.rs:55–64,364–405` first
   skips leading whitespace, maps an empty remaining input to blob, validates
   and discards a hexadecimal `usize` prefix before `:`, and rejects remaining
   characters after a completed type expression. The old trailing-whitespace
   statement did not describe blank-input fallback or the prefix form. Added
   these cases and qualified rejection as whitespace following a type
   expression. The prefix bound follows `usize::from_str_radix`, not a fixed
   protocol integer width. Added the existing successful and malformed-prefix
   fixtures to the coverage discussion without claiming blank/overflow tests.

3. **Auxiliary request decoding has a panic boundary absent from its description.**
   `scylla-cql/src/frame/request/query.rs:212–216` reads the flags byte with
   `Buf::get_u8` after reading consistency. Batch decoding similarly uses it
   for statement kind at `batch.rs:179`, batch kind at 217, and flags at 233.
   An exhausted slice at those positions panics instead of returning
   `RequestDeserializationError`. Confirmed the `get_u8` implementation and
   its documented panic in the locally resolved `bytes-1.12.1` source
   (`src/buf/buf_impl.rs:317–325`). Added this limitation specifically to the
   auxiliary API, preserving the distinction from the network response
   decoder, and recorded the missing regression coverage.

4. **The deferred-cell inventory omitted the metadata-independent raw form.**
   `scylla-cql-core/src/deserialize/frame_slice.rs:244–255` implements
   `DeserializeValue` directly for `Option<FrameSlice<'frame>>`: type checking
   always succeeds, and deserialization returns `v` unchanged. This differs
   from `FrameSliceWithMetadata` at `deserialize/value.rs:1677–1705`, which also
   retains the column-type reference. Added the raw optional form and its
   lack of a retained metadata borrow. The spec still requires subsequent
   type/value validation when a concrete destination is chosen.

## Sources and behavior checked

Paths below are relative to the pinned Rust checkout. These were read to check
behavior, including relevant branches and callers; the table is not merely a
file inventory.

| Sources and symbols | Checked behavior |
| --- | --- |
| `scylla-cql/src/frame/mod.rs:65–342`, `SerializedRequest::make`, `read_response_frame`, extension parser, compression helpers | v4 direction/version checks; stream insertion; static VOID check; full body read and unchecked advertised allocation; unchecked outgoing whole-frame length cast; trace/warnings/payload ordering; LZ4 prefix and Snappy buffers |
| `frame/request/mod.rs:42–122`, `frame/response/mod.rs:26–160`; handshake, auth, register, supported files | Opcode inventories; request trait size hints; auxiliary decoder purpose; STARTUP/PREPARE/string-map framing; optional auth tokens; old/current event surfaces |
| `scylla-cql-core/src/frame/protocol_features.rs:64–144`; protocol `request/execute.rs:60–175`, `request/query.rs:79–273` | First numeric extension match; presence-based features; v2 preference; zero/all-bit LWT mask; metadata ID and tablet byte independence; defaults, flags, named-values rejection and signed page sizes |
| Protocol `request/batch.rs:63–272`, `serialize/raw_batch.rs:17–166`; core `serialize/batch.rs` | Count checks and statement error context; one value row per statement; raw adapters; count/skip semantics and replayable value providers |
| Core `frame/types.rs`, `serialize/row.rs:85–245,502–683`, `serialize/writers.rs:11–245` | Strict request null/unset versus negative response null; ordered and named rows; deterministic unused name; row/value count boundaries; add-value rollback; owned/copying row storage; no-size builder and completion proof limits |
| Core `frame/response/result.rs:74–191,253–375`; `serialize/value.rs:93–1148`; `deserialize/value.rs:67–883` | Complete type inventory; vector fixed-width classification; scalar endian/length/null checks; time range; optional adapters; special empty capability; dynamic dispatch and dynamic empty handling |
| Core `deserialize/value.rs:933–1705,1711–1939,1948–2177` | Up-front nested typed checks; collection null conversion; exact-capacity collection loops; vector bulk/element paths, exhaustion and nth skipping; map frame-before-value ordering; tuple omitted suffix; UDT absent/null distinctions; deferred cells; container ownership |
| `scylla-macros/src/serialize/{row,value}.rs`, `deserialize/{row,value}.rs` | Matching flavors; skipped name checks; rename/skip/null-default attributes; row-only flattening/internal traits; UDT excess/missing fields; ordered suffix behavior; ignored-field framing in name matching |
| Protocol `frame/response/result.rs:198–456,539–1068` | Self-borrowed metadata owner; lazy ROWS state; cached/empty/new metadata branches; metadata-ID flag condition; row-count failure; partition-key index sorting and sequence cast; PREPARED paging rejection |
| Core `deserialize/{frame_slice,row,result}.rs`; protocol `deserialize/result.rs` | Borrowed versus independently shared storage; copy fallback; cursor preservation per cell; row-scan partial consumption; error-bounded counts; lending-owner lifetime and once-per-iterator type check |
| `scylla/src/network/connection.rs:138–165,1105–1210,1487–1525`, `statement/prepared.rs:569–601`, `statement/bound.rs:27–33`, `client/pager.rs:714–826,1299–1363` | Synchronous owned request framing before queue await; one metadata snapshot per send and refreshed snapshot after reprepare; shared cache replacement; bound-value storage; page acquisition before row consumption; initial/per-page destination checks |
| Core `value.rs:219–256,440–476,772–883,1377–1497`, `utils/safe_format.rs`; protocol `utils/parse.rs` | Timeuuid signed-byte ordering; varint zero-prefix normalization; timestamp checked difference/arithmetic; diagnostic formatting; repeatable iterator formatting; parser position helpers |
| Core `serialize/mod.rs`, `deserialize/mod.rs`, `frame/response/error.rs:36–227` | Shared/downcastable error ownership; standard-error precedence over negotiated rate-limit code; retained unknown operation/write type; recognized field parse errors |
| Core `Cargo.toml:23–63`; protocol `frame/response/custom_type_parser.rs`; referenced test sources | Optional versus full-serialization feature membership; unconditional chrono dependency; custom grammar/frozen propagation; coverage scope and limitations |

The invariants were checked against these paths: negotiated layouts, separate
wire value states, validation before normal typed iteration, frame and metadata
lifetimes, server order, and serialization failure before dispatch remain
consistent. Observed weak validation and allocation limits remain recorded as
Rust implementation facts, rather than requirements for a future Go codec.

## Test claims and validation

- Mechanically verified all 57 pinned source links resolve to local files at
  the stated revision and every line anchor is in bounds. Manually checked
  anchor context and the associated behavior as described above.
- Mechanically resolved all 79 explicitly path-qualified test-function
  references. Also checked the newly named custom parser success fixture.
  Read assertions/fixtures across the cited codec, metadata, derive, helper,
  error, and integration targets to distinguish coverage from execution.
- In particular, EXECUTE tests use metadata-ID-disabled fixtures; the vector
  bulk test compares four `int` elements at all 0–16-byte truncations; the
  collection test asserts capacity for 100-element fixtures, not allocation
  counts; blob/container tests establish contents rather than pointer identity;
  and metadata lifetime checking does not require successful value decoding.
- Checked generated UDT `allow_missing` fixtures under both matching flavors,
  LZ4 short fixtures (0/1/3 bytes), custom-parser prefix/trailing-space
  assertions, secrecy feature gates, and the serde wrapper round-trip scope.
- Checked integration feature guards and outcome limits: scalar adapters are
  feature-gated, metadata-ID tests return early without negotiation, and the
  compression test measures outgoing QUERY/EXECUTE body totals for repetitive
  fixtures and accepts proxy shutdown `DriverDisconnected`.
- No core, protocol, optional-feature, integration, or Go tests were run in
  this round. The earlier round-03 execution statement was left explicitly
  attributed to that round and is not independent execution evidence here.
- Existing gaps remain, including ownership across cancellation/page changes,
  concurrency, hostile-input allocation bounds, the extension cross-product,
  and deferred-cell APIs. New blank-input/prefix-overflow and auxiliary-decoder
  truncation gaps are explicit. No cross-section edit was required.

Result: four evidence-backed corrections, including thirteen source-anchor
adjustments; no implementation changes and no newly claimed test execution.
