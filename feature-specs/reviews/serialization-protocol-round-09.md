# Serialization and protocol review — round 09

- Section: `feature-specs/serialization-protocol.md`
- Round: 09, independent whole-section review and fix; additional focus on
  malformed lengths/types/counts, partial decoding, ownership, and extensions.
- Agent identity: `/root/review_serialization_09`
- Source: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256:
  `292c340f72052f6d02d433dc4e0014668dbc51eb6cd76541f937cce26e7c43da`
- After SHA-256:
  `bf2b16f7ea12768f68e7572e3a859618920cee592f90227ecedcdef876705829`
- Changed only the assigned specification and this report. No Go or Rust
  implementation changes, child agents, builds, or test executions.

## Findings and fixes

1. **Some malformed custom type expressions do not terminate with an error.**
   The previous statement that bad parameter counts fail was too broad.
   `CustomTypeParser::get_type_parameters` returns an iterator that repeatedly
   yields `UnexpectedEndOfInput` without advancing at EOF
   (`scylla-cql/src/frame/response/custom_type_parser.rs:133–158`). The
   fixed-arity `get_n_type_parameters` fallback counts that iterator without
   stopping on errors (`252–271`). For `ListType(Int32Type`, the one valid item
   is followed by an EOF error, which makes the array collection fail; the
   backup iterator then keeps producing EOF errors while `.count()` runs.
   List, Set, Map, and Frozen use this helper (`282–357`). Tuple instead
   collects `Result<Vec<_>, _>` and returns its first error (`309–322`).
   Confirmed the dependency behavior in the locally available itertools 0.15.0
   `src/lib.rs:2327–2332`: `collect_array` reads N items and requires the next
   item to be absent. `scylla-cql/Cargo.toml:107` and `Cargo.lock:1165–1171`
   identify that version. Corrected the parser description and general
   malformed-input limits; distinguished the existing unterminated-Tuple test
   from the uncovered fixed-arity path. This conclusion is a source trace,
   not a timed reproduction.

2. **A per-dimension bound does not provide checked aggregate vector sizes.**
   `ColumnType::type_size_for_vector` recursively multiplies `usize` widths by
   dimensions without checked arithmetic
   (`scylla-cql-core/src/frame/response/result.rs:297–307`). `size_hint` and
   `fields_size_hint` reuse those products and sum field sizes (`310–357`).
   `SerializedValues::from_serializable` sums the metadata hints and calls
   `Vec::with_capacity` before the row codec runs
   (`scylla-cql-core/src/serialize/row.rs:519–552`). This can occur even when
   application values would encode as null. `VectorIterator::nth` likewise
   multiplies the skipped element count by element width without checking
   (`scylla-cql-core/src/deserialize/value.rs:1411–1431`). Added the
   platform-sized arithmetic boundary, overflow-check-dependent panic/wrap
   behavior, and the pre-codec allocation boundary. No enormous allocation was
   attempted.

3. **Successful decoding need not consume all message or cell bytes.**
   The existing count-bounded caveat appeared under vectors and did not clearly
   cover the other independently exposed decoders. `FixedLengthBytesSequenceIterator`
   stops at its count (`deserialize/value.rs:2094–2138`); tuple decoding stops
   at its metadata fields (`1722–1770`); `UdtIterator` stops at its field list
   (`1899–1939`); raw row iteration stops at its row count
   (`deserialize/result.rs:54–76`). None adds an end-of-buffer check.
   `CqlDuration` also returns after three variable integers without checking its
   remaining bytes (`deserialize/value.rs:551–594`), and `unsigned_vint_decode`
   does not enforce canonical shortest encodings
   (`frame/types.rs:281–297`). `ResponseV2::deserialize` returns after its
   selected parser, with READY ignoring the body
   (`scylla-cql/src/frame/response/mod.rs:208–239`), while RESULT VOID ignores
   bytes after its kind (`frame/response/result.rs:1035–1057`). Header/extension
   processing only tests recognized flag bits (`frame/mod.rs:168–178,250–297`).
   Expanded the spec to cover surplus tuple/UDT/collection/row bytes, duration
   suffixes and noncanonical variable integers, ignored response-header bits,
   and lack of whole-body consumption validation. This also agrees with the
   parent-reported routing review observation about tablet payloads using the
   generic tuple/list decoders; no routing file was edited.

4. **Duplicate-name behavior differs by representation and direction.**
   Dynamic UDT serialization first collects source fields into a `HashMap`, so
   duplicate source names keep the last value. It then removes the chosen entry
   at the first matching metadata field, leaving null for any later repeated
   metadata name (`serialize/value.rs:774–798`). Generated name-matching row
   and UDT serializers instead serialize each metadata occurrence from the same
   source field, only decrementing their visited counter on its first use
   (`scylla-macros/src/serialize/row.rs:293–304`,
   `serialize/value.rs:382–405`). Their name-matching deserializer type checks
   reject repeated names mapped to a destination field
   (`deserialize/row.rs:415–440`, `deserialize/value.rs:605–626`). Documented
   dynamic duplicate collapsing/removal and replaced the ambiguous generated
   row set/count statement with the specific serialization versus decoding
   behavior. This does not claim rejection of repeated unknown UDT fields that
   the generated destination ignores.

5. **The incoming-frame allocation caveat also needs a decompression boundary.**
   `decompress` passes the LZ4 uncompressed-size prefix directly to
   `lz4_flex::decompress` and delegates Snappy to `decompress_vec` without a
   driver-level size or expansion-ratio guard
   (`scylla-cql/src/frame/mod.rs:325–347`). Added this separately from the
   existing advertised wire-frame length caveat. Short-LZ4-prefix rejection is
   retained; it is not described as an expansion or allocation bound.

6. **The auxiliary request dispatcher can panic even for a valid opcode.**
   `Request::deserialize` and `RequestV2::deserialize` support only QUERY,
   EXECUTE, and BATCH; every other `RequestOpcode` reaches `unimplemented!`
   (`scylla-cql/src/frame/request/mod.rs:166–179,218–238`). Added this to the
   existing truncated-input panic caveat and its test gap. This is an inspection
   API limitation, not a limitation on sending those client message types.

## Other inspected source and checked behavior

The entire specification and `feature-specs/README.md` were read. The following
additional checks found no correction necessary. Paths below are relative to
the pinned reference checkout; source inspection is not test execution.

- `scylla-cql/src/frame/mod.rs`, `request/mod.rs`, `response/mod.rs`:
  native v4 and direction checks, signed stream and body layout, static VOID
  optimization, owned request framing before submission, compression ordering,
  tracing/warning/custom-payload extension order, and opcode dispatch.
- `scylla-cql-core/src/frame/protocol_features.rs`:
  malformed numeric fields, first exact extension-field occurrence, presence
  flags, tablet-v2 precedence, independent metadata-ID support, and zero/all-bit
  LWT mask semantics. `scylla-cql/src/frame/types.rs` and
  `response/supported.rs`: map replacement behavior and protocol length codecs.
- `scylla-cql/src/frame/request/{options,startup,prepare,auth_response,register}.rs`,
  `response/authenticate.rs`, `response/event.rs`: CQL version default, string
  and optional token framing, old/new event surfaces, schema invalid-kind versus
  unknown-target behavior, paired client-route counts, and host UUID validation.
- `scylla-cql/src/frame/request/{query,execute,batch}.rs` and
  `scylla-cql-core/src/frame/request/query.rs`: defaults, option flags,
  null-versus-empty paging state, consistency conversion, named-value rejection,
  signed page-size pass-through, extension ordering, batch kind/count errors,
  and unchecked one-byte auxiliary decoder reads. `ExecuteV2` remains distinct
  from both featureless and older Execute decoding.
- `scylla-cql-core/src/serialize/{value,row,writers,batch}.rs`,
  `scylla-cql/src/serialize/raw_batch.rs`:
  strict scalar encoding, ASCII encoder permissiveness, null/unset bypass of
  inner codecs, exact vector dimensions and width classification, shortened
  tuples, UDT missing/unknown-name behavior, row reordering and deterministic
  unused-name errors, checked cell/count limits, incremental rollback,
  malformed custom-writer iterator panic, no-size writers and completion proof,
  replayable batch iterators, and actual buffer-copy boundaries.
- `scylla-cql-core/src/deserialize/{value,row,result,frame_slice}.rs` and
  `scylla-cql/src/deserialize/result.rs`: scalar byte-width/type checking,
  arbitrary-nonzero Boolean decoding, time-of-day bounds, native decimal/varint
  views, text validation, dynamic special-empty handling, null collections,
  absent tuple/UDT suffixes, remaining-count hints on errors, map framing-error
  precedence, nontransactional row scanning, variable-vector prefix consumption,
  exhausted-vector null quirks, bulk-vector fallback, lazy skipping, raw deferred
  cells, borrowed versus retained/copied storage, and metadata/frame lifetime
  separation.
- `scylla-cql/src/frame/response/result.rs`:
  standard type IDs and recursive descriptors, frozen metadata behavior,
  nonnegative counts, allocation before complete validation, NO_METADATA cache
  and empty fallback, newly sent metadata precedence, metadata-ID incompatibility,
  PREPARED paging rejection, unvalidated partition-key indexes/sequence casts,
  Yoke-backed metadata references, and the test serializer's unsupported vector
  branch.
- `scylla-macros/src/{serialize,deserialize}/{row,value}.rs`:
  name/order flavors, skipped names and defaults, rename/flatten constraints,
  missing-definition versus null policies, UDT excess-field strictness,
  ordered suffix handling, and ignored-field framing validation.
- `scylla/src/{network/connection.rs,statement/prepared.rs,statement/bound.rs,
  statement/batch.rs,client/session.rs,client/pager.rs}`:
  synchronous serialization before queue awaits, retained bound values,
  per-send metadata snapshots and unconditional cache stores, first-statement
  batch row reuse, typed page checks, lending cancellation boundary, and
  shared-owner values independent of page replacement.
- `scylla/src/response/{query_result,request_response}.rs`:
  gated raw-result accessors, ownership transfers, and removal of custom payload
  during ordinary non-error result conversion.
- `scylla-cql-core/src/{value,pretty}.rs`, `utils/safe_format.rs`,
  `scylla-cql/src/{value.rs,utils/parse.rs}`:
  unchanged varint serialized bytes and zero-prefix identity, exact-scale decimal
  identity, timeuuid version-mask/signed-byte ordering, timestamp arithmetic,
  diagnostic formatting and quote escaping, cloned diagnostic iterators,
  nonadvancing compatibility decoder, and character-position helper behavior.
- `scylla-cql-core/src/deserialize/mod.rs`, `frame/response/error.rs`, and
  `Cargo.toml`: error ownership/downcast boundaries, structured database error
  fields, standard-code priority over rate-limit collisions, unknown operation
  preservation, optional adapter/serde feature boundaries, and unconditional
  chrono dependency. Deferred-cell and Box/Arc/Cow behavior was traced in
  `deserialize/value.rs:1674–1704,1948–2031`.

## Test claims and validation

- A documentation-reference check resolved all 79 `path:test_symbol` references
  to actual Rust function definitions and all 61 pinned source links to existing
  files/in-range line anchors. This verifies reference existence, not behavior.
- Read the custom-type parser error/whitespace fixtures
  (`custom_type_parser.rs:552–668`), confirming the malformed-input termination
  gap above and the existing qualification on the misleading internal-whitespace
  test name.
- Read vector `nth`, bulk-decoding truncation, and collection-capacity fixtures
  (`deserialize/value_tests.rs:372–470,1108–1192`). They cover valid five-element
  skips, every truncation of the four-int fixture, and final capacities of
  100-element collections; they do not establish checked nested width products,
  hostile allocation bounds, or allocation-call counts.
- Read `test_null_errors`, metadata-independent lifetime test, and time-range
  fixtures (`deserialize/value_tests.rs:2189–2201,2325–2394,2886–2912`). Retained
  the spec's caveats: the first is an incompatible non-null map case; the second
  allows error results and establishes a compile-time lifetime property.
- Read raw and lending row tests in their respective `deserialize/result.rs`
  files; valid boundaries and too-few-rows checks do not establish rejection of
  surplus row bytes or transactional recovery after partial column consumption.
- Read compression unit fixtures in `frame/mod.rs` and integration fixture/range
  assertions in `scylla/tests/integration/session/compression.rs`; they check
  short-prefix rejection and round trips/request wire sizes, not malicious
  decompression expansion. Located the cited type integration tests and
  verified metadata-ID test early returns when the extension is unsupported
  (`statements/result_metadata_extension.rs:291–292,398–400,435–437`).
- Added explicit coverage gaps for this round's source-inspected boundaries.
  All existing Go missing-coverage entries remain future requirements. No new
  claims of successful Rust/database/Go execution were added; the round-03
  execution statement remains attributed to that earlier round.

No cross-section edit was required. The parent was informed of the parser
termination finding and the generic trailing-byte behavior relevant to tablet
payloads. The report and specification intentionally describe observed Rust
limitations without requiring a future Go implementation to reproduce them.
