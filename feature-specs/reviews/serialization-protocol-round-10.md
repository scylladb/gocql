# Serialization and protocol review — round 10

- Section: `feature-specs/serialization-protocol.md`
- Round: 10, final independent whole-section review and fix.
- Agent identity: `/root/review_serialization_10`
- Source checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit, independently verified with `git rev-parse HEAD`:
  `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256:
  `bf2b16f7ea12768f68e7572e3a859618920cee592f90227ecedcdef876705829`
- After SHA-256:
  `05702143e365a3fbd60f05d464e58665b35ad01c7c5375fc159ebad56cc76184`
- Read `REVIEW-INSTRUCTIONS.md`, `feature-specs/README.md`, and the complete
  assigned specification. The round-09 report supplied context only; conclusions
  below come from independently inspecting the pinned source.
- Edited only the assigned specification and this report. No child agents,
  implementation changes, builds, or Rust/Go/database test executions.

## Verified findings and fixes

1. **The existing empty-metadata fallback description needed its observable
   zero-column traversal consequence.**
   `RawMetadataAndRawRows::deserialize_metadata` selects `mock_empty()` for
   NO_METADATA without a cache (`scylla-cql/src/frame/response/result.rs:891–900`).
   It then reads the nonnegative row count without checking it or the advertised
   column count against the selected metadata (`920–931`). This is not limited
   to VOID responses or zero-row results.

   `RawRowIterator::next` decrements the remaining row count and scans exactly
   the selected column specs (`scylla-cql-core/src/deserialize/result.rs:54–75`).
   `RawRowLendingIterator::next` has the same boundary
   (`scylla-cql/src/deserialize/result.rs:52–89`). With empty specs, neither reads
   a length prefix or advances the row payload. `ColumnIterator::next` stops
   before reading bytes when no spec remains (`core/deserialize/row.rs:71–83`).
   Tuple row type checking uses the selected spec count, and includes the
   zero-field tuple (`core/deserialize/row.rs:243–304`); dynamic `Row` also
   accepts empty specs (`199–231`). Therefore positive advertised counts and
   arbitrary unread row payload do not prevent successful empty-row iteration.
   A destination requiring columns instead fails its metadata type check.

   Added this consequence explicitly under result metadata, and added the
   uncached NO_METADATA/positive-count/unread-payload fixture to the existing
   coverage gaps. The earlier description of cache count disagreement was
   already correct. This is a source-derived clarification, not a reproduced
   server response or a requirement for Go to preserve the weakness.

2. **Unknown auxiliary request-parameter flags were omitted from the error
   boundary.**
   `QueryParameters::deserialize` recognizes flag bits `0x01` through `0x40` and
   rejects the unknown `0x80` bit before parsing values
   (`scylla-cql/src/frame/request/query.rs:19–33,211–232`). QUERY and both
   EXECUTE decoders delegate to this parameter decoder. BATCH defines only
   serial-consistency `0x10` and timestamp `0x20` as accepted flags and rejects
   every other set bit (`request/batch.rs:24–27,233–243`). This differs from the
   already documented ignoring of unknown response-header flag bits.

   Added the exact accepted/rejected request flag boundary and an explicit
   gap for tests of those flag rejections. A source search found `UnknownFlags`
   only in these implementations and its error definition, not a direct test
   assertion. Existing named-values rejection and truncated-byte/unsupported-
   opcode panic caveats remain accurate.

## Whole-section source audit

Paths below are relative to the pinned checkout; `core/` abbreviates
`scylla-cql-core/src/`. These are inspected behaviors, not execution results.

- **Framing, compression, and message inventory:**
  `scylla-cql/src/frame/mod.rs:55–340`, `request/mod.rs`, and
  `response/mod.rs`: v4 direction/version checking, nine-byte header and signed
  stream, checked opcode conversions, unchecked outgoing whole-frame cast,
  incoming allocation without a separate cap, static VOID path, owned body
  reads, distinct I/O stages, tracing/warnings/payload order, unknown header
  flags, no final response-body consumption check, LZ4 size prefix/short-input
  error, raw Snappy, and owned decompression without an expansion budget.
- **Negotiation and parameter layout:**
  `core/frame/protocol_features.rs:38–145`,
  `scylla-cql/src/frame/request/{query,execute,batch}.rs`, and both
  `frame/types.rs` implementations: first numeric extension occurrence versus
  last top-level map entry; presence-based metadata/tablet support; v2 startup
  priority; all-bit/zero-mask LWT matching; independent metadata-ID and tablet
  bytes; old, featureless, and feature-aware EXECUTE differences; option flags,
  LOCAL_QUORUM default, serial consistency, empty paging token, signed page
  size, batch kinds and indexed/count errors. Request `-1`/`-2` handling remains
  distinct from any-negative response `[bytes]` null handling.
- **Handshake/events:**
  `request/{options,startup,prepare,auth_response,register}.rs`,
  `response/{authenticate,supported,event}.rs`, and `server_event_type.rs`:
  empty OPTIONS, CQL `4.0.0` default, map/string/token codecs, old/new event
  surfaces, topology/status recognized names, schema Invalid-kind versus
  rejected-target behavior, and UPDATE_NODES list-count/UUID validation.
- **All wire types and scalar mappings:**
  `core/frame/response/result.rs:74–204,298–383`,
  `core/serialize/value.rs:93–435`, and `core/deserialize/value.rs:67–881`:
  native, collection, tuple, UDT, vector inventories; frozen context; exact
  integer/semantic-type matching; big-endian fixed widths; Boolean nonzero
  truth; ASCII encoding versus decoding; borrowed/copied/shared blobs and
  text; decimal/varint ownership and scale bounds; date/time/timestamp ranges;
  duration's three vints and unchecked suffix; UUID/timeuuid and inet lengths;
  null/unset bypass, special-empty capability, and dynamic empty differences.
- **Aggregate paths:** `core/serialize/value.rs:624–1155` and
  `core/deserialize/value.rs:933–1944`: nested type-check timing, sequence
  compatibility, map key-before-value encoding and framing-before-decoding
  precedence, null collections, counted iteration, shortened tuple fields,
  dynamic UDT name matching/missing/unused/duplicate fields, vector dimensions
  and exact fixed-width classification, unchecked nested width multiplication,
  bulk fallback, exhausted/zero-length element behavior, `nth` skips, surplus
  bytes, and continued lazy iteration after item errors. The bulk vector path
  filters zero element widths before `chunks_exact`; no additional zero-width
  panic finding was inferred.
- **Row/value writers and replay:** `core/serialize/{row,writers,batch}.rs`,
  `scylla-cql/src/serialize/raw_batch.rs`,
  `scylla/src/client/session.rs:1328–1365`, and
  `scylla/src/statement/batch.rs:308–370`: positional/named row checks,
  deterministic unused names and repeated marker reuse, u16 row/batch counts,
  i32 cell/count bounds, metadata-derived reservation before codecs,
  incremental byte rollback, invalid unfinished builder prefix, no-size writer
  limits, count-versus-buffer iteration, iterator cloning, and the prepared
  first-statement-only row cache. A low-level custom writer can still produce
  invalid semantic data; completion proof is not whole-request validation.
- **Derive policies:** `scylla-macros/src/{lib,parser}.rs` and
  `{serialize,deserialize}/{row,value}.rs`, plus `core/_macro_internal.rs`:
  named struct shape, generic/borrowed fields, crate path, default name matching,
  enforce-order/skip-name restrictions, rename/skip/default fields, flattened
  row serialization with its matching internal trait, repeated-name asymmetry,
  strict/excess UDT fields, allow-missing versus null defaults, interior nulls
  versus omitted suffixes, and reading ignored-field framing only when the
  generated traversal reaches it.
- **Metadata, lifetime, and cancellation boundaries:**
  `scylla-cql/src/frame/response/result.rs:525–1054`,
  `core/deserialize/{row,result,frame_slice}.rs`,
  `scylla-cql/src/deserialize/result.rs`,
  `scylla/src/network/connection.rs:145–260,1080–1285`,
  `scylla/src/statement/{prepared,bound}.rs`, and
  `scylla/src/client/pager.rs:714–821,1299–1368`: deferred row cells after eager
  normal-path metadata/count decoding, cache-versus-wire precedence, metadata
  changes, PREPARED paging rejection, unvalidated PK indexes/sequence cast,
  one-snapshot-per-send ownership and unconditional cache replacement,
  synchronous request buffer construction before awaiting submission, retained
  bound values, count-bounded raw/lending scans and nontransactional failures,
  independent frame/metadata lifetimes, shared blob promotion versus copy
  fallback, row-stream lifetime bounds, fresh-page checks, and cancellation
  before a lending page wait consumes a row.
- **Optional/public APIs and copying:** Cargo feature tables of all three
  crates; `core/deserialize/value.rs:804–881,1674–1704,1948–2034`;
  `core/serialize/value.rs:983–1052`;
  `scylla/src/response/query_result.rs:130–140,395–424`; `scylla/src/lib.rs`:
  chrono/time/bigint/decimal/secrecy adapters, full-serialization versus serde,
  unconditional chrono diagnostic use, Box/Arc/Cow behavior, deferred raw cells,
  C++/Node/Python raw-result access, Python element helpers, and the separately
  gated C# lending pager. These were source-inspected, not feature-built.
- **Helpers and error families:** `core/value.rs:219–325,435–490,594–710,772–880`,
  `core/pretty.rs`, `core/utils/safe_format.rs`,
  `scylla-cql/src/{value.rs,utils/parse.rs}`,
  `core/{serialize,deserialize}/mod.rs`, and
  `core/frame/response/error.rs:36–259`: unchanged varint bytes and zero-only
  prefix normalization, exact decimal scale identity, version-masked timeuuid
  comparison/hash, timestamp arithmetic, diagnostic quote/hex/null formatting,
  nonconsuming compatibility decode, character positions, Arc error/downcast
  ownership, database error inventory and fields, standard-code priority over
  negotiated rate-limit collisions, and retained unknown operation/write types.
- **Custom type descriptors:**
  `scylla-cql/src/frame/response/custom_type_parser.rs:55–381` and recursive
  standard type decoding in `response/result.rs`: zero/u16 vector dimensions,
  frozen propagation, hex UDT names and prefix, blank blob fallback, alias and
  whitespace behavior, and the nonadvancing fixed-arity error-iterator fallback.
  The nontermination caveat remains a source trace; no hanging probe was run.

## Catalogue, consistency, and test evidence

- Checked the section's 74 source-catalog entries against its feature coverage
  and exports. The catalogue includes test modules and module/export plumbing;
  these are not additional wire types. Public response serialization is a
  proxy/test construction surface. The inventory does not imply that every
  branch has a test or that every optional feature was compiled.
- A fresh reference check resolved all **79** explicit `path:test_symbol`
  entries to function definitions, and all **61** pinned source links to
  existing files and valid line numbers. This is reference validation only.
- Inspected unit fixtures for LZ4's 0/1/3-byte prefixes; negotiation and EXECUTE
  tablet bytes; prepared metadata round trip; row u16 capacity and custom-codec
  rollback; wrapper propagation and special-empty rejection; scalar error and
  null/empty cases; borrowed/shared/copied blob contents; vector nth, truncation,
  and 100-element capacity assertions; Box/Arc/Cow contents; varint/timeuuid
  identity; secrecy feature gates; native time boundaries; derive missing-field
  policies; raw/lending row boundaries; parser whitespace/errors; unprepared
  and rate-limit fields; and the Counter/CqlDuration serde forwarding fixture.
  Retained the distinction between contents, pointer identity, allocation
  counts, and lifetime compilation. In particular, `test_null_errors` uses an
  incompatible non-null map, and the metadata lifetime fixture allows errors.
- Inspected integration fixture construction and assertions in
  `types/{cql_types,cql_collections,cql_value}.rs`,
  `statements/result_metadata_extension.rs`, and `session/compression.rs`.
  Confirmed adapter feature gates; dynamic empty-int cases; null optional
  collection destinations; shortened tuples; 26 vector fixtures without UDT or
  counter elements; dynamic UDT/duration cases; metadata-ID early returns and
  proxy-simulated mixed support; and the six request-size compression cases.
  The compression test does not measure response wire sizes, and accepted
  proxy DriverDisconnected shutdown remains distinguished from full coverage.
- The round-03 core-only build/test statement is retained as an attributed
  historical result. This round did not rerun or independently reproduce it.
  No integration success, optional-feature build, Go behavior, race safety,
  universal zero-copy proof, or hostile-allocation bound is asserted here.
- Both newly clarified behaviors have explicit missing-fixture entries. Existing
  concurrency, malformed-input, skipped-element, metadata/cache, optional raw
  API, and Go validation gaps remain visible. No test was added for a doc-only
  clarification.
- Cross-checked the execution section's NO_METADATA paragraph and communicated
  the source trace to the parent. It now states the same empty-column fallback
  and unread-payload consequence. Request execution, tablet hint construction,
  warning/payload retention, and transport cancellation remain delegated to
  their linked sections; no other agent's file was edited.
- `git diff --check -- feature-specs/serialization-protocol.md` passed after
  the edits. Local relative-link and final hash checks are documentation checks,
  not runtime validation.

No additional verified correction emerged from this audit. The two fixes record
the pinned implementation's acceptance/rejection boundaries without converting
them into a requirement to reproduce avoidable weaknesses in Go.
