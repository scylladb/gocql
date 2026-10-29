# Serialization/protocol review — round 04

- Section: `feature-specs/serialization-protocol.md`
- Round: 04 — ownership, lifetimes, concurrency, cancellation, and snapshots;
  whole-section audit
- Agent: `/root/review_serialization_04`
- Reference checkout: `/extra/scylladb/gocql-rust-reference-f8b0ee9`
- Source commit: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`
- Before SHA-256: `c2d11951cebf216e8046e0daa652a0270d96243ca6d3f781c4f16360e5b34477`
- After SHA-256: `50882c6690ce1d2eff215e59dec53c468fd2b8f81a92ca72c827b43e7d4a43ae`

Read the review instructions, section README, and complete assigned section.
Verified the reference HEAD independently. Earlier reports were context, not
source evidence. Only this report and the assigned specification were edited;
no Go/Rust implementation changes and no child agents were used.

## Source inspected

Paths and line numbers below refer to the pinned reference checkout. Inspection
covered the source paths relevant to the entire section, with deeper tracing
through the callers/callees that establish ownership and async boundaries.

| Files | Symbols and paths checked |
| --- | --- |
| `scylla-cql/src/frame/mod.rs:58–349` | `SerializedRequest::make`, complete-frame ownership, stream assignment, version/direction checks, static VOID body, response extensions, LZ4/Snappy |
| `scylla-cql-core/src/frame/protocol_features.rs:72–168` | Numeric parsing, first matching field, presence-only flags, tablet-v2 preference, LWT-mask matching |
| `scylla-cql/src/frame/request/{mod,options,startup,auth_response,query,execute,batch}.rs` | Request inventory, handshake/token framing, `QueryParameters`, `ExecuteV2`, count/error paths, batch row-codec invocation |
| `scylla-cql/src/frame/response/{supported,event,custom_type_parser,result}.rs` | SUPPORTED map, event dispatch/validation, custom-type grammar/frozen state, prepared and row metadata |
| `scylla-cql/src/frame/response/result.rs:158–443,769–975` | `ResultMetadataHolder`, `RawMetadataAndRawRows`, `SelfBorrowedMetadataContainer`, `BytesWrapper`, `Yoke`, NO_METADATA branches, row-count parse, partition-key ordering |
| `scylla-cql-core/src/frame/{types,response/result,response/error}.rs` | Consistency/length/null/unset primitives, type inventory, vector-width classification, structured database errors and rate-limit precedence |
| `scylla-cql-core/src/serialize/{mod,row,value,writers,batch}.rs`, `scylla-cql/src/serialize/raw_batch.rs` | Custom codec/error traits, exclusive writer borrows, completion proof, rollback, aggregate framing, dynamic UDT, replayable providers |
| `scylla-cql-core/src/deserialize/README.md`, `deserialize/{mod,frame_slice,result,value}.rs` | Separate frame/metadata lifetimes, shared-owner promotion, raw/typed iteration, scalar and aggregate checks, null/missing/empty distinctions, error ownership |
| `scylla-cql/src/deserialize/result.rs:19–112` | `RawRowLendingIterator::next`, owner borrow, cursor and count progression |
| `scylla/src/network/connection.rs:138–225,996–1221,1459–1530` | Request construction before queue wait, orphan notifier, metadata parameter snapshot, reprepare snapshot reload, response parsing |
| `scylla/src/statement/prepared.rs:202–245,569–603,680–691`, `statement/bound.rs:20–33` | Shared prepared state, `ArcSwap`, `ColumnSpecsGuard`, owned bound values |
| `scylla/src/client/session.rs:906–933,1048–1075`, `client/pager.rs:701–827,1270–1373`, `response/query_result.rs:315–390` | Serialization before execution await, lending-page wait, stable owned-row stream, per-page type checks, result-bound borrowed rows |
| `scylla-macros/src/{serialize,deserialize}/{row,value}.rs` | Attribute restrictions, flatten trait generation, name/order matching, omitted suffixes, missing/null defaults, ignored-field framing |
| `scylla-cql-core/Cargo.toml`, `src/value.rs:773–940`, `src/utils/safe_format.rs`, `scylla-cql/src/utils/parse.rs` | Optional adapter gates, unconditional chrono dependency, timestamp helpers, cloned diagnostic iterators, grammar error positions |
| Named unit/integration test locations in the spec | All 66 referenced test/function declarations located; focused test bodies inspected as detailed below |

## Verified findings and fixes

### 1. Request ownership and cancellation boundaries were omitted

`Session::execute_unpaged` serializes to `SerializedValues` before its first
execution await (`session.rs:906–915`); the manual-page variant does the same
at lines 1048–1056. `BoundStatement::new` stores the serialized values and
prepared handle (`bound.rs:25–33`). The row storage is an owned `Vec<u8>`
(`serialize/row.rs:493–496`).

`RouterHandle::send_request` constructs a complete `SerializedRequest` before
creating the task and awaiting queue submission (`connection.rs:138–167`).
The task owns the request, whose bytes are an owned vector (`frame/mod.rs:58–60`).
Compression is also synchronous within `SerializedRequest::make`. These calls
have no yield point inside the custom codec invocation. The orphan notifier
is an independent request-ID cleanup mechanism, not a borrowed-data lifetime
mechanism (`connection.rs:195–225`).

Added the owned value/request snapshots, binding reuse, and synchronous codec
boundary. Cancellation cannot revoke an already-submitted buffer, nor does this
path promise server-side cancellation. Existing batch behavior remains distinct:
batch codecs are invoked on sends, with only the documented first-row reuse.

### 2. Metadata snapshot safety was conflated with cache freshness by omission

`execute_raw_with_consistency` loads one `Arc` at `connection.rs:1125` and
derives both outgoing metadata parameters and the cached response metadata from
that snapshot (`1032–1102,1125–1165`). `send_request` retains the reference
across the await and passes it to `parse_response` (`1459–1519`); raw row
parsing clones the `Arc` (`response/result.rs:975–979`). Reprepare reloads the
snapshot before the second send (`connection.rs:1190–1209`).

The statement's shared cache is an `ArcSwap` (`prepared.rs:212–219`). Updates
use a plain `store` (`569–580`), following a separately loaded ID/count check
(`connection.rs:996–1029`). There is no conditional atomic replacement or
schema-version ordering. This preserves each response's selected metadata,
but does not establish that concurrent updates leave the newest metadata in
the shared statement cache.

Added the per-send snapshot and reprepare reload rules, immutable result
metadata, and cache-ordering limitation. Flagged the execution-section overlap
to the parent; that section's reviewer independently traced the same race and
is documenting the cache policy. No other section was edited here.

### 3. Metadata laziness needed an explicit API boundary

`RawMetadataAndRawRows::deserialize` only consumes the initial fields
(`response/result.rs:769–827`), and `deserialize_metadata` later consumes
metadata and row count (`877–934`). However, normal `Connection::parse_response`
immediately calls `.deserialize_metadata()` before constructing `QueryResponse`
(`connection.rs:1511–1521`). Thus a caller's ordinary query result already has
metadata and row count decoded even though cells remain serialized.

Qualified the statement about initially lazy ROWS parsing so it describes
the low-level representation separately from the normal connection API.

### 4. Paged-stream borrowing and per-page type checks were missing

`QueryPager::rows_stream` requires a row type implementing `DeserializeRow`
for all frame and metadata lifetimes (`pager.rs:821–826`); ordinary borrowed
fields cannot satisfy that bound. Owned `Bytes` decoding does qualify through
`FrameSlice::to_bytes` (`deserialize/value.rs:476–484`,
`deserialize/frame_slice.rs:130–142`).

The lending pager awaits `poll_fill_page` before consuming a row
(`pager.rs:724–738`), and page replacement happens through owned
`RawRowLendingIterator` values (`787`). `TypedRowStream` checks the initial
metadata at construction (`1303–1311`) and checks fresh-page metadata before
typed decoding (`1347–1370`). Previously returned independent `Bytes` owners
retain their allocations across that page replacement.

Added stable stream ownership restrictions, shared-owner eligibility, per-page
type checks, and the row-consumption cancellation boundary. This does not
claim that all paged work is canceled when the consumer is dropped; paging
policy belongs to execution.

### 5. Internal metadata and diagnostic lifetimes needed qualification

`SelfBorrowedMetadataContainer` stores a `Yoke` over `BytesWrapper`; the
wrapper's `StableDeref` and `CloneableCart` implementations explicitly rely on
stable immutable `Bytes` storage (`response/result.rs:267–294`). Public
`metadata()` references come from `Yoke::get` and are bounded by the container
borrow (`308–328`). `ColumnSpecsGuard` retains a loaded cache snapshot
(`prepared.rs:234–245,599–603`).

Codec error wrappers require `Error + Send + Sync + 'static` and keep it in an
`Arc`; mutable downcasts use `Arc::get_mut` (`serialize/mod.rs:31–55`,
`deserialize/mod.rs:34–92`). Thus custom errors cannot borrow request/result
storage, and sharing an error does not permit mutating its payload through the
wrapper.

Added these Rust-specific lifetime/ownership facts without treating the
internal `'static` type parameter as a public permanent-storage guarantee.

## Checked behaviors retained without findings

- Version-4 framing, direction/version checks, stream assignment, whole-body
  reads, VOID optimization, extension ordering, absent compression errors,
  decompression ownership, and unchecked whole-frame length/allocation limits.
- Extension numeric/key-presence rules, first-match behavior, zero LWT mask,
  tablet-v2 priority, and metadata-ID/tablet layout independence.
- Query defaults and flags; ordinary versus serial consistency; UTF-8 byte
  lengths; named-row ordering; unsigned-16-bit counts; batch count errors;
  incremental rollback versus partial low-level writes.
- Strict scalar type/length rules, native time range checks, arbitrary-integer
  and decimal borrowing, dynamic empty handling, collection null handling,
  tuple suffix omission, dynamic UDT names, and missing-field distinctions.
- List/map/vector count bounds, element-type checks, allocation reservations,
  fixed-vector fast path and exhaustion quirks, raw-row partial consumption,
  collected first-error behavior, and absence of universal trailing-byte checks.
- Derive name/order/rename/default/flatten policies and ignored UDT field
  framing. Safe custom deserializers cannot justify unsafe behavior merely by
  assuming their type check was previously called.
- NO_METADATA cache/empty selection, newly supplied metadata precedence,
  parse-error propagation, inconsistent count handling, partition-key index
  sorting/casts, and special metadata flag constraints.
- Immutable frame borrowing, deferred versus fully decoded metadata lifetimes,
  owner-promotion copy fallback, lending iterator exclusion of simultaneous
  advancement, timestamp arithmetic, diagnostic helper behavior, feature gates,
  and distinct error categories.

## Test claims and validation

- A read-only declaration check found all **66** named test/function targets in
  the current specification. This establishes their existence, not execution
  or full semantic coverage.
- Read `test_deserialize_bytes` (`deserialize/value_tests.rs:33–64`): it checks
  equal values for borrowed, copied, and shared blobs, plus blob round trips.
  It does not assert pointer identity or survival after dropping a result.
- Read `metadata_does_not_bound_deserialized_values` (`2325–2393`): its purpose
  is compilation of results outside the metadata scope, explicitly allowing
  decoding errors. Qualified the spec's test description accordingly.
- Read `FrameSlice` tests (`frame_slice.rs:284–331`): the owned-cell test checks
  the original owner reference and contents, not a concurrent drop/reuse case.
- Read raw and typed iterator tests in `deserialize/result.rs` and lending
  iterator tests in `scylla-cql/src/deserialize/result.rs`: they exercise basic
  rows, short rows, and type mismatch, not cancellation or concurrent retention.
- Read vector bulk/truncation and capacity assertions
  (`deserialize/value_tests.rs:1108–1200`): the existing narrowly worded claims
  match their assertions. No allocation instrumentation is asserted there.
- Read metadata-ID integration test setup/update flow and unsupported-feature
  early-return branches in `result_metadata_extension.rs`. They do not establish
  concurrent cache-store ordering or an in-flight snapshot stress test.
- Added explicit coverage gaps for concurrent metadata replacement, retained
  owners across cancellation/page replacement, and pending lending-page waits.
  Existing hostile-frame, optional-feature, database, and Go ownership/race
  gaps remain explicit.
- No Rust tests, Go tests, or database tests were run in this round. The prior
  recorded Rust 1.94.1 no-default-features core build and 228 passing tests were
  not repeated; this audit identified no new build concern requiring it.
- Checked the final specification/report for trailing whitespace and verified
  local Markdown links plus pinned source paths/line-anchor ranges. The
  specification's recorded after hash was computed after its final edit.

Result: five source-backed ownership/API clarifications, narrower borrowing-test
claims, and explicit missing concurrency coverage. No implementation changes.
