# Protocol, types, serialization, and borrowing

## Summary

The driver translates typed values and rows to CQL native-protocol requests and
exposes typed, dynamic, and partially decoded views of responses. It validates
the requested destination types against server metadata, distinguishes null,
unset, and zero-length values, and supports values borrowing the response frame.
This is a specification of the pinned Rust implementation, including its limits;
it is not a claim that all its encoding or decoding paths are zero-copy.

Reference revision: `f8b0ee928dc0dcb475247dc82cce4e42804b0621`. Request execution,
preparation, reprepare, and paging policy are specified in [execution](execution.md);
socket lifetime and multiplexing in [connections](connections.md); authentication
and compression configuration in
[configuration, security, and observability](configuration-security-observability.md).

## Language-agnostic behavior

### Native protocol and message framing

- The implementation emits native protocol **version 4**. It accepts server frames
  only when the direction bit is set and the remaining version bits equal 4.
  It does not implement native-v5 segmented framing or negotiate a lower native
  version after rejection. Scylla extensions add specific capabilities to v4.
- The nine-byte header contains version, flags, signed 16-bit stream identifier,
  opcode, and unsigned 32-bit body length, in network byte order. The stream is
  inserted after serialization, immediately before dispatch. Response opcodes are
  validated; unknown opcodes and wrong direction/version return parsing errors.
- Client messages are STARTUP, OPTIONS, QUERY, PREPARE, EXECUTE, REGISTER, BATCH,
  and AUTH_RESPONSE. Responses are ERROR, READY, AUTHENTICATE, SUPPORTED, RESULT,
  EVENT, AUTH_CHALLENGE, and AUTH_SUCCESS. RESULT distinguishes VOID, ROWS,
  SET_KEYSPACE, PREPARED, and SCHEMA_CHANGE. Authentication conversation, event
  dispatch, and connection establishment are separate features.
- A response body is read completely into owned memory before higher-level
  parsing. Early EOF and body/header I/O failures retain their parsing stage.
  The implementation currently has no explicit maximum incoming-frame-size
  guard before allocating for the advertised length. This is an observed limit,
  not a recommended acceptance rule for a new implementation. Outgoing frame
  construction casts the final body length to unsigned 32 bits without a checked
  conversion; individual cell limits do not establish a checked whole-frame limit.
- Ordinary four-byte VOID RESULT bodies use a static buffer after checking the
  bytes. A same-size non-VOID body is copied into owned storage. This optimization
  does not change response classification or bypass normal extension processing.
- Body flags identify compression, tracing, custom payload, and warnings. A
  compressed body without a configured compression algorithm is an error.
  After decompression, this implementation consumes tracing ID, then warnings,
  then custom payload, before leaving the main response body for its parser.
  The ordering here records the source implementation, not a generalized
  ordering rule for other protocol implementations.
- Unknown response-header flag bits are ignored. Response-body dispatch does
  not require full consumption: READY ignores its body, and VOID ignores bytes
  after the result-kind integer. Other ordinary response parsers likewise do
  not add a final end-of-body check. A successful parse therefore need not
  establish that every advertised body byte belongs to the decoded message.
- Tracing IDs are UUIDs; warnings are owned strings; a response custom payload
  is a string-to-byte-buffer map. Outbound request construction exposes a tracing
  flag but does not expose a symmetric arbitrary outgoing custom-payload field.
  Decoding a response payload does not expose it on the ordinary query-result
  API: EXECUTE processing can consume tablet feedback from it, and conversion
  to a non-error query response discards the payload. Warning and tracing-ID
  retention differs between ordinary results, errors, and automatic paging;
  see [configuration and observability](configuration-security-observability.md).

### Compression and negotiated extensions

- LZ4 and Snappy operate on the frame body, leaving the header uncompressed.
  LZ4 prepends a four-byte big-endian uncompressed size to the compressed block.
  A shorter-than-four-byte LZ4 body is rejected before reading that size.
  Snappy uses its raw block format. Decompression produces a new owned buffer;
  borrowed result values subsequently refer to that decompressed buffer.
  The driver imposes no separate decompressed-size or expansion-ratio limit;
  the LZ4 size prefix is passed to the decompressor without such a bound.
- OPTIONS/SUPPORTED and STARTUP establish per-connection extension support.
  Recognized numeric extension fields that are absent or fail to parse are
  treated as unsupported; the parser does not make them a connection error.
  It uses the first exact `KEY=` entry, so a malformed first value is not
  rescued by a later duplicate in that list. Before feature parsing, duplicate
  top-level SUPPORTED keys replace their entire earlier value list with the
  last wire entry. These are distinct duplicate-handling rules.
  Tablet-v1, tablet-v2, and metadata-ID support
  depend on key presence, even with an empty advertised value list.
  Extension support must not be inferred solely from the server product name.
- The rate-limit extension supplies a server-selected error code. The LWT
  metadata extension supplies a bit mask, and a prepared statement is marked as
  LWT only when all bits of that mask occur in its prepared-metadata flags.
  A negotiated zero mask therefore matches every prepared flag value; without
  negotiation, no LWT mark is inferred by this parser.
- Tablet routing v2 supersedes v1: if the server advertises both, STARTUP enables
  only v2. Every EXECUTE on a v2 connection includes exactly one trailing tablet
  version-block byte after its ordinary query parameters. A non-v2 EXECUTE does
  not include or consume that byte. Block construction and response tablet hints
  belong to [routing](routing.md).
- The metadata-ID extension adds a result-metadata ID to PREPARED and EXECUTE,
  and permits ROWS to report changed metadata with a new ID. It is independent
  of tablet-v2 negotiation. Parsing must use the negotiated connection features
  so extension bytes are not mistaken for ordinary query parameters or row data.

### Handshake and event payload codecs

- OPTIONS has an empty body. SUPPORTED is a string-to-list-of-strings map;
  STARTUP is a string-to-string map. The CQL language version option defaults
  to `4.0.0`, which is distinct from the native frame version number. PREPARE
  contains a length-prefixed query string. AUTHENTICATE names the requested
  authenticator; challenge, success, and response tokens are optional byte
  strings whose contents belong to the authentication provider.
- REGISTER carries a list of event type names. The current event surface
  includes TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE, and CLIENT_ROUTES_CHANGE;
  the older event/register types support only the first three. Unknown event
  type names fail parsing.
- Topology changes recognize NEW_NODE and REMOVED_NODE with a socket address;
  status changes recognize UP and DOWN with a socket address. Unknown change
  names in those families fail. Schema changes carry CREATED, UPDATED, or
  DROPPED and a KEYSPACE, TABLE, TYPE, FUNCTION, or AGGREGATE target. Function and
  aggregate targets also carry argument-type names. Unknown schema change kinds
  become an explicit Invalid value, while unknown schema target kinds fail.
- CLIENT_ROUTES_CHANGE currently accepts UPDATE_NODES. It carries paired lists
  of connection-ID strings and host-ID UUID strings; unequal counts and invalid
  UUIDs fail parsing. Event decoding alone does not update topology or route
  state; those consumers are specified in metadata/control/events and configuration.

### Parameters, rows, and batch encoding

- Query parameters contain ordinary consistency and optional values,
  metadata-skipping, page size, paging state, serial consistency, and timestamp.
  The protocol-level default is LOCAL_QUORUM, empty values, no page size, initial
  paging state, no timestamp, no serial-consistency override, and metadata
  included. Effective session/statement defaults are specified in execution.
- Supported ordinary consistency values are ANY, ONE, TWO, THREE, QUORUM, ALL,
  LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE, SERIAL, and LOCAL_SERIAL. The distinct
  serial-consistency parameter accepts only SERIAL or LOCAL_SERIAL. Unknown
  consistency numbers produce conversion errors.
- The presence of an option controls its wire flag. A present but zero-length
  paging token is distinguishable from no token. Values are emitted in server
  bind-marker order. Named application values are reordered client-side; outgoing
  query serialization does not set the protocol named-values flag. The auxiliary
  request decoder rejects named-values frames as unsupported.
  Auxiliary QUERY/EXECUTE parameter decoding also rejects the unknown `0x80`
  flag bit; BATCH decoding accepts only its serial-consistency (`0x10`) and
  timestamp (`0x20`) flags and rejects any other bits.
  The low-level parameter codec writes any signed 32-bit page size verbatim;
  positive-page-size validation belongs to the higher-level statement API.
- Type-aware row serialization requires column/bind-marker metadata. Empty rows
  only match a statement with no markers. Positional rows require the matching
  number of fields. Named maps require every requested marker and reject unused
  names; repeated marker names can reuse the same map value. The first unused
  name reported is lexicographically smallest for deterministic diagnostics.
- A value is framed by signed 32-bit length followed by its bytes. `-1` writes
  null, `-2` writes unset, and `0` writes a present zero-length value. Maximum
  ordinary cell payload is `2^31 - 1` bytes. The row/batch value count and batch
  statement count each fit an unsigned 16-bit integer. Collection counts fit a
  nonnegative signed 32-bit integer; overflow fails serialization.
  A low-level row writer can exceed 65,535 values; complete-row construction
  and batch serialization check the count after the row codec returns. The
  incremental `add_value` API checks that limit before invoking the cell codec.
- Protocol strings, prepared/metadata IDs, and string-list/map entry counts use
  unsigned 16-bit lengths/counts. Query/PREPARE text and ordinary byte strings
  use nonnegative signed 32-bit lengths. String lengths count UTF-8 bytes, not
  characters; outgoing conversions to these length fields are checked.
- Batches carry a value row for every statement, including empty rows. They may
  mix query text and prepared IDs. Too few or too many rows, too many statements,
  or too many values in a row are distinct encoding failures with the statement
  index where applicable. Batch type, consistency, optional serial consistency,
  and optional timestamp are encoded separately from each statement's values.
  Batch kinds are LOGGED (0), UNLOGGED (1), and COUNTER (2); the auxiliary request
  decoder rejects other kind bytes.
- Serialization errors may occur after bytes were appended to a low-level
  writer. A failed complete request must not be sent. The incremental
  pre-serialized-row `add_value` operation restores the previous byte length and
  leaves its value count unchanged on failure; that rollback promise does not
  apply to every exposed low-level writer.
- Single prepared executions serialize their values into owned row storage
  before awaiting execution; binding a prepared statement retains that storage
  for reuse. Complete request framing and compression also finish synchronously
  into an owned buffer before awaiting the router's submission queue. The queued
  bytes do not borrow application values. Dropping an async request cannot
  interrupt a synchronous codec invocation during its poll, and cancellation
  after submission does not revoke the router's buffer or guarantee server-side
  cancellation; stream cleanup is specified in [connections](connections.md).
- Replayable batch-value providers can avoid constructing a separate collection
  of rows. Each traversal obtains a new iterator; the iterator-backed adapter
  does this by cloning its stored iterator, which should be inexpensive and
  reproduce the same values. Counting can consume an iterator, and each retry
  obtains another. Batch serialization invokes row codecs on each send, except
  that the session caches the row at statement index zero when that statement
  was already prepared and its value row exists. A prepared statement later in
  the batch, or preparing the first statement during a connection attempt,
  does not activate this cache. The driver does not pre-serialize and cache every
  batch row for retries. Batch routing uses the same first-statement boundary,
  as specified in [execution](execution.md#batches-and-their-routing-inputs).

### CQL types and scalar conversions

The following wire-type inventory is complete for the pinned column-type enum.
Metadata type matching is strict; integer widths and special semantic integer
types are not implicitly interchanged. Unless stated otherwise, a non-nullable
scalar rejects null, and fixed-size scalars reject the wrong byte length.

| CQL type | Representation and observable rules |
| --- | --- |
| `ascii`, `text` / `varchar` | Byte text, decoded as validated ASCII or UTF-8. Both accept language string values for encoding. Encoding to ASCII does not itself reject non-ASCII string bytes; server validation may reject them. Borrowed and owned decoding are available. |
| `blob` | Arbitrary bytes including empty bytes; borrowed, shared-owner, and copied representations are available. No text validation. |
| `boolean` | One byte; encoding emits 0 or 1, decoding treats any nonzero byte as true. |
| `tinyint`, `smallint`, `int`, `bigint` | Signed 8-, 16-, 32-, and 64-bit big-endian integers respectively. |
| `float`, `double` | Big-endian 32- and 64-bit IEEE-754 values. |
| `counter` | Dedicated signed 64-bit counter representation; not interchangeable with ordinary bigint during type checking. |
| `varint` | Arbitrary-width signed big-endian two's-complement bytes, with owned and borrowed native wrappers and optional big-integer adapters. |
| `decimal` | Signed 32-bit scale followed by a variable-width signed unscaled integer; owned and borrowed wrappers. External decimal conversion rejects a scale outside signed 32-bit range when serializing. |
| `date` | Unsigned 32-bit day count offset by `2^31` from the Unix epoch; a native wrapper preserves the entire wire domain. Calendar-library conversions may overflow. |
| `time` | Signed 64-bit nanoseconds since midnight. Native and external destination decoders require `0..=86,399,999,999,999`; out-of-range values fail even for the native wrapper. Native wrapper serialization writes its stored integer without this range check. External time adapters also validate representability. |
| `timestamp` | Signed 64-bit milliseconds since Unix epoch; native wrapper preserves full wire range. Calendar adapters return overflow errors for values outside their supported range. |
| `duration` | Three signed variable integers: months, days, nanoseconds. Months and days must decode into signed 32-bit integers. The decoder ignores bytes after those three integers; it does not promise server-level sign/range or canonical variable-integer validation. |
| `uuid` | Exactly 16 bytes, using a UUID type. |
| `timeuuid` | Exactly 16 bytes, using a distinct wrapper and database-compatible ordering. The codec does not itself validate a version-1 UUID. |
| `inet` | Exactly four bytes for IPv4 or sixteen for IPv6; other lengths fail. |
| `list<T>`, `set<T>` | Count followed by length-framed typed elements. List/set contents can be read lazily or collected. |
| `map<K,V>` | Count followed by alternating length-framed keys and values; lazy and collected forms. |
| `tuple<T...>` | Length-framed heterogeneous fields in declared order; no separate element count in the value. |
| user-defined type | Length-framed named fields in metadata order, with keyspace/name/field metadata and schema-evolution handling. |
| `vector<T,N>` | Exactly the metadata-declared number of homogeneous elements when serializing; special fixed/variable element formats described below. |

Collection and UDT metadata can carry a frozen flag. Frozen metadata does not
switch these codecs to an opaque blob codec. Standard native type descriptors
for collection/UDT values do not encode frozen status, while custom type parsing
can propagate a frozen context to nested types.

### Null, unset, empty, and omitted fields

- Optional values distinguish null from a value. Serializing null bypasses the
  wrapped value's type-specific encoder. Deserialization still type-checks the
  wrapped destination against metadata even if a particular cell is null.
- Unset means a bound value is not supplied, and is represented separately from
  null, including an optional “set versus unset” wrapper. It is an outbound
  binding state, not a normal row-result value. The low-level request-value
  reader recognizes only `-1`, `-2`, or nonnegative lengths; more negative lengths
  fail. In contrast, the low-level response `[bytes]` reader treats *any* negative
  length as null. The spec must not claim stricter response validation than this.
- Zero-length text/blob is an ordinary value. For supported other types a
  dedicated empty-value wrapper distinguishes the server's special empty value
  from zero, null, and unset. The empty-value serialization capability check
  rejects counters, durations, collections, and UDTs; it permits other column
  types. Only host types implementing the empty-value capability can use the
  typed wrapper. Some database versions reject more cases than this check.
- Dynamic value decoding accepts a present zero-length payload as the dynamic
  `Empty` variant for every type other than ASCII/text/blob. Thus dynamic and
  typed decoding do not have identical empty-payload validation. Dynamic empty
  serialization still uses the capability check above.
- List/set/map destination decoders interpret a null collection as an empty
  collection. Wrapping the destination in an optional type preserves null as
  absent instead. An empty collection value normally contains a zero count;
  a zero-byte non-null collection payload is malformed for typed collection
  decoders. Vector decoding rejects null rather than applying the collection rule.
- For tuples, a serialized suffix may be absent after earlier fields. Typed and
  dynamic decoders treat those absent fields as null; non-nullable destinations
  then fail. Typed tuple destination arity must match metadata exactly.
  Serialization allows a tuple with fewer fields than the metadata and omits
  its suffix; too many source fields fail.
- A UDT field can be absent from the *definition*, absent from the *serialized
  value*, present with null, or present with bytes. The low-level UDT iterator
  exposes absent bytes separately from explicit null. Generated UDT codecs have
  separate missing-definition and null/default policies, described below.

### Collections, vectors, dynamic values, and user codecs

- Element/key/value metadata is checked before iteration. Lazy collection and
  map iterators avoid materializing the collection, but still validate each
  requested element's bytes. Collected vectors/lists reserve their declared
  count; hash maps/sets reserve capacity. This is not a no-allocation guarantee.
  These up-front nested type checks describe typed decoding. Encoding checks
  the outer aggregate type and validates nested types as values are serialized;
  an empty sequence or map therefore does not invoke its element/key/value
  codecs, and null/unset elements can bypass those nested checks. A map encodes
  each key before its value. Named-row and dynamic-UDT encoders report leftover
  names only after processing the requested metadata fields, so an earlier
  missing-field or cell error takes precedence over an unused-name error.
- A list or set can decode to a sequence. A set can decode to an ordered/hash
  set; a list cannot directly decode to a set because that could discard order
  and duplicates. Maps can decode to ordered/hash maps. Collection encoders do
  not independently enforce all server semantic restrictions on null/unset
  elements; element codecs determine what bytes they write.
- A vector's dimension is an unsigned 16-bit metadata value; the custom-type
  parser accepts zero as well as 1 through 65,535. The wire value has
  no element-count prefix. Boolean, float, double, int, bigint, timestamp, UUID,
  and timeuuid elements use contiguous fixed-width encodings. A nested vector
  also uses fixed-width encoding when its own element type has a fixed vector
  width; its width is that element width multiplied by its dimension. Other
  native types (including tinyint, smallint, date, time, and inet), collections,
  tuples, UDTs, and vectors with variable-width elements use an unsigned
  variable-integer byte length before each encoded element.
  Nested fixed-width dimension products use unchecked platform-sized arithmetic;
  sufficiently large nested metadata can panic with overflow checks enabled or
  wrap without them. The unsigned-16-bit dimension limit does not bound the
  aggregate byte width. Fixed-width `nth` skip-size multiplication has the same
  unchecked-arithmetic boundary.
- Serialization checks vector length equals its dimension. Fixed-width vector
  decoding can validate and process a contiguous run in bulk, then use ordinary
  element iteration for the remainder. A nonempty remainder shorter than the
  required element length fails; complete exhaustion is passed to the element
  decoder as null and may succeed for a nullable element destination. After a
  variable-length prefix, even a zero-length element is passed as null if no
  bytes remain, but as a present empty slice if later bytes remain. These are
  implementation quirks, not server validity guarantees. Errors distinguish
  element decoding from length/framing failures; the variable-width element
  error currently records the element type rather than the enclosing vector.
  Vector iteration is bounded by the metadata dimension. List/set/map iteration
  is bounded by the wire count; tuple and UDT decoding is bounded by metadata
  fields. These paths ignore surplus bytes after the expected elements/fields,
  even when the surplus is malformed. Ordinary row iterators likewise stop at
  the advertised row count without checking for surplus row bytes.
- Lazy list/set/map iteration is not terminated by an item error. A fully framed
  item whose value decoder fails has already been consumed, so later items can
  still decode. A map reads both key and value frames before decoding either;
  a value-framing failure therefore takes precedence over a key-decoding
  failure. A failed length-framed read leaves its bytes in place while consuming
  an expected-item slot, so continued iteration repeats a framing failure until
  the declared count is exhausted. Variable-width vectors differ: their length
  prefix is consumed separately and is not rolled back on a subsequent failure.
  Collected destination codecs stop at the first error and return no partial
  collection.
- Vector iteration also supports skipping to the nth remaining element. With
  fixed-width elements it advances over skipped bytes without decoding them;
  skipping at or beyond the remaining count exhausts the iterator without
  inspecting the payload. With variable-width elements it iterates and discards
  skipped results, including their errors. Thus a successful selected element
  does not establish the validity of skipped elements.
- The dynamic value representation supports every column type and retains UDT
  keyspace, name, and field names. It owns its variable-sized data. Dynamic UDT
  serialization requires matching keyspace/name, emits missing fields as null,
  rejects unknown source field names, and uses the lexicographically first
  unknown name in its error. It is distinct from generated structural UDT
  codecs, which match field layout rather than enforcing that dynamic name pair.
  Duplicate source field names in a dynamic UDT are collapsed with the last
  value winning; the selected value is removed after its first matching metadata
  field, so a repeated metadata field name then receives null.
  Dynamic ASCII/text variants both delegate to the string encoder, and dynamic
  list/set/vector variants all delegate to the sequence encoder. Consequently
  these source variant names do not require the identically named destination:
  metadata selects the sequence layout, including the vector dimension check.
- Users can implement a single-cell codec or a row codec. Encoding receives
  actual server type metadata and a cell framing writer; decoding splits
  type compatibility checks from byte interpretation. Invalid user codec output
  can still be rejected by the server: framing validity is not full semantic
  validation. A custom decoder must remain memory-safe even though ordinary
  driver calls perform its type check first.

### Generated row and UDT codecs

- Generated codecs support named-field structs, including generics and borrowed
  fields. Default matching is by field name, independently of declaration order;
  explicit ordering mode requires the server's order. An explicit switch may
  omit name checks only in ordering mode; type checks remain.
- Fields can be renamed or skipped. On decoding, skipped fields receive their
  language default. Null-default fields receive their language default for null.
  Query row names are the selected result names, including aliases, rather than
  necessarily table column names. Ordinary generated row codecs require the
  matching fields. Name-matching row serialization can repeat a source field
  for repeated bind-marker names; name-matching row deserialization instead
  rejects duplicate selected names that map to a destination field. Generated
  name-matching UDT codecs have the same serialization-reuse versus
  deserialization-rejection distinction for duplicate metadata field names.
- Generated row serialization also supports flattening another row-codec field
  into multiple parent columns. A flattened field has no own column name and
  cannot be renamed. In Rust the nested field must implement the matching
  flavor's internal row trait (`SerializeRowByName` or `SerializeRowInOrder`),
  normally supplied by a derived struct using that same flavor; an arbitrary
  `SerializeRow` implementation alone is insufficient. This capability must not
  be assumed for the other three derive macros merely because row serialization
  supports it.
- UDT serialization allows extra database fields by default: name matching
  fills missing interior fields with null and omits a missing suffix; ordered
  matching accepts only a missing suffix. An explicit strictness option rejects
  all extra database fields. Source fields without a target UDT field fail
  unless marked `allow_missing`, in which case serialization ignores them. In
  ordered matching with name checks, such a field can be skipped without
  consuming the next database field.
- UDT decoding ignores extra database fields by default (anywhere in name
  matching, suffix only in ordered matching); the strictness option rejects
  them. A field absent from the UDT definition is an error unless explicitly
  allowed and defaulted. A field present in metadata but missing from serialized
  bytes is treated through the null path, so it needs a nullable destination or
  a null-default annotation. “Allow missing” alone does not mean “default null.”
  Name-matching decoding still reads the framing of ignored database fields
  and rejects malformed framing, without decoding their values. Ordered decoding
  stops after its own fields and need not inspect an ignored database suffix.
- Invalid macro attributes, unsupported shapes, and conflicting field mappings
  fail code generation. With skipped name checks, renaming is invalid and
  allowed-missing UDT fields must be a suffix of the non-skipped fields.

### Result metadata, type checking, and ownership

- ROWS initially parses flags, column count, and paging state while leaving
  metadata and row bodies serialized. The low-level response API can retain
  this state, but the normal connection response path decodes metadata and row
  count before returning a query result. Cell decoding remains lazy. Row and
  column counts are nonnegative signed 32-bit values;
  native tuple/UDT descriptors use unsigned 16-bit field counts. The parsers do
  not impose a separate nesting-depth or allocation budget, and some collection
  and metadata allocations reserve advertised counts before validating contents.
  Pre-serializing a row also reserves the metadata-derived fixed-size hint before
  invoking its codec, even for a row whose values would be null. Nested width
  products and aggregate hint sums are unchecked; cell-length validation does
  not protect this earlier calculation or allocation.
- NO_METADATA uses shared cached prepared-result metadata when available. With
  no cached metadata it creates empty metadata; it does not invent column types.
  If the server sends metadata despite a cache, the newly sent metadata wins.
  Malformed newly sent metadata fails parsing; this path does not fall back to
  the cache. Row-count parsing can also fail after metadata was decoded, and
  returns an error rather than a partially usable metadata/rows result.
  In the NO_METADATA branches, the received column count is not checked against
  the cached metadata; the empty fallback has zero columns even if a nonzero
  count was advertised. Subsequent row traversal uses the chosen column specs.
  With the empty fallback, raw and lending iterators can therefore yield the
  advertised number of empty rows without consuming or validating any row
  payload bytes, even when the advertised column count was positive. Typed
  destinations requiring columns fail their metadata type check; destinations
  accepting zero columns can succeed.
  METADATA_CHANGED together with NO_METADATA is rejected when metadata-ID
  negotiation makes that flag meaningful. PREPARED with paging state is rejected.
- Each prepared EXECUTE send captures one owned shared metadata snapshot, uses
  it for the outgoing metadata ID and skip-metadata decision, and retains it
  for decoding that send's NO_METADATA response. Concurrent replacement of the
  statement cache cannot change that snapshot or a result's selected metadata.
  The retry after reprepare captures another snapshot. Cache updates use
  unconditional replacement after checking the current ID/count; there is no
  compare-and-swap or schema-version ordering guarantee. A later store from a
  concurrent response can restore older metadata. Execution/cache policy is
  detailed in [execution](execution.md).
- Prepared bind metadata records column specifications and partition-key marker
  indexes. Indexes are sorted by bind-marker position while retaining their
  original partition-key sequence for routing-key reconstruction within the
  unsigned 16-bit sequence range. The parser accepts a nonnegative signed
  32-bit partition-key count, casts each sequence position to unsigned 16 bits,
  and does not reject duplicate indexes or indexes beyond the column count.
- Metadata strings may borrow from their owning serialized frame. Cached
  prepared metadata is shared and owned independently. Result containers retain
  the frame owner; ordinary raw row/column iterators borrow it, while the lending
  row iterator owns its storage. Typed iteration runs the destination row type
  check once when creating an iterator, then decodes values per row. This reduces repeated schema work
  without changing per-cell length, nullability, or text validation.
- Borrowed text/blob/arbitrary-integer/decimal views refer directly to immutable
  frame bytes. Their usable lifetime cannot exceed the frame's. Fully decoded
  values need not retain the metadata lifetime; deferred decoders and iterators
  need both metadata and frame to stay alive. Owning strings/vectors and dynamic
  values instead allocate/copy as appropriate.
- A shared-owner blob view may outlive the original result object by retaining
  the frame allocation. Creating ordinary subviews only copies slice references;
  reference-counted ownership is acquired when an independently owned view is
  requested. A legacy slice without a real frame owner must copy when promoted
  to that shared-owner representation.
- The exposed raw `BytesSequenceIterator` is bounded by buffer exhaustion rather
  than a declared count. On malformed nonempty input its failed read preserves
  the cursor, so repeated calls can return the same error indefinitely; callers
  must stop or otherwise handle that error.
- A raw row iterator scans length prefixes to find the next row even if the
  caller does not decode every column. Its declared row-count hint remains
  exact even when items are errors. A framing failure retains consumption of
  earlier columns and leaves the cursor at the failing column; subsequent calls
  encounter that same failure until the declared row count is exhausted. Column
  error context may change because each later row scan starts at column zero.
  A typed value-decoding failure after a successful row scan does not prevent
  decoding the next row. The lending iterator follows the same scan behavior.
  Individual `FrameSlice::read_cql_bytes` and `read_n_bytes` calls preserve their
  cursor on failure; this is not a transaction over an entire row or compound
  parser. Neither laziness nor a successful metadata type check proves the
  remaining result body is valid before it has been traversed.
- A lending row iterator can itself own metadata and frame storage, returning a
  row view that borrows from that iterator. The row view must be consumed before
  advancing the owner again; it is not an independently owned row collection.
- The stable asynchronous paged row stream requires destination rows independent
  of both the page-buffer and metadata lifetimes. Shared-owner blobs qualify;
  ordinary borrowed text/blob fields do not. It type-checks the initial page
  when the stream is created and each newly reached nonempty page before typed
  decoding, because metadata can differ between pages. Replacing an exhausted
  page does not invalidate independently retained shared-owner values. The
  internal lending pager waits for a page before consuming a row, so cancellation
  while that wait is pending does not consume the next row. A separately gated
  public lending API exposes the row view and whether its page is fresh; callers
  of that API are responsible for checking destination types against each page.

### Value formatting and helper boundaries

Dynamic values expose diagnostic display close to CQL literals, without a
guarantee of being valid executable CQL in every case. Text quotes are doubled,
blobs use hexadecimal, null fields display as `null`, and aggregate delimiters
follow their type. Decimal/varint display uses blob-conversion expressions;
dates/timestamps outside the formatter's calendar range display a placeholder.
Formatting must not be used as a replacement for typed parameter binding.

The native timestamp wrapper also exposes minimum/maximum values, wall-clock
`now`, duration addition/subtraction, and checked elapsed duration. Arithmetic
uses whole milliseconds (discarding duration fractions below a millisecond) and
panics if the resulting timestamp is out of range. Checked elapsed duration
returns absent when the argument is later, and otherwise supports the full
unsigned 64-bit millisecond difference between the timestamp extremes. `now`
is not monotonic and panics if the system time cannot fit its conversion.

Native varint constructors preserve the supplied bytes, and serialization sends
them unchanged. Equality and hashing normalize leading zero bytes, retaining a
zero sign byte when needed; empty bytes and all-zero byte sequences compare as
zero. This is not full signed-integer canonicalization: redundant leading `ff`
bytes on negative values are not removed. Native decimal equality compares both
that varint representation and the exact scale, so numerically equal decimals
with different scales need not compare equal.

Timeuuid ordering first compares the reordered timestamp bytes with UUID version
bits masked out, then compares the remaining eight bytes as individually signed
bytes. Equality and hashing use the same masked representation; UUIDs differing
only in version bits can therefore compare equal as timeuuid wrappers. Byte,
integer, field, string, and ordinary-UUID conversions do not enforce UUID v1.

Shared iterator-format helpers clone their iterators for each formatting call,
so formatting a diagnostic twice does not consume its stored iterator. Parser
helpers preserve a lightweight cause and can compute a one-based character
position in the original input; they are support for the custom-type grammar,
not another query parser or an extension to supported types.

### Errors and validation boundaries

Serialization, metadata type checking, cell decoding, frame-header/body parsing,
and database errors remain distinct categories. Nested value-decoding failures
normally identify column names/indexes or collection/tuple/UDT field context;
this is not uniform for framing failures. For example, typed tuple framing
errors wrap the low-level read error directly without adding a tuple-field
position. Custom error payloads are retained through type-erased, downcastable
wrappers.

Database errors preserve the server reason and relevant structured fields:
syntax, invalid/configuration, already-exists, function failure, authentication,
unauthorized, unavailable, overloaded, bootstrapping, truncate, read/write timeout,
read/write failure, unprepared, server/protocol error, and negotiated rate-limit.
Unknown numeric error codes remain an “other” code. Read/write failures carry
counts/consistency and data-present or write-type details; unknown write types
remain strings. Rate-limit errors carry operation and coordinator-rejection
information. Policy consequences are specified in execution, not inferred by
the decoder. Standard database error codes are matched before the negotiated
rate-limit code, so a collision retains the standard error interpretation.
Malformed fields of a recognized database error produce a parsing error with
the error family and field name; they are not downgraded to an unknown-code
error. Unknown rate-limit operation bytes are preserved as an “other” operation.

The implementation does not provide a blanket rejection guarantee for every
malformed frame: examples above include unbounded advertised frame lengths,
negative response lengths interpreted as null, count-bounded iterators that do
not require end-of-buffer, unchecked metadata size arithmetic, and server-deferred
scalar/collection constraints. Malformed custom type names can also make parsing
fail to terminate, as described in the Rust-specific parser facts below.
These observations must not be silently promoted to stronger Go requirements or
mistaken for an instruction to reproduce avoidable validation weaknesses.

## Rust implementation evidence

All links are pinned to the reference revision. Anchors identify the beginning
of the relevant implementation or contract; the surrounding functions carry the
details. These references record code inspection, not executed test results.

| Behavior | Source entry points |
| --- | --- |
| Frame construction, direction/version checks, static VOID, decompression and extensions | [frame/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/mod.rs#L55), `SerializedRequest::make`, `read_response_frame`, `parse_response_body_extensions`, `compress_append`, `decompress` |
| Message/opcode inventories | [request/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/mod.rs#L42), [response/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/mod.rs#L23) |
| Per-connection protocol extensions | [protocol_features.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/protocol_features.rs#L38), `ProtocolFeatures` parsing, startup options, and prepared LWT test |
| Handshake body codecs and defaults | [request/options.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/options.rs), [request/startup.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/startup.rs), [request/prepare.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/prepare.rs), [response/supported.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/supported.rs) |
| Authentication token framing | [request/auth_response.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/auth_response.rs), [response/authenticate.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/authenticate.rs) |
| Registration and event payloads | [request/register.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/register.rs), [server_event_type.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/server_event_type.rs), [response/event.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/event.rs#L27) |
| Query flags and EXECUTE extension ordering | [request/query.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/query.rs#L19), [request/execute.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/execute.rs#L60), `QueryParameters`, `ExecuteV2` |
| Batch framing and replayable values | [request/batch.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/request/batch.rs#L63), [serialize/batch.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/batch.rs#L13), [serialize/raw_batch.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/serialize/raw_batch.rs#L17) |
| First-statement batch row reuse | [session.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/session.rs#L1328), [statement/batch.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/batch.rs#L308), `peek_first_token`, `BatchValuesFirstSerializedIterator` |
| Parsed custom-payload consumption and result boundary | [connection.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L1167), [request_response.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/request_response.rs#L33), `update_tablets_from_response`, `into_non_error_query_response` |
| Length/null/unset and consistency primitives | [frame/types.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/types.rs#L21), especially `read_bytes_opt` versus `read_value` |
| Column types, vector-size classification, special-empty capability | [response/result.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/response/result.rs#L74), `ColumnType`, `NativeType`, `type_size_for_vector`, `supports_special_empty_value` |
| Scalar wrappers, dynamic value, empty/unset wrappers | [value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/value.rs#L35), `CqlValue`, `CqlTimeuuid`, `CqlVarintBorrowed`, `CqlDecimalBorrowed`, `MaybeEmpty`, `MaybeUnset` |
| Timestamp helper boundaries | [value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/value.rs#L773), `CqlTimestamp::now`, `checked_duration_since`, duration arithmetic |
| Unstable inter-driver raw access | [query_result.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/response/query_result.rs#L130), `QueryResult::deserialized_metadata_and_rows`, `QueryRowsResult::into_inner`, `raw_rows_with_metadata`; [serialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/value.rs#L983), vector-element helpers |
| Native wrapper identity and external adapters | [value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/value.rs#L219), timeuuid comparison/hash, varint normalization, decimal conversion; [deserialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L804), secrecy adapters |
| Deferred cells and container ownership | [deserialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L1674), `FrameSliceWithMetadata`, `Box`, `Arc`, `Cow`; [compatibility value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/value.rs#L17), `deser_cql_value` |
| Cell encoding and typed/dynamic aggregate rules | [serialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/value.rs#L36), `SerializeValue`, `serialize_udt`, `serialize_vector`, tuple implementations |
| Value framing and completion proof | [serialize/writers.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/writers.rs#L65), `CellWriter`, `CellValueBuilder`, `WrittenCellProof` |
| Row serialization and rollback | [serialize/row.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/row.rs#L62), `SerializeRow`, named-map implementations, `SerializedValues` |
| Typed scalar/aggregate decoding and dynamic decoding | [deserialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/value.rs#L48), `DeserializeValue`, `ListlikeIterator`, `MapIterator`, `VectorIterator`, `UdtIterator`, `FrameSliceWithMetadata` |
| Borrowing and ownership | [deserialize/README.md](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/README.md), [deserialize/frame_slice.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/frame_slice.rs#L46), `FrameSlice::to_bytes`, `new_borrowed`, `read_cql_bytes` |
| Raw/typed row iteration and once-per-result type check | [deserialize/row.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/row.rs#L31), [deserialize/result.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/result.rs#L13), `ColumnIterator`, `RawRowIterator`, `TypedRowIterator` |
| Owning lending iterator | [scylla-cql/deserialize/result.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/deserialize/result.rs#L19), `RawRowLendingIterator` |
| Async request ownership and metadata snapshots | [connection.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/network/connection.rs#L138), `RouterHandle::send_request`, `execute_raw_with_consistency`, `parse_response`; [prepared.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/prepared.rs#L569), `get_current_result_metadata`, `update_current_result_metadata`, `ColumnSpecsGuard`; [bound.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/statement/bound.rs#L25), `BoundStatement::new` |
| Paged result borrowing and repeated metadata checks | [pager.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L714), `QueryPager::next`, `rows_stream`, `TypedRowStream::poll_next` |
| Metadata caches, self-borrowed metadata, lazy row decoding | [frame/response/result.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/result.rs#L877), `RawMetadataAndRawRows::deserialize_metadata`, `SelfBorrowedMetadataContainer`, `deser_prepared_metadata`, `deser_prepared` |
| Custom Cassandra-style type names | [custom_type_parser.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/response/custom_type_parser.rs#L55), `CustomTypeParser` |
| Formatting, byte-prefix parsing, grammar helpers | [pretty.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/pretty.rs), [safe_format.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/utils/safe_format.rs), [frame/types.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/frame/types.rs#L26), [utils/parse.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql/src/utils/parse.rs) |
| Derive API and compile-time validation | [scylla-macros/src/lib.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/lib.rs#L48), [serialize/row.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/serialize/row.rs), [serialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/serialize/value.rs), [deserialize/row.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/deserialize/row.rs), [deserialize/value.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-macros/src/deserialize/value.rs#L15) |
| Codec error wrappers and database error fields | [serialize/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/serialize/mod.rs#L17), [deserialize/mod.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/deserialize/mod.rs#L15), [frame/response/error.rs](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/src/frame/response/error.rs#L36) |

### Rust-specific API and allocation facts

- The stable CQL types/traits live in `scylla-cql-core`; `scylla-cql` provides
  protocol/runtime-oriented support and re-exports. The main driver re-exports
  application-facing codecs, but some lower-level public APIs (for example
  `SerializedValues`, `RawBatchValues`, and raw request types) require using the
  CQL crates directly. `SerializeValue` and `SerializeRow` can be
  custom implemented; deserializers use `DeserializeValue<'frame, 'metadata>`
  and `DeserializeRow<'frame, 'metadata>` with separate lifetimes.
- Scalar mappings include `i8/i16/i32/i64`, `f32/f64`, `bool`, `String`/`&str`,
  `Vec<u8>`/`&[u8]`/`Bytes`, `IpAddr`, `Uuid`, `Counter`, `CqlDate`, `CqlTime`,
  `CqlTimestamp`, `CqlDuration`, `CqlTimeuuid`, and owned/borrowed decimal/varint
  wrappers. Blob encoding also supports `[u8; N]`. Encoding delegates through
  `&T`, `Box<T>`, `Arc<T>`, and `Cow<T>`. Decoding `Box<T>`/`Arc<T>` first decodes
  the inner value, then constructs its owning container; `Box<str>`/`Arc<str>` copy text
  into owned storage. `Cow<'frame, T>` decoding delegates to `&'frame T` and
  returns `Cow::Borrowed`, preserving the frame lifetime.
- Homogeneous rows can use slices/vectors, named rows can use string-keyed
  ordered/hash maps, and positional rows use tuples up to sixteen fields.
  Value tuple serialization supports one through sixteen fields; deserialization
  also includes the zero-field tuple. Dynamic `CqlValue` and `Row` provide
  runtime-typed owned alternatives.
- `CqlValue`'s `as_*` and consuming `into_*` helpers return `None` for an
  incompatible variant. `into_string` accepts ASCII/text, and `into_vec` accepts
  list/set/vector but not tuple; map and UDT have separate pair-vector helpers.
  `Row` contains `Vec<Option<CqlValue>>`, retaining top-level nulls. `MaybeUnset`
  defaults to `Unset`; its `from_option` maps `None` to unset rather than null.
- `FrameSliceWithMetadata` implements `DeserializeValue` for any column type
  and returns its optional raw cell slice plus the metadata reference without
  interpreting the payload. `Option<FrameSlice<'frame>>` also accepts any column
  type and returns the optional raw slice unchanged, without retaining metadata.
  `ColumnIterator` similarly implements
  `DeserializeRow` with an unconditional type check and returns itself. These
  public deferred-decoding destinations still require later type/byte validation
  when the caller chooses a concrete destination; `ColumnIterator::type_check`
  can check a destination against its remaining column specifications.
  The compatibility `scylla_cql::value::deser_cql_value` function decodes a
  present cell into owned `CqlValue` using a borrowed slice, and does not advance
  its `&mut &[u8]` argument despite that argument's mutable type.
- `BatchValues` accepts homogeneous slices/vectors, heterogeneous tuples of one
  through sixteen rows, references, and `BatchValuesFromIterator`. Its iterator
  operations serialize, test emptiness, or skip the next row, each advancing it;
  `count` consumes the remaining iterator. `RawBatchValues` and its adapter pair
  such providers with metadata contexts for protocol use; the raw vector form
  accepts already serialized rows. `SerializedValues::from_closure` returns the
  built row plus the closure's auxiliary result, while `new_from_frame` validates
  and copies the count-prefixed request values and advances past them. Its raw
  iterator preserves null/unset/value, but assumes valid stored framing and can
  panic if a custom writer produced malformed bytes.
- Optional feature flags add `chrono-04`, `time-03`, `num-bigint-03`,
  `num-bigint-04`, `bigdecimal-04`, `secrecy-08`, and `secrecy-10` adapters;
  `full-serialization` enables those adapters. The `serde` feature supplies
  external serde derives for value wrappers and consistency deserialization;
  these representations are explicitly outside semver guarantees, and are not
  the CQL wire codec. Separately gated unstable inter-driver APIs expose raw
  result metadata/rows: `QueryRowsResult::raw_rows_with_metadata` borrows them
  with `scylla_unstable` plus `unstable-nodejs-rs`;
  `QueryResult::deserialized_metadata_and_rows` returns an optional borrow with
  `scylla_unstable` plus `unstable-python-rs`. The latter feature also exposes
  individual vector-element serialization helpers. `scylla_unstable` plus
  `unstable-cpp-rs` exposes `QueryRowsResult::into_inner`, transferring metadata,
  raw rows, tracing ID, warnings, and optional coordinator, and publicly
  re-exports `DeserializedMetadataAndRawRows` from the main driver.
  `QueryPager::next_column_iterator`, gated by
  `scylla_unstable` plus `unstable-csharp-rs`, exposes the cancellation-safe
  lending pager described above, returning a column iterator and a fresh-page
  Boolean without performing a destination type check. These unstable APIs are
  not included in `full-serialization`. See [core Cargo features](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla-cql-core/Cargo.toml#L23)
  and [lending pager wrapper](https://github.com/scylladb/scylla-rust-driver/blob/f8b0ee928dc0dcb475247dc82cce4e42804b0621/scylla/src/client/pager.rs#L748).
  Native `CqlTime` decoding and its range-checking helper are unconditional.
  `chrono` is also an unconditional dependency for diagnostic formatting, while
  the `chrono-04` feature enables its CQL adapters. A Rust 1.94.1 core build with
  `--no-default-features` succeeds; this does not establish every optional-feature
  combination (see the round-03 report).
- The optional external type mappings are `chrono::NaiveDate`,
  `chrono::NaiveTime`, and `chrono::DateTime<Utc>`; `time::Date`, `time::Time`, and
  `time::OffsetDateTime`; `num_bigint::BigInt` from either supported version;
  and `bigdecimal::BigDecimal`. They map to date/time/timestamp, varint, and
  decimal respectively, with the conversion limits described above. Native
  varint/decimal wrappers also expose feature-gated conversions to/from those
  external number types; borrowed wrappers convert to external owned numbers.
  `secrecy-08` supports `Secret<T>` and `secrecy-10` supports `SecretBox<T>` with
  the required inner codec and zeroization bounds. Both expose the inner value
  to the ordinary CQL encoder and wrap decoded values. Secrecy 0.10 additionally
  decodes `SecretString` via `String` and `SecretSlice<T>` via `Vec<T>`; these are
  owned representations, and encoding writes ordinary bytes into the request.
- All four derive macros accept `#[scylla(crate = path)]`, directing generated
  code to that crate's `_macro_internal` exports instead of the default `scylla`
  path. Their shared attributes are `flavor = "match_by_name"` (default) or
  `"enforce_order"`, `skip_name_checks`, `rename`, and `skip`. Both value derives
  accept `forbid_excess_udt_fields` and field-level `allow_missing`; only
  `SerializeRow` accepts `flatten`. `default_when_null` controls both deserialize
  derives and is accepted but ignored by both serialize derives, permitting
  those derives to coexist on one struct. These attributes have the behavioral
  restrictions in the generated-code section above.
- `FrameSlice` stores a borrowed subslice and a reference to the original
  `Bytes`. Cloning/splitting it does not clone the original buffer owner.
  `to_bytes` creates a shared slice without copying only when that owner is
  present; `new_borrowed` uses the copy fallback. Small retained `Bytes` slices
  may consequently retain a much larger result-frame allocation.
  `split_at` panics for an out-of-range split; `chunks_exact` omits a trailing
  short remainder and panics for zero chunk size. `as_slice_mut` changes the
  slice cursor, not the immutable payload; callers must keep it within the
  original owner if later calling `to_bytes`, whose `slice_ref` requires that
  relationship.
- Self-borrowed metadata keeps its backing `Bytes` in a `Yoke` container whose
  public references are bounded by the container borrow. Its internal `'static`
  parameter is not a public promise of permanent storage. Cached metadata uses
  immutable `Arc` ownership; the prepared statement's `ColumnSpecsGuard` retains
  a cache snapshot while it is inspected. Codec error wrappers similarly own
  `Arc`-shared payloads, requiring custom errors to be `Send + Sync + 'static`;
  their mutable downcast succeeds only with exclusive ownership of that payload.
  Public `TableSpec`, `ColumnSpec`, and `ColumnType` ownership conversions detach
  borrowed metadata. `TableSpec::into_owned` reuses already owned strings, while
  nested `ColumnType::into_owned` can rebuild boxes and UDT ownership even when
  already owned; metadata ownership promotion is not universally allocation-free.
- `SerializedValues` owns an intermediate vector; `write_to_request` copies its
  bytes into the request buffer. Uncompressed frame construction reserves the
  exact body size when the request supplies it. Compression first creates a body
  buffer and then compressed output. Duration encoding allocates a temporary
  buffer; variable-width vector encoding allocates temporary element and length
  buffers. Numeric endian conversion is work even when it needs no heap buffer.
  These are explicit boundaries for a future Go zero-copy design.
- Ordinary `CellValueBuilder` use reserves an invalid `-3` length until
  explicitly finished; `WrittenCellProof` establishes that the writer was
  completed. Correct use must call `finish`; merely abandoning a builder does
  not provide a valid cell. The public `CellWriter::new_without_size` and
  `make_sub_writer_without_size` paths omit the present-value length prefix,
  as needed for vector elements. `set_value` still checks its slice length,
  but a builder in this mode neither reserves `-3` nor checks total length on
  `finish`. Null and unset still write their four-byte sentinel even in this
  mode. Completion proofs do not certify the surrounding vector's layout.
- Protocol clients can implement `SerializableRequest` with an opcode, body
  serializer, and optional exact size hint. `DeserializableRequest` is an
  auxiliary inspection/testing API, not the network response decoder. Its
  feature-aware entry point matters for `ExecuteV2`: the deprecated featureless
  decoder always expects a result-metadata ID and never consumes the tablet-v2
  byte, while the older `Execute` codec has neither extension. Public low-level
  request construction relies on callers to choose extension fields consistent
  with negotiation; the normal connection path does so automatically. These
  auxiliary decoders do not guarantee an error return for every truncated input:
  query-parameter flags and batch kind/statement-kind/flags use `Buf::get_u8`,
  which panics when the next byte is absent. The `Request` and `RequestV2`
  dispatchers only decode QUERY, EXECUTE, and BATCH; supplying another valid
  request opcode reaches `unimplemented!` and panics.
- Standard type descriptors parse native IDs and recursively parse collections,
  UDTs, and tuples. Custom type ID zero invokes the Cassandra-style parser,
  including `VectorType`, `FrozenType`, and hex-encoded UDT names. It reports
  unknown custom names, bad parameter counts, malformed hex, and trailing
  characters, but not every malformed input terminates with an error. The
  fixed-arity parameter fallback counts an iterator that repeatedly yields
  errors without advancing at EOF; for example, `ListType(Int32Type` never
  finishes parsing. This affects the shared List/Set/Map/Frozen parameter path.
  The tuple path instead collects a fallible iterator and stops at its first
  error, so its unterminated-input test does not establish that shared path's
  termination.
  The current parser rejects trailing whitespace after a type expression;
  leading whitespace and whitespace inside complex expressions are accepted
  where its grammar consumes them. Empty or all-whitespace input instead
  produces the blob type. An optional hexadecimal number followed by `:` is
  validated as a platform-sized unsigned integer and then discarded before
  parsing the type name. It recognizes
  `DateType` as the driver's Date native type, an implementation detail distinct
  from assuming all external marshaller aliases are semantically identical.
- The remaining cataloged `mod.rs` and `lib.rs` modules establish exports;
  `_macro_internal.rs` supplies generated-code plumbing, including
  column-by-column flattened row serialization. They do not
  create additional independent wire types. `macros_doctests.rs` checks derive
  examples and compile failures, `book_tests.rs` imports book examples spanning
  several feature sections, and `serde_tests.rs` checks feature forwarding.
  Test utility modules are harness support, not runtime behavior. Public
  response serializers such as `SchemaChangeEvent::serialize` and
  `ResultMetadata::serialize`/`Prepared::serialize` support proxy/test response
  construction; they do not add a normal client response-sending path. The test metadata type encoder
  does not support vectors and panics on that branch.

## Invariants

- Per-connection negotiated extensions determine the wire layout.
- Null, unset, empty payload, omitted trailing field, and missing schema field
  are kept distinct until a documented conversion intentionally combines them.
- Type checking precedes normal typed result iteration and does not substitute
  for validating each value's serialized representation.
- Borrowed values cannot outlive frame storage; independent shared views retain
  their storage; owned values have no frame dependency.
- Fully decoded values do not acquire unnecessary metadata lifetime constraints.
- Query row order and UDT field order follow server metadata, with name matching
  or explicit ordering policies determining how application fields are mapped.
- An encoding failure cannot dispatch a partially constructed request.
- Allocation or no-copy claims name the exact representation and operation;
  this implementation does not promise universal zero-copy behavior.

## Test places

### Rust unit tests

Description: Verify wire layouts, type compatibility, aggregate evolution,
malformed input, borrow behavior, derive policies, and allocation-related
regressions without a database. Targets below exist in the pinned source.
Round 03 executed the core crate's library tests with no default features:
228 passed, none failed or ignored. Protocol-crate, optional-feature, and
integration targets listed here are not covered by that run.

Test cases:

- LZ4 round-trip and short-input rejection for zero-, one-, and three-byte fixtures (no two-byte fixture): `scylla-cql/src/frame/mod.rs:test_lz4_decompress,test_lz4_decompress_rejects_short_input`.
- Tablet-v2 priority and absence behavior: `scylla-cql-core/src/frame/protocol_features.rs:negotiates_only_v2_when_both_supported,negotiates_no_tablets_routing_when_unsupported`.
- EXECUTE tablet block byte, feature-aware round-trip, and non-v2 omission: `scylla-cql/src/frame/request/execute.rs:execute_v2_appends_exactly_one_tablet_block_byte,execute_v2_tablet_block_roundtrip,execute_v2_no_tablet_block_without_feature`.
- Prepared response metadata serialization round-trip: `scylla-cql/src/frame/response/result.rs:prepared_serialization_roundtrip`.
- Named row errors and unsigned-16-bit count boundary: `scylla-cql-core/src/serialize/row_tests.rs:test_map_errors,test_serialized_values_max_capacity`.
- Incremental row rollback after a custom codec appends bytes and returns an error: `scylla-cql-core/src/serialize/row_tests.rs:serialized_values`.
- Tuple/UDT errors, errors propagated through present `Option`/`MaybeUnset` values, and special-empty rejection: `scylla-cql-core/src/serialize/value_tests.rs:test_tuple_errors,test_cql_value_udt_errors,test_option_errors,test_maybe_unset_errors,test_maybe_empty_errors`.
- Scalar length/type errors, UTF-8/ASCII, and aggregate errors: `scylla-cql-core/src/deserialize/value_tests.rs:test_native_errors,test_deserialize_ascii,test_deserialize_text,test_map_errors,test_tuple_errors`.
  `test_null_errors` in that file actually supplies a non-null map and incompatible
  `MaybeEmpty<i32>` metadata; it does not establish scalar null rejection.
- Borrowed/shared/owned blob contents and independent metadata lifetime: `scylla-cql-core/src/deserialize/value_tests.rs:test_deserialize_bytes,metadata_does_not_bound_deserialized_values`.
  The blob test checks equal contents, not pointer identity or retention after
  dropping the result. The metadata-lifetime test checks that results compile
  outside the metadata scope; it deliberately permits decoding errors.
- Null versus empty wrappers: `scylla-cql-core/src/deserialize/value_tests.rs:test_null_and_empty,test_maybe_empty`.
- Four-element `vector<int>` fast path matches element-by-element errors at every truncation of its 16-byte payload; typed list/vector and dynamic vector/map capacity equals the 100-element fixture size (no allocation-counter assertion): `scylla-cql-core/src/deserialize/value_tests.rs:test_vector_bulk_decoding_matches_element_by_element,test_collections_are_allocated_once`.
- Vector nth-element skipping, exhaustion, and subsequent iteration for valid
  five-element float/text fixtures: `scylla-cql-core/src/deserialize/value_tests.rs:test_vector_iterator_nth`.
  This does not test malformed skipped values or truncated fixed-width skips.
- Native varint zero-prefix equality/hash and unchanged serialized bytes;
  timeuuid version-bit-insensitive comparison/equality/hash:
  `scylla-cql-core/src/serialize/value_tests.rs:cql_varint_normalization,cql_varint_serialization,timeuuid_ordering_properties`.
  The normalization fixtures do not test redundant negative sign extension.
- Container decoding contents: `scylla-cql-core/src/deserialize/value_tests.rs:test_box,test_arc,test_cow`.
  These do not assert allocations or pointer identity, and the Cow test does
  not inspect whether the returned variant is borrowed.
- Feature-gated secrecy encoding for 0.8 and 0.10, plus 0.10 box/string/slice
  decoding contents: `scylla-cql-core/src/serialize/value_tests.rs:secret_08_serialization,secret_010_box_serialization,secret_010_string_serialization,secret_010_slice_serialization`
  and `scylla-cql-core/src/deserialize/value_tests.rs:test_secret_010_box_deserialization,test_secret_010_string_deserialization,test_secret_010_slice_deserialization`.
  These are not transport-buffer zeroization or feature-build results.
- Dynamic time-of-day range boundaries, exercising the native decoder: `scylla-cql-core/src/deserialize/value_tests.rs:test_deserialize_time`.
- Generated UDT `allow_missing` under name and ordered matching: `scylla-cql-core/src/macros_tests.rs:derive_serialize_and_deserialize_value_loose_ordering,derive_serialize_and_deserialize_value_strict_ordering`.
- Timestamp arithmetic boundaries and the full-range checked difference: `scylla-cql-core/src/value.rs:cql_timestamp_add_duration_panics_on_overflow,cql_timestamp_sub_duration_panics_on_overflow,cql_timestamp_checked_duration_since_no_overflow_on_extreme_range`.
- Raw row boundaries and short results: `scylla-cql-core/src/deserialize/result.rs:test_raw_row_iterator_basic_parse,test_raw_row_iterator_too_few_rows`.
- Owning lending iterator boundaries: `scylla-cql/src/deserialize/result.rs:test_raw_row_lending_iterator_basic_parse,test_raw_row_lending_iterator_too_few_rows`.
- Derived row rename/ordering/flatten behavior: `scylla-cql-core/src/macros_tests.rs:test_row_serialization_with_column_rename,test_row_serialization_with_enforced_order_failing_type_check,test_row_serialization_nested_structs,test_struct_deserialization_errors`.
- Custom type grammar and trailing-whitespace distinction: `scylla-cql/src/frame/response/custom_type_parser.rs:test_custom_cassandra_type_parser_errors,custom_cassandra_type_parser_rejects_trailing_whitespace,custom_cassandra_type_parser_accepts_middle_whitespace_for_complex`.
  The last target reaches a trailing-whitespace error after parsing internal
  whitespace; despite its name, it does not assert a successful parse.
  `test_custom_cassandra_type_parser` in the same file includes a successful
  hexadecimal-prefix fixture; the error test rejects a nonhexadecimal prefix.
  Blank-input blob fallback and prefix integer-overflow boundaries are not
  established by those fixtures. Its unterminated-expression case uses
  `TupleType`; it does not cover the nonterminating fixed-arity parameter fallback.
- Structured server errors and negotiated rate limiting: `scylla-cql-core/src/frame/response/error.rs:deserialize_simple_errors,deserialize_unprepared,deserialize_rate_limit_error`.
- With the `serde` feature enabled, `Counter` and `CqlDuration` serialize to the expected JSON and round-trip: `scylla/src/serde_tests.rs:value_types_are_serializable`.
  This feature-forwarding test does not cover every wrapper or consistency type.
- Comprehensive hostile-frame allocation bounds and proof of zero-copy across all hot paths: missing (neither is an implemented universal Rust guarantee).
- Nullable vector elements at exhausted payload boundaries, final zero-length
  variable vector elements, malformed-first duplicate extension fields,
  duplicate top-level SUPPORTED keys,
  metadata/cache count disagreement, uncached NO_METADATA with positive counts
  and unread row payload, continuation after mid-row framing errors,
  and generated UDT handling of malformed ignored fields: not established by
  the test targets cited here.
- Concurrent metadata replacement during an in-flight EXECUTE, retained blob
  ownership across page replacement/cancellation, and cancellation during a
  pending lending-page wait (including the gated `next_column_iterator` API):
  not established by the test targets cited here.
- A direct null-rejection matrix for non-nullable scalars, the full metadata-ID /
  tablet-v2 EXECUTE feature cross-product, standard-code/rate-limit-code collision,
  and unknown rate-limit operation bytes: not established by the targets cited here.
- No-size builder overflow behavior, malformed custom `SerializedValues` iterator
  input, deferred-cell APIs, scale-sensitive native decimal equality, and the
  C++/Python raw-result accessors: not established by the targets cited here.
- Auxiliary request-decoder panics for truncated one-byte fields or unsupported
  request-dispatch opcodes: not established
  by the targets cited here; this is a source-inspected limitation.
- Auxiliary QUERY/EXECUTE and BATCH rejection of unknown parameter-flag bits:
  not established by the targets cited here.
- First-statement-only batch row reuse across retries, and custom-payload removal
  at ordinary query-result conversion: not established by the targets cited here.
- Unterminated fixed-arity custom types, overflow in nested vector widths or
  metadata-based reservation hints, decompression expansion limits, acceptance
  of surplus response/cell bytes, and duplicate dynamic-UDT field handling:
  not established by the targets cited here. These are source-inspected boundaries.

### Rust integration tests

Description: Verify the types against actual server encodings, schema evolution,
extension changes, and negotiated compression. These targets are coverage
locations, not recorded successful database runs.

Test cases:

- Scalar wrappers, counter, empty values, and UDT evolution: `scylla/tests/integration/types/cql_types.rs:test_scalar_types,test_counter,test_empty,test_udt_after_schema_update,test_udt_with_missing_field`.
  Scalar adapter subtests require their respective `chrono-04`, `time-03`,
  `num-bigint-03`, `num-bigint-04`, and `bigdecimal-04` features. In particular,
  creating the decimal column does not test decimal values unless `bigdecimal-04`
  is enabled. `test_empty` checks dynamic empty `int` values inserted through
  `blobasint(0x)` and through bound `CqlValue::Empty`.
- Lists/sets/maps, shortened tuples, and vectors across supported types: `scylla/tests/integration/types/cql_collections.rs:test_cql_list,test_cql_set,test_cql_map,test_cql_tuple_db_repr_shorter_than_metadata,test_vector_type_all_types,test_deserialize_empty_collections`.
  The vector test round-trips 26 fixtures: native types, list/set/map, a tuple,
  and nested vectors; it has no UDT or counter-element fixture and no server
  capability skip guard. `test_deserialize_empty_collections` checks empty typed
  results without inspecting the wire representation; the separate list/set/map
  tests also check that empty inserts decode as `None` through optional destinations.
- Dynamic UDT and duration: `scylla/tests/integration/types/cql_value.rs:test_cqlvalue_udt,test_cqlvalue_duration`.
- Metadata-ID updates and simulated mixed extension support: `scylla/tests/integration/statements/result_metadata_extension.rs:test_insert_sends_empty_result_metadata_id,test_basic_metadata_update,test_mixed_cluster`.
  These tests return early when the server does not negotiate metadata IDs;
  reaching a successful test-process exit alone would not establish coverage.
  The mixed-cluster target strips one node's SUPPORTED extension through a proxy
  and forces PREPARE/EXECUTE placement with injected bootstrapping errors; it does
  not establish interoperability between actual different database versions.
- Compression flag, request-body wire-size ranges, and returned contents for
  uncompressed, Snappy, and LZ4 sessions, each with 1,000- and 1,000,000-byte
  repetitive text fixtures: `scylla/tests/integration/session/compression.rs:compression_shrinks_frames_and_preserves_data`.
  It measures outgoing QUERY/EXECUTE body sizes, not response wire sizes or
  arbitrary-payload compression ratios. This and the metadata-ID tests accept
  the proxy harness's `DriverDisconnected` shutdown error.

### Go unit/race tests

Description: The future Go implementation must test its own explicit ownership,
generic codec API, nested type validation, byte-order boundaries, null/unset/empty
semantics, cancellation, and concurrent retention. Rust lifetimes alone do not
establish Go correctness.

Test cases:

- Generic encoding/decoding for every type and nested aggregate: missing.
- Borrowed data stays valid for its documented scope and retained views survive
  buffer reuse, paging, concurrent queries, and cancellation: missing.
- Malformed lengths cannot panic, overflow, allocate beyond configured limits,
  or expose bytes outside the result cell: missing.
- Codec binding is invalidated when result metadata changes: missing.

### Go integration tests

Description: Preserve existing gocql assertions while moving call sites to the
new generic API, including protocol-version differences between the Go baseline
and Rust reference. Source equivalence does not justify dropping Go protocol-v5
coverage or treating unsupported server features as successful test execution.

Test cases:

- Existing scalar/aggregate/custom-codec serialization cases under the new API:
  missing.
- Prepared, batch, paging, null/unset, schema changes, compression, and mixed
  protocol/extension cases under real databases: missing.

### Go allocation benchmarks

Description: Measure each hot-path stage independently, including ownership
promotion and necessary byte transformations, with payload sizes and buffer
reuse policy stated. Allocation counts alone cannot prove absence of copying.

Test cases:

- Primitive and aggregate encoding directly into transport-owned storage:
  missing.
- Borrowed blob/text/nested decoding and lazy iteration with pointer-identity
  checks in addition to allocation measurements: missing.
- Prepared values reused by retries/speculation without redundant payload copies:
  missing.
- Compression/TLS and owned-result APIs state their unavoidable transformations
  and intentional copies explicitly: missing.

## Test generation notes

Use cross-products of actual type metadata, requested host representation, and
wire value state. Include exact length boundaries, every scalar width, malformed
UTF-8/ASCII, noncanonical Boolean bytes, nested nulls, tuple/UDT suffix evolution,
duplicate marker names, names requiring rename, vectors with all fixed/variable
classifications, partial element lengths, and extension combinations. Check
error identity/context as well as success values. For lifetime tests retain a
small field while releasing the original page, advancing the pager, and
recycling unrelated buffers. For batch providers check repeated iteration and
statement/value-count mismatch without materializing an extra row list.

Review status: source inspected and corrected in
[round 01](reviews/serialization-protocol-round-01.md),
[round 02](reviews/serialization-protocol-round-02.md),
[round 03](reviews/serialization-protocol-round-03.md),
[round 04](reviews/serialization-protocol-round-04.md),
[round 05](reviews/serialization-protocol-round-05.md),
[round 06](reviews/serialization-protocol-round-06.md),
[round 07](reviews/serialization-protocol-round-07.md),
[round 08](reviews/serialization-protocol-round-08.md),
[round 09](reviews/serialization-protocol-round-09.md), and
[round 10](reviews/serialization-protocol-round-10.md). Round 03 records the
core-only build and unit-test execution described above.
