# Streaming receive decoding into caller storage

Status: draft Go v2 feature proposal. Not implemented or verified. This is a
new Go design requirement, not a claim about the pinned Rust reference driver.
The exact public construction and streaming-codec signatures require design
reconciliation before implementation. Drafting this feature does not resume
the implementation work paused in [plan.md](../plan.md).

## Summary

Decode result rows incrementally as response bytes arrive, writing into an
application-defined representation and application-provided payload storage.
Do not first collect the complete native response, result page, row or large
cell in a driver buffer. Read eligible uncompressed payloads directly into
their final caller-provided byte ranges. Parse fixed scalars with bounded
scratch and assign decoded Go values to the destination fields.

```text
connection reader -> framing/validation -> incremental decoder -> caller fields
```

The objective is to eliminate intermediate response/page storage and reduce
peak driver memory and time to the first row. It does not eliminate storage for
data the application keeps, kernel socket buffers, or the buffering required
by TLS, integrity checks and negotiated compression.

## Relationship to existing designs

| Path | Where payload bytes live | When a caller destination is available |
| --- | --- | --- |
| Current typed receive | Driver-owned frame/segment storage; decoded fields borrow views. | After reception and response parsing. |
| Planned Rows/Page.NextInto | The same leased receive storage; fields are written directly into caller's row. | At iteration over a received page. |
| Proposed streaming receive | Final caller storage, plus explicitly bounded protocol/transform staging. | Before decoding the corresponding row payload. |

The [NextInto contract](gocql-v2-api.md) remains useful independently. Adding
NextInto to an already-received page does not implement this feature. Streaming
execution needs a separate admission/receive path and reader contract; it must
never silently fall back to receiving a complete page.

## User-visible behavior and API shape

Provide an explicit streaming execution entry point and a cursor exposing
NextInto, Err and Close. The following is an illustrative API shape, not an
already-available or finalized public signature:

```go
stream, err := RunStream(ctx, session, bound, streamDecoder, streamOptions)
if err != nil {
    return err
}
defer stream.Close()

// Application-defined destination. []byte fields are final caller storage;
// a text mapping validates UTF-8 incrementally without constructing a string.
type DownloadRow struct {
    ID int32
    Name []byte
    Payload []byte
}
row := DownloadRow{
    Name: make([]byte, 0, 1024),
    Payload: make([]byte, 0, 1 << 20),
}
for stream.NextInto(&row) {
    consume(row) // synchronous; the next call may reuse these same arrays
}
return stream.Err()
```

Streaming admission installs the request's streaming decoder before sending.
Opening the cursor may read checked headers/metadata and bounded transform
units, but returns without waiting for the entire page. Row payload decoding
waits for the destination supplied by NextInto. The example's streamDecoder
declares the column mappings and text/blob storage rules; its exact typed
builder/interfaces are a design-reconciliation item below.

Initial payload destinations are writable caller-provided byte slices. Decode
into their existing capacity and adjust lengths; preserve capacity across
calls. A declared value length exceeding capacity fails before writing that
value's body. Do not grow a destination implicitly. A separate explicit
allocator policy may be added later, subject to size budgets, allocating a
final value once rather than growing/copying it chunk by chunk.

Scalar fields are overwritten with decoded values. Nullable mappings explicitly
distinguish null from an empty present value and reuse available payload
capacity. Result unset remains invalid. Exact reset behavior for custom row
representations belongs to their streaming decoder contract; do not inherit the
buffered NextInto rule that zeroes an entire R and discards its supplied slices.

NextInto returns true only for one complete, validated row. Nil destinations
fail before consuming a row. Exhaustion returns false with no error; malformed
data, insufficient capacity and cancellation are terminal errors. On failure,
the supplied storage may contain partial writes and the row is invalid. There
is no rollback promise and no successful publication of partially decoded rows.
Earlier successful rows remain delivered; callers must check Err after iteration.

## Incremental framing and decoding

1. Read the native envelope, identify its stream ID and response kind, and
   process extensions in wire order under the negotiated protocol limits.
2. Obtain and validate actual result metadata, including prepared metadata-ID
   and generation fences, before compiling/selecting the streaming row plan.
   Resolve field names/order and recursively check types once per result shape.
3. Read row counts, cell lengths and nested collection framing incrementally.
   Checked limited readers prevent a codec from consuming another cell/frame.
   Partial reads may split any header, scalar, UTF-8 sequence or nested length.
4. Fold fixed-width wire scalars into Go values using small bounded scratch;
   never reinterpret a Go struct as the wire representation.
5. For eligible raw text/blob cells, read into the final caller-provided byte
   ranges. Validate text across read boundaries. Ignored fields are consumed
   with bounded discard scratch without retaining their payloads; promised
   structural validation still applies to ignored values.
6. Require exact cell/row counts and exact envelope consumption. Process
   warnings, tracing, custom payloads, paging state and other extensions without
   hiding an unbounded response-sized allocation in metadata handling.

Reuse the canonical type, scalar-validation and framing rules from the
[public API](gocql-v2-api.md) and
[implementation contract](gocql-v2-implementation.md). Existing ValueReader and
RowReader operate over retained bytes; an arbitrary existing Decoder cannot be
treated as an incremental codec. Introduce an explicit streaming contract and
share checked primitives rather than duplicating serialization semantics.
Unsupported custom/aggregate mappings fail explicitly; adapting them by
gathering a whole large cell is not a streaming implementation.

For nested types, stream element counts/lengths and decode into declared final
application storage. Enforce nesting, count and byte limits before allocations
or writes. Returning lazy Sequence/Mapping/Bytes views requires retaining their
backing bytes; the stream must not promise lazy access to bytes already consumed
and discarded. A mapping may explicitly preserve encoded bytes in final caller
storage and build views over that storage, with its lifetime documented.

## Protocol, compression and integrity boundaries

| Transport/protocol path | Allowed staging and delivery rule |
| --- | --- |
| Native v3/v4, uncompressed | Parse incrementally. Bulk text/blob bytes are read into final caller storage without a full frame, page or cell staging buffer. Account for bounded connection read-ahead separately. |
| TLS | Use the authenticated plaintext reader supplied by the TLS implementation. Its record buffers and plaintext delivery copies are transform costs; no additional full-response driver gather is permitted. |
| Native v5, uncompressed | Buffer and validate at most a bounded segment unit before exposing its bytes to the row decoder. Rows/cells can span units; never assemble their complete payload into a second driver buffer. Segment-to-final-storage copies are accounted for explicitly. |
| Native v5, compressed | Verify the compressed segment and decompress into bounded unit storage under compressed/decompressed limits, then feed the incremental parser. Reuse units once their bytes have been consumed. |
| Native v3/v4, frame compression | A full-frame decompressor does not meet the no-response-buffer requirement. Initially reject this combination before request submission unless a separately verified incremental transform satisfies the working-memory and integrity contract. |

Native v5 payload checksums cover segments that can contain multiple rows. The
first design validates a segment before consuming it, so a caller can receive a
row without exposing unverified bytes or requiring all the segment's rows to
have supplied destinations at once. This bounded staging is intentional. A
future direct-to-final-storage checksum path requires a separate publication
design; it cannot claim success before its checksum trailer has been checked.

A row spanning units becomes successful only after every unit containing its
bytes has passed validation and its own framing/type checks have completed.
Later malformed rows, trailing bytes or a truncated later unit produce a
terminal stream error; there is no claim of atomic whole-response validation or
retraction of earlier rows. Keep at most one current validated plaintext unit
and the bounded input/output state needed by its transform, not a list of units
retained until the page ends.

## Ownership, scheduling and backpressure

The caller exclusively lends the destination and its writable payload ranges
during NextInto. The decoder may write only within those ranges and must not
retain pointers after that call completes. It never accesses caller storage
from prefetch workers or unrelated requests. No background payload receive into
that storage is permitted between calls. Caller-owned output survives cursor
and session closure; reusing the same arrays may overwrite previous output.
These arrays do not require a driver PageLease. Independently requested views
over them follow the caller's storage lifetime and immutability obligations.

The initial implementation reserves a connection exclusively for the active
stream, with no unrelated in-flight requests. Acquire exclusive transport
ownership before sending; never take raw read ownership from an active shared
connection reader. The streaming reader still handles control/event envelopes
and protocol errors. Reservation is pool-bounded and explicit; holding a cursor
holds that capacity. Do not silently open unbounded extra connections.

Pull iteration supplies backpressure. Between calls, driver memory remains
bounded and the peer eventually experiences socket backpressure. Disable
streaming prefetch and speculation as explicit properties of this execution
mode; do not queue decoded pages or speculative copies of caller destinations.
Supporting streaming on a shared multiplexed connection is future work: a slow
consumer otherwise blocks unrelated streams or requires a bounded spill policy.

Close is idempotent and may interrupt an active NextInto. It must unblock the
read and wait for all accesses to caller storage to stop before returning.
Context cancellation has the same lifetime boundary when the active call
returns. Two NextInto calls may not run concurrently. On early stop, capacity
failure, cancellation or framing loss, close the exclusively reserved connection
rather than performing an unbounded drain. Reuse is allowed only after exact
response completion and restored protocol boundaries; do not return a partially
consumed connection to the pool.

Retry/reprepare before result delivery may use existing safe request rules only
while caller output has not been written. Once decoding writes to a destination
or a row has been delivered, do not transparently replay that response. A later
failure is terminal; report delivered-row/attempt information. Keep outbound
Bound.Close/Wait accounting valid through the stream's lifetime and connection
cleanup. Register cancellation before reads, and bound cleanup so a stalled peer
cannot keep a destination writable after NextInto/Close has returned.

Automatic paging reuses the established continuation, metadata, deadline and
progress-budget rules and begins the next page on demand. Do not prefetch.
Response state/history and explicitly retained control information count toward
their own budgets. Close releases the connection reservation and request
admission, including after empty pages, errors and abandonment by the caller.

## Memory and copy contract

Driver working memory is bounded by configured header/metadata limits, compiled
plans, connection read-ahead, protocol/transform units, discard scratch and
paging-history budgets. It must not grow with total response bytes, row count,
or the largest raw skipped cell. Final application output is accounted for
separately; relabeling an internally accumulated page as output is not allowed.
Existing frame/decompressed/depth/count limits still bound total message work
and hostile lengths, including when no corresponding full frame is allocated.

For uncompressed plaintext v3/v4, instrument reads to prove that bulk payload
bytes target the caller's final ranges. Small read-ahead/prefix copies must be
bounded and reported. For TLS/v5/compression, report required transformation and
validated-unit-to-output copy bytes separately. Do not describe all modes as
zero-copy or allocation-free. No full-response driver gather is permitted in
any supported streaming mode; unsupported modes return a typed capability error
before sending rather than changing these guarantees.

## Invariants

- Compiled plans are immutable; they do not capture destinations or read state.
- Only checked bytes become successful row values; partial writes are invalid.
- Every wire byte is consumed exactly once by its framing/transform state machine;
  checksum/validation passes are explicit additional bounded scans.
- No full response/page or driver-owned complete-row temporary is accumulated.
- Scratch never escapes into output; final storage is caller-owned.
- A closed or canceled cursor cannot keep writing to a returned destination.
- Unsupported mode/codec combinations cannot trigger hidden buffered fallback.
- Successful prior rows do not hide a later stream error or justify replay.

## Test places and acceptance criteria

### Unit, race and fuzz tests

- Split reads at every header, length, scalar, text and collection boundary;
  exercise short reads, truncation, invalid lengths, overflow and excessive depth.
- Decode repeated rows into preseeded slices without changing their backing;
  test exact/insufficient capacity, null/empty distinction and invalid UTF-8.
- Generate responses much larger than scratch limits and record maximum live
  driver storage. Large skipped blobs must not allocate proportional storage.
- Gate the source after the first row: raw uncompressed streaming must deliver
  that row before the remainder of the page is supplied. For v5, gate after its
  last required validated segment rather than after a partial checksum unit.
- Cover several rows per segment, a row/cell spanning segments, compressed units,
  bad checksums and truncated trailers; verify invalid rows are never successful.
- Verify metadata mismatch/NO_METADATA fences, custom decoder errors, ignored
  columns and nested type boundaries without whole-cell fallback.
- Block reads and cancel/Close concurrently; assert they unblock, stop touching
  caller memory, release admissions, and discard incomplete connections.
- Hold cursors idle, exhaust pool reservations, handle control messages, and
  prove no unrelated active requests share a reserved connection.
- Fail before/after the first destination write and after a delivered row;
  verify permitted pre-delivery retries and prohibited replay after output.
- Fuzz framing, capacity arithmetic, incremental text state and cancellation;
  enforce all memory limits and absence of unsafe string/struct writes.

### Integration tests

Use actual Scylla/Cassandra responses for large text/blob rows, scalar/nested
values, empty results, multiple pages and result metadata changes. Cover raw
v4, TLS, and Cassandra v5 raw/LZ4 with values crossing segment boundaries.
Verify early close and subsequent pool health, server interruption after partial
delivery, and explicit rejection of unsupported frame-compressed streaming.
Use injected transport fixtures for deterministic read gates, corruption and
partial writes into destinations; mocks alone do not establish server coverage.

### Performance evidence

Compare streaming against Run/Row and planned buffered NextInto with equivalent
results, payload sizes and concurrency. Separate final output allocation from
driver working memory. Record peak live bytes, allocation bytes/count, payload
copies by category, read/syscall counts, CPU/throughput and latency to first row.
Vary page size, row count, largest cell and segment count independently, including
large ignored cells and a slow consumer. A pass requires the bounded-memory and
direct-read evidence above; fewer allocations alone is insufficient.

## Implementation seams and decisions before implementation

Current [v2_transport_receive.go](../v2_transport_receive.go) receives complete
native frame bodies or retains their segment spans before
[v2_transport_parse.go](../v2_transport_parse.go) constructs a response page.
[internal/segment/spans.go](../internal/segment/spans.go) validates owned segment
payloads; its bounded-unit integrity rules are reusable, while retaining a
complete frame's segment list is not the streaming path.
[v2_result.go](../v2_result.go) iterates already-received rows, and
[v2_rows_codec.go](../v2_rows_codec.go) uses readers over retained byte ranges.
These are implementation seams, not evidence that the proposed behavior exists.

Before implementation, reconcile the following into the public API and owning
tasks without changing the accepted buffered APIs implicitly:

1. Finalize RunStream/RowStream construction, its relationship to Statement/
   Prepared/Bound, and an explicit typed streaming row/value codec interface.
   Specify null/text/blob/nested destination builders and capacity errors.
2. Define pool reservation/read ownership and cancellation-safe exclusive
   streaming transport, including event handling and return-to-pool criteria.
3. Specify the bounded metadata and transform budgets and initial supported
   protocol/compression combinations, with admission-time capability checks.
4. Assign codec, transport, execution/paging, documentation and verification
   changes to the existing sequential implementation tasks in plan.md. Add
   focused handoff gates before claiming this feature is release-ready.

The contracts above define the requested behavior. Public signature settlement,
implementation, independent review and all acceptance evidence remain pending.
