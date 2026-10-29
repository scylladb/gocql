# Go v2 transport ownership and copy boundaries

This is a narrow implementation recommendation for a clean `gocqlv2` design,
building on [the existing hot-path audit](go-hot-path-audit.md). It does not
repeat that audit or prescribe public API names. The inspected working tree is
based on commit `1ceaac8172bda26e8b8d701ac356982d33729b27`. The local standard
library inspected for transport behavior is Go 1.25.0 on Linux/amd64. No driver
code was changed and no tests were run for this document.

The zero-copy target is removal of driver-managed payload copies in encoding,
framing, coalescing, reassembly, and borrowed decoding. Encoding changed
representations, compression/decompression, TLS record processing, and socket
I/O must be accounted for separately. A pooled gather buffer still copies the
payload and does not satisfy that target. Standard-library TLS has an additional
plaintext handoff copy described below; this design accounts for that external
boundary and does not propose a new cryptographic implementation.

## Required internal boundaries

1. **Immutable encoded input.** Represent canonical bound values as an ordered
   sequence of immutable byte spans plus checked total length and retained
   backing owners. Typed encoders emit scalar representations, lengths, and
   aggregate framing into owned small-buffer storage; already encoded text,
   blobs, and nested payloads remain retained spans. Use fixed slabs/chunks or
   sizing before emission so growing an arena does not relocate emitted
   payloads. Null, empty, and unset remain distinct. Routing reads the same
   canonical spans rather than invoking an encoder a second time.
2. **Attempt envelope.** Build stream ID, protocol flags, prepared ID, result
   metadata ID, consistency/options, and paging state for the selected
   connection as separate owned spans. Compute frame length with checked
   arithmetic. An attempt must never patch shared input or another attempt's
   header. The request is a sequence, not a mandatory contiguous `[]byte`.
3. **Transport writer.** Replace `contextWriter.writeContext(ctx, []byte)` and
   `writeRequest.data []byte` with an owned span-sequence submission and exact
   byte-completion result. Direct and coalesced writers share its ownership and
   cancellation rules.
4. **Segment codec.** Replace append-a-payload-to-wire-buffer construction with
   descriptor construction over logical subranges. Expose owned verified
   payloads on receive, rather than connection scratch whose next read
   invalidates the previous payload.
5. **Frame/page owner and cursor.** A page retains one or more immutable backing
   owners. Its independent checked cursor traverses span ranges, including
   values crossing ranges. Pooling a parser must not recycle the bytes that a
   retained page/view still owns.

These replace concrete seams at `frame.go:703` (`finish`), `frame.go:2214`
(`writeBytes`), `frame.go:2275` (`prepareModernLayout`), `conn.go:1946`
(`contextWriter`), and `internal/segment/segment.go:177,313,345`
(`ReadPayload`, `AppendUncompressed`, `AppendCompressed`). The existing
single-buffer API should not remain as a mandatory adapter underneath the new
path.

## Sending: protocol transformations without gathering

| Path | Required representation and transform |
|---|---|
| v4, uncompressed | A 9-byte CQL header followed by body spans. Write them in order; a contiguous CQL frame is unnecessary. |
| v4, compressed | Compress the logical body as one protocol-compatible block, retain the transformed output, and prepend the 9-byte header with compression flag and compressed body length. LZ4 includes its four-byte big-endian uncompressed length prefix; Snappy uses its compatible block encoding. Do not copy compressed output back into a frame buffer. |
| v5 before startup completes | Preserve the existing handshake framing boundary; OPTIONS/STARTUP and their negotiation do not unconditionally enter modern segment framing. |
| v5, uncompressed | Slice the logical CQL frame sequence at segment payload boundaries of at most `0x1ffff` bytes. Each segment is `[6-byte header including CRC24][original payload spans][4-byte CRC32]`. Boundaries can cut a value or a span. |
| v5, compressed | Compress each logical segment payload as an independent raw LZ4 block. Its 8-byte header carries compressed length, uncompressed length, and CRC24; there is no v4 LZ4 length prefix. CRC32 covers transmitted compressed bytes. Stored-as-is segments set uncompressed length to zero and transmit the original span subrange, without copying it into discarded compression output. |
| TLS | Supply the same ordered plaintext spans through `tls.Conn.Write` while holding the driver writer lock over the complete request/batch. The standard TLS record layer performs its own record assembly and encryption. Do not add a driver plaintext gather buffer merely because TLS lacks writev. |

For v5, a self-contained segment contains complete CQL frames only. A large
frame split across segments uses non-self-contained segments throughout,
including its final segment. Packing several small complete frames into one
segment is optional; packing must not accidentally change that flag. Ordinary
write coalescing can concatenate already constructed segment sequences.

The current `Compressor.Encode([]byte)` and
`SegmentCompressor.AppendCompressed(dst, src []byte)` interfaces cannot consume
fragmented input. Both `pierrec/lz4.Compressor.CompressBlock` and the current
Snappy encoder require contiguous source bytes. Retaining those calls behind a
span-to-scratch gather would preserve the copy that this design is meant to
remove. A clean v2 requires a block-codec interface accepting a bounded span
source, and implementations that read logical offsets/ranges across spans and
emit final compressed output directly. Merely changing the interface is
insufficient. A compression library's streaming writer may produce a different
wire format and is not automatically a valid substitute for native-protocol
block encoding.

Compressed output is a representation change. Gathering its input first and
copying its output into another frame are separate, avoidable operations. A
compressor must retain useful compression behavior; always emitting literals
or always selecting v5 stored-as-is is not a substitute for implementing the
span-aware compression path. The codec must enforce source, output, and
expansion bounds, and exact expected decompressed lengths where supplied.

## CRCs, iovecs, and completion

`internal/crc/crc.go:31` computes payload CRC32 using IEEE CRC32 over the four
initial bytes `FA 2D 55 CA`, followed by the payload. For fragmented payloads,
initialize that state once per segment and update it in byte order for every
span. Do not reapply the prefix per span or XOR independently finalized CRCs.
The trailing CRC32 is little-endian. Header CRC24 covers only the three or five
header field bytes, using the existing initializer/polynomial, and is written
in little-endian byte order. The checksum scan does not require a payload copy.

`net.Buffers.WriteTo` already supplies the required descriptor-only write path
for supported raw connections. It mutates descriptors, so each writer needs
private mutable descriptor scratch while backing bytes remain immutable. The
Go 1.25 Unix implementation splits oversized vectors into multiple writev
operations (1024 entries on Linux; smaller limits on some other platforms).
The driver must hold its serialization lock/deadline ownership across every
such operation. Exceeding an iovec limit is a reason to continue the vector,
not flatten its payload. Combine adjacent spans only when they already refer
to contiguous storage; assemble newly generated small fields compactly before
publication to avoid one syscall/TLS record per scalar field.

A coalesced batch needs request boundaries independent of iovec boundaries:
store each request's total byte count or cumulative end offset and distribute
partial write completion using those offsets. The current `flush` assumes one
buffer equals one request. Normalize a short successful underlying write to an
error. A partial protocol write makes the connection unusable; releasing input
does not permit reusing its stream on the partially written connection.

Cancellation before acceptance can return without sending. After a writer
accepts a request, it must either finish or fail the write while retaining all
backing owners; cancellation cannot free spans or interleave another frame
mid-write. Coalescer shutdown must complete every accepted request exactly once.
An individual request can release its write ownership after its last byte is
consumed and no writer operation can access it again. Returning all completion
results after the flush is a simpler valid implementation.

## Receiving: page storage is a chain of leased ranges

For uncompressed v4, read the body directly into its final leased buffer. Remove
the intervening `bufio.Reader` payload copy, or replace it with owned receive
chunks whose unread ranges can be transferred to page owners. Reading a small
header through the current buffer can prefetch body bytes; assuming every body
read bypasses `bufio.Reader` is incorrect. Frame header parsing and deadlines
can remain separate from body ownership.

For compressed v4, receive the encoded block in owned storage and decompress
into the final page backing buffer. Release the compressed source when the
decoder finishes. If a general receive-chunk pipeline fragments the compressed
block, its decoder must consume that span source rather than flatten it first.

For v5, replace `segment.Scratch` with acquired buffer ownership:

- Read an uncompressed/stored-as-is segment into its leased payload buffer and
  verify its CRC before publishing any contained frame.
- For a compressed segment, receive and CRC-check the compressed payload, then
  decompress into a leased output buffer of the declared size. The wire buffer
  can return to its pool after decoding; the output remains owned by pages.
- A self-contained segment can hold several response frames for different
  requests. Each frame retains a subrange and a reference to the same segment
  owner. The receive loop drops its own reference after all frames are handed
  off; one request cannot release another request's backing bytes.
- A split frame retains the sequence of verified segment subranges. Accumulate
  byte counts and descriptors, not body bytes. The CQL header itself can cross
  segments; parse its fields with the same checked cursor. No contiguous
  reassembly buffer is required. A malformed continuation, oversized declared
  frame, overshoot, unexpected self-contained segment, or zero-progress
  continuation releases all accumulated owners and follows the appropriate
  connection-failure path.

This removes both the self-contained segment-to-framer copy and
`recvSplitFrame`'s segment-to-reassembly copies. The existing final
`adoptFrameBody` transfer is useful evidence that payload and parser ownership
can be separated, but its single contiguous slice does not express the needed
general representation.

A checked cell cursor returns a logical range, which may contain several spans.
Length prefixes, scalar fields, UTF-8 sequences, and collection elements may
cross span boundaries. Decode fixed-size values by value across that boundary;
retain text/blob/collection views without linearizing. A general blob/text
view therefore cannot promise a single `[]byte` or `string` for every response:
it needs range/chunk access and explicit copied conversions. A contiguous fast
path is useful, but a fragmented cell must not silently fall back to a copy.
Nested typed decoders and custom codecs need this same source abstraction.

## Shared lifetimes and pool reuse

There are two different ownership graphs:

- An execution retains canonical encoded input across retries, speculative
  attempts, and future pages. Each asynchronous writer retains its own lease.
  Returning the winning response or canceling an attempt does not prove that
  losing writers have finished. A pager keeps the input alive while more pages
  can be issued, then drops its share on exhaustion/close. Closing a connection
  must not recycle bytes while a blocked write can still read them.
- A result/page retains receive-buffer owners independently of the connection,
  parser, cursor, and iterator. A borrowed view follows its documented page
  lifetime; a retained view takes an independent share. Advancing or closing an
  iterator drops its share, not all shares. Loser results, abandoned responses,
  failed parse paths, and unused prefetched pages drop their shares exactly
  once. Connection closure does not invalidate already delivered retained
  results.

Reference counting protects reuse, not immutability. Go `[]byte` remains mutable
through aliases. A no-copy input API therefore needs an explicit transfer or
immutable-owner contract covering every alias; a reference count on arbitrary
caller-owned slices cannot make concurrent mutation safe. Do not promise that
all borrowed arguments become reusable merely because execution returned a
winner. A scoped raw-borrow operation must join all users before its scope ends,
or use an explicit completion boundary; shareable owned inputs avoid that join
for ordinary asynchronous execution.

Keep owner identity stable until its final release. A pooled framer's resettable
`released` flag is not a safe retained lease and permits stale-handle confusion
if exposed that way. Pool storage only after the last owner releases it; avoid
making a per-cell reference mandatory when the caller already holds a page
lease. Document that retaining a tiny view can retain a much larger segment or
page, and provide an explicit copy when the user wants to reduce retention.

## Standard TLS boundary and verification obligations

In the inspected Go implementation, `net.Buffers.WriteTo(tlsConn)` falls back to
ordered `Write` calls. `crypto/tls` performs record processing internally; TLS
1.3 currently copies plaintext into record storage before in-place sealing.
On receive it decrypts into record-owned storage, then `tls.Conn.Read` copies
plaintext through `bytes.Reader.Read` into the caller's destination. Its public
API does not expose retained plaintext-record leases. These are concrete
standard-library boundary costs, not CQL/v5 framing requirements and not
permission to add driver gather/reassembly copies. The zero-copy driver claim
must state this boundary instead of implying kernel-to-caller physical
zero-copy, or claiming that encryption mathematically requires every such copy.

Implementation validation should prove byte-identical wire output and pointer/
owner preservation on v4 and v5 uncompressed paths; cross-span headers, cells,
and nested values; incremental CRC equivalence; compression round trips across
span boundaries and raw fallback; vectors exceeding OS limits; partial writes
within one request and across coalesced requests; TLS writes without a driver
gather; and retained page/input validity through cancellation, speculation,
prefetch, close, and pool reuse. Allocation benchmarks alone cannot establish
absence of payload copies. Payload-copy instrumentation or backing-identity
checks should accompany throughput/allocation measurements.
