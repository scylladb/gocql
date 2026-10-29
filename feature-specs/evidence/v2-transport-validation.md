# Typed transport implementation validation

Environment observed during these runs: Go 1.25.0, linux/amd64,
AMD Ryzen 9 9950X3D. These are focused implementation checks, not completion of
the full integration or performance acceptance gates.

## Executed checks

- `go test -race ./internal/segment -count=1` — passed, 1.031 s.
- `go test -race -run '^TestTyped' -count=1 -timeout=90s .` — passed, 1.100 s
  at the first transport checkpoint.
- `go test -tags=unit -run 'Test(WriteCoalescing|ExecCloseWithError|ConnProcessAllFramesInSingleSegment|ConnProcessReservedStreamFrameInSegment|RecvSegment|ConnReaderRead|ReadFirstSegmentHeader|ProcessFrame)' -count=1 -timeout=90s .`
  — passed, 0.575 s.
- `go test -race -tags=unit -run 'Test(Typed|WriteCoalescing|ExecCloseWithError|ConnProcessAllFramesInSingleSegment|ConnProcessReservedStreamFrameInSegment|RecvSegment|ConnReaderRead|ReadFirstSegmentHeader|ProcessFrame)' -count=1 -timeout=90s .`
  — passed, 1.687 s after the tablet hint parser and span-chain changes.
- `go test -race -run '^TestTypedTLS' -count=1 -timeout=20s .` — passed, 1.013 s.

The transport tests exercise v3/v4/v5 QUERY, PREPARE, EXECUTE and BATCH wire
output against independently implemented legacy frame writers, retained input
backing, LZ4 and Snappy block decoding, 500 KB responses, headers split across
segments, every split point of row metadata, malformed/truncated metadata,
shared segment leases, independent page closure, connection closure after
delivery, orphan streams after cancellation, blocked-write shutdown, cancellation
after writer acceptance, coalescer shutdown, short writes within and between
requests, 2300-entry socket writev, and server-error unwrapping with borrowed
custom payloads plus tracing and warnings. The tablet parser is checked at every
byte split and rejects trailing bytes.

The TLS test uses real TLS 1.3 with a generated, verified certificate. A recorder
at `tls.Conn.Write` checks each plaintext slice's backing identity before the
standard library receives it. TLS record assembly/encryption and the public TLS
read API's plaintext handoff remain standard-library costs; the driver does not
gather plaintext.

`internal/segment` tests compare incremental CRC32 with a contiguous independent
checksum, decompress through `pierrec/lz4`, verify useful compression, verify
raw fallback backing identity, and keep the first segment alive across later
reads/releases. Typed paths use owned segment buffers; the legacy read-framer
adapter still materializes payloads for old callers during migration.

## Construction benchmark

Command actually run:

```
go test -run '^$' -bench '^BenchmarkTypedBuildRequest$' -benchmem -benchtime=100ms .
```

Passed in 2.017 s. Selected observed results:

| Version/input | Compression | ns/op | B/op | allocs/op | wire bytes/op |
| --- | --- | ---: | ---: | ---: | ---: |
| v4 / 32 B | off | 578.8 | 3488 | 14 | 84 |
| v4 / 4 KiB | off | 664.6 | 3488 | 14 | 4148 |
| v4 / 500 KiB | off | 693.7 | 3488 | 14 | 512052 |
| v5 / 32 B | off | 1056 | 4528 | 25 | 97 |
| v5 / 4 KiB | off | 1309 | 4528 | 25 | 4161 |
| v5 / 500 KiB | off | 27299 | 6768 | 46 | 512095 |
| v4 / 500 KiB | LZ4 | 54410 | 10088 | 23 | 2075 |
| v5 / 500 KiB | LZ4 | 57537 | 11744 | 74 | 2152 |

This benchmark measures request construction, excluding the socket. Raw v4
throughput therefore does not represent network throughput. The repeated-byte
input intentionally measures a compressible case. Pointer checks, rather than
allocation counts alone, establish the absence of assembly payload copies.
No comparison against the pinned pre-v2 benchmark baseline has been performed
by this transport worker. Database lanes are tracked by the integration owner.

## Later focused checks

- `go test -race -run '^TestTyped(EmptyContinuation|RejectsChangedMetadata)' -count=1 -timeout=30s .`
  — passed, 1.008 s. A present empty paging token remains present on the request
  wire and response page. `METADATA_CHANGED | NO_METADATA` is rejected even
  when the replacement metadata ID is empty.
- `go test -race -tags=unit -run '^TestRecvSplitFrameAllocatesExactlyOneFrameBuffer$' -count=1 -timeout=30s .`
  — passed, 1.027 s after translating this existing test to real typed ROWS.
  Its test identity and original 1.5-times-frame-size total-allocation ceiling
  remain unchanged. It now records the actual segment Read destination slices
  and checks that all returned row spans borrow those buffers and remain split.
  The previous synthetic READY/event frame exercised the legacy contiguous
  framer adapter, which intentionally remains during migration.

- `go test -race -run '^TestTypedReceiveLargeFragmentedRows$' -count=1 -timeout=30s .`
  — passed, 1.155 s after expanding the test to both random and repetitive
  500 KB payloads. This covers genuinely compressed v5 continuation segments,
  compressed v4 frames, and v5 raw fallback through the actual connection loop.
- `go test -race -run '^TestTypedProtocolLimits$' -count=1 -timeout=30s .`
  — passed, 1.008 s. Configured frame and metadata depth limits are enforced;
  oversized v4 declared expansion is rejected before Decode, and oversized v5
  declared expansion before reading/decompressing payload. Zero ConnConfig
  limits retain 256 MiB frame/decompressed and depth-64 defaults.
