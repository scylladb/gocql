# Step 01: wire/protocol foundations

Owner: `/root/step01_wire_protocol`. Branch: `v2-01-wire-protocol`.
Accepted predecessor: `f2fb206` (which contains workspace checkpoint `661f169`).
This handoff implements and verifies step 01. It does not establish completion
of the later streaming codec, exclusive connection, execution or release gates.
The source hashes and exact executed commands/statuses are retained in
`feature-specs/evidence/step01-verification.json`.

## Changed behavior

`wire.Chain` now stores original end offsets with its immutable span descriptors.
Nested/random slices binary-search that single shared index without copying
payloads or allocating child descriptors. A sequential `Cursor.Take` advances
once from its current descriptor and constructs a shared range directly; it
never invokes that binary search per cell. Construction remains one descriptor
allocation for multi-span chains, with eight additional index bytes per tail
span; single-span construction and all range operations remain allocation-free.

`wire.Stream` is an incremental, bounded envelope/child body reader. It validates
signed cell lengths and null/unset sentinels, counts, count products, nesting,
exact consumption and declared total work before exposing unsafe sizes. Scalar
scratch, bounded discard scratch and per-depth child objects are reused; failed
or incomplete children terminally poison the envelope. Parents cannot consume
bytes during a child callback. Callbacks must not retain scoped readers.
`Read`/`ReadFull` pass caller slices to the source directly. There are no payload
workers or hidden response/row/cell gathers. `Skip` handles raw framed bytes;
semantic validation of skipped typed values belongs to agent 02's compiled plans.

`frame.DecodeHeader` is shared by existing buffered framing and the incremental
native envelope reader. `frame.CheckResponseHeader` validates version, response
direction, known flags, opcode and event/stream rules; typed buffered receive
also uses it. `frame.ReadStreamEnvelope` consumes exactly the nine-byte native
header and returns a limited `wire.Stream`, rejecting frame compression and
oversized bodies without reading their payload. For v5, the transport must
supply an already verified unit source. These are reusable foundations, not a
second shared-connection reader or a completed RunStream implementation.

`segment.UnitReader` uses one fixed arena for compressed input plus decoded
output and reusable framing scratch. It checks header lengths/budgets before
payload allocation or read, checks CRC before exposure, verifies exact decoded
length and output backing identity, and reuses scratch across arbitrary unit
counts. Raw modes reserve at most 131,071 payload bytes plus 16 framing bytes;
compressed modes reserve at most 262,158 bytes by default. Positive lower
budgets apply before payload reads. Returned bytes expire at the next Next;
independently retained results continue to use the existing owned segment leases.

Existing segment helpers now reject invalid manually supplied headers and nil
scratch/required decoders before allocation/read. Equal-size compression now
selects the raw fallback, as required when transformation is not smaller.
The existing decoder-failure tests use genuine smaller LZ4 blocks so fallback
does not bypass their assertions. A narrow schema fixture synchronization fix
also protects knownKeyspaces reads with the deletion mutex; injection hooks
remain outside the lock. No production metadata behavior was changed.

## Gate evidence

| Required boundary | Evidence and result |
| --- | --- |
| Spans, ownership, release and hashing | Existing independent-oracle, split/high-byte, builder/lease and concurrent retain/release tests passed with the race detector in all five foundation packages. |
| Shared fragmented ranges | Exhaustive nested endpoint tests plus 65,536-fragment backing/index checks pass. Range and sequential-cell benchmarks report 0 B/op and 0 allocations/op. Random lookup grows from roughly 15 ns at one fragment to roughly 200 ns at 65,536, rather than scanning every preceding descriptor. |
| Incremental envelope, cell and nested framing | Every short-read chunk from 1 byte through the tested headers/scalars, truncated bodies, signed sentinels, bad counts/depth/products, overread, underread, parent-child exclusion, sticky errors and EOF-with-data pass. A gated source proves header return before body and first-cell delivery before the next cell is supplied. |
| Direct reads and discard limits | A >300 KiB body records each Read target inside the final caller array. Capacity rejection leaves its body unread. An 8 MiB discard with a 1-byte first child stays at the configured 127-byte scratch and bounded read count. Warm child scopes and raw read benchmarks allocate zero per cell. |
| Bounded v5 integrity/transform units | Raw/LZ4 split headers/payloads, multiple and maximum-size mixed units, corruption at each byte, truncated trailers, budget rejection before payload read, exact expansion and decoder-backing rejection pass. Steady unit benchmarks now report 0 B/op and 0 allocations/op; storage is constant across unit count. |
| Cancellation and failure ownership | Pre-canceled reads do not access the source. A source Read entry signal plus owner cancellation/pipe Close proves the active read returns, without a background reader retaining the destination. Full pool/Close orchestration is agent 04/07's gate. |
| Compression and hash cost | LZ4/Snappy benchmarks independently vary 1 KiB/1 MiB, 1/16/1,024 spans and repetitive/random input. Decoder oracles and existing payload-backing checks pass. One compression index allocation above the stack descriptor threshold scales with span count; payloads are never gathered. Murmur benchmarks vary sizes and span counts with zero allocations. |
| Regression and portability | Full uncached `go test -race -tags unit -count=1 -timeout=5m ./...` passes (root package 14.458 s); foundation race/vet and GOARCH=386 pass. Integration+gocql_debug binary compilation passes. |
| Malformed-input exploration | Five 3-second fuzz smoke targets passed (cursor, incremental cells, compression, integrity units and envelope); affected new targets were rerun after final production fixes. This is smoke coverage, not an exhaustive fuzz claim. |
| Documentation traceability | `python3 feature-specs/verify.py` passes: 144 modules, 418 source links, 60 reports and 174 baseline identities. This does not prove implementation semantics. |

The initial full race failure is retained in `step01-unit-race-initial.log`:
compression mock assumptions and an existing schema fixture race were corrected;
the complete uncached command then passed. Earlier unit arena measurements
identified header/trailer scratch escaping per unit; the final benchmark
`step01-wire-segment-bench.log` and
`step01-validated-units-bench-final.log` show the corrected zero-allocation path.
All evidence paths above are under `feature-specs/evidence/`.

Synthetic final-range/read/CRC benchmarks measure these primitives, not socket,
TLS or end-to-end application throughput. Setup arena/index allocations and
explicit output allocation are separate from steady counters. Later agents
must measure actual transform copies, read-ahead and socket/syscall behavior.

## Consuming the primitives

- Agent 02 wraps `wire.Stream` with the public streaming row/value contracts,
  shared scalar/type validation and typed capacity/reset/nested builders.
  Scope nesting counts Child calls; map the configured metadata/type-depth
  budget to framing scopes explicitly rather than accidentally reducing the
  permitted type depth. The callback is synchronous and cannot retain its reader.
- Agent 03 supplies prepared metadata and paging history generation fences.
- Agent 04 provides the exclusive pool reservation and acknowledged shared I/O
  handoff, context-aware I/O interruption, v5 continuation/self-contained frame
  sequencing, event/control dispatch, and safe discard/reuse. It can use
  `segment.UnitReader.Next`, `frame.DecodeHeader`/ReadStreamEnvelope and scoped
  `wire.Stream` reads without retaining prior units.
- Agent 07 installs the receive target before sending, composes the above with
  RunStream, handles caller destinations/progress/paging and prohibits replay
  after output starts. These dependencies remain explicit and unimplemented.

Step 01's foundation gate is satisfied by the retained evidence. No unresolved
step 01 finding remains. Later live-server and final-tree gates remain pending.

Evidence log formatting: Git's staged whitespace check flagged Go-emitted CPU
label padding and space-before-tab stack trace indentation. The committed log
copies expand tabs and strip trailing whitespace; commands, outcomes and
content are unchanged. Original byte-exact copies and SHA-256 hashes are
recorded in `feature-specs/evidence/step01-log-formatting.json`, with raw copies
under `/extra/scylladb/gocql-v2-execution-checkpoints/step01-raw-evidence/`.
The staged whitespace check passes after this formatting normalization.
