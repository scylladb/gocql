# Streaming into caller storage

`NewStreamQuery(prepared, decoder, StreamOptions{})` creates a reusable query.
Its `Run(ctx, parameters)` method returns a cursor; `NextInto(&row)` decodes
into seeded caller storage. By default it buffers a complete response page on
normal multiplexed connections. `StreamOptions{Incremental: true}` selects
bounded incremental receive through an exclusive connection reservation. The standalone
[`examples/streaming/main.go`](../../../examples/streaming/main.go) program
creates its schema, checks exact results and backing identity, and runs through
the normal public package build.

```go
type Download struct {
    ID int32
    Payload []byte
    Pieces [][]byte
    ApplicationState string
}
decoder := gocql.StreamRow(func(r *Download) { r.ApplicationState = "" },
    gocql.StreamColumn("id", gocql.StreamInt32, func(r *Download) *int32 { return &r.ID }),
    gocql.StreamColumn("payload", gocql.StreamBlob, func(r *Download) *[]byte { return &r.Payload }),
    gocql.StreamColumn("pieces", gocql.StreamList(gocql.StreamBlob), func(r *Download) *[][]byte { return &r.Pieces }),
)
row := Download{
    Payload: make([]byte, 0, 1<<20),
    Pieces: [][]byte{make([]byte, 0, 1024), make([]byte, 0, 1024)},
}
query, err := gocql.NewStreamQuery(prepared, decoder, gocql.StreamOptions{})
if err != nil { return err }
defer query.Close()
stream, err := query.Run(ctx, parameters)
if err != nil { return err }
defer stream.Close()
for stream.NextInto(&row) {
    consume(row) // Finish synchronous access before reusing the destination.
}
return stream.Err()
```

Seed the outer slice slots **and each nested slice's capacity** before the first
NextInto. Decoders reuse capacity without growing or reallocating payloads. A
value exceeding available capacity returns StreamCapacityError before writing
that value body; earlier fields may already have changed. Provision capacity
from the application's bounds, not from untrusted remote lengths. Reset clears
logical values recursively while preserving mapped capacity. StreamRow's
explicit callback clears unmapped application state; it must preserve mapped
buffers. Custom plans/resetters have the same responsibility.

Only a true NextInto return publishes a complete validated row. Truncation,
capacity errors, cancellation, malformed text or integrity failure are terminal
and can leave partial destination data. Rows already delivered are never
replayed. Close cancels and joins in-progress NextInto destination access;
caller storage stays valid after Close. Concurrent NextInto calls are rejected.

One query permits one active execution; overlapping `Run` calls return `ErrBusy`.
Parameters are copied on admission and retries/pages share that owned snapshot.
`query.Wait(ctx)` joins accepted writes and callbacks before reuse. Close the
cursor on early termination, then wait for its query. `query.Close()` permanently
stops admission and cancels active execution. Session close cancels and joins
queries and writers. Output already decoded remains caller-owned.

Page-buffered memory is proportional to concurrent response pages and remains
bounded by configured frame/decompression limits. Incremental mode reserves
one configured connection through idle time and all pages; independent queries
may wait for pool capacity. Both modes disable prefetch/speculation.

The following receive capability and scratch limits apply to explicit
**incremental** mode. Default page buffering supports all normal buffered
protocol/compression combinations.

Protocol 3/4 uncompressed raw/TLS and protocol 5 uncompressed or LZ4 raw/TLS are
supported. Raw eligible payload reads target caller arrays directly; TLS,
segment integrity and LZ4 use bounded staging/transform units. Protocol 3/4 LZ4
or Snappy frame compression is explicitly rejected with StreamCapabilityError
before streaming submission. Incremental mode reports its capability error explicitly.
Set StreamOptions.Limits for metadata, read-ahead, discard and transform budgets;
Config.Limits bounds row/cell sizes and nesting. A limit error is terminal. StreamInfo reports delivered rows, pages and attempts.
