# Streaming into caller storage

`RunStream(ctx, session, bound, decoder, StreamOptions{})` installs a separate
incremental receive plan before query submission. `NextInto(&row)` writes final
caller storage as response bytes arrive. The standalone
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
stream, err := gocql.RunStream(ctx, session, bound, decoder, gocql.StreamOptions{})
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

A stream reserves one configured pool connection through idle time and all
pages. It disables prefetch/speculation and does not allocate a substitute pool
connection. A second stream may wait for capacity until its context expires.
Bound.Close prevents new admissions; an admitted stream keeps the binding's
drain active until exhaustion/Close. Only successful Bound.Wait permits reuse
of borrowed input. Session.Close cancels streams and joins workers; output
already in caller storage remains caller-owned.

Protocol 3/4 uncompressed raw/TLS and protocol 5 uncompressed or LZ4 raw/TLS are
supported. Raw eligible payload reads target caller arrays directly; TLS,
segment integrity and LZ4 use bounded staging/transform units. Protocol 3/4 LZ4
or Snappy frame compression is explicitly rejected with StreamCapabilityError
before streaming submission. A buffered fallback would violate this contract.
Set StreamOptions.Limits for metadata, read-ahead, discard and transform budgets;
Config.Limits bounds row/cell sizes and nesting. A limit error is terminal. StreamInfo reports delivered rows, pages and attempts.
