# gocql v2

gocql provides a typed Go driver for ScyllaDB and Apache Cassandra. Statements
pair explicit parameter encoders with result decoders. Worker-owned queries reuse an owned parameter snapshot across retries and paging.

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

Import `github.com/scylladb/gocql/v2`. The repository has one Go module;
compression and benchmarks are included in it. Optional cloud bundle support is
in `github.com/scylladb/gocql/v2/scyllacloud`.

## Typed execution

```go
config := gocql.DefaultConfig("127.0.0.1:9042")
session, err := gocql.Connect(ctx, config)
if err != nil { return err }
defer session.Close()

statement := gocql.Statement[int32, gocql.Text]{
    CQL: "SELECT name FROM app.users WHERE id = ?",
    Params: gocql.OneParam(gocql.Int32),
    Rows: gocql.OneColumn(gocql.TextCodec),
}
prepared, err := gocql.Prepare(ctx, session, statement, gocql.PrepareOptions{})
if err != nil { return err }
query, err := gocql.NewQuery(prepared, gocql.Options{})
if err != nil { return err }
defer query.Close()
rows, err := query.Run(ctx, int32(42))
if err != nil { return err }
defer rows.Close()
for rows.Next() {
    fmt.Println(rows.Row().String())
}
return rows.Err()
```

`Exec` handles statements with no result columns; `RunPage` gives explicit
paging. `Params`/`Row` compose typed struct mappings, while `DynamicRows` and
`DynamicParams` support metadata-driven applications. Custom codecs compile
against immutable `DataType`/`Columns` metadata; execution does not infer codecs
from arbitrary Go values.

## Ownership and streaming

`NewQuery` creates a reusable query for one worker. Each `Exec`, `Run`,
`RunPage` or `RunUnpaged` call copies its parameters during admission. Prepared
statements and compiled plans remain shareable. Overlapping executions return
`ErrBusy`; `query.Wait(ctx)` joins accepted writes and callbacks before reuse.
`Close` cancels the active execution and permanently stops the query.

Buffered rows borrow receive pages. Views expire at the next advancement or
Close unless a retained `PageLease` pins the page. `Clone` explicitly detaches
values. `NextInto` writes typed fields into caller structs while payloads still
borrow their pages.

`NewStreamQuery` decodes into seeded caller storage. It buffers each page on
normal multiplexed connections by default. `StreamOptions{Incremental: true}`
selects bounded incremental receive with an exclusive connection reservation. Read the
[streaming guide](docs/source/statements/streaming.md) and run the complete
[standalone example](examples/streaming/main.go):

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

The example creates and drops its own keyspace, validates every result, seeds
nested blob buffers, checks backing identity across pages, and exercises pool
query admission and transport drain. It accepts protocol, compression, authentication
and verified TLS options; see `go run ./examples/streaming -h`.

The following table describes explicit **incremental** mode. Page-buffered
queries support the normal protocol/compression combinations and retain memory
proportional to concurrent response pages, within configured frame and
decompression limits. Idle receive caching is bounded by 16 MiB total, 9 MiB
per object and 64 objects per session.

| Streaming mode | Behavior |
| --- | --- |
| Protocol 3/4, raw or TLS, no compression | Incremental receive into caller storage |
| Protocol 5, raw or TLS, no compression | Checked segments with bounded staging |
| Protocol 5, raw or TLS, LZ4 | Bounded segment decompression |
| Protocol 3/4, LZ4 or Snappy frame compression | Explicit `StreamCapabilityError` before streaming submission |

Normal buffered execution supports negotiated LZ4 and Snappy subject to native
protocol rules; protocol 5 compression uses LZ4. Configure
`config.Compression = gocql.LZ4Compression` without a compressor import.

## Configuration and extensions

`Connect` snapshots validated configuration before starting workers. Configure
TLS with `crypto/tls`, authentication with `PasswordAuth` or `AuthProvider`, and
routing/retry/speculation through `DefaultProfile`/named `ProfileHandle` values.
`Dialer`, `DialHost`, `TLSForHost`, `DNSResolver`, `AddressTranslator`,
`ReconnectPolicy`, `TokenPartitioner`, `RoutingPolicy`, observers and immutable
metadata snapshots provide typed extension points. Host/shard pool sizing,
source ports, cancellation and orphan budgets are independently configured.

Cloud users call `scyllacloud.NewCloudCluster(bundlePath, options...)`, then
`gocql.Connect(ctx, config)`. Bundle options run before SNI dial/TLS hooks capture
the dialer. Cloud routing disables incompatible source-port sharding.

## Development

```sh
go test -race -tags unit -timeout=5m ./...
go test -tags 'integration gocql_debug' -run '^$' ./...
go test -tags 'bench unit' -run '^$' -bench SingleConnection -benchtime=1x ./tests/bench
```

Replay benchmarks execute ten actual SELECT/INSERT cases using typed-control
recordings; [their recipe](tests/bench/README.md) explains regeneration. Native
integration requires [owned fixtures](tests/fixtures/README.md). Full native
acceptance and comprehensive performance gates are tracked in
[plan.md](plan.md) and the [execution ledger](feature-specs/execution/STATUS.md).
Earlier source-bound passes do not establish final release verification.

See the [documentation](docs/source/index.md),
[API contract](feature-specs/gocql-v2-api.md), [contribution guide](CONTRIBUTING.md)
and [license](LICENSE).
