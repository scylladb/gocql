# Quick start

Install the single v2 module:

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

Import `github.com/scylladb/gocql/v2`. Create `DefaultConfig(contactPoints...)`
and open it with `Connect(ctx, config)`. Close the session when finished.

Declare a `Statement[P,R]` with `CQL`, `Params` and `Rows`. `Prepare` validates
its codecs against server metadata. `NewQuery(prepared, options)` creates a
worker-owned reusable query; `query.Run(ctx, parameters)` returns typed rows.
Check `Rows.Err()` after iteration and close the rows. Use `query.Exec` with
`NoRows` for statements without results. Parameters are copied on admission.
Overlapping executions return `ErrBusy`; `query.Wait(ctx)` waits for transport
and callbacks to drain. Close queries when finished.

The repository's external-package `Example` and other `example_*_test.go` files
compile against the normal public API. The complete `examples/streaming` command
creates its own schema and validates its results:

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

See [streaming](statements/streaming.md) for capacity and ownership requirements,
and [configuration](connecting/index.md) for TLS/authentication.
