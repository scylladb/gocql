# Request timeouts

Timeouts bound individual stages of an operation. Contexts cancel driver work
that observes them; retry-policy code must handle cancellation separately.

## Timeout settings

`NewCluster` provides these defaults:

| Setting | Default | Scope |
| --- | --- | --- |
| `Timeout` | 11 seconds | Each prepare or query response; inherited by new queries and batches |
| `ConnectTimeout` | 60 seconds | TCP dialing with the default dialer; the complete post-TLS CQL handshake; pre-finalization connection I/O; initial `querySystem`, `REGISTER`, and `USE` responses; low-level OPTIONS heartbeats on each established connection |
| `ReadTimeout` | 11 seconds | Detecting a faulty connection while reading after connection finalization |
| `WriteTimeout` | 11 seconds | Writing one request after connection finalization |
| `MetadataSchemaRequestTimeout` | 60 seconds | Post-setup requests sent through the internal `querySystem` path, including most schema metadata and tracing queries; also the control-connection manager's heartbeat |
| `MaxWaitSchemaAgreement` | 60 seconds | Waiting for schema agreement after schema changes |

Until a connection is finalized, its reader and writer use `ConnectTimeout`
instead of `ReadTimeout` and `WriteTimeout`. Initial `querySystem` metadata
requests, control-event registration, and `USE` for a configured keyspace also
use `ConnectTimeout` as their response deadline. Finalization switches to the
operational read, write, and metadata-request timeouts.

Not every internal request uses `MetadataSchemaRequestTimeout`. Client-routes
table refreshes and `DESCRIBE KEYSPACE ... WITH INTERNALS` inherit `Timeout`
through the ordinary query path.

A custom `Dialer` or `HostDialer` is responsible for enforcing its own dialing
timeout. The driver does not apply `ConnectTimeout` to calls into custom
dialers.

The default TLS handshake performed for `SslOpts` is not bounded by
`ConnectTimeout`. To bound TLS negotiation, use a `HostDialer` that applies its
own deadline to both dialing and the TLS handshake.

TCP dialing and the post-TLS CQL handshake use separate `ConnectTimeout`
budgets. Within the CQL handshake, OPTIONS, STARTUP, and all authentication
exchanges share one overall budget, although each request is also limited
individually.

Keep `WriteTimeout` less than or equal to `Timeout`. Set connection and request
timeouts above expected server-side timeouts. If a client times out first and
retries while the original server operation is still running, it can create a
retry storm and increase cluster load.

Configure cluster-wide values before creating a session:

```go
cluster := gocql.NewCluster(hosts...)
cluster.ConnectTimeout = 15 * time.Second
cluster.Timeout = 12 * time.Second
cluster.ReadTimeout = 12 * time.Second
cluster.WriteTimeout = 5 * time.Second
cluster.MetadataSchemaRequestTimeout = 30 * time.Second
cluster.MaxWaitSchemaAgreement = 60 * time.Second
```

Override the server-response timeout for one query or batch:

```go
err := session.Query(statement, values...).
	SetRequestTimeout(30 * time.Second).
	Exec()
```

`SetRequestTimeout` applies separately to statement preparation and query
execution. It does not replace a whole-operation deadline.

## Context deadlines

Use `WithContext` to cancel query attempts and page fetches:

```go
ctx, cancel := context.WithTimeout(parentCtx, 20*time.Second)
defer cancel()

err := session.Query(statement, values...).WithContext(ctx).Exec()
```

Retry-policy methods run synchronously within each execution branch and must
observe the query context themselves if they wait. In particular,
`ExponentialBackoffRetryPolicy` uses an uncancelable sleep between attempts.
Without speculative execution, a context expiring during that sleep cannot
make the call return until the sleep finishes. With [speculative
execution](../speculative-execution.md), the call can return on context
cancellation while an execution worker remains asleep until its delay finishes.

Choose a context deadline that allows intended attempts to complete but still
fits the caller's end-to-end latency budget. Account for retry-policy delays
that may postpone cancellation.
