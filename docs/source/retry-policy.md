# Retry policy configuration

A retry policy decides whether another query attempt should run after a
retryable error. Reconnection policies are separate: they restore failed host
connections and are documented under [Connecting to the
cluster](connecting/index.md).

## Configure a policy

The default is `SimpleRetryPolicy{NumRetries: 3}`. Set a cluster default or a
per-query override:

```go
cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
	NumRetries: 3,
	Min:        100 * time.Millisecond,
	Max:        2 * time.Second,
}

query := session.Query(statement, values...).RetryPolicy(
	&gocql.SimpleRetryPolicy{NumRetries: 1},
)
```

`SimpleRetryPolicy` retries normal queries on another host. When the driver has
identified a query as a lightweight transaction before its first attempt, it
retries the same host to reduce Paxos contention. Conditional statements used
with a non-token-aware host policy, `Session.Bind`, or an explicit routing key
may be identified only after execution begins. Retry methods are selected before
the first attempt, so every retry in that execution continues to use the normal
`RetryPolicy` methods and can select another host. Custom policies that need
separate LWT behavior must implement both `RetryPolicy` and `LWTRetryPolicy`;
the LWT methods are used only when the query is identified as an LWT before
execution begins.

Avoid `DowngradingConsistencyRetryPolicy` unless reduced consistency is an
explicit application requirement. It may return a result with weaker
consistency than originally requested. It does not downgrade a query already
identified as an LWT, but a conditional statement not identified before
execution can be downgraded after a preparation or binding failure.

## Idempotence

The idempotence check in built-in retry policies suppresses a retry only when
the returned `*gocql.QueryError` has `PotentiallyExecuted() == true`. The driver
sets this flag for ambiguous client-side failures, such as a request timeout or
transport error after sending the request, but not for server error responses.
In particular, `SimpleRetryPolicy` and `ExponentialBackoffRetryPolicy` can retry
a server `WRITE_TIMEOUT` even when the operation is not marked idempotent,
although some mutations may already have been applied. Use a custom policy that
rethrows such errors when repeating a non-idempotent write would be unsafe.

Mark an operation idempotent only when repeating it is safe:

```go
query := session.Query(
	"UPDATE users SET last_seen = ? WHERE id = ?",
	timestamp,
	userID,
).Idempotent(true)
```

Reads and writes that assign the same deterministic value are usually safe.
Counter increments, list appends, and other non-deterministic mutations are
not idempotent.

The driver does not add a separate idempotence guard around a custom
`RetryPolicy`. Custom policies must inspect errors passed to `GetRetryType`,
including `PotentiallyExecuted()` and `IsIdempotent()` on
`*gocql.QueryError`, and server errors such as `*gocql.RequestErrWriteTimeout`,
before choosing to retry.

`ClusterConfig.DefaultIdempotence` changes the default for every query. Prefer
per-query marking unless all application operations have been audited.

See [Request timeouts](statements/request-timeouts.md) when designing retry
budgets. A client timeout shorter than the server timeout can cause overlapping
attempts and increase cluster load.
