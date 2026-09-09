# Speculative execution

Speculative execution starts another execution branch when an earlier branch
has not completed after a configured delay. It can reduce tail latency, but
extra executions increase cluster load. The first branch to finish, whether
successfully or with an error, determines the result returned to the caller.
The driver then cancels sibling work where cancellation is observed; a request
already sent to the server can continue running there.

Configure it for one query:

```go
query := session.Query(statement, values...).
	Idempotent(true).
	SetSpeculativeExecutionPolicy(&gocql.SimpleSpeculativeExecution{
		NumAttempts:  1,
		TimeoutDelay: 100 * time.Millisecond,
	})
```

`NumAttempts` counts additional attempts beyond the initial request. A value
of 1 permits one speculative attempt, for up to two concurrent requests while
the initial attempt remains pending. `NumAttempts` must be non-negative; zero
disables speculative execution. When it is positive, `TimeoutDelay` must be
greater than zero; otherwise query execution panics.

Use speculative execution only for idempotent operations. Choose conservative
delays from measured production latency instead of starting parallel attempts
for ordinary response times.

The context attached with `Query.WithContext` is passed to driver operations
that observe cancellation, including waits for speculative attempts. It does
not bound the complete execution: requests already sent can continue running
on the server, and speculative workers can remain blocked in retry-policy
methods that do not observe the context. Each attempt remains subject to the
query's request timeout. See [Request timeouts](statements/request-timeouts.md).
