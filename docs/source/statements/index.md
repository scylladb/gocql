# Executing CQL statements

`Session` is safe for concurrent use. Create a new `Query` for each execution
and attach a context so callers can cancel work or enforce a deadline.

Use `Exec` for a statement that returns no rows:

```go
err := session.Query(
	"INSERT INTO events (account_id, event_id, payload) VALUES (?, ?, ?)",
	accountID,
	eventID,
	payload,
).WithContext(ctx).Exec()
```

Use `Scan` for one row:

```go
var payload string
err := session.Query(
	"SELECT payload FROM events WHERE account_id = ? AND event_id = ?",
	accountID,
	eventID,
).WithContext(ctx).Scan(&payload)
```

Use an iterator for multiple rows. `Scanner.Err` closes the iterator and
returns its final error:

```go
scanner := session.Query(
	"SELECT event_id, payload FROM events WHERE account_id = ?",
	accountID,
).WithContext(ctx).Iter().Scanner()

for scanner.Next() {
	var eventID gocql.UUID
	var payload string
	if scanErr := scanner.Scan(&eventID, &payload); scanErr != nil {
		_ = scanner.Err() // Close the iterator before returning.
		return scanErr
	}
	// Process the row.
}
if err := scanner.Err(); err != nil {
	return err
}
```

## Prepared statements

The driver automatically prepares DML statements and caches prepared metadata.
Use bind markers for values. Every partition-key component must be a bound
value for automatic token-aware routing:

```go
query := session.Query(
	"SELECT payload FROM events WHERE account_id = ? AND event_id = ?",
	accountID,
	eventID,
)
```

Bind `?` markers positionally. Use `gocql.NamedValue` only for explicit
`:name` markers, never with a name the server reports for a `?` marker; see
[Bind markers](bind-markers.md).

With native protocol v4 or newer, pass `gocql.UnsetValue` for a bound column
that should not be updated. This lets applications reuse one prepared update
shape for optional fields.

Do not reuse a `Query` concurrently. Create queries from the shared `Session`
in each goroutine.

## SELECT without FROM

ScyllaDB 2026.4.0 and later accept a `SELECT` with no `FROM` clause, evaluating
the select list against a virtual `system.one_row` table; see ScyllaDB's CQL
reference for the syntax. Apache Cassandra has no equivalent. Such a statement
needs no keyspace of its own, returns at most one row — so `Scan` can return
`ErrNotFound` — and names unaliased columns after their selector's text, which
is what `MapScan` keys on and the reason to use `AS`.

It has no partition key, so there is no routing key and any host can serve it.
`TokenAwareHostPolicy` falls back to its wrapped policy. With a bind marker,
`Query.GetRoutingKey` also returns an error — the prepared bind-variable
metadata names table `one_row` but carries no keyspace, so the driver
substitutes the session's and looks for a table that does not exist. Execution
is unaffected, since host selection falls back on an error exactly as on a nil
key. Do not suppress it by setting a routing key: any non-nil value, including
an empty one, makes `TokenAwareHostPolicy` route by that key's token instead of
falling back, pinning every execution of the statement to one replica set.

```{toctree}
:maxdepth: 1
:hidden:

bind-markers
paging
batches
lightweight-transactions
request-timeouts
```
