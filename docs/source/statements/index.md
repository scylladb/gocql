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

With native protocol v4 or newer, pass `gocql.UnsetValue` for a bound column
that should not be updated. This lets applications reuse one prepared update
shape for optional fields.

Do not reuse a `Query` concurrently. Create queries from the shared `Session`
in each goroutine.

```{toctree}
:maxdepth: 1
:hidden:

paging
batches
lightweight-transactions
request-timeouts
```
